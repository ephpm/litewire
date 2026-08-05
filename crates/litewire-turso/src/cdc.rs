//! Change-data-capture tail/apply API for the Turso backend.
//!
//! # Experimental
//!
//! This module implements the primitives ePHPm's Phase 2 (CDC-native
//! replication) needs from litewire. It is gated behind the same
//! experimental status as the rest of `litewire-turso`: opt-in only, no
//! default anywhere, and subject to change while turso's CDC surface
//! stabilizes upstream.
//!
//! # What this exposes
//!
//! - [`enable_cdc`] — turn on `PRAGMA capture_data_changes_conn('full')`
//!   on a single connection. **CDC is per-connection**; the primary must
//!   call this on every write session so mutations are captured.
//! - [`CdcTailer`] — polls `turso_cdc` past a monotonic `change_id`
//!   cursor and yields one [`TxnBatch`] per **complete** transaction (the
//!   v2 CDC schema emits explicit COMMIT records — partial txns are
//!   deliberately withheld).
//! - [`apply_batch`] — replay a [`TxnBatch`] on a replica connection.
//!   Idempotent: a monotonic watermark table (`__litewire_cdc_watermark`)
//!   records the highest applied `change_id` in the same transaction as
//!   the replayed rows, so a mid-batch crash replays cleanly.
//!
//! # DDL handling (empirically verified)
//!
//! Turso 0.7.0 CDC captures DDL as row mutations on `sqlite_schema`. This
//! module treats a row whose `table_name = "sqlite_schema"` as a DDL event
//! and re-executes the SQL text carried in the `sql` column of the
//! sqlite_schema record. Verified by `tests/cdc_ddl_capture.rs`.
//!
//! That SQL text arrives from a peer, so it is **not** executed as it
//! stands: [`classify_replayed_ddl`] parses it first and admits only the
//! `CREATE` forms `sqlite_schema` can legitimately hold. See that
//! function for the allowlist and its rationale.
//!
//! # DML decoding
//!
//! Row mutations carry `before`/`after` as SQLite record blobs (the same
//! format sqlite has used for two decades). This module includes a small
//! decoder for that format so we can reconstruct column values without
//! taking a dependency on `turso_core`'s private record API. See
//! [`decode_sqlite_record`].
//!
//! # Scope caveats
//!
//! - **Turso 0.7.0 has no multiprocess support** — the primary owns the
//!   file exclusively, so replicas must run in a different process
//!   entirely (they open their own file). The transport of CDC batches
//!   between processes is the *caller's* concern; this module is
//!   in-process only.
//! - Bootstrap of a fresh replica (getting the pre-CDC data before the
//!   cursor start) is the caller's concern — snapshot file copy is the
//!   intended pattern in the Phase 2 design.
//! - Retention/pruning of `turso_cdc` is the caller's concern — this
//!   module reads but never deletes CDC rows.

use crate::{Turso, map_turso_err};
use litewire_backend::{BackendError, Value};

/// Name of Turso's CDC log table.
const CDC_LOG_TABLE: &str = "turso_cdc";

/// True when `e` is Turso reporting that the CDC log table does not
/// exist yet.
///
/// `turso::Error` has no typed "relation missing" variant — a failed
/// name resolution arrives as `Error::Error(String)` carrying the
/// parser's message — so this has to match on text. Confining that match
/// to a single place is the point of having it here: before this, every
/// consumer of [`CdcTailer`] had to recognise the cold-start case for
/// itself, and a consumer that treated it as fatal tore down its
/// replication stream on every poll until the first write landed.
///
/// Both halves are required, so an unrelated missing relation or a
/// corruption error is not mistaken for a cold start.
fn is_missing_cdc_log(e: &turso::Error) -> bool {
    let msg = e.to_string();
    msg.contains("no such table") && msg.contains(CDC_LOG_TABLE)
}

/// v2 CDC schema `change_type` values, as emitted by
/// `turso_core::translate::emitter::mod::emit_cdc_insns_v2`.
mod change_type {
    /// UPDATE and SELECT-forced captures.
    pub const UPDATE: i64 = 0;
    pub const INSERT: i64 = 1;
    pub const DELETE: i64 = -1;
    /// v2 COMMIT delimiter — closes the transaction started by prior rows
    /// sharing the same `change_txn_id`.
    pub const COMMIT: i64 = 2;
}

/// Turn on full-image CDC capture on `conn`. Idempotent — the pragma may
/// be re-issued on the same connection safely.
///
/// # Errors
///
/// Returns [`BackendError::Sqlite`] if the pragma cannot execute (e.g. the
/// underlying engine rejects the mode).
pub async fn enable_cdc(conn: &turso::Connection) -> Result<(), BackendError> {
    conn.execute("PRAGMA capture_data_changes_conn('full')", ())
        .await
        .map_err(map_turso_err)?;
    Ok(())
}

/// One row from the CDC log, already decoded into a shape the ephpm
/// replication layer can serialize opaquely (bytes on the wire) and the
/// apply side can pattern-match.
#[derive(Clone, Debug)]
pub struct CdcRow {
    /// Monotonic per-database autoincrement. Serves as the replication
    /// watermark on replicas.
    pub change_id: i64,
    /// Groups rows that committed atomically. Repeats across rows of the
    /// same transaction; distinct across transactions.
    pub change_txn_id: Option<i64>,
    /// `INSERT=1, UPDATE=0, DELETE=-1, COMMIT=2` (see [`change_type`]).
    pub change_type: i64,
    /// `Some("sqlite_schema")` = DDL; `Some(name)` = DML on that table;
    /// `None` = COMMIT delimiter (no target table).
    pub table_name: Option<String>,
    /// Row id in the target table (for DML) or the sqlite_schema rowid
    /// (for DDL).
    pub id: Option<i64>,
    /// Pre-image (SQLite record blob) for UPDATE/DELETE.
    pub before: Option<Vec<u8>>,
    /// Post-image (SQLite record blob) for INSERT/UPDATE.
    pub after: Option<Vec<u8>>,
    /// Column-level change bitmap for UPDATE (see turso 0.7.0 v2 schema).
    pub updates: Option<Vec<u8>>,
}

/// A complete, atomically-committed batch of CDC rows: all the row-level
/// changes of one primary-side transaction, followed by the COMMIT
/// delimiter row.
///
/// `TxnBatch` is the unit of replication — the applier must apply the
/// entire batch or none of it.
#[derive(Clone, Debug)]
pub struct TxnBatch {
    /// All rows in original `change_id` order. The last row is the COMMIT
    /// (`change_type == 2`). Row-level changes appear first, followed by
    /// exactly one COMMIT.
    pub rows: Vec<CdcRow>,
}

impl TxnBatch {
    /// Highest `change_id` in the batch (the COMMIT record). Advancing
    /// the replica's watermark past this value marks the batch applied.
    ///
    /// `None` for an empty batch. [`CdcTailer::poll_batch`] never yields
    /// one — it cuts each batch at a COMMIT record, so there is always a
    /// last row — but a `TxnBatch` is a plain public struct and callers
    /// that build one from *elsewhere* can. ePHPm's replication link
    /// decodes batches from a network frame, and a peer sending
    /// `{"rows":[]}` used to reach an `expect` here and abort the
    /// applying task. Reporting the absence lets every caller decide;
    /// [`apply_batch`] turns it into a [`BackendError::Other`].
    #[must_use]
    pub fn commit_change_id(&self) -> Option<i64> {
        self.rows.last().map(|r| r.change_id)
    }

    /// Transaction id shared by every non-COMMIT row in the batch.
    /// `None` if the batch is degenerate (COMMIT-only, e.g. an empty
    /// autocommit boundary).
    #[must_use]
    pub fn txn_id(&self) -> Option<i64> {
        self.rows
            .iter()
            .find(|r| r.change_type != change_type::COMMIT)
            .and_then(|r| r.change_txn_id)
    }
}

/// Polls `turso_cdc` past a monotonic cursor and produces one
/// [`TxnBatch`] per completed primary-side transaction.
///
/// The tailer is stateless w.r.t. the underlying engine — it holds only
/// the cursor `last_seen_change_id`. Create one per stream (per vhost /
/// per shard), keep it alive across polls, and advance the cursor via
/// [`Self::poll_batch`].
pub struct CdcTailer<'a> {
    factory: &'a Turso,
    last_seen_change_id: i64,
}

impl<'a> CdcTailer<'a> {
    /// Start tailing after `after_change_id` — the caller's persisted
    /// replication watermark. Use `0` to tail from the very first CDC
    /// row.
    #[must_use]
    pub fn new(factory: &'a Turso, after_change_id: i64) -> Self {
        Self {
            factory,
            last_seen_change_id: after_change_id,
        }
    }

    /// Current cursor: the highest `change_id` this tailer has yielded.
    #[must_use]
    pub fn cursor(&self) -> i64 {
        self.last_seen_change_id
    }

    /// Advance the cursor by one **complete** transaction, if one is
    /// available. Returns `Ok(None)` when no complete batch is pending —
    /// callers should poll again after a brief pause (or on a wakeup
    /// signal if one is wired in).
    ///
    /// A batch is "complete" iff it ends in a COMMIT record. Partial
    /// transactions (row rows without a trailing COMMIT) are deliberately
    /// held back so replicas never observe an uncommitted state.
    ///
    /// `Ok(None)` also covers the case where the `turso_cdc` log **does
    /// not exist yet**. Turso creates it lazily, on the first captured
    /// write — not when CDC is enabled and not when a session connects —
    /// so a database that has served no writes has no log, and "absent"
    /// carries exactly the same information as "empty": nothing to ship.
    /// Reporting that as an error made every caller responsible for
    /// distinguishing a cold start from a real fault, and a caller that
    /// treated it as fatal would tear down its replication stream on a
    /// freshly provisioned cluster and retry forever.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Sqlite`] if the underlying poll fails for
    /// any reason other than the log not existing yet.
    /// Malformed rows (missing required columns, wrong types) produce
    /// [`BackendError::Other`] — Phase 2 pins turso exactly, so this
    /// should only fire on a schema change we haven't caught up to.
    pub async fn poll_batch(&mut self) -> Result<Option<TxnBatch>, BackendError> {
        // Read up to a bounded window of rows so a single batch cannot
        // starve the tailer if the primary is committing very fast.
        const WINDOW: i64 = 1024;

        let conn = self.factory.db.connect().map_err(map_turso_err)?;
        let prepared = conn
            .prepare(
                "SELECT change_id, change_txn_id, change_type, table_name, id, \
                        before, after, updates \
                 FROM turso_cdc \
                 WHERE change_id > ? \
                 ORDER BY change_id \
                 LIMIT ?",
            )
            .await;
        let mut stmt = match prepared {
            Ok(stmt) => stmt,
            // No log table yet => no changes captured yet. Mirrors
            // `read_watermark`, which already reports 0 rather than
            // failing when its table is absent.
            Err(e) if is_missing_cdc_log(&e) => return Ok(None),
            Err(e) => return Err(map_turso_err(e)),
        };
        let mut rows = stmt
            .query((self.last_seen_change_id, WINDOW))
            .await
            .map_err(map_turso_err)?;

        let mut buffered: Vec<CdcRow> = Vec::new();
        // Find the *first* COMMIT and cut the batch there; drop trailing
        // rows on the floor (the next poll will re-read them starting from
        // the advanced cursor).
        let mut commit_idx: Option<usize> = None;

        let mut idx = 0usize;
        while let Some(row) = rows.next().await.map_err(map_turso_err)? {
            let cdc_row = decode_cdc_row(&row)?;
            let is_commit = cdc_row.change_type == change_type::COMMIT;
            buffered.push(cdc_row);
            if is_commit {
                commit_idx = Some(idx);
                break;
            }
            idx += 1;
        }

        // No COMMIT in the window → no complete batch yet.
        let Some(commit_idx) = commit_idx else {
            return Ok(None);
        };

        buffered.truncate(commit_idx + 1);
        // Read the cursor off the COMMIT row before the vec moves into the
        // batch, so this path needs no unwrap on the now-`Option` accessor.
        let commit_change_id = buffered[commit_idx].change_id;
        let batch = TxnBatch { rows: buffered };
        self.last_seen_change_id = commit_change_id;
        Ok(Some(batch))
    }
}

/// Decode one row of `turso_cdc` (in the SELECT order used by
/// [`CdcTailer::poll_batch`]) into a [`CdcRow`].
fn decode_cdc_row(row: &turso::Row) -> Result<CdcRow, BackendError> {
    let change_id = match row.get_value(0).map_err(map_turso_err)? {
        turso::Value::Integer(i) => i,
        v => {
            return Err(BackendError::Other(format!(
                "cdc: bad change_id type: {v:?}"
            )));
        }
    };
    let change_txn_id = match row.get_value(1).map_err(map_turso_err)? {
        turso::Value::Integer(i) => Some(i),
        turso::Value::Null => None,
        v => {
            return Err(BackendError::Other(format!(
                "cdc: bad change_txn_id type: {v:?}"
            )));
        }
    };
    let change_type = match row.get_value(2).map_err(map_turso_err)? {
        turso::Value::Integer(i) => i,
        v => {
            return Err(BackendError::Other(format!(
                "cdc: bad change_type type: {v:?}"
            )));
        }
    };
    let table_name = match row.get_value(3).map_err(map_turso_err)? {
        turso::Value::Text(s) => Some(s),
        turso::Value::Null => None,
        v => {
            return Err(BackendError::Other(format!(
                "cdc: bad table_name type: {v:?}"
            )));
        }
    };
    let id = match row.get_value(4).map_err(map_turso_err)? {
        turso::Value::Integer(i) => Some(i),
        turso::Value::Null => None,
        v => return Err(BackendError::Other(format!("cdc: bad id type: {v:?}"))),
    };
    let before = optional_blob(row, 5)?;
    let after = optional_blob(row, 6)?;
    let updates = optional_blob(row, 7)?;
    Ok(CdcRow {
        change_id,
        change_txn_id,
        change_type,
        table_name,
        id,
        before,
        after,
        updates,
    })
}

fn optional_blob(row: &turso::Row, i: usize) -> Result<Option<Vec<u8>>, BackendError> {
    match row.get_value(i).map_err(map_turso_err)? {
        turso::Value::Blob(b) => Ok(Some(b)),
        turso::Value::Null => Ok(None),
        v => Err(BackendError::Other(format!(
            "cdc: blob column {i} has non-blob non-null type: {v:?}"
        ))),
    }
}

/// Ensure the watermark table exists on the replica. Called once on
/// [`apply_batch`] first invocation; cheap to call every time (CREATE IF
/// NOT EXISTS is idempotent).
async fn ensure_watermark_table(conn: &turso::Connection) -> Result<(), BackendError> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __litewire_cdc_watermark (\
            id INTEGER PRIMARY KEY CHECK (id = 0), \
            applied_change_id INTEGER NOT NULL)",
        (),
    )
    .await
    .map_err(map_turso_err)?;
    conn.execute(
        "INSERT OR IGNORE INTO __litewire_cdc_watermark (id, applied_change_id) VALUES (0, 0)",
        (),
    )
    .await
    .map_err(map_turso_err)?;
    Ok(())
}

/// Read the replica's applied watermark. `0` if the table has not been
/// created yet or has never been advanced.
///
/// # Errors
///
/// Returns [`BackendError::Sqlite`] if the read fails for any reason other
/// than a missing table (a missing table returns 0).
pub async fn read_watermark(conn: &turso::Connection) -> Result<i64, BackendError> {
    // Best-effort: if the table doesn't exist, treat as 0.
    let mut stmt = match conn
        .prepare("SELECT applied_change_id FROM __litewire_cdc_watermark WHERE id = 0")
        .await
    {
        Ok(s) => s,
        Err(_) => return Ok(0),
    };
    let mut rows = stmt.query(()).await.map_err(map_turso_err)?;
    match rows.next().await.map_err(map_turso_err)? {
        Some(row) => match row.get_value(0).map_err(map_turso_err)? {
            turso::Value::Integer(i) => Ok(i),
            _ => Ok(0),
        },
        None => Ok(0),
    }
}

/// Apply one committed [`TxnBatch`] to `replica_conn`.
///
/// The apply runs inside a single `BEGIN IMMEDIATE ... COMMIT` on the
/// replica so the batch is exactly-once: the watermark advances in the
/// same transaction as the replayed rows. If `batch.commit_change_id() <=
/// current_watermark`, the batch is skipped as a no-op (idempotent).
///
/// # Errors
///
/// Returns [`BackendError`] if the apply fails at any step. On failure
/// the replica transaction is rolled back and the watermark is not
/// advanced — the caller should retry with the same batch.
///
/// An **empty** batch is a [`BackendError::Other`], not a silent no-op:
/// a `TxnBatch` is by definition a run of row changes terminated by a
/// COMMIT record, so a batch with no rows is a malformed one and the
/// caller that produced it should hear about it.
///
/// # Panics
///
/// This function does not panic; malformed CDC rows and empty batches
/// surface as [`BackendError::Other`].
pub async fn apply_batch(
    replica_conn: &turso::Connection,
    batch: &TxnBatch,
) -> Result<(), BackendError> {
    let Some(commit_change_id) = batch.commit_change_id() else {
        return Err(BackendError::Other(
            "cdc apply: empty batch (a TxnBatch always ends in a COMMIT row)".into(),
        ));
    };

    ensure_watermark_table(replica_conn).await?;

    let current = read_watermark(replica_conn).await?;
    if commit_change_id <= current {
        // Already applied; idempotent no-op.
        return Ok(());
    }

    // BEGIN IMMEDIATE takes a write lock immediately, matching what turso's
    // own sync engine does for replay sessions.
    replica_conn
        .execute("BEGIN IMMEDIATE", ())
        .await
        .map_err(map_turso_err)?;

    let result = apply_batch_inner(replica_conn, batch, current, commit_change_id).await;
    match result {
        Ok(()) => {
            replica_conn
                .execute("COMMIT", ())
                .await
                .map_err(map_turso_err)?;
        }
        Err(e) => {
            // Best-effort rollback; propagate the original error.
            let _ = replica_conn.execute("ROLLBACK", ()).await;
            return Err(e);
        }
    }
    Ok(())
}

async fn apply_batch_inner(
    conn: &turso::Connection,
    batch: &TxnBatch,
    current_watermark: i64,
    commit_change_id: i64,
) -> Result<(), BackendError> {
    for row in &batch.rows {
        // Skip rows the replica already applied inside a prior partial
        // attempt (defense-in-depth; the outer transaction should have
        // rolled them back, but this stays correct even if it didn't).
        if row.change_id <= current_watermark {
            continue;
        }

        match row.change_type {
            change_type::COMMIT => {
                // Handled by advancing the watermark below.
            }
            change_type::INSERT | change_type::UPDATE | change_type::DELETE => {
                match row.table_name.as_deref() {
                    Some("sqlite_schema") => apply_ddl(conn, row).await?,
                    Some(_) => apply_dml(conn, row).await?,
                    None => {
                        return Err(BackendError::Other(format!(
                            "cdc apply: row without table_name (change_id={})",
                            row.change_id
                        )));
                    }
                }
            }
            other => {
                return Err(BackendError::Other(format!(
                    "cdc apply: unknown change_type {other} for change_id={}",
                    row.change_id
                )));
            }
        }
    }

    // Advance the watermark in the same transaction as the replayed rows.
    let mut stmt = conn
        .prepare("UPDATE __litewire_cdc_watermark SET applied_change_id = ? WHERE id = 0")
        .await
        .map_err(map_turso_err)?;
    stmt.execute((commit_change_id,))
        .await
        .map_err(map_turso_err)?;
    Ok(())
}

/// Replay a DDL event captured as a mutation on sqlite_schema.
///
/// sqlite_schema columns: `(type, name, tbl_name, rootpage, sql)`.
/// Only INSERT/UPDATE carry an after-image with the SQL text; DELETE
/// events are DROP TABLE / DROP INDEX and we synthesize the statement
/// from the before-image `type` + `name`.
///
/// The INSERT/UPDATE text is peer-supplied and goes through
/// [`classify_replayed_ddl`] before it reaches the engine. The DELETE
/// branch is not peer-supplied in the same sense — the statement is
/// built here from a fixed template with the identifier escaped — so it
/// needs no allowlist.
async fn apply_ddl(conn: &turso::Connection, row: &CdcRow) -> Result<(), BackendError> {
    match row.change_type {
        change_type::INSERT | change_type::UPDATE => {
            let after = row.after.as_ref().ok_or_else(|| {
                BackendError::Other("cdc apply: DDL INSERT/UPDATE missing after-image".into())
            })?;
            let cols = decode_sqlite_record(after)?;
            // sqlite_schema.sql is column index 4.
            let sql = match cols.get(4) {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => {
                    // e.g. autoindex entries have NULL sql — skip.
                    return Ok(());
                }
                Some(other) => {
                    return Err(BackendError::Other(format!(
                        "cdc apply: sqlite_schema.sql is not text: {other:?}"
                    )));
                }
                None => {
                    return Err(BackendError::Other(format!(
                        "cdc apply: sqlite_schema record too short ({} cols)",
                        cols.len()
                    )));
                }
            };
            if classify_replayed_ddl(&sql)? == SchemaObject::Trigger {
                tracing::warn!(
                    change_id = row.change_id,
                    "cdc apply: not replaying a CREATE TRIGGER — CDC already carries the \
                     rows the trigger produced on the primary, so recreating it here would \
                     double-apply them"
                );
                return Ok(());
            }
            conn.execute(&sql, ()).await.map_err(map_turso_err)?;
        }
        change_type::DELETE => {
            let before = row.before.as_ref().ok_or_else(|| {
                BackendError::Other("cdc apply: DDL DELETE missing before-image".into())
            })?;
            let cols = decode_sqlite_record(before)?;
            let entity_type = match cols.first() {
                Some(Value::Text(s)) => s.clone(),
                other => {
                    return Err(BackendError::Other(format!(
                        "cdc apply: sqlite_schema.type not text: {other:?}"
                    )));
                }
            };
            let name = match cols.get(1) {
                Some(Value::Text(s)) => s.clone(),
                other => {
                    return Err(BackendError::Other(format!(
                        "cdc apply: sqlite_schema.name not text: {other:?}"
                    )));
                }
            };
            // Only replay top-level DROP for tables/indexes; skip
            // autoindex/trigger side-effects (they follow their parent).
            let sql = match entity_type.as_str() {
                "table" => format!("DROP TABLE IF EXISTS \"{}\"", escape_ident(&name)),
                "index" => format!("DROP INDEX IF EXISTS \"{}\"", escape_ident(&name)),
                _ => return Ok(()),
            };
            conn.execute(&sql, ()).await.map_err(map_turso_err)?;
        }
        _ => {}
    }
    Ok(())
}

fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

// ---------------------------------------------------------------------------
// Replayed-DDL allowlist.
// ---------------------------------------------------------------------------

/// A schema object that a CDC `sqlite_schema` row may legitimately
/// create — the complete set [`classify_replayed_ddl`] admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaObject {
    /// `CREATE TABLE`.
    Table,
    /// `CREATE INDEX` or `CREATE UNIQUE INDEX`.
    Index,
    /// `CREATE VIEW`.
    View,
    /// `CREATE TRIGGER`. Recognised as legitimate so it can be told apart
    /// from an attack, but deliberately **not replayed** — see
    /// [`apply_batch`] and the note on [`classify_replayed_ddl`].
    Trigger,
}

impl SchemaObject {
    /// The keyword form, for error and log messages.
    #[must_use]
    fn as_str(self) -> &'static str {
        match self {
            SchemaObject::Table => "TABLE",
            SchemaObject::Index => "INDEX",
            SchemaObject::View => "VIEW",
            SchemaObject::Trigger => "TRIGGER",
        }
    }
}

/// Classify the `sqlite_schema.sql` text of a replayed DDL event, and
/// reject anything outside the allowlist.
///
/// # Why this exists
///
/// The text is peer-supplied. A CDC batch reaches [`apply_batch`] only
/// from a node that already holds the cluster credential, so it is
/// entitled to dictate the replica's *data* — but before this check it
/// could also dictate arbitrary *SQL*, including `ATTACH` (open or
/// create a file of its choosing) and `PRAGMA` (retarget the journal,
/// disable integrity behaviour, and in some builds load an extension).
/// That is a strictly larger capability than "corrupt some rows", and it
/// is the gap this closes. The DML path never had it: values bind as
/// parameters and identifiers go through [`escape_ident`].
///
/// # What is allowed, and why that is the whole set
///
/// `sqlite_schema.sql` can only ever hold a `CREATE`. That is not a
/// policy choice, it is what SQLite stores:
///
/// - `CREATE TABLE` / `CREATE [UNIQUE] INDEX` / `CREATE VIEW` /
///   `CREATE TRIGGER` — the four object kinds with a stored `sql` text.
/// - `ALTER TABLE ... ADD COLUMN` (and `RENAME`) does **not** appear as
///   `ALTER` text. SQLite rewrites the object's stored `CREATE TABLE`
///   and the change surfaces as an UPDATE of that row, so the replay
///   text is still a `CREATE`.
/// - `DROP` does not appear here either: it arrives as a DELETE on the
///   schema row, and that branch of [`apply_ddl`] synthesizes its own
///   `DROP ... IF EXISTS` from a fixed template.
/// - Auto-indexes carry a NULL `sql` and are skipped before this point.
///
/// So everything else — `ATTACH`, `PRAGMA`, `BEGIN`/`COMMIT`/`SAVEPOINT`,
/// `INSERT`/`UPDATE`/`DELETE`, `SELECT`, a literal `DROP` or `ALTER`,
/// `CREATE TEMP ...` (temp objects live in `sqlite_temp_schema`, never
/// here) — cannot be a genuine CDC schema replay and is refused.
///
/// `CREATE VIRTUAL TABLE` is refused too, and that one is a judgement
/// call rather than a structural impossibility: it names a module and
/// hands it arbitrary arguments at replay time, which is a different
/// capability from building a b-tree and one whose blast radius depends
/// on which modules the replica's build happens to register. Turso
/// 0.7.0's CDC behaviour over virtual tables is unverified either way,
/// so refusing costs nothing that works today.
///
/// # One statement, not many
///
/// SQLite stores exactly one statement per schema row, without a
/// trailing `;`. A payload carrying a second statement is therefore
/// smuggling, and the scan that detects it is quote- and comment-aware:
/// `'…'` (with `''` escapes), `"…"`, `` `…` ``, `[…]`, `-- …` and
/// `/* … */` are all runs in which a `;` is not a separator. An
/// unterminated quote or block comment is refused rather than guessed
/// at.
///
/// `CREATE TRIGGER` is exempt from the separator check, because a
/// trigger body legitimately contains `;` between `BEGIN` and `END` —
/// and it is exempt safely, because a trigger is never executed on the
/// replay path. Recreating it would double-apply the row effects CDC
/// already carries from the primary, so [`apply_ddl`] logs and skips it,
/// matching what ePHPm's snapshot bootstrap does with triggers.
///
/// # Errors
///
/// Returns [`BackendError::Other`] naming the offending statement
/// (truncated) when the text is not a single allowlisted `CREATE`.
pub fn classify_replayed_ddl(sql: &str) -> Result<SchemaObject, BackendError> {
    let object = classify_leading_keywords(sql)?;
    if object != SchemaObject::Trigger {
        ensure_single_statement(sql, object)?;
    }
    Ok(object)
}

/// Read the `CREATE <kind>` preamble and map it to a [`SchemaObject`].
fn classify_leading_keywords(sql: &str) -> Result<SchemaObject, BackendError> {
    let words = leading_keywords(sql, 3);
    let reject = |reason: &str| {
        Err(BackendError::Other(format!(
            "cdc apply: replayed DDL rejected ({reason}); only CREATE \
             TABLE/[UNIQUE] INDEX/VIEW/TRIGGER may be replayed: {}",
            truncate_for_log(sql)
        )))
    };

    match words.first().map(String::as_str) {
        Some("CREATE") => {}
        Some(other) => return reject(&format!("leading keyword {other}")),
        None => return reject("no leading keyword"),
    }
    match words.get(1).map(String::as_str) {
        Some("TABLE") => Ok(SchemaObject::Table),
        Some("INDEX") => Ok(SchemaObject::Index),
        Some("VIEW") => Ok(SchemaObject::View),
        Some("TRIGGER") => Ok(SchemaObject::Trigger),
        // The only modifier SQLite stores between CREATE and the object
        // kind. TEMP/TEMPORARY and VIRTUAL fall through to the reject.
        Some("UNIQUE") => match words.get(2).map(String::as_str) {
            Some("INDEX") => Ok(SchemaObject::Index),
            Some(other) => reject(&format!("CREATE UNIQUE {other}")),
            None => reject("CREATE UNIQUE with no object kind"),
        },
        Some(other) => reject(&format!("CREATE {other}")),
        None => reject("CREATE with no object kind"),
    }
}

/// Refuse a payload that carries a second statement.
///
/// Quote- and comment-aware, so a `;` inside a string literal, a quoted
/// identifier, or a comment is not mistaken for a separator. An
/// unterminated quote or block comment is an error: the payload cannot be
/// reasoned about, so it does not get the benefit of the doubt.
fn ensure_single_statement(sql: &str, object: SchemaObject) -> Result<(), BackendError> {
    let bytes = sql.as_bytes();
    let mut first_semicolon: Option<usize> = None;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            // Doubling (`''`, `""`, ` `` `) escapes the quote character.
            quote @ (b'\'' | b'"' | b'`') => {
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err(BackendError::Other(format!(
                            "cdc apply: unterminated quoted literal in replayed DDL: {}",
                            truncate_for_log(sql)
                        )));
                    }
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            // SQLite also accepts MSSQL-style `[identifier]`. There is no
            // escape form — the first `]` closes it.
            b'[' => {
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err(BackendError::Other(format!(
                            "cdc apply: unterminated bracketed identifier in replayed DDL: {}",
                            truncate_for_log(sql)
                        )));
                    }
                    let closed = bytes[i] == b']';
                    i += 1;
                    if closed {
                        break;
                    }
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                loop {
                    if i + 1 >= bytes.len() {
                        return Err(BackendError::Other(format!(
                            "cdc apply: unterminated block comment in replayed DDL: {}",
                            truncate_for_log(sql)
                        )));
                    }
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b';' => {
                if first_semicolon.is_none() {
                    first_semicolon = Some(i);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }

    // A single trailing `;` is tolerated (SQLite does not store one, but
    // refusing it would be pedantry); anything of substance after it is
    // a second statement.
    if let Some(pos) = first_semicolon {
        if !skip_blank_and_comments(&sql[pos + 1..]).is_empty() {
            return Err(BackendError::Other(format!(
                "cdc apply: replayed CREATE {} carries more than one statement: {}",
                object.as_str(),
                truncate_for_log(sql)
            )));
        }
    }
    Ok(())
}

/// The first `n` bare keywords at the head of `sql`, upper-cased, with
/// whitespace and comments between them skipped. Stops at the first
/// token that is not a bare word — a quote, a paren, a digit — since
/// none of those can be part of a `CREATE <kind>` preamble.
fn leading_keywords(sql: &str, n: usize) -> Vec<String> {
    let mut s = sql;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        s = skip_blank_and_comments(s);
        let len = s.bytes().take_while(u8::is_ascii_alphabetic).count();
        if len == 0 {
            break;
        }
        out.push(s[..len].to_ascii_uppercase());
        s = &s[len..];
    }
    out
}

/// Drop leading whitespace and SQL comments. Returns `""` when the input
/// is nothing but whitespace and comments, including an unterminated
/// comment — which by definition swallows everything after it.
fn skip_blank_and_comments(s: &str) -> &str {
    let mut s = s;
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            let Some(nl) = rest.find('\n') else { return "" };
            s = &rest[nl + 1..];
        } else if let Some(rest) = s.strip_prefix("/*") {
            let Some(end) = rest.find("*/") else {
                return "";
            };
            s = &rest[end + 2..];
        } else {
            return s;
        }
    }
}

/// First 120 characters of `s`, so an error message cannot dump a
/// multi-megabyte payload into the log.
fn truncate_for_log(s: &str) -> String {
    const LIMIT: usize = 120;
    if s.chars().count() <= LIMIT {
        return s.to_string();
    }
    let head: String = s.chars().take(LIMIT).collect();
    format!("{head}…")
}

/// Replay one row-level DML captured on a user table.
///
/// The row id (`row.id`) is authoritative — we key INSERT/DELETE by rowid
/// and rely on the same rowid landing on the replica. This assumes
/// primary and replica evolve deterministically from the same starting
/// state (bootstrap file copy + linear CDC apply), which is the whole
/// replication invariant.
async fn apply_dml(conn: &turso::Connection, row: &CdcRow) -> Result<(), BackendError> {
    let table = row
        .table_name
        .as_ref()
        .expect("table_name checked by caller");
    let rowid = row.id.ok_or_else(|| {
        BackendError::Other(format!(
            "cdc apply: DML row without id (change_id={})",
            row.change_id
        ))
    })?;

    match row.change_type {
        change_type::INSERT => {
            let after = row.after.as_ref().ok_or_else(|| {
                BackendError::Other("cdc apply: INSERT missing after-image".into())
            })?;
            let values = decode_sqlite_record(after)?;
            let placeholders: Vec<&'static str> = std::iter::repeat_n("?", values.len()).collect();
            let sql = format!(
                "INSERT OR REPLACE INTO \"{}\" (rowid, {}) VALUES (?, {})",
                escape_ident(table),
                column_list(conn, table, values.len()).await?,
                placeholders.join(", "),
            );
            let mut params: Vec<turso::Value> = Vec::with_capacity(1 + values.len());
            params.push(turso::Value::Integer(rowid));
            for v in values {
                params.push(to_turso_value(v));
            }
            let mut stmt = conn.prepare(&sql).await.map_err(map_turso_err)?;
            stmt.execute(turso::params::Params::Positional(params))
                .await
                .map_err(map_turso_err)?;
        }
        change_type::UPDATE => {
            // UPDATE with full after-image — replay as INSERT OR REPLACE
            // by rowid. The `updates` column-change bitmap would allow a
            // narrower UPDATE, but full replacement is correct and the
            // simplest thing that works.
            let after = row.after.as_ref().ok_or_else(|| {
                BackendError::Other("cdc apply: UPDATE missing after-image".into())
            })?;
            let values = decode_sqlite_record(after)?;
            let sql = format!(
                "INSERT OR REPLACE INTO \"{}\" (rowid, {}) VALUES (?, {})",
                escape_ident(table),
                column_list(conn, table, values.len()).await?,
                std::iter::repeat_n("?", values.len())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            let mut params: Vec<turso::Value> = Vec::with_capacity(1 + values.len());
            params.push(turso::Value::Integer(rowid));
            for v in values {
                params.push(to_turso_value(v));
            }
            let mut stmt = conn.prepare(&sql).await.map_err(map_turso_err)?;
            stmt.execute(turso::params::Params::Positional(params))
                .await
                .map_err(map_turso_err)?;
        }
        change_type::DELETE => {
            let sql = format!("DELETE FROM \"{}\" WHERE rowid = ?", escape_ident(table));
            let mut stmt = conn.prepare(&sql).await.map_err(map_turso_err)?;
            stmt.execute((rowid,)).await.map_err(map_turso_err)?;
        }
        _ => {}
    }
    Ok(())
}

fn to_turso_value(v: Value) -> turso::Value {
    match v {
        Value::Null => turso::Value::Null,
        Value::Integer(i) => turso::Value::Integer(i),
        Value::Float(f) => turso::Value::Real(f),
        Value::Text(s) => turso::Value::Text(s),
        Value::Blob(b) => turso::Value::Blob(b),
    }
}

/// Look up the first `n` column names of `table` from `sqlite_schema` on
/// the replica. Rowid-alias columns and virtual generated columns are
/// intentionally *not* filtered here — the CDC record we decoded already
/// omitted virtual-generated columns (see turso `emit_cdc_full_record`),
/// so we ask for exactly the storable columns.
async fn column_list(
    conn: &turso::Connection,
    table: &str,
    n: usize,
) -> Result<String, BackendError> {
    // PRAGMA table_info returns storable columns in the same order the
    // record encodes them.
    let pragma_sql = format!("PRAGMA table_info(\"{}\")", escape_ident(table));
    let mut stmt = conn.prepare(&pragma_sql).await.map_err(map_turso_err)?;
    let mut rows = stmt.query(()).await.map_err(map_turso_err)?;
    let mut names = Vec::with_capacity(n);
    while let Some(row) = rows.next().await.map_err(map_turso_err)? {
        // Column 1 = name.
        let name = match row.get_value(1).map_err(map_turso_err)? {
            turso::Value::Text(s) => s,
            v => {
                return Err(BackendError::Other(format!(
                    "cdc apply: table_info.name not text: {v:?}"
                )));
            }
        };
        names.push(format!("\"{}\"", escape_ident(&name)));
        if names.len() == n {
            break;
        }
    }
    if names.len() != n {
        return Err(BackendError::Other(format!(
            "cdc apply: table {table} has {} storable columns but record has {n}",
            names.len()
        )));
    }
    Ok(names.join(", "))
}

// ---------------------------------------------------------------------------
// SQLite record format decoder.
//
// The record format is stable and documented at
// <https://www.sqlite.org/fileformat2.html#record_format>.
//
// A record is:
//   header:
//     header-length varint (total header bytes, including this varint)
//     N serial-type varints, one per column
//   body:
//     concatenated column values in the encoding named by their serial types
//
// Serial types (subset relevant to Turso's stored values):
//   0        NULL
//   1..=6    signed big-endian integer of 1,2,3,4,6,8 bytes
//   7        big-endian IEEE 754 double
//   8, 9     literal integers 0 and 1 (no body bytes)
//   10, 11   reserved (Turso doesn't emit these)
//   >=12 even  BLOB of ((n-12)/2) bytes
//   >=13 odd   TEXT of ((n-13)/2) bytes (UTF-8, unless a nondefault
//              database text encoding is in use — Turso 0.7.0 uses UTF-8)
// ---------------------------------------------------------------------------

/// Decode a SQLite record blob into a positional vector of values.
///
/// # Errors
///
/// Returns [`BackendError::Other`] if the blob is truncated, malformed, or
/// uses a serial type the decoder does not understand (reserved 10/11).
pub fn decode_sqlite_record(blob: &[u8]) -> Result<Vec<Value>, BackendError> {
    let mut pos = 0usize;
    let (header_len, n) = read_varint(blob, pos)?;
    pos += n;
    let header_end = header_len as usize;
    if header_end > blob.len() {
        return Err(BackendError::Other(format!(
            "sqlite record: header length {header_end} > blob length {}",
            blob.len()
        )));
    }

    let mut serial_types: Vec<u64> = Vec::new();
    while pos < header_end {
        let (st, n) = read_varint(blob, pos)?;
        pos += n;
        serial_types.push(st);
    }

    // Body starts at header_end.
    let mut body = header_end;
    let mut out = Vec::with_capacity(serial_types.len());
    for st in serial_types {
        let (val, consumed) = decode_serial(blob, body, st)?;
        body += consumed;
        out.push(val);
    }
    Ok(out)
}

/// SQLite varint: 1-9 bytes, high bit continuation, 7 bits per byte
/// except the 9th which contributes 8 bits.
fn read_varint(blob: &[u8], mut pos: usize) -> Result<(u64, usize), BackendError> {
    let start = pos;
    let mut result: u64 = 0;
    for i in 0..9 {
        if pos >= blob.len() {
            return Err(BackendError::Other(
                "sqlite record: truncated varint".into(),
            ));
        }
        let byte = blob[pos];
        pos += 1;
        if i < 8 {
            result = (result << 7) | u64::from(byte & 0x7F);
            if byte & 0x80 == 0 {
                return Ok((result, pos - start));
            }
        } else {
            result = (result << 8) | u64::from(byte);
            return Ok((result, pos - start));
        }
    }
    unreachable!("varint loop terminates in <=9 iterations")
}

fn decode_serial(blob: &[u8], pos: usize, serial: u64) -> Result<(Value, usize), BackendError> {
    fn ensure(blob: &[u8], pos: usize, n: usize) -> Result<(), BackendError> {
        if pos.checked_add(n).is_none_or(|end| end > blob.len()) {
            return Err(BackendError::Other(format!(
                "sqlite record: truncated body (need {n} at {pos}, have {})",
                blob.len()
            )));
        }
        Ok(())
    }
    match serial {
        0 => Ok((Value::Null, 0)),
        1 => {
            ensure(blob, pos, 1)?;
            Ok((Value::Integer(i64::from(blob[pos] as i8)), 1))
        }
        2 => {
            ensure(blob, pos, 2)?;
            let bytes = [blob[pos], blob[pos + 1]];
            Ok((Value::Integer(i64::from(i16::from_be_bytes(bytes))), 2))
        }
        3 => {
            ensure(blob, pos, 3)?;
            // Sign-extend 24-bit
            let hi = i32::from(blob[pos] as i8);
            let v = (hi << 16) | ((i32::from(blob[pos + 1])) << 8) | i32::from(blob[pos + 2]);
            Ok((Value::Integer(i64::from(v)), 3))
        }
        4 => {
            ensure(blob, pos, 4)?;
            let bytes = [blob[pos], blob[pos + 1], blob[pos + 2], blob[pos + 3]];
            Ok((Value::Integer(i64::from(i32::from_be_bytes(bytes))), 4))
        }
        5 => {
            ensure(blob, pos, 6)?;
            // Sign-extend 48-bit
            let hi = i64::from(blob[pos] as i8);
            let mut v: i64 = hi << 40;
            for (i, b) in blob[pos + 1..pos + 6].iter().enumerate() {
                v |= i64::from(*b) << (32 - i * 8);
            }
            Ok((Value::Integer(v), 6))
        }
        6 => {
            ensure(blob, pos, 8)?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&blob[pos..pos + 8]);
            Ok((Value::Integer(i64::from_be_bytes(bytes)), 8))
        }
        7 => {
            ensure(blob, pos, 8)?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&blob[pos..pos + 8]);
            Ok((Value::Float(f64::from_be_bytes(bytes)), 8))
        }
        8 => Ok((Value::Integer(0), 0)),
        9 => Ok((Value::Integer(1), 0)),
        10 | 11 => Err(BackendError::Other(format!(
            "sqlite record: reserved serial type {serial}"
        ))),
        n if n >= 12 && n % 2 == 0 => {
            let len = ((n - 12) / 2) as usize;
            ensure(blob, pos, len)?;
            Ok((Value::Blob(blob[pos..pos + len].to_vec()), len))
        }
        n if n >= 13 && n % 2 == 1 => {
            let len = ((n - 13) / 2) as usize;
            ensure(blob, pos, len)?;
            let s = std::str::from_utf8(&blob[pos..pos + len])
                .map_err(|e| BackendError::Other(format!("sqlite record: invalid utf8: {e}")))?
                .to_string();
            Ok((Value::Text(s), len))
        }
        _ => unreachable!("all serial-type ranges covered"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_cdc_log_recognises_the_cold_start_error() {
        // Verbatim from a real cold-start poll against turso 0.7.0.
        let e = turso::Error::Error("Parse error: no such table: turso_cdc".to_string());
        assert!(is_missing_cdc_log(&e));
    }

    #[test]
    fn missing_cdc_log_ignores_unrelated_failures() {
        // A different missing relation must not be mistaken for a cold
        // start — swallowing it would hide a genuinely broken tailer.
        assert!(!is_missing_cdc_log(&turso::Error::Error(
            "Parse error: no such table: posts".to_string()
        )));
        // Nor corruption, nor lock contention.
        assert!(!is_missing_cdc_log(&turso::Error::Corrupt(
            "database disk image is malformed".to_string()
        )));
        assert!(!is_missing_cdc_log(&turso::Error::Busy(
            "locked".to_string()
        )));
        // Naming the table is not enough on its own.
        assert!(!is_missing_cdc_log(&turso::Error::Error(
            "turso_cdc is locked".to_string()
        )));
    }

    #[test]
    fn varint_single_byte() {
        assert_eq!(read_varint(&[0x00], 0).unwrap(), (0, 1));
        assert_eq!(read_varint(&[0x7F], 0).unwrap(), (127, 1));
    }

    #[test]
    fn varint_two_byte() {
        assert_eq!(read_varint(&[0x81, 0x00], 0).unwrap(), (128, 2));
        assert_eq!(read_varint(&[0x81, 0x7F], 0).unwrap(), (255, 2));
    }

    #[test]
    fn record_null() {
        // header-len=1(varint) + 1(serial 0) = 2; serial 0 = NULL.
        let blob = [0x02, 0x00];
        assert_eq!(decode_sqlite_record(&blob).unwrap(), vec![Value::Null]);
    }

    #[test]
    fn record_int_and_text() {
        // Encode (INTEGER 42, TEXT "hi"):
        //   serial for 42 -> type 1 (1-byte int), body 0x2A
        //   serial for "hi" -> type 13+2*2 = 17, body 'h','i'
        //   header-len (varint) = 1 + 1 + 1 = 3
        let blob = [0x03, 0x01, 0x11, 0x2A, b'h', b'i'];
        assert_eq!(
            decode_sqlite_record(&blob).unwrap(),
            vec![Value::Integer(42), Value::Text("hi".into())]
        );
    }

    #[test]
    fn record_literal_0_and_1() {
        // header-len=3; serials 8,9 (both zero-body).
        let blob = [0x03, 0x08, 0x09];
        assert_eq!(
            decode_sqlite_record(&blob).unwrap(),
            vec![Value::Integer(0), Value::Integer(1)]
        );
    }

    #[test]
    fn record_blob() {
        // header-len=2; serial 12+2*3=18 (3-byte blob).
        let blob = [0x02, 0x12, 0xDE, 0xAD, 0xBE];
        assert_eq!(
            decode_sqlite_record(&blob).unwrap(),
            vec![Value::Blob(vec![0xDE, 0xAD, 0xBE])]
        );
    }

    #[test]
    fn record_truncated_body_errors() {
        // header claims 2-byte integer, body only has 1 byte.
        let blob = [0x02, 0x02, 0x01];
        let err = decode_sqlite_record(&blob).unwrap_err();
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    // -----------------------------------------------------------------
    // Replayed-DDL allowlist.
    // -----------------------------------------------------------------

    /// Assert `sql` classifies as `want`.
    #[track_caller]
    fn accepts(sql: &str, want: SchemaObject) {
        match classify_replayed_ddl(sql) {
            Ok(got) => assert_eq!(got, want, "wrong object kind for: {sql}"),
            Err(e) => panic!("legitimate schema DDL rejected: {sql}\n  error: {e}"),
        }
    }

    /// Assert `sql` is refused.
    #[track_caller]
    fn rejects(sql: &str) {
        assert!(
            classify_replayed_ddl(sql).is_err(),
            "allowlist admitted a statement it must refuse: {sql}"
        );
    }

    #[test]
    fn allowlist_accepts_the_stored_create_forms() {
        accepts(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            SchemaObject::Table,
        );
        accepts("CREATE INDEX idx_t_v ON t (v)", SchemaObject::Index);
        accepts("CREATE UNIQUE INDEX idx_t_v ON t (v)", SchemaObject::Index);
        accepts("CREATE VIEW v AS SELECT id FROM t", SchemaObject::View);
        // Case and internal whitespace are not significant.
        accepts("create   table  t (a)", SchemaObject::Table);
        accepts("CREATE\n\tUNIQUE\n\tINDEX i ON t (a)", SchemaObject::Index);
        // sqlite_schema stores the text as written, `IF NOT EXISTS` included.
        accepts("CREATE TABLE IF NOT EXISTS t (a)", SchemaObject::Table);
    }

    #[test]
    fn allowlist_accepts_a_trigger_but_only_to_skip_it() {
        // A trigger body legitimately contains `;` between BEGIN and END,
        // which is why triggers are exempt from the separator scan. The
        // exemption is safe because `apply_ddl` never executes a trigger.
        accepts(
            "CREATE TRIGGER trg AFTER INSERT ON t BEGIN \
             INSERT INTO audit (who) VALUES (NEW.id); \
             UPDATE counters SET n = n + 1; END",
            SchemaObject::Trigger,
        );
    }

    #[test]
    fn trigger_exemption_does_not_execute_anything() {
        // Consequence of the exemption, stated explicitly: a payload that
        // opens with CREATE TRIGGER is not separator-scanned, so trailing
        // SQL classifies as a trigger rather than erroring. That is not a
        // hole — `apply_ddl` logs and returns without running any of it.
        // If trigger replay is ever turned on, this test must flip to a
        // rejection and the separator scan must cover triggers.
        assert_eq!(
            classify_replayed_ddl(
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END; \
                 ATTACH DATABASE '/tmp/evil' AS evil"
            )
            .unwrap(),
            SchemaObject::Trigger
        );
    }

    #[test]
    fn allowlist_rejects_attach_and_pragma() {
        // The two that make this a security fix rather than tidiness:
        // one opens a file of the peer's choosing, the other retargets
        // the engine's behaviour.
        rejects("ATTACH DATABASE '/tmp/evil.db' AS evil");
        rejects("attach database '/etc/passwd' as p");
        rejects("PRAGMA journal_mode = OFF");
        rejects("pragma writable_schema = ON");
    }

    #[test]
    fn allowlist_rejects_transaction_control_and_dml() {
        rejects("BEGIN");
        rejects("COMMIT");
        rejects("ROLLBACK");
        rejects("SAVEPOINT s");
        rejects("RELEASE s");
        rejects("INSERT INTO t VALUES (1)");
        rejects("UPDATE t SET v = 'x'");
        rejects("DELETE FROM t");
        rejects("SELECT 1");
        rejects("VACUUM");
    }

    #[test]
    fn allowlist_rejects_drop_and_alter_text() {
        // Neither can appear in sqlite_schema.sql: DROP arrives as a
        // DELETE on the schema row (synthesized elsewhere), and ALTER
        // surfaces as an UPDATE whose text is the rewritten CREATE.
        rejects("DROP TABLE t");
        rejects("ALTER TABLE t ADD COLUMN c INTEGER");
    }

    #[test]
    fn allowlist_rejects_create_forms_outside_the_set() {
        rejects("CREATE VIRTUAL TABLE t USING fts5(body)");
        rejects("CREATE TEMP TABLE t (a)");
        rejects("CREATE TEMPORARY VIEW v AS SELECT 1");
        rejects("CREATE UNIQUE TABLE t (a)");
        rejects("CREATE");
        rejects("CREATE UNIQUE");
    }

    #[test]
    fn allowlist_rejects_empty_and_keywordless_payloads() {
        rejects("");
        rejects("   \n\t ");
        rejects("-- just a comment\n");
        rejects("/* just a comment */");
        rejects("'quoted'");
        rejects("42");
    }

    #[test]
    fn semicolon_smuggling_is_rejected() {
        rejects("CREATE TABLE t (a); ATTACH DATABASE '/tmp/evil' AS evil");
        rejects("CREATE TABLE t (a);PRAGMA writable_schema=ON");
        rejects("CREATE INDEX i ON t (a); DROP TABLE users");
        rejects("CREATE VIEW v AS SELECT 1; DELETE FROM users");
        // Three statements, and the payload opens legitimately.
        rejects("CREATE TABLE t (a); CREATE TABLE u (b); PRAGMA foo");
    }

    #[test]
    fn comment_smuggling_is_rejected() {
        // A comment must not be able to disguise the real first keyword…
        rejects("/* CREATE TABLE t (a) */ ATTACH DATABASE '/tmp/e' AS e");
        rejects("-- CREATE TABLE t (a)\nPRAGMA writable_schema=ON");
        // …nor to hide a separator from the scan.
        rejects("CREATE TABLE t (a); /* harmless */ DROP TABLE users");
        rejects("CREATE TABLE t (a); -- note\nDROP TABLE users");
    }

    #[test]
    fn comments_in_legitimate_positions_are_skipped_not_rejected() {
        // sqlite_schema.sql stores the exact CREATE text an operator
        // wrote, comments included; refusing them would make an ordinary
        // database un-replicatable.
        accepts(
            "-- the widgets table\nCREATE TABLE t (a)",
            SchemaObject::Table,
        );
        accepts(
            "/* the widgets table */ CREATE TABLE t (a)",
            SchemaObject::Table,
        );
        accepts("CREATE /* kind */ TABLE t (a)", SchemaObject::Table);
        accepts(
            "CREATE TABLE t (\n  a INTEGER -- the id\n)",
            SchemaObject::Table,
        );
        accepts("CREATE TABLE t (a) -- trailing note", SchemaObject::Table);
        // A `;` inside a comment is not a separator.
        accepts(
            "CREATE TABLE t (a) -- ; DROP TABLE users\n",
            SchemaObject::Table,
        );
        accepts(
            "CREATE TABLE t (a) /* ; DROP TABLE users */",
            SchemaObject::Table,
        );
    }

    #[test]
    fn semicolons_inside_quoted_runs_are_not_separators() {
        // Single-quoted literal, with the `''` escape form.
        accepts(
            "CREATE TABLE t (a TEXT DEFAULT 'x; DROP TABLE users')",
            SchemaObject::Table,
        );
        accepts(
            "CREATE TABLE t (a TEXT DEFAULT 'it''s; fine')",
            SchemaObject::Table,
        );
        // All three identifier-quoting forms SQLite accepts.
        accepts("CREATE TABLE \"odd;name\" (a)", SchemaObject::Table);
        accepts("CREATE TABLE `odd;name` (a)", SchemaObject::Table);
        accepts("CREATE TABLE [odd;name] (a)", SchemaObject::Table);
        accepts("CREATE TABLE \"say \"\"hi\"\";\" (a)", SchemaObject::Table);
    }

    #[test]
    fn unterminated_quotes_and_comments_are_rejected() {
        // Unreasonable-about payloads do not get the benefit of the doubt.
        rejects("CREATE TABLE t (a TEXT DEFAULT 'unterminated)");
        rejects("CREATE TABLE \"unterminated (a)");
        rejects("CREATE TABLE `unterminated (a)");
        rejects("CREATE TABLE [unterminated (a)");
        rejects("CREATE TABLE t (a) /* never closed");
    }

    #[test]
    fn trailing_semicolon_is_tolerated() {
        // SQLite does not store one, but refusing it would be pedantry.
        accepts("CREATE TABLE t (a);", SchemaObject::Table);
        accepts("CREATE TABLE t (a); \n ", SchemaObject::Table);
        accepts("CREATE TABLE t (a); -- done", SchemaObject::Table);
    }

    // -----------------------------------------------------------------
    // Empty-batch API.
    // -----------------------------------------------------------------

    fn row(change_id: i64, change_type: i64) -> CdcRow {
        CdcRow {
            change_id,
            change_txn_id: Some(1),
            change_type,
            table_name: None,
            id: None,
            before: None,
            after: None,
            updates: None,
        }
    }

    #[test]
    fn commit_change_id_is_none_for_an_empty_batch() {
        // Was an `expect`. A peer-decoded batch can be empty, and the
        // task that applied it had no way to survive the panic.
        assert_eq!(TxnBatch { rows: Vec::new() }.commit_change_id(), None);
    }

    #[test]
    fn commit_change_id_reads_the_commit_row() {
        let batch = TxnBatch {
            rows: vec![
                row(7, change_type::INSERT),
                row(8, change_type::INSERT),
                row(9, change_type::COMMIT),
            ],
        };
        assert_eq!(batch.commit_change_id(), Some(9));
        assert_eq!(batch.txn_id(), Some(1));
    }
}
