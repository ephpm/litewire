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
    #[must_use]
    pub fn commit_change_id(&self) -> i64 {
        self.rows
            .last()
            .map(|r| r.change_id)
            .expect("TxnBatch is never empty")
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
    /// # Errors
    ///
    /// Returns [`BackendError::Sqlite`] if the underlying poll fails.
    /// Malformed rows (missing required columns, wrong types) produce
    /// [`BackendError::Other`] — Phase 2 pins turso exactly, so this
    /// should only fire on a schema change we haven't caught up to.
    pub async fn poll_batch(&mut self) -> Result<Option<TxnBatch>, BackendError> {
        // Read up to a bounded window of rows so a single batch cannot
        // starve the tailer if the primary is committing very fast.
        const WINDOW: i64 = 1024;

        let conn = self.factory.db.connect().map_err(map_turso_err)?;
        let mut stmt = conn
            .prepare(
                "SELECT change_id, change_txn_id, change_type, table_name, id, \
                        before, after, updates \
                 FROM turso_cdc \
                 WHERE change_id > ? \
                 ORDER BY change_id \
                 LIMIT ?",
            )
            .await
            .map_err(map_turso_err)?;
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
        let batch = TxnBatch { rows: buffered };
        self.last_seen_change_id = batch.commit_change_id();
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
/// # Panics
///
/// This function does not panic; malformed CDC rows surface as
/// [`BackendError::Other`].
pub async fn apply_batch(
    replica_conn: &turso::Connection,
    batch: &TxnBatch,
) -> Result<(), BackendError> {
    ensure_watermark_table(replica_conn).await?;

    let current = read_watermark(replica_conn).await?;
    if batch.commit_change_id() <= current {
        // Already applied; idempotent no-op.
        return Ok(());
    }

    // BEGIN IMMEDIATE takes a write lock immediately, matching what turso's
    // own sync engine does for replay sessions.
    replica_conn
        .execute("BEGIN IMMEDIATE", ())
        .await
        .map_err(map_turso_err)?;

    let result = apply_batch_inner(replica_conn, batch, current).await;
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
    stmt.execute((batch.commit_change_id(),))
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
}
