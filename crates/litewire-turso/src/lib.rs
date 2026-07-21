//! **Experimental** in-process backend using [Turso Database], the
//! ground-up Rust rewrite of SQLite.
//!
//! [Turso Database]: https://github.com/tursodatabase/turso
//!
//! # Status
//!
//! The Turso engine is **Beta** (pinned here at `turso = "=0.7.0"`, the
//! first non-pre release of the 0.7 line, July 2026). Upstream does not
//! yet position it as a production SQLite replacement. This backend exists
//! to gather evidence (benchmarks, file-format round-trips, crash-recovery
//! smokes) behind an explicit opt-in — it is not a default anywhere.
//!
//! # Async-native
//!
//! Unlike the `rusqlite` backend, which wraps every call in
//! [`tokio::task::spawn_blocking`], the Turso engine is natively async:
//! `query`/`execute` futures are polled on the calling task with no
//! thread-pool hop. A per-session [`tokio::sync::Mutex`] serializes
//! statements on the *same* [`TursoConn`] (one wire client cannot issue
//! two overlapping statements anyway), matching the rusqlite backend's
//! per-session semantics.
//!
//! # Per-connection isolation
//!
//! [`Turso`] is a **factory**: it owns one [`turso::Database`] and hands
//! each wire session its own [`turso::Connection`] via
//! [`Backend::connect`]. Transaction state is per-connection. Concurrent
//! writers are coordinated by the engine (MVCC / `BEGIN CONCURRENT` is a
//! Turso feature; plain busy-handling applies to classic transactions) with
//! a configurable busy timeout (default 5000 ms, mirroring the rusqlite
//! backend). Each session also sets `PRAGMA synchronous = NORMAL` by
//! default — the engine's own default is `FULL`; `NORMAL` matches the
//! rusqlite backend's documented per-session behavior.
//!
//! # In-memory databases
//!
//! `":memory:"` maps to one shared in-memory database owned by the
//! [`turso::Database`] object; every [`Backend::connect`] session sees the
//! same data. (No temp-file workaround is needed here, unlike the rusqlite
//! backend, because connections derive from a single engine instance.)
//!
//! # Known-unsupported operations (Turso 0.7.0)
//!
//! Returned as clear [`BackendError`]s rather than silent misbehavior:
//!
//! - **`VACUUM`** — rejected by this backend with an "unsupported" error.
//!   Upstream support is incomplete and gated behind an experimental
//!   builder flag we do not enable.
//! - **Multi-process access** — the engine does not support a second
//!   *process* opening the same database file (multiprocess WAL is an
//!   experimental upstream flag, not enabled here). Do not point another
//!   process at the same file while this backend owns it.
//! - **`ATTACH` / `DETACH`** — gated behind an experimental upstream flag,
//!   not enabled; the engine returns its own error.
//! - **Non-UTF-8 `TEXT`** — the engine's Rust API surfaces `TEXT` as
//!   `String`; unlike the rusqlite backend (which returns such cells as
//!   `Blob`), byte-exact round-trips of invalid-UTF-8 text are not
//!   guaranteed.
//!
//! Anything else the engine cannot do surfaces as the engine's own error
//! text mapped into [`BackendError::Sqlite`], which the wire frontends'
//! error classifiers already understand (SQLite-style message shapes).
//!
//! # Bind-count parity with the rusqlite backend
//!
//! The engine executes statements with unbound parameters as `NULL`
//! instead of erroring. This backend rejects a parameter-count mismatch
//! with the same "Wrong number of parameters passed to query" error the
//! rusqlite backend produces. Without it, the `mysql` >= 8.1 CLI's
//! `select $$` startup probe (which must fail) returns a result set the
//! CLI never consumes, and every later statement dies client-side with
//! CR 2014 "Commands out of sync".

pub mod cdc;

/// Re-export of the underlying [`turso::Connection`] type. External
/// callers (e.g. ePHPm's Phase 2 CDC replication layer) need this to
/// type their apply-side function signatures without taking a direct
/// `turso` dependency.
pub use turso::Connection as TursoConnection;

use std::time::Duration;

use litewire_backend::{
    Backend, BackendConn, BackendError, Column, ExecuteResult, ResultSet, Value,
};
use tokio::sync::Mutex;

/// `synchronous` setting applied to every per-session connection.
///
/// Mirrors the rusqlite backend's `Synchronous` enum. The engine's own
/// default is `FULL`; this backend defaults to [`Synchronous::Normal`] to
/// match the rusqlite backend's documented per-session behavior (the
/// WAL-appropriate default: durable across power loss for committed
/// transactions, higher write throughput than `FULL`).
#[derive(Clone, Copy, Debug)]
pub enum Synchronous {
    /// Fastest, unsafe against power loss.
    Off,
    /// WAL-appropriate default, matches the rusqlite backend.
    Normal,
    /// Fully synchronous (the Turso engine's own default). Slowest.
    Full,
}

impl Synchronous {
    fn as_pragma_str(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
        }
    }
}

/// Builder for [`Turso`]. Use [`Turso::open`] / [`Turso::memory`] for the
/// default configuration; use [`TursoBuilder`] to tune the per-session
/// busy timeout and `synchronous` mode.
#[derive(Clone, Debug)]
pub struct TursoBuilder {
    path: String,
    busy_timeout_ms: u32,
    synchronous: Synchronous,
    enable_cdc_on_connect: bool,
}

impl TursoBuilder {
    /// Set the busy timeout (milliseconds) applied to every per-session
    /// connection. Mirrors the rusqlite backend's `busy_timeout` PRAGMA
    /// (default 5000 ms).
    #[must_use]
    pub fn busy_timeout_ms(mut self, ms: u32) -> Self {
        self.busy_timeout_ms = ms;
        self
    }

    /// Set the `synchronous` PRAGMA applied to every per-session
    /// connection (default [`Synchronous::Normal`], mirroring the rusqlite
    /// backend).
    #[must_use]
    pub fn synchronous(mut self, s: Synchronous) -> Self {
        self.synchronous = s;
        self
    }

    /// **Experimental** — when true, every session opened via
    /// [`Backend::connect`] auto-enables full CDC capture via
    /// [`cdc::enable_cdc`]. Used by ePHPm's Phase 2 CDC-native
    /// replication on the primary so writes coming in via the wire
    /// frontends are captured for replay by replicas.
    ///
    /// Default: `false`. Enabling CDC has a modest write-amp cost
    /// (`full` mode doubles the write path: pre-image + post-image
    /// records) and only makes sense when a tailer downstream is
    /// consuming the log.
    #[must_use]
    pub fn enable_cdc_on_connect(mut self, on: bool) -> Self {
        self.enable_cdc_on_connect = on;
        self
    }

    /// Finalize the builder: open (or create) the database with the Turso
    /// engine. The engine is WAL-native; no journal-mode bootstrap is
    /// required.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Sqlite`] if the database cannot be opened
    /// (bad path, corrupt file, unsupported format).
    pub async fn build(self) -> Result<Turso, BackendError> {
        let db = turso::Builder::new_local(&self.path)
            .build()
            .await
            .map_err(map_turso_err)?;
        Ok(Turso {
            db,
            busy_timeout_ms: self.busy_timeout_ms,
            synchronous: self.synchronous,
            enable_cdc_on_connect: self.enable_cdc_on_connect,
        })
    }
}

/// **Experimental** in-process backend via the Turso Database engine.
///
/// This type is a **factory**: it opens a fresh [`turso::Connection`] for
/// every wire-protocol session via [`Backend::connect`]. See the module
/// docs for status and limitations.
pub struct Turso {
    pub(crate) db: turso::Database,
    busy_timeout_ms: u32,
    synchronous: Synchronous,
    /// If set, [`Backend::connect`] enables full CDC capture on every
    /// session. See [`TursoBuilder::enable_cdc_on_connect`].
    pub(crate) enable_cdc_on_connect: bool,
}

impl Turso {
    /// Open (or create) a file-backed database at `path` with the default
    /// configuration (busy timeout 5000 ms).
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub async fn open(path: impl AsRef<str>) -> Result<Self, BackendError> {
        Self::builder(path).build().await
    }

    /// Open a shared in-memory database. All sessions from this factory
    /// see the same data; the database vanishes when the [`Turso`] is
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine cannot create the database.
    pub async fn memory() -> Result<Self, BackendError> {
        Self::builder(":memory:").build().await
    }

    /// Open a fresh raw [`turso::Connection`] against this factory's
    /// database, bypassing the litewire [`BackendConn`] wrapper.
    ///
    /// **Experimental** — this is the seam ePHPm's Phase 2 CDC
    /// replication uses to enable `capture_data_changes_conn` on write
    /// sessions on the primary and to tail `turso_cdc` on the follower
    /// side. Prefer [`Backend::connect`] for anything else.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Sqlite`] if the engine cannot open a new
    /// connection.
    pub fn raw_connection(&self) -> Result<turso::Connection, BackendError> {
        self.db.connect().map_err(map_turso_err)
    }

    /// Start a [`TursoBuilder`] to override defaults.
    #[must_use]
    pub fn builder(path: impl AsRef<str>) -> TursoBuilder {
        TursoBuilder {
            path: path.as_ref().to_string(),
            busy_timeout_ms: 5000,
            synchronous: Synchronous::Normal,
            enable_cdc_on_connect: false,
        }
    }
}

/// Per-session Turso handle.
///
/// Owns exactly one [`turso::Connection`]. A [`tokio::sync::Mutex`]
/// serializes statements on the same session (held across `.await`, which
/// is why this is the tokio mutex and not a std/parking_lot one).
pub struct TursoConn {
    conn: Mutex<turso::Connection>,
}

#[async_trait::async_trait]
impl Backend for Turso {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        let conn = self.db.connect().map_err(map_turso_err)?;
        conn.busy_timeout(Duration::from_millis(u64::from(self.busy_timeout_ms)))
            .map_err(map_turso_err)?;
        // The engine's own default is synchronous=FULL; bring the session
        // to parity with the rusqlite backend (NORMAL by default).
        conn.pragma_update("synchronous", self.synchronous.as_pragma_str())
            .await
            .map_err(map_turso_err)?;
        if self.enable_cdc_on_connect {
            // Experimental: opt every wire session into CDC capture. Only
            // set when litewire-turso is being used as the primary in
            // ePHPm's Phase 2 replication mode. The pragma is
            // per-connection, so a session that skips this (e.g. a
            // direct `raw_connection()` caller like the tail loop) is
            // unaffected.
            cdc::enable_cdc(&conn).await?;
        }
        Ok(Box::new(TursoConn {
            conn: Mutex::new(conn),
        }))
    }
}

/// Map a [`turso::Error`] into litewire's stringly-typed [`BackendError`].
///
/// Busy conditions are tagged with the classic SQLite phrasing
/// ("database is locked (SQLITE_BUSY)") so the wire frontends' substring
/// classifiers (`litewire-mysql`/`-postgres` `error_map`) map them to the
/// retryable lock-wait error codes clients expect.
pub(crate) fn map_turso_err(e: turso::Error) -> BackendError {
    match e {
        turso::Error::Busy(m) | turso::Error::BusySnapshot(m) => {
            BackendError::Sqlite(format!("database is locked (SQLITE_BUSY): {m}"))
        }
        other => BackendError::Sqlite(other.to_string()),
    }
}

/// Convert litewire values into Turso positional params.
///
/// `turso::params::Params` is `#[doc(hidden)]` but public and implements
/// `IntoParams`; the crate's own tests construct it directly. Acceptable
/// under an exact version pin (`=0.7.0`).
fn to_params(params: &[Value]) -> turso::params::Params {
    turso::params::Params::Positional(
        params
            .iter()
            .map(|v| match v {
                Value::Null => turso::Value::Null,
                Value::Integer(i) => turso::Value::Integer(*i),
                Value::Float(f) => turso::Value::Real(*f),
                Value::Text(s) => turso::Value::Text(s.clone()),
                Value::Blob(b) => turso::Value::Blob(b.clone()),
            })
            .collect(),
    )
}

/// Convert a Turso value into a litewire value.
fn from_turso(v: turso::Value) -> Value {
    match v {
        turso::Value::Null => Value::Null,
        turso::Value::Integer(i) => Value::Integer(i),
        turso::Value::Real(f) => Value::Float(f),
        turso::Value::Text(s) => Value::Text(s),
        turso::Value::Blob(b) => Value::Blob(b),
    }
}

/// Convert Turso column metadata into litewire columns.
fn to_columns(cols: &[turso::Column]) -> Vec<Column> {
    cols.iter()
        .map(|c| Column {
            name: c.name().to_string(),
            decltype: c.decl_type().map(str::to_string),
        })
        .collect()
}

/// Reject statements the Turso engine cannot execute correctly yet, with
/// an error that says so instead of an opaque engine failure.
fn reject_unsupported(sql: &str) -> Result<(), BackendError> {
    let first = sql.trim_start().get(..6).unwrap_or_default();
    if first.eq_ignore_ascii_case("VACUUM") {
        return Err(BackendError::Other(
            "VACUUM is not supported by the experimental Turso backend \
             (incomplete upstream in Turso 0.7.0)"
                .into(),
        ));
    }
    Ok(())
}

/// Does this SQL read from a pragma table-valued function
/// (`pragma_table_info(...)`, `pragma_index_list(...)`, ...)?
fn sql_uses_pragma_tvf(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("pragma_")
}

/// Number of parameter slots a SQL statement declares, following SQLite's
/// tokenizer rules: `?` takes the next free index, `?NNN` takes index `NNN`,
/// and `:name` / `@name` / `$name` each take the next free index the first
/// time the name appears. The result is the highest index assigned — the
/// same value `sqlite3_bind_parameter_count()` reports.
///
/// Quoted strings (`'…'`), quoted identifiers (`"…"`, `` `…` ``, `[…]`),
/// line comments (`--`) and block comments (`/* … */`) are skipped.
fn expected_param_count(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut max_index = 0usize;
    // Distinct named parameters seen so far (each gets one index).
    let mut named: Vec<&str> = Vec::new();
    while i < bytes.len() {
        match bytes[i] {
            // String literal or quoted identifier; doubled quote escapes.
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        if i + 1 < bytes.len() && bytes[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            // Bracket-quoted identifier (accepted by SQLite for MS compat).
            b'[' => {
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                i += 1;
            }
            // -- line comment
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            // /* block comment */
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            // `?` or `?NNN`
            b'?' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i > start {
                    let n: usize = sql[start..i].parse().unwrap_or(0);
                    max_index = max_index.max(n);
                } else {
                    max_index += 1;
                }
            }
            // `:name`, `@name`, `$name`. SQLite also accepts a bare `$`
            // variable — `SELECT $$` parses as a single parameter (the
            // mysql 8.4 CLI exploits exactly that in its startup probe).
            b':' | b'@' | b'$' => {
                let start = i;
                i += 1;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
                {
                    i += 1;
                }
                if i > start + 1 || bytes[start] == b'$' {
                    let name = &sql[start..i];
                    if !named.contains(&name) {
                        named.push(name);
                        max_index += 1;
                    }
                }
            }
            _ => i += 1,
        }
    }
    max_index
}

/// Reject a bind-count mismatch with the same error shape the rusqlite
/// backend produces (`rusqlite::Error::InvalidParameterCount`).
///
/// The Turso engine (0.7.0) silently leaves unbound parameters NULL and
/// executes anyway. SQLite's C API technically does the same, but every
/// SQLite *binding* litewire sits on (rusqlite here, and what wire clients
/// expect from a MySQL-shaped server) treats a count mismatch as an error.
/// The failure mode of not checking is severe: Oracle's `mysql` >= 8.1 CLI
/// probes dollar-quoting support at startup with `select $$` and expects an
/// error reply. If the server instead returns a result set, the CLI never
/// reads it, its client-side state machine wedges, and every subsequent
/// statement fails with CR 2014 "Commands out of sync".
fn check_param_count(sql: &str, got: usize) -> Result<(), BackendError> {
    let needed = expected_param_count(sql);
    if got != needed {
        return Err(BackendError::Sqlite(format!(
            "Wrong number of parameters passed to query. Got {got}, needed {needed}"
        )));
    }
    Ok(())
}

#[async_trait::async_trait]
impl BackendConn for TursoConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        reject_unsupported(sql)?;
        let conn = self.conn.lock().await;
        // `prepare_cached` interns the parsed statement in the engine's
        // per-connection cache, mirroring the rusqlite backend.
        let mut stmt = conn.prepare_cached(sql).await.map_err(map_turso_err)?;
        // Engine parse errors surface first (prepare above); then enforce
        // the bind count like rusqlite does — the engine itself would
        // silently run with unbound parameters as NULL.
        check_param_count(sql, params.len())?;

        let columns = to_columns(&stmt.columns());
        let col_count = columns.len();

        let mut rows = stmt.query(to_params(params)).await.map_err(map_turso_err)?;
        let mut result_rows = Vec::new();
        while let Some(row) = rows.next().await.map_err(map_turso_err)? {
            let mut values = Vec::with_capacity(col_count);
            for i in 0..col_count {
                values.push(from_turso(row.get_value(i).map_err(map_turso_err)?));
            }
            result_rows.push(values);
        }

        // WORKAROUND (turso 0.7.0): a SELECT from a pragma table-valued
        // function (e.g. `pragma_table_info(...)`) leaves the connection in
        // a phantom-transaction state: every subsequent write is accepted
        // and visible to this session but never committed — silently lost
        // on close. `COMMIT` then reports "no transaction is active", yet
        // an explicit `BEGIN; COMMIT;` pair restores normal autocommit —
        // but only once the poisoning statement handle has been dropped.
        // Verified empirically (see ePHPm docs/turso-gate5-results.md);
        // upstream issue to be filed. Without this, WordPress is unusable
        // (dbDelta's DESCRIBE poisons the session before any writes).
        if sql_uses_pragma_tvf(sql) {
            drop(rows);
            drop(stmt);
            conn.execute("BEGIN", ()).await.map_err(map_turso_err)?;
            conn.execute("COMMIT", ()).await.map_err(map_turso_err)?;
        }

        Ok(ResultSet {
            columns,
            rows: result_rows,
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        reject_unsupported(sql)?;
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(sql).await.map_err(map_turso_err)?;
        // Same bind-count parity check as `query` (see `check_param_count`).
        check_param_count(sql, params.len())?;
        let affected = stmt
            .execute(to_params(params))
            .await
            .map_err(map_turso_err)?;
        let last_id = conn.last_insert_rowid();

        Ok(ExecuteResult {
            affected_rows: affected,
            last_insert_rowid: if last_id != 0 { Some(last_id) } else { None },
        })
    }

    async fn describe_columns(&self, sql: &str) -> Result<Vec<Column>, BackendError> {
        let conn = self.conn.lock().await;
        // Prepare without executing -- same trick as the rusqlite backend,
        // no LIMIT-0 probe needed.
        let stmt = conn.prepare_cached(sql).await.map_err(map_turso_err)?;
        Ok(to_columns(&stmt.columns()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    // Mirrors the rusqlite backend's test suite so behavior differences
    // between the two engines show up as test failures here, not as
    // production surprises.

    #[tokio::test]
    async fn pragma_tvf_read_does_not_poison_session() {
        // Regression (turso 0.7.0): a SELECT from pragma_table_info() left
        // the session in a phantom-transaction state — subsequent writes
        // were visible to the same session but silently lost to others.
        let dir = std::env::temp_dir().join(format!("lw-tvf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("poison.db");
        let backend = Turso::open(path.to_str().unwrap()).await.unwrap();
        let a = backend.connect().await.unwrap();
        a.execute("CREATE TABLE anchor (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap();
        // The poisoning read (WordPress: DESCRIBE / SHOW FULL COLUMNS).
        a.query("SELECT name FROM pragma_table_info('anchor')", &[])
            .await
            .unwrap();
        // Writes after the TVF read...
        a.execute(
            "CREATE TABLE after_tvf (id INTEGER PRIMARY KEY, v TEXT)",
            &[],
        )
        .await
        .unwrap();
        a.execute("INSERT INTO after_tvf (v) VALUES ('x')", &[])
            .await
            .unwrap();
        // ...must be visible to a different session.
        let b = backend.connect().await.unwrap();
        let rs = b
            .query("SELECT COUNT(*) FROM after_tvf", &[])
            .await
            .expect("table written after pragma TVF read must exist for other sessions");
        assert_eq!(rs.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn basic_crud() {
        let backend = Turso::memory().await.unwrap();

        backend
            .execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
                &[],
            )
            .await
            .unwrap();

        let result = backend
            .execute(
                "INSERT INTO users (name) VALUES (?1)",
                &[Value::Text("Alice".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.last_insert_rowid, Some(1));

        let result = backend
            .execute(
                "INSERT INTO users (name) VALUES (?1)",
                &[Value::Text("Bob".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.last_insert_rowid, Some(2));

        let rs = backend
            .query("SELECT id, name FROM users ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rs.columns.len(), 2);
        assert_eq!(rs.columns[0].name, "id");
        assert_eq!(rs.columns[1].name, "name");
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][1], Value::Text("Alice".into()));
        assert_eq!(rs.rows[1][1], Value::Text("Bob".into()));

        let rs = backend
            .query("SELECT * FROM users WHERE id = ?1", &[Value::Integer(1)])
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], Value::Text("Alice".into()));
    }

    #[tokio::test]
    async fn types_roundtrip() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute(
                "CREATE TABLE typed (i INTEGER, r REAL, t TEXT, b BLOB)",
                &[],
            )
            .await
            .unwrap();

        backend
            .execute(
                "INSERT INTO typed VALUES (?1, ?2, ?3, ?4)",
                &[
                    Value::Integer(42),
                    Value::Float(2.72),
                    Value::Text("hello".into()),
                    Value::Blob(vec![0xDE, 0xAD]),
                ],
            )
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM typed", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(42));
        assert_eq!(rs.rows[0][1], Value::Float(2.72));
        assert_eq!(rs.rows[0][2], Value::Text("hello".into()));
        assert_eq!(rs.rows[0][3], Value::Blob(vec![0xDE, 0xAD]));
    }

    #[tokio::test]
    async fn null_handling() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Null])
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Null);
    }

    #[tokio::test]
    async fn empty_table_query() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER, name TEXT)", &[])
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.columns.len(), 2);
        assert!(rs.rows.is_empty());
    }

    #[tokio::test]
    async fn multiple_params() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)", &[])
            .await
            .unwrap();
        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                &[
                    Value::Integer(1),
                    Value::Text("hello".into()),
                    Value::Float(9.99),
                ],
            )
            .await
            .unwrap();

        let rs = backend
            .query(
                "SELECT * FROM t WHERE a = ?1 AND b = ?2",
                &[Value::Integer(1), Value::Text("hello".into())],
            )
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn affected_rows_count() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", &[])
            .await
            .unwrap();

        for i in 0..5 {
            backend
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Integer(i), Value::Text(format!("v{i}"))],
                )
                .await
                .unwrap();
        }

        let result = backend
            .execute("DELETE FROM t WHERE id >= ?1", &[Value::Integer(3)])
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 2);
    }

    #[tokio::test]
    async fn query_error_on_bad_sql() {
        let backend = Turso::memory().await.unwrap();
        let result = backend.query("DEFINITELY NOT SQL !!!", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_error_on_bad_sql() {
        let backend = Turso::memory().await.unwrap();
        let result = backend.execute("DEFINITELY NOT SQL !!!", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn blob_roundtrip() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (data BLOB)", &[])
            .await
            .unwrap();

        let data = vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF];
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Blob(data.clone())])
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Blob(data));
    }

    #[tokio::test]
    async fn column_names_preserved() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute(
                "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)",
                &[],
            )
            .await
            .unwrap();

        let rs = backend
            .query("SELECT id, name, email FROM users", &[])
            .await
            .unwrap();
        assert_eq!(rs.columns[0].name, "id");
        assert_eq!(rs.columns[1].name, "name");
        assert_eq!(rs.columns[2].name, "email");
    }

    #[tokio::test]
    async fn query_with_alias() {
        let backend = Turso::memory().await.unwrap();
        let rs = backend
            .query("SELECT 1 AS num, 'hello' AS greeting", &[])
            .await
            .unwrap();
        assert_eq!(rs.columns[0].name, "num");
        assert_eq!(rs.columns[1].name, "greeting");
        assert_eq!(rs.rows[0][0], Value::Integer(1));
        assert_eq!(rs.rows[0][1], Value::Text("hello".into()));
    }

    #[tokio::test]
    async fn describe_columns_returns_decltypes() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE users (id INTEGER, name TEXT, tags BLOB)", &[])
            .await
            .unwrap();

        let cols = backend
            .describe_columns("SELECT id, name, tags FROM users")
            .await
            .unwrap();
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].decltype.as_deref(), Some("INTEGER"));
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].decltype.as_deref(), Some("TEXT"));
        assert_eq!(cols[2].name, "tags");
        assert_eq!(cols[2].decltype.as_deref(), Some("BLOB"));
    }

    // -- Bind-count parity (mysql CLI `select $$` probe regression) -------

    #[test]
    fn param_count_scanner() {
        assert_eq!(expected_param_count("SELECT 1"), 0);
        assert_eq!(expected_param_count("SELECT ?"), 1);
        assert_eq!(expected_param_count("SELECT ?, ?"), 2);
        assert_eq!(expected_param_count("SELECT ?2"), 2);
        assert_eq!(expected_param_count("SELECT ?3, ?"), 4);
        assert_eq!(expected_param_count("SELECT :a, :b, :a"), 2);
        assert_eq!(expected_param_count("SELECT @x, $y"), 2);
        // The mysql >= 8.1 CLI probe: one TCL-style `$` parameter.
        assert_eq!(expected_param_count("SELECT $$"), 1);
        // Placeholders inside literals/identifiers/comments don't count.
        assert_eq!(expected_param_count("SELECT '?', \"?\", `?` -- ?"), 0);
        assert_eq!(expected_param_count("SELECT 'it''s ?' /* :x */, [?]"), 0);
        assert_eq!(expected_param_count("SELECT 'a@b.c', '$5'"), 0);
    }

    #[tokio::test]
    async fn unbound_parameter_rejected_like_rusqlite() {
        // Regression: Oracle's mysql 8.4 CLI sends `select $$` at startup
        // (dollar-quoting detection) and requires an error reply. Turso
        // executes unbound parameters as NULL, so this returned a result
        // set — which the CLI never reads, wedging its state machine into
        // CR 2014 "Commands out of sync" for every following statement.
        let backend = Turso::memory().await.unwrap();

        let err = backend.query("SELECT $$", &[]).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Wrong number of parameters passed to query. Got 0, needed 1"),
            "expected rusqlite-parity bind-count error, got: {err}"
        );

        let err = backend.query("SELECT ?", &[]).await.unwrap_err();
        assert!(err.to_string().contains("Wrong number of parameters"));
    }

    #[tokio::test]
    async fn execute_bind_count_mismatch_rejected() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();

        // Too few.
        let err = backend
            .execute("INSERT INTO t VALUES (?1)", &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Got 0, needed 1"), "got: {err}");

        // Too many.
        let err = backend
            .query("SELECT ?1", &[Value::Integer(1), Value::Integer(2)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Got 2, needed 1"), "got: {err}");

        // Exact count still works.
        let rs = backend
            .query("SELECT ?1", &[Value::Integer(7)])
            .await
            .unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(7));
    }

    #[tokio::test]
    async fn vacuum_rejected_with_clear_error() {
        let backend = Turso::memory().await.unwrap();
        let err = backend.execute("VACUUM", &[]).await.unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "expected clear unsupported error, got: {err}"
        );
    }

    // -- Isolation tests (mirroring the rusqlite backend) -----------------

    #[tokio::test]
    async fn per_conn_transaction_isolation() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        let a = backend.connect().await.unwrap();
        let b = backend.connect().await.unwrap();

        a.execute("BEGIN", &[]).await.unwrap();
        a.execute("INSERT INTO t VALUES (1, 'from-a')", &[])
            .await
            .unwrap();

        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(
            rs.rows[0][0],
            Value::Integer(0),
            "B saw A's uncommitted row"
        );

        let _ = b.execute("ROLLBACK", &[]).await;

        a.execute("COMMIT", &[]).await.unwrap();

        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));
        let rs = a.query("SELECT v FROM t WHERE id=1", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text("from-a".into()));
    }

    #[tokio::test]
    async fn per_conn_rollback_stays_local() {
        let backend = Turso::memory().await.unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        let a = backend.connect().await.unwrap();
        let b = backend.connect().await.unwrap();

        a.execute("BEGIN", &[]).await.unwrap();
        a.execute("INSERT INTO t VALUES (42, 'ghost')", &[])
            .await
            .unwrap();
        a.execute("ROLLBACK", &[]).await.unwrap();

        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(0));
        let rs = a.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(0));
    }

    #[tokio::test]
    async fn per_conn_concurrent_readers() {
        let backend = Arc::new(Turso::memory().await.unwrap());
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();
        for i in 0..100 {
            backend
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Integer(i), Value::Text(format!("row-{i}"))],
                )
                .await
                .unwrap();
        }

        let mut handles = Vec::new();
        for _ in 0..16 {
            let be = Arc::clone(&backend);
            handles.push(tokio::spawn(async move {
                let conn = be.connect().await.unwrap();
                for i in 0..50 {
                    let rs = conn
                        .query("SELECT v FROM t WHERE id=?1", &[Value::Integer(i % 100)])
                        .await
                        .unwrap();
                    assert_eq!(rs.rows.len(), 1);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn per_conn_shared_memory_visibility() {
        let backend = Turso::memory().await.unwrap();

        let a = backend.connect().await.unwrap();
        let b = backend.connect().await.unwrap();

        a.execute("CREATE TABLE t (id INTEGER)", &[]).await.unwrap();
        a.execute("INSERT INTO t VALUES (7)", &[]).await.unwrap();

        let rs = b.query("SELECT id FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(7));
    }
}
