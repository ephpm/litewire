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
//! backend).
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

use std::time::Duration;

use litewire_backend::{
    Backend, BackendConn, BackendError, Column, ExecuteResult, ResultSet, Value,
};
use tokio::sync::Mutex;

/// Builder for [`Turso`]. Use [`Turso::open`] / [`Turso::memory`] for the
/// default configuration; use [`TursoBuilder`] to tune the busy timeout.
#[derive(Clone, Debug)]
pub struct TursoBuilder {
    path: String,
    busy_timeout_ms: u32,
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
        })
    }
}

/// **Experimental** in-process backend via the Turso Database engine.
///
/// This type is a **factory**: it opens a fresh [`turso::Connection`] for
/// every wire-protocol session via [`Backend::connect`]. See the module
/// docs for status and limitations.
pub struct Turso {
    db: turso::Database,
    busy_timeout_ms: u32,
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

    /// Start a [`TursoBuilder`] to override defaults.
    #[must_use]
    pub fn builder(path: impl AsRef<str>) -> TursoBuilder {
        TursoBuilder {
            path: path.as_ref().to_string(),
            busy_timeout_ms: 5000,
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
fn map_turso_err(e: turso::Error) -> BackendError {
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

#[async_trait::async_trait]
impl BackendConn for TursoConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        reject_unsupported(sql)?;
        let conn = self.conn.lock().await;
        // `prepare_cached` interns the parsed statement in the engine's
        // per-connection cache, mirroring the rusqlite backend.
        let mut stmt = conn.prepare_cached(sql).await.map_err(map_turso_err)?;

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

        Ok(ResultSet {
            columns,
            rows: result_rows,
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        reject_unsupported(sql)?;
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(sql).await.map_err(map_turso_err)?;
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
