//! Dialect-aware SQL session layer for litewire.
//!
//! [`Session`] is the protocol-independent core of a litewire client
//! connection. It owns a per-client [`BackendConn`], translates incoming SQL
//! from a source [`Dialect`] to SQLite, tracks explicit-transaction state,
//! and maps backend errors to MySQL error codes.
//!
//! It was extracted from the MySQL wire frontend's handler so that embedders
//! (such as ePHPm's in-process PHP bridge) can get the exact semantics the
//! wire path has always had -- `SET NAMES` returns OK without touching the
//! backend, `SHOW TABLES` runs the metadata emulation, and
//! `BEGIN`/`COMMIT`/`ROLLBACK` update the session's transaction flag --
//! without opening a TCP connection to the proxy they are embedded in.
//! Wire frontends delegate to `Session` and keep only protocol framing.

pub mod error_map;

use std::sync::Arc;

use litewire_backend::{BackendConn, BackendError, ResultSet, Value};
use litewire_translate::{
    Dialect, StatementKind, TranslateCache, TranslateError, TranslateResult, classify,
    translate_cached,
};

/// MySQL `ER_PARSE_ERROR` (SQLSTATE 42000) -- what translation failures map
/// to at the MySQL wire layer, exposed here so embedders can report the same
/// code without depending on a wire-protocol crate.
pub const ER_PARSE_ERROR: u16 = 1064;

/// Error from a [`Session`] operation.
///
/// Both variants carry (or can produce, via [`SessionError::code`] /
/// [`SessionError::sqlstate`]) the mapped MySQL error triple, so callers can
/// surface errors exactly the way the MySQL wire frontend does.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// The SQL could not be translated to SQLite (parse error or
    /// unsupported construct). Maps to `ER_PARSE_ERROR` (1064 / 42000).
    ///
    /// `transparent` so the message is byte-identical to what the MySQL
    /// handler historically put on the wire (`TranslateError`'s Display).
    #[error(transparent)]
    Translate(#[from] TranslateError),

    /// The backend rejected the statement. The code/SQLSTATE come from
    /// [`error_map::classify`]; `message` is the backend error's Display
    /// output, forwarded verbatim (as the wire frontend has always done).
    #[error("{message}")]
    Db {
        /// Mapped MySQL error code (e.g. 1062 for duplicate entry).
        code: u16,
        /// Mapped SQLSTATE (e.g. `b"23000"`).
        sqlstate: [u8; 5],
        /// The backend error message, verbatim.
        message: String,
    },
}

impl SessionError {
    /// The mapped MySQL error code.
    #[must_use]
    pub fn code(&self) -> u16 {
        match self {
            Self::Translate(_) => ER_PARSE_ERROR,
            Self::Db { code, .. } => *code,
        }
    }

    /// The mapped SQLSTATE.
    #[must_use]
    pub fn sqlstate(&self) -> [u8; 5] {
        match self {
            Self::Translate(_) => *b"42000",
            Self::Db { sqlstate, .. } => *sqlstate,
        }
    }
}

/// The OK half of a [`SessionResult`]: everything a caller needs to frame a
/// MySQL OK packet (or an embedder needs to report an execute result).
#[derive(Debug, Clone)]
pub struct SessionOk {
    /// Rows changed by the statement (0 for DDL, transactions, and no-ops).
    pub affected_rows: u64,
    /// `last_insert_rowid`, clamped to 0 when absent or negative -- the
    /// same clamping the MySQL wire frontend has always applied.
    pub last_insert_id: u64,
    /// Whether the session is inside an explicit transaction *after* this
    /// statement. Mirrors [`Session::in_transaction`]; carried here so wire
    /// codecs can set `SERVER_STATUS_IN_TRANS` without a second lookup.
    pub in_transaction: bool,
    /// True when the statement was a dialect no-op (e.g. `SET NAMES
    /// utf8mb4`) or empty input, and therefore never reached the backend.
    ///
    /// The MySQL frontend frames no-ops in `COM_QUERY` as a default OK
    /// packet (no status flags) rather than one carrying the transaction
    /// flag; this field lets it preserve that behavior exactly.
    pub noop: bool,
}

/// Result of a successfully executed statement.
#[derive(Debug, Clone)]
pub enum SessionResult {
    /// A result set (SELECT, SHOW/DESCRIBE metadata emulation, PRAGMA, ...).
    Rows(ResultSet),
    /// An OK response (INSERT/UPDATE/DELETE/DDL/transaction control/no-op).
    Ok(SessionOk),
}

/// A dialect-aware SQL session over one backend connection.
///
/// One `Session` corresponds to one client (one MySQL wire connection, or
/// one embedder thread). It is intentionally `&mut self` on the query path:
/// transaction state is per-session and must not be shared.
pub struct Session {
    /// The underlying per-session backend connection.
    ///
    /// Public as a deliberate escape hatch: SQL sent directly through
    /// `conn` bypasses dialect translation, transaction-state tracking, and
    /// error mapping. The MySQL frontend uses it for
    /// [`BackendConn::describe_columns`] during `COM_STMT_PREPARE`.
    pub conn: Box<dyn BackendConn>,

    /// Whether the session believes it is inside an explicit transaction.
    ///
    /// Updated automatically when [`Session::query`] /
    /// [`Session::run_translated`] executes `BEGIN` / `START TRANSACTION`
    /// (set) or `COMMIT` / `ROLLBACK` (cleared) successfully. Public for
    /// introspection by wire codecs; it only drives status reporting, never
    /// backend behavior.
    pub in_transaction: bool,

    dialect: Dialect,
    cache: Arc<TranslateCache>,
}

impl Session {
    /// Create a session over `conn`, translating from `dialect`.
    ///
    /// Uses a private translation cache. If many sessions share one process
    /// (a wire frontend accepting many clients), prefer
    /// [`Session::with_cache`] with a shared cache so repeated statements
    /// are parsed once per process rather than once per session.
    #[must_use]
    pub fn new(conn: Box<dyn BackendConn>, dialect: Dialect) -> Self {
        Self::with_cache(conn, dialect, Arc::new(TranslateCache::default()))
    }

    /// Create a session that shares `cache` with other sessions.
    #[must_use]
    pub fn with_cache(
        conn: Box<dyn BackendConn>,
        dialect: Dialect,
        cache: Arc<TranslateCache>,
    ) -> Self {
        Self {
            conn,
            in_transaction: false,
            dialect,
            cache,
        }
    }

    /// The dialect this session translates from.
    #[must_use]
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// Translate `query` and return the **first** translated statement as
    /// `(sqlite_sql, kind)`. No-ops return `(String::new(), StatementKind::Other)`.
    ///
    /// # Multi-statement note
    ///
    /// [`litewire_translate::translate`] returns a `Vec` because one input
    /// can legitimately expand to several SQLite statements on the MySQL
    /// path: (a) multi-statement input text, and (b) `ALTER TABLE ... ADD
    /// {KEY|INDEX|UNIQUE}`, which expands into one `CREATE INDEX` per key
    /// plus an optional residual `ALTER TABLE`. This helper exists for the
    /// prepared-statement path (`COM_STMT_PREPARE`), which is
    /// single-statement by protocol definition, and keeps that path's
    /// historical first-result-only behavior. Text-protocol statements
    /// should go through [`Session::query`], which executes *all*
    /// translated statements.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Translate`] if the SQL cannot be parsed or
    /// contains unsupported constructs.
    pub fn translate_sql(&self, query: &str) -> Result<(String, StatementKind), SessionError> {
        let translated = translate_cached(&self.cache, query, self.dialect)?;

        let Some(result) = translated.into_iter().next() else {
            return Ok((String::new(), StatementKind::Other));
        };

        match result {
            TranslateResult::Noop => Ok((String::new(), StatementKind::Other)),
            TranslateResult::Metadata(meta) => {
                let sql = meta.to_sqlite_sql();
                Ok((sql, StatementKind::Query))
            }
            TranslateResult::Sql(sql) => {
                let kind = classify(&sql);
                Ok((sql, kind))
            }
        }
    }

    /// Translate `sql` from the session dialect and execute it.
    ///
    /// Semantics (identical to the MySQL wire frontend's text protocol):
    ///
    /// * **No-ops** (`SET NAMES ...`, `LOCK TABLES`, ...) return
    ///   [`SessionResult::Ok`] with `noop = true` without touching the
    ///   backend. Empty input behaves the same way.
    /// * **Metadata queries** (`SHOW ...`, `DESCRIBE`,
    ///   `information_schema` selects) execute their SQLite emulation SQL
    ///   and return its rows.
    /// * **Transaction control** (`BEGIN`/`START TRANSACTION`, `COMMIT`,
    ///   `ROLLBACK`) executes against the backend and, on success, updates
    ///   [`Session::in_transaction`].
    /// * When translation expands to **multiple statements** (see
    ///   [`Session::translate_sql`]), every statement is executed in order;
    ///   the first error stops execution and the result of the **last**
    ///   statement is returned.
    ///
    /// `params` bind to `?` placeholders in the translated SQL. Because a
    /// single placeholder list cannot be split across several statements,
    /// passing a non-empty `params` with input that expands to more than
    /// one statement is an error. (Metadata emulation SQL never contains
    /// placeholders; `params` are ignored on that path, as they always have
    /// been on the wire path.)
    ///
    /// # Errors
    ///
    /// [`SessionError::Translate`] on parse/translation failure,
    /// [`SessionError::Db`] with the mapped MySQL error triple when the
    /// backend rejects a statement.
    pub async fn query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<SessionResult, SessionError> {
        let translated = translate_cached(&self.cache, sql, self.dialect)?;

        if translated.len() > 1 && !params.is_empty() {
            return Err(SessionError::Translate(TranslateError::Unsupported(
                "parameters cannot be bound to a statement that expands to multiple \
                 SQLite statements"
                    .to_string(),
            )));
        }

        // Empty input (zero statements) returns OK, as the wire path does.
        let mut last = SessionResult::Ok(self.ok(0, 0, true));

        for result in translated {
            last = self.dispatch(result, params).await?;
        }

        Ok(last)
    }

    /// Execute one already-translated SQLite statement of a known
    /// [`StatementKind`], updating transaction state for
    /// [`StatementKind::Transaction`] statements.
    ///
    /// This is the prepared-statement execution path: the MySQL frontend
    /// translates at `COM_STMT_PREPARE` time via [`Session::translate_sql`]
    /// and replays the cached SQL here on every `COM_STMT_EXECUTE`.
    ///
    /// Transaction-control statements execute without parameters (they
    /// take none), matching the wire frontend's historical behavior.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Db`] with the mapped MySQL error triple when
    /// the backend rejects the statement.
    pub async fn run_translated(
        &mut self,
        sql: &str,
        kind: StatementKind,
        params: &[Value],
    ) -> Result<SessionResult, SessionError> {
        match kind {
            StatementKind::Query => {
                let rs = self
                    .conn
                    .query(sql, params)
                    .await
                    .map_err(map_backend_err)?;
                Ok(SessionResult::Rows(rs))
            }
            StatementKind::Transaction => {
                self.conn.execute(sql, &[]).await.map_err(map_backend_err)?;
                // Update state only after the backend accepted the
                // statement -- a failed COMMIT must not clear the flag.
                let upper = sql.trim().to_ascii_uppercase();
                if upper.starts_with("BEGIN") || upper.starts_with("START") {
                    self.in_transaction = true;
                } else if upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") {
                    self.in_transaction = false;
                }
                Ok(SessionResult::Ok(self.ok(0, 0, false)))
            }
            _ => {
                let r = self
                    .conn
                    .execute(sql, params)
                    .await
                    .map_err(map_backend_err)?;
                // last_insert_rowid comes back as i64 -- clamp negatives
                // (should never happen; SQLite rowids are always >= 1 for a
                // real insert) to 0 rather than reinterpret via `as u64`.
                let last_id: u64 = r
                    .last_insert_rowid
                    .and_then(|v| u64::try_from(v.max(0)).ok())
                    .unwrap_or(0);
                Ok(SessionResult::Ok(self.ok(r.affected_rows, last_id, false)))
            }
        }
    }

    /// Execute a single [`TranslateResult`].
    async fn dispatch(
        &mut self,
        result: TranslateResult,
        params: &[Value],
    ) -> Result<SessionResult, SessionError> {
        match result {
            TranslateResult::Noop => Ok(SessionResult::Ok(self.ok(0, 0, true))),
            TranslateResult::Metadata(meta) => {
                // Metadata emulation SQL never contains placeholders; the
                // wire frontend has always executed it with no parameters.
                let sql = meta.to_sqlite_sql();
                self.run_translated(&sql, StatementKind::Query, &[]).await
            }
            TranslateResult::Sql(sql) => {
                let kind = classify(&sql);
                self.run_translated(&sql, kind, params).await
            }
        }
    }

    /// Build a [`SessionOk`] snapshot of the current transaction state.
    fn ok(&self, affected_rows: u64, last_insert_id: u64, noop: bool) -> SessionOk {
        SessionOk {
            affected_rows,
            last_insert_id,
            in_transaction: self.in_transaction,
            noop,
        }
    }
}

/// Map a backend error to [`SessionError::Db`] via [`error_map::classify`].
///
/// The classified message is the error's Display output, exactly what the
/// MySQL wire frontend has always forwarded to clients.
fn map_backend_err(e: BackendError) -> SessionError {
    let mapped = error_map::classify(&e.to_string());
    SessionError::Db {
        code: mapped.code,
        sqlstate: mapped.sqlstate,
        message: mapped.message,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use litewire_backend::{Column, ExecuteResult, Rusqlite, SharedBackend};

    use super::*;

    async fn memory_session() -> Session {
        let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
        let conn = backend.connect().await.unwrap();
        Session::new(conn, Dialect::MySQL)
    }

    fn rows(result: SessionResult) -> ResultSet {
        match result {
            SessionResult::Rows(rs) => rs,
            SessionResult::Ok(ok) => panic!("expected rows, got OK: {ok:?}"),
        }
    }

    fn ok(result: SessionResult) -> SessionOk {
        match result {
            SessionResult::Ok(ok) => ok,
            SessionResult::Rows(rs) => panic!("expected OK, got rows: {rs:?}"),
        }
    }

    // ── A counting backend conn: proves no-ops never reach the backend ────

    #[derive(Default)]
    struct CountingConn {
        queries: AtomicUsize,
        executes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BackendConn for CountingConn {
        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<ResultSet, BackendError> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            Ok(ResultSet {
                columns: vec![],
                rows: vec![],
            })
        }

        async fn execute(
            &self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<ExecuteResult, BackendError> {
            self.executes.fetch_add(1, Ordering::SeqCst);
            Ok(ExecuteResult {
                affected_rows: 0,
                last_insert_rowid: None,
            })
        }

        async fn describe_columns(&self, _sql: &str) -> Result<Vec<Column>, BackendError> {
            Ok(vec![])
        }
    }

    // ── Noop path ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_names_is_ok_and_never_touches_backend() {
        let counter = Arc::new(CountingConn::default());
        // A second Arc handle so we can read the counters after Session
        // takes ownership of the Box.
        struct Shared(Arc<CountingConn>);
        #[async_trait::async_trait]
        impl BackendConn for Shared {
            async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
                self.0.query(sql, params).await
            }
            async fn execute(
                &self,
                sql: &str,
                params: &[Value],
            ) -> Result<ExecuteResult, BackendError> {
                self.0.execute(sql, params).await
            }
        }
        let mut session = Session::new(Box::new(Shared(Arc::clone(&counter))), Dialect::MySQL);

        let result = ok(session.query("SET NAMES utf8mb4", &[]).await.unwrap());
        assert!(result.noop);
        assert_eq!(result.affected_rows, 0);
        assert_eq!(result.last_insert_id, 0);
        assert!(!result.in_transaction);
        assert_eq!(counter.queries.load(Ordering::SeqCst), 0);
        assert_eq!(counter.executes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn empty_input_is_noop_ok() {
        let mut session = memory_session().await;
        let result = ok(session.query("", &[]).await.unwrap());
        assert!(result.noop);
    }

    // ── Basic CRUD ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn crud_with_params() {
        let mut session = memory_session().await;
        ok(session
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &[])
            .await
            .unwrap());

        let ins = ok(session
            .query(
                "INSERT INTO t (name) VALUES (?)",
                &[Value::Text("alice".into())],
            )
            .await
            .unwrap());
        assert_eq!(ins.affected_rows, 1);
        assert_eq!(ins.last_insert_id, 1);
        assert!(!ins.noop);

        let rs = rows(
            session
                .query("SELECT id, name FROM t WHERE id = ?", &[Value::Integer(1)])
                .await
                .unwrap(),
        );
        assert_eq!(rs.columns.len(), 2);
        assert_eq!(
            rs.rows,
            vec![vec![Value::Integer(1), Value::Text("alice".into())]]
        );
    }

    // ── Metadata path ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn show_tables_runs_metadata_emulation() {
        let mut session = memory_session().await;
        ok(session
            .query("CREATE TABLE alpha (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap());
        ok(session
            .query("CREATE TABLE beta (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap());

        let rs = rows(session.query("SHOW TABLES", &[]).await.unwrap());
        let names: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Text(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(names.contains(&"alpha".to_string()), "got: {names:?}");
        assert!(names.contains(&"beta".to_string()), "got: {names:?}");
    }

    // ── Transaction state ──────────────────────────────────────────────────

    #[tokio::test]
    async fn transaction_state_tracks_begin_commit_rollback() {
        let mut session = memory_session().await;
        ok(session
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap());
        assert!(!session.in_transaction);

        let begun = ok(session.query("BEGIN", &[]).await.unwrap());
        assert!(begun.in_transaction);
        assert!(session.in_transaction);

        // OKs issued inside the transaction carry the flag.
        let ins = ok(session
            .query("INSERT INTO t (id) VALUES (1)", &[])
            .await
            .unwrap());
        assert!(ins.in_transaction);

        let committed = ok(session.query("COMMIT", &[]).await.unwrap());
        assert!(!committed.in_transaction);
        assert!(!session.in_transaction);

        let started = ok(session.query("START TRANSACTION", &[]).await.unwrap());
        assert!(started.in_transaction);
        let rolled = ok(session.query("ROLLBACK", &[]).await.unwrap());
        assert!(!rolled.in_transaction);
        assert!(!session.in_transaction);
    }

    #[tokio::test]
    async fn failed_transaction_statement_does_not_change_state() {
        let mut session = memory_session().await;
        // COMMIT with no open transaction: SQLite rejects it; the flag must
        // stay clear (state only updates on success).
        let err = session.query("COMMIT", &[]).await.unwrap_err();
        assert!(matches!(err, SessionError::Db { .. }));
        assert!(!session.in_transaction);

        // And a failed BEGIN (nested) must not double-set anything odd.
        ok(session.query("BEGIN", &[]).await.unwrap());
        assert!(session.in_transaction);
        let err = session.query("BEGIN", &[]).await.unwrap_err();
        assert!(matches!(err, SessionError::Db { .. }));
        assert!(session.in_transaction, "flag must survive a failed BEGIN");
    }

    // ── Error mapping ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn duplicate_key_maps_to_1062() {
        let mut session = memory_session().await;
        ok(session
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap());
        ok(session
            .query("INSERT INTO t (id, v) VALUES (1, 'a')", &[])
            .await
            .unwrap());

        let err = session
            .query("INSERT INTO t (id, v) VALUES (1, 'b')", &[])
            .await
            .unwrap_err();
        assert_eq!(err.code(), error_map::ER_DUP_ENTRY);
        assert_eq!(err.sqlstate(), *b"23000");
        assert!(
            err.to_string().to_ascii_lowercase().contains("unique"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn parse_error_maps_to_1064() {
        let mut session = memory_session().await;
        let err = session
            .query("NOT VALID SQL !!! @@@ {{{}}", &[])
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::Translate(_)));
        assert_eq!(err.code(), ER_PARSE_ERROR);
        assert_eq!(err.sqlstate(), *b"42000");
    }

    // ── Multi-result inputs (the Vec<TranslateResult> question) ────────────
    //
    // `translate()` returns Vec<TranslateResult> and the MySQL path really
    // does produce more than one element for (a) `ALTER TABLE ... ADD
    // KEY/INDEX/UNIQUE` (expanded to CREATE INDEX statements) and (b)
    // multi-statement input text. Session::query must execute all of them.

    #[tokio::test]
    async fn alter_table_add_keys_executes_every_expanded_statement() {
        let mut session = memory_session().await;
        ok(session
            .query(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
                &[],
            )
            .await
            .unwrap());

        // Expands to two CREATE INDEX statements; both must execute.
        ok(session
            .query("ALTER TABLE t ADD KEY idx_a (a), ADD KEY idx_b (b)", &[])
            .await
            .unwrap());

        let rs = rows(
            session
                .query(
                    "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
                    &[],
                )
                .await
                .unwrap(),
        );
        let names: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Text(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(names.contains(&"idx_a".to_string()), "got: {names:?}");
        assert!(names.contains(&"idx_b".to_string()), "got: {names:?}");
    }

    #[tokio::test]
    async fn multi_statement_input_executes_all_and_returns_last() {
        let mut session = memory_session().await;
        ok(session
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap());

        let last = ok(session
            .query(
                "INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2)",
                &[],
            )
            .await
            .unwrap());
        // Result reflects the LAST statement.
        assert_eq!(last.affected_rows, 1);
        assert_eq!(last.last_insert_id, 2);

        let rs = rows(session.query("SELECT COUNT(*) FROM t", &[]).await.unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Integer(2)]]);
    }

    #[tokio::test]
    async fn params_with_multi_statement_expansion_is_an_error() {
        let mut session = memory_session().await;
        let err = session
            .query(
                "INSERT INTO t (id) VALUES (?); INSERT INTO t (id) VALUES (?)",
                &[Value::Integer(1), Value::Integer(2)],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SessionError::Translate(TranslateError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn multi_statement_error_stops_execution() {
        let mut session = memory_session().await;
        ok(session
            .query("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap());
        ok(session
            .query("INSERT INTO t (id) VALUES (1)", &[])
            .await
            .unwrap());

        // First statement fails (duplicate key); the second must not run.
        let err = session
            .query(
                "INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (2)",
                &[],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::Db { .. }));

        let rs = rows(session.query("SELECT COUNT(*) FROM t", &[]).await.unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![Value::Integer(1)]],
            "id=2 must not exist"
        );
    }

    // ── translate_sql (prepared path) ──────────────────────────────────────

    #[tokio::test]
    async fn translate_sql_first_result_semantics() {
        let session = memory_session().await;

        let (sql, kind) = session.translate_sql("SET NAMES utf8mb4").unwrap();
        assert!(sql.is_empty());
        assert_eq!(kind, StatementKind::Other);

        let (sql, kind) = session.translate_sql("SHOW TABLES").unwrap();
        assert!(sql.contains("sqlite_master"));
        assert_eq!(kind, StatementKind::Query);

        let (sql, kind) = session.translate_sql("SELECT 1").unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);

        assert!(session.translate_sql("!!! not sql").is_err());
    }
}
