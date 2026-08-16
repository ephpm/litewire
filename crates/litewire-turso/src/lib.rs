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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Duration, Instant};

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
    max_idle: usize,
    idle_max_age: Duration,
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

    /// **Experimental** — enable idle-connection reuse with room for
    /// `max_idle` parked connections (0, the default, disables reuse).
    ///
    /// A wire session's connection is normally freed when the session
    /// ends. With reuse on, a *clean* connection (autocommit, no temp
    /// objects, no drifted `PRAGMA`s) is instead parked and handed to a
    /// later session, keeping the engine's per-connection prepared-
    /// statement cache warm. State hygiene is validated at checkout, so a
    /// dirty connection is discarded rather than leaked (see
    /// [`ReuseStats`]).
    ///
    /// Ignored when [`TursoBuilder::enable_cdc_on_connect`] is set: CDC
    /// capture is per-connection state the reuse fingerprint does not
    /// model.
    #[must_use]
    pub fn handle_reuse(mut self, max_idle: usize) -> Self {
        self.max_idle = max_idle;
        self
    }

    /// Maximum time a parked connection may sit idle before it is
    /// discarded at checkout instead of reused (default 60s). Bounds how
    /// long a reused handle can pin resources and how stale a cached
    /// schema view may be.
    #[must_use]
    pub fn idle_max_age(mut self, age: Duration) -> Self {
        self.idle_max_age = age;
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
        let reuse = if self.max_idle > 0 && !self.enable_cdc_on_connect {
            Some(Arc::new(ReusePool::new(self.max_idle, self.idle_max_age)))
        } else {
            if self.max_idle > 0 && self.enable_cdc_on_connect {
                tracing::warn!(
                    "turso reuse: handle_reuse ignored because \
                     enable_cdc_on_connect is set (CDC capture is \
                     per-connection state the reuse layer does not track)"
                );
            }
            None
        };
        Ok(Turso {
            db,
            busy_timeout_ms: self.busy_timeout_ms,
            synchronous: self.synchronous,
            enable_cdc_on_connect: self.enable_cdc_on_connect,
            reuse,
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
    /// Idle-connection reuse pool, present iff enabled via
    /// [`TursoBuilder::handle_reuse`]. See the reuse-pool section below.
    reuse: Option<Arc<ReusePool>>,
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
            max_idle: 0,
            idle_max_age: Duration::from_secs(60),
        }
    }

    /// Snapshot the idle-connection reuse counters, or `None` if reuse
    /// is disabled (the default). See [`TursoBuilder::handle_reuse`].
    #[must_use]
    pub fn reuse_stats(&self) -> Option<ReuseStats> {
        self.reuse.as_ref().map(|p| p.stats())
    }

    /// Open a fresh per-session connection with the standard session
    /// setup (busy timeout, `synchronous`, optional CDC capture).
    async fn fresh_connection(&self) -> Result<turso::Connection, BackendError> {
        let conn = self.db.connect().map_err(map_turso_err)?;
        apply_session_setup(&conn, self.busy_timeout_ms, self.synchronous).await?;
        if self.enable_cdc_on_connect {
            cdc::enable_cdc(&conn).await?;
        }
        Ok(conn)
    }
}

/// Per-session Turso handle.
///
/// Owns exactly one [`turso::Connection`]. A [`tokio::sync::Mutex`]
/// serializes statements on the same session (held across `.await`, which
/// is why this is the tokio mutex and not a std/parking_lot one).
pub struct TursoConn {
    conn: Mutex<turso::Connection>,
    /// Pool to park this connection back into on drop, if reuse is
    /// enabled. `None` disables parking (and is what a discarded checkout
    /// candidate carries, so it cannot re-park itself).
    reuse: Option<Arc<ReusePool>>,
    /// `last_insert_rowid()` observed at checkout. A reused connection
    /// inherits the previous session's value (the engine exposes no
    /// setter), so `execute` reports a rowid only once it moves past this
    /// baseline — never leaking the prior session's insert id.
    baseline_rowid: i64,
    /// Set once this session runs a statement that leaves connection-
    /// scoped state behind (see [`statement_dirties_connection`]); a dirty
    /// connection is never parked. Always `false` when reuse is disabled.
    dirty: AtomicBool,
}

impl TursoConn {
    /// Flag the connection dirty if `sql` leaves connection-scoped state
    /// behind, so [`Drop`] will not park it. A cheap keyword scan, and a
    /// no-op when reuse is disabled (the default).
    fn mark_dirty_if_needed(&self, sql: &str) {
        if self.reuse.is_some() && statement_dirties_connection(sql) {
            self.dirty.store(true, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl Backend for Turso {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        // Reuse path: hand out a parked, still-clean connection so its
        // warm prepared-statement cache survives across sessions.
        if let Some(pool) = &self.reuse {
            while let Some(conn) = pool.take_fresh() {
                if ReusePool::reusable_at_checkout(&conn) {
                    pool.hits.fetch_add(1, Ordering::Relaxed);
                    let baseline_rowid = conn.last_insert_rowid();
                    return Ok(Box::new(TursoConn {
                        conn: Mutex::new(conn),
                        reuse: Some(Arc::clone(pool)),
                        baseline_rowid,
                        dirty: AtomicBool::new(false),
                    }));
                }
                // Defensive: not in autocommit (engine's lazy dangling-tx
                // handling). Drop it (its `reuse` is None, so no re-park)
                // and try the next parked connection.
                pool.discards.fetch_add(1, Ordering::Relaxed);
            }
            pool.misses.fetch_add(1, Ordering::Relaxed);
        }
        let conn = self.fresh_connection().await?;
        // Fresh connection: last_insert_rowid() is 0, so the baseline is a
        // no-op and reporting matches the pre-reuse behaviour exactly.
        let baseline_rowid = conn.last_insert_rowid();
        Ok(Box::new(TursoConn {
            conn: Mutex::new(conn),
            reuse: self.reuse.clone(),
            baseline_rowid,
            dirty: AtomicBool::new(false),
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
/// time the name appears. `$name` additionally swallows SQLite's TCL-heritage
/// suffixes (`$name::seg` repetitions and one trailing `(...)`) as part of
/// the variable name, matching tokenize.c. The result is the highest index
/// assigned — the same value `sqlite3_bind_parameter_count()` reports.
///
/// Quoted strings (`'…'`), quoted identifiers (`"…"`, `` `…` ``, `[…]`),
/// line comments (`--`) and block comments (`/* … */`) are skipped.
///
/// TODO(turso >0.7): delete this scanner and delegate to the statement's
/// parameter count once the turso crate exposes it publicly.
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
                if bytes[start] == b'$' {
                    // TCL-heritage suffixes are part of the variable name
                    // (sqlite tokenize.c): `$name::seg` repetitions, then
                    // optionally one non-nested `(...)`.
                    while i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
                        i += 2;
                        while i < bytes.len()
                            && (bytes[i].is_ascii_alphanumeric()
                                || bytes[i] == b'_'
                                || bytes[i] == b'$')
                        {
                            i += 1;
                        }
                    }
                    if i < bytes.len() && bytes[i] == b'(' {
                        while i < bytes.len() && bytes[i] != b')' {
                            i += 1;
                        }
                        i = (i + 1).min(bytes.len());
                    }
                }
                if i > start + 1 || bytes[start] == b'$' {
                    let name = &sql[start..i];
                    // O(n) scan is fine: real statements carry < 10 distinct
                    // named parameters.
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
        self.mark_dirty_if_needed(sql);
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
        self.mark_dirty_if_needed(sql);
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
            // Suppress the inherited rowid a reused connection carries from
            // the prior session (baseline); report only this session's own
            // inserts. For a fresh connection baseline is 0, so this is
            // identical to the previous `last_id != 0` check.
            last_insert_rowid: if last_id != 0 && last_id != self.baseline_rowid {
                Some(last_id)
            } else {
                None
            },
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

// ---------------------------------------------------------------------------
// Idle-connection reuse pool (opt-in; see `TursoBuilder::handle_reuse`)
// ---------------------------------------------------------------------------
//
// When enabled, a wire session's `turso::Connection` is parked on drop
// instead of freed, and a later `Backend::connect` checks it back out —
// keeping the engine's per-connection prepared-statement cache warm across
// sessions. That cache is the whole prize: on this engine `connect()` itself
// is cheap, but the first `prepare` of each distinct statement on a fresh
// connection is the dominant per-session cost that a warm handle avoids.

/// Apply the standard per-session setup (busy timeout, `synchronous`) shared
/// by fresh connections and the fingerprint-calibration probe.
async fn apply_session_setup(
    conn: &turso::Connection,
    busy_timeout_ms: u32,
    synchronous: Synchronous,
) -> Result<(), BackendError> {
    conn.busy_timeout(Duration::from_millis(u64::from(busy_timeout_ms)))
        .map_err(map_turso_err)?;
    // The engine's own default is synchronous=FULL; bring the session to
    // parity with the rusqlite backend (NORMAL by default).
    conn.pragma_update("synchronous", synchronous.as_pragma_str())
        .await
        .map_err(map_turso_err)?;
    Ok(())
}

/// Does executing `sql` leave *connection-scoped* state behind that must
/// not bleed into a reused session? If so the connection is marked dirty
/// and never parked — a fail-safe denylist: a false positive only costs a
/// fresh connect, never correctness.
///
/// The vectors, and why the list is short:
/// * A wire client's `SET ...` statements translate to no-ops (they never
///   reach the engine as pragmas), so the only way to change a
///   connection-scoped pragma is a literal `PRAGMA name = value` — caught
///   generically by "starts with PRAGMA and assigns".
/// * `CREATE TEMP`/`TEMPORARY ...` makes a connection-local object
///   (visible via `sqlite_temp_master`) other sessions must not see.
/// * `ATTACH`/`DETACH` change the attached-database set.
///
/// Metadata reads (`SELECT ... FROM pragma_table_info(...)`, or a
/// non-assigning `PRAGMA table_info(...)`) are *not* dirtying: they read,
/// and the pragma-TVF phantom-transaction workaround restores autocommit.
///
/// One O(len) keyword scan per statement — orders of magnitude cheaper than
/// the per-checkout `PRAGMA`-readback probe it replaced, which measured as
/// costly as the statement-cache win it was meant to protect.
fn statement_dirties_connection(sql: &str) -> bool {
    let s = strip_leading_trivia(sql);
    let kw = leading_word(s);
    if kw.eq_ignore_ascii_case("PRAGMA") {
        // A pragma *write* assigns (`PRAGMA x = y`); a read does not.
        return s.contains('=');
    }
    if kw.eq_ignore_ascii_case("ATTACH") || kw.eq_ignore_ascii_case("DETACH") {
        return true;
    }
    if kw.eq_ignore_ascii_case("CREATE") {
        let rest = strip_leading_trivia(&s[kw.len()..]);
        let w2 = leading_word(rest);
        return w2.eq_ignore_ascii_case("TEMP") || w2.eq_ignore_ascii_case("TEMPORARY");
    }
    false
}

/// Skip leading whitespace and SQL comments (`-- …`, `/* … */`) so the
/// keyword scan sees the real first token even behind a comment prefix.
fn strip_leading_trivia(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.find('\n').map_or("", |i| &rest[i + 1..]).trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.find("*/").map_or("", |i| &rest[i + 2..]).trim_start();
        } else {
            return s;
        }
    }
}

/// The leading run of identifier characters (a SQL keyword), stopping at
/// the first non-`[A-Za-z0-9_]` byte.
fn leading_word(s: &str) -> &str {
    let end = s
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(s.len());
    &s[..end]
}

/// One parked, idle connection and when it was parked (for age-out).
struct IdleEntry {
    conn: turso::Connection,
    parked_at: Instant,
}

/// Observability counters for the reuse pool. Snapshot via
/// [`Turso::reuse_stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReuseStats {
    /// Checkouts satisfied by a clean idle connection.
    pub hits: u64,
    /// Checkouts that fell through to a fresh connect (pool empty of clean
    /// candidates).
    pub misses: u64,
    /// Idle connections rejected at checkout (aged out, or not in
    /// autocommit).
    pub discards: u64,
    /// Connections successfully parked on drop.
    pub parked: u64,
    /// Connections dropped at park because the pool was already full.
    pub dropped_full: u64,
}

/// The idle pool and its counters.
struct ReusePool {
    idle: SyncMutex<Vec<IdleEntry>>,
    max_idle: usize,
    idle_max_age: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    discards: AtomicU64,
    parked: AtomicU64,
    dropped_full: AtomicU64,
}

impl ReusePool {
    /// Build an empty pool. No engine probe is needed: hygiene is enforced
    /// cheaply per statement (see [`statement_dirties_connection`]) and at
    /// park, not by a checkout-time readback.
    fn new(max_idle: usize, idle_max_age: Duration) -> Self {
        Self {
            idle: SyncMutex::new(Vec::new()),
            max_idle,
            idle_max_age,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            discards: AtomicU64::new(0),
            parked: AtomicU64::new(0),
            dropped_full: AtomicU64::new(0),
        }
    }

    /// Pop the most-recently-parked non-stale idle connection, discarding
    /// (and counting) any that have aged past `idle_max_age`.
    fn take_fresh(&self) -> Option<turso::Connection> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(entry) = idle.pop() {
            if entry.parked_at.elapsed() <= self.idle_max_age {
                return Some(entry.conn);
            }
            self.discards.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Park a clean connection, or drop it if the pool is at capacity.
    fn park(&self, conn: turso::Connection) {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if idle.len() >= self.max_idle {
            drop(idle);
            self.dropped_full.fetch_add(1, Ordering::Relaxed);
            return;
        }
        idle.push(IdleEntry {
            conn,
            parked_at: Instant::now(),
        });
        self.parked.fetch_add(1, Ordering::Relaxed);
    }

    /// Cheap checkout guard. Parking already refuses any connection that
    /// is mid-transaction or that ran a dirtying statement, so a parked
    /// handle is known-clean; this only re-confirms autocommit (a free
    /// sync call) as defence against the engine's lazy dangling-transaction
    /// handling. No `.await`, no queries — the checkout hot path stays
    /// free.
    fn reusable_at_checkout(conn: &turso::Connection) -> bool {
        matches!(conn.is_autocommit(), Ok(true))
    }

    fn stats(&self) -> ReuseStats {
        ReuseStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            discards: self.discards.load(Ordering::Relaxed),
            parked: self.parked.load(Ordering::Relaxed),
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
        }
    }
}

impl Drop for TursoConn {
    fn drop(&mut self) {
        let Some(pool) = self.reuse.take() else {
            return;
        };
        // A statement that left connection-scoped state behind (a temp
        // object, a pragma write, an ATTACH) makes this connection unsafe
        // to hand to another session — never park it.
        if self.dirty.load(Ordering::Relaxed) {
            return;
        }
        // `get_mut` needs no lock: we hold `&mut self`, so no other task
        // can be mid-statement on this connection.
        let conn = self.conn.get_mut();
        // Never park a connection with an open transaction: a reused
        // session must start in autocommit. `is_autocommit` is a cheap sync
        // call. Cloning shares the inner `Arc<TursoConnection>`, so the
        // parked handle keeps the warm prepared-statement cache alive.
        if matches!(conn.is_autocommit(), Ok(true)) {
            pool.park(conn.clone());
        }
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
        // TCL-heritage `$` suffixes are part of the variable name, matching
        // sqlite3_bind_parameter_count() (review differential-test finding).
        assert_eq!(expected_param_count("SELECT $foo::type"), 1);
        assert_eq!(expected_param_count("SELECT $foo::type(1,2)"), 1);
        assert_eq!(expected_param_count("SELECT $foo(1)"), 1);
        assert_eq!(expected_param_count("SELECT $a::b::c, $a::b::c"), 1);
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
    // -- Idle-connection reuse pool ---------------------------------------

    async fn reuse_backend(max_idle: usize) -> Turso {
        Turso::builder(":memory:")
            .handle_reuse(max_idle)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn reuse_disabled_by_default() {
        let backend = Turso::memory().await.unwrap();
        assert!(backend.reuse_stats().is_none());
        // Sessions still work with no pool.
        let a = backend.connect().await.unwrap();
        a.execute("CREATE TABLE t (id INTEGER)", &[]).await.unwrap();
        drop(a);
        let b = backend.connect().await.unwrap();
        b.execute("INSERT INTO t VALUES (1)", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn clean_connection_is_parked_and_reused() {
        let backend = reuse_backend(4).await;
        {
            let a = backend.connect().await.unwrap();
            a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
                .await
                .unwrap();
            a.execute("INSERT INTO t VALUES (1, 'x')", &[])
                .await
                .unwrap();
        } // `a` dropped -> parked (autocommit, no temp, clean pragmas).
        let s = backend.reuse_stats().unwrap();
        assert_eq!(s.parked, 1, "clean connection should have parked: {s:?}");

        let b = backend.connect().await.unwrap();
        let s = backend.reuse_stats().unwrap();
        assert_eq!(
            s.hits, 1,
            "second connect should reuse the parked conn: {s:?}"
        );
        let rs = b.query("SELECT v FROM t WHERE id = 1", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text("x".into()));
    }

    #[tokio::test]
    async fn temp_table_does_not_leak_across_reuse() {
        let backend = reuse_backend(4).await;
        {
            let a = backend.connect().await.unwrap();
            a.execute("CREATE TEMP TABLE leak (id INTEGER)", &[])
                .await
                .unwrap();
            a.execute("INSERT INTO leak VALUES (1)", &[]).await.unwrap();
        } // ran CREATE TEMP -> dirty -> must not park.
        assert_eq!(
            backend.reuse_stats().unwrap().parked,
            0,
            "temp-carrying connection must not be parked"
        );
        let b = backend.connect().await.unwrap();
        // The temp table must not be visible to the new session.
        assert!(
            b.query("SELECT * FROM leak", &[]).await.is_err(),
            "temp table leaked into reused session"
        );
    }

    #[tokio::test]
    async fn pragma_write_prevents_parking() {
        let backend = reuse_backend(4).await;
        // Only meaningful if the engine actually honours a query_only write.
        let observed = {
            let a = backend.connect().await.unwrap();
            a.execute("PRAGMA query_only = 1", &[]).await.unwrap();
            let rs = a.query("PRAGMA query_only", &[]).await.unwrap();
            rs.rows[0][0].clone()
        }; // `a` ran a pragma write -> dirty -> must not park.
        if observed != Value::Integer(1) {
            return; // Engine ignores query_only; nothing to prove.
        }
        assert_eq!(
            backend.reuse_stats().unwrap().parked,
            0,
            "pragma-writing connection must not be parked"
        );
        // A fresh session is at the clean default.
        let c = backend.connect().await.unwrap();
        let rs = c.query("PRAGMA query_only", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(0));
    }

    #[tokio::test]
    async fn open_transaction_is_not_parked() {
        let backend = reuse_backend(4).await;
        {
            let a = backend.connect().await.unwrap();
            a.execute("CREATE TABLE t (id INTEGER)", &[]).await.unwrap();
            a.execute("BEGIN", &[]).await.unwrap();
            a.execute("INSERT INTO t VALUES (99)", &[]).await.unwrap();
            // dropped mid-transaction
        }
        let s = backend.reuse_stats().unwrap();
        assert_eq!(s.parked, 0, "open-transaction conn must not park: {s:?}");
        // The uncommitted row must not be visible to a new session.
        let b = backend.connect().await.unwrap();
        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(0));
    }

    #[tokio::test]
    async fn last_insert_rowid_does_not_leak_across_reuse() {
        let backend = reuse_backend(4).await;
        {
            let a = backend.connect().await.unwrap();
            a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
                .await
                .unwrap();
            let r = a
                .execute("INSERT INTO t VALUES (1, 'a')", &[])
                .await
                .unwrap();
            assert_eq!(r.last_insert_rowid, Some(1));
        } // parked with last_insert_rowid == 1.
        let b = backend.connect().await.unwrap();
        assert_eq!(backend.reuse_stats().unwrap().hits, 1);
        // A non-insert statement on the reused conn must NOT report the
        // inherited rowid 1.
        let r = b
            .execute("UPDATE t SET v = 'b' WHERE id = 1", &[])
            .await
            .unwrap();
        assert_eq!(
            r.last_insert_rowid, None,
            "reused conn leaked prior session's insert id"
        );
        // Its own insert reports its own id.
        let r = b
            .execute("INSERT INTO t VALUES (2, 'c')", &[])
            .await
            .unwrap();
        assert_eq!(r.last_insert_rowid, Some(2));
    }

    #[tokio::test]
    async fn stale_idle_connection_is_discarded() {
        let backend = Turso::builder(":memory:")
            .handle_reuse(4)
            .idle_max_age(Duration::from_millis(1))
            .build()
            .await
            .unwrap();
        {
            let a = backend.connect().await.unwrap();
            a.execute("CREATE TABLE t (id INTEGER)", &[]).await.unwrap();
        } // parked
        assert_eq!(backend.reuse_stats().unwrap().parked, 1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _b = backend.connect().await.unwrap();
        let s = backend.reuse_stats().unwrap();
        assert_eq!(s.hits, 0, "stale conn should not count as a hit: {s:?}");
        assert!(s.discards >= 1, "stale conn should be discarded: {s:?}");
        assert!(
            s.misses >= 1,
            "checkout should fall through to fresh: {s:?}"
        );
    }

    #[tokio::test]
    async fn max_idle_bounds_the_pool() {
        let backend = reuse_backend(1).await;
        // Two connections open at once, then both dropped: only one fits.
        let a = backend.connect().await.unwrap();
        a.execute("CREATE TABLE t (id INTEGER)", &[]).await.unwrap();
        let b = backend.connect().await.unwrap();
        drop(a);
        drop(b);
        let s = backend.reuse_stats().unwrap();
        assert_eq!(s.parked, 1, "pool capacity 1 should park one: {s:?}");
        assert_eq!(s.dropped_full, 1, "the overflow should be dropped: {s:?}");
    }
}
