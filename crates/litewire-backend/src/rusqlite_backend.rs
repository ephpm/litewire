//! In-process SQLite backend using `rusqlite`.
//!
//! # Per-connection isolation
//!
//! [`Rusqlite`] is a **factory**. Each call to [`Rusqlite::connect`] gives
//! every wire-protocol client its own `rusqlite::Connection`, and therefore
//! its own transaction context. SQLite's own file locking (WAL +
//! `busy_timeout`) coordinates writers between connections; WAL readers
//! proceed concurrently. This is strictly better than the old
//! shared-`Mutex<Connection>` design, which serialized every operation
//! process-wide and let one client's `BEGIN` swallow another client's
//! statements.
//!
//! # Threading: one session worker thread per session
//!
//! rusqlite is synchronous and SQLite calls can block for a long time --
//! `busy_timeout` alone permits a 5 second stall by default, and a large
//! `ROLLBACK` or a checkpoint can take milliseconds. **SQLite must
//! therefore never be called on a tokio async worker thread.** That is the
//! invariant this module exists to uphold.
//!
//! It upholds it by giving each session its own dedicated OS thread which
//! *owns* the `Connection`. [`BackendConn::query`] and friends become a
//! channel send plus an `.await` on a `oneshot`; no SQLite code ever runs
//! on an async worker. Three properties follow from the worker owning the
//! handle rather than sharing it:
//!
//! * **Cancellation is safe.** A caller whose future is dropped mid-query
//!   simply abandons its `oneshot`; the statement finishes on the worker,
//!   the reply is discarded, and the session stays usable. Nothing is left
//!   holding the connection.
//! * **End-of-session hygiene runs on the worker**, not on whatever thread
//!   happened to drop the session. Rolling back an abandoned write
//!   transaction cannot stall an async worker.
//! * **Statements on one session are naturally serialized** in arrival
//!   order by the channel, with no mutex.
//!
//! The cost is one OS thread per *live* session, plus one thread spawn per
//! session that is not served from the reuse pool below. Sessions are wire
//! connections, so this scales with concurrent clients, not with request
//! rate. A dedicated thread is used rather than
//! [`tokio::task::spawn_blocking`] deliberately: a session actor parks its
//! thread for the whole session lifetime, and doing that on the embedding
//! application's shared blocking pool would let idle SQLite sessions starve
//! -- or, if the embedder also calls into litewire from that pool,
//! deadlock -- everything else the pool is used for.
//!
//! # In-memory database caveat
//!
//! `rusqlite::Connection::open_in_memory()` and a bare `":memory:"` path
//! each create a *distinct* database per connection -- useless for
//! per-conn isolation because clients would not see each other's tables at
//! all.
//!
//! We considered SQLite's shared-cache URI syntax
//! (`file:name?mode=memory&cache=shared`) but shared-cache imposes its own
//! table-level locking that ignores WAL and refuses concurrent readers
//! while a writer holds a lock -- exactly the isolation regression this
//! design is trying to fix. Instead, [`Rusqlite::memory`] transparently
//! backs itself with a per-process temp file (deleted on drop). Consumers
//! still call `Rusqlite::memory()`; the file is an implementation detail
//! that gives us real WAL semantics and real per-connection isolation.
//!
//! # PRAGMAs
//!
//! [`Rusqlite::open`] runs `PRAGMA journal_mode=WAL` once on a temporary
//! bootstrap connection at construction. WAL mode is persistent (recorded
//! in the DB header), so all subsequent [`Rusqlite::connect`] sessions
//! inherit it. Every session sets `busy_timeout` (default 5000ms,
//! configurable via [`RusqliteBuilder`]) and `synchronous=NORMAL`, which
//! is the WAL-appropriate default -- fully durable across power loss with
//! substantially higher write throughput than `FULL`.
//!
//! # `prepare_cached`
//!
//! rusqlite's statement cache is per-`Connection`. With per-connection
//! backends, each wire session carries its own cache. Memory footprint
//! scales as `cache_size * concurrent_sessions`; the default cache is
//! small (16 statements per rusqlite), so this is a non-issue in practice
//! but worth noting for high-concurrency deployments.
//!
//! # Handle reuse (opt-in, default OFF)
//!
//! [`RusqliteBuilder::handle_reuse`] adds a bounded free-list of idle
//! session workers -- a parked thread still holding its open
//! `Connection` -- to the factory. [`Backend::connect`] takes one if
//! available; ending a session returns it after a hygiene pass instead of
//! closing the handle and retiring the thread. The point is to skip
//! `sqlite3_open`, the WAL-index attach, the per-session PRAGMAs, the cold
//! `prepare_cached` LRU *and* the thread spawn on every short-lived wire
//! session (the ePHPm single-node-SQLite shape: one PDO connection per PHP
//! request).
//!
//! **This does not weaken per-session isolation.** A worker (and hence its
//! handle) is only ever owned by one [`RusqliteConn`] at a time; it leaves
//! the idle list on `connect()` and only returns at end of session. Two
//! live sessions can never share a handle, so the transaction-mixing
//! failure mode the per-connection design exists to prevent is
//! unreachable. What reuse *does* introduce is **state carry-over across
//! sessions**, handled by [`IdlePool::reset`] -- see its docs for exactly
//! what is scrubbed and what is deliberately not.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use regex::Regex;
use rusqlite::Connection;
use rusqlite::types::ValueRef;
use tokio::sync::oneshot;

use crate::{Backend, BackendConn, BackendError, Column, ExecuteResult, ResultSet, Value};

/// Default ceiling on how long an idle pooled worker may sit in the
/// free-list before a checkout retires it instead of handing it out. See
/// [`RusqliteBuilder::idle_max_age`].
const DEFAULT_IDLE_MAX_AGE: Duration = Duration::from_secs(60);

/// The kind of database target.
#[derive(Clone, Debug)]
enum Target {
    /// User-provided file-backed database.
    File(PathBuf),
    /// Memory-like DB backed by an internally-managed temp file. The
    /// [`TempOwner`] handle deletes the file when the [`Rusqlite`] is
    /// dropped; per-session `connect()` calls just point at this path.
    MemoryTempFile(Arc<TempOwner>),
}

impl Target {
    fn as_path(&self) -> &Path {
        match self {
            Self::File(p) => p.as_path(),
            Self::MemoryTempFile(t) => t.path.as_path(),
        }
    }
}

/// Owns a temp file backing an "in-memory" database. Deletes the file
/// on drop (best-effort). Uses `Arc` so `Rusqlite` clones don't
/// accidentally delete the underlying file early.
#[derive(Debug)]
struct TempOwner {
    path: PathBuf,
}

impl Drop for TempOwner {
    fn drop(&mut self) {
        // Best-effort: ignore errors on cleanup. Also try the -wal and
        // -shm sidecars WAL leaves behind.
        let _ = std::fs::remove_file(&self.path);
        let mut wal = self.path.clone().into_os_string();
        wal.push("-wal");
        let _ = std::fs::remove_file(&wal);
        let mut shm = self.path.clone().into_os_string();
        shm.push("-shm");
        let _ = std::fs::remove_file(&shm);
    }
}

/// Detect whether a path refers to an in-memory database.
///
/// `":memory:"`, an empty string, or an explicit `file::memory:` URI all
/// map to a temp-file-backed [`Target::MemoryTempFile`]. Everything else
/// is treated as a regular file path.
fn classify_target(path: &Path) -> Target {
    let s = path.to_string_lossy();
    if s.is_empty() || s == ":memory:" || s.contains(":memory:") {
        Target::MemoryTempFile(Arc::new(TempOwner {
            path: temp_db_path(),
        }))
    } else {
        Target::File(path.to_path_buf())
    }
}

/// Build a unique temp-file path for an in-memory-like database.
fn temp_db_path() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let file = format!("litewire-mem-{pid}-{n}.sqlite");
    std::env::temp_dir().join(file)
}

/// Session-state fingerprint, run on a handle before its worker is
/// returned to the idle pool and compared against the value the same
/// statement produced on a pristine connection at factory construction.
/// Any difference means "discard this handle, do not pool it".
///
/// Each term covers *connection-scoped* state a wire client can change with
/// plain SQL. litewire's frontends pass `PRAGMA` straight through --
/// `litewire_translate::classify` maps it to `StatementKind::Query`, and
/// unlike the Turso backend the rusqlite path has no `reject_unsupported`
/// filter -- so all of this is reachable from any MySQL/Postgres client.
///
/// **The pristine values are calibrated at runtime, not hardcoded.** They
/// are a property of how SQLite was *compiled*, not of the SQLite spec:
/// `foreign_keys`, `automatic_index` and `trusted_schema` all read `1` on a
/// fresh handle here because `libsqlite3-sys`'s bundled build defines
/// `SQLITE_DEFAULT_FOREIGN_KEYS=1`, while stock SQLite defaults
/// `foreign_keys` to `0`. Baking in constants would silently break -- and
/// turn reuse into a no-op that discards every handle -- on any
/// rusqlite/SQLite bump that moves a default.
///
/// The boolean terms get distinct power-of-two weights and the unbounded
/// count terms are scaled past them, so no combination of changes can
/// cancel out into a false "clean" fingerprint.
///
/// Deliberately a *detector*, not a *repairer*: enumerating and restoring
/// every connection PRAGMA is not tractable, and silently repairing state
/// hides that a client did something surprising. Discarding costs one
/// `sqlite3_close` plus one `sqlite3_open` on the next session -- i.e. it
/// degrades to exactly the reuse-off behaviour, which is the safe
/// direction.
const STATE_FINGERPRINT_SQL: &str = "SELECT 1024 * ( \
         (SELECT count(*) FROM sqlite_temp_master) \
       + (SELECT count(*) FROM pragma_database_list WHERE name NOT IN ('main','temp')) \
     ) \
     +   1 * (SELECT * FROM pragma_query_only) \
     +   2 * (SELECT * FROM pragma_foreign_keys) \
     +   4 * (SELECT * FROM pragma_recursive_triggers) \
     +   8 * (SELECT * FROM pragma_defer_foreign_keys) \
     +  16 * (SELECT * FROM pragma_read_uncommitted) \
     +  32 * (SELECT * FROM pragma_reverse_unordered_selects) \
     +  64 * (SELECT * FROM pragma_cell_size_check) \
     + 128 * (SELECT * FROM pragma_automatic_index) \
     + 256 * (SELECT * FROM pragma_trusted_schema)";

/// Evaluate [`STATE_FINGERPRINT_SQL`] on `conn`. `None` means the probe
/// itself failed, which callers must treat as "dirty" -- a handle whose
/// state we cannot read is a handle we must not reuse.
///
/// Uses `prepare_cached` so the probe is parsed once per handle and every
/// subsequent hygiene pass on that handle is a bare `sqlite3_step`.
fn fingerprint(conn: &Connection) -> Option<i64> {
    conn.prepare_cached(STATE_FINGERPRINT_SQL)
        .ok()?
        .query_row([], |r| r.get(0))
        .ok()
}

// -- Session worker ------------------------------------------------------

/// One unit of work for a session worker thread.
///
/// Every variant that produces a result carries its own `oneshot` reply
/// channel, so a caller that is cancelled mid-statement drops the receiver
/// and the worker's `send` becomes a no-op. The worker never blocks on the
/// caller.
enum Job {
    /// Run a row-returning statement.
    Query {
        sql: String,
        params: Vec<Value>,
        reply: oneshot::Sender<Result<ResultSet, BackendError>>,
    },
    /// Run a data-modifying statement.
    Execute {
        sql: String,
        params: Vec<Value>,
        reply: oneshot::Sender<Result<ExecuteResult, BackendError>>,
    },
    /// Describe a prepared statement's columns without executing it.
    Describe {
        sql: String,
        reply: oneshot::Sender<Result<Vec<Column>, BackendError>>,
    },
    /// The [`RusqliteConn`] was dropped and handle reuse is enabled. Run
    /// the hygiene pass and, if it passes, park this worker on the
    /// free-list for the next session.
    ///
    /// `back` is a `Sender` to this worker's own inbox. Handing it over in
    /// the message rather than keeping a copy on the worker is what lets
    /// the channel actually disconnect: while a worker is parked the *only*
    /// live `Sender` is the one held by the free-list, so dropping the
    /// factory drops it, `recv` fails, and the worker exits and closes its
    /// handle.
    EndSession { back: mpsc::Sender<Job> },
}

/// Open one per-session rusqlite `Connection` with all configured PRAGMAs
/// applied. Always called on a session worker thread, never on an async
/// worker.
fn open_session(
    path: &Path,
    busy_timeout_ms: u32,
    synchronous: Synchronous,
) -> Result<Connection, BackendError> {
    let conn = Connection::open(path).map_err(|e| BackendError::Sqlite(e.to_string()))?;
    conn.busy_timeout(Duration::from_millis(u64::from(busy_timeout_ms)))
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;
    conn.pragma_update(None, "synchronous", synchronous.as_pragma_str())
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;
    register_regexp(&conn).map_err(|e| BackendError::Sqlite(e.to_string()))?;
    Ok(conn)
}

/// Register the `regexp` scalar function that SQLite's `X REGEXP Y` operator
/// resolves to.
///
/// SQLite parses `REGEXP` but ships no implementation: the operator is
/// defined as a call to a two-argument `regexp(pattern, haystack)` function
/// the host application must supply, and without one every use fails with
/// "no such function: regexp". MySQL clients do use it — WordPress's
/// `populate_options()` deletes stale transients with
/// `WHERE option_name REGEXP '^rss_...'` at install time.
///
/// # Semantics, and where they differ from MySQL
///
/// * The pattern syntax is the Rust `regex` crate's, which is the same
///   engine Turso's built-in `regexp` uses, so both litewire backends agree.
///   It is not MySQL's ICU syntax; the constructs WordPress and Laravel
///   actually emit (anchors, classes, alternation, repetition) are common to
///   both, but ICU-only extensions and backreferences are not supported.
/// * Matching is case-sensitive. MySQL's `REGEXP` follows the column
///   collation and is therefore case-insensitive under the usual
///   `utf8mb4_general_ci`. Matching Turso's built-in was chosen over
///   matching MySQL, so that one query cannot mean two things depending on
///   which backend litewire was built with.
/// * A `NULL` operand yields `NULL`, as in MySQL. Non-text operands are
///   coerced to text, which is what MySQL's implicit conversion does.
/// * An invalid pattern is an error rather than MySQL's error code 3692,
///   but it is still an error and not a silent non-match.
///
/// Registered per session connection, and pooled handles keep it: parking a
/// worker in [`IdlePool`] keeps the same `Connection` alive. It does not
/// disturb the reuse fingerprint either — [`STATE_FINGERPRINT_SQL`] counts
/// temp objects, attached databases and boolean PRAGMAs, none of which a
/// function registration moves.
fn register_regexp(conn: &Connection) -> rusqlite::Result<()> {
    use rusqlite::functions::FunctionFlags;

    conn.create_scalar_function(
        "regexp",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // `X REGEXP Y` calls `regexp(Y, X)`: pattern first, subject
            // second. Both NULL-propagate.
            if matches!(ctx.get_raw(0), ValueRef::Null) || matches!(ctx.get_raw(1), ValueRef::Null)
            {
                return Ok(None);
            }
            // Cached per statement per argument by SQLite, so a constant
            // pattern is compiled once for the whole scan rather than once
            // per row.
            let regex = ctx.get_or_create_aux(0, |pattern| {
                Regex::new(&coerce_text(pattern).unwrap_or_default())
            })?;
            let Some(haystack) = coerce_text(ctx.get_raw(1)) else {
                return Ok(None);
            };
            Ok(Some(regex.is_match(&haystack)))
        },
    )
}

/// Render a SQLite value as the text MySQL's `REGEXP` would have operated
/// on. `None` only for `NULL`, which the caller turns into a `NULL` result.
fn coerce_text(value: ValueRef<'_>) -> Option<String> {
    match value {
        ValueRef::Null => None,
        ValueRef::Integer(i) => Some(i.to_string()),
        ValueRef::Real(f) => Some(f.to_string()),
        ValueRef::Text(t) | ValueRef::Blob(t) => Some(String::from_utf8_lossy(t).into_owned()),
    }
}

/// The session worker's main loop.
///
/// Runs until every `Sender` to `rx` is gone, then returns -- which drops
/// `conn` and closes the SQLite handle on this thread. Panics inside a job
/// unwind this thread only; the session's next statement then fails with
/// [`BackendError::Other`] rather than taking the process down.
fn session_loop(
    conn: Connection,
    rx: &mpsc::Receiver<Job>,
    pool: Option<&Arc<IdlePool>>,
    busy_timeout_ms: u32,
) {
    while let Ok(job) = rx.recv() {
        match job {
            Job::Query { sql, params, reply } => {
                let _ = reply.send(run_query(&conn, &sql, &params));
            }
            Job::Execute { sql, params, reply } => {
                let _ = reply.send(run_execute(&conn, &sql, &params));
            }
            Job::Describe { sql, reply } => {
                let _ = reply.send(run_describe(&conn, &sql));
            }
            Job::EndSession { back } => {
                // `back` is dropped with the `else` branch, which leaves no
                // live `Sender` and ends the loop on the next `recv`.
                let Some(pool) = pool else { return };
                if !pool.park(&conn, back, busy_timeout_ms) {
                    return;
                }
            }
        }
    }
}

// -- Handle reuse --------------------------------------------------------

/// A parked session worker: a live thread holding an open `Connection`,
/// waiting for its next session.
struct IdleWorker {
    /// The worker's inbox. Dropping this is what retires the worker.
    tx: mpsc::Sender<Job>,
    /// When the worker was parked, for [`RusqliteBuilder::idle_max_age`].
    since: Instant,
}

/// Bounded free-list of parked session workers plus reuse counters.
///
/// The `Mutex<VecDeque<IdleWorker>>` is intentionally the crudest thing
/// that works: the critical section is a `push_back` / `pop_back`, so
/// contention is a non-issue next to the SQLite work on either side of it.
/// The deque is used LIFO -- the most recently parked worker is the one
/// most likely to still have a warm page cache -- while the age sweep
/// consumes from the front, so a steady stream of checkouts cannot leave
/// an ancient worker pinned at the bottom of the list forever.
#[derive(Debug)]
struct IdlePool {
    idle: Mutex<VecDeque<IdleWorker>>,
    /// Maximum idle workers retained. Bounds threads, file descriptors and
    /// the aggregate per-connection statement-cache footprint.
    max_idle: usize,
    /// Retirement age for an idle worker. See
    /// [`RusqliteBuilder::idle_max_age`].
    max_idle_age: Duration,
    /// [`STATE_FINGERPRINT_SQL`] evaluated once on a pristine connection at
    /// factory construction. See that constant for why this is calibrated
    /// rather than hardcoded.
    pristine: i64,
    /// `connect()` calls served from the free-list.
    hits: AtomicU64,
    /// `connect()` calls that had to open a fresh session.
    misses: AtomicU64,
    /// Workers parked on the free-list at end of session.
    returned: AtomicU64,
    /// Workers retired at end of session instead of parked (dirty state,
    /// failed rollback, or pool full).
    discarded: AtomicU64,
    /// Parked workers retired by the [`RusqliteBuilder::idle_max_age`]
    /// sweep rather than handed to a session.
    expired: AtomicU64,
}

impl std::fmt::Debug for IdleWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdleWorker")
            .field("idle_for", &self.since.elapsed())
            .finish()
    }
}

impl IdlePool {
    fn new(max_idle: usize, max_idle_age: Duration, pristine: i64) -> Self {
        Self {
            idle: Mutex::new(VecDeque::with_capacity(max_idle)),
            max_idle,
            max_idle_age,
            pristine,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            returned: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
            expired: AtomicU64::new(0),
        }
    }

    /// Take a parked worker for a new session, retiring any that have sat
    /// idle past [`IdlePool::max_idle_age`] on the way.
    ///
    /// A worker only ever stops listening when its `Sender` is dropped, and
    /// while parked the free-list holds the only `Sender`. So a worker
    /// handed out here is always live -- there is no dead-entry race to
    /// handle at the call site.
    fn take(&self) -> Option<mpsc::Sender<Job>> {
        // Retired workers are dropped after the lock is released: dropping
        // a `Sender` wakes the worker thread, and there is no reason to
        // hold the free-list while that happens.
        let mut retired: Vec<IdleWorker> = Vec::new();
        let picked = {
            let mut idle = self.idle.lock();
            while idle
                .front()
                .is_some_and(|w| w.since.elapsed() >= self.max_idle_age)
            {
                let Some(w) = idle.pop_front() else { break };
                retired.push(w);
            }
            idle.pop_back()
        };

        if !retired.is_empty() {
            self.expired
                .fetch_add(retired.len() as u64, Ordering::Relaxed);
            drop(retired);
        }

        match picked {
            Some(w) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(w.tx)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Scrub a handle back to a session-start state.
    ///
    /// Always runs on the session's own worker thread, so a slow
    /// `ROLLBACK` cannot stall an async worker.
    ///
    /// What this **does** handle:
    ///
    /// * **Open transactions and savepoints.** A client that disconnects
    ///   mid-`BEGIN` (PHP fatal error, killed request, dropped TCP session)
    ///   leaves the handle inside a write transaction holding a WAL write
    ///   lock. `ROLLBACK` releases the transaction and every open savepoint
    ///   with it. This is the one non-negotiable step: without it a pooled
    ///   handle would hand the next session another client's uncommitted
    ///   rows *and* an inherited write lock.
    /// * **Client-mutated connection state**, via
    ///   [`STATE_FINGERPRINT_SQL`] -- temp tables/views/triggers,
    ///   `ATTACH`ed databases, and the boolean connection PRAGMAs.
    ///   Detected, not repaired: a dirty handle is closed rather than
    ///   pooled.
    /// * **`last_insert_rowid`.** Reset to 0, so a recycled session cannot
    ///   report the *previous* client's generated key to a
    ///   `SELECT last_insert_rowid()` / MySQL `LAST_INSERT_ID()` issued
    ///   before this session has inserted anything.
    /// * **`busy_timeout`**, which a client can lower with
    ///   `PRAGMA busy_timeout=0`. Re-applied unconditionally because
    ///   `sqlite3_busy_timeout` is a direct C call with no statement
    ///   execution -- it is free, so there is no reason to only fix it on
    ///   detection.
    ///
    /// What this deliberately does **not** handle, and why:
    ///
    /// * **`synchronous`, `cache_size`, `temp_store`, `analysis_limit`, and
    ///   the other non-boolean connection PRAGMAs.** Each would need its own
    ///   probe term and its own pristine constant, and they are
    ///   durability/performance knobs rather than correctness or visibility
    ///   knobs -- a client that lowers `cache_size` and disconnects can
    ///   make the next session on that handle slower, never wrong. A future
    ///   version should either extend the probe or reject
    ///   `PRAGMA <name> = <value>` at the wire frontend when reuse is
    ///   enabled, which collapses this whole surface to nothing.
    /// * **`journal_mode`.** It is a database-header property, not
    ///   connection state; a client flipping it out of WAL already breaks
    ///   every other connection today, pooled or not. Orthogonal bug.
    /// * **The `prepare_cached` LRU**, which is retained on purpose -- it is
    ///   a large part of what reuse buys. SQLite re-prepares cached
    ///   statements automatically on `SQLITE_SCHEMA`, so a DDL change on
    ///   another connection cannot serve a stale plan.
    ///
    /// Returns `false` if the handle must be closed rather than pooled.
    fn reset(&self, conn: &Connection, busy_timeout_ms: u32) -> bool {
        // Pure C call against the connection struct -- no statement, no I/O.
        // The overwhelmingly common case (client committed or never opened
        // a transaction) costs one predictable branch.
        if !conn.is_autocommit() && conn.execute_batch("ROLLBACK").is_err() {
            // A rollback that fails leaves the handle in an unknown
            // transaction state. Never pool that.
            return false;
        }

        if fingerprint(conn) != Some(self.pristine) {
            return false;
        }

        // SAFETY: `conn` is a live `rusqlite::Connection` owned by this
        // session worker thread, and the worker is between jobs, so no
        // statement is in flight and the raw handle is valid and unaliased
        // for the duration of the call. `sqlite3_set_last_insert_rowid`
        // only overwrites the connection's cached rowid field: it prepares
        // nothing, executes nothing, allocates nothing and cannot fail, so
        // there is no error to propagate and no way for it to leave the
        // connection in a state rusqlite would not expect. rusqlite 0.32
        // exposes no safe wrapper for it.
        unsafe {
            rusqlite::ffi::sqlite3_set_last_insert_rowid(conn.handle(), 0);
        }

        conn.busy_timeout(Duration::from_millis(u64::from(busy_timeout_ms)))
            .is_ok()
    }

    /// Hygiene-check `conn` and park its worker on the free-list.
    ///
    /// Returns `false` if the worker must retire instead (dirty handle,
    /// failed rollback, or the list is full), in which case `back` is
    /// dropped here and the worker's loop ends.
    fn park(&self, conn: &Connection, back: mpsc::Sender<Job>, busy_timeout_ms: u32) -> bool {
        // Cheap pre-check: if the list is already full there is no point
        // paying for hygiene at all. Racy against a concurrent `take`, which
        // only costs us an occasional avoidable retirement.
        if self.idle.lock().len() >= self.max_idle {
            self.discarded.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        if !self.reset(conn, busy_timeout_ms) {
            self.discarded.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let mut idle = self.idle.lock();
        if idle.len() < self.max_idle {
            idle.push_back(IdleWorker {
                tx: back,
                since: Instant::now(),
            });
            drop(idle);
            self.returned.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            drop(idle);
            self.discarded.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

/// Snapshot of handle-reuse counters. All fields are monotonic since
/// factory construction except `idle`, which is an instantaneous depth.
///
/// Exposed so that a pool which has silently degraded into discarding
/// every handle -- the failure mode a hardcoded state fingerprint would
/// cause -- is observable rather than merely slow. See
/// [`Rusqlite::reuse_stats`].
///
/// Note that a session returns its worker to the pool *asynchronously*:
/// `RusqliteConn::drop` only posts the end-of-session message, and the
/// counters move once the worker has run hygiene. Callers sampling these
/// immediately after a drop should expect to poll.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReuseStats {
    /// `connect()` calls served from the free-list.
    pub hits: u64,
    /// `connect()` calls that opened a fresh session.
    pub misses: u64,
    /// Workers parked on the free-list at end of session.
    pub returned: u64,
    /// Workers retired at end of session instead of parked.
    pub discarded: u64,
    /// Parked workers retired by the idle-age sweep.
    pub expired: u64,
    /// Free-list depth at the moment of the call.
    pub idle: usize,
}

/// Builder for [`Rusqlite`]. Use [`Rusqlite::open`] / [`Rusqlite::memory`]
/// for the default configuration; use [`RusqliteBuilder`] to tune the
/// per-session PRAGMAs or enable handle reuse.
#[derive(Clone, Debug)]
pub struct RusqliteBuilder {
    target: Target,
    busy_timeout_ms: u32,
    synchronous: Synchronous,
    max_idle: usize,
    max_idle_age: Duration,
}

/// SQLite `synchronous` PRAGMA setting.
#[derive(Clone, Copy, Debug)]
pub enum Synchronous {
    /// Fastest, unsafe against power loss.
    Off,
    /// WAL-appropriate default: durable across power loss for committed
    /// transactions, higher throughput than `Full`.
    Normal,
    /// Fully synchronous. Slowest.
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

impl RusqliteBuilder {
    /// Set the `busy_timeout` PRAGMA (milliseconds) applied to every
    /// per-session connection.
    #[must_use]
    pub fn busy_timeout_ms(mut self, ms: u32) -> Self {
        self.busy_timeout_ms = ms;
        self
    }

    /// Set the `synchronous` PRAGMA applied to every per-session connection.
    #[must_use]
    pub fn synchronous(mut self, s: Synchronous) -> Self {
        self.synchronous = s;
        self
    }

    /// Retain up to `max_idle` idle session workers -- a parked thread
    /// still holding its open SQLite handle -- and reuse them across wire
    /// sessions instead of opening and closing a handle per session. `0`
    /// (the default) disables reuse entirely and restores strict
    /// open-per-session behaviour.
    ///
    /// This is **off by default on purpose**: it is the only setting in
    /// this backend that lets any state outlive the wire session that
    /// created it, and litewire cannot know what an embedding application
    /// lets its clients do. Turn it on when sessions are short and
    /// numerous -- the archetype being one database connection per HTTP
    /// request -- and leave it off otherwise.
    ///
    /// # What survives a session, and what does not
    ///
    /// Before a worker is parked, its handle is scrubbed. `ROLLBACK` ends
    /// any transaction and savepoint the client abandoned;
    /// `last_insert_rowid` is reset to 0; `busy_timeout` is re-applied.
    /// The handle is then fingerprinted against a pristine connection and
    /// **discarded unless it matches**, which covers temp
    /// tables/views/triggers, `ATTACH`ed databases, and the boolean
    /// connection PRAGMAs (`query_only`, `foreign_keys`,
    /// `recursive_triggers`, `defer_foreign_keys`, `read_uncommitted`,
    /// `reverse_unordered_selects`, `cell_size_check`, `automatic_index`,
    /// `trusted_schema`).
    ///
    /// Deliberately **not** scrubbed:
    ///
    /// * the non-boolean connection PRAGMAs -- `cache_size`, `temp_store`,
    ///   `analysis_limit`, `synchronous` and friends. A client can leave
    ///   these changed for the next session on that handle. They are
    ///   performance and durability knobs, so the blast radius is "slower
    ///   or less durable", never "wrong rows";
    /// * `journal_mode`, which is a property of the database file header
    ///   rather than of the connection -- a client that takes the database
    ///   out of WAL has already broken every other connection, pooled or
    ///   not;
    /// * rusqlite's `prepare_cached` statement cache, which is kept
    ///   deliberately because it is a large part of what reuse buys.
    ///   SQLite re-prepares a cached statement automatically after a schema
    ///   change, so this cannot serve a stale plan.
    ///
    /// The fingerprint's pristine value is calibrated at runtime against a
    /// live connection rather than hardcoded, because "clean" is a property
    /// of how SQLite was compiled: this build defines
    /// `SQLITE_DEFAULT_FOREIGN_KEYS=1`, so `PRAGMA foreign_keys` reads 1
    /// where stock SQLite reads 0. A hardcoded table would silently turn
    /// the pool into a no-op that discards every handle on a dependency
    /// bump; [`Rusqlite::reuse_stats`] exists so that even a silent
    /// no-op is observable.
    ///
    /// # Isolation
    ///
    /// Reuse does **not** relax per-session isolation. A worker, and hence
    /// its handle, belongs to exactly one session at a time.
    ///
    /// # Resource bound
    ///
    /// Each idle worker costs one OS thread, one file descriptor and its
    /// own `prepare_cached` LRU, so `max_idle` is a real ceiling on
    /// retained resources. Pair it with [`Self::idle_max_age`].
    #[must_use]
    pub fn handle_reuse(mut self, max_idle: usize) -> Self {
        self.max_idle = max_idle;
        self
    }

    /// Retire a parked worker that has been idle for longer than `age`
    /// rather than handing it to a new session. Default 60 seconds; only
    /// meaningful when [`Self::handle_reuse`] is enabled.
    ///
    /// This bounds how stale a recycled handle can be. An idle SQLite
    /// connection with no statement in flight holds no read mark, so it
    /// does not by itself block WAL checkpointing -- the thing an age bound
    /// really buys is that a long-lived handle cannot keep pointing at a
    /// database file that has since been replaced underneath it (a restore,
    /// a `VACUUM INTO` swap), and that an idle pool eventually releases its
    /// threads and descriptors.
    ///
    /// The sweep runs on checkout, not on a timer: a factory that is never
    /// used again keeps its idle workers until it is dropped.
    #[must_use]
    pub fn idle_max_age(mut self, age: Duration) -> Self {
        self.max_idle_age = age;
        self
    }

    /// Finalize the builder. Opens a bootstrap connection to persist
    /// WAL journaling mode (a DB-header property, so all subsequent
    /// per-session connections inherit it) then drops it. For
    /// memory-backed targets the temp file is created here.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Sqlite`] if the database cannot be opened
    /// or WAL cannot be enabled.
    pub fn build(self) -> Result<Rusqlite, BackendError> {
        let bootstrap = Connection::open(self.target.as_path())
            .map_err(|e| BackendError::Sqlite(e.to_string()))?;
        bootstrap
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| BackendError::Sqlite(e.to_string()))?;
        // Calibrate the reuse hygiene fingerprint against a connection
        // nothing has touched yet -- see `STATE_FINGERPRINT_SQL` for why
        // this cannot be a compile-time constant.
        let pool = if self.max_idle > 0 {
            let pristine = fingerprint(&bootstrap).ok_or_else(|| {
                BackendError::Sqlite(
                    "handle reuse enabled but the session-state fingerprint probe \
                     did not run against this SQLite build"
                        .into(),
                )
            })?;
            Some(Arc::new(IdlePool::new(
                self.max_idle,
                self.max_idle_age,
                pristine,
            )))
        } else {
            None
        };

        drop(bootstrap);
        Ok(Rusqlite {
            target: self.target,
            busy_timeout_ms: self.busy_timeout_ms,
            synchronous: self.synchronous,
            pool,
        })
    }
}

/// In-process SQLite backend via `rusqlite`.
///
/// This type is a **factory**: [`Backend::connect`] gives every wire
/// session its own `rusqlite::Connection`, owned by its own worker thread.
/// See the module docs for the rationale.
pub struct Rusqlite {
    target: Target,
    busy_timeout_ms: u32,
    synchronous: Synchronous,
    /// `Some` only when [`RusqliteBuilder::handle_reuse`] was given a
    /// non-zero bound. `None` is the historical open-per-session path and
    /// costs one `Option` check on `connect`.
    pool: Option<Arc<IdlePool>>,
}

impl Rusqlite {
    /// Open (or create) a file-backed SQLite database at `path` with the
    /// default configuration (`busy_timeout=5000ms`, `synchronous=NORMAL`,
    /// WAL persisted on first open, handle reuse off).
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or WAL cannot be
    /// enabled.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BackendError> {
        Self::builder(path).build()
    }

    /// Open an "in-memory" SQLite database backed by an internally-managed
    /// temp file.
    ///
    /// The temp file gives every per-session `connect()` a real
    /// file-backed database with WAL semantics -- proper writer/reader
    /// concurrency and proper per-connection isolation. The file is
    /// deleted when the returned [`Rusqlite`] is dropped. Callers see the
    /// same API they always saw.
    ///
    /// # Errors
    ///
    /// Returns an error if the temp file cannot be created or opened.
    pub fn memory() -> Result<Self, BackendError> {
        Self::builder(":memory:").build()
    }

    /// Start a [`RusqliteBuilder`] to override defaults.
    #[must_use]
    pub fn builder(path: impl AsRef<Path>) -> RusqliteBuilder {
        RusqliteBuilder {
            target: classify_target(path.as_ref()),
            busy_timeout_ms: 5000,
            synchronous: Synchronous::Normal,
            max_idle: 0,
            max_idle_age: DEFAULT_IDLE_MAX_AGE,
        }
    }

    /// Snapshot the handle-reuse counters. Returns `None` when reuse is
    /// disabled.
    ///
    /// This is the only way to prove that a workload is actually hitting
    /// the free-list rather than silently falling back to fresh opens, so
    /// it is public API rather than test-only: a pool whose `discarded`
    /// tracks its session count one-for-one is configured but doing
    /// nothing.
    #[must_use]
    pub fn reuse_stats(&self) -> Option<ReuseStats> {
        let pool = self.pool.as_ref()?;
        Some(ReuseStats {
            hits: pool.hits.load(Ordering::Relaxed),
            misses: pool.misses.load(Ordering::Relaxed),
            returned: pool.returned.load(Ordering::Relaxed),
            discarded: pool.discarded.load(Ordering::Relaxed),
            expired: pool.expired.load(Ordering::Relaxed),
            idle: pool.idle.lock().len(),
        })
    }

    /// Start a fresh session worker thread and wait for it to open its
    /// connection.
    ///
    /// The thread opens the handle itself, so the ~200 µs of file I/O a
    /// WAL-index attach costs happens on the worker rather than on the
    /// caller's async worker thread; the caller only pays the thread spawn
    /// and then awaits.
    async fn spawn_session(&self) -> Result<mpsc::Sender<Job>, BackendError> {
        let (tx, rx) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = oneshot::channel::<Result<(), BackendError>>();

        let path = self.target.as_path().to_path_buf();
        let busy_timeout_ms = self.busy_timeout_ms;
        let synchronous = self.synchronous;
        let pool = self.pool.clone();

        std::thread::Builder::new()
            .name("litewire-sqlite".into())
            .spawn(move || {
                let conn = match open_session(&path, busy_timeout_ms, synchronous) {
                    Ok(conn) => conn,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    // Caller went away before the handle was ready.
                    return;
                }
                session_loop(conn, &rx, pool.as_ref(), busy_timeout_ms);
            })
            .map_err(|e| {
                BackendError::Other(format!("could not spawn a SQLite session thread: {e}"))
            })?;

        ready_rx
            .await
            .map_err(|_| BackendError::Other("SQLite session thread died on startup".into()))?
            .map(|()| tx)
    }
}

/// Per-session rusqlite handle.
///
/// Owns one session worker thread, which in turn owns exactly one
/// `rusqlite::Connection`. Every method is a channel send plus an await on
/// a `oneshot`; no SQLite code runs on the caller's thread. Concurrent
/// calls on the same `RusqliteConn` are serialized in arrival order by the
/// channel, which is the natural per-session semantic -- one wire client
/// cannot issue two overlapping SQL statements anyway.
pub struct RusqliteConn {
    /// The worker's inbox. Dropping the last `Sender` ends the worker.
    tx: mpsc::Sender<Job>,
    /// Whether the factory has handle reuse enabled, and therefore whether
    /// this session should hand its worker back at the end.
    pooled: bool,
}

/// The error every method returns once the worker thread is gone, which
/// happens only if a job panicked.
fn worker_gone() -> BackendError {
    BackendError::Other("SQLite session worker is no longer running".into())
}

impl Drop for RusqliteConn {
    fn drop(&mut self) {
        if self.pooled {
            // Post the end-of-session message and let the worker run
            // hygiene on its own thread. `back` is a `Sender` to the same
            // inbox, which the worker parks on the free-list if the handle
            // comes back clean. Our own `tx` is dropped immediately after
            // this, so the message carries the only remaining `Sender`.
            let _ = self.tx.send(Job::EndSession {
                back: self.tx.clone(),
            });
        }
        // With reuse off, dropping `tx` is the whole shutdown: the worker's
        // `recv` fails, it returns, and the `Connection` closes on the
        // worker thread.
    }
}

#[async_trait::async_trait]
impl Backend for Rusqlite {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        if let Some(pool) = self.pool.as_ref() {
            if let Some(tx) = pool.take() {
                return Ok(Box::new(RusqliteConn { tx, pooled: true }));
            }
        }
        let tx = self.spawn_session().await?;
        Ok(Box::new(RusqliteConn {
            tx,
            pooled: self.pool.is_some(),
        }))
    }
}

/// Convert a [`Value`] slice to rusqlite params.
fn bind_params(params: &[Value]) -> Vec<Box<dyn rusqlite::types::ToSql>> {
    params
        .iter()
        .map(|v| -> Box<dyn rusqlite::types::ToSql> {
            match v {
                Value::Null => Box::new(rusqlite::types::Null),
                Value::Integer(i) => Box::new(*i),
                Value::Float(f) => Box::new(*f),
                Value::Text(s) => Box::new(s.clone()),
                Value::Blob(b) => Box::new(b.clone()),
            }
        })
        .collect()
}

/// Extract a [`Value`] from a rusqlite row at the given column index.
///
/// TEXT handling: SQLite `TEXT` cells hold raw bytes that are *supposed* to
/// be UTF-8. If they are not, this used to lossy-decode via
/// `String::from_utf8_lossy`, which silently replaces bytes with U+FFFD and
/// corrupts round-trips (e.g. arbitrary latin-1 or WTF-8 stored by a
/// previous client). Instead, we surface invalid UTF-8 as a `Blob` so the
/// caller / wire frontend can decide how to send it back to the client.
fn extract_value(row: &rusqlite::Row<'_>, idx: usize) -> Result<Value, rusqlite::Error> {
    use rusqlite::types::ValueRef;
    match row.get_ref(idx)? {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(i) => Ok(Value::Integer(i)),
        ValueRef::Real(f) => Ok(Value::Float(f)),
        ValueRef::Text(s) => match std::str::from_utf8(s) {
            Ok(v) => Ok(Value::Text(v.to_string())),
            Err(_) => Ok(Value::Blob(s.to_vec())),
        },
        ValueRef::Blob(b) => Ok(Value::Blob(b.to_vec())),
    }
}

/// Read the column metadata off a prepared `Statement`, using rusqlite's
/// `column_decltype` feature to populate `Column.decltype`.
fn describe_stmt_columns(stmt: &rusqlite::Statement<'_>) -> Vec<Column> {
    stmt.columns()
        .iter()
        .map(|c| Column {
            name: c.name().to_string(),
            decltype: c.decl_type().map(str::to_string),
        })
        .collect()
}

/// Run a row-returning statement. Worker thread only.
fn run_query(conn: &Connection, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
    // `prepare_cached` interns the parsed statement in rusqlite's
    // per-connection LRU. Repeated identical SQL (the norm for prepared-
    // statement heavy workloads and for the KV/session handler path) then
    // avoids the sqlite3_prepare_v2 cost.
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;

    let columns = describe_stmt_columns(&stmt);
    let col_count = columns.len();

    let bound = bind_params(params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = bound.iter().map(|b| b.as_ref()).collect();

    let mut result_rows = Vec::new();
    let mut rows = stmt
        .query(param_refs.as_slice())
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;

    while let Some(row) = rows
        .next()
        .map_err(|e| BackendError::Sqlite(e.to_string()))?
    {
        let mut values = Vec::with_capacity(col_count);
        for i in 0..col_count {
            values.push(extract_value(row, i).map_err(|e| BackendError::Sqlite(e.to_string()))?);
        }
        result_rows.push(values);
    }

    Ok(ResultSet {
        columns,
        rows: result_rows,
    })
}

/// Run a data-modifying statement. Worker thread only.
fn run_execute(
    conn: &Connection,
    sql: &str,
    params: &[Value],
) -> Result<ExecuteResult, BackendError> {
    let bound = bind_params(params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = bound.iter().map(|b| b.as_ref()).collect();

    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;
    let affected = stmt
        .execute(param_refs.as_slice())
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;

    let last_id = conn.last_insert_rowid();

    Ok(ExecuteResult {
        affected_rows: affected as u64,
        last_insert_rowid: if last_id != 0 { Some(last_id) } else { None },
    })
}

/// Describe a prepared statement's columns without executing it. Worker
/// thread only.
fn run_describe(conn: &Connection, sql: &str) -> Result<Vec<Column>, BackendError> {
    let stmt = conn
        .prepare_cached(sql)
        .map_err(|e| BackendError::Sqlite(e.to_string()))?;
    Ok(describe_stmt_columns(&stmt))
}

#[async_trait::async_trait]
impl BackendConn for RusqliteConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(Job::Query {
                sql: sql.to_string(),
                params: params.to_vec(),
                reply,
            })
            .map_err(|_| worker_gone())?;
        wait.await.map_err(|_| worker_gone())?
    }

    async fn describe_columns(&self, sql: &str) -> Result<Vec<Column>, BackendError> {
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(Job::Describe {
                sql: sql.to_string(),
                reply,
            })
            .map_err(|_| worker_gone())?;
        wait.await.map_err(|_| worker_gone())?
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(Job::Execute {
                sql: sql.to_string(),
                params: params.to_vec(),
                reply,
            })
            .map_err(|_| worker_gone())?;
        wait.await.map_err(|_| worker_gone())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_crud() {
        let backend = Rusqlite::memory().unwrap();

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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
        let result = backend.query("DEFINITELY NOT SQL !!!", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_error_on_bad_sql() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.execute("DEFINITELY NOT SQL !!!", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn blob_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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
        let backend = Rusqlite::memory().unwrap();
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

    // -- Isolation tests: the whole point of this PR ----------------------

    /// Two sessions against the same in-memory DB. A's uncommitted INSERT
    /// must not be visible to B, and B's COMMIT must not touch A's tx.
    #[tokio::test]
    async fn per_conn_transaction_isolation() {
        let backend = Rusqlite::memory().unwrap();
        // Bootstrap schema via the stateless shortcut.
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

        // B must not see A's uncommitted row.
        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(
            rs.rows[0][0],
            Value::Integer(0),
            "B saw A's uncommitted row"
        );

        // B's ROLLBACK is a no-op on B (B has no open tx) and must not
        // touch A. Use ROLLBACK instead of COMMIT here because SQLite
        // rejects a bare COMMIT with "cannot commit - no transaction is
        // active" -- which is itself a good sign, but the essential check
        // is that A's transaction is still alive afterward.
        let _ = b.execute("ROLLBACK", &[]).await;

        // A can still commit its own transaction.
        a.execute("COMMIT", &[]).await.unwrap();

        // Now both sessions see the committed row.
        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));
        let rs = a.query("SELECT v FROM t WHERE id=1", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text("from-a".into()));
    }

    /// A rolled-back tx on session A must not stick around; session B
    /// must never see any of A's writes.
    #[tokio::test]
    async fn per_conn_rollback_stays_local() {
        let backend = Rusqlite::memory().unwrap();
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

    /// Multiple concurrent readers on distinct sessions must not
    /// serialize on any process-wide lock.
    #[tokio::test]
    async fn per_conn_concurrent_readers() {
        let backend = Arc::new(Rusqlite::memory().unwrap());
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

        // Fan out N concurrent point-selects; each on its own BackendConn.
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

    /// Prove the temp-file memory-DB handling: sessions must see each
    /// other's committed schema and rows.
    #[tokio::test]
    async fn per_conn_shared_memory_visibility() {
        let backend = Rusqlite::memory().unwrap();

        let a = backend.connect().await.unwrap();
        let b = backend.connect().await.unwrap();

        a.execute("CREATE TABLE t (id INTEGER)", &[]).await.unwrap();
        a.execute("INSERT INTO t VALUES (7)", &[]).await.unwrap();

        // B, on a completely separate rusqlite Connection, must still see
        // the row -- because we opened the same on-disk temp file.
        let rs = b.query("SELECT id FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(7));
    }

    /// Before-and-after concurrency measurement: run identical
    /// point-select workloads on the same DB, once against the stateless
    /// shortcut (which opens+drops a conn per call, similar to the old
    /// shared-Mutex path in terms of coordination) and once against a
    /// per-session BackendConn (statement-cached, WAL-concurrent). This
    /// is the payoff for the perf change embedded here: no per-call
    /// re-open, per-connection prepare_cached hits.
    #[tokio::test]
    async fn per_conn_beats_reopen_on_hot_selects() {
        let backend = Arc::new(Rusqlite::memory().unwrap());
        backend
            .execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();
        for i in 0..500 {
            backend
                .execute(
                    "INSERT INTO bench VALUES (?1, ?2)",
                    &[Value::Integer(i), Value::Text(format!("row-{i}"))],
                )
                .await
                .unwrap();
        }

        const ITERS: usize = 200;

        // Path A: stateless shortcut (fresh conn per call).
        let start = std::time::Instant::now();
        for i in 0..ITERS {
            let rs = backend
                .query(
                    "SELECT v FROM bench WHERE id=?1",
                    &[Value::Integer((i % 500) as i64)],
                )
                .await
                .unwrap();
            assert_eq!(rs.rows.len(), 1);
        }
        let stateless = start.elapsed();

        // Path B: one BackendConn, reused across calls.
        let conn = backend.connect().await.unwrap();
        let start = std::time::Instant::now();
        for i in 0..ITERS {
            let rs = conn
                .query(
                    "SELECT v FROM bench WHERE id=?1",
                    &[Value::Integer((i % 500) as i64)],
                )
                .await
                .unwrap();
            assert_eq!(rs.rows.len(), 1);
        }
        let per_conn = start.elapsed();

        eprintln!(
            "per_conn_beats_reopen_on_hot_selects: iters={ITERS} stateless={stateless:?} per_conn={per_conn:?} ratio={:.2}x",
            stateless.as_nanos() as f64 / per_conn.as_nanos().max(1) as f64
        );

        // We only assert per_conn is not slower than stateless -- the
        // ratio varies by machine and CI noise. The test is here to
        // catch regressions; the numbers are printed above for anyone
        // who runs with --nocapture.
        assert!(
            per_conn <= stateless * 2,
            "per-conn latency regressed vs stateless: per_conn={per_conn:?}, stateless={stateless:?}"
        );
    }

    // -- Handle reuse -----------------------------------------------------

    /// Open a connection the way a session worker would, for tests that
    /// want to inspect a handle directly rather than through a session.
    fn probe_session(backend: &Rusqlite) -> Connection {
        open_session(
            backend.target.as_path(),
            backend.busy_timeout_ms,
            backend.synchronous,
        )
        .unwrap()
    }

    /// A session hands its worker back *asynchronously*: `Drop` only posts
    /// the end-of-session message and the counters move once the worker has
    /// run hygiene on its own thread. Tests that assert on those counters
    /// have to wait for them rather than read them straight after a drop.
    ///
    /// Fails the test rather than hanging if the condition never holds.
    async fn await_stats(backend: &Rusqlite, want: impl Fn(ReuseStats) -> bool) -> ReuseStats {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let s = backend.reuse_stats().expect("reuse is enabled");
            if want(s) {
                return s;
            }
            assert!(
                Instant::now() < deadline,
                "reuse counters never reached the expected state; last seen {s:?}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// The fingerprint probe must run against this SQLite build and must
    /// agree between the bootstrap connection it was calibrated on and any
    /// later fresh session. If a pragma table-valued function stops
    /// resolving, or `open_session` starts leaving a mark the bootstrap
    /// connection does not have, this fails loudly here rather than
    /// silently discarding every handle and turning reuse into a no-op.
    #[tokio::test]
    async fn fingerprint_matches_between_bootstrap_and_fresh_session() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(2)
            .build()
            .unwrap();
        let calibrated = backend.pool.as_ref().unwrap().pristine;

        let conn = probe_session(&backend);
        assert_eq!(
            fingerprint(&conn),
            Some(calibrated),
            "a fresh session does not match the calibrated pristine fingerprint"
        );
    }

    /// Each piece of state the fingerprint claims to detect must actually
    /// move it, and must move it to a *distinct* value -- otherwise two
    /// changes could cancel out and a dirty handle would read as clean.
    #[tokio::test]
    async fn fingerprint_distinguishes_each_kind_of_dirt() {
        let backend = Rusqlite::memory().unwrap();
        let base = {
            let c = probe_session(&backend);
            fingerprint(&c).unwrap()
        };

        let mut seen = std::collections::HashSet::new();
        for dirtier in [
            "CREATE TEMP TABLE leaked (x INTEGER)",
            "PRAGMA query_only = ON",
            "PRAGMA foreign_keys = OFF",
            "PRAGMA recursive_triggers = ON",
            "PRAGMA read_uncommitted = ON",
            "PRAGMA reverse_unordered_selects = ON",
            "PRAGMA cell_size_check = ON",
            "PRAGMA automatic_index = OFF",
            "PRAGMA trusted_schema = OFF",
        ] {
            let c = probe_session(&backend);
            c.execute_batch(dirtier).unwrap();
            let fp = fingerprint(&c).unwrap();
            assert_ne!(fp, base, "fingerprint did not notice `{dirtier}`");
            assert!(seen.insert(fp), "`{dirtier}` collides with another change");
        }
    }

    /// Reuse must actually happen. Without this the benchmark's "reuse on"
    /// lane could be silently falling back to fresh opens and measuring
    /// nothing.
    #[tokio::test]
    async fn reuse_returns_and_serves_the_same_handle() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(4)
            .build()
            .unwrap();

        assert_eq!(backend.reuse_stats().unwrap(), ReuseStats::default());

        // First session: nothing to pop, so a miss; returns cleanly.
        let a = backend.connect().await.unwrap();
        a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
            .await
            .unwrap();
        drop(a);

        let s = await_stats(&backend, |s| s.returned == 1).await;
        assert_eq!(
            (s.hits, s.misses, s.returned, s.discarded, s.idle),
            (0, 1, 1, 0, 1)
        );

        // Next 5 sessions must all be served from the free-list. Each one
        // waits for its predecessor's worker to finish parking, which is
        // what a wire server does anyway -- the next client's connect is
        // never in the same microsecond as the last one's disconnect.
        for n in 1..=5 {
            let c = backend.connect().await.unwrap();
            c.query("SELECT 1", &[]).await.unwrap();
            drop(c);
            await_stats(&backend, |s| s.returned == n + 1).await;
        }

        let s = backend.reuse_stats().unwrap();
        assert_eq!(s.hits, 5, "sessions did not come from the free-list: {s:?}");
        assert_eq!(s.misses, 1);
        assert_eq!(s.discarded, 0);
        assert_eq!(
            s.idle, 1,
            "free-list should hold the single recycled worker"
        );
    }

    /// Reuse off (the default) must never pool anything.
    #[tokio::test]
    async fn reuse_disabled_by_default() {
        let backend = Rusqlite::memory().unwrap();
        assert!(backend.reuse_stats().is_none());
        for _ in 0..3 {
            let c = backend.connect().await.unwrap();
            c.query("SELECT 1", &[]).await.unwrap();
        }
        assert!(backend.reuse_stats().is_none());
    }

    /// **The critical correctness test.** A session that dies mid-write
    /// transaction (PHP fatal, killed request, dropped TCP connection) must
    /// not hand its uncommitted rows -- or its open transaction -- to
    /// whoever gets the recycled handle next.
    #[tokio::test]
    async fn handle_returned_mid_transaction_is_rolled_back() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(4)
            .build()
            .unwrap();

        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();
        // The stateless shortcut above opened and dropped its own session.
        // Let its worker finish parking so the assertions below are about
        // A's handle and nothing else.
        await_stats(&backend, |s| s.returned + s.discarded == 1).await;

        let a = backend.connect().await.unwrap();
        a.execute("BEGIN IMMEDIATE", &[]).await.unwrap();
        a.execute("INSERT INTO t VALUES (1, 'uncommitted')", &[])
            .await
            .unwrap();
        // Sanity: the row is visible on A's own handle before the drop.
        let rs = a.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));

        let before = backend.reuse_stats().unwrap();
        drop(a); // <-- dies inside the transaction
        let after = await_stats(&backend, |s| {
            s.returned + s.discarded > before.returned + before.discarded
        })
        .await;
        assert_eq!(
            after.returned,
            before.returned + 1,
            "mid-transaction handle should have been rolled back and pooled, not discarded"
        );

        // B gets the recycled handle.
        let b = backend.connect().await.unwrap();
        assert_eq!(
            backend.reuse_stats().unwrap().hits,
            after.hits + 1,
            "B did not receive the recycled handle -- test proves nothing"
        );

        // 1. B must not see A's uncommitted row.
        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(
            rs.rows[0][0],
            Value::Integer(0),
            "recycled handle leaked an uncommitted row"
        );

        // 2. B must not be inside a transaction. If the handle were still
        //    in one, this BEGIN would fail with "cannot start a
        //    transaction within a transaction".
        b.execute("BEGIN", &[])
            .await
            .expect("recycled handle was still inside a transaction");
        b.execute("ROLLBACK", &[]).await.unwrap();

        // 3. And a fresh third party must not see the row either -- i.e.
        //    the rollback really hit the database, it was not just hidden
        //    from B.
        let c = backend.connect().await.unwrap();
        let rs = c.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(0));
    }

    /// A session that leaves connection-scoped state behind (temp table,
    /// flipped PRAGMA) must have its handle closed, not recycled.
    #[tokio::test]
    async fn dirty_handles_are_discarded_not_pooled() {
        // `foreign_keys = OFF`, not `ON`: rusqlite's bundled SQLite already
        // defaults it ON, so `ON` is a no-op and would not dirty anything.
        for dirtier in [
            "CREATE TEMP TABLE leaked (x INTEGER)",
            "PRAGMA foreign_keys = OFF",
            "PRAGMA query_only = ON",
            "ATTACH DATABASE ':memory:' AS side",
        ] {
            let backend = Rusqlite::builder(":memory:")
                .handle_reuse(4)
                .build()
                .unwrap();

            let a = backend.connect().await.unwrap();
            // `query` not `execute`: PRAGMA-set is a no-row statement but
            // both paths reach the same connection.
            let _ = a.execute(dirtier, &[]).await;
            let before = backend.reuse_stats().unwrap();
            drop(a);
            let after = await_stats(&backend, |s| {
                s.returned + s.discarded > before.returned + before.discarded
            })
            .await;

            assert_eq!(
                after.discarded,
                before.discarded + 1,
                "dirty handle ({dirtier}) was pooled instead of discarded"
            );
            assert_eq!(
                after.idle, 0,
                "dirty handle ({dirtier}) landed on the free-list"
            );
        }
    }

    /// The free-list must not grow past its bound, however many sessions
    /// are alive at once.
    #[tokio::test]
    async fn idle_list_is_bounded() {
        const CAP: usize = 4;
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(CAP)
            .build()
            .unwrap();

        let mut live = Vec::new();
        for _ in 0..32 {
            live.push(backend.connect().await.unwrap());
        }
        drop(live);

        let s = await_stats(&backend, |s| s.returned + s.discarded == 32).await;
        assert_eq!(s.idle, CAP, "free-list exceeded its bound: {s:?}");
        assert_eq!(s.returned, CAP as u64);
        assert_eq!(s.discarded, 32 - CAP as u64);
    }

    /// Reuse must not weaken the isolation guarantee the per-connection
    /// design exists for: two *live* sessions never share a handle.
    #[tokio::test]
    async fn reuse_preserves_live_session_isolation() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(8)
            .build()
            .unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        // Cycle workers through the pool first so the sessions below are
        // recycled ones, not fresh opens -- otherwise the test would pass
        // trivially without exercising reuse at all.
        for _ in 0..4 {
            drop(backend.connect().await.unwrap());
        }
        await_stats(&backend, |s| s.idle >= 2).await;
        let before = backend.reuse_stats().unwrap();

        let a = backend.connect().await.unwrap();
        let b = backend.connect().await.unwrap();
        assert_eq!(
            backend.reuse_stats().unwrap().hits,
            before.hits + 2,
            "A and B were not both served recycled workers -- test proves nothing"
        );

        a.execute("BEGIN", &[]).await.unwrap();
        a.execute("INSERT INTO t VALUES (1, 'from-a')", &[])
            .await
            .unwrap();

        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(
            rs.rows[0][0],
            Value::Integer(0),
            "recycled handles let B see A's uncommitted row"
        );

        a.execute("COMMIT", &[]).await.unwrap();
        let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));
    }

    /// A recycled handle must not report the *previous* session's generated
    /// key. MySQL clients call `LAST_INSERT_ID()` and Postgres clients
    /// `lastval()`-adjacent things without inserting first far more often
    /// than one would like, and a stale non-zero answer is silently wrong
    /// rather than loudly broken.
    #[tokio::test]
    async fn last_insert_rowid_is_reset_before_reuse() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(1)
            .build()
            .unwrap();

        let a = backend.connect().await.unwrap();
        a.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
            &[],
        )
        .await
        .unwrap();
        let inserted = a
            .execute("INSERT INTO t (v) VALUES ('x')", &[])
            .await
            .unwrap();
        assert_eq!(inserted.last_insert_rowid, Some(1));
        // Sanity: it really is readable on the handle before it goes back.
        let rs = a.query("SELECT last_insert_rowid()", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));

        let before = backend.reuse_stats().unwrap();
        drop(a);
        await_stats(&backend, |s| s.returned == before.returned + 1).await;

        let b = backend.connect().await.unwrap();
        assert_eq!(
            backend.reuse_stats().unwrap().hits,
            before.hits + 1,
            "B did not receive the recycled worker -- test proves nothing"
        );
        let rs = b.query("SELECT last_insert_rowid()", &[]).await.unwrap();
        assert_eq!(
            rs.rows[0][0],
            Value::Integer(0),
            "recycled handle leaked the previous session's last_insert_rowid"
        );

        // And a non-inserting statement must not manufacture one either.
        let r = b.execute("UPDATE t SET v = 'y'", &[]).await.unwrap();
        assert_eq!(r.affected_rows, 1);
        assert_eq!(
            r.last_insert_rowid, None,
            "an UPDATE on a recycled handle reported an insert id"
        );
    }

    /// A worker that has been parked longer than the configured age must be
    /// retired on checkout rather than handed to a new session.
    #[tokio::test]
    async fn idle_workers_expire() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(4)
            .idle_max_age(Duration::from_millis(30))
            .build()
            .unwrap();

        let a = backend.connect().await.unwrap();
        a.query("SELECT 1", &[]).await.unwrap();
        drop(a);
        let parked = await_stats(&backend, |s| s.idle == 1).await;
        assert_eq!(parked.expired, 0);

        tokio::time::sleep(Duration::from_millis(80)).await;

        let b = backend.connect().await.unwrap();
        let s = backend.reuse_stats().unwrap();
        assert_eq!(s.expired, 1, "stale worker was not retired: {s:?}");
        assert_eq!(
            s.hits, parked.hits,
            "stale worker was handed to a session anyway: {s:?}"
        );
        assert_eq!(s.idle, 0, "free-list still holds the stale worker: {s:?}");
        // The fresh session still works.
        assert_eq!(
            b.query("SELECT 1", &[]).await.unwrap().rows[0][0],
            Value::Integer(1)
        );

        // A worker parked *after* the sweep is still young enough to serve.
        drop(b);
        await_stats(&backend, |s| s.idle == 1).await;
        let c = backend.connect().await.unwrap();
        assert_eq!(backend.reuse_stats().unwrap().hits, parked.hits + 1);
        drop(c);
    }

    /// Dropping a query future mid-statement must not poison anything.
    ///
    /// This is the failure mode the worker-owns-the-connection design
    /// exists to make unreachable: with a shared `Arc<Mutex<Connection>>`
    /// and a `spawn_blocking` per statement, a cancelled caller left the
    /// blocking task holding a clone of the handle, so the session could no
    /// longer reclaim its own connection and had to leak it. Here the
    /// cancelled caller drops nothing but a `oneshot` receiver.
    #[tokio::test]
    async fn cancelled_query_leaves_session_and_pool_intact() {
        let backend = Rusqlite::builder(":memory:")
            .handle_reuse(2)
            .build()
            .unwrap();

        let conn = backend.connect().await.unwrap();
        let before = backend.reuse_stats().unwrap();

        // A statement slow enough to still be running when we walk away
        // from it, so the cancellation really is mid-flight.
        let slow = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 400000) \
                    SELECT count(*) FROM c";
        let cancelled = tokio::time::timeout(Duration::from_millis(2), conn.query(slow, &[])).await;
        assert!(
            cancelled.is_err(),
            "the probe statement finished too fast to test cancellation"
        );

        // 1. The session still answers. This also proves ordering: the
        //    abandoned statement ran to completion on the worker and this
        //    one was served after it, not instead of it.
        let rs = conn.query("SELECT 1", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));

        // 2. The session is not stuck inside anything -- a cancelled read
        //    must not have left a transaction open.
        conn.execute("BEGIN", &[])
            .await
            .expect("session was left inside a transaction");
        conn.execute("ROLLBACK", &[]).await.unwrap();

        // 3. The worker still returns cleanly to the pool at end of
        //    session: the cancellation cost the pool nothing.
        drop(conn);
        let after = await_stats(&backend, |s| s.returned == before.returned + 1).await;
        assert_eq!(
            after.discarded, before.discarded,
            "a cancelled query cost the pool a handle: {after:?}"
        );

        // 4. And the recycled worker is usable by the next session.
        let next = backend.connect().await.unwrap();
        assert_eq!(
            backend.reuse_stats().unwrap().hits,
            before.hits + 1,
            "the worker did not come back to the free-list"
        );
        assert_eq!(
            next.query("SELECT 1", &[]).await.unwrap().rows[0][0],
            Value::Integer(1)
        );
    }

    /// With reuse off -- the default, and the configuration ePHPm ships --
    /// a cancelled future must equally leave the session usable. There is
    /// no pool to observe here, so the assertion is behavioural.
    #[tokio::test]
    async fn cancelled_query_is_safe_without_reuse() {
        let backend = Rusqlite::memory().unwrap();
        let conn = backend.connect().await.unwrap();

        let slow = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 400000) \
                    SELECT count(*) FROM c";
        let cancelled = tokio::time::timeout(Duration::from_millis(2), conn.query(slow, &[])).await;
        assert!(
            cancelled.is_err(),
            "the probe statement finished too fast to test cancellation"
        );

        let rs = conn.query("SELECT 1", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(1));
    }

    /// Sessions must be independent even when they run concurrently on
    /// their own worker threads -- the redesign moved every statement onto
    /// a different thread than before, so this pins the property down under
    /// real parallelism rather than interleaved-on-one-thread awaits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_sessions_are_isolated() {
        let backend = Arc::new(Rusqlite::memory().unwrap());
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, who TEXT)", &[])
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for who in 0..8i64 {
            let backend = Arc::clone(&backend);
            tasks.push(tokio::spawn(async move {
                let conn = backend.connect().await.unwrap();
                for i in 0..25i64 {
                    conn.execute(
                        "INSERT INTO t (id, who) VALUES (?1, ?2)",
                        &[Value::Integer(who * 100 + i), Value::Text(who.to_string())],
                    )
                    .await
                    .unwrap();
                }
                let rs = conn
                    .query(
                        "SELECT COUNT(*) FROM t WHERE who = ?1",
                        &[Value::Text(who.to_string())],
                    )
                    .await
                    .unwrap();
                assert_eq!(rs.rows[0][0], Value::Integer(25));
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        let rs = backend.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(200));
    }

    #[tokio::test]
    async fn last_insert_rowid_increments() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                &[],
            )
            .await
            .unwrap();

        let r1 = backend
            .execute("INSERT INTO t (v) VALUES (?1)", &[Value::Text("a".into())])
            .await
            .unwrap();
        let r2 = backend
            .execute("INSERT INTO t (v) VALUES (?1)", &[Value::Text("b".into())])
            .await
            .unwrap();
        let r3 = backend
            .execute("INSERT INTO t (v) VALUES (?1)", &[Value::Text("c".into())])
            .await
            .unwrap();

        assert_eq!(r1.last_insert_rowid, Some(1));
        assert_eq!(r2.last_insert_rowid, Some(2));
        assert_eq!(r3.last_insert_rowid, Some(3));
    }

    #[tokio::test]
    async fn query_execute_query_sequential() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        // Query empty table
        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert!(rs.rows.is_empty());

        // Insert a row
        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(1), Value::Text("first".into())],
            )
            .await
            .unwrap();

        // Query again and see the new row
        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], Value::Text("first".into()));

        // Insert another row
        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(2), Value::Text("second".into())],
            )
            .await
            .unwrap();

        // Verify both rows exist
        let rs = backend
            .query("SELECT * FROM t ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 2);
    }

    #[tokio::test]
    async fn explicit_integer_primary_key_rowid() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        // Insert with an explicit id of 42
        let result = backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(42), Value::Text("at 42".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.last_insert_rowid, Some(42));

        // Insert with an explicit id of 100
        let result = backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(100), Value::Text("at 100".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.last_insert_rowid, Some(100));
    }

    #[tokio::test]
    async fn integer_extremes() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v INTEGER)", &[])
            .await
            .unwrap();

        for val in [i64::MIN, i64::MAX, 0] {
            backend
                .execute("INSERT INTO t VALUES (?1)", &[Value::Integer(val)])
                .await
                .unwrap();
        }

        let rs = backend
            .query("SELECT v FROM t ORDER BY rowid", &[])
            .await
            .unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(i64::MIN));
        assert_eq!(rs.rows[1][0], Value::Integer(i64::MAX));
        assert_eq!(rs.rows[2][0], Value::Integer(0));
    }

    #[tokio::test]
    async fn float_extremes() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v REAL)", &[])
            .await
            .unwrap();

        for val in [f64::MIN, f64::MAX, f64::MIN_POSITIVE] {
            backend
                .execute("INSERT INTO t VALUES (?1)", &[Value::Float(val)])
                .await
                .unwrap();
        }

        let rs = backend
            .query("SELECT v FROM t ORDER BY rowid", &[])
            .await
            .unwrap();
        assert_eq!(rs.rows[0][0], Value::Float(f64::MIN));
        assert_eq!(rs.rows[1][0], Value::Float(f64::MAX));
        assert_eq!(rs.rows[2][0], Value::Float(f64::MIN_POSITIVE));
    }

    #[tokio::test]
    async fn empty_text_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Text(String::new())])
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text(String::new()));
    }

    #[tokio::test]
    async fn empty_blob_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v BLOB)", &[])
            .await
            .unwrap();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Blob(vec![])])
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Blob(vec![]));
    }

    #[tokio::test]
    async fn large_text_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();

        let large = "x".repeat(10_000);
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Text(large.clone())])
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text(large));
    }

    #[tokio::test]
    async fn large_blob_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v BLOB)", &[])
            .await
            .unwrap();

        let large: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Blob(large.clone())])
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Blob(large));
    }

    #[tokio::test]
    async fn text_with_special_characters() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();

        let specials = vec![
            "hello\0world",            // null byte
            "こんにちは",              // Japanese
            "🎉🚀💯",                  // emoji
            "café résumé naïve",       // accented
            "line1\nline2\ttab",       // newline and tab
            "'quotes' and \"double\"", // quotes
        ];

        for s in &specials {
            backend
                .execute("INSERT INTO t VALUES (?1)", &[Value::Text(s.to_string())])
                .await
                .unwrap();
        }

        let rs = backend
            .query("SELECT v FROM t ORDER BY rowid", &[])
            .await
            .unwrap();
        for (i, s) in specials.iter().enumerate() {
            assert_eq!(rs.rows[i][0], Value::Text(s.to_string()));
        }
    }

    #[tokio::test]
    async fn insert_all_types_in_one_row() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (a, b INTEGER, c REAL, d TEXT, e BLOB)", &[])
            .await
            .unwrap();

        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5)",
                &[
                    Value::Null,
                    Value::Integer(99),
                    Value::Float(1.5),
                    Value::Text("mixed".into()),
                    Value::Blob(vec![0xAB, 0xCD]),
                ],
            )
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Null);
        assert_eq!(rs.rows[0][1], Value::Integer(99));
        assert_eq!(rs.rows[0][2], Value::Float(1.5));
        assert_eq!(rs.rows[0][3], Value::Text("mixed".into()));
        assert_eq!(rs.rows[0][4], Value::Blob(vec![0xAB, 0xCD]));
    }

    #[tokio::test]
    async fn query_nonexistent_table() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.query("SELECT * FROM nonexistent_table", &[]).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no such table"),
            "expected 'no such table' in error: {err}"
        );
    }

    #[tokio::test]
    async fn execute_wrong_param_count() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (a INTEGER, b TEXT)", &[])
            .await
            .unwrap();

        // Provide 3 params when only 2 are expected
        let result = backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[
                    Value::Integer(1),
                    Value::Text("hello".into()),
                    Value::Integer(99),
                ],
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_sql_syntax_error() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.execute("INSERT INTO VALUES ()", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn update_affected_rows() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER, v TEXT)", &[])
            .await
            .unwrap();

        for i in 0..3 {
            backend
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Integer(i), Value::Text("same".into())],
                )
                .await
                .unwrap();
        }

        let result = backend
            .execute(
                "UPDATE t SET v = ?1 WHERE v = ?2",
                &[Value::Text("changed".into()), Value::Text("same".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 3);
    }

    #[tokio::test]
    async fn delete_no_matching_rows() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER)", &[])
            .await
            .unwrap();

        let result = backend
            .execute("DELETE FROM t WHERE id = ?1", &[Value::Integer(999)])
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 0);
    }
}
