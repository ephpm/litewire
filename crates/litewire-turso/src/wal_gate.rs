//! WAL checkpoint gate: keeps a Turso database's `-wal` file bounded under
//! constant read load.
//!
//! # The problem
//!
//! The Turso engine (0.8.3) checkpoints like SQLite: a passive
//! auto-checkpoint runs once the log holds ~1000 un-backfilled frames and
//! copies frames into the main file up to the oldest reader's snapshot, but
//! the log is only ever *rewound* when a writer can take every read-mark
//! slot exclusively — i.e. when there is no active reader at all. The
//! blocking checkpoint modes do not help: `PRAGMA wal_checkpoint(RESTART)` /
//! `(TRUNCATE)` return `busy=1` immediately when any reader holds a slot
//! (`CheckpointGuard` in `storage/wal.rs` tries the locks once and never
//! consults the busy handler). Under constant concurrent load some
//! connection is always inside a statement, so the log is backfilled but
//! never rewound and the `-wal` file grows by one frame per write forever —
//! measured at 1.3 GB in eight minutes on a 0.7 MB database, and multi-GB on
//! a clustered owner whose CDC tailers add continuous readers.
//!
//! # The gate
//!
//! Every statement a session runs through this backend takes the *shared*
//! side of a per-database [`tokio::sync::RwLock`] and holds it until the
//! statement finishes, or, if the statement opened or continued an explicit
//! transaction, until the session is back in autocommit. A maintenance task
//! watches the `-wal` file size and, once it exceeds a threshold, first
//! backfills the log with `PRAGMA wal_checkpoint(PASSIVE)` rounds — which
//! block nobody and reach as far as the oldest live snapshot — and then
//! takes the *exclusive* side, bounded by a short timeout, and runs
//! `PRAGMA wal_checkpoint(TRUNCATE)` while it holds it. With no statement in
//! flight the engine's read-mark locks are free, the checkpoint only has the
//! frames written since the last passive round to copy, and the file is
//! truncated to zero bytes; the next write starts a fresh log.
//!
//! The lock is write-preferring, so a statement arriving while the task is
//! *waiting* queues behind it: that is what lets the task drain a busy
//! database at all (otherwise the reader count never reaches zero). The
//! wait is bounded ([`WalGateConfig::max_wait`]): a session that sits in an
//! open transaction longer than that makes the attempt give up, the queued
//! statements proceed, and the task retries later with exponential backoff.
//! The gate therefore never deadlocks and never stalls a long transaction;
//! it only ever delays *new* statements by at most `max_wait` per attempt.
//!
//! The exclusive window itself is the truncating checkpoint's own duration
//! — a few milliseconds once the passive phase has done the copying. The
//! passive phase is not optional: under permanently pinned readers the
//! engine's own auto-checkpoint lags the head by however old the oldest
//! snapshot is, and a `TRUNCATE` that has to copy and fsync a 64 MiB backlog
//! inside the exclusive window stalls every queued statement for seconds
//! (measured on a 3-node cluster owner). See
//! [`WalGateStats::max_backfill_micros`] versus
//! [`WalGateStats::max_exclusive_micros`].
//!
//! # What participates
//!
//! - every [`BackendConn`](litewire_backend::BackendConn) session handed out
//!   by [`Turso`](crate::Turso) (`query`, `execute`, `describe_columns`);
//! - [`CdcTailer::poll_batch`](crate::cdc::CdcTailer::poll_batch), so a
//!   replication tailer counts as a reader only while it is actually
//!   polling;
//! - callers that use [`Turso::raw_connection`](crate::Turso::raw_connection)
//!   may take [`Turso::wal_gate_permit`](crate::Turso::wal_gate_permit)
//!   around their own statements. They do not have to: a raw statement that
//!   collides with a checkpoint sees the engine's ordinary `Busy` (and the
//!   checkpoint, if it loses the race, is simply retried). The permit only
//!   makes the collision impossible.
//!
//! One gate exists per database *file*, keyed by canonical path, so several
//! [`Turso`](crate::Turso) factories on the same file (the engine shares one
//! instance per path in-process) share one lock. Each factory that enables
//! the gate runs its own maintenance task; a second task on the same file is
//! harmless (the attempt is serialized by the exclusive side) and exits when
//! its factory is dropped.
//!
//! # Lifetime
//!
//! The task holds only a [`Weak`] reference to the engine handle and a
//! shutdown signal that [`Turso`](crate::Turso)'s `Drop` fires, so dropping
//! the factory (an LRU eviction in a multi-tenant embedder, say) ends the
//! task promptly and never keeps the database open. Nothing is spawned for
//! `:memory:` databases, which have no WAL file.
//!
//! # Journal modes
//!
//! The task checks `PRAGMA journal_mode` before every attempt and does
//! nothing unless it is `wal` — Turso's experimental MVCC mode keeps its own
//! logical log and this gate makes no claim about it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, watch};

/// Tuning for the WAL checkpoint gate. Enable it with
/// [`TursoBuilder::wal_checkpoint_gate`](crate::TursoBuilder::wal_checkpoint_gate).
#[derive(Clone, Debug)]
pub struct WalGateConfig {
    /// Run a truncating checkpoint once the `-wal` file is at least this
    /// large. Default 64 MiB (16384 frames of 4 KiB) — large enough that the
    /// engine's own passive checkpoint has long since backfilled everything
    /// but the tail, so the exclusive window is just the truncate.
    pub threshold_bytes: u64,
    /// How often the file size is sampled (one `stat` per tick). Also the
    /// base of the retry backoff. Default 2 s.
    pub interval: Duration,
    /// Longest the task waits for in-flight statements and open
    /// transactions to drain before giving up on an attempt. While it waits,
    /// new statements queue behind it, so this is also the worst-case stall
    /// one attempt can inflict on traffic. Default 250 ms.
    pub max_wait: Duration,
    /// Cap on the retry backoff after a failed attempt (timed out, busy, or
    /// an engine error). Default 60 s.
    pub max_backoff: Duration,
}

impl Default for WalGateConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 64 * 1024 * 1024,
            interval: Duration::from_secs(2),
            max_wait: Duration::from_millis(250),
            max_backoff: Duration::from_secs(60),
        }
    }
}

/// Counters for one gate (shared by every factory on the same file).
/// Snapshot via [`Turso::wal_gate_stats`](crate::Turso::wal_gate_stats).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalGateStats {
    /// Attempts made (the file was over the threshold at a tick).
    pub attempts: u64,
    /// Attempts that truncated the log.
    pub truncated: u64,
    /// Attempts where the engine still reported the checkpoint busy after
    /// the gate was held — a reader outside the gate (a raw connection) was
    /// active.
    pub busy: u64,
    /// Attempts abandoned because the exclusive side could not be taken
    /// within `max_wait` (an open transaction outlived the wait).
    pub timed_out: u64,
    /// Attempts skipped because the database was not in `wal` journal mode.
    pub skipped_journal_mode: u64,
    /// Attempts that failed with an engine error.
    pub errors: u64,
    /// Bytes the `-wal` file held just before each successful truncate,
    /// summed.
    pub bytes_truncated: u64,
    /// Frames moved into the main file by the passive backfill phase that
    /// runs *outside* the gate before each exclusive attempt, summed.
    pub frames_backfilled: u64,
    /// Duration of the most recent passive backfill phase (outside the
    /// gate; blocks nobody), in microseconds.
    pub last_backfill_micros: u64,
    /// Longest passive backfill phase seen, in microseconds.
    pub max_backfill_micros: u64,
    /// Duration of the most recent exclusive window, in microseconds.
    pub last_exclusive_micros: u64,
    /// Longest exclusive window seen, in microseconds.
    pub max_exclusive_micros: u64,
}

/// Shared side of the gate, held for the duration of a statement or an
/// explicit transaction. Dropping it releases the slot.
///
/// Obtained by the backend for every session statement, and available to
/// raw-connection callers through
/// [`Turso::wal_gate_permit`](crate::Turso::wal_gate_permit).
#[must_use = "the permit protects the statement only while it is held"]
pub struct WalGatePermit {
    _guard: OwnedRwLockReadGuard<()>,
}

#[derive(Default)]
struct Counters {
    attempts: AtomicU64,
    truncated: AtomicU64,
    busy: AtomicU64,
    timed_out: AtomicU64,
    skipped_journal_mode: AtomicU64,
    errors: AtomicU64,
    bytes_truncated: AtomicU64,
    frames_backfilled: AtomicU64,
    last_backfill_micros: AtomicU64,
    max_backfill_micros: AtomicU64,
    last_exclusive_micros: AtomicU64,
    max_exclusive_micros: AtomicU64,
}

/// One gate per database file.
pub(crate) struct Gate {
    lock: Arc<RwLock<()>>,
    counters: Counters,
    /// Set once the task has logged that the journal mode is not `wal`, so
    /// the warning is not repeated on every tick.
    journal_mode_warned: AtomicBool,
}

impl Gate {
    fn new() -> Self {
        Self {
            lock: Arc::new(RwLock::new(())),
            counters: Counters::default(),
            journal_mode_warned: AtomicBool::new(false),
        }
    }

    /// Take the shared side. Queues if an exclusive attempt is pending.
    pub(crate) async fn permit(&self) -> WalGatePermit {
        WalGatePermit {
            _guard: Arc::clone(&self.lock).read_owned().await,
        }
    }

    pub(crate) fn stats(&self) -> WalGateStats {
        let c = &self.counters;
        WalGateStats {
            attempts: c.attempts.load(Ordering::Relaxed),
            truncated: c.truncated.load(Ordering::Relaxed),
            busy: c.busy.load(Ordering::Relaxed),
            timed_out: c.timed_out.load(Ordering::Relaxed),
            skipped_journal_mode: c.skipped_journal_mode.load(Ordering::Relaxed),
            errors: c.errors.load(Ordering::Relaxed),
            bytes_truncated: c.bytes_truncated.load(Ordering::Relaxed),
            frames_backfilled: c.frames_backfilled.load(Ordering::Relaxed),
            last_backfill_micros: c.last_backfill_micros.load(Ordering::Relaxed),
            max_backfill_micros: c.max_backfill_micros.load(Ordering::Relaxed),
            last_exclusive_micros: c.last_exclusive_micros.load(Ordering::Relaxed),
            max_exclusive_micros: c.max_exclusive_micros.load(Ordering::Relaxed),
        }
    }

    /// Take the exclusive side: first without waiting, then with a bounded
    /// wait during which new shared acquirers queue behind us.
    async fn exclusive(&self, max_wait: Duration) -> Option<OwnedRwLockWriteGuard<()>> {
        let lock = Arc::clone(&self.lock);
        if let Ok(guard) = Arc::clone(&lock).try_write_owned() {
            return Some(guard);
        }
        tokio::time::timeout(max_wait, lock.write_owned())
            .await
            .ok()
    }
}

/// Process-wide registry of gates, one per canonical database path, so
/// every factory on the same file shares the same lock.
static GATES: SyncMutex<Option<HashMap<PathBuf, Weak<Gate>>>> = SyncMutex::new(None);

/// The gate for `path`, creating it if no live factory holds one.
pub(crate) fn gate_for(path: &str) -> Arc<Gate> {
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path));
    let mut guard = GATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let map = guard.get_or_insert_with(HashMap::new);
    if let Some(gate) = map.get(&key).and_then(Weak::upgrade) {
        return gate;
    }
    // Drop entries whose factories are all gone; the map only ever holds a
    // handful of open databases, so a linear sweep on insert is fine.
    map.retain(|_, weak| weak.strong_count() > 0);
    let gate = Arc::new(Gate::new());
    map.insert(key, Arc::downgrade(&gate));
    gate
}

/// Handle to one factory's maintenance task: the shutdown signal its owner
/// fires on drop, and a watch that flips to `true` when the task has exited.
pub(crate) struct TaskHandle {
    stop: Arc<Notify>,
    finished: watch::Receiver<bool>,
}

impl TaskHandle {
    pub(crate) fn stop(&self) {
        self.stop.notify_one();
    }

    pub(crate) fn finished(&self) -> watch::Receiver<bool> {
        self.finished.clone()
    }
}

/// Spawn the maintenance task for one factory. `db` is held weakly: the
/// task upgrades it only for the duration of an attempt and exits when the
/// factory is gone.
pub(crate) fn spawn_task(
    db: Weak<turso::Database>,
    gate: Arc<Gate>,
    path: &str,
    config: WalGateConfig,
) -> TaskHandle {
    let stop = Arc::new(Notify::new());
    let (finished_tx, finished_rx) = watch::channel(false);
    let wal_path = PathBuf::from(format!("{path}-wal"));
    let task_stop = Arc::clone(&stop);
    tokio::spawn(async move {
        run(db, gate, wal_path, config, task_stop).await;
        // Receivers see `true`; the sender then drops, which also ends any
        // `changed()` wait with an error — both read as "finished".
        let _ = finished_tx.send(true);
    });
    TaskHandle {
        stop,
        finished: finished_rx,
    }
}

/// Outcome of one checkpoint attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Attempt {
    Truncated,
    Busy,
    TimedOut,
    SkippedJournalMode,
    Error,
}

async fn run(
    db: Weak<turso::Database>,
    gate: Arc<Gate>,
    wal_path: PathBuf,
    config: WalGateConfig,
    stop: Arc<Notify>,
) {
    let mut delay = config.interval;
    loop {
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = stop.notified() => return,
        }
        let Some(db) = db.upgrade() else { return };
        let size = wal_size(&wal_path);
        if size < config.threshold_bytes {
            delay = config.interval;
            continue;
        }
        let outcome = attempt(&db, &gate, &config, size).await;
        // Release the engine handle before sleeping so a dropped factory is
        // never kept alive across a tick.
        drop(db);
        delay = match outcome {
            Attempt::Truncated => config.interval,
            Attempt::Busy | Attempt::TimedOut | Attempt::Error | Attempt::SkippedJournalMode => {
                delay.saturating_mul(2).min(config.max_backoff)
            }
        };
    }
}

fn wal_size(wal_path: &Path) -> u64 {
    std::fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0)
}

/// Upper bound on passive backfill rounds per attempt. Each round copies
/// every frame up to the oldest live reader's snapshot; under short
/// statements the second round is already near the head, so this is a
/// guard against a pathological loop, not a tuning knob.
const MAX_BACKFILL_ROUNDS: u32 = 8;

async fn attempt(
    db: &turso::Database,
    gate: &Gate,
    config: &WalGateConfig,
    size_before: u64,
) -> Attempt {
    let c = &gate.counters;
    c.attempts.fetch_add(1, Ordering::Relaxed);

    let conn = match db.connect() {
        Ok(conn) => conn,
        Err(e) => {
            c.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("wal gate: cannot open a checkpoint connection: {e}");
            return Attempt::Error;
        }
    };
    match journal_mode_is_wal(&conn, gate).await {
        Ok(true) => {}
        Ok(false) => {
            c.skipped_journal_mode.fetch_add(1, Ordering::Relaxed);
            return Attempt::SkippedJournalMode;
        }
        Err(e) => {
            c.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("wal gate: cannot read the journal mode: {e}");
            return Attempt::Error;
        }
    }

    // Phase 1, outside the gate: move the bulk of the log into the main
    // file with PASSIVE checkpoints, which never block a reader or a
    // writer. Under permanently pinned readers the engine's own
    // auto-checkpoint lags the head by however far the oldest snapshot is,
    // and a TRUNCATE has to copy (and fsync) every un-backfilled frame
    // while it holds the locks: measured at seconds for a 64 MiB log under
    // a 3-node cluster owner, which is exactly the stall the gate must not
    // cause. After this phase the exclusive window only has the frames
    // written during the last round to copy.
    let backfill_started = Instant::now();
    let frames = passive_backfill(&conn).await;
    let backfill_micros = u64::try_from(backfill_started.elapsed().as_micros()).unwrap_or(u64::MAX);
    c.last_backfill_micros
        .store(backfill_micros, Ordering::Relaxed);
    c.max_backfill_micros
        .fetch_max(backfill_micros, Ordering::Relaxed);
    match frames {
        Ok(n) => {
            c.frames_backfilled.fetch_add(n, Ordering::Relaxed);
        }
        Err(e) => {
            c.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                wal_bytes = size_before,
                "wal gate: passive backfill failed: {e}"
            );
            return Attempt::Error;
        }
    }

    // Phase 2, inside the gate: drain statements and truncate.
    let Some(guard) = gate.exclusive(config.max_wait).await else {
        c.timed_out.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            wal_bytes = size_before,
            max_wait_ms = config.max_wait.as_millis(),
            "wal gate: in-flight transactions did not drain in time; will retry"
        );
        return Attempt::TimedOut;
    };
    let started = Instant::now();
    let outcome = checkpoint(&conn, "TRUNCATE").await;
    let window = started.elapsed();
    drop(guard);

    let micros = u64::try_from(window.as_micros()).unwrap_or(u64::MAX);
    c.last_exclusive_micros.store(micros, Ordering::Relaxed);
    c.max_exclusive_micros.fetch_max(micros, Ordering::Relaxed);
    match outcome {
        Ok(CheckpointRow { busy: false, .. }) => {
            c.truncated.fetch_add(1, Ordering::Relaxed);
            c.bytes_truncated.fetch_add(size_before, Ordering::Relaxed);
            tracing::debug!(
                wal_bytes = size_before,
                backfill_micros,
                exclusive_micros = micros,
                "wal gate: truncated the write-ahead log"
            );
            Attempt::Truncated
        }
        Ok(CheckpointRow { busy: true, .. }) => {
            c.busy.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                wal_bytes = size_before,
                "wal gate: engine reported the checkpoint busy (a reader outside the gate \
                 is active); will retry"
            );
            Attempt::Busy
        }
        Err(e) => {
            c.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(wal_bytes = size_before, "wal gate: checkpoint failed: {e}");
            Attempt::Error
        }
    }
}

/// `true` when the database is in WAL journal mode; logs once otherwise.
async fn journal_mode_is_wal(conn: &turso::Connection, gate: &Gate) -> turso::Result<bool> {
    let mode = first_text(conn, "PRAGMA journal_mode").await?;
    if mode.eq_ignore_ascii_case("wal") {
        return Ok(true);
    }
    if !gate.journal_mode_warned.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            journal_mode = %mode,
            "wal gate: database is not in WAL journal mode; the gate will not checkpoint it"
        );
    }
    Ok(false)
}

/// Passive-checkpoint until the log is fully backfilled, another
/// checkpointer is running, or a round makes no progress (the oldest
/// reader's snapshot is the ceiling). Returns the number of frames
/// backfilled across rounds.
async fn passive_backfill(conn: &turso::Connection) -> turso::Result<u64> {
    let mut total = 0u64;
    let mut last_checkpointed: Option<i64> = None;
    for _ in 0..MAX_BACKFILL_ROUNDS {
        let row = checkpoint(conn, "PASSIVE").await?;
        if row.busy {
            break;
        }
        let progressed = last_checkpointed.is_none_or(|prev| row.checkpointed > prev);
        if let Some(prev) = last_checkpointed {
            total += u64::try_from(row.checkpointed - prev).unwrap_or(0);
        } else {
            total += u64::try_from(row.checkpointed).unwrap_or(0);
        }
        last_checkpointed = Some(row.checkpointed);
        if !progressed || row.checkpointed >= row.log {
            break;
        }
        // Let readers that were pinning the old snapshot finish before the
        // next round so it can reach further.
        tokio::task::yield_now().await;
    }
    Ok(total)
}

/// The `(busy, log, checkpointed)` row `PRAGMA wal_checkpoint` returns, as
/// in SQLite: `busy` is set when a lock could not be taken (the engine
/// reports that as a row, not an error), `log` is the WAL's frame count
/// and `checkpointed` how many of those are in the main file.
struct CheckpointRow {
    busy: bool,
    log: i64,
    checkpointed: i64,
}

async fn checkpoint(conn: &turso::Connection, mode: &str) -> turso::Result<CheckpointRow> {
    let mut rows = conn
        .query(format!("PRAGMA wal_checkpoint({mode})"), ())
        .await?;
    let row = match rows.next().await? {
        Some(row) => CheckpointRow {
            busy: matches!(row.get_value(0)?, turso::Value::Integer(1)),
            log: int_or_zero(row.get_value(1)?),
            checkpointed: int_or_zero(row.get_value(2)?),
        },
        None => CheckpointRow {
            busy: false,
            log: 0,
            checkpointed: 0,
        },
    };
    drop(rows);
    Ok(row)
}

fn int_or_zero(v: turso::Value) -> i64 {
    match v {
        turso::Value::Integer(i) => i,
        _ => 0,
    }
}

async fn first_text(conn: &turso::Connection, sql: &str) -> turso::Result<String> {
    let mut rows = conn.query(sql, ()).await?;
    let text = match rows.next().await? {
        Some(row) => match row.get_value(0)? {
            turso::Value::Text(s) => s,
            other => format!("{other:?}"),
        },
        None => String::new(),
    };
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exclusive_waits_for_a_held_permit_then_gives_up() {
        let gate = Gate::new();
        let held = gate.permit().await;
        let started = Instant::now();
        assert!(
            gate.exclusive(Duration::from_millis(50)).await.is_none(),
            "exclusive must not be granted while a permit is held"
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
        drop(held);
        assert!(gate.exclusive(Duration::from_millis(50)).await.is_some());
    }

    #[tokio::test]
    async fn permits_queued_behind_a_pending_exclusive_proceed_once_it_gives_up() {
        let gate = Arc::new(Gate::new());
        let held = gate.permit().await;
        // A pending exclusive makes a *new* permit queue (write-preferring).
        let pending = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.exclusive(Duration::from_millis(100)).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        let queued = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.permit().await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !queued.is_finished(),
            "new permit must queue behind a pending exclusive"
        );
        // The exclusive gives up at its deadline and the queued permit goes
        // through even though the first permit is still held.
        assert!(pending.await.unwrap().is_none());
        let _queued = tokio::time::timeout(Duration::from_secs(1), queued)
            .await
            .expect("queued permit must proceed once the exclusive gives up")
            .unwrap();
        drop(held);
    }

    #[tokio::test]
    async fn exclusive_is_granted_once_every_permit_is_released() {
        let gate = Arc::new(Gate::new());
        let a = gate.permit().await;
        let b = gate.permit().await;
        let pending = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.exclusive(Duration::from_secs(5)).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(a);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!pending.is_finished(), "one permit still held");
        drop(b);
        assert!(pending.await.unwrap().is_some());
    }

    #[test]
    fn registry_shares_one_gate_per_path_and_forgets_dead_ones() {
        let dir = std::env::temp_dir().join(format!("lw-gate-reg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("g.db");
        std::fs::write(&path, b"").unwrap();
        let p = path.to_str().unwrap();
        let a = gate_for(p);
        let b = gate_for(p);
        assert!(Arc::ptr_eq(&a, &b), "same path must share one gate");
        let other = dir.join("h.db");
        let c = gate_for(other.to_str().unwrap());
        assert!(!Arc::ptr_eq(&a, &c));
        drop((a, b));
        let d = gate_for(p);
        assert_eq!(
            d.stats(),
            WalGateStats::default(),
            "a dead gate is replaced, not revived"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn defaults_are_the_documented_ones() {
        let d = WalGateConfig::default();
        assert_eq!(d.threshold_bytes, 64 * 1024 * 1024);
        assert_eq!(d.interval, Duration::from_secs(2));
        assert_eq!(d.max_wait, Duration::from_millis(250));
        assert_eq!(d.max_backoff, Duration::from_secs(60));
    }
}
