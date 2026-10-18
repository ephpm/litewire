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
//! watches the `-wal` file size and, once it exceeds a threshold, takes the
//! *exclusive* side — bounded by a short timeout — and runs
//! `PRAGMA wal_checkpoint(TRUNCATE)` while it holds it. With no statement in
//! flight the engine's read-mark locks are free, the checkpoint succeeds and
//! the file is truncated to zero bytes; the next write starts a fresh log.
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
//! The exclusive window itself is the checkpoint's own duration — a few
//! milliseconds when the log is already backfilled (the common case: the
//! engine's passive auto-checkpoint keeps up, and `TRUNCATE` only has to
//! copy the last partial window, fsync and truncate). See
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

async fn attempt(
    db: &turso::Database,
    gate: &Gate,
    config: &WalGateConfig,
    size_before: u64,
) -> Attempt {
    let c = &gate.counters;
    c.attempts.fetch_add(1, Ordering::Relaxed);

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
    let outcome = checkpoint_truncate(db, gate).await;
    let window = started.elapsed();
    drop(guard);

    let micros = u64::try_from(window.as_micros()).unwrap_or(u64::MAX);
    c.last_exclusive_micros.store(micros, Ordering::Relaxed);
    c.max_exclusive_micros.fetch_max(micros, Ordering::Relaxed);
    match outcome {
        Ok(Attempt::Truncated) => {
            c.truncated.fetch_add(1, Ordering::Relaxed);
            c.bytes_truncated.fetch_add(size_before, Ordering::Relaxed);
            tracing::debug!(
                wal_bytes = size_before,
                exclusive_micros = micros,
                "wal gate: truncated the write-ahead log"
            );
            Attempt::Truncated
        }
        Ok(Attempt::Busy) => {
            c.busy.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                wal_bytes = size_before,
                "wal gate: engine reported the checkpoint busy (a reader outside the gate \
                 is active); will retry"
            );
            Attempt::Busy
        }
        Ok(Attempt::SkippedJournalMode) => {
            c.skipped_journal_mode.fetch_add(1, Ordering::Relaxed);
            Attempt::SkippedJournalMode
        }
        Ok(other) => other,
        Err(e) => {
            c.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(wal_bytes = size_before, "wal gate: checkpoint failed: {e}");
            Attempt::Error
        }
    }
}

/// Run the truncating checkpoint on a fresh connection and report whether
/// the engine did it. Must be called with the exclusive side held.
async fn checkpoint_truncate(db: &turso::Database, gate: &Gate) -> turso::Result<Attempt> {
    let conn = db.connect()?;
    let mode = first_text(&conn, "PRAGMA journal_mode").await?;
    if !mode.eq_ignore_ascii_case("wal") {
        if !gate.journal_mode_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                journal_mode = %mode,
                "wal gate: database is not in WAL journal mode; the gate will not checkpoint it"
            );
        }
        return Ok(Attempt::SkippedJournalMode);
    }
    // Result row is (busy, log, checkpointed), as in SQLite. The engine
    // reports a lock it could not take as `busy = 1` rather than an error.
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    let busy = match rows.next().await? {
        Some(row) => matches!(row.get_value(0)?, turso::Value::Integer(1)),
        None => false,
    };
    drop(rows);
    Ok(if busy {
        Attempt::Busy
    } else {
        Attempt::Truncated
    })
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
