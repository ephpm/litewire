//! The WAL checkpoint gate, end to end against the real engine.
//!
//! The premise test reproduces the production shape that motivated the gate
//! (`litewire_turso::wal_gate`): a constant-size table, a few writers
//! updating rows in place and many readers doing point reads. Without the
//! gate the `-wal` file grows by a frame per write and never rewinds, because
//! some reader always holds a read-mark slot; with the gate it is truncated
//! whenever it crosses the threshold. The other tests pin the guarantees the
//! gate makes around that: an open transaction is never broken (the attempt
//! times out and retries), CDC tailing keeps converging, and the maintenance
//! task dies with its factory.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use litewire_backend::{Backend, Value};
use litewire_turso::cdc::{CdcTailer, apply_batch, read_watermark};
use litewire_turso::{Turso, WalGateConfig};

const ROWS: i64 = 3000;
const MIB: u64 = 1024 * 1024;

fn gate_config(threshold_bytes: u64) -> WalGateConfig {
    WalGateConfig {
        threshold_bytes,
        interval: Duration::from_millis(50),
        max_wait: Duration::from_millis(200),
        max_backoff: Duration::from_millis(400),
        max_escalated_wait: Duration::from_millis(800),
    }
}

fn wal_bytes(db_path: &str) -> u64 {
    std::fs::metadata(format!("{db_path}-wal")).map_or(0, |m| m.len())
}

/// A fresh file-backed database in its own temp dir (the dir is removed
/// when the handle drops, `-wal` and all).
fn temp_db(name: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(format!("{name}.db"));
    (dir, path.to_str().expect("utf-8 temp path").to_string())
}

/// `big`: ROWS rows of 200-byte padding, written through gated sessions
/// (so CDC-capturing factories record the seed too).
async fn seed(factory: &Turso) {
    let conn = factory.connect().await.expect("connect");
    conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, pad TEXT)", &[])
        .await
        .expect("create");
    for chunk in 0..(ROWS / 500) {
        conn.execute("BEGIN", &[]).await.expect("begin");
        for i in 1..=500 {
            let id = chunk * 500 + i;
            conn.execute(
                "INSERT INTO big (id, pad) VALUES (?1, ?2)",
                &[Value::Integer(id), Value::Text("x".repeat(200))],
            )
            .await
            .expect("insert");
        }
        conn.execute("COMMIT", &[]).await.expect("commit");
    }
}

/// Cheap deterministic pseudo-random ids (an LCG), one stream per task.
fn next_id(state: &mut u64) -> i64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    i64::try_from((*state >> 33) % (ROWS as u64)).expect("fits") + 1
}

struct Workload {
    stop: Arc<AtomicBool>,
    reads: Arc<AtomicU64>,
    writes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Workload {
    /// `readers` tasks doing point reads and `writers` tasks rewriting rows
    /// in place, each on its own session, until `stop`.
    fn start(factory: &Arc<Turso>, readers: u64, writers: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicU64::new(0));
        let writes = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();
        for r in 0..readers {
            let (factory, stop, reads, errors) = (
                Arc::clone(factory),
                Arc::clone(&stop),
                Arc::clone(&reads),
                Arc::clone(&errors),
            );
            tasks.push(tokio::spawn(async move {
                let conn = factory.connect().await.expect("reader connect");
                let mut rng = r + 99;
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id(&mut rng);
                    match conn
                        .query("SELECT pad FROM big WHERE id = ?1", &[Value::Integer(id)])
                        .await
                    {
                        Ok(rs) => {
                            assert_eq!(rs.rows.len(), 1, "point read of id {id}");
                            reads.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("reader error: {e}");
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
        for w in 0..writers {
            let (factory, stop, writes, errors) = (
                Arc::clone(factory),
                Arc::clone(&stop),
                Arc::clone(&writes),
                Arc::clone(&errors),
            );
            tasks.push(tokio::spawn(async move {
                let conn = factory.connect().await.expect("writer connect");
                let mut rng = w + 7;
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id(&mut rng);
                    let pad = format!("{rng:0200}");
                    match conn
                        .execute(
                            "UPDATE big SET pad = ?1 WHERE id = ?2",
                            &[Value::Text(pad), Value::Integer(id)],
                        )
                        .await
                    {
                        Ok(_) => {
                            writes.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("writer error: {e}");
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
        Self {
            stop,
            reads,
            writes,
            errors,
            tasks,
        }
    }

    async fn stop(self) -> (u64, u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.tasks {
            t.await.expect("workload task");
        }
        (
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

async fn count(factory: &Turso, sql: &str) -> i64 {
    let rs = factory.query(sql, &[]).await.expect("count query");
    match rs.rows[0][0] {
        Value::Integer(n) => n,
        ref v => panic!("non-integer count: {v:?}"),
    }
}

/// Sample the WAL size every `every` until `until` returns true or `limit`
/// elapses; returns (max observed, elapsed).
async fn watch_wal(
    db_path: &str,
    every: Duration,
    limit: Duration,
    mut until: impl FnMut(u64) -> bool,
) -> (u64, Duration) {
    let started = Instant::now();
    let mut max = 0;
    loop {
        let now = wal_bytes(db_path);
        max = max.max(now);
        if until(now) || started.elapsed() >= limit {
            return (max, started.elapsed());
        }
        tokio::time::sleep(every).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wal_grows_without_bound_under_constant_readers_and_is_bounded_by_the_gate() {
    const THRESHOLD: u64 = 4 * MIB;
    // How far past the threshold the ungated run must get before it has
    // demonstrated "unbounded" — the point at which the engine would long
    // since have rewound the log if it could.
    const UNBOUNDED_AT: u64 = 8 * THRESHOLD;

    // --- Without the gate: grows until we stop looking. ----------------
    let (dir_a, path_a) = temp_db("ungated");
    let ungated = Arc::new(Turso::open(&path_a).await.expect("open ungated"));
    seed(&ungated).await;
    let load = Workload::start(&ungated, 16, 2);
    let (ungated_wal, ungated_took) = watch_wal(
        &path_a,
        Duration::from_millis(50),
        Duration::from_secs(60),
        |now| now >= UNBOUNDED_AT,
    )
    .await;
    let (reads, writes, errors) = load.stop().await;
    eprintln!(
        "ungated: wal={:.1} MiB after {:?} (reads={reads} writes={writes} errors={errors})",
        ungated_wal as f64 / MIB as f64,
        ungated_took
    );
    assert_eq!(errors, 0, "workload errors without the gate");
    assert!(
        ungated_wal >= UNBOUNDED_AT,
        "premise: with 16 continuous readers the engine never rewinds the WAL \
         (expected >= {} MiB, saw {:.1} MiB after {ungated_took:?}); if this \
         fails the engine's checkpoint behaviour changed and the gate may be moot",
        UNBOUNDED_AT / MIB,
        ungated_wal as f64 / MIB as f64,
    );
    assert_eq!(count(&ungated, "SELECT COUNT(*) FROM big").await, ROWS);
    drop(ungated);
    drop(dir_a);

    // --- With the gate: same load for longer, WAL stays near threshold. --
    let (_dir_b, path_b) = temp_db("gated");
    let gated = Arc::new(
        Turso::builder(&path_b)
            .wal_checkpoint_gate(gate_config(THRESHOLD))
            .build()
            .await
            .expect("open gated"),
    );
    seed(&gated).await;
    let load = Workload::start(&gated, 16, 2);
    let run_for = (ungated_took * 2).max(Duration::from_secs(3));
    let (gated_max, _) = watch_wal(&path_b, Duration::from_millis(10), run_for, |_| false).await;
    let (reads, writes, errors) = load.stop().await;
    let stats = gated.wal_gate_stats().expect("gate enabled");
    eprintln!(
        "gated: max wal={:.1} MiB over {run_for:?} (reads={reads} writes={writes} \
         errors={errors}) stats={stats:?}",
        gated_max as f64 / MIB as f64
    );
    assert_eq!(errors, 0, "workload errors with the gate");
    assert!(
        stats.truncated >= 2,
        "the gate must have truncated the log repeatedly: {stats:?}"
    );
    assert!(
        gated_max < ungated_wal / 2,
        "gated WAL peaked at {:.1} MiB; ungated reached {:.1} MiB in less time",
        gated_max as f64 / MIB as f64,
        ungated_wal as f64 / MIB as f64
    );
    // The exclusive window is the checkpoint itself; a truncate of a
    // backfilled log is milliseconds, not the statement-level stall a
    // naive "stop the world and checkpoint" would be.
    assert!(
        stats.max_exclusive_micros < 2_000_000,
        "exclusive window too long: {stats:?}"
    );
    // Data is intact and the truncated log left nothing behind.
    assert_eq!(count(&gated, "SELECT COUNT(*) FROM big").await, ROWS);
    let rs = gated
        .query("PRAGMA integrity_check", &[])
        .await
        .expect("integrity_check");
    assert_eq!(rs.rows[0][0], Value::Text("ok".into()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_open_explicit_transaction_is_never_broken_and_the_gate_retries() {
    const THRESHOLD: u64 = 2 * MIB;
    let (_dir, path) = temp_db("txn");
    let factory = Arc::new(
        Turso::builder(&path)
            .wal_checkpoint_gate(gate_config(THRESHOLD))
            .build()
            .await
            .expect("open"),
    );
    seed(&factory).await;

    // A session that opens a transaction, reads, and sits in it while the
    // WAL crosses the threshold under load. A *read* transaction: it pins
    // a read mark (which is what defeats a truncating checkpoint) without
    // taking the engine's single write lock, so the workload's writers keep
    // growing the log. (A held write transaction would block every other
    // writer for its busy timeout instead — a different scenario, covered
    // at the end.)
    let long = factory.connect().await.expect("long session");
    long.execute("BEGIN", &[]).await.expect("begin");
    let seen_at_begin = long
        .query("SELECT pad FROM big WHERE id = 1", &[])
        .await
        .expect("read inside txn")
        .rows[0][0]
        .clone();
    assert_eq!(seen_at_begin, Value::Text("x".repeat(200)));

    let load = Workload::start(&factory, 8, 2);
    // Let the WAL cross the threshold and give the task several attempts.
    let (_, _) = watch_wal(
        &path,
        Duration::from_millis(20),
        Duration::from_secs(30),
        |now| now >= THRESHOLD,
    )
    .await;
    let reads_before = load.reads.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let reads_during = load.reads.load(Ordering::Relaxed) - reads_before;
    let held = factory.wal_gate_stats().expect("gate enabled");
    eprintln!("while the transaction was held: {held:?}; reads during hold={reads_during}");
    assert!(
        held.timed_out >= 1,
        "the attempt must time out while a transaction is open: {held:?}"
    );
    assert_eq!(
        held.truncated, 0,
        "nothing may be truncated while a transaction holds the gate: {held:?}"
    );
    // Autocommit sessions were not starved: the bounded wait gives up and
    // lets queued statements through.
    assert!(
        reads_during > 100,
        "autocommit traffic stalled behind the gate: {reads_during} reads in 2s"
    );

    // The transaction is intact: its snapshot still shows the row as it was
    // at BEGIN even though the workload has been rewriting rows the whole
    // time, and it ends cleanly.
    let seen_now = long
        .query("SELECT pad FROM big WHERE id = 1", &[])
        .await
        .expect("read inside txn, later")
        .rows[0][0]
        .clone();
    assert_eq!(
        seen_now, seen_at_begin,
        "snapshot changed under an open transaction"
    );
    long.execute("COMMIT", &[]).await.expect("commit");

    // With the transaction gone the retry succeeds under the same load.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let now = factory.wal_gate_stats().expect("gate enabled");
        if now.truncated >= 1 {
            eprintln!("after commit: {now:?}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "gate never truncated after the transaction committed: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A write transaction under the same load is untouched by the gate: it
    // holds the gate for its lifetime, commits, and its rows are visible.
    let writer = factory.connect().await.expect("write session");
    writer.execute("BEGIN", &[]).await.expect("begin write txn");
    writer
        .execute(
            "INSERT INTO big (id, pad) VALUES (?1, ?2)",
            &[Value::Integer(ROWS + 1), Value::Text("held".into())],
        )
        .await
        .expect("insert inside txn");
    tokio::time::sleep(Duration::from_millis(300)).await;
    writer
        .execute(
            "INSERT INTO big (id, pad) VALUES (?1, ?2)",
            &[Value::Integer(ROWS + 2), Value::Text("held-too".into())],
        )
        .await
        .expect("second insert inside txn");
    writer
        .execute("COMMIT", &[])
        .await
        .expect("commit write txn");
    assert_eq!(
        count(&factory, "SELECT COUNT(*) FROM big WHERE id > 3000").await,
        2,
        "committed rows visible to other sessions"
    );

    let (_, _, errors) = load.stop().await;
    assert_eq!(errors, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cdc_tailing_keeps_working_and_the_replica_converges_with_the_gate_active() {
    const THRESHOLD: u64 = 2 * MIB;
    let (_dir_p, path_p) = temp_db("primary");
    let (_dir_r, path_r) = temp_db("replica");
    let primary = Arc::new(
        Turso::builder(&path_p)
            .enable_cdc_on_connect(true)
            .wal_checkpoint_gate(gate_config(THRESHOLD))
            .build()
            .await
            .expect("open primary"),
    );
    // The replica is gated too, and its apply loop runs on a raw connection,
    // which is exactly the case `wal_gate_permit` exists for.
    let replica = Arc::new(
        Turso::builder(&path_r)
            .wal_checkpoint_gate(gate_config(THRESHOLD))
            .build()
            .await
            .expect("open replica"),
    );
    seed(&primary).await;

    // Tail the primary and apply to the replica for as long as the
    // workload runs, then until two consecutive polls find nothing.
    let stop_tailing = Arc::new(AtomicBool::new(false));
    let tailer_task = {
        let (primary, replica, stop_tailing) = (
            Arc::clone(&primary),
            Arc::clone(&replica),
            Arc::clone(&stop_tailing),
        );
        tokio::spawn(async move {
            let apply_conn = replica.raw_connection().expect("replica apply conn");
            let mut tailer = CdcTailer::new(&primary, 0);
            let mut applied = 0u64;
            let mut idle_polls = 0;
            loop {
                match tailer.poll_batch().await.expect("poll_batch") {
                    Some(batch) => {
                        let _permit = replica.wal_gate_permit().await;
                        apply_batch(&apply_conn, &batch).await.expect("apply_batch");
                        applied += 1;
                        idle_polls = 0;
                    }
                    None => {
                        if stop_tailing.load(Ordering::Relaxed) {
                            idle_polls += 1;
                            if idle_polls >= 2 {
                                return (applied, tailer.cursor());
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
        })
    };

    let load = Workload::start(&primary, 8, 2);
    // Run until the primary has truncated at least twice (bounded).
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let stats = primary.wal_gate_stats().expect("gate enabled");
        if stats.truncated >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "primary never truncated twice under CDC load: {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (reads, writes, errors) = load.stop().await;
    assert_eq!(errors, 0);
    stop_tailing.store(true, Ordering::Relaxed);
    let (applied, cursor) = tailer_task.await.expect("tailer task");
    let pstats = primary.wal_gate_stats().expect("gate enabled");
    let rstats = replica.wal_gate_stats().expect("gate enabled");
    eprintln!(
        "cdc: reads={reads} writes={writes} batches applied={applied} cursor={cursor} \
         primary={pstats:?} replica={rstats:?}"
    );
    assert!(applied > 0);

    // The replica holds exactly the primary's rows, and its watermark is
    // the primary's last commit.
    let on_primary = primary
        .query("SELECT id, pad FROM big ORDER BY id", &[])
        .await
        .expect("primary rows");
    let on_replica = replica
        .query("SELECT id, pad FROM big ORDER BY id", &[])
        .await
        .expect("replica rows");
    assert_eq!(on_primary.rows.len(), usize::try_from(ROWS).expect("rows"));
    assert_eq!(
        on_primary.rows, on_replica.rows,
        "replica diverged from primary"
    );
    let wm = read_watermark(&replica.raw_connection().expect("conn"))
        .await
        .expect("watermark");
    assert_eq!(wm, cursor, "replica watermark must match the tailer cursor");
    let head = count(&primary, "SELECT MAX(change_id) FROM turso_cdc").await;
    assert_eq!(head, cursor, "tailer must have consumed the whole log");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_maintenance_task_exits_when_its_factory_is_dropped() {
    let (_dir_a, path_a) = temp_db("drop-a");
    let (_dir_b, path_b) = temp_db("drop-b");
    let a = Turso::builder(&path_a)
        .wal_checkpoint_gate(gate_config(MIB))
        .build()
        .await
        .expect("open a");
    let b = Turso::builder(&path_b)
        .wal_checkpoint_gate(gate_config(MIB))
        .build()
        .await
        .expect("open b");
    let mut done_a = a.wal_gate_task_finished().expect("a has a gate task");
    let mut done_b = b.wal_gate_task_finished().expect("b has a gate task");
    // Let both tasks tick a few times.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!*done_a.borrow(), "a's task exited while a is alive");
    assert!(!*done_b.borrow(), "b's task exited while b is alive");

    drop(a);
    tokio::time::timeout(Duration::from_secs(2), done_a.wait_for(|v| *v))
        .await
        .expect("a's task must exit promptly after a is dropped")
        .expect("task finished");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!*done_b.borrow(), "dropping a must not stop b's task");

    drop(b);
    tokio::time::timeout(Duration::from_secs(2), done_b.wait_for(|v| *v))
        .await
        .expect("b's task must exit promptly after b is dropped")
        .expect("task finished");

    // No file, no WAL, no task: the knob is ignored for `:memory:`.
    let mem = Turso::builder(":memory:")
        .wal_checkpoint_gate(gate_config(MIB))
        .build()
        .await
        .expect("open memory");
    assert!(mem.wal_gate_task_finished().is_none());
    assert!(mem.wal_gate_stats().is_none());
    assert!(mem.wal_gate_permit().await.is_none());
}

#[tokio::test]
async fn the_gate_is_off_by_default() {
    let (_dir, path) = temp_db("default");
    let t = Turso::open(&path).await.expect("open");
    assert!(t.wal_gate_stats().is_none());
    assert!(t.wal_gate_task_finished().is_none());
    assert!(t.wal_gate_permit().await.is_none());
    // Sessions work exactly as before.
    t.execute("CREATE TABLE t (id INTEGER)", &[])
        .await
        .expect("create");
    t.execute("INSERT INTO t VALUES (1)", &[])
        .await
        .expect("insert");
    assert_eq!(count(&t, "SELECT COUNT(*) FROM t").await, 1);
}
