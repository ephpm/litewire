//! Go/no-go probe for replacing the per-statement `spawn_blocking` handoff
//! with one long-lived blocking task per session.
//!
//! `reuse_cost_attribution` measured a bare `spawn_blocking` round trip at
//! ~51 µs p50 against ~3.5 µs of actual SQLite work -- but it measured it on
//! an *idle* runtime, where every hop pays a cold park/unpark. That number
//! is the optimistic case for a redesign and cannot be used on its own to
//! justify one. Under sustained load the blocking pool stays hot and the
//! hop should shrink; if it shrinks to nothing, the redesign buys nothing.
//!
//! So this probe runs the same statement through three dispatch designs at
//! concurrency 1, 4 and 16 on a multi-threaded runtime:
//!
//! * `A per-stmt spawn_blocking` -- what the backend does today: clone an
//!   `Arc<Mutex<Connection>>` into a fresh blocking task per statement.
//! * `B session actor (std::thread)` -- one dedicated OS thread per session
//!   owning the `Connection`, fed by `std::sync::mpsc`, replying on a
//!   `tokio::sync::oneshot`.
//! * `C session actor (spawn_blocking)` -- the same actor, but parked on a
//!   tokio blocking-pool thread instead of a dedicated one. Measured only
//!   to show that the thread *source* is not where the difference comes
//!   from, because the choice between B and C is made on deadlock safety,
//!   not on speed (see the `Backend::connect` docs).
//!
//! Reported per lane: per-statement latency percentiles across all
//! concurrent sessions, plus aggregate throughput, which is the number that
//! actually matters once the machine is loaded.
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p litewire-backend --example session_hop_probe
//! ```

use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::sync::oneshot;

/// Statements per session. Long enough that per-session setup (thread
/// spawn, `sqlite3_open`) is amortised away and the number reported is the
/// marginal per-statement dispatch cost.
const STATEMENTS: usize = 2_000;

const SQL: &str = "SELECT id, val FROM bench WHERE id = ?1";

/// One unit of work handed to a session actor.
struct Job {
    id: i64,
    reply: oneshot::Sender<i64>,
}

fn seed(path: &std::path::Path) {
    let c = Connection::open(path).expect("open");
    c.pragma_update(None, "journal_mode", "WAL").expect("wal");
    c.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("create");
    for i in 1..=10i64 {
        c.execute("INSERT INTO bench VALUES (?1, ?2)", [i, i * 100])
            .expect("seed");
    }
}

fn open_session(path: &std::path::Path) -> Connection {
    let c = Connection::open(path).expect("open");
    c.busy_timeout(Duration::from_millis(5000)).expect("busy");
    c.pragma_update(None, "synchronous", "NORMAL")
        .expect("sync");
    // Touch the file so the WAL-index attach is paid during setup rather
    // than inside the first timed statement.
    let _: i64 = c
        .query_row("SELECT count(*) FROM bench", [], |r| r.get(0))
        .expect("warm");
    c
}

/// The body every lane runs, so the three lanes differ only in dispatch.
fn point_select(conn: &Connection, id: i64) -> i64 {
    conn.prepare_cached(SQL)
        .expect("prepare")
        .query_row([id], |r| r.get(1))
        .expect("row")
}

/// Drain a session actor's inbox until its sender is dropped.
fn actor_loop(conn: &Connection, rx: &mpsc::Receiver<Job>) {
    while let Ok(job) = rx.recv() {
        let v = point_select(conn, job.id);
        // The receiver is gone only if the caller's future was cancelled.
        let _ = job.reply.send(v);
    }
}

fn report(label: &str, mut samples: Vec<Duration>, wall: Duration) {
    samples.sort_unstable();
    let us = |d: Duration| d.as_nanos() as f64 / 1000.0;
    let n = samples.len();
    let per_sec = n as f64 / wall.as_secs_f64();
    println!(
        "  {label:<34} min {:>7.2}  p50 {:>7.2}  p95 {:>7.2}  p99 {:>7.2} µs   {:>9.0} stmt/s",
        us(samples[0]),
        us(samples[n / 2]),
        us(samples[n * 95 / 100]),
        us(samples[n * 99 / 100]),
        per_sec,
    );
}

/// Lane A: one `spawn_blocking` per statement, exactly the shape of
/// `RusqliteConn::query` today.
async fn lane_per_stmt(path: &std::path::Path, sessions: usize) -> (Vec<Duration>, Duration) {
    let mut handles = Vec::with_capacity(sessions);
    let started = Instant::now();
    for _ in 0..sessions {
        let path = path.to_path_buf();
        handles.push(tokio::spawn(async move {
            let conn = Arc::new(Mutex::new(open_session(&path)));
            let mut samples = Vec::with_capacity(STATEMENTS);
            for i in 0..STATEMENTS {
                let id = (i % 10) as i64 + 1;
                let conn = Arc::clone(&conn);
                let t = Instant::now();
                let v = tokio::task::spawn_blocking(move || point_select(&conn.lock(), id))
                    .await
                    .expect("join");
                samples.push(t.elapsed());
                std::hint::black_box(v);
            }
            samples
        }));
    }
    let mut all = Vec::with_capacity(sessions * STATEMENTS);
    for h in handles {
        all.extend(h.await.expect("task"));
    }
    (all, started.elapsed())
}

/// Lane B: one dedicated OS thread per session owning the `Connection`.
async fn lane_actor_thread(path: &std::path::Path, sessions: usize) -> (Vec<Duration>, Duration) {
    let mut handles = Vec::with_capacity(sessions);
    let started = Instant::now();
    for _ in 0..sessions {
        let path = path.to_path_buf();
        handles.push(tokio::spawn(async move {
            let (tx, rx) = mpsc::channel::<Job>();
            let worker = std::thread::spawn(move || {
                let conn = open_session(&path);
                actor_loop(&conn, &rx);
            });

            let mut samples = Vec::with_capacity(STATEMENTS);
            for i in 0..STATEMENTS {
                let id = (i % 10) as i64 + 1;
                let (reply, wait) = oneshot::channel();
                let t = Instant::now();
                tx.send(Job { id, reply }).expect("send");
                let v = wait.await.expect("reply");
                samples.push(t.elapsed());
                std::hint::black_box(v);
            }
            drop(tx);
            worker.join().expect("worker");
            samples
        }));
    }
    let mut all = Vec::with_capacity(sessions * STATEMENTS);
    for h in handles {
        all.extend(h.await.expect("task"));
    }
    (all, started.elapsed())
}

/// Lane C: the same actor parked on a tokio blocking-pool thread.
async fn lane_actor_blocking(path: &std::path::Path, sessions: usize) -> (Vec<Duration>, Duration) {
    let mut handles = Vec::with_capacity(sessions);
    let started = Instant::now();
    for _ in 0..sessions {
        let path = path.to_path_buf();
        handles.push(tokio::spawn(async move {
            let (tx, rx) = mpsc::channel::<Job>();
            let worker = tokio::task::spawn_blocking(move || {
                let conn = open_session(&path);
                actor_loop(&conn, &rx);
            });

            let mut samples = Vec::with_capacity(STATEMENTS);
            for i in 0..STATEMENTS {
                let id = (i % 10) as i64 + 1;
                let (reply, wait) = oneshot::channel();
                let t = Instant::now();
                tx.send(Job { id, reply }).expect("send");
                let v = wait.await.expect("reply");
                samples.push(t.elapsed());
                std::hint::black_box(v);
            }
            drop(tx);
            worker.await.expect("worker");
            samples
        }));
    }
    let mut all = Vec::with_capacity(sessions * STATEMENTS);
    for h in handles {
        all.extend(h.await.expect("task"));
    }
    (all, started.elapsed())
}

/// Per-session setup costs, which the redesign trades against: a dedicated
/// thread per session is only affordable if spawning one is cheap relative
/// to the ~245 µs a session already pays to open a WAL handle.
fn report_setup_costs() {
    const N: usize = 2_000;
    let mut spawn_join = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        std::thread::spawn(|| std::hint::black_box(0u8))
            .join()
            .expect("join");
        spawn_join.push(t.elapsed());
    }
    report(
        "std::thread spawn + join",
        spawn_join,
        Duration::from_secs(1),
    );

    // The unit both designs are built out of: park a thread, wake it, get
    // an answer back. Every lane below pays two of these per statement, so
    // if this number is large the whole table is really measuring the
    // host's scheduler and not the two designs. Read every other row
    // against it.
    let (req_tx, req_rx) = mpsc::channel::<mpsc::Sender<u8>>();
    let pong = std::thread::spawn(move || {
        while let Ok(back) = req_rx.recv() {
            let _ = back.send(0);
        }
    });
    let mut rtt = Vec::with_capacity(N);
    for _ in 0..N {
        let (back_tx, back_rx) = mpsc::channel::<u8>();
        let t = Instant::now();
        req_tx.send(back_tx).expect("ping");
        let v = back_rx.recv().expect("pong");
        rtt.push(t.elapsed());
        std::hint::black_box(v);
    }
    drop(req_tx);
    pong.join().expect("pong thread");
    report(
        "cross-thread ping-pong (2 wakes)",
        rtt,
        Duration::from_secs(1),
    );

    let mut chan = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let (tx, rx) = mpsc::channel::<u8>();
        tx.send(0).expect("send");
        let v = rx.recv().expect("recv");
        std::hint::black_box(v);
        chan.push(t.elapsed());
    }
    report(
        "mpsc send + recv, same thread",
        chan,
        Duration::from_secs(1),
    );
}

fn main() {
    let dir = std::env::temp_dir().join(format!("litewire-hop-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("hop.sqlite");
    seed(&path);

    println!();
    println!("session dispatch probe  |  {STATEMENTS} statements/session, multi_thread(8) runtime");
    println!("(the stmt/s column is the aggregate across all concurrent sessions)");
    println!();
    println!("per-session setup costs (synchronous, uncontended):");
    report_setup_costs();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("rt");

    for sessions in [1usize, 2, 4, 8, 16, 32] {
        println!();
        println!("concurrency {sessions}:");
        rt.block_on(async {
            let (s, w) = lane_per_stmt(&path, sessions).await;
            report("A per-stmt spawn_blocking", s, w);
            let (s, w) = lane_actor_thread(&path, sessions).await;
            report("B session actor (std::thread)", s, w);
            let (s, w) = lane_actor_blocking(&path, sessions).await;
            report("C session actor (spawn_blocking)", s, w);
        });
    }

    println!();
    let _ = std::fs::remove_dir_all(&dir);
}
