//! Before/after benchmark for the two rusqlite backend changes: the
//! per-session worker thread that replaced the per-statement
//! `spawn_blocking` handoff, and the opt-in handle-reuse free-list
//! (`RusqliteBuilder::handle_reuse`).
//!
//! # What is being measured
//!
//! The unit is a **PHP-request-shaped cycle**, not a query:
//!
//! ```text
//! { let conn = backend.connect().await?;   // PDO connect
//!   for i in 1..=n { conn.query(point_select_i) }
//!   drop(conn); }                          // request ends, PDO closes
//! ```
//!
//! That is the shape ePHPm's single-node SQLite mode actually produces: a
//! PHP request opens one PDO connection to litewire's MySQL frontend, runs
//! a handful of statements, and the connection dies with the request.
//!
//! # Lanes
//!
//! Each lane is `(reuse, queries per cycle, concurrent sessions)`.
//! Concurrency matters as much as the other two: an idle runtime flatters
//! the per-statement `spawn_blocking` design, because its shared blocking
//! pool can hand work to a thread that is already awake. That advantage
//! inverts once the pool is contended, so any single-concurrency reading
//! of this benchmark is a half-truth.
//!
//! | Lane | Reuse | Queries/cycle | Sessions | Question it answers |
//! |------|-------|---------------|----------|---------------------|
//! | A    | off | 10 |  1 | Baseline -- mirrors `db.php` on an idle server. |
//! | B    | on  | 10 |  1 | The headline before/after. |
//! | A16  | off | 10 | 16 | **The default path under load.** Compared against the same lane on `main`, this isolates the worker-thread redesign with no reuse involved. |
//! | B16  | on  | 10 | 16 | Both changes together under load. |
//! | C1   | off |  1 |  1 | With C30, splits fixed connect cost from per-query marginal cost. |
//! | C30  | off | 30 |  1 | " |
//! | D    | on  |  1 |  1 | What reuse saves on the connect component alone. |
//! | B30  | on  | 30 |  1 | Confirms reuse does not change the marginal per-query cost. |
//!
//! Reported latencies are **per cycle**, so a concurrency-16 lane's p50 is
//! the latency one of sixteen concurrent clients sees, not a throughput
//! figure; the `cycles/s` column carries throughput.
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p litewire-backend --example handle_reuse_bench
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use litewire_backend::rusqlite_backend::Rusqlite;
use litewire_backend::{Backend, Value};

/// Cycles discarded before measurement starts, to let the page cache, the
/// WAL, the allocator and (in reuse lanes) the free-list reach steady state.
const WARMUP: usize = 200;
/// Measured cycles per lane, summed across concurrent sessions.
const MEASURED: usize = 2_000;
/// Rows in the bench table; point selects hit ids 1..=10.
const ROWS: i64 = 10;
/// Free-list bound used by the reuse lanes. Sized above the highest lane
/// concurrency so a loaded lane is not measuring pool starvation.
const MAX_IDLE: usize = 32;

/// One lane's latency distribution, in microseconds.
struct Stats {
    label: &'static str,
    reuse: bool,
    queries: usize,
    sessions: usize,
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    mean: f64,
    per_sec: f64,
    /// `Some((hits, misses, discarded))` for reuse lanes. Proves the lane
    /// actually exercised the free-list rather than silently falling back
    /// to fresh opens.
    pool: Option<(u64, u64, u64)>,
}

impl Stats {
    fn from(
        lane: &Lane,
        mut samples: Vec<Duration>,
        wall: Duration,
        pool: Option<(u64, u64, u64)>,
    ) -> Self {
        samples.sort_unstable();
        let us = |d: Duration| d.as_nanos() as f64 / 1000.0;
        let pct = |p: f64| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let idx = ((samples.len() - 1) as f64 * p) as usize;
            us(samples[idx])
        };
        let total: f64 = samples.iter().map(|d| us(*d)).sum();
        #[allow(clippy::cast_precision_loss)]
        let mean = total / samples.len() as f64;
        #[allow(clippy::cast_precision_loss)]
        let per_sec = samples.len() as f64 / wall.as_secs_f64();
        Self {
            label: lane.label,
            reuse: lane.reuse,
            queries: lane.queries,
            sessions: lane.sessions,
            min: us(samples[0]),
            p50: pct(0.50),
            p95: pct(0.95),
            p99: pct(0.99),
            mean,
            per_sec,
            pool,
        }
    }
}

struct Lane {
    label: &'static str,
    reuse: bool,
    queries: usize,
    sessions: usize,
}

/// Run one request-shaped cycle: connect, N point selects, disconnect.
async fn cycle(backend: &Rusqlite, queries: usize) {
    let conn = backend.connect().await.expect("connect");
    for q in 0..queries {
        #[allow(clippy::cast_possible_wrap)]
        let id = (q as i64 % ROWS) + 1;
        let rs = conn
            .query(
                "SELECT id, val FROM bench WHERE id = ?1",
                &[Value::Integer(id)],
            )
            .await
            .expect("point select");
        debug_assert_eq!(rs.rows.len(), 1);
    }
    drop(conn);
}

/// Run `WARMUP + MEASURED` cycles spread across `sessions` concurrent
/// tasks, returning per-cycle durations for the measured portion and the
/// wall time the measured portion took.
async fn run_lane(backend: &Arc<Rusqlite>, lane: &Lane) -> (Vec<Duration>, Duration) {
    let warmup_each = WARMUP / lane.sessions;
    let measured_each = MEASURED / lane.sessions;

    // Warm up every session lane before the clock starts, so the timed
    // window contains no pool-priming or thread-pool growth.
    let mut warm = Vec::with_capacity(lane.sessions);
    for _ in 0..lane.sessions {
        let backend = Arc::clone(backend);
        let queries = lane.queries;
        warm.push(tokio::spawn(async move {
            for _ in 0..warmup_each {
                cycle(&backend, queries).await;
            }
        }));
    }
    for w in warm {
        w.await.expect("warmup task");
    }

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(lane.sessions);
    for _ in 0..lane.sessions {
        let backend = Arc::clone(backend);
        let queries = lane.queries;
        tasks.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(measured_each);
            for _ in 0..measured_each {
                let start = Instant::now();
                cycle(&backend, queries).await;
                samples.push(start.elapsed());
            }
            samples
        }));
    }
    let mut all = Vec::with_capacity(MEASURED);
    for t in tasks {
        all.extend(t.await.expect("lane task"));
    }
    (all, started.elapsed())
}

/// Build a file-backed bench database seeded with [`ROWS`] rows.
///
/// A file, not `Rusqlite::memory()` -- the whole point is to measure real
/// `sqlite3_open` cost against a real file, which is what ePHPm does.
fn make_backend(dir: &std::path::Path, tag: &str, reuse: bool) -> Rusqlite {
    let path = dir.join(format!("bench-{tag}.sqlite"));
    let _ = std::fs::remove_file(&path);

    let mut builder = Rusqlite::builder(&path);
    if reuse {
        builder = builder.handle_reuse(MAX_IDLE);
    }
    let backend = builder.build().expect("build backend");

    // Seed synchronously through a single throwaway session.
    let seed = rusqlite::Connection::open(&path).expect("open for seed");
    seed.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("create table");
    for i in 1..=ROWS {
        seed.execute("INSERT INTO bench VALUES (?1, ?2)", [i, i * 100])
            .expect("seed row");
    }
    drop(seed);

    backend
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let dir = std::env::temp_dir().join(format!("litewire-reuse-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");

    let lanes = [
        Lane {
            label: "A   baseline",
            reuse: false,
            queries: 10,
            sessions: 1,
        },
        Lane {
            label: "B   reuse",
            reuse: true,
            queries: 10,
            sessions: 1,
        },
        Lane {
            label: "A16 baseline",
            reuse: false,
            queries: 10,
            sessions: 16,
        },
        Lane {
            label: "B16 reuse",
            reuse: true,
            queries: 10,
            sessions: 16,
        },
        Lane {
            label: "C1  baseline",
            reuse: false,
            queries: 1,
            sessions: 1,
        },
        Lane {
            label: "C30 baseline",
            reuse: false,
            queries: 30,
            sessions: 1,
        },
        Lane {
            label: "D   reuse",
            reuse: true,
            queries: 1,
            sessions: 1,
        },
        Lane {
            label: "B30 reuse",
            reuse: true,
            queries: 30,
            sessions: 1,
        },
    ];

    let mut results = Vec::new();
    for lane in &lanes {
        let backend = Arc::new(make_backend(
            &dir,
            lane.label.split_whitespace().next().unwrap(),
            lane.reuse,
        ));
        let (samples, wall) = run_lane(&backend, lane).await;
        let pool = backend
            .reuse_stats()
            .map(|s| (s.hits, s.misses, s.discarded));
        results.push(Stats::from(lane, samples, wall, pool));
    }

    println!();
    println!(
        "handle-reuse bench  |  warmup={WARMUP} measured={MEASURED} rows={ROWS} max_idle={MAX_IDLE}"
    );
    println!("cycle = connect + N point selects + drop   (multi_thread(8) runtime)");
    println!();
    println!(
        "{:<14} {:>5} {:>4} {:>5} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10}   pool h/m/d",
        "lane",
        "reuse",
        "q/c",
        "sess",
        "min µs",
        "p50 µs",
        "p95 µs",
        "p99 µs",
        "mean µs",
        "cycles/s"
    );
    println!("{}", "-".repeat(130));
    for r in &results {
        let pool = r
            .pool
            .map_or_else(|| "-".to_string(), |(h, m, d)| format!("{h}/{m}/{d}"));
        println!(
            "{:<14} {:>5} {:>4} {:>5} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>10.0}   {}",
            r.label,
            if r.reuse { "on" } else { "off" },
            r.queries,
            r.sessions,
            r.min,
            r.p50,
            r.p95,
            r.p99,
            r.mean,
            r.per_sec,
            pool
        );
    }
    println!();

    // --- derivations -----------------------------------------------------
    let find = |label: &str| {
        results
            .iter()
            .find(|r| r.label.starts_with(label))
            .expect("lane present")
    };
    let (a, b, a16, b16, c1, c30, d) = (
        find("A "),
        find("B "),
        find("A16"),
        find("B16"),
        find("C1"),
        find("C30"),
        find("D"),
    );

    let save_p50 = a.p50 - b.p50;
    let save_mean = a.mean - b.mean;
    println!("(a) reuse saving at 10 queries/cycle, 1 session");
    println!(
        "      p50 : {:.2} -> {:.2} µs   = {:.2} µs saved ({:.1}%)",
        a.p50,
        b.p50,
        save_p50,
        100.0 * save_p50 / a.p50
    );
    println!(
        "      mean: {:.2} -> {:.2} µs   = {:.2} µs saved ({:.1}%)",
        a.mean,
        b.mean,
        save_mean,
        100.0 * save_mean / a.mean
    );
    println!();

    println!("(a16) same at 16 concurrent sessions");
    println!(
        "      p50 : {:.2} -> {:.2} µs   = {:.2} µs saved ({:.1}%)",
        a16.p50,
        b16.p50,
        a16.p50 - b16.p50,
        100.0 * (a16.p50 - b16.p50) / a16.p50
    );
    println!(
        "      throughput: {:.0} -> {:.0} cycles/s  ({:+.1}%)",
        a16.per_sec,
        b16.per_sec,
        100.0 * (b16.per_sec - a16.per_sec) / a16.per_sec
    );
    println!();

    // Two-point line through (1, C1) and (30, C30):
    //   marginal = (C30 - C1) / 29 ;  fixed = C1 - marginal
    #[allow(clippy::cast_precision_loss)]
    let marginal = (c30.p50 - c1.p50) / (c30.queries - c1.queries) as f64;
    let fixed = c1.p50 - marginal;
    println!("(b) connect-vs-query split (lane C, p50, 1-vs-30-query line)");
    println!("      fixed cost per cycle (connect + drop) : {fixed:.2} µs");
    println!("      marginal cost per point select        : {marginal:.2} µs");
    println!(
        "      => baseline 10q cycle predicted {:.2} µs, measured {:.2} µs",
        fixed + 10.0 * marginal,
        a.p50
    );
    println!(
        "      connect share of the baseline 10q cycle: {:.1}%",
        100.0 * fixed / a.p50
    );
    println!();

    println!("(c) what reuse does to the fixed component alone (lane C1 vs D, p50)");
    println!(
        "      1-query cycle: {:.2} -> {:.2} µs = {:.2} µs saved",
        c1.p50,
        d.p50,
        c1.p50 - d.p50
    );
    println!();

    let _ = std::fs::remove_dir_all(&dir);
}
