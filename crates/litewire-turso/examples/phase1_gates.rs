//! Phase 1 gate-evidence harness for the experimental Turso backend.
//!
//! Produces the evidence for the ePHPm Turso-engine roadmap decision gates
//! 2-4, comparing the Turso engine against the rusqlite (genuine SQLite C
//! engine) backend through the exact same `Backend`/`BackendConn` seam the
//! wire frontends use.
//!
//! ```text
//! cargo run --release -p litewire-turso --example phase1_gates -- bench <dir>
//! cargo run --release -p litewire-turso --example phase1_gates -- writers <dir> <n_conns>
//! cargo run --release -p litewire-turso --example phase1_gates -- gate3 <dir>
//! cargo run --release -p litewire-turso --example phase1_gates -- crash <dir> <iterations>
//! ```
//!
//! `crash` re-executes this binary with the internal `crash-writer`
//! subcommand as a child process, SIGKILLs it mid-write-loop, then reopens
//! and integrity-checks the database with both engines.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use litewire_backend::rusqlite_backend::Rusqlite;
use litewire_backend::{Backend, BackendConn, Value};
use litewire_turso::Turso;

fn usage() -> ! {
    eprintln!(
        "usage: phase1_gates <bench <dir> | writers <dir> <n_conns> | gate3 <dir> | crash <dir> <iters>>"
    );
    std::process::exit(2);
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("bench") => bench(args.get(2).unwrap_or_else(|| usage())).await,
        Some("writers") => {
            let dir = args.get(2).unwrap_or_else(|| usage());
            let n: usize = args
                .get(3)
                .unwrap_or_else(|| usage())
                .parse()
                .expect("n_conns");
            writers(dir, n).await;
        }
        Some("gate3") => gate3(args.get(2).unwrap_or_else(|| usage())).await,
        Some("crash") => {
            let dir = args.get(2).unwrap_or_else(|| usage());
            let iters: usize = args
                .get(3)
                .unwrap_or_else(|| usage())
                .parse()
                .expect("iters");
            crash(dir, iters).await;
        }
        Some("crash-writer") => crash_writer(args.get(2).unwrap_or_else(|| usage())).await,
        _ => usage(),
    }
}

async fn open_engine(engine: &str, path: &str) -> Arc<dyn Backend> {
    match engine {
        "rusqlite" => Arc::new(Rusqlite::open(path).expect("open rusqlite")),
        "turso" => Arc::new(Turso::open(path).await.expect("open turso")),
        _ => unreachable!(),
    }
}

fn pctl(sorted_ns: &[u128], p: f64) -> f64 {
    let idx = ((sorted_ns.len() as f64) * p).ceil() as usize;
    sorted_ns[idx.saturating_sub(1).min(sorted_ns.len() - 1)] as f64 / 1000.0
}

fn report(label: &str, engine: &str, mut samples_ns: Vec<u128>) {
    samples_ns.sort_unstable();
    println!(
        "| {label} | {engine} | {:.1} | {:.1} | {:.1} | n={} |",
        pctl(&samples_ns, 0.50),
        pctl(&samples_ns, 0.95),
        pctl(&samples_ns, 0.99),
        samples_ns.len()
    );
}

// ── Gate 2: latency matrix ──────────────────────────────────────────────────

async fn bench(dir: &str) {
    println!("| op | engine | p50 us | p95 us | p99 us | samples |");
    println!("|----|--------|--------|--------|--------|---------|");
    for engine in ["rusqlite", "turso"] {
        let path = format!("{dir}/bench-{engine}.db");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
        let be = open_engine(engine, &path).await;

        be.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();
        let seed_conn = be.connect().await.unwrap();
        seed_conn.execute("BEGIN", &[]).await.unwrap();
        for i in 0..1000 {
            seed_conn
                .execute(
                    "INSERT INTO bench VALUES (?1, ?2)",
                    &[
                        Value::Integer(i),
                        Value::Text(format!("row-{i}-{}", "x".repeat(64))),
                    ],
                )
                .await
                .unwrap();
        }
        seed_conn.execute("COMMIT", &[]).await.unwrap();

        let conn = be.connect().await.unwrap();

        // Warmup.
        for i in 0..500 {
            conn.query(
                "SELECT v FROM bench WHERE id=?1",
                &[Value::Integer(i % 1000)],
            )
            .await
            .unwrap();
        }

        // Point SELECT.
        let mut samples = Vec::with_capacity(5000);
        for i in 0..5000i64 {
            let t = Instant::now();
            let rs = conn
                .query(
                    "SELECT v FROM bench WHERE id=?1",
                    &[Value::Integer(i % 1000)],
                )
                .await
                .unwrap();
            samples.push(t.elapsed().as_nanos());
            assert_eq!(rs.rows.len(), 1);
        }
        report("point SELECT", engine, samples);

        // Autocommit INSERT.
        be.execute(
            "CREATE TABLE bench_ins (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
            &[],
        )
        .await
        .unwrap();
        let mut samples = Vec::with_capacity(2000);
        for i in 0..2000 {
            let t = Instant::now();
            conn.execute(
                "INSERT INTO bench_ins (v) VALUES (?1)",
                &[Value::Text(format!("ins-{i}-{}", "y".repeat(64)))],
            )
            .await
            .unwrap();
            samples.push(t.elapsed().as_nanos());
        }
        report("INSERT (autocommit)", engine, samples);

        // 10-query page (the ephpm db.php fixture pattern).
        let mut samples = Vec::with_capacity(500);
        for i in 0..500i64 {
            let t = Instant::now();
            for j in 0..10 {
                let rs = conn
                    .query(
                        "SELECT v FROM bench WHERE id=?1",
                        &[Value::Integer((i * 10 + j) % 1000)],
                    )
                    .await
                    .unwrap();
                assert_eq!(rs.rows.len(), 1);
            }
            samples.push(t.elapsed().as_nanos());
        }
        report("10-query page", engine, samples);
    }
}

// ── Gate 2: concurrent writers ──────────────────────────────────────────────

async fn writers(dir: &str, n_conns: usize) {
    const INSERTS_PER_CONN: usize = 250;
    println!(
        "| engine | conns | total inserts | wall s | inserts/s | busy errors | other errors |"
    );
    println!(
        "|--------|-------|---------------|--------|-----------|-------------|--------------|"
    );
    for engine in ["rusqlite", "turso"] {
        let path = format!("{dir}/writers-{engine}.db");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
        let be = open_engine(engine, &path).await;
        be.execute(
            "CREATE TABLE w (id INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT, payload TEXT)",
            &[],
        )
        .await
        .unwrap();

        let payload = "z".repeat(200);
        let start = Instant::now();
        let mut handles = Vec::new();
        for c in 0..n_conns {
            let be = Arc::clone(&be);
            let payload = payload.clone();
            handles.push(tokio::spawn(async move {
                let conn: Box<dyn BackendConn> = be.connect().await.unwrap();
                let mut busy = 0u64;
                let mut other = 0u64;
                for i in 0..INSERTS_PER_CONN {
                    let r = conn
                        .execute(
                            "INSERT INTO w (tag, payload) VALUES (?1, ?2)",
                            &[
                                Value::Text(format!("c{c}-i{i}")),
                                Value::Text(payload.clone()),
                            ],
                        )
                        .await;
                    if let Err(e) = r {
                        let msg = e.to_string().to_ascii_lowercase();
                        if msg.contains("busy") || msg.contains("locked") {
                            busy += 1;
                        } else {
                            other += 1;
                            eprintln!("[{engine}] writer error: {e}");
                        }
                    }
                }
                (busy, other)
            }));
        }
        let mut busy_total = 0u64;
        let mut other_total = 0u64;
        for h in handles {
            let (b, o) = h.await.unwrap();
            busy_total += b;
            other_total += o;
        }
        let wall = start.elapsed();

        let rs = be.query("SELECT COUNT(*) FROM w", &[]).await.unwrap();
        let rows = match rs.rows[0][0] {
            Value::Integer(n) => n,
            _ => panic!("count"),
        };
        let expected = (n_conns * INSERTS_PER_CONN) as i64;
        assert_eq!(
            rows + (busy_total + other_total) as i64,
            expected,
            "row count + failures must equal attempts"
        );
        println!(
            "| {engine} | {n_conns} | {rows} | {:.2} | {:.0} | {busy_total} | {other_total} |",
            wall.as_secs_f64(),
            rows as f64 / wall.as_secs_f64()
        );
    }
}

// ── Gate 3: file-format round-trip ──────────────────────────────────────────

async fn checksum(conn: &dyn BackendConn, sql: &str) -> u64 {
    let rs = conn.query(sql, &[]).await.unwrap();
    let mut h = DefaultHasher::new();
    for row in &rs.rows {
        for v in row {
            match v {
                Value::Null => "NULL".hash(&mut h),
                Value::Integer(i) => i.hash(&mut h),
                // Hash the bit pattern; both engines must return the same f64.
                Value::Float(f) => f.to_bits().hash(&mut h),
                Value::Text(s) => s.hash(&mut h),
                Value::Blob(b) => b.hash(&mut h),
            }
        }
    }
    h.finish()
}

async fn integrity_check(engine: &str, path: &str) -> String {
    let be = open_engine(engine, path).await;
    match be.query("PRAGMA integrity_check", &[]).await {
        Ok(rs) => rs
            .rows
            .iter()
            .flat_map(|r| r.iter().map(std::string::ToString::to_string))
            .collect::<Vec<_>>()
            .join(","),
        Err(e) => format!("UNSUPPORTED/FAILED: {e}"),
    }
}

const CHECKSUM_SQL: &str = "SELECT i, r, t, b FROM typed WHERE id <= 500 ORDER BY id";

async fn seed_typed(conn: &dyn BackendConn) {
    conn.execute(
        "CREATE TABLE typed (
            id INTEGER PRIMARY KEY,
            i INTEGER, r REAL, t TEXT, b BLOB
        )",
        &[],
    )
    .await
    .unwrap();
    conn.execute("CREATE INDEX idx_typed_i ON typed(i)", &[])
        .await
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_typed_t ON typed(t)", &[])
        .await
        .unwrap();
    conn.execute("BEGIN", &[]).await.unwrap();
    for id in 1..=500i64 {
        let (i, r, t, b) = varied_row(id);
        conn.execute(
            "INSERT INTO typed VALUES (?1, ?2, ?3, ?4, ?5)",
            &[Value::Integer(id), i, r, t, b],
        )
        .await
        .unwrap();
    }
    conn.execute("COMMIT", &[]).await.unwrap();
}

fn varied_row(id: i64) -> (Value, Value, Value, Value) {
    let i = match id % 5 {
        0 => Value::Null,
        1 => Value::Integer(i64::MAX - id),
        2 => Value::Integer(i64::MIN + id),
        3 => Value::Integer(0),
        _ => Value::Integer(-id * 7919),
    };
    let r = match id % 4 {
        0 => Value::Float(0.1 + id as f64),
        1 => Value::Float(-1e308 / id as f64),
        2 => Value::Float(id as f64 * 1e-10),
        _ => Value::Null,
    };
    let t = Value::Text(format!(
        "row-{id}-\u{00e9}\u{4e2d}\u{6587}-{}",
        "t".repeat((id % 40) as usize)
    ));
    let b = if id % 3 == 0 {
        Value::Null
    } else {
        Value::Blob((0..(id % 64) as u8).collect())
    };
    (i, r, t, b)
}

async fn gate3(dir: &str) {
    for (writer, reader) in [("rusqlite", "turso"), ("turso", "rusqlite")] {
        let path = format!("{dir}/roundtrip-{writer}-to-{reader}.db");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
        println!("=== {writer} writes -> {reader} reads/writes -> {writer} reopens ===");

        // 1. Writer engine creates + seeds the database (WAL mode).
        let sum_original;
        {
            let be = open_engine(writer, &path).await;
            let conn = be.connect().await.unwrap();
            seed_typed(conn.as_ref()).await;
            sum_original = checksum(conn.as_ref(), CHECKSUM_SQL).await;
            println!("{writer} wrote 500 rows, checksum={sum_original:#018x}");
        }

        // 2. Reader engine opens the same file: read, verify, write.
        {
            let be = open_engine(reader, &path).await;
            let conn = be.connect().await.unwrap();
            let sum_read = checksum(conn.as_ref(), CHECKSUM_SQL).await;
            println!(
                "{reader} read checksum={sum_read:#018x} -> {}",
                if sum_read == sum_original {
                    "MATCH"
                } else {
                    "MISMATCH"
                }
            );
            for id in 501..=600i64 {
                let (i, r, t, b) = varied_row(id);
                conn.execute(
                    "INSERT INTO typed VALUES (?1, ?2, ?3, ?4, ?5)",
                    &[Value::Integer(id), i, r, t, b],
                )
                .await
                .unwrap();
            }
            conn.execute(
                "UPDATE typed SET i = i + 1 WHERE id % 5 = 4 AND id <= 500",
                &[],
            )
            .await
            .unwrap();
            conn.execute(
                "UPDATE typed SET i = i - 1 WHERE id % 5 = 4 AND id <= 500",
                &[],
            )
            .await
            .unwrap();
            println!("{reader} inserted rows 501..=600 and ran balanced UPDATEs");
        }

        // 3. Writer engine reopens: integrity + checksum + new-row visibility.
        {
            let be = open_engine(writer, &path).await;
            let conn = be.connect().await.unwrap();
            let sum_back = checksum(conn.as_ref(), CHECKSUM_SQL).await;
            let rs = conn.query("SELECT COUNT(*) FROM typed", &[]).await.unwrap();
            println!(
                "{writer} reopened: checksum {} (rows={}, expect 600)",
                if sum_back == sum_original {
                    "MATCH"
                } else {
                    "MISMATCH"
                },
                rs.rows[0][0]
            );
        }
        println!(
            "integrity_check (rusqlite): {}",
            integrity_check("rusqlite", &path).await
        );
        println!(
            "integrity_check (turso):    {}",
            integrity_check("turso", &path).await
        );
        println!();
    }
}

// ── Gate 4: crash smoke ─────────────────────────────────────────────────────

/// Child-process mode: open the DB with the Turso engine and insert in a
/// tight loop until killed. Prints `READY` once the schema exists so the
/// parent knows writes have started.
async fn crash_writer(path: &str) {
    let be = Turso::open(path).await.expect("open turso");
    let conn = be.connect().await.expect("connect");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS crash (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        &[],
    )
    .await
    .expect("create");
    println!("READY");
    let mut i = 0u64;
    loop {
        conn.execute(
            "INSERT INTO crash (v) VALUES (?1)",
            &[Value::Text(format!("crash-{i}-{}", "k".repeat(128)))],
        )
        .await
        .expect("insert");
        i += 1;
    }
}

async fn crash(dir: &str, iterations: usize) {
    let exe = std::env::current_exe().expect("current_exe");
    let path = format!("{dir}/crash.db");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path}-wal"));
    let _ = std::fs::remove_file(format!("{path}-shm"));

    let mut prev_rows = 0i64;
    for iter in 1..=iterations {
        let mut child = std::process::Command::new(&exe)
            .args(["crash-writer", &path])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn crash-writer");

        // Wait for READY, then let it write for a varied interval before
        // the SIGKILL lands mid-write-loop.
        {
            use std::io::{BufRead, BufReader};
            let stdout = child.stdout.take().expect("child stdout");
            let mut line = String::new();
            BufReader::new(stdout)
                .read_line(&mut line)
                .expect("read READY");
            assert_eq!(line.trim(), "READY");
        }
        std::thread::sleep(Duration::from_millis(300 + (iter as u64 * 137) % 900));
        child.kill().expect("SIGKILL child");
        let _ = child.wait();

        // Reopen with the Turso engine: count + write must work.
        let (rows, reopen_write) = {
            let be = open_engine("turso", &path).await;
            let conn = be.connect().await.unwrap();
            let rs = conn.query("SELECT COUNT(*) FROM crash", &[]).await.unwrap();
            let rows = match rs.rows[0][0] {
                Value::Integer(n) => n,
                _ => panic!("count"),
            };
            let w = conn
                .execute("INSERT INTO crash (v) VALUES ('post-crash-probe')", &[])
                .await;
            let ok = w.is_ok();
            if ok {
                conn.execute("DELETE FROM crash WHERE v = 'post-crash-probe'", &[])
                    .await
                    .unwrap();
            }
            (rows, ok)
        };

        // Cross-check the on-disk state with the genuine SQLite C engine.
        let integrity = integrity_check("rusqlite", &path).await;

        println!(
            "iter {iter:>2}: rows={rows} (prev {prev_rows}, monotonic={}) turso-reopen-write={} sqlite-integrity={integrity}",
            rows >= prev_rows,
            if reopen_write { "ok" } else { "FAILED" },
        );
        prev_rows = rows;
    }
}
