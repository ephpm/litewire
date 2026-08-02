//! Companion to `handle_reuse_bench`: attributes the cost of a
//! request-shaped cycle to its actual sources, so the reuse numbers can be
//! read against a known backdrop rather than taken on faith.
//!
//! `handle_reuse_bench` measures the whole `BackendConn` path. That path
//! wraps *every* statement in `tokio::task::spawn_blocking`, so a "per
//! query" number there is SQLite plus a thread handoff. These probes strip
//! the layers off one at a time:
//!
//! * `sqlite3_open` + close, synchronous, no litewire
//! * a synchronous point select on an already-open handle (cached stmt)
//! * a bare `spawn_blocking` round trip doing nothing
//! * the hygiene pass the reuse free-list runs on return
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p litewire-backend --example reuse_cost_attribution
//! ```

use std::time::{Duration, Instant};

use rusqlite::Connection;

const N: usize = 2_000;

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort_unstable();
    let us = |d: Duration| d.as_nanos() as f64 / 1000.0;
    println!(
        "{:<44} min {:>8.2}   p50 {:>8.2}   p99 {:>8.2} µs",
        label,
        us(samples[0]),
        us(samples[samples.len() / 2]),
        us(samples[samples.len() * 99 / 100]),
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dir = std::env::temp_dir().join(format!("litewire-attrib-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("attrib.sqlite");

    {
        let c = Connection::open(&path).expect("open");
        c.pragma_update(None, "journal_mode", "WAL").expect("wal");
        c.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)")
            .expect("create");
        for i in 1..=10i64 {
            c.execute("INSERT INTO bench VALUES (?1, ?2)", [i, i * 100])
                .expect("seed");
        }
    }

    println!();
    println!("cost attribution  |  n={N} (same container, same file, single-threaded)");
    println!();

    // 1. Raw sqlite3_open + per-session PRAGMAs + close. This is exactly
    //    what handle reuse removes.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        c.busy_timeout(Duration::from_millis(5000)).expect("busy");
        c.pragma_update(None, "synchronous", "NORMAL")
            .expect("sync");
        drop(c);
        s.push(t.elapsed());
    }
    report("1. open + session PRAGMAs + close (sync)", s);

    // 2. Same, minus the PRAGMAs -- isolates sqlite3_open/close alone.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        drop(c);
        s.push(t.elapsed());
    }
    report("2.   ... of which sqlite3_open + close", s);

    // 2b/2c. Split the per-session PRAGMAs. `busy_timeout` is a direct C
    //    call (`sqlite3_busy_timeout`); `synchronous` is a real prepared
    //    statement. If the gap between probe 1 and probe 2 lands on one of
    //    them, that is a much cheaper thing to fix than pooling handles.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        c.busy_timeout(Duration::from_millis(5000)).expect("busy");
        drop(c);
        s.push(t.elapsed());
    }
    report("2b.  open + busy_timeout + close", s);

    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        c.pragma_update(None, "synchronous", "NORMAL")
            .expect("sync");
        drop(c);
        s.push(t.elapsed());
    }
    report("2c.  open + synchronous=NORMAL + close", s);

    // 2d. Is it the PRAGMA, or is it just "the first statement on a fresh
    //     WAL handle" (opening -shm, recovering the WAL index)? Compare
    //     against a trivial first statement that is not a PRAGMA.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        let v: i64 = c.query_row("SELECT 1", [], |r| r.get(0)).expect("select 1");
        std::hint::black_box(v);
        drop(c);
        s.push(t.elapsed());
    }
    report("2d.  open + `SELECT 1` + close", s);

    // 2e. `SELECT 1` never touches the database file, so it does not prove
    //     much. This one reads a real table, which forces the pager to open
    //     the -shm file and attach to the WAL index. If *this* is where the
    //     ~215 µs lives, the cost is inherent to opening a WAL handle and
    //     keeping the handle alive is the only way to avoid it. If instead
    //     only 2c is slow, the PRAGMA itself is the problem and deleting it
    //     is a far cheaper fix than pooling.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        let v: i64 = c
            .query_row("SELECT count(*) FROM bench", [], |r| r.get(0))
            .expect("count");
        std::hint::black_box(v);
        drop(c);
        s.push(t.elapsed());
    }
    report("2e.  open + read a real table + close", s);

    // 2f. Reordered: do the real read *before* the PRAGMA. If the WAL
    //     attach is the real cost, 2f ~= 2e + epsilon and the PRAGMA is
    //     revealed as merely the statement that happened to pay for it.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let c = Connection::open(&path).expect("open");
        let v: i64 = c
            .query_row("SELECT count(*) FROM bench", [], |r| r.get(0))
            .expect("count");
        std::hint::black_box(v);
        c.pragma_update(None, "synchronous", "NORMAL")
            .expect("sync");
        drop(c);
        s.push(t.elapsed());
    }
    report("2f.  open + real read + synchronous + close", s);

    // 2g. How much of that ~220 µs is the container's filesystem rather
    //     than SQLite? Repeat the decisive probe on tmpfs. The gap is the
    //     environmental inflation factor these numbers carry.
    if std::path::Path::new("/dev/shm").is_dir() {
        let shm_path =
            std::path::Path::new("/dev/shm").join(format!("attrib-{}", std::process::id()));
        {
            let c = Connection::open(&shm_path).expect("open shm");
            c.pragma_update(None, "journal_mode", "WAL").expect("wal");
            c.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)")
                .expect("create");
            for i in 1..=10i64 {
                c.execute("INSERT INTO bench VALUES (?1, ?2)", [i, i * 100])
                    .expect("seed");
            }
        }
        let mut s = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            let c = Connection::open(&shm_path).expect("open");
            let v: i64 = c
                .query_row("SELECT count(*) FROM bench", [], |r| r.get(0))
                .expect("count");
            std::hint::black_box(v);
            drop(c);
            s.push(t.elapsed());
        }
        report("2g.  same as 2e but on tmpfs (/dev/shm)", s);
        for suffix in ["", "-wal", "-shm"] {
            let mut p = shm_path.clone().into_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(&p);
        }
    }

    // 3. A point select on an already-open handle with a warm statement
    //    cache, no async at all. The irreducible SQLite cost of one query.
    let conn = Connection::open(&path).expect("open");
    let mut s = Vec::with_capacity(N);
    for i in 0..N {
        let t = Instant::now();
        let id = (i % 10) as i64 + 1;
        let v: i64 = conn
            .prepare_cached("SELECT id, val FROM bench WHERE id = ?1")
            .expect("prepare")
            .query_row([id], |r| r.get(1))
            .expect("row");
        std::hint::black_box(v);
        s.push(t.elapsed());
    }
    report("3. point select, sync, warm stmt cache", s);

    // 4. A bare spawn_blocking round trip that does no work. litewire pays
    //    one of these per statement.
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        tokio::task::spawn_blocking(|| std::hint::black_box(0u8))
            .await
            .expect("join");
        s.push(t.elapsed());
    }
    report("4. bare spawn_blocking rt, current_thread", s);

    // 4b. Same on a multi-threaded runtime -- which is what ePHPm actually
    //     runs -- in case the handoff cost is an artifact of the
    //     single-threaded scheduler used by this bench.
    let mt = std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let mut s = Vec::with_capacity(N);
            for _ in 0..N {
                let t = Instant::now();
                tokio::task::spawn_blocking(|| std::hint::black_box(0u8))
                    .await
                    .expect("join");
                s.push(t.elapsed());
            }
            s
        })
    })
    .join()
    .expect("mt thread");
    report("4b. bare spawn_blocking rt, multi_thread(4)", mt);

    // 5. The reuse hygiene pass: autocommit check + fingerprint probe +
    //    busy_timeout re-apply, on a clean handle with the probe cached.
    let fp = "SELECT 1024 * ( \
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
    let mut s = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let dirty = if conn.is_autocommit() {
            let v: i64 = conn
                .prepare_cached(fp)
                .expect("prepare fp")
                .query_row([], |r| r.get(0))
                .expect("fp row");
            v
        } else {
            -1
        };
        std::hint::black_box(dirty);
        conn.busy_timeout(Duration::from_millis(5000))
            .expect("busy");
        s.push(t.elapsed());
    }
    report("5. reuse hygiene pass on a clean handle", s);

    println!();
    let _ = std::fs::remove_dir_all(&dir);
}
