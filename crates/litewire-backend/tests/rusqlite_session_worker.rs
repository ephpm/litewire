//! Integration tests for the rusqlite backend's per-session worker thread.
//!
//! The unit tests beside the implementation cover the handle-reuse pool's
//! internals, which need access to private state. These cover the parts a
//! consumer of the crate can see, through the public API only: that a
//! statement's result -- including its *error* -- crosses the channel
//! intact, that sessions really are independent when they run on separate
//! worker threads, that statements queued on one session stay ordered, that
//! a session's thread is reclaimed when the session goes away, and that
//! `busy_timeout` still bounds a lock wait now that the wait happens on a
//! worker thread rather than on a blocking-pool task.
//!
//! Together they are the regression net for the change that moved every
//! SQLite call off `tokio::task::spawn_blocking` and onto a thread owned by
//! the session.

use std::sync::Arc;
use std::time::{Duration, Instant};

use litewire_backend::rusqlite_backend::Rusqlite;
use litewire_backend::{Backend, BackendError, Value};

/// A statement slow enough to still be running while the test does
/// something else, without being slow enough to make the suite drag.
const SLOW_COUNT: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 400000) \
     SELECT count(*) FROM c";

/// Number of live threads in this process. Linux-only; used to prove a
/// session's worker thread is actually reclaimed rather than leaked.
#[cfg(target_os = "linux")]
fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("read /proc/self/task")
        .count()
}

/// Poll `cond` until it holds or the deadline passes.
#[cfg(target_os = "linux")]
async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A SQLite error raised inside the worker must reach the caller as a
/// `BackendError::Sqlite` carrying SQLite's own message, and must not take
/// the session with it.
///
/// Would catch: a worker that turns every failure into a generic "worker
/// gone" error, or one that unwinds and kills the session on the first bad
/// statement -- either of which would surface to a wire client as a dropped
/// connection instead of an error packet.
#[tokio::test]
async fn statement_errors_cross_the_channel_and_the_session_survives() {
    let backend = Rusqlite::memory().unwrap();
    let conn = backend.connect().await.unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
        &[],
    )
    .await
    .unwrap();

    // 1. Missing table, on the query path.
    let err = conn
        .query("SELECT * FROM does_not_exist", &[])
        .await
        .expect_err("missing table should be an error");
    assert!(
        matches!(&err, BackendError::Sqlite(m) if m.contains("no such table")),
        "lost SQLite's own message: {err}"
    );

    // 2. Syntax error, on the execute path.
    let err = conn
        .execute("INSERT INTO VALUES ()", &[])
        .await
        .expect_err("syntax error should be an error");
    assert!(
        matches!(err, BackendError::Sqlite(_)),
        "wrong variant: {err}"
    );

    // 3. Constraint violation -- a runtime failure rather than a prepare
    //    failure, so it comes back from a different point in the worker.
    conn.execute("INSERT INTO t VALUES (1, 'a')", &[])
        .await
        .unwrap();
    let err = conn
        .execute("INSERT INTO t VALUES (1, 'b')", &[])
        .await
        .expect_err("duplicate primary key should be an error");
    assert!(
        matches!(&err, BackendError::Sqlite(m) if m.to_lowercase().contains("unique")),
        "lost the constraint message: {err}"
    );

    // 4. A NOT NULL violation via a bound parameter.
    let err = conn
        .execute(
            "INSERT INTO t VALUES (?1, ?2)",
            &[Value::Integer(2), Value::Null],
        )
        .await
        .expect_err("NOT NULL violation should be an error");
    assert!(
        matches!(err, BackendError::Sqlite(_)),
        "wrong variant: {err}"
    );

    // 5. After all of that the session is still fully usable.
    conn.execute("INSERT INTO t VALUES (3, 'c')", &[])
        .await
        .unwrap();
    let rs = conn.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(2));
}

/// Two statements issued on one session must run in the order they were
/// issued, even when the first is far slower than the second.
///
/// Would catch: any future change that dispatches a session's statements to
/// more than one thread, or that lets a later statement overtake an earlier
/// one. A wire client's statements are ordered by definition, and
/// reordering `INSERT` then `SELECT` would silently corrupt results.
#[tokio::test]
async fn statements_on_one_session_stay_ordered() {
    let backend = Rusqlite::memory().unwrap();
    let conn = backend.connect().await.unwrap();
    conn.execute("CREATE TABLE log (n INTEGER)", &[])
        .await
        .unwrap();

    // The first statement takes milliseconds; the second is immediate. If
    // they were not serialized in arrival order the fast one would land
    // first.
    let slow = format!("INSERT INTO log SELECT 1 FROM ({SLOW_COUNT})");
    let (first, second) = tokio::join!(
        conn.execute(&slow, &[]),
        conn.execute("INSERT INTO log VALUES (2)", &[])
    );
    first.unwrap();
    second.unwrap();

    let rs = conn
        .query("SELECT n FROM log ORDER BY rowid", &[])
        .await
        .unwrap();
    assert_eq!(
        rs.rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
        vec![Value::Integer(1), Value::Integer(2)],
        "statements were reordered on a single session"
    );
}

/// Dropping a session -- including one dropped while a statement is still
/// running -- must retire its worker thread.
///
/// Would catch: a worker loop that can never observe its channel
/// disconnecting (the classic failure mode of an actor that holds a
/// `Sender` to its own inbox), which would leak one OS thread and one open
/// SQLite handle per wire connection for the life of the process.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn session_threads_are_reclaimed_including_mid_statement() {
    let backend = Arc::new(Rusqlite::memory().unwrap());
    // Touch the backend once so any lazily-created runtime threads exist
    // before the baseline is taken.
    backend.query("SELECT 1", &[]).await.unwrap();
    eventually("startup threads to settle", {
        let mut last = 0;
        let mut stable = 0;
        move || {
            let now = thread_count();
            if now == last {
                stable += 1;
            } else {
                stable = 0;
                last = now;
            }
            stable >= 3
        }
    })
    .await;
    let baseline = thread_count();

    // 8 sessions, each abandoned while a slow statement is in flight.
    let mut conns = Vec::new();
    for _ in 0..8 {
        conns.push(backend.connect().await.unwrap());
    }
    assert!(
        thread_count() >= baseline + 8,
        "expected one worker thread per live session, saw {} vs baseline {baseline}",
        thread_count()
    );

    let mut inflight = Vec::new();
    for conn in &conns {
        // Start the statement, then walk away from it.
        inflight.push(tokio::time::timeout(
            Duration::from_millis(2),
            conn.query(SLOW_COUNT, &[]),
        ));
    }
    for f in inflight {
        assert!(f.await.is_err(), "probe statement was not slow enough");
    }
    drop(conns);

    eventually("worker threads to be reclaimed", || {
        thread_count() <= baseline
    })
    .await;
}

/// `busy_timeout` must still bound how long a blocked write waits, now that
/// the wait happens inside a session worker thread.
///
/// Would catch: a worker that loses the per-session PRAGMAs (in which case
/// the default 5 s timeout, or none at all, would apply), and -- more
/// importantly -- any design where a blocked SQLite call cannot be observed
/// to end, which is what a hang looks like to a wire client.
#[tokio::test]
async fn busy_timeout_still_bounds_a_blocked_write() {
    let dir = std::env::temp_dir().join(format!("litewire-busy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("busy.sqlite");

    let backend = Rusqlite::builder(&path)
        .busy_timeout_ms(150)
        .build()
        .unwrap();
    backend
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
        .await
        .unwrap();

    // An outside writer holds the database's write lock for the duration.
    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let conn = backend.connect().await.unwrap();
    let started = Instant::now();
    let err = conn
        .execute("INSERT INTO t VALUES (1)", &[])
        .await
        .expect_err("a write against an exclusively locked database must fail");
    let waited = started.elapsed();

    assert!(
        matches!(&err, BackendError::Sqlite(m) if m.to_lowercase().contains("locked")
            || m.to_lowercase().contains("busy")),
        "expected a lock error, got: {err}"
    );
    assert!(
        waited >= Duration::from_millis(100),
        "gave up before the configured busy_timeout ({waited:?}) -- the PRAGMA is not reaching the worker"
    );
    assert!(
        waited < Duration::from_secs(3),
        "waited {waited:?}, far past the configured 150 ms busy_timeout"
    );

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);

    // The session recovers once the lock is released.
    conn.execute("INSERT INTO t VALUES (1)", &[]).await.unwrap();

    drop(conn);
    drop(backend);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Concurrent sessions must not see each other's uncommitted work, proven
/// through the worker-thread path with the sessions genuinely running in
/// parallel on separate threads.
///
/// Would catch: any regression that reintroduces a shared connection --
/// the failure this backend's per-session design exists to prevent, where
/// one client's `BEGIN` swallows another client's statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sessions_do_not_share_transaction_state() {
    let dir = std::env::temp_dir().join(format!("litewire-iso-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("iso.sqlite");
    let backend = Arc::new(Rusqlite::open(&path).unwrap());
    backend
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, who TEXT)", &[])
        .await
        .unwrap();

    let a = backend.connect().await.unwrap();
    let b = backend.connect().await.unwrap();

    a.execute("BEGIN IMMEDIATE", &[]).await.unwrap();
    a.execute("INSERT INTO t VALUES (1, 'a')", &[])
        .await
        .unwrap();

    // B is a different session on a different thread: it must see nothing.
    let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(
        rs.rows[0][0],
        Value::Integer(0),
        "session B saw session A's uncommitted row"
    );

    // B is also not inside A's transaction.
    b.execute("BEGIN", &[])
        .await
        .expect("session B inherited a transaction from session A");
    b.execute("ROLLBACK", &[]).await.unwrap();

    a.execute("COMMIT", &[]).await.unwrap();
    let rs = b.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(1));

    // And under real parallelism, every session's own writes are all it
    // counts.
    let mut tasks = Vec::new();
    for who in 0..8i64 {
        let backend = Arc::clone(&backend);
        tasks.push(tokio::spawn(async move {
            let conn = backend.connect().await.unwrap();
            for i in 0..20i64 {
                conn.execute(
                    "INSERT INTO t (id, who) VALUES (?1, ?2)",
                    &[
                        Value::Integer(1000 + who * 100 + i),
                        Value::Text(who.to_string()),
                    ],
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
            assert_eq!(rs.rows[0][0], Value::Integer(20));
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    drop(a);
    drop(b);
    drop(backend);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The reuse pool must survive a session that is dropped while a statement
/// is still running: the worker finishes, runs hygiene, and parks.
///
/// Would catch: an end-of-session path that races the in-flight statement,
/// which would either pool a handle mid-statement or lose the worker
/// entirely and silently degrade reuse to a no-op.
#[tokio::test]
async fn reuse_survives_a_session_dropped_mid_statement() {
    let backend = Rusqlite::builder(":memory:")
        .handle_reuse(4)
        .build()
        .unwrap();

    let conn = backend.connect().await.unwrap();
    let before = backend.reuse_stats().expect("reuse enabled");

    assert!(
        tokio::time::timeout(Duration::from_millis(2), conn.query(SLOW_COUNT, &[]))
            .await
            .is_err(),
        "probe statement was not slow enough"
    );
    drop(conn);

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let s = backend.reuse_stats().unwrap();
        if s.returned == before.returned + 1 {
            assert_eq!(
                s.discarded, before.discarded,
                "an abandoned statement cost the pool a handle: {s:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "worker never returned to the pool after a mid-statement drop: {s:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The recycled worker is healthy.
    let next = backend.connect().await.unwrap();
    assert_eq!(backend.reuse_stats().unwrap().hits, before.hits + 1);
    assert_eq!(
        next.query("SELECT 1", &[]).await.unwrap().rows[0][0],
        Value::Integer(1)
    );
}
