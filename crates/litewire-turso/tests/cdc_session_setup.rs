//! Opening a session on a CDC-capturing factory must not wait on another
//! connection's write lock.
//!
//! Regression: `enable_cdc_on_connect` used to run `PRAGMA
//! capture_data_changes_conn('full')` inside `Backend::connect`. The engine
//! executes that pragma as a *write* transaction, so while any other
//! connection held a long write transaction (a 3,000-row import, say) every
//! new session on the database — readers included — queued on the write lock
//! and then failed to open with "database is locked" once the busy timeout ran
//! out. Embedders reported that as "failed to open a session", which reads
//! like a broken database rather than a busy one.
//!
//! Capture is now switched on lazily, just before the session's first
//! statement that can write.

use std::time::{Duration, Instant};

use litewire_backend::{Backend, BackendError, Value};
use litewire_turso::Turso;

const BUSY_TIMEOUT_MS: u32 = 400;

async fn capturing_factory(dir: &tempfile::TempDir) -> Turso {
    let path = dir.path().join("site.db");
    let factory = Turso::builder(path.to_str().unwrap())
        .busy_timeout_ms(BUSY_TIMEOUT_MS)
        .enable_cdc_on_connect(true)
        .build()
        .await
        .unwrap();
    factory
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
        .await
        .unwrap();
    factory
}

/// Captured INSERTs into `t`, read off the CDC log on a raw connection.
async fn captured_inserts(factory: &Turso) -> i64 {
    let raw = factory.raw_connection().unwrap();
    let mut rows = raw
        .query(
            "SELECT COUNT(*) FROM turso_cdc WHERE table_name = 't' AND change_type = 1",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    row.get::<i64>(0).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_setup_does_not_wait_on_another_writers_lock() {
    let dir = tempfile::tempdir().unwrap();
    let factory = capturing_factory(&dir).await;

    // A long write transaction holds the database's write lock.
    let writer = factory.connect().await.unwrap();
    writer.execute("BEGIN", &[]).await.unwrap();
    for i in 0..3000 {
        writer
            .execute(
                "INSERT INTO t (v) VALUES (?)",
                &[Value::Text(format!("bulk-{i}"))],
            )
            .await
            .unwrap();
    }

    // Opening a new session must not queue behind it. Before the fix this
    // waited the whole busy timeout and then failed with "database is locked".
    let started = Instant::now();
    let other = factory
        .connect()
        .await
        .expect("opening a session must not need the write lock");
    assert!(
        started.elapsed() < Duration::from_millis(u64::from(BUSY_TIMEOUT_MS) / 2),
        "session setup waited {:?} on another connection's write lock",
        started.elapsed()
    );

    // Reads work immediately (WAL snapshot: the open transaction is invisible).
    let started = Instant::now();
    let rs = other.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(0));
    assert!(started.elapsed() < Duration::from_millis(u64::from(BUSY_TIMEOUT_MS) / 2));

    // A write genuinely needs the lock: it waits out the busy timeout and then
    // fails with a busy error that says what it was waiting for.
    let started = Instant::now();
    let err = other
        .execute("INSERT INTO t (v) VALUES ('late')", &[])
        .await
        .expect_err("a write must not get past another connection's write lock");
    let waited = started.elapsed();
    let BackendError::Sqlite(message) = &err else {
        panic!("expected a SQLite busy error, got {err:?}");
    };
    assert!(
        message.contains("database is locked (SQLITE_BUSY)"),
        "the error must say the database was busy: {message}"
    );
    assert!(
        message.contains("busy timeout"),
        "the error must say it waited out the busy timeout: {message}"
    );
    assert!(
        waited >= Duration::from_millis(u64::from(BUSY_TIMEOUT_MS) * 3 / 4),
        "gave up after {waited:?}, before the {BUSY_TIMEOUT_MS} ms busy timeout"
    );

    // Once the lock is released the same session writes, and the write is
    // captured: deferring the switch never loses a change.
    writer.execute("COMMIT", &[]).await.unwrap();
    other
        .execute("INSERT INTO t (v) VALUES ('late')", &[])
        .await
        .expect("the write succeeds once the lock is free");
    assert_eq!(captured_inserts(&factory).await, 3001);
}

/// A session whose first statements are reads still captures the write that
/// follows them.
#[tokio::test]
async fn a_session_that_reads_first_still_captures_its_writes() {
    let dir = tempfile::tempdir().unwrap();
    let factory = capturing_factory(&dir).await;

    let session = factory.connect().await.unwrap();
    session.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    session.query("SELECT 1", &[]).await.unwrap();
    session
        .execute("INSERT INTO t (v) VALUES ('after-reads')", &[])
        .await
        .unwrap();
    // And inside an explicit transaction opened after a read.
    session.query("SELECT 1", &[]).await.unwrap();
    session.execute("BEGIN", &[]).await.unwrap();
    session
        .execute("INSERT INTO t (v) VALUES ('in-txn')", &[])
        .await
        .unwrap();
    session.execute("COMMIT", &[]).await.unwrap();

    assert_eq!(captured_inserts(&factory).await, 2);
}

/// A session that only reads never takes the write lock at all, so it opens
/// and reads while a writer holds the lock even with no busy timeout to wait
/// out.
#[tokio::test]
async fn a_read_only_session_never_needs_the_write_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("site.db");
    let factory = Turso::builder(path.to_str().unwrap())
        .busy_timeout_ms(0)
        .enable_cdc_on_connect(true)
        .build()
        .await
        .unwrap();
    factory
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
        .await
        .unwrap();

    let writer = factory.connect().await.unwrap();
    writer.execute("BEGIN", &[]).await.unwrap();
    writer
        .execute("INSERT INTO t (id) VALUES (1)", &[])
        .await
        .unwrap();

    let reader = factory.connect().await.expect("open while write-locked");
    let rs = reader.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(0));

    writer.execute("COMMIT", &[]).await.unwrap();
}
