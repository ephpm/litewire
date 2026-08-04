//! End-to-end tests for write admission control on the Hrana backend.
//!
//! The unit tests beside the implementation drive the state machine
//! directly. These drive it the way sqld does: through a real
//! `HranaFrontend` over real HTTP, with real statements, so the permit's
//! interaction with the `Backend` factory and the `BackendConn` lifecycle
//! is exercised rather than assumed.
//!
//! # What this harness can and cannot show
//!
//! The server here is litewire's own [`HranaFrontend`], which is
//! **explicitly stateless**: it returns no baton (`litewire-hrana/src/
//! http.rs` -- "Stateless for now (no transaction continuity)") and takes
//! a fresh backend connection per request. So a `BEGIN` sent through it
//! never opens a server-side transaction, and the matching `COMMIT` comes
//! back "cannot commit - no transaction is active". Real sqld, which is
//! what these clients talk to in production, does honour batons.
//!
//! That gap does not weaken what these tests assert, because **write
//! admission is decided entirely on the client**, in `SessionAdmission`,
//! before a request is sent. The permit is taken and returned identically
//! regardless of what the far end does with the statement. Where a test
//! needs a transaction closed it uses [`end_txn`], which asserts the
//! *admission* effect and tolerates the stateless server's complaint --
//! and that the permit comes back even when the `COMMIT` itself errors is
//! a property worth having pinned down.
//!
//! Verifying that sqld's own locking improves under admission control is
//! the benchmark's job, not this file's.

#![cfg(all(feature = "rusqlite", feature = "hrana-client"))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use litewire_backend::{Backend, BackendConn, BackendError, HranaClient, Rusqlite, Value};
use litewire_hrana::{HranaFrontend, HranaFrontendConfig};

/// Start an in-process Hrana server and return its base URL.
async fn start_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener);

    let backend = Rusqlite::memory().expect("failed to create in-memory SQLite");
    let shared: Arc<dyn Backend> = Arc::new(backend);
    let frontend = HranaFrontend::new(HranaFrontendConfig { listen: addr }, shared);

    let handle = tokio::spawn(async move {
        frontend.serve().await.expect("Hrana server failed");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    (format!("http://{addr}"), handle)
}

/// A client with `permits` write permits, and a table to write into.
async fn setup(permits: usize) -> (HranaClient, tokio::task::JoinHandle<()>) {
    let (url, server) = start_server().await;
    let client = HranaClient::new(&url).write_permits(permits);
    client
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", &[])
        .await
        .expect("create table");
    (client, server)
}

/// Close a transaction, tolerating the stateless test server's "no
/// transaction is active".
///
/// The admission-side effect -- releasing the permit -- happens in
/// `SessionAdmission::admit` before the request goes out, and the permit
/// is dropped when the call returns whether or not the server liked the
/// statement. Any *other* error is a real failure and is raised.
async fn end_txn(conn: &dyn BackendConn, sql: &str) {
    match conn.execute(sql, &[]).await {
        Ok(_) => {}
        Err(BackendError::Other(m)) | Err(BackendError::Sqlite(m))
            if m.contains("no transaction is active") => {}
        Err(e) => panic!("unexpected error closing transaction with {sql}: {e:?}"),
    }
}

/// The headline property: with one permit, a second session's write cannot
/// land between another session's `BEGIN` and `COMMIT`.
#[tokio::test]
async fn write_cannot_interleave_into_an_open_transaction() {
    let (client, _server) = setup(1).await;

    let a = client.connect().await.unwrap();
    let b = client.connect().await.unwrap();

    a.execute("BEGIN", &[]).await.unwrap();
    a.execute("INSERT INTO t (v) VALUES (1)", &[])
        .await
        .unwrap();

    let landed = Arc::new(AtomicUsize::new(0));
    let landed2 = Arc::clone(&landed);
    let waiter = tokio::spawn(async move {
        b.execute("INSERT INTO t (v) VALUES (99)", &[])
            .await
            .unwrap();
        landed2.store(1, Ordering::SeqCst);
    });

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        landed.load(Ordering::SeqCst),
        0,
        "a second writer got into the open transaction's lock window"
    );

    // A second statement in the same transaction still works -- a session
    // is never blocked by its own parked permit.
    a.execute("INSERT INTO t (v) VALUES (2)", &[])
        .await
        .unwrap();
    assert_eq!(landed.load(Ordering::SeqCst), 0);

    end_txn(a.as_ref(), "COMMIT").await;

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("second writer never woke after COMMIT")
        .unwrap();
    assert_eq!(landed.load(Ordering::SeqCst), 1);
}

/// Reads must not queue behind held write permits.
#[tokio::test]
async fn reads_proceed_while_the_only_permit_is_held() {
    let (client, _server) = setup(1).await;

    let writer = client.connect().await.unwrap();
    writer.execute("BEGIN", &[]).await.unwrap();
    writer
        .execute("INSERT INTO t (v) VALUES (1)", &[])
        .await
        .unwrap();
    // The single permit is now parked in `writer` until COMMIT.

    let reader = client.connect().await.unwrap();
    let reads = tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..50 {
            reader.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
        }
    })
    .await;
    assert!(reads.is_ok(), "reads blocked behind a held write permit");

    end_txn(writer.as_ref(), "COMMIT").await;
}

/// Dropping a session mid-transaction must return its permit, or the pool
/// bleeds a seat per abandoned transaction until writes stop entirely.
#[tokio::test]
async fn dropping_a_session_mid_transaction_frees_the_pool() {
    let (client, _server) = setup(1).await;

    {
        let doomed = client.connect().await.unwrap();
        doomed.execute("BEGIN", &[]).await.unwrap();
        doomed
            .execute("INSERT INTO t (v) VALUES (1)", &[])
            .await
            .unwrap();
        // No COMMIT, no ROLLBACK: the client vanished.
    }

    let next = client.connect().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        next.execute("INSERT INTO t (v) VALUES (2)", &[]),
    )
    .await
    .expect("pool wedged after a session was dropped mid-transaction")
    .unwrap();
}

/// A `COMMIT` that fails must still return the permit. The stateless test
/// server makes this easy to provoke, but the case that matters in
/// production is a commit rejected by sqld.
#[tokio::test]
async fn a_failing_commit_still_returns_the_permit() {
    let (client, _server) = setup(1).await;

    let a = client.connect().await.unwrap();
    a.execute("BEGIN", &[]).await.unwrap();
    a.execute("INSERT INTO t (v) VALUES (1)", &[])
        .await
        .unwrap();

    let err = a.execute("COMMIT", &[]).await;
    assert!(
        err.is_err(),
        "expected the stateless server to reject COMMIT"
    );

    // The permit must be back despite the error.
    let b = client.connect().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        b.execute("INSERT INTO t (v) VALUES (2)", &[]),
    )
    .await
    .expect("a failed COMMIT leaked the permit")
    .unwrap();
}

/// A queued write that never gets a permit must fail with a
/// `SQLITE_BUSY`-shaped error rather than hang forever.
#[tokio::test]
async fn queued_write_times_out_with_a_busy_error() {
    let (url, _server) = start_server().await;
    let client = HranaClient::new(&url)
        .write_permits(1)
        .write_acquire_timeout(Duration::from_millis(150));
    client
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", &[])
        .await
        .unwrap();

    let holder = client.connect().await.unwrap();
    holder.execute("BEGIN", &[]).await.unwrap();
    holder
        .execute("INSERT INTO t (v) VALUES (1)", &[])
        .await
        .unwrap();

    let blocked = client.connect().await.unwrap();
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        blocked.execute("INSERT INTO t (v) VALUES (2)", &[]),
    )
    .await
    .expect("admit hung past its own acquire timeout")
    .expect_err("expected a busy error");

    assert!(
        matches!(&err, BackendError::Sqlite(m) if m.contains("SQLITE_BUSY")),
        "not SQLITE_BUSY-shaped, so wire frontends will not map it onto a \
         retriable lock error: {err:?}"
    );

    end_txn(holder.as_ref(), "COMMIT").await;
}

/// With admission off (the default), nothing serialises: a second writer
/// proceeds while another session has an open transaction. This is the
/// "permits = 0 behaves exactly as before" contract.
#[tokio::test]
async fn permits_zero_does_not_serialise_writers() {
    let (url, _server) = start_server().await;
    let client = HranaClient::new(&url); // no .write_permits() call at all
    client
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", &[])
        .await
        .unwrap();

    let a = client.connect().await.unwrap();
    a.execute("BEGIN", &[]).await.unwrap();
    a.execute("INSERT INTO t (v) VALUES (1)", &[])
        .await
        .unwrap();

    // With admission on and one permit this would block until COMMIT.
    let b = client.connect().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        b.execute("INSERT INTO t (v) VALUES (2)", &[]),
    )
    .await
    .expect("permits=0 must not gate a second writer")
    .unwrap();

    end_txn(a.as_ref(), "COMMIT").await;
}

/// Many concurrent autocommit writers with a small permit count must all
/// complete -- queued, never refused -- and every row must land.
#[tokio::test]
async fn concurrent_autocommit_writers_all_complete() {
    const WRITERS: usize = 32;
    const PERMITS: usize = 4;

    let (client, _server) = setup(PERMITS).await;

    let mut tasks = Vec::new();
    for i in 0..WRITERS {
        let conn = client.connect().await.unwrap();
        tasks.push(tokio::spawn(async move {
            conn.execute("INSERT INTO t (v) VALUES (?)", &[Value::Integer(i as i64)])
                .await
        }));
    }

    for (i, t) in tasks.into_iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(20), t)
            .await
            .unwrap_or_else(|_| panic!("writer {i} never completed"))
            .unwrap()
            .unwrap_or_else(|e| panic!("writer {i} failed: {e:?}"));
    }

    let rs = client.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(
        rs.rows[0][0],
        Value::Integer(WRITERS as i64),
        "some writes were lost"
    );
}

/// A rolled-back transaction releases its permit just as a committed one
/// does.
#[tokio::test]
async fn rollback_releases_the_permit() {
    let (client, _server) = setup(1).await;

    let a = client.connect().await.unwrap();
    a.execute("BEGIN", &[]).await.unwrap();
    a.execute("INSERT INTO t (v) VALUES (1)", &[])
        .await
        .unwrap();
    end_txn(a.as_ref(), "ROLLBACK").await;

    let b = client.connect().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        b.execute("INSERT INTO t (v) VALUES (2)", &[]),
    )
    .await
    .expect("ROLLBACK did not release the permit")
    .unwrap();
}
