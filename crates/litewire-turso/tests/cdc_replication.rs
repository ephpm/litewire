//! End-to-end tests for the litewire-turso CDC tail/apply pipeline.
//!
//! Each test opens two `Turso` factories (two databases: "primary" and
//! "replica"), enables CDC on the primary, runs some SQL, tails the CDC
//! log, applies the batches to the replica, and asserts that the
//! replica's row/schema state matches the primary.
//!
//! # Why two file-backed databases and not `":memory:"`
//!
//! Turso 0.7.0's `":memory:"` mode gives every wire session on the same
//! factory a shared view — great for the isolation tests in the base
//! backend, but for replication we specifically need two *independent*
//! engines that only communicate via the CDC stream, not the shared
//! in-memory buffer. Temp files give us that.

use std::sync::Arc;

use litewire_backend::{Backend, Value};
use litewire_turso::Turso;
use litewire_turso::cdc::{CdcTailer, TxnBatch, apply_batch, enable_cdc, read_watermark};

async fn temp_factory() -> (Arc<Turso>, tempfile::NamedTempFile) {
    let file = tempfile::NamedTempFile::new().expect("temp file");
    let path = file.path().to_str().unwrap().to_string();
    let factory = Arc::new(Turso::open(path).await.expect("open turso file"));
    (factory, file)
}

/// Drain every currently-available complete transaction from the tailer.
async fn drain_batches(tailer: &mut CdcTailer<'_>) -> Vec<TxnBatch> {
    let mut out = Vec::new();
    while let Some(batch) = tailer.poll_batch().await.expect("poll_batch") {
        out.push(batch);
    }
    out
}

/// Apply a stream of batches to a replica connection sequentially.
async fn apply_all(replica_conn: &turso::Connection, batches: &[TxnBatch]) {
    for b in batches {
        apply_batch(replica_conn, b).await.expect("apply_batch");
    }
}

async fn count_rows(backend: &Arc<Turso>, table: &str) -> i64 {
    let rs = backend
        .query(&format!("SELECT COUNT(*) FROM \"{table}\""), &[])
        .await
        .unwrap();
    match &rs.rows[0][0] {
        Value::Integer(i) => *i,
        v => panic!("count returned non-integer: {v:?}"),
    }
}

async fn all_rows(backend: &Arc<Turso>, sql: &str) -> Vec<Vec<Value>> {
    let rs = backend.query(sql, &[]).await.unwrap();
    rs.rows
}

#[tokio::test]
async fn watermark_starts_at_zero_and_advances_monotonically() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    // Primary: enable CDC on a session, run one txn.
    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (1, 'hello')", ())
        .await
        .unwrap();

    // Tail + apply.
    let mut tailer = CdcTailer::new(&primary, 0);
    let batches = drain_batches(&mut tailer).await;
    assert!(!batches.is_empty(), "expected at least one batch");
    let expected_wm = batches.last().unwrap().commit_change_id();

    let r_conn = replica.raw_connection().unwrap();
    assert_eq!(read_watermark(&r_conn).await.unwrap(), 0);

    apply_all(&r_conn, &batches).await;

    assert_eq!(
        read_watermark(&r_conn).await.unwrap(),
        expected_wm,
        "watermark must advance to the last COMMIT change_id"
    );
    // A tailer that resumes from expected_wm should now find nothing.
    let mut tailer2 = CdcTailer::new(&primary, expected_wm);
    assert!(tailer2.poll_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn ddl_replicates_replica_can_read_the_table() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();

    let mut tailer = CdcTailer::new(&primary, 0);
    let batches = drain_batches(&mut tailer).await;

    let r_conn = replica.raw_connection().unwrap();
    apply_all(&r_conn, &batches).await;

    // The DDL should have been replayed on the replica; SELECT proves it.
    let rs = replica
        .query("SELECT COUNT(*) FROM posts", &[])
        .await
        .unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(0));
}

#[tokio::test]
async fn dml_insert_replicates_row_values_match() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (1, 'a')", ())
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (2, 'b')", ())
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (3, 'c')", ())
        .await
        .unwrap();

    let mut tailer = CdcTailer::new(&primary, 0);
    let batches = drain_batches(&mut tailer).await;

    let r_conn = replica.raw_connection().unwrap();
    apply_all(&r_conn, &batches).await;

    let p_rows = all_rows(&primary, "SELECT id, v FROM t ORDER BY id").await;
    let r_rows = all_rows(&replica, "SELECT id, v FROM t ORDER BY id").await;
    assert_eq!(p_rows, r_rows, "replica rows must match primary");
    assert_eq!(count_rows(&replica, "t").await, 3);
}

#[tokio::test]
async fn dml_delete_replicates() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();
    for i in 1..=5 {
        p_conn
            .execute(&format!("INSERT INTO t VALUES ({i}, 'row-{i}')"), ())
            .await
            .unwrap();
    }
    p_conn
        .execute("DELETE FROM t WHERE id >= 3", ())
        .await
        .unwrap();

    let mut tailer = CdcTailer::new(&primary, 0);
    let batches = drain_batches(&mut tailer).await;
    let r_conn = replica.raw_connection().unwrap();
    apply_all(&r_conn, &batches).await;

    let p_rows = all_rows(&primary, "SELECT id FROM t ORDER BY id").await;
    let r_rows = all_rows(&replica, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(p_rows, r_rows);
    assert_eq!(count_rows(&replica, "t").await, 2);
}

#[tokio::test]
async fn dml_update_replicates_new_value() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (1, 'before')", ())
        .await
        .unwrap();
    p_conn
        .execute("UPDATE t SET v = 'after' WHERE id = 1", ())
        .await
        .unwrap();

    let mut tailer = CdcTailer::new(&primary, 0);
    let batches = drain_batches(&mut tailer).await;
    let r_conn = replica.raw_connection().unwrap();
    apply_all(&r_conn, &batches).await;

    let r = replica
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Text("after".into()));
}

/// Idempotency: re-applying the same batch stream after a "crash" must
/// leave the replica in the same state (no double-inserts, no stale rows).
#[tokio::test]
async fn apply_is_idempotent_across_replays() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();
    for i in 1..=10 {
        p_conn
            .execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"), ())
            .await
            .unwrap();
    }

    let mut tailer = CdcTailer::new(&primary, 0);
    let batches = drain_batches(&mut tailer).await;
    let r_conn = replica.raw_connection().unwrap();

    // Apply once.
    apply_all(&r_conn, &batches).await;
    let after_first = count_rows(&replica, "t").await;

    // Apply again — every batch's commit_change_id <= watermark, so each
    // apply is a no-op.
    apply_all(&r_conn, &batches).await;
    let after_second = count_rows(&replica, "t").await;

    assert_eq!(after_first, 10);
    assert_eq!(after_first, after_second, "idempotent apply changed state");
}

/// Bootstrap → resume: apply DDL/DML batches, then commit more txns on
/// the primary, then resume the tailer from the recorded watermark and
/// apply only the new batches. This is the "replica catches up after
/// downtime" flow.
#[tokio::test]
async fn tailer_resume_from_watermark_only_ships_new_batches() {
    let (primary, _p) = temp_factory().await;
    let (replica, _r) = temp_factory().await;

    let p_conn = primary.raw_connection().unwrap();
    enable_cdc(&p_conn).await.unwrap();
    p_conn
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (1, 'a')", ())
        .await
        .unwrap();

    let r_conn = replica.raw_connection().unwrap();
    {
        let mut tailer = CdcTailer::new(&primary, 0);
        let batches = drain_batches(&mut tailer).await;
        apply_all(&r_conn, &batches).await;
    }
    let wm = read_watermark(&r_conn).await.unwrap();
    assert!(wm > 0);

    // Primary keeps going.
    p_conn
        .execute("INSERT INTO t VALUES (2, 'b')", ())
        .await
        .unwrap();
    p_conn
        .execute("INSERT INTO t VALUES (3, 'c')", ())
        .await
        .unwrap();

    // Resume: fresh tailer starting at the persisted watermark.
    let mut tailer = CdcTailer::new(&primary, wm);
    let new_batches = drain_batches(&mut tailer).await;
    assert!(!new_batches.is_empty(), "expected new batches after resume");
    for b in &new_batches {
        assert!(
            b.commit_change_id() > wm,
            "resumed tailer emitted a batch <= old watermark: {}",
            b.commit_change_id()
        );
    }
    apply_all(&r_conn, &new_batches).await;

    assert_eq!(count_rows(&replica, "t").await, 3);
}

/// The factory-level `enable_cdc_on_connect` opt-in causes every
/// wire-session `Backend::connect` to opt into CDC — so writes coming in
/// via the litewire wire frontends are captured for replication (this is
/// the seam ePHPm's Phase 2 primary depends on).
#[tokio::test]
async fn factory_flag_enables_cdc_on_backend_connect() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_str().unwrap();
    let factory = Turso::builder(path)
        .enable_cdc_on_connect(true)
        .build()
        .await
        .unwrap();
    let factory = Arc::new(factory);

    // A wire session obtained via Backend::connect should have CDC live.
    let session = factory.connect().await.unwrap();
    session
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
        .await
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'x')", &[])
        .await
        .unwrap();

    // A tailer using the same factory should see the batch (CREATE +
    // INSERT + COMMIT).
    let mut tailer = CdcTailer::new(&factory, 0);
    let batches = drain_batches(&mut tailer).await;
    assert!(
        !batches.is_empty(),
        "enable_cdc_on_connect did not cause writes to be captured"
    );
}

/// Partial-transaction hold-back: this is subtle. The primary opens an
/// explicit BEGIN, inserts, and does *not* commit yet. The tailer must
/// return `None` (no COMMIT delimiter → no complete batch).
#[tokio::test]
async fn uncommitted_transaction_is_not_yielded() {
    let (primary, _p) = temp_factory().await;

    let setup_conn = primary.raw_connection().unwrap();
    setup_conn
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();

    let writer = primary.raw_connection().unwrap();
    enable_cdc(&writer).await.unwrap();
    writer.execute("BEGIN", ()).await.unwrap();
    writer
        .execute("INSERT INTO t VALUES (99, 'uncommitted')", ())
        .await
        .unwrap();

    let mut tailer = CdcTailer::new(&primary, 0);
    // Note: setup_conn had no CDC enabled so its CREATE TABLE isn't in
    // the log; the log currently holds only the mid-transaction insert
    // from `writer`. That row has no COMMIT delimiter yet.
    assert!(
        tailer.poll_batch().await.unwrap().is_none(),
        "tailer yielded an uncommitted batch"
    );

    // Now commit and re-poll: the batch should surface.
    writer.execute("COMMIT", ()).await.unwrap();
    let batch = tailer
        .poll_batch()
        .await
        .unwrap()
        .expect("committed batch should surface");
    assert!(batch.commit_change_id() > 0);
}
