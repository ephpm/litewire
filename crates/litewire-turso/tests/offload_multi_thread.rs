//! The backend on a multi-thread runtime, where every engine call runs
//! under `block_in_place` (see `litewire_turso::offload`).
//!
//! Most of this crate's tests use `#[tokio::test]`'s current-thread runtime,
//! where `offload::engine` awaits in place, so they never exercise the
//! offloaded path. These run the same operations — sessions with CDC
//! capture behind a WAL gate, tailing, applying, checkpointing — from a task
//! on a one-worker multi-thread runtime. (`offload`'s own unit test proves the
//! worker is released while the engine runs.)

use std::sync::Arc;
use std::time::Duration;

use litewire_backend::{Backend, Value};
use litewire_turso::cdc::{CdcTailer, apply_batch, read_watermark};
use litewire_turso::{Turso, WalGateConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sessions_tail_and_apply_on_a_multi_thread_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let primary_path = dir.path().join("p.db");
    let replica_path = dir.path().join("r.db");
    let primary = Arc::new(
        Turso::builder(primary_path.to_str().unwrap())
            .enable_cdc_on_connect(true)
            .wal_checkpoint_gate(WalGateConfig {
                threshold_bytes: 1,
                interval: Duration::from_millis(5),
                ..WalGateConfig::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let replica = Turso::open(replica_path.to_str().unwrap()).await.unwrap();

    let work = {
        let primary = Arc::clone(&primary);
        tokio::spawn(async move {
            let conn = primary.connect().await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
                .await
                .unwrap();
            for i in 0..200 {
                conn.execute(
                    "INSERT INTO t (id, v) VALUES (?, ?)",
                    &[Value::Integer(i), Value::Text(format!("row {i}"))],
                )
                .await
                .unwrap();
            }
            let rs = conn.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
            assert_eq!(rs.rows[0][0], Value::Integer(200));
            let cols = conn.describe_columns("SELECT id, v FROM t").await.unwrap();
            assert_eq!(cols.len(), 2);

            let apply_conn = replica.raw_connection().unwrap();
            let mut tailer = CdcTailer::new(&primary, 0);
            while let Some(batch) = tailer.poll_batch().await.unwrap() {
                apply_batch(&apply_conn, &batch).await.unwrap();
            }
            assert_eq!(read_watermark(&apply_conn).await.unwrap(), tailer.cursor());
            let mut rows = apply_conn
                .query("SELECT COUNT(*) FROM t", ())
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), turso::Value::Integer(200));
        })
    };
    work.await.unwrap();

    // The gate's maintenance task (threshold 1 byte) truncates on the
    // runtime too.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while primary.wal_gate_stats().unwrap().truncated == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the gate never truncated"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
