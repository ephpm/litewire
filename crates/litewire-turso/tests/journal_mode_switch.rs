//! Integration test: a `PRAGMA journal_mode` switch to MVCC issued inside a
//! transaction must not cost the database.
//!
//! On turso 0.8.2 the switch was accepted mid-transaction and wrote the MVCC
//! logical log before the MVCC metadata table existed. The transaction then
//! committed (through litewire, `COMMIT` reported success), and the file could
//! never be opened again: "Corrupt database: Missing MVCC metadata table while
//! logical log state exists". Every row was lost, not only the transaction.
//! 0.8.3 refuses the switch inside a transaction ("core/vdbe: refuse to switch
//! to or from MVCC inside a transaction").
//!
//! The statements are ordinary SQL an application can send, so this runs them
//! through the `Backend` / `BackendConn` trait objects the wire frontends use,
//! then closes and reopens the file.

use std::sync::Arc;

use litewire_backend::{SharedBackend, Value};
use litewire_turso::Turso;

async fn open(path: &str) -> SharedBackend {
    Arc::new(Turso::open(path).await.expect("open turso file"))
}

#[tokio::test]
async fn mvcc_switch_inside_a_transaction_is_refused_and_the_file_reopens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("switch.db");
    let path = path.to_str().unwrap();

    {
        let backend = open(path).await;
        let conn = backend.connect().await.unwrap();
        conn.execute(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)",
            &[],
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO posts (title) VALUES ('a'), ('b')", &[])
            .await
            .unwrap();

        conn.execute("BEGIN", &[]).await.unwrap();
        // `query`, not `execute`: the pragma answers with the resulting mode
        // as a row.
        let err = conn
            .query("PRAGMA journal_mode = 'mvcc'", &[])
            .await
            .expect_err("switching to MVCC inside a transaction must be refused");
        assert!(
            err.to_string().contains("from within a transaction"),
            "unexpected error: {err}"
        );
        conn.execute("INSERT INTO posts (title) VALUES ('x')", &[])
            .await
            .unwrap();
        conn.execute("COMMIT", &[]).await.unwrap();
    }

    // The file must open again, still in WAL mode, with every row.
    let backend = open(path).await;
    let conn = backend.connect().await.unwrap();
    let count = conn
        .query("SELECT COUNT(*) FROM posts", &[])
        .await
        .expect("database must reopen after the refused switch");
    assert_eq!(count.rows, vec![vec![Value::Integer(3)]]);
    let mode = conn.query("PRAGMA journal_mode", &[]).await.unwrap();
    assert_eq!(mode.rows, vec![vec![Value::Text("wal".into())]]);
}
