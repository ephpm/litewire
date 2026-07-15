//! Integration test: drive the Turso backend through the `Backend` /
//! `BackendConn` trait objects exactly the way litewire's wire frontends
//! do — schema init, prepared statements with positional params,
//! transactions, `last_insert_rowid`, affected rows, and `describe_columns`
//! (the `COM_STMT_PREPARE` path).

use std::sync::Arc;

use litewire_backend::{SharedBackend, Value};
use litewire_turso::Turso;

async fn shared_backend() -> SharedBackend {
    Arc::new(Turso::memory().await.expect("open turso memory db"))
}

#[tokio::test]
async fn wire_session_lifecycle() {
    let backend = shared_backend().await;

    // Schema init (what ephpm / app bootstrap does via the stateless API).
    backend
        .execute(
            "CREATE TABLE posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                views INTEGER NOT NULL DEFAULT 0
            )",
            &[],
        )
        .await
        .unwrap();
    backend
        .execute("CREATE INDEX idx_posts_title ON posts(title)", &[])
        .await
        .unwrap();

    // A wire session: connect once, run prepared statements.
    let session = backend.connect().await.unwrap();

    // COM_STMT_PREPARE metadata without execution.
    let cols = session
        .describe_columns("SELECT id, title, views FROM posts")
        .await
        .unwrap();
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].decltype.as_deref(), Some("INTEGER"));

    // INSERT with params: last_insert_rowid + affected rows.
    let r = session
        .execute(
            "INSERT INTO posts (title) VALUES (?1)",
            &[Value::Text("hello world".into())],
        )
        .await
        .unwrap();
    assert_eq!(r.affected_rows, 1);
    assert_eq!(r.last_insert_rowid, Some(1));

    // Transaction: multi-statement, committed.
    session.execute("BEGIN", &[]).await.unwrap();
    for i in 0..10 {
        session
            .execute(
                "INSERT INTO posts (title, views) VALUES (?1, ?2)",
                &[Value::Text(format!("post-{i}")), Value::Integer(i * 10)],
            )
            .await
            .unwrap();
    }
    session.execute("COMMIT", &[]).await.unwrap();

    // Point SELECT (the ephpm db.php fixture pattern).
    let rs = session
        .query(
            "SELECT title, views FROM posts WHERE id = ?1",
            &[Value::Integer(5)],
        )
        .await
        .unwrap();
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Text("post-3".into()));

    // UPDATE affected rows.
    let r = session
        .execute(
            "UPDATE posts SET views = views + 1 WHERE views >= ?1",
            &[Value::Integer(50)],
        )
        .await
        .unwrap();
    assert_eq!(r.affected_rows, 5);

    // Aggregate over the index.
    let rs = session
        .query("SELECT COUNT(*) AS n FROM posts", &[])
        .await
        .unwrap();
    assert_eq!(rs.columns[0].name, "n");
    assert_eq!(rs.rows[0][0], Value::Integer(11));

    // Rollback leaves no trace.
    session.execute("BEGIN", &[]).await.unwrap();
    session.execute("DELETE FROM posts", &[]).await.unwrap();
    session.execute("ROLLBACK", &[]).await.unwrap();
    let rs = session
        .query("SELECT COUNT(*) FROM posts", &[])
        .await
        .unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(11));
}

#[tokio::test]
async fn two_sessions_do_not_share_transactions() {
    let backend = shared_backend().await;
    backend
        .execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)", &[])
        .await
        .unwrap();

    let a = backend.connect().await.unwrap();
    let b = backend.connect().await.unwrap();

    a.execute("BEGIN", &[]).await.unwrap();
    a.execute(
        "INSERT INTO kv VALUES (?1, ?2)",
        &[Value::Text("k1".into()), Value::Text("uncommitted".into())],
    )
    .await
    .unwrap();

    // B must not see A's open transaction.
    let rs = b.query("SELECT COUNT(*) FROM kv", &[]).await.unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(0));

    a.execute("COMMIT", &[]).await.unwrap();

    let rs = b.query("SELECT COUNT(*) FROM kv", &[]).await.unwrap();
    assert_eq!(rs.rows[0][0], Value::Integer(1));
}
