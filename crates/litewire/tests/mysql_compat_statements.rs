//! End-to-end: MySQL statement shapes PHP applications use that SQLite does
//! not accept as written — `INSERT ... SELECT ... FROM DUAL WHERE NOT
//! EXISTS`, index hints, locking reads, `LIMIT 18446744073709551615`,
//! table-option-only `ALTER TABLE`, `LEAST`/`GREATEST`/`CHAR_LENGTH`.
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`. The checks are on results and affected-row counts, not on the
//! translated SQL.

use std::net::SocketAddr;

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::net::TcpListener;

async fn serve(backend: SharedBackend) -> Conn {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig {
        listen: addr,
        max_connections: 0,
    };
    let frontend = litewire::litewire_mysql::MysqlFrontend::new(config, backend);
    tokio::spawn(async move {
        frontend.serve().await.unwrap();
    });
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some(""))
        .db_name(Some("shop"))
        .into();
    for _ in 0..40 {
        if let Ok(c) = Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("litewire did not come up");
}

async fn exec(c: &mut Conn, sql: &str) -> u64 {
    c.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.affected_rows()
}

async fn ids(c: &mut Conn, sql: &str) -> Vec<i64> {
    c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn statements(backend: SharedBackend) {
    let mut c = serve(backend).await;
    exec(
        &mut c,
        "CREATE TABLE queue (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         campaign_id INT(11), sent INT(1) DEFAULT 0, email VARCHAR(100))",
    )
    .await;
    exec(
        &mut c,
        "CREATE INDEX q_campaign_sent_id ON queue (campaign_id, sent, id)",
    )
    .await;

    // INSERT ... SELECT ... FROM DUAL WHERE NOT EXISTS: affected rows is 1
    // when the row is inserted and 0 when the guard suppresses it.
    let insert_unless_present = "INSERT INTO queue (campaign_id, sent, email) \
        SELECT 7, 0, 'a@example.com' FROM DUAL \
        WHERE NOT EXISTS (SELECT 1 FROM queue WHERE campaign_id = 7 AND email = 'a@example.com')";
    assert_eq!(exec(&mut c, insert_unless_present).await, 1);
    assert_eq!(exec(&mut c, insert_unless_present).await, 0);
    assert_eq!(ids(&mut c, "SELECT COUNT(*) FROM queue").await, [1]);
    for n in 2..=5 {
        exec(
            &mut c,
            &format!(
                "INSERT INTO queue (campaign_id, sent, email) VALUES (7, 0, 'u{n}@example.com')"
            ),
        )
        .await;
    }

    // Index hints never fail, existing index or not.
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue FORCE INDEX (q_campaign_sent_id) WHERE campaign_id = 7 AND sent = 0 ORDER BY id ASC"
        )
        .await,
        [1, 2, 3, 4, 5]
    );
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue USE INDEX (no_such_index) WHERE id = 3"
        )
        .await,
        [3]
    );
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue IGNORE INDEX (PRIMARY) WHERE id = 4"
        )
        .await,
        [4]
    );

    // LIMIT 18446744073709551615 OFFSET n: everything after the offset.
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue ORDER BY id ASC LIMIT 18446744073709551615 OFFSET 2"
        )
        .await,
        [3, 4, 5]
    );
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue ORDER BY id ASC LIMIT 3, 18446744073709551615"
        )
        .await,
        [4, 5]
    );

    // Locking reads return their rows, inside and outside a transaction.
    exec(&mut c, "START TRANSACTION").await;
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue WHERE campaign_id = 7 AND id < 3 FOR UPDATE"
        )
        .await,
        [1, 2]
    );
    assert_eq!(
        exec(
            &mut c,
            "UPDATE queue SET sent = 1 WHERE campaign_id = 7 AND id < 3"
        )
        .await,
        2
    );
    exec(&mut c, "COMMIT").await;
    assert_eq!(
        ids(
            &mut c,
            "SELECT id FROM queue WHERE sent = 1 LOCK IN SHARE MODE"
        )
        .await,
        [1, 2]
    );
    assert_eq!(
        ids(&mut c, "SELECT id FROM queue WHERE sent = 1 FOR SHARE").await,
        [1, 2]
    );

    // Table-option-only ALTERs succeed and change nothing.
    for sql in [
        "ALTER TABLE queue ROW_FORMAT=DYNAMIC",
        "ALTER TABLE queue ENGINE=InnoDB",
        "ALTER TABLE queue CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "ALTER TABLE queue AUTO_INCREMENT=1000",
    ] {
        exec(&mut c, sql).await;
    }
    assert_eq!(ids(&mut c, "SELECT COUNT(*) FROM queue").await, [5]);

    // Scalar functions under their SQLite names.
    assert_eq!(
        ids(
            &mut c,
            "SELECT LEAST(campaign_id, 3) + GREATEST(1, 2, sent) + CHAR_LENGTH('h\u{e9}llo') FROM queue WHERE id = 1"
        )
        .await,
        [3 + 2 + 5]
    );
}

#[tokio::test]
async fn compat_statements_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    statements(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn compat_statements_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    statements(backend).await;
}
