//! End-to-end: restore a MySQL 8.4 `mysqldump` through the MySQL frontend.
//!
//! `fixtures/mysqldump_8_4.sql` is unedited `mysqldump --hex-blob` output
//! (MySQL 8.4.11) of a small synthetic schema written for this test. It has
//! the features real dumps carry: `/*!NNNNN ... */` conditional comments,
//! `LOCK TABLES` / `UNLOCK TABLES`, `DISABLE KEYS` / `ENABLE KEYS`, table
//! options (`ENGINE`, `AUTO_INCREMENT=n`, `ROW_FORMAT`, `COMMENT`, mixed
//! `utf8mb3`/`utf8mb4` charsets and collations at table and column level),
//! column `CHARACTER SET` / `COLLATE` / `COMMENT` / `ON UPDATE
//! CURRENT_TIMESTAMP`, `ENUM`, `DECIMAL`, inline `KEY` / `UNIQUE KEY` (incl.
//! a prefix length) — with the same key name, `list_idx`, on two tables —
//! extended INSERTs, and string escapes.
//!
//! The statements are sent one at a time, split the way the `mysql` client
//! splits a script. A ~700 KB extended INSERT is added to exercise a large
//! packet.
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`.

use std::net::SocketAddr;

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Row};
use tokio::net::TcpListener;

const DUMP: &str = include_str!("fixtures/mysqldump_8_4.sql");

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
        .db_name(Some("dumpsrc"))
        .into();
    for _ in 0..40 {
        if let Ok(c) = Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("litewire did not come up");
}

/// Split a dump into statements the way the `mysql` client does for this
/// shape of input: `--` comment lines and blank lines are dropped, and a
/// statement ends at a line ending in `;`.
fn statements(dump: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in dump.lines() {
        let line = line.trim_end_matches('\r');
        if cur.is_empty() && (line.trim().is_empty() || line.starts_with("--")) {
            continue;
        }
        if !cur.is_empty() {
            cur.push('\n');
        }
        cur.push_str(line);
        if line.trim_end().ends_with(';') {
            let stmt = cur.trim_end().trim_end_matches(';').to_string();
            out.push(stmt);
            cur.clear();
        }
    }
    assert!(cur.trim().is_empty(), "unterminated statement: {cur}");
    out
}

async fn strings(c: &mut Conn, sql: &str, col: &str) -> Vec<String> {
    let rows: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter()
        .map(|r| {
            r.get::<String, _>(col)
                .unwrap_or_else(|| panic!("{sql}: {col}"))
        })
        .collect()
}

async fn restore(backend: SharedBackend) {
    let mut c = serve(backend).await;

    let stmts = statements(DUMP);
    assert!(
        stmts.len() > 30,
        "fixture split into {} statements",
        stmts.len()
    );
    for stmt in &stmts {
        c.query_drop(stmt.as_str())
            .await
            .unwrap_or_else(|e| panic!("restore failed at:\n{stmt}\n=> {e}"));
    }

    // A large extended INSERT (~700 KB packet), as mysqldump writes for big
    // tables.
    let rows: Vec<String> = (0..3000)
        .map(|n| {
            format!(
                "({},3,'bulk{n}@x.com','{}',NULL,NULL)",
                100 + n,
                "y".repeat(200)
            )
        })
        .collect();
    let big = format!("INSERT INTO `members` VALUES {}", rows.join(","));
    assert!(big.len() > 600_000);
    c.query_drop(big).await.unwrap();

    // ── Schema ──────────────────────────────────────────────────────────
    assert_eq!(
        strings(&mut c, "SHOW TABLES", "Tables_in_database").await,
        ["accounts", "lists", "members"]
    );
    assert_eq!(
        strings(&mut c, "SHOW INDEX FROM accounts", "Key_name").await,
        [
            "PRIMARY",
            "email_unique",
            "name_idx",
            "active_created",
            "active_created"
        ]
    );
    // The same key name on two tables: both exist, each on its own table.
    assert_eq!(
        strings(&mut c, "SHOW INDEX FROM lists", "Key_name").await,
        ["PRIMARY", "account", "list_idx"]
    );
    assert_eq!(
        strings(&mut c, "SHOW INDEX FROM members", "Key_name").await,
        ["PRIMARY", "list_email", "list_email", "list_idx"]
    );
    assert_eq!(
        strings(
            &mut c,
            "SHOW INDEX FROM members WHERE Key_name = 'list_idx'",
            "Column_name"
        )
        .await,
        ["list"]
    );
    let rows: Vec<Row> = c
        .query("SHOW INDEX FROM accounts WHERE Key_name = 'email_unique'")
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64, _>("Non_unique"), Some(0));
    assert_eq!(
        strings(&mut c, "SHOW COLUMNS FROM accounts", "Field").await,
        [
            "id", "name", "email", "active", "balance", "notes", "created", "updated", "kind"
        ]
    );

    // ── Data ────────────────────────────────────────────────────────────
    assert_eq!(
        strings(&mut c, "SELECT name FROM accounts ORDER BY id", "name").await,
        [
            "Zo\u{eb} O'Brien",
            "\u{dc}n\u{ef}c\u{f6}d\u{e9} \u{65e5}\u{672c}\u{8a9e}",
            "plain"
        ]
    );
    assert_eq!(
        strings(&mut c, "SELECT notes FROM accounts WHERE id = 1", "notes").await,
        ["quote ' and \"double\" and back\\slash"]
    );
    assert_eq!(
        strings(&mut c, "SELECT notes FROM accounts WHERE id = 2", "notes").await,
        ["newline\nand tab\t and NUL-free"]
    );
    let blob: Vec<Vec<u8>> = c
        .query("SELECT blobby FROM members WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(blob, [vec![0x00, 0xFF, 0x10]]);
    let n: Vec<i64> = c.query("SELECT COUNT(*) FROM members").await.unwrap();
    assert_eq!(n, [3003]);

    // ── Restored keys are enforced ──────────────────────────────────────
    let dup = c
        .query_drop("INSERT INTO members (list, email) VALUES (1, 'a@x.com')")
        .await;
    match dup {
        Err(mysql_async::Error::Server(e)) => assert_eq!(e.code, 1062, "{e}"),
        other => panic!("duplicate (list, email) must fail with 1062, got {other:?}"),
    }
    // AUTO_INCREMENT continues after the restored ids.
    c.query_drop("INSERT INTO lists (account, title) VALUES (3, 'new')")
        .await
        .unwrap();
    assert_eq!(c.last_insert_id(), Some(4));
}

/// An index an older litewire created under its bare MySQL name (before
/// names were scoped to their table) can still be dropped with MySQL
/// syntax, and is reported under that name.
async fn legacy_index_names(backend: SharedBackend) {
    backend
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)", &[])
        .await
        .unwrap();
    backend
        .execute("CREATE INDEX a_idx ON t (a)", &[])
        .await
        .unwrap();
    let mut c = serve(backend).await;
    assert_eq!(
        strings(&mut c, "SHOW INDEX FROM t", "Key_name").await,
        ["PRIMARY", "a_idx"]
    );
    c.query_drop("ALTER TABLE t DROP INDEX a_idx")
        .await
        .unwrap();
    assert_eq!(
        strings(&mut c, "SHOW INDEX FROM t", "Key_name").await,
        ["PRIMARY"]
    );
    // A missing index is still an error.
    assert!(c.query_drop("DROP INDEX a_idx ON t").await.is_err());
    // Charset introducers (`_binary '...'`, `_utf8mb4'...'`) as older
    // mysqldump versions write for BLOB columns.
    c.query_drop("CREATE TABLE b (v BLOB, s TEXT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO b VALUES (_binary 'abc', _utf8mb4'x\u{e9}')")
        .await
        .unwrap();
    let rows: Vec<(Vec<u8>, String)> = c.query("SELECT v, s FROM b").await.unwrap();
    assert_eq!(rows, [(b"abc".to_vec(), "x\u{e9}".to_string())]);
}

#[tokio::test]
async fn legacy_index_names_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    legacy_index_names(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn legacy_index_names_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    legacy_index_names(backend).await;
}

#[test]
fn splitter_sees_every_statement() {
    let stmts = statements(DUMP);
    assert!(
        stmts
            .iter()
            .any(|s| s.starts_with("CREATE TABLE `members`"))
    );
    assert!(stmts.iter().all(|s| !s.ends_with(';')));
}

#[tokio::test]
async fn mysqldump_restores_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    restore(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn mysqldump_restores_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    restore(backend).await;
}
