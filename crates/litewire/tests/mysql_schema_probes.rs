//! End-to-end: the schema probes self-migrating PHP applications run before
//! altering their schema must see only the rows their filter selects.
//!
//! The pattern under test (written fresh here, modelled on what installers
//! and updaters commonly do):
//!
//! ```php
//! $r = mysqli_query($db, "SHOW INDEX FROM subscribers WHERE Key_name = 's_list'");
//! if (mysqli_num_rows($r) == 0) mysqli_query($db, "CREATE INDEX s_list ON subscribers (list)");
//! ```
//!
//! When the filter was dropped, every table answered with at least its
//! PRIMARY row, the guard concluded the index existed and the migration was
//! silently skipped. The same held for `SHOW COLUMNS ... WHERE Field = 'x'`
//! (column never added), `SHOW TABLES LIKE 'x'` and
//! `information_schema.tables WHERE table_name = 'x'`.
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`.

use std::net::SocketAddr;

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Row};
use tokio::net::TcpListener;

async fn serve(backend: SharedBackend) -> Conn {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig {
        listen: addr,
        max_connections: 0,
        log_failed_sql: false,
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

async fn count(conn: &mut Conn, sql: &str) -> usize {
    let rows: Vec<Row> = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.len()
}

async fn strings(conn: &mut Conn, sql: &str, col: &str) -> Vec<String> {
    let rows: Vec<Row> = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter()
        .map(|r| {
            r.get_opt::<String, _>(col)
                .unwrap_or_else(|| panic!("{sql}: no column {col}"))
                .unwrap_or_else(|e| panic!("{sql}: {col}: {e}"))
        })
        .collect()
}

async fn must_fail(conn: &mut Conn, sql: &str) {
    if let Ok(rows) = conn.query::<Row, _>(sql).await {
        panic!("{sql} must be an error, got {} rows", rows.len());
    }
}

async fn schema(conn: &mut Conn) {
    for sql in [
        "CREATE TABLE subscribers (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         email VARCHAR(100), list INT(11), name VARCHAR(100))",
        "CREATE INDEX s_list ON subscribers (list)",
        "CREATE UNIQUE INDEX s_email_list ON subscribers (email, list)",
        "CREATE UNIQUE INDEX s_name ON subscribers (name)",
        "CREATE TABLE lists (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, title TEXT)",
        "CREATE TABLE skipped_emails (id INT PRIMARY KEY, list INT)",
    ] {
        conn.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

async fn probes(backend: SharedBackend) {
    let mut c = serve(backend).await;
    schema(&mut c).await;

    // ── SHOW INDEX ... WHERE ────────────────────────────────────────────
    assert_eq!(
        count(
            &mut c,
            "SHOW INDEX FROM subscribers WHERE KEY_NAME = 's_list'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &mut c,
            "SHOW INDEX FROM subscribers WHERE Key_name = 'S_LIST'"
        )
        .await,
        1,
        "index names compare case-insensitively, as in MySQL"
    );
    assert_eq!(
        count(
            &mut c,
            "SHOW INDEX FROM subscribers WHERE Key_name = 's_missing'"
        )
        .await,
        0,
        "a missing index must not be reported as present"
    );
    assert_eq!(
        count(&mut c, "SHOW INDEX FROM lists WHERE Key_name = 's_list'").await,
        0,
        "indexes of other tables must not leak in"
    );
    assert_eq!(
        count(
            &mut c,
            "SHOW INDEX FROM subscribers WHERE Key_name = 'PRIMARY'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&mut c, "SHOW KEYS FROM subscribers WHERE Non_unique = 0").await,
        4,
        "PRIMARY(1) + s_email_list(2) + s_name(1)"
    );
    assert_eq!(
        count(
            &mut c,
            "SHOW INDEXES FROM subscribers WHERE Key_name IN ('s_list', 's_name') OR Column_name LIKE 'em%'"
        )
        .await,
        3
    );

    // Full SHOW INDEX shape and order: PRIMARY first, then creation order.
    let keys = strings(&mut c, "SHOW INDEX FROM subscribers", "Key_name").await;
    assert_eq!(
        keys,
        [
            "PRIMARY",
            "s_list",
            "s_email_list",
            "s_email_list",
            "s_name"
        ]
    );
    let cols = strings(&mut c, "SHOW INDEX FROM subscribers", "Column_name").await;
    assert_eq!(cols, ["id", "list", "email", "list", "name"]);
    let rows: Vec<Row> = c
        .query("SHOW INDEX FROM subscribers WHERE Key_name = 's_email_list'")
        .await
        .unwrap();
    let seq: Vec<i64> = rows
        .iter()
        .map(|r| r.get("Seq_in_index").unwrap())
        .collect();
    assert_eq!(seq, [1, 2]);
    let nu: i64 = rows[0].get("Non_unique").unwrap();
    assert_eq!(nu, 0);

    // ── SHOW COLUMNS ... WHERE / LIKE, DESCRIBE t col ───────────────────
    assert_eq!(
        count(
            &mut c,
            "SHOW COLUMNS FROM subscribers WHERE Field = 'email'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &mut c,
            "SHOW COLUMNS FROM subscribers WHERE Field = 'db_updated'"
        )
        .await,
        0,
        "a missing column must not be reported as present"
    );
    assert_eq!(
        count(&mut c, "SHOW COLUMNS FROM skipped_emails LIKE 'list'").await,
        1
    );
    assert_eq!(
        count(&mut c, "SHOW FULL COLUMNS FROM subscribers LIKE 'e%'").await,
        1
    );
    assert_eq!(count(&mut c, "DESCRIBE subscribers name").await, 1);
    assert_eq!(count(&mut c, "DESCRIBE subscribers nope").await, 0);
    let keys = strings(&mut c, "SHOW COLUMNS FROM subscribers", "Key").await;
    assert_eq!(keys, ["PRI", "MUL", "MUL", "UNI"]);
    let extra = strings(&mut c, "SHOW COLUMNS FROM subscribers", "Extra").await;
    assert_eq!(extra[0], "auto_increment");

    // ── SHOW TABLES LIKE / WHERE ─────────────────────────────────────────
    assert_eq!(count(&mut c, "SHOW TABLES LIKE 'lists'").await, 1);
    assert_eq!(count(&mut c, "SHOW TABLES LIKE 'nope'").await, 0);
    assert_eq!(count(&mut c, "SHOW TABLES LIKE 's%'").await, 2);
    assert_eq!(
        count(&mut c, "SHOW FULL TABLES WHERE Tables_in_shop = 'lists'").await,
        1
    );
    let all = strings(&mut c, "SHOW TABLES", "Tables_in_database").await;
    assert_eq!(all, ["lists", "skipped_emails", "subscribers"]);

    // ── information_schema runs the client's own predicate ───────────────
    assert_eq!(
        count(
            &mut c,
            "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'nope'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &mut c,
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'shop' AND table_name = 'lists'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &mut c,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mysql'"
        )
        .await,
        0
    );
    let rf = strings(
        &mut c,
        "SELECT row_format FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name='lists' LIMIT 1",
        "row_format",
    )
    .await;
    assert_eq!(rf, ["Dynamic"]);
    assert_eq!(
        count(
            &mut c,
            "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'subscribers' AND column_name = 'email'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &mut c,
            "SELECT * FROM information_schema.statistics WHERE table_name = 'subscribers' AND index_name = 's_list'"
        )
        .await,
        1
    );
    let exists: Vec<Row> = c
        .query("select exists (select 1 from information_schema.tables where table_schema = schema() and table_name = 'lists') as `exists`")
        .await
        .unwrap();
    assert_eq!(exists.len(), 1);
    let v: i64 = exists[0].get(0).unwrap();
    assert_eq!(v, 1);
    // A bound schema name (Laravel <= 10) keeps the parameter count.
    let bound: Vec<Row> = c
        .exec(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            ("shop", "lists"),
        )
        .await
        .unwrap();
    assert_eq!(bound.len(), 1);

    // ── Unsupported forms are errors, never unfiltered sets ──────────────
    must_fail(&mut c, "SHOW INDEX FROM subscribers LIKE 's_list'").await;
    must_fail(&mut c, "SHOW VARIABLES LIKE 'sql_mode'").await;
    must_fail(&mut c, "SHOW DATABASES LIKE 'shop'").await;
    must_fail(&mut c, "SELECT * FROM information_schema.routines").await;
    must_fail(&mut c, "SHOW COLUMNS FROM subscribers WHERE nosuchcol = 1").await;

    // The connection is still usable after the errors.
    assert_eq!(count(&mut c, "SHOW TABLES").await, 3);
}

/// One column's reported default: `COLUMN_DEFAULT IS NULL` (SQL NULL?),
/// `COLUMN_DEFAULT = 'NULL'` (the literal string "NULL"?), and the value.
struct ColDefault {
    real_null: Option<i64>,
    str_null: Option<i64>,
    value: Option<String>,
}

/// `COLUMN_DEFAULT` / `SHOW COLUMNS`.`Default` reports SQL NULL for a `NULL`
/// or absent default (not the literal string "NULL"), and the unquoted value
/// for a string default. SQLite's `pragma_table_info` returns `dflt_value` as
/// the DEFAULT clause's source text, so `DEFAULT NULL` would otherwise surface
/// as the 4-char string "NULL" — and since mysqldump emits `DEFAULT NULL` on
/// every nullable column, an imported schema would report "NULL" for nearly
/// every nullable column. ORMs that seed new-record form fields from the
/// column default then write that string back (observed in CakePHP 5).
async fn column_default(backend: SharedBackend) {
    let mut c = serve(backend).await;
    c.query_drop(
        "CREATE TABLE posts (\
         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         url VARCHAR(255) DEFAULT NULL, \
         title VARCHAR(255), \
         status VARCHAR(20) DEFAULT 'active', \
         greeting VARCHAR(50) DEFAULT 'it''s', \
         nullish VARCHAR(10) DEFAULT 'NULL', \
         created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
         note VARCHAR(50) DEFAULT '', \
         qty INT DEFAULT 0)",
    )
    .await
    .unwrap();

    // The mysql client renders SQL NULL and the string "NULL" identically, so
    // assert on server-side booleans: real_null=1 => correct SQL NULL,
    // str_null=1 => the bug (the literal string "NULL").
    let rows: Vec<Row> = c
        .query(
            "SELECT COLUMN_NAME AS n, \
             (COLUMN_DEFAULT IS NULL) AS real_null, \
             (COLUMN_DEFAULT = 'NULL') AS str_null, \
             COLUMN_DEFAULT AS dflt \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'posts' \
             ORDER BY ORDINAL_POSITION",
        )
        .await
        .unwrap();
    let got: std::collections::HashMap<String, ColDefault> = rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("n").unwrap(),
                ColDefault {
                    real_null: r.get_opt::<Option<i64>, _>("real_null").unwrap().unwrap(),
                    str_null: r.get_opt::<Option<i64>, _>("str_null").unwrap().unwrap(),
                    value: r.get_opt::<Option<String>, _>("dflt").unwrap().unwrap(),
                },
            )
        })
        .collect();

    // genuine no-default (SQLite dflt_value already NULL) — already correct.
    assert_eq!(
        got["id"].real_null,
        Some(1),
        "id: absent default is SQL NULL"
    );
    assert_eq!(got["id"].value, None);
    // explicit `DEFAULT NULL` — the reported bug: must be SQL NULL, not "NULL".
    assert_eq!(
        got["url"].real_null,
        Some(1),
        "url: DEFAULT NULL must be SQL NULL"
    );
    assert_ne!(
        got["url"].str_null,
        Some(1),
        "url: must not be the string \"NULL\""
    );
    assert_eq!(got["url"].value, None);
    // a nullable column with no default clause — also SQL NULL.
    assert_eq!(
        got["title"].real_null,
        Some(1),
        "title: no default is SQL NULL"
    );
    // string literal default — unquoted contents.
    assert_eq!(got["status"].real_null, Some(0));
    assert_eq!(got["status"].value.as_deref(), Some("active"));
    // embedded quote — SQLite's doubled '' unescaped to a single '.
    assert_eq!(got["greeting"].value.as_deref(), Some("it's"));
    // a string literal whose CONTENT is the text NULL (`DEFAULT 'NULL'`) stays
    // the string "NULL" — it must NOT become SQL NULL. This is the arm-ordering
    // property: the bare-`NULL` branch only fires on the unquoted token, so the
    // quoted literal falls through to quote-stripping instead.
    assert_eq!(
        got["nullish"].real_null,
        Some(0),
        "nullish: DEFAULT 'NULL' is the string, not SQL NULL"
    );
    assert_eq!(got["nullish"].str_null, Some(1));
    assert_eq!(got["nullish"].value.as_deref(), Some("NULL"));
    // an expression default (`CURRENT_TIMESTAMP`) passes through untouched — not
    // quoted, not the bare NULL token, so neither branch rewrites it.
    assert_eq!(
        got["created"].real_null,
        Some(0),
        "created: CURRENT_TIMESTAMP default is not SQL NULL"
    );
    assert_eq!(
        got["created"].value.as_deref().map(str::to_ascii_uppercase),
        Some("CURRENT_TIMESTAMP".to_string()),
        "created: CURRENT_TIMESTAMP must pass through unchanged"
    );
    // empty-string default stays an empty string, not NULL.
    assert_eq!(
        got["note"].real_null,
        Some(0),
        "note: '' default is not NULL"
    );
    assert_eq!(got["note"].value.as_deref(), Some(""));
    // numeric default passes through as text.
    assert_eq!(got["qty"].real_null, Some(0));
    assert_eq!(got["qty"].value.as_deref(), Some("0"));

    // `SHOW FULL COLUMNS`.`Default` reads the same emulation.
    let rows: Vec<Row> = c.query("SHOW FULL COLUMNS FROM posts").await.unwrap();
    let defaults: std::collections::HashMap<String, Option<String>> = rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("Field").unwrap(),
                r.get_opt::<Option<String>, _>("Default").unwrap().unwrap(),
            )
        })
        .collect();
    assert_eq!(defaults["url"], None, "SHOW COLUMNS: DEFAULT NULL -> NULL");
    assert_eq!(
        defaults["nullish"].as_deref(),
        Some("NULL"),
        "SHOW COLUMNS: DEFAULT 'NULL' -> the string"
    );
    assert_eq!(defaults["status"].as_deref(), Some("active"));
    assert_eq!(defaults["greeting"].as_deref(), Some("it's"));
    assert_eq!(defaults["qty"].as_deref(), Some("0"));
}

#[tokio::test]
async fn schema_probes_filter_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    probes(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn schema_probes_filter_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    probes(backend).await;
}

#[tokio::test]
async fn column_default_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    column_default(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn column_default_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    column_default(backend).await;
}
