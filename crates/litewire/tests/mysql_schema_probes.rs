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

/// litewire collapses every column type to a SQLite storage affinity, so the
/// catalog reports `bigint` / `double` / `longtext` / `longblob` rather than
/// the declared MySQL type — width, decimal precision, `unsigned` and
/// enum/set members do not survive CREATE TABLE. The one carve-out is MySQL's
/// boolean idiom `tinyint(1)` / `BOOL` / `BOOLEAN`, kept as `tinyint(1)` so
/// ORMs can cast the column to bool. This pins both: the carve-out works, and
/// the integer-PK `AUTO_INCREMENT` (SQLite rowid alias, needs the PK column to
/// stay exactly `INTEGER`) is unaffected by it.
async fn column_type_fidelity(backend: SharedBackend) {
    let mut c = serve(backend).await;
    c.query_drop(
        "CREATE TABLE widgets (\
         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         active TINYINT(1) DEFAULT 0, \
         is_admin BOOLEAN DEFAULT 0, \
         width INT(1), \
         qty INT, \
         big BIGINT, \
         price DECIMAL(10,2), \
         name VARCHAR(190))",
    )
    .await
    .unwrap();

    let rows: Vec<Row> = c
        .query(
            "SELECT COLUMN_NAME AS n, DATA_TYPE AS d, COLUMN_TYPE AS ct \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'widgets' \
             ORDER BY ORDINAL_POSITION",
        )
        .await
        .unwrap();
    let got: std::collections::HashMap<String, (String, String)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("n").unwrap(),
                (
                    r.get::<String, _>("d").unwrap(),
                    r.get::<String, _>("ct").unwrap(),
                ),
            )
        })
        .collect();

    // the boolean carve-out: `tinyint(1)` and `BOOL`/`BOOLEAN` report as such.
    assert_eq!(got["active"], ("tinyint".into(), "tinyint(1)".into()));
    assert_eq!(got["is_admin"], ("tinyint".into(), "tinyint(1)".into()));
    // everything else still collapses to the affinity's MySQL type. `int(1)`
    // is NOT the boolean idiom (only `tinyint(1)` is) — it stays bigint.
    assert_eq!(got["width"], ("bigint".into(), "bigint".into()));
    assert_eq!(got["id"], ("bigint".into(), "bigint".into()));
    assert_eq!(got["qty"], ("bigint".into(), "bigint".into()));
    assert_eq!(got["big"], ("bigint".into(), "bigint".into()));
    assert_eq!(got["price"], ("double".into(), "double".into()));
    assert_eq!(got["name"], ("longtext".into(), "longtext".into()));

    // SHOW COLUMNS reads the same path.
    let rows: Vec<Row> = c
        .query("SHOW COLUMNS FROM widgets WHERE Field = 'active'")
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("Type").unwrap(), "tinyint(1)");

    // the boolean carve-out must NOT disturb integer-PK AUTO_INCREMENT: `id`
    // stays an `INTEGER` rowid alias, so inserts auto-assign and climb.
    assert_eq!(
        strings(
            &mut c,
            "SHOW COLUMNS FROM widgets WHERE Field = 'id'",
            "Extra"
        )
        .await,
        ["auto_increment"]
    );
    c.query_drop("INSERT INTO widgets (active) VALUES (1)")
        .await
        .unwrap();
    let first = c.last_insert_id().unwrap();
    c.query_drop("INSERT INTO widgets (active) VALUES (0)")
        .await
        .unwrap();
    assert_eq!(c.last_insert_id(), Some(first + 1), "AUTO_INCREMENT climbs");
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
async fn column_type_fidelity_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    column_type_fidelity(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn column_type_fidelity_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    column_type_fidelity(backend).await;
}
