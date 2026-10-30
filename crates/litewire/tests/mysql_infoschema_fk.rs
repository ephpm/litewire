//! End-to-end: `information_schema.key_column_usage` and
//! `information_schema.referential_constraints`, as CakePHP 5's
//! `MysqlSchemaDialect::describeForeignKeys()` reads them.
//!
//! Before they were emulated, every Cake table reflection failed with
//! `1064 information_schema.key_column_usage is not emulated by litewire`.
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`.

use std::net::SocketAddr;

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Row};
use tokio::net::TcpListener;

/// Verbatim from `Cake\Database\Schema\MysqlSchemaDialect::describeForeignKeys()`.
const CAKE_FK_SQL: &str = "SELECT * FROM information_schema.key_column_usage AS kcu
            INNER JOIN information_schema.referential_constraints AS rc
            ON (
                kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
            )
            WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ? AND rc.TABLE_NAME = ?
            ORDER BY kcu.ORDINAL_POSITION ASC";

/// Verbatim from `Cake\Database\Schema\MysqlSchemaDialect::describeCheckConstraints()`
/// (CakePHP 5.4.3).
const CAKE_CHECK_SQL: &str = "SELECT
        cc.CONSTRAINT_NAME AS name,
        cc.CHECK_CLAUSE AS expression
        FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS cc
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc ON (
            tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
        )
        WHERE tc.CONSTRAINT_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'CHECK'";

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

fn column_names(rows: &[Row]) -> Vec<String> {
    rows[0]
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect()
}

fn s(row: &Row, col: &str) -> String {
    row.get_opt::<String, _>(col)
        .unwrap_or_else(|| panic!("no column {col}"))
        .unwrap_or_else(|e| panic!("{col}: {e}"))
}

async fn fk_views(backend: SharedBackend) {
    let mut c = serve(backend).await;
    for sql in [
        "CREATE TABLE categories (id INT NOT NULL PRIMARY KEY, name TEXT)",
        "CREATE TABLE regions (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
        "CREATE TABLE products (id INT NOT NULL PRIMARY KEY, category_id INT, \
         ra INT, rb INT, \
         FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE CASCADE ON UPDATE SET NULL, \
         FOREIGN KEY (ra, rb) REFERENCES regions)",
        "CREATE TABLE plain (id INT NOT NULL PRIMARY KEY)",
    ] {
        c.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // A table without foreign keys: well-formed, empty (Cake's common case).
    let rows: Vec<Row> = c
        .exec(CAKE_FK_SQL, ("shop", "plain", "plain"))
        .await
        .unwrap();
    assert!(rows.is_empty());

    // A table with two foreign keys, one of them composite and implicit.
    let rows: Vec<Row> = c
        .exec(CAKE_FK_SQL, ("shop", "products", "products"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "one row per FK column");
    let cols = column_names(&rows);
    for want in [
        "CONSTRAINT_NAME",
        "CONSTRAINT_SCHEMA",
        "TABLE_NAME",
        "COLUMN_NAME",
        "ORDINAL_POSITION",
        "REFERENCED_TABLE_NAME",
        "REFERENCED_COLUMN_NAME",
        "UPDATE_RULE",
        "DELETE_RULE",
    ] {
        assert!(cols.iter().any(|c| c == want), "missing {want}: {cols:?}");
    }
    let by_col = |name: &str| rows.iter().find(|r| s(r, "COLUMN_NAME") == name).unwrap();
    let cat = by_col("category_id");
    assert_eq!(s(cat, "TABLE_NAME"), "products");
    assert_eq!(s(cat, "REFERENCED_TABLE_NAME"), "categories");
    assert_eq!(s(cat, "REFERENCED_COLUMN_NAME"), "id");
    assert_eq!(s(cat, "DELETE_RULE"), "CASCADE");
    assert_eq!(s(cat, "UPDATE_RULE"), "SET NULL");
    let ra = by_col("ra");
    let rb = by_col("rb");
    assert_eq!(s(ra, "CONSTRAINT_NAME"), s(rb, "CONSTRAINT_NAME"));
    assert_ne!(s(ra, "CONSTRAINT_NAME"), s(cat, "CONSTRAINT_NAME"));
    assert_eq!(s(ra, "REFERENCED_TABLE_NAME"), "regions");
    assert_eq!(s(ra, "REFERENCED_COLUMN_NAME"), "a");
    assert_eq!(s(rb, "REFERENCED_COLUMN_NAME"), "b");
    assert_eq!(s(ra, "DELETE_RULE"), "NO ACTION");

    // Plain selects from each view carry MySQL's column names.
    let kcu: Vec<Row> = c
        .query("SELECT * FROM information_schema.key_column_usage")
        .await
        .unwrap();
    assert_eq!(kcu.len(), 3);
    assert_eq!(
        column_names(&kcu),
        [
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "POSITION_IN_UNIQUE_CONSTRAINT",
            "REFERENCED_TABLE_SCHEMA",
            "REFERENCED_TABLE_NAME",
            "REFERENCED_COLUMN_NAME",
        ]
    );
    let rc: Vec<Row> = c
        .query("SELECT * FROM information_schema.referential_constraints")
        .await
        .unwrap();
    assert_eq!(rc.len(), 2, "one row per constraint");
    assert_eq!(
        column_names(&rc),
        [
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "UNIQUE_CONSTRAINT_CATALOG",
            "UNIQUE_CONSTRAINT_SCHEMA",
            "UNIQUE_CONSTRAINT_NAME",
            "MATCH_OPTION",
            "UPDATE_RULE",
            "DELETE_RULE",
            "TABLE_NAME",
            "REFERENCED_TABLE_NAME",
        ]
    );
    let none: Vec<Row> = c
        .query(
            "SELECT * FROM information_schema.referential_constraints WHERE table_name = 'plain'",
        )
        .await
        .unwrap();
    assert!(none.is_empty());

    // Cake 5.4's check-constraint reflection (MySQL >= 8.0.16 branch): joins
    // CHECK_CONSTRAINTS to TABLE_CONSTRAINTS on a bound schema name. Always
    // empty here, but it must parse, bind and carry its aliases.
    let checks: Vec<Row> = c.exec(CAKE_CHECK_SQL, ("shop", "products")).await.unwrap();
    assert!(checks.is_empty());

    // TABLE_CONSTRAINTS lists primary and foreign keys.
    let tc: Vec<Row> = c
        .exec(
            "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.table_constraints \
             WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ? ORDER BY CONSTRAINT_TYPE, CONSTRAINT_NAME",
            ("shop", "products"),
        )
        .await
        .unwrap();
    let tc: Vec<(String, String)> = tc
        .iter()
        .map(|r| (s(r, "CONSTRAINT_NAME"), s(r, "CONSTRAINT_TYPE")))
        .collect();
    assert_eq!(tc.len(), 3, "{tc:?}");
    assert_eq!(tc.iter().filter(|t| t.1 == "FOREIGN KEY").count(), 2);
    assert!(tc.contains(&("PRIMARY".into(), "PRIMARY KEY".into())));
}

#[tokio::test]
async fn fk_views_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    fk_views(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn fk_views_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    fk_views(backend).await;
}
