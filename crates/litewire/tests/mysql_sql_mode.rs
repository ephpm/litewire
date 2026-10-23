//! End-to-end: `sql_mode` reporting, and the non-strict behaviours that
//! differ from MySQL, pinned so a change is deliberate.
//!
//! * `SELECT @@sql_mode` reports `''` until the session sets a mode, then
//!   reads the set value back (recorded, not enforced). The result column
//!   is named exactly as written (`@@sql_mode`, `@@SESSION.sql_mode`) —
//!   applications read it as `$row['@@sql_mode']`.
//! * An empty string written to an INTEGER column stays an empty *string*
//!   (MySQL in non-strict mode stores `0`; in its default strict mode it
//!   rejects the statement). See docs/sql-translation.md, "sql_mode".
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`.

use std::net::SocketAddr;

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Row};
use tokio::net::TcpListener;

async fn serve(backend: SharedBackend) -> (Conn, Conn) {
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
        if let Ok(a) = Conn::new(opts.clone()).await {
            let b = Conn::new(opts.clone()).await.unwrap();
            return (a, b);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("litewire did not come up");
}

async fn mode(c: &mut Conn, sql: &str, column: &str) -> String {
    let rows: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}");
    rows[0]
        .get_opt::<String, _>(column)
        .unwrap_or_else(|| panic!("{sql}: no column {column:?} in {:?}", rows[0].columns_ref()))
        .unwrap()
}

async fn sql_mode(backend: SharedBackend) {
    let (mut a, mut b) = serve(backend).await;

    // Default: empty, under the column names the client wrote.
    assert_eq!(mode(&mut a, "select @@sql_mode", "@@sql_mode").await, "");
    assert_eq!(
        mode(&mut a, "SELECT @@SESSION.sql_mode", "@@SESSION.sql_mode").await,
        ""
    );
    assert_eq!(mode(&mut a, "SELECT @@sql_mode AS m", "m").await, "");

    // A literal is recorded per session (normalized as MySQL displays it).
    a.query_drop("SET SESSION sql_mode = 'strict_trans_tables, no_zero_date'")
        .await
        .unwrap();
    assert_eq!(
        mode(&mut a, "SELECT @@sql_mode", "@@sql_mode").await,
        "STRICT_TRANS_TABLES,NO_ZERO_DATE"
    );
    assert_eq!(
        mode(&mut b, "SELECT @@sql_mode", "@@sql_mode").await,
        "",
        "another connection is unaffected"
    );
    // GLOBAL reports the default; SET GLOBAL does not touch this session.
    assert_eq!(
        mode(&mut a, "SELECT @@GLOBAL.sql_mode", "@@GLOBAL.sql_mode").await,
        ""
    );
    a.query_drop("SET GLOBAL sql_mode = ''").await.unwrap();
    assert_eq!(
        mode(&mut a, "SELECT @@sql_mode", "@@sql_mode").await,
        "STRICT_TRANS_TABLES,NO_ZERO_DATE"
    );

    // The common "remove one mode" idiom, evaluated against the current value.
    a.query_drop("SET sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES'")
        .await
        .unwrap();
    a.query_drop("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY,', ''))")
        .await
        .unwrap();
    assert_eq!(
        mode(&mut a, "SELECT @@sql_mode", "@@sql_mode").await,
        "STRICT_TRANS_TABLES"
    );

    // MySQL's double-quoted string form, as PHP applications write it.
    a.query_drop("SET SESSION sql_mode = \"\"").await.unwrap();
    assert_eq!(mode(&mut a, "SELECT @@sql_mode", "@@sql_mode").await, "");

    // Prepared, with a bound value.
    a.exec_drop("SET SESSION sql_mode = ?", ("TRADITIONAL",))
        .await
        .unwrap();
    assert_eq!(
        mode(&mut a, "SELECT @@sql_mode", "@@sql_mode").await,
        "TRADITIONAL"
    );
    a.query_drop("SET sql_mode = DEFAULT").await.unwrap();
    assert_eq!(mode(&mut a, "SELECT @@sql_mode", "@@sql_mode").await, "");

    // ── '' into an INTEGER column: stored as text (documented difference) ──
    a.query_drop("CREATE TABLE s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, n INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO s (n) VALUES (''), (' 7 '), (3)")
        .await
        .unwrap();
    let types: Vec<String> = a
        .query("SELECT typeof(n) FROM s ORDER BY id")
        .await
        .unwrap();
    assert_eq!(types, ["text", "integer", "integer"]);
    // MySQL (non-strict) would store 0 and match `n = 0`.
    let zero: Vec<i64> = a.query("SELECT id FROM s WHERE n = 0").await.unwrap();
    assert!(zero.is_empty());
    // Arithmetic and SUM treat it as 0, as MySQL does.
    let sum: Vec<i64> = a.query("SELECT SUM(n) + MAX(id) * 0 FROM s").await.unwrap();
    assert_eq!(sum, [10]);
}

#[tokio::test]
async fn sql_mode_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    sql_mode(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn sql_mode_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    sql_mode(backend).await;
}
