//! End-to-end tests for the MySQL functions WordPress uses that SQLite has
//! no direct equivalent for (see litewire issue #24).
//!
//! These go through the whole stack — MySQL wire protocol, translation,
//! rusqlite backend — so they assert what the database actually returns
//! rather than what SQL string the translator emitted.

use std::net::SocketAddr;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::net::TcpListener;

/// Find a free port by binding to :0.
async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Start litewire with a MySQL frontend on the given port, backed by
/// in-memory SQLite.
async fn start_litewire(port: u16) -> tokio::task::JoinHandle<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let backend = litewire::backend::Rusqlite::memory().unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig {
        listen: addr,
        max_connections: 0,
    };
    let frontend =
        litewire::litewire_mysql::MysqlFrontend::new(config, std::sync::Arc::new(backend));

    tokio::spawn(async move {
        frontend.serve().await.unwrap();
    })
}

/// Connect to litewire's MySQL frontend.
async fn connect(port: u16) -> Conn {
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some(""))
        .db_name(Some("test"))
        .into();

    for i in 0..20 {
        match Conn::new(opts.clone()).await {
            Ok(conn) => return conn,
            Err(_) if i < 19 => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            Err(e) => panic!("failed to connect after retries: {e}"),
        }
    }
    unreachable!()
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();
}

/// Start a server and return a connection with `wp_options` populated the
/// way `populate_options()` leaves it: some stale RSS transients and some
/// ordinary options that must survive.
async fn options_fixture(port: u16) -> Conn {
    let mut conn = connect(port).await;
    conn.query_drop("CREATE TABLE wp_options (option_id INTEGER PRIMARY KEY, option_name TEXT)")
        .await
        .unwrap();
    for (id, name) in [
        (1, "rss_0123456789abcdef0123456789abcdef"),
        (2, "rss_0123456789abcdef0123456789abcdef_ts"),
        (3, "rss_not_a_hash"),
        (4, "siteurl"),
        (5, "blogname"),
    ] {
        conn.exec_drop(
            "INSERT INTO wp_options (option_id, option_name) VALUES (?, ?)",
            (id, name),
        )
        .await
        .unwrap();
    }
    conn
}

/// `REGEXP` reaches SQLite as a working operator.
///
/// This is the statement `wp-admin/includes/schema.php` runs at install
/// time. Before the `regexp` function was registered it failed outright
/// with "no such function: regexp".
#[tokio::test]
async fn regexp_deletes_the_rows_wordpress_expects() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = options_fixture(port).await;

    conn.query_drop("DELETE FROM wp_options WHERE option_name REGEXP '^rss_[0-9a-f]{32}(_ts)?$'")
        .await
        .unwrap();

    let names: Vec<(String,)> = conn
        .query("SELECT option_name FROM wp_options ORDER BY option_id")
        .await
        .unwrap();
    assert_eq!(
        names,
        vec![
            ("rss_not_a_hash".to_string(),),
            ("siteurl".to_string(),),
            ("blogname".to_string(),),
        ]
    );

    drop(conn);
}

/// `REGEXP` in a `SELECT`, with and without `NOT`, and via `RLIKE`.
#[tokio::test]
async fn regexp_rlike_and_not_regexp_all_work() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = options_fixture(port).await;

    let matched: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM wp_options WHERE option_name REGEXP '^rss_'")
        .await
        .unwrap();
    assert_eq!(matched, vec![(3,)]);

    // RLIKE is MySQL's synonym; SQLite only knows REGEXP.
    let matched: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM wp_options WHERE option_name RLIKE '^rss_'")
        .await
        .unwrap();
    assert_eq!(matched, vec![(3,)]);

    let unmatched: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM wp_options WHERE option_name NOT REGEXP '^rss_'")
        .await
        .unwrap();
    assert_eq!(unmatched, vec![(2,)]);

    drop(conn);
}

/// A `NULL` operand yields `NULL`, as it does on MySQL — so a `NULL` row
/// satisfies neither `REGEXP` nor `NOT REGEXP`.
#[tokio::test]
async fn regexp_propagates_null() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id, v) VALUES (1, 'abc'), (2, NULL)")
        .await
        .unwrap();

    let row: Vec<(Option<i64>,)> = conn
        .query("SELECT v REGEXP '^a' FROM t WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(row, vec![(None,)]);

    let matching: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM t WHERE v REGEXP '^a'")
        .await
        .unwrap();
    assert_eq!(matching, vec![(1,)]);
    let not_matching: Vec<(i64,)> = conn
        .query("SELECT COUNT(*) FROM t WHERE v NOT REGEXP '^a'")
        .await
        .unwrap();
    assert_eq!(not_matching, vec![(0,)]);

    drop(conn);
}

/// `YEAR()` / `MONTH()` / `DAYOFMONTH()` return integers that compare equal
/// to integer literals.
///
/// The comparison is the whole point: `strftime` alone returns text, and
/// `'03' = 3` is false in SQLite, so a translation without the cast would
/// run cleanly and match nothing.
#[tokio::test]
async fn date_parts_compare_as_integers() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE wp_posts (ID INTEGER PRIMARY KEY, post_date TEXT)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO wp_posts (ID, post_date) VALUES \
         (1, '2024-03-17 10:00:00'), (2, '2024-03-18 10:00:00'), (3, '2023-11-02 10:00:00')",
    )
    .await
    .unwrap();

    let parts: Vec<(i64, i64, i64)> = conn
        .query("SELECT YEAR(post_date), MONTH(post_date), DAYOFMONTH(post_date) FROM wp_posts WHERE ID = 1")
        .await
        .unwrap();
    assert_eq!(parts, vec![(2024, 3, 17)]);

    // The `redirect_guess_404_permalink` shape: date parts compared against
    // integers pulled out of the URL.
    let ids: Vec<(i64,)> = conn
        .query(
            "SELECT ID FROM wp_posts WHERE YEAR(post_date) = 2024 AND MONTH(post_date) = 3 \
             AND DAYOFMONTH(post_date) = 17",
        )
        .await
        .unwrap();
    assert_eq!(ids, vec![(1,)]);

    let ids: Vec<(i64,)> = conn
        .query("SELECT ID FROM wp_posts WHERE YEAR(post_date) = 2024 ORDER BY ID")
        .await
        .unwrap();
    assert_eq!(ids, vec![(1,), (2,)]);

    drop(conn);
}

/// A `NULL` or unparseable date yields `NULL`, not a wrong number.
#[tokio::test]
async fn date_parts_propagate_null() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY, d TEXT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id, d) VALUES (1, NULL), (2, 'not a date')")
        .await
        .unwrap();

    let years: Vec<(Option<i64>,)> = conn
        .query("SELECT YEAR(d) FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(years, vec![(None,), (None,)]);

    drop(conn);
}
