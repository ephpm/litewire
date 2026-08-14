//! The MySQL error packet a real client receives (litewire #22).
//!
//! The session-level tests assert on the mapped triple; these assert on
//! what comes back over the wire, because the SQLSTATE in the packet is
//! written by `opensrv-mysql` from its own `ErrorKind` table rather than
//! from the SQLSTATE litewire mapped. The two have to agree, and only a
//! client can prove it.

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

/// Run `sql` and return the server error packet it must produce.
async fn server_error(conn: &mut Conn, sql: &str) -> mysql_async::ServerError {
    match conn.query_drop(sql).await {
        Err(mysql_async::Error::Server(e)) => e,
        Err(other) => panic!("expected a server error for `{sql}`, got: {other}"),
        Ok(()) => panic!("`{sql}` was supposed to fail"),
    }
}

/// A missing table reaches the client as 1146 / 42S02.
#[tokio::test]
async fn missing_table_reaches_the_client_as_1146() {
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    let e = server_error(&mut conn, "SELECT * FROM sprockets").await;
    assert_eq!(e.code, 1146);
    assert_eq!(e.state, "42S02");
    assert!(e.message.contains("sprockets"), "got: {}", e.message);

    // The connection is still usable -- an error packet is not a desync.
    let rows: Vec<(i64,)> = conn.query("SELECT 1").await.unwrap();
    assert_eq!(rows, vec![(1,)]);

    drop(conn);
}

/// A duplicate key reaches the client as 1062 / 23000, with the message
/// shape stock drivers match on.
#[tokio::test]
async fn duplicate_key_reaches_the_client_as_a_mysql_shaped_1062() {
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO users (id, email) VALUES (1, 'a@example.com')")
        .await
        .unwrap();

    let e = server_error(
        &mut conn,
        "INSERT INTO users (id, email) VALUES (2, 'a@example.com')",
    )
    .await;

    assert_eq!(e.code, 1062);
    assert_eq!(e.state, "23000");
    assert!(
        e.message.starts_with("Duplicate entry '"),
        "got: {}",
        e.message
    );
    assert!(
        e.message.contains("' for key 'users.email'"),
        "got: {}",
        e.message
    );
    assert!(
        e.message.contains("UNIQUE constraint failed: users.email"),
        "SQLite's own text must survive for debugging: {}",
        e.message
    );

    drop(conn);
}

/// PDO's exception text is assembled from the SQLSTATE, the code and the
/// message, and that is what several frameworks pattern-match on.
///
/// This reconstructs the string PDO would build and checks it against the
/// two patterns that matter: Laravel's `Integrity constraint violation:
/// 1062` test, and the `Duplicate entry ... for key ...` regex used by
/// stock drivers.
#[tokio::test]
async fn the_string_a_pdo_client_would_build_matches_stock_detection() {
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id) VALUES (1)")
        .await
        .unwrap();

    let e = server_error(&mut conn, "INSERT INTO t (id) VALUES (1)").await;
    let pdo_style = format!(
        "SQLSTATE[{}]: Integrity constraint violation: {} {}",
        e.state, e.code, e.message
    );

    assert!(
        pdo_style.contains("Integrity constraint violation: 1062"),
        "got: {pdo_style}"
    );
    let entry = pdo_style
        .find("Duplicate entry '")
        .expect("no entry marker");
    let for_key = pdo_style.find("' for key '").expect("no key marker");
    assert!(for_key > entry, "got: {pdo_style}");

    drop(conn);
}
