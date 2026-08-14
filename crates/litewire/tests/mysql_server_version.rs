//! The three ways a MySQL client can ask litewire its version must agree
//! (see litewire issue #21).
//!
//! A client reads the version from the wire handshake
//! (`mysqli_get_server_info()`, `PDO::ATTR_SERVER_VERSION`), from
//! `SELECT VERSION()`, or from `@@version` (`wpdb::db_version()`), and
//! branches on it for capability checks. Those three used to be answered by
//! three unrelated string literals, two of which said `8.0.0-litewire` while
//! the handshake said `8.0.36-litewire`.
//!
//! This test exists because the fix — routing all three through
//! `litewire_translate::SERVER_VERSION` — is only as good as something that
//! notices if a fourth path appears, or if one of them is hardcoded again.

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

#[tokio::test]
async fn handshake_version_function_and_system_variable_all_agree() {
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    let expected = litewire::translate::SERVER_VERSION;

    let from_function: Vec<(String,)> = conn.query("SELECT VERSION()").await.unwrap();
    assert_eq!(from_function, vec![(expected.to_string(),)]);

    let from_variable: Vec<(String,)> = conn.query("SELECT @@version").await.unwrap();
    assert_eq!(from_variable, vec![(expected.to_string(),)]);

    // The handshake, as the client parsed it out of the initial packet.
    // `mysql_async` exposes it as the numeric triple every capability check
    // is really made of.
    assert_eq!(conn.server_version(), (8, 0, 36));

    // And the triple is the same one the shared constant spells out, so a
    // future bump cannot move one without the other.
    let (major, minor, patch) = conn.server_version();
    assert!(
        expected.starts_with(&format!("{major}.{minor}.{patch}")),
        "handshake reported {major}.{minor}.{patch}, constant is {expected}"
    );

    drop(conn);
}

/// Every session-identity built-in must actually execute.
///
/// They were all rewritten to a one-argument `coalesce()`, which SQLite
/// rejects at prepare time — so none of them worked, and the unit tests did
/// not notice because they only ever inspected the emitted SQL string.
/// These assertions run the statements.
#[tokio::test]
async fn session_identity_builtins_execute() {
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    for (sql, expected) in [
        ("SELECT DATABASE()", "main"),
        ("SELECT SCHEMA()", "main"),
        ("SELECT USER()", "root@localhost"),
        ("SELECT CURRENT_USER()", "root@localhost"),
        ("SELECT SESSION_USER()", "root@localhost"),
        ("SELECT SYSTEM_USER()", "root@localhost"),
    ] {
        let rows: Vec<(String,)> = conn.query(sql).await.unwrap();
        assert_eq!(rows, vec![(expected.to_string(),)], "{sql}");
    }

    let rows: Vec<(i64,)> = conn.query("SELECT CONNECTION_ID()").await.unwrap();
    assert_eq!(rows, vec![(0,)]);

    // In a larger expression too, not just as a bare select item.
    let rows: Vec<(String,)> = conn
        .query("SELECT VERSION() WHERE DATABASE() = 'main'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(litewire::translate::SERVER_VERSION.to_string(),)]
    );

    drop(conn);
}
