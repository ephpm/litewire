//! End-to-end regression test: MySQL frontend backed by the experimental
//! Turso engine backend (`--features turso`).
//!
//! Guards the wire property that broke Oracle's `mysql` >= 8.1 CLI: the
//! CLI probes dollar-quoting support at startup with `select $$` and
//! requires an ERR reply. When the backend executed the statement with the
//! unbound `$$` parameter as NULL, the server answered with a result set
//! the CLI never reads — wedging the client state machine so every later
//! statement failed with CR 2014 "Commands out of sync".

#![cfg(feature = "turso")]

use std::net::SocketAddr;
use std::sync::Arc;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::net::TcpListener;

async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Start litewire's MySQL frontend on `port`, backed by an in-memory Turso
/// engine database.
async fn start_litewire_turso(port: u16) -> tokio::task::JoinHandle<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let backend = litewire::litewire_turso::Turso::memory().await.unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig { listen: addr };
    let frontend = litewire::litewire_mysql::MysqlFrontend::new(config, Arc::new(backend));

    tokio::spawn(async move {
        frontend.serve().await.unwrap();
    })
}

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

/// `select $$` (the mysql >= 8.1 CLI dollar-quoting probe) must produce a
/// server ERR packet — never a result set — and the connection must remain
/// usable afterwards.
#[tokio::test]
async fn dollar_probe_errors_and_connection_stays_in_sync() {
    let port = free_port().await;
    let _server = start_litewire_turso(port).await;
    let mut conn = connect(port).await;

    let probe = conn.query_iter("select $$").await;
    match probe {
        Err(mysql_async::Error::Server(e)) => {
            assert!(
                e.message.contains("Wrong number of parameters"),
                "expected bind-count error, got: {e}"
            );
        }
        Err(other) => panic!("expected a server ERR packet, got transport error: {other}"),
        Ok(_) => panic!("`select $$` must not return a result set"),
    }

    // The stream must still be in sync: same connection, next statement.
    let rows: Vec<(i64,)> = conn.query("SELECT 1").await.unwrap();
    assert_eq!(rows, vec![(1,)]);

    drop(conn);
}

/// Sanity: normal queries work over the wire against the Turso backend.
#[tokio::test]
async fn basic_crud_over_wire() {
    let port = free_port().await;
    let _server = start_litewire_turso(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
        .await
        .unwrap();
    let rows: Vec<(i64, String)> = conn.query("SELECT id, name FROM users").await.unwrap();
    assert_eq!(rows, vec![(1, "Alice".to_string())]);

    drop(conn);
}
