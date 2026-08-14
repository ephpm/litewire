//! End-to-end tests for the MySQL housekeeping commands PHP persistent
//! connections rely on (see litewire issue #19).
//!
//! `opensrv-mysql` parses nine client commands and answers everything else
//! with a default OK packet, so `COM_RESET_CONNECTION` and friends used to be
//! accepted by accident and reset nothing. These tests drive them through a
//! real client and assert both halves of the contract: the connection stays
//! usable afterwards (no packet-stream desync), and the session state the
//! command promises to clear is actually cleared.

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

/// `COM_RESET_CONNECTION` must be answered with a single OK and leave the
/// packet stream in sync.
///
/// This is the failure mode from issue #19: a reply of the wrong shape (or an
/// extra one) shifts every later response by a packet, and the next
/// `COM_STMT_PREPARE` reads somebody else's OK instead of its own
/// prepare-OK. Text queries, prepared statements and a second reset all have
/// to keep working afterwards.
#[tokio::test]
async fn reset_connection_leaves_the_connection_usable() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    conn.exec_drop("INSERT INTO t (id, name) VALUES (?, ?)", (1, "a"))
        .await
        .unwrap();

    conn.reset().await.unwrap();

    // A text-protocol statement, then a prepared one: the prepare is what
    // reported "Wrong COM_STMT_PREPARE response size" when the stream was
    // off by a packet.
    let rows: Vec<(i64,)> = conn.query("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i64, String)> = conn
        .exec("SELECT id, name FROM t WHERE id = ?", (1,))
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "a".into())]);

    // And again, to prove the reset is repeatable rather than surviving once.
    conn.reset().await.unwrap();
    let rows: Vec<(i64,)> = conn.query("SELECT 1").await.unwrap();
    assert_eq!(rows, vec![(1,)]);

    drop(conn);
}

/// `COM_RESET_CONNECTION` rolls back an open transaction.
///
/// This is what makes the reset worth honouring rather than merely
/// acknowledging: a pooled PHP handle returned to the pool mid-transaction
/// must not hand the next request uncommitted rows and an inherited write
/// lock.
#[tokio::test]
async fn reset_connection_rolls_back_an_open_transaction() {
    init_tracing();
    let port = free_port().await;
    let _server = start_litewire(port).await;
    let mut conn = connect(port).await;

    conn.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id) VALUES (1)")
        .await
        .unwrap();

    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO t (id) VALUES (2)")
        .await
        .unwrap();

    conn.reset().await.unwrap();

    let rows: Vec<(i64,)> = conn.query("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(rows, vec![(1,)], "the uncommitted row must be gone");

    // The rollback must also have released the transaction, so a fresh one
    // can start.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO t (id) VALUES (3)")
        .await
        .unwrap();
    conn.query_drop("COMMIT").await.unwrap();
    let rows: Vec<(i64,)> = conn.query("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(rows, vec![(2,)]);

    drop(conn);
}
