//! End-to-end tests for the wire frontends' `max_connections` cap.
//!
//! The cap exists because a connection is expensive here: each accepted
//! client holds a backend session for its whole lifetime, and the rusqlite
//! backend gives every session its own OS thread and its own open SQLite
//! handle. Bounding connections is how an embedder bounds threads. These
//! tests hold that contract down end to end, through real clients where a
//! real client exists.

use std::net::SocketAddr;
use std::time::Duration;

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Find a free port by binding to :0.
async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Start a MySQL frontend with the given cap, backed by in-memory SQLite.
async fn start_mysql(port: u16, max_connections: usize) -> tokio::task::JoinHandle<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let backend = litewire::backend::Rusqlite::memory().unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig {
        listen: addr,
        max_connections,
    };
    let frontend =
        litewire::litewire_mysql::MysqlFrontend::new(config, std::sync::Arc::new(backend));
    let handle = tokio::spawn(async move {
        frontend.serve().await.unwrap();
    });
    wait_until_listening(port).await;
    handle
}

/// Start a PostgreSQL frontend with the given cap.
#[cfg(feature = "postgres")]
async fn start_postgres(port: u16, max_connections: usize) -> tokio::task::JoinHandle<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let backend = litewire::backend::Rusqlite::memory().unwrap();
    let config = litewire::litewire_postgres::PostgresFrontendConfig {
        listen: addr,
        max_connections,
    };
    let frontend =
        litewire::litewire_postgres::PostgresFrontend::new(config, std::sync::Arc::new(backend));
    let handle = tokio::spawn(async move {
        frontend.serve().await.unwrap();
    });
    wait_until_listening(port).await;
    handle
}

/// Poll until the frontend is accepting, so a test never races startup.
async fn wait_until_listening(port: u16) {
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("frontend on port {port} never started listening");
}

/// Connect, retrying while the server is still releasing the seat taken by
/// the readiness probe in [`wait_until_listening`].
///
/// Only for connections a test *expects* to succeed. Tests asserting a
/// refusal call `Conn::new` directly, so a retry can never paper over a cap
/// that failed to engage.
async fn connect_ok(port: u16) -> Conn {
    let mut last = None;
    for _ in 0..300 {
        match Conn::new(mysql_opts(port)).await {
            Ok(c) => return c,
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    panic!("connection never succeeded: {last:?}");
}

fn mysql_opts(port: u16) -> Opts {
    OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .db_name(Some("litewire"))
        .into()
}

/// A raw client that reads whatever the server sends first, so the refusal
/// packet can be parsed as a client would parse it.
async fn read_first_packet(port: u16) -> Vec<u8> {
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut header = [0u8; 4];
    sock.read_exact(&mut header).await.unwrap();
    let len = u32::from(header[0]) | (u32::from(header[1]) << 8) | (u32::from(header[2]) << 16);
    let mut payload = vec![0u8; len as usize];
    sock.read_exact(&mut payload).await.unwrap();
    // Sequence id must be 0: this packet stands in for the initial
    // handshake, which is always sequence 0.
    assert_eq!(header[3], 0, "refusal packet had the wrong sequence id");
    payload
}

// ── (a) the cap is enforced ────────────────────────────────────────────────

/// With a cap of N, the N+1'th connection must be refused while N are held
/// open — and the refusal must be a protocol-valid `ER_CON_COUNT_ERROR`,
/// not a bare close.
///
/// Would catch: a cap that is not enforced at all, an off-by-one that
/// admits N+1, or a refusal that writes garbage a real client cannot parse.
#[tokio::test]
async fn mysql_cap_refuses_beyond_the_limit_with_error_1040() {
    let port = free_port().await;
    let _server = start_mysql(port, 2).await;

    // Hold the cap's worth of connections open, and prove they work.
    let mut a = connect_ok(port).await;
    let mut b = connect_ok(port).await;
    let one: Option<i64> = "SELECT 1".first(&mut a).await.unwrap();
    assert_eq!(one, Some(1));
    let one: Option<i64> = "SELECT 1".first(&mut b).await.unwrap();
    assert_eq!(one, Some(1));

    // 1. A real client must see a server error, not a torn connection.
    let err = match Conn::new(mysql_opts(port)).await {
        Err(e) => e,
        Ok(_) => panic!("3rd connection should be refused"),
    };
    let text = err.to_string();
    assert!(
        text.contains("1040") || text.to_lowercase().contains("too many connections"),
        "refusal did not reach the client as ER_CON_COUNT_ERROR: {err}"
    );

    // 2. And the bytes on the wire are a well-formed ERR packet.
    let payload = read_first_packet(port).await;
    assert_eq!(payload[0], 0xff, "not an ERR packet: {payload:02x?}");
    let code = u16::from_le_bytes([payload[1], payload[2]]);
    assert_eq!(code, 1040, "wrong error code");
    let message = String::from_utf8_lossy(&payload[3..]);
    assert!(
        message.contains("Too many connections"),
        "unexpected message: {message}"
    );
    // The pre-4.1 error form carries no `#SQLSTATE` marker, because the
    // client has not sent its capability flags yet.
    assert!(
        !message.starts_with('#'),
        "sent a SQLSTATE marker before capability negotiation: {message}"
    );

    drop(a);
    drop(b);
}

// ── (b) slots are released ─────────────────────────────────────────────────

/// Closing a connection must free its seat for the next client.
///
/// Would catch: a seat that leaks on clean disconnect, which would make the
/// server refuse everything forever after the first N clients.
#[tokio::test]
async fn mysql_cap_releases_slot_on_disconnect() {
    let port = free_port().await;
    let _server = start_mysql(port, 1).await;

    let mut first = connect_ok(port).await;
    let one: Option<i64> = "SELECT 1".first(&mut first).await.unwrap();
    assert_eq!(one, Some(1));
    assert!(
        Conn::new(mysql_opts(port)).await.is_err(),
        "cap of 1 admitted a second connection"
    );

    first.disconnect().await.unwrap();

    // The seat is released when the server-side task ends, which is a
    // moment after the client's close; poll rather than assume.
    let mut reconnected = None;
    for _ in 0..200 {
        if let Ok(c) = Conn::new(mysql_opts(port)).await {
            reconnected = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut next = reconnected.expect("seat was never released after a clean disconnect");
    let one: Option<i64> = "SELECT 1".first(&mut next).await.unwrap();
    assert_eq!(one, Some(1), "recycled seat produced a broken session");
}

/// A client that vanishes *mid-statement* must also free its seat.
///
/// This is the path that matters most: it is the same path that reclaims
/// the session's worker thread, and the cap is only trustworthy if the two
/// cannot diverge. Would catch: a seat released by an explicit call on a
/// clean-shutdown path only, which leaks under abrupt disconnects — exactly
/// when a server is under stress and most needs the cap to be accurate.
#[tokio::test]
async fn mysql_cap_releases_slot_when_client_vanishes_mid_statement() {
    let port = free_port().await;
    let _server = start_mysql(port, 1).await;

    // A statement slow enough that the socket dies while it is running.
    let slow = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 4000000) \
                SELECT count(*) FROM c";

    {
        let mut conn = connect_ok(port).await;
        let query = tokio::spawn(async move {
            let _: Result<Option<i64>, _> = slow.first(&mut conn).await;
            // Drop the connection here, still inside the statement's
            // lifetime as far as the server is concerned.
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        query.abort();
        let _ = query.await;
    }

    let mut reconnected = None;
    for _ in 0..500 {
        if let Ok(c) = Conn::new(mysql_opts(port)).await {
            reconnected = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut next = reconnected.expect("seat leaked when the client vanished mid-statement");
    let one: Option<i64> = "SELECT 1".first(&mut next).await.unwrap();
    assert_eq!(one, Some(1));
}

// ── (c) default is unlimited ───────────────────────────────────────────────

/// `max_connections: 0` must behave exactly as before this change: no cap.
///
/// Would catch: a `0` treated as "zero connections allowed", which would
/// take every existing deployment offline on upgrade.
#[tokio::test]
async fn mysql_zero_means_unlimited() {
    let port = free_port().await;
    let _server = start_mysql(port, 0).await;

    let mut conns = Vec::new();
    for i in 0..24 {
        let mut c = Conn::new(mysql_opts(port))
            .await
            .unwrap_or_else(|e| panic!("connection {i} refused with no cap configured: {e}"));
        let one: Option<i64> = "SELECT 1".first(&mut c).await.unwrap();
        assert_eq!(one, Some(1));
        conns.push(c);
    }
    assert_eq!(conns.len(), 24);
}

/// The `LiteWire` facade must default to unlimited, so an embedder that
/// never calls `max_connections` is unaffected.
#[test]
fn litewire_builder_defaults_to_unlimited() {
    // Compile-time + construction check: the builder is usable without the
    // knob, and setting it is opt-in.
    let backend = litewire::backend::Rusqlite::memory().unwrap();
    let _unbounded = litewire::LiteWire::new(backend).mysql("127.0.0.1:0");

    let backend = litewire::backend::Rusqlite::memory().unwrap();
    let _bounded = litewire::LiteWire::new(backend)
        .max_connections(16)
        .mysql("127.0.0.1:0");
}

// ── (d) the other TCP frontends ────────────────────────────────────────────

/// PostgreSQL refuses past the cap by closing, and releases the seat on
/// disconnect. No error packet is asserted here — the frontend deliberately
/// closes rather than guessing whether an `SSLRequest` or a
/// `StartupMessage` is in flight.
///
/// Would catch: the cap being wired into MySQL only, which is how this kind
/// of change usually rots.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_cap_refuses_and_releases() {
    let port = free_port().await;
    let _server = start_postgres(port, 1).await;

    // Hold one connection open and complete the startup handshake, so the
    // server-side session really exists. Retried, because the readiness
    // probe's own seat takes a moment to drain -- see `connect_ok`.
    let mut accepted = None;
    for _ in 0..300 {
        if let Ok(pair) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(port)
            .user("postgres")
            .dbname("litewire")
            .connect(tokio_postgres::NoTls)
            .await
        {
            accepted = Some(pair);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (client, connection) = accepted.expect("first connection should be accepted");
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client.query_one("SELECT 1", &[]).await.unwrap();
    let v: i64 = row.get(0);
    assert_eq!(v, 1);

    // The second must be refused. The server closes immediately, which
    // surfaces as a connection error rather than a protocol error.
    let refused = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("litewire")
        .connect(tokio_postgres::NoTls)
        .await;
    assert!(
        refused.is_err(),
        "cap of 1 admitted a second PostgreSQL connection"
    );

    // Release the seat and confirm the next client gets in.
    drop(client);
    conn_task.abort();
    let _ = conn_task.await;

    let mut ok = false;
    for _ in 0..200 {
        if let Ok((c, conn)) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(port)
            .user("postgres")
            .dbname("litewire")
            .connect(tokio_postgres::NoTls)
            .await
        {
            let t = tokio::spawn(async move {
                let _ = conn.await;
            });
            let row = c.query_one("SELECT 1", &[]).await.unwrap();
            let v: i64 = row.get(0);
            assert_eq!(v, 1);
            t.abort();
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ok, "PostgreSQL seat was never released");
}

/// TDS refuses past the cap by closing the socket, and releases the seat
/// when the connection goes.
///
/// Asserted at the TCP level rather than through `tiberius`: the seat is
/// taken at accept, before Pre-Login, so a raw socket is a faithful client
/// for this particular contract and avoids a full TDS handshake.
///
/// Would catch: the cap being wired into some frontends but not all.
#[cfg(feature = "tds")]
#[tokio::test]
async fn tds_cap_refuses_and_releases() {
    let port = free_port().await;
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let backend = litewire::backend::Rusqlite::memory().unwrap();
    let config = litewire::litewire_tds::TdsFrontendConfig {
        listen: addr,
        max_connections: 1,
    };
    let frontend = litewire::litewire_tds::TdsFrontend::new(config, std::sync::Arc::new(backend));
    tokio::spawn(async move {
        frontend.serve().await.unwrap();
    });
    wait_until_listening(port).await;

    // `wait_until_listening` already opened and dropped a socket; give the
    // server a moment to release that seat before taking the real one.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let held = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The second connection is accepted at the TCP level then closed by the
    // server, so a read returns EOF rather than blocking.
    let mut refused = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(5), refused.read(&mut buf))
        .await
        .expect("over-cap TDS connection was neither served nor closed")
        .unwrap();
    assert_eq!(n, 0, "expected the server to close, got data instead");

    drop(held);

    // The seat comes back.
    let mut released = false;
    for _ in 0..200 {
        let mut probe = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut b = [0u8; 1];
        // A seat that is available means the server keeps the socket open
        // waiting for Pre-Login, so the read times out instead of hitting
        // EOF.
        match tokio::time::timeout(Duration::from_millis(150), probe.read(&mut b)).await {
            Err(_elapsed) => {
                released = true;
                break;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    assert!(released, "TDS seat was never released");
}

/// A refused MySQL client must not consume a seat itself — otherwise the
/// refusal path would be its own denial of service, with every rejected
/// connection permanently shrinking capacity.
#[tokio::test]
async fn refused_connections_do_not_consume_capacity() {
    let port = free_port().await;
    let _server = start_mysql(port, 1).await;

    let mut held = connect_ok(port).await;

    // Bounce a pile of clients off the cap.
    for _ in 0..20 {
        assert!(Conn::new(mysql_opts(port)).await.is_err());
    }

    // The original connection is untouched...
    let one: Option<i64> = "SELECT 1".first(&mut held).await.unwrap();
    assert_eq!(one, Some(1));

    // ...and once it goes, exactly one seat is available again.
    held.disconnect().await.unwrap();
    let mut ok = false;
    for _ in 0..200 {
        if Conn::new(mysql_opts(port)).await.is_ok() {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ok, "refused connections permanently consumed capacity");
}

/// A raw TCP client that connects and disappears without speaking must not
/// strand a seat.
///
/// Would catch: a seat taken at accept but only released by a code path
/// that requires the handshake to complete.
#[tokio::test]
async fn abandoned_pre_handshake_connections_release_their_seat() {
    let port = free_port().await;
    let _server = start_mysql(port, 1).await;

    for _ in 0..5 {
        let sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // Connect, read nothing, hang up immediately.
        drop(sock);
    }

    let mut ok = false;
    for _ in 0..300 {
        if let Ok(mut c) = Conn::new(mysql_opts(port)).await {
            let one: Option<i64> = "SELECT 1".first(&mut c).await.unwrap();
            assert_eq!(one, Some(1));
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ok, "an abandoned pre-handshake connection stranded a seat");
}

/// The refusal write must not hang the accept loop when the refused client
/// has already gone away.
///
/// Would catch: refusing inline in the accept loop with a blocking write to
/// a dead socket, which would stall every subsequent accept.
#[tokio::test]
async fn accept_loop_survives_refusing_a_client_that_left() {
    let port = free_port().await;
    let _server = start_mysql(port, 1).await;
    let _held = connect_ok(port).await;

    for _ in 0..20 {
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let _ = sock.shutdown().await;
        drop(sock);
    }

    // The accept loop must still be alive and still refusing correctly.
    let payload = tokio::time::timeout(Duration::from_secs(5), read_first_packet(port))
        .await
        .expect("accept loop stalled after refusing dead sockets");
    assert_eq!(payload[0], 0xff);
    assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 1040);
}
