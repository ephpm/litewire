//! End-to-end: MySQL named locks (`GET_LOCK()` & co.) over the wire, with
//! real `mysql_async` clients — and the property everything rests on: a
//! lock dies with the connection that holds it, however that connection
//! ends.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use litewire::backend::{
    Rusqlite, SharedBackend, UserLockRegistry, UserLockScoped, user_lock::scoped_conn,
};
use litewire::litewire_mysql::native_password;
use litewire::{AuthOutcome, AuthRequest, ConnectionAuthenticator};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One fixed, lock-scoped backend behind a MySQL listener — the single-tenant
/// shape.
async fn start_single(port: u16) -> tokio::task::JoinHandle<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let registry = UserLockRegistry::new();
    let backend: SharedBackend = Arc::new(Rusqlite::memory().unwrap());
    let scoped = UserLockScoped::new(backend, &registry, "");
    let server = litewire::LiteWire::new(scoped).mysql(&addr.to_string());
    tokio::spawn(async move {
        let _ = server.serve().await;
    })
}

/// A listener whose backend has **no** lock scope.
async fn start_unscoped(port: u16) -> tokio::task::JoinHandle<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let server = litewire::LiteWire::new(Rusqlite::memory().unwrap()).mysql(&addr.to_string());
    tokio::spawn(async move {
        let _ = server.serve().await;
    })
}

async fn connect(port: u16) -> Conn {
    connect_as(port, "root", "").await.expect("connect")
}

async fn connect_as(port: u16, user: &str, pass: &str) -> Result<Conn, mysql_async::Error> {
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pass))
        .into();
    let mut last = None;
    for _ in 0..40 {
        match Conn::new(opts.clone()).await {
            Ok(conn) => return Ok(conn),
            Err(e) if e.to_string().contains("Authenticate failed") => return Err(e),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    Err(last.unwrap())
}

/// `SELECT <expr>` as `Option<i64>` (`None` = SQL NULL).
async fn int(conn: &mut Conn, sql: &str) -> Option<i64> {
    conn.query_first::<Option<i64>, _>(sql)
        .await
        .unwrap()
        .expect("one row")
}

#[tokio::test]
async fn two_connections_contend_and_release_semantics_match_mysql() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut a = connect(port).await;
    let mut b = connect(port).await;

    assert_eq!(int(&mut a, "SELECT GET_LOCK('job', 0)").await, Some(1));
    assert_eq!(
        int(&mut a, "SELECT GET_LOCK('job', 0)").await,
        Some(1),
        "re-entrant"
    );
    assert_eq!(int(&mut b, "SELECT GET_LOCK('job', 0)").await, Some(0));
    assert_eq!(int(&mut b, "SELECT IS_FREE_LOCK('job')").await, Some(0));
    assert_eq!(
        int(&mut b, "SELECT RELEASE_LOCK('job')").await,
        Some(0),
        "not the holder"
    );
    assert_eq!(int(&mut a, "SELECT RELEASE_LOCK('job')").await, Some(1));
    assert_eq!(
        int(&mut b, "SELECT GET_LOCK('job', 0)").await,
        Some(0),
        "held once more"
    );
    assert_eq!(int(&mut a, "SELECT RELEASE_LOCK('job')").await, Some(1));
    assert_eq!(
        int(&mut a, "SELECT RELEASE_LOCK('job')").await,
        None,
        "not held at all"
    );
    assert_eq!(int(&mut b, "SELECT GET_LOCK('job', 0)").await, Some(1));
    assert_eq!(int(&mut b, "SELECT GET_LOCK('other', 0)").await, Some(1));
    assert_eq!(int(&mut b, "SELECT RELEASE_ALL_LOCKS()").await, Some(2));
    assert_eq!(int(&mut a, "SELECT IS_FREE_LOCK('job')").await, Some(1));
}

/// The Sendy dance, verbatim: `SELECT GET_LOCK(..., 0) AS lock_status` then
/// `DO RELEASE_LOCK(...)`, through `mysqli`-style text queries.
#[tokio::test]
async fn sendy_statements_work_verbatim() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut worker_1 = connect(port).await;
    let mut worker_2 = connect(port).await;

    let name = "sendy_campaign_send_42";
    let acquire = format!(r#"SELECT GET_LOCK("{name}", 0) AS lock_status"#);
    let row: Option<(Option<i64>,)> = worker_1.query_first(&acquire).await.unwrap();
    assert_eq!(row, Some((Some(1),)));

    let row: Option<(Option<i64>,)> = worker_2.query_first(&acquire).await.unwrap();
    assert_eq!(row, Some((Some(0),)), "the second worker must not proceed");

    worker_1
        .query_drop(format!(r#"DO RELEASE_LOCK("{name}")"#))
        .await
        .unwrap();
    let row: Option<(Option<i64>,)> = worker_2.query_first(&acquire).await.unwrap();
    assert_eq!(row, Some((Some(1),)));

    // The column alias survives (Sendy reads `$row['lock_status']`).
    let result = worker_1
        .query_iter(r#"SELECT GET_LOCK("sendy_campaign_send_43", 0) AS lock_status"#)
        .await
        .unwrap();
    assert_eq!(result.columns().unwrap()[0].name_str(), "lock_status");
    result.drop_result().await.unwrap();
}

#[tokio::test]
async fn unaliased_column_is_named_as_written() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut a = connect(port).await;
    let mut result = a
        .query_iter("SELECT GET_LOCK('x', 0), 1 AS one")
        .await
        .unwrap();
    let cols = result.columns().unwrap();
    assert_eq!(cols[0].name_str(), "GET_LOCK('x', 0)");
    assert_eq!(cols[1].name_str(), "one");
    let rows: Vec<(Option<i64>, i64)> = result.collect().await.unwrap();
    assert_eq!(rows, vec![(Some(1), 1)]);
}

/// A waiter is woken by the release — it returns right after, far before
/// its timeout — and gives up with `0` when nobody releases.
#[tokio::test]
async fn timed_wait_wakes_on_release_and_times_out_otherwise() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut a = connect(port).await;
    let mut b = connect(port).await;
    assert_eq!(int(&mut a, "SELECT GET_LOCK('x', 0)").await, Some(1));

    let started = Instant::now();
    assert_eq!(int(&mut b, "SELECT GET_LOCK('x', 1)").await, Some(0));
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "must have waited the full second"
    );

    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        a.query_drop("DO RELEASE_LOCK('x')").await.unwrap();
        a
    });
    let started = Instant::now();
    assert_eq!(int(&mut b, "SELECT GET_LOCK('x', 30)").await, Some(1));
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(300) && waited < Duration::from_secs(5),
        "woken after {waited:?}, not at the 30s timeout"
    );
    let _a = releaser.await.unwrap();
}

/// Prepared statements: the lock arguments come from bound parameters, and
/// the statement advertises the right parameter count.
#[tokio::test]
async fn prepared_get_lock_with_placeholders() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut a = connect(port).await;
    let mut b = connect(port).await;

    let got: Option<Option<i64>> = a
        .exec_first("SELECT GET_LOCK(?, ?) AS got", ("Job-1", 0))
        .await
        .unwrap();
    assert_eq!(got, Some(Some(1)));
    let got: Option<Option<i64>> = b
        .exec_first("SELECT GET_LOCK(?, ?) AS got", ("job-1", 0))
        .await
        .unwrap();
    assert_eq!(got, Some(Some(0)), "names are case-insensitive");
    let (free, tag): (Option<i64>, String) = b
        .exec_first("SELECT IS_FREE_LOCK(?), ? AS tag", ("job-1", "t"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((free, tag), (Some(0), "t".to_string()));
    a.exec_drop("DO RELEASE_LOCK(?)", ("job-1",)).await.unwrap();
    let got: Option<Option<i64>> = b
        .exec_first("SELECT GET_LOCK(?, ?) AS got", ("job-1", 0))
        .await
        .unwrap();
    assert_eq!(got, Some(Some(1)));
}

/// A clean disconnect (`COM_QUIT`) frees the holder's locks.
#[tokio::test]
async fn clean_disconnect_releases_the_lock() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let a = {
        let mut a = connect(port).await;
        assert_eq!(int(&mut a, "SELECT GET_LOCK('x', 0)").await, Some(1));
        a
    };
    let mut b = connect(port).await;
    assert_eq!(int(&mut b, "SELECT GET_LOCK('x', 0)").await, Some(0));
    a.disconnect().await.unwrap();
    let started = Instant::now();
    assert_eq!(int(&mut b, "SELECT GET_LOCK('x', 10)").await, Some(1));
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// The critical path: the holder vanishes without a `QUIT` — the socket is
/// reset, as when its process is `SIGKILL`ed — and the lock is freed as soon
/// as the server notices the connection is gone. Hand-rolled handshake so
/// the socket can be closed with `SO_LINGER = 0` (an RST, not a FIN).
#[tokio::test]
async fn abrupt_tcp_reset_releases_the_lock() {
    let port = free_port().await;
    let _server = start_single(port).await;

    // The server must be up: a real client connects first.
    let mut b = connect(port).await;

    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let n = sock.read(&mut buf).await.unwrap();
    assert!(n > 0, "expected a server handshake");

    // HandshakeResponse41: user `root`, no password, `mysql_native_password`.
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&0x0008_8200u32.to_le_bytes()); // PROTOCOL_41 | SECURE_CONNECTION | PLUGIN_AUTH
    body.extend_from_slice(&0x0100_0000u32.to_le_bytes()); // max packet size
    body.push(33); // utf8_general_ci
    body.extend_from_slice(&[0u8; 23]);
    body.extend_from_slice(b"root\0");
    body.push(0); // auth response length
    body.extend_from_slice(b"mysql_native_password\0");
    write_packet(&mut sock, 1, &body).await;
    let n = sock.read(&mut buf).await.unwrap();
    assert_eq!(
        buf[4],
        0x00,
        "expected OK after the handshake, got {:?}",
        &buf[..n]
    );

    // COM_QUERY: take the lock.
    let mut query = vec![0x03];
    query.extend_from_slice(b"SELECT GET_LOCK('abrupt', 0)");
    write_packet(&mut sock, 0, &query).await;
    let n = sock.read(&mut buf).await.unwrap();
    assert!(n > 0);
    assert_eq!(
        int(&mut b, "SELECT IS_FREE_LOCK('abrupt')").await,
        Some(0),
        "the raw client holds it"
    );

    // Reset, don't close: SO_LINGER 0 makes the drop send an RST. (tokio
    // deprecates the setter because a non-zero linger blocks on drop; zero
    // is exactly the non-blocking, abortive close this test needs.)
    #[allow(deprecated)]
    sock.set_linger(Some(Duration::ZERO)).unwrap();
    drop(sock);

    let started = Instant::now();
    assert_eq!(int(&mut b, "SELECT GET_LOCK('abrupt', 10)").await, Some(1));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "freed as soon as the reset was seen"
    );
}

async fn write_packet(sock: &mut tokio::net::TcpStream, seq: u8, body: &[u8]) {
    let mut packet = Vec::with_capacity(body.len() + 4);
    let len = u32::try_from(body.len()).unwrap();
    packet.extend_from_slice(&len.to_le_bytes()[..3]);
    packet.push(seq);
    packet.extend_from_slice(body);
    sock.write_all(&packet).await.unwrap();
}

/// `COM_RESET_CONNECTION` (what a pool does before lending a connection)
/// releases the previous borrower's locks.
///
/// The reset is answered by the command filter and *applied* at the
/// connection's next command (the same deferral its rollback has), so the
/// release is observable once the reset connection speaks again — which a
/// pool's next borrower does immediately.
#[tokio::test]
async fn reset_connection_releases_locks() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut a = connect(port).await;
    let mut b = connect(port).await;
    assert_eq!(int(&mut a, "SELECT GET_LOCK('x', 0)").await, Some(1));
    a.reset().await.unwrap();
    assert_eq!(
        int(&mut a, "SELECT 1").await,
        Some(1),
        "the next command applies the reset"
    );
    assert_eq!(int(&mut b, "SELECT GET_LOCK('x', 0)").await, Some(1));
    assert_eq!(
        int(&mut a, "SELECT IS_USED_LOCK('x') IS NULL").await,
        Some(0)
    );
}

/// Bad names fail the statement (with `ER_WRONG_ARGUMENTS` on the wire — the
/// real `ER_USER_LOCK_*` codes post-date the wire crate's table) rather than
/// quietly answering 0 or NULL.
#[tokio::test]
async fn bad_lock_names_are_statement_errors() {
    let port = free_port().await;
    let _server = start_single(port).await;
    let mut a = connect(port).await;
    let err = a.query_drop("SELECT GET_LOCK('', 0)").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("1210") && msg.contains("Incorrect user-level lock name"),
        "{msg}"
    );
    let long = "y".repeat(65);
    let err = a
        .query_drop(format!("SELECT GET_LOCK('{long}', 0)"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("should not exceed 64 characters"),
        "{err}"
    );
    // The connection is still usable afterwards.
    assert_eq!(int(&mut a, "SELECT GET_LOCK('fine', 0)").await, Some(1));
}

/// A backend with no lock scope must **error**, never return 1: a lock that
/// does not actually exclude anything is the one outcome an application
/// cannot defend against.
#[tokio::test]
async fn an_unscoped_backend_refuses_lock_functions() {
    let port = free_port().await;
    let _server = start_unscoped(port).await;
    let mut a = connect(port).await;
    let err = a.query_drop("SELECT GET_LOCK('x', 0)").await.unwrap_err();
    assert!(err.to_string().contains("named locks"), "{err}");
    assert_eq!(
        int(&mut a, "SELECT 1").await,
        Some(1),
        "ordinary statements unaffected"
    );
}

// ── Multi-tenant: one listener, one lock namespace per tenant ────────────────

struct Tenant {
    password_hash: [u8; 20],
    backend: SharedBackend,
}

struct TenantDirectory(HashMap<Vec<u8>, Tenant>);

#[litewire::async_trait]
impl ConnectionAuthenticator for TenantDirectory {
    async fn authenticate(&self, req: &AuthRequest<'_>) -> AuthOutcome {
        let Some(tenant) = self.0.get(req.username) else {
            return AuthOutcome::Denied;
        };
        native_password::verify(&tenant.password_hash, req.salt, req.auth_response)
            .then(|| Arc::clone(&tenant.backend))
            .into()
    }
}

/// Two tenants, each scoped to its own namespace in one shared registry —
/// the shape an embedder with a per-tenant database uses.
async fn start_multi_tenant(port: u16) -> tokio::task::JoinHandle<()> {
    let registry = UserLockRegistry::new();
    let mut tenants = HashMap::new();
    for (user, pass) in [("site-a", "pw-for-a"), ("site-b", "pw-for-b")] {
        let backend: SharedBackend = Arc::new(Rusqlite::memory().unwrap());
        let scoped: SharedBackend = Arc::new(UserLockScoped::new(backend, &registry, user));
        tenants.insert(
            user.as_bytes().to_vec(),
            Tenant {
                password_hash: native_password::password_hash(pass.as_bytes()),
                backend: scoped,
            },
        );
    }
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let server = litewire::LiteWire::with_authenticator(Arc::new(TenantDirectory(tenants)))
        .mysql(&addr.to_string());
    tokio::spawn(async move {
        let _ = server.serve().await;
    })
}

#[tokio::test]
async fn tenants_cannot_see_or_block_each_others_locks() {
    let port = free_port().await;
    let _server = start_multi_tenant(port).await;
    let mut a = connect_as(port, "site-a", "pw-for-a").await.unwrap();
    let mut b = connect_as(port, "site-b", "pw-for-b").await.unwrap();
    let mut a2 = connect_as(port, "site-a", "pw-for-a").await.unwrap();

    assert_eq!(int(&mut a, "SELECT GET_LOCK('job', 0)").await, Some(1));
    assert_eq!(
        int(&mut a2, "SELECT GET_LOCK('job', 0)").await,
        Some(0),
        "same tenant: excluded"
    );
    assert_eq!(
        int(&mut b, "SELECT IS_FREE_LOCK('job')").await,
        Some(1),
        "other tenant: invisible"
    );
    assert_eq!(int(&mut b, "SELECT IS_USED_LOCK('job')").await, None);
    assert_eq!(
        int(&mut b, "SELECT GET_LOCK('job', 0)").await,
        Some(1),
        "other tenant: not blocked"
    );
    assert_eq!(int(&mut b, "SELECT RELEASE_LOCK('job')").await, Some(1));
    assert_eq!(
        int(&mut a2, "SELECT GET_LOCK('job', 0)").await,
        Some(0),
        "b's release did not touch a's lock"
    );
}

/// The tenant-screened session (what the authenticating path wraps every
/// connection in) forwards lock operations rather than swallowing them.
#[tokio::test]
async fn tenant_screen_forwards_lock_operations() {
    let registry = UserLockRegistry::new();
    let backend = Rusqlite::memory().unwrap();
    let conn = scoped_conn(
        litewire::backend::Backend::connect(&backend).await.unwrap(),
        &registry,
        "t",
    );
    let screened = litewire::backend::tenant_screen::screened(conn);
    let op = litewire::backend::UserLockOp::Get {
        name: "x".into(),
        timeout: litewire::backend::LockTimeout::Immediate,
    };
    assert_eq!(screened.user_lock(&op).await.unwrap(), Some(1));
    assert_eq!(registry.held_in("t"), 1);
}
