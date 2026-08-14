//! End-to-end test: one MySQL listener, two databases, real `mysql_async`
//! clients, and the cross-tenant attempts that must fail.
//!
//! This exercises [`LiteWire::with_authenticator`] the way an embedder is meant
//! to use it: the handshake username picks *which* tenant, and the
//! `mysql_native_password` response proves the client is entitled to it. The
//! interesting cases are the negative ones — a client that claims another
//! tenant's username, and a client that sends no username at all.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use litewire::backend::Rusqlite;
use litewire::backend::SharedBackend;
use litewire::litewire_mysql::native_password;
use litewire::{AuthRequest, ConnectionAuthenticator};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::net::TcpListener;

/// One tenant: the value the server stores for its password, plus its database.
struct Tenant {
    password_hash: [u8; 20],
    backend: SharedBackend,
}

/// The policy an embedder writes: username selects the tenant, the password
/// proves entitlement to it.
///
/// Note the ordering — the backend is only reached *after* [`verify`] succeeds,
/// so a username on its own selects nothing.
struct TenantDirectory(HashMap<Vec<u8>, Tenant>);

#[litewire::async_trait]
impl ConnectionAuthenticator for TenantDirectory {
    async fn authenticate(&self, req: &AuthRequest<'_>) -> Option<SharedBackend> {
        let tenant = self.0.get(req.username)?;
        native_password::verify(&tenant.password_hash, req.salt, req.auth_response)
            .then(|| Arc::clone(&tenant.backend))
    }
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Seed a fresh in-memory database with one row identifying its owner.
///
/// `Rusqlite::memory()` gives each `Backend` its own database, which is exactly
/// the per-tenant isolation unit under test.
async fn tenant_backend(marker: &str) -> SharedBackend {
    let backend: SharedBackend = Arc::new(Rusqlite::memory().unwrap());
    let conn = backend.connect().await.unwrap();
    conn.execute("CREATE TABLE secrets (owner TEXT)", &[])
        .await
        .unwrap();
    conn.execute(
        &format!("INSERT INTO secrets (owner) VALUES ('{marker}')"),
        &[],
    )
    .await
    .unwrap();
    backend
}

/// Start a single MySQL listener in front of two tenants.
async fn start(port: u16) -> tokio::task::JoinHandle<()> {
    let mut tenants = HashMap::new();
    tenants.insert(
        b"site-a".to_vec(),
        Tenant {
            password_hash: native_password::password_hash(b"pw-for-a"),
            backend: tenant_backend("a-secret").await,
        },
    );
    tenants.insert(
        b"site-b".to_vec(),
        Tenant {
            password_hash: native_password::password_hash(b"pw-for-b"),
            backend: tenant_backend("b-secret").await,
        },
    );

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let server = litewire::LiteWire::with_authenticator(Arc::new(TenantDirectory(tenants)))
        .mysql(&addr.to_string());

    tokio::spawn(async move {
        let _ = server.serve().await;
    })
}

/// Connect with an explicit username/password pair. Retries briefly so the test
/// does not race the listener's bind.
async fn connect(port: u16, user: &str, pass: &str) -> Result<Conn, mysql_async::Error> {
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pass))
        .into();

    let mut last = None;
    for _ in 0..20 {
        match Conn::new(opts.clone()).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                // Access-denied is a real answer, not a startup race — return
                // it immediately rather than burning the retry budget.
                if e.to_string().contains("Authenticate failed") {
                    return Err(e);
                }
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    Err(last.unwrap())
}

async fn owner_of(conn: &mut Conn) -> Vec<String> {
    conn.query("SELECT owner FROM secrets").await.unwrap()
}

/// Assert a connection attempt was refused at the handshake.
///
/// The wire form is SQLSTATE `28000` (invalid authorization specification) with
/// `opensrv-mysql`'s "Authenticate failed" text — the same shape a real MySQL
/// server uses for a bad credential, so clients and connection pools treat it
/// as an auth failure rather than a transient error worth retrying.
fn assert_denied(err: &mysql_async::Error) {
    let msg = err.to_string();
    assert!(
        msg.contains("28000") && msg.contains("Authenticate failed"),
        "expected a handshake access-denied error, got: {msg}"
    );
}

/// The happy path: each tenant's own credentials reach its own database, over
/// one shared listener.
#[tokio::test]
async fn each_tenant_reaches_only_its_own_database() {
    let port = free_port().await;
    let _server = start(port).await;

    let mut a = connect(port, "site-a", "pw-for-a").await.expect("site-a");
    assert_eq!(owner_of(&mut a).await, vec!["a-secret".to_string()]);

    let mut b = connect(port, "site-b", "pw-for-b").await.expect("site-b");
    assert_eq!(owner_of(&mut b).await, vec!["b-secret".to_string()]);
}

/// The attack: tenant B knows tenant A's *name* — names are public — and
/// connects claiming it, with the only password B has. Denied at the
/// handshake.
#[tokio::test]
async fn claiming_another_tenants_username_is_denied() {
    let port = free_port().await;
    let _server = start(port).await;

    let err = connect(port, "site-a", "pw-for-b")
        .await
        .expect_err("claiming site-a with site-b's password must be denied");
    assert_denied(&err);
}

/// The same attack with no password at all.
#[tokio::test]
async fn empty_password_is_denied() {
    let port = free_port().await;
    let _server = start(port).await;

    let err = connect(port, "site-a", "")
        .await
        .expect_err("an empty password must not authenticate");
    assert_denied(&err);
}

/// An unknown tenant is denied even with a password that is valid *somewhere*.
#[tokio::test]
async fn unknown_username_is_denied() {
    let port = free_port().await;
    let _server = start(port).await;

    let err = connect(port, "site-c", "pw-for-a")
        .await
        .expect_err("an unknown tenant must be denied");
    assert_denied(&err);
}

/// A hand-rolled handshake that names no tenant, then asks for data anyway.
///
/// Written at the byte level because no real client offers "send an empty
/// username" as an option, and the interesting question is what the *server*
/// does with a handshake a client library would never produce.
///
/// The answer must be: nothing readable. An empty username matches no tenant,
/// the authenticator refuses, and the connection is closed at the handshake —
/// and even if it were not, the connection holds no backend, so the command
/// loop has nothing to serve. See the `LiteWireHandler` docs for why that
/// second half is a structural property rather than a second check.
#[tokio::test]
async fn handshake_with_an_empty_username_reads_nothing() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let port = free_port().await;
    let _server = start(port).await;

    let mut sock = None;
    for _ in 0..20 {
        match tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            Ok(s) => {
                sock = Some(s);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    let mut sock = sock.expect("connect");

    // Read (and discard) the server's initial handshake packet.
    let mut buf = [0u8; 1024];
    let n = sock.read(&mut buf).await.unwrap();
    assert!(n > 0, "expected a server handshake");

    // HandshakeResponse41 with an empty username and no auth response.
    let mut body: Vec<u8> = Vec::new();
    // CLIENT_PROTOCOL_41 (0x200) | CLIENT_SECURE_CONNECTION (0x8000) |
    // CLIENT_PLUGIN_AUTH (0x80000). Naming the plugin we were offered avoids
    // an auth-switch round trip, so the server decides on this packet alone.
    // Deliberately no CONNECT_WITH_DB — there is no database name to hide
    // behind.
    body.extend_from_slice(&0x0008_8200u32.to_le_bytes());
    body.extend_from_slice(&0x0100_0000u32.to_le_bytes()); // max packet size
    body.push(33); // collation: utf8_general_ci
    body.extend_from_slice(&[0u8; 23]); // reserved
    body.push(0); // username: the empty NUL-terminated string
    body.push(0); // auth response length: 0
    body.extend_from_slice(b"mysql_native_password\0");

    let mut packet = Vec::new();
    let len = u32::try_from(body.len()).unwrap();
    packet.extend_from_slice(&len.to_le_bytes()[..3]);
    packet.push(1); // sequence id
    packet.extend_from_slice(&body);
    sock.write_all(&packet).await.unwrap();

    // Read the server's verdict *before* writing anything else. Sending into a
    // socket the server has already closed provokes an RST, and an RST
    // discards whatever is still sitting in our receive buffer — including the
    // error packet this test is about.
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    if let Ok(Ok(n)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut chunk)).await
    {
        response.extend_from_slice(&chunk[..n]);
    }

    // Then ask for data anyway, and append whatever comes back (usually
    // nothing: the connection is already gone).
    let sql = b"SELECT owner FROM secrets";
    let mut query = vec![0u8; 4];
    query.push(0x03);
    query.extend_from_slice(sql);
    let qlen = u32::try_from(query.len() - 4).unwrap();
    query[..3].copy_from_slice(&qlen.to_le_bytes()[..3]);
    query[3] = 0; // sequence id
    let _ = sock.write_all(&query).await;

    if let Ok(Ok(n)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut chunk)).await
    {
        response.extend_from_slice(&chunk[..n]);
    }

    // The only thing that actually matters: no tenant's data came back.
    for secret in [&b"a-secret"[..], b"b-secret"] {
        assert!(
            !contains(&response, secret),
            "an unauthenticated connection read tenant data: {:?}",
            String::from_utf8_lossy(&response)
        );
    }

    // And it was refused, not merely starved: either at the handshake
    // (`28000`, what happens today — the empty username matches no tenant) or
    // per-command by the no-backend guard, depending on whether `authenticate`
    // ran at all. Both are correct; a silent hang or an OK packet is not.
    assert!(
        contains(&response, b"28000") || contains(&response, b"not authenticated"),
        "expected an access-denied error, got: {:?}",
        String::from_utf8_lossy(&response)
    );
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
