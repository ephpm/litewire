//! An authenticated connection whose database cannot be opened is not an
//! authentication failure.
//!
//! Regression: when the credential verified but opening the connection's
//! session failed (its database was busy behind another connection's write
//! transaction), the MySQL frontend answered the handshake with
//! `1698 Authenticate failed` — the error a wrong password gets. Operators read
//! that as a credential problem. The handshake now carries the database's own
//! error, classified like a statement error: a busy database is `1205` (lock
//! wait timeout, SQLSTATE `HY000`), which is what MySQL itself reports for an
//! exhausted lock wait and what clients and frameworks already treat as
//! retryable. A wrong password still gets `1698` / `28000`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use litewire::backend::{Backend, BackendConn, BackendError, Rusqlite, SharedBackend};
use litewire::litewire_mysql::native_password;
use litewire::{AuthOutcome, AuthRequest, ConnectionAuthenticator};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use tokio::net::TcpListener;

const BUSY: &str = "database is locked (SQLITE_BUSY): database is locked";

/// A backend whose sessions cannot be opened while `busy` is set, the way a
/// database behind another connection's long write transaction behaves.
struct SometimesBusy {
    busy: Arc<AtomicBool>,
    inner: Rusqlite,
}

#[litewire::async_trait]
impl Backend for SometimesBusy {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        if self.busy.load(Ordering::SeqCst) {
            return Err(BackendError::Sqlite(BUSY.to_string()));
        }
        self.inner.connect().await
    }
}

/// One tenant, `site-a` / `pw-for-a`, whose authenticator answers with
/// `outcome` once the password verifies.
struct OneTenant {
    password_hash: [u8; 20],
    outcome: Box<dyn Fn() -> AuthOutcome + Send + Sync>,
}

#[litewire::async_trait]
impl ConnectionAuthenticator for OneTenant {
    async fn authenticate(&self, req: &AuthRequest<'_>) -> AuthOutcome {
        if req.username != b"site-a"
            || !native_password::verify(&self.password_hash, req.salt, req.auth_response)
        {
            return AuthOutcome::Denied;
        }
        (self.outcome)()
    }
}

async fn start(outcome: impl Fn() -> AuthOutcome + Send + Sync + 'static) -> u16 {
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let auth = OneTenant {
        password_hash: native_password::password_hash(b"pw-for-a"),
        outcome: Box::new(outcome),
    };
    let server =
        litewire::LiteWire::with_authenticator(Arc::new(auth)).mysql(&format!("127.0.0.1:{port}"));
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    port
}

/// Connect, retrying only while the listener is still coming up (I/O errors);
/// any answer from the server comes straight back.
///
/// The two session variables are supplied up front so `mysql_async` does not
/// run its own setup queries after the handshake: with `db = None` the
/// handshake is then the only exchange, which is what lets a test observe a
/// handshake that succeeds with no session behind it.
async fn connect(port: u16, pass: &str, db: Option<&str>) -> Result<Conn, mysql_async::Error> {
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("site-a"))
        .pass(Some(pass))
        .db_name(db)
        .prefer_socket(false)
        .max_allowed_packet(Some(16 * 1024 * 1024))
        .wait_timeout(Some(28_800))
        .into();
    let mut last = None;
    for _ in 0..40 {
        match Conn::new(opts.clone()).await {
            Err(mysql_async::Error::Io(e)) => {
                last = Some(mysql_async::Error::Io(e));
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            other => return other,
        }
    }
    Err(last.unwrap())
}

fn server_error(err: &mysql_async::Error) -> (u16, String, String) {
    match err {
        mysql_async::Error::Server(e) => (e.code, e.state.clone(), e.message.clone()),
        other => panic!("expected a server error packet, got: {other}"),
    }
}

fn busy_tenant() -> (Arc<AtomicBool>, SharedBackend) {
    let busy = Arc::new(AtomicBool::new(true));
    let backend: SharedBackend = Arc::new(SometimesBusy {
        busy: Arc::clone(&busy),
        inner: Rusqlite::memory().unwrap(),
    });
    (busy, backend)
}

/// A client that names a database in its handshake (PDO's `dbname=`) learns
/// at connect time that the database is busy — as `1205`, not `1698`.
#[tokio::test]
async fn busy_database_at_connect_is_a_lock_wait_not_an_auth_failure() {
    let (_busy, backend) = busy_tenant();
    let port = start(move || AuthOutcome::Granted(Arc::clone(&backend))).await;

    let err = connect(port, "pw-for-a", Some("app"))
        .await
        .expect_err("the session cannot open while the database is busy");
    let (code, state, message) = server_error(&err);
    assert_eq!(
        code, 1205,
        "a busy database must be ER_LOCK_WAIT_TIMEOUT, not an access-denied error: {message}"
    );
    assert_eq!(state, "HY000");
    assert!(message.contains("database is locked"), "{message}");
    assert!(
        !message.contains("Authenticate failed"),
        "must not read as a credential failure: {message}"
    );

    // A wrong password is still an authentication failure.
    let err = connect(port, "wrong", Some("app"))
        .await
        .expect_err("a wrong password must be denied");
    let (code, state, message) = server_error(&err);
    assert_eq!(code, 1698, "{message}");
    assert_eq!(state, "28000");
    assert!(message.contains("Authenticate failed"), "{message}");
}

/// A client that names no database gets the same error on its first command,
/// and the open is retried: once the database is free the same connection
/// works.
#[tokio::test]
async fn a_connection_opened_while_busy_recovers_once_the_database_is_free() {
    let (busy, backend) = busy_tenant();
    let port = start(move || AuthOutcome::Granted(Arc::clone(&backend))).await;

    let mut conn = connect(port, "pw-for-a", None)
        .await
        .expect("no database named, so the handshake itself succeeds");
    let err = conn
        .query_drop("SELECT 1")
        .await
        .expect_err("no session yet");
    let (code, _, message) = server_error(&err);
    assert_eq!(code, 1205, "{message}");
    assert!(message.contains("database is locked"), "{message}");

    busy.store(false, Ordering::SeqCst);
    let one: Vec<i64> = conn
        .query("SELECT 1")
        .await
        .expect("the session opens on retry once the database is free");
    assert_eq!(one, vec![1]);
}

/// An authenticator that verified the credential but could not produce a
/// backend at all reports why, without an access-denied code.
#[tokio::test]
async fn unavailable_backend_is_not_an_auth_failure() {
    let port = start(|| {
        AuthOutcome::Unavailable("could not open the site's database: disk I/O error".into())
    })
    .await;

    let err = connect(port, "pw-for-a", Some("app"))
        .await
        .expect_err("there is no backend to serve");
    let (code, state, message) = server_error(&err);
    assert_eq!(code, 1105, "{message}");
    assert_eq!(state, "HY000");
    assert!(message.contains("disk I/O error"), "{message}");

    // The same with no database named: the first command answers instead.
    let mut conn = connect(port, "pw-for-a", None).await.expect("handshake");
    let (code, _, message) = server_error(&conn.query_drop("SELECT 1").await.unwrap_err());
    assert_eq!(code, 1105, "{message}");

    // A busy reason classifies as a lock wait.
    let port = start(|| AuthOutcome::Unavailable(BUSY.into())).await;
    let err = connect(port, "pw-for-a", Some("app")).await.unwrap_err();
    assert_eq!(server_error(&err).0, 1205);
}
