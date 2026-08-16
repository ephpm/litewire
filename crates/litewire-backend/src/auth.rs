//! Per-connection backend selection driven by the wire handshake.
//!
//! # Why this exists
//!
//! The default litewire deployment is single-tenant: one [`Backend`] is fixed
//! when the server is built, and every accepted connection talks to it. That is
//! the right shape for "expose *this* database over the MySQL wire", and it is
//! unchanged.
//!
//! Multi-tenant embedders need the opposite: one listener, many databases, and
//! a connection bound to exactly one of them. [`ConnectionAuthenticator`] is
//! the hook for that. It runs **during** the handshake, before any backend
//! session exists, and it returns the backend the connection is bound to for
//! its whole lifetime — or `None`, which rejects the connection.
//!
//! # The security contract (read this before implementing the trait)
//!
//! Everything in [`AuthRequest`] except `local_addr` and `peer_addr` is
//! **attacker-controlled**. `username` and the database name in the handshake
//! are values the *client* typed; a hostile client will happily type its
//! neighbour's. They are an identity *claim*, never an identity.
//!
//! An implementation is only a tenant boundary if the backend it returns is
//! selected from something the client cannot forge. In practice that means one
//! of:
//!
//! * **A secret.** Verify `auth_response` against a credential derived per
//!   tenant, then use the *verified* `username` to pick the backend. This is
//!   how a real database does it, and it is the only option that works when
//!   every tenant runs as the same OS user (an embedded application server, a
//!   language runtime with in-process tenants), because then no socket
//!   permission can tell tenants apart.
//! * **The socket the connection arrived on.** When tenants are separated by
//!   OS credentials — distinct uids, containers, network namespaces — a
//!   per-tenant listener plus filesystem/network permissions is unforgeable,
//!   and `local_addr` identifies it. Note this is *only* a boundary if
//!   something actually stops tenant B from connecting to tenant A's socket.
//!
//! Selecting a backend from `username` alone, with no secret and no
//! per-tenant listener, is **not** isolation: it lets any client reach any
//! tenant by claiming its name.
//!
//! litewire deliberately ships no credential verification of its own — it has
//! no opinion on where your secrets live, and a half-policy would invite
//! exactly the mistake above. [`AuthRequest`] hands you the raw
//! `mysql_native_password` challenge/response inputs; see its field docs for
//! the exact bytes to check.
//!
//! # What the boundary buys, once established
//!
//! A session established through an authenticator is **tenant-scoped**: the
//! MySQL frontend wraps it in [`crate::tenant_screen`], which refuses the
//! statements that reach past the returned backend's own database file —
//! `ATTACH`/`DETACH`, `VACUUM` with a target, and the path-bearing
//! `PRAGMA`s — on every backend, whatever the engine underneath would do
//! with them. Fixed-backend (single-tenant) sessions are not screened.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::SharedBackend;

/// The handshake inputs an authenticator decides on.
///
/// Borrowed for the duration of the [`ConnectionAuthenticator::authenticate`]
/// call; copy out anything you need to keep.
#[derive(Debug, Clone, Copy)]
pub struct AuthRequest<'a> {
    /// Authentication plugin negotiated for this connection. litewire's MySQL
    /// frontend always negotiates `mysql_native_password`.
    pub auth_plugin: &'a str,

    /// Username from the handshake. **Client-asserted** — see the module docs.
    /// Not necessarily UTF-8; MySQL usernames are bytes on the wire.
    pub username: &'a [u8],

    /// The 20-byte challenge (scramble) this connection's handshake used.
    ///
    /// litewire generates a fresh random scramble per connection whenever an
    /// authenticator is installed, so a captured `auth_response` cannot be
    /// replayed against a later connection.
    pub salt: &'a [u8],

    /// The client's response to the challenge.
    ///
    /// For `mysql_native_password` this is
    /// `SHA1(password) XOR SHA1(salt || SHA1(SHA1(password)))`, so a server
    /// that stores `SHA1(SHA1(password))` can verify it without ever holding
    /// the password: recompute the XOR mask, recover the client's
    /// `SHA1(password)`, hash it once more, and compare — in constant time —
    /// against the stored value. An empty slice means the client sent no
    /// password at all.
    pub auth_response: &'a [u8],

    /// The **local** address the connection was accepted on: the listener's
    /// own address, not the client's.
    ///
    /// Unforgeable — the client cannot influence which of your listeners it
    /// landed on beyond choosing to connect to it. This is the field to key on
    /// for a per-tenant-listener topology.
    pub local_addr: SocketAddr,

    /// The client's address. Unforgeable for TCP on a trusted network path,
    /// but note that on loopback every in-process tenant shares it, so it is
    /// generally useless as a tenant discriminator.
    pub peer_addr: SocketAddr,
}

/// Chooses the backend a freshly-accepted connection is bound to, from its
/// authenticated identity.
///
/// See the [module docs](self) for the security contract — in particular, why
/// returning a backend chosen from `username` alone is not isolation.
#[async_trait::async_trait]
pub trait ConnectionAuthenticator: Send + Sync + 'static {
    /// Verify `req` and return the backend this connection may use.
    ///
    /// Returning `None` rejects the connection: the frontend replies with a
    /// MySQL access-denied error and closes it. **No backend session is opened
    /// for a rejected connection** — a connection that never authenticates has
    /// no backend at all, so there is nothing to fall back to and nothing to
    /// leak. Failing closed is the frontend's job, not yours; just return
    /// `None`.
    ///
    /// Async because a multi-tenant implementation usually has to *find* the
    /// backend — open a database file on first use, consult a registry, hit a
    /// credential store. It runs inline on the connection's task, so it delays
    /// only this connection's handshake, but do not park on a long lock here:
    /// every tenant's first connection funnels through whatever you await.
    ///
    /// Verify the credential **before** doing that work. Resolving a backend
    /// for a caller that has not proved anything turns an unauthenticated
    /// handshake into a way to make the server open files.
    async fn authenticate(&self, req: &AuthRequest<'_>) -> Option<SharedBackend>;
}

/// A shared [`ConnectionAuthenticator`], as the frontends hold it.
pub type SharedAuthenticator = Arc<dyn ConnectionAuthenticator>;

#[cfg(all(test, feature = "rusqlite"))]
mod tests {
    use super::*;

    /// An authenticator that keys on the unforgeable local address, i.e. the
    /// per-tenant-listener topology from the module docs.
    struct ByListener(SocketAddr, SharedBackend);

    #[async_trait::async_trait]
    impl ConnectionAuthenticator for ByListener {
        async fn authenticate(&self, req: &AuthRequest<'_>) -> Option<SharedBackend> {
            (req.local_addr == self.0).then(|| Arc::clone(&self.1))
        }
    }

    fn request(local: SocketAddr) -> AuthRequest<'static> {
        AuthRequest {
            auth_plugin: "mysql_native_password",
            username: b"whoever",
            salt: &[0u8; 20],
            auth_response: b"",
            local_addr: local,
            peer_addr: "127.0.0.1:5555".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn authenticator_matching_its_listener_returns_a_backend() {
        let mine: SocketAddr = "127.0.0.1:3306".parse().unwrap();
        let backend: SharedBackend = Arc::new(crate::Rusqlite::memory().unwrap());
        let auth = ByListener(mine, backend);
        assert!(auth.authenticate(&request(mine)).await.is_some());
    }

    /// The same claimed username on a *different* listener gets nothing —
    /// the discriminator is the address, which the client cannot forge.
    #[tokio::test]
    async fn authenticator_on_another_listener_is_denied() {
        let mine: SocketAddr = "127.0.0.1:3306".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:3307".parse().unwrap();
        let backend: SharedBackend = Arc::new(crate::Rusqlite::memory().unwrap());
        let auth = ByListener(mine, backend);
        assert!(auth.authenticate(&request(other)).await.is_none());
    }
}
