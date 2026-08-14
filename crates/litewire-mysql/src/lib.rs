//! MySQL wire protocol frontend for litewire.
//!
//! Uses `opensrv-mysql` to accept MySQL client connections, translates
//! incoming SQL from MySQL dialect to SQLite, executes against the backend,
//! and returns results in MySQL wire format.

mod command_filter;
mod error_map;
mod handler;
pub mod native_password;
mod resultset;
mod types;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use litewire_backend::{ConnectionLimiter, SharedAuthenticator, SharedBackend};
use litewire_translate::TranslateCache;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use command_filter::CommandFilter;
use handler::LiteWireHandler;

/// MySQL server error code `ER_CON_COUNT_ERROR` -- "Too many connections".
const ER_CON_COUNT_ERROR: u16 = 1040;

/// Configuration for the MySQL wire protocol frontend.
#[derive(Clone, Debug)]
pub struct MysqlFrontendConfig {
    /// Address to listen on (e.g., `127.0.0.1:3306`).
    pub listen: SocketAddr,
    /// Maximum simultaneous client connections. `0` means unlimited,
    /// which is the historical behaviour and what the `LiteWire` builder
    /// uses unless `max_connections` is called on it.
    ///
    /// Worth setting. Each accepted connection holds a backend session for
    /// its whole lifetime, and the rusqlite backend gives every session its
    /// own OS thread and its own open SQLite handle -- so bounding
    /// connections is how you bound threads and file descriptors. Beyond
    /// the cap a client is refused immediately with
    /// `ER_CON_COUNT_ERROR (1040)`; accepts are never queued.
    pub max_connections: usize,
}

/// Refuse a connection with a pre-handshake `ER_CON_COUNT_ERROR` packet.
///
/// This is the one place litewire writes MySQL wire bytes by hand, because
/// it happens *before* `opensrv-mysql` is involved -- the point of the cap
/// is to not build a session at all.
///
/// The packet is the pre-4.1 error form: a 4-byte header (3-byte length,
/// sequence 0) then `0xFF`, the error code little-endian, and the message.
/// It deliberately omits the `#SQLSTATE` marker, because that field only
/// exists once `CLIENT_PROTOCOL_41` has been negotiated and the client has
/// not sent its capability flags yet -- this is the client's *first* packet
/// from us, in place of the initial handshake. Real MySQL refuses
/// over-limit connections the same way.
async fn refuse_too_many(stream: &mut tokio::net::TcpStream, limit: usize) {
    let message = format!("Too many connections (litewire max_connections={limit})");
    let payload_len = 1 + 2 + message.len();
    // A 3-byte length field caps a packet at 16 MiB; the message above is
    // far smaller, but keep the invariant explicit rather than implied.
    debug_assert!(payload_len < 0x00ff_ffff);

    let mut packet = Vec::with_capacity(4 + payload_len);
    #[allow(clippy::cast_possible_truncation)]
    {
        packet.push((payload_len & 0xff) as u8);
        packet.push(((payload_len >> 8) & 0xff) as u8);
        packet.push(((payload_len >> 16) & 0xff) as u8);
    }
    packet.push(0); // sequence id 0 -- we are replacing the handshake
    packet.push(0xff); // ERR packet marker
    packet.extend_from_slice(&ER_CON_COUNT_ERROR.to_le_bytes());
    packet.extend_from_slice(message.as_bytes());

    // Best effort: the client may already be gone, and there is nothing
    // useful to do about it either way.
    let _ = stream.write_all(&packet).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

/// One of several independent handles to the same accepted connection.
///
/// A MySQL connection needs three: the read half feeding
/// [`CommandFilter`], the write half `opensrv-mysql` buffers its responses
/// through, and a second write handle the filter answers intercepted
/// commands on. `TcpStream::into_split` yields only two, so share the
/// socket through an `Arc` and drive it with `TcpStream`'s `&self`
/// readiness API instead.
///
/// Concurrent use is safe because the MySQL wire protocol is strictly
/// half-duplex: `opensrv-mysql` flushes its writer at the end of every
/// command and is then parked in `poll_read`, so the only moment the filter
/// writes is a moment nothing else is writing.
struct SocketHandle(Arc<TcpStream>);

impl AsyncRead for SocketHandle {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            ready!(self.0.poll_read_ready(cx))?;
            match self.0.try_read(buf.initialize_unfilled()) {
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                // Readiness is edge-triggered and can be stale; re-arm.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncWrite for SocketHandle {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.0.poll_write_ready(cx))?;
            match self.0.try_write(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    /// Kept vectored so `opensrv-mysql` can still emit a packet's header and
    /// payload in one call, as it did when this was an `OwnedWriteHalf`.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.0.poll_write_ready(cx))?;
            match self.0.try_write_vectored(bufs) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    /// A TCP socket holds nothing back, so there is nothing to flush.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// The connection is closed by dropping the last handle, which is what
    /// happens when the per-connection task ends.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// How a connection accepted by this frontend gets its backend.
#[derive(Clone)]
enum BackendSource {
    /// One backend for every connection, chosen when the frontend is built.
    Fixed(SharedBackend),
    /// Resolved per connection during the handshake, by embedder policy.
    /// There is no default backend on this path.
    PerConnection(SharedAuthenticator),
}

/// MySQL wire protocol frontend.
pub struct MysqlFrontend {
    config: MysqlFrontendConfig,
    source: BackendSource,
}

impl MysqlFrontend {
    /// Create a new MySQL frontend serving one backend to every client.
    #[must_use]
    pub fn new(config: MysqlFrontendConfig, backend: SharedBackend) -> Self {
        Self {
            config,
            source: BackendSource::Fixed(backend),
        }
    }

    /// Create a MySQL frontend whose connections are bound to a backend
    /// chosen per connection by `authenticator`.
    ///
    /// The multi-tenant shape: one listener, many databases. The frontend
    /// holds no backend of its own, so a connection whose handshake is not
    /// accepted by `authenticator` has nothing to reach — see
    /// [`ConnectionAuthenticator`](litewire_backend::ConnectionAuthenticator)
    /// for the security contract you are signing up to, in particular why the
    /// handshake username is a claim rather than an identity.
    #[must_use]
    pub fn new_authenticating(
        config: MysqlFrontendConfig,
        authenticator: SharedAuthenticator,
    ) -> Self {
        Self {
            config,
            source: BackendSource::PerConnection(authenticator),
        }
    }

    /// Start accepting MySQL client connections.
    ///
    /// Runs until the tokio runtime shuts down.
    ///
    /// # Errors
    ///
    /// Returns an error if binding the listen address fails.
    pub async fn serve(self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.config.listen).await?;
        // The address actually bound, which differs from the configured one
        // when port 0 was requested. This is what an authenticator keying on
        // the listener must see, so read it back rather than echoing config.
        let local_addr = listener.local_addr()?;
        let limiter = ConnectionLimiter::new(self.config.max_connections);
        info!(
            listen = %local_addr,
            max_connections = ?limiter.limit(),
            authenticating = matches!(self.source, BackendSource::PerConnection(_)),
            "MySQL frontend listening"
        );

        let source = self.source.clone();
        // Shared parse+rewrite cache across every accepted connection.
        // Hot workloads (WordPress, Laravel) re-issue the same handful of
        // prepared statements repeatedly; caching drops sqlparser off the
        // hot path entirely.
        let translate_cache = Arc::new(TranslateCache::default());

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("MySQL accept error: {e}");
                    continue;
                }
            };
            // MySQL wire is small request/response packets; without this,
            // Nagle + delayed ACK stalls every round trip ~40ms on Linux
            // loopback (measured 44ms/query via PDO, 2026-07-09).
            let _ = stream.set_nodelay(true);

            // Take a seat before doing any work. Over the cap the client is
            // told so and disconnected here, without a backend session --
            // and therefore without an OS thread -- ever being created.
            let Some(slot) = limiter.try_acquire() else {
                let limit = limiter.limit().unwrap_or_default();
                warn!(%peer, limit, "MySQL connection refused: max_connections reached");
                tokio::spawn(async move { refuse_too_many(&mut stream, limit).await });
                continue;
            };
            debug!(%peer, live = limiter.live(), "MySQL client connected");

            let source = source.clone();
            let cache = Arc::clone(&translate_cache);
            tokio::spawn(async move {
                // Moved in, never touched again: dropping the task -- for
                // any reason, including a client that vanished
                // mid-statement -- releases the seat on exactly the same
                // path that drops the handler and reclaims its session's
                // worker thread.
                let _slot = slot;
                // Whether `COM_CHANGE_USER` may be answered with a no-op OK:
                // safe when every connection reaches the same backend, not
                // when a handshake selects one (see `CommandFilter`).
                let single_backend = matches!(source, BackendSource::Fixed(_));
                let handler = match source {
                    BackendSource::Fixed(be) => match LiteWireHandler::new(be, cache).await {
                        Ok(h) => h,
                        Err(e) => {
                            warn!(%peer, "MySQL: failed to open backend session: {e}");
                            return;
                        }
                    },
                    // No backend is opened yet: the handshake decides which
                    // one (if any) this connection may have.
                    BackendSource::PerConnection(auth) => {
                        LiteWireHandler::new_authenticating(auth, cache, local_addr, peer)
                    }
                };
                let reset_signal = handler.reset_signal();
                // Three handles to one socket; see `SocketHandle`.
                let stream = Arc::new(stream);
                let reader = SocketHandle(Arc::clone(&stream));
                let writer = SocketHandle(Arc::clone(&stream));
                // Screen out the command packets `opensrv-mysql` would
                // answer with a stray OK packet. Buffered so reassembling a
                // packet header costs memory copies rather than syscalls.
                let reader = CommandFilter::new(
                    tokio::io::BufReader::with_capacity(8 * 1024, reader),
                    SocketHandle(stream),
                    reset_signal,
                    single_backend,
                );
                // Coalesce the whole response into a single write.
                //
                // A result set is emitted by opensrv-mysql as several distinct
                // packets (column-count, one column-def per column, EOF, one
                // packet per row, EOF), each written to the socket separately;
                // opensrv only calls `flush()` once, after the full response.
                // On a raw socket every packet becomes its own TCP segment, so
                // a client whose Nagle is enabled (PHP mysqlnd does NOT set
                // TCP_NODELAY) withholds its ACK of the first segment while it
                // waits for more data, and Linux delayed-ACK holds that ACK for
                // ~40ms, stalling every result-set round trip (measured 44ms
                // p50 per point-SELECT via PDO, vs 1.3ms for an INSERT, whose
                // response is a single OK packet). Server-side `set_nodelay`
                // alone does not cure it because the deadlock is driven by the
                // client's Nagle, not the server's. Buffering makes opensrv's
                // single trailing `flush()` emit the entire result set as one
                // segment, so there is no intermediate packet for the client to
                // sit on. The buffer is sized for the common small result set;
                // larger responses flush in chunks, which is still correct.
                let writer = tokio::io::BufWriter::with_capacity(64 * 1024, writer);
                if let Err(e) =
                    opensrv_mysql::AsyncMysqlIntermediary::run_on(handler, reader, writer).await
                {
                    debug!(%peer, "MySQL session ended: {e}");
                }
            });
        }
    }
}
