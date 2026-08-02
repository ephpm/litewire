//! MySQL wire protocol frontend for litewire.
//!
//! Uses `opensrv-mysql` to accept MySQL client connections, translates
//! incoming SQL from MySQL dialect to SQLite, executes against the backend,
//! and returns results in MySQL wire format.

mod error_map;
mod handler;
mod resultset;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use litewire_backend::{ConnectionLimiter, SharedBackend};
use litewire_translate::TranslateCache;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

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

/// MySQL wire protocol frontend.
pub struct MysqlFrontend {
    config: MysqlFrontendConfig,
    backend: SharedBackend,
}

impl MysqlFrontend {
    /// Create a new MySQL frontend.
    #[must_use]
    pub fn new(config: MysqlFrontendConfig, backend: SharedBackend) -> Self {
        Self { config, backend }
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
        let limiter = ConnectionLimiter::new(self.config.max_connections);
        info!(
            listen = %self.config.listen,
            max_connections = ?limiter.limit(),
            "MySQL frontend listening"
        );

        let backend = Arc::clone(&self.backend);
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

            let be = Arc::clone(&backend);
            let cache = Arc::clone(&translate_cache);
            tokio::spawn(async move {
                // Moved in, never touched again: dropping the task -- for
                // any reason, including a client that vanished
                // mid-statement -- releases the seat on exactly the same
                // path that drops the handler and reclaims its session's
                // worker thread.
                let _slot = slot;
                let handler = match LiteWireHandler::new(be, cache).await {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(%peer, "MySQL: failed to open backend session: {e}");
                        return;
                    }
                };
                let (reader, writer) = stream.into_split();
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
