//! TDS (SQL Server) wire protocol frontend for litewire.
//!
//! Custom TDS 7.4 implementation. Handles Pre-Login, Login7, and SQL Batch
//! messages, translating T-SQL to SQLite and returning results as TDS token
//! streams.

mod handler;
mod packet;
mod token;

use std::net::SocketAddr;
use std::sync::Arc;

use litewire_backend::{ConnectionLimiter, SharedBackend};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

/// Configuration for the TDS wire protocol frontend.
#[derive(Clone, Debug)]
pub struct TdsFrontendConfig {
    /// Address to listen on (e.g., `127.0.0.1:1433`).
    pub listen: SocketAddr,
    /// Maximum simultaneous client connections. `0` means unlimited,
    /// which is the historical behaviour and what the `LiteWire` builder
    /// uses unless `max_connections` is called on it.
    ///
    /// Worth setting. Each accepted connection holds a backend session for
    /// its whole lifetime, and the rusqlite backend gives every session its
    /// own OS thread and its own open SQLite handle -- so bounding
    /// connections is how you bound threads and file descriptors. Beyond
    /// the cap the connection is closed immediately; accepts are never
    /// queued.
    ///
    /// The close is bare rather than a TDS error token: a TDS client speaks
    /// first with Pre-Login, and answering before reading it would mean
    /// framing a response to a message we have not seen.
    pub max_connections: usize,
}

/// TDS wire protocol frontend.
pub struct TdsFrontend {
    config: TdsFrontendConfig,
    backend: SharedBackend,
}

impl TdsFrontend {
    /// Create a new TDS frontend.
    #[must_use]
    pub fn new(config: TdsFrontendConfig, backend: SharedBackend) -> Self {
        Self { config, backend }
    }

    /// Start accepting TDS client connections.
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
            "TDS frontend listening"
        );

        let backend = Arc::clone(&self.backend);

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("TDS accept error: {e}");
                    continue;
                }
            };
            // See litewire-mysql: Nagle + delayed ACK costs ~40ms per round trip.
            let _ = stream.set_nodelay(true);

            // Take a seat before doing any work, so an over-limit client
            // never causes a backend session -- or its OS thread -- to be
            // created.
            let Some(slot) = limiter.try_acquire() else {
                warn!(
                    %peer,
                    limit = limiter.limit().unwrap_or_default(),
                    "TDS connection refused: max_connections reached"
                );
                drop(stream);
                continue;
            };
            debug!(%peer, live = limiter.live(), "TDS client connected");

            let be = Arc::clone(&backend);
            tokio::spawn(async move {
                // Released when this task ends, on the same path that drops
                // the session and reclaims its worker thread.
                let _slot = slot;
                if let Err(e) = handler::handle_connection(stream, be).await {
                    debug!(%peer, "TDS session ended: {e}");
                }
            });
        }
    }
}
