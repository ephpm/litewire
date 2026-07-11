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

use litewire_backend::SharedBackend;
use litewire_translate::TranslateCache;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use handler::LiteWireHandler;

/// Configuration for the MySQL wire protocol frontend.
#[derive(Clone, Debug)]
pub struct MysqlFrontendConfig {
    /// Address to listen on (e.g., `127.0.0.1:3306`).
    pub listen: SocketAddr,
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
        info!(listen = %self.config.listen, "MySQL frontend listening");

        let backend = Arc::clone(&self.backend);
        // Shared parse+rewrite cache across every accepted connection.
        // Hot workloads (WordPress, Laravel) re-issue the same handful of
        // prepared statements repeatedly; caching drops sqlparser off the
        // hot path entirely.
        let translate_cache = Arc::new(TranslateCache::default());

        loop {
            let (stream, peer) = match listener.accept().await {
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
            debug!(%peer, "MySQL client connected");

            let be = Arc::clone(&backend);
            let cache = Arc::clone(&translate_cache);
            tokio::spawn(async move {
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
