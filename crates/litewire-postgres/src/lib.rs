//! PostgreSQL wire protocol frontend for litewire.
//!
//! Uses `pgwire` to accept PostgreSQL client connections, translates
//! incoming SQL from PostgreSQL dialect to SQLite, executes against the
//! backend, and returns results in PostgreSQL wire format.

mod error_map;
mod handler;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use litewire_backend::{ConnectionLimiter, SharedBackend};
use litewire_translate::TranslateCache;
use pgwire::api::NoopErrorHandler;
use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use handler::PostgresHandler;

/// Configuration for the PostgreSQL wire protocol frontend.
#[derive(Clone, Debug)]
pub struct PostgresFrontendConfig {
    /// Address to listen on (e.g., `127.0.0.1:5432`).
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
    /// Unlike the MySQL frontend, which answers with
    /// `ER_CON_COUNT_ERROR (1040)`, this one just closes. A PostgreSQL
    /// client speaks first -- `SSLRequest` or `StartupMessage` -- and
    /// writing an `ErrorResponse` before reading that would mean guessing
    /// which of the two is arriving. libpq reports a closed connection
    /// clearly enough that the guess is not worth the risk of desyncing
    /// the stream.
    pub max_connections: usize,
}

/// PostgreSQL wire protocol frontend.
pub struct PostgresFrontend {
    config: PostgresFrontendConfig,
    backend: SharedBackend,
}

impl PostgresFrontend {
    /// Create a new PostgreSQL frontend.
    #[must_use]
    pub fn new(config: PostgresFrontendConfig, backend: SharedBackend) -> Self {
        Self { config, backend }
    }

    /// Start accepting PostgreSQL client connections.
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
            "PostgreSQL frontend listening"
        );

        // Shared parse+rewrite cache across every accepted connection --
        // same rationale as the MySQL frontend.
        let translate_cache = Arc::new(TranslateCache::default());
        let backend = Arc::clone(&self.backend);

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("PostgreSQL accept error: {e}");
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
                    "PostgreSQL connection refused: max_connections reached"
                );
                drop(stream);
                continue;
            };
            debug!(%peer, live = limiter.live(), "PostgreSQL client connected");

            // Per-connection factory: each accepted client gets its own
            // PostgresHandler with its own BackendConn, so transactions
            // are isolated across pgwire sessions. Same rationale as
            // litewire-mysql -- see BackendConn docs.
            let be = Arc::clone(&backend);
            let cache = Arc::clone(&translate_cache);
            tokio::spawn(async move {
                // Released when this task ends, on the same path that drops
                // the handler and reclaims its session's worker thread.
                let _slot = slot;
                let handler = match PostgresHandler::new(be, cache).await {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(%peer, "PostgreSQL: failed to open backend session: {e}");
                        return;
                    }
                };
                let factory = Arc::new(LiteWireHandlerFactory {
                    handler: Arc::new(handler),
                });
                if let Err(e) = process_socket(stream, None, factory).await {
                    debug!(%peer, "PostgreSQL session ended: {e}");
                }
            });
        }
    }
}

/// No-op startup handler that accepts all connections without authentication.
struct LiteWireStartupHandler;

impl NoopStartupHandler for LiteWireStartupHandler {}

/// Factory that provides handler instances to pgwire's socket processor.
struct LiteWireHandlerFactory {
    handler: Arc<PostgresHandler>,
}

impl PgWireServerHandlers for LiteWireHandlerFactory {
    type StartupHandler = LiteWireStartupHandler;
    type SimpleQueryHandler = PostgresHandler;
    type ExtendedQueryHandler = PostgresHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(LiteWireStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}
