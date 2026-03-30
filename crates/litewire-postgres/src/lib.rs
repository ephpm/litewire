//! PostgreSQL wire protocol frontend for litewire.
//!
//! Will use `pgwire` crate. Currently a placeholder.

/// Configuration for the PostgreSQL wire protocol frontend.
#[derive(Clone, Debug)]
pub struct PostgresFrontendConfig {
    /// Address to listen on (e.g., `127.0.0.1:5432`).
    pub listen: std::net::SocketAddr,
}
