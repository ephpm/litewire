//! TDS (SQL Server) wire protocol frontend for litewire.
//!
//! Custom TDS 7.4 implementation. Currently a placeholder.

/// Configuration for the TDS wire protocol frontend.
#[derive(Clone, Debug)]
pub struct TdsFrontendConfig {
    /// Address to listen on (e.g., `127.0.0.1:1433`).
    pub listen: std::net::SocketAddr,
}
