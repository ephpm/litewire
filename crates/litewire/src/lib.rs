//! litewire -- MySQL, PostgreSQL, TDS, and Hrana protocol proxy for SQLite.
//!
//! # Quick Start (as a library)
//!
//! ```rust,no_run
//! use litewire::{LiteWire, backend::Rusqlite};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let backend = Rusqlite::open("app.db")?;
//!
//!     LiteWire::new(backend)
//!         .mysql("127.0.0.1:3306")
//!         .hrana("127.0.0.1:8080")
//!         .serve()
//!         .await
//! }
//! ```

pub use litewire_backend as backend;
pub use litewire_translate as translate;

#[cfg(feature = "mysql")]
pub use litewire_mysql;

#[cfg(feature = "hrana")]
pub use litewire_hrana;

#[cfg(feature = "postgres")]
pub use litewire_postgres;

#[cfg(feature = "tds")]
pub use litewire_tds;

use std::net::SocketAddr;
use std::sync::Arc;

use litewire_backend::SharedBackend;

/// Builder for a litewire server instance.
pub struct LiteWire {
    backend: SharedBackend,
    #[cfg(feature = "mysql")]
    mysql_listen: Option<SocketAddr>,
    #[cfg(feature = "hrana")]
    hrana_listen: Option<SocketAddr>,
    #[cfg(feature = "postgres")]
    postgres_listen: Option<SocketAddr>,
    #[cfg(feature = "tds")]
    tds_listen: Option<SocketAddr>,
}

impl LiteWire {
    /// Create a new litewire builder with the given backend.
    pub fn new(backend: impl litewire_backend::Backend) -> Self {
        Self {
            backend: Arc::new(backend),
            #[cfg(feature = "mysql")]
            mysql_listen: None,
            #[cfg(feature = "hrana")]
            hrana_listen: None,
            #[cfg(feature = "postgres")]
            postgres_listen: None,
            #[cfg(feature = "tds")]
            tds_listen: None,
        }
    }

    /// Enable the MySQL wire protocol frontend on the given address.
    #[cfg(feature = "mysql")]
    #[must_use]
    pub fn mysql(mut self, addr: &str) -> Self {
        self.mysql_listen = addr.parse().ok();
        self
    }

    /// Enable the Hrana HTTP frontend on the given address.
    #[cfg(feature = "hrana")]
    #[must_use]
    pub fn hrana(mut self, addr: &str) -> Self {
        self.hrana_listen = addr.parse().ok();
        self
    }

    /// Enable the PostgreSQL wire protocol frontend on the given address.
    #[cfg(feature = "postgres")]
    #[must_use]
    pub fn postgres(mut self, addr: &str) -> Self {
        self.postgres_listen = addr.parse().ok();
        self
    }

    /// Enable the TDS wire protocol frontend on the given address.
    #[cfg(feature = "tds")]
    #[must_use]
    pub fn tds(mut self, addr: &str) -> Self {
        self.tds_listen = addr.parse().ok();
        self
    }

    /// Start all configured frontends and serve until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if any frontend fails to bind.
    pub async fn serve(self) -> anyhow::Result<()> {
        let mut handles: Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>> = Vec::new();

        #[cfg(feature = "mysql")]
        if let Some(addr) = self.mysql_listen {
            let config = litewire_mysql::MysqlFrontendConfig { listen: addr };
            let frontend = litewire_mysql::MysqlFrontend::new(config, Arc::clone(&self.backend));
            handles.push(tokio::spawn(async move {
                frontend.serve().await.map_err(Into::into)
            }));
        }

        #[cfg(feature = "hrana")]
        if let Some(addr) = self.hrana_listen {
            let config = litewire_hrana::HranaFrontendConfig { listen: addr };
            let frontend = litewire_hrana::HranaFrontend::new(config, Arc::clone(&self.backend));
            handles.push(tokio::spawn(async move {
                frontend.serve().await.map_err(Into::into)
            }));
        }

        #[cfg(feature = "postgres")]
        if let Some(addr) = self.postgres_listen {
            let config = litewire_postgres::PostgresFrontendConfig { listen: addr };
            let frontend =
                litewire_postgres::PostgresFrontend::new(config, Arc::clone(&self.backend));
            handles.push(tokio::spawn(async move {
                frontend.serve().await.map_err(Into::into)
            }));
        }

        #[cfg(feature = "tds")]
        if let Some(addr) = self.tds_listen {
            let config = litewire_tds::TdsFrontendConfig { listen: addr };
            let frontend =
                litewire_tds::TdsFrontend::new(config, Arc::clone(&self.backend));
            handles.push(tokio::spawn(async move {
                frontend.serve().await.map_err(Into::into)
            }));
        }

        if handles.is_empty() {
            anyhow::bail!("no frontends configured -- enable at least one listener");
        }

        // Wait for any frontend to exit (which means an error occurred).
        let (result, _index, _remaining) = futures::future::select_all(handles).await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(anyhow::anyhow!("frontend task panicked: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_backend() -> backend::Rusqlite {
        backend::Rusqlite::memory().unwrap()
    }

    // ── Builder construction ───────────────────────────────────────────────

    #[test]
    fn new_does_not_panic() {
        let _lw = LiteWire::new(memory_backend());
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_builder_returns_self() {
        let _lw = LiteWire::new(memory_backend()).mysql("127.0.0.1:3306");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_builder_returns_self() {
        let _lw = LiteWire::new(memory_backend()).postgres("127.0.0.1:5432");
    }

    #[cfg(feature = "tds")]
    #[test]
    fn tds_builder_returns_self() {
        let _lw = LiteWire::new(memory_backend()).tds("127.0.0.1:1433");
    }

    #[cfg(feature = "hrana")]
    #[test]
    fn hrana_builder_returns_self() {
        let _lw = LiteWire::new(memory_backend()).hrana("127.0.0.1:8080");
    }

    #[cfg(all(feature = "mysql", feature = "hrana"))]
    #[test]
    fn builder_chaining_mysql_and_hrana() {
        let _lw = LiteWire::new(memory_backend())
            .mysql("127.0.0.1:3306")
            .hrana("127.0.0.1:8080");
    }

    #[cfg(all(feature = "mysql", feature = "hrana", feature = "postgres", feature = "tds"))]
    #[test]
    fn builder_chaining_all_frontends() {
        let _lw = LiteWire::new(memory_backend())
            .mysql("127.0.0.1:3306")
            .postgres("127.0.0.1:5432")
            .tds("127.0.0.1:1433")
            .hrana("127.0.0.1:8080");
    }

    // ── Invalid address handling ───────────────────────────────────────────

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_invalid_address_does_not_panic() {
        let _lw = LiteWire::new(memory_backend()).mysql("not-an-address");
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_empty_address_does_not_panic() {
        let _lw = LiteWire::new(memory_backend()).mysql("");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_invalid_address_does_not_panic() {
        let _lw = LiteWire::new(memory_backend()).postgres("not-an-address");
    }

    #[cfg(feature = "hrana")]
    #[test]
    fn hrana_invalid_address_does_not_panic() {
        let _lw = LiteWire::new(memory_backend()).hrana("garbage!!!");
    }

    #[cfg(feature = "tds")]
    #[test]
    fn tds_invalid_address_does_not_panic() {
        let _lw = LiteWire::new(memory_backend()).tds("");
    }

    /// An invalid address should result in the listener field remaining `None`,
    /// so `serve()` should treat it as if no frontend was configured.
    #[cfg(feature = "mysql")]
    #[tokio::test]
    async fn invalid_address_means_no_frontend() {
        let server = LiteWire::new(memory_backend()).mysql("not-an-address");
        let result = server.serve().await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no frontends configured"),
            "expected 'no frontends configured' error, got: {err}",
        );
    }

    // ── serve() with no frontends ──────────────────────────────────────────

    #[tokio::test]
    async fn serve_no_frontends_returns_error() {
        let server = LiteWire::new(memory_backend());
        let result = server.serve().await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no frontends configured"),
            "expected 'no frontends configured' error, got: {err}",
        );
    }

    // ── serve() smoke tests (server starts and runs) ───────────────────────

    #[cfg(feature = "mysql")]
    #[tokio::test]
    async fn serve_starts_mysql() {
        let backend = memory_backend();
        let server = LiteWire::new(backend).mysql("127.0.0.1:0");
        // serve() should start without immediately erroring.
        // A timeout means the server is running (it blocks until shutdown).
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server.serve(),
        )
        .await;
        assert!(result.is_err(), "should timeout, meaning server is running");
    }

    #[cfg(feature = "hrana")]
    #[tokio::test]
    async fn serve_starts_hrana() {
        let backend = memory_backend();
        let server = LiteWire::new(backend).hrana("127.0.0.1:0");
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server.serve(),
        )
        .await;
        assert!(result.is_err(), "should timeout, meaning server is running");
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn serve_starts_postgres() {
        let backend = memory_backend();
        let server = LiteWire::new(backend).postgres("127.0.0.1:0");
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server.serve(),
        )
        .await;
        assert!(result.is_err(), "should timeout, meaning server is running");
    }

    #[cfg(feature = "tds")]
    #[tokio::test]
    async fn serve_starts_tds() {
        let backend = memory_backend();
        let server = LiteWire::new(backend).tds("127.0.0.1:0");
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server.serve(),
        )
        .await;
        assert!(result.is_err(), "should timeout, meaning server is running");
    }

    #[cfg(all(feature = "mysql", feature = "hrana"))]
    #[tokio::test]
    async fn serve_starts_multiple_frontends() {
        let backend = memory_backend();
        let server = LiteWire::new(backend)
            .mysql("127.0.0.1:0")
            .hrana("127.0.0.1:0");
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server.serve(),
        )
        .await;
        assert!(result.is_err(), "should timeout, meaning server is running");
    }
}
