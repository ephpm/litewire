//! Wire targets: litewire-over-Turso and the reference servers.
//!
//! Every corpus statement is driven over a real wire protocol so that
//! wire-layer defects (type encoding, an out-of-sync state machine like the
//! `select $$` probe) are caught alongside translation gaps — not just the
//! AST rewrite.
//!
//! Isolation between corpus entries is by teardown, not by a fresh listener
//! per entry: each target holds one long-lived connection, and after every
//! entry the tables that entry created are dropped. This keeps a full corpus
//! run to a handful of TCP connections instead of hundreds.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::normalize::{Cell, Rows};

/// The dedicated database / schema the harness owns on a reference server.
/// Never shared with application data — created and reset by the harness.
const WORK_DB: &str = "litewire_compat";

/// A statement that failed, with the engine/server message. `stmt` is the
/// offending SQL (a setup statement or the measured one), used to classify
/// the failure as translation versus execution.
#[derive(Clone, Debug)]
pub struct WireFailure {
    /// The SQL that failed.
    pub stmt: String,
    /// The server/engine error text.
    pub message: String,
}

// ── free port + connect retry ───────────────────────────────────────────────

async fn free_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

/// Derive the names of tables created by a sequence of statements, so they can
/// be dropped on teardown. Dialect-agnostic text scan — the corpus authors
/// simple `CREATE TABLE <name>` statements on purpose.
#[must_use]
pub fn created_tables(setup: &[&str], measured: &str) -> Vec<String> {
    let mut out = Vec::new();
    for sql in setup.iter().copied().chain(std::iter::once(measured)) {
        if let Some(name) = created_table_name(sql) {
            out.push(name);
        }
    }
    out
}

fn created_table_name(sql: &str) -> Option<String> {
    let mut toks = sql.split_whitespace();
    if !toks.next()?.eq_ignore_ascii_case("create") {
        return None;
    }
    let mut word = toks.next()?.to_ascii_lowercase();
    if word == "temporary" || word == "temp" {
        word = toks.next()?.to_ascii_lowercase();
    }
    if word != "table" {
        return None;
    }
    let mut next = toks.next()?;
    if next.eq_ignore_ascii_case("if") {
        toks.next(); // not
        toks.next(); // exists
        next = toks.next()?;
    }
    let name = next.split('(').next().unwrap_or(next);
    Some(clean_ident(name))
}

/// Strip quoting (`` ` ``, `"`, `[`/`]`) and any schema qualifier.
fn clean_ident(raw: &str) -> String {
    let unqualified = raw.rsplit('.').next().unwrap_or(raw);
    unqualified
        .trim_matches(|c| c == '`' || c == '"' || c == '[' || c == ']')
        .to_string()
}

// ── MySQL ─────────────────────────────────────────────────────────────────

/// A MySQL wire connection — either to a litewire listener (backed by an
/// in-process Turso database) or to a reference MySQL server.
pub struct MysqlWire {
    conn: Conn,
    /// Kept alive so the litewire serve task and its Turso backend outlive the
    /// connection. `None` for a reference server.
    _serve: Option<JoinHandle<()>>,
}

impl MysqlWire {
    /// Boot a litewire MySQL frontend backed by a fresh in-memory Turso engine
    /// and connect to it.
    pub async fn litewire() -> anyhow::Result<Self> {
        let port = free_port().await?;
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
        let backend = litewire::Turso::memory()
            .await
            .map_err(|e| anyhow::anyhow!("turso memory backend: {e}"))?;
        let config = litewire::litewire_mysql::MysqlFrontendConfig {
            listen: addr,
            max_connections: 0,
            log_failed_sql: false,
        };
        let frontend = litewire::litewire_mysql::MysqlFrontend::new(config, Arc::new(backend));
        let serve = tokio::spawn(async move {
            let _ = frontend.serve().await;
        });

        let opts: Opts = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(""))
            .db_name(Some("test"))
            .into();
        let conn = connect_mysql_retry(opts).await?;
        Ok(Self {
            conn,
            _serve: Some(serve),
        })
    }

    /// Connect to a reference MySQL server from a URL, creating and selecting a
    /// dedicated work database.
    pub async fn reference(url: &str) -> anyhow::Result<Self> {
        let base = Opts::from_url(url).context("parsing COMPAT_MYSQL_URL")?;

        // Create the work database on a connection with no database selected.
        let admin_opts: Opts = OptsBuilder::from_opts(base.clone())
            .db_name(None::<String>)
            .into();
        let mut admin = Conn::new(admin_opts)
            .await
            .context("connecting to reference MySQL")?;
        admin
            .query_drop(format!("CREATE DATABASE IF NOT EXISTS {WORK_DB}"))
            .await
            .context("creating reference work database")?;
        admin.disconnect().await.ok();

        let opts: Opts = OptsBuilder::from_opts(base).db_name(Some(WORK_DB)).into();
        let mut conn = Conn::new(opts).await.context("selecting work database")?;
        // Drop FK enforcement so teardown can drop tables in any order and so
        // the reference matches SQLite/Turso (FKs off by default).
        conn.query_drop("SET FOREIGN_KEY_CHECKS=0").await.ok();
        // Clear anything left by a previous crashed run.
        drop_all_mysql_tables(&mut conn).await;
        Ok(Self { conn, _serve: None })
    }

    /// Run an entry: setup statements, then the measured statement, then
    /// teardown. Returns the measured statement's rows on success.
    pub async fn run(&mut self, setup: &[&str], sql: &str) -> Result<Rows, WireFailure> {
        for s in setup {
            if let Err(e) = self.conn.query_drop(*s).await {
                let fail = WireFailure {
                    stmt: (*s).to_string(),
                    message: fmt_mysql_err(&e),
                };
                self.teardown(setup, sql).await;
                return Err(fail);
            }
        }
        let result = self.conn.query::<Row, _>(sql).await;
        let out = match result {
            Ok(rows) => Ok(rows.iter().map(mysql_row_to_cells).collect()),
            Err(e) => Err(WireFailure {
                stmt: sql.to_string(),
                message: fmt_mysql_err(&e),
            }),
        };
        self.teardown(setup, sql).await;
        out
    }

    async fn teardown(&mut self, setup: &[&str], sql: &str) {
        // Clear any transaction a transaction-shaped entry may have left open
        // (e.g. a failed COMMIT), so it cannot wedge later entries.
        let _ = self.conn.query_drop("ROLLBACK").await;
        for name in created_tables(setup, sql).iter().rev() {
            let _ = self
                .conn
                .query_drop(format!("DROP TABLE IF EXISTS {name}"))
                .await;
        }
    }
}

async fn connect_mysql_retry(opts: Opts) -> anyhow::Result<Conn> {
    for i in 0..30 {
        match Conn::new(opts.clone()).await {
            Ok(conn) => return Ok(conn),
            Err(_) if i < 29 => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e).context("connecting to litewire MySQL frontend"),
        }
    }
    unreachable!()
}

async fn drop_all_mysql_tables(conn: &mut Conn) {
    if let Ok(tables) = conn.query::<String, _>("SHOW TABLES").await {
        for t in tables {
            let _ = conn.query_drop(format!("DROP TABLE IF EXISTS {t}")).await;
        }
    }
}

fn fmt_mysql_err(e: &mysql_async::Error) -> String {
    match e {
        mysql_async::Error::Server(se) => format!("ER {}: {}", se.code, se.message),
        other => other.to_string(),
    }
}

fn mysql_row_to_cells(row: &Row) -> Vec<Cell> {
    (0..row.len())
        .map(|i| match row.as_ref(i) {
            None | Some(Value::NULL) => None,
            Some(Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Some(Value::Int(v)) => Some(v.to_string()),
            Some(Value::UInt(v)) => Some(v.to_string()),
            Some(Value::Float(v)) => Some(v.to_string()),
            Some(Value::Double(v)) => Some(v.to_string()),
            Some(Value::Date(y, mo, d, h, mi, s, us)) => {
                Some(fmt_datetime(*y, *mo, *d, *h, *mi, *s, *us))
            }
            Some(Value::Time(neg, d, h, mi, s, us)) => {
                let sign = if *neg { "-" } else { "" };
                let hours = u32::from(*h) + *d * 24;
                Some(format!(
                    "{sign}{hours:02}:{mi:02}:{s:02}{}",
                    fmt_micros(*us)
                ))
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn fmt_datetime(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> String {
    if h == 0 && mi == 0 && s == 0 && us == 0 {
        format!("{y:04}-{mo:02}-{d:02}")
    } else {
        format!(
            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}{}",
            fmt_micros(us)
        )
    }
}

fn fmt_micros(us: u32) -> String {
    if us == 0 {
        String::new()
    } else {
        format!(".{us:06}")
    }
}

// ── PostgreSQL ──────────────────────────────────────────────────────────────

/// A PostgreSQL wire connection — either to a litewire listener (backed by an
/// in-process Turso database) or to a reference PostgreSQL server. All reads
/// use the simple-query protocol so every value arrives as text, which makes
/// normalization uniform across engines.
pub struct PgWire {
    client: Client,
    _serve: Option<JoinHandle<()>>,
    _conn_driver: JoinHandle<()>,
}

impl PgWire {
    /// Boot a litewire PostgreSQL frontend backed by a fresh in-memory Turso
    /// engine and connect to it.
    pub async fn litewire() -> anyhow::Result<Self> {
        let port = free_port().await?;
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
        let backend = litewire::Turso::memory()
            .await
            .map_err(|e| anyhow::anyhow!("turso memory backend: {e}"))?;
        let config = litewire::litewire_postgres::PostgresFrontendConfig {
            listen: addr,
            max_connections: 0,
            log_failed_sql: false,
        };
        let frontend =
            litewire::litewire_postgres::PostgresFrontend::new(config, Arc::new(backend));
        let serve = tokio::spawn(async move {
            let _ = frontend.serve().await;
        });

        let connstr = format!("host=127.0.0.1 port={port} user=test dbname=test");
        let (client, driver) = connect_pg_retry(&connstr).await?;
        Ok(Self {
            client,
            _serve: Some(serve),
            _conn_driver: driver,
        })
    }

    /// Connect to a reference PostgreSQL server and isolate the run in a
    /// dedicated, freshly recreated schema.
    pub async fn reference(url: &str) -> anyhow::Result<Self> {
        let (client, driver) = connect_pg_once(url)
            .await
            .context("connecting to reference PostgreSQL")?;
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {WORK_DB} CASCADE; \
                 CREATE SCHEMA {WORK_DB}; SET search_path TO {WORK_DB}"
            ))
            .await
            .context("resetting reference schema")?;
        Ok(Self {
            client,
            _serve: None,
            _conn_driver: driver,
        })
    }

    /// Run an entry: setup, measured statement, teardown.
    pub async fn run(&mut self, setup: &[&str], sql: &str) -> Result<Rows, WireFailure> {
        for s in setup {
            if let Err(e) = self.client.simple_query(s).await {
                let fail = WireFailure {
                    stmt: (*s).to_string(),
                    message: fmt_pg_err(&e),
                };
                self.teardown(setup, sql).await;
                return Err(fail);
            }
        }
        let out = match self.client.simple_query(sql).await {
            Ok(msgs) => Ok(pg_messages_to_rows(&msgs)),
            Err(e) => Err(WireFailure {
                stmt: sql.to_string(),
                message: fmt_pg_err(&e),
            }),
        };
        self.teardown(setup, sql).await;
        out
    }

    async fn teardown(&mut self, setup: &[&str], sql: &str) {
        // Clear any open transaction (see the MySQL note); on PostgreSQL a
        // ROLLBACK with no active transaction is a harmless warning.
        let _ = self.client.simple_query("ROLLBACK").await;
        for name in created_tables(setup, sql).iter().rev() {
            let _ = self
                .client
                .simple_query(&format!("DROP TABLE IF EXISTS {name}"))
                .await;
        }
    }
}

async fn connect_pg_retry(connstr: &str) -> anyhow::Result<(Client, JoinHandle<()>)> {
    for i in 0..30 {
        match connect_pg_once(connstr).await {
            Ok(pair) => return Ok(pair),
            Err(_) if i < 29 => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e).context("connecting to litewire PostgreSQL frontend"),
        }
    }
    unreachable!()
}

async fn connect_pg_once(connstr: &str) -> anyhow::Result<(Client, JoinHandle<()>)> {
    let (client, connection) = tokio_postgres::connect(connstr, NoTls).await?;
    let driver = tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!("PG connection driver ended: {e}");
        }
    });
    Ok((client, driver))
}

fn pg_messages_to_rows(msgs: &[SimpleQueryMessage]) -> Rows {
    let mut rows = Vec::new();
    for m in msgs {
        if let SimpleQueryMessage::Row(r) = m {
            let n = r.columns().len();
            let cells = (0..n).map(|i| r.get(i).map(ToString::to_string)).collect();
            rows.push(cells);
        }
    }
    rows
}

fn fmt_pg_err(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        e.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_tables_basic() {
        let got = created_tables(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS child (id INT)",
                "INSERT INTO parent VALUES (1)",
            ],
            "SELECT * FROM parent",
        );
        assert_eq!(got, vec!["parent".to_string(), "child".to_string()]);
    }

    #[test]
    fn created_tables_quoted_and_no_space_paren() {
        let got = created_tables(&["CREATE TABLE `wp_posts`(id INT)"], "SELECT 1");
        assert_eq!(got, vec!["wp_posts".to_string()]);
    }

    #[test]
    fn non_create_ignored() {
        assert!(created_tables(&["SELECT 1"], "INSERT INTO t VALUES (1)").is_empty());
    }
}
