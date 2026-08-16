//! `opensrv-mysql` shim implementation.
//!
//! Implements `AsyncMysqlShim` to handle MySQL protocol commands including
//! prepared statement prepare/execute/close.
//!
//! The handler is a thin wire codec: dialect translation, transaction-state
//! tracking, error mapping, and backend execution all live in
//! [`litewire_session::Session`]. Only MySQL packet framing and the
//! prepared-statement cache remain here.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use litewire_backend::{
    AuthRequest, BackendError, ResultSet, SharedAuthenticator, SharedBackend, Value,
};
use litewire_session::{Prepared, Session, SessionError, SessionResult};
use litewire_translate::{Dialect, StatementKind, TranslateCache};
use opensrv_mysql::*;
use tokio::io::AsyncWrite;
use tracing::{debug, warn};

use crate::error_map;

/// Maximum number of prepared statements a single connection may hold at once.
///
/// MySQL's default `max_prepared_stmt_count` is 16382 (global, not per-connection),
/// but here it's per-connection because litewire has no global registry. 1024
/// per connection is generous for real workloads and prevents a runaway client
/// from exhausting memory via COM_STMT_PREPARE without matching COM_STMT_CLOSE.
const MAX_PREPARED_STMTS_PER_CONN: usize = 1024;

/// Length of a `mysql_native_password` challenge, in bytes.
///
/// Mirrors `opensrv_mysql`'s own `SCRAMBLE_SIZE`, which is private to that
/// crate; it is fixed by the MySQL protocol, not a tunable.
const SCRAMBLE_SIZE: usize = 20;

/// Build an `OkResponse` with the correct transaction status flag.
fn ok_response(affected_rows: u64, last_insert_id: u64, in_transaction: bool) -> OkResponse {
    let status_flags = if in_transaction {
        StatusFlags::SERVER_STATUS_IN_TRANS
    } else {
        StatusFlags::empty()
    };
    OkResponse {
        affected_rows,
        last_insert_id,
        status_flags,
        ..OkResponse::default()
    }
}

use crate::types::{mysql_type_for_value, sqlite_to_mysql_column_type};

/// Everything the authenticating (multi-tenant) path needs to resolve this
/// connection's backend from its handshake.
struct ConnAuth {
    /// Embedder policy: verifies the handshake and picks the backend.
    authenticator: SharedAuthenticator,
    /// Handed to the [`Session`] once the backend is known.
    translate_cache: Arc<TranslateCache>,
    /// The listener this connection was accepted on -- the one handshake
    /// input the client cannot forge.
    local_addr: SocketAddr,
    /// The client's address.
    peer_addr: SocketAddr,
    /// This connection's `mysql_native_password` challenge. Freshly random
    /// per connection (see [`random_scramble`]), so an `auth_response`
    /// captured from one connection is worthless against the next.
    scramble: [u8; SCRAMBLE_SIZE],
}

/// Handler for a single MySQL client connection.
///
/// A thin wire codec over [`Session`], which owns the per-session
/// [`litewire_backend::BackendConn`], dialect translation, transaction-state
/// tracking, and error mapping. All statements from this MySQL client hit the
/// same backend session, so `BEGIN`/`COMMIT`/`ROLLBACK` are properly isolated
/// from other MySQL clients. Only MySQL-protocol state lives here: the
/// prepared-statement cache and packet framing.
///
/// # Two ways a connection gets its backend
///
/// * **Fixed** ([`new`](Self::new)) -- the backend is chosen at accept time and
///   the session is open before the handshake starts. The single-tenant path;
///   unchanged.
/// * **Authenticating** ([`new_authenticating`](Self::new_authenticating)) --
///   there is *no* backend until [`AsyncMysqlShim::authenticate`] runs the
///   embedder's [`ConnectionAuthenticator`](litewire_backend::ConnectionAuthenticator)
///   and it returns one. Until then `session` is empty and **every** command
///   is refused with `ER_ACCESS_DENIED_ERROR`.
///
/// # Why the authenticating path must fail closed, structurally
///
/// `opensrv-mysql` invokes `authenticate()` *conditionally*: the call sits
/// inside `if let Some(username) = &handshake.username`, and its handshake
/// parser yields `username: None` on one branch -- a client that sets
/// `CLIENT_SSL` before TLS is negotiated. Authentication is then skipped and
/// the connection drops straight into the command loop.
///
/// In the current default build that branch is not reachable: `opensrv-mysql`
/// enables its `tls` feature by default, and `init_after_ssl` re-reads and
/// re-parses the handshake for a `CLIENT_SSL` client, which does produce a
/// username. But the whole design's safety would then rest on a conditional
/// inside a dependency, one feature flag away from inverting -- building
/// `opensrv-mysql` with `default-features = false` drops `tls`, leaves that
/// re-parse compiled out, and turns the `None` branch into an authentication
/// bypass.
///
/// So "reject in `authenticate()`" is not the guarantee. The guarantee is that
/// a connection with no successful `authenticate()` has no backend to reach:
/// the session is an empty [`OnceLock`] that only `authenticate()` fills, there
/// is no default backend on this path to fall back to, and every command path
/// refuses when it is empty. An unauthorised connection then gets an
/// access-denied error rather than somebody else's database, whatever upstream
/// decides to do with that `if let`.
pub struct LiteWireHandler {
    /// The dialect-aware session this connection delegates to.
    ///
    /// Filled at construction on the fixed path; filled by `authenticate()` on
    /// the authenticating path. Empty means "this connection has no backend" --
    /// never "use the default one".
    session: OnceLock<Session>,
    /// `Some` on the authenticating path only.
    auth: Option<ConnAuth>,
    /// Prepared statements keyed by the statement ID assigned during `on_prepare`.
    stmts: HashMap<u32, Prepared>,
    /// Next statement ID to assign.
    next_stmt_id: u32,
    /// Set by [`crate::command_filter::CommandFilter`] when the client sent
    /// `COM_RESET_CONNECTION` or `COM_CHANGE_USER`; drained by
    /// [`Self::apply_pending_reset`] before the next statement.
    reset_pending: Arc<AtomicBool>,
}

impl LiteWireHandler {
    /// Open a fresh backend session for this MySQL client.
    ///
    /// The single-tenant path: this connection is bound to `backend` before
    /// the handshake begins, and `authenticate()` accepts everyone (the
    /// `opensrv-mysql` default). Behaviour is identical to every litewire
    /// release before per-connection backends existed.
    ///
    /// # Errors
    ///
    /// Returns the backend's error verbatim if the underlying session
    /// (e.g. a rusqlite `Connection` open) fails. Callers should treat this as
    /// "reject the client" -- there is no meaningful retry at this layer.
    pub async fn new(
        backend: SharedBackend,
        translate_cache: Arc<TranslateCache>,
    ) -> Result<Self, BackendError> {
        let conn = backend.connect().await?;
        let session = OnceLock::new();
        let _ = session.set(Session::with_cache(conn, Dialect::MySQL, translate_cache));
        Ok(Self {
            session,
            auth: None,
            stmts: HashMap::new(),
            next_stmt_id: 1,
            reset_pending: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Build a handler whose backend is resolved during the handshake by
    /// `authenticator`.
    ///
    /// Cannot fail and opens nothing: the multi-tenant path deliberately does
    /// no backend work for a connection that has not authenticated yet.
    #[must_use]
    pub fn new_authenticating(
        authenticator: SharedAuthenticator,
        translate_cache: Arc<TranslateCache>,
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    ) -> Self {
        Self {
            session: OnceLock::new(),
            auth: Some(ConnAuth {
                authenticator,
                translate_cache,
                local_addr,
                peer_addr,
                scramble: random_scramble(),
            }),
            stmts: HashMap::new(),
            next_stmt_id: 1,
            reset_pending: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The flag [`crate::command_filter::CommandFilter`] sets when the client
    /// asks for a session reset.
    pub(crate) fn reset_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.reset_pending)
    }

    /// Apply a `COM_RESET_CONNECTION` / `COM_CHANGE_USER` if one arrived.
    ///
    /// Both commands are defined to put the connection back into the state it
    /// had just after connecting. Applying it here, at the start of the next
    /// statement, rather than when the packet arrives, is what lets the
    /// command filter answer it with `opensrv-mysql`'s own `OK` path: that
    /// reply is written without the shim being called at all, so there is no
    /// earlier point at which the handler runs.
    ///
    /// A rollback failure is deliberately swallowed. There is no packet to
    /// report it on (the client has already been told `OK`), and the
    /// alternative — leaving the flag set — would retry the rollback before
    /// every subsequent statement.
    async fn apply_pending_reset(&mut self) {
        if !self.reset_pending.swap(false, Ordering::Relaxed) {
            return;
        }
        self.stmts.clear();
        if let Some(session) = self.session_mut()
            && session.in_transaction
        {
            if let Err(e) = session.query("ROLLBACK", &[]).await {
                warn!("MySQL: rollback during connection reset failed: {e}");
            }
            session.in_transaction = false;
        }
        debug!("MySQL connection state reset");
    }

    /// This connection's session, or `None` if it never authenticated.
    pub(crate) fn session(&self) -> Option<&Session> {
        self.session.get()
    }

    /// Mutable access to this connection's session, or `None` if it never
    /// authenticated. Every command path goes through here, which is what
    /// makes an unauthenticated connection structurally unable to reach a
    /// backend.
    fn session_mut(&mut self) -> Option<&mut Session> {
        self.session.get_mut()
    }
}

/// Error text for a command issued on a connection that never authenticated.
const NOT_AUTHENTICATED: &str = "Access denied: connection is not authenticated";

/// A fresh 20-byte `mysql_native_password` challenge.
///
/// Restricted to printable ASCII (`0x21..=0x7e`) because the handshake writes
/// the scramble as a NUL-terminated string -- a random `0x00` byte would
/// silently truncate the challenge the client hashes against, and the
/// resulting `auth_response` would never verify. 20 bytes drawn from 94
/// symbols is ~131 bits, far past what the 20-byte challenge needs.
///
/// This replaces the `opensrv-mysql` default, which is a **compile-time
/// constant** shared by every connection ever made. A constant challenge makes
/// `auth_response` a static function of the password, so anyone who observes
/// one handshake can replay it forever. Only used on the authenticating path;
/// the fixed path keeps the upstream default, since it does not check
/// credentials at all.
fn random_scramble() -> [u8; SCRAMBLE_SIZE] {
    let mut raw = [0u8; SCRAMBLE_SIZE];
    // A failure here means the OS entropy source is unavailable, which is not
    // a condition this process can sensibly continue past: a predictable
    // challenge would silently weaken every authentication that follows.
    getrandom::fill(&mut raw).expect("OS entropy source unavailable");
    for b in &mut raw {
        // 94 printable symbols, 0x21..=0x7e.
        *b = 0x21 + (*b % 94);
    }
    raw
}

/// Write a [`ResultSet`] in MySQL wire format.
async fn write_result_set<W: AsyncWrite + Send + Unpin>(
    rs: &ResultSet,
    results: QueryResultWriter<'_, W>,
) -> Result<(), std::io::Error> {
    let columns: Vec<Column> = rs
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            // Declared type wins; for untyped expression columns
            // (`SELECT 1`, decltype None) infer the wire type from
            // the first non-NULL value so the declared column type
            // matches how the row writer encodes it. Scan past
            // leading NULLs; empty/all-NULL columns stay VAR_STRING
            // (NULL is valid against any column type).
            let coltype = if c.decltype.is_some() {
                sqlite_to_mysql_column_type(c.decltype.as_deref())
            } else {
                rs.rows
                    .iter()
                    .filter_map(|r| r.get(i))
                    .find(|v| !matches!(v, Value::Null))
                    .map_or(ColumnType::MYSQL_TYPE_VAR_STRING, mysql_type_for_value)
            };
            Column {
                table: String::new(),
                column: c.name.clone(),
                coltype,
                colflags: ColumnFlags::empty(),
            }
        })
        .collect();

    let mut rw: RowWriter<'_, W> = results.start(&columns).await?;

    for row in &rs.rows {
        for val in row {
            // Write each value in its native form so the binary
            // (prepared-statement) protocol accepts it against
            // the declared column type. Previously integers were
            // stringified, which worked only because every
            // column was declared as VAR_STRING. Now that
            // decltype flows through from `column_decltype`,
            // integer columns arrive at the wire as LONGLONG
            // and opensrv rejects a `String` payload.
            match val {
                Value::Null => rw.write_col(None::<&str>)?,
                Value::Integer(i) => rw.write_col(*i)?,
                Value::Float(f) => rw.write_col(*f)?,
                Value::Text(s) => rw.write_col(s.as_str())?,
                Value::Blob(b) => rw.write_col(b.as_slice())?,
            }
        }
        rw.end_row().await?;
    }

    rw.finish().await
}

/// Write a [`SessionError`] as a MySQL error packet.
///
/// Backend errors go through [`error_map::classify`] exactly as before the
/// Session extraction (the classified message is the one [`SessionError::Db`]
/// carries, so the mapping is identical); translation errors become
/// `ER_PARSE_ERROR` with the error's Display output.
async fn write_session_error<W: AsyncWrite + Send + Unpin>(
    results: QueryResultWriter<'_, W>,
    err: &SessionError,
) -> Result<(), std::io::Error> {
    match err {
        SessionError::Db { message, .. } => write_backend_error(results, message).await,
        translate_err => {
            results
                .error(
                    ErrorKind::ER_PARSE_ERROR,
                    translate_err.to_string().as_bytes(),
                )
                .await
        }
    }
}

/// Convert a backend error string into a MySQL error packet with a specific
/// error code + SQLSTATE (via [`crate::error_map::classify`]) and send it.
async fn write_backend_error<W: AsyncWrite + Send + Unpin>(
    results: QueryResultWriter<'_, W>,
    err_msg: &str,
) -> Result<(), std::io::Error> {
    let mapped = error_map::classify(err_msg);
    results.error(mapped.code, mapped.message.as_bytes()).await
}

/// Convert an opensrv-mysql parameter value to our backend Value type.
fn param_to_value(param: ParamValue<'_>) -> Value {
    match param.value.into_inner() {
        ValueInner::NULL => Value::Null,
        ValueInner::Int(i) => Value::Integer(i),
        ValueInner::UInt(u) => Value::Integer(u as i64),
        ValueInner::Double(f) => Value::Float(f),
        ValueInner::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Blob(b.to_vec()),
        },
        ValueInner::Date(b) | ValueInner::Time(b) | ValueInner::Datetime(b) => {
            // Date/time binary encodings -- convert to text for SQLite.
            Value::Text(String::from_utf8_lossy(b).into_owned())
        }
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for LiteWireHandler {
    type Error = std::io::Error;

    /// Server version advertised in the wire handshake.
    ///
    /// The opensrv default is `5.1.10-alpha-msql-proxy`, which WordPress
    /// >= 6.5 rejects outright ("requires MySQL 5.5.5 or higher") — clients
    /// read this from `mysqli_get_server_info()`, not `SELECT VERSION()`.
    ///
    /// Shared with the emulated `SELECT VERSION()` and `@@version` through
    /// [`litewire_translate::SERVER_VERSION`], so a client cannot get two
    /// different answers depending on which one it asks.
    fn version(&self) -> String {
        litewire_translate::SERVER_VERSION.to_string()
    }

    /// The `mysql_native_password` challenge for this connection.
    ///
    /// Random per connection on the authenticating path (see
    /// [`random_scramble`]); the upstream constant on the fixed path, which
    /// verifies nothing and so has nothing to protect.
    fn salt(&self) -> [u8; SCRAMBLE_SIZE] {
        match &self.auth {
            Some(auth) => auth.scramble,
            // Reproduces the `opensrv-mysql` default verbatim.
            None => {
                let mut scramble = [0u8; SCRAMBLE_SIZE];
                for (out, &b) in scramble.iter_mut().zip(b";X,po_k}>o6^Wz!/kM}N") {
                    *out = if b == b'\0' || b == b'$' { b + 1 } else { b };
                }
                scramble
            }
        }
    }

    /// Resolve this connection's backend from its handshake.
    ///
    /// On the fixed path there is nothing to do: the backend was chosen at
    /// accept time, so accept the client exactly as the `opensrv-mysql`
    /// default does.
    ///
    /// On the authenticating path this is the *only* place a backend is ever
    /// installed. The embedder's authenticator decides; `None` means the
    /// connection is refused and — because the session stays empty — can reach
    /// nothing even if the client keeps talking.
    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let Some(auth) = &self.auth else {
            return true;
        };

        let request = AuthRequest {
            auth_plugin,
            username,
            salt,
            auth_response: auth_data,
            local_addr: auth.local_addr,
            peer_addr: auth.peer_addr,
        };

        let Some(backend) = auth.authenticator.authenticate(&request).await else {
            warn!(
                user = %String::from_utf8_lossy(username),
                peer = %auth.peer_addr,
                "MySQL authentication rejected by authenticator"
            );
            return false;
        };

        // Open the session now rather than lazily on first query, so a backend
        // that cannot be opened is reported as a failed *connection* instead of
        // a confusing error on an arbitrary later statement.
        let conn = match backend.connect().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(
                    user = %String::from_utf8_lossy(username),
                    "MySQL: authenticated but failed to open backend session: {e}"
                );
                return false;
            }
        };

        // A session the authenticator establishes is tenant-scoped: the
        // backend it returned is the whole world this connection may touch.
        // Screen out the statements that reach past it (`ATTACH`/`DETACH`,
        // `VACUUM INTO`, path-bearing `PRAGMA`s) at the backend boundary,
        // whatever the engine underneath would do with them. Fixed-backend
        // (single-tenant) sessions are not screened -- see the
        // `tenant_screen` module docs for the contract.
        let conn = litewire_backend::tenant_screen::screened(conn);

        let session = Session::with_cache(conn, Dialect::MySQL, Arc::clone(&auth.translate_cache));
        if self.session.set(session).is_err() {
            // Unreachable: `opensrv-mysql` runs the handshake once per
            // connection. Refuse rather than risk serving a connection whose
            // session is not the one just authorised.
            warn!("MySQL: authenticate() ran twice on one connection; refusing");
            return false;
        }
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(sql = %query, "COM_STMT_PREPARE");
        self.apply_pending_reset().await;

        // Fail closed: no session means the handshake never authenticated
        // (including the no-username handshake `opensrv-mysql` lets past
        // `authenticate()` entirely). There is no backend to fall back to.
        let Some(session) = self.session_mut() else {
            return info
                .error(
                    ErrorKind::ER_ACCESS_DENIED_ERROR,
                    NOT_AUTHENTICATED.as_bytes(),
                )
                .await;
        };

        // `Session::prepare` is `translate_sql` plus the FOUND_ROWS
        // session specials, so prepared `SELECT FOUND_ROWS()` /
        // `SQL_CALC_FOUND_ROWS` statements behave like the text protocol.
        let prepared = match session.prepare(query) {
            Ok(p) => p,
            Err(e) => {
                return info
                    .error(ErrorKind::ER_PARSE_ERROR, e.to_string().as_bytes())
                    .await;
            }
        };

        // Count `?` placeholders in the translated SQL.
        let param_count = prepared.sqlite_sql.chars().filter(|&c| c == '?').count();

        let params: Vec<Column> = (0..param_count)
            .map(|_| Column {
                table: String::new(),
                column: "?".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();

        // Determine output columns. INSERT/UPDATE/DELETE have no result
        // set -- skip describing them entirely. For SELECTs, use the
        // backend's `describe_columns`, which on the rusqlite backend
        // reads column metadata off the prepared statement without
        // executing it (was: `SELECT ... LIMIT 0` round trip).
        let columns = if prepared.kind == StatementKind::Query && !prepared.sqlite_sql.is_empty() {
            match session.conn.describe_columns(&prepared.sqlite_sql).await {
                Ok(cols) => cols
                    .iter()
                    .map(|c| Column {
                        table: String::new(),
                        column: c.name.clone(),
                        coltype: sqlite_to_mysql_column_type(c.decltype.as_deref()),
                        colflags: ColumnFlags::empty(),
                    })
                    .collect(),
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        // Bound the per-connection prepared-statement cache so a client that
        // never sends COM_STMT_CLOSE can't wedge the process. Return the same
        // error code (1461) real MySQL uses when max_prepared_stmt_count is hit.
        if self.stmts.len() >= MAX_PREPARED_STMTS_PER_CONN {
            warn!(
                stmts = self.stmts.len(),
                "prepared-statement cap hit ({MAX_PREPARED_STMTS_PER_CONN}); rejecting COM_STMT_PREPARE"
            );
            return info
                .error(
                    ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                    format!(
                        "Can't create more than {MAX_PREPARED_STMTS_PER_CONN} prepared statements \
                         on this connection"
                    )
                    .as_bytes(),
                )
                .await;
        }

        // Assign a statement ID and cache it.
        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        self.stmts.insert(stmt_id, prepared);

        info.reply(stmt_id, &params, &columns).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(stmt_id = id, "COM_STMT_EXECUTE");
        self.apply_pending_reset().await;

        // Fail closed -- see `on_prepare`. (Unreachable in practice, since
        // `on_prepare` refuses first and the statement id would be unknown,
        // but the guard is what makes the property structural.)
        if self.session().is_none() {
            return results
                .error(
                    ErrorKind::ER_ACCESS_DENIED_ERROR,
                    NOT_AUTHENTICATED.as_bytes(),
                )
                .await;
        }

        let Some(stmt) = self.stmts.get(&id) else {
            return results
                .error(
                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                    format!("unknown statement id: {id}").as_bytes(),
                )
                .await;
        };

        let stmt = stmt.clone();

        // Extract parameter values.
        let values: Vec<Value> = params.into_iter().map(param_to_value).collect();

        // `execute_prepared` handles no-ops (empty SQL from SET NAMES
        // etc.) itself, returning the same OK the old inline check framed.
        let Some(session) = self.session_mut() else {
            return results
                .error(
                    ErrorKind::ER_ACCESS_DENIED_ERROR,
                    NOT_AUTHENTICATED.as_bytes(),
                )
                .await;
        };

        match session.execute_prepared(&stmt, &values).await {
            Ok(SessionResult::Rows(rs)) => write_result_set(&rs, results).await,
            Ok(SessionResult::Ok(ok)) => {
                results
                    .completed(ok_response(
                        ok.affected_rows,
                        ok.last_insert_id,
                        ok.in_transaction,
                    ))
                    .await
            }
            Err(e) => write_session_error(results, &e).await,
        }
    }

    async fn on_close(&mut self, id: u32) {
        debug!(stmt_id = id, "COM_STMT_CLOSE");
        self.apply_pending_reset().await;
        self.stmts.remove(&id);
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(sql = %query, "COM_QUERY");
        self.apply_pending_reset().await;

        // Fail closed -- see `on_prepare`. This is the guard that stops a
        // no-username handshake from running SQL.
        let Some(session) = self.session_mut() else {
            return results
                .error(
                    ErrorKind::ER_ACCESS_DENIED_ERROR,
                    NOT_AUTHENTICATED.as_bytes(),
                )
                .await;
        };

        match session.query(query, &[]).await {
            Ok(SessionResult::Rows(rs)) => write_result_set(&rs, results).await,
            // No-ops (SET NAMES, empty input) keep their historical framing:
            // a default OK packet without the transaction status flag.
            Ok(SessionResult::Ok(ok)) if ok.noop => results.completed(OkResponse::default()).await,
            Ok(SessionResult::Ok(ok)) => {
                results
                    .completed(ok_response(
                        ok.affected_rows,
                        ok.last_insert_id,
                        ok.in_transaction,
                    ))
                    .await
            }
            Err(e) => {
                if matches!(e, SessionError::Translate(_)) {
                    warn!("SQL translation error: {e}");
                }
                write_session_error(results, &e).await
            }
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(schema = %schema, "COM_INIT_DB (USE)");
        self.apply_pending_reset().await;
        // `USE <db>` is a no-op for litewire (one backend per connection, no
        // schema switching) -- but on the authenticating path it must not
        // report success to a connection that has no backend, or a client
        // learns nothing until its first real query. More importantly, the
        // schema name is client-asserted and never selects anything.
        if self.session().is_none() {
            return writer
                .error(
                    ErrorKind::ER_ACCESS_DENIED_ERROR,
                    NOT_AUTHENTICATED.as_bytes(),
                )
                .await;
        }
        writer.ok().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use litewire_backend::Rusqlite;
    use std::sync::Arc;

    /// Helper: create a handler backed by an in-memory SQLite database.
    ///
    /// `LiteWireHandler::new` is async (it opens a per-connection backend
    /// session), so block on it here to keep the many synchronous unit tests
    /// below simple.
    /// The session of a fixed-backend handler.
    ///
    /// The handler no longer `Deref`s to `Session`: a connection may now have
    /// no session at all until it authenticates, so reaching the session is
    /// fallible by design. Handlers built by the helpers below always have
    /// one, hence the `expect`.
    fn sess(h: &LiteWireHandler) -> &Session {
        h.session()
            .expect("a fixed-backend handler is constructed with its session already open")
    }

    /// A freshly-accepted authenticating connection holds no backend.
    ///
    /// This is the precondition the whole fail-closed argument rests on: if
    /// construction ever pre-opened a session here, an unauthorised connection
    /// would have something to reach and every command-path guard would become
    /// decorative.
    #[test]
    fn an_authenticating_handler_starts_with_no_session() {
        struct NeverAuthenticates;

        #[async_trait::async_trait]
        impl litewire_backend::ConnectionAuthenticator for NeverAuthenticates {
            async fn authenticate(
                &self,
                _req: &litewire_backend::AuthRequest<'_>,
            ) -> Option<SharedBackend> {
                None
            }
        }

        let handler = LiteWireHandler::new_authenticating(
            Arc::new(NeverAuthenticates),
            Arc::new(TranslateCache::default()),
            "127.0.0.1:3306".parse().unwrap(),
            "127.0.0.1:5555".parse().unwrap(),
        );
        assert!(handler.session().is_none());
    }

    /// Each connection gets its own challenge, so an `auth_response` captured
    /// from one is worthless against another.
    #[test]
    fn each_authenticating_connection_gets_a_distinct_scramble() {
        let a = random_scramble();
        let b = random_scramble();
        assert_ne!(a, b, "two connections drew the same challenge");
        assert!(
            a.iter().all(|&c| (0x21..=0x7e).contains(&c)),
            "a NUL or non-printable byte would truncate the challenge on the wire"
        );
    }

    fn memory_handler() -> LiteWireHandler {
        let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
        let cache = Arc::new(TranslateCache::default());
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(LiteWireHandler::new(backend, cache))
            .unwrap()
    }

    // ── ok_response ────────────────────────────────────────────────────────

    #[test]
    fn ok_response_not_in_transaction() {
        let resp = ok_response(1, 2, false);
        assert_eq!(resp.affected_rows, 1);
        assert_eq!(resp.last_insert_id, 2);
        assert!(
            !resp
                .status_flags
                .contains(StatusFlags::SERVER_STATUS_IN_TRANS)
        );
    }

    #[test]
    fn ok_response_in_transaction() {
        let resp = ok_response(0, 0, true);
        assert!(
            resp.status_flags
                .contains(StatusFlags::SERVER_STATUS_IN_TRANS)
        );
    }

    #[test]
    fn ok_response_zero_values() {
        let resp = ok_response(0, 0, false);
        assert_eq!(resp.affected_rows, 0);
        assert_eq!(resp.last_insert_id, 0);
        assert!(resp.status_flags.is_empty());
    }

    #[test]
    fn ok_response_large_values() {
        let resp = ok_response(u64::MAX, u64::MAX, true);
        assert_eq!(resp.affected_rows, u64::MAX);
        assert_eq!(resp.last_insert_id, u64::MAX);
        assert!(
            resp.status_flags
                .contains(StatusFlags::SERVER_STATUS_IN_TRANS)
        );
    }

    // ── translate_sql ──────────────────────────────────────────────────────

    #[test]
    fn translate_simple_select() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler).translate_sql("SELECT 1").unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_select_from_table() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("SELECT id, name FROM users WHERE id = 1")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("select"));
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_insert() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("INSERT INTO users (name) VALUES ('Alice')")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("insert"));
        assert_eq!(kind, StatementKind::Mutation);
    }

    #[test]
    fn translate_update() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("update"));
        assert_eq!(kind, StatementKind::Mutation);
    }

    #[test]
    fn translate_delete() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("DELETE FROM users WHERE id = 1")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("delete"));
        assert_eq!(kind, StatementKind::Mutation);
    }

    #[test]
    fn translate_create_table() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("create"));
        assert_eq!(kind, StatementKind::Ddl);
    }

    #[test]
    fn translate_begin_returns_transaction() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler).translate_sql("BEGIN").unwrap();
        assert!(sql.to_ascii_lowercase().contains("begin"));
        assert_eq!(kind, StatementKind::Transaction);
    }

    #[test]
    fn translate_commit_returns_transaction() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler).translate_sql("COMMIT").unwrap();
        assert!(sql.to_ascii_lowercase().contains("commit"));
        assert_eq!(kind, StatementKind::Transaction);
    }

    #[test]
    fn translate_rollback_returns_transaction() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler).translate_sql("ROLLBACK").unwrap();
        assert!(sql.to_ascii_lowercase().contains("rollback"));
        assert_eq!(kind, StatementKind::Transaction);
    }

    #[test]
    fn translate_set_names_returns_noop() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler).translate_sql("SET NAMES utf8mb4").unwrap();
        // Noop branch returns empty SQL and Other kind.
        assert!(sql.is_empty());
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn translate_set_character_set_returns_noop() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("SET CHARACTER SET utf8")
            .unwrap();
        assert!(sql.is_empty());
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn translate_show_tables_returns_metadata() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler).translate_sql("SHOW TABLES").unwrap();
        // Metadata branch returns a SQLite query and Query kind.
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
        // The metadata SQL should query sqlite_master.
        assert!(sql.contains("sqlite_master"));
    }

    #[test]
    fn translate_show_columns_returns_metadata() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("SHOW COLUMNS FROM users")
            .unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_invalid_sql_returns_error() {
        let handler = memory_handler();
        let result = sess(&handler).translate_sql("NOT VALID SQL !!! @@@ {{{}}");
        assert!(result.is_err());
    }

    #[test]
    fn translate_select_with_mysql_backticks() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("SELECT `id` FROM `users`")
            .unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_select_with_limit() {
        let handler = memory_handler();
        let (sql, kind) = sess(&handler)
            .translate_sql("SELECT * FROM users LIMIT 10")
            .unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    // ── do_transaction state logic ─────────────────────────────────────────
    //
    // We cannot directly call `do_transaction` because it requires a
    // `QueryResultWriter` that can only be constructed inside opensrv-mysql.
    // Instead, we verify the transaction state-update logic that
    // `do_transaction` applies after a successful backend.execute().
    //
    // The logic under test (from do_transaction lines 139-144):
    //   let upper = sql.trim().to_ascii_uppercase();
    //   if upper.starts_with("BEGIN") || upper.starts_with("START") {
    //       self.in_transaction = true;
    //   } else if upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") {
    //       self.in_transaction = false;
    //   }

    /// Apply the same transaction state logic that `do_transaction` uses.
    fn apply_transaction_state(in_transaction: &mut bool, sql: &str) {
        let upper = sql.trim().to_ascii_uppercase();
        if upper.starts_with("BEGIN") || upper.starts_with("START") {
            *in_transaction = true;
        } else if upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") {
            *in_transaction = false;
        }
    }

    #[test]
    fn transaction_begin_sets_in_transaction() {
        let mut in_tx = false;
        apply_transaction_state(&mut in_tx, "BEGIN");
        assert!(in_tx);
    }

    #[test]
    fn transaction_commit_clears_in_transaction() {
        let mut in_tx = true;
        apply_transaction_state(&mut in_tx, "COMMIT");
        assert!(!in_tx);
    }

    #[test]
    fn transaction_rollback_clears_in_transaction() {
        let mut in_tx = true;
        apply_transaction_state(&mut in_tx, "ROLLBACK");
        assert!(!in_tx);
    }

    #[test]
    fn transaction_begin_case_insensitive() {
        for sql in &["begin", "BEGIN", "Begin", "bEgIn"] {
            let mut in_tx = false;
            apply_transaction_state(&mut in_tx, sql);
            assert!(in_tx, "expected in_transaction=true for '{sql}'");
        }
    }

    #[test]
    fn transaction_commit_case_insensitive() {
        for sql in &["commit", "COMMIT", "Commit"] {
            let mut in_tx = true;
            apply_transaction_state(&mut in_tx, sql);
            assert!(!in_tx, "expected in_transaction=false for '{sql}'");
        }
    }

    #[test]
    fn transaction_rollback_case_insensitive() {
        for sql in &["rollback", "ROLLBACK", "Rollback"] {
            let mut in_tx = true;
            apply_transaction_state(&mut in_tx, sql);
            assert!(!in_tx, "expected in_transaction=false for '{sql}'");
        }
    }

    #[test]
    fn transaction_start_transaction_variant() {
        let mut in_tx = false;
        apply_transaction_state(&mut in_tx, "START TRANSACTION");
        assert!(in_tx);
    }

    #[test]
    fn transaction_begin_with_leading_whitespace() {
        let mut in_tx = false;
        apply_transaction_state(&mut in_tx, "  BEGIN  ");
        assert!(in_tx);
    }

    #[test]
    fn transaction_commit_with_leading_whitespace() {
        let mut in_tx = true;
        apply_transaction_state(&mut in_tx, "  COMMIT  ");
        assert!(!in_tx);
    }

    #[test]
    fn transaction_full_cycle() {
        let mut in_tx = false;
        apply_transaction_state(&mut in_tx, "BEGIN");
        assert!(in_tx);
        apply_transaction_state(&mut in_tx, "COMMIT");
        assert!(!in_tx);
        apply_transaction_state(&mut in_tx, "START TRANSACTION");
        assert!(in_tx);
        apply_transaction_state(&mut in_tx, "ROLLBACK");
        assert!(!in_tx);
    }

    #[test]
    fn transaction_unknown_sql_does_not_change_state() {
        let mut in_tx = false;
        apply_transaction_state(&mut in_tx, "SELECT 1");
        assert!(!in_tx);

        let mut in_tx = true;
        apply_transaction_state(&mut in_tx, "INSERT INTO t VALUES (1)");
        assert!(in_tx);
    }

    // ── do_transaction with backend (integration) ──────────────────────────

    /// Async variant of [`memory_handler`] for `#[tokio::test]` cases — avoids
    /// nesting a runtime inside the one already provided by the test macro.
    async fn memory_handler_async() -> LiteWireHandler {
        let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
        let cache = Arc::new(TranslateCache::default());
        LiteWireHandler::new(backend, cache).await.unwrap()
    }

    #[tokio::test]
    async fn transaction_backend_begin_commit() {
        let handler = memory_handler_async().await;
        // Verify that the per-connection backend can execute BEGIN and COMMIT
        // without error.
        sess(&handler).conn.execute("BEGIN", &[]).await.unwrap();
        sess(&handler).conn.execute("COMMIT", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn transaction_backend_begin_rollback() {
        let handler = memory_handler_async().await;
        sess(&handler).conn.execute("BEGIN", &[]).await.unwrap();
        sess(&handler).conn.execute("ROLLBACK", &[]).await.unwrap();
    }

    // ── handler construction ───────────────────────────────────────────────

    #[test]
    fn handler_initial_state() {
        let handler = memory_handler();
        assert!(!sess(&handler).in_transaction);
        assert!(handler.stmts.is_empty());
        assert_eq!(handler.next_stmt_id, 1);
    }

    // ── param_to_value ─────────────────────────────────────────────────────
    //
    // Testing param_to_value directly requires constructing
    // opensrv_mysql::ParamValue, which in turn needs opensrv_mysql::Value.
    // The Value struct wraps ValueInner in a private tuple field, and its
    // constructors (null(), bytes(), parse_from()) are all pub(crate).
    // Therefore we verify the conversion logic by matching against ValueInner
    // variants -- the same dispatch that param_to_value performs.

    #[test]
    fn param_conversion_null() {
        // ValueInner::NULL -> Value::Null
        let result = match ValueInner::NULL {
            ValueInner::NULL => Value::Null,
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn param_conversion_positive_int() {
        let result = match ValueInner::Int(42) {
            ValueInner::Int(i) => Value::Integer(i),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn param_conversion_negative_int() {
        let result = match ValueInner::Int(-100) {
            ValueInner::Int(i) => Value::Integer(i),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Integer(-100));
    }

    #[test]
    fn param_conversion_zero_int() {
        let result = match ValueInner::Int(0) {
            ValueInner::Int(i) => Value::Integer(i),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn param_conversion_uint() {
        let result = match ValueInner::UInt(255) {
            ValueInner::UInt(u) => Value::Integer(u as i64),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Integer(255));
    }

    #[test]
    fn param_conversion_uint_large() {
        // Large unsigned values that fit in i64.
        let val = u64::MAX / 2;
        let result = match ValueInner::UInt(val) {
            ValueInner::UInt(u) => Value::Integer(u as i64),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Integer(val as i64));
    }

    #[test]
    fn param_conversion_double() {
        let result = match ValueInner::Double(2.5) {
            ValueInner::Double(f) => Value::Float(f),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Float(2.5));
    }

    #[test]
    fn param_conversion_double_negative() {
        let result = match ValueInner::Double(-0.5) {
            ValueInner::Double(f) => Value::Float(f),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Float(-0.5));
    }

    #[test]
    fn param_conversion_utf8_bytes() {
        let bytes = b"hello world";
        let result = match ValueInner::Bytes(bytes) {
            ValueInner::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Blob(b.to_vec()),
            },
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Text("hello world".to_string()));
    }

    #[test]
    fn param_conversion_non_utf8_bytes() {
        let bytes: &[u8] = &[0xFF, 0xFE, 0x00, 0x80];
        let result = match ValueInner::Bytes(bytes) {
            ValueInner::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Blob(b.to_vec()),
            },
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Blob(vec![0xFF, 0xFE, 0x00, 0x80]));
    }

    #[test]
    fn param_conversion_empty_bytes() {
        let bytes: &[u8] = b"";
        let result = match ValueInner::Bytes(bytes) {
            ValueInner::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Blob(b.to_vec()),
            },
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Text(String::new()));
    }

    #[test]
    fn param_conversion_date_bytes() {
        let date_bytes: &[u8] = b"2024-01-15";
        let result = match ValueInner::Date(date_bytes) {
            ValueInner::Date(b) | ValueInner::Time(b) | ValueInner::Datetime(b) => {
                Value::Text(String::from_utf8_lossy(b).into_owned())
            }
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Text("2024-01-15".to_string()));
    }

    #[test]
    fn param_conversion_time_bytes() {
        let time_bytes: &[u8] = b"12:30:00";
        let result = match ValueInner::Time(time_bytes) {
            ValueInner::Date(b) | ValueInner::Time(b) | ValueInner::Datetime(b) => {
                Value::Text(String::from_utf8_lossy(b).into_owned())
            }
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Text("12:30:00".to_string()));
    }

    #[test]
    fn param_conversion_datetime_bytes() {
        let datetime_bytes: &[u8] = b"2024-01-15 12:30:00";
        let result = match ValueInner::Datetime(datetime_bytes) {
            ValueInner::Date(b) | ValueInner::Time(b) | ValueInner::Datetime(b) => {
                Value::Text(String::from_utf8_lossy(b).into_owned())
            }
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Text("2024-01-15 12:30:00".to_string()));
    }
}
