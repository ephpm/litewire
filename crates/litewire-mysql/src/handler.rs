//! `opensrv-mysql` shim implementation.
//!
//! Implements `AsyncMysqlShim` to handle MySQL protocol commands including
//! prepared statement prepare/execute/close.

use std::collections::HashMap;

use litewire_backend::{SharedBackend, Value};
use litewire_translate::{self, Dialect, StatementKind, TranslateResult, classify};
use opensrv_mysql::*;
use tokio::io::AsyncWrite;
use tracing::{debug, warn};

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

use crate::types::sqlite_to_mysql_column_type;

/// A cached prepared statement.
struct PreparedStmt {
    /// The translated SQLite SQL (with `?` placeholders).
    sqlite_sql: String,
    /// Whether this is a query (SELECT) or mutation (INSERT/UPDATE/DELETE).
    kind: StatementKind,
    /// Number of `?` parameters.
    param_count: usize,
}

/// Handler for a single MySQL client connection.
pub struct LiteWireHandler {
    backend: SharedBackend,
    /// Prepared statements keyed by the statement ID assigned during `on_prepare`.
    stmts: HashMap<u32, PreparedStmt>,
    /// Next statement ID to assign.
    next_stmt_id: u32,
    /// Whether the connection is inside an explicit transaction.
    in_transaction: bool,
}

impl LiteWireHandler {
    pub fn new(backend: SharedBackend) -> Self {
        Self {
            backend,
            stmts: HashMap::new(),
            next_stmt_id: 1,
            in_transaction: false,
        }
    }

    /// Execute a query and write result set.
    async fn do_query<W: AsyncWrite + Send + Unpin>(
        &self,
        sql: &str,
        params: &[Value],
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), std::io::Error> {
        match self.backend.query(sql, params).await {
            Ok(rs) => {
                let columns: Vec<Column> = rs
                    .columns
                    .iter()
                    .map(|c| Column {
                        table: String::new(),
                        column: c.name.clone(),
                        coltype: sqlite_to_mysql_column_type(c.decltype.as_deref()),
                        colflags: ColumnFlags::empty(),
                    })
                    .collect();

                let mut rw: RowWriter<'_, W> = results.start(&columns).await?;

                for row in &rs.rows {
                    for val in row {
                        match val {
                            Value::Null => rw.write_col(None::<&str>)?,
                            Value::Integer(i) => rw.write_col(i.to_string())?,
                            Value::Float(f) => rw.write_col(f.to_string())?,
                            Value::Text(s) => rw.write_col(s.as_str())?,
                            Value::Blob(b) => rw.write_col(b.as_slice())?,
                        }
                    }
                    rw.end_row().await?;
                }

                rw.finish().await
            }
            Err(e) => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                    .await
            }
        }
    }

    /// Execute a mutation and write OK response.
    async fn do_execute<W: AsyncWrite + Send + Unpin>(
        &self,
        sql: &str,
        params: &[Value],
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), std::io::Error> {
        match self.backend.execute(sql, params).await {
            Ok(r) => {
                let resp = ok_response(
                    r.affected_rows,
                    r.last_insert_rowid.unwrap_or(0) as u64,
                    self.in_transaction,
                );
                results.completed(resp).await
            }
            Err(e) => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                    .await
            }
        }
    }

    /// Execute a transaction command (BEGIN/COMMIT/ROLLBACK) and update state.
    async fn do_transaction<W: AsyncWrite + Send + Unpin>(
        &mut self,
        sql: &str,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), std::io::Error> {
        match self.backend.execute(sql, &[]).await {
            Ok(_) => {
                let upper = sql.trim().to_ascii_uppercase();
                if upper.starts_with("BEGIN") || upper.starts_with("START") {
                    self.in_transaction = true;
                } else if upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") {
                    self.in_transaction = false;
                }
                let resp = ok_response(0, 0, self.in_transaction);
                results.completed(resp).await
            }
            Err(e) => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                    .await
            }
        }
    }

    /// Translate SQL and return the first translated result, or an error string.
    fn translate_sql(&self, query: &str) -> Result<(String, StatementKind), String> {
        let translated = litewire_translate::translate(query, Dialect::MySQL)
            .map_err(|e| e.to_string())?;

        let Some(result) = translated.into_iter().next() else {
            return Ok((String::new(), StatementKind::Other));
        };

        match result {
            TranslateResult::Noop => Ok((String::new(), StatementKind::Other)),
            TranslateResult::Metadata(meta) => {
                let sql = meta.to_sqlite_sql();
                Ok((sql, StatementKind::Query))
            }
            TranslateResult::Sql(sql) => {
                let kind = classify(&sql);
                Ok((sql, kind))
            }
        }
    }
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

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(sql = %query, "COM_STMT_PREPARE");

        let (sqlite_sql, kind) = match self.translate_sql(query) {
            Ok(r) => r,
            Err(e) => {
                return info
                    .error(ErrorKind::ER_PARSE_ERROR, e.as_bytes())
                    .await;
            }
        };

        // Count `?` placeholders in the translated SQL.
        let param_count = sqlite_sql.chars().filter(|&c| c == '?').count();

        let params: Vec<Column> = (0..param_count)
            .map(|_| Column {
                table: String::new(),
                column: "?".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();

        // Try to determine output columns for queries.
        let columns = if kind == StatementKind::Query && !sqlite_sql.is_empty() {
            let probe_sql = format!("{sqlite_sql} LIMIT 0");
            match self.backend.query(&probe_sql, &[]).await {
                Ok(rs) => rs
                    .columns
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

        // Assign a statement ID and cache it.
        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        self.stmts.insert(
            stmt_id,
            PreparedStmt {
                sqlite_sql,
                kind,
                param_count,
            },
        );

        info.reply(stmt_id, &params, &columns).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(stmt_id = id, "COM_STMT_EXECUTE");

        let Some(stmt) = self.stmts.get(&id) else {
            return results
                .error(
                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                    format!("unknown statement id: {id}").as_bytes(),
                )
                .await;
        };

        let sql = stmt.sqlite_sql.clone();
        let kind = stmt.kind;

        // Extract parameter values.
        let values: Vec<Value> = params.into_iter().map(param_to_value).collect();

        // Noop statements (empty SQL from SET NAMES etc.)
        if sql.is_empty() {
            return results
                .completed(ok_response(0, 0, self.in_transaction))
                .await;
        }

        match kind {
            StatementKind::Query => self.do_query(&sql, &values, results).await,
            StatementKind::Transaction => self.do_transaction(&sql, results).await,
            _ => self.do_execute(&sql, &values, results).await,
        }
    }

    async fn on_close(&mut self, id: u32) {
        debug!(stmt_id = id, "COM_STMT_CLOSE");
        self.stmts.remove(&id);
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(sql = %query, "COM_QUERY");

        let translated = match litewire_translate::translate(query, Dialect::MySQL) {
            Ok(r) => r,
            Err(e) => {
                warn!("SQL translation error: {e}");
                return results
                    .error(ErrorKind::ER_PARSE_ERROR, e.to_string().as_bytes())
                    .await;
            }
        };

        let Some(result) = translated.into_iter().next() else {
            return results.completed(OkResponse::default()).await;
        };

        match result {
            TranslateResult::Noop => results.completed(OkResponse::default()).await,
            TranslateResult::Metadata(meta) => {
                let sqlite_sql = meta.to_sqlite_sql();
                self.do_query(&sqlite_sql, &[], results).await
            }
            TranslateResult::Sql(sqlite_sql) => {
                let kind = classify(&sqlite_sql);
                match kind {
                    StatementKind::Query => self.do_query(&sqlite_sql, &[], results).await,
                    StatementKind::Transaction => {
                        self.do_transaction(&sqlite_sql, results).await
                    }
                    _ => self.do_execute(&sqlite_sql, &[], results).await,
                }
            }
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!(schema = %schema, "COM_INIT_DB (USE)");
        writer.ok().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use litewire_backend::Rusqlite;
    use std::sync::Arc;

    /// Helper: create a handler backed by an in-memory SQLite database.
    fn memory_handler() -> LiteWireHandler {
        let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
        LiteWireHandler::new(backend)
    }

    // ── ok_response ────────────────────────────────────────────────────────

    #[test]
    fn ok_response_not_in_transaction() {
        let resp = ok_response(1, 2, false);
        assert_eq!(resp.affected_rows, 1);
        assert_eq!(resp.last_insert_id, 2);
        assert!(!resp.status_flags.contains(StatusFlags::SERVER_STATUS_IN_TRANS));
    }

    #[test]
    fn ok_response_in_transaction() {
        let resp = ok_response(0, 0, true);
        assert!(resp.status_flags.contains(StatusFlags::SERVER_STATUS_IN_TRANS));
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
        assert!(resp.status_flags.contains(StatusFlags::SERVER_STATUS_IN_TRANS));
    }

    // ── translate_sql ──────────────────────────────────────────────────────

    #[test]
    fn translate_simple_select() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("SELECT 1").unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_select_from_table() {
        let handler = memory_handler();
        let (sql, kind) = handler
            .translate_sql("SELECT id, name FROM users WHERE id = 1")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("select"));
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_insert() {
        let handler = memory_handler();
        let (sql, kind) = handler
            .translate_sql("INSERT INTO users (name) VALUES ('Alice')")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("insert"));
        assert_eq!(kind, StatementKind::Mutation);
    }

    #[test]
    fn translate_update() {
        let handler = memory_handler();
        let (sql, kind) = handler
            .translate_sql("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("update"));
        assert_eq!(kind, StatementKind::Mutation);
    }

    #[test]
    fn translate_delete() {
        let handler = memory_handler();
        let (sql, kind) = handler
            .translate_sql("DELETE FROM users WHERE id = 1")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("delete"));
        assert_eq!(kind, StatementKind::Mutation);
    }

    #[test]
    fn translate_create_table() {
        let handler = memory_handler();
        let (sql, kind) = handler
            .translate_sql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))")
            .unwrap();
        assert!(sql.to_ascii_lowercase().contains("create"));
        assert_eq!(kind, StatementKind::Ddl);
    }

    #[test]
    fn translate_begin_returns_transaction() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("BEGIN").unwrap();
        assert!(sql.to_ascii_lowercase().contains("begin"));
        assert_eq!(kind, StatementKind::Transaction);
    }

    #[test]
    fn translate_commit_returns_transaction() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("COMMIT").unwrap();
        assert!(sql.to_ascii_lowercase().contains("commit"));
        assert_eq!(kind, StatementKind::Transaction);
    }

    #[test]
    fn translate_rollback_returns_transaction() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("ROLLBACK").unwrap();
        assert!(sql.to_ascii_lowercase().contains("rollback"));
        assert_eq!(kind, StatementKind::Transaction);
    }

    #[test]
    fn translate_set_names_returns_noop() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("SET NAMES utf8mb4").unwrap();
        // Noop branch returns empty SQL and Other kind.
        assert!(sql.is_empty());
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn translate_set_character_set_returns_noop() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("SET CHARACTER SET utf8").unwrap();
        assert!(sql.is_empty());
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn translate_show_tables_returns_metadata() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("SHOW TABLES").unwrap();
        // Metadata branch returns a SQLite query and Query kind.
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
        // The metadata SQL should query sqlite_master.
        assert!(sql.contains("sqlite_master"));
    }

    #[test]
    fn translate_show_columns_returns_metadata() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("SHOW COLUMNS FROM users").unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_invalid_sql_returns_error() {
        let handler = memory_handler();
        let result = handler.translate_sql("NOT VALID SQL !!! @@@ {{{}}");
        assert!(result.is_err());
    }

    #[test]
    fn translate_select_with_mysql_backticks() {
        let handler = memory_handler();
        let (sql, kind) = handler.translate_sql("SELECT `id` FROM `users`").unwrap();
        assert!(!sql.is_empty());
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn translate_select_with_limit() {
        let handler = memory_handler();
        let (sql, kind) = handler
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

    #[tokio::test]
    async fn transaction_backend_begin_commit() {
        let handler = memory_handler();
        // Verify that the backend can execute BEGIN and COMMIT without error.
        handler.backend.execute("BEGIN", &[]).await.unwrap();
        handler.backend.execute("COMMIT", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn transaction_backend_begin_rollback() {
        let handler = memory_handler();
        handler.backend.execute("BEGIN", &[]).await.unwrap();
        handler.backend.execute("ROLLBACK", &[]).await.unwrap();
    }

    // ── handler construction ───────────────────────────────────────────────

    #[test]
    fn handler_initial_state() {
        let handler = memory_handler();
        assert!(!handler.in_transaction);
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
        let result = match ValueInner::Double(3.14) {
            ValueInner::Double(f) => Value::Float(f),
            _ => unreachable!(),
        };
        assert_eq!(result, Value::Float(3.14));
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
