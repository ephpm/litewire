//! PostgreSQL wire protocol handler.
//!
//! Implements `SimpleQueryHandler` and `ExtendedQueryHandler` from the `pgwire`
//! crate, translating incoming PostgreSQL SQL to SQLite via `litewire-translate`
//! and executing against the backend.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use tracing::{debug, warn};

use litewire_backend::{BackendConn, BackendError, SharedBackend, Value};
use litewire_translate::{self, Dialect, StatementKind, TranslateCache, TranslateResult, classify};

use crate::error_map;
use crate::types::sqlite_to_pg_type;

/// Handler for a single PostgreSQL client connection.
///
/// Owns a per-session [`BackendConn`] so `BEGIN`/`COMMIT`/`ROLLBACK` are
/// isolated from other pgwire clients. See the `BackendConn` docs.
pub struct PostgresHandler {
    conn: Box<dyn BackendConn>,
    query_parser: Arc<NoopQueryParser>,
    translate_cache: Arc<TranslateCache>,
}

impl PostgresHandler {
    /// Open a fresh backend session for this pgwire client.
    ///
    /// # Errors
    ///
    /// Returns the backend's error if the underlying session cannot be
    /// opened.
    pub async fn new(
        backend: SharedBackend,
        translate_cache: Arc<TranslateCache>,
    ) -> Result<Self, BackendError> {
        let conn = backend.connect().await?;
        Ok(Self {
            conn,
            query_parser: Arc::new(NoopQueryParser::new()),
            translate_cache,
        })
    }

    /// Translate SQL from PostgreSQL dialect to SQLite and classify it.
    fn translate_sql(&self, query: &str) -> Result<(String, StatementKind), String> {
        let translated =
            litewire_translate::translate_cached(&self.translate_cache, query, Dialect::PostgreSQL)
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

    /// Execute a query and return a pgwire `Response`.
    async fn exec_query<'a>(
        &self,
        sql: &str,
        params: &[Value],
        format: &Format,
    ) -> PgWireResult<Response<'a>> {
        let kind = classify(sql);
        match kind {
            StatementKind::Query => self.do_select(sql, params, format).await,
            _ => self.do_mutation(sql, params, &kind).await,
        }
    }

    /// Execute a SELECT and build a query response with rows.
    async fn do_select<'a>(
        &self,
        sql: &str,
        params: &[Value],
        format: &Format,
    ) -> PgWireResult<Response<'a>> {
        let rs = self
            .conn
            .query(sql, params)
            .await
            .map_err(|e| pg_backend_error(&e))?;

        // Build field info, inferring types from declared type first,
        // then falling back to the first row's actual values for expressions
        // without a declared type (e.g. `SELECT 1 + 2`).
        let fields: Vec<FieldInfo> = rs
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let pg_type = if col.decltype.is_some() {
                    sqlite_to_pg_type(col.decltype.as_deref())
                } else {
                    // Infer from the first row's actual value.
                    rs.rows
                        .first()
                        .and_then(|row| row.get(idx))
                        .map(value_to_pg_type)
                        .unwrap_or(Type::TEXT)
                };
                FieldInfo::new(
                    col.name.clone(),
                    None,
                    None,
                    pg_type,
                    format.format_for(idx),
                )
            })
            .collect();

        let schema = Arc::new(fields);
        let mut rows: Vec<PgWireResult<DataRow>> = Vec::with_capacity(rs.rows.len());

        for row in &rs.rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for (idx, val) in row.iter().enumerate() {
                encode_value(&mut encoder, val, &schema[idx])?;
            }
            rows.push(encoder.finish());
        }

        Ok(Response::Query(QueryResponse::new(
            schema,
            stream::iter(rows),
        )))
    }

    /// Execute a mutation (INSERT/UPDATE/DELETE/DDL) and return an execution response.
    async fn do_mutation<'a>(
        &self,
        sql: &str,
        params: &[Value],
        kind: &StatementKind,
    ) -> PgWireResult<Response<'a>> {
        // Transaction commands need special Response variants, handle before
        // the generic execute path to avoid double-execution.
        if *kind == StatementKind::Transaction {
            self.conn
                .execute(sql, params)
                .await
                .map_err(|e| pg_backend_error(&e))?;

            let upper = sql.trim().to_ascii_uppercase();
            if upper.starts_with("BEGIN") || upper.starts_with("START") {
                return Ok(Response::TransactionStart(Tag::new("BEGIN")));
            }
            if upper.starts_with("COMMIT") {
                return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
            }
            if upper.starts_with("ROLLBACK") {
                return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
            }
            return Ok(Response::Execution(Tag::new("OK")));
        }

        let result = self
            .conn
            .execute(sql, params)
            .await
            .map_err(|e| pg_backend_error(&e))?;

        let tag_name = match kind {
            StatementKind::Mutation => {
                let upper = sql.trim().to_ascii_uppercase();
                if upper.starts_with("INSERT") {
                    "INSERT"
                } else if upper.starts_with("UPDATE") {
                    "UPDATE"
                } else if upper.starts_with("DELETE") {
                    "DELETE"
                } else {
                    "OK"
                }
            }
            StatementKind::Ddl => {
                let upper = sql.trim().to_ascii_uppercase();
                if upper.starts_with("CREATE") {
                    "CREATE"
                } else if upper.starts_with("DROP") {
                    "DROP"
                } else if upper.starts_with("ALTER") {
                    "ALTER"
                } else {
                    "OK"
                }
            }
            _ => "OK",
        };

        let mut tag = Tag::new(tag_name);
        if *kind == StatementKind::Mutation {
            tag = tag.with_rows(result.affected_rows as usize);
        }

        Ok(Response::Execution(tag))
    }

    /// Build column metadata for a query.
    ///
    /// Fast path: call the backend's `describe_columns`, which on rusqlite
    /// reads column types off the prepared statement without executing.
    /// If any column comes back without a declared type (typical for
    /// expression columns like `SELECT 1 + 2`), fall back to a `LIMIT 1`
    /// probe so we can infer from an actual value.
    async fn probe_columns(&self, sql: &str, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
        let cols = match self.conn.describe_columns(sql).await {
            Ok(c) => c,
            Err(_) => return Ok(vec![]),
        };

        let has_untyped = cols.iter().any(|c| c.decltype.is_none());
        if !has_untyped {
            return Ok(cols
                .iter()
                .enumerate()
                .map(|(idx, col)| {
                    FieldInfo::new(
                        col.name.clone(),
                        None,
                        None,
                        sqlite_to_pg_type(col.decltype.as_deref()),
                        format.format_for(idx),
                    )
                })
                .collect());
        }

        // At least one expression column; probe for a real row to infer.
        let probe = format!("{sql} LIMIT 1");
        match self.conn.query(&probe, &[]).await {
            Ok(rs) => Ok(rs
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| {
                    let pg_type = if col.decltype.is_some() {
                        sqlite_to_pg_type(col.decltype.as_deref())
                    } else {
                        rs.rows
                            .first()
                            .and_then(|row| row.get(idx))
                            .map(value_to_pg_type)
                            .unwrap_or(Type::TEXT)
                    };
                    FieldInfo::new(
                        col.name.clone(),
                        None,
                        None,
                        pg_type,
                        format.format_for(idx),
                    )
                })
                .collect()),
            Err(_) => Ok(vec![]),
        }
    }
}

/// Infer the PostgreSQL type from an actual runtime value.
///
/// Used when a column has no declared type (e.g. expression results).
fn value_to_pg_type(val: &Value) -> Type {
    match val {
        Value::Null => Type::TEXT,
        Value::Integer(_) => Type::INT8,
        Value::Float(_) => Type::FLOAT8,
        Value::Text(_) => Type::TEXT,
        Value::Blob(_) => Type::BYTEA,
    }
}

/// Encode a single backend `Value` into a pgwire `DataRowEncoder`.
fn encode_value(encoder: &mut DataRowEncoder, val: &Value, field: &FieldInfo) -> PgWireResult<()> {
    match val {
        Value::Null => encoder.encode_field(&None::<i8>),
        Value::Integer(i) => {
            // Encode based on the declared PG type.
            if *field.datatype() == Type::BOOL {
                encoder.encode_field(&(*i != 0))
            } else {
                encoder.encode_field(i)
            }
        }
        Value::Float(f) => encoder.encode_field(f),
        Value::Text(s) => encoder.encode_field(&s.as_str()),
        Value::Blob(b) => encoder.encode_field(&b.as_slice()),
    }
}

/// Extract parameter values from a portal, converting from PG binary format
/// to backend `Value` types.
fn extract_params(portal: &Portal<String>) -> Vec<Value> {
    let mut values = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let param_type = portal
            .statement
            .parameter_types
            .get(i)
            .cloned()
            .unwrap_or(Type::TEXT);

        let val = match &param_type {
            t if *t == Type::BOOL => portal
                .parameter::<bool>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, |v| Value::Integer(i64::from(v))),

            t if *t == Type::INT2 => portal
                .parameter::<i16>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, |v| Value::Integer(i64::from(v))),

            t if *t == Type::INT4 => portal
                .parameter::<i32>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, |v| Value::Integer(i64::from(v))),

            t if *t == Type::INT8 => portal
                .parameter::<i64>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, Value::Integer),

            t if *t == Type::FLOAT4 => portal
                .parameter::<f32>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, |v| Value::Float(f64::from(v))),

            t if *t == Type::FLOAT8 => portal
                .parameter::<f64>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, Value::Float),

            t if *t == Type::BYTEA => portal
                .parameter::<Vec<u8>>(i, &param_type)
                .ok()
                .flatten()
                .map_or(Value::Null, Value::Blob),

            // Default: treat as text
            _ => portal
                .parameter::<String>(i, &Type::TEXT)
                .ok()
                .flatten()
                .map_or(Value::Null, Value::Text),
        };
        values.push(val);
    }
    values
}

/// Build a PgWireError from an arbitrary error string with SQLSTATE `XX000`
/// (internal_error). Prefer [`pg_backend_error`] for messages that came from
/// the backend so they get classified into a specific SQLSTATE.
fn pg_error(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        msg.to_owned(),
    )))
}

/// Build a PgWireError from a litewire-backend error, classifying it into a
/// real PostgreSQL SQLSTATE (see [`crate::error_map`]).
fn pg_backend_error(err: &litewire_backend::BackendError) -> PgWireError {
    let mapped = error_map::classify(&err.to_string());
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        mapped.sqlstate.to_owned(),
        mapped.message,
    )))
}

#[async_trait]
impl SimpleQueryHandler for PostgresHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!(sql = %query, "PG simple query");

        let translated =
            litewire_translate::translate_cached(&self.translate_cache, query, Dialect::PostgreSQL)
                .map_err(|e| {
                    warn!("SQL translation error: {e}");
                    pg_error(&e.to_string())
                })?;

        let mut responses = Vec::new();

        for result in translated {
            let resp = match result {
                TranslateResult::Noop => Response::Execution(Tag::new("SET")),
                TranslateResult::Metadata(meta) => {
                    let sqlite_sql = meta.to_sqlite_sql();
                    self.exec_query(&sqlite_sql, &[], &Format::UnifiedText)
                        .await?
                }
                TranslateResult::Sql(sqlite_sql) => {
                    if sqlite_sql.is_empty() {
                        Response::Execution(Tag::new("OK"))
                    } else {
                        self.exec_query(&sqlite_sql, &[], &Format::UnifiedText)
                            .await?
                    }
                }
            };
            responses.push(resp);
        }

        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK")));
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        debug!(sql = %query, "PG extended query execute");

        let (sqlite_sql, _kind) = self.translate_sql(query).map_err(|e| pg_error(&e))?;

        if sqlite_sql.is_empty() {
            return Ok(Response::Execution(Tag::new("OK")));
        }

        let params = extract_params(portal);
        self.exec_query(&sqlite_sql, &params, &portal.result_column_format)
            .await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (sqlite_sql, kind) = self
            .translate_sql(&stmt.statement)
            .map_err(|e| pg_error(&e))?;

        let param_types = stmt.parameter_types.clone();

        if kind == StatementKind::Query && !sqlite_sql.is_empty() {
            let fields = self
                .probe_columns(&sqlite_sql, &Format::UnifiedBinary)
                .await?;
            Ok(DescribeStatementResponse::new(param_types, fields))
        } else {
            Ok(DescribeStatementResponse::new(param_types, vec![]))
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (sqlite_sql, kind) = self
            .translate_sql(&portal.statement.statement)
            .map_err(|e| pg_error(&e))?;

        if kind == StatementKind::Query && !sqlite_sql.is_empty() {
            let fields = self
                .probe_columns(&sqlite_sql, &portal.result_column_format)
                .await?;
            Ok(DescribePortalResponse::new(fields))
        } else {
            Ok(DescribePortalResponse::new(vec![]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use pgwire::api::portal::{Format, Portal};
    use pgwire::api::results::{FieldFormat, FieldInfo};
    use pgwire::api::stmt::StoredStatement;

    // ── value_to_pg_type ───────────────────────────────────────────────────

    #[test]
    fn value_to_pg_type_null() {
        assert_eq!(value_to_pg_type(&Value::Null), Type::TEXT);
    }

    #[test]
    fn value_to_pg_type_integer() {
        assert_eq!(value_to_pg_type(&Value::Integer(42)), Type::INT8);
    }

    #[test]
    fn value_to_pg_type_float() {
        assert_eq!(value_to_pg_type(&Value::Float(2.5)), Type::FLOAT8);
    }

    #[test]
    fn value_to_pg_type_text() {
        assert_eq!(value_to_pg_type(&Value::Text("hello".into())), Type::TEXT);
    }

    #[test]
    fn value_to_pg_type_blob() {
        assert_eq!(value_to_pg_type(&Value::Blob(vec![1, 2, 3])), Type::BYTEA);
    }

    // ── encode_value ───────────────────────────────────────────────────────

    /// Helper: create a single-column schema and encode one value into it.
    fn encode_single(val: &Value, pg_type: Type) -> PgWireResult<()> {
        let field = FieldInfo::new(
            "col".into(),
            None,
            None,
            pg_type.clone(),
            FieldFormat::Binary,
        );
        let schema = Arc::new(vec![FieldInfo::new(
            "col".into(),
            None,
            None,
            pg_type,
            FieldFormat::Binary,
        )]);
        let mut encoder = DataRowEncoder::new(schema);
        encode_value(&mut encoder, val, &field)?;
        let _row = encoder.finish()?;
        Ok(())
    }

    #[test]
    fn encode_null() {
        encode_single(&Value::Null, Type::TEXT).expect("encoding null should succeed");
    }

    #[test]
    fn encode_integer() {
        encode_single(&Value::Integer(42), Type::INT8).expect("encoding integer should succeed");
    }

    #[test]
    fn encode_negative_integer() {
        encode_single(&Value::Integer(-100), Type::INT8)
            .expect("encoding negative integer should succeed");
    }

    #[test]
    fn encode_float() {
        encode_single(&Value::Float(2.5), Type::FLOAT8).expect("encoding float should succeed");
    }

    #[test]
    fn encode_text() {
        encode_single(&Value::Text("hello world".into()), Type::TEXT)
            .expect("encoding text should succeed");
    }

    #[test]
    fn encode_empty_text() {
        encode_single(&Value::Text(String::new()), Type::TEXT)
            .expect("encoding empty text should succeed");
    }

    #[test]
    fn encode_blob() {
        encode_single(&Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]), Type::BYTEA)
            .expect("encoding blob should succeed");
    }

    #[test]
    fn encode_empty_blob() {
        encode_single(&Value::Blob(vec![]), Type::BYTEA)
            .expect("encoding empty blob should succeed");
    }

    #[test]
    fn encode_bool_true() {
        // Integer 1 with BOOL type should encode as true.
        encode_single(&Value::Integer(1), Type::BOOL).expect("encoding bool true should succeed");
    }

    #[test]
    fn encode_bool_false() {
        // Integer 0 with BOOL type should encode as false.
        encode_single(&Value::Integer(0), Type::BOOL).expect("encoding bool false should succeed");
    }

    #[test]
    fn encode_bool_nonzero() {
        // Any nonzero integer with BOOL type should encode as true.
        encode_single(&Value::Integer(42), Type::BOOL)
            .expect("encoding nonzero bool should succeed");
    }

    // ── extract_params ─────────────────────────────────────────────────────

    /// Helper: build a Portal with given parameter types and binary parameter bytes.
    fn make_portal(param_types: Vec<Type>, parameters: Vec<Option<Bytes>>) -> Portal<String> {
        let stmt = Arc::new(StoredStatement::new(
            String::new(),
            String::new(),
            param_types,
        ));
        let mut portal = Portal::<String>::default();
        portal.statement = stmt;
        portal.parameter_format = Format::UnifiedBinary;
        portal.parameters = parameters;
        portal
    }

    #[test]
    fn extract_params_bool_true() {
        let portal = make_portal(vec![Type::BOOL], vec![Some(Bytes::from_static(&[1]))]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Integer(1)]);
    }

    #[test]
    fn extract_params_bool_false() {
        let portal = make_portal(vec![Type::BOOL], vec![Some(Bytes::from_static(&[0]))]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Integer(0)]);
    }

    #[test]
    fn extract_params_int2() {
        let portal = make_portal(
            vec![Type::INT2],
            vec![Some(Bytes::from(42_i16.to_be_bytes().to_vec()))],
        );
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Integer(42)]);
    }

    #[test]
    fn extract_params_int4() {
        let portal = make_portal(
            vec![Type::INT4],
            vec![Some(Bytes::from(1000_i32.to_be_bytes().to_vec()))],
        );
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Integer(1000)]);
    }

    #[test]
    fn extract_params_int8() {
        let portal = make_portal(
            vec![Type::INT8],
            vec![Some(Bytes::from(i64::MAX.to_be_bytes().to_vec()))],
        );
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Integer(i64::MAX)]);
    }

    #[test]
    fn extract_params_int8_negative() {
        let portal = make_portal(
            vec![Type::INT8],
            vec![Some(Bytes::from((-99_i64).to_be_bytes().to_vec()))],
        );
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Integer(-99)]);
    }

    #[test]
    fn extract_params_float4() {
        let portal = make_portal(
            vec![Type::FLOAT4],
            vec![Some(Bytes::from(2.5_f32.to_be_bytes().to_vec()))],
        );
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Float(2.5)]);
    }

    #[test]
    fn extract_params_float8() {
        let portal = make_portal(
            vec![Type::FLOAT8],
            vec![Some(Bytes::from(2.5_f64.to_be_bytes().to_vec()))],
        );
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Float(2.5)]);
    }

    #[test]
    fn extract_params_text() {
        let portal = make_portal(vec![Type::TEXT], vec![Some(Bytes::from("hello"))]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Text("hello".into())]);
    }

    #[test]
    fn extract_params_bytea() {
        let data = vec![0xCA, 0xFE, 0xBA, 0xBE];
        let portal = make_portal(vec![Type::BYTEA], vec![Some(Bytes::from(data.clone()))]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Blob(data)]);
    }

    #[test]
    fn extract_params_null() {
        let portal = make_portal(vec![Type::TEXT], vec![None]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Null]);
    }

    #[test]
    fn extract_params_null_int() {
        let portal = make_portal(vec![Type::INT8], vec![None]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Null]);
    }

    #[test]
    fn extract_params_unknown_type_defaults_to_text() {
        // VARCHAR is not explicitly handled, so it should fall through to the
        // default TEXT branch.
        let portal = make_portal(vec![Type::VARCHAR], vec![Some(Bytes::from("fallback"))]);
        let params = extract_params(&portal);
        assert_eq!(params, vec![Value::Text("fallback".into())]);
    }

    #[test]
    fn extract_params_multiple() {
        let portal = make_portal(
            vec![Type::INT8, Type::TEXT, Type::BOOL],
            vec![
                Some(Bytes::from(7_i64.to_be_bytes().to_vec())),
                Some(Bytes::from("world")),
                Some(Bytes::from_static(&[1])),
            ],
        );
        let params = extract_params(&portal);
        assert_eq!(
            params,
            vec![
                Value::Integer(7),
                Value::Text("world".into()),
                Value::Integer(1),
            ]
        );
    }

    #[test]
    fn extract_params_empty() {
        let portal = make_portal(vec![], vec![]);
        let params = extract_params(&portal);
        assert!(params.is_empty());
    }

    // ── pg_error ───────────────────────────────────────────────────────────

    #[test]
    fn pg_error_produces_user_error() {
        let err = pg_error("something went wrong");
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(info.severity, "ERROR");
                assert_eq!(info.code, "XX000");
                assert_eq!(info.message, "something went wrong");
            }
            other => panic!("expected UserError, got: {other:?}"),
        }
    }

    #[test]
    fn pg_error_empty_message() {
        let err = pg_error("");
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(info.message, "");
            }
            other => panic!("expected UserError, got: {other:?}"),
        }
    }
}
