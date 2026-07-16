//! Hrana HTTP client backend.
//!
//! Implements the [`Backend`] trait by forwarding queries to a remote
//! server (typically sqld) via the Hrana 3 pipeline protocol over HTTP.
//!
//! # Per-session isolation via Hrana baton
//!
//! sqld's Hrana protocol scopes transactions to a *stream*, identified by
//! an opaque baton passed in each pipeline request. Without a baton every
//! call lands on the default stream, and (as with the shared-`Mutex`
//! rusqlite bug) `BEGIN` on one wire client would be visible to every
//! other.
//!
//! Each [`HranaConn`] returned by [`HranaClient::connect`] carries its own
//! baton and pins itself to a single sqld stream for the lifetime of the
//! wire session. On drop the client sends a `close` request so sqld
//! releases the stream promptly (best-effort -- sqld also times streams
//! out on its own).
//!
//! The Hrana protocol types are defined inline here to avoid a cyclic
//! dependency between `litewire-backend` and `litewire-hrana`.

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{Backend, BackendConn, BackendError, Column, ExecuteResult, ResultSet, Value};

// -- Hrana 3 wire types (inline) -------------------------------------------

#[derive(Serialize)]
struct PipelineRequest {
    baton: Option<String>,
    requests: Vec<StreamRequest>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamRequest {
    Execute(ExecuteRequest),
    Close,
}

#[derive(Serialize)]
struct ExecuteRequest {
    stmt: StmtRequest,
}

#[derive(Serialize)]
struct StmtRequest {
    sql: String,
    args: Vec<HranaValue>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HranaValue {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

#[derive(Deserialize)]
struct PipelineResponse {
    /// Baton returned by sqld. Must be sent back on subsequent requests
    /// on this stream so sqld keeps transaction state alive.
    #[serde(default)]
    baton: Option<String>,
    /// If sqld wants us to move the stream to a different URL (redirect
    /// / migration), it comes back here. We don't currently follow it --
    /// noted for future work.
    #[serde(default)]
    #[allow(dead_code)]
    base_url: Option<String>,
    results: Vec<StreamResult>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: ErrorResponse },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamResponse {
    Execute { result: ExecuteResponse },
    Close,
}

#[derive(Deserialize)]
struct ExecuteResponse {
    cols: Vec<ColResponse>,
    rows: Vec<Vec<ResponseValue>>,
    affected_row_count: u64,
    last_insert_rowid: Option<String>,
}

#[derive(Deserialize)]
struct ColResponse {
    name: String,
    decltype: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseValue {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

#[derive(Deserialize)]
struct ErrorResponse {
    message: String,
    code: Option<String>,
}

// -- Client ----------------------------------------------------------------

/// Shared connection parameters. Cheap to clone.
#[derive(Clone)]
struct Endpoint {
    client: reqwest::Client,
    pipeline_url: String,
    health_url: String,
}

/// A backend that talks to sqld (or any Hrana-compatible server) over HTTP.
///
/// This is a **factory** -- [`Backend::connect`] returns a [`HranaConn`]
/// that pins itself to a single sqld stream for transaction isolation.
///
/// The reqwest `Client` uses HTTP/2 connection reuse across all sessions,
/// so opening a new [`HranaConn`] does not open a new TCP connection.
#[derive(Clone)]
pub struct HranaClient {
    endpoint: Endpoint,
}

impl HranaClient {
    /// Create a new Hrana client pointing at the given base URL.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use litewire_backend::HranaClient;
    /// let client = HranaClient::new("http://127.0.0.1:8081");
    /// ```
    #[must_use]
    pub fn new(base_url: &str) -> Self {
        let base = base_url.trim_end_matches('/');
        Self {
            endpoint: Endpoint {
                client: reqwest::Client::new(),
                pipeline_url: format!("{base}/v2/pipeline"),
                health_url: format!("{base}/health"),
            },
        }
    }

    /// Check if the remote server is healthy.
    ///
    /// # Errors
    ///
    /// Returns an error if the health endpoint is unreachable or returns
    /// a non-success status.
    pub async fn health_check(&self) -> Result<(), BackendError> {
        let resp = self
            .endpoint
            .client
            .get(&self.endpoint.health_url)
            .send()
            .await
            .map_err(|e| BackendError::Other(format!("health check failed: {e}")))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(BackendError::Other(format!(
                "health check returned {}",
                resp.status()
            )))
        }
    }
}

#[async_trait::async_trait]
impl Backend for HranaClient {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        Ok(Box::new(HranaConn {
            endpoint: self.endpoint.clone(),
            baton: Arc::new(Mutex::new(None)),
        }))
    }
}

/// A per-session Hrana stream.
///
/// Carries the sqld-issued baton across successive `query`/`execute`
/// calls so all statements run against the same sqld stream and share
/// transaction state.
pub struct HranaConn {
    endpoint: Endpoint,
    /// Baton from the last successful response. `None` on the very first
    /// request; sqld's `_default` stream will pick us up and issue a
    /// baton in the response.
    baton: Arc<Mutex<Option<String>>>,
}

impl HranaConn {
    /// Send a single statement via the Hrana pipeline and return the response.
    /// Updates the stored baton with whatever sqld returns.
    async fn execute_pipeline(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResponse, BackendError> {
        let args: Vec<HranaValue> = params.iter().map(value_to_hrana).collect();

        let current_baton = self.baton.lock().clone();

        let request = PipelineRequest {
            baton: current_baton,
            requests: vec![StreamRequest::Execute(ExecuteRequest {
                stmt: StmtRequest {
                    sql: sql.to_string(),
                    args,
                },
            })],
        };

        let resp = self
            .endpoint
            .client
            .post(&self.endpoint.pipeline_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| BackendError::Other(format!("HTTP request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(BackendError::Other(format!(
                "sqld returned {status}: {body}"
            )));
        }

        let pipeline: PipelineResponse = resp
            .json()
            .await
            .map_err(|e| BackendError::Other(format!("failed to parse response: {e}")))?;

        // Store the new baton so the next call stays on this stream. sqld
        // rotates batons per request; if we drop this update the next call
        // reverts to a fresh stream and transaction state is lost.
        *self.baton.lock() = pipeline.baton;

        let result = pipeline
            .results
            .into_iter()
            .next()
            .ok_or_else(|| BackendError::Other("empty pipeline response".to_string()))?;

        match result {
            StreamResult::Ok { response } => match response {
                StreamResponse::Execute { result } => Ok(result),
                StreamResponse::Close => {
                    Err(BackendError::Other("unexpected close response".to_string()))
                }
            },
            StreamResult::Error { error } => Err(hrana_error_to_backend(error)),
        }
    }
}

impl Drop for HranaConn {
    fn drop(&mut self) {
        // Best-effort stream close so sqld reclaims resources without
        // waiting on its idle timer. We can't await inside Drop, so
        // spawn a detached task with a cheap clone of the endpoint.
        let baton = self.baton.lock().clone();
        let Some(baton) = baton else { return };
        let endpoint = self.endpoint.clone();
        // Only fire the close if a tokio runtime is available. In tests
        // that construct HranaConn outside a runtime this is a no-op.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let request = PipelineRequest {
                    baton: Some(baton),
                    requests: vec![StreamRequest::Close],
                };
                let _ = endpoint
                    .client
                    .post(&endpoint.pipeline_url)
                    .json(&request)
                    .send()
                    .await;
            });
        }
    }
}

#[async_trait::async_trait]
impl BackendConn for HranaConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        let exec = self.execute_pipeline(sql, params).await?;
        Ok(execute_response_to_result_set(exec))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        let exec = self.execute_pipeline(sql, params).await?;
        Ok(ExecuteResult {
            affected_rows: exec.affected_row_count,
            last_insert_rowid: exec
                .last_insert_rowid
                .as_deref()
                .and_then(|s| s.parse().ok()),
        })
    }
}

// -- Conversions -----------------------------------------------------------

/// Convert a backend [`Value`] to a Hrana wire value.
fn value_to_hrana(val: &Value) -> HranaValue {
    match val {
        Value::Null => HranaValue::Null,
        Value::Integer(i) => HranaValue::Integer {
            value: i.to_string(),
        },
        Value::Float(f) => HranaValue::Float { value: *f },
        Value::Text(s) => HranaValue::Text { value: s.clone() },
        Value::Blob(b) => {
            use base64::Engine;
            HranaValue::Blob {
                base64: base64::engine::general_purpose::STANDARD.encode(b),
            }
        }
    }
}

/// Convert a Hrana [`ResponseValue`] back to a backend [`Value`].
fn response_value_to_backend(rv: &ResponseValue) -> Value {
    match rv {
        ResponseValue::Null => Value::Null,
        ResponseValue::Integer { value } => Value::Integer(value.parse().unwrap_or(0)),
        ResponseValue::Float { value } => Value::Float(*value),
        ResponseValue::Text { value } => Value::Text(value.clone()),
        ResponseValue::Blob { base64: b64 } => {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .unwrap_or_default();
            Value::Blob(bytes)
        }
    }
}

/// Convert an [`ExecuteResponse`] to a [`ResultSet`].
fn execute_response_to_result_set(exec: ExecuteResponse) -> ResultSet {
    let columns = exec
        .cols
        .iter()
        .map(|c| Column {
            name: c.name.clone(),
            decltype: c.decltype.clone(),
        })
        .collect();

    let rows = exec
        .rows
        .iter()
        .map(|row| row.iter().map(response_value_to_backend).collect())
        .collect();

    ResultSet { columns, rows }
}

/// Convert a Hrana error to a [`BackendError`].
fn hrana_error_to_backend(err: ErrorResponse) -> BackendError {
    match err.code.as_deref() {
        Some(code) if code.starts_with("SQLITE") => {
            BackendError::Sqlite(format!("[{code}] {}", err.message))
        }
        _ => BackendError::Other(err.message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_hrana_null() {
        assert!(matches!(value_to_hrana(&Value::Null), HranaValue::Null));
    }

    #[test]
    fn value_to_hrana_integer() {
        match value_to_hrana(&Value::Integer(42)) {
            HranaValue::Integer { value } => assert_eq!(value, "42"),
            other => panic!("expected Integer, got: {other:?}"),
        }
    }

    #[test]
    fn value_to_hrana_text() {
        match value_to_hrana(&Value::Text("hello".into())) {
            HranaValue::Text { value } => assert_eq!(value, "hello"),
            other => panic!("expected Text, got: {other:?}"),
        }
    }

    #[test]
    fn value_to_hrana_blob_roundtrip() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        match value_to_hrana(&Value::Blob(data.clone())) {
            HranaValue::Blob { base64: b64 } => {
                use base64::Engine;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(&b64)
                    .unwrap();
                assert_eq!(decoded, data);
            }
            other => panic!("expected Blob, got: {other:?}"),
        }
    }

    #[test]
    fn response_value_roundtrip() {
        let cases = vec![
            (Value::Null, ResponseValue::Null),
            (
                Value::Integer(123),
                ResponseValue::Integer {
                    value: "123".into(),
                },
            ),
            (Value::Float(3.14), ResponseValue::Float { value: 3.14 }),
            (
                Value::Text("test".into()),
                ResponseValue::Text {
                    value: "test".into(),
                },
            ),
        ];

        for (val, rv) in cases {
            let back = response_value_to_backend(&rv);
            assert_eq!(back, val, "roundtrip failed for {val:?}");
        }
    }

    #[test]
    fn execute_response_to_result_set_maps_correctly() {
        let exec = ExecuteResponse {
            cols: vec![
                ColResponse {
                    name: "id".into(),
                    decltype: Some("INTEGER".into()),
                },
                ColResponse {
                    name: "name".into(),
                    decltype: Some("TEXT".into()),
                },
            ],
            rows: vec![
                vec![
                    ResponseValue::Integer { value: "1".into() },
                    ResponseValue::Text {
                        value: "alice".into(),
                    },
                ],
                vec![
                    ResponseValue::Integer { value: "2".into() },
                    ResponseValue::Text {
                        value: "bob".into(),
                    },
                ],
            ],
            affected_row_count: 0,
            last_insert_rowid: None,
        };

        let rs = execute_response_to_result_set(exec);
        assert_eq!(rs.columns.len(), 2);
        assert_eq!(rs.columns[0].name, "id");
        assert_eq!(rs.columns[0].decltype.as_deref(), Some("INTEGER"));
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][0], Value::Integer(1));
        assert_eq!(rs.rows[0][1], Value::Text("alice".into()));
        assert_eq!(rs.rows[1][0], Value::Integer(2));
    }

    #[test]
    fn hrana_sqlite_error() {
        let err = hrana_error_to_backend(ErrorResponse {
            message: "no such table: foo".into(),
            code: Some("SQLITE_ERROR".into()),
        });
        match err {
            BackendError::Sqlite(msg) => assert!(msg.contains("no such table")),
            other => panic!("expected Sqlite error, got: {other:?}"),
        }
    }

    #[test]
    fn hrana_generic_error() {
        let err = hrana_error_to_backend(ErrorResponse {
            message: "something broke".into(),
            code: None,
        });
        match err {
            BackendError::Other(msg) => assert_eq!(msg, "something broke"),
            other => panic!("expected Other error, got: {other:?}"),
        }
    }

    // ── value_to_hrana: Float (was missing) ─────────────────────────────────

    #[test]
    fn value_to_hrana_float() {
        match value_to_hrana(&Value::Float(3.14)) {
            HranaValue::Float { value } => {
                assert!((value - 3.14).abs() < f64::EPSILON);
            }
            other => panic!("expected Float, got: {other:?}"),
        }
    }

    // ── value_to_hrana: edge cases ──────────────────────────────────────────

    #[test]
    fn value_to_hrana_integer_max() {
        match value_to_hrana(&Value::Integer(i64::MAX)) {
            HranaValue::Integer { value } => assert_eq!(value, i64::MAX.to_string()),
            other => panic!("expected Integer, got: {other:?}"),
        }
    }

    #[test]
    fn value_to_hrana_integer_min() {
        match value_to_hrana(&Value::Integer(i64::MIN)) {
            HranaValue::Integer { value } => {
                assert_eq!(value, i64::MIN.to_string());
                assert!(value.starts_with('-'));
            }
            other => panic!("expected Integer, got: {other:?}"),
        }
    }

    #[test]
    fn value_to_hrana_integer_zero() {
        match value_to_hrana(&Value::Integer(0)) {
            HranaValue::Integer { value } => assert_eq!(value, "0"),
            other => panic!("expected Integer, got: {other:?}"),
        }
    }

    #[test]
    fn value_to_hrana_empty_text() {
        match value_to_hrana(&Value::Text(String::new())) {
            HranaValue::Text { value } => assert_eq!(value, ""),
            other => panic!("expected Text, got: {other:?}"),
        }
    }

    #[test]
    fn value_to_hrana_empty_blob() {
        match value_to_hrana(&Value::Blob(vec![])) {
            HranaValue::Blob { base64: b64 } => assert_eq!(b64, ""),
            other => panic!("expected Blob, got: {other:?}"),
        }
    }

    // ── response_value_to_backend: edge cases ───────────────────────────────

    #[test]
    fn response_value_integer_non_numeric_defaults_to_zero() {
        let rv = ResponseValue::Integer {
            value: "abc".into(),
        };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Integer(0));
    }

    #[test]
    fn response_value_integer_overflow_defaults_to_zero() {
        let rv = ResponseValue::Integer {
            value: "99999999999999999999".into(),
        };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Integer(0));
    }

    #[test]
    fn response_value_integer_negative() {
        let rv = ResponseValue::Integer {
            value: "-42".into(),
        };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Integer(-42));
    }

    #[test]
    fn response_value_integer_empty_string_defaults_to_zero() {
        let rv = ResponseValue::Integer {
            value: String::new(),
        };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Integer(0));
    }

    #[test]
    fn response_value_blob_invalid_base64_defaults_to_empty() {
        let rv = ResponseValue::Blob {
            base64: "!!!not-valid-base64!!!".into(),
        };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Blob(vec![]));
    }

    #[test]
    fn response_value_blob_valid_base64() {
        use base64::Engine;
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
        let rv = ResponseValue::Blob { base64: b64 };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Blob(data));
    }

    #[test]
    fn response_value_empty_text() {
        let rv = ResponseValue::Text {
            value: String::new(),
        };
        let val = response_value_to_backend(&rv);
        assert_eq!(val, Value::Text(String::new()));
    }

    // ── execute_response_to_result_set: edge cases ──────────────────────────

    #[test]
    fn execute_response_empty_rows_nonempty_columns() {
        let exec = ExecuteResponse {
            cols: vec![
                ColResponse {
                    name: "id".into(),
                    decltype: Some("INTEGER".into()),
                },
                ColResponse {
                    name: "name".into(),
                    decltype: Some("TEXT".into()),
                },
            ],
            rows: vec![],
            affected_row_count: 0,
            last_insert_rowid: None,
        };
        let rs = execute_response_to_result_set(exec);
        assert_eq!(rs.columns.len(), 2);
        assert!(rs.rows.is_empty());
    }

    #[test]
    fn execute_response_empty_columns_and_rows() {
        let exec = ExecuteResponse {
            cols: vec![],
            rows: vec![],
            affected_row_count: 0,
            last_insert_rowid: None,
        };
        let rs = execute_response_to_result_set(exec);
        assert!(rs.columns.is_empty());
        assert!(rs.rows.is_empty());
    }

    #[test]
    fn execute_response_rows_with_null_values() {
        let exec = ExecuteResponse {
            cols: vec![
                ColResponse {
                    name: "a".into(),
                    decltype: None,
                },
                ColResponse {
                    name: "b".into(),
                    decltype: None,
                },
            ],
            rows: vec![vec![ResponseValue::Null, ResponseValue::Null]],
            affected_row_count: 0,
            last_insert_rowid: None,
        };
        let rs = execute_response_to_result_set(exec);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Null);
        assert_eq!(rs.rows[0][1], Value::Null);
    }

    #[test]
    fn execute_response_mixed_value_types_in_row() {
        let exec = ExecuteResponse {
            cols: vec![
                ColResponse {
                    name: "a".into(),
                    decltype: None,
                },
                ColResponse {
                    name: "b".into(),
                    decltype: None,
                },
                ColResponse {
                    name: "c".into(),
                    decltype: None,
                },
                ColResponse {
                    name: "d".into(),
                    decltype: None,
                },
                ColResponse {
                    name: "e".into(),
                    decltype: None,
                },
            ],
            rows: vec![vec![
                ResponseValue::Null,
                ResponseValue::Integer { value: "7".into() },
                ResponseValue::Float { value: 2.5 },
                ResponseValue::Text {
                    value: "hello".into(),
                },
                ResponseValue::Blob {
                    base64: "AAEC".into(), // [0, 1, 2]
                },
            ]],
            affected_row_count: 0,
            last_insert_rowid: None,
        };
        let rs = execute_response_to_result_set(exec);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Null);
        assert_eq!(rs.rows[0][1], Value::Integer(7));
        assert_eq!(rs.rows[0][2], Value::Float(2.5));
        assert_eq!(rs.rows[0][3], Value::Text("hello".into()));
        assert_eq!(rs.rows[0][4], Value::Blob(vec![0, 1, 2]));
    }

    #[test]
    fn execute_response_column_decltype_none() {
        let exec = ExecuteResponse {
            cols: vec![ColResponse {
                name: "expr".into(),
                decltype: None,
            }],
            rows: vec![vec![ResponseValue::Integer { value: "1".into() }]],
            affected_row_count: 0,
            last_insert_rowid: None,
        };
        let rs = execute_response_to_result_set(exec);
        assert!(rs.columns[0].decltype.is_none());
    }

    // ── HranaClient::new() URL handling ─────────────────────────────────────

    #[test]
    fn hrana_client_url_trailing_slash_trimmed() {
        let client = HranaClient::new("http://localhost:8081/");
        assert_eq!(client.pipeline_url, "http://localhost:8081/v2/pipeline");
        assert_eq!(client.health_url, "http://localhost:8081/health");
    }

    #[test]
    fn hrana_client_url_no_trailing_slash() {
        let client = HranaClient::new("http://localhost:8081");
        assert_eq!(client.pipeline_url, "http://localhost:8081/v2/pipeline");
        assert_eq!(client.health_url, "http://localhost:8081/health");
    }

    #[test]
    fn hrana_client_url_multiple_trailing_slashes_trimmed() {
        let client = HranaClient::new("http://localhost:8081///");
        assert_eq!(client.pipeline_url, "http://localhost:8081/v2/pipeline");
        assert_eq!(client.health_url, "http://localhost:8081/health");
    }

    // ── hrana_error_to_backend: additional cases ────────────────────────────

    #[test]
    fn hrana_error_with_non_sqlite_code() {
        let err = hrana_error_to_backend(ErrorResponse {
            message: "something else".into(),
            code: Some("INTERNAL_ERROR".into()),
        });
        match err {
            BackendError::Other(msg) => assert_eq!(msg, "something else"),
            other => panic!("expected Other error, got: {other:?}"),
        }
    }
}
