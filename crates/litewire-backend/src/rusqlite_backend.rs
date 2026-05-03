//! In-process SQLite backend using `rusqlite`.
//!
//! All database calls are wrapped in [`tokio::task::spawn_blocking`] since
//! rusqlite is synchronous. A `Mutex` serializes access (SQLite is
//! single-writer anyway).

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::task;

use crate::{Backend, BackendError, Column, ExecuteResult, ResultSet, Value};

/// In-process SQLite backend via `rusqlite`.
pub struct Rusqlite {
    conn: Arc<Mutex<Connection>>,
}

impl Rusqlite {
    /// Open a SQLite database file. Creates it if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BackendError> {
        let conn = Connection::open(path).map_err(|e| BackendError::Sqlite(e.to_string()))?;

        // Enable WAL mode for better concurrent read performance.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| BackendError::Sqlite(e.to_string()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory SQLite database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn memory() -> Result<Self, BackendError> {
        let conn =
            Connection::open_in_memory().map_err(|e| BackendError::Sqlite(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

/// Convert a [`Value`] slice to rusqlite params.
fn bind_params(params: &[Value]) -> Vec<Box<dyn rusqlite::types::ToSql>> {
    params
        .iter()
        .map(|v| -> Box<dyn rusqlite::types::ToSql> {
            match v {
                Value::Null => Box::new(rusqlite::types::Null),
                Value::Integer(i) => Box::new(*i),
                Value::Float(f) => Box::new(*f),
                Value::Text(s) => Box::new(s.clone()),
                Value::Blob(b) => Box::new(b.clone()),
            }
        })
        .collect()
}

/// Extract a [`Value`] from a rusqlite row at the given column index.
fn extract_value(row: &rusqlite::Row<'_>, idx: usize) -> Result<Value, rusqlite::Error> {
    use rusqlite::types::ValueRef;
    match row.get_ref(idx)? {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(i) => Ok(Value::Integer(i)),
        ValueRef::Real(f) => Ok(Value::Float(f)),
        ValueRef::Text(s) => Ok(Value::Text(
            String::from_utf8_lossy(s).into_owned(),
        )),
        ValueRef::Blob(b) => Ok(Value::Blob(b.to_vec())),
    }
}

#[async_trait::async_trait]
impl Backend for Rusqlite {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| BackendError::Sqlite(e.to_string()))?;

            let col_count = stmt.column_count();
            let columns: Vec<Column> = (0..col_count)
                .map(|i| Column {
                    name: stmt.column_name(i).unwrap_or("?").to_string(),
                    decltype: None, // rusqlite 0.32 does not expose column_decltype on Statement
                })
                .collect();

            let bound = bind_params(&params);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                bound.iter().map(|b| b.as_ref()).collect();

            let mut result_rows = Vec::new();
            let mut rows = stmt
                .query(param_refs.as_slice())
                .map_err(|e| BackendError::Sqlite(e.to_string()))?;

            while let Some(row) = rows.next().map_err(|e| BackendError::Sqlite(e.to_string()))? {
                let mut values = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    values.push(
                        extract_value(row, i)
                            .map_err(|e| BackendError::Sqlite(e.to_string()))?,
                    );
                }
                result_rows.push(values);
            }

            Ok(ResultSet {
                columns,
                rows: result_rows,
            })
        })
        .await
        .map_err(|e| BackendError::Other(format!("spawn_blocking join error: {e}")))?
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
            let conn = conn.lock();

            let bound = bind_params(&params);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                bound.iter().map(|b| b.as_ref()).collect();

            let affected = conn
                .execute(&sql, param_refs.as_slice())
                .map_err(|e| BackendError::Sqlite(e.to_string()))?;

            let last_id = conn.last_insert_rowid();

            Ok(ExecuteResult {
                affected_rows: affected as u64,
                last_insert_rowid: if last_id != 0 { Some(last_id) } else { None },
            })
        })
        .await
        .map_err(|e| BackendError::Other(format!("spawn_blocking join error: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_crud() {
        let backend = Rusqlite::memory().unwrap();

        backend
            .execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
                &[],
            )
            .await
            .unwrap();

        let result = backend
            .execute(
                "INSERT INTO users (name) VALUES (?1)",
                &[Value::Text("Alice".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.last_insert_rowid, Some(1));

        let result = backend
            .execute(
                "INSERT INTO users (name) VALUES (?1)",
                &[Value::Text("Bob".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.last_insert_rowid, Some(2));

        let rs = backend.query("SELECT id, name FROM users ORDER BY id", &[]).await.unwrap();
        assert_eq!(rs.columns.len(), 2);
        assert_eq!(rs.columns[0].name, "id");
        assert_eq!(rs.columns[1].name, "name");
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][1], Value::Text("Alice".into()));
        assert_eq!(rs.rows[1][1], Value::Text("Bob".into()));

        let rs = backend
            .query("SELECT * FROM users WHERE id = ?1", &[Value::Integer(1)])
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], Value::Text("Alice".into()));
    }

    #[tokio::test]
    async fn types_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute(
                "CREATE TABLE typed (i INTEGER, r REAL, t TEXT, b BLOB)",
                &[],
            )
            .await
            .unwrap();

        backend
            .execute(
                "INSERT INTO typed VALUES (?1, ?2, ?3, ?4)",
                &[
                    Value::Integer(42),
                    Value::Float(2.72),
                    Value::Text("hello".into()),
                    Value::Blob(vec![0xDE, 0xAD]),
                ],
            )
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM typed", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(42));
        assert_eq!(rs.rows[0][1], Value::Float(2.72));
        assert_eq!(rs.rows[0][2], Value::Text("hello".into()));
        assert_eq!(rs.rows[0][3], Value::Blob(vec![0xDE, 0xAD]));
    }

    #[tokio::test]
    async fn null_handling() {
        let backend = Rusqlite::memory().unwrap();
        backend.execute("CREATE TABLE t (v TEXT)", &[]).await.unwrap();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Null])
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Null);
    }

    #[tokio::test]
    async fn empty_table_query() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER, name TEXT)", &[])
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.columns.len(), 2);
        assert!(rs.rows.is_empty());
    }

    #[tokio::test]
    async fn multiple_params() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)", &[])
            .await
            .unwrap();
        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2, ?3)",
                &[
                    Value::Integer(1),
                    Value::Text("hello".into()),
                    Value::Float(9.99),
                ],
            )
            .await
            .unwrap();

        let rs = backend
            .query("SELECT * FROM t WHERE a = ?1 AND b = ?2", &[
                Value::Integer(1),
                Value::Text("hello".into()),
            ])
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn affected_rows_count() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", &[])
            .await
            .unwrap();

        for i in 0..5 {
            backend
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Integer(i), Value::Text(format!("v{i}"))],
                )
                .await
                .unwrap();
        }

        let result = backend
            .execute("DELETE FROM t WHERE id >= ?1", &[Value::Integer(3)])
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 2);
    }

    #[tokio::test]
    async fn query_error_on_bad_sql() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.query("DEFINITELY NOT SQL !!!", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_error_on_bad_sql() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.execute("DEFINITELY NOT SQL !!!", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn blob_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (data BLOB)", &[])
            .await
            .unwrap();

        let data = vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF];
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Blob(data.clone())])
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Blob(data));
    }

    #[tokio::test]
    async fn last_insert_rowid_increments() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                &[],
            )
            .await
            .unwrap();

        let r1 = backend
            .execute("INSERT INTO t (v) VALUES (?1)", &[Value::Text("a".into())])
            .await
            .unwrap();
        let r2 = backend
            .execute("INSERT INTO t (v) VALUES (?1)", &[Value::Text("b".into())])
            .await
            .unwrap();
        let r3 = backend
            .execute("INSERT INTO t (v) VALUES (?1)", &[Value::Text("c".into())])
            .await
            .unwrap();

        assert_eq!(r1.last_insert_rowid, Some(1));
        assert_eq!(r2.last_insert_rowid, Some(2));
        assert_eq!(r3.last_insert_rowid, Some(3));
    }

    #[tokio::test]
    async fn column_names_preserved() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)", &[])
            .await
            .unwrap();

        let rs = backend.query("SELECT id, name, email FROM users", &[]).await.unwrap();
        assert_eq!(rs.columns[0].name, "id");
        assert_eq!(rs.columns[1].name, "name");
        assert_eq!(rs.columns[2].name, "email");
    }

    #[tokio::test]
    async fn query_with_alias() {
        let backend = Rusqlite::memory().unwrap();
        let rs = backend.query("SELECT 1 AS num, 'hello' AS greeting", &[]).await.unwrap();
        assert_eq!(rs.columns[0].name, "num");
        assert_eq!(rs.columns[1].name, "greeting");
        assert_eq!(rs.rows[0][0], Value::Integer(1));
        assert_eq!(rs.rows[0][1], Value::Text("hello".into()));
    }

    // ── Concurrent / sequential operations ──────────────────────────────────

    #[tokio::test]
    async fn query_execute_query_sequential() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        // Query empty table
        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert!(rs.rows.is_empty());

        // Insert a row
        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(1), Value::Text("first".into())],
            )
            .await
            .unwrap();

        // Query again and see the new row
        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], Value::Text("first".into()));

        // Insert another row
        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(2), Value::Text("second".into())],
            )
            .await
            .unwrap();

        // Verify both rows exist
        let rs = backend
            .query("SELECT * FROM t ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 2);
    }

    #[tokio::test]
    async fn explicit_integer_primary_key_rowid() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
            .await
            .unwrap();

        // Insert with an explicit id of 42
        let result = backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(42), Value::Text("at 42".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.last_insert_rowid, Some(42));

        // Insert with an explicit id of 100
        let result = backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[Value::Integer(100), Value::Text("at 100".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.last_insert_rowid, Some(100));
    }

    // ── Edge case values ────────────────────────────────────────────────────

    #[tokio::test]
    async fn integer_extremes() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v INTEGER)", &[])
            .await
            .unwrap();

        for val in [i64::MIN, i64::MAX, 0] {
            backend
                .execute("INSERT INTO t VALUES (?1)", &[Value::Integer(val)])
                .await
                .unwrap();
        }

        let rs = backend.query("SELECT v FROM t ORDER BY rowid", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Integer(i64::MIN));
        assert_eq!(rs.rows[1][0], Value::Integer(i64::MAX));
        assert_eq!(rs.rows[2][0], Value::Integer(0));
    }

    #[tokio::test]
    async fn float_extremes() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v REAL)", &[])
            .await
            .unwrap();

        for val in [f64::MIN, f64::MAX, f64::MIN_POSITIVE] {
            backend
                .execute("INSERT INTO t VALUES (?1)", &[Value::Float(val)])
                .await
                .unwrap();
        }

        let rs = backend.query("SELECT v FROM t ORDER BY rowid", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Float(f64::MIN));
        assert_eq!(rs.rows[1][0], Value::Float(f64::MAX));
        assert_eq!(rs.rows[2][0], Value::Float(f64::MIN_POSITIVE));
    }

    #[tokio::test]
    async fn empty_text_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Text(String::new())])
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text(String::new()));
    }

    #[tokio::test]
    async fn empty_blob_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v BLOB)", &[])
            .await
            .unwrap();
        backend
            .execute("INSERT INTO t VALUES (?1)", &[Value::Blob(vec![])])
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Blob(vec![]));
    }

    #[tokio::test]
    async fn large_text_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();

        let large = "x".repeat(10_000);
        backend
            .execute(
                "INSERT INTO t VALUES (?1)",
                &[Value::Text(large.clone())],
            )
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Text(large));
    }

    #[tokio::test]
    async fn large_blob_roundtrip() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v BLOB)", &[])
            .await
            .unwrap();

        let large: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        backend
            .execute(
                "INSERT INTO t VALUES (?1)",
                &[Value::Blob(large.clone())],
            )
            .await
            .unwrap();

        let rs = backend.query("SELECT v FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows[0][0], Value::Blob(large));
    }

    #[tokio::test]
    async fn text_with_special_characters() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (v TEXT)", &[])
            .await
            .unwrap();

        let specials = vec![
            "hello\0world",        // null byte
            "こんにちは",           // Japanese
            "🎉🚀💯",             // emoji
            "café résumé naïve",   // accented
            "line1\nline2\ttab",   // newline and tab
            "'quotes' and \"double\"", // quotes
        ];

        for s in &specials {
            backend
                .execute(
                    "INSERT INTO t VALUES (?1)",
                    &[Value::Text(s.to_string())],
                )
                .await
                .unwrap();
        }

        let rs = backend.query("SELECT v FROM t ORDER BY rowid", &[]).await.unwrap();
        for (i, s) in specials.iter().enumerate() {
            assert_eq!(rs.rows[i][0], Value::Text(s.to_string()));
        }
    }

    // ── Mixed types in a single row ─────────────────────────────────────────

    #[tokio::test]
    async fn insert_all_types_in_one_row() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute(
                "CREATE TABLE t (a, b INTEGER, c REAL, d TEXT, e BLOB)",
                &[],
            )
            .await
            .unwrap();

        backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5)",
                &[
                    Value::Null,
                    Value::Integer(99),
                    Value::Float(1.5),
                    Value::Text("mixed".into()),
                    Value::Blob(vec![0xAB, 0xCD]),
                ],
            )
            .await
            .unwrap();

        let rs = backend.query("SELECT * FROM t", &[]).await.unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Null);
        assert_eq!(rs.rows[0][1], Value::Integer(99));
        assert_eq!(rs.rows[0][2], Value::Float(1.5));
        assert_eq!(rs.rows[0][3], Value::Text("mixed".into()));
        assert_eq!(rs.rows[0][4], Value::Blob(vec![0xAB, 0xCD]));
    }

    // ── Error cases ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn query_nonexistent_table() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.query("SELECT * FROM nonexistent_table", &[]).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no such table"),
            "expected 'no such table' in error: {err}"
        );
    }

    #[tokio::test]
    async fn execute_wrong_param_count() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (a INTEGER, b TEXT)", &[])
            .await
            .unwrap();

        // Provide 3 params when only 2 are expected
        let result = backend
            .execute(
                "INSERT INTO t VALUES (?1, ?2)",
                &[
                    Value::Integer(1),
                    Value::Text("hello".into()),
                    Value::Integer(99),
                ],
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_sql_syntax_error() {
        let backend = Rusqlite::memory().unwrap();
        let result = backend.execute("INSERT INTO VALUES ()", &[]).await;
        assert!(result.is_err());
    }

    // ── UPDATE and DELETE affected_rows ──────────────────────────────────────

    #[tokio::test]
    async fn update_affected_rows() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER, v TEXT)", &[])
            .await
            .unwrap();

        for i in 0..3 {
            backend
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    &[Value::Integer(i), Value::Text("same".into())],
                )
                .await
                .unwrap();
        }

        let result = backend
            .execute(
                "UPDATE t SET v = ?1 WHERE v = ?2",
                &[Value::Text("changed".into()), Value::Text("same".into())],
            )
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 3);
    }

    #[tokio::test]
    async fn delete_no_matching_rows() {
        let backend = Rusqlite::memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER)", &[])
            .await
            .unwrap();

        let result = backend
            .execute("DELETE FROM t WHERE id = ?1", &[Value::Integer(999)])
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 0);
    }
}
