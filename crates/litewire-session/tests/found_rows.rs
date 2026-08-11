//! `SQL_CALC_FOUND_ROWS` / `SELECT FOUND_ROWS()` semantics through
//! [`Session`], against a real in-memory rusqlite backend.
//!
//! WordPress's `WP_Query` issues `SELECT SQL_CALC_FOUND_ROWS ... LIMIT n`
//! followed by `SELECT FOUND_ROWS()` and feeds the result into
//! `found_posts` / `X-WP-Total`. These tests assert on the values a client
//! actually receives.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use litewire_backend::{
    BackendConn, BackendError, Column, ExecuteResult, ResultSet, Rusqlite, SharedBackend, Value,
};
use litewire_session::{Session, SessionResult};
use litewire_translate::Dialect;

/// A MySQL-dialect session over an in-memory SQLite database with a table
/// `t (id INTEGER PRIMARY KEY, v TEXT)` seeded with `n` rows.
async fn seeded_session(n: i64) -> Session {
    let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
    let conn = backend.connect().await.unwrap();
    let mut session = Session::new(conn, Dialect::MySQL);
    session
        .query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
        .await
        .unwrap();
    for i in 1..=n {
        session
            .query(
                "INSERT INTO t (id, v) VALUES (?, ?)",
                &[Value::Integer(i), Value::Text(format!("row{i}"))],
            )
            .await
            .unwrap();
    }
    session
}

fn rows(result: SessionResult) -> ResultSet {
    match result {
        SessionResult::Rows(rs) => rs,
        SessionResult::Ok(ok) => panic!("expected rows, got OK: {ok:?}"),
    }
}

/// Run `SELECT FOUND_ROWS()` and return (column name, value).
async fn found_rows(session: &mut Session, sql: &str) -> (String, i64) {
    let rs = rows(session.query(sql, &[]).await.unwrap());
    assert_eq!(rs.columns.len(), 1, "one column expected");
    assert_eq!(rs.rows.len(), 1, "one row expected");
    let Value::Integer(n) = rs.rows[0][0] else {
        panic!("expected integer, got {:?}", rs.rows[0][0]);
    };
    (rs.columns[0].name.clone(), n)
}

#[tokio::test]
async fn calc_query_with_limit_reports_full_count() {
    let mut session = seeded_session(5).await;

    let rs = rows(
        session
            .query(
                "SELECT SQL_CALC_FOUND_ROWS * FROM t ORDER BY id LIMIT 2",
                &[],
            )
            .await
            .unwrap(),
    );
    assert_eq!(rs.rows.len(), 2, "LIMIT must still apply");

    let (col, n) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(col, "FOUND_ROWS()");
    assert_eq!(n, 5);
    assert_eq!(session.found_rows(), 1, "FOUND_ROWS() itself returns 1 row");
}

#[tokio::test]
async fn calc_query_with_offset_and_where() {
    let mut session = seeded_session(5).await;

    let rs = rows(
        session
            .query(
                "SELECT SQL_CALC_FOUND_ROWS id FROM t WHERE id > 1 ORDER BY id LIMIT 1 OFFSET 1",
                &[],
            )
            .await
            .unwrap(),
    );
    assert_eq!(rs.rows.len(), 1);

    let (_, n) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(n, 4, "count respects WHERE but not LIMIT/OFFSET");
}

#[tokio::test]
async fn calc_query_with_comma_limit_form() {
    let mut session = seeded_session(5).await;
    let rs = rows(
        session
            .query("SELECT SQL_CALC_FOUND_ROWS id FROM t LIMIT 1, 2", &[])
            .await
            .unwrap(),
    );
    assert_eq!(rs.rows.len(), 2);
    let (_, n) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(n, 5);
}

#[tokio::test]
async fn calc_query_is_case_insensitive_and_alias_names_column() {
    let mut session = seeded_session(3).await;
    rows(
        session
            .query("select sql_calc_found_rows id from t limit 1", &[])
            .await
            .unwrap(),
    );
    let (col, n) = found_rows(&mut session, "select found_rows() as total").await;
    assert_eq!(col, "total");
    assert_eq!(n, 3);
}

#[tokio::test]
async fn calc_query_with_bound_where_params() {
    let mut session = seeded_session(5).await;
    let rs = rows(
        session
            .query(
                "SELECT SQL_CALC_FOUND_ROWS id FROM t WHERE id > ? ORDER BY id LIMIT 1",
                &[Value::Integer(2)],
            )
            .await
            .unwrap(),
    );
    assert_eq!(rs.rows.len(), 1);
    let (_, n) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(n, 3, "rows with id > 2");
}

#[tokio::test]
async fn placeholder_limit_falls_back_to_returned_row_count() {
    // Documented subset: a LIMIT bound as a placeholder cannot be
    // stripped without desynchronizing the parameter list, so the value
    // falls back to the returned-row count.
    let mut session = seeded_session(5).await;
    let rs = rows(
        session
            .query(
                "SELECT SQL_CALC_FOUND_ROWS id FROM t LIMIT ?",
                &[Value::Integer(2)],
            )
            .await
            .unwrap(),
    );
    assert_eq!(rs.rows.len(), 2);
    let (_, n) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(n, 2);
}

#[tokio::test]
async fn without_calc_hint_reports_last_select_row_count() {
    let mut session = seeded_session(5).await;
    rows(
        session
            .query("SELECT id FROM t ORDER BY id LIMIT 2", &[])
            .await
            .unwrap(),
    );
    let (_, n) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(n, 2, "MySQL reports the limited count without the hint");
}

#[tokio::test]
async fn fresh_session_reports_zero() {
    let mut session = seeded_session(3).await;
    // A fresh session (mutations don't touch the value).
    let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
    let conn = backend.connect().await.unwrap();
    let mut fresh = Session::new(conn, Dialect::MySQL);
    let (_, n) = found_rows(&mut fresh, "SELECT FOUND_ROWS()").await;
    assert_eq!(n, 0);

    // And repeated FOUND_ROWS() reports 1 (the previous FOUND_ROWS()
    // select returned one row), as on MySQL.
    let (_, again) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(again, 0, "seeded session never ran a select");
    let (_, again) = found_rows(&mut session, "SELECT FOUND_ROWS()").await;
    assert_eq!(again, 1);
}

#[tokio::test]
async fn prepared_path_calc_and_found_rows() {
    let mut session = seeded_session(5).await;

    // COM_STMT_PREPARE / COM_STMT_EXECUTE shape, as the wire handler
    // drives it via Session::prepare + execute_prepared.
    let calc = session
        .prepare("SELECT SQL_CALC_FOUND_ROWS id FROM t WHERE id > ? ORDER BY id LIMIT 1")
        .unwrap();
    let rs = rows(
        session
            .execute_prepared(&calc, &[Value::Integer(1)])
            .await
            .unwrap(),
    );
    assert_eq!(rs.rows.len(), 1);

    let fr = session.prepare("SELECT FOUND_ROWS()").unwrap();
    let rs = rows(session.execute_prepared(&fr, &[]).await.unwrap());
    assert_eq!(rs.columns[0].name, "FOUND_ROWS()");
    assert_eq!(rs.rows, vec![vec![Value::Integer(4)]]);

    // Re-executing the calc statement with different params refreshes the
    // stored total.
    rows(
        session
            .execute_prepared(&calc, &[Value::Integer(3)])
            .await
            .unwrap(),
    );
    let rs = rows(session.execute_prepared(&fr, &[]).await.unwrap());
    assert_eq!(rs.rows, vec![vec![Value::Integer(2)]]);
}

// ── Zero extra round trips without the hint ────────────────────────────────

/// A backend conn that counts calls and returns a canned result set.
struct CountingConn {
    queries: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl BackendConn for CountingConn {
    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<ResultSet, BackendError> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        Ok(ResultSet {
            columns: vec![Column {
                name: "n".into(),
                decltype: Some("INTEGER".into()),
            }],
            rows: vec![vec![Value::Integer(7)]],
        })
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<ExecuteResult, BackendError> {
        Ok(ExecuteResult {
            affected_rows: 0,
            last_insert_rowid: None,
        })
    }
}

#[tokio::test]
async fn count_round_trip_only_for_calc_statements() {
    let queries = Arc::new(AtomicUsize::new(0));
    let mut session = Session::new(
        Box::new(CountingConn {
            queries: Arc::clone(&queries),
        }),
        Dialect::MySQL,
    );

    // Plain SELECT: exactly one backend query, no COUNT(*) round trip.
    session.query("SELECT * FROM t LIMIT 2", &[]).await.unwrap();
    assert_eq!(queries.load(Ordering::SeqCst), 1);

    // Calc SELECT: main query + one COUNT(*).
    session
        .query("SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 2", &[])
        .await
        .unwrap();
    assert_eq!(queries.load(Ordering::SeqCst), 3);
    assert_eq!(session.found_rows(), 7, "stored from the count query");

    // FOUND_ROWS() itself never touches the backend.
    let rs = rows(session.query("SELECT FOUND_ROWS()", &[]).await.unwrap());
    assert_eq!(rs.rows, vec![vec![Value::Integer(7)]]);
    assert_eq!(queries.load(Ordering::SeqCst), 3);
}
