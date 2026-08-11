//! End-to-end MySQL `LIKE` escape semantics through [`Session`].
//!
//! MySQL treats backslash as the implicit `LIKE` escape character; SQLite
//! has no default escape. These tests run real queries against an
//! in-memory rusqlite backend and assert on which rows actually match —
//! not on the emitted SQL text — so they prove the whole chain: MySQL
//! string-literal tokenization, the `ESCAPE '\'` rewrite, emission, and
//! SQLite's `LIKE` evaluation.

use std::sync::Arc;

use litewire_backend::{Rusqlite, SharedBackend, Value};
use litewire_session::{Session, SessionResult};
use litewire_translate::Dialect;

/// A MySQL-dialect session over an in-memory SQLite database seeded with
/// one TEXT column `v` containing `rows`.
async fn session_with_rows(rows: &[&str]) -> Session {
    let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
    let conn = backend.connect().await.unwrap();
    let mut session = Session::new(conn, Dialect::MySQL);
    session.query("CREATE TABLE t (v TEXT)", &[]).await.unwrap();
    for row in rows {
        session
            .query(
                "INSERT INTO t (v) VALUES (?)",
                &[Value::Text((*row).to_string())],
            )
            .await
            .unwrap();
    }
    session
}

/// Run `SELECT v FROM t WHERE <predicate>` and return the matching values
/// in sorted order.
async fn matching(session: &mut Session, predicate: &str, params: &[Value]) -> Vec<String> {
    let sql = format!("SELECT v FROM t WHERE {predicate} ORDER BY v");
    match session.query(&sql, params).await.unwrap() {
        SessionResult::Rows(rs) => rs
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::Text(s) => s.clone(),
                other => panic!("expected text value, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[tokio::test]
async fn escaped_percent_matches_literal_percent_only() {
    let mut session = session_with_rows(&["a%b", "axb", "ab"]).await;
    // MySQL pattern 'a\%b': % is escaped, so only the literal 'a%b' row.
    let hits = matching(&mut session, r"v LIKE 'a\%b'", &[]).await;
    assert_eq!(hits, vec!["a%b"]);
}

#[tokio::test]
async fn unescaped_percent_stays_a_wildcard() {
    let mut session = session_with_rows(&["a%b", "axb", "ab"]).await;
    let hits = matching(&mut session, "v LIKE 'a%b'", &[]).await;
    assert_eq!(hits, vec!["a%b", "ab", "axb"]);
}

#[tokio::test]
async fn escaped_underscore_matches_literal_underscore_only() {
    let mut session = session_with_rows(&["a_b", "axb"]).await;
    let hits = matching(&mut session, r"v LIKE 'a\_b'", &[]).await;
    assert_eq!(hits, vec!["a_b"]);
}

#[tokio::test]
async fn unescaped_underscore_stays_a_wildcard() {
    let mut session = session_with_rows(&["a_b", "axb"]).await;
    let hits = matching(&mut session, "v LIKE 'a_b'", &[]).await;
    assert_eq!(hits, vec!["a_b", "axb"]);
}

#[tokio::test]
async fn wpdb_esc_like_pattern_matches_only_the_literal() {
    // wpdb::esc_like('50%') produces the pattern 50\%.
    let mut session = session_with_rows(&["50%", "50x", "500"]).await;
    let hits = matching(&mut session, r"v LIKE '50\%'", &[]).await;
    assert_eq!(hits, vec!["50%"]);
}

#[tokio::test]
async fn double_backslash_percent_also_matches_literal_percent() {
    // MySQL semantics: '50\\%' string-processes to 50\% and the LIKE
    // engine then treats \% as an escaped %. Both spellings match a
    // literal % — verify litewire agrees.
    let mut session = session_with_rows(&["50%", "50x"]).await;
    let hits = matching(&mut session, r"v LIKE '50\\%'", &[]).await;
    assert_eq!(hits, vec!["50%"]);
}

#[tokio::test]
async fn quadruple_backslash_matches_literal_backslash() {
    // MySQL: matching one literal backslash in LIKE takes '\\\\' — string
    // processing halves it to \\ and the LIKE engine halves it again.
    let mut session = session_with_rows(&[r"a\b", "ab"]).await;
    let hits = matching(&mut session, r"v LIKE 'a\\\\b'", &[]).await;
    assert_eq!(hits, vec![r"a\b"]);

    // And '\\b' collapses to an escaped (literal) 'b', matching plain 'ab'.
    let hits = matching(&mut session, r"v LIKE 'a\\b'", &[]).await;
    assert_eq!(hits, vec!["ab"]);
}

#[tokio::test]
async fn not_like_respects_the_escape() {
    let mut session = session_with_rows(&["a%b", "axb"]).await;
    let hits = matching(&mut session, r"v NOT LIKE 'a\%b'", &[]).await;
    assert_eq!(hits, vec!["axb"]);
}

#[tokio::test]
async fn bound_parameter_pattern_respects_mysql_escapes() {
    // Bound parameters skip string-literal processing (as they do on
    // MySQL's binary protocol): the value contains a real backslash and
    // the implicit ESCAPE '\' interprets it.
    let mut session = session_with_rows(&["50%", "50x"]).await;
    let hits = matching(
        &mut session,
        "v LIKE ?",
        &[Value::Text(r"50\%".to_string())],
    )
    .await;
    assert_eq!(hits, vec!["50%"]);
}

#[tokio::test]
async fn explicit_escape_clause_is_preserved() {
    let mut session = session_with_rows(&["a%b", "axb"]).await;
    let hits = matching(&mut session, "v LIKE 'a|%b' ESCAPE '|'", &[]).await;
    assert_eq!(hits, vec!["a%b"]);
}
