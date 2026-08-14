//! The MySQL error triple a client receives for real failures, through
//! [`Session`] against a real in-memory rusqlite backend (litewire #22).
//!
//! The unit tests in `error_map` feed the classifier hand-written strings.
//! These run statements that genuinely fail, so they also pin down the
//! message text SQLite actually produces — which is what the classifier's
//! substring matching depends on, and the thing most likely to drift out
//! from under it on a rusqlite or SQLite upgrade.

use std::sync::Arc;

use litewire_backend::{Rusqlite, SharedBackend};
use litewire_session::{Session, SessionError};
use litewire_translate::Dialect;

/// A MySQL-dialect session over an in-memory SQLite database.
async fn session() -> Session {
    let backend = Arc::new(Rusqlite::memory().unwrap()) as SharedBackend;
    let conn = backend.connect().await.unwrap();
    Session::new(conn, Dialect::MySQL)
}

/// Run `sql` and return the error it must produce.
async fn error(session: &mut Session, sql: &str) -> SessionError {
    session
        .query(sql, &[])
        .await
        .expect_err("statement was supposed to fail")
}

/// Selecting from a table that does not exist is 1146 / 42S02.
///
/// Frameworks branch on this to tell "the schema is not migrated yet" apart
/// from a broken query; it used to arrive as the 1105 / HY000 catch-all,
/// which carries no such distinction.
#[tokio::test]
async fn missing_table_is_1146() {
    let mut session = session().await;

    let e = error(&mut session, "SELECT * FROM sprockets").await;
    assert_eq!(e.code(), 1146);
    assert_eq!(&e.sqlstate(), b"42S02");
    assert!(
        e.to_string().contains("no such table"),
        "lost SQLite's text: {e}"
    );
    assert!(
        e.to_string().contains("sprockets"),
        "lost the table name: {e}"
    );
}

/// An INSERT and an UPDATE onto a missing table classify the same way.
#[tokio::test]
async fn missing_table_is_1146_for_writes_too() {
    let mut session = session().await;

    for sql in [
        "INSERT INTO sprockets (id) VALUES (1)",
        "UPDATE sprockets SET id = 1",
        "DELETE FROM sprockets",
    ] {
        let e = error(&mut session, sql).await;
        assert_eq!(e.code(), 1146, "{sql}");
        assert_eq!(&e.sqlstate(), b"42S02", "{sql}");
    }
}

/// A table that *does* exist must not be caught by the 1146 rule.
#[tokio::test]
async fn an_existing_table_is_never_1146() {
    let mut session = session().await;
    session
        .query("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
        .await
        .unwrap();

    // A real query works...
    session.query("SELECT * FROM t", &[]).await.unwrap();

    // ...and an unrelated failure against it is not reported as a missing
    // table.
    let e = error(&mut session, "SELECT nosuchcolumn FROM t").await;
    assert_ne!(e.code(), 1146, "{e}");
}

/// A duplicate key is 1062 / 23000 with a message a stock MySQL driver can
/// recognise.
///
/// Drivers litewire does not control detect duplicates by matching
/// `Duplicate entry ... for key ...` in the message. SQLite's own wording
/// never matched, so those clients saw an unclassified failure.
#[tokio::test]
async fn duplicate_key_is_1062_with_a_mysql_shaped_message() {
    let mut session = session().await;
    session
        .query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
            &[],
        )
        .await
        .unwrap();
    session
        .query(
            "INSERT INTO users (id, email) VALUES (1, 'a@example.com')",
            &[],
        )
        .await
        .unwrap();

    let e = error(
        &mut session,
        "INSERT INTO users (id, email) VALUES (2, 'a@example.com')",
    )
    .await;
    let message = e.to_string();

    assert_eq!(e.code(), 1062);
    assert_eq!(&e.sqlstate(), b"23000");
    assert!(message.starts_with("Duplicate entry '"), "got: {message}");
    assert!(
        message.contains("' for key 'users.email'"),
        "got: {message}"
    );
    // The SQLite text is kept, not replaced -- the synthesis must not cost
    // anyone their debugging information.
    assert!(
        message.contains("UNIQUE constraint failed: users.email"),
        "got: {message}"
    );
}

/// A primary-key collision takes the same path.
#[tokio::test]
async fn duplicate_primary_key_is_1062_with_a_mysql_shaped_message() {
    let mut session = session().await;
    session
        .query("CREATE TABLE t (id INTEGER PRIMARY KEY)", &[])
        .await
        .unwrap();
    session
        .query("INSERT INTO t (id) VALUES (1)", &[])
        .await
        .unwrap();

    let e = error(&mut session, "INSERT INTO t (id) VALUES (1)").await;
    let message = e.to_string();

    assert_eq!(e.code(), 1062);
    assert_eq!(&e.sqlstate(), b"23000");
    assert!(message.starts_with("Duplicate entry '"), "got: {message}");
    assert!(message.contains("' for key 't.id'"), "got: {message}");
}

/// Errors litewire does not reshape still arrive with SQLite's exact words.
///
/// Only the duplicate-key message is synthesised; everything else has to
/// reach the operator unaltered, whether it was classified or not.
#[tokio::test]
async fn unreshaped_errors_keep_sqlite_wording_verbatim() {
    let mut session = session().await;
    session
        .query(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            &[],
        )
        .await
        .unwrap();

    // Classified (1146), not reshaped.
    let missing = error(&mut session, "SELECT * FROM sprockets").await;
    assert_eq!(missing.code(), 1146);
    assert!(missing.to_string().ends_with("no such table: sprockets"));

    // Unclassified (the 1105 fallback), also not reshaped -- and still
    // carrying the column SQLite named.
    let not_null = error(&mut session, "INSERT INTO t (id) VALUES (1)").await;
    assert_eq!(not_null.code(), 1105);
    assert!(
        not_null
            .to_string()
            .ends_with("NOT NULL constraint failed: t.v"),
        "got: {not_null}"
    );
}
