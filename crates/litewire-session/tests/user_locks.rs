//! Named-lock functions through the session layer: the statement shapes
//! applications issue, their MySQL return values, and the error codes for
//! bad names — against a real (rusqlite) connection with a lock scope.

use std::sync::Arc;

use litewire_backend::user_lock::{UserLockRegistry, scoped_conn};
use litewire_backend::{Backend, Rusqlite, Value};
use litewire_session::{Session, SessionError, SessionResult, error_map};
use litewire_translate::Dialect;

/// A session over an in-memory database whose connection is scoped to
/// `namespace` in `registry`.
async fn scoped_session(registry: &Arc<UserLockRegistry>, namespace: &str) -> Session {
    let backend = Rusqlite::memory().unwrap();
    let conn = backend.connect().await.unwrap();
    Session::new(scoped_conn(conn, registry, namespace), Dialect::MySQL)
}

/// A session whose connection has **no** lock scope.
async fn unscoped_session() -> Session {
    let backend = Rusqlite::memory().unwrap();
    Session::new(backend.connect().await.unwrap(), Dialect::MySQL)
}

/// The single cell of a one-row, one-column result.
fn cell(result: SessionResult) -> Value {
    match result {
        SessionResult::Rows(rs) => {
            assert_eq!(rs.rows.len(), 1, "one row expected: {rs:?}");
            assert_eq!(rs.columns.len(), 1, "one column expected: {rs:?}");
            rs.rows[0][0].clone()
        }
        SessionResult::Ok(ok) => panic!("expected rows, got OK: {ok:?}"),
    }
}

async fn eval(session: &mut Session, sql: &str) -> Value {
    cell(session.query(sql, &[]).await.unwrap())
}

#[tokio::test]
async fn get_lock_excludes_other_sessions_and_reports_mysql_values() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;

    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(0)
    );
    assert_eq!(
        eval(&mut b, "SELECT IS_FREE_LOCK('job')").await,
        Value::Integer(0)
    );
    assert_eq!(
        eval(&mut b, "SELECT IS_FREE_LOCK('other')").await,
        Value::Integer(1)
    );
    assert!(matches!(
        eval(&mut b, "SELECT IS_USED_LOCK('job')").await,
        Value::Integer(_)
    ));
    assert_eq!(
        eval(&mut b, "SELECT IS_USED_LOCK('other')").await,
        Value::Null
    );

    // Not the holder: 0, and nothing changes.
    assert_eq!(
        eval(&mut b, "SELECT RELEASE_LOCK('job')").await,
        Value::Integer(0)
    );
    assert_eq!(
        eval(&mut a, "SELECT RELEASE_LOCK('job')").await,
        Value::Integer(1)
    );
    // Never held / already released: NULL.
    assert_eq!(
        eval(&mut a, "SELECT RELEASE_LOCK('job')").await,
        Value::Null
    );
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
}

#[tokio::test]
async fn reentrant_count_and_release_all() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;

    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut a, "SELECT RELEASE_LOCK('job')").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(0),
        "still held once"
    );
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('again', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut a, "SELECT RELEASE_ALL_LOCKS()").await,
        Value::Integer(2)
    );
    assert_eq!(
        eval(&mut a, "SELECT RELEASE_ALL_LOCKS()").await,
        Value::Integer(0)
    );
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
}

/// Sendy's exact statements: the lock with an alias, the release through `DO`.
#[tokio::test]
async fn sendy_statement_shapes() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;

    let rs = match a
        .query(
            r#"SELECT GET_LOCK("sendy_campaign_send_7", 0) AS lock_status"#,
            &[],
        )
        .await
        .unwrap()
    {
        SessionResult::Rows(rs) => rs,
        other => panic!("{other:?}"),
    };
    assert_eq!(rs.columns[0].name, "lock_status");
    assert_eq!(rs.rows, vec![vec![Value::Integer(1)]]);

    assert_eq!(
        eval(
            &mut b,
            r#"SELECT GET_LOCK("sendy_campaign_send_7", 0) AS lock_status"#
        )
        .await,
        Value::Integer(0)
    );

    match a
        .query(r#"DO RELEASE_LOCK("sendy_campaign_send_7")"#, &[])
        .await
        .unwrap()
    {
        SessionResult::Ok(ok) => {
            assert_eq!(ok.affected_rows, 0);
            assert!(!ok.noop);
        }
        SessionResult::Rows(rs) => panic!("DO must not return rows: {rs:?}"),
    }
    assert_eq!(
        eval(
            &mut b,
            r#"SELECT GET_LOCK("sendy_campaign_send_7", 0) AS lock_status"#
        )
        .await,
        Value::Integer(1)
    );
}

#[tokio::test]
async fn unaliased_call_is_named_as_written_and_mixes_with_other_columns() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let rs = match a
        .query(
            "SELECT GET_LOCK('x', 0), 'tag' AS tag, IS_FREE_LOCK('x')",
            &[],
        )
        .await
        .unwrap()
    {
        SessionResult::Rows(rs) => rs,
        other => panic!("{other:?}"),
    };
    let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["GET_LOCK('x', 0)", "tag", "IS_FREE_LOCK('x')"]);
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::Integer(1),
            Value::Text("tag".into()),
            Value::Integer(0)
        ]]
    );
}

#[tokio::test]
async fn prepared_statements_bind_lock_arguments_from_parameters() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;

    let stmt = a.prepare("SELECT GET_LOCK(?, ?) AS got, ? AS tag").unwrap();
    assert_eq!(stmt.param_count(), 3);
    let params = [
        Value::Text("Job".into()),
        Value::Integer(0),
        Value::Text("t".into()),
    ];
    match a.execute_prepared(&stmt, &params).await.unwrap() {
        SessionResult::Rows(rs) => {
            assert_eq!(
                rs.rows,
                vec![vec![Value::Integer(1), Value::Text("t".into())]]
            );
        }
        other => panic!("{other:?}"),
    }
    // Case-insensitive: 'Job' and 'JOB' are one lock.
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('JOB', 0)").await,
        Value::Integer(0)
    );

    let release = b.prepare("DO RELEASE_LOCK(?)").unwrap();
    assert_eq!(release.param_count(), 1);
    assert!(matches!(
        b.execute_prepared(&release, &[Value::Text("job".into())])
            .await
            .unwrap(),
        SessionResult::Ok(_)
    ));
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('JOB', 0)").await,
        Value::Integer(0),
        "b is not the holder"
    );
    assert!(matches!(
        a.execute_prepared(&release, &[Value::Text("job".into())])
            .await
            .unwrap(),
        SessionResult::Ok(_)
    ));
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('JOB', 0)").await,
        Value::Integer(1)
    );
}

#[tokio::test]
async fn bad_names_raise_mysql_codes() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;

    for sql in [
        "SELECT GET_LOCK('', 0)",
        "SELECT GET_LOCK(NULL, 0)",
        "SELECT IS_FREE_LOCK('')",
    ] {
        let err = a.query(sql, &[]).await.unwrap_err();
        assert_eq!(
            err.code(),
            error_map::ER_USER_LOCK_WRONG_NAME,
            "{sql}: {err}"
        );
        assert_eq!(err.sqlstate(), *b"42000");
    }
    let long = "x".repeat(65);
    let err = a
        .query(&format!("SELECT GET_LOCK('{long}', 0)"), &[])
        .await
        .unwrap_err();
    assert_eq!(err.code(), error_map::ER_USER_LOCK_OVERLONG_NAME);
    assert!(
        err.to_string().contains("should not exceed 64 characters"),
        "{err}"
    );

    // A NULL timeout is not an error: the call is NULL and takes nothing.
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('x', NULL)").await,
        Value::Null
    );
    assert_eq!(
        eval(&mut a, "SELECT IS_FREE_LOCK('x')").await,
        Value::Integer(1)
    );

    // Wrong arity is a statement error.
    let err = a.query("SELECT GET_LOCK('x')", &[]).await.unwrap_err();
    assert!(matches!(err, SessionError::Translate(_)), "{err}");
}

#[tokio::test]
async fn a_connection_without_a_lock_scope_errors_instead_of_pretending() {
    let mut s = unscoped_session().await;
    let err = s.query("SELECT GET_LOCK('x', 0)", &[]).await.unwrap_err();
    assert!(
        err.to_string().contains("named locks") && err.to_string().contains("not available"),
        "{err}"
    );
    // The error must not be mistaken for a successful 0/NULL: it is an error.
    assert!(matches!(err, SessionError::Db { .. }));
}

#[tokio::test]
async fn dropping_the_session_releases_its_locks() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(1)
    );
    assert!(a.may_hold_user_locks());
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(0)
    );
    drop(a);
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(1)
    );
}

#[tokio::test]
async fn release_user_locks_is_the_boundary_hook() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;
    assert!(!a.may_hold_user_locks());
    assert_eq!(
        a.release_user_locks().await.unwrap(),
        0,
        "no-op when nothing was taken"
    );
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(1)
    );
    assert!(a.may_hold_user_locks());
    assert_eq!(a.release_user_locks().await.unwrap(), 2);
    assert!(!a.may_hold_user_locks());
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(1)
    );
}

#[tokio::test]
async fn namespaces_isolate_tenants() {
    let registry = UserLockRegistry::new();
    let mut tenant_a = scoped_session(&registry, "site-a").await;
    let mut tenant_b = scoped_session(&registry, "site-b").await;
    assert_eq!(
        eval(&mut tenant_a, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut tenant_b, "SELECT IS_FREE_LOCK('job')").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut tenant_b, "SELECT IS_USED_LOCK('job')").await,
        Value::Null
    );
    assert_eq!(
        eval(&mut tenant_b, "SELECT GET_LOCK('job', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut tenant_b, "SELECT RELEASE_LOCK('job')").await,
        Value::Integer(1)
    );
    assert_eq!(
        eval(&mut tenant_a, "SELECT IS_FREE_LOCK('job')").await,
        Value::Integer(0)
    );
}

/// A timed `GET_LOCK` returns as soon as the holder releases — it is woken,
/// not polled — and `0` when the timeout passes first.
#[tokio::test]
async fn timed_wait_wakes_on_release() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('x', 0)").await,
        Value::Integer(1)
    );

    let started = std::time::Instant::now();
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('x', 1)").await,
        Value::Integer(0)
    );
    assert!(started.elapsed() >= std::time::Duration::from_secs(1));

    let releaser = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        eval(&mut a, "SELECT RELEASE_LOCK('x')").await;
        a
    });
    let started = std::time::Instant::now();
    assert_eq!(
        eval(&mut b, "SELECT GET_LOCK('x', 10)").await,
        Value::Integer(1)
    );
    let waited = started.elapsed();
    assert!(
        waited >= std::time::Duration::from_millis(200)
            && waited < std::time::Duration::from_secs(5),
        "woken after {waited:?}"
    );
    let _a = releaser.await.unwrap();
}

/// `SELECT CONNECTION_ID()` reports a non-zero id that equals the id
/// `IS_USED_LOCK()` reports for a lock this connection holds — the MySQL
/// consistency applications compare on (issue ephpm#657). Before the fix
/// `CONNECTION_ID()` translated to the constant `0` while `IS_USED_LOCK()`
/// returned the real holder id, so the two disagreed.
#[tokio::test]
async fn connection_id_agrees_with_is_used_lock() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;
    let mut b = scoped_session(&registry, "").await;

    let conn_id = eval(&mut a, "SELECT CONNECTION_ID()").await;
    assert!(
        matches!(conn_id, Value::Integer(n) if n > 0),
        "CONNECTION_ID must be a non-zero integer, got {conn_id:?}"
    );
    // Stable across calls on the same connection.
    assert_eq!(eval(&mut a, "SELECT CONNECTION_ID()").await, conn_id);
    // An alias is honored.
    assert_eq!(eval(&mut a, "SELECT CONNECTION_ID() AS id").await, conn_id);

    // A holds the lock; IS_USED_LOCK reports A's id — observed both by A
    // itself and by another connection B.
    assert_eq!(
        eval(&mut a, "SELECT GET_LOCK('rc', 0)").await,
        Value::Integer(1)
    );
    assert_eq!(eval(&mut a, "SELECT IS_USED_LOCK('rc')").await, conn_id);
    assert_eq!(eval(&mut b, "SELECT IS_USED_LOCK('rc')").await, conn_id);

    // Two connections have distinct ids.
    assert_ne!(eval(&mut b, "SELECT CONNECTION_ID()").await, conn_id);
}

/// A connection with no lock scope answers `CONNECTION_ID()` with `0`
/// (SQLite has no per-connection id), never an error.
#[tokio::test]
async fn connection_id_is_zero_without_a_lock_scope() {
    let mut s = unscoped_session().await;
    assert_eq!(
        eval(&mut s, "SELECT CONNECTION_ID()").await,
        Value::Integer(0)
    );
}

/// The prepared-statement path answers `SELECT CONNECTION_ID()` the same way
/// as the text path.
#[tokio::test]
async fn connection_id_through_prepared_statement() {
    let registry = UserLockRegistry::new();
    let mut a = scoped_session(&registry, "").await;

    let conn_id = eval(&mut a, "SELECT CONNECTION_ID()").await;
    let prepared = a.prepare("SELECT CONNECTION_ID()").unwrap();
    assert_eq!(prepared.param_count(), 0);
    let via_prepared = cell(a.execute_prepared(&prepared, &[]).await.unwrap());
    assert_eq!(via_prepared, conn_id);
}
