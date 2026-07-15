//! Empirical answer to a Phase 2 design question: can two `Turso`
//! factories open the same database file in the same process safely?
//!
//! Turso 0.7.0 says "no multi-process support" — but that leaves the
//! multi-Database-per-process case ambiguous. ePHPm's Phase 2 primary
//! needs one factory serving the litewire wire frontends (with
//! `enable_cdc_on_connect(true)`) and a second factory for the CDC tail
//! loop; if the engine refuses, we need a different design.

use litewire_backend::{Backend, Value};
use litewire_turso::Turso;

#[tokio::test]
async fn two_factories_on_same_file_can_coexist() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_str().unwrap().to_string();

    // Factory A: the "wire frontend" role. Do some writes.
    let a = Turso::open(&path).await.expect("open A");
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
        .await
        .expect("create via A");
    a.execute("INSERT INTO t VALUES (1, 'a')", &[])
        .await
        .expect("insert via A");

    // Factory B: the "tail loop" role. Should see A's writes.
    let b = Turso::open(&path).await.expect("open B");
    let rs = b
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("query via B");
    assert_eq!(rs.rows.len(), 1, "B failed to see A's committed row");
    assert_eq!(rs.rows[0][0], Value::Integer(1));
    assert_eq!(rs.rows[0][1], Value::Text("a".into()));

    // Writes via B should also be visible via A (after commit).
    b.execute("INSERT INTO t VALUES (2, 'b')", &[])
        .await
        .expect("insert via B");
    let rs = a
        .query("SELECT COUNT(*) FROM t", &[])
        .await
        .expect("count via A");
    assert_eq!(rs.rows[0][0], Value::Integer(2));
}
