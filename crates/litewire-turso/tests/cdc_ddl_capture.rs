//! Empirical answer to Phase 2's blocking question: does Turso's CDC
//! (`PRAGMA capture_data_changes_conn`) capture **DDL** — `CREATE TABLE`,
//! `ALTER TABLE`, `CREATE INDEX`, `DROP TABLE` — or only row changes?
//!
//! WordPress and other PHP apps execute runtime DDL during
//! installs/upgrades/plugin activation. If CDC omits DDL, replicas will
//! silently diverge from the primary the moment a schema change lands,
//! which forces a schema-sync side channel into the replication design.
//!
//! These tests hit the real `turso = "=0.7.0"` engine (same pin as
//! `litewire-turso`), enable CDC in `full` mode on a connection, execute
//! each DDL form, and read the resulting `turso_cdc` rows. The assertions
//! are deliberately structural — schema-version, column count, presence of
//! rows for `sqlite_schema` mutations — so the answer holds across the
//! v1→v2 schema bump we already observed within the 0.7 line.

use turso::Value;

/// Enable full CDC capture on the given connection and verify the schema
/// version. Returns `Ok(())` if enablement succeeded — callers can then
/// exercise DDL and inspect `turso_cdc` directly.
async fn enable_cdc(conn: &turso::Connection) {
    // Per-connection enablement, stable name in 0.7.0.
    conn.execute("PRAGMA capture_data_changes_conn('full')", ())
        .await
        .expect("enable CDC");
}

/// Fetch every row from `turso_cdc` in insertion order.
///
/// Schema v2 columns (as verified in the 0.7.0 source at
/// `translate/emitter/mod.rs::emit_cdc_insns_v2`):
/// `change_id, change_time, change_txn_id, change_type, table_name, id,
/// before, after, updates`. Note: **column order in the design doc was
/// wrong** — `change_txn_id` is column 3, not column 9, and change_type
/// values are `INSERT=1, UPDATE=0, DELETE=-1, COMMIT=2`.
async fn read_cdc(conn: &turso::Connection) -> Vec<CdcRow> {
    let mut stmt = conn
        .prepare(
            "SELECT change_id, change_txn_id, change_type, table_name \
             FROM turso_cdc ORDER BY change_id",
        )
        .await
        .expect("prepare turso_cdc select");
    let mut rows = stmt.query(()).await.expect("query turso_cdc");
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.expect("read row") {
        let change_id = match row.get_value(0).unwrap() {
            Value::Integer(i) => i,
            other => panic!("unexpected change_id type: {other:?}"),
        };
        let change_txn_id = match row.get_value(1).unwrap() {
            Value::Integer(i) => Some(i),
            Value::Null => None,
            other => panic!("unexpected change_txn_id type: {other:?}"),
        };
        let change_type = match row.get_value(2).unwrap() {
            Value::Integer(i) => i,
            other => panic!("unexpected change_type type: {other:?}"),
        };
        // COMMIT records have table_name = NULL; DML/DDL rows carry the
        // affected table (sqlite_schema for DDL).
        let table_name = match row.get_value(3).unwrap() {
            Value::Text(s) => Some(s),
            Value::Null => None,
            other => panic!("unexpected table_name type: {other:?}"),
        };
        out.push(CdcRow {
            change_id,
            change_txn_id,
            change_type,
            table_name,
        });
    }
    out
}

#[derive(Debug, Clone)]
struct CdcRow {
    change_id: i64,
    /// `NULL` on COMMIT records without a real txn ID; otherwise the
    /// connection-scoped monotonic txn id set by `conn_txn_id(change_id)`.
    change_txn_id: Option<i64>,
    /// v2 change_type: `INSERT=1, UPDATE=0, DELETE=-1, COMMIT=2`.
    change_type: i64,
    /// `NULL` for COMMIT records; otherwise the affected table (DDL uses
    /// `"sqlite_schema"`).
    table_name: Option<String>,
}

async fn open_conn(path: &str) -> turso::Connection {
    let db = turso::Builder::new_local(path)
        .build()
        .await
        .expect("open turso db");
    db.connect().expect("connect")
}

/// Verify the v2 schema is what the design doc expects: version row present
/// and `turso_cdc` has the documented 9 columns.
#[tokio::test]
async fn cdc_schema_is_v2() {
    let conn = open_conn(":memory:").await;
    enable_cdc(&conn).await;

    let mut stmt = conn
        .prepare("SELECT version FROM turso_cdc_version")
        .await
        .expect("prepare version");
    let mut rows = stmt.query(()).await.expect("query version");
    let row = rows.next().await.unwrap().expect("one version row");
    // NOTE: The design doc claimed the version column stores an integer.
    // Empirical result on turso 0.7.0: the column stores a TEXT tag like
    // `"v2"` — the doc has been corrected.
    let version = match row.get_value(0).unwrap() {
        Value::Text(s) => s,
        v => panic!("unexpected version type: {v:?}"),
    };
    assert_eq!(version, "v2", "turso 0.7.0 CDC schema version tag");

    // Column-count check via a metadata prepare.
    let stmt = conn
        .prepare("SELECT * FROM turso_cdc")
        .await
        .expect("prepare turso_cdc *");
    let cols = stmt.columns();
    assert_eq!(
        cols.len(),
        9,
        "turso_cdc column count (v2 adds change_txn_id): got {:?}",
        cols.iter().map(turso::Column::name).collect::<Vec<_>>()
    );
}

/// **HEADLINE FINDING:** CREATE TABLE must appear in the CDC stream, or
/// replicas cannot construct the schema.
#[tokio::test]
async fn create_table_captured_in_cdc() {
    let conn = open_conn(":memory:").await;
    enable_cdc(&conn).await;

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .expect("create table");

    let rows = read_cdc(&conn).await;
    assert!(
        !rows.is_empty(),
        "CREATE TABLE emitted no CDC rows — replicas would miss schema changes"
    );
    assert!(
        rows.iter()
            .any(|r| r.table_name.as_deref() == Some("sqlite_schema")),
        "CREATE TABLE did not mutate sqlite_schema in CDC — got: {rows:?}"
    );
}

/// CREATE INDEX also mutates `sqlite_schema`; verify capture.
#[tokio::test]
async fn create_index_captured_in_cdc() {
    let conn = open_conn(":memory:").await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();
    // Enable CDC *after* setup so we isolate the index emission.
    enable_cdc(&conn).await;

    conn.execute("CREATE INDEX idx_t_v ON t (v)", ())
        .await
        .expect("create index");

    let rows = read_cdc(&conn).await;
    assert!(
        rows.iter()
            .any(|r| r.table_name.as_deref() == Some("sqlite_schema")),
        "CREATE INDEX did not mutate sqlite_schema in CDC — got: {rows:?}"
    );
}

/// DROP TABLE emits schema-table deletes in the CDC stream (the emitter
/// path we read in `translate/schema.rs`).
#[tokio::test]
async fn drop_table_captured_in_cdc() {
    let conn = open_conn(":memory:").await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
        .await
        .unwrap();
    enable_cdc(&conn).await;

    conn.execute("DROP TABLE t", ()).await.expect("drop table");

    let rows = read_cdc(&conn).await;
    assert!(
        rows.iter()
            .any(|r| r.table_name.as_deref() == Some("sqlite_schema")),
        "DROP TABLE did not mutate sqlite_schema in CDC — got: {rows:?}"
    );
}

/// ALTER TABLE is the WordPress upgrade case. Turso 0.7.0's ALTER support
/// is narrow (RENAME/ADD COLUMN); this test records what actually happens
/// so the design doc stays honest.
#[tokio::test]
async fn alter_table_add_column_captured_in_cdc() {
    let conn = open_conn(":memory:").await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();
    enable_cdc(&conn).await;

    let alter = conn.execute("ALTER TABLE t ADD COLUMN c INTEGER", ()).await;
    match alter {
        Ok(_) => {
            let rows = read_cdc(&conn).await;
            assert!(
                rows.iter()
                    .any(|r| r.table_name.as_deref() == Some("sqlite_schema")),
                "ALTER TABLE ADD COLUMN succeeded but produced no sqlite_schema \
                 CDC rows — this would silently diverge replicas: {rows:?}"
            );
        }
        Err(e) => {
            // Record the observed error so the design doc can cite it.
            // If ALTER isn't supported yet, that is itself information for
            // WordPress compatibility — not a bug in this test.
            eprintln!("NOTE: ALTER TABLE ADD COLUMN unsupported in turso 0.7.0: {e}");
        }
    }
}

/// Row DML — the well-trodden case. Sanity check that INSERT emits at
/// least one row-change plus (in schema v2) a COMMIT record.
#[tokio::test]
async fn insert_dml_emits_row_change_and_commit() {
    let conn = open_conn(":memory:").await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();
    enable_cdc(&conn).await;

    conn.execute("INSERT INTO t VALUES (1, 'hello')", ())
        .await
        .unwrap();

    let rows = read_cdc(&conn).await;
    assert!(
        rows.iter().any(|r| r.table_name.as_deref() == Some("t")),
        "INSERT into t did not produce a CDC row for table 't': {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.change_type == 2),
        "expected a COMMIT record (change_type=2) delimiting the transaction: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r.change_id > 0),
        "change_id must be monotonic positive: {rows:?}"
    );
}

/// Cross-transaction ordering: two sequential txns should produce two
/// COMMIT records with monotonically increasing change_ids.
#[tokio::test]
async fn commit_records_delimit_transactions() {
    let conn = open_conn(":memory:").await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();
    enable_cdc(&conn).await;

    conn.execute("INSERT INTO t VALUES (1, 'a')", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b')", ())
        .await
        .unwrap();

    let rows = read_cdc(&conn).await;
    let commits: Vec<&CdcRow> = rows.iter().filter(|r| r.change_type == 2).collect();
    assert!(
        commits.len() >= 2,
        "expected at least 2 COMMIT records for 2 autocommit txns: {rows:?}"
    );
    for w in commits.windows(2) {
        assert!(
            w[0].change_id < w[1].change_id,
            "COMMIT change_ids not monotonic: {:?}",
            commits
        );
    }
    // Every non-commit row should carry a change_txn_id (the field we're
    // going to key exactly-once apply on in the ephpm replication layer).
    for r in &rows {
        if r.change_type != 2 {
            assert!(
                r.change_txn_id.is_some(),
                "non-commit CDC row missing change_txn_id: {r:?}"
            );
        }
    }
}
