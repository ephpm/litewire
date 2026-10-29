//! End-to-end `mysqldump`-replay fidelity tests, driven over the MySQL wire
//! protocol so they assert what the database actually stores — not what SQL
//! string the translator emitted.
//!
//! Two regressions are guarded here:
//!
//!  * **String-literal escaping.** MySQL's default string syntax uses
//!    backslash escapes (`\'` is a quote, `\\` a backslash); SQLite/Turso
//!    never interpret backslashes. A value written in a dump as `'a\\\'b'`
//!    (bytes `a \ ' b`) used to be re-emitted as `'a\'b'`, which SQLite reads
//!    as `a\` followed by stray tokens — so inside a multi-row `INSERT` the
//!    row was silently dropped. See `litewire-translate::emit`.
//!
//!  * **`USING BTREE` / `USING HASH` index hints.** Stock dumps decorate key
//!    definitions with a storage-engine index method that is meaningless to
//!    SQLite. litewire parses and ignores them; this test keeps it that way.
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`.

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use std::net::SocketAddr;
use tokio::net::TcpListener;

async fn serve(backend: SharedBackend) -> Conn {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig {
        listen: addr,
        max_connections: 0,
    };
    let frontend = litewire::litewire_mysql::MysqlFrontend::new(config, backend);
    tokio::spawn(async move {
        frontend.serve().await.unwrap();
    });
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some(""))
        .db_name(Some("test"))
        .into();
    for _ in 0..40 {
        if let Ok(c) = Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("litewire did not come up");
}

/// Each case is `(sql_source_literal, expected_decoded_bytes)`. The left side
/// is written exactly as it would appear in a `mysqldump` (a raw string, so
/// the backslashes are the literal bytes the server receives); the right side
/// is the value the row must contain once SQLite has parsed it.
const ESCAPE_CASES: &[(&str, &str)] = &[
    // `\\` then `\'`: an escaped backslash immediately followed by an escaped
    // quote. This is the exact shape that dropped 45 Sendy `campaigns` rows.
    (r"'a\\\'b'", "a\\'b"),
    // A lone escaped backslash.
    (r"'a\\b'", "a\\b"),
    // Two escaped backslashes (no quote involved).
    (r"'a\\\\b'", "a\\\\b"),
    // Doubled quote — the SQL-standard escape MySQL also accepts.
    (r"'a''b'", "a'b"),
    // A single escaped quote.
    (r"'a\'b'", "a'b"),
    // Quote at the very end, right after a backslash.
    (r"'a\\\''", "a\\'"),
    // Backslash run then a doubled quote.
    (r"'\\''x'", "\\'x"),
];

async fn string_escaping(backend: SharedBackend) {
    let mut c = serve(backend).await;
    c.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .await
        .unwrap();

    for (i, (literal, expected)) in ESCAPE_CASES.iter().enumerate() {
        let id = i as i64 + 1;
        // Sent verbatim as a COM_QUERY text statement — exactly how a dump is
        // replayed. A bound parameter would bypass the literal lexer entirely.
        let sql = format!("INSERT INTO t (id, v) VALUES ({id}, {literal})");
        c.query_drop(&sql)
            .await
            .unwrap_or_else(|e| panic!("insert {literal} failed: {e}"));

        let got: Option<String> = c
            .query_first(format!("SELECT v FROM t WHERE id = {id}"))
            .await
            .unwrap();
        assert_eq!(
            got.as_deref(),
            Some(*expected),
            "literal {literal} decoded wrong",
        );
    }

    // Every case inserted exactly one row — nothing was dropped or split.
    let count: Option<i64> = c.query_first("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(count, Some(ESCAPE_CASES.len() as i64), "a row was dropped");

    // And the specific multi-row INSERT shape that first exposed the bug: the
    // `\\\'` row must not swallow the rows that follow it in the same tuple
    // list.
    c.query_drop("CREATE TABLE m (v TEXT)").await.unwrap();
    c.query_drop(r"INSERT INTO m (v) VALUES ('a\\\'b'),('second'),('third')")
        .await
        .unwrap();
    let rows: Vec<String> = c.query("SELECT v FROM m ORDER BY v").await.unwrap();
    assert_eq!(
        rows,
        vec!["a\\'b".to_string(), "second".into(), "third".into()]
    );
}

async fn using_index_hints(backend: SharedBackend) {
    let mut c = serve(backend).await;

    // `USING BTREE` on a plain key, `USING HASH` on a unique key, plus one on
    // the PRIMARY KEY — every spot a stock dump puts the hint.
    c.query_drop(
        "CREATE TABLE `items` (\
            `id` int NOT NULL AUTO_INCREMENT, \
            `sku` varchar(64) NOT NULL, \
            `name` varchar(255), \
            PRIMARY KEY (`id`) USING BTREE, \
            UNIQUE KEY `uq_sku` (`sku`) USING HASH, \
            KEY `idx_name` (`name`) USING BTREE\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    .await
    .expect("CREATE TABLE with USING hints must load");

    // The table is usable.
    c.query_drop("INSERT INTO items (sku, name) VALUES ('A-1', 'alpha'), ('B-2', 'beta')")
        .await
        .unwrap();
    let names: Vec<String> = c
        .query("SELECT name FROM items ORDER BY name")
        .await
        .unwrap();
    assert_eq!(names, vec!["alpha".to_string(), "beta".into()]);

    // The UNIQUE KEY ... USING HASH really became a unique index: a duplicate
    // `sku` is rejected.
    let dup = c
        .query_drop("INSERT INTO items (sku, name) VALUES ('A-1', 'again')")
        .await;
    assert!(dup.is_err(), "unique index from `USING HASH` not enforced");
}

#[tokio::test]
async fn string_escaping_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    string_escaping(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn string_escaping_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    string_escaping(backend).await;
}

#[tokio::test]
async fn using_index_hints_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    using_index_hints(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn using_index_hints_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    using_index_hints(backend).await;
}
