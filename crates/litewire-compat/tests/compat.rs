//! The gating compatibility test.
//!
//! Runs the whole corpus through litewire (over Turso) and, when
//! `COMPAT_MYSQL_URL` / `COMPAT_PG_URL` are set, against reference servers;
//! then fails the build on any regression (a corpus entry that fails and is
//! not baselined). Without reference servers it runs in coverage-only mode —
//! translation and execution are still gated — so `cargo test --workspace` on
//! a plain dev box passes.

use litewire_compat::{RunConfig, collect, run_suite};

fn init_tracing() {
    // Quiet by default; raise with RUST_LOG. `-- --nocapture` shows the report.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

#[tokio::test]
async fn compat_suite_gates_regressions() {
    init_tracing();

    let config = RunConfig::from_env();
    let corpus = collect();
    assert!(!corpus.is_empty(), "corpus must not be empty");

    let report = run_suite(&config, &corpus)
        .await
        .expect("suite failed to run (could not reach litewire or a configured reference server)");

    // Always print the report so CI logs show the category breakdown.
    println!("\n{}", report.summary());
    if !config.has_reference() {
        println!(
            "NOTE: no reference server configured (COMPAT_MYSQL_URL / COMPAT_PG_URL unset) — \
             result correctness was NOT checked; this run gated translation + execution only."
        );
    }

    if report.baseline_updated {
        println!("baseline regenerated — review the diff and commit it; not asserting this run");
        return;
    }

    let regressions = report.regressions();
    assert!(
        regressions.is_empty(),
        "{} compatibility regression(s) — a corpus entry failed and is not baselined. \
         Fix the regression, or if it is an accepted divergence run \
         COMPAT_UPDATE_BASELINE=1 and review the baseline diff. Failing entries:\n{}",
        regressions.len(),
        regressions
            .iter()
            .map(|r| format!("  {} [{}] {}", r.id, r.raw.category.as_str(), r.raw.detail))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// The corpus runner drives the text protocol; this independently exercises
/// the MySQL **binary / prepared-statement** protocol end to end against
/// litewire-over-Turso, so a prepared-path regression is also caught.
#[tokio::test]
async fn mysql_prepared_statement_roundtrip() {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use mysql_async::prelude::*;
    use mysql_async::{Conn, OptsBuilder};
    use tokio::net::TcpListener;

    let port = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let backend = litewire::Turso::memory().await.unwrap();
    let config = litewire::litewire_mysql::MysqlFrontendConfig {
        listen: addr,
        max_connections: 0,
        log_failed_sql: false,
    };
    let frontend = litewire::litewire_mysql::MysqlFrontend::new(config, Arc::new(backend));
    let _serve = tokio::spawn(async move {
        let _ = frontend.serve().await;
    });

    let opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some(""))
        .db_name(Some("test"));
    let mut conn = {
        let mut c = None;
        for i in 0..30 {
            match Conn::new(opts.clone()).await {
                Ok(conn) => {
                    c = Some(conn);
                    break;
                }
                Err(_) if i < 29 => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => panic!("connect: {e}"),
            }
        }
        c.unwrap()
    };

    conn.query_drop("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    // Prepared INSERT with bound params (binary protocol).
    let stmt = conn
        .prep("INSERT INTO t (id, name) VALUES (?, ?)")
        .await
        .unwrap();
    conn.exec_drop(&stmt, (1, "Alice")).await.unwrap();
    conn.exec_drop(&stmt, (2, "Bob")).await.unwrap();

    // Prepared SELECT with a bound param.
    let rows: Vec<(i64, String)> = conn
        .exec("SELECT id, name FROM t WHERE id > ? ORDER BY id", (0,))
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);

    drop(conn);
}
