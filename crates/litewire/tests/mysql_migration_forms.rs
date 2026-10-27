//! End-to-end: the `ALTER TABLE` and DML forms a self-migrating PHP
//! application's updater uses, checked by their effect (schema as
//! `SHOW COLUMNS` / `SHOW INDEX` report it, rows, affected-row counts).
//!
//! Runs against the rusqlite backend, and against Turso with `--features
//! turso`.

use std::net::SocketAddr;

use litewire::backend::SharedBackend;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Row};
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
        .db_name(Some("app"))
        .into();
    for _ in 0..40 {
        if let Ok(c) = Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("litewire did not come up");
}

async fn exec(c: &mut Conn, sql: &str) -> u64 {
    c.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.affected_rows()
}

async fn col(c: &mut Conn, sql: &str, name: &str) -> Vec<String> {
    let rows: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter()
        .map(|r| {
            r.get_opt::<Option<String>, _>(name)
                .unwrap_or_else(|| panic!("{sql}: no column {name}"))
                .unwrap()
                .unwrap_or_else(|| "NULL".into())
        })
        .collect()
}

async fn ints(c: &mut Conn, sql: &str) -> Vec<i64> {
    c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn forms(backend: SharedBackend) {
    let mut c = serve(backend).await;
    exec(
        &mut c,
        "CREATE TABLE apps (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), smtp_password VARCHAR(100), tag VARCHAR(10))",
    )
    .await;
    exec(&mut c, "CREATE INDEX tag_idx ON apps (tag)").await;
    exec(
        &mut c,
        "INSERT INTO apps (name, tag) VALUES ('a', 't1'), ('b', 't2'), ('c', '07')",
    )
    .await;

    // `||` is OR in MySQL: the probe must find the existing column.
    assert_eq!(
        col(
            &mut c,
            "SHOW COLUMNS FROM apps WHERE Field = 'nope' || Field = 'tag'",
            "Field"
        )
        .await,
        ["tag"]
    );

    // Parenthesized multi-add, AFTER / FIRST positions, several ops at once.
    exec(
        &mut c,
        "ALTER TABLE apps ADD (x INT (11) DEFAULT '0', y VARCHAR (100))",
    )
    .await;
    exec(
        &mut c,
        "ALTER TABLE apps ADD openai_key VARCHAR(500) DEFAULT NULL AFTER smtp_password, ADD COLUMN flag INT(1) DEFAULT 1 FIRST",
    )
    .await;
    assert_eq!(
        col(&mut c, "SHOW COLUMNS FROM apps", "Field").await,
        [
            "flag",
            "id",
            "name",
            "smtp_password",
            "openai_key",
            "tag",
            "x",
            "y"
        ]
    );
    // Data and keys survive the rebuild; AUTO_INCREMENT continues.
    assert_eq!(
        col(&mut c, "SELECT name FROM apps ORDER BY id", "name").await,
        ["a", "b", "c"]
    );
    assert_eq!(
        col(&mut c, "SELECT flag FROM apps WHERE id = 1", "flag").await,
        ["1"]
    );
    assert_eq!(
        col(&mut c, "SHOW INDEX FROM apps", "Key_name").await,
        ["PRIMARY", "tag_idx"]
    );
    exec(&mut c, "INSERT INTO apps (name) VALUES ('d')").await;
    assert_eq!(c.last_insert_id(), Some(4));

    // MODIFY changes type (affinity re-applied to existing rows), NOT NULL
    // and DEFAULT; CHANGE renames (index follows); ALTER ... SET DEFAULT.
    // A NULL in the column makes NOT NULL fail, as in MySQL's strict mode.
    assert!(
        c.query_drop("ALTER TABLE apps MODIFY COLUMN tag INT(11) NOT NULL")
            .await
            .is_err()
    );
    assert_eq!(
        col(&mut c, "SHOW COLUMNS FROM apps WHERE Field = 'tag'", "Null").await,
        ["YES"]
    );
    exec(&mut c, "UPDATE apps SET tag = '0' WHERE tag IS NULL").await;
    exec(
        &mut c,
        "ALTER TABLE apps MODIFY COLUMN tag INT(11) NOT NULL DEFAULT 5",
    )
    .await;
    let rows: Vec<Row> = c
        .query("SHOW COLUMNS FROM apps WHERE Field = 'tag'")
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("Type").unwrap(), "bigint");
    assert_eq!(rows[0].get::<String, _>("Null").unwrap(), "NO");
    assert_eq!(
        ints(
            &mut c,
            "SELECT typeof(tag) = 'integer' FROM apps WHERE id = 3"
        )
        .await,
        [1]
    );
    assert_eq!(ints(&mut c, "SELECT tag FROM apps WHERE id = 3").await, [7]);
    exec(&mut c, "ALTER TABLE apps CHANGE `tag` tag_id INT(11)").await;
    assert_eq!(
        col(
            &mut c,
            "SHOW INDEX FROM apps WHERE Key_name = 'tag_idx'",
            "Column_name"
        )
        .await,
        ["tag_id"]
    );
    exec(&mut c, "ALTER TABLE apps ALTER x SET DEFAULT 9").await;
    exec(&mut c, "INSERT INTO apps (name) VALUES ('e')").await;
    assert_eq!(
        ints(&mut c, "SELECT x FROM apps WHERE name = 'e'").await,
        [9]
    );
    exec(&mut c, "ALTER TABLE apps DROP y, DROP COLUMN openai_key").await;
    assert_eq!(
        col(&mut c, "SHOW COLUMNS FROM apps", "Field").await,
        ["flag", "id", "name", "smtp_password", "tag_id", "x"]
    );
    // Inside a client transaction the rebuild uses a savepoint.
    exec(&mut c, "START TRANSACTION").await;
    exec(
        &mut c,
        "ALTER TABLE apps MODIFY name VARCHAR(200) NOT NULL DEFAULT ''",
    )
    .await;
    exec(&mut c, "COMMIT").await;

    // UPDATE / DELETE ... LIMIT.
    assert_eq!(exec(&mut c, "UPDATE apps SET x = 1 LIMIT 2").await, 2);
    assert_eq!(
        exec(
            &mut c,
            "DELETE FROM apps WHERE x = 1 ORDER BY id DESC LIMIT 1"
        )
        .await,
        1
    );

    // INSERT IGNORE / INSERT ... SELECT IGNORE: duplicates skipped, 0 rows.
    exec(
        &mut c,
        "CREATE TABLE d (a INT, b INT, UNIQUE KEY ab (a, b))",
    )
    .await;
    assert_eq!(
        exec(&mut c, "INSERT IGNORE INTO d VALUES (1, 1), (1, 2)").await,
        2
    );
    assert_eq!(exec(&mut c, "INSERT IGNORE INTO d VALUES (1, 1)").await, 0);
    assert_eq!(
        exec(&mut c, "INSERT IGNORE INTO d (a, b) SELECT 1, 2").await,
        0
    );

    // Multi-table DELETE (dedup keeping the lowest id).
    exec(&mut c, "CREATE TABLE s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, email VARCHAR(50), list INT, unsub INT DEFAULT 0, last INT DEFAULT 0)").await;
    exec(&mut c, "INSERT INTO s (email, list, unsub) VALUES ('a', 1, 0), ('a', 1, 1), ('b', 1, 0), ('a', 2, 0)").await;
    // UPDATE ... JOIN (derived table) runs before the delete.
    assert_eq!(
        exec(
            &mut c,
            "UPDATE s k INNER JOIN (SELECT MIN(id) AS keep_id, MAX(unsub) AS unsub FROM s GROUP BY email, list HAVING COUNT(*) > 1) d ON k.id = d.keep_id SET k.unsub = IF(d.unsub = 1, 1, 0)",
        )
        .await,
        1
    );
    assert_eq!(
        exec(&mut c, "DELETE s1 FROM s s1 INNER JOIN s s2 ON s1.email = s2.email AND s1.list = s2.list AND s1.id > s2.id").await,
        1
    );
    assert_eq!(
        ints(&mut c, "SELECT unsub FROM s ORDER BY id").await,
        [1, 0, 0]
    );
    // UPDATE with a JOIN to another table; comma UPDATE of two tables.
    exec(
        &mut c,
        "CREATE TABLE l (id INT PRIMARY KEY, app INT, recipients INT DEFAULT 0)",
    )
    .await;
    exec(&mut c, "INSERT INTO l (id, app) VALUES (1, 7), (2, 8)").await;
    assert_eq!(
        exec(
            &mut c,
            "UPDATE s INNER JOIN l ON l.id = s.list SET s.last = 5 WHERE l.app = 7"
        )
        .await,
        2
    );
    exec(
        &mut c,
        "UPDATE l, s SET l.recipients = l.recipients+1, s.last = 9 WHERE l.id = 2 AND s.id = 4",
    )
    .await;
    assert_eq!(
        ints(&mut c, "SELECT recipients FROM l ORDER BY id").await,
        [0, 1]
    );
    assert_eq!(
        ints(&mut c, "SELECT last FROM s ORDER BY id").await,
        [5, 5, 9]
    );

    // Functions.
    assert_eq!(
        col(&mut c, "SELECT SUBSTRING_INDEX('3 days', ' ', 1) AS a, SUBSTRING_INDEX('3 days', ' ', -1) AS b", "b").await,
        ["days"]
    );
    assert_eq!(ints(&mut c, "SELECT FIND_IN_SET('b', 'a,b,c')").await, [2]);
    assert_eq!(ints(&mut c, "SELECT FIND_IN_SET('z', 'a,b,c')").await, [0]);
    assert_eq!(
        ints(
            &mut c,
            "SELECT UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY))"
        )
        .await,
        [86_400]
    );
    assert_eq!(
        ints(&mut c, "SELECT UNIX_TIMESTAMP() > 1700000000").await,
        [1]
    );
    assert_eq!(
        col(&mut c, "SELECT FROM_UNIXTIME(1700000000) AS t", "t").await,
        ["2023-11-14 22:13:20"]
    );
    // COMMIT / ROLLBACK outside a transaction are OK, as in MySQL.
    exec(&mut c, "ROLLBACK").await;
    exec(&mut c, "COMMIT").await;
    // TRUNCATE empties the table; ids start over.
    exec(&mut c, "TRUNCATE TABLE d").await;
    assert_eq!(ints(&mut c, "SELECT COUNT(*) FROM d").await, [0]);
    // Parenthesized UNION members.
    assert_eq!(
        ints(
            &mut c,
            "(SELECT id FROM s WHERE id = 1) UNION (SELECT id FROM l WHERE id = 2) ORDER BY 1"
        )
        .await,
        [1, 2]
    );
    // SHOW TABLE STATUS.
    assert_eq!(
        col(&mut c, "SHOW TABLE STATUS LIKE 'apps'", "Collation").await,
        ["utf8mb4_unicode_ci"]
    );
}

#[tokio::test]
async fn migration_forms_rusqlite() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::backend::Rusqlite::memory().unwrap());
    forms(backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn migration_forms_turso() {
    let backend: SharedBackend =
        std::sync::Arc::new(litewire::litewire_turso::Turso::memory().await.unwrap());
    forms(backend).await;
}
