//! Structured SQL-construct matrix.
//!
//! Each `c.*` call adds one corpus entry. Deterministic reads are diffed
//! against the reference ([`Diff::Rows`] / [`Diff::OrderedRows`]); DDL, DML
//! and non-deterministic reads are coverage-only ([`Diff::None`]).
//!
//! DML correctness is checked indirectly: the mutation runs in `setup` and the
//! measured statement is a `SELECT` that observes its effect, so the row diff
//! validates the write without depending on affected-row counts (which MySQL
//! and SQLite report differently).

use super::{Corpus, SEED_SHOP, SEED_USERS};
use crate::model::Diff;

#[allow(clippy::too_many_lines)]
pub(super) fn add(c: &mut Corpus<'_>) {
    ddl(c);
    dml(c);
    select_basic(c);
    joins(c);
    subqueries(c);
    set_ops(c);
    grouping(c);
    ordering_paging(c);
    cte_window(c);
    expressions(c);
    functions(c);
    quoting(c);
    transactions(c);
    metadata(c);
    known_gaps(c);
}

fn ddl(c: &mut Corpus<'_>) {
    c.both(
        "ddl",
        "create_table_types",
        &[],
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(255), body TEXT, qty INTEGER, \
         price DECIMAL(10,2), created TIMESTAMP)",
        Diff::None,
    );
    c.both(
        "ddl",
        "create_table_constraints",
        &[],
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL, slug TEXT UNIQUE, \
         score INTEGER DEFAULT 0)",
        Diff::None,
    );
    c.both(
        "ddl",
        "create_index",
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, age INTEGER)"],
        "CREATE INDEX idx_email ON t (email)",
        Diff::None,
    );
    c.both(
        "ddl",
        "create_unique_index",
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)"],
        "CREATE UNIQUE INDEX idx_email ON t (email)",
        Diff::None,
    );
    c.both(
        "ddl",
        "alter_add_column",
        &["CREATE TABLE t (id INTEGER PRIMARY KEY)"],
        "ALTER TABLE t ADD COLUMN nickname TEXT",
        Diff::None,
    );
    // MySQL inline index in CREATE TABLE (`KEY`), and ALTER ... ADD KEY.
    c.my(
        "ddl",
        "create_table_inline_key",
        &[],
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email VARCHAR(191), KEY email_idx (email))",
        Diff::None,
    );
    c.my(
        "ddl",
        "alter_add_index",
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, email VARCHAR(191))"],
        "ALTER TABLE t ADD KEY email_idx (email)",
        Diff::None,
    );
    // MySQL column-prefix index length (WordPress schema shape).
    c.my(
        "ddl",
        "index_prefix_length",
        &[],
        "CREATE TABLE t (id BIGINT PRIMARY KEY, meta_key VARCHAR(255), KEY meta_key (meta_key(191)))",
        Diff::None,
    );
    c.both(
        "ddl",
        "drop_table",
        &["CREATE TABLE t (id INTEGER PRIMARY KEY)"],
        "DROP TABLE t",
        Diff::None,
    );
}

fn dml(c: &mut Corpus<'_>) {
    // Multi-row insert, observed by count.
    c.both(
        "dml",
        "insert_multi_row",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t (id, n) VALUES (1, 10), (2, 20), (3, 30)",
        ],
        "SELECT COUNT(*) FROM t",
        Diff::Rows,
    );
    c.both(
        "dml",
        "update_expression",
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
            "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, 1)",
            "UPDATE users SET age = age + 1 WHERE id = 1",
        ],
        "SELECT age FROM users WHERE id = 1",
        Diff::Rows,
    );
    c.both(
        "dml",
        "delete_where",
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
            "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, 1)",
            "INSERT INTO users (id, name, age, active) VALUES (2, 'Bob', 25, 0)",
            "DELETE FROM users WHERE active = 0",
        ],
        "SELECT COUNT(*) FROM users",
        Diff::Rows,
    );
    c.both(
        "dml",
        "insert_select",
        &[
            "CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO a (id, n) VALUES (1, 5), (2, 6)",
            "INSERT INTO b SELECT id, n FROM a",
        ],
        "SELECT SUM(n) FROM b",
        Diff::Rows,
    );
    // MySQL upsert.
    c.my(
        "dml",
        "insert_on_duplicate_key",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t (id, n) VALUES (1, 10)",
            "INSERT INTO t (id, n) VALUES (1, 99) ON DUPLICATE KEY UPDATE n = 99",
        ],
        "SELECT n FROM t WHERE id = 1",
        Diff::Rows,
    );
    c.my(
        "dml",
        "replace_into",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t (id, n) VALUES (1, 10)",
            "REPLACE INTO t (id, n) VALUES (1, 50)",
        ],
        "SELECT n FROM t WHERE id = 1",
        Diff::Rows,
    );
    // PostgreSQL upsert.
    c.pg(
        "dml",
        "insert_on_conflict",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t (id, n) VALUES (1, 10)",
            "INSERT INTO t (id, n) VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET n = 99",
        ],
        "SELECT n FROM t WHERE id = 1",
        Diff::Rows,
    );
    // INSERT ... RETURNING is row-returning and must run on the query() path
    // exactly once. Before litewire#38 it was routed to execute(): the engine
    // wrote the row and then errored ("unexpected row during execution"), so a
    // caller that retries on error double-inserted. PostgreSQL supports
    // RETURNING natively (the result is diffed, deterministic because the id is
    // explicit); MySQL 8.4 does not, so the reference rejects it and the runner
    // records it as a coverage-only pass. Either way the measured check is that
    // litewire translates and runs it on the correct path.
    c.both(
        "dml",
        "insert_returning",
        &["CREATE TABLE rt (id INTEGER PRIMARY KEY, name TEXT)"],
        "INSERT INTO rt (id, name) VALUES (1, 'alice') RETURNING id",
        Diff::Rows,
    );
    // DELETE ... RETURNING, same routing requirement (PG-only: the clause is a
    // PostgreSQL extension and MySQL rejects it).
    c.pg(
        "dml",
        "delete_returning",
        &[
            "CREATE TABLE rt (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO rt (id, name) VALUES (1, 'alice')",
        ],
        "DELETE FROM rt WHERE id = 1 RETURNING id",
        Diff::Rows,
    );
}

fn select_basic(c: &mut Corpus<'_>) {
    c.both(
        "select",
        "literal_arithmetic",
        &[],
        "SELECT 1 + 2",
        Diff::Rows,
    );
    c.both(
        "select",
        "where_and_order",
        SEED_USERS,
        "SELECT id, name FROM users WHERE active = 1 ORDER BY id",
        Diff::OrderedRows,
    );
    c.both(
        "select",
        "where_in_list",
        SEED_USERS,
        "SELECT name FROM users WHERE id IN (1, 3) ORDER BY name",
        Diff::OrderedRows,
    );
    c.both(
        "select",
        "where_like",
        SEED_USERS,
        "SELECT name FROM users WHERE name LIKE 'A%' ORDER BY name",
        Diff::OrderedRows,
    );
    c.both(
        "select",
        "distinct",
        SEED_USERS,
        "SELECT DISTINCT age FROM users ORDER BY age",
        Diff::OrderedRows,
    );
    c.both(
        "select",
        "count_star",
        SEED_USERS,
        "SELECT COUNT(*) FROM users",
        Diff::Rows,
    );
    c.both(
        "select",
        "between",
        SEED_USERS,
        "SELECT name FROM users WHERE age BETWEEN 26 AND 34 ORDER BY name",
        Diff::OrderedRows,
    );
    c.both(
        "select",
        "is_null",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)",
            "INSERT INTO t (id, note) VALUES (1, 'x')",
            "INSERT INTO t (id, note) VALUES (2, NULL)",
        ],
        "SELECT id FROM t WHERE note IS NULL ORDER BY id",
        Diff::OrderedRows,
    );
}

fn joins(c: &mut Corpus<'_>) {
    c.both(
        "join",
        "inner",
        SEED_SHOP,
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id",
        Diff::OrderedRows,
    );
    // LEFT JOIN observed via COUNT to avoid cross-engine NULL-ordering diffs.
    c.both(
        "join",
        "left_count",
        SEED_SHOP,
        "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id \
         GROUP BY u.name ORDER BY u.name",
        Diff::OrderedRows,
    );
    c.both(
        "join",
        "cross_count",
        SEED_SHOP,
        "SELECT COUNT(*) FROM users CROSS JOIN orders",
        Diff::Rows,
    );
    // RIGHT JOIN — SQLite gained it in 3.39; Turso support is uncertain, so
    // this is a good probe (baseline absorbs an exec error if unsupported).
    c.both(
        "join",
        "right_count",
        SEED_SHOP,
        "SELECT COUNT(*) FROM orders o RIGHT JOIN users u ON u.id = o.user_id",
        Diff::Rows,
    );
    c.both(
        "join",
        "three_way",
        &[
            "CREATE TABLE a (id INTEGER PRIMARY KEY, b_id INTEGER)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, c_id INTEGER)",
            "CREATE TABLE cc (id INTEGER PRIMARY KEY, label TEXT)",
            "INSERT INTO a (id, b_id) VALUES (1, 1)",
            "INSERT INTO b (id, c_id) VALUES (1, 1)",
            "INSERT INTO cc (id, label) VALUES (1, 'leaf')",
        ],
        "SELECT cc.label FROM a JOIN b ON a.b_id = b.id JOIN cc ON b.c_id = cc.id",
        Diff::Rows,
    );
}

fn subqueries(c: &mut Corpus<'_>) {
    c.both(
        "subquery",
        "scalar",
        SEED_SHOP,
        "SELECT name FROM users WHERE id = (SELECT user_id FROM orders WHERE amount = 200)",
        Diff::Rows,
    );
    c.both(
        "subquery",
        "in",
        SEED_SHOP,
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY name",
        Diff::OrderedRows,
    );
    c.both(
        "subquery",
        "correlated_exists",
        SEED_SHOP,
        "SELECT name FROM users u WHERE EXISTS \
         (SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY name",
        Diff::OrderedRows,
    );
    c.both(
        "subquery",
        "derived_table",
        SEED_SHOP,
        "SELECT t.cnt FROM (SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id) t \
         ORDER BY t.cnt",
        Diff::OrderedRows,
    );
}

fn set_ops(c: &mut Corpus<'_>) {
    c.both(
        "setop",
        "union",
        SEED_USERS,
        "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 2 ORDER BY id",
        Diff::OrderedRows,
    );
    c.both(
        "setop",
        "union_all",
        SEED_USERS,
        "SELECT age FROM users WHERE id = 2 UNION ALL SELECT age FROM users WHERE id = 4 \
         ORDER BY age",
        Diff::OrderedRows,
    );
}

fn grouping(c: &mut Corpus<'_>) {
    c.both(
        "group",
        "group_by_count",
        SEED_USERS,
        "SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age",
        Diff::OrderedRows,
    );
    c.both(
        "group",
        "having",
        SEED_USERS,
        "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 1",
        Diff::Rows,
    );
    c.both(
        "group",
        "sum",
        SEED_SHOP,
        "SELECT SUM(amount) FROM orders",
        Diff::Rows,
    );
    c.both(
        "group",
        "avg",
        SEED_USERS,
        "SELECT AVG(age) FROM users",
        Diff::Rows,
    );
    c.both(
        "group",
        "min_max",
        SEED_USERS,
        "SELECT MIN(age), MAX(age) FROM users",
        Diff::Rows,
    );
}

fn ordering_paging(c: &mut Corpus<'_>) {
    c.both(
        "order",
        "order_by_expr_desc",
        SEED_USERS,
        "SELECT name FROM users ORDER BY age DESC, name",
        Diff::OrderedRows,
    );
    c.both(
        "order",
        "limit_offset",
        SEED_USERS,
        "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1",
        Diff::OrderedRows,
    );
    c.both(
        "order",
        "limit_only",
        SEED_USERS,
        "SELECT id FROM users ORDER BY id LIMIT 1",
        Diff::OrderedRows,
    );
}

fn cte_window(c: &mut Corpus<'_>) {
    c.both(
        "cte",
        "simple",
        SEED_USERS,
        "WITH au AS (SELECT id, name FROM users WHERE active = 1) \
         SELECT name FROM au ORDER BY name",
        Diff::OrderedRows,
    );
    // A CTE whose outer statement is a SELECT is a read. Its leading keyword is
    // WITH, not SELECT, so before litewire#38 it was mis-routed to execute()
    // and errored ("unexpected row during execution") on the Turso engine. The
    // aggregate outer SELECT keeps the result deterministic for the diff.
    c.both(
        "cte",
        "outer_select_count",
        SEED_USERS,
        "WITH au AS (SELECT id FROM users WHERE active = 1) \
         SELECT COUNT(*) AS n FROM au",
        Diff::Rows,
    );
    // Window functions: MySQL 8 / PG support them; the SQLite engine may not.
    c.both(
        "window",
        "row_number",
        SEED_USERS,
        "SELECT name, ROW_NUMBER() OVER (ORDER BY age, name) AS rn FROM users ORDER BY rn",
        Diff::OrderedRows,
    );
    c.both(
        "window",
        "count_over",
        SEED_USERS,
        "SELECT name, COUNT(*) OVER () AS total FROM users ORDER BY name",
        Diff::OrderedRows,
    );
}

fn expressions(c: &mut Corpus<'_>) {
    c.both(
        "expr",
        "case_when",
        SEED_USERS,
        "SELECT name, CASE WHEN age >= 30 THEN 'old' ELSE 'young' END AS grp FROM users \
         ORDER BY name",
        Diff::OrderedRows,
    );
    c.both(
        "expr",
        "coalesce",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)",
            "INSERT INTO t (id, note) VALUES (1, NULL)",
        ],
        "SELECT COALESCE(note, 'none') FROM t",
        Diff::Rows,
    );
    c.both("expr", "modulo_operator", &[], "SELECT 10 % 3", Diff::Rows);
    // Division: MySQL `/` is float (7/2 = 3.5); PostgreSQL and SQLite do
    // integer division on integer operands (3). A deterministic known
    // divergence — the MySQL row is expected to mismatch and is baselined.
    c.both("expr", "integer_division", &[], "SELECT 7 / 2", Diff::Rows);
    c.both(
        "expr",
        "boolean_literal",
        &[],
        "SELECT (1 = 1) AS yes",
        Diff::Rows,
    );
}

fn functions(c: &mut Corpus<'_>) {
    c.both("func", "abs", &[], "SELECT ABS(-5)", Diff::Rows);
    c.both("func", "round", &[], "SELECT ROUND(3.14159, 2)", Diff::Rows);
    c.both("func", "length", &[], "SELECT LENGTH('hello')", Diff::Rows);
    c.both("func", "upper", &[], "SELECT UPPER('abc')", Diff::Rows);
    c.both("func", "lower", &[], "SELECT LOWER('ABC')", Diff::Rows);
    c.both(
        "func",
        "substr",
        &[],
        "SELECT SUBSTR('hello', 2, 3)",
        Diff::Rows,
    );
    // String concatenation is dialect-specific.
    c.my(
        "func",
        "concat",
        SEED_USERS,
        "SELECT CONCAT(name, '!') FROM users ORDER BY name",
        Diff::OrderedRows,
    );
    c.pg(
        "func",
        "concat_operator",
        SEED_USERS,
        "SELECT name || '!' FROM users ORDER BY name",
        Diff::OrderedRows,
    );
    // Deterministic date handling, dialect-specific.
    c.my(
        "func",
        "date_extract",
        &[],
        "SELECT DATE('2024-01-15 10:20:30')",
        Diff::Rows,
    );
    c.pg(
        "func",
        "date_cast",
        &[],
        "SELECT CAST('2024-01-15' AS DATE)",
        Diff::Rows,
    );
    // Non-deterministic: coverage only (translate + execute, never diffed).
    c.both("func", "now", &[], "SELECT NOW()", Diff::None);
    c.my("func", "rand", &[], "SELECT RAND()", Diff::None);
    c.pg("func", "random", &[], "SELECT RANDOM()", Diff::None);
}

fn quoting(c: &mut Corpus<'_>) {
    c.my(
        "quote",
        "backtick_identifiers",
        SEED_USERS,
        "SELECT `id`, `name` FROM `users` ORDER BY `id`",
        Diff::OrderedRows,
    );
    c.pg(
        "quote",
        "double_quote_identifiers",
        SEED_USERS,
        "SELECT \"id\", \"name\" FROM \"users\" ORDER BY \"id\"",
        Diff::OrderedRows,
    );
    c.both(
        "quote",
        "single_quote_escape",
        &[],
        "SELECT 'it''s here' AS s",
        Diff::Rows,
    );
    // MySQL treats a double-quoted literal as a string (default sql_mode);
    // behavior under translation is a useful probe.
    c.my(
        "quote",
        "double_quote_string",
        &[],
        "SELECT \"hello\" AS s",
        Diff::Rows,
    );
    // Backslash-before-quote inside a literal: the mysqldump fidelity case
    // (SQLite has no backslash escaping).
    c.my(
        "quote",
        "backslash_quote_literal",
        &[],
        "SELECT 'a\\'b' AS s",
        Diff::Rows,
    );
}

fn transactions(c: &mut Corpus<'_>) {
    c.both(
        "txn",
        "commit_persists",
        &[
            "CREATE TABLE tx (id INTEGER PRIMARY KEY)",
            "BEGIN",
            "INSERT INTO tx (id) VALUES (1)",
            "COMMIT",
        ],
        "SELECT COUNT(*) FROM tx",
        Diff::Rows,
    );
    c.both(
        "txn",
        "rollback_discards",
        &[
            "CREATE TABLE tx (id INTEGER PRIMARY KEY)",
            "INSERT INTO tx (id) VALUES (1)",
            "BEGIN",
            "INSERT INTO tx (id) VALUES (2)",
            "ROLLBACK",
        ],
        "SELECT COUNT(*) FROM tx",
        Diff::Rows,
    );
    c.my(
        "txn",
        "start_transaction",
        &[
            "CREATE TABLE tx (id INTEGER PRIMARY KEY)",
            "START TRANSACTION",
            "INSERT INTO tx (id) VALUES (1)",
            "COMMIT",
        ],
        "SELECT COUNT(*) FROM tx",
        Diff::Rows,
    );
}

fn metadata(c: &mut Corpus<'_>) {
    // Metadata / introspection queries used at connection setup and by ORMs.
    // Coverage-only: row shapes legitimately differ from a real server.
    c.my("meta", "show_tables", SEED_USERS, "SHOW TABLES", Diff::None);
    c.my("meta", "describe", SEED_USERS, "DESCRIBE users", Diff::None);
    c.my(
        "meta",
        "information_schema_tables",
        SEED_USERS,
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'users'",
        Diff::None,
    );
    c.my(
        "meta",
        "select_version",
        &[],
        "SELECT VERSION()",
        Diff::None,
    );
    c.pg(
        "meta",
        "information_schema_columns",
        SEED_USERS,
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'users'",
        Diff::None,
    );
}

/// Constructs expected to be unsupported or to diverge — present so the gate
/// notices if their status *changes* (e.g. a translation gap gets fixed).
fn known_gaps(c: &mut Corpus<'_>) {
    // Index-level COMMENT inside CREATE TABLE: sqlparser 0.57 fails to parse
    // (translation error).
    c.my(
        "gap",
        "index_comment_in_create",
        &[],
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email VARCHAR(191), \
         KEY email_idx (email) COMMENT 'lookup')",
        Diff::None,
    );
    // Index hints — litewire must strip FORCE/USE INDEX for SQLite.
    c.my(
        "gap",
        "force_index_hint",
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER)",
            "INSERT INTO users (id, age) VALUES (1, 25), (2, 30), (4, 25)",
            "CREATE INDEX idx_age ON users (age)",
        ],
        "SELECT id FROM users FORCE INDEX (idx_age) WHERE age = 25 ORDER BY id",
        Diff::OrderedRows,
    );
    c.my(
        "gap",
        "use_index_hint",
        &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER)",
            "INSERT INTO users (id, age) VALUES (1, 25), (2, 30)",
            "CREATE INDEX idx_age ON users (age)",
        ],
        "SELECT id FROM users USE INDEX (idx_age) WHERE age = 30 ORDER BY id",
        Diff::OrderedRows,
    );
    // Row locking clause — meaningless to SQLite; probe how it's handled.
    c.both(
        "gap",
        "select_for_update",
        SEED_USERS,
        "SELECT id FROM users WHERE id = 1 FOR UPDATE",
        Diff::None,
    );
}
