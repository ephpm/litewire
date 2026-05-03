//! Emit SQLite-compatible SQL from a rewritten AST.
//!
//! Uses sqlparser's `Display` implementation which produces standard SQL.
//! SQLite is largely standard-compliant, so this works for most cases.

use sqlparser::ast::Statement;

/// Emit a statement as a SQL string.
#[must_use]
pub fn emit_statement(stmt: &Statement) -> String {
    stmt.to_string()
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
    use sqlparser::parser::Parser;

    use super::*;

    #[test]
    fn roundtrip_simple_select() {
        let sql = "SELECT 1 + 2";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("1 + 2"));
    }

    #[test]
    fn roundtrip_select_with_where() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("users"));
        assert!(emitted.contains("id = 1"));
    }

    #[test]
    fn roundtrip_insert() {
        let sql = "INSERT INTO users (name) VALUES ('Alice')";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("Alice"));
        assert!(emitted.to_ascii_uppercase().contains("INSERT"));
    }

    #[test]
    fn roundtrip_create_table() {
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.to_ascii_uppercase().contains("CREATE TABLE"));
        assert!(emitted.contains("id"));
        assert!(emitted.contains("name"));
    }

    #[test]
    fn roundtrip_update() {
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("Bob"));
        assert!(emitted.contains("id = 1"));
    }

    #[test]
    fn roundtrip_delete() {
        let sql = "DELETE FROM users WHERE id = 1";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.to_ascii_uppercase().contains("DELETE"));
    }

    #[test]
    fn roundtrip_select_with_join() {
        let sql = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("JOIN"));
        assert!(emitted.contains("u.id = o.user_id"));
    }

    #[test]
    fn roundtrip_pg_select() {
        let sql = "SELECT * FROM users WHERE active = true ORDER BY name LIMIT 10";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("LIMIT 10"));
    }

    #[test]
    fn roundtrip_subquery() {
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        let emitted = emit_statement(&stmts[0]);
        assert!(emitted.contains("SELECT user_id FROM orders"));
    }

    // ── Roundtrip tests via translate() ────────────────────────────────────

    use crate::{translate, Dialect, TranslateResult};

    /// Helper: translate SQL via a given dialect and return the emitted SQL string.
    fn translate_sql(input: &str, dialect: Dialect) -> String {
        let results = translate(input, dialect).unwrap();
        match &results[0] {
            TranslateResult::Sql(s) => s.clone(),
            other => panic!("expected Sql, got: {other:?}"),
        }
    }

    /// Helper: assert the emitted SQL contains a given fragment (case-insensitive).
    fn assert_contains(sql: &str, fragment: &str) {
        let upper = sql.to_ascii_uppercase();
        assert!(
            upper.contains(&fragment.to_ascii_uppercase()),
            "expected '{fragment}' in: {sql}"
        );
    }

    // ── 1. Complex SELECT statements ───────────────────────────────────────

    #[test]
    fn select_group_by_having() {
        let sql = translate_sql(
            "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 5",
            Dialect::MySQL,
        );
        assert_contains(&sql, "GROUP BY");
        assert_contains(&sql, "HAVING");
        assert_contains(&sql, "COUNT(*)");
    }

    #[test]
    fn select_order_by_multiple_columns() {
        let sql = translate_sql(
            "SELECT id, name, age FROM users ORDER BY age DESC, name ASC",
            Dialect::MySQL,
        );
        assert_contains(&sql, "ORDER BY");
        assert_contains(&sql, "DESC");
        assert_contains(&sql, "ASC");
    }

    #[test]
    fn select_distinct() {
        let sql = translate_sql(
            "SELECT DISTINCT department FROM employees",
            Dialect::MySQL,
        );
        assert_contains(&sql, "DISTINCT");
        assert_contains(&sql, "department");
    }

    #[test]
    fn select_limit_offset_standard() {
        let sql = translate_sql(
            "SELECT * FROM users LIMIT 10 OFFSET 20",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "LIMIT 10");
        assert_contains(&sql, "OFFSET 20");
    }

    #[test]
    fn select_aggregate_count() {
        let sql = translate_sql("SELECT COUNT(*) FROM users", Dialect::MySQL);
        assert_contains(&sql, "COUNT(*)");
    }

    #[test]
    fn select_aggregate_sum() {
        let sql = translate_sql(
            "SELECT SUM(amount) FROM orders",
            Dialect::MySQL,
        );
        assert_contains(&sql, "SUM(");
        assert_contains(&sql, "amount");
    }

    #[test]
    fn select_aggregate_avg() {
        let sql = translate_sql(
            "SELECT AVG(price) FROM products",
            Dialect::MySQL,
        );
        assert_contains(&sql, "AVG(");
        assert_contains(&sql, "price");
    }

    #[test]
    fn select_aggregate_min_max() {
        let sql = translate_sql(
            "SELECT MIN(created_at), MAX(created_at) FROM events",
            Dialect::MySQL,
        );
        assert_contains(&sql, "MIN(");
        assert_contains(&sql, "MAX(");
    }

    #[test]
    fn select_case_when() {
        let sql = translate_sql(
            "SELECT CASE WHEN status = 'active' THEN 'yes' WHEN status = 'inactive' THEN 'no' ELSE 'unknown' END AS label FROM users",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CASE");
        assert_contains(&sql, "WHEN");
        assert_contains(&sql, "THEN");
        assert_contains(&sql, "ELSE");
        assert_contains(&sql, "END");
    }

    #[test]
    fn select_subquery_in_where() {
        let sql = translate_sql(
            "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "IN (SELECT");
        assert_contains(&sql, "user_id");
        assert_contains(&sql, "total > 100");
    }

    #[test]
    fn select_subquery_in_from() {
        let sql = translate_sql(
            "SELECT sub.name FROM (SELECT name FROM users WHERE active = 1) AS sub",
            Dialect::MySQL,
        );
        assert_contains(&sql, "sub");
        assert_contains(&sql, "SELECT name FROM users");
    }

    #[test]
    fn select_with_alias() {
        let sql = translate_sql(
            "SELECT u.name AS user_name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            Dialect::MySQL,
        );
        assert_contains(&sql, "AS user_name");
        assert_contains(&sql, "AS order_count");
        assert_contains(&sql, "GROUP BY");
    }

    #[test]
    fn select_between() {
        let sql = translate_sql(
            "SELECT * FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'",
            Dialect::MySQL,
        );
        assert_contains(&sql, "BETWEEN");
        assert_contains(&sql, "2024-01-01");
        assert_contains(&sql, "2024-12-31");
    }

    #[test]
    fn select_is_null_is_not_null() {
        let sql = translate_sql(
            "SELECT * FROM users WHERE email IS NOT NULL AND phone IS NULL",
            Dialect::MySQL,
        );
        assert_contains(&sql, "IS NOT NULL");
        assert_contains(&sql, "IS NULL");
    }

    #[test]
    fn select_like() {
        let sql = translate_sql(
            "SELECT * FROM users WHERE name LIKE '%Alice%'",
            Dialect::MySQL,
        );
        assert_contains(&sql, "LIKE");
        assert_contains(&sql, "%Alice%");
    }

    #[test]
    fn select_in_list() {
        let sql = translate_sql(
            "SELECT * FROM users WHERE status IN ('active', 'pending', 'trial')",
            Dialect::MySQL,
        );
        assert_contains(&sql, "IN (");
        assert_contains(&sql, "'active'");
        assert_contains(&sql, "'pending'");
        assert_contains(&sql, "'trial'");
    }

    #[test]
    fn select_exists_subquery() {
        let sql = translate_sql(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "EXISTS");
        assert_contains(&sql, "SELECT 1");
    }

    #[test]
    fn select_count_distinct() {
        let sql = translate_sql(
            "SELECT COUNT(DISTINCT department) FROM employees",
            Dialect::MySQL,
        );
        assert_contains(&sql, "COUNT(DISTINCT");
        assert_contains(&sql, "department");
    }

    // ── 2. JOINs ───────────────────────────────────────────────────────────

    #[test]
    fn inner_join_with_on() {
        let sql = translate_sql(
            "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id",
            Dialect::MySQL,
        );
        assert_contains(&sql, "INNER JOIN");
        // Some dialects emit just JOIN; check for ON clause too.
        assert_contains(&sql, "ON u.id = o.user_id");
    }

    #[test]
    fn left_join() {
        let sql = translate_sql(
            "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id",
            Dialect::MySQL,
        );
        assert_contains(&sql, "LEFT JOIN");
        assert_contains(&sql, "ON u.id = o.user_id");
    }

    #[test]
    fn left_outer_join() {
        let sql = translate_sql(
            "SELECT u.name, o.total FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "LEFT");
        assert_contains(&sql, "JOIN");
        assert_contains(&sql, "ON u.id = o.user_id");
    }

    #[test]
    fn multiple_joins() {
        let sql = translate_sql(
            "SELECT u.name, o.total, p.name AS product FROM users u \
             JOIN orders o ON u.id = o.user_id \
             JOIN products p ON o.product_id = p.id",
            Dialect::MySQL,
        );
        assert_contains(&sql, "JOIN");
        assert_contains(&sql, "u.id = o.user_id");
        assert_contains(&sql, "o.product_id = p.id");
    }

    #[test]
    fn self_join_with_aliases() {
        let sql = translate_sql(
            "SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id",
            Dialect::MySQL,
        );
        assert_contains(&sql, "AS employee");
        assert_contains(&sql, "AS manager");
        assert_contains(&sql, "e.manager_id = m.id");
    }

    #[test]
    fn cross_join() {
        let sql = translate_sql(
            "SELECT a.x, b.y FROM t1 a CROSS JOIN t2 b",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CROSS JOIN");
    }

    #[test]
    fn right_join() {
        let sql = translate_sql(
            "SELECT u.name, o.id FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "RIGHT JOIN");
        assert_contains(&sql, "ON u.id = o.user_id");
    }

    // ── 3. Set operations ──────────────────────────────────────────────────

    #[test]
    fn union() {
        let sql = translate_sql(
            "SELECT name FROM customers UNION SELECT name FROM suppliers",
            Dialect::MySQL,
        );
        assert_contains(&sql, "UNION");
        assert_contains(&sql, "customers");
        assert_contains(&sql, "suppliers");
    }

    #[test]
    fn union_all() {
        let sql = translate_sql(
            "SELECT name FROM customers UNION ALL SELECT name FROM suppliers",
            Dialect::MySQL,
        );
        assert_contains(&sql, "UNION ALL");
    }

    #[test]
    fn multiple_unions() {
        let sql = translate_sql(
            "SELECT name FROM customers UNION SELECT name FROM suppliers UNION SELECT name FROM partners",
            Dialect::MySQL,
        );
        assert_contains(&sql, "UNION");
        assert_contains(&sql, "customers");
        assert_contains(&sql, "suppliers");
        assert_contains(&sql, "partners");
    }

    #[test]
    fn intersect() {
        let sql = translate_sql(
            "SELECT id FROM a INTERSECT SELECT id FROM b",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "INTERSECT");
    }

    #[test]
    fn except() {
        let sql = translate_sql(
            "SELECT id FROM a EXCEPT SELECT id FROM b",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "EXCEPT");
    }

    // ── 4. CTEs (Common Table Expressions) ─────────────────────────────────

    #[test]
    fn simple_cte() {
        let sql = translate_sql(
            "WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active_users",
            Dialect::MySQL,
        );
        assert_contains(&sql, "WITH");
        assert_contains(&sql, "active_users");
        assert_contains(&sql, "active = 1");
    }

    #[test]
    fn cte_used_in_join() {
        let sql = translate_sql(
            "WITH recent_orders AS (SELECT user_id, SUM(total) AS total FROM orders GROUP BY user_id) \
             SELECT u.name, r.total FROM users u JOIN recent_orders r ON u.id = r.user_id",
            Dialect::MySQL,
        );
        assert_contains(&sql, "WITH");
        assert_contains(&sql, "recent_orders");
        assert_contains(&sql, "SUM(");
        assert_contains(&sql, "JOIN");
    }

    #[test]
    fn multiple_ctes() {
        let sql = translate_sql(
            "WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT 2 AS b) SELECT * FROM cte1, cte2",
            Dialect::MySQL,
        );
        assert_contains(&sql, "WITH");
        assert_contains(&sql, "cte1");
        assert_contains(&sql, "cte2");
    }

    // ── 5. Complex DML after rewriting ─────────────────────────────────────

    #[test]
    fn insert_select() {
        let sql = translate_sql(
            "INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = 0",
            Dialect::MySQL,
        );
        assert_contains(&sql, "INSERT INTO");
        assert_contains(&sql, "archive");
        assert_contains(&sql, "SELECT id, name FROM users");
        assert_contains(&sql, "active = 0");
    }

    #[test]
    fn insert_multiple_rows() {
        let sql = translate_sql(
            "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Carol', 35)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "INSERT INTO");
        assert_contains(&sql, "Alice");
        assert_contains(&sql, "Bob");
        assert_contains(&sql, "Carol");
    }

    #[test]
    fn update_with_subquery_in_where() {
        let sql = translate_sql(
            "UPDATE users SET active = 0 WHERE id IN (SELECT user_id FROM banned)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "UPDATE");
        assert_contains(&sql, "active = 0");
        assert_contains(&sql, "IN (SELECT user_id FROM banned)");
    }

    #[test]
    fn delete_with_subquery() {
        let sql = translate_sql(
            "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 0)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "DELETE");
        assert_contains(&sql, "orders");
        assert_contains(&sql, "IN (SELECT id FROM users");
    }

    #[test]
    fn update_multiple_columns() {
        let sql = translate_sql(
            "UPDATE users SET name = 'Updated', age = 99, active = 0 WHERE id = 42",
            Dialect::MySQL,
        );
        assert_contains(&sql, "UPDATE users SET");
        assert_contains(&sql, "name = 'Updated'");
        assert_contains(&sql, "age = 99");
        assert_contains(&sql, "active = 0");
        assert_contains(&sql, "id = 42");
    }

    #[test]
    fn delete_with_and_or_conditions() {
        let sql = translate_sql(
            "DELETE FROM events WHERE (status = 'expired' OR status = 'cancelled') AND created_at < '2020-01-01'",
            Dialect::MySQL,
        );
        assert_contains(&sql, "DELETE");
        assert_contains(&sql, "expired");
        assert_contains(&sql, "cancelled");
        assert_contains(&sql, "AND");
    }

    // ── 6. DDL ─────────────────────────────────────────────────────────────

    #[test]
    fn create_table_if_not_exists() {
        let sql = translate_sql(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CREATE TABLE IF NOT EXISTS");
        assert_contains(&sql, "users");
        assert_contains(&sql, "PRIMARY KEY");
        assert_contains(&sql, "NOT NULL");
    }

    #[test]
    fn drop_table_if_exists() {
        let sql = translate_sql(
            "DROP TABLE IF EXISTS old_users",
            Dialect::MySQL,
        );
        assert_contains(&sql, "DROP TABLE IF EXISTS");
        assert_contains(&sql, "old_users");
    }

    #[test]
    fn create_index() {
        let sql = translate_sql(
            "CREATE INDEX idx_users_email ON users (email)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CREATE INDEX");
        assert_contains(&sql, "idx_users_email");
        assert_contains(&sql, "users");
        assert_contains(&sql, "email");
    }

    #[test]
    fn create_unique_index() {
        let sql = translate_sql(
            "CREATE UNIQUE INDEX idx_users_email ON users (email)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "UNIQUE INDEX");
        assert_contains(&sql, "idx_users_email");
        assert_contains(&sql, "users");
    }

    #[test]
    fn drop_table_plain() {
        let sql = translate_sql(
            "DROP TABLE sessions",
            Dialect::MySQL,
        );
        assert_contains(&sql, "DROP TABLE");
        assert_contains(&sql, "sessions");
    }

    #[test]
    fn create_table_multiple_constraints() {
        let sql = translate_sql(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount INTEGER DEFAULT 0)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "PRIMARY KEY");
        assert_contains(&sql, "NOT NULL");
        assert_contains(&sql, "DEFAULT");
    }

    #[test]
    fn create_index_composite() {
        let sql = translate_sql(
            "CREATE INDEX idx_composite ON orders (user_id, created_at)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CREATE INDEX");
        assert_contains(&sql, "user_id");
        assert_contains(&sql, "created_at");
    }

    // ── 7. Transaction statements ──────────────────────────────────────────

    #[test]
    fn begin_transaction() {
        let sql = translate_sql("BEGIN", Dialect::MySQL);
        // sqlparser may emit BEGIN TRANSACTION or just BEGIN
        let upper = sql.to_ascii_uppercase();
        assert!(
            upper.contains("BEGIN"),
            "expected BEGIN in: {sql}"
        );
    }

    #[test]
    fn commit_transaction() {
        let sql = translate_sql("COMMIT", Dialect::MySQL);
        assert_contains(&sql, "COMMIT");
    }

    #[test]
    fn rollback_transaction() {
        let sql = translate_sql("ROLLBACK", Dialect::MySQL);
        assert_contains(&sql, "ROLLBACK");
    }

    #[test]
    fn savepoint() {
        let sql = translate_sql("SAVEPOINT sp1", Dialect::MySQL);
        assert_contains(&sql, "SAVEPOINT");
        assert_contains(&sql, "sp1");
    }

    #[test]
    fn release_savepoint() {
        let sql = translate_sql("RELEASE SAVEPOINT sp1", Dialect::MySQL);
        assert_contains(&sql, "RELEASE SAVEPOINT");
        assert_contains(&sql, "sp1");
    }

    #[test]
    fn rollback_to_savepoint() {
        let sql = translate_sql(
            "ROLLBACK TO SAVEPOINT sp1",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "ROLLBACK TO SAVEPOINT");
        assert_contains(&sql, "sp1");
    }

    // ── 8. Cross-dialect roundtrip coverage ────────────────────────────────

    #[test]
    fn pg_select_group_by_having() {
        let sql = translate_sql(
            "SELECT category, AVG(price) FROM products GROUP BY category HAVING AVG(price) > 50",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "GROUP BY");
        assert_contains(&sql, "HAVING");
        assert_contains(&sql, "AVG(");
    }

    #[test]
    fn pg_cte_with_join() {
        let sql = translate_sql(
            "WITH top_users AS (SELECT id, name FROM users ORDER BY score DESC LIMIT 10) \
             SELECT t.name, COUNT(o.id) FROM top_users t LEFT JOIN orders o ON t.id = o.user_id GROUP BY t.name",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "WITH");
        assert_contains(&sql, "top_users");
        assert_contains(&sql, "LEFT JOIN");
        assert_contains(&sql, "GROUP BY");
    }

    #[test]
    fn tds_select_with_where_and_order() {
        let sql = translate_sql(
            "SELECT id, name FROM users WHERE active = 1 ORDER BY name",
            Dialect::TDS,
        );
        assert_contains(&sql, "SELECT");
        assert_contains(&sql, "WHERE");
        assert_contains(&sql, "ORDER BY");
    }

    #[test]
    fn pg_insert_select() {
        let sql = translate_sql(
            "INSERT INTO log (event) SELECT event_name FROM events WHERE created_at > '2024-01-01'",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "INSERT INTO");
        assert_contains(&sql, "SELECT event_name");
    }

    #[test]
    fn mysql_create_table_if_not_exists_simple() {
        let sql = translate_sql(
            "CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY, value TEXT)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "IF NOT EXISTS");
    }

    #[test]
    fn pg_union_all_with_order() {
        let sql = translate_sql(
            "SELECT name, 'customer' AS type FROM customers UNION ALL SELECT name, 'supplier' AS type FROM suppliers ORDER BY name",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "UNION ALL");
        assert_contains(&sql, "ORDER BY");
    }

    #[test]
    fn nested_subquery() {
        let sql = translate_sql(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE category = 'electronics'))",
            Dialect::MySQL,
        );
        assert_contains(&sql, "IN (SELECT user_id");
        assert_contains(&sql, "IN (SELECT id FROM products");
        assert_contains(&sql, "electronics");
    }

    #[test]
    fn select_coalesce() {
        let sql = translate_sql(
            "SELECT COALESCE(nickname, name, 'Anonymous') FROM users",
            Dialect::MySQL,
        );
        assert_contains(&sql, "COALESCE");
        assert_contains(&sql, "nickname");
        assert_contains(&sql, "Anonymous");
    }

    #[test]
    fn select_with_multiple_subqueries() {
        let sql = translate_sql(
            "SELECT (SELECT COUNT(*) FROM orders) AS order_count, (SELECT COUNT(*) FROM users) AS user_count",
            Dialect::MySQL,
        );
        assert_contains(&sql, "SELECT COUNT(*)");
        assert_contains(&sql, "order_count");
        assert_contains(&sql, "user_count");
    }

    #[test]
    fn insert_returning_not_supported_mysql() {
        // MySQL doesn't support RETURNING, but PostgreSQL does. Test PG.
        let sql = translate_sql(
            "INSERT INTO users (name) VALUES ('Alice') RETURNING id",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "INSERT INTO");
        assert_contains(&sql, "Alice");
        // sqlparser should preserve RETURNING
        assert_contains(&sql, "RETURNING");
    }

    #[test]
    fn pg_select_distinct_on_not_available_use_regular_distinct() {
        // Regular DISTINCT should work for all dialects.
        let sql = translate_sql(
            "SELECT DISTINCT status FROM orders",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "DISTINCT");
        assert_contains(&sql, "status");
    }

    #[test]
    fn select_having_without_group_by() {
        // MySQL allows HAVING without GROUP BY (aggregate over entire table).
        let sql = translate_sql(
            "SELECT COUNT(*) FROM users HAVING COUNT(*) > 0",
            Dialect::MySQL,
        );
        assert_contains(&sql, "HAVING");
        assert_contains(&sql, "COUNT(*)");
    }

    #[test]
    fn select_null_handling_coalesce_and_ifnull() {
        let sql = translate_sql(
            "SELECT COALESCE(a, b, c) FROM t",
            Dialect::MySQL,
        );
        assert_contains(&sql, "COALESCE");
    }

    #[test]
    fn select_order_by_expression() {
        let sql = translate_sql(
            "SELECT name, age FROM users ORDER BY age * 2 DESC",
            Dialect::MySQL,
        );
        assert_contains(&sql, "ORDER BY");
        assert_contains(&sql, "DESC");
    }

    #[test]
    fn create_index_if_not_exists() {
        let sql = translate_sql(
            "CREATE INDEX IF NOT EXISTS idx_name ON users (name)",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "IF NOT EXISTS");
        assert_contains(&sql, "idx_name");
    }

    #[test]
    fn select_group_by_multiple_columns() {
        let sql = translate_sql(
            "SELECT department, role, COUNT(*) FROM employees GROUP BY department, role",
            Dialect::MySQL,
        );
        assert_contains(&sql, "GROUP BY");
        assert_contains(&sql, "department");
        assert_contains(&sql, "role");
        assert_contains(&sql, "COUNT(*)");
    }

    #[test]
    fn select_aliased_subquery_in_from() {
        let sql = translate_sql(
            "SELECT t.total_count FROM (SELECT COUNT(*) AS total_count FROM users) AS t",
            Dialect::MySQL,
        );
        assert_contains(&sql, "total_count");
        assert_contains(&sql, "COUNT(*)");
    }

    #[test]
    fn pg_create_unique_index() {
        let sql = translate_sql(
            "CREATE UNIQUE INDEX idx_email ON accounts (email)",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "UNIQUE INDEX");
        assert_contains(&sql, "idx_email");
        assert_contains(&sql, "accounts");
    }

    #[test]
    fn select_case_when_no_else() {
        let sql = translate_sql(
            "SELECT CASE WHEN age >= 18 THEN 'adult' END FROM users",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CASE");
        assert_contains(&sql, "WHEN");
        assert_contains(&sql, "THEN");
        assert_contains(&sql, "END");
    }

    #[test]
    fn select_case_with_operand() {
        let sql = translate_sql(
            "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END FROM users",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CASE");
        assert_contains(&sql, "WHEN 'active' THEN 1");
        assert_contains(&sql, "ELSE -1");
    }

    #[test]
    fn select_not_in() {
        let sql = translate_sql(
            "SELECT * FROM users WHERE id NOT IN (1, 2, 3)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "NOT IN");
    }

    #[test]
    fn select_or_conditions() {
        let sql = translate_sql(
            "SELECT * FROM users WHERE age < 18 OR age > 65",
            Dialect::MySQL,
        );
        assert_contains(&sql, "OR");
        assert_contains(&sql, "age < 18");
        assert_contains(&sql, "age > 65");
    }

    #[test]
    fn multiple_statements_in_one_input() {
        let results = translate(
            "SELECT 1; SELECT 2",
            Dialect::MySQL,
        )
        .unwrap();
        assert_eq!(results.len(), 2, "expected 2 results");
        let s1 = match &results[0] {
            TranslateResult::Sql(s) => s.clone(),
            other => panic!("expected Sql, got: {other:?}"),
        };
        let s2 = match &results[1] {
            TranslateResult::Sql(s) => s.clone(),
            other => panic!("expected Sql, got: {other:?}"),
        };
        assert!(s1.contains('1'));
        assert!(s2.contains('2'));
    }

    #[test]
    fn cte_recursive() {
        // Recursive CTEs are valid in SQLite.
        let sql = translate_sql(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT x FROM cnt",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "WITH RECURSIVE");
        assert_contains(&sql, "cnt");
        assert_contains(&sql, "UNION ALL");
    }

    #[test]
    fn pg_begin_commit() {
        let sql_begin = translate_sql("BEGIN", Dialect::PostgreSQL);
        let sql_commit = translate_sql("COMMIT", Dialect::PostgreSQL);
        let upper_begin = sql_begin.to_ascii_uppercase();
        assert!(upper_begin.contains("BEGIN"), "got: {sql_begin}");
        assert_contains(&sql_commit, "COMMIT");
    }

    #[test]
    fn insert_default_values() {
        let sql = translate_sql(
            "INSERT INTO t DEFAULT VALUES",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "INSERT INTO");
        assert_contains(&sql, "DEFAULT VALUES");
    }

    #[test]
    fn select_with_table_alias_no_as() {
        let sql = translate_sql(
            "SELECT u.id, u.name FROM users u WHERE u.id > 0",
            Dialect::MySQL,
        );
        assert_contains(&sql, "u.id");
        assert_contains(&sql, "u.name");
    }

    #[test]
    fn select_nested_case_when() {
        let sql = translate_sql(
            "SELECT CASE WHEN x > 0 THEN CASE WHEN x > 10 THEN 'big' ELSE 'small' END ELSE 'zero' END FROM t",
            Dialect::MySQL,
        );
        assert_contains(&sql, "CASE");
        assert_contains(&sql, "'big'");
        assert_contains(&sql, "'small'");
        assert_contains(&sql, "'zero'");
    }

    #[test]
    fn select_with_not_exists() {
        let sql = translate_sql(
            "SELECT * FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
            Dialect::MySQL,
        );
        assert_contains(&sql, "NOT EXISTS");
        assert_contains(&sql, "SELECT 1");
    }

    #[test]
    fn pg_drop_table_if_exists() {
        let sql = translate_sql(
            "DROP TABLE IF EXISTS temp_data",
            Dialect::PostgreSQL,
        );
        assert_contains(&sql, "DROP TABLE IF EXISTS");
        assert_contains(&sql, "temp_data");
    }
}
