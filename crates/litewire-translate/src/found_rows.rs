//! `SQL_CALC_FOUND_ROWS` / `FOUND_ROWS()` helpers for session layers.
//!
//! SQLite cannot emulate MySQL's `FOUND_ROWS()` inside a single statement:
//! the value is *session* state produced by the previous
//! `SELECT SQL_CALC_FOUND_ROWS ...`. This module provides the two
//! detection/derivation halves that `litewire-session` stitches together:
//!
//! * [`calc_found_rows_count_sql`] — for a statement carrying the
//!   `SQL_CALC_FOUND_ROWS` hint, derive the SQLite `COUNT(*)` statement
//!   that computes the total the query would have produced without
//!   `LIMIT`/`OFFSET`.
//! * [`found_rows_select_column`] — recognize a bare `SELECT FOUND_ROWS()`
//!   so the session can answer it from stored state instead of the
//!   backend.
//!
//! Both are deliberately cheap to reject: a non-matching statement pays
//! only an allocation-free ASCII scan, never a parse.

use sqlparser::ast::{Expr, FunctionArguments, LimitClause, SelectItem, SetExpr, Statement, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::Dialect;

/// The MySQL SELECT hint that requests the total row count ignoring `LIMIT`.
const CALC_HINT: &str = "SQL_CALC_FOUND_ROWS";

/// Allocation-free ASCII case-insensitive substring test.
/// `needle_upper` must already be uppercase.
fn contains_ascii_ignore_case(haystack: &str, needle_upper: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle_upper.as_bytes();
    h.len() >= n.len()
        && h.windows(n.len())
            .any(|w| w.iter().zip(n).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

/// Does `sql` contain `word` as a standalone word outside string/identifier
/// quotes? Same quote handling as the hint-stripping pre-pass in `lib.rs`.
fn contains_word_outside_quotes(sql: &str, word: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => {
                quote = Some(c);
                i += 1;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if sql[start..i].eq_ignore_ascii_case(word) {
                    return true;
                }
            }
            _ => i += 1,
        }
    }
    false
}

/// For a MySQL statement carrying the `SQL_CALC_FOUND_ROWS` hint, derive
/// the SQLite statement computing the total row count the query would have
/// produced without `LIMIT`/`OFFSET`: the fully translated statement with
/// its `LIMIT` clause and `ORDER BY` removed (ordering cannot change a
/// count), wrapped in `SELECT COUNT(*) FROM (...)`.
///
/// Placeholders in `WHERE`/`JOIN`/... are preserved, so the caller can
/// bind the same parameter list it bound to the main statement.
///
/// Returns `None` — meaning "no count statement, fall back to the plain
/// row count" — when:
///
/// * `dialect` is not MySQL, or the hint is absent (the common case; this
///   costs one allocation-free scan and no parsing),
/// * the input holds more than one statement,
/// * the statement is not a query, or does not parse/translate (the main
///   execution path will surface that error),
/// * the `LIMIT`/`OFFSET` clause itself contains a placeholder — stripping
///   it would desynchronize the positional parameter list.
#[must_use]
pub fn calc_found_rows_count_sql(sql: &str, dialect: Dialect) -> Option<String> {
    if dialect != Dialect::MySQL
        || !contains_ascii_ignore_case(sql, CALC_HINT)
        || !contains_word_outside_quotes(sql, CALC_HINT)
    {
        return None;
    }

    let hintless = crate::strip_mysql_select_hints(sql);
    let mut statements = Parser::parse_sql(&MySqlDialect {}, &hintless).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let stmt = crate::rewrite_statement(statements.pop()?, dialect).ok()?;
    let Statement::Query(mut query) = stmt else {
        return None;
    };

    if limit_clause_has_placeholder(query.limit_clause.as_ref()) {
        return None;
    }
    query.limit_clause = None;
    query.order_by = None;
    query.fetch = None;

    Some(format!("SELECT COUNT(*) FROM ({query})"))
}

/// Does any expression in the `LIMIT`/`OFFSET` clause contain a `?`/`$n`
/// placeholder? (Both clause shapes are checked even though the MySQL
/// rewrite normalizes `LIMIT offset, count` before this runs.)
fn limit_clause_has_placeholder(clause: Option<&LimitClause>) -> bool {
    fn expr_has_placeholder(e: &Expr) -> bool {
        match e {
            Expr::Value(v) => matches!(v.value, Value::Placeholder(_)),
            Expr::BinaryOp { left, right, .. } => {
                expr_has_placeholder(left) || expr_has_placeholder(right)
            }
            Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => expr_has_placeholder(expr),
            _ => false,
        }
    }

    match clause {
        None => false,
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            limit.as_ref().is_some_and(expr_has_placeholder)
                || offset
                    .as_ref()
                    .is_some_and(|o| expr_has_placeholder(&o.value))
                || limit_by.iter().any(expr_has_placeholder)
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            expr_has_placeholder(offset) || expr_has_placeholder(limit)
        }
    }
}

/// If `sql` is a bare MySQL `SELECT FOUND_ROWS()` — a single statement,
/// no FROM/WHERE/ORDER BY/LIMIT, one projection item that is a zero-argument
/// `FOUND_ROWS` call, with an optional column alias — return the name the
/// result column should carry: the alias if one was given, otherwise the
/// call as written plus `()` (MySQL echoes the expression text, e.g.
/// `FOUND_ROWS()`).
///
/// Anything fancier (`SELECT FOUND_ROWS() + 1`, a FROM clause, ...) returns
/// `None` and takes the normal translation path.
#[must_use]
pub fn found_rows_select_column(sql: &str, dialect: Dialect) -> Option<String> {
    if dialect != Dialect::MySQL || !contains_ascii_ignore_case(sql, "FOUND_ROWS") {
        return None;
    }

    let statements = Parser::parse_sql(&MySqlDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Statement::Query(query) = &statements[0] else {
        return None;
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !select.from.is_empty()
        || select.selection.is_some()
        || select.having.is_some()
        || select.distinct.is_some()
        || select.projection.len() != 1
    {
        return None;
    }

    let (expr, alias) = match &select.projection[0] {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
        _ => return None,
    };
    let Expr::Function(func) = expr else {
        return None;
    };
    if !func.name.to_string().eq_ignore_ascii_case("FOUND_ROWS") {
        return None;
    }
    let no_args = match &func.args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty(),
        FunctionArguments::Subquery(_) => false,
    };
    if !no_args {
        return None;
    }

    Some(alias.map_or_else(|| format!("{}()", func.name), |a| a.value.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── calc_found_rows_count_sql ───────────────────────────────────────────

    #[test]
    fn calc_count_sql_strips_limit_offset_and_order_by() {
        let sql = "SELECT SQL_CALC_FOUND_ROWS * FROM wp_posts WHERE post_status = 'publish' \
                   ORDER BY post_date DESC LIMIT 10 OFFSET 20";
        let count = calc_found_rows_count_sql(sql, Dialect::MySQL).unwrap();
        let upper = count.to_ascii_uppercase();
        assert!(upper.starts_with("SELECT COUNT(*) FROM ("), "got: {count}");
        assert!(!upper.contains("LIMIT"), "got: {count}");
        assert!(!upper.contains("OFFSET"), "got: {count}");
        assert!(!upper.contains("ORDER BY"), "got: {count}");
        assert!(!upper.contains("SQL_CALC_FOUND_ROWS"), "got: {count}");
        assert!(count.contains("post_status"), "got: {count}");
    }

    #[test]
    fn calc_count_sql_handles_comma_limit_form() {
        let sql = "SELECT SQL_CALC_FOUND_ROWS id FROM t LIMIT 5, 10";
        let count = calc_found_rows_count_sql(sql, Dialect::MySQL).unwrap();
        assert!(
            !count.to_ascii_uppercase().contains("LIMIT"),
            "got: {count}"
        );
    }

    #[test]
    fn calc_count_sql_is_case_insensitive() {
        let sql = "select sql_calc_found_rows id from t limit 3";
        assert!(calc_found_rows_count_sql(sql, Dialect::MySQL).is_some());
    }

    #[test]
    fn calc_count_sql_none_without_hint() {
        assert!(calc_found_rows_count_sql("SELECT * FROM t LIMIT 3", Dialect::MySQL).is_none());
    }

    #[test]
    fn calc_count_sql_none_for_hint_inside_string_literal() {
        let sql = "SELECT * FROM t WHERE v = 'SQL_CALC_FOUND_ROWS'";
        assert!(calc_found_rows_count_sql(sql, Dialect::MySQL).is_none());
    }

    #[test]
    fn calc_count_sql_none_for_placeholder_limit() {
        let sql = "SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT ?";
        assert!(calc_found_rows_count_sql(sql, Dialect::MySQL).is_none());
        let sql = "SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 5 OFFSET ?";
        assert!(calc_found_rows_count_sql(sql, Dialect::MySQL).is_none());
    }

    #[test]
    fn calc_count_sql_keeps_where_placeholders() {
        let sql = "SELECT SQL_CALC_FOUND_ROWS * FROM t WHERE id > ? LIMIT 2";
        let count = calc_found_rows_count_sql(sql, Dialect::MySQL).unwrap();
        assert!(count.contains('?'), "got: {count}");
        assert!(
            !count.to_ascii_uppercase().contains("LIMIT"),
            "got: {count}"
        );
    }

    #[test]
    fn calc_count_sql_none_for_multi_statement_input() {
        let sql = "SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 1; SELECT 1";
        assert!(calc_found_rows_count_sql(sql, Dialect::MySQL).is_none());
    }

    #[test]
    fn calc_count_sql_none_for_other_dialects() {
        let sql = "SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 1";
        assert!(calc_found_rows_count_sql(sql, Dialect::PostgreSQL).is_none());
        assert!(calc_found_rows_count_sql(sql, Dialect::TDS).is_none());
    }

    // ── found_rows_select_column ────────────────────────────────────────────

    #[test]
    fn found_rows_select_recognized() {
        assert_eq!(
            found_rows_select_column("SELECT FOUND_ROWS()", Dialect::MySQL).as_deref(),
            Some("FOUND_ROWS()")
        );
        // Trailing semicolon and lowercase both work; the column name
        // echoes the call as written.
        assert_eq!(
            found_rows_select_column("select found_rows();", Dialect::MySQL).as_deref(),
            Some("found_rows()")
        );
    }

    #[test]
    fn found_rows_select_alias_wins() {
        assert_eq!(
            found_rows_select_column("SELECT FOUND_ROWS() AS total", Dialect::MySQL).as_deref(),
            Some("total")
        );
    }

    #[test]
    fn found_rows_select_rejects_non_bare_shapes() {
        for sql in [
            "SELECT FOUND_ROWS() + 1",
            "SELECT FOUND_ROWS() FROM t",
            "SELECT FOUND_ROWS() WHERE 1 = 1",
            "SELECT FOUND_ROWS(), 1",
            "SELECT FOUND_ROWS(1)",
            "SELECT 'FOUND_ROWS()'",
            "SELECT FOUND_ROWS() LIMIT 1",
            "SELECT FOUND_ROWS(); SELECT 1",
        ] {
            assert!(
                found_rows_select_column(sql, Dialect::MySQL).is_none(),
                "should reject: {sql}"
            );
        }
    }

    #[test]
    fn found_rows_select_none_for_other_dialects() {
        assert!(found_rows_select_column("SELECT FOUND_ROWS()", Dialect::PostgreSQL).is_none());
    }
}
