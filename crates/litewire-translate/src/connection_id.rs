//! `CONNECTION_ID()` detection for session layers.
//!
//! SQLite has no per-connection id, so the stateless function rewrite in
//! [`crate::common`] turns `CONNECTION_ID()` into the constant `0`. That is
//! fine for the function embedded in a larger expression, but MySQL
//! guarantees that a bare `SELECT CONNECTION_ID()` reports the *same* id that
//! `IS_USED_LOCK()` returns for a lock the connection holds, and applications
//! compare the two (issue ephpm#657). Only the session knows that id — it is
//! the connection's lock-session id — so, exactly like
//! [`crate::found_rows::found_rows_select_column`], this recognizes the bare
//! form and lets `litewire-session` answer it from the connection instead of
//! emitting the `0` shim.
//!
//! Rejection is cheap: a non-matching statement pays only an allocation-free
//! ASCII scan, never a parse.

use sqlparser::ast::{Expr, FunctionArguments, SelectItem, SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::Dialect;

/// Allocation-free ASCII case-insensitive substring test.
/// `needle_upper` must already be uppercase.
fn contains_ascii_ignore_case(haystack: &str, needle_upper: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle_upper.as_bytes();
    h.len() >= n.len()
        && h.windows(n.len())
            .any(|w| w.iter().zip(n).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

/// If `sql` is a bare MySQL `SELECT CONNECTION_ID()` — a single statement, no
/// FROM/WHERE/ORDER BY/LIMIT, one projection item that is a zero-argument
/// `CONNECTION_ID` call, with an optional column alias — return the name the
/// result column should carry: the alias if one was given, otherwise the
/// call as written plus `()` (MySQL echoes the expression text, e.g.
/// `CONNECTION_ID()`).
///
/// Anything fancier (`SELECT CONNECTION_ID() + 1`, a FROM clause, ...)
/// returns `None` and takes the normal translation path, where the stateless
/// `0` rewrite applies.
#[must_use]
pub fn connection_id_select_column(sql: &str, dialect: Dialect) -> Option<String> {
    if dialect != Dialect::MySQL || !contains_ascii_ignore_case(sql, "CONNECTION_ID") {
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
    if !func.name.to_string().eq_ignore_ascii_case("CONNECTION_ID") {
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

    #[test]
    fn matches_bare_connection_id() {
        assert_eq!(
            connection_id_select_column("SELECT CONNECTION_ID()", Dialect::MySQL).as_deref(),
            Some("CONNECTION_ID()")
        );
        assert_eq!(
            connection_id_select_column("select connection_id();", Dialect::MySQL).as_deref(),
            Some("connection_id()")
        );
    }

    #[test]
    fn keeps_the_alias() {
        assert_eq!(
            connection_id_select_column("SELECT CONNECTION_ID() AS id", Dialect::MySQL).as_deref(),
            Some("id")
        );
    }

    #[test]
    fn rejects_non_bare_forms() {
        for sql in [
            "SELECT CONNECTION_ID() + 1",
            "SELECT CONNECTION_ID(), 1",
            "SELECT CONNECTION_ID() FROM t",
            "SELECT CONNECTION_ID() WHERE 1",
            "SELECT 1",
            "SELECT CONNECTION_ID(1)",
        ] {
            assert!(
                connection_id_select_column(sql, Dialect::MySQL).is_none(),
                "should not match: {sql}"
            );
        }
    }

    #[test]
    fn only_mysql() {
        assert!(
            connection_id_select_column("SELECT CONNECTION_ID()", Dialect::PostgreSQL).is_none()
        );
    }

    #[test]
    fn cheap_reject_without_the_word() {
        assert!(connection_id_select_column("SELECT 1 + 1", Dialect::MySQL).is_none());
    }
}
