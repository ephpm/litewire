//! Detection and rewriting of MySQL named-lock function calls —
//! `GET_LOCK()`, `RELEASE_LOCK()`, `RELEASE_ALL_LOCKS()`, `IS_FREE_LOCK()`,
//! `IS_USED_LOCK()` — plus the `DO <expr>` statement they are usually issued
//! through.
//!
//! SQLite has no named locks, and these functions are *session* operations
//! (they wait, and their result depends on which connection asks), so they
//! cannot be translated into SQL. This module does the two halves that are
//! pure syntax, and leaves the evaluation to `litewire-session`:
//!
//! 1. [`detect`] parses a statement that mentions a lock function, pulls
//!    every call out into a [`LockCall`] with its literal (or placeholder)
//!    arguments, and leaves a sentinel in the AST where each call was.
//! 2. [`LockStatement::to_sqlite`] takes the values the session computed for
//!    those calls, substitutes them for the sentinels as literals, and runs
//!    the ordinary MySQL→SQLite rewrite on the result. The rest of the
//!    statement — other columns, `FROM`, `WHERE` — is translated exactly as
//!    it would have been without the calls.
//!
//! So `SELECT GET_LOCK('job', 0) AS lock_status, NOW()` becomes, once the
//! session has taken the lock, `SELECT 1 AS lock_status, datetime('now')`.
//! An unaliased call keeps MySQL's column name (the call as written), so
//! `SELECT GET_LOCK('job', 0)` yields a column named `GET_LOCK('job', 0)`.
//!
//! Each call is evaluated **once per statement**, before the statement runs,
//! in source order — not once per row. `SELECT GET_LOCK('x', 0) FROM t` takes
//! the lock once; MySQL would take it once per row of `t`, which no
//! application depends on.
//!
//! `DO expr [, expr]` (MySQL's "evaluate and discard") is parsed as
//! `SELECT expr [, expr]` whose rows are thrown away; it is how most code
//! releases a lock (`DO RELEASE_LOCK('job')`).
//!
//! Placeholders (`?`) are supported as lock-function arguments, which is what
//! a prepared `SELECT GET_LOCK(?, ?)` needs. A placeholder's position in the
//! statement's parameter list is its ordinal among all placeholders in
//! AST visiting order — projection, then `FROM`, then `WHERE`, ... — which is
//! source order for every statement shape these functions appear in.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, SelectItem, SetExpr, Statement,
    UnaryOperator, Value, VisitMut, VisitorMut, visit_expressions, visit_expressions_mut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::{Dialect, TranslateError};

/// Prefix of the placeholder text a lock call is replaced with in the AST
/// between [`detect`] and [`LockStatement::to_sqlite`]. Never reaches the
/// engine: every sentinel is substituted before emission.
const SENTINEL_PREFIX: &str = "$__litewire_user_lock_";

/// The five MySQL named-lock functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockFunction {
    /// `GET_LOCK(name, timeout)`.
    GetLock,
    /// `RELEASE_LOCK(name)`.
    ReleaseLock,
    /// `RELEASE_ALL_LOCKS()`.
    ReleaseAllLocks,
    /// `IS_FREE_LOCK(name)`.
    IsFreeLock,
    /// `IS_USED_LOCK(name)`.
    IsUsedLock,
}

impl LockFunction {
    fn from_name(name: &str) -> Option<Self> {
        let f = match name.to_ascii_uppercase().as_str() {
            "GET_LOCK" => Self::GetLock,
            "RELEASE_LOCK" => Self::ReleaseLock,
            "RELEASE_ALL_LOCKS" => Self::ReleaseAllLocks,
            "IS_FREE_LOCK" => Self::IsFreeLock,
            "IS_USED_LOCK" => Self::IsUsedLock,
            _ => return None,
        };
        Some(f)
    }

    /// The function's name as MySQL spells it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::GetLock => "GET_LOCK",
            Self::ReleaseLock => "RELEASE_LOCK",
            Self::ReleaseAllLocks => "RELEASE_ALL_LOCKS",
            Self::IsFreeLock => "IS_FREE_LOCK",
            Self::IsUsedLock => "IS_USED_LOCK",
        }
    }

    /// Number of arguments the function takes.
    #[must_use]
    pub fn arity(self) -> usize {
        match self {
            Self::GetLock => 2,
            Self::ReleaseLock | Self::IsFreeLock | Self::IsUsedLock => 1,
            Self::ReleaseAllLocks => 0,
        }
    }
}

/// One argument of a lock call, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockArg {
    /// `NULL`.
    Null,
    /// A string literal (single- or double-quoted), unescaped.
    Text(String),
    /// A numeric literal as written, including a leading `-` if any.
    Number(String),
    /// A `?` placeholder; the value is the parameter at this ordinal (0-based
    /// among every placeholder in the statement).
    Placeholder(usize),
}

/// One lock-function call found in a statement, in source order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockCall {
    /// Which function.
    pub function: LockFunction,
    /// Its arguments; always exactly `function.arity()` of them.
    pub args: Vec<LockArg>,
}

/// The value a lock call evaluated to, for substitution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockValue {
    /// An integer result.
    Int(i64),
    /// SQL `NULL`.
    Null,
}

/// A statement containing lock-function calls, parsed and ready to be
/// completed with their values.
#[derive(Debug, Clone)]
pub struct LockStatement {
    /// The MySQL AST with each lock call replaced by a sentinel.
    stmt: Statement,
    /// The calls, in the order they must be evaluated (source order).
    pub calls: Vec<LockCall>,
    /// `DO ...`: evaluate, discard any rows, answer OK.
    pub is_do: bool,
    /// Number of `?` placeholders in the original statement — what a
    /// prepared-statement frontend must advertise, since the SQLite text
    /// produced by [`Self::to_sqlite`] has the consumed ones removed.
    pub placeholder_count: usize,
}

impl LockStatement {
    /// Ordinals (0-based) of the placeholders consumed by lock calls, in
    /// ascending order. The parameters at these positions must be removed
    /// from the list bound to the SQLite statement.
    #[must_use]
    pub fn consumed_placeholders(&self) -> Vec<usize> {
        let mut ordinals: Vec<usize> = self
            .calls
            .iter()
            .flat_map(|c| c.args.iter())
            .filter_map(|a| match a {
                LockArg::Placeholder(i) => Some(*i),
                _ => None,
            })
            .collect();
        ordinals.sort_unstable();
        ordinals
    }

    /// Substitute `values` (one per call, in `calls` order) and translate the
    /// completed statement to SQLite SQL.
    ///
    /// # Errors
    ///
    /// Returns the translation error if the rest of the statement cannot be
    /// rewritten, or an internal error if `values` does not match `calls`.
    pub fn to_sqlite(&self, values: &[LockValue]) -> Result<String, TranslateError> {
        if values.len() != self.calls.len() {
            return Err(TranslateError::Unsupported(format!(
                "internal: {} lock values for {} lock calls",
                values.len(),
                self.calls.len()
            )));
        }
        let mut stmt = self.stmt.clone();
        let _: ControlFlow<()> = visit_expressions_mut(&mut stmt, |expr| {
            if let Some(index) = sentinel_index(expr) {
                *expr = match values.get(index) {
                    Some(LockValue::Int(n)) => {
                        Expr::Value(Value::Number(n.to_string(), false).with_empty_span())
                    }
                    Some(LockValue::Null) | None => Expr::Value(Value::Null.with_empty_span()),
                };
            }
            ControlFlow::Continue(())
        });
        let rewritten = crate::rewrite_statement(stmt, Dialect::MySQL)?;
        Ok(crate::emit::emit_statement(&rewritten))
    }
}

/// The sentinel index an expression carries, if it is one.
fn sentinel_index(expr: &Expr) -> Option<usize> {
    let Expr::Value(v) = expr else {
        return None;
    };
    let Value::Placeholder(text) = &v.value else {
        return None;
    };
    text.strip_prefix(SENTINEL_PREFIX)?.parse().ok()
}

fn sentinel(index: usize) -> Expr {
    Expr::Value(Value::Placeholder(format!("{SENTINEL_PREFIX}{index}")).with_empty_span())
}

/// Allocation-free ASCII case-insensitive substring test; `needle_upper`
/// must already be uppercase.
fn contains_ascii_ignore_case(haystack: &str, needle_upper: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle_upper.as_bytes();
    h.len() >= n.len()
        && h.windows(n.len())
            .any(|w| w.iter().zip(n).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

/// If `sql` starts with the `DO` keyword (MySQL's evaluate-and-discard
/// statement), the text after it.
fn do_body(sql: &str) -> Option<&str> {
    let trimmed = sql.trim_start();
    let rest = trimmed.get(2..)?;
    if !trimmed.as_bytes()[..2].eq_ignore_ascii_case(b"DO") {
        return None;
    }
    // A word boundary: `DO 1`, `DO\nRELEASE_LOCK(...)`, but not `DOUBLE(...)`.
    rest.chars()
        .next()
        .is_some_and(char::is_whitespace)
        .then_some(rest)
}

/// Does `sql` look like it contains a lock function? Cheap to reject: every
/// lock function's name contains `_LOCK`, so a statement without that
/// substring pays one allocation-free scan and nothing else.
fn mentions_lock_function(sql: &str) -> bool {
    contains_ascii_ignore_case(sql, "_LOCK")
}

/// Recognize a MySQL statement that needs the session's lock machinery:
/// a `DO ...` statement, or any single statement calling a lock function.
///
/// Returns `None` when the statement is not one (the common case — one
/// substring scan), `Some(Err(_))` when it is one but malformed (a lock
/// function with the wrong number of arguments, or an argument that is
/// neither a literal nor a placeholder), and `Some(Ok(_))` otherwise.
///
/// A statement that merely mentions `_LOCK` in an identifier or string
/// (`SELECT * FROM job_locks`) parses, is found to contain no lock call, and
/// returns `None` so it takes the normal translation path.
#[must_use]
pub fn detect(sql: &str, dialect: Dialect) -> Option<Result<LockStatement, TranslateError>> {
    if dialect != Dialect::MySQL {
        return None;
    }
    let (text, is_do) = match do_body(sql) {
        Some(body) => (format!("SELECT {body}"), true),
        None if mentions_lock_function(sql) => (crate::strip_mysql_select_hints(sql), false),
        None => return None,
    };
    let mut statements = match Parser::parse_sql(&MySqlDialect {}, &text) {
        Ok(s) => s,
        // Let the ordinary translation path report the parse error, with the
        // original text, unless this is a `DO` only we can handle.
        Err(e) if is_do => return Some(Err(TranslateError::Parse(e.to_string()))),
        Err(_) => return None,
    };
    if statements.len() != 1 {
        return if is_do {
            Some(Err(TranslateError::Unsupported(
                "DO: exactly one statement expected".to_string(),
            )))
        } else {
            None
        };
    }
    let mut stmt = statements.pop().expect("one statement");

    alias_lock_projections(&mut stmt);

    let mut extractor = Extractor::default();
    if let ControlFlow::Break(err) = stmt.visit(&mut extractor) {
        return Some(Err(err));
    }
    if extractor.calls.is_empty() && !is_do {
        return None;
    }
    Some(Ok(LockStatement {
        stmt,
        calls: extractor.calls,
        is_do,
        placeholder_count: extractor.placeholders,
    }))
}

/// Walks the AST once: counts placeholders in visiting order, and replaces
/// every lock-function call with a sentinel, recording the call.
///
/// A **pre-order** visitor on purpose: a call's arguments are consumed (and
/// their placeholders numbered) by the call itself, and replacing the node
/// before its children are visited is what keeps them from being counted a
/// second time. `visit_expressions_mut` is post-order, so it cannot be used.
#[derive(Default)]
struct Extractor {
    calls: Vec<LockCall>,
    placeholders: usize,
}

impl VisitorMut for Extractor {
    type Break = TranslateError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        self.visit_expr(expr)
    }
}

impl Extractor {
    fn visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<TranslateError> {
        match expr {
            Expr::Value(v) if matches!(v.value, Value::Placeholder(_)) => {
                self.placeholders += 1;
                ControlFlow::Continue(())
            }
            Expr::Function(func) => {
                let Some(function) = LockFunction::from_name(&func.name.to_string()) else {
                    return ControlFlow::Continue(());
                };
                let args = match self.lock_args(function, &func.args) {
                    Ok(args) => args,
                    Err(e) => return ControlFlow::Break(e),
                };
                let index = self.calls.len();
                self.calls.push(LockCall { function, args });
                // Replacing the node drops its children, so the visitor does
                // not descend into (and double-count) the arguments.
                *expr = sentinel(index);
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Continue(()),
        }
    }

    fn lock_args(
        &mut self,
        function: LockFunction,
        args: &FunctionArguments,
    ) -> Result<Vec<LockArg>, TranslateError> {
        let list: Vec<&FunctionArg> = match args {
            FunctionArguments::None => Vec::new(),
            FunctionArguments::List(list) => list.args.iter().collect(),
            FunctionArguments::Subquery(_) => {
                return Err(TranslateError::Unsupported(format!(
                    "{}: a subquery is not a valid argument",
                    function.name()
                )));
            }
        };
        if list.len() != function.arity() {
            return Err(TranslateError::Unsupported(format!(
                "Incorrect parameter count in the call to native function '{}'",
                function.name()
            )));
        }
        list.into_iter()
            .map(|arg| {
                let expr = match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                    _ => {
                        return Err(TranslateError::Unsupported(format!(
                            "{}: arguments must be literals or placeholders",
                            function.name()
                        )));
                    }
                };
                self.lock_arg(function, expr)
            })
            .collect()
    }

    fn lock_arg(&mut self, function: LockFunction, expr: &Expr) -> Result<LockArg, TranslateError> {
        match expr {
            Expr::Value(v) => match &v.value {
                Value::Null => Ok(LockArg::Null),
                Value::SingleQuotedString(s)
                | Value::DoubleQuotedString(s)
                | Value::NationalStringLiteral(s) => Ok(LockArg::Text(s.clone())),
                Value::Number(n, _) => Ok(LockArg::Number(n.clone())),
                Value::Placeholder(_) => {
                    let ordinal = self.placeholders;
                    self.placeholders += 1;
                    Ok(LockArg::Placeholder(ordinal))
                }
                _ => Err(unsupported_arg(function, expr)),
            },
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => match inner.as_ref() {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => Ok(LockArg::Number(format!("-{n}"))),
                    _ => Err(unsupported_arg(function, expr)),
                },
                _ => Err(unsupported_arg(function, expr)),
            },
            Expr::Nested(inner) => self.lock_arg(function, inner),
            _ => Err(unsupported_arg(function, expr)),
        }
    }
}

fn unsupported_arg(function: LockFunction, expr: &Expr) -> TranslateError {
    TranslateError::Unsupported(format!(
        "{}: argument `{expr}` is not supported; arguments must be literals or placeholders",
        function.name()
    ))
}

/// Whether `expr` contains a lock-function call anywhere inside it.
fn contains_lock_call(expr: &Expr) -> bool {
    visit_expressions(expr, |e| {
        if let Expr::Function(f) = e
            && LockFunction::from_name(&f.name.to_string()).is_some()
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

/// Give every unaliased top-level projection item that contains a lock call
/// an alias equal to its original text, so the result column keeps MySQL's
/// name for it (`GET_LOCK('x', 0)`) after the call has been substituted.
fn alias_lock_projections(stmt: &mut Statement) {
    let Statement::Query(query) = stmt else {
        return;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return;
    };
    for item in &mut select.projection {
        if let SelectItem::UnnamedExpr(expr) = item
            && contains_lock_call(expr)
        {
            let alias = Ident::with_quote('`', expr.to_string());
            let expr = std::mem::replace(expr, Expr::Value(Value::Null.with_empty_span()));
            *item = SelectItem::ExprWithAlias { expr, alias };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(sql: &str) -> LockStatement {
        detect(sql, Dialect::MySQL)
            .expect("a lock statement")
            .expect("well-formed")
    }

    #[test]
    fn non_lock_statements_are_not_detected() {
        for sql in [
            "SELECT 1",
            "SELECT * FROM job_locks WHERE name = 'GET_LOCK'",
            "SELECT * FROM t WHERE v = 'RELEASE_LOCK(x)'",
            "INSERT INTO t_lock (a) VALUES (1)",
            "SELECT DOUBLE(1)",
        ] {
            assert!(detect(sql, Dialect::MySQL).is_none(), "{sql}");
        }
        assert!(detect("SELECT GET_LOCK('x', 0)", Dialect::PostgreSQL).is_none());
    }

    #[test]
    fn sendy_shape_select_with_alias() {
        let p = plan(r#"SELECT GET_LOCK("sendy_campaign_send_7", 0) AS lock_status"#);
        assert!(!p.is_do);
        assert_eq!(p.placeholder_count, 0);
        assert_eq!(
            p.calls,
            vec![LockCall {
                function: LockFunction::GetLock,
                args: vec![
                    LockArg::Text("sendy_campaign_send_7".into()),
                    LockArg::Number("0".into())
                ],
            }]
        );
        let sql = p.to_sqlite(&[LockValue::Int(1)]).unwrap();
        assert_eq!(sql, "SELECT 1 AS lock_status");
    }

    #[test]
    fn sendy_shape_do_release() {
        let p = plan(r#"DO RELEASE_LOCK("sendy_campaign_send_7")"#);
        assert!(p.is_do);
        assert_eq!(p.calls[0].function, LockFunction::ReleaseLock);
        let sql = p.to_sqlite(&[LockValue::Null]).unwrap();
        assert_eq!(
            sql,
            "SELECT NULL AS `RELEASE_LOCK(\"sendy_campaign_send_7\")`"
        );
    }

    #[test]
    fn do_without_lock_calls_is_still_a_do() {
        let p = plan("DO 1");
        assert!(p.is_do);
        assert!(p.calls.is_empty());
        assert_eq!(p.to_sqlite(&[]).unwrap(), "SELECT 1");
    }

    #[test]
    fn unaliased_call_keeps_mysql_column_name() {
        let p = plan("select get_lock('x', 10)");
        let sql = p.to_sqlite(&[LockValue::Int(0)]).unwrap();
        assert_eq!(sql, "SELECT 0 AS `get_lock('x', 10)`");
    }

    #[test]
    fn calls_alongside_other_columns_and_expressions() {
        let p = plan(
            "SELECT IS_FREE_LOCK('a'), IS_USED_LOCK('a') AS holder, 7 AS n, RELEASE_ALL_LOCKS() + 0",
        );
        assert_eq!(p.calls.len(), 3);
        assert_eq!(p.calls[2].function, LockFunction::ReleaseAllLocks);
        assert!(p.calls[2].args.is_empty());
        let sql = p
            .to_sqlite(&[LockValue::Int(1), LockValue::Null, LockValue::Int(2)])
            .unwrap();
        assert_eq!(
            sql,
            "SELECT 1 AS `IS_FREE_LOCK('a')`, NULL AS holder, 7 AS n, 2 + 0 AS `RELEASE_ALL_LOCKS() + 0`"
        );
    }

    #[test]
    fn placeholders_get_ordinals_in_source_order_and_are_removed_from_the_sqlite_text() {
        let p = plan("SELECT ? AS tag, GET_LOCK(?, ?) AS got, ? AS other");
        assert_eq!(p.placeholder_count, 4);
        assert_eq!(
            p.calls[0].args,
            vec![LockArg::Placeholder(1), LockArg::Placeholder(2)]
        );
        assert_eq!(p.consumed_placeholders(), vec![1, 2]);
        let sql = p.to_sqlite(&[LockValue::Int(1)]).unwrap();
        assert_eq!(sql, "SELECT ? AS tag, 1 AS got, ? AS other");
    }

    #[test]
    fn negative_and_null_arguments() {
        let p = plan("SELECT GET_LOCK('x', -1), GET_LOCK(NULL, 5)");
        assert_eq!(p.calls[0].args[1], LockArg::Number("-1".into()));
        assert_eq!(p.calls[1].args[0], LockArg::Null);
    }

    #[test]
    fn wrong_arity_and_non_literal_arguments_are_errors() {
        for sql in [
            "SELECT GET_LOCK('x')",
            "SELECT RELEASE_LOCK()",
            "SELECT RELEASE_ALL_LOCKS('x')",
            "SELECT GET_LOCK(CONCAT('a', 'b'), 0)",
            "SELECT GET_LOCK(name, 0) FROM t",
            "SELECT GET_LOCK((SELECT 1), 0)",
        ] {
            assert!(
                matches!(detect(sql, Dialect::MySQL), Some(Err(_))),
                "{sql} must be rejected"
            );
        }
    }

    #[test]
    fn call_in_where_clause_is_evaluated_once_and_substituted() {
        let p = plan("SELECT id FROM t WHERE GET_LOCK('x', 0) = 1 AND id > ?");
        assert_eq!(p.placeholder_count, 1);
        assert!(p.consumed_placeholders().is_empty());
        let sql = p.to_sqlite(&[LockValue::Int(1)]).unwrap();
        assert_eq!(sql, "SELECT id FROM t WHERE 1 = 1 AND id > ?");
    }

    #[test]
    fn select_hints_are_tolerated() {
        let p = plan("SELECT SQL_NO_CACHE GET_LOCK('x', 0) AS s");
        assert_eq!(p.to_sqlite(&[LockValue::Int(1)]).unwrap(), "SELECT 1 AS s");
    }

    #[test]
    fn do_with_two_statements_is_rejected() {
        assert!(matches!(detect("DO 1; DO 2", Dialect::MySQL), Some(Err(_))));
    }
}
