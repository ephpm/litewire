//! Common SQL rewrites shared across all dialects.
//!
//! Handles expression-level transformations: function calls, boolean literals,
//! type casts, and parameter placeholders.

use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, Value, ValueWithSpan,
};

use crate::TranslateError;

/// Apply common rewrites to a statement in-place.
pub fn rewrite_statement(stmt: &mut Statement) -> Result<(), TranslateError> {
    rewrite_statement_exprs(stmt);
    Ok(())
}

/// Walk a statement and rewrite expressions.
fn rewrite_statement_exprs(stmt: &mut Statement) {
    match stmt {
        Statement::Query(query) => rewrite_query_exprs(query),
        Statement::Insert(insert) => {
            if let Some(source) = &mut insert.source {
                rewrite_query_exprs(source);
            }
        }
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for assign in assignments {
                rewrite_expr(&mut assign.value);
            }
            if let Some(sel) = selection {
                rewrite_expr(sel);
            }
        }
        Statement::Delete(delete) => {
            if let Some(sel) = &mut delete.selection {
                rewrite_expr(sel);
            }
        }
        _ => {}
    }
}

/// Walk a query and rewrite expressions.
fn rewrite_query_exprs(query: &mut sqlparser::ast::Query) {
    // Handle VALUES clauses (e.g., INSERT ... VALUES (NOW())).
    if let sqlparser::ast::SetExpr::Values(values) = query.body.as_mut() {
        for row in &mut values.rows {
            for expr in row {
                rewrite_expr(expr);
            }
        }
        return;
    }

    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
        for item in &mut select.projection {
            if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
            {
                rewrite_expr(expr);
            }
        }
        if let Some(sel) = &mut select.selection {
            rewrite_expr(sel);
        }
        if let Some(having) = &mut select.having {
            rewrite_expr(having);
        }
    }
}

/// Rewrite a single expression to SQLite-compatible form.
fn rewrite_expr(expr: &mut Expr) {
    match expr {
        Expr::Function(func) => {
            // `RAND()` has no SQLite scalar. Replace the *whole expression* with
            // a float in [0, 1) derived from `random()`; this cannot live in
            // `rewrite_function`, which only sees the `Function` node and must
            // keep it a function call.
            if is_zero_arg_call(func, "RAND") {
                *expr = rand_to_unit_float();
            } else {
                rewrite_function(func);
                if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &mut func.args {
                    for arg in args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            rewrite_expr(e);
                        }
                    }
                }
            }
        }
        // `CAST(x AS DATE)`: Turso has no DATE type and the declared type `DATE`
        // carries NUMERIC affinity, so the string is coerced to its leading
        // number (`CAST('2024-01-15' AS DATE)` -> 2024). Rewrite to SQLite's
        // `date(x)`, which returns the ISO date.
        Expr::Cast {
            data_type: DataType::Date,
            ..
        } => {
            let taken = std::mem::replace(expr, value_expr(Value::Null));
            if let Expr::Cast {
                expr: mut inner, ..
            } = taken
            {
                rewrite_expr(&mut inner);
                *expr = Expr::Function(Function {
                    name: func_name("date"),
                    args: func_args(vec![*inner]),
                    filter: None,
                    over: None,
                    within_group: vec![],
                    parameters: FunctionArguments::None,
                    null_treatment: None,
                    uses_odbc_syntax: false,
                });
            }
        }
        Expr::Value(val_with_span) => {
            rewrite_value(&mut val_with_span.value);
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left);
            rewrite_expr(right);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            rewrite_expr(inner);
        }
        Expr::Nested(inner) => {
            rewrite_expr(inner);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            rewrite_expr(inner);
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            rewrite_expr(inner);
            for e in list {
                rewrite_expr(e);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            rewrite_expr(inner);
            rewrite_expr(low);
            rewrite_expr(high);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                rewrite_expr(op);
            }
            for cw in conditions {
                rewrite_expr(&mut cw.condition);
                rewrite_expr(&mut cw.result);
            }
            if let Some(el) = else_result {
                rewrite_expr(el);
            }
        }
        Expr::Subquery(q) => {
            rewrite_query_exprs(q);
        }
        Expr::CompoundIdentifier(parts) => {
            let joined = parts
                .iter()
                .map(|p| p.value.to_ascii_uppercase())
                .collect::<Vec<_>>()
                .join(".");
            match joined.as_str() {
                "@@IDENTITY" => {
                    *expr = Expr::Function(Function {
                        name: func_name("last_insert_rowid"),
                        args: func_args(vec![]),
                        filter: None,
                        over: None,
                        within_group: vec![],
                        parameters: FunctionArguments::None,
                        null_treatment: None,
                        uses_odbc_syntax: false,
                    });
                }
                "@@ROWCOUNT" => {
                    *expr = Expr::Function(Function {
                        name: func_name("changes"),
                        args: func_args(vec![]),
                        filter: None,
                        over: None,
                        within_group: vec![],
                        parameters: FunctionArguments::None,
                        null_treatment: None,
                        uses_odbc_syntax: false,
                    });
                }
                _ => {}
            }
        }
        _ => {}
    }
}

/// True if `func` is a call to `name` (case-insensitive) with no arguments,
/// e.g. `RAND()`.
fn is_zero_arg_call(func: &Function, name: &str) -> bool {
    if !func.name.to_string().eq_ignore_ascii_case(name) {
        return false;
    }
    match &func.args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty(),
        FunctionArguments::Subquery(_) => false,
    }
}

/// Build `(0.5 + random() / 18446744073709551616.0)`, a float in `[0, 1)` — the
/// SQLite stand-in for MySQL `RAND()`. `random()` is a signed 64-bit integer in
/// `[-2^63, 2^63)`; dividing by `2^64` (as a float, forcing float division)
/// gives `[-0.5, 0.5)`, and `+ 0.5` shifts it to `[0, 1)`.
fn rand_to_unit_float() -> Expr {
    let random_call = Expr::Function(Function {
        name: func_name("random"),
        args: func_args(vec![]),
        filter: None,
        over: None,
        within_group: vec![],
        parameters: FunctionArguments::None,
        null_treatment: None,
        uses_odbc_syntax: false,
    });
    let divided = Expr::BinaryOp {
        left: Box::new(random_call),
        op: BinaryOperator::Divide,
        right: Box::new(value_expr(Value::Number(
            "18446744073709551616.0".into(),
            false,
        ))),
    };
    let sum = Expr::BinaryOp {
        left: Box::new(value_expr(Value::Number("0.5".into(), false))),
        op: BinaryOperator::Plus,
        right: Box::new(divided),
    };
    Expr::Nested(Box::new(sum))
}

/// Helper to create a `ValueWithSpan` from a `Value`.
pub(crate) fn value_expr(val: Value) -> Expr {
    Expr::Value(ValueWithSpan {
        value: val,
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Helper to build a function name `ObjectName`.
pub(crate) fn func_name(name: &str) -> ObjectName {
    ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
        Ident::new(name),
    )])
}

/// Helper to build function args list.
pub(crate) fn func_args(args: Vec<Expr>) -> FunctionArguments {
    FunctionArguments::List(FunctionArgumentList {
        args: args
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect(),
        duplicate_treatment: None,
        clauses: vec![],
    })
}

/// Turn `func` into a call that evaluates to the constant string `text`.
///
/// The rewrite pass only ever sees the [`Function`] node, never its parent
/// [`Expr`], so a built-in with no SQLite analogue cannot be replaced by a
/// bare literal — it has to stay a function call. That call has to be one
/// SQLite actually accepts with a single argument, which rules out the
/// obvious `coalesce()`: SQLite requires at least two arguments and rejects
/// `coalesce('x')` at prepare time with "wrong number of arguments". Every
/// one of these built-ins used to emit exactly that, so none of them worked
/// at runtime; the unit tests only ever inspected the emitted SQL string.
///
/// `trim()` takes one argument and returns it unchanged when there is no
/// surrounding whitespace, which there never is in a constant we chose.
fn constant_text(func: &mut Function, text: &str) {
    func.name = func_name("trim");
    func.args = func_args(vec![value_expr(Value::SingleQuotedString(text.into()))]);
}

/// Turn `func` into a call that evaluates to the constant number `number`.
///
/// `abs()` is the numeric counterpart to [`constant_text`]'s `trim()`: a
/// single-argument SQLite scalar that returns a non-negative constant
/// unchanged.
fn constant_number(func: &mut Function, number: &str) {
    func.name = func_name("abs");
    func.args = func_args(vec![value_expr(Value::Number(number.into(), false))]);
}

/// Rewrite function calls to SQLite equivalents.
fn rewrite_function(func: &mut Function) {
    let name_upper = func.name.to_string().to_ascii_uppercase();

    match name_upper.as_str() {
        "NOW" | "CURRENT_TIMESTAMP" | "GETDATE" | "GETUTCDATE" => {
            func.name = func_name("datetime");
            func.args = func_args(vec![value_expr(Value::SingleQuotedString("now".into()))]);
        }
        "CURDATE" | "CURRENT_DATE" => {
            func.name = func_name("date");
            func.args = func_args(vec![value_expr(Value::SingleQuotedString("now".into()))]);
        }
        "ISNULL" => {
            func.name = func_name("IFNULL");
        }
        // MySQL LAST_INSERT_ID() -> SQLite last_insert_rowid().
        // Note: the MySQL 2-arg form LAST_INSERT_ID(expr) sets the session's
        // last-insert-id; SQLite has no equivalent. We rewrite the name and let
        // SQLite's function-arity check reject the 2-arg form loudly rather
        // than silently changing semantics.
        "LAST_INSERT_ID" => {
            func.name = func_name("last_insert_rowid");
        }
        // MySQL ROW_COUNT() -> SQLite changes().
        "ROW_COUNT" => {
            func.name = func_name("changes");
        }
        // Session-identity built-ins. SQLite has no analogue, so we return
        // constant placeholders that mirror the values used by the metadata
        // fast path in `metadata::system_variable_value`.
        //   DATABASE() / SCHEMA()       -> 'main'
        //   VERSION()                   -> crate::SERVER_VERSION
        //   USER() / CURRENT_USER() /
        //   SESSION_USER() / SYSTEM_USER() -> 'root@localhost'
        //   CONNECTION_ID()             -> 0  (SQLite has no per-connection ID)
        "DATABASE" | "SCHEMA" => {
            constant_text(func, "main");
        }
        "VERSION" => {
            constant_text(func, crate::SERVER_VERSION);
        }
        // FOUND_ROWS(): the paired SQL_CALC_FOUND_ROWS hint is stripped
        // before parsing, and stateless translation cannot know the count.
        // `litewire_session::Session` intercepts a bare `SELECT
        // FOUND_ROWS()` before translation and answers it from session
        // state (see `crate::found_rows`); this 0 shim only remains for
        // FOUND_ROWS() calls embedded in larger expressions and for
        // callers using `translate()` without a session.
        "FOUND_ROWS" => {
            constant_number(func, "0");
        }
        "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" => {
            constant_text(func, "root@localhost");
        }
        "CONNECTION_ID" => {
            constant_number(func, "0");
        }
        "NEWID" => {
            // NEWID() -> lower(hex(randomblob(16)))
            func.name = func_name("lower");
            func.args = func_args(vec![Expr::Function(Function {
                name: func_name("hex"),
                args: func_args(vec![Expr::Function(Function {
                    name: func_name("randomblob"),
                    args: func_args(vec![value_expr(Value::Number("16".into(), false))]),
                    filter: None,
                    over: None,
                    within_group: vec![],
                    parameters: FunctionArguments::None,
                    null_treatment: None,
                    uses_odbc_syntax: false,
                })]),
                filter: None,
                over: None,
                within_group: vec![],
                parameters: FunctionArguments::None,
                null_treatment: None,
                uses_odbc_syntax: false,
            })]);
        }
        _ => {}
    }
}

/// Rewrite literal values.
fn rewrite_value(val: &mut Value) {
    match val {
        Value::Boolean(b) => {
            *val = Value::Number(if *b { "1".into() } else { "0".into() }, false);
        }
        Value::Placeholder(p) => {
            if let Some(rest) = p.strip_prefix('$')
                && rest.chars().all(|c| c.is_ascii_digit())
            {
                *p = format!("?{rest}");
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Dialect, TranslateResult, translate};

    #[test]
    fn boolean_rewrite() {
        let mut v = Value::Boolean(true);
        rewrite_value(&mut v);
        assert_eq!(v, Value::Number("1".into(), false));

        let mut v = Value::Boolean(false);
        rewrite_value(&mut v);
        assert_eq!(v, Value::Number("0".into(), false));
    }

    // ── Function call rewrites ──────────────────────────────────────────────

    #[test]
    fn now_rewrite() {
        let results = translate("SELECT NOW()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_uppercase().contains("DATETIME"), "got: {sql}");
        assert!(sql.contains("'now'"), "got: {sql}");
    }

    #[test]
    fn curdate_rewrite() {
        let results = translate("SELECT CURDATE()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("date"), "got: {sql}");
        assert!(sql.contains("'now'"), "got: {sql}");
    }

    #[test]
    fn unix_timestamp_rewrite() {
        let results = translate("SELECT UNIX_TIMESTAMP()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("strftime"), "got: {sql}");
        assert!(sql.contains("'%s'"), "got: {sql}");
    }

    #[test]
    fn isnull_rewrite() {
        let results = translate("SELECT ISNULL(a, b) FROM t", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_uppercase().contains("IFNULL"), "got: {sql}");
    }

    #[test]
    fn getdate_rewrite() {
        let results = translate("SELECT GETDATE()", Dialect::TDS).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_uppercase().contains("DATETIME"), "got: {sql}");
    }

    // ── Expression tree walking ─────────────────────────────────────────────

    #[test]
    fn nested_function_in_binary_op() {
        let results = translate("SELECT NOW() > '2024-01-01'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("datetime"), "got: {sql}");
    }

    #[test]
    fn boolean_in_where_clause() {
        let results = translate("SELECT * FROM t WHERE active = TRUE", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains('1'), "got: {sql}");
    }

    #[test]
    fn function_in_insert_values() {
        let results = translate("INSERT INTO t (created) VALUES (NOW())", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("datetime"), "got: {sql}");
    }

    #[test]
    fn function_in_update_set() {
        let results =
            translate("UPDATE t SET updated = NOW() WHERE id = 1", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("datetime"), "got: {sql}");
    }

    #[test]
    fn boolean_in_case_expression() {
        let results = translate(
            "SELECT CASE WHEN x = TRUE THEN 'yes' ELSE 'no' END FROM t",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains('1'), "got: {sql}");
    }

    // -- LAST_INSERT_ID / ROW_COUNT / DATABASE / VERSION / USER / CONNECTION_ID --

    #[test]
    fn last_insert_id_rewrite() {
        let results = translate("SELECT LAST_INSERT_ID()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.to_ascii_lowercase().contains("last_insert_rowid"),
            "got: {sql}"
        );
        assert!(
            !sql.to_ascii_uppercase().contains("LAST_INSERT_ID("),
            "got: {sql}"
        );
    }

    #[test]
    fn row_count_rewrite() {
        let results = translate("SELECT ROW_COUNT()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("changes"), "got: {sql}");
        assert!(
            !sql.to_ascii_uppercase().contains("ROW_COUNT("),
            "got: {sql}"
        );
    }

    #[test]
    fn database_rewrite() {
        let results = translate("SELECT DATABASE()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("'main'"), "got: {sql}");
    }

    #[test]
    fn version_rewrite() {
        let results = translate("SELECT VERSION()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        // Deliberate behaviour change (issue #21): this used to answer
        // `8.0.0-litewire` while the wire handshake advertised
        // `8.0.36-litewire`. Asserted against the shared constant rather
        // than a second copy of the literal, so this test cannot be the
        // thing that lets them drift apart again.
        assert!(sql.contains(crate::SERVER_VERSION), "got: {sql}");
        assert!(sql.contains("8.0.36-litewire"), "got: {sql}");
    }

    #[test]
    fn current_user_rewrite() {
        let results = translate("SELECT CURRENT_USER()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("'root@localhost'"), "got: {sql}");
    }

    #[test]
    fn connection_id_rewrite() {
        let results = translate("SELECT CONNECTION_ID()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        // Must produce a numeric literal 0 in the emitted SQL.
        assert!(sql.contains('0'), "got: {sql}");
        assert!(
            !sql.to_ascii_uppercase().contains("CONNECTION_ID("),
            "got: {sql}"
        );
    }

    // -- NEWID rewrite --------------------------------------------------------

    #[test]
    fn newid_rewrite() {
        let results = translate("SELECT NEWID()", Dialect::TDS).unwrap();
        let sql = extract_sql(&results[0]);
        let lower = sql.to_ascii_lowercase();
        assert!(lower.contains("lower"), "got: {sql}");
        assert!(lower.contains("hex"), "got: {sql}");
        assert!(lower.contains("randomblob"), "got: {sql}");
    }

    // ── Placeholder rewrite ────────────────────────────────────────────────

    #[test]
    fn dollar_placeholder_to_question_mark() {
        let results = translate("SELECT * FROM t WHERE id = $1", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("?1"), "got: {sql}");
        assert!(!sql.contains("$1"), "got: {sql}");
    }

    #[test]
    fn multiple_dollar_placeholders() {
        let results = translate(
            "SELECT * FROM t WHERE a = $1 AND b = $2",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("?1"), "got: {sql}");
        assert!(sql.contains("?2"), "got: {sql}");
    }

    #[test]
    fn rand_rewrite() {
        // `RAND()` -> a float in [0, 1) over `random()`; executes on Turso
        // (which has no `RAND`) and is non-deterministic, so only the shape is
        // asserted here (correctness is coverage-only in the compat suite).
        let results = translate("SELECT RAND()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("random()"), "got: {sql}");
        assert!(!sql.to_ascii_uppercase().contains("RAND("), "got: {sql}");
        assert!(sql.contains("0.5"), "got: {sql}");
    }

    #[test]
    fn rand_rewrite_in_where_clause() {
        let results = translate("SELECT id FROM t WHERE score > RAND()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_lowercase().contains("random()"), "got: {sql}");
        assert!(!sql.to_ascii_uppercase().contains("RAND("), "got: {sql}");
    }

    #[test]
    fn rand_with_seed_is_left_alone() {
        // The seeded form has no SQLite analogue; leave it for the engine to
        // reject loudly rather than silently drop the seed.
        let results = translate("SELECT RAND(7)", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_uppercase().contains("RAND("), "got: {sql}");
    }

    #[test]
    fn cast_as_date_rewrite() {
        // `CAST('2024-01-15' AS DATE)` -> `date('2024-01-15')` so Turso returns
        // the ISO date instead of coercing to the leading number (2024).
        for dialect in [Dialect::PostgreSQL, Dialect::MySQL] {
            let results = translate("SELECT CAST('2024-01-15' AS DATE)", dialect).unwrap();
            let sql = extract_sql(&results[0]);
            assert!(sql.to_ascii_lowercase().contains("date("), "got: {sql}");
            assert!(!sql.to_ascii_uppercase().contains("CAST("), "got: {sql}");
            assert!(sql.contains("'2024-01-15'"), "got: {sql}");
        }
    }

    #[test]
    fn cast_as_other_types_unchanged() {
        // Only DATE is special-cased; other casts pass through.
        let results = translate("SELECT CAST(x AS INTEGER) FROM t", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_uppercase().contains("CAST("), "got: {sql}");
    }

    // ── Helper ──────────────────────────────────────────────────────────────

    fn extract_sql(result: &TranslateResult) -> &str {
        match result {
            TranslateResult::Sql(s) => s.as_str(),
            other => panic!("expected Sql, got: {other:?}"),
        }
    }
}
