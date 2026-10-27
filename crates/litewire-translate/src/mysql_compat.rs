//! MySQL statement and function forms that SQLite spells differently,
//! found by replaying a self-migrating PHP application (Sendy 7.1.6) and its
//! updater against MySQL 8.4 and litewire side by side.
//!
//! Text pre-passes run before sqlparser (for syntax it cannot parse); AST
//! rewrites run with the other MySQL rewrites in [`crate::mysql`].

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, Value,
    visit_expressions_mut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::TranslateError;
use crate::metadata::{Lexed, Tok, lex};

fn is_word(toks: &[Lexed], i: usize, w: &str) -> bool {
    matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w))
}

fn is_other(toks: &[Lexed], i: usize, c: char) -> bool {
    matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Other(x)) if *x == c)
}

/// Byte offset just past token `i`.
fn tok_end(sql: &str, toks: &[Lexed], i: usize) -> usize {
    let start = toks[i].start;
    match &toks[i].tok {
        Tok::Word(w) => start + w.len(),
        Tok::Other(c) => start + c.len_utf8(),
        Tok::Dot | Tok::Semi => start + 1,
        // Quoted tokens: find the matching close quote by re-lexing is
        // unnecessary here; callers only splice around words/punctuation.
        Tok::Quoted(_) | Tok::Str(_) => toks.get(i + 1).map_or(sql.len(), |n| n.start),
    }
}

/// Apply `(start, end, replacement)` edits (sorted, non-overlapping).
fn splice(sql: &str, edits: &[(usize, usize, String)]) -> String {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut last = 0;
    for (start, end, rep) in edits {
        out.push_str(&sql[last..*start]);
        out.push_str(rep);
        last = *end;
    }
    out.push_str(&sql[last..]);
    out
}

// ── `||` / `&&` ─────────────────────────────────────────────────────────────

/// MySQL's `||` and `&&` are logical `OR` / `AND` (unless `sql_mode` has
/// `PIPES_AS_CONCAT`, which no PHP application assumes). In SQLite `||` is
/// string concatenation, so `WHERE Field = 'a' || Field = 'b'` would parse
/// as `Field = ('a' || Field) = 'b'` and match nothing — the probe silently
/// reports the column missing. Quote- and comment-aware.
pub(crate) fn logical_operators(sql: &str) -> Option<String> {
    if !sql.contains("||") && !sql.contains("&&") {
        return None;
    }
    let toks = lex(sql).ok()?;
    let mut edits = Vec::new();
    let mut i = 0;
    while i + 1 < toks.len() {
        for (c, word) in [('|', " OR "), ('&', " AND ")] {
            if is_other(&toks, i, c)
                && is_other(&toks, i + 1, c)
                && toks[i + 1].start == toks[i].start + 1
            {
                edits.push((toks[i].start, toks[i].start + 2, word.to_string()));
                i += 1;
                break;
            }
        }
        i += 1;
    }
    (!edits.is_empty()).then(|| splice(sql, &edits))
}

// ── ALTER TABLE ... ADD (col def, col def) ──────────────────────────────────

/// MySQL's parenthesized multi-column add, `ALTER TABLE t ADD (a INT, b
/// TEXT)`, which sqlparser cannot parse, becomes `ADD COLUMN a INT, ADD
/// COLUMN b TEXT`. `ADD (` followed by a key keyword is left alone.
pub(crate) fn parenthesized_add_columns(sql: &str) -> Option<String> {
    let toks = lex(sql).ok()?;
    if !(is_word(&toks, 0, "ALTER") && is_word(&toks, 1, "TABLE")) {
        return None;
    }
    let mut edits = Vec::new();
    let mut i = 2;
    while i < toks.len() {
        if is_word(&toks, i, "ADD") && is_other(&toks, i + 1, '(') {
            let first = i + 2;
            let key_like = [
                "KEY",
                "INDEX",
                "PRIMARY",
                "UNIQUE",
                "CONSTRAINT",
                "FOREIGN",
                "CHECK",
                "FULLTEXT",
                "SPATIAL",
            ]
            .iter()
            .any(|k| is_word(&toks, first, k));
            if !key_like {
                // Split the parenthesized list at top-level commas.
                let mut depth = 0i32;
                let mut parts = Vec::new();
                let mut part_start = toks.get(first).map_or(sql.len(), |l| l.start);
                let mut j = i + 1;
                let mut close = None;
                while j < toks.len() {
                    match toks[j].tok {
                        Tok::Other('(') => depth += 1,
                        Tok::Other(')') => {
                            depth -= 1;
                            if depth == 0 {
                                parts.push(sql[part_start..toks[j].start].trim().to_string());
                                close = Some(j);
                                break;
                            }
                        }
                        Tok::Other(',') if depth == 1 => {
                            parts.push(sql[part_start..toks[j].start].trim().to_string());
                            part_start = tok_end(sql, &toks, j);
                        }
                        _ => {}
                    }
                    j += 1;
                }
                let close = close?;
                let rep = parts
                    .iter()
                    .map(|p| format!("ADD COLUMN {p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                edits.push((toks[i].start, tok_end(sql, &toks, close), rep));
                i = close;
            }
        }
        i += 1;
    }
    (!edits.is_empty()).then(|| splice(sql, &edits))
}

/// Remove MySQL index-type clauses (`USING BTREE`, `USING HASH`) from table
/// DDL; sqlparser cannot parse them after a key's column list, and SQLite
/// indexes are always B-trees.
pub(crate) fn strip_index_type(sql: &str) -> Option<String> {
    let toks = lex(sql).ok()?;
    let mut edits = Vec::new();
    for i in 0..toks.len().saturating_sub(1) {
        if is_word(&toks, i, "USING")
            && (is_word(&toks, i + 1, "BTREE") || is_word(&toks, i + 1, "HASH"))
        {
            edits.push((toks[i].start, tok_end(sql, &toks, i + 1), String::new()));
        }
    }
    (!edits.is_empty()).then(|| splice(sql, &edits))
}

// ── UPDATE ... [ORDER BY ...] LIMIT n ────────────────────────────────────────

/// A single-table `UPDATE` with a trailing `[ORDER BY ...] LIMIT n` (which
/// sqlparser does not parse for UPDATE): the statement without the tail,
/// and the tail.
pub(crate) fn split_update_limit(sql: &str) -> Option<(String, String)> {
    let toks = lex(sql).ok()?;
    if !is_word(&toks, 0, "UPDATE") {
        return None;
    }
    let mut depth = 0i32;
    let mut tail_start = None;
    for (i, l) in toks.iter().enumerate() {
        match l.tok {
            Tok::Other('(') => depth += 1,
            Tok::Other(')') => depth -= 1,
            Tok::Word(_)
                if depth == 0
                    && ((is_word(&toks, i, "ORDER") && is_word(&toks, i + 1, "BY"))
                        || is_word(&toks, i, "LIMIT")) =>
            {
                tail_start.get_or_insert(l.start);
            }
            _ => {}
        }
    }
    let start = tail_start?;
    let tail = sql[start..].trim().trim_end_matches(';').trim().to_string();
    if !tail.to_ascii_uppercase().contains("LIMIT") {
        return None;
    }
    Some((sql[..start].to_string(), tail))
}

/// Parse a MySQL expression.
fn parse_expr(text: &str) -> Result<Expr, TranslateError> {
    Parser::new(&MySqlDialect {})
        .try_with_sql(text)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| TranslateError::Parse(format!("{e} (in rewritten `{text}`)")))
}

/// Restrict an UPDATE/DELETE on `table` to the rows a `[ORDER BY] LIMIT`
/// tail selects: `WHERE rowid IN (SELECT rowid FROM table WHERE <w> <tail>)`
/// (SQLite's UPDATE/DELETE take no LIMIT unless specially compiled).
fn limit_by_rowid(
    table: &str,
    selection: Option<&Expr>,
    tail: &str,
) -> Result<Expr, TranslateError> {
    let w = selection.map_or_else(String::new, |s| format!(" WHERE {s}"));
    rowid_in(&format!(
        "SELECT rowid AS __litewire_rowid FROM {table}{w} {tail}"
    ))
}

/// `rowid IN (SELECT __litewire_rowid FROM (<query>) AS __litewire_limit)`;
/// `<query>` selects `rowid AS __litewire_rowid` (Turso names a bare
/// `rowid` column of a derived table after the INTEGER PRIMARY KEY). sqlparser
/// accepts neither `LIMIT` nor `ORDER BY` in an `IN` subquery, but does in a
/// derived table, so the limited query goes one level down.
fn rowid_in(query: &str) -> Result<Expr, TranslateError> {
    let outer = format!("SELECT __litewire_rowid FROM ({query}) AS __litewire_limit");
    let stmts = Parser::parse_sql(&MySqlDialect {}, &outer)
        .map_err(|e| TranslateError::Parse(format!("{e} (in rewritten `{outer}`)")))?;
    let Some(Statement::Query(q)) = stmts.into_iter().next() else {
        return Err(TranslateError::Parse(outer));
    };
    Ok(Expr::InSubquery {
        expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("rowid"))),
        subquery: q.body,
        negated: false,
    })
}

/// Apply a tail split off by [`split_update_limit`].
pub(crate) fn apply_update_limit(stmt: &mut Statement, tail: &str) -> Result<(), TranslateError> {
    let Statement::Update {
        table, selection, ..
    } = stmt
    else {
        return Err(TranslateError::Unsupported(
            "LIMIT on this statement".into(),
        ));
    };
    if !table.joins.is_empty() {
        return Err(TranslateError::Unsupported(
            "UPDATE with a join and LIMIT (MySQL rejects it too)".into(),
        ));
    }
    let name = table.relation.to_string();
    *selection = Some(limit_by_rowid(&name, selection.as_ref(), tail)?);
    Ok(())
}

// ── DELETE ... LIMIT / multi-table DELETE ────────────────────────────────────

/// `DELETE FROM t WHERE w [ORDER BY o] LIMIT n` and the single-target
/// multi-table form `DELETE a FROM t AS a JOIN t2 AS b ON ... WHERE w`,
/// both as `DELETE FROM t WHERE rowid IN (SELECT a.rowid ...)`.
pub(crate) fn rewrite_delete(stmt: &mut Statement) -> Result<(), TranslateError> {
    use sqlparser::ast::{FromTable, TableFactor};

    let Statement::Delete(del) = stmt else {
        return Ok(());
    };
    let from = match &del.from {
        FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => f.clone(),
    };

    if !del.tables.is_empty() {
        // DELETE <targets> FROM <joined tables> [WHERE].
        if del.tables.len() != 1 || del.using.is_some() {
            return Err(TranslateError::Unsupported(
                "DELETE from several tables at once".into(),
            ));
        }
        let target = del.tables[0].to_string();
        // The target names a table or an alias in FROM: find the real table.
        let mut real = None;
        for twj in &from {
            let factors =
                std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation));
            for f in factors {
                if let TableFactor::Table { name, alias, .. } = f {
                    let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                    if alias_name
                        .as_deref()
                        .is_some_and(|a| a.eq_ignore_ascii_case(&target))
                        || (alias.is_none() && name.to_string().eq_ignore_ascii_case(&target))
                    {
                        real = Some(name.to_string());
                    }
                }
            }
        }
        let real = real.ok_or_else(|| {
            TranslateError::Unsupported(format!("DELETE target {target} is not in FROM"))
        })?;
        let joined: Vec<String> = from.iter().map(ToString::to_string).collect();
        let w = del
            .selection
            .as_ref()
            .map_or_else(String::new, |s| format!(" WHERE {s}"));
        let sub = parse_expr(&format!(
            "rowid IN (SELECT {target}.rowid FROM {}{w})",
            joined.join(", ")
        ))?;
        let new_from = parse_from(&real)?;
        del.tables.clear();
        del.from = FromTable::WithFromKeyword(new_from);
        del.selection = Some(sub);
        return Ok(());
    }

    if let Some(limit) = del.limit.take() {
        let [single] = from.as_slice() else {
            return Err(TranslateError::Unsupported(
                "DELETE with a join and LIMIT".into(),
            ));
        };
        if !single.joins.is_empty() {
            return Err(TranslateError::Unsupported(
                "DELETE with a join and LIMIT".into(),
            ));
        }
        let order = if del.order_by.is_empty() {
            String::new()
        } else {
            format!(
                "ORDER BY {} ",
                del.order_by
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        del.order_by.clear();
        let name = match &single.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            other => other.to_string(),
        };
        del.selection = Some(limit_by_rowid(
            &name,
            del.selection.as_ref(),
            &format!("{order}LIMIT {limit}"),
        )?);
    }
    Ok(())
}

fn parse_from(table: &str) -> Result<Vec<sqlparser::ast::TableWithJoins>, TranslateError> {
    let stmts = Parser::parse_sql(&MySqlDialect {}, &format!("SELECT 1 FROM {table}"))
        .map_err(|e| TranslateError::Parse(e.to_string()))?;
    match stmts.into_iter().next() {
        Some(Statement::Query(q)) => match *q.body {
            sqlparser::ast::SetExpr::Select(s) => Ok(s.from),
            _ => Err(TranslateError::Parse(table.into())),
        },
        _ => Err(TranslateError::Parse(table.into())),
    }
}

// ── Multi-table UPDATE ───────────────────────────────────────────────────────

/// `UPDATE a JOIN b ON c SET a.x = <expr> WHERE w` → SQLite's
/// `UPDATE a SET x = <expr> FROM b WHERE c AND w`. Only the first table
/// may be assigned to; assignments to a joined table are rejected (MySQL
/// can update several tables at once, SQLite cannot), except for the
/// comma-join form where every assignment only reads its own table, which
/// [`split_multi_table_update`] splits into one UPDATE per table.
pub(crate) fn rewrite_update_join(stmt: &mut Statement) -> Result<(), TranslateError> {
    use sqlparser::ast::{
        AssignmentTarget, JoinConstraint, JoinOperator, TableFactor, UpdateTableFromKind,
    };

    let Statement::Update {
        table,
        assignments,
        from,
        selection,
        ..
    } = stmt
    else {
        return Ok(());
    };
    if table.joins.is_empty() || from.is_some() {
        return Ok(());
    }
    let target_alias = match &table.relation {
        TableFactor::Table { name, alias, .. } => alias
            .as_ref()
            .map_or_else(|| name.to_string(), |a| a.name.value.clone()),
        _ => {
            return Err(TranslateError::Unsupported(
                "UPDATE of a derived table".into(),
            ));
        }
    };
    // Every assignment must target the first table.
    for a in assignments.iter() {
        if let AssignmentTarget::ColumnName(n) = &a.target
            && n.0.len() > 1
            && !n.0[n.0.len() - 2]
                .to_string()
                .trim_matches('`')
                .eq_ignore_ascii_case(&target_alias)
        {
            return Err(TranslateError::Unsupported(
                "UPDATE assigning to a joined table (SQLite updates one table per statement)"
                    .into(),
            ));
        }
    }
    let mut conditions = Vec::new();
    let mut joined = Vec::new();
    for j in std::mem::take(&mut table.joins) {
        let constraint = match &j.join_operator {
            JoinOperator::Join(c) | JoinOperator::Inner(c) => Some(c.clone()),
            JoinOperator::CrossJoin => None,
            _ => {
                return Err(TranslateError::Unsupported(
                    "UPDATE with an outer join".into(),
                ));
            }
        };
        if let Some(JoinConstraint::On(on)) = constraint {
            conditions.push(on);
        }
        joined.push(sqlparser::ast::TableWithJoins {
            relation: j.relation,
            joins: vec![],
        });
    }
    if let Some(w) = selection.take() {
        conditions.push(w);
    }
    *selection = conditions.into_iter().reduce(|a, b| Expr::BinaryOp {
        left: Box::new(Expr::Nested(Box::new(a))),
        op: BinaryOperator::And,
        right: Box::new(Expr::Nested(Box::new(b))),
    });
    *from = Some(UpdateTableFromKind::AfterSet(joined));
    Ok(())
}

/// Tables an expression reads (qualifiers of compound identifiers).
fn qualifiers(e: &Expr) -> Vec<String> {
    let mut out = Vec::new();
    let mut e = e.clone();
    let _: ControlFlow<()> = visit_expressions_mut(&mut e, |x| {
        if let Expr::CompoundIdentifier(parts) = x
            && parts.len() >= 2
        {
            out.push(parts[parts.len() - 2].value.to_ascii_lowercase());
        }
        ControlFlow::Continue(())
    });
    out
}

/// `UPDATE a, b SET ...` (comma join): sqlparser only parses a joined
/// UPDATE target, so the top-level commas between `UPDATE` and `SET` become
/// `CROSS JOIN` — the same rows MySQL's comma join produces.
pub(crate) fn comma_update_tables(sql: &str) -> Option<String> {
    let toks = lex(sql).ok()?;
    if !is_word(&toks, 0, "UPDATE") {
        return None;
    }
    let mut depth = 0i32;
    let mut edits = Vec::new();
    for (i, l) in toks.iter().enumerate().skip(1) {
        match l.tok {
            Tok::Other('(') => depth += 1,
            Tok::Other(')') => depth -= 1,
            Tok::Other(',') if depth == 0 => {
                edits.push((l.start, l.start + 1, " CROSS JOIN".to_string()))
            }
            Tok::Word(_) if depth == 0 && is_word(&toks, i, "SET") => break,
            _ => {}
        }
    }
    (!edits.is_empty()).then(|| splice(sql, &edits))
}

/// The alias (or name) an UPDATE target factor is referred to by, and its
/// real table name.
fn factor_names(f: &sqlparser::ast::TableFactor) -> Option<(String, String)> {
    match f {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => Some((
            name.to_string(),
            alias
                .as_ref()
                .map_or_else(|| name.to_string(), |a| a.name.value.clone()),
        )),
        _ => None,
    }
}

/// The table alias an assignment target is qualified with, if any.
fn target_qualifier(a: &sqlparser::ast::Assignment) -> Option<String> {
    match &a.target {
        sqlparser::ast::AssignmentTarget::ColumnName(n) if n.0.len() > 1 => {
            Some(n.0[n.0.len() - 2].to_string().trim_matches('`').to_string())
        }
        _ => None,
    }
}

/// A joined `UPDATE` that assigns to more than one table (`UPDATE a, b SET
/// a.x = ..., b.y = ... WHERE w`): one `UPDATE t SET ... WHERE rowid IN
/// (SELECT t.rowid FROM <the join> WHERE w)` per assigned table, in order.
/// Allowed only when every value reads its own table (or nothing): MySQL
/// evaluates all assignments against the joined row, and once the first
/// table is updated the second statement would read new values. `None`
/// when the statement assigns to one table only (see
/// [`rewrite_update_join`]).
pub(crate) fn split_multi_table_update(
    stmt: &Statement,
) -> Result<Option<Vec<Statement>>, TranslateError> {
    use sqlparser::ast::{AssignmentTarget, ObjectName};

    let Statement::Update {
        table,
        assignments,
        from: None,
        selection,
        ..
    } = stmt
    else {
        return Ok(None);
    };
    if table.joins.is_empty() {
        return Ok(None);
    }
    let mut names = vec![factor_names(&table.relation)];
    names.extend(table.joins.iter().map(|j| factor_names(&j.relation)));
    let mut targets: Vec<String> = Vec::new();
    for a in assignments {
        if let Some(q) = target_qualifier(a)
            && !targets.iter().any(|t| t.eq_ignore_ascii_case(&q))
        {
            targets.push(q);
        }
    }
    if targets.len() < 2 {
        return Ok(None);
    }
    let from_text = table.to_string();
    let w = selection
        .as_ref()
        .map_or_else(String::new, |s| format!(" WHERE {s}"));
    let mut out = Vec::new();
    for alias in &targets {
        let real = names
            .iter()
            .flatten()
            .find(|(_, a)| a.eq_ignore_ascii_case(alias))
            .map(|(r, _)| r.clone())
            .ok_or_else(|| {
                TranslateError::Unsupported(format!("UPDATE target {alias} is not a table"))
            })?;
        let mut sets = Vec::new();
        for a in assignments
            .iter()
            .filter(|a| target_qualifier(a).is_some_and(|q| q.eq_ignore_ascii_case(alias)))
        {
            if qualifiers(&a.value)
                .iter()
                .any(|q| !q.eq_ignore_ascii_case(alias))
            {
                return Err(TranslateError::Unsupported(
                    "multi-table UPDATE whose value reads another table".into(),
                ));
            }
            let AssignmentTarget::ColumnName(ObjectName(parts)) = &a.target else {
                continue;
            };
            let col = parts[parts.len() - 1].to_string();
            let mut v = a.value.clone();
            let _: ControlFlow<()> = visit_expressions_mut(&mut v, |x| {
                if let Expr::CompoundIdentifier(p) = x
                    && p.len() == 2
                {
                    *x = Expr::Identifier(p[1].clone());
                }
                ControlFlow::Continue(())
            });
            sets.push(format!("{col} = {v}"));
        }
        let sql = format!(
            "UPDATE {real} SET {} WHERE rowid IN (SELECT {alias}.rowid FROM {from_text}{w})",
            sets.join(", ")
        );
        let mut parsed = Parser::parse_sql(&MySqlDialect {}, &sql)
            .map_err(|e| TranslateError::Parse(format!("{e} (in rewritten `{sql}`)")))?;
        out.append(&mut parsed);
    }
    Ok(Some(out))
}

// ── TRUNCATE, parenthesized UNION members ───────────────────────────────────

/// `TRUNCATE [TABLE] t` → `DELETE FROM t` (SQLite has no TRUNCATE and
/// optimizes an unqualified DELETE the same way). As in MySQL the next
/// `AUTO_INCREMENT` id starts over: an emptied rowid table without
/// `AUTOINCREMENT` restarts at 1. Affected rows: MySQL reports 0, this
/// reports the rows deleted.
pub(crate) fn truncate_to_delete(sql: &str) -> Option<String> {
    let toks = lex(sql).ok()?;
    if !is_word(&toks, 0, "TRUNCATE") {
        return None;
    }
    let start = if is_word(&toks, 1, "TABLE") { 2 } else { 1 };
    let name_start = toks.get(start)?.start;
    let rest = sql[name_start..].trim().trim_end_matches(';').trim();
    Some(format!("DELETE FROM {rest}"))
}

/// `(SELECT ...) UNION (SELECT ...)`: SQLite does not accept parenthesized
/// compound members, so a member without its own `ORDER BY` / `LIMIT`
/// loses its parentheses (one with them is left for SQLite to reject).
pub(crate) fn unwrap_union_members(stmt: &mut Statement) {
    use sqlparser::ast::{SetExpr, VisitMut, VisitorMut};

    struct Unwrap;
    fn unwrap(e: &mut SetExpr) {
        if let SetExpr::Query(q) = e
            && q.order_by.is_none()
            && q.limit_clause.is_none()
            && q.with.is_none()
            && q.locks.is_empty()
        {
            let body = std::mem::replace(
                q.body.as_mut(),
                SetExpr::Values(sqlparser::ast::Values {
                    explicit_row: false,
                    rows: vec![],
                }),
            );
            *e = body;
        }
    }
    impl VisitorMut for Unwrap {
        type Break = ();
        fn pre_visit_query(&mut self, query: &mut sqlparser::ast::Query) -> ControlFlow<()> {
            if let SetExpr::SetOperation { left, right, .. } = query.body.as_mut() {
                unwrap(left);
                unwrap(right);
            }
            ControlFlow::Continue(())
        }
    }
    let _ = stmt.visit(&mut Unwrap);
}

#[cfg(test)]
mod truncate_union_tests {
    use crate::{Dialect, TranslateResult, translate};

    fn one(sql: &str) -> String {
        match translate(sql, Dialect::MySQL).unwrap().as_slice() {
            [TranslateResult::Sql(s)] => s.clone(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn truncate() {
        assert_eq!(one("TRUNCATE table queue"), "DELETE FROM queue");
        assert_eq!(one("truncate `queue`;"), "DELETE FROM `queue`");
    }

    #[test]
    fn parenthesized_union_members() {
        assert_eq!(
            one("(SELECT id FROM a WHERE x = 1) UNION (SELECT id FROM b)"),
            "SELECT id FROM a WHERE x = 1 UNION SELECT id FROM b"
        );
    }
}

// ── Functions ────────────────────────────────────────────────────────────────

fn positional_args(args: &FunctionArguments) -> Option<Vec<&Expr>> {
    let FunctionArguments::List(list) = args else {
        return match args {
            FunctionArguments::None => Some(vec![]),
            _ => None,
        };
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    list.args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        })
        .collect()
}

/// `INTERVAL n UNIT` → SQLite date modifier text (`'+n days'`), negated
/// for subtraction.
fn interval_modifier(e: &Expr, negate: bool) -> Option<String> {
    let Expr::Interval(iv) = e else { return None };
    let n: i64 = match iv.value.as_ref() {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.parse().ok()?,
            Value::SingleQuotedString(s) => s.trim().parse().ok()?,
            _ => return None,
        },
        _ => return None,
    };
    let unit = iv.leading_field.as_ref()?.to_string().to_ascii_uppercase();
    let (n, unit) = match unit.as_str() {
        "SECOND" => (n, "seconds"),
        "MINUTE" => (n, "minutes"),
        "HOUR" => (n, "hours"),
        "DAY" => (n, "days"),
        "WEEK" => (n * 7, "days"),
        "MONTH" => (n, "months"),
        "YEAR" => (n, "years"),
        _ => return None,
    };
    let n = if negate { -n } else { n };
    Some(format!("'{n:+} {unit}'"))
}

/// Rewrite MySQL functions SQLite lacks or names differently. See
/// docs/sql-translation.md for each mapping and its limits.
pub(crate) fn rewrite_functions(stmt: &mut Statement) -> Result<(), TranslateError> {
    let mut error = None;
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        match rewrite_function_expr(expr) {
            Ok(Some(new)) => *expr = new,
            Ok(None) => {}
            Err(e) => {
                error = Some(e);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    error.map_or(Ok(()), Err)
}

fn rewrite_function_expr(expr: &Expr) -> Result<Option<Expr>, TranslateError> {
    // `d - INTERVAL n UNIT` / `d + INTERVAL n UNIT`.
    if let Expr::BinaryOp { left, op, right } = expr
        && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus)
        && let Some(m) = interval_modifier(right, matches!(op, BinaryOperator::Minus))
    {
        return parse_expr(&format!("datetime({left}, {m})")).map(Some);
    }
    let Expr::Function(func) = expr else {
        return Ok(None);
    };
    let name = func.name.to_string().to_ascii_uppercase();
    let Some(args) = positional_args(&func.args) else {
        return Ok(None);
    };
    let text = |e: &Expr| e.to_string();
    let rewritten = match (name.as_str(), args.as_slice()) {
        ("IF", [c, a, b]) => format!("iif({}, {}, {})", text(c), text(a), text(b)),
        ("UNIX_TIMESTAMP", []) => "CAST(strftime('%s', 'now') AS INTEGER)".into(),
        ("UNIX_TIMESTAMP", [d]) => format!("CAST(strftime('%s', {}) AS INTEGER)", text(d)),
        ("FROM_UNIXTIME", [t]) => format!("datetime({}, 'unixepoch')", text(t)),
        ("FROM_UNIXTIME", [_, _]) => {
            return Err(TranslateError::Unsupported(
                "FROM_UNIXTIME with a format argument".into(),
            ));
        }
        ("DATE_SUB" | "SUBDATE", [d, iv]) | ("DATE_ADD" | "ADDDATE", [d, iv]) => {
            let m = interval_modifier(iv, name.starts_with("DATE_SUB") || name == "SUBDATE")
                .ok_or_else(|| TranslateError::Unsupported(format!("{name} with this interval")))?;
            format!("datetime({}, {m})", text(d))
        }
        ("FIND_IN_SET", [x, s]) => {
            let (x, s) = (text(x), text(s));
            let set = format!("(',' || {s} || ',')");
            let needle = format!("(',' || {x} || ',')");
            let prefix = format!("substr({set}, 1, instr({set}, {needle}))");
            format!(
                "CASE WHEN {x} IS NULL OR {s} IS NULL THEN NULL \
                 WHEN instr({set}, {needle}) > 0 \
                 THEN length({prefix}) - length(replace({prefix}, ',', '')) ELSE 0 END"
            )
        }
        ("SUBSTRING_INDEX", [s, d, n]) => {
            let count = match n {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.parse::<i64>().ok(),
                    _ => None,
                },
                Expr::UnaryOp {
                    op: sqlparser::ast::UnaryOperator::Minus,
                    expr,
                } => match expr.as_ref() {
                    Expr::Value(v) => match &v.value {
                        Value::Number(n, _) => n.parse::<i64>().ok().map(|n| -n),
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            };
            let (s, d) = (text(s), text(d));
            match count {
                Some(1) => format!(
                    "CASE WHEN instr({s}, {d}) > 0 THEN substr({s}, 1, instr({s}, {d}) - 1) ELSE {s} END"
                ),
                // After the last delimiter; exact for a one-character
                // delimiter (rtrim strips the trailing non-delimiter run).
                Some(-1) if matches!(&args[1], Expr::Value(v) if matches!(&v.value, Value::SingleQuotedString(x) | Value::DoubleQuotedString(x) if x.chars().count() == 1)) =>
                {
                    format!("substr({s}, length(rtrim({s}, replace({s}, {d}, ''))) + 1)")
                }
                _ => {
                    return Err(TranslateError::Unsupported(
                        "SUBSTRING_INDEX is supported with a count of 1, or -1 with a one-character delimiter".into(),
                    ));
                }
            }
        }
        _ => return Ok(None),
    };
    parse_expr(&rewritten).map(Some)
}

#[cfg(test)]
mod tests {
    use crate::{Dialect, TranslateResult, translate};

    fn sqls(sql: &str) -> Vec<String> {
        translate(sql, Dialect::MySQL)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .map(|r| match r {
                TranslateResult::Sql(s) => s,
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn one(sql: &str) -> String {
        let v = sqls(sql);
        assert_eq!(v.len(), 1, "{sql}: {v:?}");
        v.into_iter().next().unwrap()
    }

    #[test]
    fn pipes_and_ampersands_are_logical() {
        assert_eq!(
            super::logical_operators("SHOW COLUMNS FROM t WHERE Field = 'a' || Field = 'b'")
                .unwrap(),
            "SHOW COLUMNS FROM t WHERE Field = 'a'  OR  Field = 'b'"
        );
        let s = one("SELECT a FROM t WHERE x = 1 || y = 2 && z = '||'");
        assert_eq!(s, "SELECT a FROM t WHERE x = 1 OR y = 2 AND z = '||'");
    }

    #[test]
    fn parenthesized_add_columns() {
        let v =
            sqls("alter table login add (tied_to INT (11), app INT (11), paypal VARCHAR (100))");
        assert_eq!(
            v,
            [
                "ALTER TABLE login ADD COLUMN tied_to INTEGER",
                "ALTER TABLE login ADD COLUMN app INTEGER",
                "ALTER TABLE login ADD COLUMN paypal TEXT",
            ]
        );
        let v = sqls("ALTER TABLE lists ADD (a DECIMAL(10, 2) DEFAULT '0.00', b TEXT)");
        assert_eq!(v.len(), 2, "{v:?}");
    }

    #[test]
    fn multi_op_alter_is_split_and_positions_become_one_rebuild() {
        let v = sqls(
            "ALTER TABLE apps ADD a INT(1) DEFAULT '1' AFTER smtp_password, ADD COLUMN b TEXT FIRST, DROP c, DROP COLUMN d",
        );
        assert_eq!(
            v,
            [
                "ALTER TABLE apps ADD a INTEGER DEFAULT '1'",
                "ALTER TABLE apps ADD COLUMN b TEXT",
                "RebuildTable(ColumnRebuild { table: \"apps\", changes: [Move { column: \"a\", after: Some(\"smtp_password\") }, Move { column: \"b\", after: None }] })",
                "ALTER TABLE apps DROP c",
                "ALTER TABLE apps DROP COLUMN d",
            ]
        );
    }

    #[test]
    fn index_type_clauses_are_dropped() {
        let v = sqls("CREATE TABLE w (id INT, r INT, KEY s_brand (r) USING BTREE)");
        assert_eq!(v[1], "CREATE INDEX `w__s_brand` ON w(r)");
    }

    #[test]
    fn insert_ignore() {
        assert_eq!(
            one("INSERT IGNORE INTO t (a) VALUES (1)"),
            "INSERT OR IGNORE INTO t (a) VALUES (1)"
        );
        assert!(
            one("INSERT IGNORE INTO t (a) SELECT b FROM u").starts_with("INSERT OR IGNORE INTO t")
        );
    }

    #[test]
    fn update_and_delete_limit() {
        assert_eq!(
            one("UPDATE login SET timezone = \"UTC\" LIMIT 1"),
            "UPDATE login SET timezone = 'UTC' WHERE rowid IN (SELECT __litewire_rowid FROM (SELECT rowid AS __litewire_rowid FROM login LIMIT 1) AS __litewire_limit)"
        );
        assert_eq!(
            one("UPDATE q SET s = 1 WHERE c = 2 ORDER BY id LIMIT 5"),
            "UPDATE q SET s = 1 WHERE rowid IN (SELECT __litewire_rowid FROM (SELECT rowid AS __litewire_rowid FROM q WHERE c = 2 ORDER BY id LIMIT 5) AS __litewire_limit)"
        );
        assert_eq!(
            one("DELETE FROM passkeys WHERE id = 1 AND userID = 1 LIMIT 1"),
            "DELETE FROM passkeys WHERE rowid IN (SELECT __litewire_rowid FROM (SELECT rowid AS __litewire_rowid FROM passkeys WHERE id = 1 AND userID = 1 LIMIT 1) AS __litewire_limit)"
        );
    }

    #[test]
    fn multi_table_delete() {
        assert_eq!(
            one("DELETE q1 FROM queue q1 INNER JOIN queue q2 ON q1.c = q2.c AND q1.id > q2.id"),
            "DELETE FROM queue WHERE rowid IN (SELECT q1.rowid FROM queue AS q1 INNER JOIN queue AS q2 ON q1.c = q2.c AND q1.id > q2.id)"
        );
    }

    #[test]
    fn update_join_becomes_update_from() {
        assert_eq!(
            one(
                "UPDATE subscribers INNER JOIN lists ON lists.id = subscribers.list SET subscribers.unsubscribed = 1 WHERE lists.app = 1"
            ),
            "UPDATE subscribers SET unsubscribed = 1 FROM lists WHERE (lists.id = subscribers.list) AND (lists.app = 1)"
        );
        let s = one(
            "UPDATE s k INNER JOIN (SELECT MIN(id) AS keep_id, MAX(b) AS b FROM s GROUP BY e) d ON k.id = d.keep_id SET k.b = IF(d.b = 1, 0, d.b)",
        );
        assert_eq!(
            s,
            "UPDATE s AS k SET b = iif(d.b = 1, 0, d.b) FROM (SELECT MIN(id) AS keep_id, MAX(b) AS b FROM s GROUP BY e) AS d WHERE k.id = d.keep_id"
        );
        assert!(translate("UPDATE a JOIN b ON a.id = b.id SET b.x = 1", Dialect::MySQL).is_err());
    }

    #[test]
    fn comma_multi_table_update_is_split() {
        let v = sqls(
            "UPDATE ares_emails, subscribers SET ares_emails.recipients = ares_emails.recipients+1, subscribers.last_ares = 1 WHERE ares_emails.id = 1 AND subscribers.id = 1",
        );
        assert_eq!(
            v,
            [
                "UPDATE ares_emails SET recipients = recipients + 1 WHERE rowid IN (SELECT ares_emails.rowid FROM ares_emails CROSS JOIN subscribers WHERE ares_emails.id = 1 AND subscribers.id = 1)",
                "UPDATE subscribers SET last_ares = 1 WHERE rowid IN (SELECT subscribers.rowid FROM ares_emails CROSS JOIN subscribers WHERE ares_emails.id = 1 AND subscribers.id = 1)",
            ]
        );
    }

    #[test]
    fn functions() {
        assert_eq!(
            one("SELECT IF(a = 1, \"x\", \"y\") FROM t"),
            "SELECT iif(a = 1, 'x', 'y') FROM t"
        );
        assert_eq!(
            one("SELECT 1 WHERE t < UNIX_TIMESTAMP(NOW())"),
            "SELECT 1 WHERE t < CAST(strftime('%s', datetime('now')) AS INTEGER)"
        );
        assert_eq!(
            one("SELECT UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 3 MONTH))"),
            "SELECT CAST(strftime('%s', datetime(datetime('now'), '-3 months')) AS INTEGER)"
        );
        assert_eq!(
            one("SELECT NOW() - INTERVAL 30 DAY"),
            "SELECT datetime(datetime('now'), '-30 days')"
        );
        assert_eq!(
            one("SELECT FROM_UNIXTIME(ts) FROM t"),
            "SELECT datetime(ts, 'unixepoch') FROM t"
        );
        assert!(one("SELECT FIND_IN_SET('b', lists) FROM t").contains("instr("));
        assert_eq!(
            one("SELECT SUBSTRING_INDEX(c, ' ', 1), SUBSTRING_INDEX(c, ' ', -1) FROM t"),
            "SELECT CASE WHEN instr(c, ' ') > 0 THEN SUBSTR(c, 1, instr(c, ' ') - 1) ELSE c END, \
             SUBSTR(c, length(rtrim(c, replace(c, ' ', ''))) + 1) FROM t"
        );
        assert!(translate("SELECT SUBSTRING_INDEX(c, ' ', 2) FROM t", Dialect::MySQL).is_err());
    }
}
