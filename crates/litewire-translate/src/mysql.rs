//! MySQL-specific SQL rewrites.
//!
//! Handles MySQL DDL constructs (`AUTO_INCREMENT`, `ENGINE=`, type mappings),
//! DML rewrites (`ON DUPLICATE KEY UPDATE`, `LIMIT offset, count`), and
//! MySQL-specific expressions.

use std::ops::ControlFlow;

use sqlparser::ast::{
    CastKind, DataType, DoUpdate, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    LimitClause, Offset, OffsetRows, OnConflict, OnConflictAction, OnInsert, Statement, Value,
    visit_expressions_mut,
};

use crate::TranslateError;
use crate::common::{func_args, func_name, value_expr};

/// Apply MySQL-specific rewrites to a statement in-place.
pub fn rewrite_statement(stmt: &mut Statement) -> Result<(), TranslateError> {
    // Function rewrites build their replacement from MySQL text, so they
    // run before the passes that turn MySQL syntax into SQLite syntax.
    crate::mysql_compat::rewrite_functions(stmt)?;
    crate::mysql_compat::rewrite_update_join(stmt)?;
    crate::mysql_compat::rewrite_delete(stmt)?;
    strip_charset_introducers(stmt);
    double_quoted_strings_to_single(stmt);
    add_default_like_escape(stmt);
    rewrite_date_part_functions(stmt);
    rewrite_scalar_function_names(stmt);
    normalize_regexp(stmt);
    let _ = sqlparser::ast::VisitMut::visit(stmt, &mut QueryShape);
    match stmt {
        Statement::CreateTable(create) => {
            rewrite_create_table(create);
        }
        Statement::Insert(insert) => {
            rewrite_insert_on_duplicate(insert);
            // INSERT IGNORE: skip rows that violate a unique key. (MySQL also
            // downgrades other errors, such as bad values, to warnings;
            // SQLite's OR IGNORE covers constraint conflicts only.)
            if insert.ignore {
                insert.ignore = false;
                insert.or = Some(sqlparser::ast::SqliteOnConflict::Ignore);
            }
        }
        Statement::Query(query) => {
            rewrite_limit_clause(query);
        }
        Statement::AlterTable { operations, .. } => {
            for op in operations {
                if let sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } = op {
                    rewrite_column_def(column_def);
                }
            }
        }
        Statement::Update { assignments, .. } => {
            rewrite_update_assignment_targets(assignments);
        }
        _ => {}
    }
    Ok(())
}

// ── Query shape: FROM DUAL, index hints, locking reads, "no limit" ──────────

/// MySQL's "no upper bound" `LIMIT` value (2^64 - 1), which its manual
/// recommends for "all rows from an offset on": `LIMIT 18446744073709551615
/// OFFSET 20`. Any literal at or above 2^63 overflows SQLite's signed
/// 64-bit `LIMIT` and is mapped to SQLite's own "no limit", `-1`.
const SQLITE_MAX_LIMIT: u128 = i64::MAX as u128;

/// Query-level rewrites that drop MySQL-only syntax SQLite has no use for.
///
/// * `FROM DUAL` — MySQL's dummy table, used for `INSERT ... SELECT ...
///   FROM DUAL WHERE NOT EXISTS (...)` ("insert unless present"). SQLite
///   accepts `SELECT ... WHERE ...` without a `FROM`, which is the same
///   query; the row is inserted only when the `WHERE` holds, and the
///   affected-row count is 0 when it does not.
/// * Index hints (`FORCE INDEX (i)`, `USE INDEX`, `IGNORE INDEX`, `... KEY`,
///   `FOR JOIN/ORDER BY/GROUP BY`) — removed, and SQLite's planner picks the
///   index. Mapping `FORCE INDEX` to `INDEXED BY` would make the statement
///   fail whenever the index is missing (and MySQL index names are not
///   SQLite index names), so a hint is never an error.
/// * Locking reads (`FOR UPDATE`, `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`; the
///   older `LOCK IN SHARE MODE` is removed before parsing) — removed; the
///   rows are returned. SQLite has one writer at a time and no row locks:
///   inside `BEGIN` a read takes no write lock, so two transactions can both
///   "claim" a row, and the second to write fails with a busy/snapshot error
///   instead of waiting for the first. Correct-but-loud, never silently
///   double-applied — but not MySQL's blocking behaviour.
/// * `LIMIT 18446744073709551615` (and any limit >= 2^63) — `LIMIT -1`.
struct QueryShape;

impl QueryShape {
    fn is_dual(from: &[sqlparser::ast::TableWithJoins]) -> bool {
        use sqlparser::ast::TableFactor;
        let [only] = from else { return false };
        if !only.joins.is_empty() {
            return false;
        }
        matches!(&only.relation, TableFactor::Table { name, alias: None, args: None, .. }
            if name.0.len() == 1
                && name.0[0].as_ident().is_some_and(|i| i.quote_style.is_none() && i.value.eq_ignore_ascii_case("dual")))
    }

    fn drop_dual(body: &mut sqlparser::ast::SetExpr) {
        use sqlparser::ast::SetExpr;
        match body {
            SetExpr::Select(select) if Self::is_dual(&select.from) => select.from.clear(),
            SetExpr::SetOperation { left, right, .. } => {
                Self::drop_dual(left);
                Self::drop_dual(right);
            }
            _ => {}
        }
    }

    fn unbounded(expr: &mut Expr) {
        if let Expr::Value(v) = expr
            && let Value::Number(n, _) = &v.value
            && n.parse::<u128>().is_ok_and(|n| n > SQLITE_MAX_LIMIT)
        {
            v.value = Value::Number("-1".into(), false);
        }
    }
}

impl sqlparser::ast::VisitorMut for QueryShape {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut sqlparser::ast::Query) -> ControlFlow<()> {
        Self::drop_dual(&mut query.body);
        query.locks.clear();
        match &mut query.limit_clause {
            Some(LimitClause::LimitOffset {
                limit: Some(limit), ..
            }) => Self::unbounded(limit),
            Some(LimitClause::OffsetCommaLimit { limit, .. }) => Self::unbounded(limit),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut sqlparser::ast::TableFactor,
    ) -> ControlFlow<()> {
        if let sqlparser::ast::TableFactor::Table { index_hints, .. } = table_factor {
            index_hints.clear();
        }
        ControlFlow::Continue(())
    }
}

// ── Text-level pre-passes (before sqlparser) ─────────────────────────────────

/// Remove MySQL's legacy `LOCK IN SHARE MODE` locking-read clause, which
/// sqlparser cannot parse. Returns `None` when the statement has none.
/// Quote- and comment-aware (works on the lexer's tokens, so the words
/// inside a string literal are never touched). See [`QueryShape`] for what
/// dropping a locking read means on SQLite.
pub(crate) fn strip_lock_in_share_mode(sql: &str) -> Option<String> {
    use crate::metadata::{Tok, lex};

    if !sql.to_ascii_uppercase().contains("SHARE") {
        return None;
    }
    let toks = lex(sql).ok()?;
    let words = ["LOCK", "IN", "SHARE", "MODE"];
    let mut cuts = Vec::new();
    let mut i = 0;
    while i + words.len() <= toks.len() {
        let hit = words
            .iter()
            .enumerate()
            .all(|(k, w)| matches!(&toks[i + k].tok, Tok::Word(t) if t.eq_ignore_ascii_case(w)));
        if hit {
            let start = toks[i].start;
            let end = toks[i + 3].start + "MODE".len();
            cuts.push((start, end));
            i += words.len();
        } else {
            i += 1;
        }
    }
    if cuts.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for (start, end) in cuts {
        out.push_str(&sql[last..start]);
        last = end;
    }
    out.push_str(&sql[last..]);
    Some(out)
}

/// Remove `ASC` / `DESC` from the column list of a key defined inside
/// `CREATE TABLE` / `ALTER TABLE ... ADD` (`KEY k (col DESC)`), which
/// sqlparser cannot parse there. The direction only affects which way
/// SQLite stores the index, never which rows a query returns. `None` when
/// there is nothing to remove.
pub(crate) fn strip_key_column_direction(sql: &str) -> Option<String> {
    use crate::metadata::{Tok, lex};

    let toks = lex(sql).ok()?;
    let is = |i: usize, w: &str| matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w));
    if !((is(0, "CREATE") && (is(1, "TABLE") || is(1, "TEMPORARY")))
        || (is(0, "ALTER") && is(1, "TABLE")))
    {
        return None;
    }
    let mut cuts: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        if is(i, "KEY") || is(i, "INDEX") || is(i, "UNIQUE") {
            // Skip to the key's column list: the next `(`, at most a few
            // tokens on (`KEY name USING BTREE (`).
            let mut j = i + 1;
            while j < toks.len()
                && j <= i + 6
                && !matches!(toks[j].tok, Tok::Other('(' | ',') | Tok::Semi)
            {
                j += 1;
            }
            if j < toks.len() && matches!(toks[j].tok, Tok::Other('(')) {
                let mut depth = 0i32;
                let mut k = j;
                while k < toks.len() {
                    match &toks[k].tok {
                        Tok::Other('(') => depth += 1,
                        Tok::Other(')') => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        Tok::Word(w)
                            if depth == 1
                                && (w.eq_ignore_ascii_case("ASC")
                                    || w.eq_ignore_ascii_case("DESC")) =>
                        {
                            cuts.push((toks[k].start, toks[k].start + w.len()));
                        }
                        _ => {}
                    }
                    k += 1;
                }
                i = k;
            }
        }
        i += 1;
    }
    if cuts.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for (start, end) in cuts {
        out.push_str(&sql[last..start]);
        last = end;
    }
    out.push_str(&sql[last..]);
    Some(out)
}

/// `ALTER TABLE t DROP KEY k` → `... DROP INDEX k` (MySQL synonyms;
/// sqlparser only parses the second). `None` when there is nothing to do.
pub(crate) fn normalize_drop_key(sql: &str) -> Option<String> {
    use crate::metadata::{Tok, lex};

    let toks = lex(sql).ok()?;
    let is = |i: usize, w: &str| matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w));
    if !(is(0, "ALTER") && is(1, "TABLE")) {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + 8);
    let mut last = 0;
    for (i, tok) in toks.iter().enumerate().skip(1) {
        if is(i, "KEY") && is(i - 1, "DROP") {
            let start = tok.start;
            out.push_str(&sql[last..start]);
            out.push_str("INDEX");
            last = start + "KEY".len();
        }
    }
    if last == 0 {
        return None;
    }
    out.push_str(&sql[last..]);
    Some(out)
}

/// Is this an `ALTER TABLE` that only changes MySQL table options?
///
/// `ALTER TABLE t ROW_FORMAT=DYNAMIC`, `ENGINE=InnoDB`, `[DEFAULT]
/// CHARSET=...`, `COLLATE ...`, `AUTO_INCREMENT=n`, `COMMENT='...'`,
/// `CONVERT TO CHARACTER SET ...`, `ENABLE KEYS` / `DISABLE KEYS`, `FORCE`,
/// the storage tuning options and the `ALGORITHM=` / `LOCK=` clauses, alone
/// or comma-separated. None of them has a SQLite counterpart: storage
/// format, engine and character set (always UTF-8) are fixed, and SQLite
/// assigns the next rowid itself. Such a statement is answered OK without
/// touching the database. A statement mixing an option with a real change
/// (`ADD COLUMN ...`) is not matched and goes to the parser as usual.
///
/// `AUTO_INCREMENT=n` is the one option with a visible effect on MySQL (the
/// next id); on SQLite the next id stays `max(rowid) + 1`.
pub(crate) fn is_table_option_only_alter(sql: &str) -> bool {
    use crate::metadata::{Tok, lex};

    let Ok(toks) = lex(sql) else { return false };
    let mut it = toks.iter().map(|l| &l.tok).peekable();
    let word =
        |t: Option<&&Tok>, w: &str| matches!(t, Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w));

    if !word(it.next().as_ref(), "ALTER") || !word(it.next().as_ref(), "TABLE") {
        return false;
    }
    // Table name: ident or db.ident.
    if !matches!(it.next(), Some(Tok::Word(_) | Tok::Quoted(_))) {
        return false;
    }
    if matches!(it.peek(), Some(Tok::Dot)) {
        it.next();
        if !matches!(it.next(), Some(Tok::Word(_) | Tok::Quoted(_))) {
            return false;
        }
    }

    // `[=] value`, value being a word, number, string or quoted ident.
    fn value<'a>(it: &mut std::iter::Peekable<impl Iterator<Item = &'a Tok>>) -> bool {
        if matches!(it.peek(), Some(Tok::Other('='))) {
            it.next();
        }
        matches!(it.next(), Some(Tok::Word(_) | Tok::Str(_) | Tok::Quoted(_)))
    }

    loop {
        let Some(Tok::Word(w)) = it.next() else {
            return false;
        };
        let ok = match w.to_ascii_uppercase().as_str() {
            "ENGINE" | "ROW_FORMAT" | "AUTO_INCREMENT" | "COMMENT" | "KEY_BLOCK_SIZE"
            | "AVG_ROW_LENGTH" | "MAX_ROWS" | "MIN_ROWS" | "PACK_KEYS" | "CHECKSUM"
            | "DELAY_KEY_WRITE" | "STATS_AUTO_RECALC" | "STATS_PERSISTENT"
            | "STATS_SAMPLE_PAGES" | "ALGORITHM" | "LOCK" | "CHARSET" | "COLLATE" => value(&mut it),
            "CHARACTER" => word(it.next().as_ref(), "SET") && value(&mut it),
            "DEFAULT" => match it.next() {
                Some(Tok::Word(x))
                    if x.eq_ignore_ascii_case("CHARSET") || x.eq_ignore_ascii_case("COLLATE") =>
                {
                    value(&mut it)
                }
                Some(Tok::Word(x)) if x.eq_ignore_ascii_case("CHARACTER") => {
                    word(it.next().as_ref(), "SET") && value(&mut it)
                }
                _ => false,
            },
            "CONVERT" => {
                word(it.next().as_ref(), "TO")
                    && match it.next() {
                        Some(Tok::Word(x)) if x.eq_ignore_ascii_case("CHARSET") => value(&mut it),
                        Some(Tok::Word(x)) if x.eq_ignore_ascii_case("CHARACTER") => {
                            word(it.next().as_ref(), "SET") && value(&mut it)
                        }
                        _ => false,
                    }
                    && (!word(it.peek(), "COLLATE") || {
                        it.next();
                        value(&mut it)
                    })
            }
            "ENABLE" | "DISABLE" => word(it.next().as_ref(), "KEYS"),
            "FORCE" => true,
            _ => false,
        };
        if !ok {
            return false;
        }
        // Options are separated by commas or (as MySQL also accepts) spaces.
        match it.peek() {
            None => return true,
            Some(Tok::Semi) => {
                it.next();
                return it.next().is_none();
            }
            Some(Tok::Other(',')) => {
                it.next();
            }
            _ => {}
        }
    }
}

// ── Charset introducers ──────────────────────────────────────────────────────

/// Drop MySQL charset introducers (`_utf8mb4'x'`, `_latin1'x'`): text is
/// UTF-8 in SQLite. `_binary 'abc'` — what mysqldump writes for BLOB values
/// without `--hex-blob` — becomes the blob literal `X'616263'`, so the
/// value is stored as a BLOB, not as text.
fn strip_charset_introducers(stmt: &mut Statement) {
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        if let Expr::Prefixed { prefix, value } = expr
            && prefix.quote_style.is_none()
            && prefix.value.starts_with('_')
            && let Expr::Value(v) = value.as_ref()
        {
            let replacement = match &v.value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)
                    if prefix.value.eq_ignore_ascii_case("_binary") =>
                {
                    let hex: String = s.bytes().map(|b| format!("{b:02X}")).collect();
                    Some(Value::HexStringLiteral(hex))
                }
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    Some(Value::SingleQuotedString(s.clone()))
                }
                Value::HexStringLiteral(h) => Some(Value::HexStringLiteral(h.clone())),
                _ => None,
            };
            if let Some(r) = replacement {
                *expr = Expr::value(r);
            }
        }
        ControlFlow::Continue(())
    });
}

// ── Scalar functions with a different SQLite name ────────────────────────────

/// Rename MySQL scalar functions that exist in SQLite under another name.
///
/// * `LEAST(a, b, ...)` / `GREATEST(...)` → multi-argument `min()` / `max()`.
///   Both return `NULL` when any argument is `NULL`, as MySQL's do.
/// * `CHAR_LENGTH(s)` / `CHARACTER_LENGTH(s)` → `length(s)`, which counts
///   characters for text, as MySQL's `CHAR_LENGTH` does.
///
/// MySQL-only: PostgreSQL's `LEAST`/`GREATEST` ignore `NULL`s.
fn rewrite_scalar_function_names(stmt: &mut Statement) {
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        if let Expr::Function(func) = expr {
            let renamed = match func.name.to_string().to_ascii_uppercase().as_str() {
                "LEAST" => Some(("min", 2)),
                "GREATEST" => Some(("max", 2)),
                "CHAR_LENGTH" | "CHARACTER_LENGTH" => Some(("length", 1)),
                _ => None,
            };
            // A one-argument min()/max() would be the aggregate: malformed
            // calls are left for SQLite to reject.
            if let Some((name, min_args)) = renamed
                && matches!(&func.args, FunctionArguments::List(l) if l.args.len() >= min_args)
            {
                func.name = func_name(name);
            }
        }
        ControlFlow::Continue(())
    });
}

// ── "double-quoted" strings ──────────────────────────────────────────────────

/// Emit MySQL's double-quoted string literals single-quoted.
///
/// Without `ANSI_QUOTES` in `sql_mode` (the MySQL default, and the mode
/// every PHP application assumes), `"abc"` is a string literal. sqlparser's
/// MySQL tokenizer agrees and produces [`Value::DoubleQuotedString`], but
/// emitting it back as `"abc"` hands SQLite an *identifier*: it only falls
/// back to the string when no column of that name is in scope, so
/// `WHERE status = "active"` compares against a column called `active` the
/// moment one exists — and engines without the fallback reject it outright.
fn double_quoted_strings_to_single(stmt: &mut Statement) {
    struct Requote;
    impl sqlparser::ast::VisitorMut for Requote {
        type Break = ();
        fn post_visit_value(&mut self, value: &mut Value) -> ControlFlow<()> {
            if let Value::DoubleQuotedString(s) = value {
                *value = Value::SingleQuotedString(std::mem::take(s));
            }
            ControlFlow::Continue(())
        }
    }
    let _ = sqlparser::ast::VisitMut::visit(stmt, &mut Requote);
}

// ── YEAR() / MONTH() / DAYOFMONTH() ──────────────────────────────────────────

/// The `strftime` format specifier a MySQL date-part function maps to.
///
/// `DAY` is MySQL's documented synonym for `DAYOFMONTH`, so both land on the
/// same specifier. Deliberately absent: `DAYOFWEEK`, `DAYOFYEAR`, `WEEK` and
/// friends, whose MySQL numbering does not line up with `strftime`'s and
/// which would need arithmetic rather than a rename.
fn strftime_specifier(name: &str) -> Option<&'static str> {
    match name {
        "YEAR" => Some("%Y"),
        "MONTH" => Some("%m"),
        "DAYOFMONTH" | "DAY" => Some("%d"),
        _ => None,
    }
}

/// The single argument of a function call, if it has exactly one positional
/// argument. Anything else (no args, several, `DISTINCT`, a wildcard) is
/// left for SQLite to reject rather than silently reinterpreted.
fn single_positional_arg(args: &FunctionArguments) -> Option<&Expr> {
    let FunctionArguments::List(list) = args else {
        return None;
    };
    if list.args.len() != 1 || list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
        _ => None,
    }
}

/// Translate MySQL's date-part functions to `strftime` calls.
///
/// `YEAR(post_date)` becomes `CAST(strftime('%Y', post_date) AS INTEGER)`.
/// WordPress reaches for these in `wp_old_slug_redirect` and
/// `redirect_guess_404_permalink`, whose date-guess queries compare the
/// result against an integer from the URL.
///
/// The `CAST` is not decoration. MySQL's `YEAR()` returns an integer, while
/// `strftime` returns text — and `'2024' = 2024` is false in SQLite, which
/// applies no affinity to a function result. Without the cast the queries
/// parse, run, and quietly match nothing. The cast also drops `strftime`'s
/// leading zeros, so `MONTH()` yields 1..12 and `DAYOFMONTH()` 1..31 exactly
/// as MySQL does.
///
/// `NULL` propagates: `strftime` returns `NULL` for a `NULL` or unparseable
/// input and `CAST(NULL AS INTEGER)` is `NULL`, matching MySQL. An
/// unparseable date is where the two differ — MySQL returns `NULL` too, but
/// only after its own laxer parsing.
fn rewrite_date_part_functions(stmt: &mut Statement) {
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        let Expr::Function(func) = expr else {
            return ControlFlow::Continue(());
        };
        let name = func.name.to_string().to_ascii_uppercase();
        let Some(specifier) = strftime_specifier(&name) else {
            return ControlFlow::Continue(());
        };
        let Some(arg) = single_positional_arg(&func.args) else {
            return ControlFlow::Continue(());
        };

        let mut call = func.clone();
        call.name = func_name("strftime");
        call.args = func_args(vec![
            value_expr(Value::SingleQuotedString(specifier.into())),
            arg.clone(),
        ]);
        *expr = Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Function(call)),
            data_type: DataType::Integer(None),
            format: None,
        };
        ControlFlow::Continue(())
    });
}

// ── REGEXP / RLIKE ───────────────────────────────────────────────────────────

/// Emit MySQL's `RLIKE` as `REGEXP`, the spelling SQLite understands.
///
/// The two are the same operator in MySQL, and sqlparser keeps which one was
/// written so it can print it back. SQLite has only `REGEXP`, so normalise
/// on that; `NOT REGEXP` survives, since SQLite's grammar takes an optional
/// `NOT` in front of the operator too.
///
/// SQLite resolves `x REGEXP y` to a `regexp(y, x)` function call that the
/// host application has to provide — litewire's rusqlite backend registers
/// one, and Turso ships a built-in. Against a bare SQLite with neither, the
/// statement fails with "no such function: regexp", which is the same
/// outcome as before this rewrite and better than a silently wrong answer.
fn normalize_regexp(stmt: &mut Statement) {
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        if let Expr::RLike { regexp, .. } = expr {
            *regexp = true;
        }
        ControlFlow::Continue(())
    });
}

// ── LIKE: add MySQL's implicit ESCAPE '\' ────────────────────────────────────

/// Give every `LIKE` / `NOT LIKE` (and `ILIKE`, should the parser produce
/// one on this path) without an explicit `ESCAPE` clause the escape
/// character MySQL always has active: the backslash.
///
/// MySQL treats `\` as the implicit `LIKE` escape character, so
/// `wpdb::esc_like('50%')` emits the pattern `'50\%'` expecting `%` to
/// match literally. SQLite's `LIKE` has *no* default escape character:
/// without this rewrite the backslash matches literally and `%` stays a
/// wildcard, so MySQL-escaped patterns match the wrong rows.
///
/// String literals cooperate: sqlparser's MySQL tokenizer implements
/// MySQL's "backslash retained" rule for LIKE escapes (`'\%'` and `'\_'`
/// keep their backslash, while other escapes like `\\` and `\n` are
/// resolved), and the emitter only doubles single quotes when writing
/// string literals back out. The SQLite pattern therefore carries the same
/// `\%` / `\_` byte pairs that this `ESCAPE '\'` clause interprets — and a
/// resolved `\\` arrives as one backslash, which MySQL's LIKE engine also
/// treats as an escape, so both `'50\%'` and `'50\\%'` match a literal
/// `%` exactly as they do on MySQL. Bound parameters reach SQLite
/// verbatim, matching MySQL's binary-protocol behavior.
///
/// An explicit `ESCAPE` clause in the input (including MySQL's
/// `ESCAPE ''`) is preserved untouched.
fn add_default_like_escape(stmt: &mut Statement) {
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        if let Expr::Like { escape_char, .. } | Expr::ILike { escape_char, .. } = expr
            && escape_char.is_none()
        {
            *escape_char = Some("\\".to_string());
        }
        ControlFlow::Continue(())
    });
}

/// Strip table qualifiers from UPDATE SET targets: MySQL accepts
/// `UPDATE users SET users.updated_at = ...`, SQLite requires a bare
/// column name. (Laravel's Eloquent qualifies the `updated_at` column.)
fn rewrite_update_assignment_targets(assignments: &mut [sqlparser::ast::Assignment]) {
    use sqlparser::ast::{AssignmentTarget, ObjectName};
    for a in assignments {
        if let AssignmentTarget::ColumnName(name) = &mut a.target
            && name.0.len() > 1
            && let Some(last) = name.0.pop()
        {
            *name = ObjectName(vec![last]);
        }
    }
}

// ── Indexes: MySQL per-table names -> SQLite per-database names ─────────────

/// Separator between table and index name in the SQLite name of an index
/// created from MySQL DDL: MySQL key `s_list` on table `subscribers` is the
/// SQLite index `subscribers__s_list`.
///
/// MySQL index names are scoped to their table — two tables may each have a
/// key called `s_list`, and dumps routinely do — while SQLite index names
/// share one namespace per database, so the second `CREATE INDEX s_list`
/// would fail. Prefixing with the table name keeps every MySQL name usable.
/// `SHOW INDEX`, `information_schema.statistics` and `DROP INDEX` translate
/// back (see `catalog::index_rows`); the SQLite planner never needs the name.
pub const INDEX_NAME_SEPARATOR: &str = "__";

/// The SQLite name for MySQL index `index` on `table`.
#[must_use]
pub fn sqlite_index_name(table: &str, index: &str) -> String {
    format!("{table}{INDEX_NAME_SEPARATOR}{index}")
}

/// Unquoted last part of an object name (`db`.`t` -> `t`).
fn last_part(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .unwrap_or_default()
}

fn index_ident(table: &sqlparser::ast::ObjectName, index: &str) -> sqlparser::ast::ObjectName {
    sqlparser::ast::ObjectName::from(vec![sqlparser::ast::Ident::with_quote(
        '`',
        sqlite_index_name(&last_part(table), index),
    )])
}

/// MySQL's name for an index declared without one: its first column, with
/// `_2`, `_3`, ... appended while that name is taken on the table.
fn default_index_name(columns: &[sqlparser::ast::Ident], taken: &[String]) -> String {
    let base = columns.first().map(|c| c.value.clone()).unwrap_or_default();
    let mut name = base.clone();
    let mut n = 2;
    while taken.iter().any(|t| t.eq_ignore_ascii_case(&name)) {
        name = format!("{base}_{n}");
        n += 1;
    }
    name
}

fn create_index(
    table: &sqlparser::ast::ObjectName,
    index: &str,
    columns: &[sqlparser::ast::Ident],
    unique: bool,
    if_not_exists: bool,
) -> Statement {
    use sqlparser::ast::{CreateIndex, IndexColumn, OrderByExpr, OrderByOptions};
    Statement::CreateIndex(CreateIndex {
        name: Some(index_ident(table, index)),
        table_name: table.clone(),
        using: None,
        columns: columns
            .iter()
            .map(|c| IndexColumn {
                column: OrderByExpr {
                    expr: Expr::Identifier(c.clone()),
                    options: OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                operator_class: None,
            })
            .collect(),
        unique,
        concurrently: false,
        if_not_exists,
        include: vec![],
        nulls_distinct: None,
        with: vec![],
        predicate: None,
    })
}

/// `DROP INDEX <sqlite name>` for MySQL index `index` on `table`.
fn drop_index(table: &sqlparser::ast::ObjectName, index: &str, if_exists: bool) -> Statement {
    Statement::Drop {
        object_type: sqlparser::ast::ObjectType::Index,
        if_exists,
        names: vec![index_ident(table, index)],
        cascade: false,
        restrict: false,
        purge: false,
        temporary: false,
        table: None,
    }
}

/// One step of an expanded MySQL DDL statement.
#[derive(Debug, Clone)]
#[allow(
    clippy::large_enum_variant,
    reason = "short-lived, one per expanded statement; boxing buys nothing"
)]
pub enum Expanded {
    /// A SQLite statement.
    Stmt(Statement),
    /// A table rebuild for column changes SQLite cannot make in place.
    Rebuild(crate::alter::ColumnRebuild),
}

/// Expand MySQL DDL into the SQLite statements that implement it, in order.
///
/// * `CREATE TABLE` with inline `KEY` / `INDEX` / `UNIQUE KEY` definitions:
///   the table without them, then one `CREATE [UNIQUE] INDEX` per key
///   (`IF NOT EXISTS` when the table was `IF NOT EXISTS`). An unnamed key
///   gets MySQL's default name (first column, `_2`, ...). `FULLTEXT` /
///   `SPATIAL` keys have no SQLite analogue and are dropped.
/// * `ALTER TABLE` with several operations: one SQLite statement per
///   operation (SQLite's ALTER takes one), in order. `ADD {KEY|INDEX|UNIQUE}
///   [name] (cols)` becomes `CREATE [UNIQUE] INDEX`, `DROP {INDEX|KEY} name`
///   becomes `DROP INDEX`, `ADD COLUMN ... {FIRST|AFTER c}` loses the
///   position (SQLite appends; `SHOW COLUMNS` order then differs from
///   MySQL's), and consecutive `MODIFY` / `CHANGE` / `ALTER COLUMN ...
///   {SET|DROP} {DEFAULT|NOT NULL}` become one table rebuild
///   ([`crate::alter`]).
/// * `CREATE [UNIQUE] INDEX name ON t (...)` and `DROP INDEX name ON t`:
///   the index name becomes the SQLite name.
///
/// Every index name goes through [`sqlite_index_name`].
///
/// # Errors
///
/// [`TranslateError::Unsupported`] for a column change with no rebuild
/// mapping (`ALTER COLUMN ... SET DATA TYPE`).
pub fn expand_ddl(stmt: Statement) -> Result<Vec<Expanded>, TranslateError> {
    Ok(match stmt {
        Statement::CreateTable(create) => expand_create_table(create)
            .into_iter()
            .map(Expanded::Stmt)
            .collect(),
        Statement::AlterTable { .. } => return expand_alter_table(stmt),
        Statement::CreateIndex(mut ci) => {
            if let Some(name) = &ci.name {
                ci.name = Some(index_ident(&ci.table_name, &last_part(name)));
            }
            vec![Expanded::Stmt(Statement::CreateIndex(ci))]
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Index,
            if_exists,
            names,
            table: Some(table),
            ..
        } => names
            .iter()
            .map(|n| Expanded::Stmt(drop_index(&table, &last_part(n), if_exists)))
            .collect(),
        other => vec![Expanded::Stmt(other)],
    })
}

fn expand_create_table(mut create: sqlparser::ast::CreateTable) -> Vec<Statement> {
    use sqlparser::ast::TableConstraint;

    let if_not_exists = create.if_not_exists;
    let mut keys: Vec<(Option<String>, Vec<sqlparser::ast::Ident>, bool)> = Vec::new();
    create.constraints.retain(|c| match c {
        TableConstraint::Index { name, columns, .. } => {
            keys.push((
                name.as_ref().map(|n| n.value.clone()),
                columns.clone(),
                false,
            ));
            false
        }
        TableConstraint::Unique {
            name,
            index_name,
            columns,
            ..
        } => {
            let name = index_name
                .as_ref()
                .or(name.as_ref())
                .map(|n| n.value.clone());
            keys.push((name, columns.clone(), true));
            false
        }
        TableConstraint::FulltextOrSpatial { .. } => false,
        _ => true,
    });

    let table = create.name.clone();
    let mut taken: Vec<String> = vec!["PRIMARY".into()];
    let mut out = vec![Statement::CreateTable(create)];
    for (name, columns, unique) in keys {
        let name = name.unwrap_or_else(|| default_index_name(&columns, &taken));
        out.push(create_index(&table, &name, &columns, unique, if_not_exists));
        taken.push(name);
    }
    out
}

/// A `MODIFY` / `CHANGE` column definition as SQLite will store it.
fn changed_column_def(
    name: &sqlparser::ast::Ident,
    data_type: &DataType,
    options: &[sqlparser::ast::ColumnOption],
) -> sqlparser::ast::ColumnDef {
    let mut def = sqlparser::ast::ColumnDef {
        name: name.clone(),
        data_type: data_type.clone(),
        options: options
            .iter()
            .map(|o| sqlparser::ast::ColumnOptionDef {
                name: None,
                option: o.clone(),
            })
            .collect(),
    };
    rewrite_column_def(&mut def);
    def
}

/// See [`expand_ddl`].
///
/// # Errors
///
/// As [`expand_ddl`].
pub fn expand_alter_table(stmt: Statement) -> Result<Vec<Expanded>, TranslateError> {
    use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, TableConstraint};

    use crate::alter::{ColumnChange, ColumnRebuild};

    let Statement::AlterTable {
        name,
        if_exists,
        only,
        operations,
        location,
        on_cluster,
        iceberg,
    } = stmt
    else {
        return Ok(vec![Expanded::Stmt(stmt)]);
    };

    let table = last_part(&name);
    let mut out: Vec<Expanded> = Vec::new();
    let mut pending: Vec<ColumnChange> = Vec::new();
    let flush = |pending: &mut Vec<ColumnChange>, out: &mut Vec<Expanded>| {
        if !pending.is_empty() {
            out.push(Expanded::Rebuild(ColumnRebuild {
                table: table.clone(),
                changes: std::mem::take(pending),
            }));
        }
    };

    let move_to = |column: &sqlparser::ast::Ident,
                   pos: Option<&sqlparser::ast::MySQLColumnPosition>| {
        pos.map(|p| ColumnChange::Move {
            column: column.value.clone(),
            after: match p {
                sqlparser::ast::MySQLColumnPosition::First => None,
                sqlparser::ast::MySQLColumnPosition::After(a) => Some(a.value.clone()),
            },
        })
    };

    for op in operations {
        let column_change = match &op {
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                pending.push(ColumnChange::Redefine {
                    old: col_name.value.clone(),
                    def: changed_column_def(col_name, data_type, options),
                });
                move_to(col_name, column_position.as_ref()).or_else(|| pending.pop())
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                pending.push(ColumnChange::Redefine {
                    old: old_name.value.clone(),
                    def: changed_column_def(new_name, data_type, options),
                });
                move_to(new_name, column_position.as_ref()).or_else(|| pending.pop())
            }
            AlterTableOperation::AlterColumn { column_name, op } => Some(match op {
                AlterColumnOperation::SetDefault { value } => ColumnChange::Default {
                    column: column_name.value.clone(),
                    value: Some(value.clone()),
                },
                AlterColumnOperation::DropDefault => ColumnChange::Default {
                    column: column_name.value.clone(),
                    value: None,
                },
                AlterColumnOperation::SetNotNull => ColumnChange::NotNull {
                    column: column_name.value.clone(),
                    not_null: true,
                },
                AlterColumnOperation::DropNotNull => ColumnChange::NotNull {
                    column: column_name.value.clone(),
                    not_null: false,
                },
                other => {
                    return Err(TranslateError::Unsupported(format!(
                        "ALTER TABLE ... ALTER COLUMN {column_name} {other}"
                    )));
                }
            }),
            _ => None,
        };
        if let Some(change) = column_change {
            pending.push(change);
            continue;
        }
        // Consecutive `ADD ... AFTER` columns share one rebuild: the adds run
        // first, then every pending move at once.
        let add_after_moves = matches!(op, AlterTableOperation::AddColumn { .. })
            && pending
                .iter()
                .all(|c| matches!(c, ColumnChange::Move { .. }));
        if !add_after_moves {
            flush(&mut pending, &mut out);
        }

        let single = |op: AlterTableOperation| {
            Expanded::Stmt(Statement::AlterTable {
                name: name.clone(),
                if_exists,
                only,
                operations: vec![op],
                location: location.clone(),
                on_cluster: on_cluster.clone(),
                iceberg,
            })
        };
        match op {
            AlterTableOperation::AddConstraint(TableConstraint::Unique {
                name: cname,
                index_name,
                columns,
                ..
            }) => {
                let idx = index_name
                    .as_ref()
                    .or(cname.as_ref())
                    .map_or_else(|| default_index_name(&columns, &[]), |i| i.value.clone());
                out.push(Expanded::Stmt(create_index(
                    &name, &idx, &columns, true, false,
                )));
            }
            AlterTableOperation::AddConstraint(TableConstraint::Index {
                name: iname,
                columns,
                ..
            }) => {
                let idx = iname
                    .as_ref()
                    .map_or_else(|| default_index_name(&columns, &[]), |i| i.value.clone());
                out.push(Expanded::Stmt(create_index(
                    &name, &idx, &columns, false, false,
                )));
            }
            AlterTableOperation::DropIndex { name: iname } => {
                out.push(Expanded::Stmt(drop_index(&name, &iname.value, false)));
            }
            // FULLTEXT/SPATIAL: no SQLite analogue — drop (perf-only).
            AlterTableOperation::AddConstraint(TableConstraint::FulltextOrSpatial { .. }) => {}
            AlterTableOperation::AddColumn {
                column_keyword,
                if_not_exists,
                column_def,
                column_position,
            } => {
                // SQLite appends; a FIRST / AFTER position is applied by a
                // rebuild right after the add.
                if let Some(m) = move_to(&column_def.name, column_position.as_ref()) {
                    pending.push(m);
                }
                out.push(single(AlterTableOperation::AddColumn {
                    column_keyword,
                    if_not_exists,
                    column_def,
                    column_position: None,
                }));
                continue;
            }
            other => out.push(single(other)),
        }
    }
    flush(&mut pending, &mut out);
    Ok(out)
}

// ── ON DUPLICATE KEY UPDATE -> ON CONFLICT DO UPDATE ─────────────────────────

/// Rewrite MySQL's `ON DUPLICATE KEY UPDATE` to SQLite's `ON CONFLICT DO UPDATE`.
///
/// MySQL's `VALUES(col)` in the update list (the would-be-inserted value)
/// becomes SQLite's `excluded.col`. Without this, the emitted SQL keeps the
/// MySQL-only `VALUES(col)` call and SQLite rejects the whole upsert —
/// WordPress `add_option()`/`update_option()` depend on this shape.
/// (SQLite >= 3.35 accepts `DO UPDATE` without a conflict target.)
fn rewrite_insert_on_duplicate(insert: &mut sqlparser::ast::Insert) {
    if let Some(OnInsert::DuplicateKeyUpdate(mut assignments)) = insert.on.take() {
        for assignment in &mut assignments {
            rewrite_values_call_to_excluded(&mut assignment.value);
        }
        insert.on = Some(OnInsert::OnConflict(OnConflict {
            conflict_target: None,
            action: OnConflictAction::DoUpdate(DoUpdate {
                assignments,
                selection: None,
            }),
        }));
    }
}

/// Replace `VALUES(col)` with `excluded.col` in an upsert assignment value.
fn rewrite_values_call_to_excluded(expr: &mut sqlparser::ast::Expr) {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident};

    if let Expr::Function(func) = expr
        && func.name.to_string().eq_ignore_ascii_case("VALUES")
        && let FunctionArguments::List(args) = &func.args
        && args.args.len() == 1
        && let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(col))) = &args.args[0]
    {
        *expr = Expr::CompoundIdentifier(vec![Ident::new("excluded"), col.clone()]);
        return;
    }
    // Recurse through common wrappers so `VALUES(a) + 1` style values work.
    match expr {
        sqlparser::ast::Expr::BinaryOp { left, right, .. } => {
            rewrite_values_call_to_excluded(left);
            rewrite_values_call_to_excluded(right);
        }
        sqlparser::ast::Expr::Nested(inner) | sqlparser::ast::Expr::UnaryOp { expr: inner, .. } => {
            rewrite_values_call_to_excluded(inner);
        }
        _ => {}
    }
}

// ── LIMIT offset, count -> LIMIT count OFFSET offset ────────────────────────

/// Rewrite MySQL's `LIMIT offset, count` to standard `LIMIT count OFFSET offset`.
///
/// Every other clause shape is put back untouched. (This previously
/// `take()`-ed the clause and only restored the comma form, silently
/// dropping a plain `LIMIT n` / `LIMIT n OFFSET m` from the emitted SQL —
/// so LIMITed MySQL queries returned every row.)
fn rewrite_limit_clause(query: &mut sqlparser::ast::Query) {
    match query.limit_clause.take() {
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: Some(limit),
                offset: Some(Offset {
                    value: offset,
                    rows: OffsetRows::None,
                }),
                limit_by: vec![],
            });
        }
        other => query.limit_clause = other,
    }
}

// ── DDL: CREATE TABLE ────────────────────────────────────────────────────────

/// Rewrite `CREATE TABLE` for SQLite compatibility.
fn rewrite_create_table(create: &mut sqlparser::ast::CreateTable) {
    // Drop MySQL table options (`ENGINE=InnoDB`, `DEFAULT CHARACTER SET =
    // utf8mb4 COLLATE = 'utf8mb4_unicode_ci'`, ...): SQLite rejects them all.
    // Laravel's schema builder emits the CHARACTER SET / COLLATE pair on
    // every `create table`.
    create.table_options = sqlparser::ast::CreateTableOptions::None;

    // Inline `KEY` / `UNIQUE KEY` / `FULLTEXT` definitions stay in the AST
    // here; `expand_ddl` turns them into named `CREATE INDEX` statements.
    // `PRIMARY KEY (col)` displays fine for SQLite and is kept as-is.

    for col in &mut create.columns {
        rewrite_column_def(col);
    }
}

/// Rewrite one column definition (`CREATE TABLE`, `ALTER TABLE ... ADD`)
/// for SQLite: map the type to its affinity and drop the MySQL-only column
/// options SQLite rejects.
///
/// * `AUTO_INCREMENT` — an `INTEGER PRIMARY KEY` is SQLite's rowid alias and
///   auto-assigns.
/// * `CHARACTER SET x`, `COLLATE x` — text is always UTF-8, and MySQL
///   collation names (`utf8mb4_unicode_ci`, ...) do not exist in SQLite,
///   which rejects an unknown collation. Comparisons therefore use SQLite's
///   binary collation: **case-sensitive**, where MySQL's `_ci` collations
///   are not (documented in docs/sql-translation.md).
/// * `COMMENT '...'` — no SQLite syntax for it.
/// * `ON UPDATE CURRENT_TIMESTAMP` — no SQLite syntax for it; the column
///   keeps its value on UPDATE unless the statement sets it (logged).
fn rewrite_column_def(col: &mut sqlparser::ast::ColumnDef) {
    use sqlparser::ast::ColumnOption;

    col.data_type = rewrite_data_type(&col.data_type);
    col.options.retain(|opt| match &opt.option {
        ColumnOption::DialectSpecific(tokens) => !tokens
            .iter()
            .any(|t| t.to_string().eq_ignore_ascii_case("AUTO_INCREMENT")),
        ColumnOption::CharacterSet(_) | ColumnOption::Collation(_) | ColumnOption::Comment(_) => {
            false
        }
        ColumnOption::OnUpdate(_) => {
            tracing::debug!(
                column = %col.name,
                "ON UPDATE <expr> dropped: SQLite has no automatic column update"
            );
            false
        }
        _ => true,
    });
}

/// Map MySQL types to SQLite affinities.
fn rewrite_data_type(dt: &DataType) -> DataType {
    match dt {
        // Integer types -> INTEGER
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::MediumInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Integer(_)
        | DataType::TinyIntUnsigned(_)
        | DataType::SmallIntUnsigned(_)
        | DataType::MediumIntUnsigned(_)
        | DataType::IntUnsigned(_)
        | DataType::IntegerUnsigned(_)
        | DataType::BigIntUnsigned(_) => DataType::Integer(None),

        // Float types -> REAL
        DataType::Float(_)
        | DataType::Double(..)
        | DataType::Decimal(..)
        | DataType::Numeric(..) => DataType::Real,

        // String types -> TEXT
        DataType::Varchar(_)
        | DataType::Char(_)
        | DataType::Text
        | DataType::TinyText
        | DataType::MediumText
        | DataType::LongText
        | DataType::Enum(..)
        | DataType::Set(_) => DataType::Text,

        // Binary types -> BLOB
        DataType::Binary(_)
        | DataType::Varbinary(_)
        | DataType::Blob(_)
        | DataType::TinyBlob
        | DataType::MediumBlob
        | DataType::LongBlob => DataType::Blob(None),

        // Boolean -> INTEGER
        DataType::Boolean => DataType::Integer(None),

        // Date/time types -> TEXT
        DataType::Date
        | DataType::Datetime(_)
        | DataType::Timestamp(_, _)
        | DataType::Time(_, _) => DataType::Text,

        // JSON -> TEXT
        DataType::JSON | DataType::JSONB => DataType::Text,

        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{Dialect, TranslateResult, translate};

    fn extract_sql(result: &TranslateResult) -> &str {
        match result {
            TranslateResult::Sql(s) => s.as_str(),
            other => panic!("expected Sql, got: {other:?}"),
        }
    }

    // ── No-ops ──────────────────────────────────────────────────────────────

    #[test]
    fn set_names_is_noop() {
        let results = translate("SET NAMES utf8mb4", Dialect::MySQL).unwrap();
        assert!(matches!(results[0], TranslateResult::Noop));
    }

    #[test]
    fn set_character_set_is_noop() {
        let results = translate("SET CHARACTER SET utf8", Dialect::MySQL).unwrap();
        assert!(matches!(results[0], TranslateResult::Noop));
    }

    #[test]
    fn set_session_is_noop() {
        let results = translate("SET SESSION wait_timeout = 28800", Dialect::MySQL).unwrap();
        assert!(matches!(results[0], TranslateResult::Noop));
    }

    #[test]
    fn set_sql_mode_is_noop() {
        let results = translate("SET sql_mode = 'STRICT_TRANS_TABLES'", Dialect::MySQL).unwrap();
        assert!(matches!(results[0], TranslateResult::Noop));
    }

    #[test]
    fn set_time_zone_is_noop() {
        let results = translate("SET time_zone = '+00:00'", Dialect::MySQL).unwrap();
        assert!(matches!(results[0], TranslateResult::Noop));
    }

    // ── Expression rewrites ─────────────────────────────────────────────────

    #[test]
    fn select_now_translated() {
        let results = translate("SELECT NOW()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("DATETIME"), "expected datetime, got: {sql}");
    }

    #[test]
    fn boolean_translated() {
        let results = translate("SELECT TRUE, FALSE", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.contains('1') && sql.contains('0'),
            "expected 1 and 0, got: {sql}"
        );
    }

    // ── ON DUPLICATE KEY UPDATE ─────────────────────────────────────────────

    #[test]
    fn on_duplicate_key_update_rewritten() {
        let results = translate(
            "INSERT INTO t (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = 'Alice'",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(
            upper.contains("ON CONFLICT"),
            "expected ON CONFLICT, got: {sql}"
        );
        assert!(
            upper.contains("DO UPDATE"),
            "expected DO UPDATE, got: {sql}"
        );
        assert!(
            !upper.contains("DUPLICATE KEY"),
            "DUPLICATE KEY should be removed: {sql}"
        );
    }

    #[test]
    fn on_duplicate_key_update_preserves_assignments() {
        let results = translate(
            "INSERT INTO t (id, val) VALUES (1, 10) ON DUPLICATE KEY UPDATE val = val + 1",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("val"), "assignments lost: {sql}");
    }

    // ── LIMIT offset, count ─────────────────────────────────────────────────

    #[test]
    fn limit_offset_comma_rewritten() {
        let results = translate("SELECT * FROM t LIMIT 5, 10", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("LIMIT 10"), "expected LIMIT 10, got: {sql}");
        assert!(upper.contains("OFFSET 5"), "expected OFFSET 5, got: {sql}");
    }

    #[test]
    fn standard_limit_unchanged() {
        // Standard LIMIT without offset should not add an OFFSET clause.
        let results = translate("SELECT * FROM t LIMIT 10", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        // sqlparser may or may not preserve LIMIT in the emitted SQL for MySQL dialect.
        // The key assertion is: no spurious OFFSET is introduced.
        assert!(!upper.contains("OFFSET"), "unexpected OFFSET: {sql}");
    }

    #[test]
    fn standard_limit_preserved() {
        // rewrite_limit_clause once dropped every non-comma LIMIT clause
        // (take() only restored the OffsetCommaLimit shape), so LIMITed
        // queries returned all rows. Pin the fix.
        let results = translate("SELECT * FROM t LIMIT 10", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.to_ascii_uppercase().contains("LIMIT 10"),
            "LIMIT lost: {sql}"
        );

        let results = translate("SELECT * FROM t LIMIT 10 OFFSET 5", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("LIMIT 10"), "LIMIT lost: {sql}");
        assert!(upper.contains("OFFSET 5"), "OFFSET lost: {sql}");
    }

    // ── DDL: type rewrites ──────────────────────────────────────────────────

    #[test]
    fn int_types_to_integer() {
        let results = translate(
            "CREATE TABLE t (a TINYINT, b SMALLINT, c MEDIUMINT, d INT, e BIGINT)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("TINYINT"), "TINYINT not rewritten: {sql}");
        assert!(!upper.contains("SMALLINT"), "SMALLINT not rewritten: {sql}");
        assert!(
            !upper.contains("MEDIUMINT"),
            "MEDIUMINT not rewritten: {sql}"
        );
        assert!(!upper.contains("BIGINT"), "BIGINT not rewritten: {sql}");
    }

    #[test]
    fn varchar_to_text() {
        let results = translate(
            "CREATE TABLE t (name VARCHAR(255), bio TEXT)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("VARCHAR"), "VARCHAR not rewritten: {sql}");
    }

    #[test]
    fn float_types_to_real() {
        let results = translate(
            "CREATE TABLE t (a FLOAT, b DOUBLE, c DECIMAL(10,2))",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("REAL"), "no REAL found: {sql}");
    }

    #[test]
    fn blob_types() {
        let results = translate("CREATE TABLE t (a BLOB, b LONGBLOB)", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("LONGBLOB"), "LONGBLOB not rewritten: {sql}");
    }

    #[test]
    fn datetime_to_text() {
        let results = translate(
            "CREATE TABLE t (created DATETIME, updated TIMESTAMP)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("DATETIME"), "DATETIME not rewritten: {sql}");
        assert!(
            !upper.contains("TIMESTAMP"),
            "TIMESTAMP not rewritten: {sql}"
        );
    }

    #[test]
    fn auto_increment_removed() {
        let results = translate(
            "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(
            !upper.contains("AUTO_INCREMENT"),
            "AUTO_INCREMENT not removed: {sql}"
        );
    }

    #[test]
    fn boolean_to_integer_in_ddl() {
        let results = translate("CREATE TABLE t (active BOOLEAN)", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("BOOLEAN"), "BOOLEAN not rewritten: {sql}");
        assert!(upper.contains("INTEGER"), "no INTEGER found: {sql}");
    }

    #[test]
    fn json_to_text() {
        let results = translate("CREATE TABLE t (data JSON)", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains(" JSON"), "JSON not rewritten: {sql}");
        assert!(upper.contains("TEXT"), "no TEXT found: {sql}");
    }

    // ── Real-app DDL (WordPress / Laravel shapes) ───────────────────────────

    #[test]
    fn wordpress_index_prefix_length_parses() {
        // wp_usermeta: index prefix lengths broke parsing before the DDL
        // pre-pass (sqlparser cannot represent them).
        let results = translate(
            "CREATE TABLE wp_usermeta (\n\
             umeta_id bigint(20) unsigned NOT NULL auto_increment,\n\
             user_id bigint(20) unsigned NOT NULL default '0',\n\
             meta_key varchar(255) default NULL,\n\
             meta_value longtext,\n\
             PRIMARY KEY  (umeta_id),\n\
             KEY user_id (user_id),\n\
             KEY meta_key (meta_key(191))\n\
             ) DEFAULT CHARACTER SET utf8",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(!sql.contains("191"), "prefix length survived: {sql}");
        assert!(
            !sql.to_ascii_uppercase().contains("CHARACTER SET"),
            "table options survived: {sql}"
        );
        // Inline `KEY name (col)` constraints are MySQL-only syntax; they
        // must not reach SQLite. PRIMARY KEY stays.
        let upper = sql.to_ascii_uppercase();
        assert!(
            !upper.replace("PRIMARY KEY (", "").contains(" KEY ("),
            "inline KEY constraint survived: {sql}"
        );
        assert!(upper.contains("PRIMARY KEY"), "primary key lost: {sql}");
    }

    #[test]
    fn wordpress_unique_key_normalized() {
        // wp_options: `UNIQUE KEY option_name (option_name)` must become a
        // bare `UNIQUE (option_name)` — upserts depend on the constraint.
        let results = translate(
            "CREATE TABLE wp_options (\n\
             option_id bigint(20) unsigned NOT NULL auto_increment,\n\
             option_name varchar(191) NOT NULL default '',\n\
             option_value longtext NOT NULL,\n\
             autoload varchar(20) NOT NULL default 'yes',\n\
             PRIMARY KEY  (option_id),\n\
             UNIQUE KEY option_name (option_name),\n\
             KEY autoload (autoload)\n\
             ) DEFAULT CHARACTER SET utf8",
            Dialect::MySQL,
        )
        .unwrap();
        // The table, then one named index per key (MySQL names, scoped to
        // the table): the unique one keeps upserts working, the plain one
        // keeps the query plans.
        assert_eq!(results.len(), 3, "{results:?}");
        let table = extract_sql(&results[0]).to_ascii_uppercase();
        assert!(
            !table.contains("UNIQUE") && !table.contains("KEY AUTOLOAD"),
            "MySQL key syntax survived: {table}"
        );
        assert_eq!(
            extract_sql(&results[1]),
            "CREATE UNIQUE INDEX `wp_options__option_name` ON wp_options(option_name)"
        );
        assert_eq!(
            extract_sql(&results[2]),
            "CREATE INDEX `wp_options__autoload` ON wp_options(autoload)"
        );
    }

    #[test]
    fn inline_keys_become_named_indexes() {
        let results = translate(
            "CREATE TABLE IF NOT EXISTS `t` (id INT NOT NULL AUTO_INCREMENT, a INT, b INT, \
             PRIMARY KEY (id), KEY (a), KEY (a, b), UNIQUE KEY `u` (b), FULLTEXT KEY ft (b), INDEX idx_b (b))",
            Dialect::MySQL,
        )
        .unwrap();
        let squash = |s: &str| s.replace(", ", ",");
        let sqls: Vec<String> = results.iter().map(|r| squash(extract_sql(r))).collect();
        assert_eq!(sqls.len(), 5, "{sqls:?}");
        assert!(
            sqls[0].starts_with("CREATE TABLE IF NOT EXISTS `t`"),
            "{sqls:?}"
        );
        assert!(sqls[0].contains("PRIMARY KEY (id)"), "{sqls:?}");
        // Unnamed keys take MySQL's default name: first column, then _2.
        assert_eq!(
            &sqls[1..],
            [
                "CREATE INDEX IF NOT EXISTS `t__a` ON `t`(a)",
                "CREATE INDEX IF NOT EXISTS `t__a_2` ON `t`(a,b)",
                "CREATE UNIQUE INDEX IF NOT EXISTS `t__u` ON `t`(b)",
                "CREATE INDEX IF NOT EXISTS `t__idx_b` ON `t`(b)",
            ]
        );
    }

    #[test]
    fn key_column_direction_in_table_ddl() {
        // Inline keys: the direction is dropped (sqlparser cannot parse it).
        let results = translate(
            "CREATE TABLE s (id INT, last_campaign INT, KEY s_last (last_campaign DESC), KEY k2 (id ASC, last_campaign))",
            Dialect::MySQL,
        )
        .unwrap();
        let sqls: Vec<String> = results
            .iter()
            .map(|r| extract_sql(r).replace(", ", ","))
            .collect();
        assert_eq!(sqls[1], "CREATE INDEX `s__s_last` ON s(last_campaign)");
        assert_eq!(sqls[2], "CREATE INDEX `s__k2` ON s(id,last_campaign)");
        // A standalone CREATE INDEX keeps it (SQLite supports it there).
        let results = translate(
            "CREATE INDEX s_last ON s (last_campaign DESC)",
            Dialect::MySQL,
        )
        .unwrap();
        assert!(extract_sql(&results[0]).contains("last_campaign DESC"));
        // ALTER ... ADD INDEX too.
        let results = translate(
            "ALTER TABLE s ADD INDEX s_last (last_campaign DESC), ADD INDEX k3 (id)",
            Dialect::MySQL,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            extract_sql(&results[0]),
            "CREATE INDEX `s__s_last` ON s(last_campaign)"
        );
    }

    #[test]
    fn index_names_are_scoped_to_their_table() {
        for (sql, want) in [
            (
                "CREATE INDEX s_list ON subscribers (list)",
                "CREATE INDEX `subscribers__s_list` ON subscribers(list)",
            ),
            (
                "CREATE UNIQUE INDEX s_email ON `subscribers` (email, list)",
                "CREATE UNIQUE INDEX `subscribers__s_email` ON `subscribers`(email,list)",
            ),
            (
                "DROP INDEX s_list ON subscribers",
                "DROP INDEX `subscribers__s_list`",
            ),
            (
                "ALTER TABLE subscribers DROP INDEX s_list",
                "DROP INDEX `subscribers__s_list`",
            ),
            (
                "ALTER TABLE subscribers DROP KEY s_list",
                "DROP INDEX `subscribers__s_list`",
            ),
            (
                "ALTER TABLE subscribers ADD INDEX (list)",
                "CREATE INDEX `subscribers__list` ON subscribers(list)",
            ),
        ] {
            let results = translate(sql, Dialect::MySQL).unwrap();
            assert_eq!(results.len(), 1, "{sql}: {results:?}");
            assert_eq!(extract_sql(&results[0]).replace(", ", ","), want, "{sql}");
        }
    }

    #[test]
    fn mysql_column_options_sqlite_rejects_are_dropped() {
        let results = translate(
            "CREATE TABLE a (name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, \
             notes TEXT COMMENT 'caf\u{e9}', updated TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP, \
             created DATETIME DEFAULT CURRENT_TIMESTAMP)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        for gone in ["CHARACTER SET", "COLLATE", "COMMENT", "ON UPDATE"] {
            assert!(!sql.contains(gone), "{gone} survived: {sql}");
        }
        assert!(sql.contains("NOT NULL"), "{sql}");
        assert!(sql.contains("DEFAULT CURRENT_TIMESTAMP"), "{sql}");
        let results = translate(
            "ALTER TABLE a ADD COLUMN x VARCHAR(10) COLLATE utf8mb4_unicode_ci DEFAULT ''",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(!sql.contains("COLLATE"), "{sql}");
    }

    #[test]
    fn laravel_charset_collate_table_options_dropped() {
        let results = translate(
            "create table `migrations` (`id` int unsigned not null auto_increment primary key, \
             `migration` varchar(255) not null, `batch` int not null) \
             default character set utf8mb4 collate 'utf8mb4_unicode_ci'",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("CHARACTER SET"), "charset survived: {sql}");
        assert!(!upper.contains("COLLATE"), "collate survived: {sql}");
    }

    #[test]
    fn upsert_values_call_becomes_excluded() {
        // WordPress add_option()/update_option() shape.
        let results = translate(
            "INSERT INTO wp_options (option_name, option_value, autoload) \
             VALUES ('siteurl', 'http://x', 'yes') \
             ON DUPLICATE KEY UPDATE option_value = VALUES(option_value), autoload = VALUES(autoload)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(
            upper.contains("EXCLUDED.OPTION_VALUE") && upper.contains("EXCLUDED.AUTOLOAD"),
            "VALUES() not rewritten to excluded.*: {sql}"
        );
        assert!(
            !upper.contains("= VALUES("),
            "MySQL VALUES() call survived: {sql}"
        );
    }

    #[test]
    fn alter_add_unique_becomes_create_unique_index() {
        // Laravel: alter table `users` add unique `users_email_unique`(`email`)
        let results = translate(
            "alter table `users` add unique `users_email_unique`(`email`)",
            Dialect::MySQL,
        )
        .unwrap();
        assert_eq!(results.len(), 1, "expected exactly one statement");
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(
            upper.starts_with("CREATE UNIQUE INDEX"),
            "expected CREATE UNIQUE INDEX, got: {sql}"
        );
        assert!(
            sql.contains("users") && sql.contains("email"),
            "table/column lost: {sql}"
        );
    }

    #[test]
    fn alter_add_index_becomes_create_index() {
        let results = translate(
            "alter table `sessions` add index `sessions_user_id_index`(`user_id`)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(
            upper.starts_with("CREATE INDEX"),
            "expected CREATE INDEX, got: {sql}"
        );
        assert!(sql.contains("user_id"), "column lost: {sql}");
    }

    #[test]
    fn decimal_precision_kept_in_ddl_prepass() {
        let results = translate(
            "CREATE TABLE t (price decimal(10,2) NOT NULL)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.to_ascii_uppercase().contains("REAL"),
            "decimal not mapped: {sql}"
        );
    }

    #[test]
    fn update_qualified_set_target_dequalified() {
        // Laravel Eloquent: update `users` set `name` = ?, `users`.`updated_at` = ?
        let results = translate(
            "update `users` set `name` = 'x', `users`.`updated_at` = '2026-01-01' where `id` = 2",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            !sql.contains("`users`.`updated_at`") && !sql.contains("users.updated_at"),
            "qualified SET target survived: {sql}"
        );
        assert!(sql.contains("updated_at"), "column lost: {sql}");
    }

    #[test]
    fn sql_calc_found_rows_hint_stripped() {
        // WordPress main post/comment query shape.
        let results = translate(
            "SELECT SQL_CALC_FOUND_ROWS wp_comments.comment_ID FROM wp_comments \
             WHERE ( comment_approved = '1' ) AND comment_post_ID = 4 \
             ORDER BY wp_comments.comment_date_gmt ASC",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            !sql.to_ascii_uppercase().contains("SQL_CALC_FOUND_ROWS"),
            "hint survived: {sql}"
        );
        assert!(sql.contains("comment_ID"), "column lost: {sql}");
    }

    #[test]
    fn hint_inside_string_literal_untouched() {
        let results = translate(
            "INSERT INTO posts (content) VALUES ('how to use SQL_CALC_FOUND_ROWS by hand') ON DUPLICATE KEY UPDATE content = VALUES(content)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.contains("SQL_CALC_FOUND_ROWS"),
            "literal content mangled: {sql}"
        );
    }

    #[test]
    fn found_rows_returns_zero_shim() {
        let results = translate("SELECT FOUND_ROWS()", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.to_ascii_lowercase().contains("abs(0)"),
            "FOUND_ROWS not shimmed: {sql}"
        );
    }

    // ── Passthrough ─────────────────────────────────────────────────────────

    #[test]
    fn simple_select_passthrough() {
        let results = translate("SELECT 1 + 2", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("1 + 2"), "got: {sql}");
    }

    #[test]
    fn insert_passthrough() {
        let results = translate(
            "INSERT INTO users (name, age) VALUES ('Alice', 30)",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("Alice"), "got: {sql}");
    }

    #[test]
    fn delete_passthrough() {
        let results = translate("DELETE FROM users WHERE id = 1", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.to_ascii_uppercase().contains("DELETE"), "got: {sql}");
    }

    // ── LIKE: MySQL implicit ESCAPE '\' ─────────────────────────────────────

    #[test]
    fn double_quoted_strings_become_single_quoted() {
        let results = translate(
            "SELECT * FROM login WHERE (db_updated != \"7.1\" OR db_updated IS NULL) AND license != \"\" AND n = \"it's\"",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("db_updated <> '7.1'"), "got: {sql}");
        assert!(sql.contains("license <> ''"), "got: {sql}");
        assert!(sql.contains("n = 'it''s'"), "got: {sql}");
        assert!(!sql.contains('"'), "got: {sql}");
    }

    #[test]
    fn backtick_identifiers_are_untouched_by_requoting() {
        let results = translate(
            "SELECT `name` FROM `t` WHERE `name` = \"x\"",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("`name` = 'x'"), "got: {sql}");
    }

    #[test]
    fn like_gains_default_escape() {
        let results = translate("SELECT * FROM t WHERE name LIKE 'a%b'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"LIKE 'a%b' ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn not_like_gains_default_escape() {
        let results =
            translate("SELECT * FROM t WHERE name NOT LIKE 'a%b'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"NOT LIKE 'a%b' ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn ilike_gains_default_escape() {
        let results = translate("SELECT * FROM t WHERE name ILIKE 'a%b'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"ILIKE 'a%b' ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn explicit_escape_clause_preserved() {
        let results = translate(
            "SELECT * FROM t WHERE name LIKE 'a|%b' ESCAPE '|'",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("ESCAPE '|'"), "got: {sql}");
        assert!(!sql.contains(r"ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn like_escape_added_in_update_and_delete() {
        let results =
            translate("UPDATE t SET v = 1 WHERE name LIKE 'x\\_%'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"ESCAPE '\'"), "got: {sql}");

        let results = translate("DELETE FROM t WHERE name LIKE '%\\%%'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn like_escape_added_inside_subquery() {
        let results = translate(
            "SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE k LIKE 'a\\%')",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn like_pattern_backslash_percent_survives_to_sqlite() {
        // MySQL input `'50\%'`: the tokenizer's "backslash retained" rule
        // keeps the `\%` pair, and the emitter must not mangle it — the
        // emitted pattern literal carries a real backslash for ESCAPE to see.
        let results = translate(r"SELECT * FROM t WHERE v LIKE '50\%'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"'50\%'"), "got: {sql}");
        assert!(sql.contains(r"ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn like_bound_parameter_gains_escape() {
        let results = translate("SELECT * FROM t WHERE v LIKE ?", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains(r"LIKE ? ESCAPE '\'"), "got: {sql}");
    }

    #[test]
    fn postgres_like_untouched() {
        // The implicit-backslash rule is MySQL semantics; the PostgreSQL
        // path must not gain an ESCAPE clause.
        let results =
            translate("SELECT * FROM t WHERE name LIKE 'a%b'", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(!sql.to_ascii_uppercase().contains("ESCAPE"), "got: {sql}");
    }

    // ── Date-part functions ──────────────────────────────────────────────

    #[test]
    fn year_month_dayofmonth_become_strftime_casts() {
        for (mysql_fn, specifier) in [
            ("YEAR", "%Y"),
            ("MONTH", "%m"),
            ("DAYOFMONTH", "%d"),
            ("DAY", "%d"),
        ] {
            let results = translate(
                &format!("SELECT {mysql_fn}(post_date) FROM p"),
                Dialect::MySQL,
            )
            .unwrap();
            let sql = extract_sql(&results[0]);
            assert!(
                sql.contains(&format!("strftime('{specifier}', post_date)")),
                "{mysql_fn}: got {sql}"
            );
            // The cast is what makes the result comparable to an integer.
            assert!(
                sql.to_ascii_uppercase().contains("AS INTEGER"),
                "got: {sql}"
            );
            assert!(
                !sql.to_ascii_uppercase().contains(&format!("{mysql_fn}(")),
                "got: {sql}"
            );
        }
    }

    #[test]
    fn date_parts_translate_in_a_where_clause() {
        // The shape `wp_old_slug_redirect` emits.
        let results = translate(
            "SELECT ID FROM wp_posts WHERE YEAR(post_date) = 2024 AND MONTH(post_date) = 3 \
             AND DAYOFMONTH(post_date) = 17",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("strftime('%Y', post_date)"), "got: {sql}");
        assert!(sql.contains("strftime('%m', post_date)"), "got: {sql}");
        assert!(sql.contains("strftime('%d', post_date)"), "got: {sql}");
    }

    #[test]
    fn date_parts_with_the_wrong_arity_are_left_alone() {
        // Two arguments is not MySQL's YEAR(); rewriting it would invent
        // semantics. Leave it for SQLite to reject.
        let results = translate("SELECT YEAR(a, b) FROM t", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(!sql.contains("strftime"), "got: {sql}");
    }

    #[test]
    fn date_parts_are_not_rewritten_for_postgres() {
        // PostgreSQL has no YEAR(); its callers use EXTRACT. This rewrite is
        // MySQL-only, like the LIKE escape above.
        let results = translate("SELECT YEAR(d) FROM t", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(!sql.contains("strftime"), "got: {sql}");
    }

    // ── REGEXP / RLIKE ───────────────────────────────────────────────────

    #[test]
    fn regexp_is_preserved() {
        // The shape WordPress's populate_options() emits at install time.
        let results = translate(
            "DELETE FROM wp_options WHERE option_name REGEXP '^rss_[0-9a-f]{32}(_ts)?$'",
            Dialect::MySQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        assert!(
            sql.contains("option_name REGEXP '^rss_[0-9a-f]{32}(_ts)?$'"),
            "got: {sql}"
        );
    }

    #[test]
    fn rlike_is_emitted_as_regexp() {
        let results = translate("SELECT * FROM t WHERE v RLIKE '^a'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("v REGEXP '^a'"), "got: {sql}");
        assert!(!sql.to_ascii_uppercase().contains("RLIKE"), "got: {sql}");
    }

    #[test]
    fn negated_regexp_keeps_its_not() {
        let results = translate("SELECT * FROM t WHERE v NOT RLIKE '^a'", Dialect::MySQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("v NOT REGEXP '^a'"), "got: {sql}");
    }

    // ── Statement shapes MySQL applications rely on ──────────────────────

    fn one(sql: &str) -> String {
        let results = translate(sql, Dialect::MySQL).unwrap();
        assert_eq!(results.len(), 1, "{sql}: {results:?}");
        extract_sql(&results[0]).to_string()
    }

    fn is_noop(sql: &str) -> bool {
        matches!(
            translate(sql, Dialect::MySQL).unwrap().as_slice(),
            [TranslateResult::Noop]
        )
    }

    #[test]
    fn insert_select_from_dual_drops_dual() {
        let sql = one(
            "INSERT INTO t (a, b) SELECT 'x', 1 FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM t WHERE a = 'x')",
        );
        assert!(!sql.to_ascii_uppercase().contains("DUAL"), "got: {sql}");
        assert!(sql.contains("SELECT 'x', 1 WHERE NOT EXISTS"), "got: {sql}");
        let sql = one("SELECT 1 + 1 FROM dual");
        assert_eq!(sql, "SELECT 1 + 1");
    }

    #[test]
    fn a_real_table_named_like_dual_is_kept() {
        // Quoted, aliased or joined: a user table, not MySQL's dummy.
        assert!(one("SELECT * FROM `dual`").contains("`dual`"));
        assert!(one("SELECT * FROM dual AS d").contains("dual AS d"));
    }

    #[test]
    fn index_hints_are_removed() {
        for sql in [
            "SELECT * FROM queue FORCE INDEX (q_campaign_sent_id) WHERE campaign_id = 1",
            "SELECT * FROM queue USE INDEX (a) WHERE campaign_id = 1",
            "SELECT * FROM queue IGNORE KEY (a, b) WHERE campaign_id = 1",
            "SELECT * FROM queue FORCE INDEX FOR ORDER BY (a) WHERE campaign_id = 1",
        ] {
            let out = one(sql);
            assert_eq!(out, "SELECT * FROM queue WHERE campaign_id = 1", "{sql}");
        }
        let out = one("SELECT * FROM a USE INDEX (x) JOIN b FORCE INDEX (y) ON a.id = b.id");
        assert_eq!(out, "SELECT * FROM a JOIN b ON a.id = b.id");
    }

    #[test]
    fn locking_reads_are_removed() {
        for sql in [
            "SELECT id FROM queue WHERE sent = 2 FOR UPDATE",
            "SELECT id FROM queue WHERE sent = 2 FOR SHARE",
            "SELECT id FROM queue WHERE sent = 2 FOR UPDATE NOWAIT",
            "SELECT id FROM queue WHERE sent = 2 FOR UPDATE SKIP LOCKED",
            "SELECT id FROM queue WHERE sent = 2 LOCK IN SHARE MODE",
            "select id from queue where sent = 2 lock in share mode",
        ] {
            let out = one(sql);
            assert_eq!(
                out.to_ascii_uppercase(),
                "SELECT ID FROM QUEUE WHERE SENT = 2",
                "{sql}"
            );
        }
        // Inside a subquery too.
        let out = one("SELECT * FROM (SELECT id FROM q FOR UPDATE) AS x");
        assert!(!out.contains("FOR UPDATE"), "got: {out}");
        // The words inside a string literal are data.
        let out = one("SELECT 'lock in share mode' AS s");
        assert!(out.contains("'lock in share mode'"), "got: {out}");
    }

    #[test]
    fn unbounded_limit_becomes_minus_one() {
        assert_eq!(
            one("SELECT * FROM t ORDER BY id LIMIT 18446744073709551615 OFFSET 20"),
            "SELECT * FROM t ORDER BY id LIMIT -1 OFFSET 20"
        );
        assert_eq!(
            one("SELECT * FROM t LIMIT 20, 18446744073709551615"),
            "SELECT * FROM t LIMIT -1 OFFSET 20"
        );
        assert_eq!(
            one("SELECT * FROM t LIMIT 9223372036854775808"),
            "SELECT * FROM t LIMIT -1"
        );
        // In range: untouched.
        assert_eq!(
            one("SELECT * FROM t LIMIT 9223372036854775807"),
            "SELECT * FROM t LIMIT 9223372036854775807"
        );
    }

    #[test]
    fn table_option_only_alters_are_noops() {
        for sql in [
            "ALTER TABLE apps ROW_FORMAT=DYNAMIC",
            "ALTER TABLE `apps` ROW_FORMAT = DYNAMIC;",
            "ALTER TABLE apps ENGINE=InnoDB",
            "ALTER TABLE apps ENGINE=InnoDB ROW_FORMAT=DYNAMIC",
            "ALTER TABLE apps ENGINE=InnoDB, ROW_FORMAT=COMPRESSED, KEY_BLOCK_SIZE=8",
            "ALTER TABLE apps DEFAULT CHARSET=utf8mb4",
            "ALTER TABLE apps DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            "ALTER TABLE apps CHARACTER SET = utf8mb4",
            "ALTER TABLE apps COLLATE utf8mb4_unicode_ci",
            "ALTER TABLE apps CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            "ALTER TABLE apps CONVERT TO CHARSET utf8mb4",
            "ALTER TABLE apps AUTO_INCREMENT=100",
            "ALTER TABLE apps COMMENT='brands'",
            "ALTER TABLE sendy.apps ENGINE=InnoDB",
            "ALTER TABLE apps ENABLE KEYS",
            "ALTER TABLE apps DISABLE KEYS",
            "ALTER TABLE apps FORCE",
            "ALTER TABLE apps ENGINE=InnoDB, ALGORITHM=INPLACE, LOCK=NONE",
        ] {
            assert!(is_noop(sql), "{sql}");
        }
    }

    #[test]
    fn real_alters_are_not_swallowed() {
        for sql in [
            "ALTER TABLE apps ADD COLUMN x INT",
            "ALTER TABLE apps ADD COLUMN x INT, ENGINE=InnoDB",
            "ALTER TABLE apps ENGINE=InnoDB, ADD COLUMN x INT",
            "ALTER TABLE apps RENAME TO apps2",
            "ALTER TABLE apps",
        ] {
            assert!(
                !matches!(
                    translate(sql, Dialect::MySQL).as_deref(),
                    Ok([TranslateResult::Noop])
                ),
                "{sql} must not be a no-op"
            );
        }
    }

    #[test]
    fn least_greatest_char_length() {
        assert_eq!(
            one(
                "SELECT LEAST(a, 5), GREATEST(a, b, c), CHAR_LENGTH(s), CHARACTER_LENGTH(s) FROM t"
            ),
            "SELECT min(a, 5), max(a, b, c), length(s), length(s) FROM t"
        );
        // One argument is not LEAST/GREATEST; not turned into an aggregate.
        assert!(one("SELECT LEAST(a) FROM t").contains("LEAST(a)"));
    }

    #[test]
    fn ddl_prepass_keeps_non_ascii_text() {
        let sql = one("CREATE TABLE y (d VARCHAR(5) DEFAULT 'n\u{e9}', e INT(11) DEFAULT 0)");
        assert!(sql.contains("'n\u{e9}'"), "got: {sql}");
        assert!(!sql.contains('\u{c3}'), "mojibake: {sql}");
    }

    #[test]
    fn hint_strip_keeps_non_ascii_text() {
        let sql = one("SELECT SQL_NO_CACHE name FROM t WHERE name = 'Zo\u{eb}'");
        assert!(sql.contains("'Zo\u{eb}'"), "got: {sql}");
    }

    #[test]
    fn ddl_prepass_respects_backslash_escaped_quotes() {
        // The `(11)` inside the string is data and must survive.
        let sql = one("CREATE TABLE y (d VARCHAR(50) DEFAULT 'it\\'s (11)', e INT(11))");
        assert!(sql.contains("(11)'"), "got: {sql}");
        assert!(sql.contains("e INTEGER"), "got: {sql}");
    }
}
