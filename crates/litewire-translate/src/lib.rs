//! SQL dialect translation for litewire.
//!
//! Translates MySQL, PostgreSQL, and T-SQL dialects to SQLite-compatible SQL
//! using `sqlparser-rs` for parsing and AST manipulation.

pub mod cache;
pub mod common;
pub mod emit;
pub mod metadata;
pub mod mysql;
pub mod postgres;
pub mod tds;

pub use cache::TranslateCache;

use sqlparser::ast::Statement;
use sqlparser::dialect::{MsSqlDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

/// Source SQL dialect for translation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dialect {
    MySQL,
    PostgreSQL,
    TDS,
}

/// Errors during SQL translation.
#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("SQL parse error: {0}")]
    Parse(String),

    #[error("unsupported SQL construct: {0}")]
    Unsupported(String),
}

/// Result of translating a SQL statement.
#[derive(Debug, Clone)]
pub enum TranslateResult {
    /// Translated SQL string to execute against SQLite.
    Sql(String),
    /// A no-op statement (e.g., `SET NAMES utf8mb4`) -- should return OK without executing.
    Noop,
    /// A metadata query that requires special handling.
    Metadata(metadata::MetadataQuery),
}

/// Translate a SQL string from the given dialect to SQLite.
///
/// Returns one or more translated results (one per statement in the input).
///
/// # Errors
///
/// Returns a [`TranslateError`] if the SQL cannot be parsed or contains
/// unsupported constructs.
pub fn translate(sql: &str, dialect: Dialect) -> Result<Vec<TranslateResult>, TranslateError> {
    // Check for metadata queries first (before parsing -- some are not valid SQL).
    if let Some(meta) = metadata::detect_metadata_query(sql, dialect) {
        return Ok(vec![TranslateResult::Metadata(meta)]);
    }

    // Session/transaction statements that we handle textually before hitting
    // sqlparser -- these either don't parse cleanly across dialects
    // (`START TRANSACTION WITH CONSISTENT SNAPSHOT`, `BEGIN TRANSACTION named`)
    // or are simple keyword substitutions (`LAST_INSERT_ID()` -> `last_insert_rowid()`).
    if let Some(rewritten) = rewrite_transaction_statement(sql) {
        return Ok(vec![TranslateResult::Sql(rewritten)]);
    }

    // Check for no-op statements.
    if is_noop(sql, dialect) {
        return Ok(vec![TranslateResult::Noop]);
    }

    let parser_dialect: Box<dyn sqlparser::dialect::Dialect> = match dialect {
        Dialect::MySQL => Box::new(MySqlDialect {}),
        Dialect::PostgreSQL => Box::new(PostgreSqlDialect {}),
        Dialect::TDS => Box::new(MsSqlDialect {}),
    };

    // MySQL DDL pre-pass: sqlparser (0.57) cannot parse index column prefix
    // lengths (`KEY meta_key (meta_key(191))`), which every WordPress schema
    // uses. Display widths / prefix lengths are meaningless to SQLite, so
    // strip bare `(<digits>)` groups from DDL text before parsing. Applied
    // to CREATE/ALTER TABLE and CREATE INDEX only -- DML literals are never
    // touched. (`decimal(10,2)` is unaffected: it contains a comma.)
    let owned_sql;
    let sql = if dialect == Dialect::MySQL && is_mysql_ddl(sql) {
        owned_sql = strip_numeric_paren_groups(sql);
        owned_sql.as_str()
    } else {
        sql
    };

    // MySQL SELECT hints (`SQL_CALC_FOUND_ROWS`, `SQL_NO_CACHE`, ...) are not
    // parseable by sqlparser and meaningless to SQLite. WordPress's main
    // post/comment queries lead with SQL_CALC_FOUND_ROWS. Quote-aware strip.
    let owned_hintless;
    let sql = if dialect == Dialect::MySQL && has_mysql_select_hint(sql) {
        owned_hintless = strip_mysql_select_hints(sql);
        owned_hintless.as_str()
    } else {
        sql
    };

    let statements = Parser::parse_sql(parser_dialect.as_ref(), sql)
        .map_err(|e| TranslateError::Parse(e.to_string()))?;

    let mut results = Vec::with_capacity(statements.len());
    for stmt in statements {
        let rewritten = rewrite_statement(stmt, dialect)?;
        if dialect == Dialect::MySQL {
            // `ALTER TABLE ... ADD KEY/UNIQUE` expands to CREATE INDEX
            // statements (SQLite has no ALTER ... ADD CONSTRAINT).
            for expanded in mysql::expand_alter_table(rewritten) {
                results.push(TranslateResult::Sql(emit::emit_statement(&expanded)));
            }
        } else {
            let sqlite_sql = emit::emit_statement(&rewritten);
            results.push(TranslateResult::Sql(sqlite_sql));
        }
    }

    Ok(results)
}

/// Is this statement MySQL DDL that may carry display widths / index
/// prefix lengths (`CREATE TABLE`, `ALTER TABLE`, `CREATE [UNIQUE] INDEX`)?
fn is_mysql_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("CREATE TABLE")
        || upper.starts_with("CREATE TEMPORARY TABLE")
        || upper.starts_with("ALTER TABLE")
        || upper.starts_with("CREATE INDEX")
        || upper.starts_with("CREATE UNIQUE INDEX")
        || upper.starts_with("CREATE FULLTEXT INDEX")
}

/// Remove bare `(<digits>)` groups outside string/identifier quotes:
/// `bigint(20)` -> `bigint`, `KEY k (col(191))` -> `KEY k (col)`.
/// Groups containing anything but digits (e.g. `decimal(10,2)`) are kept.
fn strip_numeric_paren_groups(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c as char);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => {
                quote = Some(c);
                out.push(c as char);
                i += 1;
            }
            b'(' => {
                // Look ahead: digits then ')'.
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > i + 1 && j < bytes.len() && bytes[j] == b')' {
                    i = j + 1; // skip the whole (NNN) group
                } else {
                    out.push('(');
                    i += 1;
                }
            }
            _ => {
                out.push(c as char);
                i += 1;
            }
        }
    }
    out
}

/// MySQL-only SELECT hint keywords stripped before parsing.
const MYSQL_SELECT_HINTS: [&str; 6] = [
    "SQL_CALC_FOUND_ROWS",
    "SQL_NO_CACHE",
    "SQL_CACHE",
    "SQL_SMALL_RESULT",
    "SQL_BIG_RESULT",
    "SQL_BUFFER_RESULT",
];

/// Cheap pre-check (may false-positive on hints inside string literals —
/// the quote-aware strip below won't touch those).
fn has_mysql_select_hint(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    MYSQL_SELECT_HINTS.iter().any(|h| upper.contains(h))
}

/// Remove MySQL SELECT hint keywords outside string/identifier quotes.
fn strip_mysql_select_hints(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c as char);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => {
                quote = Some(c);
                out.push(c as char);
                i += 1;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                // Read a whole word, compare against the hint list.
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                let upper_word = word.to_ascii_uppercase();
                if MYSQL_SELECT_HINTS.contains(&upper_word.as_str()) {
                    // Drop the hint and one following space (if any) so
                    // `SELECT SQL_NO_CACHE x` becomes `SELECT x`.
                    if i < bytes.len() && bytes[i] == b' ' {
                        i += 1;
                    }
                } else {
                    out.push_str(word);
                }
            }
            _ => {
                out.push(c as char);
                i += 1;
            }
        }
    }
    out
}

/// Translate a SQL string, using a bounded LRU cache in front of the
/// parser + rewriter. See [`TranslateCache`].
///
/// Cache hits are O(1); misses fall through to [`translate`] and populate
/// the cache before returning. Errors are *not* cached -- a repeatedly-bad
/// statement will re-parse each time.
///
/// # Errors
///
/// Returns a [`TranslateError`] if the SQL cannot be parsed or contains
/// unsupported constructs.
pub fn translate_cached(
    cache: &TranslateCache,
    sql: &str,
    dialect: Dialect,
) -> Result<Vec<TranslateResult>, TranslateError> {
    if let Some(hit) = cache.get(dialect, sql) {
        return Ok(hit);
    }
    let results = translate(sql, dialect)?;
    cache.put(dialect, sql.to_string(), results.clone());
    Ok(results)
}

/// Rewrite MySQL/T-SQL-flavored transaction control statements to a form
/// SQLite accepts. Returns the rewritten SQL if the input matched a known
/// transaction shape, or `None` to fall through to the parser / no-op check.
///
/// Handled:
///   * `START TRANSACTION`, `START TRANSACTION READ ONLY`,
///     `START TRANSACTION READ WRITE`, `START TRANSACTION WITH CONSISTENT SNAPSHOT`
///     -> `BEGIN`
///   * `BEGIN`, `BEGIN WORK`, `BEGIN TRANSACTION`, `BEGIN TRANSACTION <name>`
///     (T-SQL named transactions) -> `BEGIN` (name is stripped; SQLite does not
///     support named transactions -- callers should use SAVEPOINT if they want
///     a nameable rollback point)
///   * `COMMIT`, `COMMIT WORK`, `COMMIT TRANSACTION [name]` -> `COMMIT`
///   * `ROLLBACK`, `ROLLBACK WORK`, `ROLLBACK TRANSACTION [name]` -> `ROLLBACK`
///     (but *not* `ROLLBACK TO [SAVEPOINT] name`, which is passed through so
///     SQLite's savepoint machinery handles it)
fn rewrite_transaction_statement(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();

    // START TRANSACTION ...  (MySQL / PG)
    if upper == "START TRANSACTION" || upper.starts_with("START TRANSACTION ") {
        return Some("BEGIN".to_string());
    }

    // BEGIN WORK / BEGIN TRANSACTION [name] -- T-SQL / SQL standard
    // Do NOT eat plain "BEGIN" or "BEGIN;" (SQLite accepts it as-is; passing it
    // through keeps `classify()` reporting Transaction and preserves any future
    // dialect-specific handling in the caller).
    if upper == "BEGIN WORK" {
        return Some("BEGIN".to_string());
    }
    if upper == "BEGIN TRANSACTION" || upper.starts_with("BEGIN TRANSACTION ") {
        return Some("BEGIN".to_string());
    }

    // COMMIT WORK / COMMIT TRANSACTION [name]
    if upper == "COMMIT WORK" {
        return Some("COMMIT".to_string());
    }
    if upper == "COMMIT TRANSACTION" || upper.starts_with("COMMIT TRANSACTION ") {
        return Some("COMMIT".to_string());
    }

    // ROLLBACK WORK / ROLLBACK TRANSACTION [name]
    // Careful: ROLLBACK TO SAVEPOINT name must pass through unchanged.
    if upper == "ROLLBACK WORK" {
        return Some("ROLLBACK".to_string());
    }
    if upper == "ROLLBACK TRANSACTION" {
        return Some("ROLLBACK".to_string());
    }
    if let Some(rest) = upper.strip_prefix("ROLLBACK TRANSACTION ") {
        // `ROLLBACK TRANSACTION TO SAVEPOINT foo` and `ROLLBACK TRANSACTION TO foo`
        // are T-SQL savepoint rollback forms -- pass those through to SQLite as
        // `ROLLBACK TO SAVEPOINT foo` / `ROLLBACK TO foo`.
        let rest = rest.trim_start();
        if let Some(after_to) = rest.strip_prefix("TO ") {
            let after_to = after_to.trim_start();
            let name_upper = if let Some(after_sp) = after_to.strip_prefix("SAVEPOINT ") {
                after_sp.trim()
            } else {
                after_to.trim()
            };
            // Extract the original-case name by taking the same trailing chars from `trimmed`.
            let orig_name = &trimmed[trimmed.len() - name_upper.len()..];
            return Some(format!("ROLLBACK TO SAVEPOINT {orig_name}"));
        }
        // Plain `ROLLBACK TRANSACTION some_name` -- strip the name.
        return Some("ROLLBACK".to_string());
    }

    None
}

/// Check if a SQL statement is a no-op for SQLite.
fn is_noop(sql: &str, _dialect: Dialect) -> bool {
    let upper = sql.trim().to_ascii_uppercase();

    // SET statements that have no SQLite equivalent.
    if let Some(after_set) = upper.strip_prefix("SET ") {
        let rest = after_set.trim_start();
        // Normalize `SET @@[session|global.]name` -> plain name, so autocommit-style
        // rules downstream match regardless of the `@@`/`SESSION.`/`GLOBAL.` prefix.
        let rest_norm = normalize_session_prefix(rest);
        let rest = rest_norm.as_deref().unwrap_or(rest);

        // Explicitly ignored no-ops (session/global toggles + T-SQL SET switches).
        if rest.starts_with("NAMES")
            || rest.starts_with("CHARACTER SET")
            || rest.starts_with("TIME_ZONE")
            || rest.starts_with("SQL_MODE")
            || rest.starts_with("NOCOUNT")
            || rest.starts_with("ANSI_NULLS")
            || rest.starts_with("QUOTED_IDENTIFIER")
            || rest.starts_with("XACT_ABORT")
        {
            tracing::debug!(sql = %sql.trim(), "SET: treating as noop");
            return true;
        }

        // SET autocommit = 1|0|ON|OFF|TRUE|FALSE
        if let Some(val) = rest.strip_prefix("AUTOCOMMIT") {
            let val = val.trim_start_matches(|c: char| c == '=' || c.is_whitespace());
            if val.starts_with('0') || val.starts_with("OFF") || val.starts_with("FALSE") {
                tracing::warn!(
                    "SET autocommit=0 requested but litewire does not emulate MySQL implicit \
                     transactions -- statements will still auto-commit unless wrapped in BEGIN/COMMIT"
                );
            } else {
                tracing::debug!("SET autocommit: noop (SQLite default matches)");
            }
            return true;
        }

        // SET [SESSION|GLOBAL] TRANSACTION ISOLATION LEVEL ... and bare
        // SET TRANSACTION ... (post-normalization the SESSION/GLOBAL prefix is
        // already stripped, so we only need to look for the TRANSACTION keyword here).
        if rest.starts_with("TRANSACTION") {
            tracing::debug!(
                "SET TRANSACTION: noop (SQLite runs at serializable isolation by default)"
            );
            return true;
        }

        // Any remaining `SET SESSION ...` / `SET GLOBAL ...` that we didn't
        // rewrite above (e.g. `SET SESSION wait_timeout=28800`) -- also noop.
        if rest.starts_with("SESSION") || rest.starts_with("GLOBAL") {
            tracing::debug!("SET SESSION/GLOBAL: noop");
            return true;
        }
    }

    // LOCK TABLES / UNLOCK TABLES -- SQLite has no equivalent; warn once so the
    // user knows the requested locking semantics are not being enforced.
    if upper.starts_with("LOCK TABLES") || upper.starts_with("LOCK TABLE ") {
        tracing::warn!("LOCK TABLES: noop (SQLite provides file-level locking only)");
        return true;
    }
    if upper == "UNLOCK TABLES" || upper.starts_with("UNLOCK TABLES ") {
        tracing::warn!("UNLOCK TABLES: noop");
        return true;
    }

    false
}

/// Strip a leading `@@` / `@@SESSION.` / `@@GLOBAL.` / `SESSION ` / `GLOBAL `
/// prefix from an uppercased SET body, returning the trimmed remainder.
/// Returns `None` if no prefix applied.
fn normalize_session_prefix(rest: &str) -> Option<String> {
    if let Some(after) = rest.strip_prefix("@@SESSION.") {
        return Some(after.to_string());
    }
    if let Some(after) = rest.strip_prefix("@@GLOBAL.") {
        return Some(after.to_string());
    }
    if let Some(after) = rest.strip_prefix("@@") {
        return Some(after.to_string());
    }
    if let Some(after) = rest.strip_prefix("SESSION.") {
        return Some(after.to_string());
    }
    if let Some(after) = rest.strip_prefix("GLOBAL.") {
        return Some(after.to_string());
    }
    // `SET SESSION TRANSACTION ...` / `SET GLOBAL TRANSACTION ...`
    if let Some(after) = rest.strip_prefix("SESSION ") {
        // Preserve the leading keyword so downstream `.starts_with("TRANSACTION")`
        // and `.starts_with("SESSION")` checks both remain accurate.
        // For `SET SESSION TRANSACTION ISOLATION LEVEL ...` we want to fall
        // through and match TRANSACTION; for `SET SESSION wait_timeout=...` we
        // want to fall through and match SESSION. Return the tail with SESSION
        // stripped only if TRANSACTION follows.
        if after.trim_start().starts_with("TRANSACTION") {
            return Some(after.trim_start().to_string());
        }
    }
    if let Some(after) = rest.strip_prefix("GLOBAL ") {
        if after.trim_start().starts_with("TRANSACTION") {
            return Some(after.trim_start().to_string());
        }
    }
    None
}

/// Rewrite a parsed statement from the source dialect to SQLite-compatible form.
fn rewrite_statement(mut stmt: Statement, dialect: Dialect) -> Result<Statement, TranslateError> {
    // Apply common rewrites (expressions, types).
    common::rewrite_statement(&mut stmt)?;

    // Apply dialect-specific rewrites.
    match dialect {
        Dialect::MySQL => mysql::rewrite_statement(&mut stmt)?,
        Dialect::PostgreSQL => postgres::rewrite_statement(&mut stmt)?,
        Dialect::TDS => tds::rewrite_statement(&mut stmt)?,
    }

    Ok(stmt)
}

/// Classify a SQL statement for routing decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind {
    Query,
    Mutation,
    Ddl,
    Transaction,
    Other,
}

/// Quickly classify a SQL string without full parsing.
#[must_use]
pub fn classify(sql: &str) -> StatementKind {
    let upper = sql.trim().to_ascii_uppercase();
    let first_word = upper.split_ascii_whitespace().next().unwrap_or("");

    match first_word {
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "PRAGMA" => StatementKind::Query,
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => StatementKind::Mutation,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" => StatementKind::Ddl,
        "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" => {
            StatementKind::Transaction
        }
        _ => StatementKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── classify ────────────────────────────────────────────────────────────

    #[test]
    fn classify_select() {
        assert_eq!(classify("SELECT * FROM users"), StatementKind::Query);
    }

    #[test]
    fn classify_show() {
        assert_eq!(classify("SHOW TABLES"), StatementKind::Query);
    }

    #[test]
    fn classify_describe() {
        assert_eq!(classify("DESCRIBE users"), StatementKind::Query);
    }

    #[test]
    fn classify_explain() {
        assert_eq!(classify("EXPLAIN SELECT 1"), StatementKind::Query);
    }

    #[test]
    fn classify_pragma() {
        assert_eq!(classify("PRAGMA table_info('users')"), StatementKind::Query);
    }

    #[test]
    fn classify_insert() {
        assert_eq!(
            classify("INSERT INTO users VALUES (1)"),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_update() {
        assert_eq!(
            classify("UPDATE users SET name = 'x'"),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_delete() {
        assert_eq!(
            classify("DELETE FROM users WHERE id = 1"),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_replace() {
        assert_eq!(
            classify("REPLACE INTO users VALUES (1, 'x')"),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_create() {
        assert_eq!(classify("CREATE TABLE users (id INT)"), StatementKind::Ddl);
    }

    #[test]
    fn classify_drop() {
        assert_eq!(classify("DROP TABLE users"), StatementKind::Ddl);
    }

    #[test]
    fn classify_alter() {
        assert_eq!(
            classify("ALTER TABLE users ADD col TEXT"),
            StatementKind::Ddl
        );
    }

    #[test]
    fn classify_begin() {
        assert_eq!(classify("BEGIN"), StatementKind::Transaction);
    }

    #[test]
    fn classify_commit() {
        assert_eq!(classify("COMMIT"), StatementKind::Transaction);
    }

    #[test]
    fn classify_rollback() {
        assert_eq!(classify("ROLLBACK"), StatementKind::Transaction);
    }

    #[test]
    fn classify_savepoint() {
        assert_eq!(classify("SAVEPOINT sp1"), StatementKind::Transaction);
    }

    #[test]
    fn classify_unknown() {
        assert_eq!(classify("VACUUM"), StatementKind::Other);
    }

    #[test]
    fn classify_case_insensitive() {
        assert_eq!(classify("select * from t"), StatementKind::Query);
    }

    #[test]
    fn classify_leading_whitespace() {
        assert_eq!(classify("  SELECT 1"), StatementKind::Query);
    }

    // ── translate integration ───────────────────────────────────────────────

    #[test]
    fn translate_empty_returns_empty() {
        // An empty input should still parse (to zero statements).
        // sqlparser may return an error or empty vec -- either is fine; a parse
        // error on empty input is acceptable.
        if let Ok(v) = translate("", Dialect::MySQL) {
            assert!(v.is_empty());
        }
    }

    #[test]
    fn translate_error_on_garbage() {
        let result = translate("NOT VALID SQL !!! @@@ {{{}}", Dialect::MySQL);
        assert!(result.is_err());
    }

    // -- Transaction rewrites --------------------------------------------------

    fn expect_sql(sql: &str, dialect: Dialect) -> String {
        let results = translate(sql, dialect).unwrap_or_else(|e| panic!("translate({sql:?}): {e}"));
        match &results[0] {
            TranslateResult::Sql(s) => s.clone(),
            other => panic!("expected Sql, got: {other:?}"),
        }
    }

    fn expect_noop(sql: &str, dialect: Dialect) {
        let results = translate(sql, dialect).unwrap_or_else(|e| panic!("translate({sql:?}): {e}"));
        assert!(
            matches!(results[0], TranslateResult::Noop),
            "expected Noop for {sql:?}, got: {:?}",
            results[0]
        );
    }

    #[test]
    fn start_transaction_becomes_begin() {
        assert_eq!(expect_sql("START TRANSACTION", Dialect::MySQL), "BEGIN");
        assert_eq!(expect_sql("start transaction", Dialect::MySQL), "BEGIN");
        assert_eq!(expect_sql("START TRANSACTION;", Dialect::MySQL), "BEGIN");
    }

    #[test]
    fn start_transaction_read_only_becomes_begin() {
        assert_eq!(
            expect_sql("START TRANSACTION READ ONLY", Dialect::MySQL),
            "BEGIN"
        );
    }

    #[test]
    fn start_transaction_with_consistent_snapshot_becomes_begin() {
        assert_eq!(
            expect_sql("START TRANSACTION WITH CONSISTENT SNAPSHOT", Dialect::MySQL),
            "BEGIN"
        );
    }

    #[test]
    fn tsql_begin_transaction_becomes_begin() {
        assert_eq!(expect_sql("BEGIN TRANSACTION", Dialect::TDS), "BEGIN");
    }

    #[test]
    fn tsql_named_begin_transaction_strips_name() {
        assert_eq!(
            expect_sql("BEGIN TRANSACTION my_txn", Dialect::TDS),
            "BEGIN"
        );
    }

    #[test]
    fn begin_work_becomes_begin() {
        assert_eq!(expect_sql("BEGIN WORK", Dialect::MySQL), "BEGIN");
    }

    #[test]
    fn commit_work_and_named_become_commit() {
        assert_eq!(expect_sql("COMMIT WORK", Dialect::MySQL), "COMMIT");
        assert_eq!(
            expect_sql("COMMIT TRANSACTION my_txn", Dialect::TDS),
            "COMMIT"
        );
    }

    #[test]
    fn rollback_work_and_named_become_rollback() {
        assert_eq!(expect_sql("ROLLBACK WORK", Dialect::MySQL), "ROLLBACK");
        assert_eq!(
            expect_sql("ROLLBACK TRANSACTION my_txn", Dialect::TDS),
            "ROLLBACK"
        );
    }

    #[test]
    fn rollback_to_savepoint_passthrough() {
        // Plain `ROLLBACK TO SAVEPOINT foo` is not a transaction control statement --
        // it's a savepoint rollback and must reach the parser so SQLite gets it.
        let results = translate("ROLLBACK TO SAVEPOINT foo", Dialect::MySQL).unwrap();
        // Whatever emit produces, it must still be a Sql result containing SAVEPOINT and foo.
        match &results[0] {
            TranslateResult::Sql(s) => {
                assert!(s.to_ascii_uppercase().contains("SAVEPOINT"), "got: {s}");
                assert!(s.contains("foo"), "got: {s}");
            }
            other => panic!("expected Sql, got: {other:?}"),
        }
    }

    #[test]
    fn tsql_rollback_transaction_to_savepoint_rewritten() {
        // T-SQL: ROLLBACK TRANSACTION TO SAVEPOINT foo -> ROLLBACK TO SAVEPOINT foo
        let sql = expect_sql("ROLLBACK TRANSACTION TO SAVEPOINT foo", Dialect::TDS);
        let up = sql.to_ascii_uppercase();
        assert!(up.contains("ROLLBACK"), "got: {sql}");
        assert!(up.contains("SAVEPOINT"), "got: {sql}");
        assert!(sql.contains("foo"), "got: {sql}");
    }

    #[test]
    fn savepoint_passthrough() {
        // SAVEPOINT foo should reach the parser and come out as valid SAVEPOINT SQL.
        let results = translate("SAVEPOINT foo", Dialect::MySQL).unwrap();
        match &results[0] {
            TranslateResult::Sql(s) => {
                assert!(s.to_ascii_uppercase().contains("SAVEPOINT"), "got: {s}");
                assert!(s.contains("foo"), "got: {s}");
            }
            other => panic!("expected Sql for SAVEPOINT, got: {other:?}"),
        }
    }

    #[test]
    fn release_savepoint_passthrough() {
        let results = translate("RELEASE SAVEPOINT foo", Dialect::MySQL).unwrap();
        match &results[0] {
            TranslateResult::Sql(s) => {
                assert!(s.to_ascii_uppercase().contains("RELEASE"), "got: {s}");
                assert!(s.contains("foo"), "got: {s}");
            }
            other => panic!("expected Sql for RELEASE SAVEPOINT, got: {other:?}"),
        }
    }

    // -- SET session/global/autocommit noops -----------------------------------

    #[test]
    fn set_autocommit_1_is_noop() {
        expect_noop("SET autocommit = 1", Dialect::MySQL);
        expect_noop("SET AUTOCOMMIT=ON", Dialect::MySQL);
        expect_noop("SET autocommit = true", Dialect::MySQL);
    }

    #[test]
    fn set_autocommit_0_is_noop_with_warning() {
        // We don't emulate implicit-transaction mode, but we do return Noop
        // rather than an error -- log will be a WARN at runtime.
        expect_noop("SET autocommit = 0", Dialect::MySQL);
        expect_noop("SET autocommit = OFF", Dialect::MySQL);
    }

    #[test]
    fn set_transaction_isolation_level_is_noop() {
        expect_noop(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            Dialect::MySQL,
        );
        expect_noop(
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
            Dialect::MySQL,
        );
        expect_noop(
            "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            Dialect::MySQL,
        );
    }

    #[test]
    fn set_at_at_session_variable_is_noop() {
        expect_noop("SET @@session.autocommit = 1", Dialect::MySQL);
        expect_noop("SET @@global.autocommit = 1", Dialect::MySQL);
        expect_noop("SET @@autocommit = 1", Dialect::MySQL);
    }

    #[test]
    fn lock_tables_is_noop() {
        expect_noop("LOCK TABLES users WRITE", Dialect::MySQL);
        expect_noop("UNLOCK TABLES", Dialect::MySQL);
    }

    // -- Cache -----------------------------------------------------------------

    #[test]
    fn translate_cached_hit_matches_uncached() {
        let cache = TranslateCache::new(16);
        let sql = "SELECT id, name FROM users WHERE id = ? ORDER BY id DESC LIMIT 10";
        let cold = translate(sql, Dialect::MySQL).unwrap();
        let warm = translate_cached(&cache, sql, Dialect::MySQL).unwrap();
        // Both should produce a single Sql result and be equal shape-wise.
        assert_eq!(cold.len(), warm.len());
        // Second call must hit the cache.
        assert_eq!(cache.len(), 1);
        let _ = translate_cached(&cache, sql, Dialect::MySQL).unwrap();
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn translate_cached_errors_are_not_stored() {
        let cache = TranslateCache::new(16);
        let bad = "!!! NOT SQL @@@";
        assert!(translate_cached(&cache, bad, Dialect::MySQL).is_err());
        assert_eq!(cache.len(), 0);
    }

    /// Micro-benchmark: translating the same WordPress-shaped query 5000
    /// times, cold vs cached. Not a criterion bench (we don't want a dev-dep
    /// on criterion); numbers get printed via `cargo test -- --nocapture
    /// translate_cache_speedup`.
    #[test]
    fn translate_cache_speedup() {
        use std::time::Instant;

        // A representative WP query with joins, ORDER BY, LIMIT.
        let sql = "SELECT wp_posts.* FROM wp_posts \
                   INNER JOIN wp_term_relationships \
                     ON wp_posts.ID = wp_term_relationships.object_id \
                   WHERE wp_posts.post_status = 'publish' \
                     AND wp_term_relationships.term_taxonomy_id IN (?, ?, ?) \
                   ORDER BY wp_posts.post_date DESC LIMIT 10";
        let iters = 5_000;

        // Cold path: no cache, translate() every time.
        let t0 = Instant::now();
        for _ in 0..iters {
            let _ = translate(sql, Dialect::MySQL).unwrap();
        }
        let cold = t0.elapsed();

        // Warm path: shared cache, one miss then N hits.
        let cache = TranslateCache::new(16);
        let t0 = Instant::now();
        for _ in 0..iters {
            let _ = translate_cached(&cache, sql, Dialect::MySQL).unwrap();
        }
        let warm = t0.elapsed();

        let cold_ns = cold.as_nanos() as f64 / f64::from(iters);
        let warm_ns = warm.as_nanos() as f64 / f64::from(iters);
        println!(
            "translate_cache_speedup: {iters} iters | cold={cold_ns:.0}ns/call \
             warm={warm_ns:.0}ns/call speedup={:.1}x",
            cold_ns / warm_ns
        );
        // Sanity: cached path must be strictly faster.
        assert!(
            warm < cold,
            "cached path is not faster: cold={cold:?} warm={warm:?}"
        );
    }
}
