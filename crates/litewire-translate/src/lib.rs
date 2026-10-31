//! SQL dialect translation for litewire.
//!
//! Translates MySQL, PostgreSQL, and T-SQL dialects to SQLite-compatible SQL
//! using `sqlparser-rs` for parsing and AST manipulation.

pub mod alter;
pub mod cache;
pub(crate) mod catalog;
pub mod common;
pub mod connection_id;
pub mod emit;
pub mod found_rows;
pub mod metadata;
pub mod mysql;
pub(crate) mod mysql_compat;
pub mod postgres;
pub mod tds;
pub mod user_locks;

pub use cache::TranslateCache;
pub use connection_id::connection_id_select_column;
pub use found_rows::{calc_found_rows_count_sql, found_rows_select_column};

/// The MySQL server version litewire reports, as a bare string literal.
///
/// Exists as a macro rather than only a constant because one of the three
/// places that needs it — the `@@version` fast path in [`metadata`] — splices
/// SQL literals together at compile time and needs the value *quoted*, which
/// `concat!` can only do with a literal. Prefer [`SERVER_VERSION`]; reach for
/// the macro only where a constant will not do.
macro_rules! server_version {
    () => {
        "8.0.36-litewire"
    };
}
pub(crate) use server_version;

/// The MySQL server version litewire reports, everywhere.
///
/// Three code paths answer "what version is this server?" and a client can
/// hit any of them: the wire handshake (`mysqli_get_server_info()`,
/// `PDO::ATTR_SERVER_VERSION`), the emulated `SELECT VERSION()`, and the
/// `@@version` system variable (`wpdb::db_version()`). They used to disagree
/// — the handshake said `8.0.36-litewire` while the two emulated paths said
/// `8.0.0-litewire` — so a client's answer depended on which one it asked.
/// See [issue #21](https://github.com/ephpm/litewire/issues/21).
///
/// This constant is the single source of truth for all three, so they cannot
/// drift apart again.
///
/// The value is a real MySQL 8.0.x version with an identifying suffix. It
/// has to be modern: WordPress >= 6.5 refuses to run against a server
/// reporting below 5.5.5, and it reads that from the handshake rather than
/// from `SELECT VERSION()`.
pub const SERVER_VERSION: &str = server_version!();

use std::sync::Arc;

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
    /// A MySQL column change SQLite cannot make in place (`MODIFY`,
    /// `CHANGE`, `ALTER COLUMN ... DEFAULT`): the session rebuilds the
    /// table (see [`alter`]).
    RebuildTable(alter::ColumnRebuild),
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
    // MySQL `||` / `&&` are logical OR / AND; rewritten before anything else
    // reads the text (SHOW filters included).
    let owned_logical;
    let sql = match (dialect == Dialect::MySQL)
        .then(|| mysql_compat::logical_operators(sql))
        .flatten()
    {
        Some(s) => {
            owned_logical = s;
            owned_logical.as_str()
        }
        None => sql,
    };

    // Check for metadata queries first (before parsing -- some are not valid SQL).
    if let Some(meta) = metadata::detect_metadata_query(sql, dialect)? {
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
    if dialect == Dialect::MySQL && mysql::is_table_option_only_alter(sql) {
        tracing::debug!(sql = %sql.trim(), "ALTER TABLE <table options>: noop");
        return Ok(vec![TranslateResult::Noop]);
    }

    // `SELECT ... LOCK IN SHARE MODE` (MySQL's legacy locking read) does not
    // parse; drop it like `FOR SHARE` (see `mysql::QueryShape`).
    let owned_unlocked;
    let sql = match (dialect == Dialect::MySQL)
        .then(|| mysql::strip_lock_in_share_mode(sql))
        .flatten()
    {
        Some(s) => {
            owned_unlocked = s;
            owned_unlocked.as_str()
        }
        None => sql,
    };

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
        let stripped = strip_numeric_paren_groups(sql);
        let stripped = mysql::normalize_drop_key(&stripped).unwrap_or(stripped);
        let stripped = mysql_compat::parenthesized_add_columns(&stripped).unwrap_or(stripped);
        let stripped = mysql_compat::strip_index_type(&stripped).unwrap_or(stripped);
        let stripped = mysql_compat::strip_index_comment(&stripped).unwrap_or(stripped);
        owned_sql = mysql::strip_key_column_direction(&stripped).unwrap_or(stripped);
        owned_sql.as_str()
    } else {
        sql
    };

    // `TRUNCATE [TABLE] t` -> `DELETE FROM t`.
    let owned_trunc;
    let sql = match (dialect == Dialect::MySQL)
        .then(|| mysql_compat::truncate_to_delete(sql))
        .flatten()
    {
        Some(s) => {
            owned_trunc = s;
            owned_trunc.as_str()
        }
        None => sql,
    };

    // `UPDATE a, b SET` (comma join) only parses as a CROSS JOIN.
    let owned_comma;
    let sql = match (dialect == Dialect::MySQL)
        .then(|| mysql_compat::comma_update_tables(sql))
        .flatten()
    {
        Some(s) => {
            owned_comma = s;
            owned_comma.as_str()
        }
        None => sql,
    };

    // `UPDATE ... [ORDER BY ...] LIMIT n` does not parse; the tail is split
    // off here and re-applied to the parsed statement below.
    let update_limit = if dialect == Dialect::MySQL {
        mysql_compat::split_update_limit(sql)
    } else {
        None
    };
    let owned_unlimited;
    let sql = match &update_limit {
        Some((head, _)) => {
            owned_unlimited = head.clone();
            owned_unlimited.as_str()
        }
        None => sql,
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

    let mut statements = Parser::parse_sql(parser_dialect.as_ref(), sql)
        .map_err(|e| TranslateError::Parse(e.to_string()))?;

    if dialect == Dialect::MySQL {
        if let Some((_, tail)) = &update_limit {
            let [stmt] = statements.as_mut_slice() else {
                return Err(TranslateError::Unsupported(
                    "LIMIT on a multi-statement UPDATE".into(),
                ));
            };
            mysql_compat::apply_update_limit(stmt, tail)?;
        }
        // `UPDATE a, b SET a.x = .., b.y = ..` updates two tables: one
        // SQLite UPDATE per table.
        let mut split = Vec::with_capacity(statements.len());
        for stmt in statements {
            match mysql_compat::split_multi_table_update(&stmt)? {
                Some(parts) => split.extend(parts),
                None => split.push(stmt),
            }
        }
        statements = split;
    }

    let check_catalog = catalog::mentions_information_schema(sql);
    let mut results = Vec::with_capacity(statements.len());
    for mut stmt in statements {
        // `information_schema.<t>` -> emulation subquery, inside the
        // client's own statement (see `catalog`).
        let catalog_tables = if check_catalog {
            catalog::substitute_information_schema(&mut stmt)?
        } else {
            Vec::new()
        };
        let rewritten = rewrite_statement(stmt, dialect)?;
        if dialect == Dialect::MySQL {
            // Inline/`ALTER TABLE ... ADD` keys expand to named CREATE INDEX
            // statements (SQLite has no ALTER ... ADD CONSTRAINT).
            for expanded in mysql::expand_ddl(rewritten)? {
                match expanded {
                    mysql::Expanded::Stmt(s) => {
                        let sqlite_sql = emit::emit_statement(&s);
                        results.push(TranslateResult::Sql(catalog::splice(
                            sqlite_sql,
                            &catalog_tables,
                        )));
                    }
                    mysql::Expanded::Rebuild(plan) => {
                        results.push(TranslateResult::RebuildTable(plan));
                    }
                }
            }
        } else {
            let sqlite_sql = emit::emit_statement(&rewritten);
            results.push(TranslateResult::Sql(catalog::splice(
                sqlite_sql,
                &catalog_tables,
            )));
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
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = quoted_span_end(bytes, i) {
            out.extend_from_slice(&bytes[i..end]);
            i = end;
            continue;
        }
        if bytes[i] == b'(' {
            // Look ahead: digits then ')'.
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 && j < bytes.len() && bytes[j] == b')' {
                i = j + 1; // skip the whole (NNN) group
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    bytes_to_string(out)
}

/// If a quoted string or identifier starts at `bytes[i]`, the index just
/// past its closing quote (or the end of input when unterminated).
///
/// Handles MySQL's backslash escapes inside `'...'` and `"..."` (so `'it\'s'`
/// does not end at the escaped quote) and doubled quotes (`'it''s'`, which
/// simply reads as two adjacent spans). Backtick identifiers have no
/// backslash escapes.
fn quoted_span_end(bytes: &[u8], i: usize) -> Option<usize> {
    let q = *bytes.get(i)?;
    if !matches!(q, b'\'' | b'"' | b'`') {
        return None;
    }
    let mut j = i + 1;
    while j < bytes.len() {
        let c = bytes[j];
        if c == b'\\' && q != b'`' {
            j += 2;
            continue;
        }
        if c == q {
            return Some(j + 1);
        }
        j += 1;
    }
    Some(bytes.len())
}

/// Reassemble text from a byte buffer built by copying whole spans of a
/// valid UTF-8 input and dropping only ASCII bytes, so it is valid UTF-8.
///
/// These pre-passes used to push each byte as a `char`, which re-encoded
/// every byte of a multi-byte character separately: `'café'` in a DDL
/// default or comment came out as `'cafÃ©'`.
fn bytes_to_string(out: Vec<u8>) -> String {
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
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
pub(crate) fn strip_mysql_select_hints(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = quoted_span_end(bytes, i) {
            out.extend_from_slice(&bytes[i..end]);
            i = end;
            continue;
        }
        let c = bytes[i];
        if c.is_ascii_alphabetic() || c == b'_' {
            // Read a whole word, compare against the hint list.
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &sql[start..i];
            if MYSQL_SELECT_HINTS
                .iter()
                .any(|h| h.eq_ignore_ascii_case(word))
            {
                // Drop the hint and one following space (if any) so
                // `SELECT SQL_NO_CACHE x` becomes `SELECT x`.
                if i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
            } else {
                out.extend_from_slice(word.as_bytes());
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    bytes_to_string(out)
}

/// Translate a SQL string, using a bounded LRU cache in front of the
/// parser + rewriter. See [`TranslateCache`].
///
/// Cache hits are O(1) and allocation-free: the returned
/// `Arc<[TranslateResult]>` is a shared handle on the cached entry, not a
/// copy of it. Misses fall through to [`translate`] and populate the cache
/// before returning. Errors are *not* cached -- a repeatedly-bad statement
/// will re-parse each time.
///
/// # Errors
///
/// Returns a [`TranslateError`] if the SQL cannot be parsed or contains
/// unsupported constructs.
pub fn translate_cached(
    cache: &TranslateCache,
    sql: &str,
    dialect: Dialect,
) -> Result<Arc<[TranslateResult]>, TranslateError> {
    if let Some(hit) = cache.get(dialect, sql) {
        return Ok(hit);
    }
    let results: Arc<[TranslateResult]> = translate(sql, dialect)?.into();
    cache.put(dialect, sql, Arc::clone(&results));
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
    if let Some(after) = rest.strip_prefix("GLOBAL ")
        && after.trim_start().starts_with("TRANSACTION")
    {
        return Some(after.trim_start().to_string());
    }
    None
}

/// Rewrite a parsed statement from the source dialect to SQLite-compatible form.
pub(crate) fn rewrite_statement(
    mut stmt: Statement,
    dialect: Dialect,
) -> Result<Statement, TranslateError> {
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
///
/// Routing hinges on whether the statement **produces a row set**: a
/// [`StatementKind::Query`] is run on the backend's `query()` path (which can
/// pull rows), everything else on `execute()` (which errors — "unexpected row
/// during execution" on Turso — if the statement yields a row). Two ordinary
/// forms are not identified by their leading keyword and must be recognised
/// explicitly, or they are mis-routed to `execute()` and fail (litewire#38):
///
/// * a **CTE whose outer statement is a `SELECT`** (`WITH … SELECT …`) is a
///   read — the leading keyword is `WITH`, not `SELECT`;
/// * a **`INSERT`/`UPDATE`/`DELETE` … `RETURNING`** returns the affected rows
///   and so must run on the `query()` path. This also fixes a data-integrity
///   bug: on the `execute()` path the row is written *before* the "unexpected
///   row" error, so a caller that retries on error double-inserts. On the
///   `query()` path the statement runs exactly once.
///
/// A **data-modifying CTE** (`WITH … INSERT/UPDATE/DELETE …` with no top-level
/// `RETURNING`) still produces no row set and stays a [`StatementKind::Mutation`].
#[must_use]
pub fn classify(sql: &str) -> StatementKind {
    let upper = sql.trim().to_ascii_uppercase();
    let first_word = upper.split_ascii_whitespace().next().unwrap_or("");

    match first_word {
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "PRAGMA" => StatementKind::Query,
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => classify_dml(sql),
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" => StatementKind::Ddl,
        "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" => {
            StatementKind::Transaction
        }
        "WITH" => classify_cte(sql),
        _ => StatementKind::Other,
    }
}

/// Classify a statement whose leading keyword is `INSERT`/`UPDATE`/`DELETE`/
/// `REPLACE`: a top-level `RETURNING` makes it row-returning (query path),
/// otherwise it is a plain mutation.
fn classify_dml(sql: &str) -> StatementKind {
    if has_top_level_returning(sql) {
        StatementKind::Query
    } else {
        StatementKind::Mutation
    }
}

/// Classify a `WITH …` statement by its outer (body) statement, which decides
/// read vs write. CTE subqueries live inside parentheses (depth > 0); the body
/// keyword is the first depth-0 statement keyword after the CTE list.
fn classify_cte(sql: &str) -> StatementKind {
    for word in top_level_words(sql) {
        match word.as_str() {
            "SELECT" | "VALUES" => return StatementKind::Query,
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => return classify_dml(sql),
            _ => {}
        }
    }
    StatementKind::Other
}

/// Whether a `RETURNING` keyword appears at parenthesis depth 0 (i.e. as the
/// statement's own `RETURNING` clause, not inside a subquery, string literal,
/// quoted identifier, or comment).
fn has_top_level_returning(sql: &str) -> bool {
    top_level_words(sql).iter().any(|w| w == "RETURNING")
}

/// Collect the ASCII word tokens of `sql` that lie at parenthesis depth 0 and
/// outside any string literal, quoted identifier, or comment — uppercased and
/// in order. Deliberately lightweight (no full parse): enough for [`classify`]
/// to find a CTE's outer statement and a top-level `RETURNING` without being
/// fooled by quoted text such as `INSERT INTO t VALUES ('returning home')`.
fn top_level_words(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut words = Vec::new();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < n {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                // Skip a quoted literal / identifier. A doubled quote is an
                // escaped quote (SQL standard); a backslash escapes the next
                // byte inside '…'/"…" (MySQL), but never inside `…`.
                i += 1;
                while i < n {
                    if bytes[i] == b'\\' && c != b'`' && i + 1 < n {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == c {
                        if i + 1 < n && bytes[i + 1] == c {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < n && bytes[i + 1] == b'-' => {
                i += 2;
                while i < n && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < n && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < n && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(n);
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                i += 1;
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < n && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if depth == 0 {
                    words.push(sql[start..i].to_ascii_uppercase());
                }
            }
            _ => i += 1,
        }
    }
    words
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

    // ── classify: CTE outer statement (litewire#38) ──────────────────────────

    #[test]
    fn classify_cte_outer_select_is_query() {
        assert_eq!(
            classify("WITH au AS (SELECT id FROM users WHERE active = 1) SELECT * FROM au"),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_cte_recursive_outer_select_is_query() {
        assert_eq!(
            classify(
                "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) \
                 SELECT x FROM cnt"
            ),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_multi_cte_outer_select_is_query() {
        assert_eq!(
            classify("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b"),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_cte_lowercase_outer_select_is_query() {
        assert_eq!(
            classify("with au as (select id from users) select * from au"),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_data_modifying_cte_stays_mutation() {
        // The outer statement writes and returns no rows -> execute path.
        assert_eq!(
            classify(
                "WITH stale AS (SELECT id FROM users WHERE active = 0) \
                      DELETE FROM users WHERE id IN (SELECT id FROM stale)"
            ),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_data_modifying_cte_with_returning_is_query() {
        assert_eq!(
            classify(
                "WITH t AS (SELECT 1 AS id) \
                      INSERT INTO log (id) SELECT id FROM t RETURNING id"
            ),
            StatementKind::Query
        );
    }

    // ── classify: DML RETURNING (litewire#38) ────────────────────────────────

    #[test]
    fn classify_insert_returning_is_query() {
        assert_eq!(
            classify("INSERT INTO users (name) VALUES ('Alice') RETURNING id"),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_update_returning_is_query() {
        assert_eq!(
            classify("UPDATE users SET age = 31 WHERE id = 1 RETURNING age"),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_delete_returning_is_query() {
        assert_eq!(
            classify("DELETE FROM users WHERE id = 1 RETURNING id"),
            StatementKind::Query
        );
    }

    #[test]
    fn classify_plain_insert_stays_mutation() {
        assert_eq!(
            classify("INSERT INTO users (name) VALUES ('Alice')"),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_returning_inside_string_literal_is_not_a_returning_clause() {
        // The word RETURNING only appears inside a string literal, so this is a
        // plain mutation -- mis-routing it to query() would lose last_insert_id
        // and affected-row reporting.
        assert_eq!(
            classify("INSERT INTO notes (body) VALUES ('she is returning id tomorrow')"),
            StatementKind::Mutation
        );
    }

    #[test]
    fn classify_returning_inside_subquery_is_not_top_level() {
        // A RETURNING nested in a subquery does not make the outer INSERT
        // row-returning; only a top-level RETURNING does.
        assert_eq!(
            classify("INSERT INTO t (id) SELECT id FROM (SELECT 1 AS id) s"),
            StatementKind::Mutation
        );
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
