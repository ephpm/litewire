//! Metadata query detection and translation.
//!
//! Handles MySQL's `SHOW TABLES`, `SHOW COLUMNS`, `SHOW INDEX`,
//! `SHOW CREATE TABLE`, `SHOW DATABASES` and `DESCRIBE`, `SELECT @@var`
//! system-variable reads, and the PostgreSQL/T-SQL catalog shortcuts
//! (`pg_catalog`, `sys.tables`, `sp_tables`). `information_schema` is not
//! handled here: the translator substitutes an emulation for the
//! `information_schema` table inside the client's own query (see
//! [`crate::catalog`]), so the client's projection and predicates apply.
//!
//! # `SHOW` filters are applied, or the statement fails
//!
//! `SHOW ... LIKE 'pat'` and `SHOW ... WHERE <expr>` are parsed, and the
//! predicate runs (through the normal MySQL translator) against the emulated
//! rows. Self-migrating applications probe their schema with exactly these
//! forms — `SHOW COLUMNS FROM t WHERE Field = 'x'`,
//! `SHOW INDEX FROM t WHERE Key_name = 'x'` — and add the column or index
//! only when the probe returns no rows. An implementation that ignored the
//! predicate (as this module once did, by reading only the table name)
//! returns every row, so the guard concludes the column or index already
//! exists and the migration silently never runs.
//!
//! Every `SHOW` form or clause this module does not implement is an
//! **error**, never an unfiltered result.

use sqlparser::ast::{Expr, Ident, SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::catalog::{self, Tables, sql_str};
use crate::{Dialect, TranslateError, server_version};

/// A detected metadata query that requires special handling.
#[derive(Debug, Clone)]
pub enum MetadataQuery {
    /// `SHOW [FULL] TABLES [FROM db] [LIKE 'pat' | WHERE expr]`.
    ShowTables {
        /// `FULL`: add the `Table_type` column.
        full: bool,
        /// Translated SQLite predicate over the result columns, if any.
        filter: Option<String>,
    },
    /// Unfiltered `SHOW DATABASES` / `SHOW SCHEMAS`.
    ShowDatabases,
    /// `SHOW [FULL] COLUMNS FROM <table> [LIKE | WHERE]` or
    /// `DESCRIBE <table> [column]`.
    ShowColumns {
        /// Table name, unquoted.
        table: String,
        /// `FULL`: add the `Collation`, `Privileges` and `Comment` columns.
        full: bool,
        /// Translated SQLite predicate over the result columns, if any.
        filter: Option<String>,
    },
    /// `SHOW CREATE TABLE <table>`.
    ShowCreateTable {
        /// Table name, unquoted.
        table: String,
    },
    /// `SHOW {INDEX|INDEXES|KEYS} FROM <table> [WHERE expr]`.
    ShowIndex {
        /// Table name, unquoted.
        table: String,
        /// Translated SQLite predicate over the result columns, if any.
        filter: Option<String>,
    },
    /// `SELECT @@variable [[AS] alias], ... [LIMIT n]` — MySQL system
    /// variables, answered with constants (and, for `sql_mode`, the
    /// session's value; see [`SessionVars`]).
    SystemVariables { variables: Vec<SystemVariable> },
    /// `SELECT ... FROM pg_catalog.pg_tables` or similar.
    PgCatalogTables,
    /// `SELECT ... FROM pg_catalog.pg_columns` or `pg_attribute` for a table.
    PgCatalogColumns { table: String },
    /// `SELECT ... FROM sys.tables` (T-SQL).
    SysTables,
    /// `SELECT ... FROM sys.columns` (T-SQL) for a table.
    SysColumns { table: String },
}

/// One `@@variable` read in a [`MetadataQuery::SystemVariables`] query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemVariable {
    /// Variable name as written, without `@@` and scope (`sql_mode`).
    pub name: String,
    /// `@@GLOBAL.x` (as opposed to `@@x`, `@@SESSION.x`, `@@LOCAL.x`).
    pub global: bool,
    /// The result column name: the select-list text as written
    /// (`@@SESSION.sql_mode`), or the alias. MySQL names the column this
    /// way and PHP reads it back by that key (`$row['@@sql_mode']`).
    pub label: String,
}

/// Per-session values a metadata answer depends on.
///
/// Translation is stateless and cached; the session layer passes these in
/// when it turns a [`MetadataQuery`] into SQL
/// ([`MetadataQuery::to_sqlite_sql_with`]).
#[derive(Debug, Clone, Copy, Default)]
pub struct SessionVars<'a> {
    /// The session `sql_mode`, as last set by `SET [SESSION] sql_mode = ...`
    /// on this connection. `None` = never set: the server default,
    /// [`DEFAULT_SQL_MODE`].
    pub sql_mode: Option<&'a str>,
}

/// The `sql_mode` litewire reports by default (`@@GLOBAL.sql_mode`, and
/// `@@sql_mode` until a session sets one): empty.
///
/// It is the closest honest answer. litewire enforces none of the modes in
/// MySQL 8.4's default (`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,
/// NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,
/// NO_ENGINE_SUBSTITUTION`): SQLite accepts bare columns next to an
/// aggregate, stores out-of-range or ill-typed values instead of rejecting
/// them, and returns `NULL` for division by zero. Reporting those modes
/// would claim checks that do not happen. Applications that probe
/// `@@sql_mode` to *remove* `ONLY_FULL_GROUP_BY` see it absent, which is
/// accurate. A session `SET sql_mode = ...` is recorded and read back, but
/// not enforced.
pub const DEFAULT_SQL_MODE: &str = "";

/// Alias of the derived table the `SHOW` emulations select from — and the
/// placeholder table name a `SHOW` filter is translated against.
const SHOW_ALIAS: &str = "__litewire_show";

/// `WHERE (<filter>)`, or nothing.
fn where_clause(filter: Option<&String>) -> String {
    filter.map_or_else(String::new, |f| format!(" WHERE ({f})"))
}

impl MetadataQuery {
    /// Convert this metadata query to the SQLite SQL that produces
    /// equivalent results, for a session that has set nothing (see
    /// [`MetadataQuery::to_sqlite_sql_with`]).
    #[must_use]
    pub fn to_sqlite_sql(&self) -> String {
        self.to_sqlite_sql_with(&SessionVars::default())
    }

    /// Convert this metadata query to SQLite SQL, answering session-scoped
    /// variables from `vars`.
    #[must_use]
    pub fn to_sqlite_sql_with(&self, vars: &SessionVars<'_>) -> String {
        match self {
            Self::ShowTables { full, filter } => {
                let cols = if *full {
                    "\"Tables_in_database\", \"Table_type\""
                } else {
                    "\"Tables_in_database\""
                };
                format!(
                    "SELECT {cols} FROM (SELECT TABLE_NAME AS \"Tables_in_database\", \
                     TABLE_TYPE AS \"Table_type\" FROM ({})) AS {SHOW_ALIAS}{} \
                     ORDER BY \"Tables_in_database\"",
                    catalog::table_rows(),
                    where_clause(filter.as_ref()),
                )
            }
            Self::ShowDatabases => "SELECT 'main' AS \"Database\"".into(),
            Self::ShowColumns {
                table,
                full,
                filter,
            } => {
                // MySQL-shaped `SHOW [FULL] COLUMNS` / `DESCRIBE` output.
                //
                // WordPress's wpdb::get_table_charset() reads `Collation` and
                // `Type` from `SHOW FULL COLUMNS`; if they're missing it
                // returns a WP_Error and sanitize_option() then blanks the
                // value being saved (observed: every option routed through
                // strip_invalid_text_for_column stored as ''). Text affinity
                // maps to longtext/longblob so WP applies no 64 KB length cap.
                let cols = if *full {
                    "\"Field\", \"Type\", \"Collation\", \"Null\", \"Key\", \"Default\", \
                     \"Extra\", \"Privileges\", \"Comment\""
                } else {
                    "\"Field\", \"Type\", \"Null\", \"Key\", \"Default\", \"Extra\""
                };
                format!(
                    "SELECT {cols} FROM (SELECT COLUMN_NAME AS \"Field\", COLUMN_TYPE AS \"Type\", \
                     COLLATION_NAME AS \"Collation\", IS_NULLABLE AS \"Null\", \
                     COLUMN_KEY AS \"Key\", COLUMN_DEFAULT AS \"Default\", EXTRA AS \"Extra\", \
                     PRIVILEGES AS \"Privileges\", COLUMN_COMMENT AS \"Comment\", \
                     ORDINAL_POSITION AS __ord FROM ({})) AS {SHOW_ALIAS}{} ORDER BY __ord",
                    catalog::column_rows(Tables::One(table)),
                    where_clause(filter.as_ref()),
                )
            }
            Self::ShowCreateTable { table } => {
                format!(
                    "SELECT name AS \"Table\", sql AS \"Create Table\" FROM sqlite_master \
                     WHERE type = 'table' AND name = {} COLLATE NOCASE",
                    sql_str(table)
                )
            }
            Self::ShowIndex { table, filter } => {
                format!(
                    "SELECT \"Table\", \"Non_unique\", \"Key_name\", \"Seq_in_index\", \
                     \"Column_name\", \"Collation\", \"Cardinality\", \"Sub_part\", \"Packed\", \
                     \"Null\", \"Index_type\", \"Comment\", \"Index_comment\", \"Visible\", \
                     \"Expression\" FROM (SELECT TABLE_NAME AS \"Table\", \
                     NON_UNIQUE AS \"Non_unique\", INDEX_NAME AS \"Key_name\", \
                     SEQ_IN_INDEX AS \"Seq_in_index\", COLUMN_NAME AS \"Column_name\", \
                     'A' AS \"Collation\", NULL AS \"Cardinality\", NULL AS \"Sub_part\", \
                     NULL AS \"Packed\", NULLABLE AS \"Null\", 'BTREE' AS \"Index_type\", \
                     '' AS \"Comment\", '' AS \"Index_comment\", 'YES' AS \"Visible\", \
                     NULL AS \"Expression\", __ord FROM ({})) AS {SHOW_ALIAS}{} \
                     ORDER BY __ord, \"Seq_in_index\"",
                    catalog::index_rows(Tables::One(table)),
                    where_clause(filter.as_ref()),
                )
            }
            Self::SystemVariables { variables } => {
                // Build a SELECT with synthetic values for each variable.
                let cols: Vec<String> = variables
                    .iter()
                    .map(|v| {
                        let val = if v.name.eq_ignore_ascii_case("sql_mode") {
                            let mode = if v.global {
                                DEFAULT_SQL_MODE
                            } else {
                                vars.sql_mode.unwrap_or(DEFAULT_SQL_MODE)
                            };
                            sql_str(mode)
                        } else {
                            system_variable_value(&v.name).to_string()
                        };
                        format!("{val} AS \"{}\"", v.label.replace('"', "\"\""))
                    })
                    .collect();
                format!("SELECT {}", cols.join(", "))
            }
            Self::PgCatalogTables => {
                "SELECT name AS tablename, 'main' AS schemaname FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name".into()
            }
            Self::PgCatalogColumns { table } => {
                format!(
                    "SELECT '{table}' AS table_name, name AS column_name, \
                     type AS data_type, \
                     CASE WHEN \"notnull\" = 1 THEN 'NO' ELSE 'YES' END AS is_nullable, \
                     dflt_value AS column_default \
                     FROM pragma_table_info('{table}') ORDER BY cid"
                )
            }
            Self::SysTables => {
                "SELECT name, 'U' AS type FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name".into()
            }
            Self::SysColumns { table } => {
                format!(
                    "SELECT name AS COLUMN_NAME, type AS DATA_TYPE, \
                     CASE WHEN \"notnull\" = 1 THEN 0 ELSE 1 END AS is_nullable \
                     FROM pragma_table_info('{table}') ORDER BY cid"
                )
            }
        }
    }
}

/// Return a synthetic value for a MySQL system variable.
fn system_variable_value(name: &str) -> &'static str {
    match name.to_ascii_lowercase().as_str() {
        "max_allowed_packet" => "67108864", // 64 MiB
        "wait_timeout" => "28800",          // 8 hours
        "interactive_timeout" => "28800",
        "net_write_timeout" => "60",
        "net_read_timeout" => "30",
        "socket" => "'/tmp/litewire.sock'",
        "character_set_client" => "'utf8mb4'",
        "character_set_connection" => "'utf8mb4'",
        "character_set_results" => "'utf8mb4'",
        "character_set_server" => "'utf8mb4'",
        "collation_connection" => "'utf8mb4_general_ci'",
        "collation_server" => "'utf8mb4_general_ci'",
        // Quoted here, but the value itself comes from `crate::SERVER_VERSION`
        // via the macro so the handshake and `SELECT VERSION()` cannot drift
        // away from `@@version` again (issue #21).
        "version" => concat!('\'', server_version!(), '\''),
        "version_comment" => "'litewire'",
        "sql_mode" => "''",
        "lower_case_table_names" => "0",
        "autocommit" => "1",
        "transaction_isolation" | "tx_isolation" => "'SERIALIZABLE'",
        "identity" => "last_insert_rowid()",
        "rowcount" => "changes()",
        _ => "NULL",
    }
}

/// Detect if a SQL string is a metadata query that needs special handling.
///
/// Returns `Ok(None)` if it's a regular query that should go through the
/// parser.
///
/// # Errors
///
/// A `SHOW` / `DESCRIBE` statement this module recognises but cannot answer
/// faithfully — an unsupported `SHOW` form, an unparseable or unsupported
/// `LIKE` / `WHERE` clause, trailing tokens — is an error rather than an
/// unfiltered result set.
pub fn detect_metadata_query(
    sql: &str,
    _dialect: Dialect,
) -> Result<Option<MetadataQuery>, TranslateError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();

    if let Some(show) = detect_show(trimmed)? {
        return Ok(Some(show));
    }

    // SELECT @@variable queries (MySQL system variables).
    if upper.starts_with("SELECT")
        && trimmed.contains("@@")
        && let Some(variables) = parse_system_variable_select(trimmed)
    {
        return Ok(Some(MetadataQuery::SystemVariables { variables }));
    }

    // pg_catalog queries.
    if upper.contains("PG_CATALOG.PG_TABLES") || upper.contains("PG_CATALOG.PG_CLASS") {
        return Ok(Some(MetadataQuery::PgCatalogTables));
    }
    if upper.contains("PG_CATALOG.PG_ATTRIBUTE") {
        let table_filter = extract_where_value_original(trimmed, "TABLE_NAME")
            .or_else(|| extract_where_value_original(trimmed, "ATTRELID"));
        return Ok(Some(MetadataQuery::PgCatalogColumns {
            table: table_filter.unwrap_or_default(),
        }));
    }

    // T-SQL sys.tables / sys.columns.
    if upper.contains("SYS.TABLES") || upper.contains("SYSOBJECTS") {
        return Ok(Some(MetadataQuery::SysTables));
    }
    if upper.contains("SYS.COLUMNS") || upper.contains("SYSCOLUMNS") {
        let table_filter = extract_where_value_original(trimmed, "TABLE_NAME");
        return Ok(Some(MetadataQuery::SysColumns {
            table: table_filter.unwrap_or_default(),
        }));
    }

    // T-SQL stored procedure metadata: sp_tables, sp_columns.
    if upper.starts_with("EXEC SP_TABLES") || upper.starts_with("SP_TABLES") {
        return Ok(Some(MetadataQuery::SysTables));
    }
    if upper.starts_with("EXEC SP_COLUMNS") || upper.starts_with("SP_COLUMNS") {
        // Try to extract the table name argument.
        let table = trimmed
            .split_ascii_whitespace()
            .nth(1)
            .map(|s| {
                sanitize_identifier(s.trim_matches('\'').trim_matches('"').trim_end_matches(';'))
            })
            .unwrap_or_default();
        return Ok(Some(MetadataQuery::SysColumns { table }));
    }

    Ok(None)
}

// ── SELECT @@variable ───────────────────────────────────────────────────────

/// Parse `SELECT @@[scope.]name [[AS] alias], ... [LIMIT n] [;]`.
///
/// `None` unless *every* select item is a bare system variable — a
/// statement mixing in anything else (`SELECT @@version, NOW()`,
/// `SELECT @@x + 1`, a `FROM`) goes to the parser instead of having the
/// rest dropped. The trailing `LIMIT n` is accepted because the `mysql`
/// command-line client sends `select @@version_comment limit 1` on connect.
fn parse_system_variable_select(sql: &str) -> Option<Vec<SystemVariable>> {
    let toks = lex(sql).ok()?;
    let mut i = 0;
    let word = |i: usize, w: &str| matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w));
    if !word(i, "SELECT") {
        return None;
    }
    i += 1;
    let mut out = Vec::new();
    loop {
        // `@@`
        if !(matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Other('@')))
            && matches!(toks.get(i + 1).map(|l| &l.tok), Some(Tok::Other('@'))))
        {
            return None;
        }
        let start = toks[i].start;
        i += 2;
        let Some(Tok::Word(first)) = toks.get(i).map(|l| &l.tok) else {
            return None;
        };
        let (name, global) = if matches!(toks.get(i + 1).map(|l| &l.tok), Some(Tok::Dot)) {
            let scope = first.to_ascii_uppercase();
            if !matches!(scope.as_str(), "GLOBAL" | "SESSION" | "LOCAL") {
                return None;
            }
            let Some(Tok::Word(n)) = toks.get(i + 2).map(|l| &l.tok) else {
                return None;
            };
            i += 3;
            (n.clone(), scope == "GLOBAL")
        } else {
            i += 1;
            (first.clone(), false)
        };
        let end = toks.get(i - 1).map_or(sql.len(), |l| {
            l.start
                + match &l.tok {
                    Tok::Word(w) => w.len(),
                    _ => 1,
                }
        });
        let mut label = sql[start..end].to_string();
        // Optional alias.
        if word(i, "AS") {
            i += 1;
        }
        match toks.get(i).map(|l| &l.tok) {
            Some(Tok::Word(a)) if !a.eq_ignore_ascii_case("LIMIT") => {
                label.clone_from(a);
                i += 1;
            }
            Some(Tok::Quoted(a) | Tok::Str(a)) => {
                label.clone_from(a);
                i += 1;
            }
            _ => {}
        }
        out.push(SystemVariable {
            name,
            global,
            label,
        });
        match toks.get(i).map(|l| &l.tok) {
            Some(Tok::Other(',')) => i += 1,
            _ => break,
        }
    }
    // Optional `LIMIT n`, optional `;`, then the end.
    if word(i, "LIMIT") {
        if !matches!(toks.get(i + 1).map(|l| &l.tok), Some(Tok::Word(n)) if n.bytes().all(|b| b.is_ascii_digit()))
        {
            return None;
        }
        i += 2;
    }
    if matches!(toks.get(i).map(|l| &l.tok), Some(Tok::Semi)) {
        i += 1;
    }
    (i == toks.len()).then_some(out)
}

// ── SET sql_mode ────────────────────────────────────────────────────────────

/// The right-hand side of a `SET ... sql_mode = <value>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlModeValue {
    /// A string literal, normalized like MySQL does for display: upper-case,
    /// no spaces around the commas. (Combination modes such as
    /// `TRADITIONAL` are not expanded.)
    Literal(String),
    /// `DEFAULT`: back to [`DEFAULT_SQL_MODE`].
    Default,
    /// Any other expression, as written — e.g.
    /// `REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', '')` or a bound `?`. The
    /// session evaluates it (see [`SetSqlMode::evaluation_sql`]).
    Expression(String),
}

/// A detected `SET [SESSION|GLOBAL] sql_mode = ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetSqlMode {
    /// `SET GLOBAL` / `SET PERSIST` / `@@GLOBAL.`: changes nothing a client
    /// can observe on this connection (MySQL applies it to *new* sessions;
    /// litewire's default never changes).
    pub global: bool,
    /// The value assigned.
    pub value: SqlModeValue,
}

impl SetSqlMode {
    /// For an [`SqlModeValue::Expression`]: a `SELECT` (in the MySQL
    /// dialect, to be translated and run as usual) that evaluates it, with
    /// every `@@[SESSION.|LOCAL.]sql_mode` replaced by `current` and
    /// `@@GLOBAL.sql_mode` by the default. Bound `?` parameters stay in
    /// place.
    #[must_use]
    pub fn evaluation_sql(&self, current: &str) -> Option<String> {
        let SqlModeValue::Expression(expr) = &self.value else {
            return None;
        };
        let toks = lex(expr).ok()?;
        let mut out = String::from("SELECT ");
        let mut last = 0;
        let mut i = 0;
        while i < toks.len() {
            if let Some((end_tok, global)) = sql_mode_reference(&toks, i) {
                let end = toks[end_tok].start + "sql_mode".len();
                out.push_str(&expr[last..toks[i].start]);
                out.push_str(&sql_str(if global { DEFAULT_SQL_MODE } else { current }));
                last = end;
                i = end_tok + 1;
            } else {
                i += 1;
            }
        }
        out.push_str(&expr[last..]);
        Some(out)
    }
}

/// If tokens at `i` spell `@@[scope.]sql_mode`, the index of the
/// `sql_mode` token and whether the scope is `GLOBAL`.
fn sql_mode_reference(toks: &[Lexed], i: usize) -> Option<(usize, bool)> {
    let tok = |k: usize| toks.get(k).map(|l| &l.tok);
    if !(matches!(tok(i), Some(Tok::Other('@'))) && matches!(tok(i + 1), Some(Tok::Other('@')))) {
        return None;
    }
    match (tok(i + 2), tok(i + 3), tok(i + 4)) {
        (Some(Tok::Word(scope)), Some(Tok::Dot), Some(Tok::Word(name)))
            if name.eq_ignore_ascii_case("sql_mode")
                && ["GLOBAL", "SESSION", "LOCAL"]
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(scope)) =>
        {
            Some((i + 4, scope.eq_ignore_ascii_case("GLOBAL")))
        }
        (Some(Tok::Word(name)), _, _) if name.eq_ignore_ascii_case("sql_mode") => {
            Some((i + 2, false))
        }
        _ => None,
    }
}

/// Number of `?` placeholders in `sql`, outside quotes and comments.
#[must_use]
pub fn count_placeholders(sql: &str) -> usize {
    lex(sql).map_or(0, |toks| {
        toks.iter()
            .filter(|l| matches!(l.tok, Tok::Other('?')))
            .count()
    })
}

/// Normalize a `sql_mode` literal the way MySQL displays it.
fn normalize_sql_mode(s: &str) -> String {
    s.split(',')
        .map(|m| m.trim().to_ascii_uppercase())
        .filter(|m| !m.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

/// Detect `SET [SESSION|LOCAL|GLOBAL|PERSIST|PERSIST_ONLY]
/// [@@[scope.]]sql_mode {=|:=} <value> [;]` — a single assignment.
///
/// `None` for anything else, including a `SET` that assigns several
/// variables at once (those stay plain no-ops).
#[must_use]
pub fn detect_set_sql_mode(sql: &str) -> Option<SetSqlMode> {
    let trimmed = sql.trim();
    if trimmed.len() < 3 || !trimmed[..3].eq_ignore_ascii_case("SET") {
        return None;
    }
    let toks = lex(trimmed).ok()?;
    let tok = |k: usize| toks.get(k).map(|l| &l.tok);
    let is_word =
        |k: usize, w: &str| matches!(tok(k), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w));
    if !is_word(0, "SET") {
        return None;
    }
    let mut i = 1;
    let mut global = false;
    for scope in ["SESSION", "LOCAL", "GLOBAL", "PERSIST", "PERSIST_ONLY"] {
        if is_word(i, scope) {
            global = !matches!(scope, "SESSION" | "LOCAL");
            i += 1;
            break;
        }
    }
    if let Some((end, g)) = sql_mode_reference(&toks, i) {
        global |= g;
        i = end + 1;
    } else if is_word(i, "sql_mode") {
        i += 1;
    } else {
        return None;
    }
    // `=` or `:=`
    match (tok(i), tok(i + 1)) {
        (Some(Tok::Other(':')), Some(Tok::Other('='))) => i += 2,
        (Some(Tok::Other('=')), _) => i += 1,
        _ => return None,
    }
    let value_start = toks.get(i)?.start;
    // The value runs to the end (minus a trailing `;`); a top-level comma
    // means several assignments.
    let mut end_tok = toks.len();
    if matches!(toks.last().map(|l| &l.tok), Some(Tok::Semi)) {
        end_tok -= 1;
    }
    let mut depth = 0i32;
    for l in &toks[i..end_tok] {
        match l.tok {
            Tok::Other('(') => depth += 1,
            Tok::Other(')') => depth -= 1,
            Tok::Other(',') if depth == 0 => return None,
            Tok::Semi => return None,
            _ => {}
        }
    }
    let value_end = if end_tok < toks.len() {
        toks[end_tok].start
    } else {
        trimmed.len()
    };
    let raw = trimmed[value_start..value_end].trim();
    let value = match &toks[i..end_tok] {
        [
            Lexed {
                tok: Tok::Str(s), ..
            },
        ] => SqlModeValue::Literal(normalize_sql_mode(s)),
        [
            Lexed {
                tok: Tok::Word(w), ..
            },
        ] if w.eq_ignore_ascii_case("DEFAULT") => SqlModeValue::Default,
        [] => return None,
        _ => SqlModeValue::Expression(raw.to_string()),
    };
    Some(SetSqlMode { global, value })
}

// ── SHOW / DESCRIBE grammar ─────────────────────────────────────────────────

/// One lexical token of a `SHOW` / `DESCRIBE` prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Tok {
    /// Bare word (keyword or unquoted identifier), as written.
    Word(String),
    /// Backtick- or double-quoted identifier, unescaped.
    Quoted(String),
    /// Single-quoted string, unescaped (only used for `DESCRIBE t 'pat'`).
    Str(String),
    /// `.`
    Dot,
    /// `;`
    Semi,
    /// Anything else (one character).
    Other(char),
}

/// A token and the byte offset where it starts in the statement.
pub(crate) struct Lexed {
    pub(crate) tok: Tok,
    pub(crate) start: usize,
}

/// Quote-aware lexer for the `SHOW` / `DESCRIBE` prefix. Skips whitespace
/// and comments. Fails on an unterminated quote.
pub(crate) fn lex(sql: &str) -> Result<Vec<Lexed>, TranslateError> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments: `/* ... */`, `-- ...`, `# ...`.
        if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            let end = sql[i + 2..]
                .find("*/")
                .ok_or_else(|| TranslateError::Parse("unterminated comment".into()))?;
            i += 2 + end + 2;
            continue;
        }
        if (c == b'-' && bytes.get(i + 1) == Some(&b'-')) || c == b'#' {
            i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n + 1);
            continue;
        }
        let start = i;
        match c {
            b'`' | b'"' | b'\'' => {
                let mut text = String::new();
                let mut j = i + 1;
                loop {
                    let Some(&b) = bytes.get(j) else {
                        return Err(TranslateError::Parse(format!(
                            "unterminated quote starting at byte {i}"
                        )));
                    };
                    if b == c {
                        if bytes.get(j + 1) == Some(&c) {
                            text.push(c as char);
                            j += 2;
                            continue;
                        }
                        j += 1;
                        break;
                    }
                    if b == b'\\' && c != b'`' && j + 1 < bytes.len() {
                        // MySQL backslash escape inside a string.
                        let next = sql[j + 1..].chars().next().unwrap_or('\\');
                        text.push(next);
                        j += 1 + next.len_utf8();
                        continue;
                    }
                    let ch = sql[j..].chars().next().unwrap_or(' ');
                    text.push(ch);
                    j += ch.len_utf8();
                }
                let tok = if c == b'\'' {
                    Tok::Str(text)
                } else {
                    Tok::Quoted(text)
                };
                out.push(Lexed { tok, start });
                i = j;
            }
            b'.' => {
                out.push(Lexed {
                    tok: Tok::Dot,
                    start,
                });
                i += 1;
            }
            b';' => {
                out.push(Lexed {
                    tok: Tok::Semi,
                    start,
                });
                i += 1;
            }
            _ if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 => {
                let mut j = i;
                while j < bytes.len()
                    && (bytes[j].is_ascii_alphanumeric()
                        || bytes[j] == b'_'
                        || bytes[j] == b'$'
                        || bytes[j] >= 0x80)
                {
                    j += 1;
                }
                out.push(Lexed {
                    tok: Tok::Word(sql[i..j].to_string()),
                    start,
                });
                i = j;
            }
            _ => {
                let ch = sql[i..].chars().next().unwrap_or(' ');
                out.push(Lexed {
                    tok: Tok::Other(ch),
                    start,
                });
                i += ch.len_utf8();
            }
        }
    }
    Ok(out)
}

/// Cursor over the lexed `SHOW` prefix.
struct Cursor<'a> {
    sql: &'a str,
    toks: Vec<Lexed>,
    pos: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|l| &l.tok)
    }

    /// Consume the next token if it is the keyword `kw`.
    fn keyword(&mut self, kw: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// The upper-cased next word, without consuming it.
    fn peek_word(&self) -> Option<String> {
        match self.peek() {
            Some(Tok::Word(w)) => Some(w.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// One identifier (bare or quoted).
    fn ident(&mut self) -> Result<String, TranslateError> {
        match self.peek().cloned() {
            Some(Tok::Word(w) | Tok::Quoted(w)) => {
                self.pos += 1;
                Ok(w)
            }
            other => Err(TranslateError::Parse(format!(
                "expected an identifier, found {other:?}"
            ))),
        }
    }

    /// `name` or `db.name`; returns `(db, name)`.
    fn qualified(&mut self) -> Result<(Option<String>, String), TranslateError> {
        let first = self.ident()?;
        if matches!(self.peek(), Some(Tok::Dot)) {
            self.pos += 1;
            let second = self.ident()?;
            Ok((Some(first), second))
        } else {
            Ok((None, first))
        }
    }

    /// Optional `{FROM|IN} db`.
    fn optional_db(&mut self) -> Result<Option<String>, TranslateError> {
        if self.keyword("FROM") || self.keyword("IN") {
            Ok(Some(self.ident()?))
        } else {
            Ok(None)
        }
    }

    /// The raw statement text from the current token to the end.
    fn rest(&self) -> &str {
        self.toks
            .get(self.pos)
            .map_or("", |l| self.sql[l.start..].trim())
    }

    /// Succeeds only at end of input (an optional trailing `;` allowed).
    fn end(&mut self) -> Result<(), TranslateError> {
        if matches!(self.peek(), Some(Tok::Semi)) {
            self.pos += 1;
        }
        match self.peek() {
            None => Ok(()),
            Some(t) => Err(TranslateError::Unsupported(format!(
                "unexpected {t:?} in metadata statement: {}",
                self.rest()
            ))),
        }
    }
}

/// A `SHOW` filter clause, as written.
enum Filter {
    /// No filter.
    None,
    /// `LIKE <pattern text>`.
    Like(String),
    /// `WHERE <expression text>`.
    Where(String),
}

/// Read an optional `LIKE` / `WHERE` tail (or the end of the statement).
fn filter_clause(cur: &mut Cursor<'_>, allow_like: bool) -> Result<Filter, TranslateError> {
    if allow_like && cur.keyword("LIKE") {
        let rest = cur.rest().to_string();
        if rest.is_empty() {
            return Err(TranslateError::Parse("LIKE without a pattern".into()));
        }
        return Ok(Filter::Like(rest));
    }
    if cur.keyword("WHERE") {
        let rest = cur.rest().to_string();
        if rest.is_empty() {
            return Err(TranslateError::Parse("WHERE without a condition".into()));
        }
        return Ok(Filter::Where(rest));
    }
    cur.end()?;
    Ok(Filter::None)
}

/// Reject a database qualifier naming a system schema: litewire has only
/// the user's database, and answering with its tables would be wrong.
fn check_db(db: Option<&str>) -> Result<(), TranslateError> {
    match db {
        Some(d) if catalog::is_system_schema(d) => Err(TranslateError::Unsupported(format!(
            "litewire serves a single user database; schema `{d}` is not available"
        ))),
        _ => Ok(()),
    }
}

/// Translate a `SHOW` filter to a SQLite predicate over the emulated rows.
///
/// The predicate goes through the regular MySQL translator (as
/// `SELECT * FROM <placeholder> WHERE <expr>`), so `LIKE` gets MySQL's
/// implicit `\` escape and functions are mapped exactly as in any other
/// query. `LIKE 'pat'` becomes `<like_column> LIKE 'pat'`. Identifiers
/// spelled `Tables_in_<anything>` (`SHOW TABLES` names its column after the
/// database) are renamed to the emulated column.
fn translate_filter(filter: Filter, like_column: &str) -> Result<Option<String>, TranslateError> {
    let expr = match filter {
        Filter::None => return Ok(None),
        Filter::Like(pattern) => format!("`{like_column}` LIKE {pattern}"),
        Filter::Where(expr) => expr,
    };
    let wrapper = format!("SELECT * FROM {SHOW_ALIAS} WHERE {expr}");
    let mut stmts = Parser::parse_sql(&MySqlDialect {}, &wrapper)
        .map_err(|e| TranslateError::Parse(format!("in SHOW filter: {e}")))?;
    let [stmt] = stmts.as_mut_slice() else {
        return Err(TranslateError::Unsupported(
            "a SHOW filter must be a single condition".into(),
        ));
    };
    // Only a bare `WHERE <expr>` may follow: no ORDER BY / LIMIT / GROUP BY,
    // which MySQL rejects for SHOW and which would not compose here.
    let selection = match stmt {
        Statement::Query(q)
            if q.order_by.is_none()
                && q.limit_clause.is_none()
                && q.with.is_none()
                && q.fetch.is_none()
                && q.locks.is_empty() =>
        {
            match q.body.as_mut() {
                SetExpr::Select(s)
                    if s.having.is_none()
                        && matches!(&s.group_by, sqlparser::ast::GroupByExpr::Expressions(g, _) if g.is_empty()) =>
                {
                    s.selection.as_mut()
                }
                _ => None,
            }
        }
        _ => None,
    };
    let Some(selection) = selection else {
        return Err(TranslateError::Unsupported(format!(
            "unsupported SHOW filter: {expr}"
        )));
    };
    rename_tables_in(selection);
    let stmt = crate::rewrite_statement(stmt.clone(), Dialect::MySQL)?;
    let emitted = crate::emit::emit_statement(&stmt);
    let prefix = format!("SELECT * FROM {SHOW_ALIAS} WHERE ");
    let predicate = emitted
        .strip_prefix(&prefix)
        .ok_or_else(|| TranslateError::Unsupported(format!("unsupported SHOW filter: {expr}")))?;
    Ok(Some(predicate.to_string()))
}

/// Rename `Tables_in_<db>` identifiers to the emulated `SHOW TABLES` column.
fn rename_tables_in(expr: &mut Expr) {
    use std::ops::ControlFlow;

    use sqlparser::ast::visit_expressions_mut;

    let _: ControlFlow<()> = visit_expressions_mut(expr, |e| {
        if let Expr::Identifier(id) = e
            && id.value.len() > "tables_in_".len()
            && id.value[.."tables_in_".len()].eq_ignore_ascii_case("tables_in_")
        {
            *e = Expr::Identifier(Ident::with_quote('`', "Tables_in_database"));
        }
        ControlFlow::Continue(())
    });
}

/// Unsupported `SHOW` form.
fn unsupported_show(what: &str) -> TranslateError {
    TranslateError::Unsupported(format!(
        "SHOW {what} is not supported by litewire (supported: SHOW [FULL] TABLES, \
         SHOW [FULL] COLUMNS|FIELDS, SHOW INDEX|INDEXES|KEYS, SHOW CREATE TABLE, \
         SHOW DATABASES, DESCRIBE)"
    ))
}

/// Detect and parse `SHOW ...` / `DESCRIBE ...` / `DESC ...`.
///
/// `Ok(None)` when `sql` is not one of those statements.
fn detect_show(sql: &str) -> Result<Option<MetadataQuery>, TranslateError> {
    // Cheap first-word test before lexing.
    let first = sql
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if !matches!(first.as_str(), "SHOW" | "DESCRIBE" | "DESC") {
        return Ok(None);
    }
    let mut cur = Cursor {
        sql,
        toks: lex(sql)?,
        pos: 0,
    };

    if cur.keyword("DESCRIBE") || cur.keyword("DESC") {
        return describe(&mut cur).map(Some);
    }
    if !cur.keyword("SHOW") {
        return Ok(None);
    }

    // `EXTENDED` (hidden columns/indexes) has nothing extra to show here.
    let mut full = false;
    loop {
        if cur.keyword("FULL") {
            full = true;
        } else if !cur.keyword("EXTENDED") {
            break;
        }
    }

    let Some(kw) = cur.peek_word() else {
        return Err(unsupported_show(cur.rest()));
    };
    cur.pos += 1;
    let query = match kw.as_str() {
        "TABLES" => {
            check_db(cur.optional_db()?.as_deref())?;
            let filter = translate_filter(filter_clause(&mut cur, true)?, "Tables_in_database")?;
            MetadataQuery::ShowTables { full, filter }
        }
        "DATABASES" | "SCHEMAS" => {
            // litewire serves one database under whatever name the client
            // connected with, which the translator cannot see: a filtered
            // answer would be guesswork, so it is an error instead.
            match filter_clause(&mut cur, true)? {
                Filter::None => MetadataQuery::ShowDatabases,
                _ => {
                    return Err(TranslateError::Unsupported(
                        "SHOW DATABASES with LIKE/WHERE is not supported: litewire serves a \
                         single database"
                            .into(),
                    ));
                }
            }
        }
        "COLUMNS" | "FIELDS" => {
            if !(cur.keyword("FROM") || cur.keyword("IN")) {
                return Err(TranslateError::Parse(format!(
                    "expected FROM after SHOW {kw}"
                )));
            }
            let (db, table) = cur.qualified()?;
            check_db(db.as_deref())?;
            check_db(cur.optional_db()?.as_deref())?;
            let filter = translate_filter(filter_clause(&mut cur, true)?, "Field")?;
            MetadataQuery::ShowColumns {
                table,
                full,
                filter,
            }
        }
        "INDEX" | "INDEXES" | "KEYS" => {
            if !(cur.keyword("FROM") || cur.keyword("IN")) {
                return Err(TranslateError::Parse(format!(
                    "expected FROM after SHOW {kw}"
                )));
            }
            let (db, table) = cur.qualified()?;
            check_db(db.as_deref())?;
            check_db(cur.optional_db()?.as_deref())?;
            // MySQL's SHOW INDEX takes WHERE but not LIKE.
            let filter = translate_filter(filter_clause(&mut cur, false)?, "Key_name")?;
            MetadataQuery::ShowIndex { table, filter }
        }
        "CREATE" if cur.keyword("TABLE") => {
            let (db, table) = cur.qualified()?;
            check_db(db.as_deref())?;
            cur.end()?;
            MetadataQuery::ShowCreateTable { table }
        }
        _ => return Err(unsupported_show(&format!("{kw} ..."))),
    };
    Ok(Some(query))
}

/// `DESCRIBE <table> [<column> | '<pattern>']` — MySQL's shorthand for
/// `SHOW COLUMNS FROM <table> [LIKE '<pattern>']`. `DESCRIBE <statement>`
/// (MySQL's `EXPLAIN` synonym) is not supported.
fn describe(cur: &mut Cursor<'_>) -> Result<MetadataQuery, TranslateError> {
    if let Some(w) = cur.peek_word()
        && matches!(
            w.as_str(),
            "SELECT"
                | "INSERT"
                | "UPDATE"
                | "DELETE"
                | "REPLACE"
                | "WITH"
                | "TABLE"
                | "FORMAT"
                | "ANALYZE"
                | "EXTENDED"
                | "PARTITIONS"
                | "FOR"
        )
    {
        return Err(TranslateError::Unsupported(format!(
            "DESCRIBE {w} (EXPLAIN) is not supported by litewire"
        )));
    }
    let (db, table) = cur.qualified()?;
    check_db(db.as_deref())?;
    let pattern = match cur.peek().cloned() {
        Some(Tok::Word(c) | Tok::Quoted(c) | Tok::Str(c)) => {
            cur.pos += 1;
            Some(c)
        }
        _ => None,
    };
    cur.end()?;
    let filter = pattern
        .map(|p| translate_filter(Filter::Like(sql_str(&p)), "Field"))
        .transpose()?
        .flatten();
    Ok(MetadataQuery::ShowColumns {
        table,
        full: false,
        filter,
    })
}

/// Sanitize an identifier (table or schema name) by stripping any character
/// that isn't alphanumeric, underscore, or period. This prevents SQL injection
/// when identifiers are interpolated into generated SQL via `format!()`.
fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
        .collect()
}

/// Extract a simple `column = 'value'` filter from a WHERE clause, preserving
/// the value's original case. Used only by the PostgreSQL/T-SQL catalog
/// shortcuts.
fn extract_where_value_original(original_sql: &str, column: &str) -> Option<String> {
    let upper = original_sql.to_ascii_uppercase();
    let pattern = format!("{column} = ");
    if let Some(pos) = upper.find(&pattern) {
        let after = &original_sql[pos + pattern.len()..];
        let after = after.trim_start();
        if after.starts_with('\'') || after.starts_with('"') {
            let quote = after.as_bytes()[0] as char;
            let rest = &after[1..];
            if let Some(end) = rest.find(quote) {
                return Some(sanitize_identifier(&rest[..end]));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dialect;

    fn detect(sql: &str) -> Option<MetadataQuery> {
        detect_metadata_query(sql, Dialect::MySQL).unwrap()
    }

    fn detect_err(sql: &str) -> TranslateError {
        match detect_metadata_query(sql, Dialect::MySQL) {
            Err(e) => e,
            Ok(q) => panic!("expected an error for {sql:?}, got {q:?}"),
        }
    }

    /// The generated SQL is exactly one SQLite statement (no injected
    /// statement or comment can survive the quoting).
    fn assert_single_statement(sql: &str) {
        let stmts = Parser::parse_sql(&sqlparser::dialect::SQLiteDialect {}, sql)
            .unwrap_or_else(|e| panic!("generated SQL does not parse ({e}): {sql}"));
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
    }

    // ── SHOW TABLES ─────────────────────────────────────────────────────────

    #[test]
    fn show_tables_plain() {
        for sql in ["SHOW TABLES", "show tables", "SHOW TABLES ", "SHOW TABLES;"] {
            assert!(
                matches!(
                    detect(sql),
                    Some(MetadataQuery::ShowTables {
                        full: false,
                        filter: None
                    })
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn show_full_tables_and_from_db() {
        assert!(matches!(
            detect("SHOW FULL TABLES FROM `sendy`"),
            Some(MetadataQuery::ShowTables {
                full: true,
                filter: None
            })
        ));
        assert!(matches!(
            detect("SHOW TABLES IN sendy"),
            Some(MetadataQuery::ShowTables { filter: None, .. })
        ));
    }

    #[test]
    fn show_tables_like_is_filtered() {
        let Some(MetadataQuery::ShowTables { filter, .. }) =
            detect("SHOW TABLES LIKE 'skipped_emails'")
        else {
            panic!()
        };
        let f = filter.expect("LIKE must produce a filter");
        assert!(f.contains("LIKE 'skipped_emails'"), "{f}");
        assert!(f.contains("ESCAPE"), "MySQL implicit LIKE escape: {f}");
    }

    #[test]
    fn show_tables_where_renames_tables_in_column() {
        let Some(MetadataQuery::ShowTables { filter, .. }) =
            detect("SHOW FULL TABLES WHERE Tables_in_sendy = 'apps' AND Table_type = 'BASE TABLE'")
        else {
            panic!()
        };
        let f = filter.unwrap();
        assert!(f.contains("`Tables_in_database` = 'apps'"), "{f}");
        assert!(f.contains("Table_type = 'BASE TABLE'"), "{f}");
    }

    #[test]
    fn show_tables_from_system_schema_errors() {
        detect_err("SHOW TABLES FROM information_schema");
        detect_err("SHOW TABLES FROM mysql");
    }

    #[test]
    fn show_tables_sql_shape() {
        let sql = MetadataQuery::ShowTables {
            full: false,
            filter: None,
        }
        .to_sqlite_sql();
        assert!(sql.contains("sqlite_master"), "{sql}");
        assert!(sql.contains("\"Tables_in_database\""), "{sql}");
        assert!(!sql.contains("WHERE ("), "{sql}");
        assert_single_statement(&sql);
    }

    // ── SHOW DATABASES ──────────────────────────────────────────────────────

    #[test]
    fn show_databases() {
        assert!(matches!(
            detect("SHOW DATABASES"),
            Some(MetadataQuery::ShowDatabases)
        ));
        assert!(matches!(
            detect("SHOW SCHEMAS"),
            Some(MetadataQuery::ShowDatabases)
        ));
        assert!(
            MetadataQuery::ShowDatabases
                .to_sqlite_sql()
                .contains("'main'")
        );
    }

    #[test]
    fn show_databases_filtered_errors() {
        detect_err("SHOW DATABASES LIKE 'sendy'");
        detect_err("SHOW DATABASES WHERE `Database` = 'x'");
    }

    // ── SHOW COLUMNS / DESCRIBE ─────────────────────────────────────────────

    #[test]
    fn show_columns_forms() {
        for sql in [
            "SHOW COLUMNS FROM users",
            "SHOW FIELDS FROM users",
            "SHOW COLUMNS IN users",
            "SHOW COLUMNS FROM `users`",
            "SHOW COLUMNS FROM sendy.users",
            "SHOW COLUMNS FROM `sendy`.`users`",
            "SHOW COLUMNS FROM users FROM sendy",
            "SHOW EXTENDED COLUMNS FROM users",
            "DESCRIBE users",
            "DESC users",
            "describe `users`;",
        ] {
            assert!(
                matches!(detect(sql), Some(MetadataQuery::ShowColumns { ref table, filter: None, .. }) if table == "users"),
                "{sql} -> {:?}",
                detect(sql)
            );
        }
    }

    #[test]
    fn show_full_columns_detected() {
        for q in [
            "SHOW FULL COLUMNS FROM wp_options",
            "SHOW FULL COLUMNS FROM `wp_options`",
            "show full fields from wp_options",
        ] {
            assert!(
                matches!(detect(q), Some(MetadataQuery::ShowColumns { ref table, full: true, .. }) if table == "wp_options"),
                "not detected: {q}"
            );
        }
    }

    /// The bug this module's rewrite fixes: the WHERE clause used to be
    /// swallowed, so a "does column x exist?" probe always found rows.
    #[test]
    fn show_columns_where_is_kept() {
        let Some(MetadataQuery::ShowColumns { table, filter, .. }) =
            detect("SHOW COLUMNS FROM login WHERE Field = 'db_updated'")
        else {
            panic!()
        };
        assert_eq!(table, "login");
        let f = filter.expect("WHERE must produce a filter");
        assert!(f.contains("Field = 'db_updated'"), "{f}");
    }

    #[test]
    fn show_columns_like_is_kept() {
        let Some(MetadataQuery::ShowColumns { filter, .. }) =
            detect("SHOW COLUMNS FROM skipped_emails LIKE 'list'")
        else {
            panic!()
        };
        assert!(filter.unwrap().contains("`Field` LIKE 'list'"));
    }

    #[test]
    fn describe_with_column_is_a_like_filter() {
        for sql in [
            "DESCRIBE users email",
            "DESC users 'em%'",
            "DESCRIBE users `email`",
        ] {
            let Some(MetadataQuery::ShowColumns { filter, .. }) = detect(sql) else {
                panic!("{sql}")
            };
            assert!(filter.unwrap().contains("`Field` LIKE"), "{sql}");
        }
    }

    #[test]
    fn describe_statement_errors() {
        detect_err("DESCRIBE SELECT * FROM t");
        detect_err("DESC FORMAT=JSON SELECT 1");
    }

    #[test]
    fn show_columns_sql_shape() {
        let full = MetadataQuery::ShowColumns {
            table: "users".into(),
            full: true,
            filter: None,
        }
        .to_sqlite_sql();
        assert!(full.contains("pragma_table_info"), "{full}");
        assert!(full.contains("'users'"), "{full}");
        // MySQL SHOW FULL COLUMNS shape: wpdb::get_table_charset needs
        // Field/Type/Collation; text affinity must not map to a capped type.
        for col in [
            "\"Field\"",
            "\"Type\"",
            "\"Null\"",
            "\"Key\"",
            "\"Collation\"",
        ] {
            assert!(full.contains(col), "missing column {col}: {full}");
        }
        assert!(full.contains("longtext"), "{full}");
        assert!(full.contains("utf8mb4_unicode_ci"), "{full}");
        assert_single_statement(&full);

        let plain = MetadataQuery::ShowColumns {
            table: "users".into(),
            full: false,
            filter: Some("Field = 'x'".into()),
        }
        .to_sqlite_sql();
        assert!(
            plain.starts_with("SELECT \"Field\", \"Type\", \"Null\","),
            "{plain}"
        );
        assert!(plain.contains("WHERE (Field = 'x')"), "{plain}");
        assert_single_statement(&plain);
    }

    // ── SHOW INDEX ──────────────────────────────────────────────────────────

    #[test]
    fn show_index_aliases() {
        for sql in [
            "SHOW INDEX FROM users",
            "SHOW INDEXES FROM users",
            "SHOW KEYS FROM users",
            "SHOW INDEX IN users",
            "SHOW EXTENDED INDEX FROM users",
            "SHOW INDEX FROM users FROM sendy",
            "SHOW INDEX FROM sendy.users",
        ] {
            assert!(
                matches!(detect(sql), Some(MetadataQuery::ShowIndex { ref table, filter: None }) if table == "users"),
                "{sql}"
            );
        }
    }

    /// Item that motivated the rewrite: `SHOW INDEX FROM t WHERE KEY_NAME =
    /// 'x'` used to drop its WHERE, and every table has a PRIMARY row.
    #[test]
    fn show_index_where_is_kept() {
        for (sql, needle) in [
            (
                "SHOW INDEX FROM subscribers WHERE KEY_NAME = 's_list'",
                "KEY_NAME = 's_list'",
            ),
            (
                "SHOW INDEX FROM subscribers WHERE Key_name = \"s_list\"",
                "Key_name = 's_list'",
            ),
            (
                "SHOW KEYS FROM t WHERE Key_name IN ('a', 'b') AND Non_unique = 0",
                "Key_name IN ('a', 'b') AND Non_unique = 0",
            ),
            (
                "SHOW INDEX FROM t WHERE Column_name LIKE 'em%' OR NOT (Non_unique = 1)",
                "Column_name LIKE 'em%'",
            ),
        ] {
            let Some(MetadataQuery::ShowIndex { filter, .. }) = detect(sql) else {
                panic!("{sql}")
            };
            let f = filter.unwrap_or_else(|| panic!("filter dropped: {sql}"));
            assert!(f.contains(needle), "{sql} -> {f}");
        }
    }

    #[test]
    fn show_index_like_errors() {
        // MySQL has no SHOW INDEX ... LIKE; it must not be silently ignored.
        detect_err("SHOW INDEX FROM t LIKE 'x'");
    }

    #[test]
    fn show_index_sql_shape() {
        let sql = MetadataQuery::ShowIndex {
            table: "users".into(),
            filter: Some("KEY_NAME = 'x'".into()),
        }
        .to_sqlite_sql();
        for col in [
            "\"Key_name\"",
            "\"Column_name\"",
            "\"Non_unique\"",
            "\"Seq_in_index\"",
        ] {
            assert!(sql.contains(col), "missing {col}: {sql}");
        }
        assert!(sql.contains("pragma_index_list"), "{sql}");
        assert!(sql.contains("'PRIMARY'"), "{sql}");
        assert!(sql.contains("WHERE (KEY_NAME = 'x')"), "{sql}");
        assert_single_statement(&sql);
    }

    // ── Errors instead of unfiltered results ────────────────────────────────

    #[test]
    fn trailing_garbage_errors() {
        detect_err("SHOW INDEX FROM t junk");
        detect_err("SHOW TABLES junk");
        detect_err("SHOW CREATE TABLE t WHERE 1");
        detect_err("DESCRIBE t c extra");
        detect_err("SHOW COLUMNS FROM t WHERE");
        detect_err("SHOW COLUMNS FROM t LIKE");
    }

    #[test]
    fn unsupported_filters_error() {
        detect_err("SHOW COLUMNS FROM t WHERE Field = 'x' ORDER BY Field");
        detect_err("SHOW COLUMNS FROM t WHERE Field = 'x' LIMIT 1");
        detect_err("SHOW COLUMNS FROM t WHERE Field = 'x'; DROP TABLE t");
        detect_err("SHOW COLUMNS FROM t WHERE Field = = 'x'");
    }

    #[test]
    fn unsupported_show_forms_error() {
        for sql in [
            "SHOW VARIABLES LIKE 'sql_mode'",
            "SHOW TABLE STATUS",
            "SHOW WARNINGS",
            "SHOW ENGINES",
            "SHOW GRANTS",
            "SHOW",
        ] {
            detect_err(sql);
        }
    }

    #[test]
    fn non_metadata_statements_pass_through() {
        for sql in [
            "SELECT * FROM users",
            "INSERT INTO users VALUES (1)",
            "SELECT email FROM users",
            "SHOWCASE",
        ] {
            assert!(detect(sql).is_none(), "{sql}");
        }
    }

    // ── SHOW CREATE TABLE ───────────────────────────────────────────────────

    #[test]
    fn show_create_table() {
        assert!(
            matches!(detect("SHOW CREATE TABLE `posts`"), Some(MetadataQuery::ShowCreateTable { ref table }) if table == "posts")
        );
        let sql = MetadataQuery::ShowCreateTable {
            table: "posts".into(),
        }
        .to_sqlite_sql();
        assert!(sql.contains("sqlite_master"), "{sql}");
        assert!(sql.contains("\"Table\""), "{sql}");
        assert!(sql.contains("\"Create Table\""), "{sql}");
    }

    // ── Identifiers are quoted, never spliced ───────────────────────────────

    #[test]
    fn quoted_identifiers_keep_their_characters() {
        let Some(MetadataQuery::ShowColumns { table, .. }) = detect("SHOW COLUMNS FROM `my-table`")
        else {
            panic!()
        };
        assert_eq!(table, "my-table", "must not be rewritten to another table");
        let Some(MetadataQuery::ShowColumns { table, .. }) = detect("SHOW COLUMNS FROM `a``b`")
        else {
            panic!()
        };
        assert_eq!(table, "a`b");
    }

    #[test]
    fn injection_through_bare_name_is_rejected() {
        detect_err("DESCRIBE users'; DROP TABLE x; --");
        detect_err("SHOW INDEX FROM users'); DROP TABLE x; --");
        detect_err("SHOW CREATE TABLE users'; DROP TABLE sqlite_master; --");
    }

    #[test]
    fn injection_through_quoted_name_stays_one_statement() {
        for sql in [
            "SHOW FULL COLUMNS FROM `users'); DROP TABLE x; --`",
            "SHOW INDEX FROM `users'); DROP TABLE x; --`",
            "SHOW CREATE TABLE `users'; DROP TABLE x; --`",
            "DESCRIBE `users'` 'a''b'",
        ] {
            let sql = detect(sql).unwrap().to_sqlite_sql();
            assert_single_statement(&sql);
        }
    }

    // ── System variable detection (SELECT @@...) ────────────────────────────

    fn vars(sql: &str) -> Vec<SystemVariable> {
        match detect(sql) {
            Some(MetadataQuery::SystemVariables { variables }) => variables,
            other => panic!("expected SystemVariables for {sql}, got: {other:?}"),
        }
    }

    fn sv(name: &str) -> SystemVariable {
        SystemVariable {
            name: name.into(),
            global: false,
            label: format!("@@{name}"),
        }
    }

    #[test]
    fn detect_single_system_variable() {
        assert_eq!(vars("SELECT @@version"), [sv("version")]);
    }

    #[test]
    fn detect_multiple_system_variables() {
        let v = vars("SELECT @@version, @@max_allowed_packet, @@wait_timeout");
        assert_eq!(
            v,
            [sv("version"), sv("max_allowed_packet"), sv("wait_timeout")]
        );
    }

    #[test]
    fn scoped_variables_keep_their_written_label() {
        let v = vars("SELECT @@global.max_connections, @@SESSION.sql_mode, @@local.wait_timeout");
        assert_eq!(v[0].name, "max_connections");
        assert!(v[0].global);
        assert_eq!(v[0].label, "@@global.max_connections");
        assert_eq!(v[1].name, "sql_mode");
        assert!(!v[1].global);
        assert_eq!(v[1].label, "@@SESSION.sql_mode");
        assert_eq!(v[2].name, "wait_timeout");
        assert_eq!(v[2].label, "@@local.wait_timeout");
    }

    #[test]
    fn aliases_and_limit() {
        let v = vars(
            "SELECT @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client cs",
        );
        assert_eq!(v[0].label, "auto_increment_increment");
        assert_eq!(v[1].label, "cs");
        // The mysql CLI's connect-time probe.
        assert_eq!(vars("select @@version_comment limit 1").len(), 1);
        assert_eq!(vars("SELECT @@sql_mode;"), [sv("sql_mode")]);
    }

    #[test]
    fn mixed_select_lists_are_not_system_variable_queries() {
        for sql in [
            "SELECT @@version FROM dual",
            "SELECT @@version, NOW()",
            "SELECT @@version + 1",
            "SELECT @@foo.bar",
            "SELECT @@version LIMIT x",
        ] {
            assert!(
                !matches!(detect(sql), Some(MetadataQuery::SystemVariables { .. })),
                "{sql}"
            );
        }
    }

    #[test]
    fn system_variables_sql() {
        let sql = MetadataQuery::SystemVariables {
            variables: vec![sv("version"), sv("max_allowed_packet")],
        }
        .to_sqlite_sql();
        assert!(sql.contains("\"@@version\""), "got: {sql}");
        assert!(sql.contains("\"@@max_allowed_packet\""), "got: {sql}");
        // Deliberate behaviour change (issue #21): was `8.0.0-litewire`,
        // which disagreed with the version the wire handshake advertises.
        assert!(sql.contains(crate::SERVER_VERSION), "got: {sql}");
        assert!(sql.contains("8.0.36-litewire"), "got: {sql}");
        assert!(sql.contains("67108864"), "got: {sql}");
    }

    #[test]
    fn sql_mode_comes_from_the_session() {
        let q = detect("SELECT @@sql_mode, @@SESSION.sql_mode, @@GLOBAL.sql_mode").unwrap();
        let default = q.to_sqlite_sql();
        assert_eq!(
            default,
            "SELECT '' AS \"@@sql_mode\", '' AS \"@@SESSION.sql_mode\", '' AS \"@@GLOBAL.sql_mode\""
        );
        let set = q.to_sqlite_sql_with(&SessionVars {
            sql_mode: Some("STRICT_TRANS_TABLES,NO_ZERO_DATE"),
        });
        assert_eq!(
            set,
            "SELECT 'STRICT_TRANS_TABLES,NO_ZERO_DATE' AS \"@@sql_mode\", \
             'STRICT_TRANS_TABLES,NO_ZERO_DATE' AS \"@@SESSION.sql_mode\", '' AS \"@@GLOBAL.sql_mode\""
        );
    }

    #[test]
    fn set_sql_mode_forms() {
        let lit = |s: &str| SqlModeValue::Literal(s.into());
        for (sql, global, value) in [
            ("SET sql_mode = ''", false, lit("")),
            ("SET SESSION sql_mode = 'a, b'", false, lit("A,B")),
            ("set @@session.sql_mode='x';", false, lit("X")),
            ("SET @@sql_mode := 'x'", false, lit("X")),
            ("SET LOCAL sql_mode = 'x'", false, lit("X")),
            ("SET GLOBAL sql_mode = 'x'", true, lit("X")),
            ("SET @@GLOBAL.sql_mode = 'x'", true, lit("X")),
            ("SET PERSIST sql_mode = 'x'", true, lit("X")),
            ("SET sql_mode = DEFAULT", false, SqlModeValue::Default),
            (
                "SET sql_mode = (SELECT REPLACE(@@sql_mode, 'A', ''))",
                false,
                SqlModeValue::Expression("(SELECT REPLACE(@@sql_mode, 'A', ''))".into()),
            ),
            (
                "SET sql_mode = ?",
                false,
                SqlModeValue::Expression("?".into()),
            ),
        ] {
            assert_eq!(
                detect_set_sql_mode(sql),
                Some(SetSqlMode { global, value }),
                "{sql}"
            );
        }
    }

    #[test]
    fn set_sql_mode_ignores_other_statements() {
        for sql in [
            "SET NAMES utf8mb4",
            "SET sql_mode = 'a', NAMES utf8mb4",
            "SET @sql_mode = 'x'",
            "SET sql_mode",
            "SELECT @@sql_mode",
            "SET sql_mode_x = 'a'",
        ] {
            assert_eq!(detect_set_sql_mode(sql), None, "{sql}");
        }
    }

    #[test]
    fn sql_mode_expression_substitutes_the_current_value() {
        let set = detect_set_sql_mode(
            "SET sql_mode = REPLACE(@@SESSION.sql_mode, 'X', @@GLOBAL.sql_mode)",
        )
        .unwrap();
        assert_eq!(
            set.evaluation_sql("X,Y").unwrap(),
            "SELECT REPLACE('X,Y', 'X', '')"
        );
        assert_eq!(count_placeholders("CONCAT(?, '?', ?)"), 2);
    }

    #[test]
    fn unknown_system_variable_returns_null() {
        let sql = MetadataQuery::SystemVariables {
            variables: vec![sv("nonexistent_var")],
        }
        .to_sqlite_sql();
        assert!(sql.contains("NULL"), "got: {sql}");
    }

    #[test]
    fn known_system_variables_produce_expected_values() {
        let cases = vec![
            // Deliberate behaviour change (issue #21): was `8.0.0-litewire`.
            ("version", "8.0.36-litewire"),
            ("max_allowed_packet", "67108864"),
            ("autocommit", "1"),
            ("character_set_client", "utf8mb4"),
            ("transaction_isolation", "SERIALIZABLE"),
            ("tx_isolation", "SERIALIZABLE"),
            ("identity", "last_insert_rowid()"),
            ("rowcount", "changes()"),
        ];
        for (var, expected_fragment) in cases {
            let sql = MetadataQuery::SystemVariables {
                variables: vec![sv(var)],
            }
            .to_sqlite_sql();
            assert!(
                sql.contains(expected_fragment),
                "@@{var}: expected {expected_fragment} in: {sql}"
            );
        }
    }

    #[test]
    fn detect_select_at_identity_and_rowcount() {
        for sql in ["SELECT @@IDENTITY", "SELECT @@ROWCOUNT"] {
            let q = detect_metadata_query(sql, Dialect::TDS).unwrap();
            assert!(
                matches!(q, Some(MetadataQuery::SystemVariables { .. })),
                "got: {q:?}"
            );
        }
    }

    // ── information_schema is not handled here ──────────────────────────────

    #[test]
    fn information_schema_goes_through_the_parser() {
        // Substituted inside the client's query by `crate::catalog`.
        assert!(detect("SELECT * FROM information_schema.tables").is_none());
        assert!(
            detect("SELECT * FROM information_schema.columns WHERE TABLE_NAME = 'users'").is_none()
        );
    }

    // ── pg_catalog / sys.* / sp_* ───────────────────────────────────────────

    #[test]
    fn detect_pg_catalog() {
        for sql in [
            "SELECT * FROM pg_catalog.pg_tables",
            "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'",
        ] {
            let q = detect_metadata_query(sql, Dialect::PostgreSQL).unwrap();
            assert!(
                matches!(q, Some(MetadataQuery::PgCatalogTables)),
                "got: {q:?}"
            );
        }
        let sql = MetadataQuery::PgCatalogTables.to_sqlite_sql();
        assert!(sql.contains("tablename"), "got: {sql}");
        let sql = MetadataQuery::PgCatalogColumns {
            table: "users".into(),
        }
        .to_sqlite_sql();
        assert!(sql.contains("pragma_table_info"), "got: {sql}");
    }

    #[test]
    fn detect_sys_tables_and_columns() {
        let q = detect_metadata_query("SELECT * FROM sys.tables", Dialect::TDS).unwrap();
        assert!(matches!(q, Some(MetadataQuery::SysTables)), "got: {q:?}");
        let q = detect_metadata_query(
            "SELECT * FROM sys.columns WHERE TABLE_NAME = 'users'",
            Dialect::TDS,
        )
        .unwrap();
        assert!(
            matches!(q, Some(MetadataQuery::SysColumns { ref table }) if table == "users"),
            "got: {q:?}"
        );
        let q = detect_metadata_query("EXEC sp_tables", Dialect::TDS).unwrap();
        assert!(matches!(q, Some(MetadataQuery::SysTables)), "got: {q:?}");
        let q = detect_metadata_query("EXEC sp_columns 'users'", Dialect::TDS).unwrap();
        assert!(
            matches!(q, Some(MetadataQuery::SysColumns { .. })),
            "got: {q:?}"
        );
    }

    /// pg_catalog column path (`pg_attribute`) interpolates a sanitized name.
    #[test]
    fn pg_catalog_columns_injection_safe() {
        let q = detect_metadata_query(
            "SELECT * FROM PG_CATALOG.PG_ATTRIBUTE \
             WHERE TABLE_NAME = 'users''; DROP TABLE x; --'",
            Dialect::PostgreSQL,
        )
        .unwrap();
        if let Some(MetadataQuery::PgCatalogColumns { table }) = &q
            && !table.is_empty()
        {
            assert_single_statement(&q.as_ref().unwrap().to_sqlite_sql());
        }
    }
}
