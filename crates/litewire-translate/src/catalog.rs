//! MySQL-shaped views of the SQLite catalog.
//!
//! One place builds the SQLite SQL that answers "which tables, columns and
//! indexes exist" in the shape MySQL reports them. Two consumers share it:
//!
//! * the `SHOW TABLES` / `SHOW COLUMNS` / `SHOW INDEX` emulation in
//!   [`crate::metadata`], which projects the rows under MySQL's `SHOW`
//!   column names (`Field`, `Key_name`, ...), and
//! * the `information_schema` substitution in this module, which replaces
//!   a reference to `information_schema.tables` (`columns`, `statistics`,
//!   `schemata`) **inside the client's own query** with a derived table
//!   carrying MySQL's `information_schema` column names. The client's
//!   projection, `WHERE`, `ORDER BY` and `LIMIT` then run unchanged against
//!   it — nothing is filtered by pattern-matching the SQL text, so no
//!   predicate can be silently dropped.
//!
//! Name-valued columns (table, column and index names) carry
//! `COLLATE NOCASE`, so `WHERE Key_name = 'S_LIST'` matches `s_list`. That
//! mirrors MySQL, whose catalog compares column and index names
//! case-insensitively, and SQLite, whose table names are case-insensitive.
//!
//! Every table/database name interpolated here goes through [`sql_str`], a
//! quoted SQLite string literal, never a bare splice.

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, Statement, TableAlias, TableFactor, Value, VisitMut,
    VisitorMut,
};

use crate::TranslateError;

/// Schemas that are *not* the user's database. Every other schema name a
/// client mentions refers to the one database litewire serves on this
/// connection (litewire has no notion of several databases).
pub(crate) const SYSTEM_SCHEMAS: [&str; 6] = [
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
    "pg_catalog",
    "sqlite_temp_master",
];

/// The schema name the emulation reports — the same value the translated
/// `DATABASE()` / `SCHEMA()` return, so `table_schema = DATABASE()` holds.
pub(crate) const SCHEMA_NAME: &str = "main";

/// Is `name` one of the [`SYSTEM_SCHEMAS`]?
pub(crate) fn is_system_schema(name: &str) -> bool {
    SYSTEM_SCHEMAS.iter().any(|s| s.eq_ignore_ascii_case(name))
}

/// Quote `s` as a SQLite string literal.
pub(crate) fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The SQLite expression mapping a declared column type to the MySQL type
/// name the catalog reports. Text affinity maps to `longtext` / `longblob`
/// so clients apply no length cap (WordPress's `wpdb` reads this for its
/// charset/length checks).
const MYSQL_TYPE: &str = "CASE \
    WHEN upper(p.type) LIKE '%INT%' THEN 'bigint' \
    WHEN upper(p.type) IN ('REAL', 'DOUBLE', 'FLOAT', 'NUMERIC', 'DECIMAL') THEN 'double' \
    WHEN upper(p.type) LIKE '%BLOB%' THEN 'longblob' \
    WHEN upper(p.type) LIKE '%CHAR%' OR upper(p.type) LIKE '%CLOB%' OR upper(p.type) LIKE '%TEXT%' THEN 'longtext' \
    WHEN p.type = '' THEN 'longtext' \
    ELSE lower(p.type) END";

/// SQLite condition: is column `p` text-typed (as [`MYSQL_TYPE`] sees it)?
const IS_TEXT: &str = "(upper(p.type) LIKE '%CHAR%' OR upper(p.type) LIKE '%CLOB%' \
    OR upper(p.type) LIKE '%TEXT%' OR p.type = '')";

/// Which tables a catalog query covers.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Tables<'a> {
    /// One named table (or view). Matched case-insensitively against the
    /// catalog, like SQLite resolves table names; an unknown name yields no
    /// rows.
    One(&'a str),
    /// Every user table and view.
    All,
}

impl Tables<'_> {
    /// A derived table `m(name, type)` listing the covered tables.
    fn source(self) -> String {
        let base = "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') \
                    AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'";
        match self {
            Tables::One(t) => format!("({base} AND name = {} COLLATE NOCASE) AS m", sql_str(t)),
            Tables::All => format!("({base}) AS m"),
        }
    }
}

/// `information_schema.TABLES`-shaped rows for every user table and view.
pub(crate) fn table_rows() -> String {
    format!(
        "SELECT 'def' AS TABLE_CATALOG, '{SCHEMA_NAME}' AS TABLE_SCHEMA, \
         m.name COLLATE NOCASE AS TABLE_NAME, \
         CASE m.type WHEN 'view' THEN 'VIEW' ELSE 'BASE TABLE' END AS TABLE_TYPE, \
         CASE m.type WHEN 'view' THEN NULL ELSE 'InnoDB' END AS ENGINE, \
         10 AS VERSION, 'Dynamic' AS ROW_FORMAT, NULL AS TABLE_ROWS, \
         0 AS AVG_ROW_LENGTH, 0 AS DATA_LENGTH, 0 AS MAX_DATA_LENGTH, \
         0 AS INDEX_LENGTH, 0 AS DATA_FREE, NULL AS AUTO_INCREMENT, \
         NULL AS CREATE_TIME, NULL AS UPDATE_TIME, NULL AS CHECK_TIME, \
         'utf8mb4_unicode_ci' AS TABLE_COLLATION, NULL AS CHECKSUM, \
         '' AS CREATE_OPTIONS, '' AS TABLE_COMMENT \
         FROM {}",
        Tables::All.source()
    )
}

/// `information_schema.COLUMNS`-shaped rows for the covered tables.
pub(crate) fn column_rows(tables: Tables<'_>) -> String {
    // UNI: the column is the whole of a single-column UNIQUE index.
    // MUL: the column leads some other index. (PRI > UNI > MUL, as MySQL.)
    let uni = "EXISTS (SELECT 1 FROM pragma_index_list(m.name) AS il \
               WHERE il.\"unique\" = 1 \
               AND (SELECT count(*) FROM pragma_index_info(il.name)) = 1 \
               AND (SELECT ii.name FROM pragma_index_info(il.name) AS ii) = p.name)";
    let mul = "EXISTS (SELECT 1 FROM pragma_index_list(m.name) AS il \
               JOIN pragma_index_info(il.name) AS ii \
               WHERE ii.seqno = 0 AND ii.name = p.name)";
    // An INTEGER PRIMARY KEY is SQLite's rowid alias: it auto-assigns.
    let auto = "p.pk = 1 AND upper(p.type) = 'INTEGER' \
                AND (SELECT count(*) FROM pragma_table_info(m.name) AS q WHERE q.pk > 0) = 1";
    format!(
        "SELECT 'def' AS TABLE_CATALOG, '{SCHEMA_NAME}' AS TABLE_SCHEMA, \
         m.name COLLATE NOCASE AS TABLE_NAME, \
         p.name COLLATE NOCASE AS COLUMN_NAME, \
         p.cid + 1 AS ORDINAL_POSITION, \
         p.dflt_value AS COLUMN_DEFAULT, \
         CASE WHEN p.\"notnull\" = 1 OR p.pk > 0 THEN 'NO' ELSE 'YES' END AS IS_NULLABLE, \
         {MYSQL_TYPE} AS DATA_TYPE, \
         CASE WHEN {IS_TEXT} THEN 4294967295 END AS CHARACTER_MAXIMUM_LENGTH, \
         CASE WHEN {IS_TEXT} THEN 4294967295 END AS CHARACTER_OCTET_LENGTH, \
         NULL AS NUMERIC_PRECISION, NULL AS NUMERIC_SCALE, NULL AS DATETIME_PRECISION, \
         CASE WHEN {IS_TEXT} THEN 'utf8mb4' END AS CHARACTER_SET_NAME, \
         CASE WHEN {IS_TEXT} THEN 'utf8mb4_unicode_ci' END AS COLLATION_NAME, \
         {MYSQL_TYPE} AS COLUMN_TYPE, \
         CASE WHEN p.pk > 0 THEN 'PRI' WHEN {uni} THEN 'UNI' WHEN {mul} THEN 'MUL' ELSE '' END \
           AS COLUMN_KEY, \
         CASE WHEN {auto} THEN 'auto_increment' ELSE '' END AS EXTRA, \
         'select,insert,update,references' AS PRIVILEGES, \
         '' AS COLUMN_COMMENT, '' AS GENERATION_EXPRESSION, NULL AS SRS_ID \
         FROM {} JOIN pragma_table_info(m.name) AS p",
        tables.source()
    )
}

/// `information_schema.STATISTICS`-shaped rows (one per index column) for
/// the covered tables, plus a hidden `__ord` sort key that lists `PRIMARY`
/// first and the remaining indexes in creation order, as MySQL does.
///
/// The primary key is synthesized from `pragma_table_info` (an `INTEGER
/// PRIMARY KEY` has no index of its own in SQLite); the automatic index
/// SQLite builds for any other primary key (`origin = 'pk'`) is therefore
/// skipped so the key is not listed twice.
pub(crate) fn index_rows(tables: Tables<'_>) -> String {
    let src = tables.source();
    format!(
        "SELECT m.name COLLATE NOCASE AS TABLE_NAME, 0 AS NON_UNIQUE, \
         'PRIMARY' COLLATE NOCASE AS INDEX_NAME, p.pk AS SEQ_IN_INDEX, \
         p.name COLLATE NOCASE AS COLUMN_NAME, '' AS NULLABLE, -1000000000 AS __ord \
         FROM {src} JOIN pragma_table_info(m.name) AS p WHERE p.pk > 0 \
         UNION ALL \
         SELECT m.name, 1 - il.\"unique\", il.name, ii.seqno + 1, ii.name, \
         CASE WHEN c.\"notnull\" = 1 OR c.pk > 0 THEN '' ELSE 'YES' END, -il.seq \
         FROM {src} JOIN pragma_index_list(m.name) AS il \
         JOIN pragma_index_info(il.name) AS ii \
         LEFT JOIN pragma_table_info(m.name) AS c ON c.name = ii.name \
         WHERE il.origin <> 'pk'"
    )
}

/// `information_schema.STATISTICS`-shaped rows with every MySQL column.
fn statistics_rows() -> String {
    format!(
        "SELECT 'def' AS TABLE_CATALOG, '{SCHEMA_NAME}' AS TABLE_SCHEMA, TABLE_NAME, \
         NON_UNIQUE, '{SCHEMA_NAME}' AS INDEX_SCHEMA, INDEX_NAME, SEQ_IN_INDEX, \
         COLUMN_NAME, 'A' AS COLLATION, NULL AS CARDINALITY, NULL AS SUB_PART, \
         NULL AS PACKED, NULLABLE, 'BTREE' AS INDEX_TYPE, '' AS COMMENT, \
         '' AS INDEX_COMMENT, 'YES' AS IS_VISIBLE, NULL AS EXPRESSION \
         FROM ({}) ORDER BY TABLE_NAME, __ord, SEQ_IN_INDEX",
        index_rows(Tables::All)
    )
}

/// `information_schema.SCHEMATA`-shaped rows: the one database.
fn schemata_rows() -> String {
    format!(
        "SELECT 'def' AS CATALOG_NAME, '{SCHEMA_NAME}' COLLATE NOCASE AS SCHEMA_NAME, \
         'utf8mb4' AS DEFAULT_CHARACTER_SET_NAME, \
         'utf8mb4_unicode_ci' AS DEFAULT_COLLATION_NAME, NULL AS SQL_PATH, \
         'NO' AS DEFAULT_ENCRYPTION"
    )
}

// ── information_schema substitution ─────────────────────────────────────────

/// Placeholder table names spliced into the AST and replaced with the
/// emulation subquery after emission. The odd spelling keeps them from
/// colliding with anything a client writes.
const PLACEHOLDER_PREFIX: &str = "__litewire_is_";

/// An emulated table: its canonical name and the builder of its SQL.
type Emulation = (&'static str, fn() -> String);

/// The emulation for `information_schema.<table>`, if there is one.
fn emulation_for(table: &str) -> Option<Emulation> {
    match table.to_ascii_lowercase().as_str() {
        "tables" => Some(("tables", table_rows)),
        "columns" => Some(("columns", || column_rows(Tables::All))),
        "statistics" => Some(("statistics", statistics_rows)),
        "schemata" => Some(("schemata", schemata_rows)),
        _ => None,
    }
}

/// Unquoted text of each part of an object name.
fn name_parts(name: &ObjectName) -> Vec<&str> {
    name.0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.as_str()))
        .collect()
}

/// Columns whose comparisons name a schema and therefore get rewritten by
/// [`SchemaPredicates`].
fn is_schema_column(expr: &Expr) -> bool {
    let ident = match expr {
        Expr::Identifier(i) => i,
        Expr::CompoundIdentifier(parts) => match parts.last() {
            Some(i) => i,
            None => return false,
        },
        _ => return false,
    };
    ["table_schema", "schema_name", "index_schema"]
        .iter()
        .any(|c| ident.value.eq_ignore_ascii_case(c))
}

/// Replaces `information_schema.<t>` table references with placeholders.
struct Substitute {
    /// `(placeholder, emulation SQL)` pairs to splice in after emission.
    found: Vec<(String, String)>,
}

impl VisitorMut for Substitute {
    type Break = TranslateError;

    fn pre_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<Self::Break> {
        let TableFactor::Table {
            name, alias, args, ..
        } = tf
        else {
            return ControlFlow::Continue(());
        };
        let parts = name_parts(name);
        let [schema, table] = parts.as_slice() else {
            return ControlFlow::Continue(());
        };
        if !schema.eq_ignore_ascii_case("information_schema") || args.is_some() {
            return ControlFlow::Continue(());
        }
        let Some((canonical, build)) = emulation_for(table) else {
            return ControlFlow::Break(TranslateError::Unsupported(format!(
                "information_schema.{table} is not emulated by litewire \
                 (supported: tables, columns, statistics, schemata)"
            )));
        };
        let placeholder = format!("{PLACEHOLDER_PREFIX}{canonical}");
        if !self.found.iter().any(|(p, _)| *p == placeholder) {
            self.found.push((placeholder.clone(), build()));
        }
        *name = ObjectName::from(vec![Ident::new(placeholder)]);
        if alias.is_none() {
            *alias = Some(TableAlias {
                name: Ident::new(canonical),
                columns: vec![],
            });
        }
        ControlFlow::Continue(())
    }
}

/// Rewrites comparisons against a schema-name column.
///
/// litewire serves exactly one database per connection, under whatever name
/// the client connected with — a name the stateless translator never sees.
/// So `table_schema = 'shop'` (or `= DATABASE()`, or `= ?` with the bound
/// database name, as Laravel <= 10 sends) means "this database" and becomes
/// true, while a comparison against a system schema (`'mysql'`,
/// `'information_schema'`, ...) becomes false. A bound placeholder is kept
/// in place (`(? IS NULL OR 1)`) so the statement's parameter count does
/// not change.
struct SchemaPredicates;

impl SchemaPredicates {
    /// The truth value a schema comparison against `other` reduces to.
    fn reduce(other: &Expr) -> Option<Expr> {
        let one = || Expr::value(Value::Number("1".into(), false));
        let zero = || Expr::value(Value::Number("0".into(), false));
        match other {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    Some(if is_system_schema(s) { zero() } else { one() })
                }
                Value::Placeholder(_) => Some(Expr::Nested(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::IsNull(Box::new(other.clone()))),
                    op: BinaryOperator::Or,
                    right: Box::new(one()),
                }))),
                _ => None,
            },
            // DATABASE() / SCHEMA() and other expressions naming the
            // current schema.
            Expr::Function(_) => Some(one()),
            _ => None,
        }
    }
}

impl VisitorMut for SchemaPredicates {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let replacement = match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                if is_schema_column(left) {
                    Self::reduce(right)
                } else if is_schema_column(right) {
                    Self::reduce(left)
                } else {
                    None
                }
            }
            Expr::InList {
                expr: col,
                list,
                negated: false,
            } if is_schema_column(col) => {
                let reduced: Option<Vec<Expr>> = list.iter().map(Self::reduce).collect();
                reduced.map(|terms| {
                    let joined = terms
                        .into_iter()
                        .reduce(|a, b| Expr::BinaryOp {
                            left: Box::new(a),
                            op: BinaryOperator::Or,
                            right: Box::new(b),
                        })
                        .unwrap_or_else(|| Expr::value(Value::Number("0".into(), false)));
                    Expr::Nested(Box::new(joined))
                })
            }
            _ => None,
        };
        if let Some(r) = replacement {
            *expr = r;
        }
        ControlFlow::Continue(())
    }
}

/// Cheap pre-check: could `sql` reference `information_schema` at all?
pub(crate) fn mentions_information_schema(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("information_schema")
}

/// Replace every `information_schema.{tables,columns,statistics,schemata}`
/// reference in `stmt` with a placeholder table and rewrite schema-name
/// predicates (see [`SchemaPredicates`]).
///
/// Returns the `(placeholder, emulation SQL)` pairs [`splice`] needs after
/// the statement is emitted; empty when the statement touches no
/// `information_schema` table.
///
/// # Errors
///
/// [`TranslateError::Unsupported`] for an `information_schema` table that is
/// not emulated — an error, never an empty or unrelated result.
pub(crate) fn substitute_information_schema(
    stmt: &mut Statement,
) -> Result<Vec<(String, String)>, TranslateError> {
    let mut sub = Substitute { found: Vec::new() };
    if let ControlFlow::Break(e) = stmt.visit(&mut sub) {
        return Err(e);
    }
    if !sub.found.is_empty() {
        let _ = stmt.visit(&mut SchemaPredicates);
        let _ = stmt.visit(&mut NameAsWritten);
    }
    Ok(sub.found)
}

/// Gives every bare column in a projection an explicit alias spelled as
/// the client wrote it.
///
/// MySQL names a result column after the select-list text, so
/// `SELECT row_format FROM information_schema.tables` yields a column
/// `row_format` and PHP's `$row['row_format']` finds it. SQLite engines
/// name a column taken from a derived table after the derived table's own
/// column (`ROW_FORMAT`), and PHP array keys are case-sensitive.
struct NameAsWritten;

impl NameAsWritten {
    fn alias_projection(body: &mut sqlparser::ast::SetExpr) {
        use sqlparser::ast::{SelectItem, SetExpr};
        match body {
            SetExpr::Select(select) => {
                for item in &mut select.projection {
                    let alias = match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.clone(),
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                            match parts.last() {
                                Some(id) => id.clone(),
                                None => continue,
                            }
                        }
                        _ => continue,
                    };
                    if let SelectItem::UnnamedExpr(expr) = item {
                        *item = SelectItem::ExprWithAlias {
                            expr: expr.clone(),
                            alias: Ident::with_quote('`', alias.value),
                        };
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                Self::alias_projection(left);
                Self::alias_projection(right);
            }
            SetExpr::Query(q) => Self::alias_projection(&mut q.body),
            _ => {}
        }
    }
}

impl VisitorMut for NameAsWritten {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut sqlparser::ast::Query) -> ControlFlow<()> {
        Self::alias_projection(&mut query.body);
        ControlFlow::Continue(())
    }
}

/// Splice emulation subqueries over the placeholders in emitted SQL.
pub(crate) fn splice(mut sql: String, found: &[(String, String)]) -> String {
    for (placeholder, emulation) in found {
        sql = sql.replace(placeholder.as_str(), &format!("({emulation})"));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_str_doubles_quotes() {
        assert_eq!(sql_str("a'b"), "'a''b'");
    }

    #[test]
    fn system_schemas_are_case_insensitive() {
        assert!(is_system_schema("INFORMATION_SCHEMA"));
        assert!(is_system_schema("mysql"));
        assert!(!is_system_schema("sendy"));
    }

    #[test]
    fn one_table_source_is_quoted() {
        let src = Tables::One("o'brien").source();
        assert!(src.contains("'o''brien'"), "{src}");
    }
}
