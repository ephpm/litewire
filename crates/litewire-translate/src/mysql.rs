//! MySQL-specific SQL rewrites.
//!
//! Handles MySQL DDL constructs (`AUTO_INCREMENT`, `ENGINE=`, type mappings),
//! DML rewrites (`ON DUPLICATE KEY UPDATE`, `LIMIT offset, count`), and
//! MySQL-specific expressions.

use sqlparser::ast::{
    DataType, DoUpdate, LimitClause, Offset, OffsetRows, OnConflict, OnConflictAction, OnInsert,
    Statement,
};

use crate::TranslateError;

/// Apply MySQL-specific rewrites to a statement in-place.
pub fn rewrite_statement(stmt: &mut Statement) -> Result<(), TranslateError> {
    match stmt {
        Statement::CreateTable(create) => {
            rewrite_create_table(create);
        }
        Statement::Insert(insert) => {
            rewrite_insert_on_duplicate(insert);
        }
        Statement::Query(query) => {
            rewrite_limit_clause(query);
        }
        Statement::Update { assignments, .. } => {
            rewrite_update_assignment_targets(assignments);
        }
        _ => {}
    }
    Ok(())
}

/// Strip table qualifiers from UPDATE SET targets: MySQL accepts
/// `UPDATE users SET users.updated_at = ...`, SQLite requires a bare
/// column name. (Laravel's Eloquent qualifies the `updated_at` column.)
fn rewrite_update_assignment_targets(assignments: &mut [sqlparser::ast::Assignment]) {
    use sqlparser::ast::{AssignmentTarget, ObjectName};
    for a in assignments {
        if let AssignmentTarget::ColumnName(name) = &mut a.target {
            if name.0.len() > 1 {
                if let Some(last) = name.0.pop() {
                    *name = ObjectName(vec![last]);
                }
            }
        }
    }
}

// ── ALTER TABLE ADD KEY/UNIQUE -> CREATE INDEX ───────────────────────────────

/// Expand MySQL `ALTER TABLE ... ADD {KEY|INDEX|UNIQUE} name (cols)` into
/// standalone `CREATE [UNIQUE] INDEX` statements (SQLite has no
/// `ALTER TABLE ... ADD CONSTRAINT`). Laravel's schema builder emits one
/// such ALTER per `->unique()` / `->index()` after every `create table`.
///
/// Non-index operations are preserved in a residual `ALTER TABLE`.
#[must_use]
pub fn expand_alter_table(stmt: Statement) -> Vec<Statement> {
    use sqlparser::ast::{
        AlterTableOperation, CreateIndex, IndexColumn, OrderByExpr, OrderByOptions, TableConstraint,
    };

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
        return vec![stmt];
    };

    let to_index_columns = |cols: &[sqlparser::ast::Ident]| -> Vec<IndexColumn> {
        cols.iter()
            .map(|c| IndexColumn {
                column: OrderByExpr {
                    expr: sqlparser::ast::Expr::Identifier(c.clone()),
                    options: OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                operator_class: None,
            })
            .collect()
    };

    let mut indexes: Vec<Statement> = Vec::new();
    let mut residual_ops: Vec<AlterTableOperation> = Vec::new();

    for op in operations {
        match &op {
            AlterTableOperation::AddConstraint(TableConstraint::Unique {
                name: cname,
                index_name,
                columns,
                ..
            }) => {
                let idx_name = index_name.clone().or_else(|| cname.clone());
                indexes.push(Statement::CreateIndex(CreateIndex {
                    name: idx_name.map(|i| sqlparser::ast::ObjectName::from(vec![i])),
                    table_name: name.clone(),
                    using: None,
                    columns: to_index_columns(columns),
                    unique: true,
                    concurrently: false,
                    if_not_exists: false,
                    include: vec![],
                    nulls_distinct: None,
                    with: vec![],
                    predicate: None,
                }));
            }
            AlterTableOperation::AddConstraint(TableConstraint::Index {
                name: iname,
                columns,
                ..
            }) => {
                indexes.push(Statement::CreateIndex(CreateIndex {
                    name: iname
                        .clone()
                        .map(|i| sqlparser::ast::ObjectName::from(vec![i])),
                    table_name: name.clone(),
                    using: None,
                    columns: to_index_columns(columns),
                    unique: false,
                    concurrently: false,
                    if_not_exists: false,
                    include: vec![],
                    nulls_distinct: None,
                    with: vec![],
                    predicate: None,
                }));
            }
            // FULLTEXT/SPATIAL: no SQLite analogue — drop (perf-only).
            AlterTableOperation::AddConstraint(TableConstraint::FulltextOrSpatial { .. }) => {}
            _ => residual_ops.push(op),
        }
    }

    let mut out = Vec::new();
    if !residual_ops.is_empty() {
        out.push(Statement::AlterTable {
            name,
            if_exists,
            only,
            operations: residual_ops,
            location,
            on_cluster,
            iceberg,
        });
    }
    out.extend(indexes);
    out
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

    if let Expr::Function(func) = expr {
        if func.name.to_string().eq_ignore_ascii_case("VALUES") {
            if let FunctionArguments::List(args) = &func.args {
                if args.args.len() == 1 {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(col))) =
                        &args.args[0]
                    {
                        *expr = Expr::CompoundIdentifier(vec![Ident::new("excluded"), col.clone()]);
                        return;
                    }
                }
            }
        }
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
fn rewrite_limit_clause(query: &mut sqlparser::ast::Query) {
    if let Some(LimitClause::OffsetCommaLimit { offset, limit }) = query.limit_clause.take() {
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit: Some(limit),
            offset: Some(Offset {
                value: offset,
                rows: OffsetRows::None,
            }),
            limit_by: vec![],
        });
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

    // Constraint fixups. The emitter is sqlparser `Display`, so MySQL-only
    // constraint syntax must be removed or normalized here:
    // - inline `KEY name (cols)` / FULLTEXT / SPATIAL: not valid SQLite —
    //   dropped (secondary indexes are a performance concern, not a
    //   correctness one; WordPress dbDelta emits one per table).
    // - `UNIQUE KEY name (cols)`: normalize to bare `UNIQUE (cols)` —
    //   functionally required (MySQL upserts target these).
    // - `PRIMARY KEY (col)`: displays fine for SQLite; kept as-is.
    create.constraints.retain(|c| {
        !matches!(
            c,
            sqlparser::ast::TableConstraint::Index { .. }
                | sqlparser::ast::TableConstraint::FulltextOrSpatial { .. }
        )
    });
    for c in &mut create.constraints {
        if let sqlparser::ast::TableConstraint::Unique {
            name,
            index_name,
            index_type_display,
            index_type,
            index_options,
            ..
        } = c
        {
            *name = None;
            *index_name = None;
            *index_type = None;
            *index_type_display = sqlparser::ast::KeyOrIndexDisplay::None;
            index_options.clear();
        }
    }

    // Rewrite column types.
    for col in &mut create.columns {
        col.data_type = rewrite_data_type(&col.data_type);

        // Remove AUTO_INCREMENT from column options.
        col.options.retain(|opt| {
            !matches!(
                &opt.option,
                sqlparser::ast::ColumnOption::DialectSpecific(tokens)
                    if tokens.iter().any(|t| t.to_string().eq_ignore_ascii_case("AUTO_INCREMENT"))
            )
        });
    }
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
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("UNIQUE ("), "unique constraint lost: {sql}");
        assert!(
            !upper.contains("UNIQUE KEY"),
            "MySQL UNIQUE KEY survived: {sql}"
        );
        assert!(
            !upper.contains("KEY AUTOLOAD"),
            "plain KEY constraint survived: {sql}"
        );
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
}
