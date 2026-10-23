//! MySQL `ALTER TABLE` column changes SQLite cannot do in place.
//!
//! SQLite's `ALTER TABLE` adds, drops and renames columns but cannot change
//! a column's type, nullability or default. MySQL migrations do that all
//! the time: `MODIFY [COLUMN] c <def>`, `CHANGE [COLUMN] old new <def>`,
//! `ALTER [COLUMN] c {SET DEFAULT x | DROP DEFAULT}`. SQLite's documented
//! answer is to rebuild the table — create the new shape under a temporary
//! name, copy the rows, drop the old table, rename the new one, recreate
//! its indexes and triggers — inside one transaction
//! (<https://www.sqlite.org/lang_altertable.html#otheralter>).
//!
//! The translator cannot do that alone: it needs the table's current
//! definition, which only the database knows. So translation produces a
//! [`ColumnRebuild`] plan ([`crate::TranslateResult::RebuildTable`]), the
//! session reads the table's `CREATE` statement and dependents from
//! `sqlite_master`, and [`ColumnRebuild::statements`] (pure, unit-tested)
//! turns the three into the statements to run.
//!
//! What carries over: rows (copied column by column, so a type change
//! re-applies SQLite affinity — `'12'` becomes `12` in an INTEGER column),
//! the column order, every other column untouched, the table's `PRIMARY
//! KEY` / `UNIQUE` constraints, inline `PRIMARY KEY` on a redefined column
//! (MySQL's `MODIFY id INT AUTO_INCREMENT` does not drop the key, which in
//! SQLite is a column option), and indexes and triggers, renamed columns
//! included. Views that name a renamed column are not rewritten.

use std::ops::ControlFlow;

use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, Expr, Ident, ObjectName, Statement, VisitMut,
    VisitorMut,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::TranslateError;

/// One column change of a [`ColumnRebuild`].
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnChange {
    /// `MODIFY c <def>` (`old == new`) or `CHANGE old new <def>`: the column
    /// gets this definition (already translated for SQLite).
    Redefine {
        /// Current column name.
        old: String,
        /// The definition, including the (possibly new) name.
        def: ColumnDef,
    },
    /// `ALTER c SET DEFAULT <expr>` (`Some`) / `DROP DEFAULT` (`None`).
    Default {
        /// Column name.
        column: String,
        /// The new default.
        value: Option<Expr>,
    },
    /// `ALTER c SET NOT NULL` (`true`) / `DROP NOT NULL` (`false`).
    NotNull {
        /// Column name.
        column: String,
        /// Whether the column becomes `NOT NULL`.
        not_null: bool,
    },
    /// `... FIRST` (`after: None`) / `... AFTER c`: move the column.
    Move {
        /// Column to move (its name after any rename in this rebuild).
        column: String,
        /// The column it goes after; `None` = first.
        after: Option<String>,
    },
}

/// A table rebuild that applies MySQL column changes. See the module docs.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRebuild {
    /// The table, unquoted.
    pub table: String,
    /// The changes, in statement order.
    pub changes: Vec<ColumnChange>,
}

/// Name of the temporary table a rebuild builds into.
fn temp_name(table: &str) -> String {
    format!("__litewire_rebuild_{table}")
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn unsupported(msg: impl Into<String>) -> TranslateError {
    TranslateError::Unsupported(msg.into())
}

/// Is `opt` a key option a MySQL column redefinition does not touch
/// (MySQL keeps indexes and keys separate from the column definition)?
fn is_key_option(opt: &ColumnOption) -> bool {
    matches!(opt, ColumnOption::Unique { .. })
        || matches!(opt, ColumnOption::DialectSpecific(tokens)
            if tokens.iter().any(|t| t.to_string().eq_ignore_ascii_case("AUTOINCREMENT")))
}

/// Renames column identifiers (bare or qualified) in a statement.
struct RenameColumns<'a> {
    renames: &'a [(String, String)],
}

impl VisitorMut for RenameColumns<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        let rename = |id: &mut Ident| {
            if let Some((_, new)) = self
                .renames
                .iter()
                .find(|(old, _)| old.eq_ignore_ascii_case(&id.value))
            {
                id.value.clone_from(new);
            }
        };
        match expr {
            Expr::Identifier(id) => rename(id),
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last_mut() {
                    rename(last);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl ColumnRebuild {
    /// The statements that rebuild the table, given its current `CREATE
    /// TABLE` statement and the `CREATE INDEX` / `CREATE TRIGGER`
    /// statements that depend on it (all as stored in `sqlite_master`).
    ///
    /// The caller runs them in one transaction (or savepoint).
    ///
    /// # Errors
    ///
    /// [`TranslateError`] when the stored definition cannot be parsed, a
    /// named column does not exist, or a renamed column would collide.
    pub fn statements(
        &self,
        table_sql: &str,
        dependents: &[String],
    ) -> Result<Vec<String>, TranslateError> {
        let dialect = SQLiteDialect {};
        let mut stmts = Parser::parse_sql(&dialect, table_sql).map_err(|e| {
            unsupported(format!(
                "cannot parse the definition of {}: {e}",
                self.table
            ))
        })?;
        let Some(Statement::CreateTable(mut create)) = stmts.pop() else {
            return Err(unsupported(format!("{} is not a table", self.table)));
        };

        // Each column with the source column its rows are copied from.
        let mut cols: Vec<(String, ColumnDef)> = create
            .columns
            .drain(..)
            .map(|c| (c.name.value.clone(), c))
            .collect();
        let mut renames: Vec<(String, String)> = Vec::new();

        for change in &self.changes {
            let target = match change {
                ColumnChange::Redefine { old, .. } => old,
                ColumnChange::Default { column, .. }
                | ColumnChange::NotNull { column, .. }
                | ColumnChange::Move { column, .. } => column,
            };
            if let ColumnChange::Move { after, .. } = change {
                let from = cols
                    .iter()
                    .position(|(_, c)| c.name.value.eq_ignore_ascii_case(target))
                    .ok_or_else(|| {
                        unsupported(format!("Unknown column '{target}' in '{}'", self.table))
                    })?;
                let moved = cols.remove(from);
                let to = match after {
                    None => 0,
                    Some(a) => {
                        cols.iter()
                            .position(|(_, c)| c.name.value.eq_ignore_ascii_case(a))
                            .ok_or_else(|| {
                                unsupported(format!("Unknown column '{a}' in '{}'", self.table))
                            })?
                            + 1
                    }
                };
                cols.insert(to, moved);
                continue;
            }
            let col = cols
                .iter_mut()
                .map(|(_, c)| c)
                .find(|c| c.name.value.eq_ignore_ascii_case(target))
                .ok_or_else(|| {
                    TranslateError::Unsupported(format!(
                        "Unknown column '{target}' in '{}'",
                        self.table
                    ))
                })?;
            match change {
                ColumnChange::Redefine { def, .. } => {
                    let kept: Vec<ColumnOptionDef> = col
                        .options
                        .iter()
                        .filter(|o| is_key_option(&o.option))
                        .cloned()
                        .collect();
                    let mut new_def = def.clone();
                    for k in kept {
                        if !new_def.options.iter().any(|o| o.option == k.option) {
                            new_def.options.push(k);
                        }
                    }
                    if !new_def.name.value.eq_ignore_ascii_case(&col.name.value) {
                        renames.push((col.name.value.clone(), new_def.name.value.clone()));
                    }
                    *col = new_def;
                }
                ColumnChange::Default { value, .. } => {
                    col.options
                        .retain(|o| !matches!(o.option, ColumnOption::Default(_)));
                    if let Some(v) = value {
                        col.options.push(ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(v.clone()),
                        });
                    }
                }
                ColumnChange::NotNull { not_null, .. } => {
                    col.options.retain(|o| {
                        !matches!(o.option, ColumnOption::NotNull | ColumnOption::Null)
                    });
                    if *not_null {
                        col.options.push(ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        });
                    }
                }
                ColumnChange::Move { .. } => {}
            }
        }
        let old_names: Vec<String> = cols.iter().map(|(src, _)| src.clone()).collect();
        create.columns = cols.into_iter().map(|(_, c)| c).collect();

        // No two columns may end up with the same name.
        let new_names: Vec<String> = create
            .columns
            .iter()
            .map(|c| c.name.value.clone())
            .collect();
        for (i, a) in new_names.iter().enumerate() {
            if new_names[i + 1..].iter().any(|b| b.eq_ignore_ascii_case(a)) {
                return Err(unsupported(format!("Duplicate column name '{a}'")));
            }
        }

        // Table-level constraints and dependents follow renamed columns.
        if !renames.is_empty() {
            let mut rename = RenameColumns { renames: &renames };
            for c in &mut create.constraints {
                let _ = c.visit(&mut rename);
                rename_constraint_columns(c, &renames);
            }
        }

        let temp = temp_name(&self.table);
        create.name = ObjectName::from(vec![Ident::with_quote('"', temp.clone())]);
        create.if_not_exists = false;

        let mut out = Vec::with_capacity(4 + dependents.len());
        out.push(create.to_string());
        out.push(format!(
            "INSERT INTO {} ({}) SELECT {} FROM {}",
            quote(&temp),
            new_names
                .iter()
                .map(|n| quote(n))
                .collect::<Vec<_>>()
                .join(", "),
            old_names
                .iter()
                .map(|n| quote(n))
                .collect::<Vec<_>>()
                .join(", "),
            quote(&self.table),
        ));
        out.push(format!("DROP TABLE {}", quote(&self.table)));
        out.push(format!(
            "ALTER TABLE {} RENAME TO {}",
            quote(&temp),
            quote(&self.table)
        ));
        for dep in dependents {
            if renames.is_empty() {
                out.push(dep.clone());
                continue;
            }
            let mut parsed = Parser::parse_sql(&dialect, dep)
                .map_err(|e| unsupported(format!("cannot parse dependent `{dep}`: {e}")))?;
            for s in &mut parsed {
                rename_statement_columns(s, &renames);
            }
            out.extend(parsed.iter().map(ToString::to_string));
        }
        Ok(out)
    }
}

fn rename_ident(id: &mut Ident, renames: &[(String, String)]) {
    if let Some((_, new)) = renames
        .iter()
        .find(|(old, _)| old.eq_ignore_ascii_case(&id.value))
    {
        id.value.clone_from(new);
    }
}

/// Rename the plain column lists of a table constraint (`PRIMARY KEY (a)`,
/// `UNIQUE (a, b)`), which hold idents rather than expressions.
fn rename_constraint_columns(
    c: &mut sqlparser::ast::TableConstraint,
    renames: &[(String, String)],
) {
    use sqlparser::ast::TableConstraint;
    match c {
        TableConstraint::Unique { columns, .. }
        | TableConstraint::PrimaryKey { columns, .. }
        | TableConstraint::Index { columns, .. } => {
            for col in columns {
                rename_ident(col, renames);
            }
        }
        TableConstraint::ForeignKey { columns, .. } => {
            for col in columns {
                rename_ident(col, renames);
            }
        }
        _ => {}
    }
}

/// Rename columns in a dependent `CREATE INDEX` / `CREATE TRIGGER`.
fn rename_statement_columns(s: &mut Statement, renames: &[(String, String)]) {
    let _ = s.visit(&mut RenameColumns { renames });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(sql: &str) -> ColumnDef {
        let Statement::CreateTable(c) =
            Parser::parse_sql(&SQLiteDialect {}, &format!("CREATE TABLE x ({sql})"))
                .unwrap()
                .remove(0)
        else {
            panic!()
        };
        c.columns[0].clone()
    }

    #[test]
    fn modify_keeps_other_columns_and_inline_primary_key() {
        let plan = ColumnRebuild {
            table: "rules".into(),
            changes: vec![
                ColumnChange::Redefine {
                    old: "endpoint".into(),
                    def: def("endpoint TEXT NOT NULL DEFAULT ''"),
                },
                ColumnChange::Redefine {
                    old: "id".into(),
                    def: def("id INTEGER NOT NULL"),
                },
            ],
        };
        let out = plan
            .statements(
                "CREATE TABLE rules (id INTEGER PRIMARY KEY AUTOINCREMENT, endpoint TEXT, n INTEGER)",
                &["CREATE INDEX \"rules__e\" ON rules (endpoint)".into()],
            )
            .unwrap();
        assert_eq!(
            out[0],
            "CREATE TABLE \"__litewire_rebuild_rules\" (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, endpoint TEXT NOT NULL DEFAULT '', n INTEGER)"
        );
        assert_eq!(
            out[1],
            "INSERT INTO \"__litewire_rebuild_rules\" (\"id\", \"endpoint\", \"n\") SELECT \"id\", \"endpoint\", \"n\" FROM \"rules\""
        );
        assert_eq!(out[2], "DROP TABLE \"rules\"");
        assert_eq!(
            out[3],
            "ALTER TABLE \"__litewire_rebuild_rules\" RENAME TO \"rules\""
        );
        assert_eq!(out[4], "CREATE INDEX \"rules__e\" ON rules (endpoint)");
    }

    #[test]
    fn change_renames_in_constraints_and_indexes() {
        let plan = ColumnRebuild {
            table: "seg_cons".into(),
            changes: vec![ColumnChange::Redefine {
                old: "grouping".into(),
                def: def("group_id INTEGER"),
            }],
        };
        let out = plan
            .statements(
                "CREATE TABLE seg_cons (id INTEGER, \"grouping\" INTEGER, UNIQUE (id, \"grouping\"))",
                &["CREATE INDEX \"seg_cons__g\" ON seg_cons (\"grouping\")".into()],
            )
            .unwrap();
        assert!(out[0].contains("group_id INTEGER"), "{}", out[0]);
        assert!(out[0].contains("UNIQUE (id, \"group_id\")"), "{}", out[0]);
        assert!(
            out[1].contains("(\"id\", \"group_id\") SELECT \"id\", \"grouping\""),
            "{}",
            out[1]
        );
        assert!(
            out[4].contains("group_id") && !out[4].contains("grouping"),
            "{}",
            out[4]
        );
    }

    #[test]
    fn set_and_drop_default() {
        let plan = ColumnRebuild {
            table: "c".into(),
            changes: vec![
                ColumnChange::Default {
                    column: "a".into(),
                    value: Some(Expr::value(sqlparser::ast::Value::Number(
                        "1".into(),
                        false,
                    ))),
                },
                ColumnChange::Default {
                    column: "b".into(),
                    value: None,
                },
            ],
        };
        let out = plan
            .statements(
                "CREATE TABLE c (a INTEGER DEFAULT 0, b TEXT DEFAULT 'x')",
                &[],
            )
            .unwrap();
        assert_eq!(
            out[0],
            "CREATE TABLE \"__litewire_rebuild_c\" (a INTEGER DEFAULT 1, b TEXT)"
        );
    }

    #[test]
    fn move_reorders_and_copies_by_source_column() {
        let plan = ColumnRebuild {
            table: "apps".into(),
            changes: vec![
                ColumnChange::Move {
                    column: "z".into(),
                    after: Some("a".into()),
                },
                ColumnChange::Move {
                    column: "b".into(),
                    after: None,
                },
            ],
        };
        let out = plan
            .statements(
                "CREATE TABLE apps (id INTEGER PRIMARY KEY, a TEXT, b TEXT, z INTEGER)",
                &[],
            )
            .unwrap();
        assert_eq!(
            out[0],
            "CREATE TABLE \"__litewire_rebuild_apps\" (b TEXT, id INTEGER PRIMARY KEY, a TEXT, z INTEGER)"
        );
        assert_eq!(
            out[1],
            "INSERT INTO \"__litewire_rebuild_apps\" (\"b\", \"id\", \"a\", \"z\") SELECT \"b\", \"id\", \"a\", \"z\" FROM \"apps\""
        );
    }

    #[test]
    fn unknown_column_errors() {
        let plan = ColumnRebuild {
            table: "c".into(),
            changes: vec![ColumnChange::Default {
                column: "zz".into(),
                value: None,
            }],
        };
        assert!(plan.statements("CREATE TABLE c (a INTEGER)", &[]).is_err());
    }
}
