//! PostgreSQL-specific SQL rewrites.

use std::ops::ControlFlow;

use sqlparser::ast::{DataType, Query, Statement, VisitMut, VisitorMut};

use crate::TranslateError;

/// Apply PostgreSQL-specific rewrites to a statement in-place.
pub fn rewrite_statement(stmt: &mut Statement) -> Result<(), TranslateError> {
    if let Statement::CreateTable(create) = stmt {
        for col in &mut create.columns {
            col.data_type = rewrite_data_type(&col.data_type);
        }
    }
    // Drop locking-read clauses (`FOR UPDATE`, `FOR SHARE`, ...). SQLite has no
    // row-level locking, so Turso rejects the `FOR` keyword. The MySQL path
    // already clears these (see `mysql::QueryShape`); this closes the same gap
    // on the PostgreSQL side. The rows are returned; the lock is not enforced.
    let _ = VisitMut::visit(stmt, &mut StripLocks);
    Ok(())
}

/// Clears every query's locking-read clause (`query.locks`), nested queries
/// included.
struct StripLocks;

impl VisitorMut for StripLocks {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        query.locks.clear();
        ControlFlow::Continue(())
    }
}

/// Map PostgreSQL types to SQLite affinities.
fn rewrite_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::SmallInt(_) | DataType::Int(_) | DataType::BigInt(_) | DataType::Integer(_) => {
            DataType::Integer(None)
        }

        DataType::Real
        | DataType::Float(_)
        | DataType::Double(..)
        | DataType::Numeric(..)
        | DataType::Decimal(..) => DataType::Real,

        DataType::Varchar(_) | DataType::Char(_) | DataType::Text | DataType::Uuid => {
            DataType::Text
        }

        DataType::Bytea => DataType::Blob(None),

        DataType::Boolean => DataType::Integer(None),

        DataType::Date | DataType::Timestamp(_, _) | DataType::Time(_, _) | DataType::Interval => {
            DataType::Text
        }

        DataType::JSON | DataType::JSONB => DataType::Text,

        DataType::Custom(name, _) => {
            let upper = name.to_string().to_ascii_uppercase();
            match upper.as_str() {
                "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" => DataType::Integer(None),
                _ => dt.clone(),
            }
        }

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

    #[test]
    fn for_update_is_stripped() {
        for sql in [
            "SELECT id FROM users WHERE id = 1 FOR UPDATE",
            "SELECT id FROM users FOR SHARE",
            "SELECT id FROM users WHERE id = 1 FOR UPDATE NOWAIT",
        ] {
            let results = translate(sql, Dialect::PostgreSQL).unwrap();
            let out = extract_sql(&results[0]);
            assert!(
                !out.to_ascii_uppercase().contains("FOR UPDATE")
                    && !out.to_ascii_uppercase().contains("FOR SHARE"),
                "locking clause survived for `{sql}`: {out}"
            );
        }
    }

    #[test]
    fn for_update_stripped_in_subquery() {
        let results = translate(
            "SELECT * FROM (SELECT id FROM users FOR UPDATE) AS x",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let out = extract_sql(&results[0]);
        assert!(
            !out.to_ascii_uppercase().contains("FOR UPDATE"),
            "nested locking clause survived: {out}"
        );
    }

    #[test]
    fn int_types_to_integer() {
        let results = translate(
            "CREATE TABLE t (a SMALLINT, b INT, c BIGINT)",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("SMALLINT"), "SMALLINT not rewritten: {sql}");
        assert!(!upper.contains("BIGINT"), "BIGINT not rewritten: {sql}");
    }

    #[test]
    fn float_to_real() {
        let results = translate(
            "CREATE TABLE t (a FLOAT(8), b NUMERIC(10,2))",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("REAL"), "no REAL found: {sql}");
        assert!(!upper.contains("NUMERIC"), "NUMERIC not rewritten: {sql}");
    }

    #[test]
    fn varchar_to_text() {
        let results = translate(
            "CREATE TABLE t (name VARCHAR(255), bio TEXT)",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("VARCHAR"), "VARCHAR not rewritten: {sql}");
    }

    #[test]
    fn bytea_to_blob() {
        let results = translate("CREATE TABLE t (data BYTEA)", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("BYTEA"), "BYTEA not rewritten: {sql}");
        assert!(upper.contains("BLOB"), "no BLOB found: {sql}");
    }

    #[test]
    fn uuid_to_text() {
        let results = translate("CREATE TABLE t (id UUID)", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("UUID"), "UUID not rewritten: {sql}");
        assert!(upper.contains("TEXT"), "no TEXT found: {sql}");
    }

    #[test]
    fn boolean_to_integer() {
        let results = translate("CREATE TABLE t (active BOOLEAN)", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("BOOLEAN"), "BOOLEAN not rewritten: {sql}");
        assert!(upper.contains("INTEGER"), "no INTEGER found: {sql}");
    }

    #[test]
    fn timestamp_to_text() {
        let results = translate(
            "CREATE TABLE t (created TIMESTAMP, updated DATE)",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(
            !upper.contains("TIMESTAMP"),
            "TIMESTAMP not rewritten: {sql}"
        );
    }

    #[test]
    fn jsonb_to_text() {
        let results = translate("CREATE TABLE t (data JSONB)", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("JSONB"), "JSONB not rewritten: {sql}");
        assert!(upper.contains("TEXT"), "no TEXT found: {sql}");
    }

    #[test]
    fn simple_select_passthrough() {
        let results = translate("SELECT 1 + 2", Dialect::PostgreSQL).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("1 + 2"), "got: {sql}");
    }

    #[test]
    fn serial_to_integer() {
        let results = translate(
            "CREATE TABLE t (id SERIAL PRIMARY KEY)",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("SERIAL"), "SERIAL not rewritten: {sql}");
        assert!(upper.contains("INTEGER"), "no INTEGER found: {sql}");
    }

    #[test]
    fn bigserial_to_integer() {
        let results = translate(
            "CREATE TABLE t (id BIGSERIAL PRIMARY KEY)",
            Dialect::PostgreSQL,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("INTEGER"), "no INTEGER found: {sql}");
    }
}
