//! T-SQL (SQL Server) specific SQL rewrites.

use sqlparser::ast::{DataType, Statement};

use crate::TranslateError;

/// Apply T-SQL-specific rewrites to a statement in-place.
pub fn rewrite_statement(stmt: &mut Statement) -> Result<(), TranslateError> {
    if let Statement::CreateTable(create) = stmt {
        for col in &mut create.columns {
            col.data_type = rewrite_data_type(&col.data_type);
        }
    }
    Ok(())
}

/// Map T-SQL types to SQLite affinities.
fn rewrite_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Integer(_) => DataType::Integer(None),

        DataType::Float(_)
        | DataType::Double(..)
        | DataType::Decimal(..)
        | DataType::Numeric(..) => DataType::Real,

        DataType::Varchar(_)
        | DataType::Char(_)
        | DataType::Nvarchar(_)
        | DataType::Text => DataType::Text,

        DataType::Binary(_) | DataType::Varbinary(_) => DataType::Blob(None),

        DataType::Boolean => DataType::Integer(None),

        DataType::Date | DataType::Datetime(_) | DataType::Timestamp(_, _) | DataType::Time(_, _) => {
            DataType::Text
        }

        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{translate, Dialect, TranslateResult};

    fn extract_sql(result: &TranslateResult) -> &str {
        match result {
            TranslateResult::Sql(s) => s.as_str(),
            other => panic!("expected Sql, got: {other:?}"),
        }
    }

    #[test]
    fn int_types_to_integer() {
        let results = translate(
            "CREATE TABLE t (a TINYINT, b SMALLINT, c INT, d BIGINT)",
            Dialect::TDS,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("TINYINT"), "TINYINT not rewritten: {sql}");
        assert!(!upper.contains("SMALLINT"), "SMALLINT not rewritten: {sql}");
        assert!(!upper.contains("BIGINT"), "BIGINT not rewritten: {sql}");
    }

    #[test]
    fn nvarchar_to_text() {
        let results = translate(
            "CREATE TABLE t (name NVARCHAR(255))",
            Dialect::TDS,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("NVARCHAR"), "NVARCHAR not rewritten: {sql}");
        assert!(upper.contains("TEXT"), "no TEXT found: {sql}");
    }

    #[test]
    fn varbinary_to_blob() {
        let results = translate("CREATE TABLE t (data VARBINARY(MAX))", Dialect::TDS).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("VARBINARY"), "VARBINARY not rewritten: {sql}");
        assert!(upper.contains("BLOB"), "no BLOB found: {sql}");
    }

    #[test]
    fn datetime_to_text() {
        let results =
            translate("CREATE TABLE t (created DATETIME)", Dialect::TDS).unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(!upper.contains("DATETIME"), "DATETIME not rewritten: {sql}");
        assert!(upper.contains("TEXT"), "no TEXT found: {sql}");
    }

    #[test]
    fn float_types_to_real() {
        let results = translate(
            "CREATE TABLE t (a FLOAT, b DECIMAL(10,2))",
            Dialect::TDS,
        )
        .unwrap();
        let sql = extract_sql(&results[0]);
        let upper = sql.to_ascii_uppercase();
        assert!(upper.contains("REAL"), "no REAL found: {sql}");
    }

    #[test]
    fn set_nocount_is_noop() {
        let results = translate("SET NOCOUNT ON", Dialect::TDS).unwrap();
        assert!(matches!(results[0], TranslateResult::Noop));
    }

    #[test]
    fn simple_select_passthrough() {
        let results = translate("SELECT 1 + 2", Dialect::TDS).unwrap();
        let sql = extract_sql(&results[0]);
        assert!(sql.contains("1 + 2"), "got: {sql}");
    }
}
