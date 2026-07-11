//! MySQL type mapping from SQLite column declarations.

use litewire_backend::Value;
use opensrv_mysql::ColumnType;

/// Map a runtime [`Value`] to the MySQL column type its binary encoding
/// requires.
///
/// SQLite is dynamically typed: expression columns (`SELECT 1`, `SELECT
/// a + b`) have no declared type, so `decltype` is `None` and the
/// declaration-based mapping would fall back to `VAR_STRING`. But the row
/// writer emits each value in its native form (an integer as `LONGLONG`),
/// and opensrv's binary protocol rejects a declared/actual type mismatch
/// ("tried to use 1 as MYSQL_TYPE_VAR_STRING") and drops the connection —
/// surfacing to the client as `2006 server has gone away`. For untyped
/// columns we therefore derive the wire type from the actual data.
#[must_use]
pub fn mysql_type_for_value(value: &Value) -> ColumnType {
    match value {
        Value::Null => ColumnType::MYSQL_TYPE_VAR_STRING,
        Value::Integer(_) => ColumnType::MYSQL_TYPE_LONGLONG,
        Value::Float(_) => ColumnType::MYSQL_TYPE_DOUBLE,
        Value::Text(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
        Value::Blob(_) => ColumnType::MYSQL_TYPE_BLOB,
    }
}

/// Map a SQLite column declared type to a MySQL column type.
///
/// Uses the declared type string from `PRAGMA table_info` or the column
/// `decltype` in query results.
#[must_use]
pub fn sqlite_to_mysql_column_type(decltype: Option<&str>) -> ColumnType {
    let Some(dt) = decltype else {
        return ColumnType::MYSQL_TYPE_VAR_STRING;
    };

    let upper = dt.to_ascii_uppercase();

    if upper.contains("INT") {
        ColumnType::MYSQL_TYPE_LONGLONG
    } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
        ColumnType::MYSQL_TYPE_DOUBLE
    } else if upper.contains("BLOB") || upper == "BYTEA" {
        ColumnType::MYSQL_TYPE_BLOB
    } else if upper.contains("TEXT")
        || upper.contains("CHAR")
        || upper.contains("CLOB")
        || upper.contains("VARCHAR")
    {
        ColumnType::MYSQL_TYPE_VAR_STRING
    } else {
        // Default: treat as string.
        ColumnType::MYSQL_TYPE_VAR_STRING
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping_integer() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("INTEGER")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn type_mapping_int_substring() {
        // "BIGINT", "TINYINT", etc. all contain "INT"
        assert_eq!(
            sqlite_to_mysql_column_type(Some("BIGINT")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
        assert_eq!(
            sqlite_to_mysql_column_type(Some("TINYINT")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn type_mapping_real() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("REAL")),
            ColumnType::MYSQL_TYPE_DOUBLE
        );
    }

    #[test]
    fn type_mapping_float() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("FLOAT")),
            ColumnType::MYSQL_TYPE_DOUBLE
        );
    }

    #[test]
    fn type_mapping_double() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("DOUBLE")),
            ColumnType::MYSQL_TYPE_DOUBLE
        );
    }

    #[test]
    fn type_mapping_text() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("TEXT")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_varchar() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("VARCHAR(255)")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_char() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("CHAR(10)")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_blob() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("BLOB")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_bytea() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("BYTEA")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_none_defaults_to_string() {
        assert_eq!(
            sqlite_to_mysql_column_type(None),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_unknown_defaults_to_string() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("UNKNOWN_TYPE")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_case_insensitive() {
        // The function uppercases, so lowercase should work too.
        assert_eq!(
            sqlite_to_mysql_column_type(Some("integer")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
        assert_eq!(
            sqlite_to_mysql_column_type(Some("real")),
            ColumnType::MYSQL_TYPE_DOUBLE
        );
        assert_eq!(
            sqlite_to_mysql_column_type(Some("text")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
        assert_eq!(
            sqlite_to_mysql_column_type(Some("blob")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }
}
