//! MySQL type mapping from SQLite column declarations.

use opensrv_mysql::ColumnType;

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

    // ── Edge case tests ────────────────────────────────────────────────────

    #[test]
    fn type_mapping_empty_string() {
        // An empty string has no keyword matches, falls through to default.
        assert_eq!(
            sqlite_to_mysql_column_type(Some("")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_whitespace_only() {
        // Whitespace-only string does not contain any type keywords.
        assert_eq!(
            sqlite_to_mysql_column_type(Some("   ")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_integer_with_extra_whitespace() {
        // uppercased "  INTEGER  " still contains "INT".
        assert_eq!(
            sqlite_to_mysql_column_type(Some("  INTEGER  ")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn type_mapping_text_with_extra_whitespace() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("  TEXT  ")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_blob_with_extra_whitespace() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("  BLOB  ")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_real_with_extra_whitespace() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("  REAL  ")),
            ColumnType::MYSQL_TYPE_DOUBLE
        );
    }

    #[test]
    fn type_mapping_clob() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("CLOB")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_mediumint() {
        // "MEDIUMINT" contains "INT"
        assert_eq!(
            sqlite_to_mysql_column_type(Some("MEDIUMINT")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn type_mapping_smallint() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("SMALLINT")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }

    #[test]
    fn type_mapping_tinyblob() {
        // "TINYBLOB" contains "BLOB"
        assert_eq!(
            sqlite_to_mysql_column_type(Some("TINYBLOB")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_mediumblob() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("MEDIUMBLOB")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_longblob() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("LONGBLOB")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_mixed_case_varchar() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("VarChar(100)")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_nchar() {
        // "NCHAR" contains "CHAR"
        assert_eq!(
            sqlite_to_mysql_column_type(Some("NCHAR(10)")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_nvarchar() {
        // "NVARCHAR" contains "VARCHAR"
        assert_eq!(
            sqlite_to_mysql_column_type(Some("NVARCHAR(255)")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_numeric_falls_to_default() {
        // "NUMERIC" does not contain INT, REAL, FLOAT, DOUBLE, BLOB, TEXT,
        // CHAR, CLOB, or VARCHAR -- falls to default.
        assert_eq!(
            sqlite_to_mysql_column_type(Some("NUMERIC")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_boolean_falls_to_default() {
        assert_eq!(
            sqlite_to_mysql_column_type(Some("BOOLEAN")),
            ColumnType::MYSQL_TYPE_VAR_STRING
        );
    }

    #[test]
    fn type_mapping_bytea_case_insensitive() {
        // "bytea" uppercases to "BYTEA" which is checked with ==
        assert_eq!(
            sqlite_to_mysql_column_type(Some("bytea")),
            ColumnType::MYSQL_TYPE_BLOB
        );
    }

    #[test]
    fn type_mapping_int_priority_over_later_checks() {
        // "INT" is checked first, so "MEDIUMINT UNSIGNED" still hits INT.
        assert_eq!(
            sqlite_to_mysql_column_type(Some("MEDIUMINT UNSIGNED")),
            ColumnType::MYSQL_TYPE_LONGLONG
        );
    }
}
