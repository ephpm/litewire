//! SQLite type affinity to PostgreSQL type OID mapping.

use pgwire::api::Type;

/// Map a SQLite declared type string to a PostgreSQL [`Type`].
///
/// Falls back to `TEXT` for unknown types, which is safe because SQLite
/// stores everything as text-compatible anyway.
pub fn sqlite_to_pg_type(decltype: Option<&str>) -> Type {
    let Some(dt) = decltype else {
        return Type::TEXT;
    };

    let upper = dt.to_ascii_uppercase();

    if upper.contains("INT") {
        return Type::INT8;
    }
    if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
        return Type::FLOAT8;
    }
    if upper.contains("BLOB") || upper.contains("BYTEA") {
        return Type::BYTEA;
    }
    if upper.contains("BOOL") {
        return Type::BOOL;
    }

    // TEXT, VARCHAR, CHAR, CLOB, DATE, DATETIME, TIMESTAMP, etc.
    Type::TEXT
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── None / missing decltype ────────────────────────────────────────────
    #[test]
    fn none_defaults_to_text() {
        assert_eq!(sqlite_to_pg_type(None), Type::TEXT);
    }

    // ── Integer family ─────────────────────────────────────────────────────
    #[test]
    fn integer_maps_to_int8() {
        assert_eq!(sqlite_to_pg_type(Some("INTEGER")), Type::INT8);
    }

    #[test]
    fn int_maps_to_int8() {
        assert_eq!(sqlite_to_pg_type(Some("INT")), Type::INT8);
    }

    #[test]
    fn bigint_maps_to_int8() {
        assert_eq!(sqlite_to_pg_type(Some("BIGINT")), Type::INT8);
    }

    // ── Float family ───────────────────────────────────────────────────────
    #[test]
    fn real_maps_to_float8() {
        assert_eq!(sqlite_to_pg_type(Some("REAL")), Type::FLOAT8);
    }

    #[test]
    fn float_maps_to_float8() {
        assert_eq!(sqlite_to_pg_type(Some("FLOAT")), Type::FLOAT8);
    }

    #[test]
    fn double_maps_to_float8() {
        assert_eq!(sqlite_to_pg_type(Some("DOUBLE")), Type::FLOAT8);
    }

    // ── Text family ────────────────────────────────────────────────────────
    #[test]
    fn text_maps_to_text() {
        assert_eq!(sqlite_to_pg_type(Some("TEXT")), Type::TEXT);
    }

    #[test]
    fn varchar_maps_to_text() {
        assert_eq!(sqlite_to_pg_type(Some("VARCHAR")), Type::TEXT);
    }

    #[test]
    fn char_maps_to_text() {
        assert_eq!(sqlite_to_pg_type(Some("CHAR")), Type::TEXT);
    }

    #[test]
    fn clob_maps_to_text() {
        assert_eq!(sqlite_to_pg_type(Some("CLOB")), Type::TEXT);
    }

    // ── Binary family ──────────────────────────────────────────────────────
    #[test]
    fn blob_maps_to_bytea() {
        assert_eq!(sqlite_to_pg_type(Some("BLOB")), Type::BYTEA);
    }

    #[test]
    fn bytea_maps_to_bytea() {
        assert_eq!(sqlite_to_pg_type(Some("BYTEA")), Type::BYTEA);
    }

    // ── Boolean ────────────────────────────────────────────────────────────
    #[test]
    fn bool_maps_to_bool() {
        assert_eq!(sqlite_to_pg_type(Some("BOOL")), Type::BOOL);
    }

    #[test]
    fn boolean_maps_to_bool() {
        assert_eq!(sqlite_to_pg_type(Some("BOOLEAN")), Type::BOOL);
    }

    // ── Case insensitivity ─────────────────────────────────────────────────
    #[test]
    fn lowercase_integer() {
        assert_eq!(sqlite_to_pg_type(Some("integer")), Type::INT8);
    }

    #[test]
    fn mixed_case_integer() {
        assert_eq!(sqlite_to_pg_type(Some("Integer")), Type::INT8);
    }

    #[test]
    fn lowercase_real() {
        assert_eq!(sqlite_to_pg_type(Some("real")), Type::FLOAT8);
    }

    #[test]
    fn lowercase_blob() {
        assert_eq!(sqlite_to_pg_type(Some("blob")), Type::BYTEA);
    }

    // ── Unknown / fallback ─────────────────────────────────────────────────
    #[test]
    fn unknown_type_defaults_to_text() {
        assert_eq!(sqlite_to_pg_type(Some("FOOBAR")), Type::TEXT);
    }

    #[test]
    fn empty_string_defaults_to_text() {
        assert_eq!(sqlite_to_pg_type(Some("")), Type::TEXT);
    }
}
