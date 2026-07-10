//! Map litewire-backend error messages to real MySQL error codes.
//!
//! `BackendError` is a stringly-typed pass-through of the underlying
//! rusqlite error (see `litewire-backend`), so this module works by matching
//! substrings of the message text against the shape of `rusqlite::Error`'s
//! Display impl and the SQLite constraint-failure message conventions.
//!
//! This is deliberately conservative: any error we can't classify falls back
//! to `ER_UNKNOWN_ERROR` (1105 / HY000) so callers still see the raw text.
//!
//! Reference: <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>

use opensrv_mysql::ErrorKind;

/// The full MySQL error triple: code + SQLSTATE + message.
#[derive(Debug, Clone)]
pub struct MysqlError {
    /// MySQL error code (e.g. 1062 for duplicate entry).
    pub code: ErrorKind,
    /// SQLSTATE (5 chars, e.g. "23000"). Not read by production code -- the
    /// wire packet SQLSTATE is derived from `ErrorKind::sqlstate()` inside
    /// `opensrv-mysql`. Retained on the struct so tests can pin down the exact
    /// SQLSTATE we intend each mapping to produce and so future callers that
    /// want to log both the code and the SQLSTATE can do so from one place.
    #[cfg_attr(not(test), allow(dead_code))]
    pub sqlstate: [u8; 5],
    /// Human-readable message, forwarded verbatim from the backend.
    pub message: String,
}

/// Classify a backend error string into a `MysqlError`.
///
/// This function is pure and infallible; unknown errors return
/// `ER_UNKNOWN_ERROR` (MySQL 1105).
#[must_use]
pub fn classify(err_msg: &str) -> MysqlError {
    let lower = err_msg.to_ascii_lowercase();

    // -- Locking / busy --------------------------------------------------------
    // SQLITE_BUSY / SQLITE_LOCKED -> MySQL 1205 "Lock wait timeout exceeded"
    // (SQLSTATE HY000). This is the closest analogue clients will actually
    // retry on.
    if lower.contains("database is locked")
        || lower.contains("database table is locked")
        || lower.contains("sqlite_busy")
        || lower.contains("sqlite_locked")
    {
        return MysqlError {
            code: ErrorKind::ER_LOCK_WAIT_TIMEOUT,
            sqlstate: *b"HY000",
            message: err_msg.to_string(),
        };
    }

    // -- Constraint violations -------------------------------------------------
    // Unique / primary key -> 1062 (SQLSTATE 23000).
    if lower.contains("unique constraint failed") || lower.contains("primary key constraint failed")
    {
        return MysqlError {
            code: ErrorKind::ER_DUP_ENTRY,
            sqlstate: *b"23000",
            message: err_msg.to_string(),
        };
    }

    // Foreign key -> 1452 (SQLSTATE 23000).
    if lower.contains("foreign key constraint failed") {
        return MysqlError {
            code: ErrorKind::ER_NO_REFERENCED_ROW_2,
            sqlstate: *b"23000",
            message: err_msg.to_string(),
        };
    }

    // -- Read-only ------------------------------------------------------------
    // SQLITE_READONLY -> 1290 "The MySQL server is running with the ...
    // --read-only option so it cannot execute this statement" (HY000).
    if lower.contains("attempt to write a readonly database")
        || lower.contains("readonly database")
        || lower.contains("sqlite_readonly")
    {
        return MysqlError {
            code: ErrorKind::ER_OPTION_PREVENTS_STATEMENT,
            sqlstate: *b"HY000",
            message: err_msg.to_string(),
        };
    }

    // Fallback.
    MysqlError {
        code: ErrorKind::ER_UNKNOWN_ERROR,
        sqlstate: *b"HY000",
        message: err_msg.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_constraint_maps_to_1062() {
        let e = classify("UNIQUE constraint failed: users.email");
        assert!(matches!(e.code, ErrorKind::ER_DUP_ENTRY));
        assert_eq!(&e.sqlstate, b"23000");
    }

    #[test]
    fn primary_key_constraint_maps_to_1062() {
        let e = classify("PRIMARY KEY constraint failed: users.id");
        assert!(matches!(e.code, ErrorKind::ER_DUP_ENTRY));
        assert_eq!(&e.sqlstate, b"23000");
    }

    #[test]
    fn foreign_key_maps_to_1452() {
        let e = classify("FOREIGN KEY constraint failed");
        assert!(matches!(e.code, ErrorKind::ER_NO_REFERENCED_ROW_2));
        assert_eq!(&e.sqlstate, b"23000");
    }

    #[test]
    fn busy_maps_to_1205() {
        let e = classify("database is locked");
        assert!(matches!(e.code, ErrorKind::ER_LOCK_WAIT_TIMEOUT));
        assert_eq!(&e.sqlstate, b"HY000");
    }

    #[test]
    fn readonly_maps_to_1290() {
        let e = classify("attempt to write a readonly database");
        assert!(matches!(e.code, ErrorKind::ER_OPTION_PREVENTS_STATEMENT));
        assert_eq!(&e.sqlstate, b"HY000");
    }

    #[test]
    fn unknown_falls_back_to_1105() {
        let e = classify("no such table: sprockets");
        assert!(matches!(e.code, ErrorKind::ER_UNKNOWN_ERROR));
        assert_eq!(&e.sqlstate, b"HY000");
    }

    #[test]
    fn classify_preserves_message() {
        let msg = "UNIQUE constraint failed: users.email";
        let e = classify(msg);
        assert_eq!(e.message, msg);
    }
}
