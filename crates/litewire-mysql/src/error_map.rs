//! Map litewire-backend error messages to real MySQL error codes.
//!
//! The classification logic lives in [`litewire_session::error_map`] (it was
//! extracted there so embedders get the same mapping without a wire-protocol
//! dependency); this module is the thin wire-side adapter that converts the
//! numeric code into `opensrv-mysql`'s [`ErrorKind`].
//!
//! The mapping is deliberately conservative: any error the classifier can't
//! recognize falls back to `ER_UNKNOWN_ERROR` (1105 / HY000) so callers
//! still see the raw text.
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
/// Delegates to [`litewire_session::error_map::classify`] -- the single
/// source of truth for the message-substring mapping -- and converts the
/// numeric MySQL code to the wire [`ErrorKind`].
///
/// This function is pure and infallible; unknown errors return
/// `ER_UNKNOWN_ERROR` (MySQL 1105).
#[must_use]
pub fn classify(err_msg: &str) -> MysqlError {
    let mapped = litewire_session::error_map::classify(err_msg);
    MysqlError {
        code: ErrorKind::from(mapped.code),
        sqlstate: mapped.sqlstate,
        message: mapped.message,
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
