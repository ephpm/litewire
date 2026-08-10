//! Map litewire-backend error messages to MySQL error codes.
//!
//! `BackendError` is a stringly-typed pass-through of the underlying
//! rusqlite error (see `litewire-backend`), so this module works by matching
//! substrings of the message text against the shape of `rusqlite::Error`'s
//! Display impl and the SQLite constraint-failure message conventions.
//!
//! This is deliberately conservative: any error we can't classify falls back
//! to `ER_UNKNOWN_ERROR` (1105 / HY000) so callers still see the raw text.
//!
//! This is the single source of truth for the mapping; the MySQL wire
//! frontend's `error_map` delegates here and only converts the numeric code
//! into its wire-protocol `ErrorKind` type.
//!
//! Reference: <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>

/// MySQL `ER_DUP_ENTRY` -- duplicate key on a unique / primary key index.
pub const ER_DUP_ENTRY: u16 = 1062;
/// MySQL `ER_UNKNOWN_ERROR` -- generic fallback.
pub const ER_UNKNOWN_ERROR: u16 = 1105;
/// MySQL `ER_LOCK_WAIT_TIMEOUT` -- lock wait timeout exceeded.
pub const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
/// MySQL `ER_OPTION_PREVENTS_STATEMENT` -- server running read-only.
pub const ER_OPTION_PREVENTS_STATEMENT: u16 = 1290;
/// MySQL `ER_NO_REFERENCED_ROW_2` -- foreign key constraint failure.
pub const ER_NO_REFERENCED_ROW_2: u16 = 1452;

/// The full MySQL error triple: numeric code + SQLSTATE + message.
#[derive(Debug, Clone)]
pub struct MappedError {
    /// MySQL error code (e.g. 1062 for duplicate entry).
    pub code: u16,
    /// SQLSTATE (5 chars, e.g. `"23000"`).
    pub sqlstate: [u8; 5],
    /// Human-readable message, forwarded verbatim from the backend.
    pub message: String,
}

/// Classify a backend error string into a [`MappedError`].
///
/// This function is pure and infallible; unknown errors return
/// `ER_UNKNOWN_ERROR` (MySQL 1105).
#[must_use]
pub fn classify(err_msg: &str) -> MappedError {
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
        return MappedError {
            code: ER_LOCK_WAIT_TIMEOUT,
            sqlstate: *b"HY000",
            message: err_msg.to_string(),
        };
    }

    // -- Constraint violations -------------------------------------------------
    // Unique / primary key -> 1062 (SQLSTATE 23000).
    if lower.contains("unique constraint failed") || lower.contains("primary key constraint failed")
    {
        return MappedError {
            code: ER_DUP_ENTRY,
            sqlstate: *b"23000",
            message: err_msg.to_string(),
        };
    }

    // Foreign key -> 1452 (SQLSTATE 23000).
    if lower.contains("foreign key constraint failed") {
        return MappedError {
            code: ER_NO_REFERENCED_ROW_2,
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
        return MappedError {
            code: ER_OPTION_PREVENTS_STATEMENT,
            sqlstate: *b"HY000",
            message: err_msg.to_string(),
        };
    }

    // Fallback.
    MappedError {
        code: ER_UNKNOWN_ERROR,
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
        assert_eq!(e.code, ER_DUP_ENTRY);
        assert_eq!(&e.sqlstate, b"23000");
    }

    #[test]
    fn primary_key_constraint_maps_to_1062() {
        let e = classify("PRIMARY KEY constraint failed: users.id");
        assert_eq!(e.code, ER_DUP_ENTRY);
        assert_eq!(&e.sqlstate, b"23000");
    }

    #[test]
    fn foreign_key_maps_to_1452() {
        let e = classify("FOREIGN KEY constraint failed");
        assert_eq!(e.code, ER_NO_REFERENCED_ROW_2);
        assert_eq!(&e.sqlstate, b"23000");
    }

    #[test]
    fn busy_maps_to_1205() {
        let e = classify("database is locked");
        assert_eq!(e.code, ER_LOCK_WAIT_TIMEOUT);
        assert_eq!(&e.sqlstate, b"HY000");
    }

    #[test]
    fn readonly_maps_to_1290() {
        let e = classify("attempt to write a readonly database");
        assert_eq!(e.code, ER_OPTION_PREVENTS_STATEMENT);
        assert_eq!(&e.sqlstate, b"HY000");
    }

    #[test]
    fn unknown_falls_back_to_1105() {
        let e = classify("no such table: sprockets");
        assert_eq!(e.code, ER_UNKNOWN_ERROR);
        assert_eq!(&e.sqlstate, b"HY000");
    }

    #[test]
    fn classify_preserves_message() {
        let msg = "UNIQUE constraint failed: users.email";
        let e = classify(msg);
        assert_eq!(e.message, msg);
    }
}
