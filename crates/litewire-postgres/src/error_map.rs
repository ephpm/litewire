//! Map litewire-backend error messages to PostgreSQL SQLSTATE codes.
//!
//! `BackendError` is a stringly-typed pass-through of the underlying
//! rusqlite error; this module matches on the same substrings the MySQL
//! frontend's `error_map` uses and produces PG SQLSTATE codes instead.
//!
//! Reference: <https://www.postgresql.org/docs/current/errcodes-appendix.html>

/// A pgwire-shaped error identifier: SQLSTATE + human-readable message.
#[derive(Debug, Clone)]
pub struct PgError {
    /// 5-char SQLSTATE, e.g. `"23505"`.
    pub sqlstate: &'static str,
    /// Human-readable message, forwarded verbatim from the backend.
    pub message: String,
}

/// Classify a backend error string into a `PgError`.
///
/// Unknown errors return SQLSTATE `XX000` ("internal error").
#[must_use]
pub fn classify(err_msg: &str) -> PgError {
    let lower = err_msg.to_ascii_lowercase();

    // Unique / primary key violation -> 23505 unique_violation
    if lower.contains("unique constraint failed") || lower.contains("primary key constraint failed")
    {
        return PgError { sqlstate: "23505", message: err_msg.to_string() };
    }

    // Foreign key violation -> 23503 foreign_key_violation
    if lower.contains("foreign key constraint failed") {
        return PgError { sqlstate: "23503", message: err_msg.to_string() };
    }

    // NOT NULL violation -> 23502 not_null_violation
    if lower.contains("not null constraint failed") {
        return PgError { sqlstate: "23502", message: err_msg.to_string() };
    }

    // CHECK constraint -> 23514 check_violation
    if lower.contains("check constraint failed") {
        return PgError { sqlstate: "23514", message: err_msg.to_string() };
    }

    // SQLITE_BUSY / SQLITE_LOCKED -> 55P03 lock_not_available
    if lower.contains("database is locked")
        || lower.contains("database table is locked")
        || lower.contains("sqlite_busy")
        || lower.contains("sqlite_locked")
    {
        return PgError { sqlstate: "55P03", message: err_msg.to_string() };
    }

    // SQLITE_READONLY -> 25006 read_only_sql_transaction
    if lower.contains("attempt to write a readonly database")
        || lower.contains("readonly database")
        || lower.contains("sqlite_readonly")
    {
        return PgError { sqlstate: "25006", message: err_msg.to_string() };
    }

    // Fallback: internal_error
    PgError { sqlstate: "XX000", message: err_msg.to_string() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_maps_to_23505() {
        let e = classify("UNIQUE constraint failed: users.email");
        assert_eq!(e.sqlstate, "23505");
    }

    #[test]
    fn primary_key_maps_to_23505() {
        let e = classify("PRIMARY KEY constraint failed: users.id");
        assert_eq!(e.sqlstate, "23505");
    }

    #[test]
    fn foreign_key_maps_to_23503() {
        let e = classify("FOREIGN KEY constraint failed");
        assert_eq!(e.sqlstate, "23503");
    }

    #[test]
    fn not_null_maps_to_23502() {
        let e = classify("NOT NULL constraint failed: users.name");
        assert_eq!(e.sqlstate, "23502");
    }

    #[test]
    fn check_constraint_maps_to_23514() {
        let e = classify("CHECK constraint failed: users_age_check");
        assert_eq!(e.sqlstate, "23514");
    }

    #[test]
    fn busy_maps_to_55p03() {
        let e = classify("database is locked");
        assert_eq!(e.sqlstate, "55P03");
    }

    #[test]
    fn readonly_maps_to_25006() {
        let e = classify("attempt to write a readonly database");
        assert_eq!(e.sqlstate, "25006");
    }

    #[test]
    fn unknown_maps_to_xx000() {
        let e = classify("something random");
        assert_eq!(e.sqlstate, "XX000");
    }

    #[test]
    fn classify_preserves_message() {
        let msg = "UNIQUE constraint failed: users.email";
        let e = classify(msg);
        assert_eq!(e.message, msg);
    }
}
