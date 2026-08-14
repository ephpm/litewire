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
/// MySQL `ER_NO_SUCH_TABLE` -- the referenced table does not exist.
pub const ER_NO_SUCH_TABLE: u16 = 1146;
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
    /// Human-readable message.
    ///
    /// Forwarded verbatim from the backend for every classification except
    /// [`ER_DUP_ENTRY`], where a MySQL-shaped prefix is prepended because
    /// stock drivers detect duplicates by matching the message text. The
    /// backend's own words are always present -- see
    /// [`duplicate_entry_message`].
    pub message: String,
}

/// The placeholder litewire uses where MySQL would print the duplicated
/// value.
///
/// SQLite's constraint-failure message names the columns and nothing else --
/// the offending value is simply not in the error text, and the statement's
/// parameters are not available at this layer. Rather than omit the field
/// (which breaks the message shape clients match on) or invent a value
/// (which would be a lie), the slot says so.
const UNKNOWN_VALUE: &str = "<unknown>";

/// Rewrite SQLite's unique-constraint message into MySQL's shape.
///
/// MySQL says `Duplicate entry 'alice@example.com' for key 'users.email'`;
/// SQLite says `UNIQUE constraint failed: users.email`. Stock MySQL drivers
/// detect a duplicate by matching the *message*, not just the code -- so a
/// client litewire does not control (and cannot patch) never recognised
/// these, and ePHPm's Laravel integration had to override
/// `isUniqueConstraintError()` to work around it.
///
/// The SQLite text is kept, in parentheses, so nothing is lost for
/// debugging: the synthesised part is a prefix, not a replacement.
///
/// Two honest imprecisions, both unavoidable at this layer:
///
/// * The duplicated **value** is not in SQLite's message, so the slot holds
///   [`UNKNOWN_VALUE`].
/// * MySQL names the **index**; SQLite names the **columns**. For the
///   single-column unique index that produces almost all real duplicates,
///   and for the index-named-after-its-column convention WordPress and
///   Laravel both follow, these coincide. For a composite constraint the
///   column list stands in for the index name.
///
/// A message with no column list after the marker is left exactly as it
/// was: without one there is nothing to put in the `for key` slot, and a
/// content-free `Duplicate entry '<unknown>' for key '<unknown>'` would be
/// worse than the original text.
///
/// The column list is located by the `constraint failed: ` marker rather
/// than by splitting on the first colon, because the backends wrap the
/// message: rusqlite produces `SQLite error: UNIQUE constraint failed:
/// users.email`, so the first colon belongs to the wrapper, not the column
/// list.
fn duplicate_entry_message(err_msg: &str) -> String {
    /// The text SQLite always puts immediately before the column list, in
    /// both the `UNIQUE` and `PRIMARY KEY` wordings.
    const MARKER: &str = "constraint failed: ";

    // `to_ascii_lowercase` is byte-for-byte length preserving, so an index
    // found in the lowered copy is valid in the original.
    let Some(at) = err_msg.to_ascii_lowercase().rfind(MARKER) else {
        return err_msg.to_string();
    };
    let key = err_msg[at + MARKER.len()..].trim();
    if key.is_empty() {
        return err_msg.to_string();
    }
    format!("Duplicate entry '{UNKNOWN_VALUE}' for key '{key}' ({err_msg})")
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

    // -- Missing table ---------------------------------------------------------
    // SQLite's "no such table: X" -> MySQL 1146 (SQLSTATE 42S02). Frameworks
    // branch on this to tell "the schema is not migrated yet" apart from "the
    // query is broken": Doctrine raises TableNotFoundException off 1146, and
    // Laravel's schema tooling keys off 42S02. Falling back to 1105/HY000
    // made a missing table indistinguishable from any other failure.
    //
    // The message is forwarded verbatim -- SQLite already names the table,
    // and unlike the duplicate-key case below there is no MySQL message
    // shape that clients match on.
    if lower.contains("no such table") {
        return MappedError {
            code: ER_NO_SUCH_TABLE,
            sqlstate: *b"42S02",
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
            message: duplicate_entry_message(err_msg),
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
        // Re-fixtured: this used to be `"no such table: sprockets"`, which
        // is now deliberately classified as 1146 (issue #22). The fixtures
        // below are a real SQLite error with no MySQL analogue, and a
        // string that is not a SQLite error at all -- two independent
        // reasons for the fallback to be reached, so the test cannot go
        // vacuous if one of them ever gains a mapping. If `disk I/O error`
        // does gain one, move this fixture rather than deleting the test.
        for msg in ["disk I/O error", "something the classifier never heard of"] {
            let e = classify(msg);
            assert_eq!(e.code, ER_UNKNOWN_ERROR, "{msg}");
            assert_eq!(&e.sqlstate, b"HY000", "{msg}");
            assert_eq!(e.message, msg, "unclassified errors keep their text");
        }
    }

    #[test]
    fn classify_preserves_message() {
        // Narrowed from "the message is exactly the backend's text" to "the
        // backend's text survives", because 1062 now carries a MySQL-shaped
        // prefix in front of it (issue #22). The property that matters --
        // that SQLite's own words reach the operator -- is still asserted,
        // and for every classification that is *not* reshaped the stricter
        // byte-for-byte check is kept below.
        let msg = "UNIQUE constraint failed: users.email";
        let e = classify(msg);
        assert!(
            e.message.contains(msg),
            "lost SQLite's own text: {}",
            e.message
        );

        for msg in [
            "database is locked",
            "FOREIGN KEY constraint failed",
            "attempt to write a readonly database",
            "no such table: sprockets",
            "disk I/O error",
        ] {
            assert_eq!(classify(msg).message, msg, "{msg} must be forwarded as-is");
        }
    }

    // ── Missing table (issue #22) ────────────────────────────────────────

    #[test]
    fn no_such_table_maps_to_1146() {
        let e = classify("no such table: sprockets");
        assert_eq!(e.code, ER_NO_SUCH_TABLE);
        assert_eq!(&e.sqlstate, b"42S02");
        assert_eq!(e.message, "no such table: sprockets");
    }

    #[test]
    fn no_such_table_is_matched_inside_a_longer_message() {
        // Turso prefixes its version of this with "Parse error: ".
        let e = classify("Parse error: no such table: wp_posts");
        assert_eq!(e.code, ER_NO_SUCH_TABLE);
        assert_eq!(&e.sqlstate, b"42S02");
    }

    // ── Duplicate entry message shape (issue #22) ────────────────────────

    #[test]
    fn duplicate_entry_gets_a_mysql_shaped_message() {
        let e = classify("UNIQUE constraint failed: users.email");
        assert_eq!(e.code, ER_DUP_ENTRY);
        assert_eq!(
            e.message,
            "Duplicate entry '<unknown>' for key 'users.email' \
             (UNIQUE constraint failed: users.email)"
        );
    }

    #[test]
    fn duplicate_entry_message_matches_the_shape_stock_drivers_look_for() {
        // The reason this synthesis exists: drivers litewire does not
        // control detect duplicates with a regex over the message.
        let message = classify("UNIQUE constraint failed: users.email").message;
        let key_start = message.find("Duplicate entry '").expect("no entry marker");
        let for_key = message.find("' for key '").expect("no key marker");
        assert!(for_key > key_start);
        assert!(message[for_key..].contains("users.email'"));
    }

    #[test]
    fn primary_key_violation_gets_the_same_shape() {
        let e = classify("PRIMARY KEY constraint failed: users.id");
        assert_eq!(e.code, ER_DUP_ENTRY);
        assert!(
            e.message
                .starts_with("Duplicate entry '<unknown>' for key 'users.id'")
        );
        assert!(
            e.message
                .contains("PRIMARY KEY constraint failed: users.id")
        );
    }

    #[test]
    fn composite_unique_constraint_uses_the_whole_column_list_as_the_key() {
        let e = classify("UNIQUE constraint failed: t.a, t.b");
        assert!(
            e.message
                .starts_with("Duplicate entry '<unknown>' for key 't.a, t.b'"),
            "got: {}",
            e.message
        );
    }

    #[test]
    fn duplicate_entry_key_survives_a_backend_message_prefix() {
        // What rusqlite actually produces. Splitting on the first `": "`
        // would name the key `UNIQUE constraint failed: users.email`.
        let e = classify("SQLite error: UNIQUE constraint failed: users.email");
        assert!(
            e.message
                .starts_with("Duplicate entry '<unknown>' for key 'users.email'"),
            "got: {}",
            e.message
        );
    }

    #[test]
    fn duplicate_entry_without_a_column_list_keeps_its_original_message() {
        // Nothing to name the key with, so reshaping would only subtract
        // information.
        let e = classify("UNIQUE constraint failed");
        assert_eq!(e.code, ER_DUP_ENTRY);
        assert_eq!(e.message, "UNIQUE constraint failed");
    }
}
