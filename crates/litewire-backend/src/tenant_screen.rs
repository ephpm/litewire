//! SQL screen for tenant-scoped backend sessions.
//!
//! # Why this exists
//!
//! `ATTACH DATABASE '/path/to/other-tenant.db'` is *the* cross-tenant
//! primitive for a file-per-tenant deployment: every tenant's database is
//! opened by the same process under the same uid, so nothing at the
//! filesystem level stops a session bound to tenant A from attaching tenant
//! B's file and reading -- or writing -- it. `DETACH`, `VACUUM INTO`, and
//! the path-bearing / schema-reopening `PRAGMA`s are variations on the same
//! primitive.
//!
//! A session established through a
//! [`ConnectionAuthenticator`](crate::ConnectionAuthenticator) is exactly
//! that shape: the authenticator is the tenant boundary, and the backend it
//! returns is supposed to be the *whole* world that connection can touch.
//! The MySQL frontend therefore wraps every authenticator-established
//! session in [`screened`], so these statements are refused at the backend
//! boundary regardless of what the engine underneath would do with them.
//!
//! Single-tenant sessions -- a fixed backend chosen at build time, or an
//! embedder driving a [`BackendConn`](crate::BackendConn) directly -- are
//! deliberately not screened. `ATTACH` is legitimate and useful in a
//! single-user embedded setup, and litewire has no business refusing it
//! there. The screen keys off the session being tenant-scoped, not off the
//! statement being scary.
//!
//! # Why at this layer and not the engine
//!
//! The rusqlite backend executes whatever SQLite accepts, which includes all
//! of the statements above; the Turso backend happens to refuse `ATTACH`
//! today because the engine does. Neither is a property litewire owns. This
//! screen makes the refusal litewire's own, on every backend, so a future
//! engine (or engine flag) that accepts `ATTACH` does not silently turn the
//! authenticator's boundary into a suggestion.
//!
//! # What it costs
//!
//! One quote- and comment-aware linear scan of each statement, with no
//! allocation on the accept path -- on a path that is about to do file I/O.

use crate::{Backend, BackendConn, BackendError, Column, ExecuteResult, ResultSet, Value};

/// Statement keywords always rejected on a tenant session: the
/// cross-database primitives. `ATTACH` opens an arbitrary file into the
/// session; `DETACH` is its cleanup half.
const FORBIDDEN_KEYWORDS: [&str; 2] = ["ATTACH", "DETACH"];

/// `PRAGMA` names rejected because they name or move a filesystem path, or
/// re-open the schema for arbitrary edits (`writable_schema`). Ordinary
/// tuning pragmas (`foreign_keys`, `journal_mode`, `busy_timeout`, ...) are
/// unaffected.
///
/// Matched against **every** dot-separated component of the pragma's target
/// (see [`pragma_name_parts`]), so the schema-qualified and quoted spellings
/// -- `PRAGMA main.data_store_directory`, `PRAGMA "writable_schema"` -- are
/// refused the same way as the bare form.
const FORBIDDEN_PRAGMAS: [&str; 3] = [
    "writable_schema",
    "temp_store_directory",
    "data_store_directory",
];

/// Wrap a session so every statement is screened before it reaches the
/// engine. This is what the MySQL frontend applies to each session a
/// [`ConnectionAuthenticator`](crate::ConnectionAuthenticator) establishes.
#[must_use]
pub fn screened(inner: Box<dyn BackendConn>) -> Box<dyn BackendConn> {
    Box::new(ScreenedConn { inner })
}

/// Wrap a whole backend so every session it opens is [`screened`].
///
/// For embedders that hold a per-tenant [`Backend`](crate::Backend) and want
/// the screen guaranteed on every route to it, not only on wire sessions.
pub struct TenantScreened<B> {
    inner: B,
}

impl<B: Backend> TenantScreened<B> {
    /// Screen every session opened against `inner`.
    pub const fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B: Backend> Backend for TenantScreened<B> {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        Ok(screened(self.inner.connect().await?))
    }
}

/// Turn a rejected statement into the error the caller sees.
///
/// [`BackendError::Sqlite`] rather than `Other`, so the wire frontends'
/// error mapping treats it like any other statement-level failure: the
/// client gets a clean SQL error and its connection stays usable, instead of
/// a transport error that would drop the session.
fn refuse(offending: &str) -> BackendError {
    BackendError::Sqlite(format!(
        "statement type `{offending}` is not permitted on a tenant session"
    ))
}

struct ScreenedConn {
    inner: Box<dyn BackendConn>,
}

#[async_trait::async_trait]
impl BackendConn for ScreenedConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        screen_sql(sql).map_err(|o| refuse(&o))?;
        self.inner.query(sql, params).await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        screen_sql(sql).map_err(|o| refuse(&o))?;
        self.inner.execute(sql, params).await
    }

    async fn describe_columns(&self, sql: &str) -> Result<Vec<Column>, BackendError> {
        // Describing does not execute, but it does hand the statement to the
        // engine's parser/planner. Screen it too rather than reason about
        // what a planner does with a path it was not supposed to see.
        screen_sql(sql).map_err(|o| refuse(&o))?;
        self.inner.describe_columns(sql).await
    }
}

/// Screen a (possibly multi-statement) SQL string for statements that are
/// forbidden on a tenant session. Quote- and comment-aware, so a `;` or
/// keyword hidden inside a string literal or comment is not mistaken for a
/// statement.
///
/// # Errors
///
/// Returns the offending keyword (e.g. `"ATTACH"`,
/// `"PRAGMA data_store_directory"`) when a forbidden statement is found. An
/// unterminated quote or block comment is treated conservatively as
/// forbidden (`"malformed SQL"`), so a truncated `ATTACH` cannot slip
/// through.
pub fn screen_sql(sql: &str) -> Result<(), String> {
    let bytes = sql.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err("malformed SQL".to_string());
                    }
                    if bytes[i] == quote {
                        // A doubled quote is an escaped quote, not a close.
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                loop {
                    if i + 1 >= bytes.len() {
                        return Err("malformed SQL".to_string());
                    }
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b';' => {
                check_statement(&sql[start..i])?;
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    check_statement(&sql[start..])
}

/// Reject one statement whose leading verb is forbidden. Empty /
/// comment-only statements are allowed.
///
/// A leading `EXPLAIN` / `EXPLAIN QUERY PLAN` diagnostic wrapper is seen
/// through first (see [`strip_explain_prefixes`]), so `EXPLAIN ATTACH ...`
/// is refused for the same reason and with the same error as `ATTACH ...`.
/// A screen that stops at the first keyword is bypassable by exactly that
/// wrapper, and the point of this screen is that the refusal is litewire's
/// own -- not whatever the engine underneath happens to do with the wrapped
/// verb today.
fn check_statement(stmt: &str) -> Result<(), String> {
    let trimmed = strip_explain_prefixes(stmt)?;
    if trimmed.is_empty() {
        return Ok(());
    }
    let (keyword, rest) = leading_keyword(trimmed);

    if FORBIDDEN_KEYWORDS.contains(&keyword.as_str()) {
        return Err(keyword);
    }
    if keyword == "VACUUM" {
        // Bare `VACUUM` only rewrites the session's own database and stays
        // permitted -- it is ordinary maintenance with no cross-tenant
        // reach. Any argued form is refused: `VACUUM INTO '<path>'` writes a
        // copy of the database to an arbitrary path, and a schema-qualified
        // `VACUUM <name>` is meaningless without `ATTACH` anyway.
        if !strip_leading_noise(rest).is_empty() {
            return Err("VACUUM INTO".to_string());
        }
    }
    if keyword == "PRAGMA" {
        for part in pragma_name_parts(rest) {
            if FORBIDDEN_PRAGMAS.contains(&part.as_str()) {
                return Err(format!("PRAGMA {part}"));
            }
        }
    }
    Ok(())
}

/// The dot-separated identifier components of a `PRAGMA`'s target,
/// lowercased -- e.g. `main.data_store_directory` yields
/// `["main", "data_store_directory"]`.
///
/// SQLite's grammar is `PRAGMA [schema-name '.'] pragma-name`, and the
/// identifiers may be quoted (`"x"`, `` `x` ``, `[x]`, and -- given SQLite's
/// lenient identifier handling -- `'x'`). A screen that reads only the first
/// bare token extracts `main` from `PRAGMA main.data_store_directory` and
/// waves the statement through; every component is therefore returned and
/// the caller matches all of them against the denylist. No schema is
/// legitimately named `writable_schema`, so this costs nothing in false
/// positives.
///
/// Whitespace and comments between the tokens are skipped
/// ([`strip_leading_noise`]), so `PRAGMA main /* c */ . writable_schema` is
/// seen. An unterminated quoted identifier stops the scan: the statement is
/// not valid SQL, and the outer [`screen_sql`] scanner has already rejected
/// the quote styles it tracks.
fn pragma_name_parts(rest: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut s = strip_leading_noise(rest);
    loop {
        let Some((ident, after)) = take_identifier(s) else {
            return parts;
        };
        parts.push(ident.to_ascii_lowercase());
        let after = strip_leading_noise(after);
        let Some(next) = after.strip_prefix('.') else {
            return parts;
        };
        s = strip_leading_noise(next);
    }
}

/// Split one leading SQL identifier off `s`, returning its unquoted text and
/// the remainder. Handles the bare form (`[A-Za-z0-9_$]+`) and the quoted
/// forms `"x"`, `` `x` ``, `[x]` and `'x'`, including the doubled-delimiter
/// escape. Returns `None` when `s` does not begin with an identifier, or
/// when a quoted one is unterminated.
fn take_identifier(s: &str) -> Option<(String, &str)> {
    let bytes = s.as_bytes();
    let close = match bytes.first()? {
        b'"' => b'"',
        b'`' => b'`',
        b'\'' => b'\'',
        b'[' => b']',
        _ => {
            let len = bytes
                .iter()
                .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
                .count();
            if len == 0 {
                return None;
            }
            return Some((s[..len].to_string(), &s[len..]));
        }
    };

    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == close {
            // A doubled delimiter escapes itself in the quote styles that
            // have one (`[x]]` is not an escape -- brackets do not nest).
            if close != b']' && bytes.get(i + 1) == Some(&close) {
                i += 2;
                continue;
            }
            // `i` and `1` are ASCII delimiter positions, so both are char
            // boundaries even if the identifier holds multi-byte text.
            let inner = &s[1..i];
            let unescaped = match close {
                b'"' => inner.replace("\"\"", "\""),
                b'`' => inner.replace("``", "`"),
                b'\'' => inner.replace("''", "'"),
                _ => inner.to_string(),
            };
            return Some((unescaped, &s[i + 1..]));
        }
        i += 1;
    }
    None
}

/// Split off the leading run of ASCII letters (a SQL keyword) from `s`,
/// uppercased, and return it together with the remainder that follows it.
/// Returns `("", s)` when `s` does not start with a letter.
fn leading_keyword(s: &str) -> (String, &str) {
    let len = s.bytes().take_while(u8::is_ascii_alphabetic).count();
    (s[..len].to_ascii_uppercase(), &s[len..])
}

/// See through leading `EXPLAIN` / `EXPLAIN QUERY PLAN` prefixes so the
/// statement they wrap is what [`check_statement`]'s forbidden-verb check
/// inspects. `EXPLAIN ATTACH DATABASE '...'` parses its *first* keyword as
/// `EXPLAIN`, so a screen that only looked at the leading keyword would wave
/// it through and leave the refusal to the engine's default -- exactly the
/// property this screen exists to own.
///
/// Comment- and whitespace-aware between every token
/// ([`strip_leading_noise`]), case-insensitive, and applied in a loop so a
/// stacked `EXPLAIN EXPLAIN ATTACH` still exposes the inner `ATTACH`.
/// `EXPLAIN QUERY PLAN` is consumed only as the exact three-token unit; a
/// malformed `EXPLAIN QUERY <not PLAN>` is rejected conservatively rather
/// than passed on. Legitimate `EXPLAIN SELECT ...` is unaffected: once the
/// wrapper is stripped, the inner `SELECT` is a permitted verb.
fn strip_explain_prefixes(stmt: &str) -> Result<&str, String> {
    let mut rest = strip_leading_noise(stmt);
    loop {
        let (keyword, after) = leading_keyword(rest);
        if keyword != "EXPLAIN" {
            return Ok(rest);
        }
        rest = strip_leading_noise(after);

        // Optional `QUERY PLAN` -- the only valid two-word form of EXPLAIN.
        let (next, after_next) = leading_keyword(rest);
        if next == "QUERY" {
            let after_query = strip_leading_noise(after_next);
            let (plan, after_plan) = leading_keyword(after_query);
            if plan != "PLAN" {
                return Err("EXPLAIN".to_string());
            }
            rest = strip_leading_noise(after_plan);
        }
        // Loop again: peel any further stacked EXPLAIN prefix.
    }
}

/// Drop leading whitespace and leading SQL comments so a statement's first
/// keyword is what gets checked. Returns `""` when the input is only
/// whitespace and comments (including an unterminated one).
fn strip_leading_noise(stmt: &str) -> &str {
    let mut s = stmt;
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            let Some(nl) = rest.find('\n') else { return "" };
            s = &rest[nl + 1..];
        } else if let Some(rest) = s.strip_prefix("/*") {
            let Some(end) = rest.find("*/") else {
                return "";
            };
            s = &rest[end + 2..];
        } else {
            return s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── screen_sql: the pure screen ────────────────────────────────────────

    #[test]
    fn attach_and_detach_are_refused() {
        for sql in [
            "ATTACH DATABASE '/srv/tenants/other.db' AS stolen",
            "attach database 'x.db' as x",
            "  ATTACH 'x.db' AS x",
            "DETACH DATABASE stolen",
            "detach x",
        ] {
            assert!(screen_sql(sql).is_err(), "must be refused: {sql:?}");
        }
    }

    #[test]
    fn vacuum_with_a_target_is_refused_bare_vacuum_is_not() {
        assert!(screen_sql("VACUUM INTO '/tmp/copy.db'").is_err());
        assert!(screen_sql("vacuum into 'x'").is_err());
        assert!(screen_sql("VACUUM main INTO '/tmp/copy.db'").is_err());
        // Bare VACUUM rewrites only the session's own database.
        assert!(screen_sql("VACUUM").is_ok());
        assert!(screen_sql("  vacuum  ").is_ok());
    }

    #[test]
    fn path_and_schema_pragmas_are_refused() {
        for sql in [
            "PRAGMA data_store_directory = '/tmp'",
            "PRAGMA temp_store_directory = '/tmp'",
            "PRAGMA writable_schema = ON",
            // Schema-qualified and quoted spellings are the same statement.
            "PRAGMA main.data_store_directory = '/tmp'",
            "PRAGMA \"writable_schema\" = ON",
            "PRAGMA `main`.`writable_schema` = ON",
            "PRAGMA [writable_schema] = ON",
            "PRAGMA main /* c */ . writable_schema = ON",
        ] {
            assert!(screen_sql(sql).is_err(), "must be refused: {sql:?}");
        }
    }

    #[test]
    fn tuning_pragmas_pass() {
        for sql in [
            "PRAGMA foreign_keys = ON",
            "PRAGMA journal_mode = WAL",
            "PRAGMA busy_timeout = 5000",
            "PRAGMA table_info('users')",
        ] {
            assert!(screen_sql(sql).is_ok(), "must pass: {sql:?}");
        }
    }

    #[test]
    fn explain_wrappers_do_not_hide_a_forbidden_verb() {
        for sql in [
            "EXPLAIN ATTACH DATABASE 'x.db' AS x",
            "EXPLAIN QUERY PLAN ATTACH DATABASE 'x.db' AS x",
            "explain /* c */ attach database 'x.db' as x",
            "EXPLAIN EXPLAIN ATTACH 'x.db' AS x",
        ] {
            assert!(screen_sql(sql).is_err(), "must be refused: {sql:?}");
        }
        assert!(screen_sql("EXPLAIN SELECT 1").is_ok());
        assert!(screen_sql("EXPLAIN QUERY PLAN SELECT 1").is_ok());
    }

    #[test]
    fn forbidden_statement_after_a_benign_one_is_refused() {
        assert!(screen_sql("SELECT 1; ATTACH DATABASE 'x.db' AS x").is_err());
        assert!(screen_sql("SELECT 1; SELECT 2").is_ok());
    }

    #[test]
    fn keywords_inside_literals_and_comments_are_data() {
        for sql in [
            "INSERT INTO t (v) VALUES ('ATTACH DATABASE evil')",
            "SELECT 'VACUUM INTO ''/tmp/x''' AS s",
            "SELECT 1 -- ATTACH DATABASE 'x' AS x",
            "SELECT /* ATTACH */ 1",
        ] {
            assert!(screen_sql(sql).is_ok(), "must pass: {sql:?}");
        }
    }

    #[test]
    fn malformed_sql_is_refused_conservatively() {
        assert!(screen_sql("SELECT 'unterminated").is_err());
        assert!(screen_sql("SELECT 1 /* unterminated").is_err());
    }

    // ── the wrapper, against the real rusqlite backend ─────────────────────

    #[cfg(feature = "rusqlite")]
    mod wrapped {
        use super::super::*;
        use crate::Rusqlite;

        async fn tenant_conn() -> Box<dyn BackendConn> {
            let backend = Rusqlite::memory().expect("open");
            screened(backend.connect().await.expect("connect"))
        }

        /// The cross-tenant primitive is refused by *this* layer, with
        /// litewire's own error -- not by whatever the engine happens to do.
        #[tokio::test]
        async fn attach_on_a_tenant_session_is_refused_with_our_error() {
            let conn = tenant_conn().await;
            let err = conn
                .execute("ATTACH DATABASE '/srv/tenants/other.db' AS stolen", &[])
                .await
                .expect_err("ATTACH must be refused");
            assert!(
                err.to_string()
                    .contains("not permitted on a tenant session"),
                "the refusal must be litewire's own: {err}"
            );
        }

        #[tokio::test]
        async fn detach_vacuum_into_and_path_pragmas_are_refused() {
            let conn = tenant_conn().await;
            for sql in [
                "DETACH DATABASE stolen",
                "VACUUM INTO '/tmp/copy.db'",
                "PRAGMA data_store_directory = '/tmp'",
                "PRAGMA main.writable_schema = ON",
                "EXPLAIN ATTACH DATABASE '/tmp/x.db' AS x",
            ] {
                let err = conn.execute(sql, &[]).await.expect_err("must be refused");
                assert!(
                    err.to_string().contains("not permitted"),
                    "expected litewire's refusal for {sql:?}, got: {err}"
                );
            }
        }

        /// `describe_columns` hands SQL to the engine's planner; it is
        /// screened like execution.
        #[tokio::test]
        async fn describe_columns_is_screened() {
            let conn = tenant_conn().await;
            assert!(
                conn.describe_columns("ATTACH DATABASE '/tmp/x.db' AS x")
                    .await
                    .is_err()
            );
        }

        /// Ordinary SQL is untouched -- a screen that broke real queries
        /// would just get turned off.
        #[tokio::test]
        async fn ordinary_sql_passes_through() {
            let conn = tenant_conn().await;
            conn.execute("CREATE TABLE t (v TEXT)", &[])
                .await
                .expect("create");
            conn.execute("INSERT INTO t (v) VALUES ('ATTACH DATABASE x')", &[])
                .await
                .expect("a keyword inside a literal is data, not a statement");
            let rs = conn.query("SELECT v FROM t", &[]).await.expect("select");
            assert_eq!(rs.rows.len(), 1);
        }

        /// The single-tenant contract: an *unscreened* session keeps full
        /// SQL freedom, including ATTACH. The screen keys off the session
        /// being tenant-scoped, never off the statement alone.
        #[tokio::test]
        async fn unscreened_sessions_keep_attach() {
            let dir = tempfile::tempdir().expect("tempdir");
            let main = dir.path().join("main.db");
            let other = dir.path().join("other.db");
            Rusqlite::open(&other)
                .expect("open other")
                .execute("CREATE TABLE t (v TEXT)", &[])
                .await
                .expect("create in other");

            let backend = Rusqlite::open(&main).expect("open main");
            let conn = backend.connect().await.expect("connect");
            conn.execute(
                &format!("ATTACH DATABASE '{}' AS other", other.display()),
                &[],
            )
            .await
            .expect("single-tenant ATTACH must keep working");
            let rs = conn
                .query("SELECT v FROM other.t", &[])
                .await
                .expect("cross-db select");
            assert_eq!(rs.rows.len(), 0);
        }

        /// `TenantScreened` as a backend decorator screens every session it
        /// opens.
        #[tokio::test]
        async fn tenant_screened_backend_screens_its_sessions() {
            let backend = TenantScreened::new(Rusqlite::memory().expect("open"));
            let conn = backend.connect().await.expect("connect");
            assert!(
                conn.execute("ATTACH DATABASE '/tmp/x.db' AS x", &[])
                    .await
                    .is_err()
            );
            conn.execute("CREATE TABLE t (a INTEGER)", &[])
                .await
                .expect("create");
        }
    }
}
