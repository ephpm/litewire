# SQL Translation Reference

What SQL litewire accepts, how it is rewritten for SQLite, what is emulated
at the session layer, and what is rejected. Everything here is implemented in
`crates/litewire-translate` (rewrites and metadata emulation) and
`crates/litewire-session` (session-state emulation and error mapping); file
references are given per section.

## The pipeline

`litewire_translate::translate(sql, dialect)` (in `src/lib.rs`) processes a
statement in this order — the first stage that matches wins:

1. **Metadata queries** are detected textually (some are not parseable SQL)
   and answered by emulation SQL against `sqlite_master` / `PRAGMA`.
2. **Transaction-control statements** are rewritten textually.
3. **No-op statements** return OK without touching the database.
4. **MySQL pre-passes** strip constructs sqlparser cannot parse.
5. Everything else is **parsed** (sqlparser 0.57, dialect-matched), the AST is
   **rewritten** (shared rewrites, then dialect-specific ones), and the result
   is **emitted** as SQL text via sqlparser's `Display`.

A statement that fails to parse, or contains a construct sqlparser rejects,
returns `TranslateError` — surfaced to MySQL clients as `ER_PARSE_ERROR`
(1064, SQLSTATE 42000). There is no silent fallback to passthrough.

Translation results are cached in a bounded LRU (`src/cache.rs`,
`translate_cached`); errors are not cached.

## Transaction control (`lib.rs::rewrite_transaction_statement`)

| Input | Becomes |
|-------|---------|
| `START TRANSACTION` (incl. `READ ONLY`, `READ WRITE`, `WITH CONSISTENT SNAPSHOT`) | `BEGIN` |
| `BEGIN WORK`, `BEGIN TRANSACTION [name]` | `BEGIN` (T-SQL transaction names are stripped) |
| `COMMIT WORK`, `COMMIT TRANSACTION [name]` | `COMMIT` |
| `ROLLBACK WORK`, `ROLLBACK TRANSACTION [name]` | `ROLLBACK` |
| `ROLLBACK TRANSACTION TO [SAVEPOINT] name` (T-SQL) | `ROLLBACK TO SAVEPOINT name` |
| `ROLLBACK TO [SAVEPOINT] name`, `SAVEPOINT name`, `RELEASE SAVEPOINT name` | passed through to SQLite |

The session layer (`litewire-session`) tracks explicit-transaction state from
these statements and reports it in the MySQL `SERVER_STATUS_IN_TRANS` flag.

## No-ops (`lib.rs::is_noop`)

Answered with OK, never executed:

* `SET NAMES ...`, `SET CHARACTER SET ...`
* `SET time_zone = ...`, `SET sql_mode = ...`
* `SET [SESSION|GLOBAL] <anything>` (e.g. `wait_timeout`), including the
  `@@session.` / `@@global.` / `@@` spellings
* `SET [SESSION|GLOBAL] TRANSACTION ISOLATION LEVEL ...` (SQLite runs
  serializable)
* `SET autocommit = ...` — **`autocommit=0` is not emulated**: litewire logs a
  warning and statements still auto-commit unless wrapped in
  `BEGIN`/`COMMIT`. `autocommit=1` matches SQLite's default.
* T-SQL: `SET NOCOUNT`, `SET ANSI_NULLS`, `SET QUOTED_IDENTIFIER`,
  `SET XACT_ABORT`
* `LOCK TABLES ...` / `UNLOCK TABLES` — no-op **with a warning**: the
  requested locking semantics are not enforced (SQLite provides file-level
  locking only).

## Metadata emulation (`src/metadata.rs`)

Detected before parsing and answered from `sqlite_master` / `PRAGMA`:

| Query | Emulated with |
|-------|---------------|
| `SHOW TABLES` | `sqlite_master` (type='table', `sqlite_%` excluded) |
| `SHOW DATABASES` | synthetic single row (`main`) |
| `SHOW [FULL] COLUMNS/FIELDS FROM t`, `DESCRIBE t`, `DESC t` | `pragma_table_info`, shaped like MySQL `SHOW FULL COLUMNS` (Field/Type/Null/Key/Default/Extra/Collation/Privileges/Comment; affinities mapped to `bigint`/`double`/`longtext`/`longblob`) |
| `SHOW CREATE TABLE t` | `sqlite_master.sql` |
| `SHOW INDEX/INDEXES/KEYS FROM t` | `PRAGMA index_list` |
| `SELECT EXISTS (... information_schema.tables WHERE table_name = 't')` | single-scalar existence probe (Laravel `Schema::hasTable`) |
| `SELECT ... FROM information_schema.tables` | `sqlite_master` (honors a `TABLE_SCHEMA = ...` filter; only `main`/`def` match) |
| `SELECT ... FROM information_schema.columns` | `pragma_table_info` when a `TABLE_NAME = ...` filter is present; table list otherwise |
| `SELECT ... FROM information_schema.schemata` | synthetic `main` row |
| `SELECT @@var, ...` (no `FROM`) | synthetic values, see below |
| `pg_catalog.pg_tables` / `pg_class` | `sqlite_master` |
| `pg_catalog.pg_attribute` | `pragma_table_info` |
| `sys.tables` / `sysobjects`, `sp_tables` | `sqlite_master` |
| `sys.columns` / `syscolumns`, `sp_columns t` | `pragma_table_info` |

Table and schema names extracted from these queries are sanitized to
`[A-Za-z0-9_.]` before being interpolated into emulation SQL
(`sanitize_identifier`), closing the injection hole fixed in PR #12.

**System variables** (`system_variable_value`) return MySQL-plausible
constants: `max_allowed_packet` 64 MiB, `wait_timeout`/`interactive_timeout`
28800, the `character_set_*`/`collation_*` family as utf8mb4,
`version` = `8.0.36-litewire` (the same `SERVER_VERSION` constant the
handshake and `SELECT VERSION()` use — issue #21), `sql_mode` empty,
`autocommit` 1, `transaction_isolation` `SERIALIZABLE`, `@@identity` →
`last_insert_rowid()`, `@@rowcount` → `changes()`. Unknown variables return
`NULL`.

## Shared expression rewrites (`src/common.rs`)

Applied for every dialect:

| Input | Becomes |
|-------|---------|
| `NOW()`, `CURRENT_TIMESTAMP()`, `GETDATE()`, `GETUTCDATE()` | `datetime('now')` |
| `CURDATE()`, `CURRENT_DATE()` | `date('now')` |
| `UNIX_TIMESTAMP()` | `strftime('%s', 'now')` |
| `ISNULL(a, b)` | `IFNULL(a, b)` |
| `LAST_INSERT_ID()` | `last_insert_rowid()` (the 2-arg MySQL form is left to fail loudly) |
| `ROW_COUNT()` | `changes()` |
| `DATABASE()`, `SCHEMA()` | constant `'main'` |
| `VERSION()` | constant `'8.0.36-litewire'` (`SERVER_VERSION`) |
| `USER()`, `CURRENT_USER()`, `SESSION_USER()`, `SYSTEM_USER()` | constant `'root@localhost'` |
| `CONNECTION_ID()` | constant `0` |
| `FOUND_ROWS()` *embedded in a larger expression* | constant `0` (the bare `SELECT FOUND_ROWS()` is emulated statefully — see below) |
| `NEWID()` | `lower(hex(randomblob(16)))` |
| `TRUE` / `FALSE` | `1` / `0` |
| `$1`, `$2`, ... (PG placeholders) | `?1`, `?2`, ... |
| `@@IDENTITY` | `last_insert_rowid()` |
| `@@ROWCOUNT` | `changes()` |

## MySQL dialect (`src/mysql.rs`)

**Pre-passes** (quote-aware text transforms, before parsing):

* DDL display widths and index prefix lengths are stripped: `bigint(20)` →
  `bigint`, `KEY meta_key (meta_key(191))` → `KEY meta_key (meta_key)`.
  Applied to `CREATE [TEMPORARY] TABLE`, `ALTER TABLE`,
  `CREATE [UNIQUE|FULLTEXT] INDEX` only; `decimal(10,2)` is untouched.
* SELECT hints are stripped: `SQL_CALC_FOUND_ROWS` (see below),
  `SQL_NO_CACHE`, `SQL_CACHE`, `SQL_SMALL_RESULT`, `SQL_BIG_RESULT`,
  `SQL_BUFFER_RESULT`.

**Expressions:**

* `YEAR(x)` / `MONTH(x)` / `DAYOFMONTH(x)` / `DAY(x)` →
  `CAST(strftime('%Y'|'%m'|'%d', x) AS INTEGER)`. The cast matters: MySQL
  returns integers and `'03' = 3` is false in SQLite. Deliberately absent:
  `DAYOFWEEK`, `DAYOFYEAR`, `WEEK` — their MySQL numbering does not line up
  with `strftime`'s and they are left to fail rather than be silently wrong.
* `x RLIKE p` → `x REGEXP p` (`NOT` variants preserved). SQLite parses
  `REGEXP` but ships no implementation; the rusqlite backend registers a
  `regexp()` function backed by the Rust `regex` crate, and the Turso engine
  has a built-in using the same crate. Both are case-sensitive, where MySQL
  follows the column collation.
* `LIKE` / `NOT LIKE` without an explicit `ESCAPE` gets `ESCAPE '\'` — MySQL's
  always-on implicit escape. Without it, `wpdb::esc_like()`-style patterns
  (`'50\%'`) match the wrong rows on SQLite, which has *no* default escape
  character. An explicit `ESCAPE` clause in the input is preserved.

**DML:**

* `INSERT ... ON DUPLICATE KEY UPDATE ...` → `INSERT ... ON CONFLICT DO
  UPDATE ...`, with `VALUES(col)` in the update list rewritten to
  `excluded.col` (WordPress `add_option`/`update_option` shape).
* `LIMIT offset, count` → `LIMIT count OFFSET offset`. Plain `LIMIT n` and
  `LIMIT n OFFSET m` are preserved (regression-pinned: an earlier version
  dropped them).
* `UPDATE t SET t.col = ...` → table qualifier stripped from the assignment
  target (Laravel qualifies `updated_at`).

**DDL:**

* Column types mapped to SQLite affinities:
  * `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT` (incl. `UNSIGNED`) → `INTEGER`
  * `FLOAT`/`DOUBLE`/`DECIMAL`/`NUMERIC` → `REAL`
  * `VARCHAR`/`CHAR`/`TEXT`/`TINYTEXT`/`MEDIUMTEXT`/`LONGTEXT`/`ENUM`/`SET` → `TEXT`
  * `BINARY`/`VARBINARY`/`BLOB`/`TINYBLOB`/`MEDIUMBLOB`/`LONGBLOB` → `BLOB`
  * `BOOLEAN` → `INTEGER`; `DATE`/`DATETIME`/`TIMESTAMP`/`TIME` → `TEXT`;
    `JSON` → `TEXT`
* `AUTO_INCREMENT` column option removed (rowid aliasing via
  `INTEGER PRIMARY KEY` provides the behavior).
* Table options stripped entirely: `ENGINE=...`, `DEFAULT CHARSET=...`,
  `COLLATE=...`.
* Inline `KEY name (cols)` and `FULLTEXT`/`SPATIAL` constraints dropped
  (secondary indexes are a performance concern, not correctness);
  `UNIQUE KEY name (cols)` normalized to bare `UNIQUE (cols)` (upserts
  target these, so they must survive).
* `ALTER TABLE ... ADD {KEY|INDEX|UNIQUE} name (cols)` expands into
  standalone `CREATE [UNIQUE] INDEX` statements (`expand_alter_table`);
  `ADD FULLTEXT/SPATIAL` is dropped; other ALTER operations remain in a
  residual `ALTER TABLE`.

## `SQL_CALC_FOUND_ROWS` / `FOUND_ROWS()` (`src/found_rows.rs` + session)

WordPress pagination relies on this pair. Stateless translation cannot emulate
it (the value is session state), so the **session layer** does:

* A `SELECT` carrying `SQL_CALC_FOUND_ROWS` runs with the hint stripped; the
  session then derives and runs the matching `COUNT(*)` query (same WHERE,
  no LIMIT/OFFSET) and stores the total.
* A subsequent bare `SELECT FOUND_ROWS()` is answered from that stored state
  without touching the backend.
* A `FOUND_ROWS()` call embedded in a larger expression (not the bare SELECT
  shape) falls back to the constant `0` shim from `common.rs`.

## PostgreSQL dialect (`src/postgres.rs`)

`CREATE TABLE` type mappings: `SMALLINT`/`INT`/`BIGINT` → `INTEGER`;
`REAL`/`FLOAT`/`DOUBLE`/`NUMERIC`/`DECIMAL` → `REAL`;
`VARCHAR`/`CHAR`/`TEXT`/`UUID` → `TEXT`; `BYTEA` → `BLOB`; `BOOLEAN` →
`INTEGER`; `DATE`/`TIMESTAMP`/`TIME`/`INTERVAL` → `TEXT`; `JSON`/`JSONB` →
`TEXT`; `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` → `INTEGER`.

Plus the shared rewrites above (notably `$N` → `?N` placeholders).
`::type` casts are handled by sqlparser's PostgreSQL dialect parsing into
standard `CAST` AST nodes.

## T-SQL dialect (`src/tds.rs`)

* `SELECT TOP n ...` / `TOP(n)` → `LIMIT n` (when no LIMIT already present).
* `IDENTITY(...)` column options removed.
* Type mappings: `TINYINT`/`SMALLINT`/`INT`/`BIGINT` → `INTEGER`;
  `FLOAT`/`DOUBLE`/`DECIMAL`/`NUMERIC`/`MONEY`/`SMALLMONEY` → `REAL`;
  `VARCHAR`/`CHAR`/`NVARCHAR`/`TEXT`/`UNIQUEIDENTIFIER` → `TEXT`;
  `BINARY`/`VARBINARY`/`IMAGE` → `BLOB`; `BIT`/`BOOLEAN` → `INTEGER`;
  date/time types → `TEXT`.
* Plus the shared rewrites (`GETDATE()`, `NEWID()`, `ISNULL()`,
  `@@IDENTITY`, `@@ROWCOUNT`, T-SQL `SET` no-ops, named-transaction
  normalization).

## Error mapping (`litewire-session/src/error_map.rs`)

Backend (SQLite) errors are classified by message text into MySQL error
triples:

| SQLite condition | MySQL code | SQLSTATE |
|------------------|-----------|----------|
| `UNIQUE` / `PRIMARY KEY constraint failed` | 1062 `ER_DUP_ENTRY` — message re-shaped to MySQL's `Duplicate entry '<unknown>' for key '...'` form (drivers match the text; the original SQLite text is kept in parentheses) | 23000 |
| `no such table` | 1146 `ER_NO_SUCH_TABLE` | 42S02 |
| `FOREIGN KEY constraint failed` | 1452 `ER_NO_REFERENCED_ROW_2` | 23000 |
| `database is locked` / `SQLITE_BUSY` / `SQLITE_LOCKED` | 1205 `ER_LOCK_WAIT_TIMEOUT` | HY000 |
| readonly database | 1290 `ER_OPTION_PREVENTS_STATEMENT` | HY000 |
| untranslatable / unparseable SQL | 1064 `ER_PARSE_ERROR` | 42000 |
| anything else | 1105 `ER_UNKNOWN_ERROR` (message forwarded verbatim) | HY000 |

## What does not work

* **Stored procedures, triggers with MySQL syntax, events** — no translation
  exists; statements fail with a parse error.
* **`SET autocommit = 0`** — not emulated (warning logged; statements still
  auto-commit outside explicit transactions).
* **`LOCK TABLES` semantics** — accepted as a no-op; not enforced.
* **`DAYOFWEEK` / `DAYOFYEAR` / `WEEK`** and other date functions whose
  numbering needs arithmetic — not translated.
* **MySQL-only functions with no rewrite** (e.g. `DATE_ADD`, `DATE_SUB`,
  `INTERVAL` arithmetic, `GROUP_CONCAT` MySQL extensions) — passed to SQLite
  as-is if they parse, where SQLite rejects unknown functions at prepare
  time; there is no silent substitution.
* **Dollar-quoted PL/pgSQL bodies**, PG server-side features — fail to parse.
* **`SELECT ... FOR UPDATE` row locking semantics** — SQLite's locking model
  applies, not MySQL's.

The wire-level test suites (`crates/litewire/tests/`) pin the constructs that
matter to WordPress and Laravel: CRUD, prepared statements, transactions,
`LAST_INSERT_ID()`, metadata probes, `SQL_CALC_FOUND_ROWS` pagination,
`REGEXP`, date-part functions, and MySQL error-code fidelity.
