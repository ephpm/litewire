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

## Metadata emulation (`src/metadata.rs`, `src/catalog.rs`)

`SHOW` / `DESCRIBE` are parsed before the SQL parser runs and answered from
`sqlite_master` and the `pragma_table_info` / `pragma_index_list` /
`pragma_index_info` table-valued functions, in the column shape MySQL uses:

| Query | Result |
|-------|--------|
| `SHOW [FULL] TABLES [FROM db] [LIKE 'p' \| WHERE expr]` | `Tables_in_database` (+ `Table_type` with `FULL`); tables and views, `sqlite_*` excluded. A `WHERE` may name the column `Tables_in_<anything>` |
| `SHOW DATABASES` / `SHOW SCHEMAS` | synthetic single row (`main`). With `LIKE`/`WHERE`: **error** (litewire cannot see the name the client connected with) |
| `SHOW [FULL] COLUMNS\|FIELDS {FROM\|IN} t [{FROM\|IN} db] [LIKE 'p' \| WHERE expr]`, `DESCRIBE t [col\|'p']`, `DESC t` | `Field`, `Type`, `Null`, `Key` (`PRI`/`UNI`/`MUL`), `Default`, `Extra` (`auto_increment` for an `INTEGER PRIMARY KEY`); `FULL` adds `Collation`, `Privileges`, `Comment`. Affinities map to `bigint`/`double`/`longtext`/`longblob` |
| `SHOW {INDEX\|INDEXES\|KEYS} {FROM\|IN} t [{FROM\|IN} db] [WHERE expr]` | MySQL's 15 columns (`Table`, `Non_unique`, `Key_name`, `Seq_in_index`, `Column_name`, ...). `PRIMARY` first (synthesized from the table's primary key), then indexes in creation order |
| `SHOW CREATE TABLE t` | `Table`, `Create Table` (`sqlite_master.sql`) |
| `SELECT @@var, ...` (no `FROM`) | synthetic values, see below |
| `pg_catalog.pg_tables` / `pg_class` | `sqlite_master` |
| `pg_catalog.pg_attribute` | `pragma_table_info` |
| `sys.tables` / `sysobjects`, `sp_tables` | `sqlite_master` |
| `sys.columns` / `syscolumns`, `sp_columns t` | `pragma_table_info` |

**`LIKE` / `WHERE` filters are applied — or the statement fails.**
Self-migrating applications guard each schema change with a probe such as
`SHOW INDEX FROM t WHERE Key_name = 'x'` or `SHOW COLUMNS FROM t WHERE Field
= 'x'` and alter the table only when the probe returns no rows. litewire
once read only the table name and dropped the rest, so every probe returned
rows (every table has a `PRIMARY` index) and those migrations never ran. The
filter now goes through the normal MySQL translator (`LIKE` keeps MySQL's
implicit `\` escape) and runs against the emulated rows. Name columns
(`Field`, `Key_name`, `Column_name`, table names) compare
case-insensitively, as MySQL's catalog does. Anything not implemented — a
`SHOW` form not in the table (`SHOW VARIABLES`, `SHOW TABLE STATUS`, `SHOW
WARNINGS`, ...), `SHOW INDEX ... LIKE`, `ORDER BY`/`LIMIT` after a filter,
trailing tokens, a system schema (`FROM information_schema`, `mysql`, ...),
an unknown column in the filter — is an error, never an unfiltered result.
A `SHOW` on a table that does not exist returns no rows (MySQL errors with
1146).

**`information_schema`** is not pattern-matched. A reference to
`information_schema.tables`, `.columns`, `.statistics` or `.schemata`
anywhere in a statement (subqueries included) is replaced by a derived table
with MySQL's column names, and the client's own projection, `WHERE`,
`ORDER BY` and `LIMIT` run against it. Two adjustments make that faithful:

* litewire serves one database under whatever name the client connected
  with, which the translator never sees. A comparison of `table_schema` /
  `schema_name` / `index_schema` against any non-system name (`'shop'`,
  `DATABASE()`, a bound `?`) is therefore true, and against a system schema
  (`'mysql'`, `'information_schema'`, ...) false. The reported schema name
  is `main`, which is also what `DATABASE()` returns. A bound placeholder is
  kept (`(? IS NULL OR 1)`) so the parameter count does not change.
* A bare column in the projection is aliased as written, so
  `SELECT row_format FROM information_schema.tables` yields a column named
  `row_format` (PHP array keys are case-sensitive).

Other `information_schema` tables (`routines`, `key_column_usage`, ...) are
an error. `TABLES` reports `ENGINE = 'InnoDB'`, `ROW_FORMAT = 'Dynamic'`,
`TABLE_COLLATION = 'utf8mb4_unicode_ci'` and zero sizes.

Table names in the emulation SQL are always quoted SQLite string literals
(`'a''b'`), never spliced: a quoted identifier such as `` `my-table` ``
keeps its characters instead of being "sanitized" into another table's
name. The PostgreSQL/T-SQL shortcuts still use `sanitize_identifier`.

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
* `LOCK IN SHARE MODE` is removed (sqlparser cannot parse it; see locking
  reads below).
* `ALTER TABLE t <table options>` alone (`ENGINE`, `ROW_FORMAT`,
  `[DEFAULT] CHARSET|CHARACTER SET`, `COLLATE`, `AUTO_INCREMENT`, `COMMENT`,
  `CONVERT TO CHARACTER SET ...`, `ENABLE|DISABLE KEYS`, `FORCE`, the storage
  tuning options, `ALGORITHM=`/`LOCK=`) is a no-op: OK, nothing executed.
  Storage format, engine and character set (always UTF-8) are fixed in
  SQLite. `AUTO_INCREMENT=n` does not move the next id (still
  `max(rowid) + 1`). Mixed with a real change (`ADD COLUMN ...`) the
  statement is not matched and goes to the parser.

The pre-passes copy multi-byte UTF-8 text through unchanged and honour
MySQL's backslash escapes inside strings. An earlier version re-encoded each
byte, turning `'café'` in DDL or in a hinted `SELECT` into `'cafÃ©'`.

**Expressions:**

* `"double-quoted"` literals are strings in MySQL (no `ANSI_QUOTES`), so they
  are emitted single-quoted. SQLite reads `"x"` as an identifier first.
* `LEAST(a, b, ...)` / `GREATEST(...)` → multi-argument `min()` / `max()`
  (`NULL` if any argument is `NULL`, as in MySQL); `CHAR_LENGTH` /
  `CHARACTER_LENGTH` → `length` (characters).
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
* `... FROM DUAL` → the `FROM` is dropped (SQLite accepts `SELECT ... WHERE`
  without one). So `INSERT ... SELECT ... FROM DUAL WHERE NOT EXISTS (...)`
  ("insert unless present") works and reports 1 affected row when it inserts
  and 0 when the guard suppresses it. A quoted or aliased `dual` is a user
  table and is kept.
* Index hints (`FORCE|USE|IGNORE {INDEX|KEY} [FOR JOIN|ORDER BY|GROUP BY]
  (...)`) are removed and SQLite's planner chooses. They are never mapped to
  `INDEXED BY`, which fails when the index does not exist.
* `LIMIT n` with `n` ≥ 2^63 becomes `LIMIT -1` (SQLite's "no limit"). That
  covers MySQL's documented "no limit" value `18446744073709551615` in both
  spellings: `LIMIT 18446744073709551615 OFFSET m` and
  `LIMIT m, 18446744073709551615`.
* Locking reads (`FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE MODE`, `NOWAIT`,
  `SKIP LOCKED`) are removed and the rows returned. **Semantics differ**:
  SQLite has a single writer and no row locks. Inside `BEGIN` a locking read
  takes no lock, so two transactions can both read ("claim") the same row.
  The second to *write* then fails with a busy/snapshot error instead of
  blocking until the first commits. The row cannot be claimed twice, but code
  must treat that write error as "someone else got it" and roll back, where
  MySQL would have made it wait.

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

## Named locks: `GET_LOCK()` & co. (`src/user_locks.rs` + session + backend)

MySQL's user-level locks — `GET_LOCK(name, timeout)`, `RELEASE_LOCK(name)`,
`RELEASE_ALL_LOCKS()`, `IS_FREE_LOCK(name)`, `IS_USED_LOCK(name)` — and the
`DO <expr>` statement they are usually issued through. SQLite has nothing
like them; they are *session* operations (they wait, and their answer
depends on which connection asks), so they cannot be translated. Three
layers share the work:

* **Translate** (`user_locks::detect`): a statement that mentions a lock
  function (one `_LOCK` substring scan rejects everything else) is parsed,
  each call is pulled out with its literal or `?` arguments and replaced by
  a sentinel, and — once the session has the values — the sentinels become
  literals and the rest of the statement is translated as usual
  (`LockStatement::to_sqlite`). `SELECT GET_LOCK('job', 0) AS s, NOW()`
  runs as `SELECT 1 AS s, datetime('now')`. An unaliased call keeps MySQL's
  column name (`GET_LOCK('job', 0)`). `DO exprs` is `SELECT exprs` with the
  rows discarded.
* **Session** (`litewire-session`): validates and normalizes the name as
  MySQL does — `NULL`/empty → error 3057 `ER_USER_LOCK_WRONG_NAME`, over 64
  characters → 3938 `ER_USER_LOCK_OVERLONG_NAME`, otherwise **lowercased**
  (names are case-insensitive) — evaluates each call **once per statement,
  in source order** (not once per row), and binds the remaining `?`
  parameters to the SQLite statement. Prepared statements work
  (`SELECT GET_LOCK(?, ?)`); the lock calls are re-evaluated with the
  parameters bound at each execute.
* **Backend** (`litewire_backend::user_lock`): the registry of held locks,
  reached through `BackendConn::user_lock`. Locks are **exclusive across
  sessions, re-entrant within one** (a count per name), **released when the
  connection ends** — a clean `QUIT`, a TCP reset, a dropped task, all drop
  the connection's `LockSession`, whose `Drop` releases everything it holds
  — and untouched by `COMMIT`/`ROLLBACK`. `COM_RESET_CONNECTION` /
  `COM_CHANGE_USER` release them too (applied at the connection's next
  command, like the reset's rollback). A timed `GET_LOCK` parks on a
  per-lock `Notify` and is woken by the release, never polled; a negative
  timeout waits forever, as on MySQL. There is no deadlock detection.

Return values follow the MySQL manual: `GET_LOCK` 1/0 (NULL for a NULL
timeout); `RELEASE_LOCK` 1 released / 0 held by another session / NULL not
held; `RELEASE_ALL_LOCKS` the number of acquisitions released; `IS_FREE_LOCK`
1/0; `IS_USED_LOCK` the holder's session id or NULL. The session id is
litewire's own per-connection counter, not the `CONNECTION_ID()` constant.

**Scope is the embedder's choice, and refusing is the default.** A plain
`BackendConn` answers every lock operation with an error ("named locks …
are not available on this connection"), never a fake `1`: a lock that does
not actually exclude anything is the one answer a caller cannot defend
against. A backend gains locks by being wrapped — `UserLockScoped::new(
backend, &registry, namespace)` (or `scoped_conn` per connection) — and the
`namespace` is the isolation unit: one listener serving many tenants gives
each tenant its own namespace, so a name taken by one is invisible to the
others. The `litewire` CLI scopes its single database to the `""`
namespace. A backend that forwards statements elsewhere implements
`user_lock` by forwarding the operation, so the lock lives with the session
on the far side.

On the wire, the two name errors are reported as `ER_WRONG_ARGUMENTS`
(1210, `HY000`): `opensrv-mysql`'s error table stops at 1885 and panics on
codes it does not know. Embedders reading `SessionError::code()` directly
see 3057 / 3938.

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
* **`SELECT ... FOR UPDATE` row locking semantics**: accepted and stripped.
  SQLite's single-writer model applies, not MySQL's row locks (see above).

The wire-level test suites (`crates/litewire/tests/`) pin the constructs that
matter to WordPress and Laravel: CRUD, prepared statements, transactions,
`LAST_INSERT_ID()`, metadata probes, `SQL_CALC_FOUND_ROWS` pagination,
`REGEXP`, date-part functions, and MySQL error-code fidelity.
