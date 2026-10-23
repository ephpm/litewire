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
* `SET time_zone = ...`; `SET GLOBAL sql_mode = ...` (a session `SET sql_mode`
  is recorded by the session, see "sql_mode" below)
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
handshake and `SELECT VERSION()` use — issue #21), `sql_mode` (see below),
`autocommit` 1, `transaction_isolation` `SERIALIZABLE`, `@@identity` →
`last_insert_rowid()`, `@@rowcount` → `changes()`. Unknown variables return
`NULL`. A query qualifies only when every select item is a bare
`@@[GLOBAL.|SESSION.|LOCAL.]name [[AS] alias]` (optionally followed by
`LIMIT n`); each result column is named as written (`@@SESSION.sql_mode`) or
by its alias, as MySQL names it. A select list mixing in anything else goes
to the parser instead of having the rest dropped.

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
  `COLLATE=...`, `AUTO_INCREMENT=n`, `ROW_FORMAT=...`, `COMMENT=...`.
* Column options SQLite rejects are dropped: `CHARACTER SET x`, `COLLATE x`,
  `COMMENT '...'`, `ON UPDATE CURRENT_TIMESTAMP`. Two of these are
  **behaviour differences**. Text compares with SQLite's binary collation, so
  equality, `UNIQUE` keys and `ORDER BY` are **case-sensitive**, where
  MySQL's `_ci` collations (its default) are not (`LIKE` is
  case-insensitive for ASCII on both). And a column declared `ON UPDATE
  CURRENT_TIMESTAMP` is not refreshed automatically.
* **Indexes keep their MySQL names, scoped to their table.** MySQL index
  names are per table and SQLite's are per database, and dumps routinely
  reuse a key name across tables. So index `k` on table `t` is created in
  SQLite as `` `t__k` `` (`mysql::sqlite_index_name`). `SHOW INDEX`,
  `information_schema.statistics` and `SHOW COLUMNS`' `Key` report the MySQL
  name, and `DROP INDEX k ON t` / `ALTER TABLE t DROP {INDEX|KEY} k` drop
  `t__k`. An index created under its bare name by an older litewire is
  reported unchanged. Dropping it with MySQL syntax still works: on "no such
  index" the session drops the one index of `t` named `k`.
  * Inline `KEY [name] (cols)`, `INDEX ...` and `UNIQUE KEY [name] (cols)`
    in `CREATE TABLE` become `CREATE [UNIQUE] INDEX` statements after the
    table (`IF NOT EXISTS` when the table is `IF NOT EXISTS`, so an existing
    table can gain an index MySQL would not have added). An unnamed key gets
    MySQL's default name: its first column, then `_2`, `_3`. `FULLTEXT` /
    `SPATIAL` keys have no SQLite analogue and are dropped. Index prefix
    lengths (`name(20)`) are dropped, so the whole value is indexed.
  * `ALTER TABLE ... ADD {KEY|INDEX|UNIQUE} [name] (cols)` expands the same
    way; `DROP {INDEX|KEY} name` becomes `DROP INDEX`; other ALTER
    operations remain in a residual `ALTER TABLE`.
  * An inline `UNIQUE` column constraint keeps SQLite's automatic index and
    is reported under its first column's name, as MySQL names it.
* Charset introducers are dropped: `_utf8mb4'x'` → `'x'`. `_binary 'abc'`
  → `X'616263'`, a BLOB.

### Restoring a mysqldump

A `mysqldump` from MySQL 8.4 restores statement by statement, through the
`mysql` client or any MySQL driver. `crates/litewire/tests/mysql_dump_restore.rs`
replays an unedited dump on both backends and checks schema, keys, data and
key enforcement. What happens to each part of a dump:

* `/*!NNNNN ... */` conditional comments (the `SET @OLD_...`, `SET NAMES`,
  `SET TIME_ZONE`, `DISABLE KEYS` / `ENABLE KEYS` wrappers) are comments to
  the parser and are skipped. **Views, triggers and routines that mysqldump
  wraps in `/*!50001 ... */` / `/*!50003 ... */` are skipped too** and must
  be recreated by hand.
* `LOCK TABLES` / `UNLOCK TABLES` are no-ops, and `DROP TABLE IF EXISTS` runs.
* `CREATE TABLE` translates as above: types to affinities, keys to named
  indexes, MySQL-only options dropped.
* Extended INSERTs of any size up to the reported `max_allowed_packet` (64 MiB)
  are accepted; string escapes (`\'`, `\"`, `\`, `\n`, `\t`) are decoded as
  MySQL does.
* **Use `mysqldump --hex-blob`** when the dump has BLOB/BINARY data. Without
  it, binary values are written as raw bytes inside string literals, the
  statement is not valid UTF-8, and the MySQL frontend cannot accept it.
  With `--hex-blob` they are `0x...` literals and restore byte-exact.
* `AUTO_INCREMENT` continues after the highest restored id.

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

## `sql_mode` (`metadata.rs` + session)

**Reporting.** `@@sql_mode` / `@@SESSION.sql_mode` is `''` until the
connection runs `SET [SESSION|LOCAL] sql_mode = ...`, and from then on it is
the value the connection set. Literals are normalized as MySQL displays them:
upper-case, no spaces around commas. Combination modes such as `TRADITIONAL`
are not expanded. `DEFAULT` resets the value. Any other value expression is
evaluated with the current mode substituted for `@@sql_mode`, so the idiom
`SET sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))`
works. Bound parameters (`SET sql_mode = ?`) work too. `@@GLOBAL.sql_mode` is
always `''`, and `SET GLOBAL|PERSIST sql_mode` changes nothing on the
connection, as on MySQL.

**Enforcement: none.** The mode is recorded so applications can read back
what they set. litewire does not change behaviour by mode, and it logs a
warning when a session sets a non-empty one. The default `''` was chosen
because none of the modes in MySQL 8.4's default
(`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`)
is enforced:

* **`ONLY_FULL_GROUP_BY`**: SQLite accepts bare columns next to `GROUP BY`,
  as MySQL does without this mode. The value comes from one row of the
  group. Both engines take it from the first row in scan order, so
  `SELECT list, email FROM s GROUP BY list` returns the same row on every
  run. But the row is the first *visited*, which depends on the plan (an
  index can change it), just as MySQL calls it indeterminate. **With
  `MIN()`/`MAX()` in the same select the engines differ.** The rusqlite
  (SQLite) backend takes bare columns from the row holding the min/max (a
  documented SQLite extension). Turso does not: it stays on the first row.
  Don't rely on either. Verified on Turso 0.8.3 and on SQLite through
  rusqlite (`crates/litewire/tests/mysql_sql_mode.rs` pins the parts that
  are deterministic).
* **`STRICT_TRANS_TABLES`**: litewire neither rejects nor coerces
  ill-typed values. In particular an **empty string written to an INTEGER
  column stays an empty string**. MySQL 8.4 strict rejects it (error 1366).
  MySQL non-strict (`sql_mode = ''`, which many PHP apps set) stores `0`
  with a warning. On litewire:

  | Expression on a row with `n = ''` | MySQL non-strict (`n` is 0) | litewire |
  |---|---|---|
  | `typeof(n)` | n/a | `text` |
  | `n = 0` | true | **false** |
  | `n > 5` | false | **true** (text sorts after numbers) |
  | `n + 1`, `SUM(n)` | 1, counts as 0 | 1, counts as 0 |
  | `ORDER BY n` | among the zeros | **after every number** |
  | `MAX(n)` | numeric | **`''`/text wins** |

  `'12abc'` likewise stays text where MySQL non-strict stores 12 (`n + 1` is
  13 on both). Whitespace-padded numbers (`' 7 '`) are converted by SQLite's
  INTEGER affinity, as on MySQL.

  **Decision: no coercion in litewire for now.** Translation is stateless
  and does not know a column's declared type, so it cannot tell `''` bound
  for an INTEGER column from `''` bound for a TEXT one. Coercing would take
  a schema-aware session layer: cache `pragma_table_info` per table,
  invalidate it on DDL, and wrap literals and bound parameters assigned to
  INTEGER-affinity columns in `CAST(... AS INTEGER)`, which yields exactly
  MySQL's non-strict values (`''` → 0, `'12abc'` → 12). That is tracked as
  a follow-up. Applications that are clean under MySQL's strict default are
  unaffected. Ones that write `''` into integer columns after
  `SET sql_mode = ''` see the differences in the table above.
* **`NO_ZERO_DATE` / `NO_ZERO_IN_DATE`**: dates are text, and any value is
  stored.
* **`ERROR_FOR_DIVISION_BY_ZERO`**: division by zero yields `NULL`, as in
  MySQL with the mode off.

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
