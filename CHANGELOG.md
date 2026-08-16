# Changelog

All notable changes to litewire. Entries reference the pull request that
landed them. The format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased] — 0.2.0

Everything merged since the `v0.1.0` tag. Proposed release: tag `v0.2.0` on
the commit that merges this changelog.

### Changed

- Declared MSRV corrected from 1.85 to **1.88** and now verified by a CI job.
  1.85 was never true with the locked dependency graph: the Turso engine's
  `aristo` dependency requires rustc 1.88.

### Added

- Per-connection backend selection for multi-tenant embedders: a
  `ConnectionAuthenticator` runs during the MySQL handshake and binds each
  connection to its own backend (or rejects it); `serve()` refuses to start
  other frontends under an authenticator (#27)
- Public `Session` layer extracted from the MySQL handler — translation,
  transaction-state tracking, and MySQL error mapping usable in-process
  without a wire connection (#18)
- `SQL_CALC_FOUND_ROWS` / `SELECT FOUND_ROWS()` emulation at the session
  layer (WordPress pagination) (#26)
- MySQL `REGEXP`/`RLIKE` translation (with a `regexp()` function registered
  in the rusqlite backend) and `YEAR()`/`MONTH()`/`DAYOFMONTH()`/`DAY()` →
  `CAST(strftime(...) AS INTEGER)` (#29)
- Experimental Turso Database engine backend (`litewire-turso`, feature
  `turso`; engine pinned `=0.7.0`) (#9)
- Experimental CDC tail/apply API on the Turso backend for external
  replication layers (#10)
- `max_connections` cap on the wire frontends — over-limit MySQL clients are
  refused immediately with `ER_CON_COUNT_ERROR (1040)`; PG/TDS close before
  handshake (#15)
- Write admission control for the Hrana client (sqld) backend: a FIFO
  semaphore bounds concurrent writes; reads and deferred `BEGIN`s take no
  permit (#16)

### Fixed

- MySQL command packets `opensrv-mysql` cannot parse are no longer answered
  with a stray OK packet: `COM_STMT_RESET`/`COM_RESET_CONNECTION`/
  `COM_CHANGE_USER` get correct OK-shaped replies, everything else gets
  `ER_UNKNOWN_COM_ERROR (1047)` — fixes PHP persistent connections dying with
  "Packets out of order" (#28)
- One server version everywhere: handshake, `SELECT VERSION()`, and
  `@@version` all report `8.0.36-litewire` from a single constant (#30)
- Error mapping: SQLite "no such table" → MySQL 1146/42S02, and duplicate-key
  errors re-shaped into MySQL's `Duplicate entry ... for key ...` message
  form that drivers match on (#31)
- MySQL `LIKE` patterns get MySQL's implicit `ESCAPE '\'` on SQLite, so
  `esc_like()`-style escaped patterns match the same rows as on MySQL (#25)
- Plain `LIMIT n` / `LIMIT n OFFSET m` are no longer dropped from translated
  MySQL queries (#26)
- Turso CDC: replayed schema SQL is allowlisted and `commit_change_id` no
  longer panics (#17); an absent `turso_cdc` table reports as empty rather
  than an error (#14)
- Turso backend: parameter-count mismatches are rejected instead of executing
  unbound parameters as NULL — fixes `mysql` CLI ≥ 8.1 "commands out of
  sync" (#13)
- Metadata emulation sanitizes interpolated identifiers (SQL injection via
  `SHOW`/`DESCRIBE`/`INFORMATION_SCHEMA` table names) (#12)
- Framework wire-compat: WordPress and Laravel run over the MySQL frontend —
  `SHOW FULL COLUMNS` shaping, DDL display-width stripping, upsert
  `VALUES()` → `excluded`, `ALTER TABLE ADD KEY` expansion, and related
  gaps (#11)
- Untyped expression columns (e.g. `SELECT 1`) are typed by value instead of
  defaulting to text — fixes "MySQL server has gone away" with mysqlnd (#8)
- Per-wire-connection backend sessions: transactions are isolated per client
  instead of interleaving through one shared connection (#6)
- Session/transaction statement rewrites (`START TRANSACTION` variants,
  `BEGIN WORK`, named transactions) and real MySQL/PG error codes instead of
  catch-all failures (#4)

### Performance

- LRU translate cache shared across connections, `prepare_cached` on the
  rusqlite backend, and removal of the `LIMIT 0` describe probe (#5)
- Result-set responses coalesced into a single write; with `TCP_NODELAY` this
  removes a ~40 ms Nagle/delayed-ACK stall per query against PHP's mysqlnd
  (#3, #7)
- Rusqlite backend: dedicated per-session worker thread owning the SQLite
  connection (SQLite never runs on a tokio worker), with opt-in handle
  reuse (#15)

### CI

- Clippy and tests also run with `--all-features`, so feature-gated code
  (hrana-client, turso) is compiled and tested by CI (#16)

## [0.1.0] — 2026-05-03

Initial tagged state: workspace with the `LiteWire` builder and CLI; MySQL
(opensrv-mysql), PostgreSQL (pgwire), TDS (custom, experimental), and Hrana
HTTP (stateless subset) frontends; sqlparser-based MySQL/PG/T-SQL → SQLite
translation with metadata emulation; rusqlite backend; Hrana client (sqld)
backend; README accuracy fix (#1).
