# Changelog

All notable changes to litewire. Entries reference the pull request that
landed them. The format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased] — 0.2.0

Everything merged since the `v0.1.0` tag. Proposed release: tag `v0.2.0` on
the commit that merges this changelog.

### Changed

- The MySQL frontend writes a session error's **own** code to the wire
  instead of re-classifying its message. Same codes as before for every
  existing error; a duplicate-key message is now shaped once
  (`Duplicate entry '<unknown>' for key 'users.email' (…)`) rather than
  wrapped twice. (#54)
- Turso engine bumped `=0.8.2` → `=0.8.3` (ephpm#573). No litewire API
  change; file format and the `turso_cdc` log schema are unchanged. 0.8.3
  refuses `PRAGMA journal_mode = 'mvcc'` inside a transaction. 0.8.2 accepted
  it, and once the transaction ended the database file could not be opened
  again ("Missing MVCC metadata table while logical log state exists"), so
  four statements from a client lost the whole database. Pinned by
  `crates/litewire-turso/tests/journal_mode_switch.rs`.
- Turso engine bumped `=0.7.2` → `=0.8.2` (ephpm#535). No litewire API
  change. Database files are compatible in both directions (0.7.2 files,
  including one with an uncheckpointed WAL, open under 0.8.2 and the reverse;
  the `turso_cdc` log schema stays "v2"). Behaviour changes from upstream:
  `PRAGMA journal_mode` changes are now refused on a connection with CDC
  capture on (previously accepted, after which capture silently stopped), and
  the `turso` binding's `execute_batch` now fails instead of running while a
  result set from the same connection is still open.
- Declared MSRV corrected from 1.85 to **1.88** and now verified by a CI job.
  1.85 was never true with the locked dependency graph: the Turso engine's
  `aristo` dependency requires rustc 1.88.

### Added

- MySQL named locks — `GET_LOCK()`, `RELEASE_LOCK()`, `RELEASE_ALL_LOCKS()`,
  `IS_FREE_LOCK()`, `IS_USED_LOCK()` — and the `DO <expr>` statement, with
  MySQL 8 semantics: exclusive across connections, re-entrant within one
  (counted; `RELEASE_LOCK` decrements, `RELEASE_ALL_LOCKS` returns the
  acquisitions released), case-insensitive names of at most 64 characters
  (3057 / 3938 on bad names, `ER_WRONG_ARGUMENTS` on the wire), a
  wakeup-driven timed wait (negative = forever), and **release on
  disconnect however the connection ends** (clean `QUIT`, TCP reset,
  dropped task, `COM_RESET_CONNECTION`) — a `LockSession` dies with the
  connection. Calls are evaluated once per statement, in source order, and
  substituted as literals, so they work with aliases, alongside other
  columns, and as prepared statements with `?` arguments. Scope is explicit:
  `BackendConn::user_lock` refuses by default and a backend gets locks by
  being wrapped in `UserLockScoped` (or `scoped_conn`) with a namespace —
  one per tenant under an authenticator, so tenants never see each other's
  locks; a forwarding backend implements the method itself. The CLI scopes
  its database automatically. `Session` gains `may_hold_user_locks` /
  `release_user_locks` for embedders that borrow a session across request
  boundaries, and `Prepared::param_count`. `docs/sql-translation.md` has the
  full semantics. (#54)
- WAL checkpoint gate for the Turso backend
  (`TursoBuilder::wal_checkpoint_gate`, module `litewire_turso::wal_gate`;
  off by default). Under constant read load the engine never rewinds its
  write-ahead log: the passive auto-checkpoint backfills frames, but a
  `RESTART`/`TRUNCATE` checkpoint needs every read-mark slot free and Turso
  0.8.3 reports `busy` the instant one is held instead of waiting, so some
  reader always wins and the `-wal` file grows by a frame per write — 1.3 GB
  in eight minutes on a 0.7 MB database, multi-GB on a clustered owner whose
  CDC tailers are continuous readers (ephpm WAL-growth stress runs). The gate
  gives every session statement, and every `CdcTailer` poll, the shared side
  of a per-file lock; once the file crosses a threshold (default 64 MiB) a
  maintenance task backfills the log with `PRAGMA wal_checkpoint(PASSIVE)`
  rounds (outside the lock; blocks nobody), then takes the exclusive side
  with a bounded wait (default 250 ms, new statements queue behind it) and
  runs `PRAGMA wal_checkpoint(TRUNCATE)`. An open transaction holds the shared
  side until it ends, so the attempt times out and retries with backoff
  rather than blocking it; the exclusive window is the checkpoint itself
  (milliseconds on a backfilled log). One gate per canonical path, so several
  factories on one file share it; the task holds the engine weakly and stops
  when its factory drops. `Turso::wal_gate_stats` exposes counters and
  `Turso::wal_gate_permit` lets raw-connection callers opt in.
- WAL checkpoint gate escalation: statements that routinely outlast
  `max_wait` no longer starve the checkpoint. Under 48 clients running
  `COUNT`/`GROUP BY`/join reads that took longer than 250 ms, every exclusive
  attempt timed out, the backoff stretched attempts to about one a minute,
  truncations stopped, the `-wal` file reached 1.2 GiB and p99 latency went
  from 241 ms to 1,045 ms (ephpm WAL-gate stress run). Once the file is at
  least `ESCALATION_THRESHOLD_MULTIPLE` (4) × `threshold_bytes`, a timed-out
  attempt retries on the next tick with double the wait, up to the new
  `WalGateConfig::max_escalated_wait` (default 2 s), and only backs off
  once a wait at that ceiling times out. The escalation resets after a
  truncate or when the file drops back under 4× the threshold; below that
  point the schedule is unchanged. New statements queue for as long as an
  escalated attempt waits, so the ceiling is the worst-case stall it adds.
  Setting `max_escalated_wait` to `max_wait` or lower disables escalation.
  `WalGateStats` gains `escalation_level`, `consecutive_timeouts` and
  `next_wait_micros`. `WalGateConfig` has a new public field, so code that
  builds it with a struct literal must add the field or use
  `..WalGateConfig::default()`.
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

- `SHOW ... LIKE` / `SHOW ... WHERE` filters were silently dropped (ephpm
  Sendy compatibility). Detection read only the table name, so
  `SHOW INDEX FROM t WHERE Key_name = 'x'`, `SHOW COLUMNS FROM t WHERE
  Field = 'x'` and `SHOW TABLES LIKE 'x'` returned every row; the probes
  self-migrating applications use to decide whether to add an index or a
  column always "found" it, and the migration never ran. The filter is now
  translated and applied; unsupported `SHOW` forms, clauses and trailing
  tokens are errors. `SHOW INDEX` now returns MySQL's columns (`Key_name`,
  `Column_name`, `Non_unique`, `Seq_in_index`, ... with a synthesized
  `PRIMARY`) instead of `PRAGMA index_list`'s; `SHOW COLUMNS` reports
  `UNI`/`MUL` keys and `auto_increment`, and returns MySQL's six columns
  unless `FULL` is given. `SHOW CREATE TABLE` gains the `Table` column.
- `information_schema` queries ignored every predicate except one
  `TABLE_SCHEMA`/`TABLE_NAME` literal and ignored the projection
  (`SELECT 1 FROM information_schema.tables WHERE table_name = 'missing'`
  returned every table). The `information_schema` table is now replaced by a
  derived table inside the client's own statement, so its `WHERE`,
  projection and `LIMIT` apply. `statistics` is newly emulated; other
  `information_schema` tables are an error.
- MySQL double-quoted string literals (`WHERE status != ""`) were emitted
  double-quoted, which SQLite reads as an identifier first.
- The Turso backend no longer runs engine work on tokio's async worker
  threads (#52). The engine's `async` API runs each step's page I/O and the
  commit `fsync` synchronously inside `poll`, yielding only between steps, so
  every session statement, `Turso::build`, `CdcTailer::poll_batch`,
  `apply_batch`/`read_watermark`/`enable_cdc` and the WAL gate's checkpoints
  held a worker for each step as long as the disk took; with enough of them
  in flight no worker was left to turn the runtime's I/O and timer drivers,
  and every other task on the runtime stalled (ePHPm measured its `SIGTERM`
  handler running up to 53 s late under replication load). They now run
  through the new `litewire_turso::offload::engine`, which uses
  `block_in_place` on a multi-thread runtime and awaits in place on a
  current-thread runtime. Embedders running statements on a
  `Turso::raw_connection` should use it too. Nested calls run in place, so
  an embedder that offloads a whole `select!` keeps its interleaving. An
  offloaded scope cannot be cancelled from outside (an enclosing `timeout`
  fires only after it ends), and a busy wait inside one is a hot spin that
  pins its thread for up to the busy timeout; both are documented on
  `offload`.
- Session statements taking 1 s or longer are logged at WARN with the split
  between waiting (session mutex + WAL-gate permit) and engine time, and only
  the statement's leading keyword.
- `CdcTailer::poll_batch` now ships a transaction that changes more rows than
  one 1024-row read window. Before, a full window with no COMMIT returned
  `Ok(None)` ("nothing pending") on every poll, so CDC replication stopped
  for good at the first transaction with 1024+ row changes, with no error and
  no cursor movement. Found by ePHPm's clustered stress run (a 3000-row seed
  INSERT froze every replica at change_id 12 while the primary reached
  705k).
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
