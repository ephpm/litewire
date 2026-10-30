# litewire-compat

A standalone, differential **MySQL + PostgreSQL wire-compatibility test suite**
for litewire. It exists to stop translation gaps and silent wrong-answer bugs
from reaching production: a large corpus of real-world SQL is driven through
litewire's MySQL and PostgreSQL wire listeners (backed by the in-process Turso
engine, exactly as ePHPm links it) and, when reference servers are available,
against a real MySQL and PostgreSQL. Every statement is categorized and — for
deterministic reads — the result sets are normalized and diffed.

litewire targets SQLite/Turso, so **some MySQL/PG semantics intentionally
differ** (integer vs float division, boolean rendering, missing engine
features). Those divergences are not bugs; they are recorded in a checked-in
baseline so the suite can gate PRs without flaking on them.

## How it runs

For each corpus entry the runner records one outcome:

| Outcome | Meaning |
|---|---|
| `TranslatedOk` | Translated, executed, and (if diffed) matched the reference. |
| `TranslationError` | litewire could not translate the statement (parse / unsupported). |
| `ExecError` | Translated, but the engine rejected it at execution time. |
| `ResultMismatch` | Executed on both sides but the normalized results differ. |

The runner **fails the build** only on a *failing* entry whose id is **not** in
the baseline — i.e. a regression (a query that used to pass) or an un-triaged
new entry. A baselined failure is tolerated. A baselined entry that now passes
is flagged ("tighten the baseline") but does not fail.

### Running locally

```bash
# Coverage only — no reference servers. Translation + execution are gated;
# result correctness is NOT checked. This is what `cargo test --workspace`
# runs on a plain dev box, and it passes.
cargo test -p litewire-compat -- --nocapture

# Full correctness — point at a reference MySQL and/or PostgreSQL. Either or
# both may be set; an unset dialect falls back to coverage-only.
COMPAT_MYSQL_URL='mysql://root:root@127.0.0.1:3306' \
COMPAT_PG_URL='host=127.0.0.1 user=postgres password=postgres' \
  cargo test -p litewire-compat -- --nocapture
```

`COMPAT_MYSQL_URL` is a `mysql://user:pass@host:port` URL; the suite creates and
uses a dedicated `litewire_compat` database on that server. `COMPAT_PG_URL` is a
`tokio-postgres` keyword/value connection string; the suite recreates a
dedicated `litewire_compat` schema on each run. Neither database/schema should
hold application data.

The reference servers are isolated per run and reset per entry — the suite is
non-destructive to anything outside its own `litewire_compat` namespace.

## The baseline

`baseline.toml` lists every corpus entry known not to pass fully, with the
expected failure category and a human reason:

```toml
[[entry]]
id = "mysql/expr/integer_division"
kind = "result_mismatch"   # ok | translation_error | exec_error | result_mismatch
reason = "MySQL `/` is float division (7/2 = 3.5); SQLite does integer division (3)"
```

Regenerate it deliberately (never edit observed categories by hand) and review
the diff:

```bash
# With the same reference env you want the baseline to reflect:
COMPAT_UPDATE_BASELINE=1 \
COMPAT_MYSQL_URL=... COMPAT_PG_URL=... \
  cargo test -p litewire-compat -- --nocapture
```

`COMPAT_UPDATE_BASELINE=1` rewrites `baseline.toml` from the observed outcomes
(sorted by id) and the run passes without asserting. **Always review the diff**:
a new row means a query stopped passing, and the generated `reason` (prefixed
`observed:`) should be replaced with a real explanation.

Because result mismatches can only be observed with a reference server, you
normally regenerate the baseline in CI (or locally with both servers up). A
coverage-only regeneration captures translation/exec failures but not
mismatches.

## Adding corpus entries

The corpus lives in `src/corpus/`:

* `matrix.rs` — the structured SQL-construct matrix (DDL, DML, SELECT variety,
  joins, subqueries, set ops, grouping, ordering/paging, CTE/window,
  expressions, functions, quoting, transactions, metadata, known gaps).
* `frameworks.rs` — real query shapes from WordPress/wpdb, Laravel, Doctrine
  DBAL, CakePHP, and Sendy.

To add one, call `c.my(...)`, `c.pg(...)` or `c.both(...)` (ANSI-compatible
statements only) with:

* a `tag` (groups the entry in reports) and a fresh `name` (ids are the
  baseline key — never reuse one);
* `setup`: statements run before the measured one to build schema/seed. Use a
  shared seed (`SEED_USERS`, `SEED_SHOP`) where it fits. For DML correctness,
  put the mutation in `setup` and measure a `SELECT` that observes it.
* the measured `sql`;
* a `Diff` mode: `Rows` (sorted compare), `OrderedRows` (the query pins order
  with `ORDER BY`), or `None` (coverage only — DDL, DML, non-deterministic
  reads like `NOW()`/`RAND()`, prepared shapes).

Only mark an entry diffable if its answer is deterministic. If a new diffable
entry is expected to diverge, run `COMPAT_UPDATE_BASELINE=1` once and write a
real reason into the new baseline row.

## What's covered vs. what's next

Covered: the full construct matrix above over both dialects, framework shapes,
transactions (commit/rollback persistence), the MySQL binary/prepared protocol
(a dedicated round-trip test), and result normalization that folds
boolean/numeric/float/whitespace representation differences across engines.

Top extension points:

* **Prepared-statement corpus diffing** — the prepared path is smoke-tested,
  not yet driven from the corpus with bound params. Add a `params` dimension to
  the model + a binary-protocol runner branch.
* **More framework shapes** — meta-query joins, Eloquent eager-load `IN`
  batches, Doctrine schema-diff introspection, WooCommerce lookups.
* **TDS / Hrana** — litewire also has SQL Server and Hrana frontends; the same
  harness shape extends to them.
* **A larger, scheduled corpus** — keep the PR gate fast; a fuller set can run
  on a schedule (see the CI section in the repo).
