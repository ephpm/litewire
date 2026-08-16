# Contributing to litewire

## Build

```bash
cargo build                      # library crates, default features
cargo build --features cli       # the litewire binary (clap + tracing-subscriber)
cargo check --workspace --all-features
```

Default features are `mysql`, `hrana`, `backend-rusqlite`. The `postgres`,
`tds`, `backend-hrana-client`, and `turso` features are off by default —
changes touching feature-gated code must compile under `--all-features`, and
CI enforces it.

Minimum supported Rust version: **1.88** (declared as `rust-version` in the
workspace `Cargo.toml`, checked by the `msrv` CI job — the floor comes from
the Turso engine's dependency graph). Don't use language or library features
newer than that without bumping it deliberately.

## Test

CI runs the suite twice, and so should you before pushing:

```bash
cargo test --workspace                  # default features
cargo test --workspace --all-features   # + hrana-client, turso, postgres, tds
```

Both runs matter: feature unification under `--all-features` can mask a break
that only shows with default features, and the default run alone never
compiles the optional backends.

### The wire-level regression test convention

A bugfix for anything a client can observe on the wire lands **with a
wire-level regression test** that reproduces the client-observed failure,
using a real client library against an in-process server. These live in
`crates/litewire/tests/`:

- `mysql_*.rs` drive the MySQL frontend via `mysql_async`
- `postgres_e2e.rs` drives the PG frontend via `tokio-postgres`
- `tds_e2e.rs` drives the TDS frontend via `tiberius`

Examples of the pattern: `mysql_server_version.rs` (handshake vs `VERSION()`
vs `@@version` agreement), `mysql_housekeeping.rs` (command packets that used
to elicit a stray OK), `mysql_error_codes.rs` (1062/1146 fidelity),
`max_connections.rs` (refusal behavior). A unit test on the rewrite alone is
not enough — the historical failures here were almost all in the gap between
"the translated SQL looks right" and "the bytes on the wire are right".

Pure translation changes also get unit tests next to the code in
`crates/litewire-translate`.

## Lint and format

CI gates on both; run them locally:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

rustfmt runs on **stable** with default settings (the repo's `rustfmt.toml`
only pins `edition = "2024"`). Warnings are errors — `RUSTFLAGS: -D warnings`
in CI.

## Conventions

- Commit / PR titles follow conventional-commit style:
  `fix(mysql): ...`, `feat(session): ...`, `perf(hrana): ...`.
- Doc comments state what the code does *and why* — especially for protocol
  quirks and workarounds, cite the observable failure (issue number, client
  error text) the code exists to prevent.
- Documentation must match the code. Don't document behavior you haven't
  verified in source; label unimplemented ideas explicitly as not
  implemented.
- Dependencies are added deliberately, with a comment in the workspace
  `Cargo.toml` explaining why that crate (see the existing entries). The
  `turso` crate is pinned exactly (`=0.7.0`) — never bump it via
  `cargo update`.
