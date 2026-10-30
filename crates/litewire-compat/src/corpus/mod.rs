//! The compatibility corpus.
//!
//! Entries are organized into two groups, each in its own submodule and each
//! easy to extend by appending to its `add` function:
//!
//! * [`matrix`] — a structured SQL-construct matrix (DDL, DML, SELECT
//!   variety, functions, quoting, transactions, …).
//! * [`frameworks`] — the real query *shapes* emitted by the frameworks ePHPm
//!   targets (WordPress/wpdb, Laravel/Eloquent, Doctrine DBAL, CakePHP) and
//!   legacy hand-rolled SQL (Sendy).
//!
//! To add a case: pick the right submodule, call `c.my(...)`, `c.pg(...)` or
//! `c.both(...)` with a fresh `name`, and (if it's a deterministic read)
//! choose a shared seed and a [`Diff`] mode. If it is expected to fail or
//! diverge, run `COMPAT_UPDATE_BASELINE=1` once and review the new row.

mod frameworks;
mod matrix;

use crate::model::{CorpusEntry, Dialect, Diff};

/// A small builder that corpus modules push entries through.
pub(crate) struct Corpus<'a> {
    out: &'a mut Vec<CorpusEntry>,
}

impl<'a> Corpus<'a> {
    fn new(out: &'a mut Vec<CorpusEntry>) -> Self {
        Self { out }
    }

    /// Add one entry for a specific dialect.
    pub(crate) fn add(
        &mut self,
        dialect: Dialect,
        tag: &'static str,
        name: &str,
        setup: &[&'static str],
        sql: &'static str,
        diff: Diff,
    ) {
        self.out.push(CorpusEntry::new(
            dialect,
            tag,
            name,
            setup.to_vec(),
            sql,
            diff,
        ));
    }

    /// Add a MySQL entry.
    pub(crate) fn my(
        &mut self,
        tag: &'static str,
        name: &str,
        setup: &[&'static str],
        sql: &'static str,
        diff: Diff,
    ) {
        self.add(Dialect::Mysql, tag, name, setup, sql, diff);
    }

    /// Add a PostgreSQL entry.
    pub(crate) fn pg(
        &mut self,
        tag: &'static str,
        name: &str,
        setup: &[&'static str],
        sql: &'static str,
        diff: Diff,
    ) {
        self.add(Dialect::Pg, tag, name, setup, sql, diff);
    }

    /// Add the same ANSI-compatible statement for both dialects.
    pub(crate) fn both(
        &mut self,
        tag: &'static str,
        name: &str,
        setup: &[&'static str],
        sql: &'static str,
        diff: Diff,
    ) {
        self.my(tag, name, setup, sql, diff);
        self.pg(tag, name, setup, sql, diff);
    }
}

/// Assemble the full corpus.
///
/// # Panics
///
/// Panics if two entries share an id — ids are the baseline key and must be
/// unique. This is a programming error in the corpus, caught at startup.
#[must_use]
pub fn collect() -> Vec<CorpusEntry> {
    let mut entries = Vec::new();
    {
        let mut c = Corpus::new(&mut entries);
        matrix::add(&mut c);
        frameworks::add(&mut c);
    }

    let mut seen = std::collections::HashSet::new();
    for e in &entries {
        assert!(seen.insert(e.id.clone()), "duplicate corpus id: {}", e.id);
    }
    entries
}

// ── Shared seeds ─────────────────────────────────────────────────────────────
//
// These run verbatim on litewire (translated) AND on the reference servers, so
// they stick to ANSI types (`INTEGER`, `TEXT`) that MySQL, PostgreSQL and
// SQLite/Turso all accept. Keep them minimal and deterministic.

/// A single `users` table with four rows; user 2 is inactive, users 2 and 4
/// have no orders (for outer-join tests when combined with [`SEED_SHOP`]).
pub(crate) const SEED_USERS: &[&str] = &[
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
    "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, 1)",
    "INSERT INTO users (id, name, age, active) VALUES (2, 'Bob', 25, 0)",
    "INSERT INTO users (id, name, age, active) VALUES (3, 'Carol', 35, 1)",
    "INSERT INTO users (id, name, age, active) VALUES (4, 'Dave', 25, 1)",
];

/// `users` plus an `orders` table referencing it.
pub(crate) const SEED_SHOP: &[&str] = &[
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
    "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, 1)",
    "INSERT INTO users (id, name, age, active) VALUES (2, 'Bob', 25, 0)",
    "INSERT INTO users (id, name, age, active) VALUES (3, 'Carol', 35, 1)",
    "INSERT INTO users (id, name, age, active) VALUES (4, 'Dave', 25, 1)",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
    "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)",
    "INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 50)",
    "INSERT INTO orders (id, user_id, amount) VALUES (3, 3, 200)",
];
