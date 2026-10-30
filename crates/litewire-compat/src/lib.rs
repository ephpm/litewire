//! litewire MySQL + PostgreSQL wire-compatibility harness.
//!
//! A differential test suite that drives a corpus of real-world SQL through
//! litewire's MySQL and PostgreSQL wire listeners (backed by the in-process
//! Turso engine, exactly as ePHPm links it) and, when reference servers are
//! configured, against a real MySQL and PostgreSQL. Each statement is
//! categorized — translated OK, translation error, execution error, or a
//! result mismatch against the reference — and the result is checked against a
//! committed baseline so the suite can gate PRs without flaking on the many
//! *legitimate* MySQL/PG-vs-SQLite semantic differences.
//!
//! # Running
//!
//! ```text
//! # Coverage only (no reference servers) — translation + execution:
//! cargo test -p litewire-compat
//!
//! # Full correctness — set either or both reference URLs:
//! COMPAT_MYSQL_URL=mysql://root:root@127.0.0.1:3306 \
//! COMPAT_PG_URL=host=127.0.0.1 user=postgres password=postgres \
//!     cargo test -p litewire-compat -- --nocapture
//!
//! # Regenerate the baseline deliberately (then review the diff):
//! COMPAT_UPDATE_BASELINE=1 cargo test -p litewire-compat -- --nocapture
//! ```
//!
//! See the crate README for the baseline format and how to add corpus
//! entries.

pub mod baseline;
pub mod corpus;
pub mod model;
pub mod normalize;
pub mod runner;
pub mod targets;

pub use corpus::collect;
pub use model::{Category, CorpusEntry, Dialect, Diff};
pub use runner::{Report, RunConfig, Status, run_suite};
