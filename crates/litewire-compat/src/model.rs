//! Corpus and outcome data model.
//!
//! A [`CorpusEntry`] is one unit of the compatibility corpus: a single
//! measured SQL statement in one dialect, the schema/seed it needs, and how
//! (or whether) its result set should be diffed against a reference server.
//!
//! Running an entry produces a [`RawOutcome`]. The runner later folds the
//! checked-in baseline into each raw outcome to decide whether it counts as a
//! pass, a tolerated-known divergence, or a build-failing regression.

use std::fmt;

/// The wire dialect an entry is driven over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dialect {
    /// MySQL wire protocol (`mysql_async`), translated by litewire's MySQL path.
    Mysql,
    /// PostgreSQL wire protocol (`tokio-postgres`), translated by the PG path.
    Pg,
}

impl Dialect {
    /// Short lowercase tag used as the first segment of an entry id.
    #[must_use]
    pub fn tag(self) -> &'static str {
        match self {
            Dialect::Mysql => "mysql",
            Dialect::Pg => "pg",
        }
    }

    /// The matching [`litewire::translate::Dialect`], used to classify a wire
    /// failure as a translation gap versus an execution error.
    #[must_use]
    pub fn translate_dialect(self) -> litewire::translate::Dialect {
        match self {
            Dialect::Mysql => litewire::translate::Dialect::MySQL,
            Dialect::Pg => litewire::translate::Dialect::PostgreSQL,
        }
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.tag())
    }
}

/// How an entry's result should be compared against the reference server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Diff {
    /// Not result-diffed — the entry proves only that it translates and
    /// executes. Use for DDL, DML, non-deterministic reads (`NOW()`,
    /// `RAND()`, auto-increment values), and prepared-statement shapes.
    None,
    /// Result-diffed. Rows are sorted before comparison because the query has
    /// no `ORDER BY` and SQL set order is unspecified.
    Rows,
    /// Result-diffed with row order significant — the query pins order with
    /// `ORDER BY`, so a reordering is a real divergence.
    OrderedRows,
}

impl Diff {
    /// Whether the entry participates in reference result diffing at all.
    #[must_use]
    pub fn is_diffed(self) -> bool {
        !matches!(self, Diff::None)
    }
}

/// A single corpus statement plus everything needed to run and judge it.
#[derive(Clone, Debug)]
pub struct CorpusEntry {
    /// Stable, unique id. Convention: `<dialect>/<tag>/<name>`, e.g.
    /// `mysql/select/left_join`. This is the baseline key — never reuse one.
    pub id: String,
    /// Dialect this entry is driven over.
    pub dialect: Dialect,
    /// Category tag for reporting (`"ddl"`, `"select"`, `"framework:wordpress"`).
    pub tag: &'static str,
    /// Statements run before the measured statement to establish schema/seed.
    /// Not themselves judged for result correctness; a failure here still
    /// fails the entry (classified by the offending statement).
    pub setup: Vec<&'static str>,
    /// The one statement whose translation / execution / result is measured.
    pub sql: &'static str,
    /// How the result is diffed.
    pub diff: Diff,
}

impl CorpusEntry {
    /// Build an entry, deriving the id from `dialect`, `tag` and `name`.
    #[must_use]
    pub fn new(
        dialect: Dialect,
        tag: &'static str,
        name: &str,
        setup: Vec<&'static str>,
        sql: &'static str,
        diff: Diff,
    ) -> Self {
        Self {
            id: format!("{}/{}/{name}", dialect.tag(), tag),
            dialect,
            tag,
            setup,
            sql,
            diff,
        }
    }
}

/// The category of a failing (or passing) run, before baseline folding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    /// Translated, executed, and (if diffed) matched the reference.
    TranslatedOk,
    /// litewire could not translate the statement (parse / unsupported).
    TranslationError,
    /// Translated, but the engine rejected it at execution time.
    ExecError,
    /// Executed on both sides but the result sets differ after normalization.
    ResultMismatch,
}

impl Category {
    /// The string written to / read from the baseline file.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Category::TranslatedOk => "ok",
            Category::TranslationError => "translation_error",
            Category::ExecError => "exec_error",
            Category::ResultMismatch => "result_mismatch",
        }
    }

    /// Parse a baseline `kind` string.
    #[must_use]
    pub fn from_kind(s: &str) -> Option<Self> {
        match s {
            "ok" => Some(Category::TranslatedOk),
            "translation_error" => Some(Category::TranslationError),
            "exec_error" => Some(Category::ExecError),
            "result_mismatch" => Some(Category::ResultMismatch),
            _ => None,
        }
    }

    /// Whether this category represents a failing outcome (anything but OK).
    #[must_use]
    pub fn is_failure(self) -> bool {
        !matches!(self, Category::TranslatedOk)
    }
}

/// The result of running one entry, before baseline folding.
#[derive(Clone, Debug)]
pub struct RawOutcome {
    /// Observed category.
    pub category: Category,
    /// Human-readable detail (error text or mismatch summary). Printed in
    /// reports; deliberately **not** part of the baseline key, because error
    /// text drifts across engine/server versions.
    pub detail: String,
}

impl RawOutcome {
    /// A clean pass.
    #[must_use]
    pub fn ok() -> Self {
        Self {
            category: Category::TranslatedOk,
            detail: String::new(),
        }
    }

    /// A failure in the given category with a detail message.
    #[must_use]
    pub fn fail(category: Category, detail: impl Into<String>) -> Self {
        Self {
            category,
            detail: detail.into(),
        }
    }
}
