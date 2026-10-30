//! The suite runner: drive the corpus through litewire and (when configured)
//! the reference servers, categorize each entry, fold in the baseline, and
//! report regressions.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::baseline::{Baseline, BaselineEntry};
use crate::model::{Category, CorpusEntry, Dialect, Diff, RawOutcome};
use crate::normalize::{RowDiff, diff_rows};
use crate::targets::{MysqlWire, PgWire};

/// Runtime configuration, assembled from the environment.
#[derive(Clone, Debug, Default)]
pub struct RunConfig {
    /// Reference MySQL server URL (`COMPAT_MYSQL_URL`). `None` → MySQL
    /// correctness diffing is skipped (coverage only).
    pub mysql_ref_url: Option<String>,
    /// Reference PostgreSQL server URL (`COMPAT_PG_URL`).
    pub pg_ref_url: Option<String>,
    /// Rewrite the baseline from the observed outcomes (`COMPAT_UPDATE_BASELINE=1`).
    pub update_baseline: bool,
    /// Baseline file location.
    pub baseline_path: PathBuf,
}

impl RunConfig {
    /// Build from the standard environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        let non_empty = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        Self {
            mysql_ref_url: non_empty("COMPAT_MYSQL_URL"),
            pg_ref_url: non_empty("COMPAT_PG_URL"),
            update_baseline: std::env::var("COMPAT_UPDATE_BASELINE")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            baseline_path: Baseline::default_path(),
        }
    }

    /// Whether any reference server is configured.
    #[must_use]
    pub fn has_reference(&self) -> bool {
        self.mysql_ref_url.is_some() || self.pg_ref_url.is_some()
    }
}

/// The final status of one entry after baseline folding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Passed; no baseline row.
    Pass,
    /// Failed, but the failure is baselined — tolerated.
    Known,
    /// Failed and NOT baselined — fails the build.
    Regression,
    /// Passed, but a baseline row exists — the baseline can be tightened.
    UnexpectedPass,
}

/// One entry's complete result.
#[derive(Clone, Debug)]
pub struct EntryResult {
    /// Corpus id.
    pub id: String,
    /// Dialect.
    pub dialect: Dialect,
    /// Category tag.
    pub tag: &'static str,
    /// Observed raw outcome.
    pub raw: RawOutcome,
    /// Final status.
    pub status: Status,
    /// Extra note (reference-rejected, baseline-kind drift, …).
    pub note: Option<String>,
    /// Whether this entry was actually result-diffed against a reference.
    pub diffed: bool,
}

/// The full suite report.
#[derive(Clone, Debug, Default)]
pub struct Report {
    /// Per-entry results, in corpus order.
    pub results: Vec<EntryResult>,
    /// Whether the baseline was rewritten this run.
    pub baseline_updated: bool,
}

impl Report {
    /// Entries that fail the build.
    #[must_use]
    pub fn regressions(&self) -> Vec<&EntryResult> {
        self.results
            .iter()
            .filter(|r| r.status == Status::Regression)
            .collect()
    }

    /// Entries that now pass despite being baselined.
    #[must_use]
    pub fn unexpected_passes(&self) -> Vec<&EntryResult> {
        self.results
            .iter()
            .filter(|r| r.status == Status::UnexpectedPass)
            .collect()
    }

    /// `true` if the build should fail.
    #[must_use]
    pub fn has_regressions(&self) -> bool {
        self.results.iter().any(|r| r.status == Status::Regression)
    }

    /// A human-readable multi-line summary.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut cat: BTreeMap<&str, usize> = BTreeMap::new();
        let mut status: BTreeMap<&str, usize> = BTreeMap::new();
        let mut diffed = 0usize;
        for r in &self.results {
            *cat.entry(r.raw.category.as_str()).or_default() += 1;
            *status
                .entry(match r.status {
                    Status::Pass => "pass",
                    Status::Known => "known (baselined)",
                    Status::Regression => "REGRESSION",
                    Status::UnexpectedPass => "unexpected-pass",
                })
                .or_default() += 1;
            if r.diffed {
                diffed += 1;
            }
        }

        let mut out = String::new();
        out.push_str(&format!(
            "litewire compat suite: {} entries ({diffed} result-diffed against a reference)\n",
            self.results.len()
        ));
        out.push_str("  by category:\n");
        for (k, v) in &cat {
            out.push_str(&format!("    {k:<18} {v}\n"));
        }
        out.push_str("  by status:\n");
        for (k, v) in &status {
            out.push_str(&format!("    {k:<18} {v}\n"));
        }
        for r in self.regressions() {
            out.push_str(&format!(
                "  REGRESSION  {} [{}] {}\n",
                r.id,
                r.raw.category.as_str(),
                truncate(&r.raw.detail, 160)
            ));
        }
        for r in self.unexpected_passes() {
            out.push_str(&format!("  now-passing (tighten baseline): {}\n", r.id));
        }
        for r in &self.results {
            if let Some(note) = &r.note {
                out.push_str(&format!("  note  {}: {note}\n", r.id));
            }
        }
        if self.baseline_updated {
            out.push_str("  baseline REWRITTEN (COMPAT_UPDATE_BASELINE=1)\n");
        }
        out
    }
}

fn truncate(s: &str, n: usize) -> String {
    let s = s.replace('\n', " ");
    if s.chars().count() <= n {
        s
    } else {
        let mut t: String = s.chars().take(n).collect();
        t.push('…');
        t
    }
}

/// Run the whole suite and return a [`Report`].
///
/// Boots the litewire MySQL and PostgreSQL frontends (each over an in-process
/// Turso engine) and, when the matching URL is configured, the reference
/// servers. Then runs every corpus entry.
///
/// # Errors
///
/// Returns an error only if the litewire listeners or a configured reference
/// server cannot be reached, or if the baseline cannot be read/written — never
/// for an individual corpus failure (those are categorized, not fatal).
pub async fn run_suite(config: &RunConfig, corpus: &[CorpusEntry]) -> anyhow::Result<Report> {
    let baseline = Baseline::load(&config.baseline_path)?;

    let mut lw_mysql = MysqlWire::litewire().await?;
    let mut lw_pg = PgWire::litewire().await?;

    let mut ref_mysql = match &config.mysql_ref_url {
        Some(url) => Some(MysqlWire::reference(url).await?),
        None => None,
    };
    let mut ref_pg = match &config.pg_ref_url {
        Some(url) => Some(PgWire::reference(url).await?),
        None => None,
    };

    let mut results = Vec::with_capacity(corpus.len());
    for entry in corpus {
        let (raw, note, diffed) = run_one(
            entry,
            &mut lw_mysql,
            &mut lw_pg,
            ref_mysql.as_mut(),
            ref_pg.as_mut(),
        )
        .await;
        let (status, drift_note) = fold_baseline(&baseline, &entry.id, raw.category, diffed);
        let note = note.or(drift_note);
        results.push(EntryResult {
            id: entry.id.clone(),
            dialect: entry.dialect,
            tag: entry.tag,
            raw,
            status,
            note,
            diffed,
        });
    }

    let mut report = Report {
        results,
        baseline_updated: false,
    };

    if config.update_baseline {
        let mut rows: Vec<BaselineEntry> = Vec::new();
        for r in &report.results {
            if r.raw.category.is_failure() {
                rows.push(BaselineEntry {
                    id: r.id.clone(),
                    kind: r.raw.category.as_str().to_string(),
                    reason: baseline_reason(&r.raw.detail),
                });
            }
        }
        let mut new_baseline = Baseline::default();
        new_baseline.set_entries(rows);
        new_baseline.save(&config.baseline_path)?;
        report.baseline_updated = true;
        // After a rewrite nothing is a regression: re-fold against the new set.
        for r in &mut report.results {
            let (status, _) = fold_baseline(&new_baseline, &r.id, r.raw.category, r.diffed);
            r.status = status;
        }
    }

    Ok(report)
}

/// Pick a reason string for a generated baseline row.
fn baseline_reason(detail: &str) -> String {
    let d = truncate(detail.trim(), 180);
    if d.is_empty() {
        "known divergence (triage: add a human reason)".to_string()
    } else {
        format!("observed: {d}")
    }
}

/// Decide the final status for an entry given the baseline.
///
/// `diffed` says whether a reference result comparison actually happened — it
/// gates the "unexpected pass" signal so a `result_mismatch` baseline isn't
/// mislabelled as now-passing on a coverage-only run (where it was never
/// diffed and so couldn't fail).
fn fold_baseline(
    baseline: &Baseline,
    id: &str,
    observed: Category,
    diffed: bool,
) -> (Status, Option<String>) {
    if observed.is_failure() {
        if baseline.contains(id) {
            // Tolerated. Note if the failure category changed from what was
            // recorded — useful signal, not a build failure.
            let drift = match baseline.expected(id) {
                Some(exp) if exp != observed => Some(format!(
                    "baseline kind drift: recorded {}, now {}",
                    exp.as_str(),
                    observed.as_str()
                )),
                _ => None,
            };
            (Status::Known, drift)
        } else {
            (Status::Regression, None)
        }
    } else if baseline.contains(id) {
        // A mismatch baseline that was never diffed (no reference for this
        // dialect) was not actually evaluated — treat it as tolerated, not as
        // a tighten-the-baseline signal.
        if baseline.expected(id) == Some(Category::ResultMismatch) && !diffed {
            (Status::Known, None)
        } else {
            (Status::UnexpectedPass, None)
        }
    } else {
        (Status::Pass, None)
    }
}

/// Run a single entry against litewire and, if diffed and configured, the
/// reference. Returns the raw outcome, an optional note, and whether a real
/// reference diff happened.
async fn run_one(
    entry: &CorpusEntry,
    lw_mysql: &mut MysqlWire,
    lw_pg: &mut PgWire,
    ref_mysql: Option<&mut MysqlWire>,
    ref_pg: Option<&mut PgWire>,
) -> (RawOutcome, Option<String>, bool) {
    // 1. litewire side.
    let lw_result = match entry.dialect {
        Dialect::Mysql => lw_mysql.run(&entry.setup, entry.sql).await,
        Dialect::Pg => lw_pg.run(&entry.setup, entry.sql).await,
    };

    let lw_rows = match lw_result {
        Ok(rows) => rows,
        Err(fail) => {
            let category = classify_failure(&fail.stmt, entry.dialect);
            return (RawOutcome::fail(category, fail.message), None, false);
        }
    };

    // 2. Reference diff, only when the entry is diffable and a reference for
    //    this dialect is configured.
    if !entry.diff.is_diffed() {
        return (RawOutcome::ok(), None, false);
    }

    let ref_result = match entry.dialect {
        Dialect::Mysql => match ref_mysql {
            Some(r) => Some(r.run(&entry.setup, entry.sql).await),
            None => None,
        },
        Dialect::Pg => match ref_pg {
            Some(r) => Some(r.run(&entry.setup, entry.sql).await),
            None => None,
        },
    };

    match ref_result {
        None => (RawOutcome::ok(), None, false), // coverage only
        Some(Err(fail)) => {
            // The reference rejected a statement litewire accepted. Not a
            // litewire regression — flag it, don't fail.
            (
                RawOutcome::ok(),
                Some(format!("reference rejected (not diffed): {}", fail.message)),
                false,
            )
        }
        Some(Ok(ref_rows)) => {
            let ordered = matches!(entry.diff, Diff::OrderedRows);
            match diff_rows(&lw_rows, &ref_rows, ordered) {
                RowDiff::Equal => (RawOutcome::ok(), None, true),
                RowDiff::Different(summary) => (
                    RawOutcome::fail(Category::ResultMismatch, summary),
                    None,
                    true,
                ),
            }
        }
    }
}

/// Classify a wire failure as a translation gap or an execution error by
/// re-running the offending statement through litewire's translator.
fn classify_failure(stmt: &str, dialect: Dialect) -> Category {
    match litewire::translate::translate(stmt, dialect.translate_dialect()) {
        Err(_) => Category::TranslationError,
        Ok(_) => Category::ExecError,
    }
}
