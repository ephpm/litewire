//! The checked-in expectations baseline.
//!
//! The baseline lists every corpus entry that is *known* not to pass fully —
//! an unsupported construct, or a MySQL/PG-vs-SQLite semantic divergence —
//! with the expected failure category and a reason. The runner fails the
//! build only on a failure whose id is **not** baselined (a regression, or a
//! newly added entry that doesn't pass), never on a baselined one.
//!
//! Format (`baseline.toml`):
//!
//! ```toml
//! [[entry]]
//! id = "mysql/window/row_number"
//! kind = "translation_error"   # ok | translation_error | exec_error | result_mismatch
//! reason = "sqlparser 0.57 does not parse window functions"
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::model::Category;

/// One baselined expectation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BaselineEntry {
    /// Corpus entry id.
    pub id: String,
    /// Expected failure category (`translation_error`, `exec_error`,
    /// `result_mismatch`). `ok` is never written — a passing entry has no
    /// baseline row.
    pub kind: String,
    /// Why this entry is known not to pass. Required so the file documents
    /// itself.
    pub reason: String,
}

/// The parsed baseline file.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Baseline {
    #[serde(default, rename = "entry")]
    entries: Vec<BaselineEntry>,
}

impl Baseline {
    /// The canonical on-disk location, next to the crate manifest.
    #[must_use]
    pub fn default_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("baseline.toml")
    }

    /// Load the baseline from `path`. A missing file is an empty baseline
    /// (every failure is then a regression), which is the correct behavior
    /// for a fresh checkout before the file is generated.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be parsed.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Ok(toml::from_str(&text)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Look up the expected category for an id, if baselined.
    #[must_use]
    pub fn expected(&self, id: &str) -> Option<Category> {
        self.entries
            .iter()
            .find(|e| e.id == id)
            .and_then(|e| Category::from_kind(&e.kind))
    }

    /// Whether an id is present in the baseline at all.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.entries.iter().any(|e| e.id == id)
    }

    /// All baselined ids (for detecting stale rows).
    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|e| e.id.as_str())
    }

    /// Replace the entry set, keeping it sorted by id for a stable diff.
    pub fn set_entries(&mut self, mut entries: Vec<BaselineEntry>) {
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        self.entries = entries;
    }

    /// Serialize to the canonical TOML form with a header comment.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        // Group reasons by id for a tidy, reviewable file.
        let mut by_id: BTreeMap<&str, &BaselineEntry> = BTreeMap::new();
        for e in &self.entries {
            by_id.insert(e.id.as_str(), e);
        }
        let header = "# litewire MySQL/PG wire-compatibility baseline.\n\
             #\n\
             # Each row is a corpus entry known NOT to pass fully: an\n\
             # unsupported construct or a documented MySQL/PG-vs-SQLite\n\
             # divergence. The suite fails the build on any FAILING entry whose\n\
             # id is absent here (a regression or an un-triaged new entry).\n\
             #\n\
             # Regenerate deliberately with COMPAT_UPDATE_BASELINE=1 (see the\n\
             # crate README) and review every added/removed row.\n\n";
        let body = toml::to_string_pretty(self)?;
        Ok(format!("{header}{body}"))
    }

    /// Write the baseline to `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the file write fails.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        std::fs::write(path, self.to_toml()?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut b = Baseline::default();
        b.set_entries(vec![
            BaselineEntry {
                id: "mysql/b/two".into(),
                kind: "exec_error".into(),
                reason: "r2".into(),
            },
            BaselineEntry {
                id: "mysql/a/one".into(),
                kind: "translation_error".into(),
                reason: "r1".into(),
            },
        ]);
        let text = b.to_toml().unwrap();
        let parsed: Baseline = toml::from_str(&text).unwrap();
        // Sorted by id.
        assert_eq!(
            parsed.ids().collect::<Vec<_>>(),
            ["mysql/a/one", "mysql/b/two"]
        );
        assert_eq!(
            parsed.expected("mysql/a/one"),
            Some(Category::TranslationError)
        );
        assert_eq!(parsed.expected("mysql/b/two"), Some(Category::ExecError));
        assert!(parsed.expected("missing").is_none());
    }

    #[test]
    fn missing_file_is_empty() {
        let b = Baseline::load(Path::new("/no/such/baseline.toml")).unwrap();
        assert!(!b.contains("anything"));
    }
}
