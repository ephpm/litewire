//! Result-set normalization and differential comparison.
//!
//! litewire executes against SQLite/Turso; the reference is a real MySQL or
//! PostgreSQL server. The two render the *same logical value* differently
//! over the wire — `1` vs `t` for booleans, `1.0` vs `1`, trailing-zero
//! floats, whitespace. [`canon_cell`] folds those representational
//! differences away so that a reported [`RowDiff`] reflects a genuine
//! semantic divergence rather than a formatting one.
//!
//! This is deliberately lenient. The design accepts a small risk of two
//! genuinely different values canonicalizing equal (a false match) in
//! exchange for not flaking the merge gate on representation noise — a false
//! *mismatch* would. Divergences the normalizer cannot absorb are expected
//! and are what the checked-in baseline records.

/// One cell of a result set, already stringified by the wire client. `None`
/// is SQL `NULL`.
pub type Cell = Option<String>;

/// A result set: rows of cells.
pub type Rows = Vec<Vec<Cell>>;

/// Canonical sentinel for SQL `NULL` (distinct from an empty string).
const NULL: &str = "\u{0}NULL\u{0}";

/// Canonicalize one cell to a comparison key.
#[must_use]
pub fn canon_cell(cell: &Cell) -> String {
    let Some(raw) = cell else {
        return NULL.to_string();
    };
    let t = raw.trim();

    // Exact integers stay exact (never routed through f64).
    if let Ok(i) = t.parse::<i128>() {
        return i.to_string();
    }
    // Floats: fold trailing-zero / precision differences.
    if let Ok(f) = t.parse::<f64>()
        && f.is_finite()
    {
        return canon_float(f);
    }
    // Boolean spellings: PG simple-query renders bool as `t`/`f`; SQLite
    // stores it as the integer `1`/`0`. Fold the common spellings together.
    match t.to_ascii_lowercase().as_str() {
        "true" | "t" => "1".to_string(),
        "false" | "f" => "0".to_string(),
        _ => t.to_string(),
    }
}

/// Format a float with enough precision to be meaningful but little enough to
/// absorb engine-to-engine rounding. Whole-valued floats collapse to the
/// integer form so `1.0` and `1` compare equal.
fn canon_float(f: f64) -> String {
    if f == f.trunc() && f.abs() < 1e15 {
        // Safe: value is integral and within i128 range checked above.
        return format!("{}", f as i128);
    }
    let mut s = format!("{f:.9}");
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    s
}

/// Canonicalize a full result set: every cell, and (unless `ordered`) the row
/// order, so two set-equal results compare equal.
#[must_use]
pub fn canon_rows(rows: &Rows, ordered: bool) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(canon_cell).collect())
        .collect();
    if !ordered {
        out.sort();
    }
    out
}

/// The outcome of diffing two result sets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowDiff {
    /// Equal after normalization.
    Equal,
    /// Different; carries a short human summary of the first divergence.
    Different(String),
}

/// Diff a litewire result against a reference result.
#[must_use]
pub fn diff_rows(litewire: &Rows, reference: &Rows, ordered: bool) -> RowDiff {
    let lw = canon_rows(litewire, ordered);
    let rf = canon_rows(reference, ordered);

    if lw.len() != rf.len() {
        return RowDiff::Different(format!(
            "row count: litewire={} reference={}",
            lw.len(),
            rf.len()
        ));
    }
    for (i, (l, r)) in lw.iter().zip(rf.iter()).enumerate() {
        if l.len() != r.len() {
            return RowDiff::Different(format!(
                "row {i} column count: litewire={} reference={}",
                l.len(),
                r.len()
            ));
        }
        if l != r {
            return RowDiff::Different(format!("row {i}: litewire={l:?} reference={r:?}"));
        }
    }
    RowDiff::Equal
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(s: &str) -> Cell {
        Some(s.to_string())
    }

    #[test]
    fn integers_exact() {
        assert_eq!(canon_cell(&c("42")), "42");
        assert_eq!(canon_cell(&c("  42 ")), "42");
    }

    #[test]
    fn float_whole_collapses_to_int() {
        assert_eq!(canon_cell(&c("1.0")), "1");
        assert_eq!(canon_cell(&c("1")), "1");
        assert_eq!(canon_cell(&c("1.500000")), canon_cell(&c("1.5")));
    }

    #[test]
    fn booleans_fold() {
        assert_eq!(canon_cell(&c("t")), "1");
        assert_eq!(canon_cell(&c("true")), "1");
        assert_eq!(canon_cell(&c("f")), "0");
        assert_eq!(canon_cell(&c("FALSE")), "0");
    }

    #[test]
    fn null_distinct_from_empty() {
        assert_ne!(canon_cell(&None), canon_cell(&c("")));
    }

    #[test]
    fn unordered_rows_compare_set_equal() {
        let a = vec![vec![c("1")], vec![c("2")]];
        let b = vec![vec![c("2")], vec![c("1")]];
        assert_eq!(diff_rows(&a, &b, false), RowDiff::Equal);
        // When order matters, the same two differ.
        assert!(matches!(diff_rows(&a, &b, true), RowDiff::Different(_)));
    }

    #[test]
    fn row_count_mismatch_reported() {
        let a = vec![vec![c("1")]];
        let b = vec![vec![c("1")], vec![c("2")]];
        assert!(matches!(diff_rows(&a, &b, false), RowDiff::Different(_)));
    }
}
