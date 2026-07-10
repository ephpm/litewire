//! Bounded LRU cache for translated SQL.
//!
//! The parse-then-rewrite pipeline in [`crate::translate`] dominates
//! per-request latency for short queries; WordPress alone can issue ~60
//! prepared-statement `SELECT`s to render a single page. Caching the exact
//! text -> `Vec<TranslateResult>` mapping shaves that cost off every repeat
//! statement.
//!
//! Design notes:
//! * The key is `(Dialect, String)` -- same text under different dialects
//!   can produce different output (e.g. `SELECT $1` is a placeholder in PG
//!   but a syntax error in MySQL), so dialect must be part of the key.
//! * Cache misses always call the underlying translator; there's no
//!   negative caching, so a parse error is not stored.
//! * Values are cheap to clone (`String` + a small enum), so the LRU is
//!   fine returning owned copies to the caller.

use std::num::NonZeroUsize;

use lru::LruCache;
use parking_lot::Mutex;

use crate::{Dialect, TranslateResult};

/// Default per-process cache capacity. Sized to comfortably hold every
/// distinct prepared statement WordPress + Woocommerce issue in a request
/// (~200) plus headroom for admin dashboards; small enough that even with
/// worst-case 4 KB queries the total memory is a few MB.
pub const DEFAULT_CAPACITY: usize = 1024;

/// A thread-safe bounded LRU cache keyed by dialect+SQL text.
pub struct TranslateCache {
    inner: Mutex<LruCache<(Dialect, String), Vec<TranslateResult>>>,
}

impl TranslateCache {
    /// Build a cache with the given capacity. Panics if `capacity` is zero.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).expect("cache capacity must be > 0");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Look up a previously translated result. Marks the entry as
    /// recently-used on hit.
    pub fn get(&self, dialect: Dialect, sql: &str) -> Option<Vec<TranslateResult>> {
        // `lru::LruCache::get` requires `&mut`, hence the mutex.
        let mut inner = self.inner.lock();
        inner.get(&(dialect, sql.to_string())).cloned()
    }

    /// Insert a translated result.
    pub fn put(&self, dialect: Dialect, sql: String, results: Vec<TranslateResult>) {
        let mut inner = self.inner.lock();
        inner.put((dialect, sql), results);
    }

    /// Return the number of entries currently held.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

impl Default for TranslateCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit_returns_cloned_value() {
        let cache = TranslateCache::new(4);
        cache.put(
            Dialect::MySQL,
            "SELECT 1".to_string(),
            vec![TranslateResult::Sql("SELECT 1".to_string())],
        );
        let hit = cache.get(Dialect::MySQL, "SELECT 1").expect("should hit");
        assert_eq!(hit.len(), 1);
        assert!(matches!(hit[0], TranslateResult::Sql(ref s) if s == "SELECT 1"));
    }

    #[test]
    fn cache_miss_on_different_dialect() {
        // Same SQL text under a different dialect must not collide.
        let cache = TranslateCache::new(4);
        cache.put(
            Dialect::MySQL,
            "SELECT 1".to_string(),
            vec![TranslateResult::Sql("mysql".to_string())],
        );
        assert!(cache.get(Dialect::PostgreSQL, "SELECT 1").is_none());
    }

    #[test]
    fn cache_evicts_lru() {
        let cache = TranslateCache::new(2);
        cache.put(
            Dialect::MySQL,
            "A".to_string(),
            vec![TranslateResult::Sql("a".into())],
        );
        cache.put(
            Dialect::MySQL,
            "B".to_string(),
            vec![TranslateResult::Sql("b".into())],
        );
        // Touching A promotes it to most-recently-used.
        let _ = cache.get(Dialect::MySQL, "A");
        // Adding C evicts B (LRU).
        cache.put(
            Dialect::MySQL,
            "C".to_string(),
            vec![TranslateResult::Sql("c".into())],
        );
        assert!(cache.get(Dialect::MySQL, "B").is_none());
        assert!(cache.get(Dialect::MySQL, "A").is_some());
        assert!(cache.get(Dialect::MySQL, "C").is_some());
    }

    #[test]
    fn len_tracks_inserts() {
        let cache = TranslateCache::new(4);
        assert!(cache.is_empty());
        cache.put(Dialect::MySQL, "A".into(), vec![TranslateResult::Noop]);
        assert_eq!(cache.len(), 1);
    }
}
