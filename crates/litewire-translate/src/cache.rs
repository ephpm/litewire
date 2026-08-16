//! Bounded LRU cache for translated SQL.
//!
//! The parse-then-rewrite pipeline in [`crate::translate`] dominates
//! per-request latency for short queries; WordPress alone can issue ~60
//! prepared-statement `SELECT`s to render a single page. Caching the exact
//! text -> `[TranslateResult]` mapping shaves that cost off every repeat
//! statement.
//!
//! Design notes:
//! * **Dialect is a routing dimension, not part of the key.** The same text
//!   under different dialects can produce different output (e.g. `SELECT $1`
//!   is a placeholder in PG but a syntax error in MySQL), so the two must
//!   not collide. Folding dialect into a `(Dialect, String)` tuple key would
//!   force every lookup to allocate a `String` just to ask a question --
//!   `lru` cannot look up a tuple key from a borrow. Keeping one map per
//!   dialect makes the key the raw SQL text, which `lru` *can* look up from a
//!   borrowed `&str`, so a hit allocates nothing.
//! * **Values are shared, not copied.** A hit hands back an
//!   `Arc<[TranslateResult]>`, so the cost of a hit is one atomic increment
//!   regardless of how much SQL the entry holds. The previous `Vec` clone
//!   was proportional to the translated text and was paid on every statement
//!   of every request.
//! * **One lock per dialect, and no more.** A wire frontend speaks a single
//!   dialect, so its traffic funnels through one `Mutex`. Sub-dividing that
//!   lock into hash shards was measured and rejected: an LRU `get` is a
//!   *write* (it promotes the entry to most-recently-used), so concurrent
//!   hits on a hot statement contend on that entry's list links no matter
//!   how many locks guard the map -- aggregate throughput did not improve --
//!   while the second hash needed to pick a shard cost more than the lookup
//!   it was meant to parallelise, and dividing a fixed capacity across shards
//!   turned a skewed key distribution into cache thrashing.
//! * Cache misses always call the underlying translator; there is no
//!   negative caching, so a parse error is not stored.

use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::{Dialect, TranslateResult};

/// Default per-dialect cache capacity. Sized to comfortably hold every
/// distinct prepared statement WordPress + Woocommerce issue in a request
/// (~200) plus headroom for admin dashboards; small enough that even with
/// worst-case 4 KB queries the total memory is a few MB.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Number of dialects, i.e. the number of independent caches held. Keep in
/// sync with [`Dialect`]; [`dialect_index`] is the compiler-checked side of
/// that pairing.
const DIALECTS: usize = 3;

/// Index of the per-dialect cache. The `match` is exhaustive, so adding a
/// [`Dialect`] variant fails to compile here rather than silently sharing a
/// cache with another dialect.
const fn dialect_index(d: Dialect) -> usize {
    match d {
        Dialect::MySQL => 0,
        Dialect::PostgreSQL => 1,
        Dialect::TDS => 2,
    }
}

/// One dialect's map: SQL text -> shared translated result.
///
/// The key is `Box<str>` rather than `String` so the stored key is exactly
/// its bytes with no spare capacity, and -- via `Box<str>: Borrow<str>` --
/// a lookup can be keyed by a borrowed `&str` without allocating.
type DialectCache = Mutex<LruCache<Box<str>, Arc<[TranslateResult]>>>;

/// A thread-safe bounded LRU cache of translated SQL, keyed by dialect+text.
///
/// One independent LRU per dialect (see the module docs for why dialect is a
/// routing dimension, not part of the key). A single `TranslateCache` is
/// shared by every wire session in the process behind an `Arc`.
pub struct TranslateCache {
    /// One LRU per [`Dialect`], indexed by [`dialect_index`].
    per_dialect: [DialectCache; DIALECTS],
}

impl TranslateCache {
    /// Build a cache holding up to `capacity` entries **per dialect**.
    ///
    /// A process that speaks one wire protocol -- the normal case, since
    /// each frontend builds its own cache -- therefore holds at most
    /// `capacity` entries; one that runs a MySQL and a Postgres listener off
    /// a shared cache can hold `capacity` of each.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).expect("cache capacity must be > 0");
        Self {
            per_dialect: std::array::from_fn(|_| Mutex::new(LruCache::new(cap))),
        }
    }

    /// The LRU for `dialect`.
    fn map(&self, dialect: Dialect) -> &DialectCache {
        &self.per_dialect[dialect_index(dialect)]
    }

    /// Look up a previously translated result. Marks the entry as
    /// recently-used on hit.
    ///
    /// Takes the SQL by reference and returns a shared handle, so a hit
    /// allocates nothing.
    pub fn get(&self, dialect: Dialect, sql: &str) -> Option<Arc<[TranslateResult]>> {
        // `lru::LruCache::get` requires `&mut` (it promotes to MRU), hence
        // the mutex. `Box<str>: Borrow<str>`, so the borrowed `sql` keys the
        // lookup directly -- no owned `String` is built to ask the question.
        self.map(dialect).lock().get(sql).map(Arc::clone)
    }

    /// Insert a translated result.
    pub fn put(&self, dialect: Dialect, sql: &str, results: Arc<[TranslateResult]>) {
        self.map(dialect).lock().put(sql.into(), results);
    }

    /// Return the number of entries currently held, across every dialect.
    #[must_use]
    pub fn len(&self) -> usize {
        self.per_dialect.iter().map(|m| m.lock().len()).sum()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.per_dialect.iter().all(|m| m.lock().is_empty())
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

    fn one(sql: &str) -> Arc<[TranslateResult]> {
        Arc::from(vec![TranslateResult::Sql(sql.to_string())])
    }

    #[test]
    fn cache_hit_returns_shared_value() {
        let cache = TranslateCache::new(4);
        cache.put(Dialect::MySQL, "SELECT 1", one("SELECT 1"));
        let hit = cache.get(Dialect::MySQL, "SELECT 1").expect("should hit");
        assert_eq!(hit.len(), 1);
        assert!(matches!(hit[0], TranslateResult::Sql(ref s) if s == "SELECT 1"));
        // Two hits hand out the same allocation rather than copies.
        let again = cache.get(Dialect::MySQL, "SELECT 1").expect("should hit");
        assert!(Arc::ptr_eq(&hit, &again));
    }

    #[test]
    fn cache_miss_on_different_dialect() {
        // Same SQL text under a different dialect must not collide.
        let cache = TranslateCache::new(4);
        cache.put(Dialect::MySQL, "SELECT 1", one("mysql"));
        assert!(cache.get(Dialect::PostgreSQL, "SELECT 1").is_none());
        assert!(cache.get(Dialect::TDS, "SELECT 1").is_none());
    }

    #[test]
    fn dialects_do_not_share_capacity() {
        // One dialect filling its cache must not evict another's entries:
        // each dialect gets its own `capacity`.
        let cache = TranslateCache::new(2);
        cache.put(Dialect::MySQL, "A", one("a"));
        cache.put(Dialect::PostgreSQL, "B", one("b"));
        cache.put(Dialect::PostgreSQL, "C", one("c"));
        cache.put(Dialect::PostgreSQL, "D", one("d"));
        assert!(cache.get(Dialect::MySQL, "A").is_some());
    }

    #[test]
    fn cache_evicts_lru() {
        let cache = TranslateCache::new(2);
        cache.put(Dialect::MySQL, "A", one("a"));
        cache.put(Dialect::MySQL, "B", one("b"));
        // Touching A promotes it to most-recently-used.
        let _ = cache.get(Dialect::MySQL, "A");
        // Adding C evicts B (LRU).
        cache.put(Dialect::MySQL, "C", one("c"));
        assert!(cache.get(Dialect::MySQL, "B").is_none());
        assert!(cache.get(Dialect::MySQL, "A").is_some());
        assert!(cache.get(Dialect::MySQL, "C").is_some());
    }

    #[test]
    fn len_tracks_inserts() {
        let cache = TranslateCache::new(4);
        assert!(cache.is_empty());
        cache.put(Dialect::MySQL, "A", Arc::from(vec![TranslateResult::Noop]));
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
    }
}
