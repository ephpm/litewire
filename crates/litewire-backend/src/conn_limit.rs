//! Accept-time connection cap shared by litewire's wire frontends.
//!
//! # Why this lives next to the backend
//!
//! Because a wire connection is not a cheap object here. Each accepted
//! client holds a [`crate::BackendConn`] for its whole session, and the
//! rusqlite backend gives every session **its own OS thread** and its own
//! open SQLite handle (see [`crate::rusqlite_backend`]). Unbounded accepts
//! therefore mean unbounded threads and file descriptors, which is a
//! backend resource question wearing a networking costume.
//!
//! # Shape
//!
//! A [`ConnectionLimiter`] hands out a [`ConnectionSlot`] per accepted
//! connection. The slot releases its seat when it is **dropped**, so the
//! correct way to use it is to move the slot into the per-connection task:
//! the count then falls exactly when that task ends, for every reason a
//! task can end -- clean disconnect, protocol error, a client that
//! vanished mid-statement, or a panic (tokio drops a panicking task's
//! locals). There is no explicit release call to forget.
//!
//! Over the cap, [`ConnectionLimiter::try_acquire`] returns `None` and the
//! caller refuses the connection **immediately**. Accepts are deliberately
//! not queued: a client that is told "no" can back off or fail over, while
//! a client parked in an invisible accept queue just times out with no
//! information. This matches how ePHPm's own listeners behave.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A cap on the number of simultaneously live connections.
///
/// Cheap to clone; every clone shares one counter.
#[derive(Clone, Debug)]
pub struct ConnectionLimiter {
    /// `None` means unlimited -- the historical behaviour, and the default.
    limit: Option<usize>,
    live: Arc<AtomicUsize>,
}

impl ConnectionLimiter {
    /// Build a limiter allowing at most `max` simultaneous connections.
    /// `0` means **unlimited**, which is the default everywhere so that
    /// enabling nothing changes nothing.
    #[must_use]
    pub fn new(max: usize) -> Self {
        Self {
            limit: (max > 0).then_some(max),
            live: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// An explicitly unlimited limiter. Still counts live connections, so
    /// [`Self::live`] is meaningful either way.
    #[must_use]
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// The configured cap, or `None` when unlimited.
    #[must_use]
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Connections currently holding a slot.
    #[must_use]
    pub fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// Take a slot for one connection, or `None` if the cap is already
    /// reached.
    ///
    /// The compare-exchange loop never publishes a count above `limit`. The
    /// simpler `fetch_add`-then-check-and-subtract-back would admit exactly
    /// the same set of connections -- an overshooting caller does not get a
    /// slot either way -- but it makes [`Self::live`] transiently read
    /// higher than the cap while concurrent accepts race. `live` is public
    /// and is emitted in the frontends' connection logs, so it is worth the
    /// few extra lines to keep it honest rather than have operators see a
    /// connection count that exceeds a limit which was in fact enforced.
    #[must_use]
    pub fn try_acquire(&self) -> Option<ConnectionSlot> {
        match self.limit {
            None => {
                self.live.fetch_add(1, Ordering::AcqRel);
                Some(ConnectionSlot {
                    live: Arc::clone(&self.live),
                })
            }
            Some(limit) => {
                let mut current = self.live.load(Ordering::Acquire);
                loop {
                    if current >= limit {
                        return None;
                    }
                    match self.live.compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            return Some(ConnectionSlot {
                                live: Arc::clone(&self.live),
                            });
                        }
                        Err(actual) => current = actual,
                    }
                }
            }
        }
    }
}

impl Default for ConnectionLimiter {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// A live connection's seat against a [`ConnectionLimiter`].
///
/// Releases on drop. Move it into the per-connection task so the seat's
/// lifetime is exactly the session's lifetime -- the same lifetime that
/// governs when the backend session's worker thread is reclaimed.
#[derive(Debug)]
pub struct ConnectionSlot {
    live: Arc<AtomicUsize>,
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_refuses() {
        let limiter = ConnectionLimiter::new(0);
        assert_eq!(limiter.limit(), None);
        let slots: Vec<_> = (0..1000).map(|_| limiter.try_acquire()).collect();
        assert!(slots.iter().all(Option::is_some));
        assert_eq!(limiter.live(), 1000);
    }

    #[test]
    fn cap_is_enforced_and_slots_release_on_drop() {
        let limiter = ConnectionLimiter::new(3);
        let a = limiter.try_acquire().expect("1st");
        let b = limiter.try_acquire().expect("2nd");
        let c = limiter.try_acquire().expect("3rd");
        assert_eq!(limiter.live(), 3);
        assert!(limiter.try_acquire().is_none(), "4th should be refused");

        drop(b);
        assert_eq!(limiter.live(), 2);
        let d = limiter.try_acquire().expect("slot freed by the drop");
        assert!(limiter.try_acquire().is_none());

        drop((a, c, d));
        assert_eq!(limiter.live(), 0);
    }

    /// The cap must hold under concurrent acquires: 32 threads racing for
    /// 8 seats must be handed exactly 8, and the counter must return to
    /// zero once they are all dropped.
    ///
    /// Note on what this does *not* prove: a `fetch_add`-then-subtract-back
    /// implementation also passes it, because it admits the same set of
    /// connections and only differs in a transient `live()` reading. That
    /// was verified by patching the implementation, not assumed. The CAS
    /// loop is chosen for the honesty of `live()`, not because this test
    /// distinguishes the two.
    #[test]
    fn cap_is_exact_under_contention() {
        const LIMIT: usize = 8;
        const THREADS: usize = 32;
        let limiter = ConnectionLimiter::new(LIMIT);
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let peak = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let limiter = limiter.clone();
                let barrier = Arc::clone(&barrier);
                let peak = Arc::clone(&peak);
                std::thread::spawn(move || {
                    barrier.wait();
                    let mut held = Vec::new();
                    for _ in 0..64 {
                        if let Some(slot) = limiter.try_acquire() {
                            peak.fetch_max(limiter.live(), Ordering::AcqRel);
                            held.push(slot);
                        }
                    }
                    held
                })
            })
            .collect();

        let all: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let granted: usize = all.iter().map(Vec::len).sum();
        assert_eq!(granted, LIMIT, "handed out more seats than the cap allows");
        assert!(
            peak.load(Ordering::Acquire) <= LIMIT,
            "live count exceeded the cap transiently"
        );
        drop(all);
        assert_eq!(limiter.live(), 0);
    }
}
