//! MySQL named ("user-level") locks: the state behind `GET_LOCK()`,
//! `RELEASE_LOCK()`, `RELEASE_ALL_LOCKS()`, `IS_FREE_LOCK()` and
//! `IS_USED_LOCK()`.
//!
//! # What a named lock is
//!
//! An advisory, exclusive, in-memory lock identified by a string. It has
//! nothing to do with rows or tables: applications use it to make sure two
//! workers do not do the same job twice (a cron runner taking
//! `GET_LOCK('send_campaign_42', 0)` before it starts sending). MySQL's
//! guarantees, which this module reproduces:
//!
//! * **Exclusive across sessions, re-entrant within one.** A session may take
//!   the same name several times; it then holds a count, and the name is free
//!   for others only once every acquisition has been released.
//! * **Released when the session ends** — "either normally or abnormally". This
//!   is the property the whole feature stands on: a worker that is killed
//!   mid-job must not leave its lock behind, or the job is never run again.
//!   Here a lock belongs to a [`LockSession`], and the session releases every
//!   lock it holds in its `Drop`, so whatever drops the connection — a clean
//!   `QUIT`, a TCP reset, a timeout, a panic in the connection task — frees
//!   its locks on the same path.
//! * **Not transactional.** `COMMIT` / `ROLLBACK` leave named locks alone.
//!
//! # Scope: the namespace
//!
//! MySQL's lock namespace is server-wide. litewire can serve many unrelated
//! databases from one process (one listener in front of many tenants), and a
//! lock taken by one tenant must be invisible to every other: a name is only
//! unique *within a namespace*, and a [`LockSession`] is created for exactly
//! one. The embedder picks the namespace when it scopes a backend
//! ([`UserLockScoped`]) or a connection ([`scoped_conn`]); a single-tenant
//! deployment uses one namespace (`""`) for everything.
//!
//! # How a connection gets locks
//!
//! Lock operations reach the registry through
//! [`BackendConn::user_lock`](crate::BackendConn::user_lock), which the
//! session layer calls when it finds a lock function in a statement. The
//! trait's **default** implementation refuses: a connection only supports
//! named locks when something gave it a [`LockSession`] — the wrappers in this
//! module do that for in-process backends, and a backend that forwards
//! statements to another process implements the method itself by forwarding
//! the operation, so the lock lives (and dies) with the session on the far
//! side. Refusing by default is deliberate: a lock that *appears* to work but
//! is scoped to the wrong process, or that outlives its holder, is worse than
//! an error, because callers use `GET_LOCK` precisely to prevent duplicate
//! work.
//!
//! # Waiting
//!
//! A `GET_LOCK` with a timeout parks on a per-lock [`Notify`] and is woken when
//! the holder releases; it never polls. A waiter registers its interest while
//! it still holds the registry mutex (see [`LockSession::acquire`]), so a
//! release between "saw it held" and "started waiting" cannot be missed.
//! There is no deadlock detection (MySQL raises `ER_USER_LOCK_DEADLOCK`; two
//! sessions waiting on each other with an infinite timeout stay waiting here),
//! so callers that take several locks should use finite timeouts.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::{Backend, BackendConn, BackendError, Column, ExecuteResult, ResultSet, Value};

/// Maximum length of a lock name, in characters — MySQL's `NAME_CHAR_LEN`.
/// Enforced by the session layer before an operation reaches the registry.
pub const MAX_NAME_CHARS: usize = 64;

/// The error every [`BackendConn`] reports for a lock operation unless it was
/// given a lock scope. Stable text: the session layer and embedders key on it.
pub const USER_LOCKS_UNSUPPORTED: &str = "named locks (GET_LOCK and friends) are not available \
                                           on this connection: its backend provides no user-lock \
                                           scope";

/// How long `GET_LOCK` may wait for a name that is held by another session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockTimeout {
    /// `timeout = 0`: one attempt, never wait.
    Immediate,
    /// `timeout > 0`: wait up to this long.
    Wait(Duration),
    /// `timeout < 0`: wait until the lock is obtained, as MySQL does.
    Forever,
}

impl LockTimeout {
    /// The timeout MySQL's `GET_LOCK(name, timeout)` means by `seconds`.
    #[must_use]
    pub fn from_seconds(seconds: i64) -> Self {
        match seconds {
            s if s < 0 => Self::Forever,
            0 => Self::Immediate,
            s => Self::Wait(Duration::from_secs(s.unsigned_abs())),
        }
    }
}

/// One named-lock operation, carrying an already-validated, already-normalized
/// name (non-empty, at most [`MAX_NAME_CHARS`] characters, lowercased — MySQL
/// lock names are case-insensitive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserLockOp {
    /// `GET_LOCK(name, timeout)`.
    Get {
        /// The normalized lock name.
        name: String,
        /// How long to wait for a held name.
        timeout: LockTimeout,
    },
    /// `RELEASE_LOCK(name)`.
    Release {
        /// The normalized lock name.
        name: String,
    },
    /// `RELEASE_ALL_LOCKS()`.
    ReleaseAll,
    /// `IS_FREE_LOCK(name)`.
    IsFree {
        /// The normalized lock name.
        name: String,
    },
    /// `IS_USED_LOCK(name)`.
    IsUsed {
        /// The normalized lock name.
        name: String,
    },
}

/// What a lock operation evaluates to: the integer MySQL would return, or
/// `None` for SQL `NULL`.
///
/// * `Get`: `Some(1)` obtained, `Some(0)` timed out.
/// * `Release`: `Some(1)` released, `Some(0)` held by another session, `None`
///   if nobody holds the name.
/// * `ReleaseAll`: `Some(n)`, the number of acquisitions released (a name held
///   three times counts three, as in MySQL).
/// * `IsFree`: `Some(1)` free, `Some(0)` held.
/// * `IsUsed`: `Some(holder session id)` or `None` if free.
pub type UserLockReply = Option<i64>;

/// A lock that is currently held.
struct Held {
    /// [`LockSession::id`] of the holder.
    owner: u64,
    /// Re-entrant acquisition count.
    count: u32,
    /// Woken (`notify_waiters`) when the holder releases the last count.
    released: Arc<Notify>,
}

#[derive(Default)]
struct Inner {
    /// namespace → name → holder.
    locks: HashMap<Arc<str>, HashMap<String, Held>>,
    /// session id → names it holds (in its own namespace), so a session's
    /// locks can be released without scanning every lock in the process.
    by_session: HashMap<u64, HashSet<String>>,
}

/// The process-wide (or per-listener — the embedder decides) table of held
/// named locks. Cheap to share; hand `Arc` clones to every scope that must
/// exclude against the others.
#[derive(Default)]
pub struct UserLockRegistry {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
}

impl UserLockRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Start a session in `namespace`. The session owns the locks it takes
    /// and releases them when dropped.
    #[must_use]
    pub fn session(self: &Arc<Self>, namespace: impl Into<Arc<str>>) -> LockSession {
        LockSession {
            registry: Arc::clone(self),
            namespace: namespace.into(),
            // Starts at 1: `IS_USED_LOCK` reports the holder's id, and 0 reads
            // like "nobody" in the clients that print it.
            id: self.next_id.fetch_add(1, Ordering::Relaxed) + 1,
        }
    }

    /// Number of names currently held in `namespace`. For tests and metrics.
    #[must_use]
    pub fn held_in(&self, namespace: &str) -> usize {
        self.inner
            .lock()
            .locks
            .get(namespace)
            .map_or(0, HashMap::len)
    }

    /// Release every lock `session` holds; returns the acquisition count
    /// released. Wakes any waiter on each name.
    fn release_all(&self, session: u64, namespace: &str) -> u64 {
        let mut inner = self.inner.lock();
        let Some(names) = inner.by_session.remove(&session) else {
            return 0;
        };
        let mut released = 0u64;
        let Some(ns) = inner.locks.get_mut(namespace) else {
            return 0;
        };
        for name in names {
            if let Some(held) = ns.remove(&name) {
                released += u64::from(held.count);
                held.released.notify_waiters();
            }
        }
        if ns.is_empty() {
            inner.locks.remove(namespace);
        }
        released
    }
}

/// One session's view of a [`UserLockRegistry`]: the owner of the locks it
/// takes. Dropping it releases them all — this is what ties a lock's lifetime
/// to the connection that holds it.
pub struct LockSession {
    registry: Arc<UserLockRegistry>,
    namespace: Arc<str>,
    id: u64,
}

impl LockSession {
    /// This session's id, the value `IS_USED_LOCK` reports for locks it holds.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The namespace this session's names live in.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Perform `op` for this session. Infallible: every outcome MySQL defines
    /// is a value, never an error. Only `Get` can wait.
    pub async fn apply(&self, op: &UserLockOp) -> UserLockReply {
        match op {
            UserLockOp::Get { name, timeout } => self.acquire(name, *timeout).await,
            UserLockOp::Release { name } => self.release(name),
            UserLockOp::ReleaseAll => i64::try_from(self.release_all()).ok(),
            UserLockOp::IsFree { name } => Some(i64::from(!self.is_held(name))),
            UserLockOp::IsUsed { name } => {
                self.holder_of(name).and_then(|id| i64::try_from(id).ok())
            }
        }
    }

    /// `GET_LOCK`: `Some(1)` when obtained, `Some(0)` when `timeout` expired
    /// while another session held the name.
    ///
    /// The wait is wakeup-driven. The `Notified` future is created **and
    /// enabled** while the registry mutex is held, so a holder that releases
    /// (and calls `notify_waiters`) between this session observing the name as
    /// held and starting to wait cannot slip past unnoticed: `notify_waiters`
    /// wakes every future enabled before the call, and enabling happened
    /// under the same mutex the release takes.
    pub async fn acquire(&self, name: &str, timeout: LockTimeout) -> UserLockReply {
        let deadline = match timeout {
            LockTimeout::Wait(d) => Some(Instant::now() + d),
            LockTimeout::Immediate | LockTimeout::Forever => None,
        };
        loop {
            let notify = {
                let mut guard = self.registry.inner.lock();
                match try_acquire(&mut guard, &self.namespace, name, self.id) {
                    Ok(()) => return Some(1),
                    Err(notify) => notify,
                }
            };
            if matches!(timeout, LockTimeout::Immediate) {
                return Some(0);
            }
            // Register the wait, then confirm under the mutex that the lock
            // is still the one we saw. A release in between removed (or
            // replaced) the entry and already ran its `notify_waiters`, which
            // this future — enabled too late — would never see; retrying is
            // the correct answer there. Once confirmed, any later release
            // necessarily calls `notify_waiters` after `enable()`, so the
            // wakeup cannot be missed. (The guards live in blocks so none is
            // held across the await below.)
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let still_that_lock = {
                let guard = self.registry.inner.lock();
                guard
                    .locks
                    .get(&*self.namespace)
                    .and_then(|ns| ns.get(name))
                    .is_some_and(|held| Arc::ptr_eq(&held.released, &notify))
            };
            if !still_that_lock {
                continue;
            }

            match deadline {
                None => notified.await,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Some(0);
                    }
                    if tokio::time::timeout(deadline - now, notified)
                        .await
                        .is_err()
                    {
                        return Some(0);
                    }
                }
            }
        }
    }

    /// `RELEASE_LOCK`: `Some(1)` released one acquisition, `Some(0)` held by
    /// another session (nothing released), `None` nobody holds the name.
    pub fn release(&self, name: &str) -> UserLockReply {
        let mut inner = self.registry.inner.lock();
        let inner = &mut *inner;
        let ns = inner.locks.get_mut(&*self.namespace)?;
        let held = ns.get_mut(name)?;
        if held.owner != self.id {
            return Some(0);
        }
        held.count -= 1;
        if held.count == 0 {
            let held = ns.remove(name).expect("entry was just found");
            if ns.is_empty() {
                inner.locks.remove(&*self.namespace);
            }
            if let Some(names) = inner.by_session.get_mut(&self.id) {
                names.remove(name);
                if names.is_empty() {
                    inner.by_session.remove(&self.id);
                }
            }
            held.released.notify_waiters();
        }
        Some(1)
    }

    /// `RELEASE_ALL_LOCKS`: release everything this session holds; returns
    /// the number of acquisitions released.
    pub fn release_all(&self) -> u64 {
        self.registry.release_all(self.id, &self.namespace)
    }

    /// Whether some session holds `name` in this namespace.
    #[must_use]
    pub fn is_held(&self, name: &str) -> bool {
        self.holder_of(name).is_some()
    }

    /// The id of the session holding `name`, if any.
    #[must_use]
    pub fn holder_of(&self, name: &str) -> Option<u64> {
        let inner = self.registry.inner.lock();
        inner
            .locks
            .get(&*self.namespace)?
            .get(name)
            .map(|h| h.owner)
    }
}

impl Drop for LockSession {
    /// Releases everything the session holds: the auto-release-on-disconnect
    /// guarantee. Nothing here can block or fail.
    fn drop(&mut self) {
        self.release_all();
    }
}

/// Take `name` for `session` if it is free or already ours; otherwise hand
/// back the holder's notify to wait on.
fn try_acquire(
    inner: &mut Inner,
    namespace: &Arc<str>,
    name: &str,
    session: u64,
) -> Result<(), Arc<Notify>> {
    let ns = inner.locks.entry(Arc::clone(namespace)).or_default();
    if let Some(held) = ns.get_mut(name) {
        if held.owner == session {
            held.count += 1;
            return Ok(());
        }
        return Err(Arc::clone(&held.released));
    }
    ns.insert(
        name.to_string(),
        Held {
            owner: session,
            count: 1,
            released: Arc::new(Notify::new()),
        },
    );
    inner
        .by_session
        .entry(session)
        .or_default()
        .insert(name.to_string());
    Ok(())
}

// ---------------------------------------------------------------------------
// Wrappers: give a backend / connection a lock scope.
// ---------------------------------------------------------------------------

/// A [`Backend`] whose every connection carries a [`LockSession`] in one
/// namespace, so statements on those connections can use the lock functions.
///
/// The connection's lock session is dropped with the connection, which is
/// what releases a disconnected client's locks.
pub struct UserLockScoped {
    inner: crate::SharedBackend,
    registry: Arc<UserLockRegistry>,
    namespace: Arc<str>,
}

impl UserLockScoped {
    /// Scope every connection of `inner` to `namespace` in `registry`.
    ///
    /// Scope the **outermost** backend an application reaches, once: two
    /// scopes on one path would answer from two different registries and the
    /// locks would not exclude each other.
    #[must_use]
    pub fn new(
        inner: crate::SharedBackend,
        registry: &Arc<UserLockRegistry>,
        namespace: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            inner,
            registry: Arc::clone(registry),
            namespace: namespace.into(),
        }
    }

    /// The backend this scope wraps.
    #[must_use]
    pub fn inner(&self) -> &crate::SharedBackend {
        &self.inner
    }
}

#[async_trait::async_trait]
impl Backend for UserLockScoped {
    async fn connect(&self) -> Result<Box<dyn BackendConn>, BackendError> {
        let conn = self.inner.connect().await?;
        Ok(scoped_conn(
            conn,
            &self.registry,
            Arc::clone(&self.namespace),
        ))
    }

    // The stateless shortcuts go straight to the inner backend: a throw-away
    // session has no business taking locks, and routing through `connect`
    // would allocate a lock session per call for nothing.
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        self.inner.query(sql, params).await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        self.inner.execute(sql, params).await
    }

    async fn describe_columns(&self, sql: &str) -> Result<Vec<Column>, BackendError> {
        self.inner.describe_columns(sql).await
    }
}

/// Give one connection a lock scope: `namespace` in `registry`. The returned
/// connection answers [`BackendConn::user_lock`] from the registry and
/// forwards everything else to `inner`.
#[must_use]
pub fn scoped_conn(
    inner: Box<dyn BackendConn>,
    registry: &Arc<UserLockRegistry>,
    namespace: impl Into<Arc<str>>,
) -> Box<dyn BackendConn> {
    Box::new(LockScopedConn {
        inner,
        locks: registry.session(namespace),
    })
}

struct LockScopedConn {
    inner: Box<dyn BackendConn>,
    locks: LockSession,
}

#[async_trait::async_trait]
impl BackendConn for LockScopedConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet, BackendError> {
        self.inner.query(sql, params).await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, BackendError> {
        self.inner.execute(sql, params).await
    }

    async fn describe_columns(&self, sql: &str) -> Result<Vec<Column>, BackendError> {
        self.inner.describe_columns(sql).await
    }

    async fn user_lock(&self, op: &UserLockOp) -> Result<UserLockReply, BackendError> {
        Ok(self.locks.apply(op).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(name: &str, timeout: LockTimeout) -> UserLockOp {
        UserLockOp::Get {
            name: name.into(),
            timeout,
        }
    }

    #[tokio::test]
    async fn exclusive_across_sessions_and_reentrant_within_one() {
        let registry = UserLockRegistry::new();
        let a = registry.session("");
        let b = registry.session("");

        assert_eq!(a.apply(&get("x", LockTimeout::Immediate)).await, Some(1));
        assert_eq!(
            a.apply(&get("x", LockTimeout::Immediate)).await,
            Some(1),
            "re-entrant"
        );
        assert_eq!(
            b.apply(&get("x", LockTimeout::Immediate)).await,
            Some(0),
            "held by a"
        );
        assert_eq!(
            b.apply(&UserLockOp::IsFree { name: "x".into() }).await,
            Some(0)
        );
        assert_eq!(
            b.apply(&UserLockOp::IsUsed { name: "x".into() }).await,
            Some(i64::try_from(a.id()).unwrap())
        );

        // B cannot release A's lock; A must release twice.
        assert_eq!(
            b.apply(&UserLockOp::Release { name: "x".into() }).await,
            Some(0)
        );
        assert_eq!(
            a.apply(&UserLockOp::Release { name: "x".into() }).await,
            Some(1)
        );
        assert_eq!(
            b.apply(&get("x", LockTimeout::Immediate)).await,
            Some(0),
            "still held once"
        );
        assert_eq!(
            a.apply(&UserLockOp::Release { name: "x".into() }).await,
            Some(1)
        );
        assert_eq!(
            a.apply(&UserLockOp::Release { name: "x".into() }).await,
            None,
            "not held"
        );
        assert_eq!(b.apply(&get("x", LockTimeout::Immediate)).await, Some(1));
    }

    #[tokio::test]
    async fn release_all_counts_every_acquisition_and_wakes_waiters() {
        let registry = UserLockRegistry::new();
        let a = registry.session("");
        for _ in 0..3 {
            a.apply(&get("x", LockTimeout::Immediate)).await;
        }
        a.apply(&get("y", LockTimeout::Immediate)).await;
        assert_eq!(a.apply(&UserLockOp::ReleaseAll).await, Some(4));
        assert_eq!(a.apply(&UserLockOp::ReleaseAll).await, Some(0));
        assert_eq!(registry.held_in(""), 0);
    }

    /// A waiter is woken by the release, not by a timer: with the clock
    /// paused, the only way the wait can end before the timeout is a wakeup.
    #[tokio::test(start_paused = true)]
    async fn a_timed_wait_is_woken_by_the_release() {
        let registry = UserLockRegistry::new();
        let a = Arc::new(registry.session(""));
        let b = registry.session("");
        assert_eq!(a.apply(&get("x", LockTimeout::Immediate)).await, Some(1));

        let holder = Arc::clone(&a);
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            holder.release("x");
        });
        let started = Instant::now();
        let got = b
            .apply(&get("x", LockTimeout::Wait(Duration::from_secs(30))))
            .await;
        assert_eq!(got, Some(1));
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_secs(2) && waited < Duration::from_secs(3),
            "woken at {waited:?}, expected right after the 2s release"
        );
        releaser.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_wait_gives_up_at_the_timeout() {
        let registry = UserLockRegistry::new();
        let a = registry.session("");
        let b = registry.session("");
        a.apply(&get("x", LockTimeout::Immediate)).await;
        let started = Instant::now();
        let got = b
            .apply(&get("x", LockTimeout::Wait(Duration::from_secs(3))))
            .await;
        assert_eq!(got, Some(0));
        assert!(started.elapsed() >= Duration::from_secs(3));
        assert!(
            a.is_held("x"),
            "a timed-out waiter must not disturb the holder"
        );
    }

    /// Dropping the session — what every disconnect path does — frees its
    /// locks and wakes whoever was waiting for them.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_holder_releases_and_wakes() {
        let registry = UserLockRegistry::new();
        let a = registry.session("");
        let b = registry.session("");
        a.apply(&get("x", LockTimeout::Immediate)).await;
        a.apply(&get("y", LockTimeout::Immediate)).await;

        let dropper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(a);
        });
        assert_eq!(b.apply(&get("x", LockTimeout::Forever)).await, Some(1));
        assert_eq!(b.apply(&get("y", LockTimeout::Immediate)).await, Some(1));
        dropper.await.unwrap();
        assert_eq!(registry.held_in(""), 2);
        drop(b);
        assert_eq!(registry.held_in(""), 0);
    }

    /// The same name in two namespaces is two locks.
    #[tokio::test]
    async fn namespaces_do_not_see_each_other() {
        let registry = UserLockRegistry::new();
        let tenant_a = registry.session("site-a");
        let tenant_b = registry.session("site-b");
        assert_eq!(
            tenant_a.apply(&get("job", LockTimeout::Immediate)).await,
            Some(1)
        );
        assert_eq!(
            tenant_b
                .apply(&UserLockOp::IsFree { name: "job".into() })
                .await,
            Some(1)
        );
        assert_eq!(
            tenant_b.apply(&get("job", LockTimeout::Immediate)).await,
            Some(1)
        );
        assert_eq!(
            tenant_b
                .apply(&UserLockOp::Release { name: "job".into() })
                .await,
            Some(1)
        );
        assert!(
            tenant_a.is_held("job"),
            "releasing b's lock must not touch a's"
        );
    }

    /// Several waiters on one name: each release lets exactly one through,
    /// and nobody is left waiting on a stale notify once the lock is free.
    #[tokio::test]
    async fn waiters_are_served_one_at_a_time() {
        let registry = UserLockRegistry::new();
        let holder = Arc::new(registry.session(""));
        holder.apply(&get("x", LockTimeout::Immediate)).await;

        let mut waiters = Vec::new();
        for _ in 0..4 {
            let session = registry.session("");
            waiters.push(tokio::spawn(async move {
                let got = session
                    .acquire("x", LockTimeout::Wait(Duration::from_secs(10)))
                    .await;
                // Hold briefly, then release so the next waiter proceeds.
                tokio::time::sleep(Duration::from_millis(5)).await;
                session.release("x");
                got
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        holder.release("x");
        for w in waiters {
            assert_eq!(w.await.unwrap(), Some(1));
        }
        assert_eq!(registry.held_in(""), 0);
    }

    #[test]
    fn timeout_from_seconds() {
        assert_eq!(LockTimeout::from_seconds(-1), LockTimeout::Forever);
        assert_eq!(LockTimeout::from_seconds(0), LockTimeout::Immediate);
        assert_eq!(
            LockTimeout::from_seconds(7),
            LockTimeout::Wait(Duration::from_secs(7))
        );
    }
}
