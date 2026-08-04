//! Write admission control for single-writer backends.
//!
//! # The problem
//!
//! SQLite allows exactly one writer at a time. When litewire's Hrana
//! backend fronts a `sqld` server, every statement is an HTTP round trip
//! and every *write* contends for that one lock inside sqld. Past a
//! handful of concurrent writers sqld's queueing degrades badly: writers
//! pile up, `SQLITE_BUSY` escapes to clients, and throughput *collapses*
//! rather than plateauing.
//!
//! The fix is not to make SQLite concurrent -- it is not -- but to stop
//! *offering* it more concurrent writes than it can absorb. A semaphore in
//! front of the write path lets `n` writes proceed and parks the rest in a
//! FIFO queue. The single-writer ceiling is unchanged; what changes is that
//! the excess waits in an orderly line on this side of the wire instead of
//! thrashing on the far side.
//!
//! # Queue, never refuse
//!
//! Waiters park on a [`tokio::sync::Semaphore`], which is FIFO-fair: no
//! stampede when a permit frees, and no starvation of an unlucky session.
//! A waiter is only ever failed by the *timeout*, which exists so a wedged
//! sqld surfaces as an error the client can act on rather than as a hang.
//!
//! # Reads never take a permit
//!
//! SQLite's WAL mode lets readers run concurrently with the writer, so
//! admitting reads would throttle traffic that was never the problem. A
//! read storm proceeds at full rate while every permit is held.
//!
//! # Transactions hold their permit
//!
//! This is the correctness crux. An explicit transaction's write lock lives
//! from its first write until `COMMIT`, so its permit must too:
//!
//! * releasing between statements would let another writer interleave into
//!   the same sqld lock window -- reintroducing exactly the contention the
//!   semaphore exists to prevent; and
//! * a `COMMIT` that had to *acquire* a permit would deadlock whenever all
//!   permits were held by transactions waiting to commit.
//!
//! So `COMMIT`/`ROLLBACK` never acquire -- they only release. Acquisition
//! is **lazy**: a plain (deferred) `BEGIN` takes no permit, because SQLite
//! itself takes no write lock until the transaction's first write. A
//! read-only transaction -- the shape ORMs emit constantly -- therefore
//! costs no permit at all. `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE` *do* take
//! the write lock up front, so those acquire eagerly.
//!
//! # Statement lifetime vs. session lifetime
//!
//! [`SessionAdmission::admit`] returns `Some(permit)` when the permit's
//! life ends with *this statement* (an autocommit write, or the `COMMIT`
//! that closes a transaction) and `None` when no permit is involved or when
//! the permit is parked in the session and outlives the statement. The
//! caller holds the returned permit across the round trip and drops it
//! after -- so the permit covers the commit round trip, not just the send.
//!
//! A permit parked in the session is owned by the session, so **dropping
//! the connection mid-transaction releases it** with no explicit cleanup
//! path to forget -- including on panic or on a client that vanished.

use std::sync::Arc;
use std::time::Duration;

use litewire_translate::{StatementKind, classify};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::BackendError;

/// Default ceiling on how long a write waits for a permit before failing.
///
/// Deliberately generous: this is a backstop against a wedged server, not
/// a load-shedding control. Under healthy load the queue drains long
/// before it matters.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// What a statement does to a session's write-admission state.
///
/// Derived from [`litewire_translate::classify`], refined for the
/// distinctions a transaction-aware permit needs and `classify` does not
/// draw -- notably `BEGIN` vs `COMMIT` (both `StatementKind::Transaction`)
/// and `ROLLBACK` vs `ROLLBACK TO` (only the first ends a transaction).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteIntent {
    /// A pure read. Never takes a permit.
    Read,
    /// A write. Takes a permit -- for the statement if in autocommit, for
    /// the rest of the transaction if one is open.
    Write,
    /// `BEGIN` / `START TRANSACTION`. Deferred, so no permit yet.
    BeginDeferred,
    /// `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`. Takes SQLite's write lock at
    /// once, so it acquires at once.
    BeginImmediate,
    /// `SAVEPOINT x`. Opens (or nests inside) transaction scope without
    /// taking a write lock.
    Savepoint,
    /// `RELEASE [SAVEPOINT] x`. Closes one savepoint level; closing the
    /// outermost one when no `BEGIN` is outstanding commits.
    ReleaseSavepoint,
    /// `ROLLBACK TO [SAVEPOINT] x`. Unwinds to a savepoint but leaves the
    /// transaction -- and therefore the write lock, and therefore the
    /// permit -- in place.
    RollbackTo,
    /// `COMMIT` / `END` / bare `ROLLBACK`. Ends the transaction.
    EndTransaction,
    /// Anything with no bearing on the write lock (`SET`, `USE`, ...).
    Neutral,
}

impl WriteIntent {
    /// Classify a statement of already-translated SQLite SQL.
    ///
    /// # Conservative handling of `StatementKind::Other`
    ///
    /// `classify` keys on the first word, so several real writes land in
    /// `Other`: a data-modifying CTE (`WITH x AS (...) INSERT ...`), and
    /// `VACUUM` / `ANALYZE` / `REINDEX`. Those are treated as writes here.
    /// The cost is that a *read-only* `WITH ... SELECT` also takes a
    /// permit; the alternative -- letting a data-modifying CTE bypass
    /// admission entirely -- would silently defeat the mechanism for any
    /// application that uses one. Over-admitting is a throughput
    /// nuisance; under-admitting is the bug this module exists to fix.
    #[must_use]
    pub fn of(sql: &str) -> Self {
        match classify(sql) {
            StatementKind::Query => Self::Read,
            StatementKind::Mutation | StatementKind::Ddl => Self::Write,
            StatementKind::Transaction => Self::transaction_intent(sql),
            StatementKind::Other => Self::other_intent(sql),
        }
    }

    /// Refine a `StatementKind::Transaction` into its effect on the
    /// transaction stack.
    fn transaction_intent(sql: &str) -> Self {
        let mut words = sql.trim().split_ascii_whitespace();
        let first = words.next().unwrap_or_default().to_ascii_uppercase();
        let second = words.next().unwrap_or_default().to_ascii_uppercase();

        match first.as_str() {
            "BEGIN" => match second.as_str() {
                "IMMEDIATE" | "EXCLUSIVE" => Self::BeginImmediate,
                _ => Self::BeginDeferred,
            },
            "START" => Self::BeginDeferred,
            "SAVEPOINT" => Self::Savepoint,
            "RELEASE" => Self::ReleaseSavepoint,
            // `ROLLBACK TO [SAVEPOINT] x` keeps the transaction open. Only
            // a bare `ROLLBACK` ends it. Getting this backwards would drop
            // the permit while the session still held sqld's write lock.
            "ROLLBACK" if second == "TO" => Self::RollbackTo,
            _ => Self::EndTransaction,
        }
    }

    /// Decide whether a statement `classify` could not place is a write.
    /// See the note on [`WriteIntent::of`].
    ///
    /// `END` is here rather than in [`Self::transaction_intent`] because
    /// `classify` does not recognise it as transaction control at all --
    /// its keyword list has `COMMIT` but not `COMMIT`'s SQLite synonym.
    /// Leaving it as neutral would be a **permit leak**: a session that
    /// wrote inside `BEGIN ... END` would park a permit and never give it
    /// back until the connection closed. `END` is only ever a statement's
    /// first word as transaction control (the `END` that closes a `CASE`
    /// is mid-expression), so keying on it is safe.
    fn other_intent(sql: &str) -> Self {
        let first = sql
            .trim()
            .split_ascii_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();

        match first.as_str() {
            "WITH" | "VACUUM" | "ANALYZE" | "REINDEX" => Self::Write,
            "END" => Self::EndTransaction,
            _ => Self::Neutral,
        }
    }

    /// Whether this statement needs a permit before it may run.
    #[must_use]
    pub fn needs_permit(self) -> bool {
        matches!(self, Self::Write | Self::BeginImmediate)
    }
}

/// Shared write-permit pool. Cheap to clone; every clone shares one
/// semaphore.
///
/// Construct via [`WriteAdmission::new`], which returns `None` for a
/// permit count of `0` so that "disabled" is representable as `Option::None`
/// and the whole mechanism can be skipped without a branch on a count.
#[derive(Clone, Debug)]
pub struct WriteAdmission {
    sem: Arc<Semaphore>,
    acquire_timeout: Duration,
    permits: usize,
}

impl WriteAdmission {
    /// Build a pool of `permits` concurrent writes.
    ///
    /// Returns `None` when `permits == 0`, meaning admission control is
    /// disabled and writes proceed exactly as they did before this module
    /// existed.
    #[must_use]
    pub fn new(permits: usize, acquire_timeout: Duration) -> Option<Self> {
        (permits > 0).then(|| Self {
            sem: Arc::new(Semaphore::new(permits)),
            acquire_timeout,
            permits,
        })
    }

    /// The configured permit count.
    #[must_use]
    pub fn permits(&self) -> usize {
        self.permits
    }

    /// Permits not currently held. Primarily for tests and diagnostics.
    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    /// Wait for a write permit.
    ///
    /// # Errors
    ///
    /// Returns a `SQLITE_BUSY`-shaped [`BackendError::Sqlite`] if no permit
    /// becomes available within the configured timeout. The message is
    /// shaped that way on purpose: litewire's wire frontends map
    /// `SQLITE_BUSY` onto each protocol's retriable lock error (MySQL 1205
    /// "lock wait timeout"), which is what a client should see for "the
    /// write queue never drained", and is a far better outcome than a hang.
    ///
    /// # Cancellation
    ///
    /// Cancel-safe. `Semaphore::acquire_owned` hands out a permit only on
    /// completion, and dropping the future before then leaves the count
    /// untouched; dropping it at completion drops the permit, which returns
    /// it. Neither path leaks.
    async fn acquire(&self) -> Result<OwnedSemaphorePermit, BackendError> {
        let wait = Arc::clone(&self.sem).acquire_owned();
        match tokio::time::timeout(self.acquire_timeout, wait).await {
            Ok(Ok(permit)) => Ok(permit),
            // The semaphore is owned by this struct and never closed, so
            // this arm is unreachable in practice -- reported rather than
            // panicked on, since a panic here would poison a request path.
            Ok(Err(_)) => Err(BackendError::Other(
                "write admission semaphore closed".to_string(),
            )),
            Err(_) => Err(BackendError::Sqlite(format!(
                "[SQLITE_BUSY] timed out after {:?} waiting for a write permit \
                 ({} concurrent writes allowed)",
                self.acquire_timeout, self.permits
            ))),
        }
    }
}

/// Per-session view of the shared pool, tracking that session's
/// transaction depth and the permit it may be holding.
///
/// One of these belongs to each `BackendConn`. When `admission` is `None`
/// every operation is a single predictable branch and nothing else -- no
/// lock, no classification, no allocation.
#[derive(Debug)]
pub struct SessionAdmission {
    admission: Option<WriteAdmission>,
    state: Mutex<SessionState>,
}

/// A session's transaction stack and parked permit.
///
/// The permit lives here (rather than in a guard the caller must remember
/// to return) so that dropping the session releases it, however the session
/// ends.
#[derive(Debug, Default)]
struct SessionState {
    /// An explicit `BEGIN` / `START TRANSACTION` is outstanding.
    begun: bool,
    /// Open savepoint nesting depth.
    savepoints: u32,
    /// Permit held for the rest of the current transaction.
    permit: Option<OwnedSemaphorePermit>,
}

impl SessionState {
    /// Whether any explicit transaction scope is open.
    fn in_transaction(&self) -> bool {
        self.begun || self.savepoints > 0
    }
}

impl SessionAdmission {
    /// Build a session view. `admission` of `None` disables the mechanism
    /// for this session.
    #[must_use]
    pub fn new(admission: Option<WriteAdmission>) -> Self {
        Self {
            admission,
            state: Mutex::new(SessionState::default()),
        }
    }

    /// Whether admission control is active for this session.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.admission.is_some()
    }

    /// Admit `sql` to run, blocking until a permit is free if it needs one.
    ///
    /// The returned permit, when `Some`, must be held for the duration of
    /// the statement and dropped after it completes -- so that the permit
    /// covers the whole round trip, including the response. `None` means
    /// either that no permit is needed or that the permit for this
    /// statement is parked in the session and outlives it.
    ///
    /// # Errors
    ///
    /// Propagates the acquire timeout from [`WriteAdmission::acquire`].
    /// A timeout leaves the session's transaction state untouched: the
    /// transaction is still open and a later write may retry.
    pub async fn admit(&self, sql: &str) -> Result<Option<OwnedSemaphorePermit>, BackendError> {
        // Disabled: the entire mechanism, classification included, is out
        // of the path.
        let Some(admission) = self.admission.as_ref() else {
            return Ok(None);
        };

        let intent = WriteIntent::of(sql);
        let mut state = self.state.lock().await;

        match intent {
            WriteIntent::Read | WriteIntent::Neutral | WriteIntent::RollbackTo => Ok(None),

            WriteIntent::BeginDeferred => {
                state.begun = true;
                Ok(None)
            }

            WriteIntent::BeginImmediate => {
                state.begun = true;
                if state.permit.is_none() {
                    state.permit = Some(admission.acquire().await?);
                }
                Ok(None)
            }

            WriteIntent::Savepoint => {
                state.savepoints = state.savepoints.saturating_add(1);
                Ok(None)
            }

            WriteIntent::Write => {
                if state.in_transaction() {
                    // Park it: the write lock is held until COMMIT, so the
                    // permit must be too.
                    if state.permit.is_none() {
                        state.permit = Some(admission.acquire().await?);
                    }
                    Ok(None)
                } else {
                    // Autocommit: the caller holds it for this statement.
                    Ok(Some(admission.acquire().await?))
                }
            }

            WriteIntent::ReleaseSavepoint => {
                state.savepoints = state.savepoints.saturating_sub(1);
                if state.in_transaction() {
                    Ok(None)
                } else {
                    // Releasing the outermost savepoint with no BEGIN
                    // outstanding commits. Hand the permit back so it
                    // survives the RELEASE round trip.
                    Ok(state.permit.take())
                }
            }

            WriteIntent::EndTransaction => {
                state.begun = false;
                state.savepoints = 0;
                // Handing the permit to the caller -- rather than dropping
                // it here -- keeps it held across the COMMIT round trip.
                // sqld does not release the write lock until the commit
                // lands, so releasing on entry would let the next writer
                // in during exactly the window we are protecting.
                Ok(state.permit.take())
            }
        }
    }

    /// Whether this session currently has a permit parked for an open
    /// transaction. Test and diagnostic use.
    pub async fn holds_permit(&self) -> bool {
        self.state.lock().await.permit.is_some()
    }

    /// Whether this session has explicit transaction scope open. Test and
    /// diagnostic use.
    pub async fn in_transaction(&self) -> bool {
        self.state.lock().await.in_transaction()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(permits: usize) -> Option<WriteAdmission> {
        WriteAdmission::new(permits, DEFAULT_ACQUIRE_TIMEOUT)
    }

    // -- WriteIntent classification ----------------------------------------

    #[test]
    fn reads_are_reads() {
        for sql in [
            "SELECT * FROM t",
            "  select 1",
            "PRAGMA table_info('t')",
            "EXPLAIN SELECT 1",
        ] {
            assert_eq!(WriteIntent::of(sql), WriteIntent::Read, "{sql}");
            assert!(!WriteIntent::of(sql).needs_permit(), "{sql}");
        }
    }

    #[test]
    fn mutations_and_ddl_are_writes() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "update t set a = 1",
            "DELETE FROM t",
            "REPLACE INTO t VALUES (1)",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
        ] {
            assert_eq!(WriteIntent::of(sql), WriteIntent::Write, "{sql}");
            assert!(WriteIntent::of(sql).needs_permit(), "{sql}");
        }
    }

    #[test]
    fn begin_is_deferred_but_immediate_and_exclusive_are_not() {
        assert_eq!(WriteIntent::of("BEGIN"), WriteIntent::BeginDeferred);
        assert_eq!(
            WriteIntent::of("START TRANSACTION"),
            WriteIntent::BeginDeferred
        );
        assert_eq!(
            WriteIntent::of("BEGIN DEFERRED"),
            WriteIntent::BeginDeferred
        );
        assert_eq!(
            WriteIntent::of("BEGIN IMMEDIATE"),
            WriteIntent::BeginImmediate
        );
        assert_eq!(
            WriteIntent::of("begin exclusive"),
            WriteIntent::BeginImmediate
        );
        assert!(WriteIntent::of("BEGIN IMMEDIATE").needs_permit());
        assert!(!WriteIntent::of("BEGIN").needs_permit());
    }

    /// `ROLLBACK TO x` leaves the transaction open; only a bare `ROLLBACK`
    /// ends it. Conflating the two would release the permit while the
    /// session still held sqld's write lock.
    #[test]
    fn rollback_to_savepoint_is_not_an_end_of_transaction() {
        assert_eq!(WriteIntent::of("ROLLBACK"), WriteIntent::EndTransaction);
        assert_eq!(WriteIntent::of("COMMIT"), WriteIntent::EndTransaction);
        assert_eq!(WriteIntent::of("END"), WriteIntent::EndTransaction);
        assert_eq!(WriteIntent::of("ROLLBACK TO sp1"), WriteIntent::RollbackTo);
        assert_eq!(
            WriteIntent::of("rollback to savepoint sp1"),
            WriteIntent::RollbackTo
        );
    }

    #[test]
    fn savepoint_and_release_are_distinguished() {
        assert_eq!(WriteIntent::of("SAVEPOINT sp1"), WriteIntent::Savepoint);
        assert_eq!(
            WriteIntent::of("RELEASE SAVEPOINT sp1"),
            WriteIntent::ReleaseSavepoint
        );
        assert_eq!(
            WriteIntent::of("RELEASE sp1"),
            WriteIntent::ReleaseSavepoint
        );
    }

    #[test]
    fn data_modifying_cte_and_vacuum_are_treated_as_writes() {
        for sql in [
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
            "VACUUM",
            "ANALYZE",
            "REINDEX",
        ] {
            assert_eq!(WriteIntent::of(sql), WriteIntent::Write, "{sql}");
        }
        // And the documented cost of that conservatism.
        assert_eq!(
            WriteIntent::of("WITH x AS (SELECT 1) SELECT * FROM x"),
            WriteIntent::Write,
            "read-only CTE is conservatively admitted as a write"
        );
    }

    #[test]
    fn neutral_statements_take_no_permit() {
        for sql in ["SET foo = 1", "USE db", ""] {
            assert_eq!(WriteIntent::of(sql), WriteIntent::Neutral, "{sql:?}");
        }
    }

    // -- Disabled path ------------------------------------------------------

    #[tokio::test]
    async fn permits_zero_disables_everything() {
        assert!(WriteAdmission::new(0, DEFAULT_ACQUIRE_TIMEOUT).is_none());

        let session = SessionAdmission::new(admission(0));
        assert!(!session.is_enabled());

        // Every statement shape passes through with no permit and no state.
        for sql in [
            "BEGIN",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t VALUES (2)",
            "COMMIT",
            "SELECT 1",
        ] {
            assert!(session.admit(sql).await.unwrap().is_none(), "{sql}");
        }
        assert!(!session.holds_permit().await);
        assert!(!session.in_transaction().await);
    }

    // -- Autocommit ---------------------------------------------------------

    #[tokio::test]
    async fn autocommit_write_acquires_and_releases_per_statement() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        assert_eq!(adm.available_permits(), 1);
        let permit = session.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert!(permit.is_some(), "autocommit write must hold a permit");
        assert_eq!(adm.available_permits(), 0);
        // Nothing parked in the session -- the statement owns it.
        assert!(!session.holds_permit().await);

        drop(permit);
        assert_eq!(adm.available_permits(), 1, "permit returns when stmt ends");
    }

    #[tokio::test]
    async fn reads_never_take_a_permit() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        for _ in 0..100 {
            assert!(session.admit("SELECT * FROM t").await.unwrap().is_none());
        }
        assert_eq!(adm.available_permits(), 1);
    }

    // -- Explicit transactions ----------------------------------------------

    #[tokio::test]
    async fn permit_is_held_across_an_explicit_transaction() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        // Deferred BEGIN takes nothing.
        assert!(session.admit("BEGIN").await.unwrap().is_none());
        assert_eq!(
            adm.available_permits(),
            1,
            "deferred BEGIN must not acquire"
        );
        assert!(session.in_transaction().await);

        // First write acquires and parks.
        assert!(
            session
                .admit("INSERT INTO t VALUES (1)")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(adm.available_permits(), 0);
        assert!(session.holds_permit().await);

        // Second write reuses the parked permit -- no second acquire.
        assert!(
            session
                .admit("INSERT INTO t VALUES (2)")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(adm.available_permits(), 0);
        assert!(session.holds_permit().await);

        // A read inside the transaction changes nothing.
        assert!(session.admit("SELECT 1").await.unwrap().is_none());
        assert_eq!(adm.available_permits(), 0);

        // COMMIT hands the permit back so it covers the commit round trip.
        let commit_permit = session.admit("COMMIT").await.unwrap();
        assert!(
            commit_permit.is_some(),
            "COMMIT must carry the permit across its own round trip"
        );
        assert_eq!(adm.available_permits(), 0, "still held during COMMIT");
        assert!(!session.holds_permit().await);
        assert!(!session.in_transaction().await);

        drop(commit_permit);
        assert_eq!(adm.available_permits(), 1);
    }

    /// The headline behaviour: with one permit, a second writer cannot get
    /// in between another session's `BEGIN` and `COMMIT`.
    #[tokio::test]
    async fn second_writer_is_blocked_until_the_first_commits() {
        let adm = admission(1).unwrap();
        let a = Arc::new(SessionAdmission::new(Some(adm.clone())));
        let b = Arc::new(SessionAdmission::new(Some(adm.clone())));

        a.admit("BEGIN").await.unwrap();
        a.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert_eq!(adm.available_permits(), 0);

        let b2 = Arc::clone(&b);
        let waiter = tokio::spawn(async move { b2.admit("INSERT INTO t VALUES (9)").await });

        // Give the waiter every chance to (wrongly) succeed.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !waiter.is_finished(),
            "second writer got in mid-transaction"
        );

        a.admit("INSERT INTO t VALUES (2)").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "second writer got in mid-transaction"
        );

        let commit_permit = a.admit("COMMIT").await.unwrap();
        assert!(
            !waiter.is_finished(),
            "permit freed before COMMIT completed"
        );
        drop(commit_permit);

        let got = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("second writer never woke after COMMIT")
            .expect("waiter task panicked")
            .expect("waiter errored");
        assert!(
            got.is_some(),
            "second writer should hold an autocommit permit"
        );
    }

    /// A read storm must proceed at full rate while every permit is held.
    #[tokio::test]
    async fn reads_proceed_while_all_permits_are_held() {
        let adm = admission(2).unwrap();
        let holders: Vec<_> = {
            let mut v = Vec::new();
            for _ in 0..2 {
                let s = SessionAdmission::new(Some(adm.clone()));
                v.push(s.admit("INSERT INTO t VALUES (1)").await.unwrap());
            }
            v
        };
        assert_eq!(adm.available_permits(), 0);

        let reader = SessionAdmission::new(Some(adm.clone()));
        let storm = tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..1000 {
                reader.admit("SELECT 1").await.unwrap();
            }
        })
        .await;
        assert!(storm.is_ok(), "reads blocked behind write permits");
        drop(holders);
    }

    /// `END` is SQLite's synonym for `COMMIT`, and `classify` does not know
    /// it. Treating it as a neutral statement parks a permit forever --
    /// found by this test, not by review.
    #[tokio::test]
    async fn end_commits_and_returns_the_permit() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        session.admit("BEGIN").await.unwrap();
        session.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert_eq!(adm.available_permits(), 0);

        let p = session.admit("END").await.unwrap();
        assert!(p.is_some(), "END must commit and hand back the permit");
        assert!(!session.in_transaction().await);
        drop(p);
        assert_eq!(adm.available_permits(), 1, "END leaked a permit");
    }

    #[tokio::test]
    async fn begin_immediate_acquires_eagerly() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        assert!(session.admit("BEGIN IMMEDIATE").await.unwrap().is_none());
        assert_eq!(
            adm.available_permits(),
            0,
            "IMMEDIATE takes the lock at once"
        );
        assert!(session.holds_permit().await);

        let p = session.admit("ROLLBACK").await.unwrap();
        assert!(p.is_some());
        drop(p);
        assert_eq!(adm.available_permits(), 1);
    }

    // -- Session teardown ---------------------------------------------------

    /// Dropping a session mid-transaction must return its permit. Without
    /// this the pool bleeds a permit per abandoned transaction until every
    /// write blocks forever.
    #[tokio::test]
    async fn dropping_a_session_mid_transaction_releases_the_permit() {
        let adm = admission(1).unwrap();

        {
            let session = SessionAdmission::new(Some(adm.clone()));
            session.admit("BEGIN").await.unwrap();
            session.admit("INSERT INTO t VALUES (1)").await.unwrap();
            assert_eq!(adm.available_permits(), 0);
            assert!(session.holds_permit().await);
            // No COMMIT: the client vanished.
        }

        assert_eq!(
            adm.available_permits(),
            1,
            "permit leaked when the session dropped mid-transaction"
        );

        // And the pool is genuinely reusable afterwards.
        let next = SessionAdmission::new(Some(adm.clone()));
        let p = tokio::time::timeout(
            Duration::from_secs(5),
            next.admit("INSERT INTO t VALUES (2)"),
        )
        .await
        .expect("pool wedged after a dropped session")
        .unwrap();
        assert!(p.is_some());
    }

    // -- Savepoints ---------------------------------------------------------

    #[tokio::test]
    async fn savepoint_nesting_holds_the_permit_until_the_outermost_release() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        session.admit("SAVEPOINT sp1").await.unwrap();
        session.admit("SAVEPOINT sp2").await.unwrap();
        assert_eq!(adm.available_permits(), 1, "savepoints take no write lock");

        session.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert_eq!(adm.available_permits(), 0);

        // Inner release: transaction scope still open, permit stays.
        assert!(
            session.admit("RELEASE sp2").await.unwrap().is_none(),
            "inner RELEASE must not end the transaction"
        );
        assert_eq!(adm.available_permits(), 0);
        assert!(session.holds_permit().await);

        // Rolling back to a savepoint keeps the transaction open too.
        assert!(session.admit("ROLLBACK TO sp1").await.unwrap().is_none());
        assert_eq!(adm.available_permits(), 0);
        assert!(session.holds_permit().await);

        // Outermost release commits.
        let p = session.admit("RELEASE sp1").await.unwrap();
        assert!(
            p.is_some(),
            "outermost RELEASE commits and returns the permit"
        );
        drop(p);
        assert_eq!(adm.available_permits(), 1);
    }

    #[tokio::test]
    async fn savepoint_inside_begin_does_not_end_the_transaction_on_release() {
        let adm = admission(1).unwrap();
        let session = SessionAdmission::new(Some(adm.clone()));

        session.admit("BEGIN").await.unwrap();
        session.admit("SAVEPOINT sp1").await.unwrap();
        session.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert_eq!(adm.available_permits(), 0);

        // The BEGIN is still outstanding, so this RELEASE commits nothing.
        assert!(session.admit("RELEASE sp1").await.unwrap().is_none());
        assert_eq!(adm.available_permits(), 0, "BEGIN still open");
        assert!(session.holds_permit().await);

        let p = session.admit("COMMIT").await.unwrap();
        assert!(p.is_some());
        drop(p);
        assert_eq!(adm.available_permits(), 1);
    }

    // -- Timeout ------------------------------------------------------------

    #[tokio::test]
    async fn acquire_timeout_returns_a_busy_error_rather_than_hanging() {
        let adm = WriteAdmission::new(1, Duration::from_millis(100)).unwrap();
        let holder = SessionAdmission::new(Some(adm.clone()));
        let _held = holder.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert_eq!(adm.available_permits(), 0);

        let waiter = SessionAdmission::new(Some(adm.clone()));
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            waiter.admit("INSERT INTO t VALUES (2)"),
        )
        .await
        .expect("admit hung past its own timeout")
        .expect_err("expected a timeout error");

        // Must be SQLITE_BUSY-shaped so the wire frontends map it onto
        // each protocol's retriable lock error.
        let msg = err.to_string();
        assert!(msg.contains("SQLITE_BUSY"), "not busy-shaped: {msg}");
        assert!(matches!(err, BackendError::Sqlite(_)), "{err:?}");
    }

    #[tokio::test]
    async fn timeout_leaves_transaction_state_intact_for_a_retry() {
        let adm = WriteAdmission::new(1, Duration::from_millis(50)).unwrap();
        let holder = SessionAdmission::new(Some(adm.clone()));
        let held = holder.admit("INSERT INTO t VALUES (1)").await.unwrap();

        let session = SessionAdmission::new(Some(adm.clone()));
        session.admit("BEGIN").await.unwrap();
        assert!(session.admit("INSERT INTO t VALUES (2)").await.is_err());
        assert!(session.in_transaction().await, "transaction was torn down");
        assert!(!session.holds_permit().await);

        // Once the pool frees up, the same session's retry succeeds and
        // parks the permit as it would have the first time.
        drop(held);
        assert!(
            session
                .admit("INSERT INTO t VALUES (2)")
                .await
                .unwrap()
                .is_none()
        );
        assert!(session.holds_permit().await);
    }

    // -- Cancellation -------------------------------------------------------

    /// Dropping an `admit` future while it waits must not consume a permit.
    #[tokio::test]
    async fn cancelled_acquire_does_not_leak_a_permit() {
        let adm = admission(1).unwrap();
        let holder = SessionAdmission::new(Some(adm.clone()));
        let held = holder.admit("INSERT INTO t VALUES (1)").await.unwrap();
        assert_eq!(adm.available_permits(), 0);

        for _ in 0..50 {
            let waiter = SessionAdmission::new(Some(adm.clone()));
            let fut = waiter.admit("INSERT INTO t VALUES (2)");
            // Poll it into the semaphore's wait queue, then abandon it.
            let cancelled = tokio::time::timeout(Duration::from_millis(5), fut).await;
            assert!(cancelled.is_err(), "should still have been waiting");
        }

        drop(held);
        assert_eq!(
            adm.available_permits(),
            1,
            "cancelled waiters consumed permits"
        );

        // The pool still hands out exactly one permit.
        let next = SessionAdmission::new(Some(adm.clone()));
        let p = tokio::time::timeout(
            Duration::from_secs(5),
            next.admit("INSERT INTO t VALUES (3)"),
        )
        .await
        .expect("pool wedged after cancellations")
        .unwrap();
        assert!(p.is_some());
    }

    // -- Pool arithmetic ----------------------------------------------------

    #[tokio::test]
    async fn permit_count_is_the_concurrency_ceiling() {
        const PERMITS: usize = 4;
        let adm = admission(PERMITS).unwrap();

        let mut held = Vec::new();
        for _ in 0..PERMITS {
            let s = SessionAdmission::new(Some(adm.clone()));
            held.push(s.admit("INSERT INTO t VALUES (1)").await.unwrap());
        }
        assert_eq!(adm.available_permits(), 0);
        assert_eq!(adm.permits(), PERMITS);

        let extra = SessionAdmission::new(Some(adm.clone()));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                extra.admit("INSERT INTO t VALUES (2)")
            )
            .await
            .is_err(),
            "admitted more concurrent writes than the permit count"
        );

        held.pop();
        let p = tokio::time::timeout(
            Duration::from_secs(5),
            extra.admit("INSERT INTO t VALUES (2)"),
        )
        .await
        .expect("freed permit was not handed on")
        .unwrap();
        assert!(p.is_some());
    }
}
