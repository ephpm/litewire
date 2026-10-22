//! Keeping engine work off the async runtime's worker threads.
//!
//! The Turso Rust API is `async`, but only in name: a statement's future
//! does all of its work — VDBE steps, B-tree reads, `pwrite`, and the
//! `fsync` at commit — synchronously inside `poll`. Turso's default Unix
//! I/O backend issues each read and write as a blocking syscall at
//! submission and its `step()` is a no-op, so a future that returns
//! `Pending` has already done its I/O. Awaiting one on a tokio worker
//! therefore blocks that worker for the whole statement, exactly as a
//! synchronous call would.
//!
//! That matters more than it looks. A multi-thread runtime's I/O and timer
//! drivers are turned only by a worker that has run out of tasks and
//! parks. When every worker is inside an engine call — a few replication
//! applies, a checkpoint, a handful of wire statements — nobody turns the
//! drivers, so *every* task on the runtime stops seeing socket readiness
//! and timer expiry: unrelated HTTP accepts, heartbeats, and the signal
//! handler alike. An embedder measured a `SIGTERM` handled 20–35 s after
//! the signal under heavy per-site replication.
//!
//! [`engine`] runs a future that does engine work under
//! [`tokio::task::block_in_place`] on a multi-thread runtime: the calling
//! worker hands its scheduler core (with its queued tasks) to another
//! thread before the engine runs, so the runtime keeps a full set of
//! workers turning the drivers while the call blocks. Unlike
//! `spawn_blocking`, it needs neither `'static` nor `Send` data, so callers
//! keep borrowing their connection, statement, and session state.
//!
//! Where `block_in_place` is not allowed — a current-thread runtime (the
//! default for `#[tokio::test]`), or no runtime at all — the future is
//! simply awaited in place, which is what every call did before.
//!
//! # What not to put inside
//!
//! Only engine work. The thread is a blocking-pool thread for the whole
//! call, so an `.await` inside that waits on something other than the
//! engine (a gate permit, a socket, a channel) parks a thread for nothing.
//! Acquire the WAL gate permit, the session mutex, and anything else that
//! waits on another task *before* calling [`engine`].
//!
//! # Bound
//!
//! Each call in flight occupies one tokio blocking-pool thread (512 by
//! default), on top of whatever else the embedder runs there. Concurrency
//! is bounded by the callers: one statement per session (the session
//! mutex), one apply per replication link, one checkpoint per database.
//! A separate, smaller cap would be unsafe: a session holding a write
//! transaction could not get a slot to `COMMIT` while every slot is taken
//! by statements waiting (inside the engine's busy handler) for that very
//! lock.

use std::cell::Cell;
use std::future::Future;

use tokio::runtime::{Handle, RuntimeFlavor};

thread_local! {
    /// Set while this thread is driving an [`engine`] future, so a nested
    /// call awaits in place instead of starting another `block_on`.
    static OFFLOADED: Cell<bool> = const { Cell::new(false) };
}

/// Clears [`OFFLOADED`] when the outermost [`engine`] call ends, including by
/// panic.
struct OffloadedGuard;

impl OffloadedGuard {
    fn enter() -> Self {
        OFFLOADED.with(|f| f.set(true));
        Self
    }
}

impl Drop for OffloadedGuard {
    fn drop(&mut self) {
        OFFLOADED.with(|f| f.set(false));
    }
}

/// Run `work` — a future that drives Turso engine calls — without
/// blocking an async worker thread. See the [module docs](self).
///
/// On a multi-thread runtime the future is driven to completion on the
/// current thread inside [`tokio::task::block_in_place`]; anywhere else it
/// is awaited in place. Either way the output is the future's own.
///
/// Nested calls — `work` itself reaching another `engine` call, as a session
/// statement does through [`crate::cdc::enable_cdc`] — run in place on the
/// thread the outer call already moved off the runtime. That keeps an outer
/// caller's own concurrency intact: a caller that offloads a whole
/// `select!` (say, a statement raced against a heartbeat timer) still sees
/// the statement yield at the engine's `Pending` points, instead of a
/// nested `block_on` that would run it to completion first.
pub async fn engine<F: Future>(work: F) -> F::Output {
    if OFFLOADED.with(Cell::get) {
        return work.await;
    }
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(move || {
                let _offloaded = OffloadedGuard::enter();
                handle.block_on(work)
            })
        }
        _ => work.await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;

    /// A blocking engine call on the only worker must not stop another task
    /// on the same runtime from running: the core moves to another thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn engine_work_does_not_hold_the_worker() {
        let ticked = Arc::new(AtomicBool::new(false));
        let observer = {
            let ticked = Arc::clone(&ticked);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                ticked.store(true, Ordering::SeqCst);
            })
        };
        // Spawned, so the engine call runs on the runtime's single worker
        // (the test body itself is polled on the test thread, not a worker).
        let saw_tick = tokio::spawn(async move {
            engine(async {
                // Stand-in for a statement that blocks inside `poll`.
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !ticked.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(5));
                }
                ticked.load(Ordering::SeqCst)
            })
            .await
        })
        .await
        .unwrap();
        assert!(
            saw_tick,
            "the timer task could not run while engine work blocked the only worker"
        );
        observer.await.unwrap();
    }

    /// A nested call runs in place, so an outer offloaded `select!` keeps
    /// interleaving its other branches with the nested work's `Pending`
    /// points (a nested `block_on` would run the work to completion first).
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn nested_calls_keep_the_outer_select_interleaving() {
        let ticks = tokio::spawn(engine(async {
            let nested = engine(async {
                for _ in 0..20 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            });
            tokio::pin!(nested);
            let mut interval = tokio::time::interval(Duration::from_millis(1));
            let mut ticks = 0u32;
            loop {
                tokio::select! {
                    biased;
                    () = &mut nested => break,
                    _ = interval.tick() => ticks += 1,
                }
            }
            ticks
        }))
        .await
        .unwrap();
        assert!(
            ticks > 2,
            "only {ticks} ticks: the nested call ran to completion before the outer select resumed"
        );
    }

    /// On a current-thread runtime `block_in_place` would panic; the work
    /// runs in place instead.
    #[tokio::test]
    async fn current_thread_runtime_runs_in_place() {
        assert_eq!(engine(async { 7 }).await, 7);
    }

    /// Called from a non-runtime thread that entered a multi-thread runtime
    /// through `Handle::block_on` (how an embedder's synchronous callers
    /// reach the backend), the work still completes — including nested use.
    #[test]
    fn works_under_handle_block_on_from_a_plain_thread() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.handle().clone();
        let out = std::thread::spawn(move || {
            handle.block_on(async { engine(async { engine(async { 3 }).await + 1 }).await })
        })
        .join()
        .unwrap();
        assert_eq!(out, 4);
    }

    /// The nesting flag is per call: cleared when the outermost call ends,
    /// even when the work panics, so later calls on the thread offload again.
    #[test]
    fn the_nesting_flag_is_cleared_after_a_call_and_after_a_panic() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.handle().clone();
        std::thread::spawn(move || {
            handle.block_on(async {
                engine(async { assert!(OFFLOADED.with(Cell::get)) }).await;
                assert!(!OFFLOADED.with(Cell::get));
                let panicked = std::panic::AssertUnwindSafe(engine(async { panic!("boom") }));
                let caught = futures_catch(panicked).await;
                assert!(caught, "the work's panic propagates");
                assert!(!OFFLOADED.with(Cell::get));
            });
        })
        .join()
        .unwrap();
    }

    /// `catch_unwind` for a future without pulling in the `futures` crate.
    async fn futures_catch<F: Future<Output = ()>>(fut: std::panic::AssertUnwindSafe<F>) -> bool {
        let mut fut = std::pin::pin!(fut.0);
        std::future::poll_fn(|cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
                Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                Ok(std::task::Poll::Ready(())) => std::task::Poll::Ready(false),
                Err(_) => std::task::Poll::Ready(true),
            }
        })
        .await
    }
}
