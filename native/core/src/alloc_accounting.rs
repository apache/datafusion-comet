// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Process-wide accounting of the bytes currently handed out by the Rust global allocator.
//!
//! [`AccountingAllocator`] wraps the selected global allocator and maintains a single signed
//! process-wide byte balance, which [`current_balance`] exposes so it can be compared against the
//! memory pool's reservations in the executor's memory usage log and in tracing output. This is observability only: it never rejects an
//! allocation, never panics, and never gates the memory pool.
//!
//! The balance counts `Layout` bytes, not resident pages: it excludes allocator fragmentation,
//! jemalloc's retained pages, `mmap`ed regions, and anything a C dependency allocates through libc
//! `malloc` rather than Rust's `GlobalAlloc`. See the [memory management contributor guide].
//!
//! [memory management contributor guide]:
//!     https://datafusion.apache.org/comet/contributor-guide/memory_management.html

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};

/// A thread flushes its accumulated delta into the shared balance once the magnitude reaches this.
/// Batching keeps the common path to a thread-local add-and-compare, so only about one atomic
/// read-modify-write per 64 KiB of churn touches the shared cacheline.
const SETTLE_THRESHOLD: isize = 64 * 1024;

/// Outstanding bytes, process-wide. Signed because a thread can flush a negative delta before
/// another flushes the matching positive one.
static BALANCE: AtomicIsize = AtomicIsize::new(0);

/// The phases of a thread's [`ThreadState`].
///
/// A thread starts `UNREGISTERED`. Its first tracked allocation moves it through `REGISTERING`,
/// while [`SETTLE_ON_EXIT`]'s destructor is registered, to `REGISTERED`, where deltas accumulate in
/// the thread's drift. [`SettleOnExit::drop`] moves it to `EXITED` when the thread ends. In every
/// phase but `REGISTERED`, deltas go straight to the shared balance: while registering, because
/// registration can itself allocate on some platforms; after exiting, because nothing would
/// settle the drift.
const UNREGISTERED: u8 = 0;
const REGISTERING: u8 = 1;
const REGISTERED: u8 = 2;
const EXITED: u8 = 3;

/// A thread's accounting state, kept in one thread-local so that tracking an allocation costs one
/// thread-local access.
///
/// `libcomet` is a shared library that the JVM loads with `dlopen`, so its thread-locals use the
/// general-dynamic TLS model: every access calls `__tls_get_addr` in the dynamic loader. That
/// call is most of the wrapper's cost on allocation-heavy queries, so the fast path makes exactly
/// one. The state is const-initialized and has no destructor, which also spares the fast path the
/// lazy-initialization check a thread-local with a destructor needs, and leaves it readable while
/// the thread's other thread-local destructors run.
struct ThreadState {
    /// This thread's un-flushed delta.
    drift: Cell<isize>,
    /// One of the phases above.
    phase: Cell<u8>,
}

thread_local! {
    static STATE: ThreadState = const {
        ThreadState {
            drift: Cell::new(0),
            phase: Cell::new(UNREGISTERED),
        }
    };

    /// Settles the thread's remaining drift when the thread exits; see [`SettleOnExit`]. Touched
    /// once per thread, to register its destructor.
    static SETTLE_ON_EXIT: SettleOnExit = const { SettleOnExit };
}

/// Settles a thread's remaining drift when the thread exits. Without it, up to
/// [`SETTLE_THRESHOLD`] bytes of accounting would be discarded every time a thread died, and
/// tokio's blocking pool churns threads on its idle timeout.
///
/// It lives apart from [`ThreadState`] so that the state needs no destructor.
struct SettleOnExit;

impl Drop for SettleOnExit {
    fn drop(&mut self) {
        STATE.with(|state| {
            state.phase.set(EXITED);
            let drift = state.drift.replace(0);
            if drift != 0 {
                BALANCE.fetch_add(drift, Ordering::Relaxed);
            }
        });
    }
}

/// Bytes currently handed out by the Rust global allocator, process-wide.
///
/// Never reported negative: the
/// balance can dip below zero transiently while per-thread deltas settle out of order.
///
/// The value is approximate. Each live thread holds up to [`SETTLE_THRESHOLD`] bytes of
/// un-flushed delta in either direction, so the reported balance can lag the true one by up to
/// that amount times the number of live threads.
pub fn current_balance() -> usize {
    BALANCE.load(Ordering::Relaxed).max(0) as usize
}

/// Adds `delta` to `local_drift`, flushing into the shared balance once the magnitude reaches
/// [`SETTLE_THRESHOLD`].
fn settle(local_drift: &Cell<isize>, delta: isize) {
    let drift = local_drift.get().wrapping_add(delta);
    if drift.unsigned_abs() >= SETTLE_THRESHOLD as usize {
        local_drift.set(0);
        BALANCE.fetch_add(drift, Ordering::Relaxed);
    } else {
        local_drift.set(drift);
    }
}

/// Records a signed byte delta against the process balance.
#[inline]
fn track(delta: isize) {
    if delta == 0 {
        return;
    }

    STATE.with(|state| match state.phase.get() {
        REGISTERED => settle(&state.drift, delta),
        UNREGISTERED => register_and_track(state, delta),
        _ => {
            BALANCE.fetch_add(delta, Ordering::Relaxed);
        }
    })
}

/// Registers the thread's [`SettleOnExit`] destructor on its first tracked allocation, then
/// tracks `delta`.
///
/// Registering a thread-local destructor allocates through the global allocator on some
/// platforms, where the standard library keeps the thread's destructors in a list. That
/// allocation re-enters [`track`] in the `REGISTERING` phase, which settles it straight into the
/// shared balance instead of recursing. If registration fails because the
/// thread is already tearing down, the thread is treated as exited.
#[cold]
fn register_and_track(state: &ThreadState, delta: isize) {
    state.phase.set(REGISTERING);
    let registered = SETTLE_ON_EXIT.try_with(|_| ()).is_ok();
    state
        .phase
        .set(if registered { REGISTERED } else { EXITED });
    if registered {
        settle(&state.drift, delta);
    } else {
        BALANCE.fetch_add(delta, Ordering::Relaxed);
    }
}

/// Wraps a global allocator, accounting the `Layout` bytes it hands out.
///
/// Adapted from the `AccountingAllocator` in
/// [apache/datafusion#22626](https://github.com/apache/datafusion/pull/22626), which lives in
/// DataFusion's test-only `sqllogictest` crate and so cannot be depended on directly.
pub struct AccountingAllocator<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> AccountingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// SAFETY: every method delegates to `inner`, which upholds the `GlobalAlloc` contract. The
// accounting is pure bookkeeping over an `AtomicIsize` and thread-local `Cell`s: it does not
// inspect, retain, or alter any pointer, and it cannot unwind.
unsafe impl<A: GlobalAlloc> GlobalAlloc for AccountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc_zeroed(layout);
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Settle before delegating. A free cannot fail, so there is nothing to wait for, and the
        // inner free can be slow: jemalloc returns oversize blocks to the OS eagerly, and unmapping
        // a few hundred megabytes takes milliseconds. Accounting afterwards would keep the block on
        // the balance for that whole window, after the allocator's own statistics had already
        // dropped it.
        track(-(layout.size() as isize));
        self.inner.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // Accounting after the fact is only safe because this allocator cannot fail the
            // allocation or unwind. A variant that enforced a limit would have to decide *before*
            // delegating: `realloc` may free or move the old block, and a caller that never
            // received the new pointer would free the stale one while unwinding.
            //
            // A single allocation cannot exceed `isize::MAX` on any real platform, so neither cast
            // wraps.
            track(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::System;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Mutex, MutexGuard};

    /// `BALANCE` is process-wide and the crate's tests run in parallel, so a test that reads it
    /// sees every other test's allocations. The tests that move it by tens of megabytes take this
    /// lock so they cannot land inside each other's windows; the rest of the crate is kept out by
    /// making each window microseconds wide and each expected move far larger than anything else
    /// allocates in that time.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn serial() -> MutexGuard<'static, ()> {
        SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    const MIB: usize = 1024 * 1024;

    /// Slack allowed between an observed balance and the expected one, to absorb whatever the
    /// rest of the crate allocates during a test's window. It is half the smallest move any test
    /// below expects, so a wrongly ordered or wrongly sized update still lands outside it.
    const MARGIN: usize = 16 * MIB;

    fn about(actual: usize, expected: usize) -> bool {
        actual.abs_diff(expected) <= MARGIN
    }

    /// An inner allocator that records the reported balance at the moment each inner call is
    /// made, which pins down whether the wrapper accounts before or after delegating.
    struct Recording {
        balance_at_dealloc: AtomicUsize,
        balance_at_realloc: AtomicUsize,
    }

    impl Recording {
        fn new() -> Self {
            Self {
                balance_at_dealloc: AtomicUsize::new(usize::MAX),
                balance_at_realloc: AtomicUsize::new(usize::MAX),
            }
        }
    }

    unsafe impl GlobalAlloc for Recording {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            System.alloc(layout)
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            self.balance_at_dealloc
                .store(current_balance(), Ordering::Relaxed);
            System.dealloc(ptr, layout)
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            self.balance_at_realloc
                .store(current_balance(), Ordering::Relaxed);
            System.realloc(ptr, layout, new_size)
        }
    }

    #[test]
    fn settle_flushes_only_at_the_threshold() {
        for (delta, residue) in [
            (1024, 1024),
            (-1024, -1024),
            (SETTLE_THRESHOLD - 1, SETTLE_THRESHOLD - 1),
            (SETTLE_THRESHOLD, 0),
            (-SETTLE_THRESHOLD, 0),
        ] {
            let drift = Cell::new(0);
            settle(&drift, delta);
            // A flush resets the drift to zero, so the residue alone says whether the shared
            // balance was touched. Reading `BALANCE` here would race with every other test.
            assert_eq!(drift.get(), residue, "delta {delta}");
        }
    }

    /// A real allocation must move the reported balance. This is the one test that checks the
    /// wrapper is actually installed as the global allocator. The block is zeroed and never
    /// touched, so it costs address space rather than resident memory.
    #[test]
    fn a_real_allocation_raises_the_balance() {
        use std::hint::black_box;

        const SIZE: usize = 256 * MIB;
        let _guard = serial();
        let before = current_balance();
        // `black_box` keeps the allocation observable so it cannot be elided.
        let held: Vec<u8> = black_box(vec![0u8; SIZE]);
        let during = current_balance();
        black_box(&held);
        assert!(
            during >= before + SIZE / 2,
            "a {SIZE} byte allocation should raise the balance (before={before}, during={during}); \
             is the accounting wrapper installed as the global allocator?"
        );
        drop(held);
    }

    /// The balance must drop before the inner allocator is asked to free the block, because
    /// jemalloc drops its own count at the start of a large free and then spends milliseconds
    /// unmapping the pages; see the comment on `dealloc`.
    #[test]
    fn dealloc_settles_before_delegating() {
        // Well above the settle threshold, so both the allocation and the free flush immediately.
        const SIZE: usize = 64 * MIB;
        let _guard = serial();
        let allocator = AccountingAllocator::new(Recording::new());
        let layout = Layout::from_size_align(SIZE, 8).unwrap();

        // SAFETY: the layout is valid and non-zero, and the block is freed below through the same
        // allocator that produced it.
        let ptr = unsafe { allocator.alloc(layout) };
        assert!(!ptr.is_null());
        let after_alloc = current_balance();
        unsafe { allocator.dealloc(ptr, layout) };

        let seen = allocator.inner.balance_at_dealloc.load(Ordering::Relaxed);
        assert!(
            about(seen + SIZE, after_alloc),
            "inner dealloc saw balance {seen}, expected about {} (balance after alloc was \
             {after_alloc})",
            after_alloc.saturating_sub(SIZE)
        );
    }

    /// `realloc` moves the balance by the size difference, not by the new size, and does so after
    /// delegating: the inner allocator must see the balance still carrying the old size.
    #[test]
    fn realloc_accounts_the_size_difference_after_delegating() {
        const OLD: usize = 64 * MIB;
        const GROWN: usize = 96 * MIB;
        const SHRUNK: usize = 32 * MIB;
        let _guard = serial();
        let allocator = AccountingAllocator::new(Recording::new());
        let layout = Layout::from_size_align(OLD, 8).unwrap();

        // SAFETY: each layout matches the block's current size, and the block is freed at the end
        // through the same allocator that produced it.
        let ptr = unsafe { allocator.alloc(layout) };
        assert!(!ptr.is_null());
        let before_grow = current_balance();

        let ptr = unsafe { allocator.realloc(ptr, layout, GROWN) };
        assert!(!ptr.is_null());
        let after_grow = current_balance();
        let seen = allocator.inner.balance_at_realloc.load(Ordering::Relaxed);
        assert!(
            about(seen, before_grow),
            "inner realloc saw balance {seen}, expected about {before_grow}: the wrapper must \
             account after delegating"
        );
        assert!(
            about(after_grow, before_grow + (GROWN - OLD)),
            "growing {OLD} -> {GROWN} moved the balance {before_grow} -> {after_grow}, expected \
             about +{}",
            GROWN - OLD
        );

        let layout = Layout::from_size_align(GROWN, 8).unwrap();
        let ptr = unsafe { allocator.realloc(ptr, layout, SHRUNK) };
        assert!(!ptr.is_null());
        let after_shrink = current_balance();
        assert!(
            about(after_shrink + (GROWN - SHRUNK), after_grow),
            "shrinking {GROWN} -> {SHRUNK} moved the balance {after_grow} -> {after_shrink}, \
             expected about -{}",
            GROWN - SHRUNK
        );

        unsafe { allocator.dealloc(ptr, Layout::from_size_align(SHRUNK, 8).unwrap()) };
    }

    /// A thread's remaining drift must reach the shared balance when the thread exits.
    ///
    /// This injects a drift into a registered thread's state and drops a `SettleOnExit` directly,
    /// rather than letting the thread exit. With the wrapper installed, the thread's teardown
    /// allocates, and those allocations flush an oversized drift through `track` before the
    /// destructor runs, so a thread-exit test would pass without the destructor. Nothing between
    /// the injection and the drop allocates, so only the destructor can move the drift. That the
    /// destructor runs when a registered thread exits is the `thread_local!` guarantee; what needs
    /// testing is that it settles the drift. The drop marks the thread exited, so the test runs on
    /// a thread of its own. The injected amount is far larger than any real allocation, and is
    /// taken back out afterwards.
    #[test]
    fn dropping_the_exit_hook_settles_the_drift() {
        const INJECTED: isize = 1 << 40;
        let _guard = serial();

        let (moved, phase) = std::thread::spawn(|| {
            track(1);
            let before = BALANCE.load(Ordering::Relaxed);
            STATE.with(|state| state.drift.set(state.drift.get() + INJECTED));
            drop(SettleOnExit);
            let moved = BALANCE.load(Ordering::Relaxed) - before;
            let phase = STATE.with(|state| state.phase.get());
            track(-1);
            (moved, phase)
        })
        .join()
        .unwrap();
        BALANCE.fetch_sub(INJECTED, Ordering::Relaxed);

        assert!(
            moved >= INJECTED / 2,
            "a dropped exit hook never settled the thread's drift: balance moved {moved} bytes, \
             expected at least {}",
            INJECTED / 2
        );
        assert_eq!(
            phase, EXITED,
            "a dropped exit hook did not mark the thread exited"
        );
    }

    /// A thread's first tracked delta registers its exit hook, after which deltas accumulate in
    /// the thread's drift. With the wrapper installed, the thread's own allocations have already
    /// done this by the time the closure runs, so only the end state is checked.
    #[test]
    fn a_tracked_delta_registers_the_exit_hook() {
        std::thread::spawn(|| {
            track(1);
            assert_eq!(STATE.with(|state| state.phase.get()), REGISTERED);
            track(-1);
        })
        .join()
        .unwrap();
    }

    /// Outside the `REGISTERED` phase, a delta must go straight to the shared balance: while
    /// registering, because the drift is not yet settled on exit, and after exiting, because it
    /// never will be.
    #[test]
    fn deltas_bypass_the_drift_outside_the_registered_phase() {
        const INJECTED: isize = 1 << 40;
        let _guard = serial();

        std::thread::spawn(|| {
            track(1);
            for phase in [REGISTERING, EXITED] {
                let (drift_before, balance_before) = STATE.with(|state| {
                    state.phase.set(phase);
                    (state.drift.get(), BALANCE.load(Ordering::Relaxed))
                });
                track(INJECTED);
                let (drift_after, balance_after) = STATE.with(|state| {
                    state.phase.set(REGISTERED);
                    (state.drift.get(), BALANCE.load(Ordering::Relaxed))
                });
                BALANCE.fetch_sub(INJECTED, Ordering::Relaxed);

                assert_eq!(drift_after, drift_before, "phase {phase} moved the drift");
                assert!(
                    balance_after - balance_before >= INJECTED / 2,
                    "phase {phase} did not settle into the shared balance"
                );
            }
            track(-1);
        })
        .join()
        .unwrap();
    }
}
