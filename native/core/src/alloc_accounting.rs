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

thread_local! {
    /// This thread's un-flushed delta, and the only thread-local [`track`] touches. `libcomet` is
    /// loaded with `dlopen`, so each thread-local an allocation touches costs a call into the
    /// dynamic loader.
    static LOCAL_DRIFT: ThreadDrift = const { ThreadDrift(Cell::new(0)) };
}

/// Owns a thread's un-flushed delta and settles the remainder when the thread exits. Without the
/// destructor, up to [`SETTLE_THRESHOLD`] bytes of accounting would be discarded every time a
/// thread died, and tokio's blocking pool churns threads on its idle timeout.
struct ThreadDrift(Cell<isize>);

impl Drop for ThreadDrift {
    fn drop(&mut self) {
        let drift = self.0.replace(0);
        if drift != 0 {
            BALANCE.fetch_add(drift, Ordering::Relaxed);
        }
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

    // `thread_local!` never allocates through the global allocator (a `GlobalAlloc` guarantee
    // since Rust 1.93), so first touching `LOCAL_DRIFT` cannot re-enter `track`.
    //
    // `try_with` rather than `with`: during thread teardown `LOCAL_DRIFT`'s destructor has already
    // run, and any allocation after that point must not panic inside the allocator.
    if LOCAL_DRIFT
        .try_with(|thread_drift| settle(&thread_drift.0, delta))
        .is_err()
    {
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
// accounting is pure bookkeeping over an `AtomicIsize` and a thread-local `Cell`: it does not
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
    /// This drops a `ThreadDrift` holding a drift directly, rather than injecting one into a real
    /// thread's `LOCAL_DRIFT` and letting the thread exit. With the wrapper installed, the thread's
    /// teardown allocates, and those allocations flush an oversized drift through `track` before
    /// the destructor runs, so a thread-exit test would pass without the destructor. That the
    /// destructor runs when a thread exits is the `thread_local!` guarantee; what needs testing is
    /// that it settles the drift. The injected amount is far larger than any real allocation, and
    /// is taken back out afterwards.
    #[test]
    fn dropping_a_thread_drift_settles_it() {
        const INJECTED: isize = 1 << 40;
        let _guard = serial();

        let before = BALANCE.load(Ordering::Relaxed);
        drop(ThreadDrift(Cell::new(INJECTED)));
        let moved = BALANCE.load(Ordering::Relaxed) - before;
        BALANCE.fetch_sub(INJECTED, Ordering::Relaxed);

        assert!(
            moved >= INJECTED / 2,
            "a dropped thread drift never reached the shared balance: balance moved {moved} \
             bytes, expected at least {}",
            INJECTED / 2
        );
    }
}
