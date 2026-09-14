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
//! Comet's [`MemoryPool`](datafusion::execution::memory_pool::MemoryPool) counts *declared
//! reservations*: bytes an operator explicitly asked for. Plenty of real allocation never goes
//! through it — Arrow builders, expression kernels, decompression buffers, Parquet metadata,
//! `object_store` buffers, tokio's own machinery — so pool reservations are a lower bound on
//! Comet's footprint, and the size of the gap is workload-dependent and currently unmeasurable at
//! runtime. See the [memory management contributor guide] for the full picture.
//!
//! [`AccountingAllocator`] wraps the selected global allocator and maintains a single signed
//! process-wide byte balance, which [`current_balance`] exposes. This is **observability only**: it
//! never rejects an allocation, never panics, and never gates the memory pool. It exists so the
//! accounting gap can be seen in tracing output next to the pool reservations it should be
//! compared against.
//!
//! The balance counts `Layout` bytes, not resident pages. It excludes allocator fragmentation,
//! jemalloc's retained pages, `mmap`ed regions, and anything a C dependency allocates through libc
//! `malloc` rather than Rust's `GlobalAlloc` — so it is a lower bound on RSS as well, just a much
//! tighter one than pool reservations.
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
    /// Set while this thread is inside [`track`], so an allocation made *by* `track` settles
    /// directly instead of recursing. The only such allocation today is the one some platforms
    /// make when registering `LOCAL_DRIFT`'s destructor on first touch.
    ///
    /// Const-initialized and destructor-free, so reading it never allocates and never fails —
    /// which is what makes it safe to consult before touching `LOCAL_DRIFT`.
    static IN_TRACK: Cell<bool> = const { Cell::new(false) };

    /// This thread's un-flushed delta.
    static LOCAL_DRIFT: ThreadDrift = const { ThreadDrift(Cell::new(0)) };
}

/// Owns a thread's un-flushed delta and settles the remainder when the thread exits.
///
/// Without the destructor, up to [`SETTLE_THRESHOLD`] bytes of accounting would be silently
/// discarded every time a thread died. Worker threads live for the process lifetime, but the
/// blocking pool churns on tokio's idle timeout, so on a long-lived executor that would be a
/// slowly accumulating bias in the reported balance.
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
/// Returns 0 when the [`AccountingAllocator`] is not installed. Never reported negative: the
/// balance can dip below zero transiently while per-thread deltas settle out of order.
pub fn current_balance() -> usize {
    clamp_balance(BALANCE.load(Ordering::Relaxed))
}

/// Clamps a signed balance to the unsigned value reported to callers.
fn clamp_balance(balance: isize) -> usize {
    balance.max(0) as usize
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

    // A re-entrant call is one made by `track` itself; the outer frame owns the flag and will
    // clear it, so this frame must only settle and return.
    if IN_TRACK.with(|in_track| in_track.replace(true)) {
        BALANCE.fetch_add(delta, Ordering::Relaxed);
        return;
    }

    // `try_with` rather than `with`: during thread teardown `LOCAL_DRIFT`'s destructor has already
    // run, and any allocation after that point must not panic inside the allocator.
    if LOCAL_DRIFT
        .try_with(|thread_drift| settle(&thread_drift.0, delta))
        .is_err()
    {
        BALANCE.fetch_add(delta, Ordering::Relaxed);
    }

    IN_TRACK.with(|in_track| in_track.set(false));
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

    #[test]
    fn settle_accumulates_below_the_threshold() {
        let drift = Cell::new(0);
        let before = BALANCE.load(Ordering::Relaxed);
        settle(&drift, 1024);
        assert_eq!(drift.get(), 1024, "small delta stays thread-local");
        assert_eq!(BALANCE.load(Ordering::Relaxed), before);
    }

    #[test]
    fn settle_flushes_at_the_threshold() {
        let drift = Cell::new(0);
        settle(&drift, SETTLE_THRESHOLD);
        assert_eq!(drift.get(), 0, "drift resets once flushed");
    }

    #[test]
    fn settle_flushes_negative_drift() {
        let drift = Cell::new(0);
        settle(&drift, -SETTLE_THRESHOLD);
        assert_eq!(drift.get(), 0);
    }

    #[test]
    fn a_transiently_negative_balance_reports_as_zero() {
        assert_eq!(clamp_balance(-1), 0);
        assert_eq!(clamp_balance(isize::MIN), 0);
        assert_eq!(clamp_balance(0), 0);
        assert_eq!(clamp_balance(4096), 4096);
    }

    /// A real allocation must move the reported balance. Parallel test noise can only add to the
    /// balance, so the assertion is one-sided.
    #[test]
    #[cfg(feature = "alloc-accounting")]
    fn a_real_allocation_raises_the_balance() {
        let before = current_balance();
        // Well above the settle threshold, so it is guaranteed to flush.
        let held: Vec<u8> = vec![0u8; 8 * 1024 * 1024];
        let during = current_balance();
        assert!(
            during >= before + 4 * 1024 * 1024,
            "8 MiB allocation should raise the balance (before={before}, during={during})"
        );
        drop(held);
    }

    /// The balance must drop before the inner allocator is asked to free the block.
    ///
    /// jemalloc decrements its own `stats.allocated` at the start of a large free and then, for
    /// blocks above its oversize threshold, unmaps the pages eagerly, which takes milliseconds for
    /// a block of a few hundred megabytes. If the subtraction happened after delegating, the balance
    /// would keep reporting a block the allocator had already given back for that whole window,
    /// and `native_allocated` would read above `jemalloc_allocated`.
    #[test]
    fn dealloc_settles_before_delegating() {
        use std::alloc::System;
        use std::sync::atomic::AtomicUsize;

        /// Records the reported balance at the moment the inner free is called.
        struct Recording {
            balance_at_dealloc: AtomicUsize,
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
        }

        // Well above the settle threshold, so both the allocation and the free flush immediately.
        const SIZE: usize = 64 * 1024 * 1024;
        let allocator = AccountingAllocator::new(Recording {
            balance_at_dealloc: AtomicUsize::new(usize::MAX),
        });
        let layout = Layout::from_size_align(SIZE, 8).unwrap();

        // SAFETY: the layout is valid and non-zero, and the block is freed below through the same
        // allocator that produced it.
        let ptr = unsafe { allocator.alloc(layout) };
        assert!(!ptr.is_null());
        let after_alloc = current_balance();
        unsafe { allocator.dealloc(ptr, layout) };

        let seen = allocator.inner.balance_at_dealloc.load(Ordering::Relaxed);
        // Half the block is a wide margin against parallel test noise while still being far
        // outside anything the mutation (subtracting after delegating) could produce.
        assert!(
            seen + SIZE / 2 <= after_alloc,
            "inner dealloc saw balance {seen}, expected at most {} (balance after alloc was \
             {after_alloc})",
            after_alloc - SIZE / 2
        );
    }

    /// Threads must settle their remaining drift on exit.
    ///
    /// Each worker allocates a sub-threshold buffer — so the bytes are still sitting in its local
    /// drift, never flushed — and hands ownership back to this thread before exiting. The matching
    /// free therefore happens here, after the worker is gone, so the only way those bytes can ever
    /// reach the shared balance is `ThreadDrift::drop`. Without the destructor the balance does not
    /// move at all, and the later frees drive it *below* where it started.
    #[test]
    #[cfg(feature = "alloc-accounting")]
    fn thread_exit_settles_remaining_drift() {
        use std::sync::mpsc;
        use std::thread;

        const THREADS: usize = 64;
        const PER_THREAD: usize = 32 * 1024;
        assert!(
            (PER_THREAD as isize) < SETTLE_THRESHOLD,
            "the per-thread buffer must stay in local drift for this test to mean anything"
        );

        let (tx, rx) = mpsc::channel();
        let before = current_balance() as isize;

        for _ in 0..THREADS {
            let tx = tx.clone();
            thread::spawn(move || tx.send(vec![0u8; PER_THREAD]).unwrap())
                .join()
                .unwrap();
        }
        drop(tx);

        let held: Vec<Vec<u8>> = rx.iter().collect();
        assert_eq!(held.len(), THREADS);

        let moved = current_balance() as isize - before;
        let allocated = (THREADS * PER_THREAD) as isize;
        // Half the expected total is a wide margin against parallel test noise while still being
        // far outside anything the mutation (a destructor that discards the drift) could produce.
        assert!(
            moved >= allocated / 2,
            "drift from exited threads never reached the shared balance: \
             balance moved {moved} bytes, expected at least {}",
            allocated / 2
        );

        drop(held);
    }
}
