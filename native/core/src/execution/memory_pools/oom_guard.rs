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

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

/// Per-thread drift is flushed into the shared balance once it crosses this.
const SETTLE_THRESHOLD: isize = 64 * 1024;

/// Process-wide outstanding bytes (signed so transient under-settle is fine).
static BALANCE: AtomicIsize = AtomicIsize::new(0);
/// Enforcement limit in bytes; 0 means unset.
static LIMIT: AtomicUsize = AtomicUsize::new(0);
/// Master enforcement gate (single relaxed load on the hot path).
static ARMED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// Un-flushed per-thread delta.
    static LOCAL_DRIFT: Cell<isize> = const { Cell::new(0) };
    /// Is this a query-worker thread eligible for enforcement?
    static STAMPED: Cell<bool> = const { Cell::new(false) };
    /// Set while a guard panic is unwinding this thread, to avoid double-faults.
    static UNWINDING: Cell<bool> = const { Cell::new(false) };
}

/// Payload of the panic raised when an armed, stamped thread exceeds the limit.
#[derive(Debug)]
#[allow(dead_code)] // fields read by the allocator wrapper in Task 3
pub struct OomGuardPanic {
    pub balance: usize,
    pub limit: usize,
}

/// Arm the guard with a byte limit. Idempotent.
pub fn arm(limit_bytes: usize) {
    LIMIT.store(limit_bytes, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
}

/// Disarm the guard (enforcement off; tracking continues cheaply).
#[allow(dead_code)]
pub fn disarm() {
    ARMED.store(false, Ordering::Relaxed);
}

/// Mark the current thread as a query-worker thread eligible for enforcement.
pub fn stamp_current_thread() {
    STAMPED.with(|s| s.set(true));
}

/// Reset the per-thread unwinding guard after a guard panic has been caught on
/// this thread. Safe to call when not unwinding. The JNI caller thread is
/// reused across tasks, so this must run after catching an OomGuardPanic.
pub fn clear_unwinding() {
    UNWINDING.with(|u| u.set(false));
}

/// Current process-wide balance in bytes (never reported negative).
#[allow(dead_code)]
pub fn current_balance() -> usize {
    BALANCE.load(Ordering::Relaxed).max(0) as usize
}

/// Record an allocation of `size` bytes; may trip the breaker.
#[inline]
fn record_alloc(size: usize) {
    track(size as isize);
}

/// Record a deallocation of `size` bytes; never trips (credit only).
#[inline]
fn record_dealloc(size: usize) {
    track(-(size as isize));
}

/// Core tracking + enforcement. Flushes drift; on a debit flush that crosses the
/// limit on an armed, stamped, non-unwinding thread, panics with `OomGuardPanic`.
#[inline]
fn track(delta: isize) {
    let new_balance = LOCAL_DRIFT.with(|d| {
        let mut drift = d.get();
        let flushed = settle(&mut drift, delta, &BALANCE);
        d.set(drift);
        flushed
    });

    if delta <= 0 {
        return; // credits never enforce
    }
    let Some(balance) = new_balance else { return };
    if !ARMED.load(Ordering::Relaxed) {
        return;
    }
    if !STAMPED.with(|s| s.get()) {
        return;
    }
    if UNWINDING.with(|u| u.get()) {
        return;
    }
    let limit = LIMIT.load(Ordering::Relaxed);
    if should_trip(balance, limit) {
        // panic_any boxes the payload, which re-enters this allocator and calls
        // track() again. Set UNWINDING first so that re-entrant call short-circuits
        // above, preventing infinite recursion / a double panic from inside alloc.
        UNWINDING.with(|u| u.set(true));
        std::panic::panic_any(OomGuardPanic {
            balance: balance.max(0) as usize,
            limit,
        });
    }
}

/// Pure helper: given the current shared balance and a limit, decide whether an
/// armed+stamped thread should trip the breaker. `limit == 0` means "unset".
fn should_trip(balance: isize, limit: usize) -> bool {
    limit != 0 && balance > limit.try_into().unwrap_or(isize::MAX)
}

/// Pure helper: add `delta` to `local_drift`; if it reaches or exceeds `SETTLE_THRESHOLD`
/// in magnitude, flush it into `shared` and return the new shared balance.
/// Otherwise return `None` (nothing flushed).
fn settle(local_drift: &mut isize, delta: isize, shared: &AtomicIsize) -> Option<isize> {
    *local_drift = local_drift.wrapping_add(delta);
    if local_drift.unsigned_abs() >= SETTLE_THRESHOLD as usize {
        let flushed = *local_drift;
        *local_drift = 0;
        let prev = shared.fetch_add(flushed, Ordering::Relaxed);
        Some(prev.wrapping_add(flushed))
    } else {
        None
    }
}

/// Wraps an inner global allocator, tracking layout bytes for the OomGuard.
pub struct AccountingAllocator<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> AccountingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for AccountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.inner.dealloc(ptr, layout);
        record_dealloc(layout.size());
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc_zeroed(layout);
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // Casts and subtraction are safe in practice: a single allocation cannot
            // exceed isize::MAX on any real platform, so no wrapping or overflow occurs.
            let old = layout.size() as isize;
            let new = new_size as isize;
            track(new - old);
        }
        new_ptr
    }
}

#[cfg(test)]
fn reset_for_test() {
    BALANCE.store(0, Ordering::Relaxed);
    LIMIT.store(0, Ordering::Relaxed);
    ARMED.store(false, Ordering::Relaxed);
    LOCAL_DRIFT.with(|d| d.set(0));
    STAMPED.with(|s| s.set(false));
    UNWINDING.with(|u| u.set(false));
}

#[cfg(test)]
fn clear_unwinding_for_test() {
    UNWINDING.with(|u| u.set(false));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serializes tests that mutate the process-global guard state.
    static GUARD: Mutex<()> = Mutex::new(());

    #[test]
    fn test_should_trip() {
        assert!(!should_trip(100, 0)); // unset limit never trips
        assert!(!should_trip(100, 200)); // under limit
        assert!(!should_trip(200, 200)); // at limit (strictly greater required)
        assert!(should_trip(201, 200)); // over limit
    }

    #[test]
    fn test_settle_accumulates_then_flushes() {
        let shared = AtomicIsize::new(0);
        let mut drift = 0isize;
        // small allocs below threshold do not flush
        assert_eq!(settle(&mut drift, 1024, &shared), None);
        assert_eq!(shared.load(Ordering::Relaxed), 0);
        // crossing the threshold flushes the accumulated drift
        let new_balance = settle(&mut drift, SETTLE_THRESHOLD, &shared);
        assert_eq!(new_balance, Some(1024 + SETTLE_THRESHOLD));
        assert_eq!(shared.load(Ordering::Relaxed), 1024 + SETTLE_THRESHOLD);
        assert_eq!(drift, 0); // drift reset after flush
    }

    #[test]
    fn test_settle_flushes_negative_drift() {
        let shared = AtomicIsize::new(1_000_000);
        let mut drift = 0isize;
        assert_eq!(
            settle(&mut drift, -SETTLE_THRESHOLD, &shared),
            Some(1_000_000 - SETTLE_THRESHOLD)
        );
        assert_eq!(drift, 0);
    }

    #[test]
    fn test_settle_flushes_at_exact_threshold() {
        let shared = AtomicIsize::new(0);
        let mut drift = 0isize;
        assert_eq!(settle(&mut drift, SETTLE_THRESHOLD, &shared), Some(SETTLE_THRESHOLD));
        assert_eq!(drift, 0);
    }

    #[test]
    fn test_disarmed_never_trips() {
        let _g = GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_for_test();
        stamp_current_thread();
        // not armed -> record_alloc must never panic regardless of size
        record_alloc(usize::MAX / 2);
        record_alloc(usize::MAX / 2);
    }

    #[test]
    fn test_unstamped_thread_never_trips() {
        let _g = GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_for_test();
        // arm with a tiny limit relative to current balance, but DO NOT stamp
        let limit = current_balance() + 1;
        arm(limit);
        record_alloc(SETTLE_THRESHOLD as usize * 4); // big enough to flush
        disarm();
    }

    #[test]
    fn test_stamped_over_budget_trips() {
        let _g = GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_for_test();
        stamp_current_thread();
        let limit = current_balance() + SETTLE_THRESHOLD as usize; // headroom
        arm(limit);
        let result = std::panic::catch_unwind(|| {
            // exceed the headroom in one flush
            record_alloc(SETTLE_THRESHOLD as usize * 4);
        });
        disarm();
        clear_unwinding_for_test();
        assert!(result.is_err(), "expected OomGuardPanic");
        let panic = result.unwrap_err();
        assert!(
            panic.downcast_ref::<OomGuardPanic>().is_some(),
            "panic payload should be OomGuardPanic"
        );
    }

    // Drives a real heap allocation through the installed AccountingAllocator (only
    // wrapped under the `oom-guard` feature) and confirms the guard trips.
    #[test]
    #[cfg(feature = "oom-guard")]
    fn test_real_allocation_trips_guard() {
        let _g = GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset_for_test();
        stamp_current_thread();
        // 8 MiB headroom over the current (noisy) baseline.
        let headroom = 8 * 1024 * 1024;
        arm(current_balance() + headroom);

        let result = std::panic::catch_unwind(|| {
            // Allocate well past the headroom in 1 MiB chunks so a flush crosses the limit.
            let mut held: Vec<Vec<u8>> = Vec::new();
            for _ in 0..64 {
                held.push(vec![0u8; 1024 * 1024]);
            }
            // Touch the data so the allocation cannot be optimized away.
            held.iter().map(|v| v.len()).sum::<usize>()
        });

        // Disarm BEFORE clearing UNWINDING so no post-catch allocation on this still-armed,
        // still-stamped thread can re-trip outside the catch.
        disarm();
        clear_unwinding_for_test();

        assert!(
            result.is_err(),
            "large allocation on a stamped, armed thread should trip the guard"
        );
        assert!(
            result.unwrap_err().downcast_ref::<OomGuardPanic>().is_some(),
            "panic payload should be OomGuardPanic"
        );
    }
}
