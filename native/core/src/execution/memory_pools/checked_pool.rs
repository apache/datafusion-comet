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

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::{
    common::{resources_datafusion_err, DataFusionError},
    execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
};
use log::warn;

use crate::alloc_accounting;

/// Wraps an off-heap memory pool so that the bytes the native allocator has actually handed out
/// are compared against a budget, and not just the bytes operators declared.
///
/// A pool on its own only counts what operators voluntarily reserve, so native memory that
/// bypasses it is invisible until the executor exceeds its container limit and is killed. This
/// wrapper consults the `alloc-accounting` balance before the inner pool's own check, which makes
/// that overrun visible at the next reservation instead.
///
/// `enforce` decides what a crossing does. By default it is false and the crossing is only logged,
/// once per pool, because the rate of false positives on real workloads is not yet established and
/// a spurious denial is worse than a late one: spilling releases *reserved* bytes, so if the
/// overshoot is in untracked allocations a denial may not relieve it and the task fails anyway.
/// With `spark.comet.exec.memoryPool.enforceNativeUsage` the reservation is refused, operators
/// that can spill do so, and those that cannot fail the task rather than the executor.
///
/// Both `fair_unified` and `greedy_unified` wear this. The budget is `spark.memory.offHeap.size`,
/// deliberately not the pool's own limit: that limit is the off-heap size times
/// `spark.comet.exec.memoryPool.fraction`, and the fraction is how operators hold back reservable
/// memory to force spilling. Deriving this budget from it too would turn a small fraction into
/// denied reservations rather than the spills it was set to cause.
///
/// Note that the budget and the balance are not measuring the same population:
/// `spark.memory.offHeap.size` is shared with Spark's own Tungsten off-heap allocations and with
/// Comet's JVM-side shuffle pages, while the balance counts only Comet's Rust allocations. The
/// comparison is therefore a loose backstop against the executor being killed, not a bound on
/// total off-heap usage.
///
/// Both the balance and the budget are process-wide, so there is no per-task attribution: once any
/// task pushes real usage past the budget, every task's next non-zero reservation sees it.
/// Allocations themselves are never refused; this is a reservation gate, not a hard limit.
///
/// A build without the `alloc-accounting` feature reports a balance of zero, which leaves the
/// wrapper a passthrough.
pub struct CheckedMemoryPool<P: MemoryPool> {
    inner: P,
    budget: usize,
    enforce: bool,
    /// Set once this pool has logged a crossing, so that observing does not flood the log:
    /// `try_grow` is called constantly and the condition is sticky once real usage is high.
    reported: AtomicBool,
}

impl<P: MemoryPool> CheckedMemoryPool<P> {
    pub fn new(inner: P, budget: usize, enforce: bool) -> Self {
        Self {
            inner,
            budget,
            enforce,
            reported: AtomicBool::new(false),
        }
    }
}

impl<P: MemoryPool> Debug for CheckedMemoryPool<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CheckedMemoryPool")
            .field("budget", &self.budget)
            .field("enforce", &self.enforce)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<P: MemoryPool> Display for CheckedMemoryPool<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "CheckedMemoryPool(budget={}, enforce={}, inner={})",
            self.budget, self.enforce, self.inner
        )
    }
}

impl<P: MemoryPool> MemoryPool for CheckedMemoryPool<P> {
    fn name(&self) -> &str {
        "CheckedMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional).unwrap()
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional == 0 {
            return Ok(());
        }
        // Checked first because it is a single atomic load, whereas the inner unified pools cross
        // JNI to ask Spark. A request denied here never reaches Spark's ledger.
        let in_use = alloc_accounting::current_balance();
        if in_use.saturating_add(additional) > self.budget {
            if self.enforce {
                return Err(resources_datafusion_err!(
                    "Failed to reserve {additional} bytes for {}: native memory in use is \
                     {in_use} bytes of a {} byte budget (spark.memory.offHeap.size). Reserved: \
                     {}. Raise spark.memory.offHeap.size, or disable this check with \
                     spark.comet.exec.memoryPool.enforceNativeUsage=false",
                    reservation.consumer().name(),
                    self.budget,
                    self.reserved()
                ));
            }
            if !self.reported.swap(true, Ordering::Relaxed) {
                warn!(
                    "Comet's real native memory usage ({in_use} bytes) has passed \
                     spark.memory.offHeap.size ({} bytes) while reserving {additional} bytes for \
                     {}. This memory is not covered by the pool's reservations and the executor \
                     may be killed for exceeding its container limit. Set \
                     spark.comet.exec.memoryPool.enforceNativeUsage=true to refuse such \
                     reservations instead, or raise spark.memory.offHeap.size.",
                    self.budget,
                    reservation.consumer().name()
                );
            }
        }
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        // Only a real limit when it is enforced. Reporting it while merely observing would tell
        // callers that planning against a budget nothing rejects is safe.
        if self.enforce {
            MemoryLimit::Finite(self.budget)
        } else {
            self.inner.memory_limit()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use std::sync::Arc;

    fn enforcing(budget: usize) -> Arc<dyn MemoryPool> {
        Arc::new(CheckedMemoryPool::new(
            UnboundedMemoryPool::default(),
            budget,
            true,
        ))
    }

    fn observing(budget: usize) -> Arc<dyn MemoryPool> {
        Arc::new(CheckedMemoryPool::new(
            UnboundedMemoryPool::default(),
            budget,
            false,
        ))
    }

    #[test]
    fn a_zero_byte_grow_never_fails() {
        let pool = enforcing(0);
        let reservation = MemoryConsumer::new("zero").register(&pool);
        reservation.try_grow(0).unwrap();
    }

    #[test]
    fn reports_the_budget_as_its_limit_when_enforcing() {
        assert!(matches!(
            enforcing(4096).memory_limit(),
            MemoryLimit::Finite(4096)
        ));
    }

    #[test]
    fn defers_to_the_inner_limit_when_only_observing() {
        assert!(matches!(
            observing(4096).memory_limit(),
            MemoryLimit::Infinite
        ));
    }

    #[test]
    fn successful_grows_and_shrinks_reach_the_inner_pool() {
        let pool = enforcing(usize::MAX);
        let reservation = MemoryConsumer::new("delegate").register(&pool);
        reservation.try_grow(1024).unwrap();
        assert_eq!(pool.reserved(), 1024);
        reservation.shrink(1024);
        assert_eq!(pool.reserved(), 0);
    }

    /// The gate compares real bytes, not reservations: a block this pool never heard about is
    /// enough to deny a one-byte request, and freeing it is enough to allow the same request.
    ///
    /// The block is touched so it is really allocated, and the margins are far wider than anything
    /// the rest of the crate allocates in the microseconds between the checks. The serial lock
    /// keeps the accounting tests that move the balance by tens of megabytes out of that window.
    #[test]
    #[cfg(feature = "alloc-accounting")]
    fn denies_when_real_bytes_plus_request_exceed_the_budget() {
        use std::hint::black_box;

        const HEADROOM: usize = 64 * 1024 * 1024;
        const BLOCK: usize = 256 * 1024 * 1024;

        let _guard = alloc_accounting::test_support::serial();
        let budget = alloc_accounting::current_balance() + HEADROOM;
        let pool = enforcing(budget);
        let reservation = MemoryConsumer::new("checked").register(&pool);

        let held: Vec<u8> = black_box(vec![1u8; BLOCK]);
        let denied = reservation.try_grow(1).unwrap_err();
        black_box(&held);
        assert!(
            matches!(denied, DataFusionError::ResourcesExhausted(_)),
            "expected ResourcesExhausted, got {denied:?}"
        );
        let message = denied.to_string();
        assert!(
            message.contains("native memory in use is") && message.contains("checked"),
            "message should name the real bytes in use and the consumer: {message}"
        );
        assert_eq!(pool.reserved(), 0, "a denied request must not be reserved");

        drop(held);
        reservation.try_grow(1).unwrap();
        assert_eq!(pool.reserved(), 1);
    }

    /// The same crossing, with enforcement off: the reservation still succeeds and reaches the
    /// inner pool, which is what makes the default safe to ship.
    #[test]
    #[cfg(feature = "alloc-accounting")]
    fn allows_the_same_crossing_when_only_observing() {
        use std::hint::black_box;

        const HEADROOM: usize = 64 * 1024 * 1024;
        const BLOCK: usize = 256 * 1024 * 1024;

        let _guard = alloc_accounting::test_support::serial();
        let budget = alloc_accounting::current_balance() + HEADROOM;
        let pool = observing(budget);
        let reservation = MemoryConsumer::new("observed").register(&pool);

        let held: Vec<u8> = black_box(vec![1u8; BLOCK]);
        reservation.try_grow(1).unwrap();
        black_box(&held);
        assert_eq!(pool.reserved(), 1, "observing must not withhold the bytes");
        drop(held);
    }
}
