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

use crate::execution::memory_pools::oom_guard;
use datafusion::common::{resources_datafusion_err, DataFusionError};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::sync::Arc;

/// Source of the current process-wide real allocator usage in bytes. Production
/// wiring uses `oom_guard::current_balance`; tests inject a controllable value.
type BalanceSource = Arc<dyn Fn() -> usize + Send + Sync>;

/// A `MemoryPool` decorator that, on top of the inner pool's tracked-reservation
/// accounting, rejects growth when *real* allocator usage (untracked Arrow / join /
/// kernel bytes included) plus the requested amount would exceed a process-global
/// ceiling. Returning `ResourcesExhausted` lets DataFusion spill and retry.
pub(crate) struct RealUsagePool {
    inner: Arc<dyn MemoryPool>,
    /// Process-global real-usage ceiling in bytes; 0 means unset (no gating).
    ceiling: usize,
    balance_source: BalanceSource,
}

impl std::fmt::Debug for RealUsagePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealUsagePool")
            .field("inner", &self.inner)
            .field("ceiling", &self.ceiling)
            .finish_non_exhaustive()
    }
}

impl RealUsagePool {
    /// Wrap `inner` with the real-usage gate using the live OomGuard balance.
    pub(crate) fn new(inner: Arc<dyn MemoryPool>, ceiling: usize) -> Self {
        Self {
            inner,
            ceiling,
            balance_source: Arc::new(oom_guard::current_balance),
        }
    }

    /// Wrap `inner` with an explicit balance source (test seam).
    #[cfg(test)]
    fn with_balance_source(
        inner: Arc<dyn MemoryPool>,
        ceiling: usize,
        balance_source: BalanceSource,
    ) -> Self {
        Self {
            inner,
            ceiling,
            balance_source,
        }
    }
}

impl MemoryPool for RealUsagePool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        // Check the real-usage ceiling before delegating, so an over-budget request is
        // rejected without speculatively reserving (and then rolling back) the inner
        // pool. Returning `ResourcesExhausted` lets DataFusion spill and retry.
        if self.ceiling != 0 && additional != 0 {
            let real = (self.balance_source)();
            if real.saturating_add(additional) > self.ceiling {
                return Err(resources_datafusion_err!(
                    "Comet real-usage gate: native usage {real} bytes + requested \
                     {additional} bytes exceeds the off-heap budget of {} bytes; \
                     spilling/failing this consumer",
                    self.ceiling
                ));
            }
        }
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fixed_source(bytes: Arc<AtomicUsize>) -> BalanceSource {
        Arc::new(move || bytes.load(Ordering::Relaxed))
    }

    #[test]
    fn under_ceiling_succeeds_and_delegates() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let balance = Arc::new(AtomicUsize::new(100));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::with_balance_source(
            Arc::clone(&inner),
            1000,
            fixed_source(balance),
        ));
        let reservation = MemoryConsumer::new("test").register(&pool);
        // real usage 100 + request 100 = 200 <= ceiling 1000
        assert!(pool.try_grow(&reservation, 100).is_ok());
        assert_eq!(inner.reserved(), 100);
    }

    #[test]
    fn over_ceiling_rejects_without_reserving_inner() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let balance = Arc::new(AtomicUsize::new(900));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::with_balance_source(
            Arc::clone(&inner),
            1000,
            fixed_source(balance),
        ));
        let reservation = MemoryConsumer::new("test").register(&pool);
        // real usage 900 + request 200 = 1100 > ceiling 1000 -> reject
        let result = pool.try_grow(&reservation, 200);
        assert!(result.is_err(), "over-ceiling grow should be rejected");
        // inner pool is never touched on rejection, so there is nothing to roll back
        assert_eq!(inner.reserved(), 0);
    }

    #[test]
    fn zero_ceiling_never_gates() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let balance = Arc::new(AtomicUsize::new(usize::MAX / 2));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::with_balance_source(
            Arc::clone(&inner),
            0,
            fixed_source(balance),
        ));
        let reservation = MemoryConsumer::new("test").register(&pool);
        assert!(pool.try_grow(&reservation, 1024).is_ok());
        assert_eq!(inner.reserved(), 1024);
    }

    #[test]
    fn shrink_delegates() {
        let inner: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let balance = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::with_balance_source(
            Arc::clone(&inner),
            1_000_000,
            fixed_source(balance),
        ));
        let reservation = MemoryConsumer::new("test").register(&pool);
        pool.try_grow(&reservation, 500).unwrap();
        assert_eq!(pool.reserved(), 500);
        pool.shrink(&reservation, 200);
        assert_eq!(pool.reserved(), 300);
    }

    // Drives a real heap allocation through the installed AccountingAllocator (only
    // wrapped under the `oom-guard` feature) and confirms the real-usage gate rejects.
    // Robust to parallel test noise: other allocations only raise the balance further,
    // which can only make the over-ceiling assertion more true.
    #[test]
    fn real_allocation_trips_real_usage_gate() {
        let inner: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let base = oom_guard::current_balance();
        // 4 MiB headroom over the (noisy) baseline.
        let ceiling = base + 4 * 1024 * 1024;
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::new(Arc::clone(&inner), ceiling));
        let reservation = MemoryConsumer::new("test").register(&pool);

        // Push real usage ~8 MiB above the baseline, held alive across the check so the
        // balance stays elevated. 8 MiB > 64 KiB settle threshold, so it flushes to BALANCE.
        let held: Vec<u8> = vec![0u8; 8 * 1024 * 1024];
        assert!(
            oom_guard::current_balance() > ceiling,
            "allocation should push balance over ceiling"
        );

        let result = pool.try_grow(&reservation, 1);
        assert!(
            result.is_err(),
            "real usage over the ceiling should reject the grow"
        );
        // Keep `held` alive until after the assertion above.
        drop(held);
    }
}
