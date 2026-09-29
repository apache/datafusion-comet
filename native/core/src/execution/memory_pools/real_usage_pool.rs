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

use crate::execution::memory_pools::{active_task_count, oom_guard};
use datafusion::common::{resources_datafusion_err, DataFusionError};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::sync::Arc;

/// Source of the current process-wide real allocator usage in bytes. Production reads the
/// live `oom_guard` balance; tests inject a fixed value without touching global state.
#[derive(Debug)]
enum BalanceSource {
    Live,
    #[cfg(test)]
    Fixed(usize),
}

impl BalanceSource {
    #[inline]
    fn current(&self) -> usize {
        match self {
            BalanceSource::Live => oom_guard::current_balance(),
            #[cfg(test)]
            BalanceSource::Fixed(bytes) => *bytes,
        }
    }
}

/// A `MemoryPool` decorator that, on top of the inner pool's tracked-reservation
/// accounting, rejects growth when *real* allocator usage (untracked Arrow / join /
/// kernel bytes included) plus the requested amount would exceed a process-global
/// ceiling. Returning `ResourcesExhausted` lets DataFusion spill and retry.
#[derive(Debug)]
pub(crate) struct RealUsageMemoryPool {
    inner: Arc<dyn MemoryPool>,
    /// Process-global real-usage ceiling in bytes; 0 means unset (no gating).
    ceiling: usize,
    /// Fixed fallback divisor (concurrent-task count) used when the dynamic
    /// active-task count is 0. `None` disables the fair-share guard (first-come),
    /// used for pools whose `reserved()` is process-wide.
    fair_share: Option<usize>,
    balance_source: BalanceSource,
}

impl std::fmt::Display for RealUsageMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RealUsageMemoryPool(ceiling={}, inner={})",
            self.ceiling, self.inner
        )
    }
}

impl RealUsageMemoryPool {
    /// Wrap `inner` with the real-usage gate using the live OomGuard balance.
    pub(crate) fn new(
        inner: Arc<dyn MemoryPool>,
        ceiling: usize,
        fair_share: Option<usize>,
    ) -> Self {
        Self {
            inner,
            ceiling,
            fair_share,
            balance_source: BalanceSource::Live,
        }
    }
}

/// Per-task fair share of `ceiling` given the number of concurrently active
/// tasks, or `cores_fallback` when the dynamic count is unavailable (0). The
/// divisor is floored at 1 so it is never zero.
fn fair_share_limit(ceiling: usize, active_tasks: usize, cores_fallback: usize) -> usize {
    let n = if active_tasks > 0 {
        active_tasks
    } else {
        cores_fallback
    };
    ceiling / n.max(1)
}

impl MemoryPool for RealUsageMemoryPool {
    fn name(&self) -> &str {
        "RealUsageMemoryPool"
    }

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
        // rejected without speculatively reserving the inner pool.
        if self.ceiling != 0 && additional != 0 {
            let real = self.balance_source.current();
            if real.saturating_add(additional) > self.ceiling {
                // `None` is first-come. `Some` spares a task still under its fair share so
                // one runaway task cannot starve small ones; the breaker backstops it.
                let reject = match self.fair_share {
                    None => true,
                    Some(cores) => {
                        let share = fair_share_limit(self.ceiling, active_task_count(), cores);
                        self.inner.reserved().saturating_add(additional) > share
                    }
                };
                if reject {
                    return Err(resources_datafusion_err!(
                        "Comet real-usage gate: native usage {real} bytes + requested \
                         {additional} bytes exceeds the off-heap budget of {} bytes; \
                         spilling/failing this consumer",
                        self.ceiling
                    ));
                }
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

    /// Pool with an injected real-usage balance, returned alongside its inner pool and a
    /// registered reservation so tests can assert on what was delegated.
    fn fixed_balance_pool(
        ceiling: usize,
        fair_share: Option<usize>,
        real: usize,
    ) -> (Arc<dyn MemoryPool>, Arc<dyn MemoryPool>, MemoryReservation) {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsageMemoryPool {
            inner: Arc::clone(&inner),
            ceiling,
            fair_share,
            balance_source: BalanceSource::Fixed(real),
        });
        let reservation = MemoryConsumer::new("test").register(&pool);
        (inner, pool, reservation)
    }

    #[test]
    fn under_ceiling_delegates_grow_and_shrink() {
        // real usage 100 + request 100 = 200 <= ceiling 1000
        let (inner, pool, reservation) = fixed_balance_pool(1000, None, 100);
        assert!(pool.try_grow(&reservation, 100).is_ok());
        assert_eq!(inner.reserved(), 100);
        pool.shrink(&reservation, 40);
        assert_eq!(inner.reserved(), 60);
    }

    #[test]
    fn over_ceiling_rejects_without_reserving_inner() {
        // real usage 900 + request 200 = 1100 > ceiling 1000
        let (inner, pool, reservation) = fixed_balance_pool(1000, None, 900);
        assert!(pool.try_grow(&reservation, 200).is_err());
        // inner pool is never touched on rejection, so there is nothing to roll back
        assert_eq!(inner.reserved(), 0);
    }

    #[test]
    fn zero_ceiling_never_gates() {
        let (inner, pool, reservation) = fixed_balance_pool(0, None, usize::MAX / 2);
        assert!(pool.try_grow(&reservation, 1024).is_ok());
        assert_eq!(inner.reserved(), 1024);
    }

    // The two fair-share cases below use ceiling 1000 and fallback divisor 2. No
    // task-shared pool is registered in tests, so the active count is 0 and the divisor
    // falls back to 2, giving a fair share of 500.

    #[test]
    fn over_ceiling_rejects_task_over_fair_share() {
        let (inner, pool, reservation) = fixed_balance_pool(1000, Some(2), 900);
        inner.grow(&reservation, 600);
        // over ceiling (900 + 200 > 1000) and over share (600 + 200 > 500) -> reject
        assert!(pool.try_grow(&reservation, 200).is_err());
    }

    #[test]
    fn over_ceiling_spares_task_at_or_under_fair_share() {
        let (inner, pool, reservation) = fixed_balance_pool(1000, Some(2), 1000);
        inner.grow(&reservation, 300);
        // over ceiling, but exactly at the share boundary (300 + 200 == 500) -> allowed
        assert!(pool.try_grow(&reservation, 200).is_ok());
        assert_eq!(inner.reserved(), 500);
    }

    // Drives a real heap allocation through the installed AccountingAllocator (only
    // wrapped under the `oom-guard` feature) and confirms the real-usage gate rejects.
    // Robust to parallel test noise: other allocations only raise the balance further,
    // which can only make the over-ceiling assertion more true.
    #[test]
    fn real_allocation_trips_real_usage_gate() {
        // The accounting allocator only updates the balance once tracking is on.
        oom_guard::enable_tracking();
        let inner: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let base = oom_guard::current_balance();
        // 4 MiB headroom over the (noisy) baseline.
        let ceiling = base + 4 * 1024 * 1024;
        let pool: Arc<dyn MemoryPool> =
            Arc::new(RealUsageMemoryPool::new(Arc::clone(&inner), ceiling, None));
        let reservation = MemoryConsumer::new("test").register(&pool);

        // Push real usage ~8 MiB above the baseline, held alive across the check so the
        // balance stays elevated. 8 MiB > 64 KiB settle threshold, so it flushes to BALANCE.
        let held: Vec<u8> = vec![0u8; 8 * 1024 * 1024];
        assert!(
            oom_guard::current_balance() > ceiling,
            "allocation should push balance over ceiling"
        );
        assert!(
            pool.try_grow(&reservation, 1).is_err(),
            "real usage over the ceiling should reject the grow"
        );
        // Keep `held` alive until after the assertions above.
        drop(held);
    }

    #[test]
    fn test_fair_share_limit() {
        assert_eq!(fair_share_limit(1000, 4, 8), 250); // active count wins
        assert_eq!(fair_share_limit(1000, 0, 5), 200); // falls back to cores
        assert_eq!(fair_share_limit(1000, 0, 0), 1000); // divisor floored at 1
        assert_eq!(fair_share_limit(3, 4, 8), 0); // ceiling below the divisor
    }
}
