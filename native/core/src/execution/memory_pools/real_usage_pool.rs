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
    /// Fixed fallback divisor (concurrent-task count) used when the dynamic
    /// active-task count is 0. `None` disables the fair-share guard (first-come),
    /// used for pools whose `reserved()` is process-wide.
    fair_share: Option<usize>,
    balance_source: BalanceSource,
}

impl std::fmt::Debug for RealUsagePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealUsagePool")
            .field("inner", &self.inner)
            .field("ceiling", &self.ceiling)
            .field("fair_share", &self.fair_share)
            .finish_non_exhaustive()
    }
}

impl RealUsagePool {
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
            balance_source: Arc::new(oom_guard::current_balance),
        }
    }

    /// Wrap `inner` with an explicit balance source (test seam).
    #[cfg(test)]
    fn with_balance_source(
        inner: Arc<dyn MemoryPool>,
        ceiling: usize,
        fair_share: Option<usize>,
        balance_source: BalanceSource,
    ) -> Self {
        Self {
            inner,
            ceiling,
            fair_share,
            balance_source,
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

/// Given the process is already over the real-usage ceiling, decide whether to
/// reject this task's grow. `None` is first-come (reject whoever hit the ceiling);
/// `Some(s)` rejects only a task whose tracked reservation would exceed its fair
/// share `s`, sparing under-share tasks (the OomGuard breaker backstops runaway
/// cases).
fn should_reject_over_ceiling(reserved: usize, additional: usize, share: Option<usize>) -> bool {
    match share {
        None => true,
        Some(s) => reserved.saturating_add(additional) > s,
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
        // rejected without speculatively reserving the inner pool. When the process is
        // over the ceiling, the fair-share guard rejects only a task whose own tracked
        // reservation exceeds its fair share, sparing innocent small tasks; the OomGuard
        // breaker backstops runaway cases. Returning `ResourcesExhausted` lets DataFusion
        // spill and retry.
        if self.ceiling != 0 && additional != 0 {
            let real = (self.balance_source)();
            if real.saturating_add(additional) > self.ceiling {
                let share = self
                    .fair_share
                    .map(|cores| fair_share_limit(self.ceiling, active_task_count(), cores));
                if should_reject_over_ceiling(self.inner.reserved(), additional, share) {
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
            None,
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
            None,
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
            None,
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
            None,
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
        let pool: Arc<dyn MemoryPool> =
            Arc::new(RealUsagePool::new(Arc::clone(&inner), ceiling, None));
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

    #[test]
    fn fair_share_limit_uses_active_count_when_positive() {
        // active count wins over the fallback divisor
        assert_eq!(fair_share_limit(1000, 4, 8), 250);
    }

    #[test]
    fn fair_share_limit_falls_back_when_no_active_tasks() {
        assert_eq!(fair_share_limit(1000, 0, 5), 200);
    }

    #[test]
    fn fair_share_limit_floors_divisor_at_one() {
        // active and fallback both zero -> divide by 1, no panic
        assert_eq!(fair_share_limit(1000, 0, 0), 1000);
    }

    #[test]
    fn fair_share_limit_zero_when_ceiling_below_n() {
        assert_eq!(fair_share_limit(3, 4, 8), 0);
    }

    #[test]
    fn should_reject_none_is_first_come() {
        assert!(should_reject_over_ceiling(0, 1, None));
        assert!(should_reject_over_ceiling(1000, 0, None));
    }

    #[test]
    fn should_reject_some_only_above_share() {
        // strictly above share -> reject
        assert!(should_reject_over_ceiling(400, 200, Some(500)));
        // exactly at share -> allow
        assert!(!should_reject_over_ceiling(300, 200, Some(500)));
        // below share -> allow
        assert!(!should_reject_over_ceiling(100, 100, Some(500)));
    }

    #[test]
    fn over_ceiling_rejects_task_over_fair_share() {
        // ceiling 1000, fallback divisor 2, active count 0 in tests -> fair share 500
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let balance = Arc::new(AtomicUsize::new(900));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::with_balance_source(
            Arc::clone(&inner),
            1000,
            Some(2),
            fixed_source(balance),
        ));
        let reservation = MemoryConsumer::new("test").register(&pool);
        // Put this task above its 500-byte fair share.
        inner.grow(&reservation, 600);
        // Over ceiling (900 + 200 > 1000) AND over fair share (600 + 200 > 500) -> reject.
        assert!(pool.try_grow(&reservation, 200).is_err());
    }

    #[test]
    fn over_ceiling_spares_task_under_fair_share() {
        // ceiling 1000, fallback divisor 2, active count 0 in tests -> fair share 500
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let balance = Arc::new(AtomicUsize::new(1000));
        let pool: Arc<dyn MemoryPool> = Arc::new(RealUsagePool::with_balance_source(
            Arc::clone(&inner),
            1000,
            Some(2),
            fixed_source(balance),
        ));
        let reservation = MemoryConsumer::new("test").register(&pool);
        // This task holds only 100, under its 500 fair share.
        inner.grow(&reservation, 100);
        // Over ceiling (1000 + 50 > 1000) but under fair share (100 + 50 <= 500) -> allowed.
        assert!(pool.try_grow(&reservation, 50).is_ok());
        // The grow was delegated to the inner pool.
        assert_eq!(inner.reserved(), 150);
    }
}
