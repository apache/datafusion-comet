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

use datafusion::{
    common::{resources_datafusion_err, DataFusionError},
    execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
};

use crate::alloc_accounting;

/// Wraps an off-heap memory pool so it refuses a reservation when the bytes the native allocator
/// has actually handed out, plus the request, would exceed a budget.
///
/// A pool on its own only counts what operators voluntarily reserve, so native memory that
/// bypasses it is invisible until the executor exceeds its container limit and is killed. This
/// wrapper consults the `alloc-accounting` balance before the inner pool's own check, which turns
/// that overrun into a `ResourcesExhausted` error at the next reservation instead. The operators
/// that can spill do so; the ones that cannot fail the task.
///
/// Both `fair_unified` and `greedy_unified` wear this unless
/// `spark.comet.exec.memoryPool.checkNativeUsage` is disabled. The budget is
/// `spark.memory.offHeap.size` multiplied by `spark.comet.exec.memoryPool.fraction`, the same
/// value the pools already receive as their limit.
///
/// Both the balance and the budget are process-wide. The balance counts every byte the Rust
/// allocator has served in this executor, not just this task's, and the budget is Comet's whole
/// off-heap allotment. So once any task pushes real usage to the budget, every task's next
/// non-zero reservation is denied. There is no per-task attribution, and allocations themselves
/// are never refused: this is a reservation gate, not a hard limit.
///
/// A build without the `alloc-accounting` feature reports a balance of zero, which leaves the
/// wrapper a passthrough.
pub struct CheckedMemoryPool<P: MemoryPool> {
    inner: P,
    budget: usize,
}

impl<P: MemoryPool> CheckedMemoryPool<P> {
    pub fn new(inner: P, budget: usize) -> Self {
        Self { inner, budget }
    }
}

impl<P: MemoryPool> Debug for CheckedMemoryPool<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CheckedMemoryPool")
            .field("budget", &self.budget)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<P: MemoryPool> Display for CheckedMemoryPool<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "CheckedMemoryPool(budget={}, inner={})",
            self.budget, self.inner
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
            return Err(resources_datafusion_err!(
                "Failed to reserve {additional} bytes for {}: native memory in use is {in_use} \
                 bytes of a {} byte budget. Reserved: {}. Lower \
                 spark.comet.exec.memoryPool.fraction to leave more headroom, or disable this \
                 check with spark.comet.exec.memoryPool.checkNativeUsage=false",
                reservation.consumer().name(),
                self.budget,
                self.reserved()
            ));
        }
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use std::sync::Arc;

    fn pool_with_budget(budget: usize) -> Arc<dyn MemoryPool> {
        Arc::new(CheckedMemoryPool::new(
            UnboundedMemoryPool::default(),
            budget,
        ))
    }

    #[test]
    fn a_zero_byte_grow_never_fails() {
        let pool = pool_with_budget(0);
        let reservation = MemoryConsumer::new("zero").register(&pool);
        reservation.try_grow(0).unwrap();
    }

    #[test]
    fn reports_the_budget_as_its_limit() {
        assert!(matches!(
            pool_with_budget(4096).memory_limit(),
            MemoryLimit::Finite(4096)
        ));
    }

    #[test]
    fn successful_grows_and_shrinks_reach_the_inner_pool() {
        let pool = pool_with_budget(usize::MAX);
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
        let pool = pool_with_budget(budget);
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
}
