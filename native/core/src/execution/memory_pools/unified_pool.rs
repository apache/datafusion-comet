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

use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use super::{spark_memory::SparkMemory, spill_replay};
use datafusion::{
    common::{resources_datafusion_err, DataFusionError},
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
};
use log::{debug, warn};
use parking_lot::Mutex;

/// A DataFusion `MemoryPool` implementation for Comet that delegates to
/// Spark's off-heap executor memory pool via JNI by calling
/// [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometUnifiedMemoryPool {
    spark: SparkMemory,
    used: AtomicUsize,
    /// Bytes held by each final aggregate's consumer across its reservations, keyed by
    /// [`MemoryConsumer::id`], which [`spill_replay`] needs. Other consumers aren't tracked, so
    /// they take the lock only when Spark refuses them.
    final_aggregates: Mutex<HashMap<usize, usize>>,
}

impl Debug for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CometUnifiedMemoryPool")
            .field("used", &self.used.load(Relaxed))
            .field("overcommit", &self.spark.overcommit())
            .finish()
    }
}

impl CometUnifiedMemoryPool {
    pub(super) fn with_spark(spark: SparkMemory) -> CometUnifiedMemoryPool {
        Self {
            spark,
            used: AtomicUsize::new(0),
            final_aggregates: Mutex::new(HashMap::new()),
        }
    }

    /// The part of [`MemoryPool::reserved`] that Spark has not granted; see [`SparkMemory`].
    pub(super) fn overcommit(&self) -> usize {
        self.spark.overcommit()
    }

    /// Applies `update` to what `reservation`'s consumer holds, if it is a final aggregate.
    fn track(&self, reservation: &MemoryReservation, update: impl FnOnce(&mut usize)) {
        if spill_replay::is_final_aggregate(reservation.consumer()) {
            if let Some(used) = self
                .final_aggregates
                .lock()
                .get_mut(&reservation.consumer().id())
            {
                update(used);
            }
        }
    }

    /// Whether a refused request from `reservation` comes from a final aggregate reading its spill
    /// files back, which can't spill; see [`spill_replay`].
    fn is_spill_replay(&self, reservation: &MemoryReservation) -> bool {
        let consumer_used = self
            .final_aggregates
            .lock()
            .get(&reservation.consumer().id())
            .copied()
            .unwrap_or(0);
        spill_replay::is_spill_replay(reservation, consumer_used)
    }
}

impl Drop for CometUnifiedMemoryPool {
    fn drop(&mut self) {
        let used = self.used.load(Relaxed);
        if used != 0 {
            warn!(
                "Task {} dropped CometUnifiedMemoryPool with {used} bytes still reserved",
                self.spark.task_attempt_id()
            );
        }
    }
}

impl Display for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "CometUnifiedMemoryPool(used={}, overcommit={})",
            self.used.load(Relaxed),
            self.spark.overcommit()
        )
    }
}

impl MemoryPool for CometUnifiedMemoryPool {
    fn name(&self) -> &str {
        "CometUnifiedMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if spill_replay::is_final_aggregate(consumer) {
            self.final_aggregates.lock().insert(consumer.id(), 0);
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if spill_replay::is_final_aggregate(consumer) {
            self.final_aggregates.lock().remove(&consumer.id());
        }
    }

    /// Records memory that already exists, so it must not fail; see [`SparkMemory`].
    // Rust 1.99 deprecates `fetch_update` in favor of `try_update`, which needs Rust 1.95, newer
    // than the workspace `rust-version`.
    #[allow(deprecated)]
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        if additional == 0 {
            return;
        }
        self.spark.acquire(additional);
        self.used
            .fetch_update(Relaxed, Relaxed, |old| Some(old.saturating_add(additional)))
            .unwrap();
        self.track(reservation, |used| *used = used.saturating_add(additional));
    }

    // Rust 1.99 deprecates `fetch_update` in favor of `try_update`, which needs Rust 1.95, newer
    // than the workspace `rust-version`.
    #[allow(deprecated)]
    fn shrink(&self, reservation: &MemoryReservation, size: usize) {
        if let Err(e) = self.spark.release(size) {
            panic!(
                "Task {} failed to return {size} bytes to Spark: {e:?}",
                self.spark.task_attempt_id()
            );
        }
        if let Err(prev) = self
            .used
            .fetch_update(Relaxed, Relaxed, |old| old.checked_sub(size))
        {
            panic!(
                "Task {} overflow when releasing {size} of {prev} bytes",
                self.spark.task_attempt_id()
            );
        }
        self.track(reservation, |used| *used = used.saturating_sub(size));
    }

    // Rust 1.99 deprecates `fetch_update` in favor of `try_update`, which needs Rust 1.95, newer
    // than the workspace `rust-version`.
    #[allow(deprecated)]
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            // A partial grant is handed back and refused, which triggers spilling in the caller.
            if let Err(refusal) = self.spark.try_acquire(additional)? {
                let err = resources_datafusion_err!(
                    "Task {} failed to acquire {} bytes plus {} bytes overcommitted, only got {}. Reserved: {}",
                    self.spark.task_attempt_id(),
                    additional,
                    refusal.overcommit,
                    refusal.granted,
                    self.reserved()
                );
                if !self.is_spill_replay(reservation) {
                    return Err(err);
                }
                debug!(
                    "Task {} records {additional} bytes for {} while it reads its spill files back: {err}",
                    self.spark.task_attempt_id(),
                    reservation.consumer().name()
                );
                self.grow(reservation, additional);
                return Ok(());
            }
            if let Err(prev) = self
                .used
                .fetch_update(Relaxed, Relaxed, |old| old.checked_add(additional))
            {
                return Err(resources_datafusion_err!(
                    "Task {} failed to acquire {} bytes due to overflow. Reserved: {}",
                    self.spark.task_attempt_id(),
                    additional,
                    prev
                ));
            }
            self.track(reservation, |used| *used = used.saturating_add(additional));
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::super::spark_memory::fake::FakeSpark;
    use super::*;
    use std::sync::Arc;

    #[test]
    fn grow_past_spark_is_recorded_and_refuses_try_grow_until_repaid() {
        let fake = FakeSpark::with(100);
        let pool: Arc<dyn MemoryPool> = Arc::new(CometUnifiedMemoryPool::with_spark(fake.memory()));
        let reservation = MemoryConsumer::new("smj").register(&pool);

        // Spark grants 100 of the 150 bytes.
        reservation.grow(150);
        assert_eq!(pool.reserved(), 150);
        assert_eq!(fake.held(), 100);
        // Spark has room for one more byte, but not for the 50 bytes it was never asked to back.
        fake.set_limit(101);
        assert!(reservation.try_grow(1).is_err());

        // Shrinking repays the overcommit first, then hands the rest back to Spark.
        reservation.shrink(60);
        assert_eq!(fake.held(), 90);
        assert!(reservation.try_grow(1).is_ok());

        drop(reservation);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(fake.held(), 0);
    }

    #[test]
    fn concurrent_consumers_hand_spark_back_exactly_what_it_granted() {
        use rand::{rngs::StdRng, RngExt, SeedableRng};
        use std::thread;

        let fake = FakeSpark::with(1_000);
        let pool = Arc::new(CometUnifiedMemoryPool::with_spark(fake.memory()));
        let threads: Vec<_> = (0..8)
            .map(|seed| {
                let pool = Arc::clone(&pool) as Arc<dyn MemoryPool>;
                let fake = Arc::clone(&fake);
                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(seed);
                    let reservation = MemoryConsumer::new(format!("c{seed}")).register(&pool);
                    for _ in 0..10_000 {
                        match rng.random_range(0..4) {
                            0 => reservation.grow(rng.random_range(1..200)),
                            1 => {
                                let _ = reservation.try_grow(rng.random_range(1..200));
                            }
                            2 => {
                                let size = reservation.size();
                                if size > 0 {
                                    reservation.shrink(rng.random_range(1..=size));
                                }
                            }
                            // Spark's other consumers take and return memory.
                            _ => fake.set_limit(rng.random_range(0..2_000)),
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        // FakeSpark panics on an over-return, so a bad interleaving fails the join above, and
        // anything left once every reservation has been dropped is a leak.
        assert_eq!(pool.reserved(), 0);
        assert_eq!(pool.spark.overcommit(), 0);
        assert_eq!(fake.held(), 0);
    }

    #[test]
    fn a_final_aggregate_is_tracked_across_its_reservations_until_it_unregisters() {
        let pool = Arc::new(CometUnifiedMemoryPool::with_spark(
            FakeSpark::with(100).memory(),
        ));
        let dyn_pool: Arc<dyn MemoryPool> = Arc::clone(&pool) as _;
        let merge = MemoryConsumer::new("FinalHashAggregateStream[0]").register(&dyn_pool);
        let replay = merge.new_empty();
        merge.try_grow(90).unwrap();
        replay.grow(20);
        replay.shrink(5);
        let id = merge.consumer().id();
        assert_eq!(pool.final_aggregates.lock().get(&id), Some(&105));

        drop(merge);
        drop(replay);
        assert!(pool.final_aggregates.lock().is_empty());
    }
}
