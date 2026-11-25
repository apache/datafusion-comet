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

//! Lets a final aggregate read its spill files back past its share of memory (#6254).
//!
//! Once one of DataFusion's final or single aggregates has spilled, it merges its sorted spill
//! files and replays them through a `ClusteredFinalAggregateStream` that has no way to spill, so a
//! refused memory request there fails the task once no group is complete. The aggregates that do
//! this are `FinalHashAggregateStream`, `ClusteredFinalAggregateStream` (over input sorted on some
//! of the grouping keys, and which Comet also runs for a Spark PartialMerge over such input,
//! #6880), `SingleHashAggregateStream` (which Comet runs for other Spark PartialMerges, #6857) and
//! `ClusteredSingleAggregateStream`. The merge reserves read buffers for as many spill files as
//! fit, and those buffers belong to the same consumer as the replay.
//!
//! apache/datafusion#25383 makes the merge check that the consumer could also hold as much again
//! for the replay, but it gives that headroom back before the replay starts. The replay can still
//! be refused when Spark grants the memory elsewhere, when another consumer lowers the fair share
//! in the meantime, or when one group grows past the headroom, as a large `array_agg` state can.
//!
//! [`SpillReplayPool`] wraps a Comet pool. When the pool refuses a request from the replay, it
//! records the request with the pool's `grow` instead, which ignores `CometFairMemoryPool`'s limits
//! and carries what Spark doesn't grant as overcommit.
//!
//! This relies on the following in DataFusion 56, which a DataFusion upgrade has to re-check:
//!
//! - The four aggregates name their consumers `FinalHashAggregateStream[{partition}]`,
//!   `ClusteredFinalAggregateStream[{partition}]`, `SingleHashAggregateStream[{partition}]` and
//!   `ClusteredSingleAggregateStream[{partition}]`, and their merge and replay reserve through
//!   sibling reservations of that consumer (`aggregates/spill.rs`).
//! - A final aggregate grows one reservation while another of its reservations holds memory only
//!   during the replay, while the merge holds its read buffers. Before that, the aggregate's table
//!   is its only reservation holding memory, so a refusal stands and makes it spill. The merge
//!   picks its files, including its headroom check, through a single reservation, so a refusal
//!   there still limits how many it opens.
//! - The replay asks for memory only after it has aggregated a batch, so like a `grow`, the request
//!   is for memory that already exists.
//! - The replay emits every finished group after each batch, so it holds about one batch of groups
//!   plus the group in progress, and releasing memory repays the overcommit first.
//! - `ClusteredFinalAggregateStream` registers as able to spill even when its input is sorted on
//!   every grouping key, where it has no spill path, to work around apache/datafusion#17334.
//!   `CometFairMemoryPool` checks its requests only against the pool for that reason (see
//!   `limited_to_its_share` in `fair_pool.rs`). Drop that exception once #17334 is fixed and the
//!   stream registers as able to spill only when it can.
//!
//! #6583 tracks removing this. That needs the replay to be able to spill, or the merge to keep
//! its headroom for the replay.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use log::debug;
use parking_lot::Mutex;

/// Wraps a Comet pool and records the spill replay requests that it refuses; see the
/// [module documentation](self). Every other call, and every other refusal, is passed through
/// unchanged.
#[derive(Debug)]
pub(super) struct SpillReplayPool {
    task_attempt_id: i64,
    inner: Arc<dyn MemoryPool>,
    /// Bytes held by each final aggregate's consumer across all of its reservations, keyed by
    /// [`MemoryConsumer::id`], because `reservation.size()` covers only one reservation. Other
    /// consumers aren't tracked, so they never take the lock.
    final_aggregates: Mutex<HashMap<usize, usize>>,
}

impl SpillReplayPool {
    pub(super) fn new(task_attempt_id: i64, inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            task_attempt_id,
            inner,
            final_aggregates: Mutex::new(HashMap::new()),
        }
    }

    /// Applies `update` to what `reservation`'s consumer holds, if it is a final aggregate.
    fn track(&self, reservation: &MemoryReservation, update: impl FnOnce(&mut usize)) {
        if is_final_aggregate(reservation.consumer()) {
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
    /// files back, the only time one of its reservations grows while another holds memory.
    fn is_spill_replay(&self, reservation: &MemoryReservation) -> bool {
        is_final_aggregate(reservation.consumer())
            && self
                .final_aggregates
                .lock()
                .get(&reservation.consumer().id())
                .is_some_and(|&consumer_used| consumer_used > reservation.size())
    }
}

impl fmt::Display for SpillReplayPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.inner.as_ref(), f)
    }
}

impl MemoryPool for SpillReplayPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if is_final_aggregate(consumer) {
            self.final_aggregates.lock().insert(consumer.id(), 0);
        }
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if is_final_aggregate(consumer) {
            self.final_aggregates.lock().remove(&consumer.id());
        }
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.track(reservation, |used| *used = used.saturating_add(additional));
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.track(reservation, |used| *used = used.saturating_sub(shrink));
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        match self.inner.try_grow(reservation, additional) {
            Ok(()) => {}
            Err(DataFusionError::ResourcesExhausted(refusal))
                if self.is_spill_replay(reservation) =>
            {
                debug!(
                    "Task {} records {additional} bytes for {} while it reads its spill files back: {refusal}",
                    self.task_attempt_id,
                    reservation.consumer().name()
                );
                self.inner.grow(reservation, additional);
            }
            Err(e) => return Err(e),
        }
        self.track(reservation, |used| *used = used.saturating_add(additional));
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// The pool that `pool` wraps, if it is a [`SpillReplayPool`].
pub(super) fn unwrap_spill_replay(pool: &Arc<dyn MemoryPool>) -> Option<&Arc<dyn MemoryPool>> {
    pool.downcast_ref::<SpillReplayPool>()
        .map(|replay| &replay.inner)
}

/// The prefix of the name DataFusion gives the consumer of a `ClusteredFinalAggregateStream`, the
/// final aggregate over input sorted on some or all of its keys.
pub(super) const CLUSTERED_FINAL_AGGREGATE: &str = "ClusteredFinalAggregateStream[";

/// Whether `consumer` belongs to a final or single aggregate, whose spill replay can't spill.
fn is_final_aggregate(consumer: &MemoryConsumer) -> bool {
    let name = consumer.name();
    name.starts_with("FinalHashAggregateStream[")
        || name.starts_with(CLUSTERED_FINAL_AGGREGATE)
        || name.starts_with("SingleHashAggregateStream[")
        || name.starts_with("ClusteredSingleAggregateStream[")
}

#[cfg(test)]
mod tests {
    use super::super::spark_memory::fake::FakeSpark;
    use super::super::testing::{hold, spillable};
    use super::super::{create_pool, overcommit, MemoryPoolConfig, MemoryPoolType, RunningTasks};
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;

    /// A pool of each type for a task that runs alone, built the way `createPlan` builds it and
    /// connected to a fake Spark that grants at most 100 bytes. The fair pool's own limits are far
    /// above that, so only Spark refuses. Task-shared pools are keyed by task attempt process-wide,
    /// so each pool needs its own id.
    fn each_pool_type(
        task_attempt_ids: [i64; 2],
    ) -> Vec<(&'static str, Arc<dyn MemoryPool>, Arc<FakeSpark>)> {
        [
            ("greedy_unified", MemoryPoolType::GreedyUnified),
            ("fair_unified", MemoryPoolType::FairUnified),
        ]
        .into_iter()
        .zip(task_attempt_ids)
        .map(|((name, pool_type), task_attempt_id)| {
            let fake = FakeSpark::with(100);
            let config = MemoryPoolConfig::new(pool_type, 1000);
            let pool = create_pool(&config, task_attempt_id, &RunningTasks::default(), || {
                fake.memory()
            });
            (name, pool, fake)
        })
        .collect()
    }

    /// A `fair_unified` pool of `pool_size` bytes for a task that runs alone, built the way
    /// `createPlan` builds it and connected to a fake Spark that grants everything, so only the
    /// pool's own limits refuse.
    fn fair_pool(task_attempt_id: i64, pool_size: usize) -> (Arc<dyn MemoryPool>, Arc<FakeSpark>) {
        let fake = FakeSpark::with(usize::MAX);
        let config = MemoryPoolConfig::new(MemoryPoolType::FairUnified, pool_size);
        let pool = create_pool(&config, task_attempt_id, &RunningTasks::default(), || {
            fake.memory()
        });
        (pool, fake)
    }

    #[test]
    fn a_final_aggregate_reading_its_spill_files_back_carries_what_spark_refuses() {
        for (name, pool, fake) in each_pool_type([-3011, -3012]) {
            // Like DataFusion's final hash aggregate, whose spill merge and replay table are
            // sibling reservations of one consumer.
            let merge = spillable("FinalHashAggregateStream[0]", &pool);
            let replay = merge.new_empty();
            merge.try_grow(90).unwrap();

            // Spark grants 10 of the replay's 30 bytes, and the other 20 are overcommit.
            replay.try_grow(30).unwrap();
            assert_eq!(pool.reserved(), 120, "{name}");
            assert_eq!(fake.held(), 100, "{name}");
            assert_eq!(overcommit(&pool), 20, "{name}");

            // The replay emits groups and shrinks, which repays the overcommit before Spark.
            replay.shrink(25);
            assert_eq!(overcommit(&pool), 0, "{name}");
            assert_eq!(fake.held(), 95, "{name}");
            drop(merge);
            drop(replay);
            assert_eq!(pool.reserved(), 0, "{name}");
            assert_eq!(fake.held(), 0, "{name}");
        }
    }

    #[test]
    fn a_final_aggregate_reading_its_spill_files_back_may_pass_its_fair_limit() {
        let (pool, fake) = fair_pool(-3013, 100);
        let merge = spillable("FinalHashAggregateStream[0]", &pool);
        let replay = merge.new_empty();
        merge.try_grow(90).unwrap();

        // The aggregate is the only consumer, so its share is the whole pool.
        replay.try_grow(30).unwrap();
        assert_eq!(pool.reserved(), 120);
        assert_eq!(fake.held(), 120);
        assert_eq!(overcommit(&pool), 0);
    }

    /// DataFusion 56's merge asks for its read buffers plus as much again for the replay, then gives
    /// the extra back before the replay starts. A consumer that becomes active in between lowers
    /// the aggregate's share below what the replay needs.
    #[test]
    fn a_final_aggregate_reading_its_spill_files_back_may_pass_a_share_lowered_after_the_merge() {
        let (pool, fake) = fair_pool(-3021, 100);
        let merge = spillable("FinalHashAggregateStream[0]", &pool);
        let replay = merge.new_empty();
        hold(&merge, 80);
        merge.shrink(40);

        let sort = spillable("ExternalSorter[0]", &pool);
        hold(&sort, 10);

        // 40 + 15 is over the aggregate's share of 50 now that the sort is active.
        replay.try_grow(15).unwrap();
        assert_eq!(pool.reserved(), 65);
        assert_eq!(fake.held(), 65);
        assert_eq!(overcommit(&pool), 0);
    }

    #[test]
    fn a_final_aggregate_reading_its_spill_files_back_may_pass_the_pool_limit() {
        let (pool, fake) = fair_pool(-3014, 100);
        let sort = spillable("ExternalSorter[0]", &pool);
        hold(&sort, 70);
        let merge = spillable("FinalHashAggregateStream[0]", &pool);
        let replay = merge.new_empty();
        merge.try_grow(25).unwrap();

        // 25 + 20 is within the aggregate's share of 50, but 70 + 25 + 20 is over the pool's 100.
        replay.try_grow(20).unwrap();
        assert_eq!(pool.reserved(), 115);
        assert_eq!(fake.held(), 115);
        assert_eq!(overcommit(&pool), 0);
    }

    #[test]
    fn other_refusals_are_unchanged() {
        for (name, pool, fake) in each_pool_type([-3015, -3016]) {
            // While the aggregate reads its input, its table is the consumer's only reservation
            // holding memory, so a refusal makes it spill.
            let table = spillable("FinalHashAggregateStream[0]", &pool);
            let replay = table.new_empty();
            table.try_grow(90).unwrap();
            assert!(table.try_grow(30).is_err(), "{name}");
            drop(table);

            // The merge picks its spill files while nothing else is held, so a refusal still
            // limits how many it opens.
            let merge = replay.new_empty();
            merge.try_grow(90).unwrap();
            assert!(merge.try_grow(30).is_err(), "{name}");
            drop(merge);

            // Any other operator with a sibling holding memory is still refused.
            let sort = MemoryConsumer::new("ExternalSorterMerge[0]").register(&pool);
            let sibling = sort.new_empty();
            sort.try_grow(90).unwrap();
            assert!(sibling.try_grow(30).is_err(), "{name}");
            assert_eq!(pool.reserved(), 90, "{name}");
            assert_eq!(fake.held(), 90, "{name}");
        }
    }

    #[test]
    fn a_failed_spark_call_during_the_replay_is_not_recorded() {
        for (name, pool_type, task_attempt_id) in [
            ("greedy_unified", MemoryPoolType::GreedyUnified, -3017),
            ("fair_unified", MemoryPoolType::FairUnified, -3018),
        ] {
            let fake = FakeSpark::failing();
            let config = MemoryPoolConfig::new(pool_type, 1000);
            let pool = create_pool(&config, task_attempt_id, &RunningTasks::default(), || {
                fake.memory()
            });
            let merge = spillable("FinalHashAggregateStream[0]", &pool);
            let replay = merge.new_empty();
            merge.grow(90);

            // Only a refusal is recorded. A JNI error goes back to the aggregate.
            let err = replay.try_grow(30).unwrap_err();
            assert!(
                !matches!(err, DataFusionError::ResourcesExhausted(_)),
                "{name}: {err}"
            );
            assert_eq!(pool.reserved(), 90, "{name}");
        }
    }

    #[test]
    fn a_final_aggregate_is_tracked_across_its_reservations_until_it_unregisters() {
        let pool = Arc::new(SpillReplayPool::new(
            -3019,
            Arc::new(UnboundedMemoryPool::default()),
        ));
        let dyn_pool: Arc<dyn MemoryPool> = Arc::clone(&pool) as _;
        let merge = MemoryConsumer::new("FinalHashAggregateStream[0]").register(&dyn_pool);
        let replay = merge.new_empty();
        merge.try_grow(90).unwrap();
        replay.grow(20);
        replay.shrink(5);
        let sort = MemoryConsumer::new("ExternalSorter[0]").register(&dyn_pool);
        sort.grow(10);
        let id = merge.consumer().id();
        assert_eq!(*pool.final_aggregates.lock(), HashMap::from([(id, 105)]));

        drop(merge);
        drop(replay);
        assert!(pool.final_aggregates.lock().is_empty());
    }

    #[test]
    fn only_final_aggregates_replay_their_spill_files() {
        for name in [
            "FinalHashAggregateStream[3]",
            "ClusteredFinalAggregateStream[0]",
            "SingleHashAggregateStream[0]",
            "ClusteredSingleAggregateStream[0]",
        ] {
            assert!(is_final_aggregate(&MemoryConsumer::new(name)), "{name}");
        }
        // Partial aggregates emit early instead of spilling.
        for name in [
            "PartialHashAggregateStream[0]",
            "ClusteredPartialAggregateStream[0]",
            "PartialReduceHashAggregateStream[0]",
            "ExternalSorterMerge[0]",
        ] {
            assert!(!is_final_aggregate(&MemoryConsumer::new(name)), "{name}");
        }
    }
}
