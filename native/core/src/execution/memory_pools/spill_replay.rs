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

//! Lets a final aggregate read its spill files back past its share of memory.
//!
//! Once one of DataFusion 55's final aggregates has spilled, it merges its sorted spill files and
//! replays them through an `OrderedFinalAggregateStream` that has no way to spill, so a refused
//! memory request there fails the task (#6254). `FinalHashAggregateStream` does this, and so does
//! `OrderedFinalAggregateStream` itself, which DataFusion uses when the input is sorted on some of
//! the grouping keys. The merge reserves read buffers for as many spill files as fit, and those
//! buffers belong to the same consumer, so the replay often finds the consumer's share already
//! taken. The replay only asks for memory once it has aggregated a batch, so like a `grow`, the
//! request is for memory that already exists. The pools record it the way they record a `grow`,
//! carrying what Spark doesn't grant as overcommit. The replay emits every finished group after
//! each batch, so it holds about one batch of groups, and releasing memory repays the overcommit
//! first.
//!
//! Remove this once Comet's DataFusion has apache/datafusion#25383, which leaves the replay room
//! when the merge picks its files. The #6254 tests in `CometAggregateSuite` fail without this on
//! DataFusion 55.1, so they show whether the replay still needs it.

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};

/// Whether `consumer` belongs to a final aggregate whose spill replay can't spill.
pub(super) fn is_final_aggregate(consumer: &MemoryConsumer) -> bool {
    let name = consumer.name();
    name.starts_with("FinalHashAggregateStream[")
        || name.starts_with("OrderedFinalAggregateStream[")
}

/// Whether a refused request from `reservation` should be recorded instead. `consumer_used` is
/// what the reservation's consumer holds across all of its reservations.
///
/// A final aggregate grows one reservation while another of its reservations holds memory only
/// during the replay, while the merge holds its read buffers. Before that, the aggregate's table
/// is its only reservation holding memory, so a refusal stands and makes it spill. The merge picks
/// its files while nothing else is held, so a refusal there still limits how many it opens.
pub(super) fn is_spill_replay(reservation: &MemoryReservation, consumer_used: usize) -> bool {
    consumer_used > reservation.size() && is_final_aggregate(reservation.consumer())
}

#[cfg(test)]
mod tests {
    use super::super::spark_memory::fake::FakeSpark;
    use super::super::{create_pool, overcommit, MemoryPoolConfig, MemoryPoolType};
    use super::*;
    use datafusion::execution::memory_pool::MemoryPool;
    use std::sync::Arc;

    /// A pool of each type, built the way `createPlan` builds it and connected to a fake Spark
    /// that grants at most 100 bytes. The fair pool's own limits are far above that, so only
    /// Spark refuses. Task-shared pools are keyed by task attempt process-wide, so each pool needs
    /// its own id.
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
            let pool = create_pool(&config, task_attempt_id, || fake.memory());
            (name, pool, fake)
        })
        .collect()
    }

    #[test]
    fn a_final_aggregate_reading_its_spill_files_back_carries_what_spark_refuses() {
        for (name, pool, fake) in each_pool_type([-3011, -3012]) {
            // Like DataFusion 55's final hash aggregate, whose spill merge and replay table are
            // sibling reservations of one consumer.
            let merge = MemoryConsumer::new("FinalHashAggregateStream[0]").register(&pool);
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
    fn other_refusals_are_unchanged() {
        for (name, pool, fake) in each_pool_type([-3013, -3014]) {
            // While the aggregate reads its input, its table is the consumer's only reservation
            // holding memory, so a refusal makes it spill.
            let table = MemoryConsumer::new("FinalHashAggregateStream[0]").register(&pool);
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
    fn only_final_aggregates_replay_their_spill_files() {
        for name in [
            "FinalHashAggregateStream[3]",
            "OrderedFinalAggregateStream[0]",
        ] {
            assert!(is_final_aggregate(&MemoryConsumer::new(name)), "{name}");
        }
        // Partial aggregates emit early instead of spilling, and Comet never plans single ones.
        for name in [
            "PartialHashAggregateStream[0]",
            "OrderedPartialAggregateStream[0]",
            "SingleHashAggregateStream[0]",
            "ExternalSorterMerge[0]",
        ] {
            assert!(!is_final_aggregate(&MemoryConsumer::new(name)), "{name}");
        }
    }
}
