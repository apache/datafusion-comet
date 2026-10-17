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

mod config;
mod fair_pool;
pub mod logging_pool;
mod plan_pool;
mod spark_memory;
mod spill_replay;
mod task_shared;
#[cfg(test)]
pub(crate) mod testing;
mod unified_pool;

use datafusion::execution::memory_pool::{MemoryPool, TrackConsumersPool, UnboundedMemoryPool};
use fair_pool::CometFairMemoryPool;
use jni::objects::{Global, JObject};
use spark_memory::SparkMemory;
use spill_replay::SpillReplayPool;
use std::num::NonZeroUsize;
use std::sync::Arc;
use unified_pool::CometUnifiedMemoryPool;

pub(crate) use config::*;
pub(crate) use plan_pool::PlanMemoryPool;
use task_shared::{acquire_task_shared_pool, RunningTasks};

/// Creates the memory pool for a native plan.
///
/// Task-shared pools use their returned `Arc` as the RAII handle, so they remain registered for as
/// long as the plan or any of its reservations retain the pool.
pub(crate) fn create_memory_pool(
    memory_pool_config: &MemoryPoolConfig,
    comet_task_memory_manager: Arc<Global<JObject<'static>>>,
    task_attempt_id: i64,
) -> Arc<dyn MemoryPool> {
    create_pool(
        memory_pool_config,
        task_attempt_id,
        RunningTasks::of_executor(),
        || SparkMemory::new(comet_task_memory_manager, task_attempt_id),
    )
}

/// Creates the pool that [`create_memory_pool`] does, counting the task in `running_tasks` and
/// with `spark` connecting it to Spark's memory manager, so that tests can use their own count and
/// connect it to a fake instead.
fn create_pool(
    memory_pool_config: &MemoryPoolConfig,
    task_attempt_id: i64,
    running_tasks: &RunningTasks,
    spark: impl FnOnce() -> SparkMemory,
) -> Arc<dyn MemoryPool> {
    const NUM_TRACKED_CONSUMERS: usize = 10;

    fn tracked(pool: impl MemoryPool + 'static) -> Arc<dyn MemoryPool> {
        Arc::new(TrackConsumersPool::new(
            pool,
            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
        ))
    }

    let pool_type = memory_pool_config.pool_type;
    let pool_size = memory_pool_config.pool_size;
    let off_heap_size = memory_pool_config.off_heap_size;

    match pool_type {
        MemoryPoolType::GreedyUnified => {
            acquire_task_shared_pool(task_attempt_id, running_tasks, || {
                Arc::new(SpillReplayPool::new(
                    task_attempt_id,
                    tracked(CometUnifiedMemoryPool::with_spark(spark())),
                ))
            })
        }
        MemoryPoolType::FairUnified => {
            acquire_task_shared_pool(task_attempt_id, running_tasks, || {
                Arc::new(SpillReplayPool::new(
                    task_attempt_id,
                    tracked(CometFairMemoryPool::with_spark(
                        spark(),
                        pool_size,
                        off_heap_size,
                        running_tasks.clone(),
                    )),
                ))
            })
        }
        MemoryPoolType::Unbounded => Arc::new(UnboundedMemoryPool::default()),
    }
}

/// The bytes that `pool` has recorded beyond what Spark granted it, which it carries as overcommit
/// until Spark grants them or the pool frees memory; see [`SparkMemory`]. This looks through the
/// wrappers that [`create_memory_pool`] puts around a Comet pool, and is zero for a pool that
/// takes nothing from Spark or that the function did not create.
pub(crate) fn overcommit(pool: &Arc<dyn MemoryPool>) -> usize {
    let pool = task_shared::unwrap_task_shared(pool).unwrap_or(pool);
    let pool = spill_replay::unwrap_spill_replay(pool).unwrap_or(pool);
    if let Some(tracked) = pool.downcast_ref::<TrackConsumersPool<CometUnifiedMemoryPool>>() {
        tracked.inner().overcommit()
    } else if let Some(tracked) = pool.downcast_ref::<TrackConsumersPool<CometFairMemoryPool>>() {
        tracked.inner().overcommit()
    } else {
        0
    }
}

/// [`create_memory_pool`] for a task that runs alone, connected to a fake Spark that grants at most
/// `limit` bytes.
#[cfg(test)]
pub(crate) fn create_memory_pool_with_fake_spark(
    memory_pool_config: &MemoryPoolConfig,
    task_attempt_id: i64,
    limit: usize,
) -> Arc<dyn MemoryPool> {
    let fake = spark_memory::fake::FakeSpark::with(limit);
    create_pool(
        memory_pool_config,
        task_attempt_id,
        &RunningTasks::default(),
        || fake.memory(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    #[test]
    fn overcommit_is_read_through_the_wrappers_of_each_pool_type() {
        // Task-shared pools are keyed by task attempt process-wide, so each gets its own id.
        for (name, task_attempt_id, pool_type) in [
            ("greedy_unified", -3001, MemoryPoolType::GreedyUnified),
            ("fair_unified", -3002, MemoryPoolType::FairUnified),
        ] {
            let config = MemoryPoolConfig::new(pool_type, 1000);
            let pool = create_memory_pool_with_fake_spark(&config, task_attempt_id, 100);
            let reservation = MemoryConsumer::new("spill reader").register(&pool);

            // Spark grants 100 of the 150 bytes, and the pool records all of them.
            reservation.grow(150);
            assert_eq!(pool.reserved(), 150, "{name}");
            assert_eq!(overcommit(&pool), 50, "{name}");

            // Freeing memory repays the overcommit first.
            reservation.shrink(30);
            assert_eq!(overcommit(&pool), 20, "{name}");
            drop(reservation);
            assert_eq!(overcommit(&pool), 0, "{name}");
        }
    }

    #[test]
    fn a_task_shares_the_pool_with_the_other_tasks_running_native_plans() {
        let tasks = RunningTasks::default();
        let fake = spark_memory::fake::FakeSpark::with(usize::MAX);
        let fair = MemoryPoolConfig::new(MemoryPoolType::FairUnified, 100);
        let greedy = MemoryPoolConfig::new(MemoryPoolType::GreedyUnified, 0);
        let pool = create_pool(&fair, -3004, &tasks, || fake.memory());
        let other_task = create_pool(&greedy, -3005, &tasks, || fake.memory());
        let sort = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&pool);

        // A task with the other pool type counts too.
        sort.try_grow(50).unwrap();
        assert!(sort.try_grow(1).is_err());

        // Once it finishes, the task has the pool to itself.
        drop(other_task);
        sort.try_grow(50).unwrap();
        assert!(sort.try_grow(1).is_err());
    }

    #[test]
    fn the_fraction_limits_a_task_that_shares_the_off_heap_pool() {
        // `spark.comet.exec.memoryPool.fraction` at 0.1 of a 1000-byte off-heap pool, which two
        // running tasks share.
        let tasks = RunningTasks::default();
        let _other_task = tasks.start();
        let fake = spark_memory::fake::FakeSpark::with(usize::MAX);
        let config = MemoryPoolConfig::with_off_heap_size(MemoryPoolType::FairUnified, 100, 1000);
        let pool = create_pool(&config, -3006, &tasks, || fake.memory());
        let sort = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&pool);
        let merge = MemoryConsumer::new("ExternalSorterMerge[0]").register(&pool);

        // The task's share of the off-heap pool is 500 bytes, but the fraction limits it to 100.
        sort.try_grow(100).unwrap();
        assert!(sort.try_grow(1).is_err());
        assert!(merge.try_grow(1).is_err());
    }

    #[test]
    fn a_pool_that_takes_nothing_from_spark_has_no_overcommit() {
        let config = MemoryPoolConfig::new(MemoryPoolType::Unbounded, 0);
        let pool = create_memory_pool_with_fake_spark(&config, -3003, 0);
        let reservation = MemoryConsumer::new("sort").register(&pool);
        reservation.grow(150);
        assert_eq!(overcommit(&pool), 0);
    }
}
