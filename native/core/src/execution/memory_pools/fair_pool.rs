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
};

use super::spark_memory::SparkMemory;
use super::spill_replay::ORDERED_FINAL_AGGREGATE;
use super::task_shared::RunningTasks;
use datafusion::common::resources_err;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::{
    common::DataFusionError,
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use parking_lot::Mutex;

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
///
/// Spark limits each task to an even share of its off-heap pool among the tasks that hold memory
/// from it, and it cannot ask a native consumer to spill. So, as DataFusion's `FairSpillPool` does
/// over its whole pool, this pool limits each consumer that can spill to an even share of what the
/// consumers that cannot spill leave of the task's share:
///
/// ```text
/// task_share = min(off_heap_size / running_tasks, pool_size)
/// limit      = (task_share - unspillable) / num_spill
/// ```
///
/// `running_tasks` counts the tasks running native plans, this one included, `unspillable` is the
/// bytes held by consumers that cannot spill and `num_spill` is the number of registered
/// consumers that can. A request from a consumer that can spill is refused when what the consumer
/// holds plus the request passes `limit`, or when the task's total plus the request passes
/// `task_share`. That keeps the memory that Spark grants the task for the consumers that cannot
/// spill, which take what Spark grants. A request from one of them is refused only when the task's
/// total would pass `pool_size`.
pub struct CometFairMemoryPool {
    spark: SparkMemory,
    /// The most that the task may reserve.
    pool_size: usize,
    /// Spark's off-heap pool, which the tasks running native plans share.
    off_heap_size: usize,
    /// The tasks running native plans, this pool's task included.
    running_tasks: RunningTasks,
    state: Mutex<CometFairPoolState>,
}

struct CometFairPoolState {
    used: usize,
    /// Bytes held by consumers that cannot spill.
    unspillable: usize,
    /// Registered consumers that can spill.
    num_spill: usize,
    /// Bytes held by each registered consumer, keyed by [`MemoryConsumer::id`]. The sibling
    /// reservations that `new_empty()`, `split()` and `take()` create belong to the same consumer,
    /// so they count against one share. The pool keeps these totals itself because
    /// `reservation.size()` covers only one reservation, and DataFusion updates it after calling
    /// `try_grow` but before calling `shrink`.
    consumers: HashMap<usize, usize>,
}

impl CometFairPoolState {
    /// The bytes held by `reservation`'s consumer across all of its reservations.
    fn consumer_used(&mut self, reservation: &MemoryReservation) -> &mut usize {
        self.consumers
            .get_mut(&reservation.consumer().id())
            .expect("reservation's consumer is not registered with the pool")
    }
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("off_heap_size", &self.off_heap_size)
            .field("running_tasks", &self.running_tasks.count())
            .field("used", &state.used)
            .field("unspillable", &state.unspillable)
            .field("num", &state.consumers.len())
            .field("num_spill", &state.num_spill)
            .field("overcommit", &self.spark.overcommit())
            .finish()
    }
}

impl CometFairMemoryPool {
    pub(super) fn with_spark(
        spark: SparkMemory,
        pool_size: usize,
        off_heap_size: usize,
        running_tasks: RunningTasks,
    ) -> CometFairMemoryPool {
        Self {
            spark,
            pool_size,
            off_heap_size,
            running_tasks,
            state: Mutex::new(CometFairPoolState {
                used: 0,
                unspillable: 0,
                num_spill: 0,
                consumers: HashMap::new(),
            }),
        }
    }

    /// The part of [`MemoryPool::reserved`] that Spark has not granted; see [`SparkMemory`].
    pub(super) fn overcommit(&self) -> usize {
        self.spark.overcommit()
    }

    /// The task's share of the off-heap pool and the number of tasks running native plans that it
    /// is an even share among, as Spark limits each task holding execution memory to an even share
    /// of its pool. `running_tasks` counts this pool's task once it holds the pool.
    fn task_share(&self) -> (usize, usize) {
        let tasks = self.running_tasks.count().max(1);
        ((self.off_heap_size / tasks).min(self.pool_size), tasks)
    }
}

impl Display for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        write!(
            f,
            "CometFairMemoryPool(pool_size={}, off_heap_size={}, running_tasks={}, used={}, unspillable={}, num={}, num_spill={}, overcommit={})",
            self.pool_size,
            self.off_heap_size,
            self.running_tasks.count(),
            state.used,
            state.unspillable,
            state.consumers.len(),
            state.num_spill,
            self.spark.overcommit()
        )
    }
}

/// Whether a request from `consumer` is held to its fair limit and the task's share.
///
/// That is the case for every consumer that can spill, with one exception. DataFusion 55.2
/// registers `OrderedFinalAggregateStream` as able to spill even when its input is sorted on every
/// grouping key, to work around apache/datafusion#17334, but it builds a spill path only for
/// partially sorted input. Fully sorted, as in a `CometSortAggregateExec` over a native sort, a
/// refused request fails the query. So its requests are checked only against the pool, like those
/// of a consumer that cannot spill. The partially sorted stream still spills when Spark refuses it.
///
/// Only the check differs. The aggregate still counts as a consumer that can spill everywhere
/// else, so it is one of `num_spill` and its bytes are not `unspillable`. A sort below it then
/// stays limited to its fair share and leaves the aggregate the rest.
fn limited_to_its_share(consumer: &MemoryConsumer) -> bool {
    consumer.can_spill() && !consumer.name().starts_with(ORDERED_FINAL_AGGREGATE)
}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.consumers.insert(consumer.id(), 0);
        if consumer.can_spill() {
            state.num_spill += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        // DataFusion unregisters a consumer after its last reservation has dropped and released
        // its bytes. If a release panicked, this runs while unwinding, so it must not panic too.
        let mut state = self.state.lock();
        if state.consumers.remove(&consumer.id()).is_some() && consumer.can_spill() {
            state.num_spill = state.num_spill.saturating_sub(1);
        }
    }

    /// Records memory that already exists, so it must not fail and ignores the fair limit and
    /// the task's share.
    /// See [`SparkMemory`].
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        if additional == 0 {
            return;
        }
        let mut state = self.state.lock();
        self.spark.acquire(additional);
        state.used = state.used.saturating_add(additional);
        if !reservation.consumer().can_spill() {
            state.unspillable = state.unspillable.saturating_add(additional);
        }
        let consumer_used = state.consumer_used(reservation);
        *consumer_used = consumer_used.saturating_add(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let mut state = self.state.lock();
            let consumer_used = *state.consumer_used(reservation);
            if consumer_used < subtractive {
                panic!(
                    "Failed to release {subtractive} bytes where only {consumer_used} bytes tracked for the consumer"
                )
            }
            self.spark
                .release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            state.used = state.used.checked_sub(subtractive).unwrap();
            if !reservation.consumer().can_spill() {
                state.unspillable = state.unspillable.saturating_sub(subtractive);
            }
            *state.consumer_used(reservation) -= subtractive;
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            let mut state = self.state.lock();
            let can_spill = reservation.consumer().can_spill();
            let used = state.used;
            if limited_to_its_share(reservation.consumer()) {
                let (task_share, tasks) = self.task_share();
                // `num_spill` counts this consumer.
                let limit = task_share.saturating_sub(state.unspillable) / state.num_spill.max(1);
                let consumer_used = *state.consumer_used(reservation);
                if limit < consumer_used.saturating_add(additional) {
                    return resources_err!(
                        "Failed to acquire {additional} bytes where this consumer already holds {consumer_used} bytes and the fair limit is {limit} bytes: the task's share of {task_share} bytes with {tasks} tasks running, less {} bytes held by consumers that cannot spill, divided among {} consumers that can ({} bytes overcommitted)",
                        state.unspillable,
                        state.num_spill,
                        self.spark.overcommit()
                    );
                }
                // The shares alone do not bound the task's total, because a consumer keeps what
                // it reserved before another consumer registered or another task started.
                if task_share < used.saturating_add(additional) {
                    return resources_err!(
                        "Failed to acquire {additional} bytes where {used} bytes already reserved ({} bytes overcommitted) and the task's share is {task_share} bytes with {tasks} tasks running",
                        self.spark.overcommit()
                    );
                }
            } else if self.pool_size < used.saturating_add(additional) {
                // Spark grants no task more than the off-heap pool, so this binds only when the
                // deprecated `spark.comet.exec.memoryPool.fraction` is below 1.0.
                return resources_err!(
                    "Failed to acquire {additional} bytes where {used} bytes already reserved ({} bytes overcommitted) and the pool limit is {} bytes",
                    self.spark.overcommit(),
                    self.pool_size
                );
            }

            // A partial grant is handed back and refused, which triggers spilling in the caller.
            if let Err(refusal) = self.spark.try_acquire(additional)? {
                return resources_err!(
                    "Failed to acquire {} bytes plus {} bytes overcommitted, only got {} bytes. Reserved: {} bytes",
                    additional,
                    refusal.overcommit,
                    refusal.granted,
                    state.used
                );
            }
            state.used = state
                .used
                .checked_add(additional)
                .expect("overflow in checked_add");
            if !can_spill {
                state.unspillable += additional;
            }
            *state.consumer_used(reservation) += additional;
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }
}

#[cfg(test)]
mod tests {
    use super::super::spark_memory::fake::FakeSpark;
    use super::super::task_shared::RunningTask;
    use super::super::testing::spillable;
    use super::*;
    use std::sync::Arc;

    /// A pool of `pool_size` bytes, all of the off-heap pool as with the default fraction, which
    /// `tasks` share, connected to `fake`.
    fn shared_pool(
        fake: &Arc<FakeSpark>,
        pool_size: usize,
        tasks: &RunningTasks,
    ) -> Arc<dyn MemoryPool> {
        Arc::new(CometFairMemoryPool::with_spark(
            fake.memory(),
            pool_size,
            pool_size,
            tasks.clone(),
        ))
    }

    /// A pool of `pool_size` bytes for a task that runs alone, connected to `fake`.
    fn pool(fake: &Arc<FakeSpark>, pool_size: usize) -> Arc<dyn MemoryPool> {
        shared_pool(fake, pool_size, &RunningTasks::default())
    }

    /// Starts `n` tasks, which run until the returned guards drop.
    fn start(tasks: &RunningTasks, n: usize) -> Vec<RunningTask> {
        (0..n).map(|_| tasks.start()).collect()
    }

    #[test]
    fn consumers_that_can_spill_share_their_tasks_share_of_the_pool() {
        // Eight running tasks share a 1040-byte pool, and Spark grants this one its 130 bytes.
        let fake = FakeSpark::with(130);
        let tasks = RunningTasks::default();
        let _running = start(&tasks, 8);
        let pool = shared_pool(&fake, 1040, &tasks);
        let sort = spillable("ExternalSorter[0]", &pool);
        let _other_sort = spillable("ExternalSorter[1]", &pool);
        let other_merge = MemoryConsumer::new("ExternalSorterMerge[1]").register(&pool);

        // The two sorts share the task's 130 bytes, so the first is refused, and spills, at 65.
        sort.try_grow(65).unwrap();
        assert!(sort.try_grow(1).is_err());
        // That leaves the rest of what Spark grants the task for the other sort's merge, which
        // cannot spill.
        other_merge.try_grow(65).unwrap();
        assert_eq!(fake.held(), 130);
    }

    #[test]
    fn a_consumer_that_cannot_spill_is_limited_only_by_the_pool() {
        let fake = FakeSpark::with(usize::MAX);
        let tasks = RunningTasks::default();
        let _running = start(&tasks, 4);
        let pool = shared_pool(&fake, 400, &tasks);
        let _sort = spillable("ExternalSorter[0]", &pool);
        let _other_sort = spillable("ExternalSorter[1]", &pool);
        let merge = MemoryConsumer::new("ExternalSorterMerge[0]").register(&pool);

        // Neither a share of the task's 100 bytes nor the task's share itself limits the merge.
        merge.try_grow(400).unwrap();
        // The pool does, but only binds before Spark does when the deprecated
        // `spark.comet.exec.memoryPool.fraction` is below 1.0.
        assert!(merge.try_grow(1).is_err());
        assert_eq!(fake.held(), 400);
    }

    #[test]
    fn consumers_that_can_spill_share_what_the_others_leave() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let first = spillable("ExternalSorter[0]", &pool);
        let second = spillable("ExternalSorter[1]", &pool);
        let merge = MemoryConsumer::new("ExternalSorterMerge[0]").register(&pool);
        merge.try_grow(40).unwrap();

        // The two sorts share the 60 bytes that the merge leaves.
        first.try_grow(30).unwrap();
        assert!(first.try_grow(1).is_err());

        // Their shares grow as the merge releases memory.
        merge.shrink(40);
        first.try_grow(20).unwrap();
        assert!(first.try_grow(1).is_err());
        second.try_grow(50).unwrap();
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn memory_recorded_for_a_consumer_that_cannot_spill_comes_out_of_the_shares() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let sort = spillable("ExternalSorter[0]", &pool);
        let _other_sort = spillable("ExternalSorter[1]", &pool);
        // Like a batch read back from a spill file, which already exists.
        let reader = MemoryConsumer::new("spill reader").register(&pool);
        reader.grow(40);

        // The two sorts share the 60 bytes left.
        sort.try_grow(30).unwrap();
        assert!(sort.try_grow(1).is_err());
    }

    #[test]
    fn a_tasks_share_follows_the_number_of_running_tasks() {
        let fake = FakeSpark::with(usize::MAX);
        let tasks = RunningTasks::default();
        let mut running = start(&tasks, 4);
        let pool = shared_pool(&fake, 400, &tasks);
        let sort = spillable("ExternalSorter[0]", &pool);

        // Four running tasks share the 400-byte pool.
        sort.try_grow(100).unwrap();
        assert!(sort.try_grow(1).is_err());

        // Once two of them finish, the other two share it.
        running.truncate(2);
        sort.try_grow(100).unwrap();
        assert!(sort.try_grow(1).is_err());

        // A task that starts takes part of the share back, so the sort spills before it grows.
        running.push(tasks.start());
        assert!(sort.try_grow(1).is_err());
        sort.shrink(150);
        sort.try_grow(83).unwrap();
        assert!(sort.try_grow(1).is_err());
    }

    #[test]
    fn the_tasks_total_is_checked_against_the_tasks_share_not_the_pool() {
        // One task has the whole 200-byte pool, and its sort takes half.
        let fake = FakeSpark::with(usize::MAX);
        let tasks = RunningTasks::default();
        let _running = tasks.start();
        let pool = shared_pool(&fake, 200, &tasks);
        let first = spillable("ExternalSorter[0]", &pool);
        first.try_grow(100).unwrap();

        // Another task starts, which halves the task's share to the 100 bytes that the sort holds.
        // A second sort is under its own limit of 50 bytes, but the task's total is at its share.
        let _other_task = tasks.start();
        let second = spillable("ExternalSorter[1]", &pool);
        let error = second.try_grow(50).unwrap_err().to_string();
        assert!(error.contains("the task's share is 100 bytes"), "{error}");

        // A consumer that cannot spill is limited by the pool, not by the task's share.
        let merge = MemoryConsumer::new("ExternalSorterMerge[1]").register(&pool);
        merge.try_grow(100).unwrap();
        assert_eq!(pool.reserved(), 200);
    }

    /// The trade-off that the migration guide describes: a consumer that cannot spill can take
    /// what Spark grants, which can be the task's whole share, and then a consumer that can spill
    /// is refused its first request, so a sort fails where it used to run.
    #[test]
    fn a_consumer_that_can_spill_is_refused_once_the_others_hold_the_tasks_share() {
        let fake = FakeSpark::with(usize::MAX);
        let tasks = RunningTasks::default();
        let _running = start(&tasks, 4);
        let pool = shared_pool(&fake, 400, &tasks);
        // Like a shuffled hash join's build side, which Spark lets grow past `off_heap_size / 4`
        // while the other tasks hold nothing yet.
        let build = MemoryConsumer::new("HashJoinInput[0]").register(&pool);
        build.try_grow(100).unwrap();
        let sort = spillable("ExternalSorter[0]", &pool);

        let error = sort.try_grow(1).unwrap_err().to_string();
        assert!(error.contains("the fair limit is 0 bytes"), "{error}");

        // The sort can reserve again once the build side gives memory back.
        build.shrink(60);
        sort.try_grow(60).unwrap();
        assert!(sort.try_grow(1).is_err());
    }

    #[test]
    fn an_ordered_final_aggregate_is_limited_as_a_consumer_that_cannot_spill() {
        // DataFusion registers it as able to spill, but over input sorted on every grouping key it
        // has no spill path, so a refusal would fail the query.
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let sort = spillable("ExternalSorter[0]", &pool);
        let aggregate = spillable("OrderedFinalAggregateStream[0]", &pool);

        // Not held to half of the task's 100 bytes, as a second consumer that can spill would be.
        aggregate.try_grow(60).unwrap();
        // Its bytes come out of the sort's share instead.
        assert!(sort.try_grow(41).is_err());
        sort.try_grow(40).unwrap();
    }

    #[test]
    fn a_sort_below_an_ordered_final_aggregate_leaves_it_a_share() {
        // In a `CometSortAggregateExec` over a native sort, the sort reads all of its input before
        // the aggregate asks for anything, and keeps what it holds while it emits.
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let sort = spillable("ExternalSorter[0]", &pool);
        let aggregate = spillable("OrderedFinalAggregateStream[0]", &pool);

        // The aggregate counts as a consumer that can spill, so the sort gets half.
        assert!(sort.try_grow(51).is_err());
        sort.try_grow(50).unwrap();
        // And the aggregate has the other half.
        aggregate.try_grow(50).unwrap();
    }

    #[test]
    fn grow_past_the_fair_limit_is_recorded_and_refuses_the_next_try_grow() {
        let fake = FakeSpark::with(100);
        let pool = pool(&fake, 100);
        let reservation = spillable("ExternalSorter[0]", &pool);

        // Past both the fair limit and what Spark will grant.
        reservation.grow(150);
        assert_eq!(pool.reserved(), 150);
        assert_eq!(fake.held(), 100);
        assert!(reservation.try_grow(1).is_err());

        drop(reservation);
        assert_eq!(pool.reserved(), 0);
        // Spark gets back exactly the 100 bytes it granted.
        assert_eq!(fake.released(), vec![100]);
    }

    #[test]
    fn each_consumer_is_limited_to_its_own_share() {
        // Spark grants everything, so only the pool's own checks refuse.
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let first = spillable("first", &pool);
        let second = spillable("second", &pool);

        // Each consumer's share is 50 bytes, whatever the other one holds.
        first.try_grow(40).unwrap();
        second.try_grow(20).unwrap();
        first.try_grow(10).unwrap();
        assert!(first.try_grow(1).is_err());
        second.try_grow(30).unwrap();
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
        assert_eq!(fake.held(), 100);
    }

    #[test]
    fn a_consumer_registered_late_is_limited_by_the_tasks_share() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 90);
        let first = spillable("first", &pool);
        // Alone, the first consumer's share is the whole task's.
        first.try_grow(60).unwrap();

        // A second consumer halves both shares, but the first keeps the 60 bytes it holds.
        let second = spillable("second", &pool);
        assert!(first.try_grow(1).is_err());
        second.try_grow(30).unwrap();
        // The second consumer is 15 bytes under its share, but the task's share is full.
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 90);

        // Once the first consumer releases memory, the second can use the rest of its share.
        first.shrink(30);
        second.try_grow(15).unwrap();
        assert!(second.try_grow(1).is_err());

        // Unregistering the second consumer gives the first the whole share again.
        drop(second);
        first.try_grow(60).unwrap();
        assert_eq!(pool.reserved(), 90);
        assert_eq!(fake.held(), 90);
    }

    #[test]
    fn sibling_reservations_draw_on_one_share() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let first = spillable("first", &pool);
        // Like the reservation that a sort's streaming merge creates for each batch it reads.
        let sibling = first.new_empty();
        let second = spillable("second", &pool);

        // Both of the first consumer's reservations draw on its one 50-byte share.
        first.try_grow(40).unwrap();
        assert!(sibling.try_grow(40).is_err());
        sibling.try_grow(10).unwrap();
        assert!(first.try_grow(1).is_err());
        assert!(sibling.try_grow(1).is_err());

        // So the second consumer can still reserve its whole share.
        second.try_grow(50).unwrap();
        assert_eq!(pool.reserved(), 100);

        // Dropping a reservation returns its bytes to the consumer's share. The consumer stays
        // registered while its other reservation lives.
        drop(first);
        sibling.try_grow(40).unwrap();
        assert!(sibling.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
        assert_eq!(fake.held(), 100);
    }

    #[test]
    fn split_and_take_keep_the_bytes_on_their_consumer() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let mut first = spillable("first", &pool);
        let _second = spillable("second", &pool);

        first.try_grow(50).unwrap();
        let split = first.split(20);
        let taken = first.take();
        // The consumer's 50 bytes now sit in split and taken, and the emptied reservation gets no
        // share of its own.
        for reservation in [&first, &split, &taken] {
            assert!(reservation.try_grow(1).is_err());
        }

        // Shrinking one reservation makes room in the share for another.
        split.shrink(10);
        first.try_grow(10).unwrap();
        assert!(taken.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 50);
        assert_eq!(fake.held(), 50);
    }
}
