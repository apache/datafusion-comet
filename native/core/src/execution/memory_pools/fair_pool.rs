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
use super::spill_replay::CLUSTERED_FINAL_AGGREGATE;
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
/// bytes held by consumers that cannot spill and `num_spill` is the number of active consumers that
/// can spill, counting the one asking and, unless it is a sort, the sorts that have not started (see
/// [`is_sort`]). A consumer is active while it holds more than its low-water mark (see
/// [`ConsumerUsage::active`]). Unlike `FairSpillPool`, this leaves out the other consumers that have
/// not started work. DataFusion registers each operator's consumer when it builds the stream tree,
/// before any batch flows, so the operators above a blocking one are registered while it reads its
/// input, and would otherwise hold it to a fraction of the task's share while their own shares stay
/// unused (#7628).
///
/// A request from a consumer that can spill is refused when what the consumer holds plus the
/// request passes `limit`, or when the task's total plus the request passes `task_share`. That
/// keeps the memory that Spark grants the task for the consumers that cannot spill, which take
/// what Spark grants. A request from one of them is refused only when the task's total would pass
/// `pool_size`.
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
    /// Consumers that can spill and are active; see [`ConsumerUsage::active`].
    num_spill: usize,
    /// Registered sorts that have not reserved memory yet; see [`is_sort`].
    unstarted_sorts: usize,
    /// What each registered consumer holds, keyed by [`MemoryConsumer::id`]. The sibling
    /// reservations that `new_empty()`, `split()` and `take()` create belong to the same consumer,
    /// so they count against one share. The pool keeps these totals itself because
    /// `reservation.size()` covers only one reservation, and DataFusion updates it after calling
    /// `try_grow` but before calling `shrink`.
    consumers: HashMap<usize, ConsumerUsage>,
}

/// What a registered consumer holds across all of its reservations.
#[derive(Default)]
struct ConsumerUsage {
    used: usize,
    /// The least the consumer has held since its first reservation, or `None` before it made one.
    low_water: Option<usize>,
}

impl ConsumerUsage {
    /// Whether the consumer holds more than its low-water mark. The aggregates reserve their
    /// initial table when the plan starts, before any batch flows (apache/datafusion#25900),
    /// so a first reservation does not show that an operator has started work. A consumer that has
    /// emptied, such as an aggregate that emitted early, is active again once it holds memory, and
    /// one that only gives memory back, such as a sort emitting what it buffered, is not.
    fn active(&self) -> bool {
        self.low_water
            .is_some_and(|low_water| self.used > low_water)
    }
}

impl CometFairPoolState {
    /// What `reservation`'s consumer holds across all of its reservations.
    fn consumer(&mut self, reservation: &MemoryReservation) -> &mut ConsumerUsage {
        self.consumers
            .get_mut(&reservation.consumer().id())
            .expect("reservation's consumer is not registered with the pool")
    }

    /// Adds `additional` bytes to what `reservation`'s consumer holds, counting a consumer that
    /// can spill in `num_spill` once it becomes active.
    fn add_to_consumer(&mut self, reservation: &MemoryReservation, additional: usize) {
        let consumer = self.consumer(reservation);
        let was_active = consumer.active();
        let starts = consumer.low_water.is_none();
        consumer.used = consumer.used.saturating_add(additional);
        consumer.low_water.get_or_insert(consumer.used);
        if !was_active && consumer.active() && reservation.consumer().can_spill() {
            self.num_spill += 1;
        }
        if starts && is_sort(reservation.consumer()) {
            self.unstarted_sorts = self.unstarted_sorts.saturating_sub(1);
        }
    }

    /// Takes `subtractive` bytes off what `reservation`'s consumer holds, which must be at least
    /// that much, and stops counting a consumer that can spill in `num_spill` once it is no
    /// longer active.
    fn subtract_from_consumer(&mut self, reservation: &MemoryReservation, subtractive: usize) {
        let consumer = self.consumer(reservation);
        let was_active = consumer.active();
        consumer.used -= subtractive;
        consumer.low_water = consumer
            .low_water
            .map(|low_water| low_water.min(consumer.used));
        if was_active && !consumer.active() && reservation.consumer().can_spill() {
            self.num_spill = self.num_spill.saturating_sub(1);
        }
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
                unstarted_sorts: 0,
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
/// That is the case for every consumer that can spill, with one exception. DataFusion
/// registers `ClusteredFinalAggregateStream` as able to spill even when its input is sorted on every
/// grouping key, to work around apache/datafusion#17334, but it builds a spill path only for
/// partially sorted input. Fully sorted, as in a `CometSortAggregateExec` over a native sort, a
/// refused request fails the query. So its requests are checked only against the pool, like those
/// of a consumer that cannot spill. The partially sorted stream still spills when Spark refuses it.
///
/// The exemption goes by name, so it covers every `ClusteredFinalAggregateStream`, including a
/// pure `PartialMerge` that the planner runs as `Final` over input sorted on some of its keys
/// (#6880). That one is not a `CometSortAggregateExec` and its input is only partially sorted, but
/// it is checked only against the pool as well. The planner does not switch a `PartialMerge` over
/// fully sorted input, where the final aggregate would have no spill path.
///
/// Only the check differs. The aggregate still counts as a consumer that can spill everywhere
/// else, so it is one of `num_spill` once it is active, and its bytes are not `unspillable`.
/// A sort below it reads all of its input before the aggregate asks for any, so the sort can take
/// the task's whole share. It gives part of that back before the aggregate's first request, all of
/// it if it spilled. Otherwise `ExternalSorter` frees its merge reservation, and, since it reserves
/// about twice the bytes of each batch it buffers, resizes each sorted run to its own size before
/// it emits the first batch. A merge of the runs reserves only the batches it holds and their sort
/// keys.
/// `a_sort_that_fills_the_share_leaves_room_for_the_final_aggregate_above_it` checks both.
fn limited_to_its_share(consumer: &MemoryConsumer) -> bool {
    consumer.can_spill() && !consumer.name().starts_with(CLUSTERED_FINAL_AGGREGATE)
}

/// The name prefix of the consumer that `ExternalSorter` registers for what it buffers. Its merge
/// registers a separate consumer, `ExternalSorterMerge[`, that cannot spill.
const EXTERNAL_SORTER: &str = "ExternalSorter[";

/// Whether `consumer` is a sort's. `ExternalSorter` fails when its first request is refused,
/// since it has nothing to spill yet, so a sort that has registered but not reserved takes a share
/// from every consumer that can spill except another sort. The others can keep what they hold
/// until they have emitted it: the hash aggregates keep their output reserved until they have
/// emitted all of it, and emit slices of it, whose buffers a sort above is charged for in
/// full. A sort gives memory back before it emits (see [`limited_to_its_share`]) and emits compact
/// batches, so it needs to leave a sort above it no room.
fn is_sort(consumer: &MemoryConsumer) -> bool {
    consumer.name().starts_with(EXTERNAL_SORTER)
}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        let mut state = self.state.lock();
        state
            .consumers
            .insert(consumer.id(), ConsumerUsage::default());
        if is_sort(consumer) {
            state.unstarted_sorts += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        // DataFusion unregisters a consumer after its last reservation has dropped and released
        // its bytes. If a release panicked, this runs while unwinding, so it must not panic too.
        let mut state = self.state.lock();
        let Some(usage) = state.consumers.remove(&consumer.id()) else {
            return;
        };
        if usage.active() && consumer.can_spill() {
            state.num_spill = state.num_spill.saturating_sub(1);
        }
        if usage.low_water.is_none() && is_sort(consumer) {
            state.unstarted_sorts = state.unstarted_sorts.saturating_sub(1);
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
        state.add_to_consumer(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let mut state = self.state.lock();
            let consumer_used = state.consumer(reservation).used;
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
            state.subtract_from_consumer(reservation, subtractive);
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
                let consumer = state.consumer(reservation);
                let (consumer_used, active) = (consumer.used, consumer.active());
                // `num_spill` counts this consumer only if it is already active. A sort that has not
                // started takes a share from every consumer but another sort.
                let unstarted_sorts = if is_sort(reservation.consumer()) {
                    0
                } else {
                    state.unstarted_sorts
                };
                let num_spill = (state.num_spill + usize::from(!active) + unstarted_sorts).max(1);
                let limit = task_share.saturating_sub(state.unspillable) / num_spill;
                if limit < consumer_used.saturating_add(additional) {
                    return resources_err!(
                        "Failed to acquire {additional} bytes where this consumer already holds {consumer_used} bytes and the fair limit is {limit} bytes: the task's share of {task_share} bytes with {tasks} tasks running, less {} bytes held by consumers that cannot spill, divided among {num_spill} consumers that can, this one included, counting those doing work and {unstarted_sorts} sorts that have not started ({} bytes overcommitted)",
                        state.unspillable,
                        self.spark.overcommit()
                    );
                }
                // The shares alone do not bound the task's total, because a consumer keeps what
                // it reserved before another consumer became active or another task started.
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
            state.add_to_consumer(reservation, additional);
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
    use super::super::testing::{hold, spillable, PeakMemoryPool};
    use super::super::{create_pool, MemoryPoolConfig, MemoryPoolType};
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::Result as DFResult;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::TaskContext;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::aggregates::{
        AggregateExec, AggregateMode, GroupClusteringMode, PhysicalGroupBy,
    };
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{collect, ExecutionPlan};
    use datafusion::prelude::SessionConfig;
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
        let other_sort = spillable("ExternalSorter[1]", &pool);
        let other_merge = MemoryConsumer::new("ExternalSorterMerge[1]").register(&pool);
        hold(&other_sort, 10);

        // Both sorts are active, so they share the task's 130 bytes, and the first is refused,
        // and spills, at 65.
        sort.try_grow(65).unwrap();
        assert!(sort.try_grow(1).is_err());
        // The other sort spills before it merges. That leaves the rest of what Spark grants the
        // task for its merge, which cannot spill.
        other_sort.free();
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

        // Both sorts are active, so they share the 60 bytes that the merge leaves.
        hold(&second, 10);
        first.try_grow(30).unwrap();
        assert!(first.try_grow(1).is_err());

        // Their shares grow as the merge releases memory.
        merge.shrink(40);
        first.try_grow(20).unwrap();
        assert!(first.try_grow(1).is_err());
        second.try_grow(40).unwrap();
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn memory_recorded_for_a_consumer_that_cannot_spill_comes_out_of_the_shares() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let sort = spillable("ExternalSorter[0]", &pool);
        let other_sort = spillable("ExternalSorter[1]", &pool);
        hold(&other_sort, 10);
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
        hold(&first, 100);

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
        let aggregate = spillable("ClusteredFinalAggregateStream[0]", &pool);
        hold(&sort, 10);

        // Not held to half of the task's 100 bytes, as a second consumer that can spill would be
        // while the sort is active.
        aggregate.try_grow(60).unwrap();
        // Its bytes come out of the sort's share instead.
        assert!(sort.try_grow(31).is_err());
        sort.try_grow(30).unwrap();
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

        // Once both are active, each consumer's share is 50 bytes, whatever the other one holds.
        hold(&first, 40);
        hold(&second, 20);
        first.try_grow(10).unwrap();
        assert!(first.try_grow(1).is_err());
        second.try_grow(30).unwrap();
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
        assert_eq!(fake.held(), 100);
    }

    #[test]
    fn a_consumer_that_starts_late_is_limited_by_the_tasks_share() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 90);
        let first = spillable("first", &pool);
        let second = spillable("second", &pool);
        // While the second consumer is not active, the first consumer's share is the whole task's.
        hold(&first, 60);

        // Once the second consumer is active, it halves both shares, but the first keeps the 60
        // bytes it holds.
        hold(&second, 30);
        assert!(first.try_grow(1).is_err());
        // The second consumer is 15 bytes under its share, but the task's share is full.
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 90);

        // Once the first consumer releases memory, the second can use the rest of its share.
        first.shrink(30);
        second.try_grow(15).unwrap();
        assert!(second.try_grow(1).is_err());

        // Once the second consumer holds nothing, it is no longer active, and the first has the
        // whole share again.
        second.free();
        first.try_grow(60).unwrap();
        assert_eq!(pool.reserved(), 90);
        assert_eq!(fake.held(), 90);
    }

    /// The aggregates reserve their initial table when the plan starts, before any batch flows
    /// (apache/datafusion#25900), so a consumer takes a share only while it holds more than the
    /// least it has held since its first reservation.
    #[test]
    fn a_consumer_takes_a_share_while_it_holds_more_than_its_low_water_mark() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let sort = spillable("ExternalSorter[0]", &pool);
        let aggregate = spillable("SingleHashAggregateStream[0]", &pool);
        aggregate.try_grow(10).unwrap();

        // So the sort can use what the aggregate leaves of the task's share, and the task's total
        // keeps the aggregate's bytes for it.
        sort.try_grow(90).unwrap();
        assert!(sort.try_grow(1).is_err());

        // Once the aggregate holds more than its initial table, it takes a share.
        sort.shrink(40);
        aggregate.try_grow(1).unwrap();
        assert!(sort.try_grow(1).is_err());

        // An aggregate that emptied, as one that emitted early does, takes a share again as soon
        // as it holds memory, even less than its initial table.
        aggregate.free();
        sort.try_grow(1).unwrap();
        aggregate.try_grow(5).unwrap();
        assert!(sort.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 56);
    }

    #[test]
    fn sibling_reservations_draw_on_one_share() {
        let fake = FakeSpark::with(usize::MAX);
        let pool = pool(&fake, 100);
        let first = spillable("first", &pool);
        // Like the reservation that a sort's streaming merge creates for each batch it reads.
        let sibling = first.new_empty();
        let second = spillable("second", &pool);
        hold(&second, 10);

        // Both of the first consumer's reservations draw on its one 50-byte share.
        first.try_grow(40).unwrap();
        assert!(sibling.try_grow(40).is_err());
        sibling.try_grow(10).unwrap();
        assert!(first.try_grow(1).is_err());
        assert!(sibling.try_grow(1).is_err());

        // So the second consumer can still reserve the rest of its share.
        second.try_grow(40).unwrap();
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
        let second = spillable("second", &pool);
        hold(&second, 10);

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
        assert_eq!(pool.reserved(), 60);
        assert_eq!(fake.held(), 60);
    }

    /// 16 batches of 1024 rows, whose columns `columns` builds from a distinct key in `0..16384`
    /// for each row, in no particular order.
    fn batches(schema: &SchemaRef, columns: impl Fn(&[i64]) -> Vec<ArrayRef>) -> Vec<RecordBatch> {
        (0..16)
            .map(|batch| {
                let keys: Vec<i64> = (batch * 1024..(batch + 1) * 1024)
                    .map(|i| i * 7919 % 16384)
                    .collect();
                RecordBatch::try_new(Arc::clone(schema), columns(&keys)).unwrap()
            })
            .collect()
    }

    fn int64s(values: impl Iterator<Item = i64>) -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(values))
    }

    /// Batches of 1024 rows, without the sort's merge reservation, which cannot spill and so would
    /// take its bytes out of the shares.
    fn session_config() -> SessionConfig {
        SessionConfig::new()
            .with_batch_size(1024)
            .with_sort_spill_reservation_bytes(0)
    }

    async fn collect_with(
        plan: Arc<dyn ExecutionPlan>,
        pool: Arc<dyn MemoryPool>,
        config: SessionConfig,
    ) -> Vec<RecordBatch> {
        try_collect_with(plan, pool, config).await.unwrap()
    }

    async fn try_collect_with(
        plan: Arc<dyn ExecutionPlan>,
        pool: Arc<dyn MemoryPool>,
        config: SessionConfig,
    ) -> DFResult<Vec<RecordBatch>> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .unwrap();
        let context = TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime);
        collect(plan, Arc::new(context)).await
    }

    fn sort_on(column: &str, input: Arc<dyn ExecutionPlan>) -> Arc<SortExec> {
        let expr = col(column, &input.schema()).unwrap();
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(expr)]).unwrap();
        Arc::new(SortExec::new(ordering, input))
    }

    /// Sorts on `a` in a task whose share holds what the sort reserves for all of its input, under
    /// the plan that `above` builds on the sort, and returns how many times the sort spilled.
    async fn spills_of_a_sort_below(
        above: impl FnOnce(Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>>,
    ) -> usize {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batches = batches(&schema, |keys| {
            vec![
                int64s(keys.iter().copied()),
                int64s(keys.iter().map(|key| key % 16)),
            ]
        });
        // `ExternalSorter` reserves about twice the bytes of each batch it buffers, so three times
        // the input holds all of it, and half of that does not.
        let input_bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let fake = FakeSpark::with(usize::MAX);

        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let sort = sort_on("a", input);
        let plan = above(Arc::clone(&sort) as Arc<dyn ExecutionPlan>).unwrap();
        let output = collect_with(plan, pool(&fake, 3 * input_bytes), session_config()).await;
        assert!(output.iter().any(|batch| batch.num_rows() > 0));
        sort.metrics().unwrap().spill_count().unwrap()
    }

    fn sorted_on_b(input: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(sort_on("b", input))
    }

    fn counted_by_b(input: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let count = AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("count(a)")
            .build()?;
        let group_by = PhysicalGroupBy::new_single(vec![(col("b", &schema)?, "b".to_string())]);
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![Arc::new(count)],
            vec![None],
            input,
            schema,
        )?))
    }

    /// DataFusion registers each operator's consumer when it builds the stream tree, before any
    /// batch flows, so the consumers above a blocking operator are registered while it reads its
    /// input, although they hold nothing until it emits (#7628). The pool asks Spark for memory in
    /// `block_in_place`, which needs the multi-threaded runtime.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_operators_above_a_sort_take_no_share_until_they_are_active() {
        let spills = [
            spills_of_a_sort_below(Ok).await,
            spills_of_a_sort_below(sorted_on_b).await,
            spills_of_a_sort_below(counted_by_b).await,
        ];
        // Alone, below a sort, and below an aggregate.
        assert_eq!(spills, [0, 0, 0]);
    }

    /// In a `CometSortAggregateExec` over a native sort, the final aggregate's input is sorted on
    /// every grouping key, so DataFusion runs it as a `ClusteredFinalAggregateStream` with no spill
    /// path, and a refused request fails the query. While the sort reads its input, the aggregate
    /// holds only the table it reserves when the plan starts (apache/datafusion#25900), so the
    /// sort can fill the rest of the task's share. It must give part of it back before the
    /// aggregate's first request, which it does whether it emits from one sorted batch or
    /// merges sorted runs, with or without its merge reservation, and with 256 or 1024 groups in
    /// each batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sort_that_fills_the_share_leaves_room_for_the_final_aggregate_above_it() {
        // The partial counts that a final aggregate reads.
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("count(a)[count]", DataType::Int64, false),
        ]));
        // The schema that the partial aggregate read, which the final aggregate is built against.
        let partial_input = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let a = col("a", &partial_input).unwrap();
        let count = AggregateExprBuilder::new(count_udaf(), vec![a])
            .schema(Arc::clone(&partial_input))
            .alias("count(a)")
            .build()
            .unwrap();
        let group_by = PhysicalGroupBy::new_single(vec![(col("b", &schema).unwrap(), "b".into())]);
        let default_merge_reservation = SessionConfig::new()
            .options()
            .execution
            .sort_spill_reservation_bytes;

        // 256 or 1024 values of `b` in each batch.
        for rows_per_group in [4, 1] {
            let batches = batches(&schema, |keys| {
                vec![
                    int64s(keys.iter().map(|key| key / rows_per_group)),
                    int64s(keys.iter().map(|_| 1)),
                ]
            });
            let input: Arc<dyn ExecutionPlan> =
                MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
            // The sort's merge reservation, which cannot spill, comes out of the share. Comet runs
            // with DataFusion's default.
            for merge_reservation in [0, default_merge_reservation] {
                // Emits from one sorted batch, then merges sorted runs.
                for in_place_bytes in [usize::MAX, 0] {
                    let config = SessionConfig::new()
                        .with_batch_size(1024)
                        .with_sort_spill_reservation_bytes(merge_reservation)
                        .with_sort_in_place_threshold_bytes(in_place_bytes);
                    // The most that the sort reserves on its own.
                    let alone = Arc::new(PeakMemoryPool::new(usize::MAX));
                    let sort = sort_on("b", Arc::clone(&input));
                    collect_with(sort, Arc::clone(&alone) as _, config.clone()).await;

                    let final_over = |sort: Arc<SortExec>| {
                        AggregateExec::try_new(
                            AggregateMode::Final,
                            group_by.clone(),
                            vec![Arc::new(count.clone())],
                            vec![None],
                            sort as _,
                            Arc::clone(&partial_input),
                        )
                        .unwrap()
                    };
                    // What the aggregate reserves when the plan starts, before any input.
                    let initial = Arc::new(PeakMemoryPool::new(usize::MAX));
                    let empty: Arc<dyn ExecutionPlan> =
                        MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)
                            .unwrap();
                    let aggregate = final_over(sort_on("b", empty));
                    collect_with(
                        Arc::new(aggregate),
                        Arc::clone(&initial) as _,
                        config.clone(),
                    )
                    .await;

                    // A task share of exactly the two.
                    let fake = FakeSpark::with(usize::MAX);
                    let sort = sort_on("b", Arc::clone(&input));
                    let aggregate = final_over(Arc::clone(&sort));
                    // Sorted on every grouping key, so a `ClusteredFinalAggregateStream` with no
                    // spill path.
                    assert_eq!(
                        aggregate.group_clustering_mode(),
                        &GroupClusteringMode::Full
                    );
                    let pool = pool(&fake, alone.peak() + initial.peak());
                    let output = collect_with(Arc::new(aggregate), pool, config).await;
                    let groups: usize = output.iter().map(RecordBatch::num_rows).sum();
                    let case = format!(
                        "{rows_per_group} rows per group, merge reservation {merge_reservation}, \
                         sort_in_place_threshold_bytes {in_place_bytes}"
                    );
                    assert_eq!(groups, 16384 / rows_per_group as usize, "{case}");
                    assert_eq!(sort.metrics().unwrap().spill_count(), Some(0), "{case}");
                }
            }
        }
    }

    /// Spark's `count(DISTINCT d) GROUP BY k` as Comet runs it in one native plan when the input
    /// is already partitioned on `k`: Spark's `PartialMerge` keyed by `(k, d)` as a `Single`
    /// aggregate, then the `Partial` and `Final` aggregates keyed by `k`.
    fn distinct_count(input: Arc<dyn ExecutionPlan>) -> [Arc<AggregateExec>; 3] {
        let schema = input.schema();
        let keys = vec![
            (col("k", &schema).unwrap(), "k".to_string()),
            (col("d", &schema).unwrap(), "d".to_string()),
        ];
        let group_by = PhysicalGroupBy::new_single(keys);
        let distinct = AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        );
        let distinct = Arc::new(distinct.unwrap());

        let schema = distinct.schema();
        let count = AggregateExprBuilder::new(count_udaf(), vec![col("d", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("count(d)")
            .build()
            .unwrap();
        let count = Arc::new(count);
        let by_k = PhysicalGroupBy::new_single(vec![(col("k", &schema).unwrap(), "k".to_string())]);
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            by_k.clone(),
            vec![Arc::clone(&count)],
            vec![None],
            Arc::clone(&distinct) as _,
            Arc::clone(&schema),
        );
        let partial = Arc::new(partial.unwrap());
        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            by_k.as_final(),
            vec![count],
            vec![None],
            Arc::clone(&partial) as _,
            schema,
        );
        [distinct, partial, Arc::new(final_agg.unwrap())]
    }

    /// The aggregates above the first one in `distinct_count` hold only the tables they reserve
    /// when the plan starts (apache/datafusion#25900) while it reads its input, so it can fill the
    /// rest of the task's share, and it holds all of its output until it has emitted it. With wide
    /// values that leaves the `Partial` aggregate too little room, so it emits early, and the
    /// `Final` aggregate starts while the first one still holds nearly all of the share.
    /// `FinalHashAggregateStream` then spills and completes rather than failing the query.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_final_aggregate_that_starts_while_the_one_below_fills_the_share_spills() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("d", DataType::Utf8, false),
        ]));
        // A distinct `k` and a 1 KiB `d` in each row.
        let batches = batches(&schema, |keys| {
            let d = keys.iter().map(|key| format!("{key:0>1024}"));
            vec![
                int64s(keys.iter().copied()),
                Arc::new(StringArray::from_iter_values(d)),
            ]
        });
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();

        // The most that the first aggregate reserves on its own.
        let alone = Arc::new(PeakMemoryPool::new(usize::MAX));
        let [distinct, _, _] = distinct_count(Arc::clone(&input));
        collect_with(distinct, Arc::clone(&alone) as _, session_config()).await;

        // What the aggregates above it reserve when the plan starts, before any input.
        let empty: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None).unwrap();
        let [first_alone, _, all] = distinct_count(Arc::clone(&empty));
        let first_initial = Arc::new(PeakMemoryPool::new(usize::MAX));
        collect_with(
            first_alone,
            Arc::clone(&first_initial) as _,
            session_config(),
        )
        .await;
        let all_initial = Arc::new(PeakMemoryPool::new(usize::MAX));
        collect_with(all, Arc::clone(&all_initial) as _, session_config()).await;
        let above_initial = all_initial.peak() - first_initial.peak();

        // A task share of exactly the two, in a pool built the way `createPlan` builds it, so that
        // the final aggregate reads its spill files back as it would in a task.
        let fake = FakeSpark::with(usize::MAX);
        let config =
            MemoryPoolConfig::new(MemoryPoolType::FairUnified, alone.peak() + above_initial);
        let pool = create_pool(&config, -3020, &RunningTasks::default(), || fake.memory());
        let [distinct, _, final_agg] = distinct_count(input);
        let output = collect_with(Arc::clone(&final_agg) as _, pool, session_config()).await;
        let groups: usize = output.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(groups, 16384);

        let spills = |agg: &Arc<AggregateExec>| agg.metrics().unwrap().spill_count().unwrap();
        assert_eq!(spills(&distinct), 0);
        // Guards against a vacuous pass: the final aggregate had to spill, so it started short of
        // memory.
        assert!(
            spills(&final_agg) > 0,
            "the final aggregate had room to spare"
        );
    }

    /// A sort above a hash aggregate, as for a window or a sort-merge join over an aggregate.
    /// The hash aggregates keep their output reserved until they have emitted all of it, and `ExternalSorter` fails when its first request is refused, since it has nothing to
    /// spill yet.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sort_above_a_hash_aggregate_gets_room_for_its_first_batch() {
        // 16384 groups of a 64-byte `d` over 4096 values of `k`, one row each. A final aggregate
        // reads `c` as partial counts, a single one counts its values.
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("d", DataType::Utf8, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let batches = batches(&schema, |keys| {
            let d = keys.iter().map(|key| format!("{key:0>64}"));
            vec![
                int64s(keys.iter().map(|key| key % 4096)),
                Arc::new(StringArray::from_iter_values(d)),
                int64s(keys.iter().map(|_| 1)),
            ]
        });
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
        // The schema that a partial aggregate would have read, which a final one is built against.
        let partial_input = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));
        let aggregate = |mode: AggregateMode| -> Arc<dyn ExecutionPlan> {
            let counted = if mode == AggregateMode::Final {
                Arc::clone(&partial_input)
            } else {
                Arc::clone(&schema)
            };
            let count = AggregateExprBuilder::new(count_udaf(), vec![col("c", &counted).unwrap()])
                .schema(Arc::clone(&counted))
                .alias("count(c)")
                .build()
                .unwrap();
            let keys = vec![
                (col("k", &schema).unwrap(), "k".to_string()),
                (col("d", &schema).unwrap(), "d".to_string()),
            ];
            Arc::new(
                AggregateExec::try_new(
                    mode,
                    PhysicalGroupBy::new_single(keys),
                    vec![Arc::new(count)],
                    vec![None],
                    Arc::clone(&input),
                    counted,
                )
                .unwrap(),
            )
        };

        for mode in [AggregateMode::Final, AggregateMode::Single] {
            // The most that the aggregate reserves on its own.
            let alone = Arc::new(PeakMemoryPool::new(usize::MAX));
            collect_with(aggregate(mode), Arc::clone(&alone) as _, session_config()).await;
            for percent in [100, 110, 125, 150, 200] {
                let fake = FakeSpark::with(usize::MAX);
                let share = alone.peak() * percent / 100;
                let plan = sort_on("k", aggregate(mode));
                let output = try_collect_with(plan, pool(&fake, share), session_config()).await;
                let case = format!("{mode:?} aggregate, share {percent}% of its peak");
                let groups: usize = output
                    .unwrap_or_else(|e| panic!("{case}: {e}"))
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum();
                assert_eq!(groups, 16384, "{case}");
            }
        }
    }
}
