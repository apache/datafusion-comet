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
use parking_lot::{Mutex, MutexGuard};

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
///
/// The pool has two locks, the way Spark's `TaskMemoryManager` splits a task's calls. `grow` and
/// `try_grow` hold `acquire` across their call into Spark, which can wait until another task
/// frees memory, as `acquireExecutionMemory` holds the task's monitor. `state` is held only to
/// read or update the totals. `shrink`, `reserved`, `register` and `unregister` take only
/// `state`, like `releaseExecutionMemory`, so they never wait behind an acquire. `acquire` is
/// always taken before `state`.
pub struct CometFairMemoryPool {
    spark: SparkMemory,
    /// The most that the task may reserve.
    pool_size: usize,
    /// Spark's off-heap pool, which the tasks running native plans share.
    off_heap_size: usize,
    /// The tasks running native plans, this pool's task included.
    running_tasks: RunningTasks,
    acquire: Mutex<()>,
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
    /// Whether the consumer holds more than its low-water mark. DataFusion 56's aggregates reserve
    /// their initial table when the plan starts, before any batch flows (apache/datafusion#25900),
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
            acquire: Mutex::new(()),
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

    /// Takes `acquire`. Its holder can be waiting in Spark, so when it is taken the wait runs in
    /// `block_in_place`, for the same reason as the call into Spark.
    fn lock_acquire(&self) -> MutexGuard<'_, ()> {
        self.acquire
            .try_lock()
            .unwrap_or_else(|| tokio::task::block_in_place(|| self.acquire.lock()))
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
/// The exemption goes by name, so it covers every `OrderedFinalAggregateStream`, including a
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
    consumer.can_spill() && !consumer.name().starts_with(ORDERED_FINAL_AGGREGATE)
}

/// The name prefix of the consumer that DataFusion 55.2's `ExternalSorter` registers for what it
/// buffers. Its merge registers a separate consumer, `ExternalSorterMerge[`, that cannot spill.
const EXTERNAL_SORTER: &str = "ExternalSorter[";

/// Whether `consumer` is a sort's. `ExternalSorter` fails when its first request is refused,
/// since it has nothing to spill yet, so a sort that has registered but not reserved takes a share
/// from every consumer that can spill except another sort. The others can keep what they hold
/// until they have emitted it: DataFusion 55.2's hash aggregates keep their output reserved until
/// they have emitted all of it, and emit slices of it, whose buffers a sort above is charged for in
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
        let _acquire = self.lock_acquire();
        self.spark.acquire(additional);
        let mut state = self.state.lock();
        state.used = state.used.saturating_add(additional);
        if !reservation.consumer().can_spill() {
            state.unspillable = state.unspillable.saturating_add(additional);
        }
        state.add_to_consumer(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let consumer_used = self.state.lock().consumer(reservation).used;
            if consumer_used < subtractive {
                panic!(
                    "Failed to release {subtractive} bytes where only {consumer_used} bytes tracked for the consumer"
                )
            }
            // Released with no lock held, so it never waits behind an acquire. The totals still
            // count these bytes until Spark has them back.
            self.spark
                .release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            let mut state = self.state.lock();
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
            let _acquire = self.lock_acquire();
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
            // While `acquire` is held nothing else adds to the totals, and a shrink only lowers
            // them, so the checks above still hold when the bytes are recorded below. A consumer
            // that registers or a task that starts meanwhile lowers the shares as it would a moment
            // later.
            drop(state);

            // A partial grant is handed back and refused, which triggers spilling in the caller.
            if let Err(refusal) = self.spark.try_acquire(additional)? {
                return resources_err!(
                    "Failed to acquire {} bytes plus {} bytes overcommitted, only got {} bytes. Reserved: {} bytes",
                    additional,
                    refusal.overcommit,
                    refusal.granted,
                    self.reserved()
                );
            }
            let mut state = self.state.lock();
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
    use super::super::spark_memory::{fake::FakeSpark, SparkMemoryManager};
    use super::super::task_shared::RunningTask;
    use super::super::testing::{hold, spillable, PeakMemoryPool};
    use super::super::{create_pool, MemoryPoolConfig, MemoryPoolType};
    use super::*;
    use crate::errors::{CometError, CometResult};
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
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{collect, ExecutionPlan, InputOrderMode};
    use datafusion::prelude::SessionConfig;
    use parking_lot::Condvar;
    use std::collections::{hash_map::Entry, HashMap};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    const GIB: usize = 1 << 30;

    const THIS_TASK: i64 = 0;
    const OTHER_TASK: i64 = 1;

    /// Real Spark parks a starved acquire forever; the stub panics after this so a deadlock
    /// fails the test instead of hanging it.
    const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_TIMEOUT: Duration = Duration::from_secs(20);
    /// Well below `WAIT_TIMEOUT`, so a call stuck behind an acquire parked in Spark fails here.
    const PROMPT: Duration = Duration::from_secs(3);

    /// While armed, holds a bridge call until the test opens it, to interleave another thread.
    struct Gate {
        armed: AtomicBool,
        entered: (Sender<()>, Mutex<Receiver<()>>),
        open: (Sender<()>, Mutex<Receiver<()>>),
    }

    impl Gate {
        fn new() -> Self {
            let entered = channel();
            let open = channel();
            Self {
                armed: AtomicBool::new(false),
                entered: (entered.0, Mutex::new(entered.1)),
                open: (open.0, Mutex::new(open.1)),
            }
        }

        fn arm(&self) {
            self.armed.store(true, SeqCst);
        }

        fn disarm(&self) {
            self.armed.store(false, SeqCst);
        }

        fn pass(&self) {
            if self.armed.load(SeqCst) {
                let _ = self.entered.0.send(());
                // A test that fails before opening the gate must not hang the bridge thread.
                let _ = self.open.1.lock().recv_timeout(TEST_TIMEOUT);
            }
        }

        fn wait_entered(&self, what: &str) {
            self.entered
                .1
                .lock()
                .recv_timeout(TEST_TIMEOUT)
                .unwrap_or_else(|_| panic!("{what} never reached the gate"));
        }

        fn open(&self) {
            let _ = self.open.0.send(());
        }
    }

    /// The state Spark's `ExecutionMemoryPool` keeps under its `lock` monitor.
    struct SparkPool {
        pool_size: i64,
        /// `memoryForTask`: created on a task's first acquire, removed when a release drains it.
        memory_for_task: HashMap<i64, i64>,
    }

    impl SparkPool {
        fn memory_free(&self) -> i64 {
            self.pool_size - self.memory_for_task.values().sum::<i64>()
        }
    }

    /// Models Spark 4.1.3's `TaskMemoryManager` and `ExecutionMemoryPool` for this task: the
    /// task's monitor, the entry lifecycle, the 1/N and 1/(2N) shares and the wait loop.
    /// Re-adding the entry in the wait loop stands in for the `CometTaskMemoryManager` retry,
    /// which these tests do not cover.
    struct StubTaskMemory {
        /// The `TaskMemoryManager` monitor. `acquireExecutionMemory` holds it for the whole call,
        /// including while it waits in `lock.wait()`, so the task's acquires run one at a time.
        /// `releaseExecutionMemory` does not take it.
        monitor: Mutex<()>,
        pool: Mutex<SparkPool>,
        /// Spark's `lock`: parked acquires wait on it and every release does `notifyAll`.
        lock: Condvar,
        acquires: AtomicUsize,
        /// `CometTaskMemoryManager.used`, which the JVM checks for leaks at task end.
        used: AtomicI64,
        /// When non-zero, every n-th acquire asks Spark for only half of the requested bytes.
        short_every: usize,
        fail_acquire: AtomicBool,
        panic_acquire: AtomicBool,
        /// Pauses an acquire under the task's monitor, like `TaskMemoryManager` spilling.
        acquire_gate: Gate,
        /// Pauses a release before it takes the pool monitor, like the JNI hop.
        release_gate: Gate,
        /// Announces every trip into `lock.wait()`, so a waiter that parks again announces twice.
        parked: (Sender<()>, Mutex<Receiver<()>>),
    }

    impl StubTaskMemory {
        fn new(pool_size: usize) -> Self {
            let parked = channel();
            Self {
                monitor: Mutex::new(()),
                pool: Mutex::new(SparkPool {
                    pool_size: pool_size as i64,
                    memory_for_task: HashMap::new(),
                }),
                lock: Condvar::new(),
                acquires: AtomicUsize::new(0),
                used: AtomicI64::new(0),
                short_every: 0,
                fail_acquire: AtomicBool::new(false),
                panic_acquire: AtomicBool::new(false),
                acquire_gate: Gate::new(),
                release_gate: Gate::new(),
                parked: (parked.0, Mutex::new(parked.1)),
            }
        }

        fn short_every(mut self, n: usize) -> Self {
            self.short_every = n;
            self
        }

        /// This task's balance in `memoryForTask`, or 0 once Spark has removed the entry.
        fn outstanding(&self) -> i64 {
            self.pool
                .lock()
                .memory_for_task
                .get(&THIS_TASK)
                .copied()
                .unwrap_or(0)
        }

        fn memory_free(&self) -> i64 {
            self.pool.lock().memory_free()
        }

        /// A task new to the pool raises `numActiveTasks` and shrinks this task's shares.
        fn task_holds(&self, task: i64, bytes: i64) {
            let mut pool = self.pool.lock();
            assert!(
                bytes <= pool.memory_free(),
                "task {task} cannot hold {bytes} bytes, only {} free",
                pool.memory_free()
            );
            *pool.memory_for_task.entry(task).or_insert(0) += bytes;
        }

        fn other_task_holds(&self, bytes: i64) {
            self.task_holds(OTHER_TASK, bytes);
        }

        /// Like Spark, drops the task's entry at zero and wakes every parked acquire.
        fn task_releases(&self, task: i64, bytes: i64) {
            let mut pool = self.pool.lock();
            let balance = pool
                .memory_for_task
                .get_mut(&task)
                .expect("task holds nothing");
            assert!(*balance >= bytes, "task {task} holds only {balance} bytes");
            *balance -= bytes;
            if *balance <= 0 {
                pool.memory_for_task.remove(&task);
            }
            self.lock.notify_all();
        }

        fn other_task_releases(&self, bytes: i64) {
            self.task_releases(OTHER_TASK, bytes);
        }

        fn wait_parked(&self, what: &str) {
            self.parked
                .1
                .lock()
                .recv_timeout(TEST_TIMEOUT)
                .unwrap_or_else(|_| panic!("{what} never parked inside Spark"));
        }
    }

    impl StubTaskMemory {
        /// Spark's `acquireExecutionMemory` for this task.
        fn grant(&self, additional: usize) -> CometResult<i64> {
            let _monitor = self.monitor.lock();
            let n = self.acquires.fetch_add(1, SeqCst) + 1;
            if self.fail_acquire.load(SeqCst) {
                return Err(CometError::Internal("injected acquire failure".to_string()));
            }
            if self.panic_acquire.load(SeqCst) {
                panic!("injected acquire panic");
            }
            self.acquire_gate.pass();
            let num_bytes = if self.short_every != 0 && n.is_multiple_of(self.short_every) {
                additional.div_ceil(2)
            } else {
                additional
            } as i64;
            assert!(
                num_bytes > 0,
                "invalid number of bytes requested: {num_bytes}"
            );

            let mut pool = self.pool.lock();
            loop {
                // A woken waiter whose entry a release removed gets NoSuchElementException in
                // Spark, and the `CometTaskMemoryManager` retry adds the entry back.
                if let Entry::Vacant(entry) = pool.memory_for_task.entry(THIS_TASK) {
                    entry.insert(0);
                    self.lock.notify_all();
                }
                let num_active_tasks = pool.memory_for_task.len() as i64;
                let cur_mem = pool.memory_for_task[&THIS_TASK];
                let max_memory_per_task = pool.pool_size / num_active_tasks;
                let min_memory_per_task = pool.pool_size / (2 * num_active_tasks);
                let max_to_grant = num_bytes.min((max_memory_per_task - cur_mem).max(0));
                let to_grant = max_to_grant.min(pool.memory_free());
                if to_grant < num_bytes && cur_mem + to_grant < min_memory_per_task {
                    let _ = self.parked.0.send(());
                    // Gives up the pool monitor only, as `lock.wait()` does; the task's monitor
                    // stays held.
                    let timed_out = self.lock.wait_for(&mut pool, WAIT_TIMEOUT).timed_out();
                    assert!(
                        !timed_out,
                        "deadlock: acquire of {num_bytes} bytes waited {WAIT_TIMEOUT:?} for \
                         memory that never came"
                    );
                } else {
                    *pool.memory_for_task.get_mut(&THIS_TASK).unwrap() += to_grant;
                    return Ok(to_grant);
                }
            }
        }

        /// Spark's `releaseExecutionMemory` for this task.
        fn hand_back(&self, size: usize) -> CometResult<()> {
            self.release_gate.pass();
            let mut pool = self.pool.lock();
            // Spark only warns and clamps here; the stub fails so an over-release cannot hide.
            let cur_mem = pool.memory_for_task.get(&THIS_TASK).copied().unwrap_or(0);
            assert!(
                cur_mem >= size as i64,
                "released {size} bytes with only {cur_mem} outstanding"
            );
            if let Some(balance) = pool.memory_for_task.get_mut(&THIS_TASK) {
                *balance -= size as i64;
                if *balance <= 0 {
                    pool.memory_for_task.remove(&THIS_TASK);
                }
            }
            self.lock.notify_all();
            Ok(())
        }
    }

    impl SparkMemoryManager for Arc<StubTaskMemory> {
        fn acquire(&self, additional: usize) -> CometResult<i64> {
            let granted = self.grant(additional)?;
            self.used.fetch_add(granted, SeqCst);
            Ok(granted)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            self.hand_back(size)?;
            self.used.fetch_sub(size as i64, SeqCst);
            Ok(())
        }
    }

    fn pool_with(stub: &Arc<StubTaskMemory>, pool_size: usize) -> Arc<dyn MemoryPool> {
        fair_pool_with(stub, pool_size) as _
    }

    /// A pool whose `overcommit` a test can read, connected to `stub`.
    fn fair_pool_with(stub: &Arc<StubTaskMemory>, pool_size: usize) -> Arc<CometFairMemoryPool> {
        Arc::new(CometFairMemoryPool::with_spark(
            SparkMemory::with_manager(Box::new(Arc::clone(stub)), THIS_TASK),
            pool_size,
            pool_size,
            RunningTasks::default(),
        ))
    }

    /// Runs `try_grow` on another thread, so a lock left held fails the test instead of
    /// hanging it.
    fn try_grow_on_another_thread(
        res: MemoryReservation,
        size: usize,
    ) -> (MemoryReservation, Result<(), String>) {
        let (tx, rx) = channel();
        let thread = thread::spawn(move || {
            let _ = tx.send(res.try_grow(size).map_err(|e| e.to_string()));
            res
        });
        let result = rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("try_grow stayed blocked");
        (thread.join().unwrap(), result)
    }

    #[test]
    fn try_grow_refusal_with_overcommit_outstanding_hands_back_the_full_grant() {
        let fake = FakeSpark::with(100);
        let fair = Arc::new(CometFairMemoryPool::with_spark(
            fake.memory(),
            10_000,
            10_000,
            RunningTasks::default(),
        ));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&fair) as _;
        let reservation = MemoryConsumer::new("consumer").register(&pool);

        reservation.grow(150);
        assert_eq!(fake.held(), 100);
        assert_eq!(pool.reserved(), 150);
        assert_eq!(fair.overcommit(), 50);

        // 20 bytes free: more than the 5 requested, short of 5 plus the 50 owed.
        fake.set_limit(120);
        let err = reservation.try_grow(5).unwrap_err();
        assert!(err.to_string().contains("only got"), "{err}");

        assert_eq!(pool.reserved(), 150);
        assert_eq!(fake.held(), 100);
        assert_eq!(fake.released(), vec![20]);
        assert_eq!(
            fair.overcommit(),
            50,
            "a refused try_grow repays no overcommit"
        );
    }

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
        let aggregate = spillable("OrderedFinalAggregateStream[0]", &pool);
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
    fn short_grant_is_released_and_reported_as_error() {
        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(1));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        let err = res.try_grow(100).unwrap_err();
        assert!(err.to_string().contains("only got"), "{err}");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0, "partial grant must be handed back");
    }

    #[test]
    fn acquire_failure_leaves_accounting_unchanged() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        stub.fail_acquire.store(true, SeqCst);
        assert!(res.try_grow(100).is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn panicking_try_grow_records_nothing_and_frees_the_pool() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        res.try_grow(100).unwrap();

        stub.panic_acquire.store(true, SeqCst);
        let panic = catch_unwind(AssertUnwindSafe(|| res.try_grow(600)));
        assert!(panic.is_err(), "the bridge panic must propagate");
        assert_eq!(pool.reserved(), 100, "the panicked grow left bytes behind");

        stub.panic_acquire.store(false, SeqCst);
        let (res, result) = try_grow_on_another_thread(res, 600);
        result.unwrap();
        assert_eq!(pool.reserved(), 700);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn panicking_grow_records_nothing_and_frees_the_pool() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = spillable("consumer", &pool);
        res.grow(100);

        stub.panic_acquire.store(true, SeqCst);
        let panic = catch_unwind(AssertUnwindSafe(|| res.grow(600)));
        assert!(panic.is_err(), "the bridge panic must propagate");
        assert_eq!(pool.reserved(), 100, "the panicked grow left bytes behind");
        assert_eq!(res.size(), 100);

        stub.panic_acquire.store(false, SeqCst);
        let (res, result) = try_grow_on_another_thread(res, 900);
        result.expect("the consumer's share still holds the panicked grow");
        assert_eq!(pool.reserved(), 1_000);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn concurrent_try_grow_and_shrink_keep_accounting_consistent() {
        const THREADS: usize = 8;
        const ITERS: usize = 500;
        const POOL_SIZE: usize = 80_000;

        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(7));
        let pool = pool_with(&stub, POOL_SIZE);

        // Register every consumer up front so the fair limit stays fixed while threads run.
        let reservations: Vec<_> = (0..THREADS)
            .map(|t| spillable(&format!("consumer-{t}"), &pool))
            .collect();

        let handles: Vec<_> = reservations
            .into_iter()
            .enumerate()
            .map(|(t, res)| {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    // Catch a failed invariant so this consumer stays registered, and the fair
                    // limit of the others fixed, until every thread has been joined.
                    let outcome = catch_unwind(AssertUnwindSafe(|| {
                        for i in 0..ITERS {
                            let size = 1 + (i * 37 + t * 101) % 509;
                            // Refusals are expected; only the invariants below must hold.
                            let _ = res.try_grow(size);
                            if i % 3 == 0 && res.size() > 0 {
                                res.shrink(res.size() / 2 + 1);
                            }
                            // Drains the task's balance to zero under acquires in flight.
                            if (i + t) % 41 == 0 {
                                res.free();
                            }
                            // One reservation per consumer, so its size is the consumer's share.
                            assert!(
                                res.size() <= POOL_SIZE / THREADS,
                                "consumer exceeded its fair limit"
                            );
                            assert!(pool.reserved() <= POOL_SIZE, "pool exceeded its size");
                        }
                    }));
                    (res, outcome)
                })
            })
            .collect();

        // Join every thread before dropping any reservation, then report the first failure.
        let (reservations, outcomes): (Vec<_>, Vec<_>) = handles
            .into_iter()
            .map(|handle| handle.join().expect("worker panicked outside its loop"))
            .unzip();
        if let Some(panic) = outcomes.into_iter().find_map(Result::err) {
            std::panic::resume_unwind(panic);
        }
        for res in reservations {
            res.free();
        }

        assert_eq!(pool.reserved(), 0, "pool still tracks bytes after quiesce");
        assert_eq!(
            stub.outstanding(),
            0,
            "Spark-side bytes leaked or double-released"
        );
        assert_eq!(
            stub.used.load(SeqCst),
            stub.outstanding(),
            "the JVM's count drifted from Spark's balance"
        );
    }

    /// A parked `grow` or `try_grow` holds the pool's acquire lock, and nothing else of the pool
    /// may wait for it. The last release takes the task's balance to zero, so Spark drops the
    /// entry under the waiter, which is granted once the bridge has asked again.
    fn state_only_calls_run_while_an_acquire_is_parked(infallible: bool) {
        let stub = Arc::new(StubTaskMemory::new(100));
        let fair = fair_pool_with(&stub, 1_000_000);
        let pool: Arc<dyn MemoryPool> = Arc::clone(&fair) as _;

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();
        // Two active tasks, this one at 10 of a 25 byte minimum share, so 10 bytes parks.
        let other_task = stub.memory_free();
        stub.other_task_holds(other_task);

        let grower_thread = thread::spawn(move || {
            if infallible {
                grower.grow(10);
            } else {
                grower.try_grow(10).unwrap();
            }
            grower.free();
        });
        stub.wait_parked("grower");

        let (done_tx, done_rx) = channel();
        let caller_pool = Arc::clone(&pool);
        let caller_thread = thread::spawn(move || {
            let send = |what: String| {
                let _ = done_tx.send(what);
            };
            send(format!("reserved {}", caller_pool.reserved()));
            let late = MemoryConsumer::new("late").register(&caller_pool);
            send("registered".to_string());
            send(format!("{fair}"));
            drop(late);
            send("unregistered".to_string());
            // Leaves 5 bytes with Spark, still below the minimum share, so the grower stays
            // parked.
            holder.shrink(5);
            send("shrunk".to_string());
            holder.free();
            send("freed".to_string());
        });
        let mut done = Vec::new();
        while let Ok(what) = done_rx.recv_timeout(PROMPT) {
            done.push(what);
            if done.len() == 6 {
                break;
            }
        }
        if done.len() < 6 {
            // Frees the other task's memory so no thread stays parked when the assertion fails.
            stub.other_task_releases(other_task);
        }

        let grower_result = grower_thread.join();
        caller_thread.join().unwrap();
        assert_eq!(
            done,
            [
                "reserved 10",
                "registered",
                "CometFairMemoryPool(pool_size=1000000, off_heap_size=1000000, running_tasks=0, used=10, unspillable=10, num=3, num_spill=0, overcommit=0)",
                "unregistered",
                "shrunk",
                "freed",
            ],
            "a call waited behind the acquire parked in Spark"
        );
        grower_result.expect("the parked acquire was not granted after the full release");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn state_only_calls_run_while_a_try_grow_is_parked() {
        state_only_calls_run_while_an_acquire_is_parked(false);
    }

    #[test]
    fn state_only_calls_run_while_a_grow_is_parked() {
        state_only_calls_run_while_an_acquire_is_parked(true);
    }

    /// The release removes the task's entry from under the waiter, and the bridge asks again.
    #[test]
    fn late_acquire_survives_a_release_already_on_its_way() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();
        stub.other_task_holds(stub.memory_free());

        stub.release_gate.arm();
        let holder_thread = thread::spawn(move || holder.free());
        stub.release_gate.wait_entered("holder release");

        let grower_thread = thread::spawn(move || {
            grower.try_grow(10).unwrap();
            grower.free();
        });
        stub.wait_parked("grower");

        stub.release_gate.disarm();
        stub.release_gate.open();
        holder_thread.join().unwrap();
        grower_thread
            .join()
            .expect("late acquire crashed when the earlier release landed");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn try_grow_racing_a_shrink_is_not_admitted_on_bytes_the_jvm_still_holds() {
        let stub = Arc::new(StubTaskMemory::new(100));
        // Two consumers: a 100 byte fair limit against a task whose Spark share is 100 bytes.
        let pool = pool_with(&stub, 200);
        let holder = spillable("holder", &pool);
        let grower = holder.new_empty();
        let other = spillable("ExternalSorter[other]", &pool);
        holder.try_grow(100).unwrap();
        assert_eq!(stub.outstanding(), 100, "the task sits at its Spark share");

        stub.release_gate.arm();
        let holder_thread = thread::spawn(move || {
            holder.shrink(50);
            holder
        });
        stub.release_gate.wait_entered("holder release");

        let acquires_before = stub.acquires.load(SeqCst);
        let result = grower.try_grow(50);
        let acquires_during = stub.acquires.load(SeqCst);
        let reserved_during = pool.reserved();

        // Opened before asserting so no thread stays parked on failure.
        stub.release_gate.disarm();
        stub.release_gate.open();
        let holder = holder_thread.join().unwrap();

        let err = result.expect_err("the shrunk bytes are not free until Spark has them");
        assert!(err.to_string().contains("fair limit"), "{err}");
        assert_eq!(
            acquires_during, acquires_before,
            "grow reached Spark on bytes the JVM still held"
        );
        assert_eq!(
            reserved_during, 100,
            "bytes stay recorded until the release lands"
        );

        grower
            .try_grow(50)
            .expect("the same grow fits once the release has landed");
        assert_eq!(pool.reserved(), 100);
        assert_eq!(stub.outstanding(), 100, "50 held and the 50 granted");
        holder.free();
        grower.free();
        drop(holder);
        drop(grower);
        drop(other);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
    }

    /// A short grant is handed back inside the acquire that got it, so the next acquire is
    /// checked once Spark has the bytes again.
    #[test]
    fn a_try_grow_waits_for_a_short_grant_to_be_handed_back() {
        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(2));
        // Two consumers: a 100 byte fair limit.
        let pool = pool_with(&stub, 200);
        let first = spillable("first", &pool);
        let second = first.new_empty();
        let other = spillable("other", &pool);
        // Uses up the first acquire, so the 100 byte request is the halved second and the 60
        // byte grow after it is granted in full.
        first.try_grow(1).unwrap();
        first.free();

        stub.release_gate.arm();
        let first_thread = thread::spawn(move || first.try_grow(100));
        stub.release_gate.wait_entered("short grant hand-back");
        stub.release_gate.disarm();
        let reserved_during = pool.reserved();

        let (second_done_tx, second_done_rx) = channel();
        let second_thread = thread::spawn(move || {
            let result = second.try_grow(60);
            let _ = second_done_tx.send(());
            (second, result)
        });
        let second_done_during = second_done_rx.recv_timeout(PROMPT).is_ok();

        stub.release_gate.open();
        let first_result = first_thread.join().unwrap();
        let (second, second_result) = second_thread.join().unwrap();

        let err = first_result.expect_err("100 bytes were requested and 50 granted");
        assert!(err.to_string().contains("only got 50"), "{err}");
        assert_eq!(reserved_during, 0, "a short grant is never recorded");
        assert!(
            !second_done_during,
            "the second try_grow finished before the short grant was back with Spark"
        );
        second_result.expect("60 bytes fit once the short grant is back with Spark");
        assert_eq!(pool.reserved(), 60);
        assert_eq!(stub.outstanding(), 60);
        drop(second);
        drop(other);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn a_try_grow_queues_behind_an_in_flight_acquire_and_is_checked_after_it() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 100);
        let grower = MemoryConsumer::new("grower").register(&pool);

        stub.acquire_gate.arm();
        let grower_thread = thread::spawn(move || {
            grower.try_grow(60).unwrap();
            grower
        });
        stub.acquire_gate.wait_entered("grower acquire");
        stub.acquire_gate.disarm();

        // 50 bytes fit the pool until the 60 in flight are recorded.
        let other = MemoryConsumer::new("other").register(&pool);
        let (other_done_tx, other_done_rx) = channel();
        let other_thread = thread::spawn(move || {
            let result = other.try_grow(50);
            let _ = other_done_tx.send(());
            (other, result)
        });
        let other_done_during = other_done_rx.recv_timeout(PROMPT).is_ok();
        stub.acquire_gate.open();
        let grower = grower_thread.join().unwrap();
        let (other, result) = other_thread.join().unwrap();

        assert!(
            !other_done_during,
            "the second try_grow finished while the first was still in Spark"
        );
        let err = result.expect_err("60 granted plus 50 exceeds the 100 byte pool");
        assert!(err.to_string().contains("pool limit"), "{err}");
        assert_eq!(pool.reserved(), 60);
        other
            .try_grow(40)
            .expect("40 bytes fit beside the recorded 60");
        assert_eq!(pool.reserved(), 100);
        drop(grower);
        drop(other);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
    }

    /// One worker, so the second task can only run if the waiting try_grow gave it up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_try_grow_waiting_for_the_acquire_lock_leaves_its_worker_running_other_tasks() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);

        stub.acquire_gate.arm();
        let first_thread = thread::spawn(move || {
            first.try_grow(10).unwrap();
            first
        });
        stub.acquire_gate.wait_entered("first acquire");
        stub.acquire_gate.disarm();

        let (started_tx, started_rx) = channel();
        let waiter = tokio::spawn(async move {
            let _ = started_tx.send(());
            second.try_grow(10).unwrap();
            second
        });
        started_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the waiting try_grow never started");
        let (ran_tx, ran_rx) = channel();
        tokio::spawn(async move {
            let _ = ran_tx.send(());
        });
        let ran = ran_rx.recv_timeout(PROMPT);

        stub.acquire_gate.open();
        let first = first_thread.join().unwrap();
        let second = waiter.await.unwrap();
        assert!(
            ran.is_ok(),
            "the worker was blocked while the try_grow waited for the acquire lock"
        );
        assert_eq!(pool.reserved(), 20);
        drop(first);
        drop(second);
        assert_eq!(stub.outstanding(), 0);
    }

    /// Pins what a shrink in flight leaves open: Spark does not have the bytes back yet, so a
    /// try_grow that passes the pool's checks can get a short grant at the task's Spark share.
    #[test]
    fn try_grow_racing_a_shrink_gets_a_short_grant_at_the_spark_share() {
        let stub = Arc::new(StubTaskMemory::new(100));
        // Two consumers: a 100 byte fair limit against a task whose Spark share is 100 bytes.
        let pool = pool_with(&stub, 200);
        let holder = spillable("holder", &pool);
        let grower = spillable("grower", &pool);
        holder.try_grow(100).unwrap();

        stub.release_gate.arm();
        let holder_thread = thread::spawn(move || {
            holder.shrink(50);
            holder
        });
        stub.release_gate.wait_entered("holder release");

        let acquires_before = stub.acquires.load(SeqCst);
        let result = grower.try_grow(1);
        let acquires_during = stub.acquires.load(SeqCst);
        let reserved_during = pool.reserved();

        stub.release_gate.disarm();
        stub.release_gate.open();
        let holder = holder_thread.join().unwrap();

        let err = result.expect_err("the task is at its Spark share until the release lands");
        assert!(err.to_string().contains("only got 0"), "{err}");
        assert_eq!(
            acquires_during,
            acquires_before + 1,
            "the grow reached Spark"
        );
        assert_eq!(reserved_during, 100);

        grower
            .try_grow(1)
            .expect("the grow fits once the release has landed");
        assert_eq!(stub.outstanding(), 51, "50 held and the 1 granted");
        drop(holder);
        drop(grower);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
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

    /// DataFusion 56's aggregates reserve their initial table when the plan starts, before any
    /// batch flows (apache/datafusion#25900), so a consumer takes a share only while it holds more
    /// than the least it has held since its first reservation.
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
    /// every grouping key, so DataFusion runs it as an `OrderedFinalAggregateStream` with no spill
    /// path, and a refused request fails the query. The aggregate holds nothing while the sort
    /// reads its input, so the sort can fill the task's share. It must give part of it back before
    /// the aggregate's first request, which it does whether it emits from one sorted batch or
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

                    // A task share of exactly that.
                    let fake = FakeSpark::with(usize::MAX);
                    let sort = sort_on("b", Arc::clone(&input));
                    let aggregate = AggregateExec::try_new(
                        AggregateMode::Final,
                        group_by.clone(),
                        vec![Arc::new(count.clone())],
                        vec![None],
                        Arc::clone(&sort) as _,
                        Arc::clone(&partial_input),
                    )
                    .unwrap();
                    // Sorted on every grouping key, so an `OrderedFinalAggregateStream` with no
                    // spill path.
                    assert_eq!(aggregate.input_order_mode(), &InputOrderMode::Sorted);
                    let pool = pool(&fake, alone.peak());
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

    /// The aggregates above the first one in `distinct_count` hold nothing while it reads its
    /// input, so it can fill the task's share, and it holds all of its output until it has emitted
    /// it. With wide values that leaves the `Partial` aggregate too little room, so it emits early,
    /// and the `Final` aggregate starts while the first one still holds nearly all of the share.
    /// DataFusion 55.2's `FinalHashAggregateStream` then spills and completes rather than failing
    /// the query.
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

        // A task share of exactly that, in a pool built the way `createPlan` builds it, so that
        // the final aggregate reads its spill files back as it would in a task.
        let fake = FakeSpark::with(usize::MAX);
        let config = MemoryPoolConfig::new(MemoryPoolType::FairUnified, alone.peak());
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
    /// DataFusion 55.2's hash aggregates keep their output reserved until they have emitted all of
    /// it, and `ExternalSorter` fails when its first request is refused, since it has nothing to
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
