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
/// bytes held by consumers that cannot spill and `num_spill` is the number of registered
/// consumers that can. A request from a consumer that can spill is refused when what the consumer
/// holds plus the request passes `limit`, or when the task's total plus the request passes
/// `task_share`. That keeps the memory that Spark grants the task for the consumers that cannot
/// spill, which take what Spark grants. A request from one of them is refused only when the task's
/// total would pass `pool_size`.
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
            acquire: Mutex::new(()),
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
/// fully sorted input, which would gain no spill path and only halve the limit of a sort below it.
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
        let _acquire = self.lock_acquire();
        self.spark.acquire(additional);
        let mut state = self.state.lock();
        state.used = state.used.saturating_add(additional);
        if !reservation.consumer().can_spill() {
            state.unspillable = state.unspillable.saturating_add(additional);
        }
        let consumer_used = state.consumer_used(reservation);
        *consumer_used = consumer_used.saturating_add(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let consumer_used = *self.state.lock().consumer_used(reservation);
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
            *state.consumer_used(reservation) -= subtractive;
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
    use super::super::spark_memory::{fake::FakeSpark, SparkMemoryManager};
    use super::super::task_shared::RunningTask;
    use super::super::testing::spillable;
    use super::*;
    use crate::errors::{CometError, CometResult};
    use parking_lot::Condvar;
    use std::collections::{hash_map::Entry, HashMap};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::{Arc, Barrier};
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
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = reservations
            .into_iter()
            .enumerate()
            .map(|(t, res)| {
                let pool = Arc::clone(&pool);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
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
                    // Stay registered until every thread stops so the fair limit is fixed.
                    barrier.wait();
                    res.free();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
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
        let other = spillable("other", &pool);
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
