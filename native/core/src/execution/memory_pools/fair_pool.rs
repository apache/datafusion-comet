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

use log::warn;

use super::spark_memory::SparkMemory;
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
/// No lock is held across a JVM call, so a release can drain the task's `memoryForTask` entry
/// under an acquire parked in Spark; `CometTaskMemoryManager` asks Spark again in that case.
pub struct CometFairMemoryPool {
    spark: SparkMemory,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
}

struct CometFairPoolState {
    used: usize,
    /// Bytes held by each registered consumer, keyed by [`MemoryConsumer::id`]. The sibling
    /// reservations that `new_empty()`, `split()` and `take()` create belong to the same consumer,
    /// so they count against one share. The pool keeps these totals itself because
    /// `reservation.size()` covers only one reservation, and DataFusion updates it after calling
    /// `try_grow` but before calling `shrink`.
    consumers: HashMap<usize, usize>,
}

impl CometFairPoolState {
    /// The bytes held by `consumer` across all of its reservations.
    fn consumer_used(&mut self, consumer: usize) -> &mut usize {
        self.consumers
            .get_mut(&consumer)
            .expect("reservation's consumer is not registered with the pool")
    }

    fn charge(&mut self, consumer: usize, bytes: usize) {
        self.used = self.used.saturating_add(bytes);
        let consumer_used = self.consumer_used(consumer);
        *consumer_used = consumer_used.saturating_add(bytes);
    }

    fn settle(&mut self, consumer: usize, charged: usize, held: usize) {
        let settle = |total: usize| {
            total
                .checked_sub(charged)
                .and_then(|total| total.checked_add(held))
                .expect("settled more bytes than the pool tracks")
        };
        self.used = settle(self.used);
        let consumer_used = self.consumer_used(consumer);
        *consumer_used = settle(*consumer_used);
    }
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("used", &state.used)
            .field("num", &state.consumers.len())
            .field("overcommit", &self.spark.overcommit())
            .finish()
    }
}

impl CometFairMemoryPool {
    pub(super) fn with_spark(spark: SparkMemory, pool_size: usize) -> CometFairMemoryPool {
        Self {
            spark,
            pool_size,
            state: Mutex::new(CometFairPoolState {
                used: 0,
                consumers: HashMap::new(),
            }),
        }
    }

    /// The part of [`MemoryPool::reserved`] that Spark has not granted; see [`SparkMemory`].
    pub(super) fn overcommit(&self) -> usize {
        self.spark.overcommit()
    }

    /// Called only once the JVM has accepted the release, so no grow is admitted on bytes Spark
    /// still holds.
    fn settle_release(&self, consumer: usize, bytes: usize) {
        self.state.lock().settle(consumer, bytes, 0);
    }

    /// `held` is what Spark granted and still holds; it stays charged until handed back.
    fn settle_acquire(&self, consumer: usize, charged: usize, held: usize) {
        self.state.lock().settle(consumer, charged, held);
    }
}

impl Display for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        write!(
            f,
            "CometFairMemoryPool(pool_size={}, used={}, num={}, overcommit={})",
            self.pool_size,
            state.used,
            state.consumers.len(),
            self.spark.overcommit()
        )
    }
}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.state.lock().consumers.insert(consumer.id(), 0);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        // DataFusion unregisters a consumer after its last reservation has dropped and released
        // its bytes. If a release panicked, this runs while unwinding, so it must not panic too.
        self.state.lock().consumers.remove(&consumer.id());
    }

    /// Records memory that already exists, so it must not fail and ignores the fair and pool
    /// limits. What Spark declines is carried as overcommit, see [`SparkMemory`].
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        if additional == 0 {
            return;
        }
        let consumer = reservation.consumer().id();
        // Charged before the JVM call so a racing try_grow is checked against these bytes.
        self.state.lock().charge(consumer, additional);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.spark.acquire(additional);
        }));
        if let Err(panic) = outcome {
            // The reservation does not record bytes whose grow panicked, so drop the charge too.
            self.settle_acquire(consumer, additional, 0);
            std::panic::resume_unwind(panic);
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let consumer = reservation.consumer().id();
            {
                let consumer_used = *self.state.lock().consumer_used(consumer);
                if consumer_used < subtractive {
                    panic!(
                        "Failed to release {subtractive} bytes where only {consumer_used} bytes tracked for the consumer"
                    )
                }
            }
            // Released without the lock so an acquire parked in Spark cannot stall it. A failure
            // panics because the caller has already given the bytes up. DataFusion never shrinks
            // a reservation past its size, which keeps settle_release in range once the lock is
            // dropped.
            self.spark
                .release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            self.settle_release(consumer, subtractive);
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            let consumer = reservation.consumer().id();
            // Check and charge under one lock so concurrent grows cannot jointly exceed a share
            // or pool_size.
            {
                let mut state = self.state.lock();
                let num = state.consumers.len();
                let limit = self
                    .pool_size
                    .checked_div(num)
                    .expect("overflow in checked_div");
                let consumer_used = *state.consumer_used(consumer);
                if limit < consumer_used.saturating_add(additional) {
                    return resources_err!(
                        "Failed to acquire {additional} bytes where this consumer already holds {consumer_used} bytes and the fair limit is {limit} bytes, {num} registered ({} bytes overcommitted)",
                        self.spark.overcommit()
                    );
                }
                // The shares alone do not bound the pool's total, because a consumer keeps what
                // it reserved before another consumer registered.
                let used = state.used;
                if self.pool_size < used.saturating_add(additional) {
                    return resources_err!(
                        "Failed to acquire {additional} bytes where {used} bytes already reserved ({} bytes overcommitted) and the pool limit is {} bytes",
                        self.spark.overcommit(),
                        self.pool_size
                    );
                }
                state.charge(consumer, additional);
            }

            // A leaked charge would poison the task-shared pool for every other consumer, so it
            // is rolled back on error and on panic.
            let refusal = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.spark.try_acquire_leaving_a_short_grant(additional)
            })) {
                Ok(Ok(Ok(()))) => return Ok(()),
                Ok(Ok(Err(refusal))) => refusal,
                Ok(Err(e)) => {
                    self.settle_acquire(consumer, additional, 0);
                    return Err(e.into());
                }
                Err(panic) => {
                    self.settle_acquire(consumer, additional, 0);
                    std::panic::resume_unwind(panic);
                }
            };
            // The granted bytes, which can exceed the request by the overcommit asked for, stay
            // charged until Spark has them back. If that release fails Spark keeps them until the
            // task ends, so they stay charged to match. The caller still gets the short-grant
            // error so it can spill.
            let granted = refusal.granted;
            self.settle_acquire(consumer, additional, granted);
            if granted > 0 {
                if let Err(e) = self.spark.manager().release(granted) {
                    warn!(
                        "Task {} failed to return a short grant of {granted} bytes, which stay charged to the pool: {e:?}",
                        self.spark.task_attempt_id()
                    );
                } else {
                    self.settle_release(consumer, granted);
                }
            }

            return resources_err!(
                "Failed to acquire {} bytes plus {} of {} bytes overcommitted, only got {} bytes. Reserved: {} bytes",
                additional,
                refusal.overcommit,
                self.spark.overcommit(),
                granted,
                self.reserved()
            );
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
    use super::*;
    use crate::errors::{CometError, CometResult};
    use parking_lot::Condvar;
    use std::collections::{hash_map::Entry, HashMap};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    const MIB: usize = 1 << 20;
    const GIB: usize = 1 << 30;

    const THIS_TASK: i64 = 0;
    const OTHER_TASK: i64 = 1;
    const THIRD_TASK: i64 = 2;

    /// Real Spark parks a starved acquire forever; the stub panics after this so a deadlock
    /// fails the test instead of hanging it.
    const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_TIMEOUT: Duration = Duration::from_secs(20);

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

    /// Models Spark 4.1.3's `ExecutionMemoryPool` acquire and release for this task: the entry
    /// lifecycle, the 1/N and 1/(2N) shares and the wait loop. Re-adding the entry in the wait
    /// loop stands in for the `CometTaskMemoryManager` retry, which these tests do not cover.
    struct StubTaskMemory {
        pool: Mutex<SparkPool>,
        /// Spark's `lock`: parked acquires wait on it and every release does `notifyAll`.
        lock: Condvar,
        releases: AtomicUsize,
        acquires: AtomicUsize,
        requests: Mutex<Vec<usize>>,
        /// `CometTaskMemoryManager.used`, which the JVM checks for leaks at task end.
        used: AtomicI64,
        /// When non-zero, every n-th acquire asks Spark for only half of the requested bytes.
        short_every: usize,
        fail_acquire: AtomicBool,
        fail_release: AtomicBool,
        panic_acquire: AtomicBool,
        /// Pauses an acquire before the pool monitor, like `TaskMemoryManager` spilling.
        acquire_gate: Gate,
        /// Pauses a release before it takes the pool monitor, like the JNI hop.
        release_gate: Gate,
        /// Pauses an acquire after Spark has granted it, before the pool sees the answer.
        granted_gate: Gate,
        /// Announces every trip into `lock.wait()`, so a waiter that parks again announces twice.
        parked: (Sender<()>, Mutex<Receiver<()>>),
    }

    impl StubTaskMemory {
        fn new(pool_size: usize) -> Self {
            let parked = channel();
            Self {
                pool: Mutex::new(SparkPool {
                    pool_size: pool_size as i64,
                    memory_for_task: HashMap::new(),
                }),
                lock: Condvar::new(),
                releases: AtomicUsize::new(0),
                acquires: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                used: AtomicI64::new(0),
                short_every: 0,
                fail_acquire: AtomicBool::new(false),
                fail_release: AtomicBool::new(false),
                panic_acquire: AtomicBool::new(false),
                acquire_gate: Gate::new(),
                release_gate: Gate::new(),
                granted_gate: Gate::new(),
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

        /// Another consumer of this task, such as the shuffle allocator, takes `bytes`.
        fn sibling_consumer_holds(&self, bytes: i64) {
            self.task_holds(THIS_TASK, bytes);
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

        fn sibling_consumer_releases(&self, bytes: i64) {
            self.task_releases(THIS_TASK, bytes);
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
            let n = self.acquires.fetch_add(1, SeqCst) + 1;
            self.requests.lock().push(additional);
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
            if self.fail_release.load(SeqCst) {
                return Err(CometError::Internal("injected release failure".to_string()));
            }
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
            self.releases.fetch_add(1, SeqCst);
            self.lock.notify_all();
            Ok(())
        }
    }

    /// Like `CometTaskMemoryManager`, a release moves `used` only once Spark has answered.
    impl SparkMemoryManager for Arc<StubTaskMemory> {
        fn acquire(&self, additional: usize) -> CometResult<i64> {
            let granted = self.grant(additional)?;
            self.used.fetch_add(granted, SeqCst);
            self.granted_gate.pass();
            Ok(granted)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            self.hand_back(size)?;
            self.used.fetch_sub(size as i64, SeqCst);
            Ok(())
        }
    }

    fn pool_with(stub: &Arc<StubTaskMemory>, pool_size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(CometFairMemoryPool::with_spark(
            SparkMemory::with_manager(Box::new(Arc::clone(stub)), THIS_TASK),
            pool_size,
        ))
    }

    #[test]
    fn try_grow_refusal_with_overcommit_outstanding_hands_back_the_full_grant() {
        let fake = FakeSpark::with(100);
        let fair = Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 10_000));
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

    #[test]
    fn try_grow_refusal_reports_debt_another_try_grow_is_asking_for() {
        let fake = FakeSpark::with(100);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 10_000));
        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        first.grow(150);

        let (err_tx, err_rx) = channel();
        fake.during_next_acquire(move || {
            let _ = err_tx.send(second.try_grow(1).unwrap_err().to_string());
        });
        assert!(first.try_grow(5).is_err());
        let err = err_rx.recv().unwrap();
        assert!(err.contains("plus 0 of 50 bytes overcommitted"), "{err}");
    }

    #[test]
    fn grow_past_the_fair_limit_is_recorded_and_refuses_the_next_try_grow() {
        let fake = FakeSpark::with(100);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let reservation = MemoryConsumer::new("smj").register(&pool);

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
    fn grow_and_shrink_update_pool_and_spark_accounting() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        res.try_grow(600).unwrap();
        assert_eq!(pool.reserved(), 600);
        assert_eq!(stub.outstanding(), 600);

        res.shrink(200);
        assert_eq!(pool.reserved(), 400);
        assert_eq!(stub.outstanding(), 400);

        res.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn try_grow_beyond_fair_limit_fails_without_calling_spark() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        res.try_grow(600).unwrap();
        let acquires_before = stub.acquires.load(SeqCst);
        let err = res.try_grow(500).unwrap_err();
        assert!(err.to_string().contains("fair limit"), "{err}");
        assert_eq!(pool.reserved(), 600);
        assert_eq!(
            stub.acquires.load(SeqCst),
            acquires_before,
            "over-limit grow must be rejected before reaching Spark"
        );
        res.free();
    }

    #[test]
    fn fair_limit_shrinks_as_consumers_register() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let first = MemoryConsumer::new("first").register(&pool);

        first.try_grow(600).unwrap();

        let second = MemoryConsumer::new("second").register(&pool);
        let err = first.try_grow(1).unwrap_err();
        assert!(err.to_string().contains("fair limit"), "{err}");

        drop(second);
        first.try_grow(1).unwrap();
        first.free();
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
    fn zero_sized_grow_does_not_call_spark() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        let acquires_before = stub.acquires.load(SeqCst);
        pool.try_grow(&res, 0).unwrap();
        pool.shrink(&res, 0);
        assert_eq!(stub.acquires.load(SeqCst), acquires_before);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    #[should_panic(expected = "Failed to release")]
    fn shrinking_more_than_tracked_panics() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        pool.shrink(&res, 100);
    }

    #[test]
    fn panicking_acquire_rolls_back_the_reservation() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        res.try_grow(100).unwrap();

        stub.panic_acquire.store(true, SeqCst);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| res.try_grow(600)));
        assert!(panic.is_err(), "the bridge panic must propagate");
        assert_eq!(
            pool.reserved(),
            100,
            "panicked grow left phantom bytes behind"
        );

        stub.panic_acquire.store(false, SeqCst);
        res.try_grow(600).unwrap();
        assert_eq!(pool.reserved(), 700);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn panicking_acquire_in_grow_rolls_back_the_reservation() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        res.grow(100);

        stub.panic_acquire.store(true, SeqCst);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| res.grow(600)));
        assert!(panic.is_err(), "the bridge panic must propagate");
        assert_eq!(
            pool.reserved(),
            100,
            "panicked grow left phantom bytes behind"
        );
        assert_eq!(res.size(), 100);

        stub.panic_acquire.store(false, SeqCst);
        res.try_grow(900)
            .expect("the consumer's share still holds the panicked grow");
        assert_eq!(pool.reserved(), 1_000);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn concurrent_grow_and_shrink_keep_accounting_consistent() {
        const THREADS: usize = 8;
        const ITERS: usize = 500;
        const POOL_SIZE: usize = 80_000;

        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(7));
        let pool = pool_with(&stub, POOL_SIZE);

        // Register every consumer up front so the fair limit stays fixed while threads run.
        let reservations: Vec<_> = (0..THREADS)
            .map(|t| MemoryConsumer::new(format!("consumer-{t}")).register(&pool))
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

    #[test]
    fn shrink_is_not_blocked_by_a_slow_acquire_on_another_thread() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(1_000).unwrap();

        stub.acquire_gate.arm();
        let grower_thread = thread::spawn(move || {
            // The result is irrelevant; the test only needs this acquire to be in flight.
            let _ = grower.try_grow(500);
            grower.free();
        });
        stub.acquire_gate.wait_entered("grower");

        let (done_tx, done_rx) = channel();
        let releaser_thread = thread::spawn(move || {
            holder.free();
            let _ = done_tx.send(());
        });
        let released = done_rx.recv_timeout(TEST_TIMEOUT);

        // Open the gate before asserting so no thread stays parked if the assertion fails.
        stub.acquire_gate.disarm();
        stub.acquire_gate.open();
        releaser_thread.join().unwrap();
        grower_thread.join().unwrap();
        assert!(
            released.is_ok(),
            "release was blocked behind an in-flight acquire"
        );
    }

    /// The memory usage logger reads `reserved` from its own thread while an acquire may be
    /// parked in Spark.
    #[test]
    fn reserved_is_not_blocked_by_an_acquire_parked_in_spark() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();
        // Parks the grower below its minimum share until the holder frees.
        stub.other_task_holds(stub.memory_free());

        let grower_thread = thread::spawn(move || {
            grower.try_grow(10).unwrap();
            grower.free();
        });
        stub.wait_parked("grower");

        let (reserved_tx, reserved_rx) = channel();
        let reader_pool = Arc::clone(&pool);
        let reader_thread = thread::spawn(move || {
            let _ = reserved_tx.send(reader_pool.reserved());
        });
        // Well below WAIT_TIMEOUT so a blocked read fails here, not as a count mismatch after the
        // rollback.
        let reserved_during = reserved_rx.recv_timeout(Duration::from_secs(1));

        holder.free();
        let grower_result = grower_thread.join();
        reader_thread.join().unwrap();
        assert_eq!(
            reserved_during,
            Ok(20),
            "reserved() must return at once and count the holder's 10 bytes plus the grower's \
             10 bytes in flight"
        );
        grower_result.expect("parked acquire failed after the holder freed");
        assert_eq!(pool.reserved(), 0);
    }

    /// The release takes the task's balance to zero, so Spark drops the entry under the waiter,
    /// which fails on waking until the bridge asks again.
    #[test]
    fn full_release_wakes_an_acquire_parked_below_its_minimum_share() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();
        // Fill the pool: two active tasks, this one at 10 of a 25 byte minimum share.
        stub.other_task_holds(stub.memory_free());

        let (grower_done_tx, grower_done_rx) = channel();
        let grower_thread = thread::spawn(move || {
            grower.try_grow(10).unwrap();
            grower.free();
            let _ = grower_done_tx.send(());
        });
        stub.wait_parked("grower");

        let (holder_done_tx, holder_done_rx) = channel();
        let holder_thread = thread::spawn(move || {
            holder.free();
            let _ = holder_done_tx.send(());
        });

        assert!(
            holder_done_rx.recv_timeout(TEST_TIMEOUT).is_ok(),
            "full release deadlocked behind the parked acquire"
        );
        assert!(
            grower_done_rx.recv_timeout(TEST_TIMEOUT).is_ok(),
            "parked acquire never completed after the release"
        );
        holder_thread.join().unwrap();
        grower_thread
            .join()
            .expect("parked acquire crashed after the full release");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    /// Spark parks the 100 MiB request below the 256 MiB minimum share. Unless the holder frees
    /// all 100 MiB, toGrant = min(request, memoryFree) stays short and the waiter sleeps again
    /// with nobody left to wake it.
    #[test]
    fn min_share_wait_is_granted_after_the_holder_frees_its_memory_in_full() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, GIB);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(100 * MIB).unwrap();
        stub.other_task_holds(stub.memory_free());
        assert_eq!(stub.memory_free(), 0);

        let (grower_done_tx, grower_done_rx) = channel();
        let grower_thread = thread::spawn(move || {
            grower.try_grow(100 * MIB).unwrap();
            grower.free();
            let _ = grower_done_tx.send(());
        });
        stub.wait_parked("grower");

        let holder_thread = thread::spawn(move || holder.free());
        holder_thread.join().unwrap();

        assert!(
            grower_done_rx.recv_timeout(TEST_TIMEOUT).is_ok(),
            "acquire stayed parked after the holder freed 100 MiB"
        );
        grower_thread
            .join()
            .expect("parked acquire crashed or timed out");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
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

    /// The rollback takes the task's balance to zero, dropping the entry under the parked
    /// sibling, which is granted once the bridge has asked again.
    #[test]
    fn short_grant_rollback_landing_under_a_parked_sibling_wakes_it() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        // Three active tasks: 17 free and a 16 byte minimum share, so 30 bytes gets 17.
        stub.task_holds(OTHER_TASK, 82);
        stub.task_holds(THIRD_TASK, 1);

        stub.release_gate.arm();
        let first_thread = thread::spawn(move || {
            let result = first.try_grow(30);
            assert!(result.is_err(), "30 bytes cannot fit into 17 free bytes");
            first.free();
        });
        stub.release_gate
            .wait_entered("first consumer's rollback release");

        // The third task leaves: two active tasks now, a 25 byte minimum share, 1 byte free.
        stub.task_releases(THIRD_TASK, 1);
        let second_thread = thread::spawn(move || {
            second.try_grow(10).unwrap();
            second.free();
        });
        stub.wait_parked("second acquire");

        stub.release_gate.disarm();
        stub.release_gate.open();
        first_thread.join().unwrap();
        second_thread
            .join()
            .expect("second acquire crashed when the rollback release landed");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn pool_that_never_grows_never_calls_spark() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        drop(res);
        drop(pool);
        assert_eq!(stub.acquires.load(SeqCst), 0);
        assert_eq!(stub.releases.load(SeqCst), 0);
    }

    /// Spark's shuffle allocator is another consumer of the task; its last free drops the
    /// task's entry under the parked acquire.
    #[test]
    fn sibling_consumer_freeing_its_last_page_under_a_parked_acquire() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);
        let grower = MemoryConsumer::new("grower").register(&pool);

        // With the pool full, a 10 byte request parks below the 25 byte minimum share.
        stub.sibling_consumer_holds(10);
        stub.other_task_holds(stub.memory_free());
        let grower_thread = thread::spawn(move || {
            grower.try_grow(10).unwrap();
            grower.free();
        });
        stub.wait_parked("grower");

        stub.sibling_consumer_releases(10);
        grower_thread
            .join()
            .expect("parked acquire crashed when a sibling consumer freed its last page");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    /// Covers the Rust pool's lock only: in the JVM, `TaskMemoryManager` serializes a task's
    /// acquires on its own monitor.
    #[test]
    fn second_acquire_does_not_queue_behind_a_parked_one() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        // 4 bytes free with a 25 byte minimum share: 20 bytes parks, 4 bytes is granted.
        stub.other_task_holds(stub.memory_free() - 4);

        let first_thread = thread::spawn(move || {
            let result = first.try_grow(20);
            first.free();
            result
        });
        stub.wait_parked("first acquire");

        let (second_done_tx, second_done_rx) = channel();
        let second_thread = thread::spawn(move || {
            let result = second.try_grow(4);
            second.free();
            let _ = second_done_tx.send(result);
        });
        let second_result = second_done_rx.recv_timeout(Duration::from_secs(2));

        // Room for the parked request, released before asserting so no thread stays parked.
        stub.other_task_releases(50);
        let first_result = first_thread.join().expect("parked first acquire crashed");
        first_result.expect("parked first acquire was not granted once memory was freed");
        second_thread.join().unwrap();
        second_result
            .expect("second acquire queued behind the parked first acquire")
            .expect("second acquire was not granted");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    /// A pool whose `overcommit` a test can read, connected to `stub`.
    fn fair_pool_with(stub: &Arc<StubTaskMemory>, pool_size: usize) -> Arc<CometFairMemoryPool> {
        Arc::new(CometFairMemoryPool::with_spark(
            SparkMemory::with_manager(Box::new(Arc::clone(stub)), THIS_TASK),
            pool_size,
        ))
    }

    fn assert_granted_in_one_call(
        stub: &StubTaskMemory,
        fair: &CometFairMemoryPool,
        res: &MemoryReservation,
        size: usize,
        infallible: bool,
    ) {
        let acquires_before = stub.acquires.load(SeqCst);
        let outstanding_before = stub.outstanding();
        if infallible {
            res.grow(size);
        } else {
            res.try_grow(size)
                .unwrap_or_else(|e| panic!("a {size} byte request Spark can grant failed: {e}"));
        }
        assert_eq!(
            stub.acquires.load(SeqCst),
            acquires_before + 1,
            "one Spark call"
        );
        assert_eq!(
            stub.outstanding(),
            outstanding_before + size as i64,
            "Spark granted the request in full"
        );
        assert_eq!(fair.overcommit(), 0, "nothing carried as overcommit");
        assert!(stub.parked.1.lock().try_recv().is_err(), "nothing parked");
    }

    /// 10 bytes is exactly what is free and under the 25 byte minimum share, so the pool must
    /// ask for nothing besides it.
    fn exact_fit_below_the_minimum_share(infallible: bool) {
        let stub = Arc::new(StubTaskMemory::new(100));
        stub.other_task_holds(90);
        let fair = fair_pool_with(&stub, 1_000);
        let pool: Arc<dyn MemoryPool> = Arc::clone(&fair) as _;
        let res = MemoryConsumer::new("consumer").register(&pool);

        assert_granted_in_one_call(&stub, &fair, &res, 10, infallible);
        assert_eq!(pool.reserved(), 10);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn exact_fit_try_grow_below_the_minimum_share_is_granted_in_one_call() {
        exact_fit_below_the_minimum_share(false);
    }

    #[test]
    fn exact_fit_grow_below_the_minimum_share_is_granted_in_one_call() {
        exact_fit_below_the_minimum_share(true);
    }

    /// 30 bytes is exactly what is free and reaches the minimum share.
    fn exact_fit_at_the_minimum_share(infallible: bool) {
        let stub = Arc::new(StubTaskMemory::new(100));
        stub.other_task_holds(70);
        let fair = fair_pool_with(&stub, 1_000);
        let pool: Arc<dyn MemoryPool> = Arc::clone(&fair) as _;
        let res = MemoryConsumer::new("consumer").register(&pool);

        assert_granted_in_one_call(&stub, &fair, &res, 30, infallible);
        assert_eq!(pool.reserved(), 30);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn exact_fit_try_grow_at_the_minimum_share_is_granted_in_full() {
        exact_fit_at_the_minimum_share(false);
    }

    #[test]
    fn exact_fit_grow_at_the_minimum_share_is_granted_in_full() {
        exact_fit_at_the_minimum_share(true);
    }

    fn exact_fit_after_a_full_release(infallible: bool) {
        let stub = Arc::new(StubTaskMemory::new(100));
        let fair = fair_pool_with(&stub, 1_000);
        let pool: Arc<dyn MemoryPool> = Arc::clone(&fair) as _;
        let res = MemoryConsumer::new("consumer").register(&pool);
        res.try_grow(10).unwrap();
        res.free();
        assert_eq!(
            stub.outstanding(),
            0,
            "a full release leaves nothing with Spark"
        );

        stub.other_task_holds(90);
        assert_granted_in_one_call(&stub, &fair, &res, 10, infallible);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn exact_fit_try_grow_after_a_full_release_is_granted_in_one_call() {
        exact_fit_after_a_full_release(false);
    }

    #[test]
    fn exact_fit_grow_after_a_full_release_is_granted_in_one_call() {
        exact_fit_after_a_full_release(true);
    }

    #[test]
    fn grow_racing_a_shrink_is_not_admitted_on_bytes_the_jvm_still_holds() {
        let stub = Arc::new(StubTaskMemory::new(100));
        // Two consumers: a 100 byte fair limit against a task whose Spark share is 100 bytes.
        let pool = pool_with(&stub, 200);
        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = holder.new_empty();
        let other = MemoryConsumer::new("other").register(&pool);
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
            "bytes stay charged until the release lands"
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

    #[test]
    fn short_grant_rollback_keeps_the_bytes_charged_until_the_jvm_has_them_back() {
        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(2));
        // Two consumers: a 100 byte fair limit.
        let pool = pool_with(&stub, 200);
        let first = MemoryConsumer::new("first").register(&pool);
        let second = first.new_empty();
        let other = MemoryConsumer::new("other").register(&pool);
        // Uses up the first acquire, so the 100 byte request is the halved second and the 60
        // byte grow after the rollback is granted in full.
        first.try_grow(1).unwrap();
        first.free();

        stub.release_gate.arm();
        let first_thread = thread::spawn(move || {
            let result = first.try_grow(100);
            first.free();
            result
        });
        stub.release_gate.wait_entered("short grant rollback");

        let reserved_during = pool.reserved();
        let acquires_before = stub.acquires.load(SeqCst);
        let result = second.try_grow(60);
        let acquires_during = stub.acquires.load(SeqCst);

        stub.release_gate.disarm();
        stub.release_gate.open();
        let first_result = first_thread.join().unwrap();

        let err = first_result.expect_err("100 bytes were requested and 50 granted");
        assert!(err.to_string().contains("only got"), "{err}");
        assert_eq!(
            reserved_during, 50,
            "the granted bytes stay charged during the rollback"
        );
        let err = result.expect_err("60 bytes do not fit beside 50 still charged");
        assert!(err.to_string().contains("fair limit"), "{err}");
        assert_eq!(
            acquires_during, acquires_before,
            "grow reached Spark on bytes the JVM still held"
        );

        assert_eq!(
            pool.reserved(),
            0,
            "the rollback settles once Spark has the bytes"
        );
        assert_eq!(stub.outstanding(), 0);
        second
            .try_grow(60)
            .expect("the same grow fits once the rollback has landed");
        assert_eq!(pool.reserved(), 60);
        second.free();
        drop(second);
        drop(other);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn a_grow_in_flight_counts_against_the_pool_limit_until_it_settles() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 100);
        let grower = MemoryConsumer::new("grower").register(&pool);

        stub.acquire_gate.arm();
        let grower_thread = thread::spawn(move || {
            grower.try_grow(60).unwrap();
            grower
        });
        stub.acquire_gate.wait_entered("grower acquire");

        // 50 bytes fit the halved share, but not the pool beside the 60 in flight.
        let other = MemoryConsumer::new("other").register(&pool);
        let reserved_during = pool.reserved();
        let acquires_before = stub.acquires.load(SeqCst);
        let result = other.try_grow(50);
        let acquires_during = stub.acquires.load(SeqCst);

        stub.acquire_gate.disarm();
        stub.acquire_gate.open();
        let grower = grower_thread.join().unwrap();

        assert_eq!(reserved_during, 60, "the grow in flight is already charged");
        let err = result.expect_err("60 in flight plus 50 exceeds the 100 byte pool");
        assert!(err.to_string().contains("pool limit"), "{err}");
        assert_eq!(
            acquires_during, acquires_before,
            "the refused grow must not reach Spark"
        );

        assert_eq!(pool.reserved(), 60);
        other
            .try_grow(40)
            .expect("40 bytes fit beside the settled 60");
        assert_eq!(pool.reserved(), 100);
        drop(grower);
        drop(other);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn failed_short_grant_rollback_keeps_the_bytes_charged_and_reports_a_short_grant() {
        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(1));
        // Two consumers: a 100 byte fair limit.
        let pool = pool_with(&stub, 200);
        let first = MemoryConsumer::new("first").register(&pool);
        let second = first.new_empty();
        let other = MemoryConsumer::new("other").register(&pool);
        stub.fail_release.store(true, SeqCst);

        let err = first.try_grow(100).unwrap_err();
        assert!(err.to_string().contains("only got"), "{err}");
        assert_eq!(
            pool.reserved(),
            50,
            "the bytes Spark still holds stay charged"
        );
        assert_eq!(stub.outstanding(), 50, "the stranded grant");
        assert_eq!(stub.used.load(SeqCst), 50, "the JVM counts it as held too");

        let acquires_before = stub.acquires.load(SeqCst);
        let err = second.try_grow(51).unwrap_err();
        assert!(err.to_string().contains("fair limit"), "{err}");
        assert_eq!(stub.acquires.load(SeqCst), acquires_before);

        stub.fail_release.store(false, SeqCst);
        drop(first);
        drop(second);
        drop(other);
        drop(pool);
        assert_eq!(
            stub.outstanding(),
            50,
            "the stranded grant stays with Spark"
        );
    }

    #[test]
    fn grow_that_overflows_the_total_is_refused_without_calling_spark() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        res.try_grow(1).unwrap();

        let acquires_before = stub.acquires.load(SeqCst);
        let err = res.try_grow(usize::MAX).unwrap_err();
        assert!(err.to_string().contains("fair limit"), "{err}");
        assert_eq!(stub.acquires.load(SeqCst), acquires_before);
        assert_eq!(pool.reserved(), 1);
        res.free();
    }

    /// Pins the window that settling after the JVM call leaves open: a grow that fits without
    /// the shrunk bytes reaches Spark ahead of the release and gets a zero grant.
    #[test]
    fn grow_racing_a_shrink_gets_a_short_grant_at_the_spark_share() {
        let stub = Arc::new(StubTaskMemory::new(100));
        // Two consumers: a 100 byte fair limit against a task whose Spark share is 100 bytes.
        let pool = pool_with(&stub, 200);
        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
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

    /// If the second also asked for the byte owed, its two byte request would park below the
    /// minimum share with nothing left to wake it once the first repays the debt.
    #[test]
    fn concurrent_try_grows_ask_spark_for_the_overcommit_once() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let fair = fair_pool_with(&stub, 100);
        let pool: Arc<dyn MemoryPool> = Arc::clone(&fair) as _;
        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        // Three active tasks: 16 free and a 16 byte minimum share, so 17 bytes owes one.
        stub.task_holds(OTHER_TASK, 83);
        stub.task_holds(THIRD_TASK, 1);
        first.grow(17);
        assert_eq!(stub.outstanding(), 16);
        assert_eq!(fair.overcommit(), 1);
        // Two active tasks, a 25 byte minimum share and 3 bytes free.
        stub.task_releases(THIRD_TASK, 1);
        stub.other_task_releases(2);

        stub.granted_gate.arm();
        let first_thread = thread::spawn(move || first.try_grow(1).map(|()| first));
        stub.granted_gate
            .wait_entered("first try_grow after its grant");
        stub.granted_gate.disarm();

        let (second_done_tx, second_done_rx) = channel();
        let second_thread = thread::spawn(move || {
            let result = second.try_grow(1);
            let _ = second_done_tx.send(());
            result.map(|()| second)
        });
        // Well below WAIT_TIMEOUT, so a request parked in Spark shows up here.
        let second_done_while_first_held =
            second_done_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        stub.granted_gate.open();
        let requests = stub.requests.lock().clone();
        if !second_done_while_first_held {
            // Repaying the debt wakes nothing in Spark, so free memory to let both finish.
            stub.other_task_releases(10);
        }
        let first = first_thread.join().unwrap().expect("first try_grow");
        let second_result = second_thread.join();

        assert!(
            second_done_while_first_held,
            "the second try_grow did not finish while the first was held after its grant"
        );
        assert_eq!(requests, vec![17, 2, 1]);
        let second = second_result.unwrap().expect("second try_grow");
        assert_eq!(fair.overcommit(), 0);
        assert_eq!(fair.spark.claimed(), 0);
        assert_eq!(
            stub.outstanding() as usize + fair.overcommit(),
            pool.reserved(),
            "Spark's grant plus the overcommit is what the pool recorded"
        );
        assert_eq!(pool.reserved(), 19);
        drop(first);
        drop(second);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn each_consumer_is_limited_to_its_own_share() {
        // Spark grants everything, so only the pool's own checks refuse.
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);

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
    fn a_consumer_registered_late_is_limited_by_the_pool_total() {
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 90));
        let first = MemoryConsumer::new("first").register(&pool);
        // Alone, the first consumer's share is the whole pool.
        first.try_grow(60).unwrap();

        // A second consumer halves both shares, but the first keeps the 60 bytes it holds.
        let second = MemoryConsumer::new("second").register(&pool);
        assert!(first.try_grow(1).is_err());
        second.try_grow(30).unwrap();
        // The second consumer is 15 bytes under its share, but the pool is full.
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 90);

        // Once the first consumer releases memory, the second can use the rest of its share.
        first.shrink(30);
        second.try_grow(15).unwrap();
        assert!(second.try_grow(1).is_err());

        // Unregistering the second consumer gives the first the whole pool again.
        drop(second);
        first.try_grow(60).unwrap();
        assert_eq!(pool.reserved(), 90);
        assert_eq!(fake.held(), 90);
    }

    #[test]
    fn sibling_reservations_draw_on_one_share() {
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let first = MemoryConsumer::new("first").register(&pool);
        // Like the reservation that a sort's streaming merge creates for each batch it reads.
        let sibling = first.new_empty();
        let second = MemoryConsumer::new("second").register(&pool);

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
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let mut first = MemoryConsumer::new("first").register(&pool);
        let _second = MemoryConsumer::new("second").register(&pool);

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
