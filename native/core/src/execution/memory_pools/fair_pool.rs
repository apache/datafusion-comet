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
/// No lock is held across a JVM call, so a release can land while an acquire of the same task
/// waits inside Spark. If that release takes the task's balance to zero, Spark drops the task's
/// `memoryForTask` entry and the waiting acquire fails when it wakes; `CometTaskMemoryManager`
/// asks Spark again in that case.
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
    /// `try_grow` but before calling `shrink`. Bytes are charged here and to `used` together, and
    /// settle together once the JVM has answered.
    consumers: HashMap<usize, usize>,
}

impl CometFairPoolState {
    /// The bytes held by `consumer` across all of its reservations.
    fn consumer_used(&mut self, consumer: usize) -> &mut usize {
        self.consumers
            .get_mut(&consumer)
            .expect("reservation's consumer is not registered with the pool")
    }

    /// Charges `bytes` to the pool's total and to `consumer`'s share.
    fn charge(&mut self, consumer: usize, bytes: usize) {
        self.used = self.used.saturating_add(bytes);
        let consumer_used = self.consumer_used(consumer);
        *consumer_used = consumer_used.saturating_add(bytes);
    }

    /// Takes `charged` bytes off the pool's total and `consumer`'s share and puts `held` back.
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

    /// Settles a release the JVM has accepted: the bytes come off the pool's total and the
    /// consumer's share only now, so a grow is never admitted on bytes Spark still holds.
    fn settle_release(&self, consumer: usize, bytes: usize) {
        self.state.lock().settle(consumer, bytes, 0);
    }

    /// Settles a finished JVM acquire. `charged` bytes went on the pool's total and the
    /// consumer's share before the call and `held` is what Spark granted and still holds, which
    /// stays charged until it is handed back. The difference is rolled back.
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
        // Charged before the JVM call, as in try_grow, so a try_grow racing this one is checked
        // against totals that already include these bytes. The JVM call then runs without the
        // lock held.
        self.state.lock().charge(consumer, additional);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.spark.acquire(additional);
        }));
        if let Err(panic) = outcome {
            // The caller's reservation never records bytes its grow panicked on, so the charge
            // must not outlive the panic either.
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
            // The JVM release runs without the lock so a blocked acquire on another thread can
            // never stall this release. The bytes stay charged until Spark has them back, so a
            // grow racing this shrink is not admitted on them and sent to Spark ahead of the
            // release. Outstanding overcommit is repaid before anything goes back to Spark. A
            // failed release here panics (the caller already gave the bytes up, there is no one
            // left to handle an error), while the short-grant path in try_grow returns Err
            // because its caller can still spill. The check above is advisory once the lock is
            // dropped: DataFusion never shrinks a reservation past its size, which is what keeps
            // the subtraction in settle_release in range.
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
            // Checking both limits and charging the bytes is one atomic step, so concurrent grows
            // can never jointly take a consumer past its share or the pool past pool_size. The
            // blocking JVM calls then run without any lock held, and the charge rolls back if
            // the JVM does not back it.
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

            // Spark is asked for the request plus any outstanding overcommit, and a full grant
            // repays the overcommit. A short grant stays with Spark until this pool hands it
            // back below, so the bytes can stay charged meanwhile. The JVM call can panic inside
            // its JNI frame; the optimistic reservation must not outlive it, or the leaked bytes
            // poison the task-shared pool for every other consumer.
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
            // A grant that falls short of the request is handed back whole and reported so the
            // caller can spill. The bytes Spark did not back come off the books at once. The
            // granted bytes, which can exceed the request when overcommit was asked for on top
            // of it, stay charged until Spark has them back, like a shrink, so no grow is
            // admitted on them meanwhile. A failed return leaves Spark holding them until the
            // task ends, and they stay charged here to match. The caller still gets the short
            // grant error below so a spillable operator spills. A panic in that release leaves
            // the same state as an error.
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
                "Failed to acquire {} bytes plus {} bytes overcommitted, only got {} bytes. Reserved: {} bytes",
                additional,
                refusal.overcommit,
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

    /// The task this pool belongs to and neighbours that compete with it for the same pool.
    const THIS_TASK: i64 = 0;
    const OTHER_TASK: i64 = 1;
    const THIRD_TASK: i64 = 2;

    /// Real Spark parks a starved acquire forever; the stub gives up after this long and fails
    /// the test instead, so a deadlock shows up as a panic rather than a hung test binary. A
    /// heavily loaded CI box may need this lengthened.
    const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_TIMEOUT: Duration = Duration::from_secs(20);

    /// A pause point inside a bridge call. While armed, the call announces itself on `entered`
    /// and blocks until the test opens the gate, so a test can interleave a second thread at a
    /// precise moment of a JNI call.
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

        /// Called from the bridge thread.
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

    /// In-process stand-in for Spark's task memory manager that models Spark 4.1.3's
    /// `ExecutionMemoryPool.acquireMemory` and `releaseMemory` for this task, including the
    /// per-task entry lifecycle, the 1/N and 1/(2N) share rules, and the wait loop, together
    /// with `CometTaskMemoryManager` asking again when a waiter's entry is gone. Re-adding the
    /// entry in the wait loop stands in for that JVM retry, so these tests do not cover the
    /// retry itself.
    struct StubTaskMemory {
        pool: Mutex<SparkPool>,
        /// Spark's `lock`: parked acquires wait on it and every release does `notifyAll`.
        lock: Condvar,
        releases: AtomicUsize,
        acquires: AtomicUsize,
        /// `CometTaskMemoryManager.used`: bytes granted and not yet released. This is what the
        /// JVM checks for leaked reservations once the task's last plan has closed.
        used: AtomicI64,
        /// When non-zero, every n-th acquire asks Spark for only half of the requested bytes,
        /// which is how a caller sees a short grant that must be rolled back.
        short_every: usize,
        /// When set, acquire fails outright.
        fail_acquire: AtomicBool,
        /// When set, release fails outright, like a JVM exception on the way back.
        fail_release: AtomicBool,
        /// When set, acquire panics, like a failure inside the bridge's JNI frame.
        panic_acquire: AtomicBool,
        /// Pauses an acquire before it takes the pool monitor, like `TaskMemoryManager`
        /// spilling other consumers between two pool calls.
        acquire_gate: Gate,
        /// Pauses a release before it takes the pool monitor, like the JNI hop.
        release_gate: Gate,
        /// Announces every trip into `lock.wait()`, so a waiter that is woken and parks again
        /// announces twice.
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
                used: AtomicI64::new(0),
                short_every: 0,
                fail_acquire: AtomicBool::new(false),
                fail_release: AtomicBool::new(false),
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

        /// Another task of the same executor takes `bytes` from the pool, which also raises
        /// `numActiveTasks` and shrinks this task's shares.
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

        /// A neighbour hands `bytes` back, dropping out of the active set at zero, which wakes
        /// every parked acquire like Spark's notifyAll.
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

        /// The sibling consumer hands `bytes` of this task back, which removes the task's entry
        /// if that empties it.
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
                // Spark adds the task's entry on the way in. A woken waiter whose entry a
                // release has removed meanwhile gets a NoSuchElementException, and
                // `CometTaskMemoryManager` then asks again, which adds it back.
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
            // Spark only warns and clamps here; the pool must never hand back more than the
            // task holds, so the stub makes that a hard failure.
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

    /// Keeps the count `CometTaskMemoryManager` keeps on top of Spark's balance. Like the JVM,
    /// a release calls Spark first and moves the count only once it has answered.
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

        // Spark grants 100 of the 150 byte grow and 50 becomes overcommit.
        reservation.grow(150);
        assert_eq!(fake.held(), 100);
        assert_eq!(pool.reserved(), 150);
        assert_eq!(fair.overcommit(), 50);

        // 20 bytes of new headroom, not enough for the 5 byte request plus the 50 owed. Spark
        // hands over more than the request itself, and all of it must go back.
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

        // A second consumer halves the fair limit, so the pool is now over it.
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

    /// A panic escaping the bridge's acquire must propagate, but it must not leave the
    /// optimistically reserved bytes behind, or the task-shared pool would be poisoned for
    /// every other consumer.
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

    /// `grow` cannot refuse, but a panic in the bridge's acquire still propagates, and the bytes
    /// it charged before the call must come off the pool's total and the consumer's share.
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

        // The consumer's share is back to 100 bytes, so its 1000 byte fair limit admits 900.
        stub.panic_acquire.store(false, SeqCst);
        res.try_grow(900)
            .expect("the consumer's share still holds the panicked grow");
        assert_eq!(pool.reserved(), 1_000);
        res.free();
        assert_eq!(stub.outstanding(), 0);
    }

    /// Many threads hammering grow/shrink must keep the pool's accounting and the Spark-side
    /// balance consistent, including through fairness rejections and partial-grant rollbacks.
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
                        // Fairness rejections and short grants are expected; only the
                        // accounting invariants below must hold.
                        let _ = res.try_grow(size);
                        if i % 3 == 0 && res.size() > 0 {
                            res.shrink(res.size() / 2 + 1);
                        }
                        // Occasional full drains push the task's balance toward zero while
                        // other threads still have acquires in flight.
                        if (i + t) % 41 == 0 {
                            res.free();
                        }
                        // Each consumer has one reservation, so its size is the consumer's
                        // share of the pool.
                        assert!(
                            res.size() <= POOL_SIZE / THREADS,
                            "consumer exceeded its fair limit"
                        );
                        assert!(pool.reserved() <= POOL_SIZE, "pool exceeded its size");
                    }
                    // Keep all consumers registered until every thread stops growing, so the
                    // fair-limit assertion above stays valid for the whole run.
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

    /// A thread stuck inside the blocking acquire call must not prevent another thread from
    /// releasing memory: the release path cannot wait on any lock held across that call.
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

    /// Spark can park an acquire for as long as other tasks hold the memory it needs. Reading
    /// the pool's usage, as the memory usage logger does from its own thread, must return at
    /// once meanwhile, so no lock that `reserved` takes may be held across the Spark call.
    #[test]
    fn reserved_is_not_blocked_by_an_acquire_parked_in_spark() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();
        // Fill the pool so Spark parks the grower below its minimum share until the holder
        // frees, which only happens after the read below.
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
        // Keep this wait well below the stub's WAIT_TIMEOUT so a blocked read shows up as a
        // timeout. Once the stub gives up, the grower's charge rolls back and a blocked read
        // returns 10 instead, which fails as a less telling count mismatch.
        let reserved_during = reserved_rx.recv_timeout(Duration::from_secs(1));

        // Freeing the holder wakes the grower. If the lock were held across the Spark call, the
        // free would itself wait until the stub gives up, which still bounds the run.
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

    /// Two threads of one task: the pool is full and this task sits below its 1/(2N) minimum
    /// share, so Spark parks the second acquire until memory is freed. The first thread then
    /// frees everything it holds. The release must go through in full and be what wakes the
    /// waiter. It takes the task's balance to zero, so Spark drops the task's entry and the
    /// waiter fails on waking; the bridge asks again and the freed bytes are granted.
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

    /// Modeled on Spark 4.1.3's `ExecutionMemoryPool`: a 1 GiB execution pool, another task
    /// holding 924 MiB, this task holding 100 MiB, and a second native consumer of this task
    /// asking for 100 MiB. Spark parks it below the 256 MiB minimum share. When the holder frees
    /// its 100 MiB, Spark computes toGrant = min(request, memoryFree); anything less than the
    /// full 100 MiB leaves toGrant < request with curMem + toGrant still under 256 MiB, and the
    /// waiter sleeps again with nobody left to wake it.
    #[test]
    fn min_share_wait_is_granted_after_the_holder_frees_its_memory_in_full() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, GIB);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(100 * MIB).unwrap();
        // The neighbour takes the rest of the pool, 924 MiB.
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

    /// A release that is already on its way to Spark when a new acquire arrives and parks: the
    /// release removes the task's entry from under the waiter, and the bridge asks again.
    /// Reproduced by holding the release at the JNI hop, starting a grow that parks below its
    /// minimum share, then letting the release land.
    #[test]
    fn late_acquire_survives_a_release_already_on_its_way() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();
        stub.other_task_holds(stub.memory_free());

        // The holder's full release is planned and dispatched, then held before Spark sees it.
        stub.release_gate.arm();
        let holder_thread = thread::spawn(move || holder.free());
        stub.release_gate.wait_entered("holder release");

        // Only now does the grower arrive; the pool is full so it parks inside Spark.
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

    /// A short grant is handed back whole, and that release can land while a sibling of this
    /// task is parked inside Spark. Here the first consumer's rollback is held at the JNI hop, a
    /// third task then leaves the pool (which raises this task's minimum share), and a second
    /// consumer's acquire arrives and parks. The rollback takes the task's balance to zero and
    /// wakes it, and the acquire gets the bytes once the bridge has asked again.
    #[test]
    fn short_grant_rollback_landing_under_a_parked_sibling_wakes_it() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        // Three active tasks: 17 free with a 16 byte minimum share, so a 30 byte request gets
        // a short grant of 17 rather than parking.
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

    /// A pool that never grows never touches Spark.
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

    /// Spark's shuffle allocator is another off-heap consumer of the same task. When it frees
    /// its last page while an acquire of this pool is parked, Spark drops the task's entry, and
    /// the acquire is granted once the bridge has asked again.
    #[test]
    fn sibling_consumer_freeing_its_last_page_under_a_parked_acquire() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);
        let grower = MemoryConsumer::new("grower").register(&pool);

        // A sibling consumer holds 10 bytes of this task and a neighbour fills the rest, so a
        // 10 byte request parks below the 25 byte minimum share.
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

    /// One consumer's acquire is parked inside Spark. A second consumer's acquire that Spark can
    /// grant at once must not queue behind it on the pool's lock. This covers the Rust pool's lock
    /// only: in the JVM, `TaskMemoryManager` serializes a task's acquires on its own monitor,
    /// which the parked acquire keeps while it waits.
    #[test]
    fn second_acquire_does_not_queue_behind_a_parked_one() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        // 4 bytes free with a 25 byte minimum share: a 20 byte request parks, a 4 byte one
        // is granted outright.
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

    /// Asks for `size` bytes through `try_grow` or, with `infallible`, through `grow`, and checks
    /// that Spark granted all of them in a single call without parking.
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

    /// Another task holds 90 of Spark's 100 bytes and this task holds nothing, so a 10 byte
    /// request is exactly what is free and is under the task's 25 byte minimum share. Spark
    /// grants it at once, and the pool must ask for nothing besides it.
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

    /// The same exact fit for a request that reaches the task's minimum share: another task
    /// holds 70 bytes, and 30 are granted in full rather than short.
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

    /// Once the pool has handed everything back, it holds nothing from Spark, so the next exact
    /// fit sees the same free bytes as the first one and is granted in one call too.
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

    /// A shrink hands its bytes back to Spark before the pool stops counting them. While that
    /// release is still on its way, a grow of the same consumer that only fits if the shrunk
    /// bytes were free is refused at the fair limit without a JVM call, instead of reaching
    /// Spark ahead of the release and coming back with a short grant.
    #[test]
    fn grow_racing_a_shrink_is_not_admitted_on_bytes_the_jvm_still_holds() {
        let stub = Arc::new(StubTaskMemory::new(100));
        // Two consumers: a 100 byte fair limit against a task whose Spark share is 100 bytes.
        let pool = pool_with(&stub, 200);
        let holder = MemoryConsumer::new("holder").register(&pool);
        // A sibling reservation draws on the holder's share.
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

        // Let the release land before asserting so no thread stays parked on failure.
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

    /// A short grant is rolled back by handing the granted bytes to Spark. Until that
    /// release lands, the pool keeps charging them, so a grow of the same consumer on another
    /// thread cannot be admitted on bytes Spark still holds for this task.
    #[test]
    fn short_grant_rollback_keeps_the_bytes_charged_until_the_jvm_has_them_back() {
        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(2));
        // Two consumers: a 100 byte fair limit.
        let pool = pool_with(&stub, 200);
        let first = MemoryConsumer::new("first").register(&pool);
        // A sibling reservation draws on the first consumer's share.
        let second = first.new_empty();
        let other = MemoryConsumer::new("other").register(&pool);
        // The stub halves every second acquire. This 1 byte grow takes the first, so the 100
        // byte request below is granted 50 and the 60 byte grow after the rollback, the third,
        // in full.
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

    /// A grow is charged to the pool when it passes the limits, before Spark answers it. While
    /// it is in flight, another consumer's grow that would push the total past the pool size is
    /// refused, which is what keeps two grows from jointly exceeding the pool. The charge is
    /// rolled back if Spark does not back it.
    #[test]
    fn a_grow_in_flight_counts_against_the_pool_limit_until_it_settles() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 100);
        let grower = MemoryConsumer::new("grower").register(&pool);

        // Alone, the grower's share is the whole pool.
        stub.acquire_gate.arm();
        let grower_thread = thread::spawn(move || {
            grower.try_grow(60).unwrap();
            grower
        });
        stub.acquire_gate.wait_entered("grower acquire");

        // A second consumer halves the shares. The 60 bytes in flight still count for the pool.
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

    /// The rollback of a short grant hands the granted bytes back, and that release can fail.
    /// Spark then holds the bytes until the task ends, so the pool keeps them charged, and the
    /// caller still gets the short grant error so a spillable operator spills.
    #[test]
    fn failed_short_grant_rollback_keeps_the_bytes_charged_and_reports_a_short_grant() {
        // Every acquire is granted half.
        let stub = Arc::new(StubTaskMemory::new(GIB).short_every(1));
        // Two consumers: a 100 byte fair limit.
        let pool = pool_with(&stub, 200);
        let first = MemoryConsumer::new("first").register(&pool);
        // A sibling reservation draws on the first consumer's share.
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

    /// A request that overflows the running total is refused like any other over-limit grow,
    /// without reaching Spark.
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

    /// The window that settling after the JVM call leaves open. A grow that fits under the fair
    /// limit without the shrunk bytes still reaches Spark ahead of the shrink's release, and at
    /// the task's Spark share it comes back with a zero grant. This pins the behaviour rather
    /// than changing it.
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
