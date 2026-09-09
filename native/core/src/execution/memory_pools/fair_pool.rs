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
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    sync::Arc,
};

use jni::objects::{Global, JObject};
use log::warn;

use crate::{
    errors::{CometError, CometResult},
    jvm_bridge::JVMClasses,
};
use datafusion::common::resources_err;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::{
    common::DataFusionError,
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use parking_lot::Mutex;

/// The task memory backend the pool acquires from and releases to. The production implementation
/// calls Spark's task memory manager over JNI, which can block while Spark spills other consumers;
/// keeping it behind a trait lets tests exercise the pool without a live JVM.
trait TaskMemoryBridge: Send + Sync {
    fn acquire(&self, additional: usize) -> CometResult<i64>;
    fn release(&self, size: usize) -> CometResult<()>;
}

/// Delegates to the JVM side `CometTaskMemoryManager`.
struct JniTaskMemoryBridge {
    task_memory_manager_handle: Arc<Global<JObject<'static>>>,
}

impl TaskMemoryBridge for JniTaskMemoryBridge {
    fn acquire(&self, additional: usize) -> CometResult<i64> {
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env,
              comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)
        })
    }

    fn release(&self, size: usize) -> CometResult<()> {
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        })
    }
}

/// Size of the anchor, the byte the pool keeps on the JVM side for its whole life.
const ANCHOR_BYTES: usize = 1;

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
///
/// Spark drops a task's `memoryForTask` entry when its balance hits zero, and an acquire parked
/// inside Spark indexes that entry on wake. The pool takes one byte at setup and keeps it until
/// it drops, so no release, this pool's or a sibling consumer's, can zero the balance under a
/// waiter. Releases go to the JVM whole: Spark grants a parked request only when they cover it.
pub struct CometFairMemoryPool {
    bridge: Box<dyn TaskMemoryBridge>,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
}

struct CometFairPoolState {
    used: usize,
    num: usize,
    /// Bytes the JVM side has granted us and not yet been handed back, anchor included.
    jvm_held: usize,
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("used", &state.used)
            .field("num", &state.num)
            .finish()
    }
}

impl CometFairMemoryPool {
    /// Takes the anchor byte from Spark before the pool is usable. A starved task parks here
    /// like any acquire, so plan creation waits until memory frees, and a task already at its
    /// share gets an error instead of a pool. A sibling consumer zeroing the balance while this
    /// is parked makes Spark fail the acquire, and construction fails with it, holding nothing.
    pub fn try_new(
        task_memory_manager_handle: Arc<Global<JObject<'static>>>,
        pool_size: usize,
    ) -> CometResult<CometFairMemoryPool> {
        Self::with_bridge(
            Box::new(JniTaskMemoryBridge {
                task_memory_manager_handle,
            }),
            pool_size,
        )
    }

    fn with_bridge(
        bridge: Box<dyn TaskMemoryBridge>,
        pool_size: usize,
    ) -> CometResult<CometFairMemoryPool> {
        let granted = usize::try_from(bridge.acquire(ANCHOR_BYTES)?).unwrap_or(0);
        if granted < ANCHOR_BYTES {
            return Err(CometError::Internal(format!(
                "Failed to acquire the memory pool's {ANCHOR_BYTES} byte anchor, the task \
                 memory manager granted {granted} bytes"
            )));
        }
        if granted > ANCHOR_BYTES {
            warn!("Requested {ANCHOR_BYTES} bytes from the JVM but it reports {granted} granted");
        }
        Ok(Self {
            bridge,
            pool_size,
            state: Mutex::new(CometFairPoolState {
                used: 0,
                num: 0,
                jvm_held: ANCHOR_BYTES,
            }),
        })
    }

    fn acquire(&self, additional: usize) -> CometResult<i64> {
        self.bridge.acquire(additional)
    }

    fn release(&self, size: usize) -> CometResult<()> {
        self.bridge.release(size)
    }

    /// Debits a release from the JVM-side balance before it is handed back.
    fn debit(state: &mut CometFairPoolState, bytes: usize) {
        state.jvm_held = state
            .jvm_held
            .checked_sub(bytes)
            .expect("released more bytes than the JVM side holds");
    }

    /// Settles a finished bridge acquire: rolls back the bytes the JVM did not back and records
    /// the bytes the pool keeps.
    fn finish_acquire(&self, kept: usize, unbacked: usize) {
        let mut state = self.state.lock();
        state.used = state
            .used
            .checked_sub(unbacked)
            .expect("rolled back more bytes than the pool tracks");
        state.jvm_held = state
            .jvm_held
            .checked_add(kept)
            .expect("overflow in checked_add");
    }
}

impl Drop for CometFairMemoryPool {
    /// The last plan of the task letting go of the pool runs this, on whatever thread that
    /// happens on; `with_env` attaches the thread and the JVM object handle outlives the pool.
    /// Nothing may panic out of a drop, so failures are only logged, and Spark frees the task's
    /// whole balance when the task ends anyway.
    fn drop(&mut self) {
        let state = self.state.get_mut();
        match state.jvm_held.checked_sub(ANCHOR_BYTES) {
            Some(rest) => state.jvm_held = rest,
            None => {
                warn!("Memory pool anchor is held but the JVM-side balance is already zero");
                return;
            }
        }
        let released = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.bridge.release(ANCHOR_BYTES)
        }));
        match released {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to release the memory pool anchor byte: {e:?}"),
            Err(_) => warn!("Bridge panicked while releasing the memory pool anchor byte"),
        }
    }
}

impl Display for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        write!(
            f,
            "CometFairMemoryPool(pool_size={}, used={}, num={})",
            self.pool_size, state.used, state.num
        )
    }
}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, _: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.num = state
            .num
            .checked_add(1)
            .expect("unexpected amount of register happened");
    }

    fn unregister(&self, _: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.num = state
            .num
            .checked_sub(1)
            .expect("unexpected amount of unregister happened");
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.try_grow(_reservation, additional).unwrap();
    }

    fn shrink(&self, _reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            {
                let mut state = self.state.lock();
                // We don't use reservation.size() here because DataFusion 53+ decrements
                // the reservation's atomic size before calling pool.shrink(), so it would
                // reflect the post-shrink value rather than the pre-shrink value.
                if state.used < subtractive {
                    panic!(
                        "Failed to release {subtractive} bytes where only {} bytes tracked by pool",
                        state.used
                    )
                }
                state.used -= subtractive;
                Self::debit(&mut state, subtractive);
            }
            // The JVM release runs without the lock so a blocked acquire on another thread can
            // never stall this release. A failed release here panics (the caller already gave the
            // bytes up, there is no one left to handle an error), while the short-grant path in
            // try_grow returns Err because its caller can still spill.
            self.release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
        }
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            // Checking the fair limit and reserving the bytes is one atomic step, so concurrent
            // grows can never jointly exceed pool_size / num. The blocking JVM acquire then runs
            // without any lock held, and the reservation rolls back if the JVM does not back it.
            {
                let mut state = self.state.lock();
                let num = state.num;
                let limit = self
                    .pool_size
                    .checked_div(num)
                    .expect("overflow in checked_div");
                // We use state.used instead of reservation.size() because DataFusion 53+
                // calls pool.try_grow() before incrementing the reservation's atomic size,
                // so reservation.size() would not include prior grows.
                let used = state.used;
                if limit < used + additional {
                    return resources_err!(
                        "Failed to acquire {additional} bytes where {used} bytes already reserved and the fair limit is {limit} bytes, {num} registered"
                    );
                }
                state.used = used
                    .checked_add(additional)
                    .expect("overflow in checked_add");
            }

            // The bridge can panic inside its JNI frame; the optimistic reservation must not
            // outlive the call, or the leaked bytes poison the task-shared pool for every
            // other consumer.
            let acquired = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.acquire(additional)
            })) {
                Ok(Ok(acquired)) => acquired,
                Ok(Err(e)) => {
                    self.finish_acquire(0, additional);
                    return Err(e.into());
                }
                Err(panic) => {
                    self.finish_acquire(0, additional);
                    std::panic::resume_unwind(panic);
                }
            };
            let granted = usize::try_from(acquired).unwrap_or(0);
            if granted > additional {
                // Spark never grants more than it is asked for. Clamp so a misbehaving bridge
                // cannot push jvm_held past what the pool later releases.
                warn!("Requested {additional} bytes from the JVM but it reports {granted} granted");
            }
            let granted = granted.min(additional);
            // A grant that falls short of the request is handed back whole and reported so the
            // caller can spill.
            if granted < additional {
                // Return the headroom before handing the grant back to the JVM, so other
                // threads can use it even if the release itself fails.
                self.finish_acquire(0, additional);
                if granted > 0 {
                    // The bytes are already off the books, so a failed return only leaves Spark
                    // holding them until the task ends.
                    self.release(granted)?;
                }

                return resources_err!(
                    "Failed to acquire {} bytes, only got {} bytes. Reserved: {} bytes",
                    additional,
                    acquired,
                    self.reserved()
                );
            }
            self.finish_acquire(granted, 0);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CometError;
    use parking_lot::Condvar;
    use std::collections::{hash_map::Entry, HashMap};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Barrier;
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
    /// per-task entry lifecycle, the 1/N and 1/(2N) share rules, and the wait loop.
    struct StubTaskMemory {
        pool: Mutex<SparkPool>,
        /// Spark's `lock`: parked acquires wait on it and every release does `notifyAll`.
        lock: Condvar,
        releases: AtomicUsize,
        acquires: AtomicUsize,
        /// When non-zero, every n-th acquire asks Spark for only half of the requested bytes,
        /// which is how a caller sees a short grant that must be rolled back.
        short_every: usize,
        /// When set, acquire fails outright.
        fail_acquire: AtomicBool,
        /// When set, acquire panics, like a failure inside the bridge's JNI frame.
        panic_acquire: AtomicBool,
        /// Pauses an acquire before it takes the pool monitor, like `TaskMemoryManager`
        /// spilling other consumers between two pool calls.
        acquire_gate: Gate,
        /// Pauses a release before it takes the pool monitor, like the JNI hop.
        release_gate: Gate,
        /// Announces every trip into `lock.wait()`, so a waiter that is woken and parks again
        /// announces twice; the tests here expect exactly one park per scenario.
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

    impl TaskMemoryBridge for Arc<StubTaskMemory> {
        fn acquire(&self, additional: usize) -> CometResult<i64> {
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
            if let Entry::Vacant(entry) = pool.memory_for_task.entry(THIS_TASK) {
                entry.insert(0);
                self.lock.notify_all();
            }
            loop {
                let num_active_tasks = pool.memory_for_task.len() as i64;
                // A woken waiter indexes memoryForTask unconditionally; a removed entry is a
                // NoSuchElementException in ExecutionMemoryPool.acquireMemory.
                let Some(&cur_mem) = pool.memory_for_task.get(&THIS_TASK) else {
                    panic!("key not found: {THIS_TASK}");
                };
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

        fn release(&self, size: usize) -> CometResult<()> {
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

    fn try_pool_with(
        stub: &Arc<StubTaskMemory>,
        pool_size: usize,
    ) -> CometResult<CometFairMemoryPool> {
        CometFairMemoryPool::with_bridge(Box::new(Arc::clone(stub)), pool_size)
    }

    fn pool_with(stub: &Arc<StubTaskMemory>, pool_size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(try_pool_with(stub, pool_size).expect("anchor acquire failed"))
    }

    #[test]
    fn grow_and_shrink_update_pool_and_spark_accounting() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        res.try_grow(600).unwrap();
        assert_eq!(pool.reserved(), 600);
        assert_eq!(stub.outstanding(), 601, "anchor plus the grant");

        res.shrink(200);
        assert_eq!(pool.reserved(), 400);
        assert_eq!(stub.outstanding(), 401);

        res.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(
            stub.outstanding(),
            1,
            "anchor outlives the last reservation"
        );

        drop(res);
        drop(pool);
        assert_eq!(
            stub.outstanding(),
            0,
            "anchor is returned when the pool drops"
        );
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
        assert_eq!(stub.outstanding(), 1, "partial grant must be handed back");
    }

    #[test]
    fn acquire_failure_leaves_accounting_unchanged() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        stub.fail_acquire.store(true, SeqCst);
        assert!(res.try_grow(100).is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
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
        assert_eq!(stub.outstanding(), 1);
    }

    /// The bridge fails the anchor acquire outright: construction fails and the task memory
    /// manager is left exactly as it was.
    #[test]
    fn construction_fails_when_the_bridge_errors_on_the_anchor() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        stub.fail_acquire.store(true, SeqCst);

        let err = try_pool_with(&stub, 1_000).expect_err("anchor was declined");
        assert!(
            err.to_string().contains("injected acquire failure"),
            "{err}"
        );
        assert_eq!(stub.outstanding(), 0, "a failed anchor holds nothing");
        assert_eq!(stub.releases.load(SeqCst), 0, "nothing to hand back");
    }

    /// Spark declines the anchor with a zero grant when the task is already at its share, here
    /// because a sibling consumer holds all of it. Construction fails and nothing is released,
    /// so the sibling's bytes are untouched.
    #[test]
    fn construction_fails_when_spark_grants_no_anchor() {
        let stub = Arc::new(StubTaskMemory::new(100));
        stub.sibling_consumer_holds(100);

        let err = try_pool_with(&stub, 1_000).expect_err("anchor was declined");
        assert!(err.to_string().contains("granted 0 bytes"), "{err}");
        assert_eq!(stub.outstanding(), 100, "sibling's bytes must be untouched");
        assert_eq!(stub.releases.load(SeqCst), 0, "nothing to hand back");
    }

    /// Dropping the pool is the only path that hands the anchor back, and it must do so
    /// exactly once.
    #[test]
    fn anchor_is_released_exactly_once_when_the_pool_drops() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        res.try_grow(10).unwrap();
        res.free();
        drop(res);

        let releases_before_drop = stub.releases.load(SeqCst);
        assert_eq!(stub.outstanding(), 1);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
        assert_eq!(stub.releases.load(SeqCst), releases_before_drop + 1);
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
                        assert!(
                            pool.reserved() <= POOL_SIZE / THREADS,
                            "pool exceeded its fair limit"
                        );
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
            1,
            "Spark-side bytes leaked or double-released, only the anchor should remain"
        );
        drop(pool);
        assert_eq!(stub.outstanding(), 0, "anchor was not returned at drop");
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

    /// Two threads of one task: the pool is full and this task sits below its 1/(2N) minimum
    /// share, so Spark parks the second acquire until memory is freed. The first thread then
    /// frees everything it holds. The release must go through in full and be what wakes the
    /// waiter, and the waiter must find the task's entry still present.
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
    }

    /// The maintainer's reproduction against Spark 4.1.3: a 1 GiB execution pool, another task
    /// holding 900 MiB, this task holding 100 MiB, and a second native consumer of this task
    /// asking for 100 MiB. Spark parks it below the 256 MiB minimum share. When the holder
    /// frees its 100 MiB, Spark computes toGrant = min(request, memoryFree); anything less than
    /// the full 100 MiB leaves toGrant < request with curMem + toGrant still under 256 MiB, and
    /// the waiter sleeps again with nobody left to wake it.
    #[test]
    fn min_share_wait_is_granted_after_the_holder_frees_its_memory_in_full() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, GIB);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(100 * MIB).unwrap();
        // The neighbour takes the rest of the pool (900 MiB less the anchor byte).
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
    }

    /// A release that is already on its way to Spark when a new acquire arrives and parks: the
    /// release must not remove the task's entry from under the waiter. Reproduced by holding
    /// the release at the JNI hop, starting a grow that parks below its minimum share, then
    /// letting the release land.
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
    }

    /// A short grant is handed back whole, and that release can land while a sibling of this
    /// task is parked inside Spark. Here the first consumer's rollback is held at the JNI hop, a
    /// third task then leaves the pool (which raises this task's minimum share), and a second
    /// consumer's acquire arrives and parks. The rollback must wake it, not drop the entry.
    #[test]
    fn short_grant_rollback_landing_under_a_parked_sibling_keeps_the_entry_alive() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000_000);

        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);
        // Three active tasks: 16 free with a 16 byte minimum share, so a 30 byte request gets
        // a short grant of 16 rather than parking.
        stub.task_holds(OTHER_TASK, 82);
        stub.task_holds(THIRD_TASK, 1);

        stub.release_gate.arm();
        let first_thread = thread::spawn(move || {
            let result = first.try_grow(30);
            assert!(result.is_err(), "30 bytes cannot fit into 16 free bytes");
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
    }

    /// The anchor is taken once when the pool is set up, before any consumer grows, and
    /// handed back once when the pool drops.
    #[test]
    fn pool_takes_its_anchor_at_setup_and_returns_it_at_drop() {
        let stub = Arc::new(StubTaskMemory::new(GIB));
        let pool = pool_with(&stub, 1_000);
        assert_eq!(stub.outstanding(), 1, "anchor is taken at setup");
        assert_eq!(pool.reserved(), 0, "anchor is not part of the reservation");

        drop(pool);
        assert_eq!(stub.outstanding(), 0, "anchor is returned at drop");
        assert_eq!(stub.releases.load(SeqCst), 1);
    }

    /// Spark's shuffle allocator is another off-heap consumer of the same task. When it frees
    /// its last page while an acquire of this pool is parked, Spark drops the task's entry
    /// unless this pool already holds its anchor byte.
    #[test]
    fn sibling_consumer_freeing_its_last_page_does_not_drop_the_task_entry() {
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
    }

    /// One consumer's acquire is parked inside Spark. A second consumer's acquire that Spark can
    /// grant at once must not queue behind it inside the pool.
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
        assert_eq!(stub.outstanding(), 1, "only the anchor may remain");
    }

    /// A request for exactly the bytes Spark has free is a grant Spark satisfies in full, and
    /// the pool must accept it.
    #[test]
    fn first_grow_of_exactly_the_free_bytes_is_accepted() {
        let stub = Arc::new(StubTaskMemory::new(100));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);
        stub.other_task_holds(stub.memory_free() - 25);

        res.try_grow(25)
            .expect("an exact-fit grant must be accepted");
        assert_eq!(pool.reserved(), 25);
        assert_eq!(stub.outstanding(), 26, "anchor plus the grant");
        res.free();
        assert_eq!(stub.outstanding(), 1);
        drop(res);
        drop(pool);
        assert_eq!(stub.outstanding(), 0);
    }

    /// The anchor acquire itself can park, and a sibling consumer can zero the task's balance
    /// under it; Spark then fails that acquire (the stub panics like the JVM exception) and the
    /// pool must not exist, with nothing held or released.
    #[test]
    fn anchor_acquire_failing_under_a_vanished_entry_leaves_nothing_behind() {
        let stub = Arc::new(StubTaskMemory::new(100));
        // The sibling holds this task's whole share, the neighbour fills the rest: the anchor
        // request parks below the minimum share with nothing free.
        stub.sibling_consumer_holds(10);
        stub.other_task_holds(90);

        let construction = {
            let stub = Arc::clone(&stub);
            thread::spawn(move || try_pool_with(&stub, 1_000).map(drop))
        };
        stub.wait_parked("anchor acquire");
        stub.sibling_consumer_releases(10);

        assert!(
            construction.join().is_err(),
            "the parked anchor acquire must fail once its task entry is gone"
        );
        assert_eq!(stub.outstanding(), 0);
        assert_eq!(stub.releases.load(SeqCst), 0, "nothing to hand back");
    }

    /// Two plans of one task create their pools at once: both take an anchor, the registry keeps
    /// the first, and the loser's drop hands its anchor back exactly once.
    #[test]
    fn concurrent_creates_through_the_registry_keep_one_anchor() {
        use crate::execution::memory_pools::acquire_task_shared_pool;

        let stub = Arc::new(StubTaskMemory::new(GIB));
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let stub = Arc::clone(&stub);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    acquire_task_shared_pool(-2001, || {
                        let pool = try_pool_with(&stub, 1_000)?;
                        // Both anchors are taken before either pool reaches the registry.
                        barrier.wait();
                        Ok(Arc::new(pool) as Arc<dyn MemoryPool>)
                    })
                    .unwrap()
                })
            })
            .collect();
        let pools: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();

        assert!(Arc::ptr_eq(&pools[0], &pools[1]));
        assert_eq!(stub.acquires.load(SeqCst), 2, "each create takes an anchor");
        assert_eq!(
            stub.releases.load(SeqCst),
            1,
            "the loser returns its anchor once"
        );
        assert_eq!(stub.outstanding(), 1, "the surviving pool keeps its anchor");
        drop(pools);
        assert_eq!(stub.outstanding(), 0);
        assert_eq!(stub.releases.load(SeqCst), 2);
    }
}
