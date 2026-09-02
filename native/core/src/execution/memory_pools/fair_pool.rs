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

use crate::{errors::CometResult, jvm_bridge::JVMClasses};
use datafusion::common::resources_err;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::{
    common::DataFusionError,
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use parking_lot::{Condvar, Mutex};

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

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometFairMemoryPool {
    bridge: Box<dyn TaskMemoryBridge>,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
    /// Signals that the held-back release finished; see `paying_deferred`.
    deferred_done: Condvar,
}

struct CometFairPoolState {
    used: usize,
    num: usize,
    /// Bytes the JVM side has granted us and not yet been handed back.
    jvm_held: usize,
    /// Number of bridge acquire calls currently in flight.
    pending_acquires: usize,
    /// Bytes held back from a release that would have zeroed the JVM-side balance.
    deferred_release: usize,
    /// True while the held-back bytes are on their way to the JVM; acquires wait it out.
    paying_deferred: bool,
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
    pub fn new(
        task_memory_manager_handle: Arc<Global<JObject<'static>>>,
        pool_size: usize,
    ) -> CometFairMemoryPool {
        Self::with_bridge(
            Box::new(JniTaskMemoryBridge {
                task_memory_manager_handle,
            }),
            pool_size,
        )
    }

    fn with_bridge(bridge: Box<dyn TaskMemoryBridge>, pool_size: usize) -> CometFairMemoryPool {
        Self {
            bridge,
            pool_size,
            state: Mutex::new(CometFairPoolState {
                used: 0,
                num: 0,
                jvm_held: 0,
                pending_acquires: 0,
                deferred_release: 0,
                paying_deferred: false,
            }),
            deferred_done: Condvar::new(),
        }
    }

    fn acquire(&self, additional: usize) -> CometResult<i64> {
        self.bridge.acquire(additional)
    }

    fn release(&self, size: usize) -> CometResult<()> {
        self.bridge.release(size)
    }

    /// Debits a release from the JVM-side balance and returns how much to hand back now. A
    /// release that would zero the balance while acquires are in flight keeps one byte back,
    /// because Spark drops the task's accounting entry at zero and a parked acquire then indexes
    /// the missing entry. Blocking instead could deadlock: the waiter may need this very memory.
    /// The n-1 bytes freed here still wake Spark's waiter; the single held byte only matters
    /// in a pool small enough that one byte decides the fair-share threshold, and even there
    /// the deferred payoff releases it as soon as in-flight acquires drain.
    fn plan_release(state: &mut CometFairPoolState, bytes: usize) -> usize {
        state.jvm_held = state
            .jvm_held
            .checked_sub(bytes)
            .expect("released more bytes than the JVM side holds");
        if bytes > 0 && state.jvm_held == 0 && state.pending_acquires > 0 {
            state.jvm_held = 1;
            state.deferred_release += 1;
            bytes - 1
        } else {
            bytes
        }
    }

    /// Settles a finished bridge acquire, whatever its outcome: rolls back unbacked bytes,
    /// records what the JVM granted, and once no acquires remain in flight hands any held-back
    /// bytes over while new acquires briefly wait, so no acquire can park on a dying balance.
    fn finish_acquire(&self, granted: usize, unbacked: usize) {
        let payment = {
            let mut state = self.state.lock();
            state.used = state
                .used
                .checked_sub(unbacked)
                .expect("rolled back more bytes than the pool tracks");
            state.jvm_held = state
                .jvm_held
                .checked_add(granted)
                .expect("overflow in checked_add");
            state.pending_acquires -= 1;
            if state.pending_acquires == 0 && state.deferred_release > 0 {
                let bytes = std::mem::take(&mut state.deferred_release);
                state.jvm_held -= bytes;
                state.paying_deferred = true;
                bytes
            } else {
                0
            }
        };
        self.pay_deferred(payment);
    }

    fn pay_deferred(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        // The flag must clear even if the JVM call blows up, or every later acquire would
        // wait on it forever.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.release(bytes)));
        {
            let mut state = self.state.lock();
            state.paying_deferred = false;
            self.deferred_done.notify_all();
        }
        match result {
            Ok(result) => result.unwrap_or_else(|_| panic!("Failed to release {bytes} bytes")),
            Err(panic) => std::panic::resume_unwind(panic),
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
            let to_release = {
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
                Self::plan_release(&mut state, subtractive)
            };
            // The JVM release runs without the lock so a blocked acquire on another thread can
            // never stall this release. A failed release here panics (the caller already gave the
            // bytes up, there is no one left to handle an error), while the partial-grant path in
            // try_grow returns Err after the same debit because its caller can still spill.
            if to_release > 0 {
                self.release(to_release)
                    .unwrap_or_else(|_| panic!("Failed to release {to_release} bytes"));
            }
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
                // A held-back release is on its way to the JVM; an acquire started now could
                // park on a balance about to hit zero, so wait out the short payment. No cycle:
                // the payer waits on nothing of ours and always clears the flag.
                while state.paying_deferred {
                    self.deferred_done.wait(&mut state);
                }
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
                state.pending_acquires += 1;
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
            // If the number of bytes we acquired is less than the requested, return an error,
            // and hopefully will trigger spilling from the caller side.
            if acquired < additional as i64 {
                // Return the headroom before handing the partial grant back to the JVM, so other
                // threads can use it even if the release itself fails.
                let granted = usize::try_from(acquired).unwrap_or(0);
                self.finish_acquire(granted, additional);
                // Hand the partial grant back through the guarded path so it cannot zero the
                // JVM-side balance under someone else's parked acquire.
                let to_release = {
                    let mut state = self.state.lock();
                    Self::plan_release(&mut state, granted)
                };
                if to_release > 0 {
                    self.release(to_release)?;
                }

                return resources_err!(
                    "Failed to acquire {} bytes, only got {} bytes. Reserved: {} bytes",
                    additional,
                    acquired,
                    self.reserved()
                );
            }
            self.finish_acquire(additional, 0);
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    /// In-process stand-in for Spark's task memory manager. Models the per-task entry that
    /// ExecutionMemoryPool keeps in memoryForTask: created when an acquire arrives, removed
    /// when a release drains it to zero, and indexed again by any acquire that parked.
    struct StubTaskMemory {
        /// The task's granted balance; None means Spark removed the entry.
        entry: Mutex<Option<i64>>,
        /// Wakes acquires parked in wait-for-release mode, like Spark's notifyAll.
        released: Condvar,
        releases: AtomicUsize,
        acquires: AtomicUsize,
        /// When non-zero, every n-th acquire is granted only half of the requested bytes.
        short_every: usize,
        /// When set, acquire fails outright.
        fail_acquire: AtomicBool,
        /// When set, acquire panics, like a failure inside the bridge's JNI frame.
        panic_acquire: AtomicBool,
        /// When armed, acquire announces itself on `entered` and parks until `gate` fires or drops.
        park_armed: AtomicBool,
        park: Option<(Sender<()>, Mutex<Receiver<()>>)>,
        /// When armed, acquire announces itself and parks until some release lands, like a
        /// task below its minimum share waiting for memory to be freed.
        wait_for_release_armed: AtomicBool,
        wait_entered: Option<Sender<()>>,
    }

    impl StubTaskMemory {
        fn new() -> Self {
            Self {
                entry: Mutex::new(None),
                released: Condvar::new(),
                releases: AtomicUsize::new(0),
                acquires: AtomicUsize::new(0),
                short_every: 0,
                fail_acquire: AtomicBool::new(false),
                panic_acquire: AtomicBool::new(false),
                park_armed: AtomicBool::new(false),
                park: None,
                wait_for_release_armed: AtomicBool::new(false),
                wait_entered: None,
            }
        }

        fn short_every(mut self, n: usize) -> Self {
            self.short_every = n;
            self
        }

        fn with_park(mut self, entered: Sender<()>, gate: Receiver<()>) -> Self {
            self.park = Some((entered, Mutex::new(gate)));
            self
        }

        fn with_wait_for_release(mut self, entered: Sender<()>) -> Self {
            self.wait_entered = Some(entered);
            self
        }

        fn outstanding(&self) -> i64 {
            self.entry.lock().unwrap_or(0)
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
            // Spark creates the task's entry on the way into acquireMemory and holds the pool
            // monitor for the whole call, giving it up only while parked in lock.wait().
            let mut entry = self.entry.lock();
            entry.get_or_insert(0);
            if let Some((entered, gate)) = &self.park {
                if self.park_armed.load(SeqCst) {
                    let _ = entered.send(());
                    drop(entry);
                    // A dropped gate also unparks, so a failing test can still unwind cleanly.
                    let _ = gate.lock().recv();
                    entry = self.entry.lock();
                }
            }
            if self.wait_for_release_armed.load(SeqCst) {
                let before = self.releases.load(SeqCst);
                if let Some(entered) = &self.wait_entered {
                    let _ = entered.send(());
                }
                while self.releases.load(SeqCst) == before {
                    let timed_out = self
                        .released
                        .wait_for(&mut entry, Duration::from_secs(10))
                        .timed_out();
                    assert!(!timed_out, "parked acquire was never woken by a release");
                }
            }
            // A woken waiter indexes memoryForTask unconditionally, so a removed entry means
            // a NoSuchElementException in ExecutionMemoryPool.acquireMemory.
            let Some(balance) = entry.as_mut() else {
                panic!("key not found: task entry removed while acquire waited");
            };
            let granted = if self.short_every != 0 && n.is_multiple_of(self.short_every) {
                additional / 2
            } else {
                additional
            };
            *balance += granted as i64;
            Ok(granted as i64)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            let mut entry = self.entry.lock();
            // Mirrors ExecutionMemoryPool.releaseMemory: debit the entry, remove it at zero,
            // notify waiters. The pool must never hand back more than the task holds.
            let Some(balance) = entry.as_mut() else {
                panic!("released {size} bytes with no task entry");
            };
            assert!(
                *balance >= size as i64,
                "released {size} bytes with only {balance} outstanding"
            );
            *balance -= size as i64;
            if *balance <= 0 {
                *entry = None;
            }
            self.releases.fetch_add(1, SeqCst);
            self.released.notify_all();
            Ok(())
        }
    }

    fn pool_with(stub: &Arc<StubTaskMemory>, pool_size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(CometFairMemoryPool::with_bridge(
            Box::new(Arc::clone(stub)),
            pool_size,
        ))
    }

    #[test]
    fn grow_and_shrink_update_pool_and_spark_accounting() {
        let stub = Arc::new(StubTaskMemory::new());
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
        let stub = Arc::new(StubTaskMemory::new());
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        res.try_grow(600).unwrap();
        let err = res.try_grow(500).unwrap_err();
        assert!(err.to_string().contains("fair limit"), "{err}");
        assert_eq!(pool.reserved(), 600);
        assert_eq!(
            stub.acquires.load(SeqCst),
            1,
            "over-limit grow must be rejected before reaching Spark"
        );
        res.free();
    }

    #[test]
    fn fair_limit_shrinks_as_consumers_register() {
        let stub = Arc::new(StubTaskMemory::new());
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
        let stub = Arc::new(StubTaskMemory::new().short_every(1));
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        let err = res.try_grow(100).unwrap_err();
        assert!(err.to_string().contains("only got"), "{err}");
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0, "partial grant must be handed back");
    }

    #[test]
    fn acquire_failure_leaves_accounting_unchanged() {
        let stub = Arc::new(StubTaskMemory::new());
        stub.fail_acquire.store(true, SeqCst);
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        assert!(res.try_grow(100).is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    fn zero_sized_grow_does_not_call_spark() {
        let stub = Arc::new(StubTaskMemory::new());
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        pool.try_grow(&res, 0).unwrap();
        pool.shrink(&res, 0);
        assert_eq!(stub.acquires.load(SeqCst), 0);
        assert_eq!(stub.outstanding(), 0);
    }

    #[test]
    #[should_panic(expected = "Failed to release")]
    fn shrinking_more_than_tracked_panics() {
        let stub = Arc::new(StubTaskMemory::new());
        let pool = pool_with(&stub, 1_000);
        let res = MemoryConsumer::new("consumer").register(&pool);

        pool.shrink(&res, 100);
    }

    /// A panic escaping the bridge's acquire must propagate, but it must not leave the
    /// optimistically reserved bytes behind, or the task-shared pool would be poisoned for
    /// every other consumer.
    #[test]
    fn panicking_acquire_rolls_back_the_reservation() {
        let stub = Arc::new(StubTaskMemory::new());
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

    /// Many threads hammering grow/shrink must keep the pool's accounting and the Spark-side
    /// balance consistent, including through fairness rejections and partial-grant rollbacks.
    #[test]
    fn concurrent_grow_and_shrink_keep_accounting_consistent() {
        const THREADS: usize = 8;
        const ITERS: usize = 500;
        const POOL_SIZE: usize = 80_000;

        let stub = Arc::new(StubTaskMemory::new().short_every(7));
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
            0,
            "Spark-side bytes leaked or double-released"
        );
    }

    /// A thread stuck inside the blocking acquire call must not prevent another thread from
    /// releasing memory: the release path cannot wait on any lock held across that call.
    #[test]
    fn shrink_is_not_blocked_by_a_slow_acquire_on_another_thread() {
        let (entered_tx, entered_rx) = channel();
        let (gate_tx, gate_rx) = channel::<()>();
        let stub = Arc::new(StubTaskMemory::new().with_park(entered_tx, gate_rx));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(1_000).unwrap();

        stub.park_armed.store(true, SeqCst);
        let grower_thread = thread::spawn(move || {
            // The result is irrelevant; the test only needs this acquire to be in flight.
            let _ = grower.try_grow(500);
            grower.free();
        });
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("grower never reached the acquire call");

        let (done_tx, done_rx) = channel();
        let releaser_thread = thread::spawn(move || {
            holder.free();
            let _ = done_tx.send(());
        });
        let released = done_rx.recv_timeout(Duration::from_secs(10));

        // Open the gate before asserting so no thread stays parked if the assertion fails.
        stub.park_armed.store(false, SeqCst);
        let _ = gate_tx.send(());
        releaser_thread.join().unwrap();
        grower_thread.join().unwrap();
        assert!(
            released.is_ok(),
            "release was blocked behind an in-flight acquire"
        );
    }

    /// Two threads of one task: one parks inside the acquire while the other hands back the
    /// task's entire balance. Spark drops the per-task entry once its balance hits zero, so
    /// the release must keep the balance alive until the parked acquire has finished.
    #[test]
    fn full_release_does_not_strand_a_parked_acquire() {
        let (entered_tx, entered_rx) = channel();
        let (gate_tx, gate_rx) = channel::<()>();
        let stub = Arc::new(StubTaskMemory::new().with_park(entered_tx, gate_rx));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();

        stub.park_armed.store(true, SeqCst);
        let grower_thread = thread::spawn(move || {
            grower.try_grow(10).unwrap();
            grower.free();
        });
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("grower never reached the acquire call");

        // The full release lands while the grower is still parked inside the acquire.
        holder.free();

        stub.park_armed.store(false, SeqCst);
        let _ = gate_tx.send(());
        grower_thread
            .join()
            .expect("parked acquire crashed after the full release");

        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0, "held-back bytes were never returned");
    }

    /// Here the parked acquire can only proceed after memory is freed, so a release that
    /// waited for in-flight acquires to finish first would deadlock. The release must go
    /// through immediately and be what wakes the waiter.
    #[test]
    fn full_release_wakes_an_acquire_waiting_for_memory() {
        let (entered_tx, entered_rx) = channel();
        let stub = Arc::new(StubTaskMemory::new().with_wait_for_release(entered_tx));
        let pool = pool_with(&stub, 1_000_000);

        let holder = MemoryConsumer::new("holder").register(&pool);
        let grower = MemoryConsumer::new("grower").register(&pool);
        holder.try_grow(10).unwrap();

        stub.wait_for_release_armed.store(true, SeqCst);
        let (grower_done_tx, grower_done_rx) = channel();
        let grower_thread = thread::spawn(move || {
            grower.try_grow(10).unwrap();
            grower.free();
            let _ = grower_done_tx.send(());
        });
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("grower never reached the acquire call");
        stub.wait_for_release_armed.store(false, SeqCst);

        let (holder_done_tx, holder_done_rx) = channel();
        let holder_thread = thread::spawn(move || {
            holder.free();
            let _ = holder_done_tx.send(());
        });

        assert!(
            holder_done_rx.recv_timeout(Duration::from_secs(20)).is_ok(),
            "full release deadlocked behind the parked acquire"
        );
        assert!(
            grower_done_rx.recv_timeout(Duration::from_secs(20)).is_ok(),
            "parked acquire never completed after the release"
        );
        holder_thread.join().unwrap();
        grower_thread.join().unwrap();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(stub.outstanding(), 0, "held-back bytes were never returned");
    }
}
