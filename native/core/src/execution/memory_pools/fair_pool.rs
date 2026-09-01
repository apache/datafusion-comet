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

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometFairMemoryPool {
    bridge: Box<dyn TaskMemoryBridge>,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
}

struct CometFairPoolState {
    used: usize,
    num: usize,
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
            state: Mutex::new(CometFairPoolState { used: 0, num: 0 }),
        }
    }

    fn acquire(&self, additional: usize) -> CometResult<i64> {
        self.bridge.acquire(additional)
    }

    fn release(&self, size: usize) -> CometResult<()> {
        self.bridge.release(size)
    }

    /// Returns bytes optimistically reserved by `try_grow` after the JVM failed to back them.
    fn rollback(&self, additional: usize) {
        let mut state = self.state.lock();
        state.used = state
            .used
            .checked_sub(additional)
            .expect("rolled back more bytes than the pool tracks");
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
            }
            // The JVM release runs without the lock so a blocked acquire on another thread can
            // never stall this release. A failed release here panics (the caller already gave the
            // bytes up, there is no one left to handle an error), while the partial-grant path in
            // try_grow returns Err after the same debit because its caller can still spill.
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
                    self.rollback(additional);
                    return Err(e.into());
                }
                Err(panic) => {
                    self.rollback(additional);
                    std::panic::resume_unwind(panic);
                }
            };
            // If the number of bytes we acquired is less than the requested, return an error,
            // and hopefully will trigger spilling from the caller side.
            if acquired < additional as i64 {
                // Return the headroom before handing the partial grant back to the JVM, so other
                // threads can use it even if the release itself fails.
                self.rollback(additional);
                // Release the acquired bytes before throwing error
                self.release(acquired as usize)?;

                return resources_err!(
                    "Failed to acquire {} bytes, only got {} bytes. Reserved: {} bytes",
                    additional,
                    acquired,
                    self.reserved()
                );
            }
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
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    /// In-process stand-in for Spark's task memory manager. Tracks the bytes it has granted so
    /// tests can assert the pool never releases more than it acquired.
    struct StubTaskMemory {
        /// Bytes currently granted; a release must never drive this negative.
        outstanding: AtomicI64,
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
    }

    impl StubTaskMemory {
        fn new() -> Self {
            Self {
                outstanding: AtomicI64::new(0),
                acquires: AtomicUsize::new(0),
                short_every: 0,
                fail_acquire: AtomicBool::new(false),
                panic_acquire: AtomicBool::new(false),
                park_armed: AtomicBool::new(false),
                park: None,
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

        fn outstanding(&self) -> i64 {
            self.outstanding.load(SeqCst)
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
            if let Some((entered, gate)) = &self.park {
                if self.park_armed.load(SeqCst) {
                    let _ = entered.send(());
                    // A dropped gate also unparks, so a failing test can still unwind cleanly.
                    let _ = gate.lock().recv();
                }
            }
            let granted = if self.short_every != 0 && n.is_multiple_of(self.short_every) {
                additional / 2
            } else {
                additional
            };
            self.outstanding.fetch_add(granted as i64, SeqCst);
            Ok(granted as i64)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            let prev = self.outstanding.fetch_sub(size as i64, SeqCst);
            assert!(
                prev >= size as i64,
                "released {size} bytes with only {prev} outstanding"
            );
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
}
