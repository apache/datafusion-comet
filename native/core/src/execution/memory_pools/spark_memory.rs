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

use std::sync::Arc;

use jni::objects::{Global, JObject};
use log::warn;
use parking_lot::Mutex;

use crate::{errors::CometResult, jvm_bridge::JVMClasses};

/// Spark's side of a Comet pool: the calls that acquire and release off-heap execution memory.
pub(super) trait SparkMemoryManager: Send + Sync {
    /// Asks Spark for `size` bytes and returns how many it granted.
    fn acquire(&self, size: usize) -> CometResult<i64>;
    fn release(&self, size: usize) -> CometResult<()>;
}

/// Calls [`crate::jvm_bridge::CometTaskMemoryManager`] over JNI.
pub(super) struct JniMemoryManager(Arc<Global<JObject<'static>>>);

impl SparkMemoryManager for JniMemoryManager {
    fn acquire(&self, size: usize) -> CometResult<i64> {
        let handle = self.0.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env,
              comet_task_memory_manager(handle).acquire_memory(size as i64) -> i64)
        })
    }

    fn release(&self, size: usize) -> CometResult<()> {
        let handle = self.0.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        })
    }
}

/// Memory a Comet pool holds from Spark, including any it has recorded without Spark's grant.
///
/// `MemoryPool::grow` must always succeed: DataFusion calls it for memory that already exists,
/// such as a spilled batch read back from disk. When Spark grants less than [`Self::acquire`]
/// asked for, the shortfall is carried as overcommit rather than failing. [`Self::release`] repays
/// it before returning anything to Spark, so Spark is never handed back more than it granted.
/// While any is outstanding, [`Self::try_acquire`] also asks Spark for the part no other call is
/// asking for, so a `try_grow` that carries debt is refused unless Spark covers both, and the
/// operator spills.
///
/// A release while a claim is parked in Spark leaves that request too large, so it may come back
/// short and be refused. Handing the claim to Spark on release instead would break the invariant
/// below whenever the claim is refused.
///
/// # Invariant
///
/// Every byte a pool records through this type is backed either by Spark's grant or by
/// overcommit, so Spark's grant plus `overcommit` equals the bytes recorded and not yet released.
/// Spark is handed back more than it granted only if a release takes less from `overcommit` than
/// it could. `CometUnifiedMemoryPool` and `CometFairMemoryPool` both call in here from several
/// threads without holding a lock, and each updates its own `used` separately, so this rests on
/// three things:
///
/// - `overcommit` only grows by bytes that are being recorded in the same call.
/// - Each repayment takes its share of `overcommit` under the ledger's lock, so two concurrent
///   calls can never repay the same debt.
/// - A caller never releases more than it recorded. DataFusion guarantees this, because a shrink
///   cannot exceed the reservation it comes from.
pub(super) struct SparkMemory {
    manager: Box<dyn SparkMemoryManager>,
    /// Held only for arithmetic, never across a call to Spark.
    ledger: Mutex<Ledger>,
    task_attempt_id: i64,
}

#[derive(Default)]
struct Ledger {
    /// Bytes recorded without Spark's grant.
    overcommit: usize,
    /// Debt that `try_acquire` calls in flight have put into their Spark requests. A release
    /// can repay that debt meanwhile, so this can exceed `overcommit`.
    claimed: usize,
}

/// Debt one `try_acquire` has put into its Spark request. Dropped without [`Claim::settle`],
/// as on a refusal, an error or a panic, it gives the debt back for another `try_acquire`.
struct Claim<'a> {
    ledger: &'a Mutex<Ledger>,
    bytes: usize,
    settled: bool,
}

impl Claim<'_> {
    /// Returns the part of the claim a release repaid meanwhile, which Spark granted for nothing.
    fn settle(mut self) -> usize {
        self.settled = true;
        if self.bytes == 0 {
            return 0;
        }
        let mut ledger = self.ledger.lock();
        let owed = self.bytes.min(ledger.overcommit);
        ledger.overcommit -= owed;
        ledger.claimed -= self.bytes;
        self.bytes - owed
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        if !self.settled && self.bytes > 0 {
            self.ledger.lock().claimed -= self.bytes;
        }
    }
}

/// Why [`SparkMemory::try_acquire`] refused a request.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Refusal {
    /// Overcommit this call claimed and asked for on top of the request.
    pub(super) overcommit: usize,
    /// What Spark granted toward the request plus the overcommit, which can exceed the request.
    pub(super) granted: usize,
}

impl SparkMemory {
    pub(super) fn new(handle: Arc<Global<JObject<'static>>>, task_attempt_id: i64) -> Self {
        Self::with_manager(Box::new(JniMemoryManager(handle)), task_attempt_id)
    }

    pub(super) fn with_manager(manager: Box<dyn SparkMemoryManager>, task_attempt_id: i64) -> Self {
        Self {
            manager,
            ledger: Mutex::default(),
            task_attempt_id,
        }
    }

    pub(super) fn task_attempt_id(&self) -> i64 {
        self.task_attempt_id
    }

    /// Bypasses the overcommit ledger, for a pool that hands back a short grant itself.
    pub(super) fn manager(&self) -> &dyn SparkMemoryManager {
        self.manager.as_ref()
    }

    /// Acquires `size` bytes plus the overcommit it claims, or nothing. A full grant repays that
    /// debt; a partial one is handed back and reported as a [`Refusal`].
    pub(super) fn try_acquire(&self, size: usize) -> CometResult<Result<(), Refusal>> {
        let refusal = match self.try_acquire_leaving_a_short_grant(size)? {
            Ok(()) => return Ok(Ok(())),
            Err(refusal) => refusal,
        };
        if refusal.granted > 0 {
            self.manager.release(refusal.granted)?;
        }
        Ok(Err(refusal))
    }

    /// Like [`Self::try_acquire`], but a short grant stays with Spark for the caller to hand back
    /// through [`Self::manager`].
    pub(super) fn try_acquire_leaving_a_short_grant(
        &self,
        size: usize,
    ) -> CometResult<Result<(), Refusal>> {
        let claim = self.claim_debt();
        let request = size.saturating_add(claim.bytes);
        let granted = granted(request, self.ask_spark(request)?);
        if granted < request {
            return Ok(Err(Refusal {
                overcommit: claim.bytes,
                granted,
            }));
        }
        // A concurrent release may have repaid part of the claim while Spark was asked for it,
        // and Spark gets that part back.
        let excess = claim.settle();
        if excess > 0 {
            self.manager.release(excess)?;
        }
        Ok(Ok(()))
    }

    /// Acquires what Spark will grant toward `size` bytes and carries the rest as overcommit.
    /// Never fails; a failed call to Spark counts as a zero grant.
    pub(super) fn acquire(&self, size: usize) {
        let granted = match self.ask_spark(size) {
            Ok(acquired) => granted(size, acquired),
            Err(e) => {
                warn!(
                    "Task {} failed to acquire {size} bytes from Spark: {e:?}",
                    self.task_attempt_id
                );
                0
            }
        };
        if granted < size {
            self.ledger.lock().overcommit += size - granted;
        }
    }

    /// Frees `size` bytes, repaying overcommit before releasing the rest to Spark.
    pub(super) fn release(&self, size: usize) -> CometResult<()> {
        let to_release = size - self.repay(size);
        if to_release > 0 {
            self.manager.release(to_release)?;
        }
        Ok(())
    }

    pub(super) fn overcommit(&self) -> usize {
        self.ledger.lock().overcommit
    }

    #[cfg(test)]
    pub(super) fn claimed(&self) -> usize {
        self.ledger.lock().claimed
    }

    fn claim_debt(&self) -> Claim<'_> {
        let mut ledger = self.ledger.lock();
        let bytes = ledger.overcommit.saturating_sub(ledger.claimed);
        ledger.claimed += bytes;
        Claim {
            ledger: &self.ledger,
            bytes,
            settled: false,
        }
    }

    /// Asks Spark for `size` bytes and returns how many it granted.
    ///
    /// Spark can block the call until other tasks release memory, so it runs in `block_in_place`.
    /// On a Tokio worker that hands the worker's other tasks to another thread while the call
    /// blocks, so they keep running. One of them may be what would release the memory, such as a
    /// task of a released plan that only needs to be cancelled.
    fn ask_spark(&self, size: usize) -> CometResult<i64> {
        tokio::task::block_in_place(|| self.manager.acquire(size))
    }

    /// Takes up to `size` bytes off the overcommit and returns how many.
    fn repay(&self, size: usize) -> usize {
        let mut ledger = self.ledger.lock();
        let owed = size.min(ledger.overcommit);
        ledger.overcommit -= owed;
        owed
    }
}

/// Clamps Spark's reply to an acquire: it never grants more than asked, and never a negative.
fn granted(requested: usize, acquired: i64) -> usize {
    usize::try_from(acquired).unwrap_or(0).min(requested)
}

/// A stand-in for Spark, shared by the pool tests.
#[cfg(test)]
pub(super) mod fake {
    use super::*;
    use crate::errors::CometError;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    /// Grants up to `limit` bytes in total and records every release. Panics if it is handed
    /// back more than it granted, which is the guarantee the pools must keep.
    #[derive(Default)]
    pub(in crate::execution::memory_pools) struct FakeSpark {
        limit: Mutex<usize>,
        held: Mutex<usize>,
        released: Mutex<Vec<usize>>,
        requests: Mutex<Vec<usize>>,
        fail: AtomicBool,
        during_acquire: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl FakeSpark {
        pub(in crate::execution::memory_pools) fn with(limit: usize) -> Arc<Self> {
            Arc::new(Self {
                limit: Mutex::new(limit),
                ..Default::default()
            })
        }

        pub(in crate::execution::memory_pools) fn failing() -> Arc<Self> {
            Arc::new(Self {
                fail: AtomicBool::new(true),
                ..Default::default()
            })
        }

        pub(in crate::execution::memory_pools) fn fail_acquires(&self, fail: bool) {
            self.fail.store(fail, Relaxed);
        }

        pub(in crate::execution::memory_pools) fn set_limit(&self, limit: usize) {
            *self.limit.lock() = limit;
        }

        /// Runs `f` inside the next acquire, before Spark answers. That is where a call from
        /// another thread lands while this one waits on JNI.
        pub(in crate::execution::memory_pools) fn during_next_acquire(
            &self,
            f: impl FnOnce() + Send + 'static,
        ) {
            *self.during_acquire.lock() = Some(Box::new(f));
        }

        /// Bytes granted and not yet handed back.
        pub(in crate::execution::memory_pools) fn held(&self) -> usize {
            *self.held.lock()
        }

        pub(in crate::execution::memory_pools) fn released(&self) -> Vec<usize> {
            self.released.lock().clone()
        }

        pub(in crate::execution::memory_pools) fn requests(&self) -> Vec<usize> {
            self.requests.lock().clone()
        }

        pub(in crate::execution::memory_pools) fn memory(self: &Arc<Self>) -> SparkMemory {
            SparkMemory::with_manager(Box::new(Arc::clone(self)), 0)
        }
    }

    impl SparkMemoryManager for Arc<FakeSpark> {
        fn acquire(&self, size: usize) -> CometResult<i64> {
            self.requests.lock().push(size);
            // Run before taking any other lock, so `f` can call back into this fake.
            let during = self.during_acquire.lock().take();
            if let Some(f) = during {
                f();
            }
            if self.fail.load(Relaxed) {
                return Err(CometError::Internal("jni".to_string()));
            }
            let limit = *self.limit.lock();
            let mut held = self.held.lock();
            let granted = size.min(limit.saturating_sub(*held));
            *held += granted;
            Ok(granted as i64)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            let mut held = self.held.lock();
            assert!(
                size <= *held,
                "Spark was handed back {size} bytes but only granted {held}"
            );
            *held -= size;
            self.released.lock().push(size);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeSpark;
    use super::*;

    #[test]
    fn try_acquire_hands_back_a_partial_grant() {
        let fake = FakeSpark::with(40);
        let spark = fake.memory();
        assert_eq!(
            spark.try_acquire(100).unwrap(),
            Err(Refusal {
                overcommit: 0,
                granted: 40
            })
        );
        assert_eq!(fake.released(), vec![40]);
        assert_eq!(spark.try_acquire(40).unwrap(), Ok(()));
        assert_eq!(fake.held(), 40);
        assert_eq!(spark.overcommit(), 0);
    }

    #[test]
    fn release_repays_overcommit_before_returning_bytes_to_spark() {
        let fake = FakeSpark::with(40);
        let spark = fake.memory();
        spark.acquire(100);
        assert_eq!(spark.overcommit(), 60);
        spark.release(30).unwrap();
        assert_eq!(spark.overcommit(), 30);
        assert!(fake.released().is_empty());
        spark.release(70).unwrap();
        assert_eq!(spark.overcommit(), 0);
        // Spark gets back exactly the 40 bytes it granted.
        assert_eq!(fake.released(), vec![40]);
        assert_eq!(fake.held(), 0);
    }

    #[test]
    fn try_acquire_asks_for_the_overcommit_too() {
        let fake = FakeSpark::with(40);
        let spark = fake.memory();
        spark.acquire(100);
        // Spark has nothing left for the request, let alone the 60 bytes owed.
        assert_eq!(
            spark.try_acquire(10).unwrap(),
            Err(Refusal {
                overcommit: 60,
                granted: 0
            })
        );
        fake.set_limit(200);
        assert_eq!(spark.try_acquire(10).unwrap(), Ok(()));
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(fake.held(), 110);
        spark.release(110).unwrap();
        assert_eq!(fake.held(), 0);
    }

    #[test]
    fn try_acquire_hands_back_debt_repaid_by_a_concurrent_release() {
        let fake = FakeSpark::with(40);
        let spark = Arc::new(fake.memory());
        spark.acquire(100);
        fake.set_limit(200);
        // try_acquire reads 60 bytes owed and asks Spark for them with the request. Before
        // Spark answers, another consumer shrinks by 30, which repays half of that debt.
        let other = Arc::clone(&spark);
        fake.during_next_acquire(move || other.release(30).unwrap());
        assert_eq!(spark.try_acquire(10).unwrap(), Ok(()));
        assert_eq!(spark.overcommit(), 0);
        // Spark granted 70 bytes where 40 were still needed, and gets the other 30 back.
        assert_eq!(fake.released(), vec![30]);
        // 70 bytes of the first acquire and all 10 of the second are still recorded.
        assert_eq!(fake.held(), 80);
        spark.release(80).unwrap();
        assert_eq!(fake.held(), 0);
    }

    fn five_bytes_owed() -> (Arc<FakeSpark>, Arc<SparkMemory>) {
        let fake = FakeSpark::with(0);
        let spark = Arc::new(fake.memory());
        spark.acquire(5);
        assert_eq!(spark.overcommit(), 5);
        fake.set_limit(1_000);
        (fake, spark)
    }

    #[test]
    fn concurrent_try_acquires_ask_spark_for_the_debt_once() {
        let (fake, spark) = five_bytes_owed();
        let other = Arc::clone(&spark);
        fake.during_next_acquire(move || assert_eq!(other.try_acquire(1).unwrap(), Ok(())));
        assert_eq!(spark.try_acquire(1).unwrap(), Ok(()));
        assert_eq!(fake.requests(), vec![5, 6, 1]);
        assert!(fake.released().is_empty(), "nothing was granted twice");
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(spark.claimed(), 0);
        assert_eq!(fake.held(), 7);
    }

    #[test]
    fn refused_try_acquire_leaves_the_debt_for_the_next_one() {
        let (fake, spark) = five_bytes_owed();
        fake.set_limit(3);
        assert_eq!(
            spark.try_acquire(1).unwrap(),
            Err(Refusal {
                overcommit: 5,
                granted: 3
            })
        );
        assert_eq!(spark.claimed(), 0);
        fake.set_limit(1_000);
        assert_eq!(spark.try_acquire(1).unwrap(), Ok(()));
        assert_eq!(fake.requests(), vec![5, 6, 6]);
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(fake.held(), 6);
    }

    #[test]
    fn failed_try_acquire_leaves_the_debt_for_the_next_one() {
        let (fake, spark) = five_bytes_owed();
        fake.fail_acquires(true);
        assert!(spark.try_acquire(1).is_err());
        assert_eq!(spark.claimed(), 0);
        fake.fail_acquires(false);
        assert_eq!(spark.try_acquire(1).unwrap(), Ok(()));
        assert_eq!(fake.requests(), vec![5, 6, 6]);
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(fake.held(), 6);
    }

    #[test]
    fn panicking_try_acquire_leaves_the_debt_for_the_next_one() {
        let (fake, spark) = five_bytes_owed();
        fake.during_next_acquire(|| panic!("injected Spark panic"));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spark.try_acquire(1)));
        assert!(panic.is_err());
        assert_eq!(spark.claimed(), 0);
        assert_eq!(spark.try_acquire(1).unwrap(), Ok(()));
        assert_eq!(fake.requests(), vec![5, 6, 6]);
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(fake.held(), 6);
    }

    /// A release shrinks the debt under a 5 byte claim to 1 and a zero grant adds 3, so the claim
    /// still covers all 4 bytes owed.
    #[test]
    fn try_acquire_claims_no_debt_an_earlier_claim_still_covers() {
        let (fake, spark) = five_bytes_owed();
        let (inner_fake, inner_spark) = (Arc::clone(&fake), Arc::clone(&spark));
        fake.during_next_acquire(move || {
            inner_spark.release(4).unwrap();
            assert_eq!(inner_spark.overcommit(), 1);
            assert_eq!(inner_spark.claimed(), 5);
            inner_fake.set_limit(0);
            inner_spark.acquire(3);
            inner_fake.set_limit(1_000);
            assert_eq!(inner_spark.overcommit(), 4);
            assert_eq!(inner_spark.try_acquire(1).unwrap(), Ok(()));
            assert_eq!(inner_spark.overcommit(), 4);
            assert_eq!(inner_spark.claimed(), 5);
        });
        assert_eq!(spark.try_acquire(1).unwrap(), Ok(()));

        assert_eq!(fake.requests(), vec![5, 6, 3, 1]);
        // Spark gets back the byte the release repaid.
        assert_eq!(fake.released(), vec![1]);
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(spark.claimed(), 0);
        // 5 + 1 + 3 + 1 recorded and 4 released.
        assert_eq!(fake.held() + spark.overcommit(), 6);
        spark.release(6).unwrap();
        assert_eq!(fake.held(), 0);
    }

    /// Two claims of 3 and 2 bytes are in flight when a release repays 4 of the 5 bytes owed.
    #[test]
    fn release_during_two_claims_hands_spark_back_only_the_excess() {
        let fake = FakeSpark::with(10);
        let spark = Arc::new(fake.memory());
        spark.acquire(13);
        assert_eq!(spark.overcommit(), 3);

        let (inner_fake, inner_spark) = (Arc::clone(&fake), Arc::clone(&spark));
        fake.during_next_acquire(move || {
            // 2 more bytes owed while the first claim waits on Spark.
            inner_fake.set_limit(13);
            inner_spark.acquire(5);
            assert_eq!(inner_spark.overcommit(), 5);
            inner_fake.set_limit(1_000);
            // The second claim waits on Spark when the release lands.
            let releaser = Arc::clone(&inner_spark);
            inner_fake.during_next_acquire(move || releaser.release(4).unwrap());
            assert_eq!(inner_spark.try_acquire(1).unwrap(), Ok(()));
        });
        assert_eq!(spark.try_acquire(1).unwrap(), Ok(()));

        assert_eq!(fake.requests(), vec![13, 4, 5, 3]);
        // The second settled first and repaid the byte still owed: 1 of its 2 bytes and all 3
        // of the first's go back.
        assert_eq!(fake.released(), vec![1, 3]);
        assert_eq!(spark.overcommit(), 0);
        assert_eq!(spark.claimed(), 0);
        // 13 + 5 + 1 + 1 recorded and 4 released.
        assert_eq!(fake.held(), 16);
        spark.release(16).unwrap();
        assert_eq!(fake.held(), 0);
    }

    /// FakeSpark panics if it is handed back more than it granted.
    #[test]
    fn concurrent_callers_keep_the_ledger_balanced() {
        use rand::{rngs::StdRng, RngExt, SeedableRng};
        use std::thread;

        let fake = FakeSpark::with(500);
        let spark = Arc::new(fake.memory());
        let threads: Vec<_> = (0..4)
            .map(|seed| {
                let (fake, spark) = (Arc::clone(&fake), Arc::clone(&spark));
                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(seed);
                    let mut recorded = 0;
                    for _ in 0..5_000 {
                        let size = rng.random_range(1..100);
                        match rng.random_range(0..4) {
                            0 => {
                                spark.acquire(size);
                                recorded += size;
                            }
                            1 => {
                                if spark.try_acquire(size).unwrap().is_ok() {
                                    recorded += size;
                                }
                            }
                            2 if recorded > 0 => {
                                let size = rng.random_range(1..=recorded);
                                spark.release(size).unwrap();
                                recorded -= size;
                            }
                            _ => fake.set_limit(rng.random_range(0..1_000)),
                        }
                    }
                    recorded
                })
            })
            .collect();
        let recorded: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();

        assert_eq!(spark.claimed(), 0);
        assert_eq!(fake.held() + spark.overcommit(), recorded);
        spark.release(recorded).unwrap();
        assert_eq!(fake.held(), 0);
        assert_eq!(spark.overcommit(), 0);
    }

    #[test]
    fn failed_acquire_is_all_overcommit() {
        let fake = FakeSpark::failing();
        let spark = fake.memory();
        spark.acquire(64);
        assert_eq!(spark.overcommit(), 64);
        spark.release(64).unwrap();
        assert!(fake.released().is_empty());
    }

    #[test]
    fn granted_is_clamped_to_the_request() {
        assert_eq!(granted(100, 40), 40);
        assert_eq!(granted(100, 150), 100);
        assert_eq!(granted(100, -1), 0);
    }

    /// Spark can block an acquire until another task releases memory, and Comet acquires on Tokio
    /// workers. The worker's other tasks have to keep running meanwhile, since one of them may be
    /// what would release the memory.
    #[test]
    fn a_blocked_acquire_leaves_its_worker_running_other_tasks() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let fake = FakeSpark::with(100);
        let spark = Arc::new(fake.memory());
        let (blocked_tx, blocked_rx) = std::sync::mpsc::channel();
        let (released_tx, released_rx) = std::sync::mpsc::channel::<()>();
        // Spark waits until a task that needs the only worker releases memory.
        fake.during_next_acquire(move || {
            blocked_tx.send(()).unwrap();
            released_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("the worker ran nothing else while the acquire blocked");
        });
        let acquiring = {
            let spark = Arc::clone(&spark);
            runtime.spawn(async move { spark.try_acquire(10) })
        };
        blocked_rx.recv().unwrap();
        runtime.spawn(async move { released_tx.send(()).unwrap() });

        assert_eq!(runtime.block_on(acquiring).unwrap().unwrap(), Ok(()));
        assert_eq!(fake.held(), 10);
    }
}
