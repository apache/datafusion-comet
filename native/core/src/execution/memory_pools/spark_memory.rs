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

use std::sync::{
    atomic::{AtomicUsize, Ordering::Relaxed},
    Arc,
};

use jni::objects::{Global, JObject};
use log::warn;

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
/// While any is outstanding, [`Self::try_acquire`] also asks Spark for it, so a pool refuses
/// `try_grow` until Spark can cover both the request and the debt, and the operator spills.
///
/// # Invariant
///
/// Every byte a pool records through this type is backed either by Spark's grant or by
/// overcommit, so Spark's grant plus `overcommit` equals the bytes recorded and not yet released.
/// Spark is handed back more than it granted only if a release takes less from `overcommit` than
/// it could. `CometUnifiedMemoryPool` calls in here from several threads without a lock, and
/// updates its own `used` separately, so this rests on three things:
///
/// - `overcommit` only grows by bytes that are being recorded in the same call.
/// - Each repayment takes its share of `overcommit` in a single atomic update, so two concurrent
///   calls can never repay the same debt.
/// - A caller never releases more than it recorded. DataFusion guarantees this, because a shrink
///   cannot exceed the reservation it comes from.
pub(super) struct SparkMemory {
    manager: Box<dyn SparkMemoryManager>,
    overcommit: AtomicUsize,
    task_attempt_id: i64,
}

/// Why [`SparkMemory::try_acquire`] refused a request.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Refusal {
    /// Outstanding overcommit that was asked for on top of the request.
    pub(super) overcommit: usize,
    /// What Spark offered before it was handed back.
    pub(super) granted: usize,
}

impl SparkMemory {
    pub(super) fn new(handle: Arc<Global<JObject<'static>>>, task_attempt_id: i64) -> Self {
        Self::with_manager(Box::new(JniMemoryManager(handle)), task_attempt_id)
    }

    pub(super) fn with_manager(manager: Box<dyn SparkMemoryManager>, task_attempt_id: i64) -> Self {
        Self {
            manager,
            overcommit: AtomicUsize::new(0),
            task_attempt_id,
        }
    }

    pub(super) fn task_attempt_id(&self) -> i64 {
        self.task_attempt_id
    }

    /// Acquires `size` bytes plus any outstanding overcommit, or nothing. A full grant repays the
    /// overcommit; a partial one is handed back and reported as a [`Refusal`].
    pub(super) fn try_acquire(&self, size: usize) -> CometResult<Result<(), Refusal>> {
        let debt = self.overcommit.load(Relaxed);
        let request = size.saturating_add(debt);
        let granted = granted(request, self.manager.acquire(request)?);
        if granted < request {
            if granted > 0 {
                self.manager.release(granted)?;
            }
            return Ok(Err(Refusal {
                overcommit: debt,
                granted,
            }));
        }
        if debt > 0 {
            // A concurrent release may have repaid part of the debt since it was read, in which
            // case Spark granted more than is still owed and the excess goes back.
            let owed = self.repay(debt);
            if owed < debt {
                self.manager.release(debt - owed)?;
            }
        }
        Ok(Ok(()))
    }

    /// Acquires what Spark will grant toward `size` bytes and carries the rest as overcommit.
    /// Never fails; a failed call to Spark counts as a zero grant.
    pub(super) fn acquire(&self, size: usize) {
        let granted = match self.manager.acquire(size) {
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
            self.overcommit.fetch_add(size - granted, Relaxed);
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
        self.overcommit.load(Relaxed)
    }

    /// Takes up to `size` bytes off the overcommit in one atomic step and returns how many.
    fn repay(&self, size: usize) -> usize {
        let debt = self
            .overcommit
            .fetch_update(Relaxed, Relaxed, |debt| Some(debt.saturating_sub(size)))
            .unwrap();
        debt.min(size)
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

    /// Grants up to `limit` bytes in total and records every release. Panics if it is handed
    /// back more than it granted, which is the guarantee the pools must keep.
    #[derive(Default)]
    pub(in crate::execution::memory_pools) struct FakeSpark {
        limit: Mutex<usize>,
        held: Mutex<usize>,
        released: Mutex<Vec<usize>>,
        fail: bool,
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
                fail: true,
                ..Default::default()
            })
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

        pub(in crate::execution::memory_pools) fn memory(self: &Arc<Self>) -> SparkMemory {
            SparkMemory::with_manager(Box::new(Arc::clone(self)), 0)
        }
    }

    impl SparkMemoryManager for Arc<FakeSpark> {
        fn acquire(&self, size: usize) -> CometResult<i64> {
            // Run before taking any other lock, so `f` can call back into this fake.
            let during = self.during_acquire.lock().take();
            if let Some(f) = during {
                f();
            }
            if self.fail {
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
}
