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
pub(super) trait SparkMemoryManager {
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
/// The pool's own `used` still counts the full amount, so its next `try_grow` is refused.
pub(super) struct SparkMemory<M = JniMemoryManager> {
    manager: M,
    overcommit: AtomicUsize,
    task_attempt_id: i64,
}

impl SparkMemory {
    pub(super) fn new(handle: Arc<Global<JObject<'static>>>, task_attempt_id: i64) -> Self {
        Self::with_manager(JniMemoryManager(handle), task_attempt_id)
    }
}

impl<M: SparkMemoryManager> SparkMemory<M> {
    fn with_manager(manager: M, task_attempt_id: i64) -> Self {
        Self {
            manager,
            overcommit: AtomicUsize::new(0),
            task_attempt_id,
        }
    }

    /// Acquires `size` bytes, or none: a partial grant is handed back and reported as `Err` with
    /// the number of bytes Spark offered.
    pub(super) fn try_acquire(&self, size: usize) -> CometResult<Result<(), usize>> {
        let granted = granted(size, self.manager.acquire(size)?);
        if granted < size {
            self.manager.release(granted)?;
            return Ok(Err(granted));
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
        let mut to_release = size;
        if self.overcommit.load(Relaxed) > 0 {
            let prev = self
                .overcommit
                .fetch_update(Relaxed, Relaxed, |debt| Some(debt.saturating_sub(size)))
                .unwrap();
            to_release = size.saturating_sub(prev);
        }
        if to_release > 0 {
            self.manager.release(to_release)?;
        }
        Ok(())
    }

    pub(super) fn overcommit(&self) -> usize {
        self.overcommit.load(Relaxed)
    }
}

/// Clamps Spark's reply to an acquire: it never grants more than asked, and never a negative.
fn granted(requested: usize, acquired: i64) -> usize {
    usize::try_from(acquired).unwrap_or(0).min(requested)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CometError;
    use std::sync::Mutex;

    /// Grants at most `available` bytes and records every release.
    #[derive(Default)]
    struct FakeSpark {
        available: Mutex<i64>,
        released: Mutex<Vec<usize>>,
        fail: bool,
    }

    impl FakeSpark {
        fn with(available: i64) -> Self {
            Self {
                available: Mutex::new(available),
                ..Default::default()
            }
        }
    }

    impl SparkMemoryManager for FakeSpark {
        fn acquire(&self, size: usize) -> CometResult<i64> {
            if self.fail {
                return Err(CometError::Internal("jni".to_string()));
            }
            let mut available = self.available.lock().unwrap();
            let granted = (size as i64).min(*available);
            *available -= granted;
            Ok(granted)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            *self.available.lock().unwrap() += size as i64;
            self.released.lock().unwrap().push(size);
            Ok(())
        }
    }

    #[test]
    fn try_acquire_hands_back_a_partial_grant() {
        let spark = SparkMemory::with_manager(FakeSpark::with(40), 0);
        assert_eq!(spark.try_acquire(100).unwrap(), Err(40));
        assert_eq!(*spark.manager.released.lock().unwrap(), vec![40]);
        assert_eq!(spark.try_acquire(40).unwrap(), Ok(()));
        assert_eq!(spark.overcommit(), 0);
    }

    #[test]
    fn release_repays_overcommit_before_returning_bytes_to_spark() {
        let spark = SparkMemory::with_manager(FakeSpark::with(40), 0);
        spark.acquire(100);
        assert_eq!(spark.overcommit(), 60);
        spark.release(30).unwrap();
        assert_eq!(spark.overcommit(), 30);
        assert!(spark.manager.released.lock().unwrap().is_empty());
        spark.release(70).unwrap();
        assert_eq!(spark.overcommit(), 0);
        // Spark gets back exactly the 40 bytes it granted.
        assert_eq!(*spark.manager.released.lock().unwrap(), vec![40]);
    }

    #[test]
    fn failed_acquire_is_all_overcommit() {
        let spark = SparkMemory::with_manager(
            FakeSpark {
                fail: true,
                ..Default::default()
            },
            0,
        );
        spark.acquire(64);
        assert_eq!(spark.overcommit(), 64);
        spark.release(64).unwrap();
        assert!(spark.manager.released.lock().unwrap().is_empty());
    }

    #[test]
    fn granted_is_clamped_to_the_request() {
        assert_eq!(granted(100, 40), 40);
        assert_eq!(granted(100, 150), 100);
        assert_eq!(granted(100, -1), 0);
    }
}
