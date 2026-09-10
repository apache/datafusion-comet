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
    sync::Arc,
};

use jni::objects::{Global, JObject};

use crate::{errors::CometResult, jvm_bridge::JVMClasses};
use datafusion::common::resources_err;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::{
    common::DataFusionError,
    execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation},
};
use parking_lot::Mutex;

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometFairMemoryPool {
    task_memory_manager_handle: Arc<Global<JObject<'static>>>,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
    #[cfg(test)]
    test_memory_manager: Option<Arc<tests::TestMemoryManager>>,
}

#[derive(Default)]
struct CometFairPoolState {
    used: usize,
    // new_empty(), split(), and take() share a registered consumer but have independent sizes.
    // All sibling reservations must count against the same fair share.
    consumer_usage: HashMap<usize, usize>,
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("used", &state.used)
            .field("num", &state.consumer_usage.len())
            .finish()
    }
}

impl CometFairMemoryPool {
    pub fn new(
        task_memory_manager_handle: Arc<Global<JObject<'static>>>,
        pool_size: usize,
    ) -> CometFairMemoryPool {
        Self {
            task_memory_manager_handle,
            pool_size,
            state: Mutex::new(CometFairPoolState::default()),
            #[cfg(test)]
            test_memory_manager: None,
        }
    }

    fn acquire(&self, additional: usize) -> CometResult<i64> {
        #[cfg(test)]
        if let Some(manager) = &self.test_memory_manager {
            return manager.acquire(additional);
        }
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env,
              comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)
        })
    }

    fn release(&self, size: usize) -> CometResult<()> {
        #[cfg(test)]
        if let Some(manager) = &self.test_memory_manager {
            manager.release(size);
            return Ok(());
        }
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        })
    }
}

impl Display for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        write!(
            f,
            "CometFairMemoryPool(pool_size={}, used={}, num={})",
            self.pool_size,
            state.used,
            state.consumer_usage.len()
        )
    }
}

unsafe impl Send for CometFairMemoryPool {}
unsafe impl Sync for CometFairMemoryPool {}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        assert!(
            self.state
                .lock()
                .consumer_usage
                .insert(consumer.id(), 0)
                .is_none(),
            "memory consumer was registered more than once"
        );
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let usage = self.state.lock().consumer_usage.remove(&consumer.id());
        assert_eq!(
            usage,
            Some(0),
            "consumer must release its reservations before unregistering"
        );
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional).unwrap();
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let mut state = self.state.lock();
            let usage = state.consumer_usage[&reservation.consumer().id()];
            assert!(
                usage >= subtractive,
                "consumer released more bytes than it reserved"
            );
            self.release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            *state
                .consumer_usage
                .get_mut(&reservation.consumer().id())
                .unwrap() -= subtractive;
            state.used -= subtractive;
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            let mut state = self.state.lock();
            // Preserve the policy of sharing among all registered consumers. Spillability
            // annotations and sharing only among spillable consumers are a separate change.
            let num = state.consumer_usage.len();
            let limit = self.pool_size / num;
            let used = state.consumer_usage[&reservation.consumer().id()];
            if used
                .checked_add(additional)
                .is_none_or(|requested| requested > limit)
            {
                return resources_err!(
                    "Failed to acquire {additional} bytes where {used} bytes already reserved by this consumer and the fair limit is {limit} bytes, {num} registered"
                );
            }
            // Existing allocations may exceed their new fair share when another consumer
            // registers. A per-consumer bound alone cannot enforce the configured pool size.
            if additional > self.pool_size.saturating_sub(state.used) {
                return resources_err!(
                    "Failed to acquire {additional} bytes where {} bytes already reserved pool-wide and the pool limit is {} bytes",
                    state.used, self.pool_size
                );
            }

            let acquired = self.acquire(additional)?;
            // If the number of bytes we acquired is less than the requested, return an error,
            // and hopefully will trigger spilling from the caller side.
            if acquired < additional as i64 {
                // Release the acquired bytes before throwing error
                self.release(acquired as usize)?;

                return resources_err!(
                    "Failed to acquire {} bytes, only got {} bytes. Reserved: {} bytes",
                    additional,
                    acquired,
                    state.used
                );
            }
            *state
                .consumer_usage
                .get_mut(&reservation.consumer().id())
                .unwrap() += additional;
            state.used = state
                .used
                .checked_add(additional)
                .expect("overflow in checked_add");
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    // Replace only JNI grants/releases so the tests exercise actual MemoryPool admission and
    // DataFusion reservation lifetimes without requiring a running Spark task.
    #[derive(Default)]
    pub(super) struct TestMemoryManager {
        used: AtomicUsize,
        acquires: AtomicUsize,
        partial_grant: AtomicBool,
        fail_acquire: AtomicBool,
    }

    impl TestMemoryManager {
        pub(super) fn acquire(&self, requested: usize) -> CometResult<i64> {
            self.acquires.fetch_add(1, SeqCst);
            if self.fail_acquire.load(SeqCst) {
                return Err(crate::errors::CometError::Internal(
                    "test acquire failure".into(),
                ));
            }
            let granted = if self.partial_grant.load(SeqCst) {
                requested / 2
            } else {
                requested
            };
            self.used.fetch_add(granted, SeqCst);
            Ok(granted as i64)
        }

        pub(super) fn release(&self, bytes: usize) {
            self.used
                .fetch_update(SeqCst, SeqCst, |used| used.checked_sub(bytes))
                .unwrap();
        }
    }

    fn pool(size: usize) -> (Arc<dyn MemoryPool>, Arc<TestMemoryManager>) {
        let manager = Arc::new(TestMemoryManager::default());
        let mut pool = CometFairMemoryPool::new(Arc::new(Global::null()), size);
        pool.test_memory_manager = Some(Arc::clone(&manager));
        (Arc::new(pool), manager)
    }

    #[test]
    fn each_consumer_can_use_its_fair_share() {
        let (pool, manager) = pool(32);
        let other = MemoryConsumer::new("other").register(&pool);
        let requesting = MemoryConsumer::new("requesting").register(&pool);
        other.try_grow(10).unwrap();
        requesting.try_grow(6).unwrap();
        requesting.try_grow(10).unwrap();
        assert!(requesting.try_grow(1).is_err());
        other.try_grow(6).unwrap();
        assert_eq!(pool.reserved(), 32);
        assert_eq!(manager.used.load(SeqCst), 32);
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(32)));
    }

    #[test]
    fn sibling_reservations_share_their_consumers_limit() {
        let (pool, manager) = pool(100);
        let parent = MemoryConsumer::new("same name")
            .with_can_spill(true)
            .register(&pool);
        let first = parent.new_empty();
        let second = parent.new_empty();
        let other = MemoryConsumer::new("same name")
            .with_can_spill(true)
            .register(&pool);
        first.try_grow(30).unwrap();
        second.try_grow(20).unwrap();
        assert!(parent.try_grow(1).is_err());
        assert!(second.try_grow(1).is_err());
        // Rejected growth never asks Spark for memory, even though the pool has free capacity.
        assert_eq!(manager.acquires.load(SeqCst), 2);
        other.try_grow(50).unwrap();
        drop(parent);
        drop(first);
        second.try_grow(30).unwrap();
        assert_eq!(pool.reserved(), 100);
        drop(second);
        drop(other);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(manager.used.load(SeqCst), 0);
    }

    #[test]
    fn split_and_take_do_not_create_another_allowance() {
        let (pool, manager) = pool(100);
        let mut parent = MemoryConsumer::new("consumer").register(&pool);
        let other = MemoryConsumer::new("other").register(&pool);
        parent.try_grow(50).unwrap();
        let split = parent.split(20);
        let taken = parent.take();
        assert_eq!(parent.size(), 0);
        assert_eq!(split.size(), 20);
        assert_eq!(taken.size(), 30);
        for reservation in [&parent, &split, &taken] {
            assert!(reservation.try_grow(1).is_err());
        }
        assert_eq!(manager.acquires.load(SeqCst), 1);
        split.shrink(10);
        parent.try_grow(10).unwrap();
        drop(parent);
        drop(split);
        taken.try_grow(20).unwrap();
        other.try_grow(50).unwrap();
        assert_eq!(pool.reserved(), 100);
        drop(taken);
        drop(other);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(manager.used.load(SeqCst), 0);
    }

    #[test]
    fn registration_after_allocation_cannot_exceed_pool_capacity() {
        let (pool, manager) = pool(100);
        let first = MemoryConsumer::new("first").register(&pool);
        first.try_grow(100).unwrap();
        let second = MemoryConsumer::new("second").register(&pool);
        assert!(second.try_grow(1).is_err());
        assert_eq!(manager.acquires.load(SeqCst), 1);
        assert_eq!(pool.reserved(), 100);
        first.shrink(50);
        second.try_grow(50).unwrap();
        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn mixed_consumers_keep_the_existing_sharing_policy() {
        let (pool, _) = pool(100);
        let fixed = MemoryConsumer::new("fixed").register(&pool);
        let spilling = MemoryConsumer::new("spilling")
            .with_can_spill(true)
            .register(&pool);
        assert!(spilling.try_grow(51).is_err());
        spilling.try_grow(50).unwrap();
        fixed.try_grow(50).unwrap();
        assert_eq!(pool.reserved(), 100);
        fixed.free();
        assert!(spilling.try_grow(1).is_err());
        drop(fixed);
        spilling.try_grow(50).unwrap();
        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn failed_acquisition_does_not_change_consumer_or_pool_usage() {
        let (pool, manager) = pool(100);
        let reservation = MemoryConsumer::new("consumer").register(&pool);
        manager.partial_grant.store(true, SeqCst);
        assert!(reservation.try_grow(100).is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(manager.used.load(SeqCst), 0);
        manager.partial_grant.store(false, SeqCst);
        manager.fail_acquire.store(true, SeqCst);
        assert!(reservation.try_grow(100).is_err());
        assert_eq!(pool.reserved(), 0);
        manager.fail_acquire.store(false, SeqCst);
        reservation.try_grow(100).unwrap();
        reservation.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(manager.used.load(SeqCst), 0);
    }

    #[test]
    fn overflowing_request_is_rejected_before_acquisition() {
        let (pool, manager) = pool(100);
        let reservation = MemoryConsumer::new("consumer").register(&pool);
        reservation.try_grow(1).unwrap();
        assert!(reservation.try_grow(usize::MAX).is_err());
        assert_eq!(pool.reserved(), 1);
        assert_eq!(manager.acquires.load(SeqCst), 1);
    }
}
