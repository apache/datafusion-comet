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

use crate::errors::CometResult;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};

/// The memory pools for active task attempts. Weak references let the pool's normal `Arc`
/// ownership determine its lifetime, and each pool removes its entry when the last reference drops.
static TASK_SHARED_MEMORY_POOLS: Lazy<Mutex<HashMap<i64, Weak<TaskSharedMemoryPool>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// A transparent `MemoryPool` wrapper whose lifetime also controls its registry entry.
#[derive(Debug)]
struct TaskSharedMemoryPool {
    task_attempt_id: i64,
    inner: Arc<dyn MemoryPool>,
}

impl fmt::Display for TaskSharedMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.inner.as_ref(), f)
    }
}

impl MemoryPool for TaskSharedMemoryPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

impl Drop for TaskSharedMemoryPool {
    fn drop(&mut self) {
        if let Entry::Occupied(entry) = TASK_SHARED_MEMORY_POOLS.lock().entry(self.task_attempt_id)
        {
            // An acquire racing with this drop can replace our expired `Weak` before we obtain the
            // lock. Do not let the old pool remove that replacement's entry.
            if std::ptr::eq(entry.get().as_ptr(), self) {
                entry.remove();
            }
        }
    }
}

/// Returns the memory pool shared by every native plan in `task_attempt_id`, creating it with
/// `create` if no live pool exists for the task. The returned `Arc` is the RAII handle: the pool
/// stays registered until the last reference to it drops. `create` runs without the registry
/// lock, since it can block inside Spark; concurrent creates for one task keep the first to land.
pub(crate) fn acquire_task_shared_pool(
    task_attempt_id: i64,
    create: impl FnOnce() -> CometResult<Arc<dyn MemoryPool>>,
) -> CometResult<Arc<dyn MemoryPool>> {
    if let Some(memory_pool) = lookup(task_attempt_id) {
        return Ok(memory_pool);
    }

    let memory_pool = Arc::new(TaskSharedMemoryPool {
        task_attempt_id,
        inner: create()?,
    });
    let existing = {
        let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock();
        match memory_pool_map
            .get(&task_attempt_id)
            .and_then(Weak::upgrade)
        {
            Some(existing) => Some(existing),
            None => {
                memory_pool_map.insert(task_attempt_id, Arc::downgrade(&memory_pool));
                None
            }
        }
    };
    // The registry lock is gone by now, so a losing pool dropped here releases its Spark-side
    // memory without holding it.
    Ok(existing.unwrap_or(memory_pool))
}

fn lookup(task_attempt_id: i64) -> Option<Arc<dyn MemoryPool>> {
    TASK_SHARED_MEMORY_POOLS
        .lock()
        .get(&task_attempt_id)
        .and_then(Weak::upgrade)
        .map(|memory_pool| memory_pool as Arc<dyn MemoryPool>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;

    /// Tests share the process-wide pool map, so each uses its own task attempt id.
    fn acquire(task_attempt_id: i64) -> Arc<dyn MemoryPool> {
        acquire_task_shared_pool(task_attempt_id, || {
            Ok(Arc::new(UnboundedMemoryPool::default()))
        })
        .unwrap()
    }

    fn is_registered(task_attempt_id: i64) -> bool {
        TASK_SHARED_MEMORY_POOLS
            .lock()
            .contains_key(&task_attempt_id)
    }

    #[test]
    fn plans_in_the_same_task_share_one_pool() {
        let first = acquire(-1001);
        let second = acquire(-1001);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn plans_in_different_tasks_get_different_pools() {
        let first = acquire(-1002);
        let second = acquire(-1003);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn pool_is_removed_only_after_the_last_reference_drops() {
        let first = acquire(-1004);
        let second = acquire(-1004);

        drop(first);
        assert!(
            is_registered(-1004),
            "pool must outlive the first reference to release it"
        );

        drop(second);
        assert!(!is_registered(-1004));
    }

    #[test]
    fn dropping_the_reference_releases_the_pool() {
        // Stands in for `createPlan` failing after the pool was acquired. The ordinary `Arc` drops
        // on unwind, so no explicit release path is needed.
        {
            let _pool = acquire(-1005);
            assert!(is_registered(-1005));
        }
        assert!(!is_registered(-1005));
    }

    #[test]
    fn an_old_pool_does_not_remove_its_replacement() {
        let old_pool = acquire(-1006);
        TASK_SHARED_MEMORY_POOLS.lock().remove(&-1006);
        let replacement = acquire(-1006);

        drop(old_pool);
        assert!(is_registered(-1006));

        drop(replacement);
        assert!(!is_registered(-1006));
    }

    /// Exercises the drop/acquire race for real: an acquire can replace an expired `Weak` between
    /// another thread's last `Arc` drop and that drop obtaining the registry lock, and the old
    /// pool's `Drop` must not evict the replacement's entry.
    #[test]
    fn concurrent_acquire_and_drop_leaves_a_consistent_registry() {
        use std::thread;

        let threads: Vec<_> = (0..8)
            .map(|_| {
                thread::spawn(|| {
                    for _ in 0..1_000 {
                        drop(acquire(-1007));
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        assert!(
            !is_registered(-1007),
            "registry entry survived after every reference was dropped"
        );

        // The registry must still work for the task after the churn.
        let _pool = acquire(-1007);
        assert!(is_registered(-1007));
    }

    /// `create` can block inside Spark while it takes the pool's anchor, so it must run without
    /// the registry lock, or every other task's plan creation on the executor waits behind it.
    #[test]
    fn create_runs_without_the_registry_lock_held() {
        use std::sync::mpsc::channel;
        use std::thread;
        use std::time::Duration;

        let pool = acquire_task_shared_pool(-1008, || {
            // A helper thread probes the registry; it can only answer if `create` does not hold
            // the lock. Other tests hold it briefly, so the probe waits generously.
            let (tx, rx) = channel();
            thread::spawn(move || {
                let _ = tx.send(is_registered(-1008));
            });
            let registered = rx
                .recv_timeout(Duration::from_secs(10))
                .expect("create ran under the registry lock");
            assert!(!registered, "pool was registered before create finished");
            Ok(Arc::new(UnboundedMemoryPool::default()))
        })
        .unwrap();
        drop(pool);
        assert!(!is_registered(-1008));
    }

    /// A failed `create` registers nothing, so the next plan of the task tries again.
    #[test]
    fn failed_create_registers_nothing() {
        let result = acquire_task_shared_pool(-1009, || {
            Err(crate::errors::CometError::Internal("declined".to_string()))
        });
        assert!(result.is_err());
        assert!(!is_registered(-1009));

        let _pool = acquire(-1009);
        assert!(is_registered(-1009));
    }

    /// Two plans of one task that both miss the registry create two pools; both must end up
    /// sharing the one that registered first, and only one pool survives.
    #[test]
    fn concurrent_creates_for_one_task_share_a_single_pool() {
        use std::sync::Barrier;
        use std::thread;

        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    acquire_task_shared_pool(-1010, || {
                        // Both creates run at once, so neither can see the other's registration.
                        barrier.wait();
                        Ok(Arc::new(UnboundedMemoryPool::default()) as Arc<dyn MemoryPool>)
                    })
                    .unwrap()
                })
            })
            .collect();
        let pools: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();

        assert!(Arc::ptr_eq(&pools[0], &pools[1]));
        assert_eq!(
            Arc::strong_count(&pools[0]),
            2,
            "the losing pool must be dropped"
        );
        drop(pools);
        assert!(!is_registered(-1010));
    }
}
