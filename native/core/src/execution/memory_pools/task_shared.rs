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

/// Number of distinct task-attempt ids with a live task-shared memory pool, derived from
/// the registry so there is no separate counter to keep in sync. The real-usage fair-share
/// guard uses this as the divisor for each task's share of the budget; it returns 0 when no
/// task-shared pool is active, in which case the guard falls back to a fixed divisor.
#[cfg_attr(not(feature = "oom-guard"), allow(dead_code))]
pub(crate) fn active_task_count() -> usize {
    TASK_SHARED_MEMORY_POOLS.lock().len()
}

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
/// stays registered until the last reference to it drops.
pub(crate) fn acquire_task_shared_pool(
    task_attempt_id: i64,
    create: impl FnOnce() -> Arc<dyn MemoryPool>,
) -> Arc<dyn MemoryPool> {
    let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock();
    if let Some(memory_pool) = memory_pool_map
        .get(&task_attempt_id)
        .and_then(Weak::upgrade)
    {
        return memory_pool;
    }

    let memory_pool = Arc::new(TaskSharedMemoryPool {
        task_attempt_id,
        inner: create(),
    });
    memory_pool_map.insert(task_attempt_id, Arc::downgrade(&memory_pool));
    memory_pool
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;

    /// Tests share the process-wide pool map, so each uses its own task attempt id.
    fn acquire(task_attempt_id: i64) -> Arc<dyn MemoryPool> {
        acquire_task_shared_pool(task_attempt_id, || Arc::new(UnboundedMemoryPool::default()))
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
}
