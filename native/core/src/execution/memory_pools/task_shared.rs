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

use crate::execution::memory_pools::MemoryPoolType;
use datafusion::execution::memory_pool::MemoryPool;
use log::warn;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// The per-task memory pools keyed by task attempt id.
static TASK_SHARED_MEMORY_POOLS: Lazy<Mutex<HashMap<i64, PerTaskMemoryPool>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Locks the pool map, recovering from poisoning rather than propagating it.
///
/// A panic while the map is locked would otherwise poison the mutex permanently, and because
/// every `createPlan` and plan release goes through this map that would take down all subsequent
/// native execution in the executor. The map holds only a refcount and an `Arc`, so a poisoned
/// state is still safe to observe.
fn lock_pools() -> MutexGuard<'static, HashMap<i64, PerTaskMemoryPool>> {
    TASK_SHARED_MEMORY_POOLS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

struct PerTaskMemoryPool {
    memory_pool: Arc<dyn MemoryPool>,
    num_plans: usize,
}

/// One native plan's reference to a task-shared memory pool.
///
/// Dropping this releases the reference, removing the pool from the map once the last plan using
/// it is gone. It is stored on the `ExecutionContext` so the reference is released whenever that
/// context is freed -- both when `releasePlan` frees it normally and when `createPlan` fails
/// partway through and unwinds.
///
/// Tying the release to a guard rather than an explicit call at plan-release time matters because
/// a stranded map entry is never reclaimed: keys are unique task attempt ids and nothing prunes
/// the map, and each entry holds a JNI global ref to the task's `CometTaskMemoryManager`, which
/// transitively pins its `TaskMemoryManager` and `TaskContext`.
pub(crate) struct TaskSharedPoolRef {
    pool_type: MemoryPoolType,
    task_attempt_id: i64,
}

impl Drop for TaskSharedPoolRef {
    fn drop(&mut self) {
        let mut memory_pool_map = lock_pools();
        match memory_pool_map.get_mut(&self.task_attempt_id) {
            Some(per_task_memory_pool) => {
                // `saturating_sub` rather than `-=`: the refcount is balanced by construction
                // (one increment per `TaskSharedPoolRef`, one decrement on drop), but underflow
                // here would panic while holding the map lock.
                per_task_memory_pool.num_plans = per_task_memory_pool.num_plans.saturating_sub(1);
                if per_task_memory_pool.num_plans == 0 {
                    // Last plan using this pool, so drop it from the map. This releases the
                    // map's `Arc`; the pool itself is freed once the owning session context has
                    // also dropped its clone.
                    memory_pool_map.remove(&self.task_attempt_id);
                }
            }
            None => warn!(
                "Task {} released a {:?} memory pool reference but no pool was registered",
                self.task_attempt_id, self.pool_type
            ),
        }
    }
}

/// Returns the memory pool shared by every native plan in `task_attempt_id`, creating it with
/// `create` if this is the first plan in the task, along with a reference that releases it on
/// drop.
///
/// `create` is only called when no pool exists for the task yet, so the pool size and type come
/// from the first plan in the task; later plans reuse that pool.
pub(crate) fn acquire_task_shared_pool(
    pool_type: MemoryPoolType,
    task_attempt_id: i64,
    create: impl FnOnce() -> Arc<dyn MemoryPool>,
) -> (Arc<dyn MemoryPool>, TaskSharedPoolRef) {
    debug_assert!(pool_type.is_task_shared());

    let mut memory_pool_map = lock_pools();
    let per_task_memory_pool =
        memory_pool_map
            .entry(task_attempt_id)
            .or_insert_with(|| PerTaskMemoryPool {
                memory_pool: create(),
                num_plans: 0,
            });
    per_task_memory_pool.num_plans += 1;

    (
        Arc::clone(&per_task_memory_pool.memory_pool),
        TaskSharedPoolRef {
            pool_type,
            task_attempt_id,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;

    /// Tests share the process-wide pool map, so each uses its own task attempt id.
    fn acquire(task_attempt_id: i64) -> (Arc<dyn MemoryPool>, TaskSharedPoolRef) {
        acquire_task_shared_pool(MemoryPoolType::GreedyTaskShared, task_attempt_id, || {
            Arc::new(UnboundedMemoryPool::default())
        })
    }

    fn is_registered(task_attempt_id: i64) -> bool {
        lock_pools().contains_key(&task_attempt_id)
    }

    #[test]
    fn plans_in_the_same_task_share_one_pool() {
        let (first, _first_ref) = acquire(-1001);
        let (second, _second_ref) = acquire(-1001);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(lock_pools()[&-1001].num_plans, 2);
    }

    #[test]
    fn plans_in_different_tasks_get_different_pools() {
        let (first, _first_ref) = acquire(-1002);
        let (second, _second_ref) = acquire(-1003);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn pool_is_removed_only_once_the_last_plan_releases_it() {
        let (_pool, first_ref) = acquire(-1004);
        let (_pool, second_ref) = acquire(-1004);

        drop(first_ref);
        assert!(
            is_registered(-1004),
            "pool must outlive the first plan to release it"
        );

        drop(second_ref);
        assert!(!is_registered(-1004));
    }

    #[test]
    fn dropping_the_reference_releases_the_pool() {
        // Stands in for `createPlan` failing after the pool was acquired: nothing calls a release
        // path, the `ExecutionContext` is simply never built, and the guard drops on unwind.
        {
            let (_pool, _pool_ref) = acquire(-1005);
            assert!(is_registered(-1005));
        }
        assert!(!is_registered(-1005));
    }

    #[test]
    fn releasing_an_unregistered_pool_does_not_panic() {
        let (_pool, pool_ref) = acquire(-1006);
        lock_pools().remove(&-1006);
        drop(pool_ref);
    }
}
