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
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The per-task memory pools keyed by task attempt id.
pub(crate) static TASK_SHARED_MEMORY_POOLS: Lazy<Mutex<HashMap<i64, PerTaskMemoryPool>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(crate) struct PerTaskMemoryPool {
    pub(crate) memory_pool: Arc<dyn MemoryPool>,
    pub(crate) num_plans: usize,
}

impl PerTaskMemoryPool {
    pub(crate) fn new(memory_pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            memory_pool,
            num_plans: 0,
        }
    }
}

// This function reduces the refcount of a per-task memory pool when a native plan is released.
// If the refcount reaches zero, the memory pool is removed from the map and dropped.
pub(crate) fn handle_task_shared_poll_release(pool_type: MemoryPoolType, task_attempt_id: i64) {
    if !pool_type.is_task_shared() {
        return;
    }

    // Decrement the number of native plans using the per-task shared memory pool, and
    // remove the memory pool if the released native plan is the last native plan using it.
    let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock().unwrap();
    if let Some(per_task_memory_pool) = memory_pool_map.get_mut(&task_attempt_id) {
        per_task_memory_pool.num_plans -= 1;
        if per_task_memory_pool.num_plans == 0 {
            // Drop the memory pool from the per-task memory pool map if there are no
            // more native plans using it.
            memory_pool_map.remove(&task_attempt_id);
        }
    }
}
