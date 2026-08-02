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

mod config;
mod fair_pool;
pub mod logging_pool;
mod task_shared;
mod unified_pool;

use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use fair_pool::CometFairMemoryPool;
use jni::objects::{Global, JObject};
use once_cell::sync::OnceCell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use unified_pool::CometUnifiedMemoryPool;

pub(crate) use config::*;
pub(crate) use task_shared::*;

/// Creates the memory pool for a native plan.
///
/// For task-shared pool types the returned [`TaskSharedPoolRef`] must be kept alive for as long as
/// the plan uses the pool; dropping it releases the plan's reference. It is `None` for pool types
/// that are not task-shared.
pub(crate) fn create_memory_pool(
    memory_pool_config: &MemoryPoolConfig,
    comet_task_memory_manager: Arc<Global<JObject<'static>>>,
    task_attempt_id: i64,
) -> (Arc<dyn MemoryPool>, Option<TaskSharedPoolRef>) {
    const NUM_TRACKED_CONSUMERS: usize = 10;

    fn tracked(pool: impl MemoryPool + 'static) -> Arc<dyn MemoryPool> {
        Arc::new(TrackConsumersPool::new(
            pool,
            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
        ))
    }

    let pool_type = memory_pool_config.pool_type;
    let pool_size = memory_pool_config.pool_size;

    match pool_type {
        MemoryPoolType::GreedyUnified => {
            let (pool, pool_ref) = acquire_task_shared_pool(pool_type, task_attempt_id, || {
                tracked(CometUnifiedMemoryPool::new(
                    comet_task_memory_manager,
                    task_attempt_id,
                ))
            });
            (pool, Some(pool_ref))
        }
        MemoryPoolType::FairUnified => {
            let (pool, pool_ref) = acquire_task_shared_pool(pool_type, task_attempt_id, || {
                tracked(CometFairMemoryPool::new(
                    comet_task_memory_manager,
                    pool_size,
                ))
            });
            (pool, Some(pool_ref))
        }
        MemoryPoolType::GreedyTaskShared => {
            let (pool, pool_ref) = acquire_task_shared_pool(pool_type, task_attempt_id, || {
                tracked(GreedyMemoryPool::new(pool_size))
            });
            (pool, Some(pool_ref))
        }
        MemoryPoolType::FairSpillTaskShared => {
            let (pool, pool_ref) = acquire_task_shared_pool(pool_type, task_attempt_id, || {
                tracked(FairSpillPool::new(pool_size))
            });
            (pool, Some(pool_ref))
        }
        MemoryPoolType::Greedy => (tracked(GreedyMemoryPool::new(pool_size)), None),
        MemoryPoolType::FairSpill => (tracked(FairSpillPool::new(pool_size)), None),
        MemoryPoolType::GreedyGlobal => {
            static GLOBAL_MEMORY_POOL_GREEDY: OnceCell<Arc<dyn MemoryPool>> = OnceCell::new();
            let memory_pool =
                GLOBAL_MEMORY_POOL_GREEDY.get_or_init(|| tracked(GreedyMemoryPool::new(pool_size)));
            (Arc::clone(memory_pool), None)
        }
        MemoryPoolType::FairSpillGlobal => {
            static GLOBAL_MEMORY_POOL_FAIR: OnceCell<Arc<dyn MemoryPool>> = OnceCell::new();
            let memory_pool =
                GLOBAL_MEMORY_POOL_FAIR.get_or_init(|| tracked(FairSpillPool::new(pool_size)));
            (Arc::clone(memory_pool), None)
        }
        MemoryPoolType::Unbounded => (Arc::new(UnboundedMemoryPool::default()), None),
    }
}
