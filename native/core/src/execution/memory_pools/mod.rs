mod config;
mod fair_pool;
mod task_shared;
mod unified_pool;

use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use fair_pool::CometFairMemoryPool;
use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use unified_pool::CometMemoryPool;

pub(crate) use config::*;
pub(crate) use task_shared::*;

pub(crate) fn create_memory_pool(
    memory_pool_config: &MemoryPoolConfig,
    comet_task_memory_manager: Arc<GlobalRef>,
    task_attempt_id: i64,
) -> Arc<dyn MemoryPool> {
    const NUM_TRACKED_CONSUMERS: usize = 10;
    match memory_pool_config.pool_type {
        MemoryPoolType::Unified => {
            // Set Comet memory pool for native
            let memory_pool = CometMemoryPool::new(comet_task_memory_manager);
            Arc::new(TrackConsumersPool::new(
                memory_pool,
                NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
            ))
        }
        MemoryPoolType::FairUnified => {
            // Set Comet fair memory pool for native
            let memory_pool =
                CometFairMemoryPool::new(comet_task_memory_manager, memory_pool_config.pool_size);
            Arc::new(TrackConsumersPool::new(
                memory_pool,
                NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
            ))
        }
        MemoryPoolType::Greedy => Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_config.pool_size),
            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
        )),
        MemoryPoolType::FairSpill => Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(memory_pool_config.pool_size),
            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
        )),
        MemoryPoolType::GreedyGlobal => {
            static GLOBAL_MEMORY_POOL_GREEDY: OnceCell<Arc<dyn MemoryPool>> = OnceCell::new();
            let memory_pool = GLOBAL_MEMORY_POOL_GREEDY.get_or_init(|| {
                Arc::new(TrackConsumersPool::new(
                    GreedyMemoryPool::new(memory_pool_config.pool_size),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                ))
            });
            Arc::clone(memory_pool)
        }
        MemoryPoolType::FairSpillGlobal => {
            static GLOBAL_MEMORY_POOL_FAIR: OnceCell<Arc<dyn MemoryPool>> = OnceCell::new();
            let memory_pool = GLOBAL_MEMORY_POOL_FAIR.get_or_init(|| {
                Arc::new(TrackConsumersPool::new(
                    FairSpillPool::new(memory_pool_config.pool_size),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                ))
            });
            Arc::clone(memory_pool)
        }
        MemoryPoolType::GreedyTaskShared | MemoryPoolType::FairSpillTaskShared => {
            let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock().unwrap();
            let per_task_memory_pool =
                memory_pool_map.entry(task_attempt_id).or_insert_with(|| {
                    let pool: Arc<dyn MemoryPool> =
                        if memory_pool_config.pool_type == MemoryPoolType::GreedyTaskShared {
                            Arc::new(TrackConsumersPool::new(
                                GreedyMemoryPool::new(memory_pool_config.pool_size),
                                NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                            ))
                        } else {
                            Arc::new(TrackConsumersPool::new(
                                FairSpillPool::new(memory_pool_config.pool_size),
                                NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                            ))
                        };
                    PerTaskMemoryPool::new(pool)
                });
            per_task_memory_pool.num_plans += 1;
            Arc::clone(&per_task_memory_pool.memory_pool)
        }
        MemoryPoolType::Unbounded => Arc::new(UnboundedMemoryPool::default()),
    }
}
