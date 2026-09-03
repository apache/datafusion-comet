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

use crate::errors::{CometError, CometResult};

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum MemoryPoolType {
    GreedyUnified,
    FairUnified,
    Greedy,
    FairSpill,
    GreedyTaskShared,
    FairSpillTaskShared,
    GreedyGlobal,
    FairSpillGlobal,
    Unbounded,
    #[cfg(feature = "oom-guard")]
    RealUsage,
}

impl MemoryPoolType {
    /// True when this pool's `reserved()` reflects a single task's usage, so a per-task
    /// fair-share comparison is meaningful (false for process-wide pools). The non-shared
    /// per-task pools (`Greedy`/`FairSpill`) return true but keep no task registry, so the
    /// fair-share divisor falls back to `executor_cores` rather than the active-task count.
    #[cfg_attr(not(feature = "oom-guard"), allow(dead_code))]
    pub(crate) fn has_per_task_budget(&self) -> bool {
        // The dedicated `real_usage` pool gates on process-wide real usage
        // (first-come), not a per-task reservation, so it has no per-task budget.
        #[cfg(feature = "oom-guard")]
        if matches!(self, MemoryPoolType::RealUsage) {
            return false;
        }
        !matches!(
            self,
            MemoryPoolType::GreedyGlobal
                | MemoryPoolType::FairSpillGlobal
                | MemoryPoolType::Unbounded
        )
    }
}

pub(crate) struct MemoryPoolConfig {
    pub(crate) pool_type: MemoryPoolType,
    pub(crate) pool_size: usize,
}

impl MemoryPoolConfig {
    pub(crate) fn new(pool_type: MemoryPoolType, pool_size: usize) -> Self {
        Self {
            pool_type,
            pool_size,
        }
    }
}

pub(crate) fn parse_memory_pool_config(
    off_heap_mode: bool,
    memory_pool_type: String,
    memory_limit: i64,
    memory_limit_per_task: i64,
) -> CometResult<MemoryPoolConfig> {
    let pool_size = memory_limit as usize;
    let memory_pool_config = if off_heap_mode {
        match memory_pool_type.as_str() {
            "fair_unified" => MemoryPoolConfig::new(MemoryPoolType::FairUnified, pool_size),
            "greedy_unified" => {
                // the `unified` memory pool interacts with Spark's memory pool to allocate
                // memory therefore does not need a size to be explicitly set. The pool size
                // shared with Spark is set by `spark.memory.offHeap.size`.
                MemoryPoolConfig::new(MemoryPoolType::GreedyUnified, 0)
            }
            #[cfg(feature = "oom-guard")]
            "real_usage" => {
                // Gate growth on real allocator usage against the off-heap budget
                // (`pool_size`) instead of delegating per-task accounting to Spark's
                // TaskMemoryManager. See `RealUsagePool`.
                MemoryPoolConfig::new(MemoryPoolType::RealUsage, pool_size)
            }
            #[cfg(not(feature = "oom-guard"))]
            "real_usage" => {
                return Err(CometError::Config(
                    "Memory pool type 'real_usage' requires a Comet build with the \
                     'oom-guard' native feature"
                        .to_string(),
                ))
            }
            _ => {
                return Err(CometError::Config(format!(
                    "Unsupported memory pool type for off-heap mode: {memory_pool_type}"
                )))
            }
        }
    } else {
        // Use the memory pool from DF
        let pool_size_per_task = memory_limit_per_task as usize;
        match memory_pool_type.as_str() {
            "fair_spill_task_shared" => {
                MemoryPoolConfig::new(MemoryPoolType::FairSpillTaskShared, pool_size_per_task)
            }
            "greedy_task_shared" => {
                MemoryPoolConfig::new(MemoryPoolType::GreedyTaskShared, pool_size_per_task)
            }
            "fair_spill_global" => {
                MemoryPoolConfig::new(MemoryPoolType::FairSpillGlobal, pool_size)
            }
            "greedy_global" => MemoryPoolConfig::new(MemoryPoolType::GreedyGlobal, pool_size),
            "fair_spill" => MemoryPoolConfig::new(MemoryPoolType::FairSpill, pool_size_per_task),
            "greedy" => MemoryPoolConfig::new(MemoryPoolType::Greedy, pool_size_per_task),
            "unbounded" => MemoryPoolConfig::new(MemoryPoolType::Unbounded, 0),
            _ => {
                return Err(CometError::Config(format!(
                    "Unsupported memory pool type for on-heap mode: {memory_pool_type}"
                )))
            }
        }
    };
    Ok(memory_pool_config)
}
