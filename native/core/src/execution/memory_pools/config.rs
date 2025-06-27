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
    Unified,
    FairUnified,
    Greedy,
    FairSpill,
    GreedyTaskShared,
    FairSpillTaskShared,
    GreedyGlobal,
    FairSpillGlobal,
    Unbounded,
}

impl MemoryPoolType {
    pub(crate) fn is_task_shared(&self) -> bool {
        matches!(
            self,
            MemoryPoolType::GreedyTaskShared | MemoryPoolType::FairSpillTaskShared
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
            "default" | "unified" => {
                // the `unified` memory pool interacts with Spark's memory pool to allocate
                // memory therefore does not need a size to be explicitly set. The pool size
                // shared with Spark is set by `spark.memory.offHeap.size`.
                MemoryPoolConfig::new(MemoryPoolType::Unified, 0)
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
            "default" | "greedy_task_shared" => {
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
