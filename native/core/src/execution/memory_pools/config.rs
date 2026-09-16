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
}

pub(crate) struct MemoryPoolConfig {
    pub(crate) pool_type: MemoryPoolType,
    pub(crate) pool_size: usize,
    /// Budget the off-heap pools compare real native usage against, or `None` when there is
    /// nothing to compare to. Always `None` in on-heap mode, where Comet has no off-heap
    /// allotment. Set whether or not the crossing is enforced, because observing it is the
    /// default behaviour.
    pub(crate) native_usage_budget: Option<usize>,
    /// Whether crossing `native_usage_budget` refuses the reservation. When false the pools only
    /// log the crossing.
    pub(crate) enforce_native_usage: bool,
}

impl MemoryPoolConfig {
    pub(crate) fn new(pool_type: MemoryPoolType, pool_size: usize) -> Self {
        Self {
            pool_type,
            pool_size,
            native_usage_budget: None,
            enforce_native_usage: false,
        }
    }

    fn with_native_usage_budget(mut self, budget: Option<usize>, enforce: bool) -> Self {
        self.native_usage_budget = budget;
        self.enforce_native_usage = enforce;
        self
    }
}

pub(crate) fn parse_memory_pool_config(
    off_heap_mode: bool,
    memory_pool_type: String,
    memory_limit: i64,
    memory_limit_per_task: i64,
    enforce_native_usage: bool,
    off_heap_size: usize,
) -> CometResult<MemoryPoolConfig> {
    let pool_size = memory_limit as usize;
    let memory_pool_config = if off_heap_mode {
        // Deliberately the whole off-heap size rather than `pool_size`, which is that size times
        // `spark.comet.exec.memoryPool.fraction`. The fraction bounds what Comet may *reserve*,
        // and lowering it is how operators force spilling; reusing it here would also lower the
        // ceiling on *real* usage, so a small fraction would deny reservations outright instead
        // of provoking the spills it was set to cause.
        let native_usage_budget = (off_heap_size > 0).then_some(off_heap_size);
        match memory_pool_type.as_str() {
            "fair_unified" => MemoryPoolConfig::new(MemoryPoolType::FairUnified, pool_size)
                .with_native_usage_budget(native_usage_budget, enforce_native_usage),
            "greedy_unified" => {
                // the `unified` memory pool interacts with Spark's memory pool to allocate
                // memory therefore does not need a size to be explicitly set. The pool size
                // shared with Spark is set by `spark.memory.offHeap.size`.
                MemoryPoolConfig::new(MemoryPoolType::GreedyUnified, 0)
                    .with_native_usage_budget(native_usage_budget, enforce_native_usage)
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
