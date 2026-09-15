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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum MemoryPoolType {
    GreedyUnified,
    /// `GreedyUnified` behind a gate on the bytes the native allocator has actually handed out.
    GreedyUnifiedChecked,
    FairUnified,
    Greedy,
    FairSpill,
    GreedyTaskShared,
    FairSpillTaskShared,
    GreedyGlobal,
    FairSpillGlobal,
    Unbounded,
}

#[derive(Debug)]
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
            // The checked pool gates on real native usage, so it needs the budget the balance is
            // compared against: the same number `fair_unified` receives.
            #[cfg(feature = "alloc-accounting")]
            "greedy_unified_checked" => {
                MemoryPoolConfig::new(MemoryPoolType::GreedyUnifiedChecked, pool_size)
            }
            #[cfg(not(feature = "alloc-accounting"))]
            "greedy_unified_checked" => {
                return Err(CometError::Config(
                    "Memory pool type greedy_unified_checked requires the native library to be \
                     built with the alloc-accounting cargo feature"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "alloc-accounting")]
    fn checked_pool_takes_the_off_heap_limit_as_its_budget() {
        let config =
            parse_memory_pool_config(true, "greedy_unified_checked".to_string(), 1 << 30, 1 << 20)
                .unwrap();
        assert_eq!(config.pool_type, MemoryPoolType::GreedyUnifiedChecked);
        assert_eq!(config.pool_size, 1 << 30);
    }

    #[test]
    #[cfg(not(feature = "alloc-accounting"))]
    fn checked_pool_is_rejected_without_the_accounting_feature() {
        let err =
            parse_memory_pool_config(true, "greedy_unified_checked".to_string(), 1 << 30, 1 << 20)
                .unwrap_err();
        assert!(
            err.to_string().contains("alloc-accounting"),
            "error should name the missing feature: {err}"
        );
    }

    #[test]
    fn checked_pool_is_off_heap_only() {
        let err = parse_memory_pool_config(
            false,
            "greedy_unified_checked".to_string(),
            1 << 30,
            1 << 20,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("on-heap mode"),
            "unexpected error: {err}"
        );
    }
}
