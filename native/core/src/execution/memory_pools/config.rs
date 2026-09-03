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
    Unbounded,
    #[cfg(feature = "oom-guard")]
    RealUsage,
}

impl MemoryPoolType {
    /// True when this pool's `reserved()` reflects a single task's usage, so a per-task
    /// fair-share comparison is meaningful. Only the task-shared unified pools qualify.
    /// `Unbounded` is created per plan, so its `reserved()` misses the task's other plans.
    #[cfg_attr(not(feature = "oom-guard"), allow(dead_code))]
    pub(crate) fn has_per_task_budget(&self) -> bool {
        // The dedicated `real_usage` pool gates on process-wide real usage
        // (first-come), not a per-task reservation, so it has no per-task budget.
        #[cfg(feature = "oom-guard")]
        if matches!(self, MemoryPoolType::RealUsage) {
            return false;
        }
        !matches!(self, MemoryPoolType::Unbounded)
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
) -> CometResult<MemoryPoolConfig> {
    if !off_heap_mode {
        // On-heap mode exists so that the Spark SQL tests can run against Comet without changing
        // Spark's memory configuration. Comet's native allocations are not on the JVM heap, so
        // there is no Spark pool they can honestly be charged to, and the fixed-size pool that
        // used to stand in for one bounded nothing the container cares about. It is not a
        // production configuration, so it accounts for nothing.
        return Ok(MemoryPoolConfig::new(MemoryPoolType::Unbounded, 0));
    }

    let pool_size = memory_limit as usize;
    match memory_pool_type.as_str() {
        "fair_unified" => Ok(MemoryPoolConfig::new(
            MemoryPoolType::FairUnified,
            pool_size,
        )),
        "greedy_unified" => {
            // the `unified` memory pool interacts with Spark's memory pool to allocate
            // memory therefore does not need a size to be explicitly set. The pool size
            // shared with Spark is set by `spark.memory.offHeap.size`.
            Ok(MemoryPoolConfig::new(MemoryPoolType::GreedyUnified, 0))
        }
        #[cfg(feature = "oom-guard")]
        "real_usage" => {
            // Gate growth on real allocator usage against the off-heap budget
            // (`pool_size`) instead of delegating per-task accounting to Spark's
            // TaskMemoryManager. See `RealUsagePool`.
            Ok(MemoryPoolConfig::new(MemoryPoolType::RealUsage, pool_size))
        }
        #[cfg(not(feature = "oom-guard"))]
        "real_usage" => Err(CometError::Config(
            "Memory pool type 'real_usage' requires a Comet build with the \
             'oom-guard' native feature"
                .to_string(),
        )),
        _ => Err(CometError::Config(format!(
            "Unsupported memory pool type: {memory_pool_type}"
        ))),
    }
}
