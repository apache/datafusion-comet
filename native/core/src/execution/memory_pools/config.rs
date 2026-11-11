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
}

pub(crate) struct MemoryPoolConfig {
    pub(crate) pool_type: MemoryPoolType,
    /// The most that a task may reserve: `spark.memory.offHeap.size` times the deprecated
    /// `spark.comet.exec.memoryPool.fraction`.
    pub(crate) pool_size: usize,
    /// `spark.memory.offHeap.size`, which the executor's tasks share.
    pub(crate) off_heap_size: usize,
}

impl MemoryPoolConfig {
    /// A pool that is all of `spark.memory.offHeap.size`, as it is when the fraction is 1.0.
    pub(crate) fn new(pool_type: MemoryPoolType, pool_size: usize) -> Self {
        Self::with_off_heap_size(pool_type, pool_size, pool_size)
    }

    pub(crate) fn with_off_heap_size(
        pool_type: MemoryPoolType,
        pool_size: usize,
        off_heap_size: usize,
    ) -> Self {
        Self {
            pool_type,
            pool_size,
            off_heap_size,
        }
    }
}

pub(crate) fn parse_memory_pool_config(
    off_heap_mode: bool,
    memory_pool_type: String,
    memory_limit: i64,
    off_heap_size: i64,
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
        "fair_unified" => Ok(MemoryPoolConfig::with_off_heap_size(
            MemoryPoolType::FairUnified,
            pool_size,
            off_heap_size as usize,
        )),
        "greedy_unified" => {
            // the `unified` memory pool interacts with Spark's memory pool to allocate
            // memory therefore does not need a size to be explicitly set. The pool size
            // shared with Spark is set by `spark.memory.offHeap.size`.
            Ok(MemoryPoolConfig::new(MemoryPoolType::GreedyUnified, 0))
        }
        _ => Err(CometError::Config(format!(
            "Unsupported memory pool type: {memory_pool_type}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_unified_is_sized_from_the_limit_and_the_off_heap_size() {
        let config = parse_memory_pool_config(true, "fair_unified".to_string(), 100, 1000).unwrap();
        assert!(config.pool_type == MemoryPoolType::FairUnified);
        assert_eq!((config.pool_size, config.off_heap_size), (100, 1000));
    }
}
