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
        _ => Err(CometError::Config(format!(
            "Unsupported memory pool type: {memory_pool_type}"
        ))),
    }
}
