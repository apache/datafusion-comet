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

use std::collections::HashMap;

pub(crate) const COMET_TRACING_ENABLED: &str = "spark.comet.tracing.enabled";
pub(crate) const COMET_DEBUG_ENABLED: &str = "spark.comet.debug.enabled";
pub(crate) const COMET_EXPLAIN_NATIVE_ENABLED: &str = "spark.comet.explain.native.enabled";
pub(crate) const COMET_MAX_TEMP_DIRECTORY_SIZE: &str = "spark.comet.maxTempDirectorySize";
pub(crate) const COMET_DEBUG_MEMORY: &str = "spark.comet.debug.memory";
pub(crate) const COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED: &str =
    "spark.comet.parquet.rowFilterPushdown.enabled";
pub(crate) const SPARK_EXECUTOR_CORES: &str = "spark.executor.cores";

// Object-store data cache (see OBJECT_STORE_CACHE_DESIGN.md §2.8). All read on the native
// side at first `createPlan` to build the process-global block cache.
pub(crate) const COMET_DATA_CACHE_ENABLED: &str = "spark.comet.scan.dataCache.enabled";
pub(crate) const COMET_DATA_CACHE_MEMORY_LIMIT: &str = "spark.comet.scan.dataCache.memoryLimit";
pub(crate) const COMET_DATA_CACHE_BLOCK_SIZE: &str = "spark.comet.scan.dataCache.blockSize";
pub(crate) const COMET_DATA_CACHE_SSD_LIMIT: &str = "spark.comet.scan.dataCache.ssd.limit";
// Read by the phase-2 SSD tier; declared now so the key set is stable.
#[allow(dead_code)]
pub(crate) const COMET_DATA_CACHE_SSD_PATH: &str = "spark.comet.scan.dataCache.ssd.path";

pub(crate) trait SparkConfig {
    fn get_bool(&self, name: &str) -> bool;
    fn get_u64(&self, name: &str, default_value: u64) -> u64;
    fn get_usize(&self, name: &str, default_value: usize) -> usize;
}

impl SparkConfig for HashMap<String, String> {
    fn get_bool(&self, name: &str) -> bool {
        self.get(name)
            .and_then(|str_val| str_val.parse::<bool>().ok())
            .unwrap_or(false)
    }

    fn get_u64(&self, name: &str, default_value: u64) -> u64 {
        self.get(name)
            .and_then(|str_val| str_val.parse::<u64>().ok())
            .unwrap_or(default_value)
    }

    fn get_usize(&self, name: &str, default_value: usize) -> usize {
        self.get(name)
            .and_then(|str_val| str_val.parse::<usize>().ok())
            .unwrap_or(default_value)
    }
}
