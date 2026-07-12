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

//! Process-global object-store data cache (OBJECT_STORE_CACHE_DESIGN.md §2.7).
//!
//! A single [`BlockCache`] lives for the executor's lifetime, initialized from the Spark
//! configs passed on the first `createPlan`. It is `None` when the feature is disabled, so
//! the wrapper is never constructed and there is zero overhead. First plan wins: changing
//! cache configs requires an executor restart (mirrors the existing process-global
//! object-store instance cache).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion_comet_block_cache::{
    BlockCache, BlockCacheConfig, DEFAULT_BLOCK_SIZE, DEFAULT_MAX_COALESCE_BYTES,
    DEFAULT_NUM_SHARDS,
};
use log::info;

use crate::execution::spark_config::{
    SparkConfig, COMET_DATA_CACHE_BLOCK_SIZE, COMET_DATA_CACHE_ENABLED,
    COMET_DATA_CACHE_MEMORY_LIMIT, COMET_DATA_CACHE_SSD_LIMIT,
};

/// Default memory-tier budget when the config is absent (512 MiB, matches `CometConf`).
const DEFAULT_MEMORY_LIMIT: u64 = 512 << 20;

static DATA_CACHE: OnceLock<Option<Arc<BlockCache>>> = OnceLock::new();

/// Initialize the global data cache from Spark configs, once. Idempotent and cheap on
/// subsequent calls (a `OnceLock` read). `local_dirs` are reserved for the phase-2 SSD tier.
pub(crate) fn init_once(spark_config: &HashMap<String, String>, local_dirs: &[String]) {
    DATA_CACHE.get_or_init(|| build(spark_config, local_dirs));
}

/// The process-global cache, or `None` when disabled / not yet initialized.
pub(crate) fn global() -> Option<Arc<BlockCache>> {
    DATA_CACHE.get().and_then(|opt| opt.clone())
}

fn build(
    spark_config: &HashMap<String, String>,
    _local_dirs: &[String],
) -> Option<Arc<BlockCache>> {
    if !spark_config.get_bool(COMET_DATA_CACHE_ENABLED) {
        return None;
    }
    let memory_budget = spark_config.get_u64(COMET_DATA_CACHE_MEMORY_LIMIT, DEFAULT_MEMORY_LIMIT);
    let block_size = spark_config.get_u64(COMET_DATA_CACHE_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
    let ssd_limit = spark_config.get_u64(COMET_DATA_CACHE_SSD_LIMIT, 0);
    if ssd_limit > 0 {
        // The SSD tier is phase 2; honor memory-tier config now and note the deferral.
        info!(
            "Comet data cache: SSD tier requested ({} MiB) but not yet implemented (phase 2); \
             using memory tier only",
            ssd_limit >> 20
        );
    }

    let cache = BlockCache::new(BlockCacheConfig {
        block_size,
        memory_budget,
        num_shards: DEFAULT_NUM_SHARDS,
        max_coalesce_bytes: DEFAULT_MAX_COALESCE_BYTES,
    });
    info!(
        "Comet object-store data cache enabled: memory budget {} MiB, block size {} MiB, {} shards",
        memory_budget >> 20,
        cache.block_size() >> 20,
        DEFAULT_NUM_SHARDS,
    );
    Some(cache)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conf(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn disabled_by_default_builds_nothing() {
        assert!(build(&conf(&[]), &[]).is_none());
        assert!(build(&conf(&[(COMET_DATA_CACHE_ENABLED, "false")]), &[]).is_none());
    }

    #[test]
    fn enabled_uses_defaults() {
        let cache = build(&conf(&[(COMET_DATA_CACHE_ENABLED, "true")]), &[]).unwrap();
        assert_eq!(cache.block_size(), DEFAULT_BLOCK_SIZE);
        assert_eq!(cache.memory_budget(), DEFAULT_MEMORY_LIMIT);
    }

    #[test]
    fn enabled_honors_explicit_byte_configs() {
        // Values arrive as byte counts (Scala `bytesConf` serializes bytes over JNI).
        let cache = build(
            &conf(&[
                (COMET_DATA_CACHE_ENABLED, "true"),
                (COMET_DATA_CACHE_MEMORY_LIMIT, &(1024u64 << 20).to_string()),
                (COMET_DATA_CACHE_BLOCK_SIZE, &(8u64 << 20).to_string()),
            ]),
            &[],
        )
        .unwrap();
        assert_eq!(cache.block_size(), 8 << 20);
        assert_eq!(cache.memory_budget(), 1024 << 20);
    }
}
