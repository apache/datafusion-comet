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

use datafusion::execution::cache::cache_manager::{
    FileMetadataCache, DEFAULT_METADATA_CACHE_LIMIT,
};
use datafusion::execution::cache::DefaultFilesMetadataCache;
use datafusion::execution::object_store::ObjectStoreUrl;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, PoisonError, RwLock};

/// Process-wide cache of Parquet file metadata (footer + page index), keyed by object store.
///
/// ## Why process lifetime?
///
/// Comet builds a fresh DataFusion `RuntimeEnv` for every Spark task, so the `FileMetadataCache`
/// that `RuntimeEnv` owns is discarded when the task ends and two tasks on the same executor
/// re-fetch and re-parse the same footer. There is no executor-scoped Rust object with a longer
/// lifetime to own this cache, so the process is the natural scope, as it already is for
/// `parquet_support::object_store_cache`.
///
/// ## Why keyed by object store URL?
///
/// The cache key inside `DefaultFilesMetadataCache` is a bare `object_store::path::Path`, and
/// `PhysicalPlanner::get_partitioned_files` builds that path from `url.path()`, which drops the
/// bucket for `s3://bucket/key`. A single shared instance would therefore let two buckets that
/// share a relative path collide. One cache per `scheme://host` removes that entirely. Unlike
/// the object store cache, the key deliberately omits the credential config hash: a footer's
/// identity does not depend on which credentials read it.
///
/// ## Staleness
///
/// Entries are validated by DataFusion against `(size, last_modified)` on every lookup, and
/// Comet populates both from Spark's `PartitionedFile`. Eviction is the LRU policy of
/// `DefaultFilesMetadataCache`, bounded by the configured memory limit.
pub(crate) struct MetadataCacheRegistry {
    config: RwLock<MetadataCacheConfig>,
    caches: RwLock<HashMap<String, Arc<dyn FileMetadataCache>>>,
}

#[derive(Clone, Copy)]
struct MetadataCacheConfig {
    enabled: bool,
    memory_limit: usize,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            memory_limit: DEFAULT_METADATA_CACHE_LIMIT,
        }
    }
}

impl MetadataCacheRegistry {
    pub(crate) fn new() -> Self {
        Self {
            config: RwLock::new(MetadataCacheConfig::default()),
            caches: RwLock::new(HashMap::new()),
        }
    }

    /// Applies the Spark-provided settings. Called once per task, so it must be idempotent.
    pub(crate) fn configure(&self, enabled: bool, memory_limit: usize) {
        let mut config = self.config.write().unwrap_or_else(PoisonError::into_inner);
        if config.enabled == enabled && config.memory_limit == memory_limit {
            return;
        }
        config.enabled = enabled;
        config.memory_limit = memory_limit;
        drop(config);

        let caches = self.caches.read().unwrap_or_else(PoisonError::into_inner);
        for cache in caches.values() {
            cache.update_cache_limit(memory_limit);
        }
    }

    /// Returns the shared cache for `url`, or `None` when sharing is disabled, in which case the
    /// caller should fall back to the per-task `RuntimeEnv` cache.
    pub(crate) fn cache_for(&self, url: &ObjectStoreUrl) -> Option<Arc<dyn FileMetadataCache>> {
        let config = *self.config.read().unwrap_or_else(PoisonError::into_inner);
        if !config.enabled {
            return None;
        }

        let key = url.as_str();
        {
            let caches = self.caches.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(cache) = caches.get(key) {
                return Some(Arc::clone(cache));
            }
        }

        let mut caches = self.caches.write().unwrap_or_else(PoisonError::into_inner);
        let cache = caches
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(DefaultFilesMetadataCache::new(config.memory_limit)));
        Some(Arc::clone(cache))
    }
}

fn registry() -> &'static MetadataCacheRegistry {
    static REGISTRY: OnceLock<MetadataCacheRegistry> = OnceLock::new();
    REGISTRY.get_or_init(MetadataCacheRegistry::new)
}

/// Returns the process-wide metadata cache for `url`, or `None` when sharing is disabled.
pub(crate) fn shared_metadata_cache(url: &ObjectStoreUrl) -> Option<Arc<dyn FileMetadataCache>> {
    registry().cache_for(url)
}

/// Applies Spark's metadata cache settings to the process-wide registry.
pub(crate) fn configure_metadata_cache(enabled: bool, memory_limit: usize) {
    registry().configure(enabled, memory_limit);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> ObjectStoreUrl {
        ObjectStoreUrl::parse(s).unwrap()
    }

    #[test]
    fn same_url_returns_the_same_cache() {
        let registry = MetadataCacheRegistry::new();
        let first = registry.cache_for(&url("s3://bucket-a")).unwrap();
        let second = registry.cache_for(&url("s3://bucket-a")).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn different_buckets_get_separate_caches() {
        let registry = MetadataCacheRegistry::new();
        let a = registry.cache_for(&url("s3://bucket-a")).unwrap();
        let b = registry.cache_for(&url("s3://bucket-b")).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn disabled_registry_returns_none() {
        let registry = MetadataCacheRegistry::new();
        registry.configure(false, DEFAULT_METADATA_CACHE_LIMIT);
        assert!(registry.cache_for(&url("s3://bucket-a")).is_none());
    }

    #[test]
    fn default_limit_applies_to_new_caches() {
        let registry = MetadataCacheRegistry::new();
        let cache = registry.cache_for(&url("s3://bucket-a")).unwrap();
        assert_eq!(cache.cache_limit(), DEFAULT_METADATA_CACHE_LIMIT);
    }

    #[test]
    fn configure_updates_the_limit_of_live_caches() {
        let registry = MetadataCacheRegistry::new();
        let cache = registry.cache_for(&url("s3://bucket-a")).unwrap();
        registry.configure(true, 1024);
        assert_eq!(cache.cache_limit(), 1024);
        // and newly created caches pick up the configured limit
        let other = registry.cache_for(&url("s3://bucket-b")).unwrap();
        assert_eq!(other.cache_limit(), 1024);
    }
}
