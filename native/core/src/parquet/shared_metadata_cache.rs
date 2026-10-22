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

//! A registry that lets the tasks of one Spark stage share a Parquet `FileMetadataCache`.
//!
//! Comet builds a `RuntimeEnv`, and with it a metadata cache, for every Spark task, so two tasks
//! that read splits of the same file each fetch and parse its footer and page index. This
//! registry hands every task of a stage on this executor the same cache instead.
//!
//! The scope is the stage, not the executor, on purpose. A cache that outlives the query would
//! answer a later query from metadata of a file that has since been deleted or rewritten, and
//! `(size, modification time)` validation cannot catch that: Spark's cached file index lists the
//! same size and time as the cached footer. Spark's `HiveMetadataCacheSuite` reads a table,
//! deletes one data file, reads again, and expects `FILE_NOT_EXIST`. Every new query is a new
//! stage, so it opens its files and sees the deletion, as it does without the registry.
//!
//! A stage attempt is part of the key because a retried stage may read a changed file. So is the
//! Spark application: stage ids restart at zero in every `SparkContext`, and this registry is
//! process-wide, so a context started after another one stopped in the same JVM must not match
//! the stages the old one left behind.
//!
//! The registry holds a weak reference to the cache of every stage with a running task, since the
//! tasks own it through their `RuntimeEnv`. It additionally holds strong references to the last
//! [`MAX_IDLE_CACHES`] caches it handed out. Those matter when an executor runs one task at a
//! time: the next task of the stage starts after the previous one released its plan. Whatever the
//! tasks do, at most [`MAX_IDLE_CACHES`] caches outlive the tasks that used them.
//!
//! A stage's cache holds DataFusion's per-task limit for every task the executor runs at once, so
//! that sharing does not shrink the room each task had with a cache of its own: tasks that read
//! different files would otherwise evict each other's entries, among them the footers the Delta
//! deletion vector planning loads before the scan opens them. Shared entries are stored once, so
//! the tasks of a stage hold no more than they did, but the retained caches can hold as much after
//! their tasks finish.
//!
//! The stage scopes the cache; it does not identify the files in it. DataFusion keys an entry by
//! the file's path within its object store, which drops the bucket, so [`scope_to_store`] gives
//! each scan a view of the cache that also keys by the scan's object store.

use crate::execution::spark_config::{
    SparkConfig, COMET_APP_ID, COMET_PARQUET_SHARED_METADATA_CACHE_ENABLED, COMET_STAGE_ATTEMPT,
    COMET_STAGE_ID,
};
use datafusion::common::TableReference;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadataCache, DEFAULT_METADATA_CACHE_LIMIT,
};
use datafusion::execution::cache::default_cache::DefaultCache;
use datafusion::execution::cache::{Cache, CacheEntryInfo};
use object_store::path::Path;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

/// How many caches of stages with no running task the registry holds on to.
const MAX_IDLE_CACHES: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StageKey {
    pub app_id: String,
    pub stage_id: i32,
    pub stage_attempt: i32,
}

#[derive(Default)]
struct Registry {
    /// Every cache that is still alive, running or retained.
    alive: HashMap<StageKey, Weak<FileMetadataCache>>,
    /// The caches kept alive for the next task of their stage, least recently requested first.
    retained: VecDeque<(StageKey, Arc<FileMetadataCache>)>,
}

#[derive(Default)]
pub(crate) struct SharedMetadataCaches {
    registry: Mutex<Registry>,
}

impl SharedMetadataCaches {
    /// The cache shared by the tasks of `key`, created with a memory limit of `limit` bytes if no
    /// task of that stage has asked for one yet.
    pub(crate) fn cache_for(&self, key: StageKey, limit: usize) -> Arc<FileMetadataCache> {
        let mut registry = self.registry.lock().unwrap();
        let cache = registry
            .alive
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Arc::new(DefaultCache::new(limit).with_name("SharedFileMetadataCache"))
            });
        registry.alive.insert(key.clone(), Arc::downgrade(&cache));

        registry.retained.retain(|(retained, _)| *retained != key);
        registry.retained.push_back((key, Arc::clone(&cache)));
        while registry.retained.len() > MAX_IDLE_CACHES {
            registry.retained.pop_front();
        }
        registry.alive.retain(|_, weak| weak.strong_count() > 0);
        cache
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.registry.lock().unwrap().alive.len()
    }
}

/// A view of a [`FileMetadataCache`] that keys every entry by the object store as well as the
/// path within it.
///
/// DataFusion keys an entry by `ObjectMeta.location`, and the location of a file in a scan has no
/// bucket or authority, so equal paths in two stores collide. Two files at the same path with the
/// same size then pass the cache's `(size, modification time)` check, and a task would plan its
/// scan with the other file's footer and statistics. Only the key changes; the entries, the memory
/// limit and the eviction are those of the cache underneath, which is why a view can be made per
/// scan from the cache of the whole stage. `len`, `clear` and `list_entries` act on the whole
/// cache, not on this scope.
#[derive(Debug)]
struct StoreScopedCache {
    scope: String,
    inner: Arc<FileMetadataCache>,
}

impl StoreScopedCache {
    fn scoped(&self, key: &Path) -> Path {
        std::iter::once(self.scope.clone())
            .chain(key.parts().map(|part| part.as_ref().to_string()))
            .collect()
    }
}

impl Cache<Path, CachedFileMetadataEntry> for StoreScopedCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.get(&self.scoped(key))
    }

    fn put(&self, key: &Path, value: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        self.inner.put(&self.scoped(key), value)
    }

    fn remove(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.remove(&self.scoped(key))
    }

    fn contains_key(&self, key: &Path) -> bool {
        self.inner.contains_key(&self.scoped(key))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&self) {
        self.inner.clear()
    }

    fn name(&self) -> String {
        self.inner.name()
    }

    fn cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.update_cache_limit(limit)
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.inner.cache_ttl()
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.inner.update_cache_ttl(ttl)
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> datafusion::error::Result<()> {
        self.inner.drop_table_entries(table_ref)
    }

    fn list_entries(
        &self,
    ) -> datafusion::common::HashMap<Path, CacheEntryInfo<CachedFileMetadataEntry>> {
        self.inner.list_entries()
    }
}

/// The metadata cache a scan of `object_store_url` should use, given the one of its
/// `RuntimeEnv`.
///
/// `object_store_url` is the URL the scan's store is registered under, which already carries the
/// backend and a hash of the store's configuration (see `object_store_registration_url`), so two
/// stores with equal paths never share an entry. The scan and any code that reads the footers
/// ahead of it, such as the Delta deletion vector planning, must pass the same URL to find each
/// other's entries.
pub(crate) fn scope_to_store(
    cache: Arc<FileMetadataCache>,
    object_store_url: &ObjectStoreUrl,
) -> Arc<FileMetadataCache> {
    Arc::new(StoreScopedCache {
        scope: object_store_url.as_str().to_string(),
        inner: cache,
    })
}

/// The memory limit of the cache shared by the tasks of a stage on an executor that runs
/// `task_slots` tasks at once: DataFusion's limit for one task's cache, for each of them.
pub(crate) fn shared_cache_limit(task_slots: usize) -> usize {
    DEFAULT_METADATA_CACHE_LIMIT.saturating_mul(task_slots.max(1))
}

/// The cache the task with these Spark configs should use, with its memory limit, or `None` when
/// sharing is off or the task's stage is unknown, in which case DataFusion's per-task cache
/// applies. `task_slots` is how many tasks the executor runs at once.
///
/// The limit must also be the `metadata_cache_limit` of the task's `CacheManagerConfig`, since
/// `CacheManager::try_new` applies that limit to the cache it is given.
pub(crate) fn shared_metadata_cache_for_task(
    spark_config: &HashMap<String, String>,
    task_slots: usize,
) -> Option<(Arc<FileMetadataCache>, usize)> {
    static CACHES: OnceLock<SharedMetadataCaches> = OnceLock::new();

    if !spark_config.get_bool(COMET_PARQUET_SHARED_METADATA_CACHE_ENABLED) {
        return None;
    }
    let parse = |name: &str| spark_config.get(name)?.parse::<i32>().ok();
    let key = StageKey {
        app_id: spark_config.get(COMET_APP_ID)?.clone(),
        stage_id: parse(COMET_STAGE_ID)?,
        stage_attempt: parse(COMET_STAGE_ATTEMPT)?,
    };
    let limit = shared_cache_limit(task_slots);
    Some((
        CACHES
            .get_or_init(SharedMetadataCaches::default)
            .cache_for(key, limit),
        limit,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(stage_id: i32, stage_attempt: i32) -> StageKey {
        StageKey {
            app_id: "app-1".to_string(),
            stage_id,
            stage_attempt,
        }
    }

    #[test]
    fn tasks_of_one_stage_share_a_cache() {
        let caches = SharedMetadataCaches::default();
        let a = caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT);
        let b = caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT);
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn stages_and_attempts_do_not_share() {
        let caches = SharedMetadataCaches::default();
        let first = caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT);
        let other_stage = caches.cache_for(key(2, 0), DEFAULT_METADATA_CACHE_LIMIT);
        let retry = caches.cache_for(key(1, 1), DEFAULT_METADATA_CACHE_LIMIT);
        assert!(!Arc::ptr_eq(&first, &other_stage));
        assert!(!Arc::ptr_eq(&first, &retry));
    }

    #[test]
    fn an_idle_cache_is_kept_for_the_next_task_of_its_stage() {
        let caches = SharedMetadataCaches::default();
        let first = caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT);
        let address = Arc::as_ptr(&first) as *const () as usize;
        drop(first);
        let next = caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT);
        assert_eq!(Arc::as_ptr(&next) as *const () as usize, address);
    }

    #[test]
    fn applications_do_not_share() {
        // A context started after another one stopped in the same JVM reuses its stage ids.
        let caches = SharedMetadataCaches::default();
        let old = caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT);
        drop(old);
        let mut other = key(1, 0);
        other.app_id = "app-2".to_string();
        let new = caches.cache_for(other, DEFAULT_METADATA_CACHE_LIMIT);
        assert_eq!(new.len(), 0);
        assert!(!Arc::ptr_eq(
            &new,
            &caches.cache_for(key(1, 0), DEFAULT_METADATA_CACHE_LIMIT)
        ));
    }

    #[test]
    fn idle_retention_is_bounded_after_all_tasks_finish() {
        let caches = SharedMetadataCaches::default();
        let handles: Vec<_> = (0..16)
            .map(|stage| caches.cache_for(key(stage, 0), DEFAULT_METADATA_CACHE_LIMIT))
            .collect();
        let weak: Vec<_> = handles.iter().map(Arc::downgrade).collect();
        drop(handles);
        assert!(weak.iter().filter(|w| w.upgrade().is_some()).count() <= MAX_IDLE_CACHES);
    }

    #[test]
    fn a_stage_cache_holds_the_per_task_limit_for_every_task_slot() {
        let caches = SharedMetadataCaches::default();
        let cache = caches.cache_for(key(1, 0), shared_cache_limit(8));
        assert_eq!(cache.cache_limit(), 8 * DEFAULT_METADATA_CACHE_LIMIT);
        assert_eq!(shared_cache_limit(0), DEFAULT_METADATA_CACHE_LIMIT);
    }

    #[test]
    fn idle_caches_are_bounded_but_running_ones_are_not_dropped() {
        let caches = SharedMetadataCaches::default();
        let running = caches.cache_for(key(0, 0), DEFAULT_METADATA_CACHE_LIMIT);
        for stage in 1..=10 {
            drop(caches.cache_for(key(stage, 0), DEFAULT_METADATA_CACHE_LIMIT));
        }
        // The running stage plus the idle ones that fit.
        assert_eq!(caches.len(), 1 + MAX_IDLE_CACHES);
        assert!(Arc::ptr_eq(
            &running,
            &caches.cache_for(key(0, 0), DEFAULT_METADATA_CACHE_LIMIT)
        ));
    }
}
