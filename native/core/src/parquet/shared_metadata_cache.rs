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
//! tasks do, at most [`MAX_IDLE_CACHES`] caches outlive the tasks that used them. A retained cache
//! is freed when tasks of [`MAX_IDLE_CACHES`] other stages have asked for theirs since, or when
//! the plugin shuts down and [`clear_shared_metadata_caches`] runs. Nothing frees it sooner: an
//! executor is not told that a stage has finished, so one that goes idle after a query keeps the
//! caches of its last stages until it shuts down.
//!
//! A stage's cache holds DataFusion's per-task limit for each of its tasks that is running, and one
//! task's limit once none is, so that sharing does not shrink the room each task had with a cache
//! of its own: tasks that read different files would otherwise evict each other's entries, among
//! them the footers the Delta deletion vector planning loads before the scan opens them. Each task
//! takes its share when it asks for the cache and gives it back when its `RuntimeEnv` drops it, so
//! the limit follows the tasks that actually run, whatever the executor's configured cores say.
//! Shared entries are stored once, so the tasks of a stage hold no more than they did, and a
//! retained cache holds no more than one task's.
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
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;

/// How many caches of stages with no running task the registry holds on to.
const MAX_IDLE_CACHES: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StageKey {
    pub app_id: String,
    pub stage_id: i32,
    pub stage_attempt: i32,
}

/// The cache of one stage, and how many of its tasks are running.
struct StageCache {
    cache: Arc<DefaultCache<Path, CachedFileMetadataEntry>>,
    running: Mutex<usize>,
}

impl StageCache {
    fn new() -> Self {
        Self {
            cache: Arc::new(
                DefaultCache::new(DEFAULT_METADATA_CACHE_LIMIT)
                    .with_name("SharedFileMetadataCache"),
            ),
            running: Mutex::new(0),
        }
    }

    /// Counts a task in or out, and sizes the cache for the tasks left running. Lowering the limit
    /// evicts the least recently used entries down to it.
    fn update_running(&self, update: impl FnOnce(usize) -> usize) {
        let mut running = self.running.lock().unwrap();
        *running = update(*running);
        self.cache
            .update_cache_limit(DEFAULT_METADATA_CACHE_LIMIT.saturating_mul((*running).max(1)));
    }
}

/// One task's handle on its stage's cache, which counts the task as running for as long as it
/// lives. The task's `RuntimeEnv` and the scans' views of the cache hold it, so it drops when the
/// task's plan does. Strictly it counts native plans, of which a task usually has one; a task that
/// runs several at once is counted once for each, which errs toward a larger cache.
///
/// It is the cache the task's `CacheManager` is given. `CacheManager::try_new` sets the limit of
/// that cache to the one in its config, so this ignores limit updates: the stage's running tasks
/// decide it.
pub(crate) struct TaskCache {
    stage: Arc<StageCache>,
}

impl TaskCache {
    fn new(stage: Arc<StageCache>) -> Self {
        stage.update_running(|running| running + 1);
        Self { stage }
    }
}

impl Drop for TaskCache {
    fn drop(&mut self) {
        self.stage
            .update_running(|running| running.saturating_sub(1));
    }
}

impl std::fmt::Debug for TaskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskCache")
            .field("running", &*self.stage.running.lock().unwrap())
            .finish()
    }
}

impl Cache<Path, CachedFileMetadataEntry> for TaskCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.stage.cache.get(key)
    }

    fn put(&self, key: &Path, value: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        self.stage.cache.put(key, value)
    }

    fn remove(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.stage.cache.remove(key)
    }

    fn contains_key(&self, key: &Path) -> bool {
        self.stage.cache.contains_key(key)
    }

    fn len(&self) -> usize {
        self.stage.cache.len()
    }

    fn clear(&self) {
        self.stage.cache.clear()
    }

    fn name(&self) -> String {
        self.stage.cache.name()
    }

    fn cache_limit(&self) -> usize {
        self.stage.cache.cache_limit()
    }

    fn update_cache_limit(&self, _limit: usize) {}

    fn cache_ttl(&self) -> Option<Duration> {
        self.stage.cache.cache_ttl()
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.stage.cache.update_cache_ttl(ttl)
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> datafusion::error::Result<()> {
        self.stage.cache.drop_table_entries(table_ref)
    }

    fn list_entries(
        &self,
    ) -> datafusion::common::HashMap<Path, CacheEntryInfo<CachedFileMetadataEntry>> {
        self.stage.cache.list_entries()
    }
}

#[derive(Default)]
struct Registry {
    /// Every stage cache that is still alive, running or retained.
    alive: HashMap<StageKey, Weak<StageCache>>,
    /// The caches kept alive for the next task of their stage, least recently requested first.
    retained: VecDeque<(StageKey, Arc<StageCache>)>,
}

#[derive(Default)]
pub(crate) struct SharedMetadataCaches {
    registry: Mutex<Registry>,
}

impl SharedMetadataCaches {
    /// A task's handle on the cache shared by the tasks of `key`, created if no task of that stage
    /// has asked for one yet.
    pub(crate) fn cache_for(&self, key: StageKey) -> Arc<TaskCache> {
        let mut registry = self.registry.lock().unwrap();
        let stage = registry
            .alive
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| Arc::new(StageCache::new()));
        registry.alive.insert(key.clone(), Arc::downgrade(&stage));

        registry.retained.retain(|(retained, _)| *retained != key);
        registry.retained.push_back((key, Arc::clone(&stage)));
        while registry.retained.len() > MAX_IDLE_CACHES {
            registry.retained.pop_front();
        }
        registry.alive.retain(|_, weak| weak.strong_count() > 0);
        Arc::new(TaskCache::new(stage))
    }

    /// Forgets every stage, freeing the retained caches. Running tasks keep the caches they hold.
    fn clear(&self) {
        let dropped = std::mem::take(&mut *self.registry.lock().unwrap());
        drop(dropped);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.registry.lock().unwrap().alive.len()
    }
}

/// The registry of the executor's stages.
static CACHES: LazyLock<SharedMetadataCaches> = LazyLock::new(SharedMetadataCaches::default);

/// Frees the caches retained for stages with no running task, as the plugin shuts down.
pub fn clear_shared_metadata_caches() {
    CACHES.clear();
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

/// The cache the task with these Spark configs should use, or `None` when sharing is off or the
/// task's stage is unknown, in which case DataFusion's per-task cache applies.
pub(crate) fn shared_metadata_cache_for_task(
    spark_config: &HashMap<String, String>,
) -> Option<Arc<FileMetadataCache>> {
    if !spark_config.get_bool(COMET_PARQUET_SHARED_METADATA_CACHE_ENABLED) {
        return None;
    }
    let parse = |name: &str| spark_config.get(name)?.parse::<i32>().ok();
    let key = StageKey {
        app_id: spark_config.get(COMET_APP_ID)?.clone(),
        stage_id: parse(COMET_STAGE_ID)?,
        stage_attempt: parse(COMET_STAGE_ATTEMPT)?,
    };
    let cache: Arc<FileMetadataCache> = CACHES.cache_for(key);
    Some(cache)
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

    fn same_stage(a: &TaskCache, b: &TaskCache) -> bool {
        Arc::ptr_eq(&a.stage, &b.stage)
    }

    #[test]
    fn tasks_of_one_stage_share_a_cache() {
        let caches = SharedMetadataCaches::default();
        let a = caches.cache_for(key(1, 0));
        let b = caches.cache_for(key(1, 0));
        assert!(same_stage(&a, &b));
    }

    #[test]
    fn stages_and_attempts_do_not_share() {
        let caches = SharedMetadataCaches::default();
        let first = caches.cache_for(key(1, 0));
        let other_stage = caches.cache_for(key(2, 0));
        let retry = caches.cache_for(key(1, 1));
        assert!(!same_stage(&first, &other_stage));
        assert!(!same_stage(&first, &retry));
    }

    #[test]
    fn an_idle_cache_is_kept_for_the_next_task_of_its_stage() {
        let caches = SharedMetadataCaches::default();
        let first = caches.cache_for(key(1, 0));
        let stage = Arc::downgrade(&first.stage);
        drop(first);
        let next = caches.cache_for(key(1, 0));
        assert!(Arc::ptr_eq(&next.stage, &stage.upgrade().unwrap()));
    }

    #[test]
    fn applications_do_not_share() {
        // A context started after another one stopped in the same JVM reuses its stage ids.
        let caches = SharedMetadataCaches::default();
        let old = caches.cache_for(key(1, 0));
        drop(old);
        let mut other = key(1, 0);
        other.app_id = "app-2".to_string();
        let new = caches.cache_for(other);
        assert_eq!(new.len(), 0);
        assert!(!same_stage(&new, &caches.cache_for(key(1, 0))));
    }

    #[test]
    fn idle_retention_is_bounded_after_all_tasks_finish() {
        let caches = SharedMetadataCaches::default();
        let handles: Vec<_> = (0..16)
            .map(|stage| caches.cache_for(key(stage, 0)))
            .collect();
        let stages: Vec<_> = handles.iter().map(|h| Arc::downgrade(&h.stage)).collect();
        drop(handles);
        assert!(stages.iter().filter(|w| w.upgrade().is_some()).count() <= MAX_IDLE_CACHES);
    }

    #[test]
    fn idle_caches_are_bounded_but_running_ones_are_not_dropped() {
        let caches = SharedMetadataCaches::default();
        let running = caches.cache_for(key(0, 0));
        for stage in 1..=10 {
            drop(caches.cache_for(key(stage, 0)));
        }
        // The running stage plus the idle ones that fit.
        assert_eq!(caches.len(), 1 + MAX_IDLE_CACHES);
        assert!(same_stage(&running, &caches.cache_for(key(0, 0))));
    }

    #[test]
    fn clearing_frees_the_retained_caches_but_not_a_running_one() {
        let caches = SharedMetadataCaches::default();
        let running = caches.cache_for(key(0, 0));
        let idle = Arc::downgrade(&caches.cache_for(key(1, 0)).stage);
        assert!(idle.upgrade().is_some());

        caches.clear();
        assert!(idle.upgrade().is_none());
        assert_eq!(caches.len(), 0);
        assert_eq!(running.cache_limit(), DEFAULT_METADATA_CACHE_LIMIT);
        drop(running);
        assert_eq!(caches.len(), 0);
    }

    #[test]
    fn a_stage_cache_holds_one_task_limit_per_running_task() {
        let caches = SharedMetadataCaches::default();
        let tasks: Vec<_> = (0..3).map(|_| caches.cache_for(key(1, 0))).collect();
        assert_eq!(tasks[0].cache_limit(), 3 * DEFAULT_METADATA_CACHE_LIMIT);
        // The limit a task's CacheManager sets does not apply to the shared cache.
        tasks[0].update_cache_limit(DEFAULT_METADATA_CACHE_LIMIT / 2);
        assert_eq!(tasks[0].cache_limit(), 3 * DEFAULT_METADATA_CACHE_LIMIT);

        let stage = Arc::clone(&tasks[0].stage);
        drop(tasks);
        // Idle, retained for the next task, with one task's limit.
        assert_eq!(stage.cache.cache_limit(), DEFAULT_METADATA_CACHE_LIMIT);
        let next = caches.cache_for(key(1, 0));
        assert_eq!(next.cache_limit(), DEFAULT_METADATA_CACHE_LIMIT);
    }

    #[test]
    fn a_task_runtime_env_does_not_reset_the_stage_limit() {
        use datafusion::execution::cache::cache_manager::CacheManagerConfig;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        let caches = SharedMetadataCaches::default();
        let other = caches.cache_for(key(1, 0));
        let task: Arc<FileMetadataCache> = caches.cache_for(key(1, 0));
        // CacheManager::try_new applies the config's default limit to the cache it is given.
        RuntimeEnvBuilder::new()
            .with_cache_manager(
                CacheManagerConfig::default().with_file_metadata_cache(Some(Arc::clone(&task))),
            )
            .build()
            .unwrap();
        assert_eq!(task.cache_limit(), 2 * DEFAULT_METADATA_CACHE_LIMIT);
        drop(other);
        assert_eq!(task.cache_limit(), DEFAULT_METADATA_CACHE_LIMIT);
    }
}
