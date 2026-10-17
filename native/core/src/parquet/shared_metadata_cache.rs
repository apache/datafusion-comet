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
//! A stage attempt is part of the key because a retried stage may read a changed file.
//!
//! The registry keeps the cache of the stages that still have a running task, plus the last
//! [`MAX_IDLE_CACHES`] that do not. The idle ones matter when an executor runs one task at a
//! time: the next task of the stage starts after the previous one released its plan.

use crate::execution::spark_config::{
    SparkConfig, COMET_PARQUET_SHARED_METADATA_CACHE_ENABLED, COMET_STAGE_ATTEMPT, COMET_STAGE_ID,
};
use datafusion::execution::cache::cache_manager::{
    FileMetadataCache, DEFAULT_METADATA_CACHE_LIMIT,
};
use datafusion::execution::cache::default_cache::DefaultCache;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// How many caches of stages with no running task the registry holds on to.
const MAX_IDLE_CACHES: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StageKey {
    pub stage_id: i32,
    pub stage_attempt: i32,
}

struct Entry {
    key: StageKey,
    cache: Arc<FileMetadataCache>,
}

#[derive(Default)]
pub(crate) struct SharedMetadataCaches {
    /// Ordered from least to most recently requested.
    entries: Mutex<Vec<Entry>>,
}

impl SharedMetadataCaches {
    /// The cache shared by the tasks of `key`, created if no task of that stage has asked for one
    /// yet.
    fn cache_for(&self, key: StageKey) -> Arc<FileMetadataCache> {
        let mut entries = self.entries.lock().unwrap();
        let entry = match entries.iter().position(|e| e.key == key) {
            Some(pos) => entries.remove(pos),
            None => Entry {
                key,
                cache: Arc::new(
                    DefaultCache::new(DEFAULT_METADATA_CACHE_LIMIT)
                        .with_name("SharedFileMetadataCache"),
                ),
            },
        };
        let cache = Arc::clone(&entry.cache);
        entries.push(entry);

        // The registry holds one reference; a task holds another for as long as its
        // `RuntimeEnv` lives. The newest entry also has the reference returned from here, which
        // the task takes over, so it counts as idle at two. The newest entry is never evicted.
        let newest = entries.len() - 1;
        let is_idle = |index: usize, entry: &Entry| {
            Arc::strong_count(&entry.cache) == if index == newest { 2 } else { 1 }
        };
        let mut idle = entries
            .iter()
            .enumerate()
            .filter(|(i, e)| is_idle(*i, e))
            .count();
        let mut index = 0;
        entries.retain(|e| {
            let evict = index != newest && idle > MAX_IDLE_CACHES && is_idle(index, e);
            index += 1;
            if evict {
                idle -= 1;
            }
            !evict
        });
        cache
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// The cache the task with these Spark configs should use, or `None` when sharing is off or the
/// task's stage is unknown, in which case DataFusion's per-task cache applies.
pub(crate) fn shared_metadata_cache_for_task(
    spark_config: &HashMap<String, String>,
) -> Option<Arc<FileMetadataCache>> {
    static CACHES: OnceLock<SharedMetadataCaches> = OnceLock::new();

    if !spark_config.get_bool(COMET_PARQUET_SHARED_METADATA_CACHE_ENABLED) {
        return None;
    }
    let parse = |name: &str| spark_config.get(name)?.parse::<i32>().ok();
    let key = StageKey {
        stage_id: parse(COMET_STAGE_ID)?,
        stage_attempt: parse(COMET_STAGE_ATTEMPT)?,
    };
    Some(
        CACHES
            .get_or_init(SharedMetadataCaches::default)
            .cache_for(key),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(stage_id: i32, stage_attempt: i32) -> StageKey {
        StageKey {
            stage_id,
            stage_attempt,
        }
    }

    #[test]
    fn tasks_of_one_stage_share_a_cache() {
        let caches = SharedMetadataCaches::default();
        let a = caches.cache_for(key(1, 0));
        let b = caches.cache_for(key(1, 0));
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn stages_and_attempts_do_not_share() {
        let caches = SharedMetadataCaches::default();
        let first = caches.cache_for(key(1, 0));
        let other_stage = caches.cache_for(key(2, 0));
        let retry = caches.cache_for(key(1, 1));
        assert!(!Arc::ptr_eq(&first, &other_stage));
        assert!(!Arc::ptr_eq(&first, &retry));
    }

    #[test]
    fn an_idle_cache_is_kept_for_the_next_task_of_its_stage() {
        let caches = SharedMetadataCaches::default();
        let first = caches.cache_for(key(1, 0));
        let address = Arc::as_ptr(&first) as *const () as usize;
        drop(first);
        let next = caches.cache_for(key(1, 0));
        assert_eq!(Arc::as_ptr(&next) as *const () as usize, address);
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
        assert!(Arc::ptr_eq(&running, &caches.cache_for(key(0, 0))));
    }
}
