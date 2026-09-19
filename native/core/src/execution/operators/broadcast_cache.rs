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

//! Coordinates concurrent tasks preparing the same immutable broadcast build.
//! The first task for a compatible key loads it; other tasks wait and receive
//! their own strong references. This module owns lookup metadata, not Arrow
//! buffers or Spark storage grants.
//!
//! Successful entries keep only weak references, so the last active probe can
//! drop the prepared hash table. Its owner returns the Spark storage charge when
//! the last lease drops or, if earlier, on executor retirement. A later task
//! wave may build the same table again. Resource failures briefly reject
//! matching loads to avoid repeated partial work; other errors and canceled
//! loaders leave the key retryable. Retiring a SparkEnv generation clears its
//! lookups without revoking leases already held by active probes.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use datafusion::common::{resources_datafusion_err, DataFusionError, Result};
use futures::channel::oneshot;
use parking_lot::Mutex;

/// Share failed admission across a task wave without permanently disabling reuse
/// when executor storage pressure changes. Rejections occupy bounded cache slots.
const RESOURCE_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Identifies a compatible materialization: the Spark broadcast, its declared
/// build and probe schemas, and ordered build key columns must all match.
/// SparkEnv generation is scoped by the enclosing cache, not by this key.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct BuildKey {
    pub broadcast_id: i64,
    pub schema: SchemaRef,
    pub probe_schema: SchemaRef,
    pub key_columns: Vec<usize>,
}

/// Per-key state: one loader with waiters, a weak active build, or a short-lived
/// resource rejection. No entry owns a prepared build after loading finishes.
#[derive(Debug)]
enum Entry<T> {
    Loading(Vec<oneshot::Sender<Option<Arc<T>>>>),
    Ready(Weak<T>),
    Rejected(Instant),
}

/// Bounds keys, unfinished loads, weak ready entries and temporary rejections
/// without retaining prepared builds. The payload byte cap belongs to the
/// allocation pool; callers own strong references returned by `get_or_load`.
#[derive(Debug)]
pub(super) struct BuildCache<T> {
    entries: Mutex<HashMap<BuildKey, Entry<T>>>,
    max_entries: usize,
    retired: AtomicBool,
}

impl<T> BuildCache<T> {
    /// Creates an empty cache. A zero limit disables admission. Does not allocate
    /// payload memory or install any task/context ownership.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries,
            retired: AtomicBool::new(false),
        }
    }

    /// Returns an active build or elects one loader for this task wave. Waiting
    /// tasks receive strong references before the loader can drop its build.
    /// A canceled load wakes waiters to retry; a resource error from the loader
    /// is shared briefly so peers take ordinary fallback instead of repeating
    /// partial preparation. Other errors remain uncached. Entry-capacity failure
    /// occurs before `load` runs. The boolean is true on a hit or waited load.
    pub async fn get_or_load<F, Fut>(&self, key: BuildKey, load: F) -> Result<(Arc<T>, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<T>>>,
    {
        enum Action<T> {
            Hit(Arc<T>),
            Wait(oneshot::Receiver<Option<Arc<T>>>),
            Load,
        }
        let mut load = Some(load);
        loop {
            let action = {
                let mut entries = self.entries.lock();
                if self.retired.load(Ordering::Relaxed) {
                    return Err(resources_datafusion_err!(
                        "Broadcast build cache is retired"
                    ));
                }
                match entries.get_mut(&key) {
                    Some(Entry::Ready(value)) => match value.upgrade() {
                        Some(value) => Action::Hit(value),
                        None => {
                            // The last probe released its build; this key may
                            // elect a new loader for a later task wave.
                            entries.remove(&key);
                            continue;
                        }
                    },
                    Some(Entry::Rejected(retry_at)) => {
                        if Instant::now() < *retry_at {
                            return Err(DataFusionError::ResourcesExhausted(
                                "Broadcast build cache admission rejected".into(),
                            ));
                        }
                        // Expired rejections contain no payload or task ownership.
                        // Re-enter election so only one caller retries admission.
                        entries.remove(&key);
                        continue;
                    }
                    Some(Entry::Loading(waiters)) => {
                        let (sender, receiver) = oneshot::channel();
                        waiters.push(sender);
                        Action::Wait(receiver)
                    }
                    None => {
                        if entries.len() >= self.max_entries {
                            // The cap also counts bookkeeping for old waves;
                            // reclaim dead weak refs and expired rejections.
                            let now = Instant::now();
                            entries.retain(|_, entry| match entry {
                                Entry::Loading(_) => true,
                                Entry::Ready(value) => value.strong_count() != 0,
                                Entry::Rejected(retry_at) => *retry_at > now,
                            });
                            if entries.len() >= self.max_entries {
                                return Err(resources_datafusion_err!(
                                    "Broadcast build cache entry limit reached"
                                ));
                            }
                        }
                        entries.insert(key.clone(), Entry::Loading(vec![]));
                        Action::Load
                    }
                }
            };
            match action {
                Action::Hit(value) => return Ok((value, true)),
                Action::Wait(receiver) => {
                    if let Ok(Some(value)) = receiver.await {
                        if !self.retired.load(Ordering::Relaxed) {
                            return Ok((value, true));
                        }
                    }
                    // Shared rejection or loader cancellation re-enters election.
                }
                Action::Load => {
                    let guard = LoadGuard {
                        cache: self,
                        key: key.clone(),
                        active: true,
                    };
                    match load.take().expect("only the elected caller loads")().await {
                        Ok(value) => {
                            guard.publish(Entry::Ready(Arc::downgrade(&value)), Some(&value));
                            return Ok((value, false));
                        }
                        Err(DataFusionError::ResourcesExhausted(error)) => {
                            guard.publish(
                                Entry::Rejected(Instant::now() + RESOURCE_RETRY_DELAY),
                                None,
                            );
                            return Err(DataFusionError::ResourcesExhausted(error));
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    /// Retires all lookup entries on native-runtime shutdown. Active leases keep
    /// their values and charges; loading callers cannot republish after retirement.
    /// Dropping senders wakes waiting tasks without retaining any task future.
    pub fn clear(&self) {
        let entries = {
            let mut entries = self.entries.lock();
            self.retired.store(true, Ordering::Relaxed);
            std::mem::take(&mut *entries)
        };
        drop(entries);
    }
}

/// A task-owned loading slot. Cancellation removes it and wakes waiters. The
/// guard becomes inactive before publishing a value or rejection.
struct LoadGuard<'a, T> {
    cache: &'a BuildCache<T>,
    key: BuildKey,
    active: bool,
}

impl<T> LoadGuard<'_, T> {
    /// Publishes a weak lookup or rejection and hands the build to waiting tasks.
    /// Runtime shutdown may already have retired the slot; canceled loaders
    /// cannot publish into a newer environment.
    fn publish(mut self, entry: Entry<T>, value: Option<&Arc<T>>) {
        let removed = {
            let mut entries = self.cache.entries.lock();
            let removed = if matches!(entries.get(&self.key), Some(Entry::Loading(_))) {
                entries.insert(self.key.clone(), entry)
            } else {
                None
            };
            self.active = false;
            removed
        };
        if let Some(Entry::Loading(waiters)) = removed {
            for waiter in waiters {
                let _ = waiter.send(value.cloned());
            }
        }
    }
}

impl<T> Drop for LoadGuard<'_, T> {
    /// Retracts an unfinished load and wakes waiters. Published slots are untouched.
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let removed = {
            let mut entries = self.cache.entries.lock();
            if matches!(entries.get(&self.key), Some(Entry::Loading(_))) {
                entries.remove(&self.key)
            } else {
                None
            }
        };
        drop(removed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Builds a stable fake materialization key; tests own separate caches.
    fn key(id: i64) -> BuildKey {
        BuildKey {
            broadcast_id: id,
            schema: Arc::new(Schema::empty()),
            probe_schema: Arc::new(Schema::empty()),
            key_columns: vec![],
        }
    }

    #[tokio::test]
    async fn waiting_probe_retains_build_then_next_wave_rebuilds() {
        use futures::{poll, FutureExt};

        let cache = BuildCache::new(1);
        let loads = AtomicUsize::new(0);
        let mut loader = Box::pin(cache.get_or_load(key(1), || async {
            loads.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(Arc::new(7))
        }));
        assert!(poll!(&mut loader).is_pending());
        let mut waiter = Box::pin(cache.get_or_load(key(1), || async {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(8))
        }));
        assert!(poll!(&mut waiter).is_pending());

        let (built, hit) = loader.await.unwrap();
        assert!(!hit);
        let weak = Arc::downgrade(&built);
        drop(built);
        let (shared, hit) = waiter.now_or_never().unwrap().unwrap();
        assert!(hit);
        assert_eq!(*shared, 7);
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        drop(shared);
        assert!(weak.upgrade().is_none());

        let (next, hit) = cache
            .get_or_load(key(1), || async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(9))
            })
            .await
            .unwrap();
        assert!(!hit);
        assert_eq!(*next, 9);
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn released_build_frees_lookup_slot() {
        let cache = BuildCache::new(2);
        let (released, _) = cache
            .get_or_load(key(1), || async { Ok(Arc::new(1)) })
            .await
            .unwrap();
        drop(released);
        let (active, _) = cache
            .get_or_load(key(2), || async { Ok(Arc::new(2)) })
            .await
            .unwrap();
        let (new, _) = cache
            .get_or_load(key(3), || async { Ok(Arc::new(3)) })
            .await
            .unwrap();
        let entries = cache.entries.lock();
        assert_eq!(entries.len(), 2);
        assert!(!entries.contains_key(&key(1)));
        assert!(entries.contains_key(&key(2)));
        assert!(entries.contains_key(&key(3)));
        assert_eq!(*active, 2);
        assert_eq!(*new, 3);
    }

    /// A failed loader rejects its waiters without repeating preparation. The
    /// next task can load the same key after the short resource cooldown.
    #[tokio::test]
    async fn admission_failure_is_shared_then_retried() {
        let cache = BuildCache::<usize>::new(1);
        let loads = AtomicUsize::new(0);
        let (failed, waiter) = futures::join!(
            cache.get_or_load(key(1), || async {
                loads.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Err(DataFusionError::ResourcesExhausted("full".into()))
            }),
            cache.get_or_load(key(1), || async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(7))
            })
        );
        assert!(matches!(
            failed,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert!(matches!(
            waiter,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert!(cache
            .get_or_load(key(1), || async { panic!("cooldown must bypass loading") })
            .await
            .is_err());
        {
            let mut entries = cache.entries.lock();
            let Entry::Rejected(retry_at) = entries.get_mut(&key(1)).unwrap() else {
                panic!("resource denial must install a rejection");
            };
            *retry_at = Instant::now();
        }
        let (value, hit) = cache
            .get_or_load(key(1), || async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(7))
            })
            .await
            .unwrap();
        assert!(!hit);
        assert_eq!(*value, 7);
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    /// Non-resource errors remain visible to their caller and are not cached;
    /// an already-waiting consumer can immediately attempt its own load.
    #[tokio::test]
    async fn data_error_is_not_shared_as_resource_fallback() {
        let cache = BuildCache::new(1);
        let (failed, retried) = futures::join!(
            cache.get_or_load(key(1), || async {
                tokio::task::yield_now().await;
                Err(DataFusionError::Execution("invalid input".into()))
            }),
            cache.get_or_load(key(1), || async { Ok(Arc::new(7)) })
        );
        assert!(matches!(failed, Err(DataFusionError::Execution(error))
            if error == "invalid input"));
        let (value, hit) = retried.unwrap();
        assert!(!hit);
        assert_eq!(*value, 7);
    }

    #[tokio::test]
    async fn canceled_loader_wakes_waiter_and_allows_retry() {
        use futures::{poll, FutureExt};
        let cache = BuildCache::new(1);
        let mut loader = Box::pin(cache.get_or_load(key(1), futures::future::pending));
        assert!(poll!(&mut loader).is_pending());
        let mut waiter = Box::pin(cache.get_or_load(key(1), || async { Ok(Arc::new(7)) }));
        assert!(poll!(&mut waiter).is_pending());
        drop(loader);
        assert_eq!(*waiter.now_or_never().unwrap().unwrap().0, 7);
    }

    #[tokio::test]
    async fn leases_survive_retirement_and_bound_admission() {
        let cache = BuildCache::new(1);
        let (lease, _) = cache
            .get_or_load(key(1), || async { Ok(Arc::new(9)) })
            .await
            .unwrap();
        assert!(cache
            .get_or_load(key(2), || async { panic!("must not load") })
            .await
            .is_err());
        cache.clear();
        assert_eq!(*lease, 9);
        assert!(cache
            .get_or_load(key(2), || async { panic!("retired cache cannot load") })
            .await
            .is_err());
        assert!(cache.entries.lock().is_empty());
    }
}
