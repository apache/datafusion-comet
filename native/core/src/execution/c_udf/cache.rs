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

//! Process-wide cache of loaded UDF cdylibs.
//!
//! Same-path lookups always return the same `Arc<LoadedLibrary>` for
//! the lifetime of the process — libraries are deliberately never
//! unloaded. Calling `dlclose` while a thread is mid-call would be a
//! use-after-free, and there is no safe point to unload without
//! per-invocation refcounting we don't want on the hot path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock};

use super::loader::{load, LoadedLibrary, LoaderError};

/// One library's slot in the cache. The mutex is held while the library loads, so concurrent
/// requests for the same library wait for the one load, and a request for any other library does
/// not wait at all.
type Slot<T> = Arc<Mutex<Option<Arc<T>>>>;

/// Loaded libraries keyed by path. Both the path as the caller wrote it and its canonical form map
/// to the same slot, so a symlink and its target share one library.
///
/// The map lock is only ever held for a lookup or an insert, never across a load. Neither it nor a
/// slot is treated as lost when a thread panicked while holding it: a map operation cannot leave
/// the map half-updated, and a slot is only filled once its load has succeeded, so a poisoned slot
/// still reads as "not loaded" and the next request retries. A library whose load panics therefore
/// fails its own queries and nothing else.
struct Cache<T> {
    slots: RwLock<HashMap<PathBuf, Slot<T>>>,
}

impl<T> Cache<T> {
    fn new() -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
        }
    }

    fn existing(&self, path: &Path) -> Option<Slot<T>> {
        self.slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .cloned()
    }

    fn slot(&self, path: &Path) -> Slot<T> {
        if let Some(slot) = self.existing(path) {
            return slot;
        }
        let mut slots = self.slots.write().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(slots.entry(path.to_path_buf()).or_default())
    }

    fn alias(&self, path: PathBuf, slot: &Slot<T>) {
        self.slots
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(path)
            .or_insert_with(|| Arc::clone(slot));
    }

    fn get_or_load_with<E>(
        &self,
        path: &Path,
        load: impl FnOnce(&Path) -> Result<T, E>,
    ) -> Result<Arc<T>, E> {
        if let Some(slot) = self.existing(path) {
            if let Some(lib) = lock(&slot).clone() {
                return Ok(lib);
            }
        }

        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let slot = self.slot(&canonical);
        let lib = {
            let mut guard = lock(&slot);
            match guard.as_ref() {
                Some(lib) => Arc::clone(lib),
                None => {
                    // An error leaves the slot empty, so a later request tries again.
                    let lib = Arc::new(load(&canonical)?);
                    *guard = Some(Arc::clone(&lib));
                    lib
                }
            }
        };
        if canonical != path {
            self.alias(path.to_path_buf(), &slot);
        }
        Ok(lib)
    }
}

fn lock<T>(slot: &Slot<T>) -> MutexGuard<'_, Option<Arc<T>>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

static CACHE: OnceLock<Cache<LoadedLibrary>> = OnceLock::new();

/// Get an already-loaded library, or load and cache it.
///
/// Loading one library does not block lookups of any other; see
/// <https://github.com/apache/datafusion-comet/issues/5297>.
pub fn get_or_load(path: impl AsRef<Path>) -> Result<Arc<LoadedLibrary>, LoaderError> {
    CACHE
        .get_or_init(Cache::new)
        .get_or_load_with(path.as_ref(), |p| load(p))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::c_udf::test_support::{test_udfs_path, BUILD_HINT};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn same_path_returns_same_arc() {
        let p = test_udfs_path();
        let a = get_or_load(&p).expect(BUILD_HINT);
        let b = get_or_load(&p).expect(BUILD_HINT);
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn missing_path_propagates_error() {
        let err = get_or_load("/no/such/file.dylib").unwrap_err();
        assert!(matches!(err, LoaderError::Open { .. }));
    }

    /// While one library is loading, a request for another library is answered. With a single
    /// lock held across the load, the second request would wait for the first to finish, and this
    /// test would time out.
    #[test]
    fn a_slow_load_does_not_block_another_library() {
        let cache = Arc::new(Cache::<&'static str>::new());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let slow = {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                cache.get_or_load_with::<()>(Path::new("/no/such/slow.so"), |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok("slow")
                })
            })
        };
        started_rx.recv_timeout(Duration::from_secs(30)).unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let _ = tx.send(
                    cache.get_or_load_with::<()>(Path::new("/no/such/fast.so"), |_| Ok("fast")),
                );
            });
        }
        let fast = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("loading a second library waited for the first")
            .unwrap();
        assert_eq!(*fast, "fast");

        release_tx.send(()).unwrap();
        assert_eq!(*slow.join().unwrap().unwrap(), "slow");
    }

    /// Requests for one library that arrive while it loads wait for that load and share its result.
    #[test]
    fn concurrent_requests_for_one_library_load_it_once() {
        let cache = Arc::new(Cache::<usize>::new());
        let loads = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let loads = Arc::clone(&loads);
                std::thread::spawn(move || {
                    cache
                        .get_or_load_with::<()>(Path::new("/no/such/one.so"), |_| {
                            std::thread::sleep(Duration::from_millis(50));
                            Ok(loads.fetch_add(1, Ordering::SeqCst))
                        })
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert!(results.iter().all(|r| Arc::ptr_eq(r, &results[0])));
    }

    /// A load that panics fails only that library: the cache keeps serving every other path, and a
    /// later request for the same path retries instead of panicking on a poisoned lock.
    #[test]
    fn a_load_that_panics_does_not_disable_the_cache() {
        let cache = Arc::new(Cache::<&'static str>::new());
        let bad = Path::new("/no/such/bad.so");

        let panicked = {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let _ = cache.get_or_load_with::<()>(bad, |_| panic!("static initializer failed"));
            })
            .join()
        };
        assert!(panicked.is_err());

        let other = cache
            .get_or_load_with::<()>(Path::new("/no/such/other.so"), |_| Ok("other"))
            .unwrap();
        assert_eq!(*other, "other");
        let retried = cache
            .get_or_load_with::<()>(bad, |_| Ok("recovered"))
            .unwrap();
        assert_eq!(*retried, "recovered");
    }

    /// A failed load is not remembered.
    #[test]
    fn a_failed_load_is_retried() {
        let cache = Cache::<&'static str>::new();
        let path = Path::new("/no/such/flaky.so");
        assert!(cache.get_or_load_with(path, |_| Err("boom")).is_err());
        assert_eq!(
            *cache.get_or_load_with::<()>(path, |_| Ok("ok")).unwrap(),
            "ok"
        );
    }

    /// A path seen for the first time that resolves to an already-loaded library, here a symlink,
    /// takes the branch that records the new path under the write lock. That branch used to take
    /// the lock while still holding its own read guard, and hung. The lookup runs on a thread of
    /// its own so a regression fails this test rather than hanging the run.
    #[cfg(unix)]
    #[test]
    fn new_path_to_a_loaded_library_returns_the_same_arc() {
        let canonical = test_udfs_path().canonicalize().expect(BUILD_HINT);
        let loaded = get_or_load(&canonical).expect(BUILD_HINT);

        let dir = tempfile::tempdir().expect("tempdir");
        let alias = dir.path().join(canonical.file_name().expect("file name"));
        std::os::unix::fs::symlink(&canonical, &alias).expect("symlink");

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(get_or_load(&alias));
        });
        let via_alias = rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("looking the library up through a symlink hung")
            .expect(BUILD_HINT);
        assert!(Arc::ptr_eq(&loaded, &via_alias));
    }
}
