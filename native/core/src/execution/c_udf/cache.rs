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
use std::sync::{Arc, OnceLock, RwLock};

use super::loader::{load, LoadedLibrary, LoaderError};

static CACHE: OnceLock<RwLock<HashMap<PathBuf, Arc<LoadedLibrary>>>> = OnceLock::new();

fn cache() -> &'static RwLock<HashMap<PathBuf, Arc<LoadedLibrary>>> {
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Get an already-loaded library, or load and cache it.
pub fn get_or_load(path: impl AsRef<Path>) -> Result<Arc<LoadedLibrary>, LoaderError> {
    let raw = path.as_ref().to_path_buf();

    if let Some(lib) = cache().read().unwrap().get(&raw).cloned() {
        return Ok(lib);
    }

    let canonical = raw.canonicalize().unwrap_or_else(|_| raw.clone());
    if canonical != raw {
        // A statement of its own, so the read guard is dropped before the write lock is taken. In
        // an `if let` scrutinee it would live until the end of the block, and this thread would
        // wait on its own read lock forever.
        let hit = cache().read().unwrap().get(&canonical).cloned();
        if let Some(lib) = hit {
            cache().write().unwrap().insert(raw, Arc::clone(&lib));
            return Ok(lib);
        }
    }

    // The write lock is held across `load`, which runs the cdylib's static initializers, so a slow
    // load of one library blocks lookups of every other path. See
    // <https://github.com/apache/datafusion-comet/issues/5297>.
    let mut w = cache().write().unwrap();
    if let Some(lib) = w.get(&canonical).cloned() {
        if canonical != raw {
            w.insert(raw, Arc::clone(&lib));
        }
        return Ok(lib);
    }
    let loaded = Arc::new(load(&canonical)?);
    w.insert(canonical.clone(), Arc::clone(&loaded));
    if canonical != raw {
        w.insert(raw, Arc::clone(&loaded));
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::c_udf::test_support::{test_udfs_path, BUILD_HINT};

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
