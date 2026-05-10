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

//! Process-wide cache of loaded UDF cdylibs. Same-path lookups always
//! return the same `Arc<LoadedLibrary>` for the lifetime of the process —
//! libraries are deliberately never unloaded (see module-level comment
//! on the `rust_udf` module for rationale).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use crate::execution::rust_udf::loader::{load, LoadedLibrary, LoaderError};

static CACHE: OnceLock<RwLock<HashMap<PathBuf, Arc<LoadedLibrary>>>> = OnceLock::new();

fn cache() -> &'static RwLock<HashMap<PathBuf, Arc<LoadedLibrary>>> {
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Get an already-loaded library, or load and cache it. The same path
/// always returns the same `Arc<LoadedLibrary>` for the lifetime of the
/// process — libraries are never unloaded. Calling `dlclose` while a
/// thread is mid-call would be a use-after-free, and there is no safe
/// point to unload without per-invocation refcounting we don't want on
/// the hot path. JVM lifetime equals library lifetime.
///
/// The path is canonicalized when possible so that `./lib.so` and
/// `/abs/lib.so` collapse to the same cache entry. Falls back to the
/// original path if canonicalization fails (e.g. file not found — the
/// load will then surface the real error).
pub fn get_or_load(path: impl AsRef<Path>) -> Result<Arc<LoadedLibrary>, LoaderError> {
    let canonical = path
        .as_ref()
        .canonicalize()
        .unwrap_or_else(|_| path.as_ref().to_path_buf());

    if let Some(lib) = cache().read().unwrap().get(&canonical).cloned() {
        return Ok(lib);
    }

    let mut w = cache().write().unwrap();
    // Re-check inside the write lock to avoid duplicate loads when two
    // threads race for the same path.
    if let Some(lib) = w.get(&canonical).cloned() {
        return Ok(lib);
    }
    let loaded = Arc::new(load(&canonical)?);
    w.insert(canonical, loaded.clone());
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::rust_udf::test_support::test_udfs_path;

    #[test]
    fn same_path_returns_same_arc() {
        let p = test_udfs_path();
        let a = get_or_load(&p).unwrap();
        let b = get_or_load(&p).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "expected pointer equality");
    }

    #[test]
    fn missing_path_propagates_error() {
        let err = get_or_load("/no/such/file.dylib").unwrap_err();
        // Should be a LoaderError::Open since canonicalize falls back to
        // the original path and load() then fails to open it.
        assert!(matches!(err, LoaderError::Open { .. }), "got: {err:?}");
    }
}
