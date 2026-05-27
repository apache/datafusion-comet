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
        if let Some(lib) = cache().read().unwrap().get(&canonical).cloned() {
            cache().write().unwrap().insert(raw, lib.clone());
            return Ok(lib);
        }
    }

    let mut w = cache().write().unwrap();
    if let Some(lib) = w.get(&canonical).cloned() {
        if canonical != raw {
            w.insert(raw, lib.clone());
        }
        return Ok(lib);
    }
    let loaded = Arc::new(load(&canonical)?);
    w.insert(canonical.clone(), loaded.clone());
    if canonical != raw {
        w.insert(raw, loaded.clone());
    }
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
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn missing_path_propagates_error() {
        let err = get_or_load("/no/such/file.dylib").unwrap_err();
        assert!(matches!(err, LoaderError::Open { .. }));
    }
}
