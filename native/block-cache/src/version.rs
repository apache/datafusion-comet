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

/// Identity of a cached file, as seen by the cache core.
///
/// `namespace` identifies the logical store (the caller derives it — e.g. Comet hashes
/// its `(url_key, config_hash)` store key). `path` is the object path within that store.
/// The pair is interned to a compact `u64` file id internally (see `cache.rs`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FileKey {
    pub namespace: u64,
    pub path: String,
}

impl FileKey {
    pub fn new(namespace: u64, path: impl Into<String>) -> Self {
        Self {
            namespace,
            path: path.into(),
        }
    }
}

/// The observed version of an object, captured at fetch time from the store's metadata.
///
/// Used to detect in-place overwrites: a miss fetch piggybacks the current version and,
/// if it disagrees with what the cache stored for the file, all cached blocks of that
/// file are invalidated (see `BlockCache`). The target workloads read immutable objects
/// (Iceberg/Delta/Hive data files), so in steady state versions never change and
/// validation costs nothing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileVersion {
    /// Object ETag, if the store reports one.
    pub e_tag: Option<String>,
    /// Last-modified time in epoch milliseconds, if known.
    pub last_modified: Option<i64>,
    /// Total object size in bytes (full object, not the fetched range).
    pub size: u64,
}

impl FileVersion {
    /// Whether `self` and `other` describe the same object version.
    ///
    /// Prefers the ETag when both sides report one (the strongest signal and what S3/GCS/
    /// Azure expose), and falls back to `(size, last_modified)` otherwise. Two versions
    /// that share an ETag but differ in size are treated as different — a defensive guard
    /// against weak/duplicated ETags.
    pub fn matches(&self, other: &FileVersion) -> bool {
        match (&self.e_tag, &other.e_tag) {
            (Some(a), Some(b)) => a == b && self.size == other.size,
            _ => self.size == other.size && self.last_modified == other.last_modified,
        }
    }
}
