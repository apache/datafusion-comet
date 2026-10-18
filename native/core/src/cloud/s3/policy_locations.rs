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

//! Policy locations from a `CometS3LocationScopedCredentialProvider`, and routing by them.
//!
//! Shared by the two stores that serve each request with the credential of the longest policy
//! location covering its path: `parquet::objectstore::location_scoped` for native Parquet reads,
//! and `execution::operators::iceberg_location_scoped` for native Iceberg reads and writes.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use object_store::path::Path;
use object_store::{Error, Result};
use tokio::sync::Mutex;

const STORE: &str = "LocationScopedS3";

/// The credential path used for paths that no returned location covers.
pub(crate) const ROOT_CREDENTIAL_PATH: &str = "/";

/// One snapshot of the provider's locations for a bucket.
pub(crate) struct LocationIndex {
    /// Counts refresh attempts, including failed ones.
    generation: u64,
    /// Canonical location to credential path: the location as the provider returned it, with a
    /// leading slash.
    locations: Arc<HashMap<Path, String>>,
    /// Why the attempt that produced this snapshot failed, when it kept the previous locations.
    failure: Option<Arc<str>>,
}

impl LocationIndex {
    pub(crate) fn new(generation: u64, locations: Vec<String>) -> Result<Self> {
        let mut index = HashMap::with_capacity(locations.len());
        for location in locations {
            // Request paths are percent-decoded the same way, so both sides compare as raw keys.
            let canonical = Path::from_url_path(&location).map_err(|e| Error::Generic {
                store: STORE,
                source: format!("Invalid policy location {location:?}: {e}").into(),
            })?;
            // A duplicate keeps the first spelling, which is the path the provider is given.
            index
                .entry(canonical)
                .or_insert_with(|| credential_path(&location));
        }
        Ok(Self {
            generation,
            locations: Arc::new(index),
            failure: None,
        })
    }

    /// The snapshot after a failed refresh: the same locations under the next generation, with the
    /// failure recorded for the requests routed before it.
    fn after_failure(&self, failure: &str) -> Self {
        Self {
            generation: self.generation + 1,
            locations: Arc::clone(&self.locations),
            failure: Some(failure.into()),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn len(&self) -> usize {
        self.locations.len()
    }

    /// Returns the credential path of the longest location that covers `path`.
    pub(crate) fn route(&self, path: &Path) -> &str {
        let mut longest = self
            .locations
            .get(&Path::default())
            .map_or(ROOT_CREDENTIAL_PATH, String::as_str);
        let mut prefix = Path::default();
        for part in path.parts() {
            prefix = prefix.join(part);
            if let Some(credential_path) = self.locations.get(&prefix) {
                longest = credential_path;
            }
        }
        longest
    }
}

fn credential_path(location: &str) -> String {
    if location.starts_with('/') {
        location.to_string()
    } else {
        format!("/{location}")
    }
}

/// Why [`PolicyLocations::refresh`] failed.
pub(crate) enum RefreshError<E> {
    /// This call fetched the locations, and the fetch failed.
    Fetch(E),
    /// A refresh attempted after the caller's request was routed failed, and the caller shares
    /// its outcome. Carries that failure's message.
    Earlier(Arc<str>),
}

/// The current snapshot of a bucket's locations, fetched again at most once per generation.
pub(crate) struct PolicyLocations {
    index: RwLock<Arc<LocationIndex>>,
    /// Serializes refreshes, so the failed requests from one snapshot share one attempt.
    refresh_lock: Mutex<()>,
}

impl PolicyLocations {
    pub(crate) fn new(index: LocationIndex) -> Self {
        Self {
            index: RwLock::new(Arc::new(index)),
            refresh_lock: Mutex::new(()),
        }
    }

    pub(crate) fn current(&self) -> Arc<LocationIndex> {
        Arc::clone(&self.index.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Fetches the locations again for a request routed from the snapshot of `routed_generation`,
    /// unless a refresh was attempted after it was routed, in which case this call shares that
    /// attempt's outcome. `fetch` receives the generation of the snapshot it builds.
    pub(crate) async fn refresh<E: fmt::Display>(
        &self,
        routed_generation: u64,
        fetch: impl FnOnce(u64) -> std::result::Result<LocationIndex, E>,
    ) -> std::result::Result<(), RefreshError<E>> {
        let _refresh = self.refresh_lock.lock().await;
        let current = self.current();
        if current.generation != routed_generation {
            return match &current.failure {
                Some(failure) => Err(RefreshError::Earlier(Arc::clone(failure))),
                None => Ok(()),
            };
        }
        let (index, result) = match fetch(current.generation + 1) {
            Ok(index) => (index, Ok(())),
            Err(e) => (
                current.after_failure(&e.to_string()),
                Err(RefreshError::Fetch(e)),
            ),
        };
        *self.index.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(index);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_to_the_longest_covering_location() {
        let index = LocationIndex::new(
            0,
            vec![
                "warehouse/sales".into(),
                "warehouse/sales/eu/".into(),
                "/warehouse/finance".into(),
            ],
        )
        .unwrap();
        let route = |path: &str| index.route(&Path::from(path)).to_string();
        assert_eq!(route("warehouse/sales/a.parquet"), "/warehouse/sales");
        assert_eq!(
            route("warehouse/sales/eu/b.parquet"),
            "/warehouse/sales/eu/"
        );
        assert_eq!(route("warehouse/sales"), "/warehouse/sales");
        assert_eq!(route("warehouse/finance/c.parquet"), "/warehouse/finance");
        // Locations match whole segments, so a sibling that shares a name prefix is not covered.
        assert_eq!(route("warehouse/sales_eu/d.parquet"), "/");
        assert_eq!(route("warehouse/e.parquet"), "/");
        assert_eq!(route("other/f.parquet"), "/");
    }

    #[test]
    fn compares_locations_and_paths_percent_decoded() {
        // A URI path escapes '%' as %25, so Spark's %3A partition escape arrives as %253A.
        let index = LocationIndex::new(0, vec!["tbl/ts=2024-01-01%2000%253A00".into()]).unwrap();
        let spark_path = Path::from_url_path("/tbl/ts=2024-01-01%2000%253A00/part-0.parquet");
        assert_eq!(
            index.route(&spark_path.unwrap()),
            "/tbl/ts=2024-01-01%2000%253A00",
            "the provider is given the location as it was returned"
        );
        // Decoded once, %3A is ':', which names a different key.
        let other_key = Path::from_url_path("/tbl/ts=2024-01-01%2000%3A00/part-0.parquet");
        assert_eq!(index.route(&other_key.unwrap()), "/");
    }

    #[test]
    fn keeps_the_first_spelling_of_a_duplicate_location() {
        let index = LocationIndex::new(0, vec!["a/b".into(), "/a/b/".into()]).unwrap();
        assert_eq!(index.locations.len(), 1);
        assert_eq!(index.route(&Path::from("a/b/c")), "/a/b");
    }

    #[test]
    fn accepts_the_bucket_root_as_a_location() {
        let index = LocationIndex::new(0, vec!["".into(), "a".into()]).unwrap();
        assert!(index.locations.contains_key(&Path::default()));
        assert_eq!(index.route(&Path::from("x/y")), "/");
        assert_eq!(index.route(&Path::from("a/y")), "/a");
    }

    #[test]
    fn rejects_locations_that_are_not_bucket_paths() {
        // Empty and relative segments, a URI (its "//" is an empty segment), a control character
        // once decoded, and bytes that do not decode to UTF-8.
        for location in ["a//b", "a/../b", "s3://bucket/a", "a/%0Ab", "a/%FF"] {
            assert!(
                LocationIndex::new(0, vec![location.into()]).is_err(),
                "{location:?} should be rejected"
            );
        }
    }
}
