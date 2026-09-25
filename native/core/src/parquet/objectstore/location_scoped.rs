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

//! The object store for a `CometS3LocationScopedCredentialProvider`.
//!
//! `object_store::CredentialProvider::get_credential` receives no request path, so one S3 store
//! presents one credential. A bucket whose policies differ by location needs a store per location
//! and something that picks the right one for each request. [`LocationScopedObjectStore`] is
//! registered once per bucket in place of a plain S3 store. It keeps the provider's policy
//! locations and serves each request with the store of the longest location that covers the
//! request's path, compared one path segment at a time. The bucket root is an implicit location
//! that covers every other path. A location's store is built the first time a request needs it and
//! kept for the life of this store.
//!
//! The locations are a snapshot, so a 403 can mean a location was added or removed after it was
//! taken. A read (`get_opts` or `get_ranges`) that gets a 403 fetches the locations again, unless
//! another read already tried since this one was routed, and retries once if its path now routes to
//! a different location; otherwise the 403 is returned. A failed fetch fails every read that shared
//! it. Other operations route by path without retrying, because Comet only reads through this store.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};
use tokio::sync::Mutex;

const STORE: &str = "LocationScopedS3";

/// The credential path used for paths that no returned location covers.
const ROOT_CREDENTIAL_PATH: &str = "/";

/// Fetches the provider's current policy locations for the bucket.
pub(crate) type LocationSource = Arc<dyn Fn() -> Result<Vec<String>> + Send + Sync>;

/// Builds the store for one location from the path passed to `getCredentialsForPath`.
pub(crate) type LocationStoreFactory =
    Arc<dyn Fn(&str) -> Result<Arc<dyn ObjectStore>> + Send + Sync>;

/// One snapshot of the provider's locations.
struct LocationIndex {
    /// Counts refresh attempts, including failed ones.
    generation: u64,
    /// Canonical location to credential path: the location as the provider returned it, with a
    /// leading slash.
    locations: Arc<HashMap<Path, String>>,
    /// Why the attempt that produced this snapshot failed, when it kept the previous locations.
    failure: Option<Arc<str>>,
}

impl LocationIndex {
    fn new(generation: u64, locations: Vec<String>) -> Result<Self> {
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
    fn after_failure(&self, failure: &Error) -> Self {
        Self {
            generation: self.generation + 1,
            locations: Arc::clone(&self.locations),
            failure: Some(failure.to_string().into()),
        }
    }

    /// Returns the credential path of the longest location that covers `path`.
    fn route(&self, path: &Path) -> &str {
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

fn is_forbidden(err: &Error) -> bool {
    matches!(err, Error::PermissionDenied { .. })
}

/// The store chosen for one request, and the snapshot it was chosen from.
struct Route {
    generation: u64,
    credential_path: String,
    store: Arc<dyn ObjectStore>,
}

struct Inner {
    bucket: String,
    source: LocationSource,
    factory: LocationStoreFactory,
    index: RwLock<Arc<LocationIndex>>,
    /// Serializes refreshes, so the 403s from one snapshot share one attempt.
    refresh_lock: Mutex<()>,
    /// Location stores by credential path.
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl Inner {
    fn index(&self) -> Arc<LocationIndex> {
        Arc::clone(&self.index.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn route(&self, path: &Path) -> Result<Route> {
        let index = self.index();
        let credential_path = index.route(path).to_string();
        let store = self.store(&credential_path)?;
        Ok(Route {
            generation: index.generation,
            credential_path,
            store,
        })
    }

    fn store(&self, credential_path: &str) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self
            .stores
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(credential_path)
        {
            return Ok(Arc::clone(store));
        }
        // Build outside the lock, since building creates a bridge through JNI. When two requests
        // race to build the same location, the first insert wins and the other store is dropped.
        let store = (self.factory)(credential_path)?;
        let mut stores = self.stores.write().unwrap_or_else(PoisonError::into_inner);
        Ok(Arc::clone(
            stores.entry(credential_path.to_string()).or_insert(store),
        ))
    }

    /// Returns the route to retry on after `failed` returned the 403 `err` for `path`, or the error
    /// the request should return.
    async fn retry_route(&self, path: &Path, failed: &Route, err: Error) -> Result<Route> {
        if let Err(refresh) = self.refresh(failed).await {
            return Err(Error::Generic {
                store: STORE,
                source: format!(
                    "{err}; fetching the policy locations for bucket {} again failed: {refresh}",
                    self.bucket
                )
                .into(),
            });
        }
        let route = self.route(path)?;
        if route.credential_path == failed.credential_path {
            return Err(err);
        }
        Ok(route)
    }

    /// Fetches the locations again, unless a refresh was attempted after `failed` was routed, in
    /// which case this request shares that attempt's outcome.
    async fn refresh(&self, failed: &Route) -> Result<()> {
        let _refresh = self.refresh_lock.lock().await;
        let current = self.index();
        if current.generation != failed.generation {
            return match &current.failure {
                Some(failure) => Err(Error::Generic {
                    store: STORE,
                    source: failure.to_string().into(),
                }),
                None => Ok(()),
            };
        }
        let next = (self.source)()
            .and_then(|locations| LocationIndex::new(current.generation + 1, locations));
        let (index, result) = match next {
            Ok(index) => (index, Ok(())),
            Err(e) => (current.after_failure(&e), Err(e)),
        };
        *self.index.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(index);
        result
    }
}

/// Serves each request with the store of the longest policy location covering its path. See the
/// module documentation.
pub struct LocationScopedObjectStore {
    inner: Arc<Inner>,
}

impl LocationScopedObjectStore {
    /// `locations` is the provider's first answer for `bucket`; `source` fetches it again after a
    /// 403, and `factory` builds a location's store on first use. Fails if a location is not a
    /// valid path.
    pub(crate) fn new(
        bucket: String,
        locations: Vec<String>,
        source: LocationSource,
        factory: LocationStoreFactory,
    ) -> Result<Self> {
        let index = LocationIndex::new(0, locations)?;
        Ok(Self {
            inner: Arc::new(Inner {
                bucket,
                source,
                factory,
                index: RwLock::new(Arc::new(index)),
                refresh_lock: Mutex::new(()),
                stores: RwLock::new(HashMap::new()),
            }),
        })
    }
}

impl fmt::Debug for LocationScopedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocationScopedObjectStore")
            .field("bucket", &self.inner.bucket)
            .field("locations", &self.inner.index().locations.len())
            .finish()
    }
}

impl fmt::Display for LocationScopedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocationScopedS3({})", self.inner.bucket)
    }
}

#[async_trait]
impl ObjectStore for LocationScopedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let route = self.inner.route(location)?;
        route.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let route = self.inner.route(location)?;
        route.store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let route = self.inner.route(location)?;
        match route.store.get_opts(location, options.clone()).await {
            Err(e) if is_forbidden(&e) => {
                let retry = self.inner.retry_route(location, &route, e).await?;
                retry.store.get_opts(location, options).await
            }
            other => other,
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let route = self.inner.route(location)?;
        match route.store.get_ranges(location, ranges).await {
            Err(e) if is_forbidden(&e) => {
                let retry = self.inner.retry_route(location, &route, e).await?;
                retry.store.get_ranges(location, ranges).await
            }
            other => other,
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = Arc::clone(&self.inner);
        locations
            .and_then(move |location| {
                let route = inner.route(&location);
                async move {
                    route?.store.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        match self.inner.route(prefix.unwrap_or(&Path::default())) {
            Ok(route) => route.store.list(prefix),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        match self.inner.route(prefix.unwrap_or(&Path::default())) {
            Ok(route) => route.store.list_with_offset(prefix, offset),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let route = self.inner.route(prefix.unwrap_or(&Path::default()))?;
        route.store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let route = self.inner.route(from)?;
        route.store.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let route = self.inner.route(from)?;
        route.store.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::sync::Barrier;

    /// What S3 allows for one credential: reads under `allowed` succeed and every other read gets a
    /// 403. A success is reported as `NotFound` carrying the credential path, which shows which
    /// location served the request without producing data.
    #[derive(Debug)]
    struct CredentialView {
        credential_path: String,
        allowed: Vec<Path>,
        gets: AtomicUsize,
        /// When set, each read waits here first, so a test can hold requests in flight together.
        gate: Option<Arc<Barrier>>,
    }

    impl fmt::Display for CredentialView {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CredentialView({})", self.credential_path)
        }
    }

    #[async_trait]
    impl ObjectStore for CredentialView {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> Result<PutResult> {
            unimplemented!("reads only")
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            unimplemented!("reads only")
        }

        async fn get_opts(&self, location: &Path, _options: GetOptions) -> Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.wait().await;
            }
            let source = self.credential_path.clone().into();
            let path = location.to_string();
            if self.allowed.iter().any(|p| location.prefix_matches(p)) {
                Err(Error::NotFound { path, source })
            } else {
                Err(Error::PermissionDenied { path, source })
            }
        }

        fn delete_stream(
            &self,
            _locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            unimplemented!("reads only")
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            unimplemented!("reads only")
        }

        async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
            unimplemented!("reads only")
        }

        async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
            unimplemented!("reads only")
        }
    }

    /// A provider with changeable locations whose credentials read exactly the prefixes in
    /// `grants`.
    struct Provider {
        locations: StdMutex<Vec<String>>,
        grants: HashMap<String, Vec<Path>>,
        fail_refresh: AtomicBool,
        /// How long fetching the locations takes, so concurrent refreshes overlap.
        refresh_delay: Option<Duration>,
        refreshes: AtomicUsize,
        /// Credential path whose reads wait at a shared barrier.
        gate: Option<(String, Arc<Barrier>)>,
        /// Credential path whose store cannot be built.
        fail_build: Option<String>,
        views: StdMutex<HashMap<String, Arc<CredentialView>>>,
        builds: AtomicUsize,
    }

    impl Provider {
        fn new(locations: &[&str], grants: &[(&str, &[&str])]) -> Self {
            Self {
                locations: StdMutex::new(locations.iter().map(|l| l.to_string()).collect()),
                grants: grants
                    .iter()
                    .map(|(credential_path, prefixes)| {
                        let prefixes = prefixes.iter().map(|p| Path::from(*p)).collect();
                        (credential_path.to_string(), prefixes)
                    })
                    .collect(),
                fail_refresh: AtomicBool::new(false),
                refresh_delay: None,
                refreshes: AtomicUsize::new(0),
                gate: None,
                fail_build: None,
                views: StdMutex::new(HashMap::new()),
                builds: AtomicUsize::new(0),
            }
        }

        fn with_gate(mut self, credential_path: &str, parties: usize) -> Self {
            self.gate = Some((credential_path.to_string(), Arc::new(Barrier::new(parties))));
            self
        }

        fn with_slow_refresh(mut self, delay: Duration) -> Self {
            self.refresh_delay = Some(delay);
            self
        }

        fn with_failed_build(mut self, credential_path: &str) -> Self {
            self.fail_build = Some(credential_path.to_string());
            self
        }

        fn set_locations(&self, locations: &[&str]) {
            *self.locations.lock().unwrap() = locations.iter().map(|l| l.to_string()).collect();
        }

        fn store(self: &Arc<Self>) -> LocationScopedObjectStore {
            let provider = Arc::clone(self);
            let source: LocationSource = Arc::new(move || {
                provider.refreshes.fetch_add(1, Ordering::SeqCst);
                if let Some(delay) = provider.refresh_delay {
                    std::thread::sleep(delay);
                }
                if provider.fail_refresh.load(Ordering::SeqCst) {
                    return Err(Error::Generic {
                        store: "test",
                        source: "policy service unavailable".into(),
                    });
                }
                Ok(provider.locations.lock().unwrap().clone())
            });
            let provider = Arc::clone(self);
            let factory: LocationStoreFactory = Arc::new(move |credential_path: &str| {
                provider.builds.fetch_add(1, Ordering::SeqCst);
                if provider.fail_build.as_deref() == Some(credential_path) {
                    return Err(Error::Generic {
                        store: "test",
                        source: "bridge init failed".into(),
                    });
                }
                let gate = provider
                    .gate
                    .as_ref()
                    .filter(|(gated, _)| gated == credential_path)
                    .map(|(_, barrier)| Arc::clone(barrier));
                let view = Arc::new(CredentialView {
                    credential_path: credential_path.to_string(),
                    allowed: provider
                        .grants
                        .get(credential_path)
                        .cloned()
                        .unwrap_or_default(),
                    gets: AtomicUsize::new(0),
                    gate,
                });
                provider
                    .views
                    .lock()
                    .unwrap()
                    .insert(credential_path.to_string(), Arc::clone(&view));
                Ok(view as Arc<dyn ObjectStore>)
            });
            let locations = self.locations.lock().unwrap().clone();
            LocationScopedObjectStore::new("bucket".to_string(), locations, source, factory)
                .unwrap()
        }

        fn gets(&self, credential_path: &str) -> usize {
            self.views.lock().unwrap()[credential_path]
                .gets
                .load(Ordering::SeqCst)
        }

        fn built(&self) -> Vec<String> {
            let mut built: Vec<String> = self.views.lock().unwrap().keys().cloned().collect();
            built.sort();
            built
        }
    }

    /// Returns the credential path that served a read, or panics if the read failed.
    fn served_by<T>(result: Result<T>) -> String {
        match result {
            Err(Error::NotFound { source, .. }) => source.to_string(),
            Err(e) => panic!("expected the read to be served, got {e}"),
            Ok(_) => panic!("test stores never return data"),
        }
    }

    async fn get(store: &LocationScopedObjectStore, path: &str) -> Result<GetResult> {
        store
            .get_opts(&Path::from(path), GetOptions::default())
            .await
    }

    /// The two ranges are close enough to be coalesced into one request.
    async fn get_ranges(store: &LocationScopedObjectStore, path: &str) -> Result<Vec<Bytes>> {
        store.get_ranges(&Path::from(path), &[0..4, 8..12]).await
    }

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

    /// Several locations can be read through one store, in any order, which is what a scan does
    /// when one partition holds files from several locations.
    #[tokio::test]
    async fn routes_each_read_to_its_own_location() {
        let provider = Arc::new(Provider::new(
            &["a", "b", "c"],
            &[("/a", &["a"]), ("/b", &["b"]), ("/c", &["c"])],
        ));
        let store = provider.store();
        assert!(provider.built().is_empty(), "stores are built on first use");

        for (path, location) in [("a/1", "/a"), ("b/1", "/b"), ("a/2", "/a")] {
            assert_eq!(served_by(get(&store, path).await), location);
        }
        assert_eq!(provider.built(), ["/a", "/b"]);
        for (path, location) in [("c/1", "/c"), ("b/2", "/b"), ("a/3", "/a")] {
            assert_eq!(served_by(get_ranges(&store, path).await), location);
        }
        assert_eq!(provider.built(), ["/a", "/b", "/c"]);
        assert_eq!(provider.builds.load(Ordering::SeqCst), 3);
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn returns_a_403_when_the_locations_are_unchanged() {
        let provider = Arc::new(Provider::new(&["a"], &[("/a", &["a/public"])]));
        let store = provider.store();

        let err = get(&store, "a/private/1").await.unwrap_err();
        assert!(matches!(err, Error::PermissionDenied { .. }), "got {err}");
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(provider.gets("/a"), 1, "not retried on the same location");
    }

    #[tokio::test]
    async fn retries_on_a_location_added_after_the_snapshot() {
        let provider = Arc::new(Provider::new(
            &["warehouse"],
            &[
                ("/warehouse", &["warehouse/sales"]),
                ("/warehouse/finance", &["warehouse/finance"]),
            ],
        ));
        let store = provider.store();
        provider.set_locations(&["warehouse", "warehouse/finance"]);

        assert_eq!(
            served_by(get(&store, "warehouse/finance/1").await),
            "/warehouse/finance"
        );
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(
            served_by(get(&store, "warehouse/finance/2").await),
            "/warehouse/finance"
        );
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(provider.gets("/warehouse"), 1);
    }

    /// Two reads that get a 403 from the same snapshot fetch the locations once, and both retry on
    /// the new location instead of one of them returning its 403.
    async fn concurrent_403s_share_one_refresh(use_ranges: bool) {
        let provider = Arc::new(
            Provider::new(
                &["warehouse"],
                &[
                    ("/warehouse", &["warehouse/sales"]),
                    ("/warehouse/finance", &["warehouse/finance"]),
                ],
            )
            .with_gate("/warehouse", 2),
        );
        let store = provider.store();
        provider.set_locations(&["warehouse", "warehouse/finance"]);

        let (first, second) = if use_ranges {
            let (first, second) = tokio::join!(
                get_ranges(&store, "warehouse/finance/1"),
                get_ranges(&store, "warehouse/finance/2")
            );
            (served_by(first), served_by(second))
        } else {
            let (first, second) = tokio::join!(
                get(&store, "warehouse/finance/1"),
                get(&store, "warehouse/finance/2")
            );
            (served_by(first), served_by(second))
        };
        assert_eq!(first, "/warehouse/finance");
        assert_eq!(second, "/warehouse/finance");
        assert_eq!(provider.gets("/warehouse"), 2, "both reads were in flight");
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_get_opts_403s_share_one_refresh() {
        concurrent_403s_share_one_refresh(false).await;
    }

    #[tokio::test]
    async fn concurrent_get_ranges_403s_share_one_refresh() {
        concurrent_403s_share_one_refresh(true).await;
    }

    #[tokio::test]
    async fn fails_the_read_when_fetching_the_locations_again_fails() {
        let provider = Arc::new(Provider::new(&["a"], &[("/a", &["a/public"])]));
        let store = provider.store();
        provider.fail_refresh.store(true, Ordering::SeqCst);

        let err = get(&store, "a/private/1").await.unwrap_err();
        let message = err.to_string();
        assert!(matches!(err, Error::Generic { .. }), "got {message}");
        assert!(message.contains("policy service unavailable"), "{message}");
        assert!(message.contains("a/private/1"), "{message}");
    }

    /// Reads routed before a failed refresh share its failure instead of each asking the provider
    /// in turn.
    #[tokio::test]
    async fn concurrent_403s_share_one_failed_refresh() {
        let provider = Arc::new(Provider::new(&["a"], &[("/a", &["a/public"])]).with_gate("/a", 2));
        let store = provider.store();
        provider.fail_refresh.store(true, Ordering::SeqCst);

        let (first, second) = tokio::join!(get(&store, "a/private/1"), get(&store, "a/private/2"));
        for result in [first, second] {
            let message = result.unwrap_err().to_string();
            assert!(message.contains("policy service unavailable"), "{message}");
        }
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
    }

    /// A read routed after a failed refresh asks the provider again.
    #[tokio::test]
    async fn asks_again_after_a_failed_refresh() {
        let provider = Arc::new(Provider::new(&["a"], &[("/a", &["a/public"])]));
        let store = provider.store();
        provider.fail_refresh.store(true, Ordering::SeqCst);
        assert!(get(&store, "a/private/1").await.is_err());

        provider.fail_refresh.store(false, Ordering::SeqCst);
        let err = get(&store, "a/private/2").await.unwrap_err();
        assert!(matches!(err, Error::PermissionDenied { .. }), "got {err}");
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 2);
    }

    /// Reads that get a 403 while another read is fetching the locations wait for that fetch
    /// instead of starting their own. Each read runs on its own thread and runtime, so the two
    /// refreshes overlap; tasks on one multi-thread runtime can end up serialized.
    #[test]
    fn waits_for_a_refresh_in_progress() {
        let provider = Arc::new(
            Provider::new(
                &["warehouse"],
                &[
                    ("/warehouse", &["warehouse/sales"]),
                    ("/warehouse/finance", &["warehouse/finance"]),
                ],
            )
            .with_gate("/warehouse", 2)
            .with_slow_refresh(Duration::from_millis(100)),
        );
        let store = Arc::new(provider.store());
        provider.set_locations(&["warehouse", "warehouse/finance"]);

        let reads: Vec<_> = ["warehouse/finance/1", "warehouse/finance/2"]
            .into_iter()
            .map(|path| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    served_by(runtime.block_on(get(&store, path)))
                })
            })
            .collect();
        for read in reads {
            assert_eq!(read.join().unwrap(), "/warehouse/finance");
        }
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
    }

    /// The retry gets one attempt: if the new location's credential is denied too, that 403 is
    /// returned without another refresh.
    #[tokio::test]
    async fn returns_the_retry_403_without_retrying_again() {
        let provider = Arc::new(Provider::new(
            &["warehouse"],
            &[
                ("/warehouse", &["warehouse/sales"]),
                ("/warehouse/finance", &[]),
            ],
        ));
        let store = provider.store();
        provider.set_locations(&["warehouse", "warehouse/finance"]);

        let err = get(&store, "warehouse/finance/1").await.unwrap_err();
        assert!(matches!(err, Error::PermissionDenied { .. }), "got {err}");
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(provider.gets("/warehouse"), 1);
        assert_eq!(provider.gets("/warehouse/finance"), 1);
    }

    /// A store that cannot be built after a successful refresh reports its own error, not a failed
    /// refresh.
    #[tokio::test]
    async fn reports_a_failed_build_after_a_refresh_as_itself() {
        let provider = Arc::new(
            Provider::new(&["warehouse"], &[("/warehouse", &["warehouse/sales"])])
                .with_failed_build("/warehouse/finance"),
        );
        let store = provider.store();
        provider.set_locations(&["warehouse", "warehouse/finance"]);

        let message = get(&store, "warehouse/finance/1")
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains("bridge init failed"), "{message}");
        assert!(!message.contains("policy locations"), "{message}");
    }
}
