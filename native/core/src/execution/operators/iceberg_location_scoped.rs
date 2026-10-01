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

//! The Iceberg storage for a `CometS3LocationScopedCredentialProvider`.
//!
//! iceberg-rust's OpenDAL S3 storage attaches one credential loader to every file a `FileIO`
//! touches, and Comet builds that loader from one reference path: the table's metadata location
//! for a scan, its data location for a write. When the configured provider is location-scoped,
//! [`LocationScopedS3Storage`] takes that storage's place. It routes each call by the path the call
//! names to the longest policy location covering that path in that path's bucket, the routing
//! `parquet::objectstore::location_scoped` uses, and delegates to an OpenDAL S3 storage whose loader
//! is bound to that location. A bucket's locations are fetched the first time a call names the
//! bucket (the reference bucket's when the `FileIO` is built), and a location's storage is built
//! the first time a call needs it. Both belong to [`SharedLocations`], which every `FileIO` of the
//! provider registration shares, so the commits of a table, and the tables whose catalog properties
//! match, do not fetch them again.
//!
//! A path is routed by the key the OpenDAL storage asks S3 for: everything after
//! `{scheme}://{bucket}/`, normalized as opendal normalizes it but not percent-decoded. Iceberg
//! paths are not URIs. A partition value's escape, such as the `%3A` in `ts=2024-01-01T00%3A00`,
//! is part of the key, so decoding it would route the file by a key S3 never sees.
//!
//! As on the Parquet path, the locations are a snapshot, so a failed request can mean a location
//! was added or removed after it was taken. A request that S3 rejects with 403, or that cannot be
//! signed because the provider did not produce its location's credential, fetches the bucket's
//! locations again, unless another request already tried since this one was routed. It retries
//! once if its path now routes to a different location, and otherwise returns the error. reqsign's
//! credential chain logs a provider exception and reports no credential, so the signer's
//! `CredentialInvalid` error stands for the exception here. Reads, writes and deletes all do this,
//! and each range read of a reader is routed as a request of its own. A streaming writer is the
//! exception: bytes it already handed to a location's writer cannot be sent again, so it fetches
//! the locations but does not retry, and the task's next attempt routes by the new ones.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::{Path, PathPart};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::cloud::s3::policy_locations::{LocationIndex, PolicyLocations, RefreshError};
use crate::parquet::objectstore::s3_blob_fs_support::BlobHostPromotingS3Storage;

/// Fetches the provider's current policy locations for a bucket. It may block, as a JVM call does;
/// the storage runs it through [`run_blocking`].
pub(crate) type BucketLocationSource = Arc<dyn Fn(&str) -> Result<Vec<String>> + Send + Sync>;

/// Runs `f`, a blocking call, made from an async storage call or from the thread that builds a
/// `FileIO`. On a multi-thread runtime that is `block_in_place`, so the worker hands its other
/// tasks to another thread first. Anywhere else `f` just runs: `block_in_place` panics on a
/// current-thread runtime, which is where `AbortOnDrop` deletes a failed write's files, and a 403
/// on one of those deletes fetches the locations again.
fn run_blocking<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Builds the storage for one policy location from the `FileIO`'s storage configuration, the
/// bucket, and the path passed to `getCredentialsForPath`.
pub(crate) type LocationStorageFactory =
    Arc<dyn Fn(&StorageConfig, &str, &str) -> Result<Arc<dyn Storage>> + Send + Sync>;

/// Whether `err` can mean the locations changed since the snapshot: S3 rejected the request with
/// 403, or the request could not be signed because the provider did not produce the credential of
/// the location it was routed to. The OpenDAL storage wraps opendal's error as the source of an
/// iceberg error, and a signing failure has reqsign's error as its source.
fn may_mean_stale_locations(err: &Error) -> bool {
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(e) = e.downcast_ref::<opendal::Error>() {
            if e.kind() == opendal::ErrorKind::PermissionDenied {
                return true;
            }
        } else if let Some(e) = e.downcast_ref::<reqsign_core::Error>() {
            if e.kind() == reqsign_core::ErrorKind::CredentialInvalid {
                return true;
            }
        }
        source = e.source();
    }
    false
}

/// Splits `path` the way the OpenDAL storage does: the bucket is the URL's host, and the key is
/// the rest of `path` after `{scheme}://{bucket}/`, as written.
fn bucket_and_key(path: &str) -> Result<(String, &str)> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid S3 path {path}: {e}"),
        )
    })?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("S3 path {path} has no bucket"),
        )
    })?;
    let prefix = format!("{}://{bucket}/", url.scheme());
    let key = path.strip_prefix(&prefix).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid S3 path {path}: should start with {prefix}"),
        )
    })?;
    Ok((bucket.to_string(), key))
}

/// The key S3 receives for `key`, as a path that [`LocationIndex::route`] routes the way it would
/// route that whole key. opendal normalizes every key it is given: it trims whitespace and drops
/// leading and empty segments, so it asks S3 for `t/x/f` when given `t//x/f`. The path then stops
/// before the first segment no location can have, `.`, `..`, or one with a control character,
/// since no location covers the key beyond it.
fn routing_path(key: &str) -> Path {
    opendal::raw::normalize_path(key)
        .split('/')
        .map_while(|segment| {
            if segment.is_empty() {
                None
            } else {
                PathPart::parse(segment).ok()
            }
        })
        .collect()
}

fn deserialized() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "A location-scoped S3 storage cannot be used after deserialization: its credential \
         provider does not survive serialization",
    )
}

/// [`StorageFactory`] for S3 when the configured provider implements
/// `CometS3LocationScopedCredentialProvider`. Every storage it builds serves from the shared
/// locations of the provider registration. Serde exists only to satisfy the typetag supertraits;
/// Comet never serializes storage.
#[derive(Serialize, Deserialize)]
pub(crate) struct LocationScopedS3StorageFactory {
    #[serde(skip)]
    state: Option<FactoryState>,
}

struct FactoryState {
    shared: Arc<SharedLocations>,
    /// Promote hostless paths first, for a configured s3-compliant alias scheme.
    promote_hostless_alias: bool,
}

impl LocationScopedS3StorageFactory {
    pub(crate) fn new(shared: Arc<SharedLocations>, promote_hostless_alias: bool) -> Self {
        Self {
            state: Some(FactoryState {
                shared,
                promote_hostless_alias,
            }),
        }
    }
}

impl fmt::Debug for LocationScopedS3StorageFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("LocationScopedS3StorageFactory");
        if let Some(state) = &self.state {
            s.field("buckets", &state.shared.bucket_names());
        }
        s.finish()
    }
}

#[typetag::serde(name = "CometLocationScopedS3StorageFactory")]
impl StorageFactory for LocationScopedS3StorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let state = self.state.as_ref().ok_or_else(deserialized)?;
        let storage: Arc<dyn Storage> = Arc::new(LocationScopedS3Storage {
            inner: Some(Arc::new(Inner {
                config: config.clone(),
                shared: Arc::clone(&state.shared),
            })),
        });
        if state.promote_hostless_alias {
            Ok(Arc::new(BlobHostPromotingS3Storage::new(storage)))
        } else {
            Ok(storage)
        }
    }
}

/// One bucket's locations.
struct BucketLocations {
    bucket: String,
    locations: PolicyLocations,
}

/// The storage chosen for one call, and the snapshot it was chosen from.
struct Route {
    bucket: Arc<BucketLocations>,
    generation: u64,
    credential_path: String,
    storage: Arc<dyn Storage>,
}

/// The locations of one provider registration and access mode, which every `FileIO` of the
/// registration shares: each bucket's snapshot, and each location's storage. The commits of a
/// table, and the tables whose catalog properties match, then fetch a bucket's locations once, and
/// a refresh through one `FileIO` serves them all.
pub(crate) struct SharedLocations {
    source: BucketLocationSource,
    storage_factory: LocationStorageFactory,
    buckets: RwLock<HashMap<String, Arc<BucketLocations>>>,
    /// Location storages by bucket and credential path.
    storages: RwLock<HashMap<(String, String), Arc<dyn Storage>>>,
}

impl SharedLocations {
    /// `locations` is the provider's first answer for `bucket`. `source` fetches a bucket's
    /// locations when a call first names another bucket, and again after a failed request, and
    /// `storage_factory` builds a location's storage on first use. Fails if a location is not a
    /// valid path.
    pub(crate) fn new(
        bucket: &str,
        locations: Vec<String>,
        source: BucketLocationSource,
        storage_factory: LocationStorageFactory,
    ) -> Result<Self> {
        let shared = Self {
            source,
            storage_factory,
            buckets: RwLock::default(),
            storages: RwLock::default(),
        };
        shared.insert(bucket, locations)?;
        Ok(shared)
    }

    /// Fetches `bucket`'s locations unless they are known already, so that a `FileIO` for a table
    /// in another bucket fetches them on the thread that builds it, as the first `FileIO` did.
    pub(crate) fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        self.bucket(bucket).map(|_| ())
    }

    fn bucket_names(&self) -> Vec<String> {
        let buckets = self.buckets.read().unwrap_or_else(PoisonError::into_inner);
        buckets.keys().cloned().collect()
    }

    fn bucket(&self, bucket: &str) -> Result<Arc<BucketLocations>> {
        if let Some(locations) = self
            .buckets
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(bucket)
        {
            return Ok(Arc::clone(locations));
        }
        // Fetch outside the lock, since it calls the provider.
        self.insert(bucket, self.fetch(bucket)?)
    }

    fn fetch(&self, bucket: &str) -> Result<Vec<String>> {
        run_blocking(|| (self.source)(bucket))
    }

    /// Adds `bucket`'s first snapshot. When two calls race to add one, the first insert wins and
    /// the other snapshot is dropped.
    fn insert(&self, bucket: &str, locations: Vec<String>) -> Result<Arc<BucketLocations>> {
        let index = LocationIndex::new(0, locations).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid policy locations for bucket {bucket}: {e}"),
            )
        })?;
        let locations = Arc::new(BucketLocations {
            bucket: bucket.to_string(),
            locations: PolicyLocations::new(index),
        });
        let mut buckets = self.buckets.write().unwrap_or_else(PoisonError::into_inner);
        Ok(Arc::clone(
            buckets.entry(bucket.to_string()).or_insert(locations),
        ))
    }

    fn storage(
        &self,
        config: &StorageConfig,
        bucket: &str,
        credential_path: &str,
    ) -> Result<Arc<dyn Storage>> {
        let key = (bucket.to_string(), credential_path.to_string());
        if let Some(storage) = self
            .storages
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(Arc::clone(storage));
        }
        // Build outside the lock, since building creates a bridge through JNI. When two calls
        // race to build the same location, the first insert wins and the other storage is
        // dropped.
        let storage = (self.storage_factory)(config, bucket, credential_path)?;
        let mut storages = self
            .storages
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(Arc::clone(storages.entry(key).or_insert(storage)))
    }

    /// Fetches `bucket`'s locations again for a request routed from the snapshot of `generation`,
    /// unless a refresh was attempted after that snapshot, in which case this shares that
    /// attempt's outcome. Returns why the refresh failed.
    async fn refresh(
        &self,
        bucket: &BucketLocations,
        generation: u64,
    ) -> std::result::Result<(), String> {
        bucket
            .locations
            .refresh(generation, |generation| {
                self.fetch(&bucket.bucket)
                    .map_err(|e| e.to_string())
                    .and_then(|locations| {
                        LocationIndex::new(generation, locations).map_err(|e| e.to_string())
                    })
            })
            .await
            .map_err(|e| match e {
                RefreshError::Fetch(e) => e,
                RefreshError::Earlier(failure) => failure.to_string(),
            })
    }
}

/// One `FileIO`'s storage: its storage configuration, and the shared locations of its
/// registration. A location's storage is built from the configuration of the first `FileIO` to
/// need it. That is the same for every `FileIO` of a registration, since it comes from the catalog
/// properties the registration is keyed by.
struct Inner {
    config: StorageConfig,
    shared: Arc<SharedLocations>,
}

impl Inner {
    fn route(&self, path: &str) -> Result<Route> {
        let (bucket, key) = bucket_and_key(path)?;
        let bucket = self.shared.bucket(&bucket)?;
        let index = bucket.locations.current();
        let credential_path = index.route(&routing_path(key)).to_string();
        let storage = self
            .shared
            .storage(&self.config, &bucket.bucket, &credential_path)?;
        Ok(Route {
            bucket,
            generation: index.generation(),
            credential_path,
            storage,
        })
    }

    /// Returns the route to retry on after `failed` returned `err` for `path`, an error that may
    /// mean the locations changed, or the error the call should return.
    async fn retry_route(&self, path: &str, failed: &Route, err: Error) -> Result<Route> {
        if let Err(refresh) = self.shared.refresh(&failed.bucket, failed.generation).await {
            return Err(refresh_failed(&failed.bucket.bucket, &refresh, err));
        }
        let route = self.route(path)?;
        if route.credential_path == failed.credential_path {
            return Err(err);
        }
        Ok(route)
    }

    /// Groups `paths` by the location each routes to.
    fn group(&self, paths: Vec<String>) -> Result<Vec<(Route, Vec<String>)>> {
        let mut groups: HashMap<(String, String), (Route, Vec<String>)> = HashMap::new();
        for path in paths {
            let route = self.route(&path)?;
            let key = (route.bucket.bucket.clone(), route.credential_path.clone());
            groups
                .entry(key)
                .or_insert_with(|| (route, Vec::new()))
                .1
                .push(path);
        }
        Ok(groups.into_values().collect())
    }
}

/// `err`, from a request that failed in a way that may mean the locations changed, after
/// fetching `bucket`'s locations again failed with `refresh`.
fn refresh_failed(bucket: &str, refresh: &str, err: Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!(
            "The request failed, and fetching the policy locations for bucket {bucket} again \
             failed: {refresh}"
        ),
    )
    .with_source(err)
}

/// Serves each call with the storage of the longest policy location covering its path. See the
/// module documentation. Serde exists only to satisfy the typetag supertraits.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct LocationScopedS3Storage {
    #[serde(skip)]
    inner: Option<Arc<Inner>>,
}

impl LocationScopedS3Storage {
    fn inner(&self) -> Result<&Arc<Inner>> {
        self.inner.as_ref().ok_or_else(deserialized)
    }

    /// Runs `call` on the storage `path` routes to and, after an error that may mean the locations
    /// changed, on the storage it routes to once they are fetched again. `call` must be a single
    /// request that is safe to send again.
    async fn with_retry<'a, T, F, Fut>(&self, path: &'a str, call: F) -> Result<T>
    where
        F: Fn(Arc<dyn Storage>) -> Fut,
        Fut: Future<Output = Result<T>> + 'a,
    {
        let inner = self.inner()?;
        let route = inner.route(path)?;
        match call(Arc::clone(&route.storage)).await {
            Err(e) if may_mean_stale_locations(&e) => {
                let retry = inner.retry_route(path, &route, e).await?;
                call(retry.storage).await
            }
            other => other,
        }
    }
}

impl fmt::Debug for LocationScopedS3Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("LocationScopedS3Storage");
        if let Some(inner) = &self.inner {
            s.field("buckets", &inner.shared.bucket_names());
        }
        s.finish()
    }
}

#[async_trait]
#[typetag::serde(name = "CometLocationScopedS3Storage")]
impl Storage for LocationScopedS3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.with_retry(path, |storage| async move { storage.exists(path).await })
            .await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.with_retry(path, |storage| async move { storage.metadata(path).await })
            .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.with_retry(path, |storage| async move { storage.read(path).await })
            .await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let inner = Arc::clone(self.inner()?);
        let route = inner.route(path)?;
        // Opening sends nothing to S3, so a stale route shows up on a range read, which retries.
        let reader = route.storage.reader(path).await?;
        Ok(Box::new(LocationScopedFileRead {
            inner,
            path: path.to_string(),
            open: Mutex::new(Arc::new(OpenReader {
                credential_path: route.credential_path,
                reader,
            })),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.with_retry(path, |storage| {
            let bs = bs.clone();
            async move { storage.write(path, bs).await }
        })
        .await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let inner = Arc::clone(self.inner()?);
        let route = inner.route(path)?;
        let writer = route.storage.writer(path).await?;
        Ok(Box::new(LocationScopedFileWrite {
            inner,
            route,
            writer,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.with_retry(path, |storage| async move { storage.delete(path).await })
            .await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        // Routed by the prefix itself, as a list is on the Parquet path.
        self.with_retry(
            path,
            |storage| async move { storage.delete_prefix(path).await },
        )
        .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        let inner = self.inner()?;
        // Each storage deletes the paths routed to it, in one stream per location.
        for (route, paths) in inner.group(paths.collect::<Vec<_>>().await)? {
            match route
                .storage
                .delete_stream(stream::iter(paths.clone()).boxed())
                .await
            {
                // The paths failed as one batch. Once the locations are fetched again, if any of
                // them routes elsewhere, the batch is deleted again through its new routes.
                Err(e) if may_mean_stale_locations(&e) => {
                    if let Err(refresh) =
                        inner.shared.refresh(&route.bucket, route.generation).await
                    {
                        return Err(refresh_failed(&route.bucket.bucket, &refresh, e));
                    }
                    let groups = inner.group(paths)?;
                    if groups
                        .iter()
                        .all(|(retry, _)| retry.credential_path == route.credential_path)
                    {
                        return Err(e);
                    }
                    for (retry, paths) in groups {
                        retry
                            .storage
                            .delete_stream(stream::iter(paths).boxed())
                            .await?;
                    }
                }
                other => other?,
            }
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.inner()?;
        // Bound to this storage rather than the location's, so every later read of the file is
        // routed and can retry.
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.inner()?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// A reader and the location it was opened on.
struct OpenReader {
    credential_path: String,
    reader: Box<dyn FileRead>,
}

/// Reads a file through the location it routes to. Each range read is routed at the current
/// snapshot, as a fresh request is, and reopens the file when that snapshot routes it elsewhere.
/// So a refresh by another request moves the reader too, and a failed range read shares only a
/// refresh attempted after it was routed.
struct LocationScopedFileRead {
    inner: Arc<Inner>,
    path: String,
    open: Mutex<Arc<OpenReader>>,
}

impl LocationScopedFileRead {
    /// The reader for `route`'s location: the open one, or one opened there to replace it.
    async fn open_on(&self, route: &Route) -> Result<Arc<OpenReader>> {
        let open = Arc::clone(&self.open.lock().unwrap_or_else(PoisonError::into_inner));
        if open.credential_path == route.credential_path {
            return Ok(open);
        }
        let reopened = Arc::new(OpenReader {
            credential_path: route.credential_path.clone(),
            reader: route.storage.reader(&self.path).await?,
        });
        *self.open.lock().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&reopened);
        Ok(reopened)
    }
}

#[async_trait]
impl FileRead for LocationScopedFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let route = self.inner.route(&self.path)?;
        let open = self.open_on(&route).await?;
        match open.reader.read(range.clone()).await {
            Err(e) if may_mean_stale_locations(&e) => {
                let retry = self.inner.retry_route(&self.path, &route, e).await?;
                self.open_on(&retry).await?.reader.read(range).await
            }
            other => other,
        }
    }
}

/// Writes a file through the location it was opened on. Bytes already handed to that location's
/// writer cannot be sent again, so a failed write is not retried. When the failure may mean the
/// locations changed, it fetches them again, so the task's next attempt routes by the new ones.
struct LocationScopedFileWrite {
    inner: Arc<Inner>,
    route: Route,
    writer: Box<dyn FileWrite>,
}

impl LocationScopedFileWrite {
    /// The generation of the current snapshot, which a call routed now would be routed from.
    fn generation(&self) -> u64 {
        self.route.bucket.locations.current().generation()
    }
}

/// `result` of a write call made at the snapshot of `generation`, after fetching `bucket`'s
/// locations again if it failed in a way that may mean they changed. A free function so the
/// writer, which is not `Sync`, is not borrowed across the refresh.
async fn refresh_on_failure(
    inner: &Inner,
    bucket: &BucketLocations,
    generation: u64,
    result: Result<()>,
) -> Result<()> {
    match result {
        Err(e) if may_mean_stale_locations(&e) => {
            match inner.shared.refresh(bucket, generation).await {
                Ok(()) => Err(e),
                Err(refresh) => Err(refresh_failed(&bucket.bucket, &refresh, e)),
            }
        }
        other => other,
    }
}

#[async_trait]
impl FileWrite for LocationScopedFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let generation = self.generation();
        let result = self.writer.write(bs).await;
        refresh_on_failure(&self.inner, &self.route.bucket, generation, result).await
    }

    async fn close(&mut self) -> Result<()> {
        let generation = self.generation();
        let result = self.writer.close().await;
        refresh_on_failure(&self.inner, &self.route.bucket, generation, result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn denied() -> Error {
        Error::new(ErrorKind::Unexpected, "Failure in doing io operation").with_source(
            opendal::Error::new(opendal::ErrorKind::PermissionDenied, "access denied"),
        )
    }

    fn not_found() -> Error {
        Error::new(ErrorKind::Unexpected, "Failure in doing io operation").with_source(
            opendal::Error::new(opendal::ErrorKind::NotFound, "no such key"),
        )
    }

    /// What the OpenDAL storage returns when the provider throws: the request is never sent.
    fn unsigned() -> Error {
        Error::new(ErrorKind::Unexpected, "Failure in doing io operation").with_source(
            opendal::Error::new(opendal::ErrorKind::Unexpected, "signing http request").set_source(
                reqsign_core::Error::credential_invalid("failed to load signing credential"),
            ),
        )
    }

    /// Every call a fake storage served: operation, bucket, credential path, and file path.
    type Calls = Arc<Mutex<Vec<(&'static str, String, String, String)>>>;

    /// What S3 allows for one credential. Reads and writes of a key under one of `allowed`
    /// succeed, and every other one gets a 403. If the provider refuses the credential, every call
    /// fails to sign. `read` returns the credential path, which shows which location served the
    /// call.
    #[derive(Clone, Serialize, Deserialize)]
    struct FakeStorage {
        #[serde(skip)]
        bucket: String,
        #[serde(skip)]
        credential_path: String,
        #[serde(skip)]
        allowed: Vec<String>,
        #[serde(skip)]
        refused: bool,
        #[serde(skip)]
        missing: bool,
        /// Holds each range read until as many are in flight as the barrier counts.
        #[serde(skip)]
        gate: Option<Arc<tokio::sync::Barrier>>,
        #[serde(skip)]
        calls: Calls,
    }

    impl fmt::Debug for FakeStorage {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FakeStorage({}{})", self.bucket, self.credential_path)
        }
    }

    impl FakeStorage {
        fn check(&self, op: &'static str, path: &str) -> Result<()> {
            self.calls.lock().unwrap().push((
                op,
                self.bucket.clone(),
                self.credential_path.clone(),
                path.to_string(),
            ));
            if self.refused {
                return Err(unsigned());
            }
            if self.missing {
                return Err(not_found());
            }
            // The key S3 receives, as opendal sends it.
            let key = format!(
                "/{}",
                opendal::raw::normalize_path(bucket_and_key(path).unwrap().1)
            );
            if self
                .allowed
                .iter()
                .any(|a| key == *a || key.starts_with(&format!("{a}/")))
            {
                Ok(())
            } else {
                Err(denied())
            }
        }
    }

    struct FakeRead(Arc<FakeStorage>, String);

    #[async_trait]
    impl FileRead for FakeRead {
        async fn read(&self, _range: Range<u64>) -> Result<Bytes> {
            if let Some(gate) = &self.0.gate {
                gate.wait().await;
            }
            self.0.check("range", &self.1)?;
            Ok(Bytes::from(self.0.credential_path.clone()))
        }
    }

    /// Buffers until close, as opendal does for a file this small, so the upload happens then.
    struct FakeWrite(Arc<FakeStorage>, String);

    #[async_trait]
    impl FileWrite for FakeWrite {
        async fn write(&mut self, _bs: Bytes) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            self.0.check("close", &self.1)
        }
    }

    #[async_trait]
    #[typetag::serde(name = "CometTestFakeLocationStorage")]
    impl Storage for FakeStorage {
        async fn exists(&self, path: &str) -> Result<bool> {
            self.check("exists", path).map(|_| true)
        }

        async fn metadata(&self, path: &str) -> Result<FileMetadata> {
            self.check("metadata", path)
                .map(|_| FileMetadata { size: 1 })
        }

        async fn read(&self, path: &str) -> Result<Bytes> {
            self.check("read", path)?;
            Ok(Bytes::from(self.credential_path.clone()))
        }

        async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
            // Opening does not reach S3; the range reads do.
            Ok(Box::new(FakeRead(Arc::new(self.clone()), path.to_string())))
        }

        async fn write(&self, path: &str, _bs: Bytes) -> Result<()> {
            self.check("write", path)
        }

        async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
            Ok(Box::new(FakeWrite(
                Arc::new(self.clone()),
                path.to_string(),
            )))
        }

        async fn delete(&self, path: &str) -> Result<()> {
            self.check("delete", path)
        }

        async fn delete_prefix(&self, path: &str) -> Result<()> {
            self.check("delete_prefix", path)
        }

        async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
            for path in paths.collect::<Vec<_>>().await {
                self.check("delete_stream", &path)?;
            }
            Ok(())
        }

        fn new_input(&self, _path: &str) -> Result<InputFile> {
            unimplemented!()
        }

        fn new_output(&self, _path: &str) -> Result<OutputFile> {
            unimplemented!()
        }
    }

    /// A provider and S3 in one: per-bucket locations that a test can change, per-credential
    /// grants, and the credentials the provider refuses to produce.
    struct Fixture {
        locations: Mutex<HashMap<String, Vec<String>>>,
        /// Bucket and credential path to the paths that credential may use.
        grants: HashMap<(String, String), Vec<String>>,
        /// Bucket and credential path of each location whose credential the provider refuses.
        refused: Vec<(String, String)>,
        /// Credential path whose range reads wait at the barrier.
        gate: Option<(String, Arc<tokio::sync::Barrier>)>,
        missing: bool,
        fetches: AtomicUsize,
        built: AtomicUsize,
        fail_fetch: Mutex<bool>,
        calls: Calls,
    }

    impl Fixture {
        fn new(locations: &[(&str, &[&str])], grants: &[(&str, &str, &[&str])]) -> Arc<Self> {
            Arc::new(Self {
                locations: Mutex::new(
                    locations
                        .iter()
                        .map(|(b, l)| (b.to_string(), l.iter().map(|s| s.to_string()).collect()))
                        .collect(),
                ),
                grants: grants
                    .iter()
                    .map(|(b, c, a)| {
                        (
                            (b.to_string(), c.to_string()),
                            a.iter().map(|s| s.to_string()).collect(),
                        )
                    })
                    .collect(),
                refused: Vec::new(),
                gate: None,
                missing: false,
                fetches: AtomicUsize::new(0),
                built: AtomicUsize::new(0),
                fail_fetch: Mutex::new(false),
                calls: Arc::default(),
            })
        }

        fn set_locations(&self, bucket: &str, locations: &[&str]) {
            self.locations.lock().unwrap().insert(
                bucket.to_string(),
                locations.iter().map(|s| s.to_string()).collect(),
            );
        }

        /// The storage for bucket `b`, as `storage_factory_for` builds it.
        fn storage(self: &Arc<Self>) -> Arc<dyn Storage> {
            storage_on(&self.shared())
        }

        /// The shared locations of a registration whose reference bucket is `b`.
        fn shared(self: &Arc<Self>) -> Arc<SharedLocations> {
            let source_fixture = Arc::clone(self);
            let source: BucketLocationSource = Arc::new(move |bucket: &str| {
                source_fixture.fetches.fetch_add(1, Ordering::SeqCst);
                if *source_fixture.fail_fetch.lock().unwrap() {
                    return Err(Error::new(ErrorKind::Unexpected, "provider unavailable"));
                }
                Ok(source_fixture
                    .locations
                    .lock()
                    .unwrap()
                    .get(bucket)
                    .cloned()
                    .unwrap_or_default())
            });
            let fixture = Arc::clone(self);
            let storage_factory: LocationStorageFactory = Arc::new(
                move |_config: &StorageConfig, bucket: &str, credential_path: &str| {
                    fixture.built.fetch_add(1, Ordering::SeqCst);
                    let location = (bucket.to_string(), credential_path.to_string());
                    let allowed = fixture.grants.get(&location).cloned().unwrap_or_default();
                    Ok(Arc::new(FakeStorage {
                        bucket: bucket.to_string(),
                        credential_path: credential_path.to_string(),
                        allowed,
                        refused: fixture.refused.contains(&location),
                        missing: fixture.missing,
                        gate: fixture
                            .gate
                            .as_ref()
                            .filter(|(path, _)| path == credential_path)
                            .map(|(_, gate)| Arc::clone(gate)),
                        calls: Arc::clone(&fixture.calls),
                    }) as Arc<dyn Storage>)
                },
            );
            let initial = self.locations.lock().unwrap().get("b").cloned().unwrap();
            let Ok(shared) = SharedLocations::new("b", initial, source, storage_factory) else {
                panic!("invalid initial locations");
            };
            Arc::new(shared)
        }

        fn fetches(&self) -> usize {
            self.fetches.load(Ordering::SeqCst)
        }

        /// How many location storages were built.
        fn built(&self) -> usize {
            self.built.load(Ordering::SeqCst)
        }

        /// The credential paths that served `op`, in order.
        fn served(&self, op: &str) -> Vec<String> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(o, ..)| *o == op)
                .map(|(_, b, c, _)| format!("{b}{c}"))
                .collect()
        }
    }

    /// A `FileIO`'s storage on `shared`, as one `FileIO` of a registration builds it.
    fn storage_on(shared: &Arc<SharedLocations>) -> Arc<dyn Storage> {
        LocationScopedS3StorageFactory::new(Arc::clone(shared), false)
            .build(&StorageConfig::default())
            .unwrap()
    }

    async fn read(storage: &Arc<dyn Storage>, path: &str) -> Result<String> {
        storage
            .read(path)
            .await
            .map(|bs| String::from_utf8(bs.to_vec()).unwrap())
    }

    #[test]
    fn recognizes_a_403_or_a_signing_failure_inside_an_iceberg_error() {
        assert!(may_mean_stale_locations(&denied()));
        assert!(may_mean_stale_locations(&unsigned()));
        assert!(!may_mean_stale_locations(&not_found()));
        assert!(!may_mean_stale_locations(&Error::new(
            ErrorKind::Unexpected,
            "no source"
        )));
    }

    /// A loader whose provider always throws, as the bridge's does when `getCredentialsForPath`
    /// throws.
    #[derive(Debug)]
    struct ThrowingLoader;

    impl reqsign_core::ProvideCredential for ThrowingLoader {
        type Credential = iceberg_storage_opendal::AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &reqsign_core::Context,
        ) -> reqsign_core::Result<Option<Self::Credential>> {
            Err(reqsign_core::Error::credential_invalid(
                "getCredentialsForPath threw",
            ))
        }
    }

    /// The kind of the opendal error inside `err`, if it is the opendal Comet depends on.
    fn opendal_kind(err: &Error) -> Option<opendal::ErrorKind> {
        let mut source = std::error::Error::source(err);
        while let Some(e) = source {
            if let Some(e) = e.downcast_ref::<opendal::Error>() {
                return Some(e.kind());
            }
            source = e.source();
        }
        None
    }

    /// The OpenDAL storage does not send a request its provider could not sign, so S3 never
    /// answers it with a 403. The signing failure alone has to trigger the refresh. Nothing listens
    /// on the endpoint: the request fails before it connects.
    #[tokio::test]
    async fn recognizes_a_provider_failure_in_the_opendal_storage() {
        let config = StorageConfig::from_props(HashMap::from([
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.endpoint".to_string(), "http://127.0.0.1:1".to_string()),
        ]));
        let storage = iceberg_storage_opendal::OpenDalStorageFactory::S3 {
            customized_credential_load: Some(
                iceberg_storage_opendal::CustomAwsCredentialLoader::new(ThrowingLoader),
            ),
        }
        .build(&config)
        .unwrap();
        let err = storage.read("s3://b/t/f.parquet").await.unwrap_err();
        assert!(may_mean_stale_locations(&err), "{err}");
        // A 403 is recognized by downcasting to Comet's opendal, so the storage must build its
        // errors with that same crate. If the two resolve to different versions, this fails.
        assert_eq!(
            opendal_kind(&err),
            Some(opendal::ErrorKind::Unexpected),
            "{err}"
        );
        // Opening a file sends nothing to S3, so the first range read is what fails.
        let reader = storage.reader("s3://b/t/f.parquet").await.unwrap();
        let err = reader.read(0..4).await.unwrap_err();
        assert!(may_mean_stale_locations(&err), "{err}");
    }

    #[tokio::test]
    async fn routes_each_file_to_its_own_location() {
        let fixture = Fixture::new(
            &[("b", &["warehouse/db/t", "warehouse/db/t/data/region=eu"])],
            &[
                ("b", "/warehouse/db/t", &["/warehouse/db/t"]),
                (
                    "b",
                    "/warehouse/db/t/data/region=eu",
                    &["/warehouse/db/t/data/region=eu"],
                ),
                ("b", "/", &["/elsewhere"]),
            ],
        );
        let storage = fixture.storage();
        assert_eq!(
            read(&storage, "s3://b/warehouse/db/t/metadata/v1.metadata.json")
                .await
                .unwrap(),
            "/warehouse/db/t"
        );
        assert_eq!(
            read(&storage, "s3://b/warehouse/db/t/data/region=eu/f.parquet")
                .await
                .unwrap(),
            "/warehouse/db/t/data/region=eu"
        );
        assert_eq!(
            read(&storage, "s3://b/elsewhere/f.parquet").await.unwrap(),
            "/"
        );
        assert_eq!(fixture.fetches(), 0, "the first snapshot needs no fetch");
        // Locations match whole segments, so a sibling that shares a name prefix gets the root's
        // credential, which does not cover it.
        assert!(read(&storage, "s3://b/warehouse/db/t_other/f.parquet")
            .await
            .is_err());
    }

    /// Iceberg writes a partition value's escape into the key, as in `ts=2024-01-01T00%3A00`, and
    /// the provider, whose locations are percent-encoded, writes that key as `%253A`.
    #[tokio::test]
    async fn routes_by_the_key_s3_receives() {
        let fixture = Fixture::new(
            &[(
                "b",
                &["t", "t/data/ts=2024-01-01T00%253A00", "t/a%23b", "t/x"],
            )],
            &[
                ("b", "/t", &["/t"]),
                (
                    "b",
                    "/t/data/ts=2024-01-01T00%253A00",
                    &["/t/data/ts=2024-01-01T00%3A00"],
                ),
                ("b", "/t/a%23b", &["/t/a#b"]),
                ("b", "/t/x", &["/t/x"]),
            ],
        );
        let storage = fixture.storage();
        assert_eq!(
            read(&storage, "s3://b/t/data/ts=2024-01-01T00%3A00/f.parquet")
                .await
                .unwrap(),
            "/t/data/ts=2024-01-01T00%253A00"
        );
        // A '#' is part of the key, not the start of a URI fragment.
        assert_eq!(
            read(&storage, "s3://b/t/a#b/f.parquet").await.unwrap(),
            "/t/a%23b"
        );
        // opendal drops the empty segment, so S3 is asked for `t/x/f.parquet`, which `t/x` covers.
        assert_eq!(
            read(&storage, "s3://b/t//x/f.parquet").await.unwrap(),
            "/t/x"
        );
    }

    #[tokio::test]
    async fn routes_files_in_another_bucket_by_that_buckets_locations() {
        let fixture = Fixture::new(
            &[("b", &["warehouse"]), ("other", &["archive/t"])],
            &[
                ("b", "/warehouse", &["/warehouse"]),
                ("other", "/archive/t", &["/archive/t"]),
            ],
        );
        let storage = fixture.storage();
        assert_eq!(
            read(&storage, "s3://other/archive/t/f.parquet")
                .await
                .unwrap(),
            "/archive/t"
        );
        assert_eq!(
            read(&storage, "s3://other/archive/t/g.parquet")
                .await
                .unwrap(),
            "/archive/t"
        );
        assert_eq!(fixture.fetches(), 1, "one fetch for the new bucket");
        assert_eq!(
            fixture.served("read"),
            ["other/archive/t", "other/archive/t"]
        );
    }

    /// Every `FileIO` of a registration shares its locations. A refresh through one serves the
    /// others, and each location's storage is built once.
    #[tokio::test]
    async fn file_ios_share_their_registrations_locations() {
        let fixture = Fixture::new(
            &[("b", &["warehouse"])],
            &[
                ("b", "/warehouse", &["/warehouse/old"]),
                ("b", "/warehouse/new", &["/warehouse/new"]),
            ],
        );
        let shared = fixture.shared();
        let (first, second) = (storage_on(&shared), storage_on(&shared));
        fixture.set_locations("b", &["warehouse", "warehouse/new"]);
        assert_eq!(
            read(&first, "s3://b/warehouse/new/f.parquet")
                .await
                .unwrap(),
            "/warehouse/new"
        );
        assert_eq!(
            read(&second, "s3://b/warehouse/new/g.parquet")
                .await
                .unwrap(),
            "/warehouse/new"
        );
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(
            fixture.served("read"),
            ["b/warehouse", "b/warehouse/new", "b/warehouse/new"],
            "the second FileIO went straight to the refreshed route"
        );
        assert_eq!(fixture.built(), 2, "one storage per location");
    }

    #[test]
    fn ensure_bucket_fetches_only_an_unknown_bucket() {
        let fixture = Fixture::new(&[("b", &["t"]), ("other", &["u"])], &[]);
        let shared = fixture.shared();
        shared.ensure_bucket("b").unwrap();
        assert_eq!(fixture.fetches(), 0, "the first snapshot needs no fetch");
        shared.ensure_bucket("other").unwrap();
        shared.ensure_bucket("other").unwrap();
        assert_eq!(fixture.fetches(), 1);
    }

    #[tokio::test]
    async fn every_operation_routes_by_its_path() {
        let fixture = Fixture::new(
            &[("b", &["t1", "t2"])],
            &[("b", "/t1", &["/t1"]), ("b", "/t2", &["/t2"])],
        );
        let storage = fixture.storage();
        assert!(storage.exists("s3://b/t1/a").await.unwrap());
        assert_eq!(storage.metadata("s3://b/t2/a").await.unwrap().size, 1);
        storage.write("s3://b/t1/w", Bytes::new()).await.unwrap();
        storage.delete("s3://b/t2/d").await.unwrap();
        storage.delete_prefix("s3://b/t1/p").await.unwrap();
        storage
            .delete_stream(
                stream::iter(["s3://b/t1/x".to_string(), "s3://b/t2/y".to_string()]).boxed(),
            )
            .await
            .unwrap();
        assert_eq!(fixture.served("exists"), ["b/t1"]);
        assert_eq!(fixture.served("metadata"), ["b/t2"]);
        assert_eq!(fixture.served("write"), ["b/t1"]);
        assert_eq!(fixture.served("delete"), ["b/t2"]);
        assert_eq!(fixture.served("delete_prefix"), ["b/t1"]);
        let mut deleted = fixture.served("delete_stream");
        deleted.sort();
        assert_eq!(deleted, ["b/t1", "b/t2"]);
    }

    #[tokio::test]
    async fn input_files_read_through_the_routing() {
        let fixture = Fixture::new(&[("b", &["t"])], &[("b", "/t", &["/t"])]);
        let storage = fixture.storage();
        let input = storage.new_input("s3://b/t/f.parquet").unwrap();
        assert_eq!(input.metadata().await.unwrap().size, 1);
        let reader = input.reader().await.unwrap();
        assert_eq!(reader.read(0..4).await.unwrap(), Bytes::from("/t"));
        assert_eq!(fixture.served("metadata"), ["b/t"]);
        assert_eq!(fixture.served("range"), ["b/t"]);
    }

    #[tokio::test]
    async fn retries_on_a_location_added_after_the_snapshot() {
        let fixture = Fixture::new(
            &[("b", &["warehouse"])],
            &[
                ("b", "/warehouse", &["/warehouse/old"]),
                ("b", "/warehouse/new", &["/warehouse/new"]),
            ],
        );
        let storage = fixture.storage();
        fixture.set_locations("b", &["warehouse", "warehouse/new"]);
        assert_eq!(
            read(&storage, "s3://b/warehouse/new/f.parquet")
                .await
                .unwrap(),
            "/warehouse/new"
        );
        assert_eq!(fixture.fetches(), 1);
        // The refreshed snapshot routes later reads straight to the new location.
        read(&storage, "s3://b/warehouse/new/g.parquet")
            .await
            .unwrap();
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(
            fixture.served("read"),
            ["b/warehouse", "b/warehouse/new", "b/warehouse/new"]
        );
    }

    #[tokio::test]
    async fn a_reader_reopens_on_the_new_location_after_a_403() {
        let fixture = Fixture::new(
            &[("b", &["warehouse"])],
            &[
                ("b", "/warehouse", &["/warehouse/old"]),
                ("b", "/warehouse/new", &["/warehouse/new"]),
            ],
        );
        let storage = fixture.storage();
        let reader = storage
            .new_input("s3://b/warehouse/new/f.parquet")
            .unwrap()
            .reader()
            .await
            .unwrap();
        fixture.set_locations("b", &["warehouse", "warehouse/new"]);
        assert_eq!(
            reader.read(0..4).await.unwrap(),
            Bytes::from("/warehouse/new")
        );
        assert_eq!(
            reader.read(4..8).await.unwrap(),
            Bytes::from("/warehouse/new")
        );
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(
            fixture.served("range"),
            ["b/warehouse", "b/warehouse/new", "b/warehouse/new"]
        );
    }

    /// Range reads of two readers routed from one snapshot share one refresh, whether or not it
    /// finds the locations changed, even though one of them handles its 403 only after the other
    /// has refreshed.
    async fn concurrent_range_reads_share_one_refresh(change_locations: bool) {
        let mut fixture = Fixture::new(
            &[("b", &["warehouse"])],
            &[
                ("b", "/warehouse", &["/warehouse/old"]),
                ("b", "/warehouse/new", &["/warehouse/new"]),
            ],
        );
        Arc::get_mut(&mut fixture).unwrap().gate =
            Some(("/warehouse".into(), Arc::new(tokio::sync::Barrier::new(2))));
        let storage = fixture.storage();
        let open = |path: &'static str| {
            let input = storage.new_input(path).unwrap();
            async move { input.reader().await.unwrap() }
        };
        let first = open("s3://b/warehouse/new/1").await;
        let second = open("s3://b/warehouse/new/2").await;
        if change_locations {
            fixture.set_locations("b", &["warehouse", "warehouse/new"]);
        }
        let (first, second) = tokio::join!(first.read(0..4), second.read(0..4));
        if change_locations {
            assert_eq!(first.unwrap(), Bytes::from("/warehouse/new"));
            assert_eq!(second.unwrap(), Bytes::from("/warehouse/new"));
        } else {
            assert!(may_mean_stale_locations(&first.unwrap_err()));
            assert!(may_mean_stale_locations(&second.unwrap_err()));
        }
        assert_eq!(fixture.fetches(), 1);
    }

    #[tokio::test]
    async fn concurrent_range_reads_share_one_refresh_that_moves_them() {
        concurrent_range_reads_share_one_refresh(true).await;
    }

    #[tokio::test]
    async fn concurrent_range_reads_share_one_refresh_that_changes_nothing() {
        concurrent_range_reads_share_one_refresh(false).await;
    }

    /// Another read refreshes the locations while a reader is open, and finds them unchanged. When
    /// they change later, the reader's own 403 still has to fetch them again.
    #[tokio::test]
    async fn a_reader_refreshes_after_a_refresh_it_did_not_see() {
        let fixture = Fixture::new(
            &[("b", &["warehouse"])],
            &[
                ("b", "/warehouse", &["/warehouse/old"]),
                ("b", "/warehouse/new", &["/warehouse/new"]),
            ],
        );
        let storage = fixture.storage();
        let reader = storage
            .new_input("s3://b/warehouse/new/f.parquet")
            .unwrap()
            .reader()
            .await
            .unwrap();
        read(&storage, "s3://b/warehouse/other/f.parquet")
            .await
            .unwrap_err();
        assert_eq!(fixture.fetches(), 1);
        fixture.set_locations("b", &["warehouse", "warehouse/new"]);
        assert_eq!(
            reader.read(0..4).await.unwrap(),
            Bytes::from("/warehouse/new")
        );
        assert_eq!(fixture.fetches(), 2);
    }

    #[tokio::test]
    async fn retries_when_the_provider_refuses_a_removed_location() {
        let mut fixture = Fixture::new(
            &[("b", &["warehouse", "warehouse/old"])],
            &[("b", "/warehouse", &["/warehouse"])],
        );
        Arc::get_mut(&mut fixture).unwrap().refused = vec![("b".into(), "/warehouse/old".into())];
        let storage = fixture.storage();
        fixture.set_locations("b", &["warehouse"]);
        assert_eq!(
            read(&storage, "s3://b/warehouse/old/f.parquet")
                .await
                .unwrap(),
            "/warehouse"
        );
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(fixture.served("read"), ["b/warehouse/old", "b/warehouse"]);
    }

    #[tokio::test]
    async fn returns_the_403_when_the_locations_are_unchanged() {
        let fixture = Fixture::new(&[("b", &["t"])], &[("b", "/t", &["/t/allowed"])]);
        let storage = fixture.storage();
        let err = read(&storage, "s3://b/t/denied/f.parquet")
            .await
            .unwrap_err();
        assert!(may_mean_stale_locations(&err), "{err}");
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(
            fixture.served("read"),
            ["b/t"],
            "no second attempt on the same route"
        );
    }

    #[tokio::test]
    async fn fails_the_read_when_fetching_the_locations_again_fails() {
        let fixture = Fixture::new(&[("b", &["t"])], &[("b", "/t", &[])]);
        let storage = fixture.storage();
        *fixture.fail_fetch.lock().unwrap() = true;
        let err = read(&storage, "s3://b/t/f.parquet").await.unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("fetching the policy locations for bucket b again failed")
                && message.contains("provider unavailable"),
            "{message}"
        );
        // A later read shares nothing with the failed attempt, so it asks again.
        read(&storage, "s3://b/t/f.parquet").await.unwrap_err();
        assert_eq!(fixture.fetches(), 2);
    }

    #[tokio::test]
    async fn does_not_refresh_on_other_errors() {
        let mut fixture = Fixture::new(&[("b", &["t"])], &[("b", "/t", &["/t"])]);
        Arc::get_mut(&mut fixture).unwrap().missing = true;
        let storage = fixture.storage();
        let err = read(&storage, "s3://b/t/f.parquet").await.unwrap_err();
        assert!(!may_mean_stale_locations(&err));
        assert_eq!(fixture.fetches(), 0);
    }

    /// A delete under a location the snapshot does not have, which gets a 403 and refreshes.
    async fn delete_under_a_new_location() {
        let fixture = Fixture::new(
            &[("b", &["t"])],
            &[("b", "/t", &[]), ("b", "/t/new", &["/t/new"])],
        );
        let storage = fixture.storage();
        fixture.set_locations("b", &["t", "t/new"]);
        storage.delete("s3://b/t/new/f.parquet").await.unwrap();
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(fixture.served("delete"), ["b/t", "b/t/new"]);
    }

    /// `AbortOnDrop` deletes a failed write's files on a current-thread runtime, where
    /// `block_in_place` panics, so a refresh there must not use it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_delete_refreshes_on_a_current_thread_runtime() {
        delete_under_a_new_location().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_delete_refreshes_on_a_runtime_worker() {
        tokio::spawn(delete_under_a_new_location()).await.unwrap();
    }

    #[tokio::test]
    async fn writes_retry_on_a_location_added_after_the_snapshot() {
        let fixture = Fixture::new(
            &[("b", &["t"])],
            &[("b", "/t", &[]), ("b", "/t/new", &["/t/new"])],
        );
        let storage = fixture.storage();
        fixture.set_locations("b", &["t", "t/new"]);
        storage
            .write("s3://b/t/new/f.parquet", Bytes::new())
            .await
            .unwrap();
        assert_eq!(fixture.fetches(), 1);
        assert_eq!(fixture.served("write"), ["b/t", "b/t/new"]);
    }

    /// A streaming writer cannot send again what it already handed to the failed location, so it
    /// is not retried. It fetches the locations again, so the task's next attempt writes through
    /// the new one.
    #[tokio::test]
    async fn a_failed_writer_refreshes_for_the_next_attempt() {
        let fixture = Fixture::new(
            &[("b", &["t"])],
            &[("b", "/t", &[]), ("b", "/t/new", &["/t/new"])],
        );
        let storage = fixture.storage();
        fixture.set_locations("b", &["t", "t/new"]);
        let output = storage.new_output("s3://b/t/new/f.parquet").unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("data")).await.unwrap();
        let err = writer.close().await.unwrap_err();
        assert!(may_mean_stale_locations(&err), "{err}");
        assert_eq!(fixture.fetches(), 1);
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("data")).await.unwrap();
        writer.close().await.unwrap();
        assert_eq!(fixture.served("close"), ["b/t", "b/t/new"]);
    }

    /// Paths deleted as one batch are deleted again, through their new routes, when any of them
    /// routes elsewhere once the locations are fetched again.
    #[tokio::test]
    async fn delete_stream_deletes_a_failed_batch_again_through_its_new_routes() {
        let fixture = Fixture::new(
            &[("b", &["t"])],
            &[("b", "/t", &["/t/old"]), ("b", "/t/new", &["/t/new"])],
        );
        let storage = fixture.storage();
        fixture.set_locations("b", &["t", "t/new"]);
        storage
            .delete_stream(
                stream::iter(["s3://b/t/new/x".to_string(), "s3://b/t/old/y".to_string()]).boxed(),
            )
            .await
            .unwrap();
        assert_eq!(fixture.fetches(), 1);
        let mut deleted = fixture.served("delete_stream");
        deleted.sort();
        assert_eq!(deleted, ["b/t", "b/t", "b/t/new"]);
    }

    #[test]
    fn rejects_an_invalid_location() {
        let source: BucketLocationSource = Arc::new(|_: &str| Ok(Vec::new()));
        let storage_factory: LocationStorageFactory =
            Arc::new(|_: &StorageConfig, _: &str, _: &str| {
                Err(Error::new(ErrorKind::Unexpected, "unused"))
            });
        let Err(err) = SharedLocations::new("b", vec!["a//b".into()], source, storage_factory)
        else {
            panic!("an invalid location was accepted");
        };
        assert!(
            err.to_string()
                .contains("Invalid policy locations for bucket b"),
            "{err}"
        );
    }

    #[test]
    fn a_deserialized_storage_refuses_to_serve() {
        let storage: LocationScopedS3Storage =
            serde_json::from_str("{}").expect("deserializes without its provider");
        assert!(storage.new_input("s3://b/t/f").is_err());
    }
}
