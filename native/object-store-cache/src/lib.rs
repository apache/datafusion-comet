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

//! `object_store::ObjectStore` adapter over the Comet block cache core.
//!
//! [`CachingObjectStore`] wraps any `Arc<dyn ObjectStore>` and serves block-aligned range
//! reads out of a shared [`BlockCache`]. It is the ONLY crate that touches the
//! `object_store` trait, so a DataFusion / `object_store` version bump compiles the adapter
//! in the same PR (see OBJECT_STORE_CACHE_DESIGN.md §2.1, §2.6).
//!
//! ## What is cached
//!
//! - `get_range` / `get_ranges` — the whole Parquet I/O path in Comet — are served from
//!   the cache, block-aligned, with single-flight miss dedup.
//! - `get_opts` is served from the cache only for a plain `GetRange::Bounded` with no
//!   conditions / version / head; anything else (conditional gets, suffix/offset ranges,
//!   full-object streams, head requests) passes straight through to the inner store.
//! - `put*` / `delete` / `copy*` pass through and invalidate the affected path.
//! - `get` (full-object stream), `list*`, and `head` pass through unchanged.

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion_comet_block_cache::{
    BlockCache, CacheError, FileKey, FileVersion, RangeFetcher, Result as CacheResult,
};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as OsResult,
};

/// An [`ObjectStore`] that serves block-aligned range reads from a shared [`BlockCache`].
///
/// `namespace` distinguishes logically distinct stores that share the cache (Comet passes a
/// hash of its `(url_key, config_hash)` store key), so identical object paths in different
/// stores never collide in the cache.
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<BlockCache>,
    namespace: u64,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<BlockCache>, namespace: u64) -> Self {
        CachingObjectStore {
            inner,
            cache,
            namespace,
        }
    }

    fn file_key(&self, location: &Path) -> FileKey {
        FileKey::new(self.namespace, location.as_ref())
    }

    fn range_fetcher(&self, location: &Path) -> StoreRangeFetcher {
        StoreRangeFetcher {
            store: Arc::clone(&self.inner),
            path: location.clone(),
        }
    }

    /// Serve a plain bounded-range `get_opts` from the cache, synthesizing an `ObjectMeta`
    /// from the cache's captured version (no extra `head` request). Returns `None` when the
    /// version has not been captured yet, so the caller falls back to the inner store.
    async fn cached_get_opts(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> OsResult<Option<GetResult>> {
        let file = self.file_key(location);
        let fetcher = self.range_fetcher(location);
        let bytes = self
            .cache
            .get_ranges(&file, std::slice::from_ref(&range), &fetcher)
            .await
            .map_err(to_os_error)?
            .pop()
            .unwrap_or_default();

        let Some(version) = self.cache.file_version(&file) else {
            return Ok(None);
        };
        let returned = range.start..range.start + bytes.len() as u64;
        let meta = version_to_meta(location, &version);
        let payload = GetResultPayload::Stream(
            futures::stream::once(async move { Ok(bytes) }).boxed(),
        );
        Ok(Some(GetResult {
            payload,
            meta,
            range: returned,
            attributes: Attributes::default(),
        }))
    }
}

impl Display for CachingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

impl Debug for CachingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingObjectStore")
            .field("inner", &self.inner)
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await;
        // The object at this path just changed; drop any cached blocks for it.
        self.cache.invalidate_file(&self.file_key(location));
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        // A multipart write targets this path; invalidate up front so stale blocks cannot
        // be served while/after the upload lands.
        self.cache.invalidate_file(&self.file_key(location));
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if let Some(range) = plain_bounded_range(&options) {
            if let Some(result) = self.cached_get_opts(location, range).await? {
                return Ok(result);
            }
        }
        self.inner.get_opts(location, options).await
    }

    // `get_range` (the single-range convenience) lives in `ObjectStoreExt` and is not
    // overridable; it delegates to `get_opts` with a bounded range, which we cache above.
    // `get_ranges` is a core trait method, so we route it straight through the cache.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let fetcher = self.range_fetcher(location);
        self.cache
            .get_ranges(&self.file_key(location), ranges, &fetcher)
            .await
            .map_err(to_os_error)
    }

    // The single-path `delete` convenience delegates here; invalidate each path as it flows
    // to the inner store so a deleted object cannot be served stale from the cache.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        let cache = Arc::clone(&self.cache);
        let namespace = self.namespace;
        let mapped = locations
            .inspect(move |res| {
                if let Ok(path) = res {
                    cache.invalidate_file(&FileKey::new(namespace, path.as_ref()));
                }
            })
            .boxed();
        self.inner.delete_stream(mapped)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    // `copy` and `copy_if_not_exists` are `ObjectStoreExt` conveniences that both delegate
    // to `copy_opts`, so invalidating the destination here covers them all.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        let result = self.inner.copy_opts(from, to, options).await;
        self.cache.invalidate_file(&self.file_key(to));
        result
    }
}

/// A [`RangeFetcher`] that pulls absolute byte ranges from an inner `ObjectStore` and
/// harvests the object version from the response metadata.
struct StoreRangeFetcher {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

#[async_trait]
impl RangeFetcher for StoreRangeFetcher {
    async fn fetch(&self, ranges: &[Range<u64>]) -> CacheResult<(Vec<Bytes>, FileVersion)> {
        let mut out = Vec::with_capacity(ranges.len());
        let mut version = FileVersion::default();
        for r in ranges {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(r.clone())),
                ..Default::default()
            };
            let result = self
                .store
                .get_opts(&self.path, opts)
                .await
                .map_err(CacheError::fetch)?;
            // Capture the version before consuming the payload.
            version = FileVersion {
                e_tag: result.meta.e_tag.clone(),
                last_modified: Some(result.meta.last_modified.timestamp_millis()),
                size: result.meta.size,
            };
            let bytes = result.bytes().await.map_err(CacheError::fetch)?;
            out.push(bytes);
        }
        Ok((out, version))
    }
}

/// Extract a plain bounded range from `get_opts` options, or `None` if any condition,
/// version pin, head flag, or non-bounded range is present (those bypass the cache).
fn plain_bounded_range(options: &GetOptions) -> Option<Range<u64>> {
    if options.if_match.is_some()
        || options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_unmodified_since.is_some()
        || options.version.is_some()
        || options.head
    {
        return None;
    }
    match &options.range {
        Some(GetRange::Bounded(r)) => Some(r.clone()),
        _ => None,
    }
}

/// Reconstruct an `ObjectMeta` from a captured cache [`FileVersion`].
fn version_to_meta(location: &Path, version: &FileVersion) -> ObjectMeta {
    let last_modified = version
        .last_modified
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_millis(0).unwrap());
    ObjectMeta {
        location: location.clone(),
        last_modified,
        size: version.size,
        e_tag: version.e_tag.clone(),
        version: None,
    }
}

/// Map a cache error onto an `object_store::Error`.
fn to_os_error(err: CacheError) -> object_store::Error {
    object_store::Error::Generic {
        store: "CachingObjectStore",
        source: Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_comet_block_cache::BlockCacheConfig;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// An `ObjectStore` that counts `get_opts` calls, wrapping an inner store.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        get_opts_calls: AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(CountingStore {
                inner,
                get_opts_calls: AtomicU64::new(0),
            })
        }
        fn calls(&self) -> u64 {
            self.get_opts_calls.load(Ordering::SeqCst)
        }
    }

    impl Display for CountingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.get_opts_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn setup(data: &[u8]) -> (Arc<CountingStore>, CachingObjectStore, Path) {
        let mem = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        futures::executor::block_on(mem.put(&path, PutPayload::from(data.to_vec()))).unwrap();
        let counting = CountingStore::new(mem);
        let cache = BlockCache::new(BlockCacheConfig {
            block_size: 1 << 20,
            memory_budget: 64 << 20,
            num_shards: 4,
            ..Default::default()
        });
        let store = CachingObjectStore::new(counting.clone(), cache, 42);
        (counting, store, path)
    }

    fn seq(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn second_get_ranges_issues_zero_upstream_requests() {
        let data = seq((1 << 20) * 2 + 500);
        let (counting, store, path) = setup(&data);
        let ranges = vec![10..2000u64, (1 << 20) - 5..(1 << 20) + 300];

        let a = store.get_ranges(&path, &ranges).await.unwrap();
        let calls_after_first = counting.calls();
        assert!(calls_after_first >= 1);

        let b = store.get_ranges(&path, &ranges).await.unwrap();
        assert_eq!(counting.calls(), calls_after_first, "second read must be all hits");
        assert_eq!(a, b);
        for (bytes, r) in b.iter().zip(&ranges) {
            assert_eq!(&bytes[..], &data[r.start as usize..r.end as usize]);
        }
    }

    #[tokio::test]
    async fn byte_exact_vs_unwrapped_store() {
        let data = seq((1 << 20) * 3 + 4242);
        let (_counting, store, path) = setup(&data);
        let mem = Arc::new(InMemory::new());
        futures::executor::block_on(mem.put(&path, PutPayload::from(data.clone()))).unwrap();

        let mut state: u64 = 99;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 20
        };
        for _ in 0..64 {
            let a = (next() as usize) % data.len();
            let b = (next() as usize) % data.len();
            let (lo, hi) = if a <= b { (a, b + 1) } else { (b, a + 1) };
            let hi = hi.min(data.len());
            if lo == hi {
                continue;
            }
            let cached = store.get_range(&path, lo as u64..hi as u64).await.unwrap();
            let direct = mem.get_range(&path, lo as u64..hi as u64).await.unwrap();
            assert_eq!(cached, direct, "mismatch for {lo}..{hi}");
        }
    }

    #[tokio::test]
    async fn get_opts_conditional_passes_through() {
        let data = seq(1 << 20);
        let (counting, store, path) = setup(&data);
        // Warm the cache.
        store.get_range(&path, 0..100).await.unwrap();
        let before = counting.calls();

        // A conditional get must bypass the cache and hit the inner store.
        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..100)),
            if_match: Some("\"nope\"".to_string()),
            ..Default::default()
        };
        let _ = store.get_opts(&path, opts).await; // precondition may fail; that's fine
        assert!(counting.calls() > before, "conditional get must reach inner store");
    }

    #[tokio::test]
    async fn put_invalidates_cached_blocks() {
        let data = seq(1 << 20);
        let (counting, store, path) = setup(&data);
        store.get_range(&path, 0..100).await.unwrap();
        let before = counting.calls();

        // Overwrite through the wrapper: must invalidate, so the next read re-fetches.
        let new = seq(1 << 20).iter().map(|b| b.wrapping_add(1)).collect::<Vec<_>>();
        store
            .put(&path, PutPayload::from(new.clone()))
            .await
            .unwrap();
        let out = store.get_range(&path, 0..100).await.unwrap();
        assert!(counting.calls() > before, "read after put must re-fetch");
        assert_eq!(&out[..], &new[0..100]);
    }
}
