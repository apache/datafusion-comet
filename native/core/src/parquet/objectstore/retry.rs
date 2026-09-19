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

//! Correctness safety net for the scope-hint SPI.
//!
//! # Why
//!
//! `CometS3ScopedCredentialProvider::getPolicyLocationsFor` is *advisory*: vendors are
//! encouraged to report narrower scopes than the policy grants, so a request outside the
//! reported scope can still legitimately fail with 403 at S3. The scope hint is a latency
//! optimization that lets Comet share a bridge across paths inside one scope, saving one
//! JVM round-trip per get. S3 itself remains authoritative.
//!
//! This wrapper is the mechanism that keeps that split honest: on a 403 from a cached
//! (scope-bound) store, we invoke a caller-provided rebuild closure exactly once, passing
//! it the `Path` of the request that failed. The rebuild constructs a fresh bridge bound
//! to that path, re-fires the SPI against it (so the vendor answers `getPolicyLocationsFor`
//! *for the actual failing request*), and appends the resulting scope entry to the outer
//! `object_store` registry cache — alongside any pre-existing entries, not replacing them.
//! We then retry the same operation against the rebuilt store; a second 403 propagates
//! unchanged. Because the new entry is inserted with the vendor's fresh (narrower) scope
//! rather than an all-covering catchall, disjoint scoped stores on the same bucket can
//! continue to coexist after a 403 recovery.
//!
//! # Non-goals
//!
//! - Not a generic retry policy. Only `Error::PermissionDenied` (403) is intercepted; every
//!   other error (including 401/`Unauthenticated`) passes through as-is.
//! - Not an infinite retry. Exactly one rebuild per wrapper instance, exactly one retry per
//!   operation.
//! - Not a stream-level retry. `list`, `list_with_offset`, and `delete_stream` return
//!   `BoxStream`s whose per-item errors are surfaced as-is; wrapping them would require
//!   materializing the stream. Comet's parquet path first hits 403 at `get_opts`/`get_ranges`
//!   which are covered; the rebuild there populates the shared cache and subsequent stream
//!   requests use the newly-inserted scope entry.
//! - Not applied to `put_multipart_opts`: a partial multipart upload cannot be transparently
//!   retried, so a 403 mid-upload propagates for the caller to handle.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use log::debug;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use once_cell::sync::OnceCell;

/// Rebuild function contract: construct a fresh backing `ObjectStore` scoped for the failing
/// request and register it in the outer `object_store` registry cache. Called at most once
/// per wrapper.
///
/// The `Option<&Path>` argument is the location that triggered the 403 (source path for
/// copy/rename). The rebuild closure passes this into the fresh bridge so the SPI's
/// `getPolicyLocationsFor` reflects the actual failing request rather than the path baked in
/// at construction time. `None` is reserved for callers that intercept a 403 without a path
/// (none of the current retry sites) and lets the closure fall back to its pre-baked path.
pub type RebuildFn =
    Arc<dyn Fn(Option<&Path>) -> Result<Arc<dyn ObjectStore>> + Send + Sync + 'static>;

/// Wraps an `Arc<dyn ObjectStore>` so a single 403 rebuilds the store once and retries.
///
/// See the module-level doc-comment for rationale and non-goals.
pub struct RetryOn403ObjectStore {
    inner: Arc<dyn ObjectStore>,
    rebuild: RebuildFn,
    /// Populated on the first 403 we successfully recover from. Cached so a subsequent
    /// request against this same wrapper skips straight to the rebuilt store, and a 403 there
    /// is treated as authoritative.
    rebuilt: OnceCell<Arc<dyn ObjectStore>>,
}

impl RetryOn403ObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, rebuild: RebuildFn) -> Self {
        Self {
            inner,
            rebuild,
            rebuilt: OnceCell::new(),
        }
    }

    /// Return the store that should service *this* request: the post-rebuild store when we
    /// have one, else the original.
    fn current(&self) -> Arc<dyn ObjectStore> {
        self.rebuilt
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.inner))
    }

    /// Whether we have already spent our single rebuild allowance.
    fn already_rebuilt(&self) -> bool {
        self.rebuilt.get().is_some()
    }

    /// Attempt to install a rebuilt store, calling `rebuild` at most once. Concurrent 403s
    /// race harmlessly here — `once_cell::sync::OnceCell` guarantees only one initializer
    /// runs; the rest observe the same result. The `path` of the request that first triggered
    /// the rebuild is threaded into the closure so it can request a scope for the actual
    /// failing location.
    fn rebuild_once(&self, path: Option<&Path>) -> Result<Arc<dyn ObjectStore>> {
        self.rebuilt
            .get_or_try_init(|| (self.rebuild)(path))
            .map(Arc::clone)
    }
}

/// Only `PermissionDenied` (S3 403) triggers the retry. `Unauthenticated` (401) is treated as
/// a permanent credential-config error and not retried.
fn is_forbidden(err: &Error) -> bool {
    matches!(err, Error::PermissionDenied { .. })
}

impl fmt::Debug for RetryOn403ObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryOn403ObjectStore")
            .field("inner", &self.inner)
            .field("rebuilt_cached", &self.rebuilt.get().is_some())
            .finish()
    }
}

impl fmt::Display for RetryOn403ObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RetryOn403({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RetryOn403ObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let store = self.current();
        match store.put_opts(location, payload.clone(), opts.clone()).await {
            Err(e) if is_forbidden(&e) && !self.already_rebuilt() => {
                debug!("RetryOn403: 403 on put({location}); rebuilding store");
                let rebuilt = self.rebuild_once(Some(location))?;
                rebuilt.put_opts(location, payload, opts).await
            }
            other => other,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Multipart uploads can't be transparently retried mid-flight; propagate 403 as-is.
        self.current().put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let store = self.current();
        match store.get_opts(location, options.clone()).await {
            Err(e) if is_forbidden(&e) && !self.already_rebuilt() => {
                debug!("RetryOn403: 403 on get({location}); rebuilding store");
                let rebuilt = self.rebuild_once(Some(location))?;
                rebuilt.get_opts(location, options).await
            }
            other => other,
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let store = self.current();
        match store.get_ranges(location, ranges).await {
            Err(e) if is_forbidden(&e) && !self.already_rebuilt() => {
                debug!("RetryOn403: 403 on get_ranges({location}); rebuilding store");
                let rebuilt = self.rebuild_once(Some(location))?;
                rebuilt.get_ranges(location, ranges).await
            }
            other => other,
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Stream-level: per-item 403 propagates. A later `get_opts` failure will invalidate.
        self.current().delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        // Stream-level: see delete_stream comment.
        self.current().list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        // Stream-level: see delete_stream comment.
        self.current().list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let store = self.current();
        match store.list_with_delimiter(prefix).await {
            Err(e) if is_forbidden(&e) && !self.already_rebuilt() => {
                debug!("RetryOn403: 403 on list_with_delimiter; rebuilding store");
                let rebuilt = self.rebuild_once(prefix)?;
                rebuilt.list_with_delimiter(prefix).await
            }
            other => other,
        }
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let store = self.current();
        match store.copy_opts(from, to, options.clone()).await {
            Err(e) if is_forbidden(&e) && !self.already_rebuilt() => {
                debug!("RetryOn403: 403 on copy({from} -> {to}); rebuilding store");
                // Read side (source) is the 403 side for copy.
                let rebuilt = self.rebuild_once(Some(from))?;
                rebuilt.copy_opts(from, to, options).await
            }
            other => other,
        }
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let store = self.current();
        match store.rename_opts(from, to, options.clone()).await {
            Err(e) if is_forbidden(&e) && !self.already_rebuilt() => {
                debug!("RetryOn403: 403 on rename({from} -> {to}); rebuilding store");
                // Read side (source) is the 403 side for rename.
                let rebuilt = self.rebuild_once(Some(from))?;
                rebuilt.rename_opts(from, to, options).await
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::{self, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A test ObjectStore that returns `PermissionDenied` on the first N `get_opts` calls and
    /// then succeeds. Counts how many total gets landed on this instance and reports whether
    /// they were before or after the "quota" ran out.
    #[derive(Debug)]
    struct FlakyStore {
        name: &'static str,
        fail_first: AtomicUsize,
        gets: AtomicUsize,
    }

    impl FlakyStore {
        fn new(name: &'static str, fail_first: usize) -> Self {
            Self {
                name,
                fail_first: AtomicUsize::new(fail_first),
                gets: AtomicUsize::new(0),
            }
        }
        fn gets(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }
    }

    impl fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FlakyStore({})", self.name)
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> Result<PutResult> {
            unimplemented!("not needed for tests")
        }
        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            unimplemented!("not needed for tests")
        }
        async fn get_opts(&self, location: &Path, _options: GetOptions) -> Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let remaining = self.fail_first.load(Ordering::SeqCst);
            if remaining > 0 {
                self.fail_first.fetch_sub(1, Ordering::SeqCst);
                return Err(Error::PermissionDenied {
                    path: location.to_string(),
                    source: format!("{}: quota still {}", self.name, remaining).into(),
                });
            }
            // Return a "not found" — good enough to prove the call was routed to us.
            Err(Error::NotFound {
                path: location.to_string(),
                source: format!("{}: success sentinel", self.name).into(),
            })
        }
        fn delete_stream(
            &self,
            _locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            stream::empty().boxed()
        }
        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            stream::empty().boxed()
        }
        async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
            unimplemented!("not needed for tests")
        }
        async fn copy_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _options: CopyOptions,
        ) -> Result<()> {
            unimplemented!("not needed for tests")
        }
    }

    fn rebuild_to(target: Arc<dyn ObjectStore>, call_count: Arc<AtomicUsize>) -> RebuildFn {
        Arc::new(move |_path: Option<&Path>| {
            call_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&target))
        })
    }

    #[tokio::test]
    async fn passes_through_success() {
        // Both the initial store and the "rebuild" target succeed; rebuild must not fire.
        let initial = Arc::new(FlakyStore::new("initial", 0));
        let rebuilt_target = Arc::new(FlakyStore::new("rebuilt", 0));
        let calls = Arc::new(AtomicUsize::new(0));

        let wrapper = RetryOn403ObjectStore::new(
            Arc::clone(&initial) as Arc<dyn ObjectStore>,
            rebuild_to(
                Arc::clone(&rebuilt_target) as Arc<dyn ObjectStore>,
                Arc::clone(&calls),
            ),
        );

        let err = wrapper
            .get_opts(&Path::from("a"), GetOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
        assert_eq!(initial.gets(), 1);
        assert_eq!(rebuilt_target.gets(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rebuilds_once_then_retries_on_403() {
        // Initial 403s exactly once; the rebuilt store then succeeds.
        let initial = Arc::new(FlakyStore::new("initial", 1));
        let rebuilt_target = Arc::new(FlakyStore::new("rebuilt", 0));
        let calls = Arc::new(AtomicUsize::new(0));

        let wrapper = RetryOn403ObjectStore::new(
            Arc::clone(&initial) as Arc<dyn ObjectStore>,
            rebuild_to(
                Arc::clone(&rebuilt_target) as Arc<dyn ObjectStore>,
                Arc::clone(&calls),
            ),
        );

        let err = wrapper
            .get_opts(&Path::from("a"), GetOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "retry landed on rebuilt (NotFound sentinel), got {err:?}");
        assert_eq!(initial.gets(), 1, "initial fired once (returned 403)");
        assert_eq!(rebuilt_target.gets(), 1, "retry fired against rebuilt");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "rebuild fired once");
    }

    #[tokio::test]
    async fn second_403_after_rebuild_propagates() {
        // Both stores 403 forever. The wrapper should rebuild exactly once, retry, then
        // propagate the rebuilt store's 403 without a third attempt.
        let initial = Arc::new(FlakyStore::new("initial", usize::MAX));
        let rebuilt_target = Arc::new(FlakyStore::new("rebuilt", usize::MAX));
        let calls = Arc::new(AtomicUsize::new(0));

        let wrapper = RetryOn403ObjectStore::new(
            Arc::clone(&initial) as Arc<dyn ObjectStore>,
            rebuild_to(
                Arc::clone(&rebuilt_target) as Arc<dyn ObjectStore>,
                Arc::clone(&calls),
            ),
        );

        let err = wrapper
            .get_opts(&Path::from("a"), GetOptions::default())
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::PermissionDenied { .. }),
            "expected propagated 403, got {err:?}"
        );
        assert_eq!(initial.gets(), 1);
        assert_eq!(rebuilt_target.gets(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rebuild_is_idempotent_across_operations() {
        // First get triggers rebuild + retry; second get should go straight to the rebuilt
        // store without invoking rebuild again.
        let initial = Arc::new(FlakyStore::new("initial", 1));
        let rebuilt_target = Arc::new(FlakyStore::new("rebuilt", 0));
        let calls = Arc::new(AtomicUsize::new(0));

        let wrapper = RetryOn403ObjectStore::new(
            Arc::clone(&initial) as Arc<dyn ObjectStore>,
            rebuild_to(
                Arc::clone(&rebuilt_target) as Arc<dyn ObjectStore>,
                Arc::clone(&calls),
            ),
        );

        let _ = wrapper
            .get_opts(&Path::from("a"), GetOptions::default())
            .await;
        let _ = wrapper
            .get_opts(&Path::from("b"), GetOptions::default())
            .await;
        assert_eq!(initial.gets(), 1, "initial called once (before rebuild)");
        assert_eq!(rebuilt_target.gets(), 2, "both retries + follow-up hit rebuilt");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "rebuild fired exactly once");
    }

    #[tokio::test]
    async fn rebuild_error_is_reported_not_swallowed() {
        // If the rebuild closure itself errors, the original 403 caller sees the rebuild
        // error surfaced (not the original 403). OnceCell stays empty so a later request may
        // retry the rebuild — that's acceptable for a transient rebuild failure.
        let initial = Arc::new(FlakyStore::new("initial", usize::MAX));
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = Arc::clone(&attempts);
        let rebuild: RebuildFn = Arc::new(move |_path: Option<&Path>| {
            attempts_for_closure.fetch_add(1, Ordering::SeqCst);
            Err(Error::Generic {
                store: "test",
                source: "rebuild wired to fail".into(),
            })
        });
        let wrapper =
            RetryOn403ObjectStore::new(Arc::clone(&initial) as Arc<dyn ObjectStore>, rebuild);
        let err = wrapper
            .get_opts(&Path::from("a"), GetOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Generic { .. }), "got {err:?}");
        assert_eq!(initial.gets(), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        // A follow-up 403 tries the rebuild again since the prior attempt failed.
        let err = wrapper
            .get_opts(&Path::from("b"), GetOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Generic { .. }), "got {err:?}");
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "rebuild retried after prior failure");
    }

    #[tokio::test]
    async fn unauthenticated_401_is_not_retried() {
        // 401 → Unauthenticated variant. This wrapper only fires on PermissionDenied (403).
        #[derive(Debug)]
        struct Auth401 {
            called: AtomicUsize,
        }
        impl fmt::Display for Auth401 {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("Auth401")
            }
        }
        #[async_trait]
        impl ObjectStore for Auth401 {
            async fn put_opts(
                &self,
                _location: &Path,
                _payload: PutPayload,
                _opts: PutOptions,
            ) -> Result<PutResult> {
                unimplemented!()
            }
            async fn put_multipart_opts(
                &self,
                _location: &Path,
                _opts: PutMultipartOptions,
            ) -> Result<Box<dyn MultipartUpload>> {
                unimplemented!()
            }
            async fn get_opts(
                &self,
                location: &Path,
                _options: GetOptions,
            ) -> Result<GetResult> {
                self.called.fetch_add(1, Ordering::SeqCst);
                Err(Error::Unauthenticated {
                    path: location.to_string(),
                    source: "401".into(),
                })
            }
            fn delete_stream(
                &self,
                _locations: BoxStream<'static, Result<Path>>,
            ) -> BoxStream<'static, Result<Path>> {
                stream::empty().boxed()
            }
            fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                stream::empty().boxed()
            }
            async fn list_with_delimiter(
                &self,
                _prefix: Option<&Path>,
            ) -> Result<ListResult> {
                unimplemented!()
            }
            async fn copy_opts(
                &self,
                _from: &Path,
                _to: &Path,
                _options: CopyOptions,
            ) -> Result<()> {
                unimplemented!()
            }
        }
        let initial = Arc::new(Auth401 {
            called: AtomicUsize::new(0),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = Arc::clone(&calls);
        let rebuild: RebuildFn = Arc::new(move |_path: Option<&Path>| {
            calls_for_closure.fetch_add(1, Ordering::SeqCst);
            Err(Error::Generic {
                store: "test",
                source: "should not be called on 401".into(),
            })
        });
        let wrapper =
            RetryOn403ObjectStore::new(Arc::clone(&initial) as Arc<dyn ObjectStore>, rebuild);
        let err = wrapper
            .get_opts(&Path::from("a"), GetOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unauthenticated { .. }), "got {err:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "rebuild must not fire on 401");
    }

    /// The rebuild closure must receive the location of the request that triggered the 403,
    /// so the SPI can be re-fired with the actual failing path (rather than whatever was baked
    /// into the pre-rebuild bridge).
    #[tokio::test]
    async fn rebuild_receives_failing_path() {
        let initial = Arc::new(FlakyStore::new("initial", 1));
        let rebuilt_target = Arc::new(FlakyStore::new("rebuilt", 0));
        let observed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let observed_for_closure = Arc::clone(&observed);
        let target_for_closure: Arc<dyn ObjectStore> =
            Arc::clone(&rebuilt_target) as Arc<dyn ObjectStore>;
        let rebuild: RebuildFn = Arc::new(move |path: Option<&Path>| {
            *observed_for_closure.lock().unwrap() =
                path.map(|p| p.to_string());
            Ok(Arc::clone(&target_for_closure))
        });
        let wrapper =
            RetryOn403ObjectStore::new(Arc::clone(&initial) as Arc<dyn ObjectStore>, rebuild);

        let _ = wrapper
            .get_opts(&Path::from("warehouse/db/tbl/part-0"), GetOptions::default())
            .await;
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some("warehouse/db/tbl/part-0"),
            "rebuild received the failing path"
        );
    }

    /// Ensures Send + Sync so it can be inserted into the process-wide store cache.
    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RetryOn403ObjectStore>();
    }

    /// Compiles-only guard: Mutex here just anchors that the wrapper composes with common
    /// external synchronization patterns without moving to a heavier `RwLock<Option<...>>`.
    #[test]
    fn composes_with_arc_mutex_pattern() {
        let _guard: Mutex<Option<Arc<RetryOn403ObjectStore>>> = Mutex::new(None);
    }
}
