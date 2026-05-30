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

//! Construction of a delta-kernel-rs `DefaultEngine` backed by `object_store`.
//!
//! Ported from tantivy4java's `delta_reader/engine.rs` (Apache-2.0) with
//! minor changes: uses Comet's error type instead of `anyhow`, and uses the
//! renamed `object_store_kernel` (object_store 0.12) dependency that kernel
//! requires. Comet's main `object_store = "0.13"` tree is untouched.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use object_store_kernel::aws::AmazonS3Builder;
use object_store_kernel::azure::MicrosoftAzureBuilder;
use object_store_kernel::local::LocalFileSystem;
use object_store_kernel::ObjectStore;

use super::error::{DeltaError, DeltaResult};

/// Concrete engine type returned by [`get_or_create_engine`].
pub type DeltaEngine = DefaultEngine<TokioBackgroundExecutor>;

/// Storage credentials used to construct kernel's engine.
///
/// Mirrors tantivy4java's `DeltaStorageConfig`. Field-per-knob rather than a
/// generic map so we can validate at the boundary; the Scala side will
/// populate this from a Spark options map.
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct DeltaStorageConfig {
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint: Option<String>,
    pub aws_force_path_style: bool,

    pub azure_account_name: Option<String>,
    pub azure_access_key: Option<String>,
    pub azure_bearer_token: Option<String>,
}

/// Build an `ObjectStore` for the given URL and credentials.
///
/// Supports `s3://` / `s3a://`, `az://` / `azure://` / `abfs://` / `abfss://`,
/// and `file://`. Any other scheme is rejected with
/// [`DeltaError::UnsupportedScheme`].
pub fn create_object_store(
    url: &Url,
    config: &DeltaStorageConfig,
) -> DeltaResult<Arc<dyn ObjectStore>> {
    let scheme = url.scheme();

    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" | "s3a" => {
            let bucket = url.host_str().ok_or_else(|| DeltaError::MissingBucket {
                url: url.to_string(),
            })?;
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            if let Some(ref key) = config.aws_access_key {
                builder = builder.with_access_key_id(key);
            }
            if let Some(ref secret) = config.aws_secret_key {
                builder = builder.with_secret_access_key(secret);
            }
            if let Some(ref token) = config.aws_session_token {
                builder = builder.with_token(token);
            }
            if let Some(ref region) = config.aws_region {
                builder = builder.with_region(region);
            }
            if let Some(ref endpoint) = config.aws_endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if config.aws_force_path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            // Allow HTTP endpoints (MinIO, LocalStack, custom S3-compat)
            if config
                .aws_endpoint
                .as_ref()
                .is_some_and(|e| e.starts_with("http://"))
            {
                builder = builder.with_allow_http(true);
            }

            Arc::new(builder.build()?)
        }
        "az" | "azure" | "abfs" | "abfss" => {
            let container = url.host_str().ok_or_else(|| DeltaError::MissingBucket {
                url: url.to_string(),
            })?;
            let mut builder = MicrosoftAzureBuilder::new().with_container_name(container);

            if let Some(ref account) = config.azure_account_name {
                builder = builder.with_account(account);
            }
            if let Some(ref key) = config.azure_access_key {
                builder = builder.with_access_key(key);
            }
            if let Some(ref token) = config.azure_bearer_token {
                builder = builder.with_bearer_token_authorization(token);
            }

            Arc::new(builder.build()?)
        }
        "file" | "" => Arc::new(LocalFileSystem::new()),
        other => {
            return Err(DeltaError::UnsupportedScheme {
                scheme: other.to_string(),
                url: url.to_string(),
            });
        }
    };

    Ok(store)
}

/// Process-wide cache of constructed engines, keyed by (scheme, authority, config).
///
/// Each `DefaultEngine` owns a `TokioBackgroundExecutor` which spawns one std::thread
/// running a current_thread tokio runtime; the runtime's blocking pool (used by
/// kernel for parquet/object_store IO) holds spawned threads for `thread_keep_alive`
/// (~10s) after each spawn_blocking call. Constructing a fresh engine per JNI
/// `planDeltaScan` call therefore accumulates OS threads during regression runs that
/// hit kernel hundreds of times per minute, eventually tripping the per-process
/// thread cap (e.g. `pthread_create EAGAIN` aborts on macOS where `ulimit -u`
/// defaults to ~1300). Sharing one engine per (scheme, authority, config) bounds the
/// thread count by table-storage diversity instead of by request count.
///
/// **LRU-bounded**: the cache holds at most `MAX_CACHE_ENTRIES` engines. When full,
/// the least-recently-used entry is evicted and its `Arc<DeltaEngine>` drops --
/// `DefaultEngine`'s `TokioBackgroundExecutor` joins its OS thread on drop, so the
/// thread count stabilizes even when long-running drivers rotate credentials (e.g.
/// hourly STS / IRSA rotations on production). Without this bound, every rotation
/// produced a new cache entry (because `DeltaStorageConfig` is part of the key and
/// `aws_session_token` rotates) and leaked one tokio thread per rotation -- a
/// production-grade memory + thread leak over days.
///
/// `Arc<DeltaEngine>` is handed out so callers don't hold the mutex while using the
/// engine; concurrent in-flight scans against an evicted engine keep it alive until
/// they complete.
const MAX_CACHE_ENTRIES: usize = 32;

type EngineKey = (String, String, DeltaStorageConfig);

/// Cache state: maps key to (engine, monotonic last-use counter). The counter is the
/// LRU recency stamp; we bump it on every hit AND on every insert, and evict the
/// entry with the smallest counter when full.
struct EngineCacheState {
    map: HashMap<EngineKey, (Arc<DeltaEngine>, u64)>,
    counter: u64,
}

fn engine_cache() -> &'static Mutex<EngineCacheState> {
    static CACHE: OnceLock<Mutex<EngineCacheState>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(EngineCacheState {
            map: HashMap::new(),
            counter: 0,
        })
    })
}

fn engine_key(url: &Url, config: &DeltaStorageConfig) -> EngineKey {
    let scheme = url.scheme().to_string();
    // host+port form the storage target (e.g. S3 bucket, ABFS account); for file://
    // the authority is empty which collapses every local table to a single entry.
    let authority = match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => String::new(),
    };
    (scheme, authority, config.clone())
}

// Suppress dead_code: the standalone constructor stays useful for tests that want
// to exercise a fresh engine without polluting the cache.
#[allow(dead_code)]
pub fn create_engine(table_url: &Url, config: &DeltaStorageConfig) -> DeltaResult<DeltaEngine> {
    let store = create_object_store(table_url, config)?;
    Ok(DefaultEngine::new(store))
}

/// Return a shared `DeltaEngine` for the given URL+config, building one on first use.
///
/// LRU-bounded: when the cache is full, the least-recently-used entry is evicted.
/// In-flight users of an evicted engine keep it alive via their `Arc` clone until
/// they're done; only THEN does the evicted entry's `TokioBackgroundExecutor` join
/// its OS thread.
pub fn get_or_create_engine(
    table_url: &Url,
    config: &DeltaStorageConfig,
) -> DeltaResult<Arc<DeltaEngine>> {
    let key = engine_key(table_url, config);
    // Mutex is held only across the (cheap) HashMap lookup and, on miss, the engine
    // construction. Multi-threaded JNI callers serialize here on first miss per key
    // but proceed lock-free on subsequent hits via the returned Arc clone.
    let mut cache = engine_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.counter = cache.counter.wrapping_add(1);
    let stamp = cache.counter;
    if let Some(entry) = cache.map.get_mut(&key) {
        // Hit: bump the LRU stamp and return the existing Arc.
        entry.1 = stamp;
        return Ok(Arc::clone(&entry.0));
    }
    // Miss: build a fresh engine. If the cache is at capacity, evict the LRU entry
    // first so the bound is respected.
    if cache.map.len() >= MAX_CACHE_ENTRIES {
        if let Some(victim_key) = cache
            .map
            .iter()
            .min_by_key(|(_, (_, s))| *s)
            .map(|(k, _)| k.clone())
        {
            cache.map.remove(&victim_key);
        }
    }
    let store = create_object_store(table_url, config)?;
    let engine = Arc::new(DefaultEngine::new(store));
    cache.map.insert(key, (Arc::clone(&engine), stamp));
    Ok(engine)
}

/// Test-only: clear the cache so tests don't see entries from prior tests.
#[cfg(test)]
pub(crate) fn _clear_cache_for_tests() {
    let mut cache = engine_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.map.clear();
    cache.counter = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    fn empty_config() -> DeltaStorageConfig {
        DeltaStorageConfig::default()
    }

    #[test]
    fn create_object_store_local_file() {
        let store = create_object_store(&url("file:///tmp/x"), &empty_config()).unwrap();
        // Just verify Arc construction succeeded; LocalFileSystem doesn't expose
        // anything we can usefully assert on without doing IO.
        assert!(format!("{store:?}").contains("LocalFileSystem"));
    }

    #[test]
    fn create_object_store_empty_scheme_is_local() {
        // The "file" | "" arm maps the empty-scheme case (URL like `relative/path`
        // wouldn't actually parse, but the arm exists for code paths that hand us
        // a Url with an empty scheme).
        let mut u = url("file:///x");
        u.set_scheme("").ok(); // best-effort; if it fails, the file:// arm still hits
        let store = create_object_store(&u, &empty_config()).unwrap();
        assert!(format!("{store:?}").contains("LocalFileSystem"));
    }

    #[test]
    fn create_object_store_s3_requires_bucket() {
        // `s3://` with empty host is rejected as MissingBucket.
        // url::Url::parse("s3:///x") gives host=None.
        let bad = url("s3:///just-a-path");
        let err = create_object_store(&bad, &empty_config()).unwrap_err();
        match err {
            DeltaError::MissingBucket { .. } => {}
            other => panic!("expected MissingBucket, got {other:?}"),
        }
    }

    #[test]
    fn create_object_store_s3_builds_with_full_creds() {
        let cfg = DeltaStorageConfig {
            aws_access_key: Some("AKIA…".into()),
            aws_secret_key: Some("secret".into()),
            aws_session_token: Some("token".into()),
            aws_region: Some("us-west-2".into()),
            aws_endpoint: Some("https://s3.example.com".into()),
            aws_force_path_style: true,
            ..Default::default()
        };
        let store = create_object_store(&url("s3://my-bucket/path"), &cfg).unwrap();
        assert!(format!("{store:?}").contains("AmazonS3") || format!("{store:?}").contains("S3"));
    }

    #[test]
    fn create_object_store_s3_http_endpoint_allows_http() {
        let cfg = DeltaStorageConfig {
            aws_access_key: Some("k".into()),
            aws_secret_key: Some("s".into()),
            aws_endpoint: Some("http://localhost:9000".into()),
            aws_force_path_style: true,
            ..Default::default()
        };
        // MinIO-style: endpoint starts with http:// → builder enables allow_http.
        // We can't introspect the builder's flag, but ensuring construction
        // succeeds covers the branch.
        create_object_store(&url("s3://minio-bucket"), &cfg).unwrap();
    }

    #[test]
    fn create_object_store_azure_requires_container() {
        let bad = url("abfss:///just-a-path");
        let err = create_object_store(&bad, &empty_config()).unwrap_err();
        assert!(matches!(err, DeltaError::MissingBucket { .. }));
    }

    #[test]
    fn create_object_store_azure_builds_with_creds() {
        let cfg = DeltaStorageConfig {
            azure_account_name: Some("myacct".into()),
            azure_access_key: Some("key".into()),
            azure_bearer_token: Some("bearer".into()),
            ..Default::default()
        };
        // Either "az://", "azure://", "abfs://" or "abfss://" should work.
        for scheme in ["az", "azure", "abfs", "abfss"] {
            let u = url(&format!("{scheme}://my-container/path"));
            create_object_store(&u, &cfg).unwrap();
        }
    }

    #[test]
    fn create_object_store_unsupported_scheme() {
        let err = create_object_store(&url("gs://bucket/p"), &empty_config()).unwrap_err();
        match err {
            DeltaError::UnsupportedScheme { scheme, .. } => assert_eq!(scheme, "gs"),
            other => panic!("expected UnsupportedScheme, got {other:?}"),
        }
    }

    #[test]
    fn engine_key_collapses_local_paths() {
        let cfg = empty_config();
        let a = engine_key(&url("file:///tmp/a"), &cfg);
        let b = engine_key(&url("file:///tmp/b/c/d"), &cfg);
        assert_eq!(a, b, "all local file:// URLs share one engine entry");
    }

    #[test]
    fn engine_key_distinguishes_s3_buckets() {
        let cfg = empty_config();
        let a = engine_key(&url("s3://bucket-a/path"), &cfg);
        let b = engine_key(&url("s3://bucket-b/path"), &cfg);
        assert_ne!(a, b);
    }

    #[test]
    fn engine_key_includes_port() {
        let cfg = empty_config();
        let a = engine_key(&url("s3://host:9000/p"), &cfg);
        let b = engine_key(&url("s3://host:9001/p"), &cfg);
        let c = engine_key(&url("s3://host/p"), &cfg);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn engine_key_distinguishes_credentials() {
        let cfg_a = DeltaStorageConfig {
            aws_access_key: Some("AKIA1".into()),
            ..Default::default()
        };
        let cfg_b = DeltaStorageConfig {
            aws_access_key: Some("AKIA2".into()),
            ..Default::default()
        };
        let a = engine_key(&url("s3://bucket/p"), &cfg_a);
        let b = engine_key(&url("s3://bucket/p"), &cfg_b);
        assert_ne!(
            a, b,
            "different credentials must NOT share a cached engine"
        );
    }

    #[test]
    fn engine_key_path_does_not_affect_key() {
        let cfg = empty_config();
        let a = engine_key(&url("s3://bucket/path/a"), &cfg);
        let b = engine_key(&url("s3://bucket/path/b/c"), &cfg);
        assert_eq!(a, b, "paths within the same bucket share one engine");
    }

    #[test]
    fn get_or_create_engine_returns_same_arc_on_hit() {
        let cfg = empty_config();
        let u = url("file:///tmp/cache-test");
        let e1 = get_or_create_engine(&u, &cfg).unwrap();
        let e2 = get_or_create_engine(&u, &cfg).unwrap();
        assert!(
            Arc::ptr_eq(&e1, &e2),
            "second call must return the cached Arc, not a fresh engine"
        );
    }

    #[test]
    fn get_or_create_engine_distinct_keys_yield_distinct_engines() {
        let cfg = empty_config();
        let e_file = get_or_create_engine(&url("file:///tmp/distinct-a"), &cfg).unwrap();
        // s3:// would actually try to set up an AWS client; use a different file path
        // which collapses to the same key per `engine_key_collapses_local_paths`. So we
        // exercise a distinct-key case via a different cred config.
        let cfg_b = DeltaStorageConfig {
            aws_access_key: Some("dummy".into()),
            ..Default::default()
        };
        let e_creds = get_or_create_engine(&url("file:///tmp/distinct-a"), &cfg_b).unwrap();
        assert!(
            !Arc::ptr_eq(&e_file, &e_creds),
            "differing config keys must yield distinct engines"
        );
    }

    #[test]
    fn get_or_create_engine_evicts_lru_when_full() {
        _clear_cache_for_tests();
        // Build MAX_CACHE_ENTRIES distinct engines, each with a distinct credential
        // tuple so they all key uniquely against the same local URL.
        let urls_and_engines: Vec<(String, Arc<DeltaEngine>)> = (0..MAX_CACHE_ENTRIES)
            .map(|i| {
                let cfg = DeltaStorageConfig {
                    aws_access_key: Some(format!("key-{i}")),
                    ..Default::default()
                };
                let url_s = format!("file:///tmp/lru-{i}");
                let eng = get_or_create_engine(&url(&url_s), &cfg).unwrap();
                (format!("key-{i}"), eng)
            })
            .collect();

        // Cache is now exactly full.
        assert_eq!(
            engine_cache().lock().unwrap().map.len(),
            MAX_CACHE_ENTRIES,
            "cache should be at capacity after filling it"
        );

        // Touch entry 1 so it becomes most-recently-used. Entry 0 is now LRU.
        let cfg_1 = DeltaStorageConfig {
            aws_access_key: Some(urls_and_engines[1].0.clone()),
            ..Default::default()
        };
        let _hit_1 = get_or_create_engine(&url("file:///tmp/lru-1"), &cfg_1).unwrap();

        // Insert one more -- entry 0 (LRU after the touch) should be evicted.
        let cfg_new = DeltaStorageConfig {
            aws_access_key: Some("key-new".into()),
            ..Default::default()
        };
        let _new = get_or_create_engine(&url("file:///tmp/lru-new"), &cfg_new).unwrap();

        assert_eq!(
            engine_cache().lock().unwrap().map.len(),
            MAX_CACHE_ENTRIES,
            "cache size should stay at capacity after eviction"
        );

        // Hitting key-0 again should construct a fresh engine (not return the original Arc).
        let cfg_0 = DeltaStorageConfig {
            aws_access_key: Some(urls_and_engines[0].0.clone()),
            ..Default::default()
        };
        let fresh_0 = get_or_create_engine(&url("file:///tmp/lru-0"), &cfg_0).unwrap();
        assert!(
            !Arc::ptr_eq(&urls_and_engines[0].1, &fresh_0),
            "key-0 was LRU and should have been evicted -> fresh engine on re-insert"
        );
    }
}
