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

//! Helpers shared between the Iceberg scan and Iceberg write operators.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Weak};

use datafusion::common::DataFusionError;
use iceberg::io::{FileIO, FileIOBuilder, StorageConfig, StorageFactory};
use iceberg::{Error as IcebergError, ErrorKind as IcebergErrorKind};
use iceberg_storage_opendal::{CustomAwsCredentialLoader, OpenDalStorageFactory};
use parking_lot::Mutex;

use crate::cloud::s3::credential_bridge::{AccessMode, CometS3CredentialBridge};
use crate::cloud::s3::policy_locations::ROOT_CREDENTIAL_PATH;
use crate::cloud::s3::web_identity::take_over_if_irsa;
use crate::execution::operators::iceberg_location_scoped::{
    BucketLocationSource, LocationScopedS3StorageFactory, LocationStorageFactory, SharedLocations,
};
use crate::parquet::objectstore::s3_blob_fs_support::{
    is_s3_compliant_alias_scheme, BlobHostPromotingS3StorageFactory,
};

/// Activation key for the `CometS3CredentialProvider` SPI, read from a catalog's `s3.*` property
/// bag.
const ICEBERG_PROVIDER_CLASS_PROPERTY: &str = "s3.comet.credential.provider.class";

/// Key prefixes forwarded to iceberg-rust's `FileIO`. The full unfiltered catalog bag (catalog
/// URI, OAuth tokens, credentials.uri, tenant-id, etc.) is kept upstream so
/// `CometS3CredentialBridge` can read whatever the vendor needs. `opendal.` carries
/// iceberg-storage-opendal's own settings, such as `opendal.io-timeout-ms`.
const STORAGE_PROPERTY_PREFIXES: &[&str] = &["s3.", "gcs.", "adls.", "client.", "opendal."];

/// Pick an OpenDAL storage backend for a URI whose scheme `builtin_storage_schemes` lists for
/// `access_mode`, or that is an opted-in S3-compliant alias written in lowercase; anything else is
/// rejected before the match, so the list alone decides what each mode admits. For S3, the Comet
/// credential bridge is wired in when a provider class is configured and `access_mode` is
/// forwarded to the JVM SPI.
///
/// The bool is false when a read fell back to opendal's default chain because the configured S3
/// access provider failed to initialise; such a `FileIO` must not be cached, so the next task
/// retries.
pub(crate) fn storage_factory_for(
    path: &str,
    catalog_properties: &HashMap<String, String>,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<(Arc<dyn StorageFactory>, bool), DataFusionError> {
    // Verbatim match: OpenDAL strips the scheme prefix from every path case-sensitively at open
    // time, so admitting `S3://` here would only defer the failure. Aliases are held to the same
    // rule (`is_iceberg_alias_scheme`). The JVM gates match verbatim.
    let scheme = scheme_of(path);
    if !builtin_storage_schemes(access_mode).contains(&scheme)
        && !is_s3_family_scheme(scheme, catalog_properties)
    {
        return Err(DataFusionError::Execution(format!(
            "Unsupported storage scheme: {scheme}"
        )));
    }
    match scheme {
        "file" => Ok((Arc::new(OpenDalStorageFactory::Fs), true)),
        "memory" => Ok((Arc::new(OpenDalStorageFactory::Memory), true)),
        "gs" => Ok((Arc::new(OpenDalStorageFactory::Gcs), true)),
        "oss" => Ok((Arc::new(OpenDalStorageFactory::Oss), true)),
        // s3, s3a, and any opted-in s3-compliant alias (e.g. blob) route to the S3 backend. Listed
        // last so the built-in backends above stay authoritative even if one of their schemes is
        // also named in `fs.comet.s3Compliant.schemes`. An alias additionally gets a wrapper that
        // promotes a HOSTLESS `blob:///bucket/key` into the host at the open boundary -- see
        // s3_blob_fs_support for why that never touches the recorded delete-matching string. A
        // location-scoped provider gets a storage that serves each file with the credential of
        // its policy location -- see iceberg_location_scoped.
        s if is_s3_family_scheme(s, catalog_properties) => {
            let alias = is_iceberg_alias_scheme(s, catalog_properties);
            let (access, cacheable) =
                build_s3_access(path, catalog_properties, catalog_name, access_mode)?;
            let factory: Arc<dyn StorageFactory> = match access {
                S3Access::LocationScoped(shared) => {
                    Arc::new(LocationScopedS3StorageFactory::new(shared, alias))
                }
                S3Access::Loader(customized_credential_load) if alias => Arc::new(
                    BlobHostPromotingS3StorageFactory::new(customized_credential_load),
                ),
                S3Access::Loader(customized_credential_load) => {
                    Arc::new(OpenDalStorageFactory::S3 {
                        customized_credential_load,
                    })
                }
            };
            Ok((factory, cacheable))
        }
        // Only a listed scheme without an arm reaches here; the tests below fail on that.
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported storage scheme: {scheme}"
        ))),
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct FileIoCacheKey {
    access_mode: u8,
    catalog_name: String,
    /// The full path: the S3 access bridge is scoped to the exact path it was built for.
    reference_path: String,
    properties: Vec<(String, String)>,
}

impl fmt::Debug for FileIoCacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileIoCacheKey")
            .field("access_mode", &self.access_mode)
            .field("catalog_name", &self.catalog_name)
            .field("reference_path", &self.reference_path)
            .finish()
    }
}

impl FileIoCacheKey {
    /// `None` for `memory:///`, whose namespace must stay private to its task.
    fn new(
        catalog_properties: &HashMap<String, String>,
        reference_path: &str,
        catalog_name: &str,
        access_mode: AccessMode,
    ) -> Option<Self> {
        if scheme_of(reference_path) == "memory" {
            return None;
        }
        Some(Self {
            access_mode: access_mode as u8,
            catalog_name: catalog_name.to_string(),
            reference_path: reference_path.to_string(),
            properties: sorted_properties(catalog_properties),
        })
    }
}

const FILE_IO_CACHE_CAPACITY: usize = 64;

/// Least recently used entries are evicted first.
struct FileIoCache {
    entries: HashMap<FileIoCacheKey, FileIO>,
    order: VecDeque<FileIoCacheKey>,
    capacity: usize,
}

impl FileIoCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get(&mut self, key: &FileIoCacheKey) -> Option<FileIO> {
        let file_io = self.entries.get(key)?.clone();
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            let recent = self.order.remove(pos)?;
            self.order.push_back(recent);
        }
        Some(file_io)
    }

    /// Returns the replaced or evicted `FileIO` so the caller can drop it outside the lock.
    fn insert(&mut self, key: FileIoCacheKey, file_io: FileIO) -> Option<FileIO> {
        if let Some(previous) = self.entries.insert(key.clone(), file_io) {
            return Some(previous);
        }
        self.order.push_back(key);
        if self.entries.len() > self.capacity {
            let oldest = self.order.pop_front()?;
            return self.entries.remove(&oldest);
        }
        None
    }
}

/// Shared per executor so tasks reuse one FileIO: its factory, parsed config and access bridge. The
/// OpenDAL S3 backend builds an operator, and with it a signer, for each storage call; the bridge's
/// own credential reuse is what spans calls.
static FILE_IO_CACHE: LazyLock<Mutex<FileIoCache>> =
    LazyLock::new(|| Mutex::new(FileIoCache::new(FILE_IO_CACHE_CAPACITY)));

pub fn clear_file_io_cache() {
    let dropped: Vec<FileIO> = {
        let mut cache = FILE_IO_CACHE.lock();
        cache.order.clear();
        cache.entries.drain().map(|(_, file_io)| file_io).collect()
    };
    drop(dropped);
    LOCATION_SCOPED.clear();
}

fn sorted_properties(catalog_properties: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut properties: Vec<(String, String)> = catalog_properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    properties.sort();
    properties
}

/// A provider registration and access mode, which a location-scoped provider's locations and
/// location storages belong to: the `FileIO` cache key without the reference path. The dispatch
/// key and properties are what `ensureInitialized` registers a provider by.
#[derive(Clone, PartialEq, Eq, Hash)]
struct RegistrationKey {
    access_mode: u8,
    dispatch_key: String,
    properties: Vec<(String, String)>,
}

impl RegistrationKey {
    fn new(
        access_mode: AccessMode,
        dispatch_key: &str,
        catalog_properties: &HashMap<String, String>,
    ) -> Self {
        Self {
            access_mode: access_mode as u8,
            dispatch_key: dispatch_key.to_string(),
            properties: sorted_properties(catalog_properties),
        }
    }
}

/// Values shared by every holder of one key. The registry keeps only a weak reference, so a value
/// lives as long as some holder still has it.
struct Registry<K, V> {
    slots: Mutex<HashMap<K, Arc<Mutex<Weak<V>>>>>,
}

impl<K: Clone + Eq + Hash, V> Registry<K, V> {
    fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// Runs `f` on `key`'s slot, which holds the key's value while it is alive. Calls for one key
    /// run one at a time, so a value `f` stores is what every later call finds.
    fn with_slot<R>(&self, key: &K, f: impl FnOnce(&mut Weak<V>) -> R) -> R {
        let slot = {
            let mut slots = self.slots.lock();
            // Drop the slots nobody is using whose value is gone. Only this map holds such a slot,
            // so nobody else can have it locked.
            slots.retain(|_, slot| Arc::strong_count(slot) > 1 || slot.lock().strong_count() > 0);
            Arc::clone(slots.entry(key.clone()).or_default())
        };
        let mut value = slot.lock();
        f(&mut value)
    }

    fn clear(&self) {
        self.slots.lock().clear();
    }
}

/// The shared locations of each location-scoped provider registration. A registration's cached
/// `FileIO`s keep its locations alive; see `build_s3_access`.
static LOCATION_SCOPED: LazyLock<Registry<RegistrationKey, SharedLocations>> =
    LazyLock::new(Registry::new);

fn cached_file_io(
    cache: &Mutex<FileIoCache>,
    key: Option<FileIoCacheKey>,
    build: impl FnOnce() -> Result<(FileIO, bool), DataFusionError>,
) -> Result<FileIO, DataFusionError> {
    let Some(key) = key else {
        return Ok(build()?.0);
    };
    if let Some(file_io) = cache.lock().get(&key) {
        return Ok(file_io);
    }
    let (file_io, cacheable) = build()?;
    if cacheable {
        // Dropped after the lock is released: the last clone of a FileIO releases JNI global refs.
        let evicted = cache.lock().insert(key, file_io.clone());
        drop(evicted);
    }
    Ok(file_io)
}

/// The single point of change for the built-in schemes `storage_factory_for` admits per access
/// mode; the JVM read and write gates load these lists over JNI. `memory` is write-only: an OpenDAL
/// memory backend is a fresh empty in-process store the write path assembles manifests in, so a
/// read finds nothing. `oss` is read-only: no `oss.*` property is forwarded and no test covers it.
pub(crate) fn builtin_storage_schemes(access_mode: AccessMode) -> &'static [&'static str] {
    match access_mode {
        AccessMode::Read => &["file", "gs", "oss", "s3", "s3a"],
        AccessMode::Write => &["file", "memory", "gs", "s3", "s3a"],
    }
}

pub(crate) fn load_file_io(
    catalog_properties: &HashMap<String, String>,
    reference_path: &str,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<FileIO, DataFusionError> {
    cached_file_io(
        &FILE_IO_CACHE,
        FileIoCacheKey::new(
            catalog_properties,
            reference_path,
            catalog_name,
            access_mode,
        ),
        || {
            build_file_io(
                catalog_properties,
                reference_path,
                catalog_name,
                access_mode,
            )
        },
    )
}

/// Build a `FileIO` whose storage scheme is inferred from `reference_path` and whose properties
/// come from the catalog. The reference path is the metadata location for reads or the data
/// location for writes — anything that carries the right URI scheme. `catalog_name` is the
/// credential dispatch key and `access_mode` is the access intent forwarded to the S3 credential
/// bridge, so the write path can request write-capable credentials.
fn build_file_io(
    catalog_properties: &HashMap<String, String>,
    reference_path: &str,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<(FileIO, bool), DataFusionError> {
    let (factory, cacheable) = storage_factory_for(
        reference_path,
        catalog_properties,
        catalog_name,
        access_mode,
    )?;
    let mut file_io_builder = FileIOBuilder::new(factory);

    // Narrow to storage-prefix keys before forwarding to iceberg-rust's FileIO. The full
    // unfiltered bag (catalog URI, OAuth tokens, credentials.uri, tenant-id, etc.) is kept
    // upstream so CometS3CredentialBridge can read whatever the vendor needs.
    for (key, value) in catalog_properties {
        if STORAGE_PROPERTY_PREFIXES.iter().any(|p| key.starts_with(p)) {
            file_io_builder = file_io_builder.with_prop(key, value);
        }
    }

    // Object-store's AmazonS3Builder defaults the SigV4 region to `us-east-1` when unset;
    // iceberg-storage-opendal's S3 factory instead errors with `region is missing. Please
    // find it by S3::detect_region() or set them in env.` Non-AWS S3-compliant storage
    // services accept any region in the credential, so default to `us-east-1` -- but ONLY when
    // neither the catalog NOR the AWS environment supplies a region. opendal/reqsign reads
    // `AWS_REGION` / `AWS_DEFAULT_REGION`, so forcing `us-east-1` unconditionally would
    // override an `AWS_REGION=us-west-2` and break auth for AWS buckets outside us-east-1.
    // Both catalog key spellings iceberg-rust reads (`client.region` wins over `s3.region`).
    //
    // reference_path reaches here raw (see storage_factory_for), so alias schemes must match too.
    // Scheme is tested first so a file://, gs:// or oss:// FileIO does not pay the env lookups.
    let scheme = scheme_of(reference_path);
    if is_s3_family_scheme(scheme, catalog_properties)
        && !catalog_properties.contains_key("s3.region")
        && !catalog_properties.contains_key("client.region")
        && !env_region_present()
    {
        file_io_builder = file_io_builder.with_prop("s3.region", "us-east-1");
    }

    Ok((file_io_builder.build(), cacheable))
}

/// How the S3 backend gets its credentials.
pub(crate) enum S3Access {
    /// One loader signs every file of the `FileIO`, or `None` leaves opendal's default chain.
    Loader(Option<CustomAwsCredentialLoader>),
    /// The configured provider implements `CometS3LocationScopedCredentialProvider`, so each file
    /// is served with the credential of its policy location. The locations are the registration's,
    /// shared by every `FileIO` built for it.
    LocationScoped(Arc<SharedLocations>),
}

/// Wires the configured Comet credential provider into opendal's S3 service.
/// `Ok(S3Access::Loader(None))` means no provider is configured (or the path carries no bucket) and
/// opendal's default credential chain applies. When a provider IS configured but fails to
/// initialize, the failure mode depends on the access intent: reads warn and fall back to the
/// default chain (a wrong-credential read fails on permissions), but writes fail closed --
/// silently switching which credentials perform a write after the configured provider failed is
/// not acceptable. A location-scoped provider that cannot list its locations fails both, as on the
/// Parquet path, rather than falling back to one credential for every file.
pub(crate) fn build_s3_access(
    reference_path: &str,
    catalog_properties: &HashMap<String, String>,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<(S3Access, bool), DataFusionError> {
    let Ok(url) = url::Url::parse(reference_path) else {
        return Ok((S3Access::Loader(None), true));
    };
    let Some(bucket) = url.host_str() else {
        return Ok((S3Access::Loader(None), true));
    };
    let Some(provider_class) = catalog_properties
        .get(ICEBERG_PROVIDER_CLASS_PROPERTY)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    else {
        // No explicit Comet provider class. On EKS/IRSA, take over credential resolution with the
        // Comet web-identity provider (retry on STS throttle, no node-role downgrade, shared
        // jittered cache) instead of leaving it to opendal's default reqsign chain, which
        // downgrades to the node instance role under throttling. Non-IRSA setups (static keys,
        // env, profile) keep the default chain via S3Access::Loader(None). We also defer to any
        // credentials the user configured explicitly in the catalog (static keys or an
        // assume-role arn) -- explicit config always wins, same as a named provider class does.
        let explicit = has_explicit_s3_credentials(catalog_properties);
        // Config keys arrive on the Iceberg side under the `s3.` prefix (that is how a catalog
        // property reaches the FileIO property bag, the same as `s3.comet.credential.provider.class`),
        // so resolve the bare keys under that prefix.
        return Ok((
            S3Access::Loader(
                take_over_if_irsa(explicit, |key| {
                    catalog_properties.get(&format!("s3.{key}")).cloned()
                })
                .map(CustomAwsCredentialLoader::new),
            ),
            true,
        ));
    };
    // Fall back to the bucket when the table has no catalog identity (e.g. HadoopTables loaded by
    // raw path).
    let dispatch_key: &str = if catalog_name.is_empty() {
        bucket
    } else {
        catalog_name
    };
    // A location-scoped provider's locations belong to its registration, not to this table, so
    // every FileIO of the registration shares them, however many tables and commits it spans.
    // Builds for one registration run one at a time: a registration already known to be
    // location-scoped needs no bridge and no provider call, and otherwise the first build asks the
    // provider while the rest wait for its answer.
    let key = RegistrationKey::new(access_mode, dispatch_key, catalog_properties);
    LOCATION_SCOPED.with_slot(&key, |slot| {
        if let Some(shared) = slot.upgrade() {
            shared
                .ensure_bucket(bucket)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            return Ok((S3Access::LocationScoped(shared), true));
        }
        let bridge = CometS3CredentialBridge::new(
            provider_class,
            dispatch_key,
            bucket,
            url.path(),
            access_mode,
            catalog_properties,
        );
        match bridge {
            Ok(bridge) => match bridge.policy_locations() {
                Ok(None) => Ok((
                    S3Access::Loader(Some(CustomAwsCredentialLoader::new(bridge))),
                    true,
                )),
                Ok(Some(locations)) => {
                    let shared = Arc::new(location_scoped_state(bridge, bucket, locations)?);
                    *slot = Arc::downgrade(&shared);
                    Ok((S3Access::LocationScoped(shared), true))
                }
                Err(e) => Err(DataFusionError::Execution(format!(
                    "Failed to get policy locations for {bucket} from {provider_class}: {e}"
                ))),
            },
            Err(e) => match access_mode {
                AccessMode::Write => Err(DataFusionError::Execution(format!(
                    "Configured S3 credential provider {provider_class} failed to initialize: \
                     {e}; refusing to write through the default opendal credential chain"
                ))),
                AccessMode::Read => {
                    log::warn!(
                        "Failed to initialize CometS3CredentialBridge for {provider_class}: {e}; \
                         falling back to default opendal credential chain"
                    );
                    Ok((S3Access::Loader(None), false))
                }
            },
        }
    })
}

/// The shared locations of a location-scoped provider's registration, seeded with `locations` for
/// `bucket`. A bucket's locations come from a bridge derived from `bridge` for that bucket, and
/// each location's storage signs with a bridge derived for that location, so neither calls
/// `ensureInitialized` again.
fn location_scoped_state(
    bridge: CometS3CredentialBridge,
    bucket: &str,
    locations: Vec<String>,
) -> Result<SharedLocations, DataFusionError> {
    let bridge = Arc::new(bridge);
    let source_bridge = Arc::clone(&bridge);
    // A blocking JVM call, which the storage makes through `run_blocking`.
    let source: BucketLocationSource = Arc::new(move |bucket: &str| {
        source_bridge
            .for_location(bucket, ROOT_CREDENTIAL_PATH)
            .and_then(|bridge| bridge.policy_locations())
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("Failed to get policy locations for {bucket}: {e}"),
                )
            })?
            .ok_or_else(|| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("The provider for {bucket} stopped returning policy locations"),
                )
            })
    });
    let storage_factory: LocationStorageFactory = Arc::new(
        move |config: &StorageConfig, bucket: &str, credential_path: &str| {
            let location_bridge = bridge.for_location(bucket, credential_path).map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("CometS3CredentialBridge init failed for {bucket}: {e}"),
                )
            })?;
            OpenDalStorageFactory::S3 {
                customized_credential_load: Some(CustomAwsCredentialLoader::new(location_bridge)),
            }
            .build(config)
        },
    );
    SharedLocations::new(bucket, locations, source, storage_factory)
        .map_err(|e| DataFusionError::Execution(e.to_string()))
}

/// True if the catalog configures S3 credentials explicitly: static access keys
/// (`s3.access-key-id` + `s3.secret-access-key`) or an assume-role arn (`client.assume-role.arn`).
/// When it does, the Comet web-identity take-over stands aside so opendal uses what the user asked
/// for. Key names mirror iceberg-rust's `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` /
/// `S3_ASSUME_ROLE_ARN`.
fn has_explicit_s3_credentials(catalog_properties: &HashMap<String, String>) -> bool {
    let has = |key: &str| {
        catalog_properties
            .get(key)
            .is_some_and(|v| !v.trim().is_empty())
    };
    (has("s3.access-key-id") && has("s3.secret-access-key")) || has("client.assume-role.arn")
}

/// True if the AWS environment supplies a region (see the region defaulting in `load_file_io`).
fn env_region_present() -> bool {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.is_empty()))
}

/// Extracts the URI scheme, defaulting to `file` for schemeless local paths (e.g. `/tmp/x`).
///
/// Splits on the first `:` (RFC 3986), NOT `://`: hostless vendor forms like `blob:/bucket/key`
/// (opaque, empty authority) carry no `://`, and treating them as `file` would route an
/// S3-compliant scan to the local filesystem. A `/` before the `:` means there is no scheme (the
/// `:` sits inside a path segment, e.g. `/tmp/a:b`), so those and truly schemeless paths default
/// to `file`.
///
/// The JVM write gate (`CometIcebergNativeWrite.storageScheme`) mirrors this rule exactly, case
/// included. Change both together, and keep the cases in
/// `scheme_of_extracts_scheme_from_all_uri_forms` in step with its `storageScheme` test.
pub(crate) fn scheme_of(path: &str) -> &str {
    match path.split_once(':') {
        Some((scheme, _)) if !scheme.is_empty() && !scheme.contains('/') => scheme,
        _ => "file",
    }
}

/// True if `scheme` routes to the S3 backend: `s3`/`s3a`, or any opted-in s3-compliant alias
/// (`fs.comet.s3Compliant.schemes`). The Scala `NativeConfig.isS3FamilyScheme` additionally treats
/// `s3n` as S3-family for bucket resolution; `s3n` is intentionally omitted here because
/// iceberg-rust's storage factory has no `s3n` backend and the Scala Iceberg scheme gate
/// (`isIcebergReadableScheme`) rejects `s3n` before a path ever reaches this operator.
fn is_s3_family_scheme(scheme: &str, catalog_properties: &HashMap<String, String>) -> bool {
    matches!(scheme, "s3" | "s3a") || is_iceberg_alias_scheme(scheme, catalog_properties)
}

/// True if `scheme` is an opted-in S3-compliant alias written exactly as the lowercase form of its
/// list entry. The shared `is_s3_compliant_alias_scheme` is case-insensitive, which is safe for
/// the Parquet path because it rewrites an alias URL to `s3://` before anything opens it. The
/// Iceberg path opens the recorded location as written, and OpenDAL's S3 backend checks it against
/// a `scheme://bucket/` prefix whose scheme comes from `Url::parse` and so is lowercase, so
/// `BLOB://bucket/key` would pass a case-insensitive gate here and fail at open time. The JVM
/// Iceberg gate matches the same way.
fn is_iceberg_alias_scheme(scheme: &str, catalog_properties: &HashMap<String, String>) -> bool {
    !scheme.bytes().any(|b| b.is_ascii_uppercase())
        && is_s3_compliant_alias_scheme(scheme, catalog_properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registry_shares_a_value_while_it_lives() {
        let registry = Registry::<&str, String>::new();
        let value = Arc::new("locations".to_string());
        registry.with_slot(&"cat", |slot| *slot = Arc::downgrade(&value));
        let found = registry.with_slot(&"cat", |slot| slot.upgrade()).unwrap();
        assert!(Arc::ptr_eq(&found, &value));
        drop((found, value));
        assert!(registry.with_slot(&"cat", |slot| slot.upgrade()).is_none());
    }

    #[test]
    fn a_registry_drops_the_slots_of_values_that_are_gone() {
        let registry = Registry::<u32, String>::new();
        for key in 0..100 {
            let value = Arc::new(key.to_string());
            registry.with_slot(&key, |slot| *slot = Arc::downgrade(&value));
        }
        let kept = Arc::new("kept".to_string());
        registry.with_slot(&1000, |slot| *slot = Arc::downgrade(&kept));
        registry.with_slot(&1001, |_| ());
        assert_eq!(
            registry.slots.lock().len(),
            2,
            "the live value's slot and 1001's"
        );
    }

    /// Concurrent builds for one registration wait for the first, which creates the value.
    #[test]
    fn concurrent_callers_for_one_key_create_its_value_once() {
        let registry = Arc::new(Registry::<&str, String>::new());
        let created = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callers: Vec<_> = (0..8)
            .map(|_| {
                let (registry, created) = (Arc::clone(&registry), Arc::clone(&created));
                std::thread::spawn(move || {
                    registry.with_slot(&"cat", |slot| {
                        if let Some(value) = slot.upgrade() {
                            return value;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        created.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let value = Arc::new("locations".to_string());
                        *slot = Arc::downgrade(&value);
                        value
                    })
                })
            })
            .collect();
        let values: Vec<_> = callers.into_iter().map(|c| c.join().unwrap()).collect();
        assert_eq!(created.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(values.iter().all(|v| Arc::ptr_eq(v, &values[0])));
    }

    fn local_file_io() -> FileIO {
        build_file_io(
            &HashMap::new(),
            "file:///tmp/warehouse",
            "",
            AccessMode::Read,
        )
        .unwrap()
        .0
    }

    #[test]
    fn cache_key_separates_client_configurations() {
        let props = HashMap::from([("s3.region".to_string(), "eu-west-1".to_string())]);
        let table = "s3://bucket/warehouse/db/t";
        let key = |mode| FileIoCacheKey::new(&props, table, "cat", mode);
        assert_ne!(key(AccessMode::Read), key(AccessMode::Write));
        assert_ne!(
            key(AccessMode::Read),
            FileIoCacheKey::new(
                &props,
                "s3://bucket/warehouse/db/other",
                "cat",
                AccessMode::Read
            )
        );
        assert_ne!(
            key(AccessMode::Read),
            FileIoCacheKey::new(&props, table, "other_cat", AccessMode::Read)
        );
        let mut moved = props.clone();
        moved.insert("s3.endpoint".to_string(), "http://minio:9000".to_string());
        assert_ne!(
            key(AccessMode::Read),
            FileIoCacheKey::new(&moved, table, "cat", AccessMode::Read)
        );
        assert!(
            FileIoCacheKey::new(&HashMap::new(), "memory:///", "", AccessMode::Write).is_none()
        );
    }

    #[test]
    fn cache_key_debug_does_not_leak_properties() {
        let props = HashMap::from([
            (
                "s3.secret-access-key".to_string(),
                "super-secret-key".to_string(),
            ),
            (
                "s3.session-token".to_string(),
                "super-secret-token".to_string(),
            ),
        ]);
        let key = FileIoCacheKey::new(
            &props,
            "s3://bucket/warehouse/db/t",
            "prod_catalog",
            AccessMode::Write,
        )
        .unwrap();
        assert_eq!(
            format!("{key:?}"),
            "FileIoCacheKey { access_mode: 1, catalog_name: \"prod_catalog\", \
             reference_path: \"s3://bucket/warehouse/db/t\" }"
        );
    }

    #[test]
    fn cached_file_io_builds_once_per_key_and_always_without_a_key() {
        let cache = Mutex::new(FileIoCache::new(4));
        let key = FileIoCacheKey::new(
            &HashMap::new(),
            "file:///tmp/warehouse",
            "",
            AccessMode::Read,
        );
        let mut builds = 0;
        for _ in 0..2 {
            cached_file_io(&cache, key.clone(), || {
                builds += 1;
                Ok((local_file_io(), true))
            })
            .unwrap();
        }
        assert_eq!(builds, 1);
        for _ in 0..2 {
            cached_file_io(&cache, None, || {
                builds += 1;
                Ok((local_file_io(), true))
            })
            .unwrap();
        }
        assert_eq!(builds, 3);
        assert_eq!(cache.lock().entries.len(), 1);
    }

    #[test]
    fn cached_file_io_does_not_cache_a_degraded_build() {
        let cache = Mutex::new(FileIoCache::new(4));
        let key = FileIoCacheKey::new(
            &HashMap::new(),
            "file:///tmp/warehouse",
            "",
            AccessMode::Read,
        );
        let mut builds = 0;
        for _ in 0..2 {
            cached_file_io(&cache, key.clone(), || {
                builds += 1;
                Ok((local_file_io(), false))
            })
            .unwrap();
        }
        assert_eq!(builds, 2);
        assert!(cache.lock().entries.is_empty());
    }

    #[test]
    fn load_file_io_does_not_cache_memory() {
        load_file_io(&HashMap::new(), "memory:///", "", AccessMode::Write).unwrap();
        assert!(FILE_IO_CACHE
            .lock()
            .entries
            .keys()
            .all(|k| scheme_of(&k.reference_path) != "memory"));
    }

    /// The JVM sets this key from `spark.comet.iceberg.ioTimeout`.
    #[test]
    fn io_timeout_reaches_the_file_io() {
        let key = iceberg_storage_opendal::OPENDAL_IO_TIMEOUT_MS;
        assert_eq!(key, "opendal.io-timeout-ms");
        let props = HashMap::from([(key.to_string(), "30000".to_string())]);
        let (file_io, _) =
            build_file_io(&props, "file:///tmp/warehouse", "", AccessMode::Read).unwrap();
        assert_eq!(file_io.config().get(key).map(String::as_str), Some("30000"));
    }

    #[test]
    fn cache_evicts_the_least_recently_used_entry() {
        let mut cache = FileIoCache::new(2);
        let key = |name: &str| {
            FileIoCacheKey::new(
                &HashMap::new(),
                &format!("file:///{name}"),
                "",
                AccessMode::Read,
            )
            .unwrap()
        };
        assert!(cache.insert(key("a"), local_file_io()).is_none());
        assert!(cache.insert(key("b"), local_file_io()).is_none());
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.insert(key("c"), local_file_io()).is_some());
        assert!(cache.get(&key("b")).is_none());
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("c")).is_some());
        assert!(cache.insert(key("c"), local_file_io()).is_some());
        assert_eq!(cache.entries.len(), 2);
    }

    fn factory_result(path: &str, mode: AccessMode) -> Result<(), String> {
        storage_factory_for(path, &HashMap::new(), "test_cat", mode)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    #[test]
    fn oss_scheme_is_readable_but_not_writable() {
        // CometScanRule admits oss scan locations (through HadoopFileIO), so removing the read
        // arm would regress an existing native-scan capability; writes stay unsupported until
        // oss.* property forwarding exists and is tested.
        assert!(factory_result("oss://bucket/db/table", AccessMode::Read).is_ok());
        let err = factory_result("oss://bucket/db/table", AccessMode::Write).unwrap_err();
        assert!(
            err.contains("Unsupported storage scheme: oss"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn memory_scheme_is_writable_but_not_readable() {
        // The write path assembles manifests in a fresh in-process memory store. A read against
        // a new memory store can never find data, so the factory must decline it up front.
        assert!(factory_result("memory:manifest.avro", AccessMode::Write).is_ok());
        assert!(factory_result("memory:///manifest.avro", AccessMode::Write).is_ok());
        for path in ["memory:manifest.avro", "memory:///key.parquet"] {
            let err = factory_result(path, AccessMode::Read).unwrap_err();
            assert!(
                err.contains("Unsupported storage scheme: memory"),
                "unexpected error for {path}: {err}"
            );
        }
    }

    #[test]
    fn common_schemes_resolve_for_both_modes() {
        for mode in [AccessMode::Read, AccessMode::Write] {
            assert!(factory_result("file:///tmp/x", mode).is_ok());
            assert!(factory_result("/tmp/no-scheme", mode).is_ok());
            // No credential provider configured: the default chain applies in both modes.
            assert!(factory_result("s3://bucket/db/table", mode).is_ok());
            assert!(factory_result("gs://bucket/db/table", mode).is_ok());
        }
    }

    #[test]
    fn scheme_list_pre_check_is_load_bearing() {
        // The oss and memory arms build a backend for either mode; only their absence from the
        // list for the other mode rejects them. Both facts are asserted so that adding an
        // access-mode branch back into an arm, or listing the scheme, breaks this test.
        assert!(factory_result("oss://bucket/path", AccessMode::Read).is_ok());
        assert!(!builtin_storage_schemes(AccessMode::Write).contains(&"oss"));
        let err = factory_result("oss://bucket/path", AccessMode::Write).unwrap_err();
        assert!(
            err.contains("Unsupported storage scheme: oss"),
            "unexpected error: {err}"
        );
        assert!(factory_result("memory:///path", AccessMode::Write).is_ok());
        assert!(!builtin_storage_schemes(AccessMode::Read).contains(&"memory"));
        let err = factory_result("memory:///path", AccessMode::Read).unwrap_err();
        assert!(
            err.contains("Unsupported storage scheme: memory"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mixed_case_scheme_is_rejected_for_both_modes() {
        // OpenDAL strips the scheme prefix from every path case-sensitively at open time
        // (`S3://bucket/key` fails its `s3://bucket/` prefix check), so the factory must not
        // admit what the open cannot serve. The JVM gates match the built-in set verbatim too.
        for mode in [AccessMode::Read, AccessMode::Write] {
            for path in ["S3://bucket/key", "File:///tmp/x", "GS://bucket/key"] {
                let err = factory_result(path, mode).unwrap_err();
                assert!(
                    err.contains("Unsupported storage scheme"),
                    "unexpected error for {path} in {mode:?}: {err}"
                );
            }
        }
    }

    #[test]
    fn alias_scheme_is_matched_verbatim() {
        // An alias location is opened as written, and OpenDAL's S3 backend checks it against a
        // lowercase `scheme://bucket/` prefix, so a mixed-case alias that passed a
        // case-insensitive gate here would only fail at open time. The list entry may be written
        // in any case; the location's scheme must match its lowercase form exactly.
        for listed in ["blob", " BLOB ", "minio,Blob"] {
            let props = HashMap::from([(
                "fs.comet.s3Compliant.schemes".to_string(),
                listed.to_string(),
            )]);
            for mode in [AccessMode::Read, AccessMode::Write] {
                assert!(
                    storage_factory_for("blob://bucket/key", &props, "test_cat", mode).is_ok(),
                    "blob://bucket/key must be admitted for {mode:?} with list {listed:?}"
                );
                for path in [
                    "BLOB://bucket/key",
                    "Blob://bucket/key",
                    "BLOB:///bucket/key",
                ] {
                    let err = storage_factory_for(path, &props, "test_cat", mode)
                        .map(|_| ())
                        .expect_err(&format!(
                            "{path} must be rejected for {mode:?} with list {listed:?}"
                        ))
                        .to_string();
                    assert!(
                        err.contains("Unsupported storage scheme"),
                        "unexpected error for {path} in {mode:?}: {err}"
                    );
                }
            }
        }
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        let err = factory_result("hdfs://nn/db/table", AccessMode::Read).unwrap_err();
        assert!(
            err.contains("Unsupported storage scheme"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn explicit_s3_credentials_detected() {
        let mut props = HashMap::new();
        assert!(!has_explicit_s3_credentials(&props));

        // Access key without a secret is not a complete static credential.
        props.insert("s3.access-key-id".to_string(), "AKIA".to_string());
        assert!(!has_explicit_s3_credentials(&props));
        props.insert("s3.secret-access-key".to_string(), "secret".to_string());
        assert!(has_explicit_s3_credentials(&props));

        // Blank values do not count as configured.
        let mut blank = HashMap::new();
        blank.insert("client.assume-role.arn".to_string(), "  ".to_string());
        assert!(!has_explicit_s3_credentials(&blank));
        blank.insert(
            "client.assume-role.arn".to_string(),
            "arn:aws:iam::1:role/r".to_string(),
        );
        assert!(has_explicit_s3_credentials(&blank));
    }

    #[test]
    fn listed_schemes_are_accepted_by_storage_factory() {
        for mode in [AccessMode::Read, AccessMode::Write] {
            for scheme in builtin_storage_schemes(mode) {
                let url = format!("{scheme}://bucket/path");
                assert!(
                    factory_result(&url, mode).is_ok(),
                    "{scheme} is listed for {mode:?} but the factory rejects it"
                );
            }
        }
        assert!(builtin_storage_schemes(AccessMode::Read).contains(&"oss"));
        assert!(!builtin_storage_schemes(AccessMode::Write).contains(&"oss"));
        assert!(!builtin_storage_schemes(AccessMode::Read).contains(&"memory"));
        assert!(builtin_storage_schemes(AccessMode::Write).contains(&"memory"));
    }

    #[test]
    fn unlisted_schemes_are_rejected_for_both_modes() {
        let unlisted = [
            "hdfs", "abfs", "abfss", "wasb", "wasbs", "gcs", "http", "https", "azure",
        ];
        for mode in [AccessMode::Read, AccessMode::Write] {
            for scheme in unlisted {
                let err = factory_result(&format!("{scheme}://bucket/path"), mode).unwrap_err();
                assert!(
                    err.contains("Unsupported storage scheme"),
                    "unexpected error for {scheme} in {mode:?}: {err}"
                );
                assert!(
                    !builtin_storage_schemes(mode).contains(&scheme),
                    "{scheme} must not be listed for {mode:?}"
                );
            }
        }
    }

    #[test]
    fn scheme_of_extracts_scheme_from_all_uri_forms() {
        // Host-bearing and hostless/opaque vendor forms must resolve to the same scheme, so an
        // S3-compliant scan is not misrouted to the local FS. `blob:/bucket/key` (single slash, the
        // hostless vendor form) regressed here: splitting on `://` returned `file`.
        assert_eq!(scheme_of("blob://bucket/key"), "blob");
        assert_eq!(scheme_of("blob:/bucket/key"), "blob");
        assert_eq!(scheme_of("s3://bucket/key"), "s3");
        assert_eq!(scheme_of("s3:/bucket/key"), "s3");
        // Hadoop normalises `hdfs:///p` to `hdfs:/p`. The JVM write gate must read both as
        // `hdfs` (unsupported) rather than admit the hostless form as `file`.
        assert_eq!(scheme_of("hdfs:/warehouse/t"), "hdfs");
        assert_eq!(scheme_of("hdfs:///warehouse/t"), "hdfs");
        assert_eq!(scheme_of("hdfs://nn:8020/warehouse/t"), "hdfs");
        assert_eq!(scheme_of("memory:/x"), "memory");
        assert_eq!(scheme_of("file:///tmp/x"), "file");
        assert_eq!(scheme_of("file:/tmp/x"), "file");
        // Not lowercased: `storage_factory_for` matches case-sensitively.
        assert_eq!(scheme_of("S3://bucket/key"), "S3");
        // Schemeless and colon-in-path locals default to the local FS.
        assert_eq!(scheme_of("/tmp/no-scheme"), "file");
        assert_eq!(scheme_of("/tmp/a:b"), "file");
    }
}
