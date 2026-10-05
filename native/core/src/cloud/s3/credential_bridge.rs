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

//! JNI bridge to the JVM `CometS3CredentialDispatcher` SPI, exposed as
//! `object_store::CredentialProvider` (raw Parquet path) and `reqsign_core::ProvideCredential`
//! (Iceberg via `opendal`). See `docs/source/contributor-guide/s3-credential-provider-design.md`.

use crate::execution::operators::ExecutionError;
use crate::jvm_bridge::{jni_new_global_ref, jni_static_call, JVMClasses};
use async_trait::async_trait;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use jni::objects::{Global, JFieldID, JObject, JObjectArray, JString, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::strings::JNIString;
use jni::sys::jint;
use log::warn;
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use reqsign_core::time::Timestamp;
use reqsign_core::{
    Context, Error as ReqsignError, ErrorKind as ReqsignErrorKind,
    ProvideCredential as IcebergProvideCredential,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Expiry the Iceberg path assumes when the provider does not report one. It bounds how long a
/// long-lived reader or writer reuses a credential, but it can outlast a short-lived credential,
/// so a provider that knows the expiry should report it. Shared with the IRSA web-identity
/// provider (`super::web_identity`).
pub(crate) const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

/// How long before its expiry a bridge stops reusing a credential and asks the provider again. It
/// matches the refresh-ahead of the other credential caches Comet keeps, and it covers object_store,
/// which signs a request once and sends that signature again on every retry for up to 3 minutes by
/// default.
pub(crate) const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(300);

/// The earliest `expirationEpochMillis` taken at face value, 2000-01-01T00:00:00Z. An earlier one is
/// almost always seconds since the epoch sent as milliseconds.
const EARLIEST_PLAUSIBLE_EXPIRY_MILLIS: i64 = 946_684_800_000;

/// Once-per-process latch for the "missing expiry" warning. Bridges live as long as their entry in
/// the executor's FileIO cache, so a per-bridge latch would re-log for every new configuration.
static WARNED_MISSING_EXPIRY: OnceCell<()> = OnceCell::new();

/// Once-per-process latch for the "implausible expiry" warning.
static WARNED_IMPLAUSIBLE_EXPIRY: OnceCell<()> = OnceCell::new();

/// When a provider's credential stops working, as its `expirationEpochMillis` says.
#[derive(Debug, PartialEq, Eq)]
enum Expiry {
    /// `0` or negative, or too early to be a real expiry: the provider does not know.
    Unknown,
    /// Too far ahead to represent, as `Long.MAX_VALUE` is: the credential does not expire.
    Never,
    At(Timestamp),
}

impl Expiry {
    fn from_millis(millis: i64) -> Self {
        if millis <= 0 {
            return Expiry::Unknown;
        }
        if millis < EARLIEST_PLAUSIBLE_EXPIRY_MILLIS {
            if WARNED_IMPLAUSIBLE_EXPIRY.set(()).is_ok() {
                warn!(
                    "CometS3CredentialProvider returned expirationEpochMillis {millis}, which is \
                     before 2000 and probably in seconds; treating the expiry as unknown"
                );
            }
            return Expiry::Unknown;
        }
        Timestamp::from_millisecond(millis).map_or(Expiry::Never, Expiry::At)
    }
}

/// A bridge's last provider call: its outcome, its number among the bridge's calls, and until when
/// its credential may be reused.
struct LastFetch<E> {
    number: u64,
    result: Result<RawCredentials, E>,
    reusable_until: Option<Timestamp>,
}

/// The provider calls of one bridge. A bridge stands for one bucket and path, which on a
/// location-scoped store is one policy location, so this coordinates the requests for one location
/// and never makes another location's wait.
///
/// - At most one provider call runs at a time. A request that waited for one shares its outcome, a
///   credential or an error, rather than asking again, so a burst of requests costs one call.
/// - A credential with a known expiry is reused until [`REFRESH_BEFORE_EXPIRY`] before that
///   expiry, so the provider is asked about once per credential rather than once per request
///   (Parquet) or storage call (Iceberg).
/// - A credential whose expiry is unknown, or that does not expire, is not reused: a request that
///   arrives after the call that fetched it asks again.
struct CredentialCache<E> {
    /// How many provider calls have completed. A request reads it before it waits for the lock, so
    /// it can tell whether a call completed while it waited.
    completed: AtomicU64,
    last: Mutex<Option<LastFetch<E>>>,
}

impl<E> Default for CredentialCache<E> {
    fn default() -> Self {
        Self {
            completed: AtomicU64::new(0),
            last: Mutex::new(None),
        }
    }
}

impl<E: Clone> CredentialCache<E> {
    /// The credential for a request at `now`: the outcome of a call that completed while the
    /// request waited, or a kept credential that is still fresh, or else what `fetch` returns.
    fn get_or_fetch(
        &self,
        now: Timestamp,
        fetch: impl FnOnce() -> Result<RawCredentials, E>,
    ) -> Result<RawCredentials, E> {
        let seen = self.completed.load(Ordering::Acquire);
        let mut slot = self.last.lock();
        if let Some(last) = slot.as_ref() {
            if last.number > seen {
                return last.result.clone();
            }
            if let (Ok(raw), Some(until)) = (&last.result, last.reusable_until) {
                if now < until {
                    return Ok(raw.clone());
                }
            }
        }
        let result = fetch();
        let reusable_until = match &result {
            Ok(raw) => match Expiry::from_millis(raw.expiration_epoch_millis) {
                Expiry::At(at) if now < at - REFRESH_BEFORE_EXPIRY => {
                    Some(at - REFRESH_BEFORE_EXPIRY)
                }
                _ => None,
            },
            Err(_) => None,
        };
        let number = slot.as_ref().map_or(0, |last| last.number) + 1;
        *slot = Some(LastFetch {
            number,
            result: result.clone(),
            reusable_until,
        });
        self.completed.store(number, Ordering::Release);
        result
    }
}

/// Access intent forwarded to the Java SPI. Ordinal must match the JVM `CometS3AccessMode` enum.
#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    Read = 0,
    Write = 1,
}

/// Credential provider that delegates to the JVM SPI via JNI. Instances live in the executor's
/// FileIO and object store caches, so one serves many tasks. `handle` is the JVM-side identity
/// for the `(provider_class, dispatch_key, catalog_properties)` triple returned by
/// `ensureInitialized`. `bucket_jstr` / `path_jstr` are interned once at construction to avoid
/// per-call `new_string` allocations on the hot path.
///
/// Granularity: although the JVM SPI accepts `(bucket, path)`, neither
/// `object_store::CredentialProvider::get_credential` nor
/// `reqsign_core::ProvideCredential::provide_credential` carries a per-request path, so the
/// effective identity is per-bucket (Parquet) or per-table-location (Iceberg). A provider that
/// implements `CometS3LocationScopedCredentialProvider` gets one bridge per policy location
/// instead, on both paths; see `parquet::objectstore::location_scoped` and
/// `execution::operators::iceberg_location_scoped`.
pub struct CometS3CredentialBridge {
    provider_class: String,
    dispatch_key: String,
    bucket: String,
    path: String,
    mode: AccessMode,
    handle: i64,
    bucket_jstr: Arc<Global<JString<'static>>>,
    path_jstr: Arc<Global<JString<'static>>>,
    /// The credential this bridge last fetched, while it is fresh. A derived bridge starts with an
    /// empty cache, since it asks about another path.
    cache: CredentialCache<String>,
}

impl fmt::Debug for CometS3CredentialBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CometS3CredentialBridge")
            .field("provider_class", &self.provider_class)
            .field("dispatch_key", &self.dispatch_key)
            .field("handle", &self.handle)
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .field("mode", &self.mode)
            .finish()
    }
}

impl CometS3CredentialBridge {
    pub fn new(
        provider_class: impl Into<String>,
        dispatch_key: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
        mode: AccessMode,
        catalog_properties: &HashMap<String, String>,
    ) -> Result<Self, ExecutionError> {
        let provider_class = provider_class.into();
        let dispatch_key = dispatch_key.into();
        let bucket = bucket.into();
        let path = path.into();

        let (bucket_jstr, path_jstr) = JVMClasses::with_env(|env| -> Result<_, ExecutionError> {
            let b = env
                .new_string(&bucket)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(bucket): {e}")))?;
            let p = env
                .new_string(&path)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(path): {e}")))?;
            let b_g =
                Arc::new(jni_new_global_ref!(env, b).map_err(|e| {
                    ExecutionError::GeneralError(format!("global_ref(bucket): {e}"))
                })?);
            let p_g = Arc::new(
                jni_new_global_ref!(env, p)
                    .map_err(|e| ExecutionError::GeneralError(format!("global_ref(path): {e}")))?,
            );
            Ok((b_g, p_g))
        })?;

        let handle = ensure_initialized(&provider_class, &dispatch_key, catalog_properties)?;
        Ok(Self {
            provider_class,
            dispatch_key,
            bucket,
            path,
            mode,
            handle,
            bucket_jstr,
            path_jstr,
            cache: CredentialCache::default(),
        })
    }

    /// Returns a bridge to the same provider registration for another path in the bucket. It
    /// shares this bridge's handle and bucket string and creates only the path string, so it makes
    /// no `ensureInitialized` call and needs no class loading on the calling thread.
    pub fn for_path(&self, path: impl Into<String>) -> Result<Self, ExecutionError> {
        let path = path.into();
        let path_jstr = JVMClasses::with_env(|env| -> Result<_, ExecutionError> {
            let p = env
                .new_string(&path)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(path): {e}")))?;
            Ok(Arc::new(jni_new_global_ref!(env, p).map_err(|e| {
                ExecutionError::GeneralError(format!("global_ref(path): {e}"))
            })?))
        })?;
        Ok(Self {
            provider_class: self.provider_class.clone(),
            dispatch_key: self.dispatch_key.clone(),
            bucket: self.bucket.clone(),
            path,
            mode: self.mode,
            handle: self.handle,
            bucket_jstr: Arc::clone(&self.bucket_jstr),
            path_jstr,
            cache: CredentialCache::default(),
        })
    }

    /// The provider's credential for this bridge's bucket, path and mode, shared with concurrent
    /// requests and reused while it is fresh. See [`CredentialCache`]. A failure is shared as its
    /// message, which is all the callers report.
    fn credential(&self) -> Result<RawCredentials, String> {
        self.cache.get_or_fetch(Timestamp::now(), || {
            self.fetch_raw().map_err(|e| e.to_string())
        })
    }

    /// Returns a bridge to the same provider registration for a path in any bucket. Like
    /// [`Self::for_path`] it makes no `ensureInitialized` call. The Iceberg path keys the
    /// registration by catalog name, or by the reference bucket when there is none, and the
    /// provider is always given the bucket it is asked about, so one registration serves every
    /// bucket a table's files are in.
    pub fn for_location(
        &self,
        bucket: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self, ExecutionError> {
        let bucket = bucket.into();
        let path = path.into();
        let (bucket_jstr, path_jstr) = JVMClasses::with_env(|env| -> Result<_, ExecutionError> {
            let b = env
                .new_string(&bucket)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(bucket): {e}")))?;
            let p = env
                .new_string(&path)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(path): {e}")))?;
            let b_g =
                Arc::new(jni_new_global_ref!(env, b).map_err(|e| {
                    ExecutionError::GeneralError(format!("global_ref(bucket): {e}"))
                })?);
            let p_g = Arc::new(
                jni_new_global_ref!(env, p)
                    .map_err(|e| ExecutionError::GeneralError(format!("global_ref(path): {e}")))?,
            );
            Ok((b_g, p_g))
        })?;
        Ok(Self {
            provider_class: self.provider_class.clone(),
            dispatch_key: self.dispatch_key.clone(),
            bucket,
            path,
            mode: self.mode,
            handle: self.handle,
            bucket_jstr,
            path_jstr,
            cache: CredentialCache::default(),
        })
    }

    fn fetch_raw(&self) -> Result<RawCredentials, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<RawCredentials, ExecutionError> {
            let mode = self.mode as jint;

            let creds_obj: JObject = unsafe {
                jni_static_call!(env,
                    comet_s3_credential_dispatcher.get_credentials_for_path(
                        self.handle,
                        self.bucket_jstr.as_obj(),
                        self.path_jstr.as_obj(),
                        mode
                    ) -> JObject
                )?
            };
            if creds_obj.is_null() {
                return Err(ExecutionError::GeneralError(
                    "getCredentialsForPath returned null (contract violation)".to_string(),
                ));
            }

            let d = &JVMClasses::get().comet_s3_credential_dispatcher;
            Ok(RawCredentials {
                access_key_id: read_required_string(
                    env,
                    &creds_obj,
                    d.field_access_key_id,
                    "accessKeyId",
                )?,
                secret_access_key: read_required_string(
                    env,
                    &creds_obj,
                    d.field_secret_access_key,
                    "secretAccessKey",
                )?,
                session_token: read_optional_string(env, &creds_obj, d.field_session_token)?,
                expiration_epoch_millis: unsafe {
                    env.get_field_unchecked(
                        &creds_obj,
                        d.field_expiration_epoch_millis,
                        ReturnType::Primitive(Primitive::Long),
                    )
                }
                .and_then(|v| v.j())
                .map_err(|e| {
                    ExecutionError::GeneralError(format!("read expirationEpochMillis: {e}"))
                })?,
            })
        })
    }

    /// Returns the bucket's policy locations when the provider implements
    /// `CometS3LocationScopedCredentialProvider`, or `None` for any other provider. The
    /// dispatcher copies the provider's list into a `String[]`, so provider code, including a lazy
    /// list, runs inside the checked JNI call and its exceptions come back as errors here.
    pub fn policy_locations(&self) -> Result<Option<Vec<String>>, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<Option<Vec<String>>, ExecutionError> {
            let locations: JObject = unsafe {
                jni_static_call!(env,
                    comet_s3_credential_dispatcher.get_policy_locations(
                        self.handle,
                        self.bucket_jstr.as_obj()
                    ) -> JObject
                )?
            };
            if locations.is_null() {
                return Ok(None);
            }
            // SAFETY: `getPolicyLocations` is declared to return `String[]`, and the dispatcher
            // rejects null elements, so every element is a non-null `java.lang.String`.
            let locations = unsafe { JObjectArray::<JObject>::from_raw(env, locations.into_raw()) };
            let len = locations.len(env).map_err(|e| {
                ExecutionError::GeneralError(format!("policy locations length: {e}"))
            })?;
            let mut out = Vec::with_capacity(len);
            for i in 0..len {
                let element = locations.get_element(env, i).map_err(|e| {
                    ExecutionError::GeneralError(format!("policy location {i}: {e}"))
                })?;
                let element = unsafe { JString::from_raw(&*env, element.into_raw()) };
                let location = element.try_to_string(env).map_err(|e| {
                    ExecutionError::GeneralError(format!("policy location {i}: {e}"))
                })?;
                // A bucket can have more locations than the local frame holds, so free each one.
                env.delete_local_ref(element);
                out.push(location);
            }
            Ok(Some(out))
        })
    }
}

fn ensure_initialized(
    provider_class: &str,
    dispatch_key: &str,
    catalog_properties: &HashMap<String, String>,
) -> Result<i64, ExecutionError> {
    JVMClasses::with_env(|env| -> Result<i64, ExecutionError> {
        let provider_class_jstr = env.new_string(provider_class).map_err(|e| {
            ExecutionError::GeneralError(format!("new_string(provider_class): {e}"))
        })?;
        let dispatch_key_jstr = env
            .new_string(dispatch_key)
            .map_err(|e| ExecutionError::GeneralError(format!("new_string(dispatch_key): {e}")))?;
        let props_obj = build_java_string_map(env, catalog_properties)?;

        let handle: i64 = unsafe {
            jni_static_call!(env,
                comet_s3_credential_dispatcher.ensure_initialized(
                    &provider_class_jstr, &dispatch_key_jstr, &props_obj
                ) -> i64
            )?
        };
        Ok(handle)
    })
}

/// Construct a `java.util.HashMap<String,String>` and populate it. Called once per bridge at
/// construction, so per-call HashMap/put cost stays off the hot path.
fn build_java_string_map<'a>(
    env: &mut jni::Env<'a>,
    map: &HashMap<String, String>,
) -> Result<JObject<'a>, ExecutionError> {
    let hashmap_class = env
        .find_class(JNIString::new("java/util/HashMap"))
        .map_err(|e| ExecutionError::GeneralError(format!("find_class(HashMap): {e}")))?;
    let ctor = env
        .get_method_id(
            &hashmap_class,
            jni::jni_str!("<init>"),
            jni::jni_sig!("(I)V"),
        )
        .map_err(|e| ExecutionError::GeneralError(format!("HashMap.<init>(I): {e}")))?;
    let put = env
        .get_method_id(
            &hashmap_class,
            jni::jni_str!("put"),
            jni::jni_sig!("(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"),
        )
        .map_err(|e| ExecutionError::GeneralError(format!("HashMap.put: {e}")))?;
    let initial_capacity = JValue::Int(map.len() as jint);
    let instance =
        unsafe { env.new_object_unchecked(&hashmap_class, ctor, &[initial_capacity.as_jni()]) }
            .map_err(|e| ExecutionError::GeneralError(format!("new HashMap(int): {e}")))?;

    for (k, v) in map {
        let k_jstr = env
            .new_string(k)
            .map_err(|e| ExecutionError::GeneralError(format!("new_string(key): {e}")))?;
        let v_jstr = env
            .new_string(v)
            .map_err(|e| ExecutionError::GeneralError(format!("new_string(value): {e}")))?;
        let prev = unsafe {
            env.call_method_unchecked(
                &instance,
                put,
                ReturnType::Object,
                &[
                    JValue::Object(&k_jstr).as_jni(),
                    JValue::Object(&v_jstr).as_jni(),
                ],
            )
        }
        .map_err(|e| ExecutionError::GeneralError(format!("HashMap.put call: {e}")))?;
        // Discard return value; Java would have reused the existing key but our maps have no dupes.
        let _ = prev.l();
    }

    Ok(instance)
}

#[derive(Clone)]
struct RawCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    /// Absolute expiry. `0` means the provider did not report one.
    expiration_epoch_millis: i64,
}

/// The bridge could not get a credential from the provider, as opposed to S3 rejecting one. It is
/// the source of the error `get_credential` returns, which `object_store` passes through to the
/// read unchanged. A `LocationScopedObjectStore` treats it like a 403, because a provider that has
/// no policy for a location throws, and that can mean the location changed since its snapshot.
#[derive(Debug)]
pub(crate) struct CredentialProviderError(pub(crate) String);

impl fmt::Display for CredentialProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CredentialProviderError {}

#[async_trait]
impl CredentialProvider for CometS3CredentialBridge {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        // object_store's credential carries no expiry, and object_store asks for one on every
        // request, so the bridge's cache is what honors the provider's expiry on this path.
        let raw = self
            .credential()
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: Box::new(CredentialProviderError(e)),
            })?;
        Ok(Arc::new(AwsCredential {
            key_id: raw.access_key_id,
            secret_key: raw.secret_access_key,
            token: raw.session_token,
        }))
    }
}

impl IcebergProvideCredential for CometS3CredentialBridge {
    type Credential = IcebergAwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let raw = self
            .credential()
            .map_err(|e| ReqsignError::new(ReqsignErrorKind::CredentialInvalid, e))?;

        let expires_in = match Expiry::from_millis(raw.expiration_epoch_millis) {
            Expiry::At(at) => Some(at),
            Expiry::Never => None,
            Expiry::Unknown => {
                if WARNED_MISSING_EXPIRY.set(()).is_ok() {
                    warn!(
                        "CometS3CredentialProvider returned credentials without expiration; \
                     defaulting to {}s expiry to bound opendal caching",
                        DEFAULT_EXPIRY_WHEN_UNKNOWN.as_secs()
                    );
                }
                Some(Timestamp::now() + DEFAULT_EXPIRY_WHEN_UNKNOWN)
            }
        };

        Ok(Some(IcebergAwsCredential {
            access_key_id: raw.access_key_id,
            secret_access_key: raw.secret_access_key,
            session_token: raw.session_token,
            expires_in,
        }))
    }
}

fn read_required_string(
    env: &mut jni::Env,
    instance: &JObject,
    field: JFieldID,
    name: &str,
) -> Result<String, ExecutionError> {
    read_optional_string(env, instance, field)?
        .ok_or_else(|| ExecutionError::GeneralError(format!("{name} was null")))
}

fn read_optional_string(
    env: &mut jni::Env,
    instance: &JObject,
    field: JFieldID,
) -> Result<Option<String>, ExecutionError> {
    let value = unsafe { env.get_field_unchecked(instance, field, ReturnType::Object) }
        .and_then(|v| v.l())
        .map_err(|e| ExecutionError::GeneralError(format!("get_field_unchecked: {e}")))?;
    if value.is_null() {
        return Ok(None);
    }
    let jstr = unsafe { JString::from_raw(env, value.into_raw()) };
    jstr.try_to_string(env)
        .map(Some)
        .map_err(|e| ExecutionError::GeneralError(format!("try_to_string: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    const NOW_MILLIS: i64 = 1_790_000_000_000;
    const HOUR_MILLIS: i64 = 3_600_000;

    fn now() -> Timestamp {
        Timestamp::from_millisecond(NOW_MILLIS).unwrap()
    }

    fn credential(key: &str, expiration_epoch_millis: i64) -> RawCredentials {
        RawCredentials {
            access_key_id: key.to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expiration_epoch_millis,
        }
    }

    #[test]
    fn reads_what_an_expiry_says() {
        assert_eq!(Expiry::from_millis(0), Expiry::Unknown);
        assert_eq!(Expiry::from_millis(-1), Expiry::Unknown);
        // Seconds sent as milliseconds would be a day in January 1970.
        assert_eq!(Expiry::from_millis(NOW_MILLIS / 1000), Expiry::Unknown);
        assert_eq!(Expiry::from_millis(i64::MAX), Expiry::Never);
        assert_eq!(Expiry::from_millis(NOW_MILLIS), Expiry::At(now()));
    }

    #[test]
    fn reuses_a_credential_until_shortly_before_it_expires() {
        let cache = CredentialCache::default();
        let fetches = AtomicUsize::new(0);
        let fetch = || {
            let n = fetches.fetch_add(1, SeqCst);
            let expiry = NOW_MILLIS + (n as i64 + 1) * HOUR_MILLIS;
            Ok::<_, ()>(credential(&format!("key-{n}"), expiry))
        };
        let key_at = |minutes: u64| {
            cache
                .get_or_fetch(now() + Duration::from_secs(minutes * 60), fetch)
                .unwrap()
                .access_key_id
        };
        assert_eq!(key_at(0), "key-0");
        assert_eq!(key_at(54), "key-0", "more than five minutes left");
        assert_eq!(key_at(56), "key-1", "within five minutes of the expiry");
        assert_eq!(fetches.load(SeqCst), 2);
    }

    #[test]
    fn asks_every_time_for_a_credential_it_cannot_keep() {
        // An unknown expiry, one that never comes, and one within five minutes.
        for expiration in [0, i64::MAX, NOW_MILLIS + 60_000] {
            let cache = CredentialCache::default();
            let fetches = AtomicUsize::new(0);
            let fetch = || {
                fetches.fetch_add(1, SeqCst);
                Ok::<_, ()>(credential("key", expiration))
            };
            cache.get_or_fetch(now(), fetch).unwrap();
            cache.get_or_fetch(now(), fetch).unwrap();
            assert_eq!(fetches.load(SeqCst), 2, "expiration {expiration}");
        }
    }

    #[test]
    fn a_failed_fetch_is_not_kept() {
        let cache = CredentialCache::default();
        assert!(cache
            .get_or_fetch(now(), || Err::<RawCredentials, _>("provider threw"))
            .is_err());
        let fetched = cache
            .get_or_fetch(now(), || {
                Ok::<_, &str>(credential("key", NOW_MILLIS + HOUR_MILLIS))
            })
            .unwrap();
        assert_eq!(fetched.access_key_id, "key");
    }

    /// Requests that overlap a provider call share its outcome even when it cannot be kept, a
    /// credential or an error, so they cost one call rather than one call after another.
    #[test]
    fn concurrent_requests_share_a_fetch_they_cannot_keep() {
        let outcomes: [Result<i64, &str>; 4] = [
            Ok(0),
            Ok(i64::MAX),
            Ok(NOW_MILLIS + 60_000),
            Err("provider threw"),
        ];
        for outcome in outcomes {
            let cache = Arc::new(CredentialCache::default());
            let fetches = Arc::new(AtomicUsize::new(0));
            let start = Arc::new(std::sync::Barrier::new(8));
            let requests: Vec<_> = (0..8)
                .map(|_| {
                    let (cache, fetches, start) =
                        (Arc::clone(&cache), Arc::clone(&fetches), Arc::clone(&start));
                    std::thread::spawn(move || {
                        start.wait();
                        cache
                            .get_or_fetch(now(), || {
                                fetches.fetch_add(1, SeqCst);
                                std::thread::sleep(Duration::from_millis(100));
                                outcome.map(|expiry| credential("key", expiry))
                            })
                            .map(|c| c.access_key_id)
                    })
                })
                .collect();
            for request in requests {
                assert_eq!(request.join().unwrap(), outcome.map(|_| "key".to_string()));
            }
            assert_eq!(fetches.load(SeqCst), 1, "outcome {outcome:?}");
        }
    }

    #[test]
    fn concurrent_requests_wait_for_one_fetch() {
        let cache = Arc::new(CredentialCache::default());
        let fetches = Arc::new(AtomicUsize::new(0));
        let requests: Vec<_> = (0..8)
            .map(|_| {
                let (cache, fetches) = (Arc::clone(&cache), Arc::clone(&fetches));
                std::thread::spawn(move || {
                    cache
                        .get_or_fetch(now(), || {
                            fetches.fetch_add(1, SeqCst);
                            std::thread::sleep(Duration::from_millis(20));
                            Ok::<_, ()>(credential("key", NOW_MILLIS + HOUR_MILLIS))
                        })
                        .unwrap()
                })
            })
            .collect();
        for request in requests {
            assert_eq!(request.join().unwrap().access_key_id, "key");
        }
        assert_eq!(fetches.load(SeqCst), 1);
    }
}
