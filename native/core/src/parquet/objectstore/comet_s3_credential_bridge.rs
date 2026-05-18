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

//! JNI bridge to the `CometS3CredentialDispatcher` SPI, exposed as
//! `object_store::CredentialProvider` and `reqsign_core::ProvideCredential` for the raw Parquet
//! and Iceberg scan paths respectively.
//!
//! The bridge is activated by setting `fs.s3a.comet.credential.provider.class` (optionally
//! per-bucket) in the Hadoop configuration. The vendor's named class is instantiated once on
//! first use inside the JVM dispatcher and reused for the executor lifetime.
//!
//! ```text
//!   JVM                                        Native (Rust)
//!   ---                                        -------------
//!
//!   fs.s3a.comet.credential.provider.class     s3.rs (object_store)
//!         |                                    iceberg_scan.rs (opendal)
//!         v                                              |
//!   CometS3CredentialDispatcher                          v
//!   (per-class instance cache)                  CometS3CredentialBridge
//!         ^                                       impl object_store::CredentialProvider
//!         |                                       impl reqsign_core::ProvideCredential
//!         |                                              |
//!         +<---- JNI call ----------------------------+
//!         |   getCredentialsForPath(className, bucket, path, mode ordinal)
//!         v
//!   vendor CometS3CredentialProvider
//!         |
//!         v
//!   CometS3Credentials POJO
//!         |
//!         +------- JNI field reads ---------------->+
//!                                                   |
//!                                                   v
//!                                        AwsCredential / IcebergAwsCredential
//!                                        (used to sign S3 requests)
//! ```

use crate::execution::operators::ExecutionError;
use crate::jvm_bridge::{jni_new_global_ref, jni_static_call, JVMClasses};
use async_trait::async_trait;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use jni::objects::{Global, JFieldID, JObject, JString, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::strings::JNIString;
use jni::sys::jint;
use log::warn;
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;
use once_cell::sync::OnceCell;
use reqsign_core::time::Timestamp;
use reqsign_core::{
    Context, Error as ReqsignError, ErrorKind as ReqsignErrorKind,
    ProvideCredential as IcebergProvideCredential,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Hadoop-style config key (without `fs.s3a.` prefix) naming the vendor `CometS3CredentialProvider`
/// FQCN. Iceberg's catalog properties use the same suffix under their `s3.` namespace.
pub(crate) const PROVIDER_CLASS_PROPERTY: &str = "comet.credential.provider.class";

/// Iceberg-namespaced form of [`PROVIDER_CLASS_PROPERTY`], read from a Spark catalog's `s3.*`
/// property bag (`spark.sql.catalog.<name>.s3.comet.credential.provider.class`).
pub(crate) const ICEBERG_PROVIDER_CLASS_PROPERTY: &str = "s3.comet.credential.provider.class";

/// Cap on opendal's credential cache when the provider does not report an expiry. Prevents the
/// executor from holding a stale credential for the entire job lifetime.
const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

/// Once-per-process latch for the "missing expiry" warning; bridges are per-scan so a per-bridge
/// latch would log once per scan on the same misbehaving provider.
static WARNED_MISSING_EXPIRY: OnceCell<()> = OnceCell::new();

/// Access intent forwarded to the Java SPI. Ordinal must match the JVM `CometS3AccessMode` enum.
#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    Read = 0,
    /// No native write path yet; kept so the SPI contract is complete.
    #[allow(dead_code)]
    Write = 1,
}

/// Resolve the configured provider class for the given bucket via `super::s3::get_config_trimmed`,
/// which already implements the per-bucket-then-global `fs.s3a.` lookup. Returns the trimmed FQCN
/// if non-empty.
pub fn lookup_provider_class<'a>(
    configs: &'a HashMap<String, String>,
    bucket: &str,
) -> Option<&'a str> {
    super::s3::get_config_trimmed(configs, bucket, PROVIDER_CLASS_PROPERTY)
        .filter(|s| !s.is_empty())
}

/// Per-request credential provider that delegates to the Java SPI via JNI. Constructed once per S3
/// store or FileIO; calls `ensureInitialized` synchronously at construction so the JVM provider
/// instance is ready before any per-request fetch.
///
/// The four String arguments threaded through every `getCredentialsForPath` call (provider class,
/// dispatch key, bucket, path) are immutable for the bridge's lifetime, so we cache them once at
/// construction as JNI global refs to avoid per-call `env.new_string` allocations on the hot path.
pub struct CometS3CredentialBridge {
    provider_class: String,
    dispatch_key: String,
    bucket: String,
    path: String,
    mode: AccessMode,
    /// Cached JNI globals for the four constant String arguments to `getCredentialsForPath`.
    provider_class_jstr: Arc<Global<JString<'static>>>,
    dispatch_key_jstr: Arc<Global<JString<'static>>>,
    bucket_jstr: Arc<Global<JString<'static>>>,
    path_jstr: Arc<Global<JString<'static>>>,
}

impl fmt::Debug for CometS3CredentialBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CometS3CredentialBridge")
            .field("provider_class", &self.provider_class)
            .field("dispatch_key", &self.dispatch_key)
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .field("mode", &self.mode)
            .finish()
    }
}

impl CometS3CredentialBridge {
    /// Construct the bridge and run a one-shot `ensureInitialized` call against the JVM
    /// dispatcher. `dispatch_key` scopes provider instances on the JVM side: bucket name on the
    /// Parquet path, catalog name on the Iceberg path. `catalog_properties` is forwarded to
    /// `CometS3CredentialProvider.initialize(Map)` exactly once per `(class, dispatchKey)` pair.
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

        let (provider_class_jstr, dispatch_key_jstr, bucket_jstr, path_jstr) =
            JVMClasses::with_env(|env| -> Result<_, ExecutionError> {
                let pc = env.new_string(&provider_class).map_err(|e| {
                    ExecutionError::GeneralError(format!("new_string(provider_class): {e}"))
                })?;
                let dk = env.new_string(&dispatch_key).map_err(|e| {
                    ExecutionError::GeneralError(format!("new_string(dispatch_key): {e}"))
                })?;
                let b = env.new_string(&bucket).map_err(|e| {
                    ExecutionError::GeneralError(format!("new_string(bucket): {e}"))
                })?;
                let p = env
                    .new_string(&path)
                    .map_err(|e| ExecutionError::GeneralError(format!("new_string(path): {e}")))?;
                let pc_g = Arc::new(jni_new_global_ref!(env, pc).map_err(|e| {
                    ExecutionError::GeneralError(format!("global_ref(provider_class): {e}"))
                })?);
                let dk_g = Arc::new(jni_new_global_ref!(env, dk).map_err(|e| {
                    ExecutionError::GeneralError(format!("global_ref(dispatch_key): {e}"))
                })?);
                let b_g = Arc::new(jni_new_global_ref!(env, b).map_err(|e| {
                    ExecutionError::GeneralError(format!("global_ref(bucket): {e}"))
                })?);
                let p_g =
                    Arc::new(jni_new_global_ref!(env, p).map_err(|e| {
                        ExecutionError::GeneralError(format!("global_ref(path): {e}"))
                    })?);
                Ok((pc_g, dk_g, b_g, p_g))
            })?;

        ensure_initialized(&provider_class, &dispatch_key, catalog_properties)?;
        Ok(Self {
            provider_class,
            dispatch_key,
            bucket,
            path,
            mode,
            provider_class_jstr,
            dispatch_key_jstr,
            bucket_jstr,
            path_jstr,
        })
    }

    /// Shared constructor for the s3.rs and iceberg_scan.rs call sites. Returns `Ok(None)` when no
    /// provider class is configured so callers can fall through to their default credential path.
    pub fn for_url(
        url: &Url,
        configs: &HashMap<String, String>,
        mode: AccessMode,
        dispatch_key: &str,
        catalog_properties: &HashMap<String, String>,
    ) -> Result<Option<Self>, ExecutionError> {
        let bucket = match url.host_str() {
            Some(b) => b,
            None => return Ok(None),
        };
        let provider_class = match lookup_provider_class(configs, bucket) {
            Some(c) => c.to_string(),
            None => return Ok(None),
        };
        Self::new(
            provider_class,
            dispatch_key,
            bucket,
            url.path(),
            mode,
            catalog_properties,
        )
        .map(Some)
    }

    fn fetch_raw(&self) -> Result<RawCredentials, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<RawCredentials, ExecutionError> {
            let mode = self.mode as jint;

            let creds_obj: JObject = unsafe {
                jni_static_call!(env,
                    comet_s3_credential_dispatcher.get_credentials_for_path(
                        self.provider_class_jstr.as_obj(),
                        self.dispatch_key_jstr.as_obj(),
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
}

fn ensure_initialized(
    provider_class: &str,
    dispatch_key: &str,
    catalog_properties: &HashMap<String, String>,
) -> Result<(), ExecutionError> {
    JVMClasses::with_env(|env| -> Result<(), ExecutionError> {
        let provider_class_jstr = env.new_string(provider_class).map_err(|e| {
            ExecutionError::GeneralError(format!("new_string(provider_class): {e}"))
        })?;
        let dispatch_key_jstr = env
            .new_string(dispatch_key)
            .map_err(|e| ExecutionError::GeneralError(format!("new_string(dispatch_key): {e}")))?;
        let props_obj = build_java_string_map(env, catalog_properties)?;

        unsafe {
            jni_static_call!(env,
                comet_s3_credential_dispatcher.ensure_initialized(
                    &provider_class_jstr, &dispatch_key_jstr, &props_obj
                ) -> ()
            )?;
        }
        Ok(())
    })
}

/// Construct a `java.util.HashMap<String,String>` and populate it. Called once per bridge at
/// construction (per-scan), so the per-call HashMap/put cost is amortized away from the hot path.
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

struct RawCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    /// Absolute expiry. `0` means the provider did not report one.
    expiration_epoch_millis: i64,
}

#[async_trait]
impl CredentialProvider for CometS3CredentialBridge {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let raw = self.fetch_raw().map_err(|e| object_store::Error::Generic {
            store: "S3",
            source: e.to_string().into(),
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
            .fetch_raw()
            .map_err(|e| ReqsignError::new(ReqsignErrorKind::CredentialInvalid, e.to_string()))?;

        let expires_in = if raw.expiration_epoch_millis > 0 {
            Some(
                Timestamp::from_millisecond(raw.expiration_epoch_millis).map_err(|e| {
                    ReqsignError::new(
                        ReqsignErrorKind::CredentialInvalid,
                        format!(
                            "Invalid expirationEpochMillis {}: {e}",
                            raw.expiration_epoch_millis
                        ),
                    )
                })?,
            )
        } else {
            if WARNED_MISSING_EXPIRY.set(()).is_ok() {
                warn!(
                    "CometS3CredentialProvider returned credentials without expiration; \
                     defaulting to {}s expiry to bound opendal caching",
                    DEFAULT_EXPIRY_WHEN_UNKNOWN.as_secs()
                );
            }
            Some(Timestamp::now() + DEFAULT_EXPIRY_WHEN_UNKNOWN)
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
