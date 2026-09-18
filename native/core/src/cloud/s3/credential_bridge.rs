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

/// Cap on opendal's credential cache when the provider does not report an expiry. Prevents the
/// executor from holding a stale credential for the entire job lifetime.
const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

/// Once-per-process latch for the "missing expiry" warning. Bridges are per-scan, so a per-bridge
/// latch would re-log on every scan.
static WARNED_MISSING_EXPIRY: OnceCell<()> = OnceCell::new();

/// Access intent forwarded to the Java SPI. Ordinal must match the JVM `CometS3AccessMode` enum.
#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    Read = 0,
    Write = 1,
}

/// Per-scan credential provider that delegates to the JVM SPI via JNI. `handle` is the JVM-side
/// identity for the `(provider_class, dispatch_key, catalog_properties)` triple returned by
/// `ensureInitialized`. `bucket_jstr` / `path_jstr` are interned once at construction to avoid
/// per-call `new_string` allocations on the hot path.
///
/// Granularity: although the JVM SPI accepts `(bucket, path)`, neither
/// `object_store::CredentialProvider::get_credential` nor
/// `reqsign_core::ProvideCredential::provide_credential` carries a per-request path. The layer
/// above (Comet's scope-aware `object_store` cache in `parquet_support`) reads
/// [`Self::fetch_policy_locations`] once at construction so it can key a distinct
/// `CometS3CredentialBridge` per scope on the bucket, and wraps the resulting store in a
/// 403-retry safety net that re-fires the SPI with the failing path in context if S3 rejects a
/// read outside the advertised scope. Scoped vendors (`CometS3ScopedCredentialProvider`
/// implementors) narrow the effective identity from per-bucket to per-scope; base providers
/// retain the per-bucket identity that predates the sub-interface.
pub struct CometS3CredentialBridge {
    provider_class: String,
    dispatch_key: String,
    bucket: String,
    path: String,
    mode: AccessMode,
    handle: i64,
    bucket_jstr: Arc<Global<JString<'static>>>,
    path_jstr: Arc<Global<JString<'static>>>,
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

    /// Advisory policy-scope hint from the JVM SPI. Returns the vendor's advertised prefixes for
    /// this bridge's `(bucket, path, mode)` when the provider implements
    /// `CometS3ScopedCredentialProvider`; returns an empty vec otherwise (single-entry-per-bucket
    /// cache semantics). Comet's scope-aware `object_store` registry calls this once per bridge at
    /// construction time — S3 remains authoritative via the 403-retry wrapper layered on top.
    ///
    /// A null return from Java, or a base-only provider, both normalize to an empty vec here.
    pub fn fetch_policy_locations(&self) -> Result<Vec<String>, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<Vec<String>, ExecutionError> {
            let mode = self.mode as jint;
            let list_obj: JObject = unsafe {
                jni_static_call!(env,
                    comet_s3_credential_dispatcher.get_policy_locations_for(
                        self.handle,
                        self.bucket_jstr.as_obj(),
                        self.path_jstr.as_obj(),
                        mode
                    ) -> JObject
                )?
            };
            read_java_string_list(env, &list_obj)
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

/// Decode a `java.util.List<String>` into `Vec<String>`, treating null list, null elements, and
/// non-string elements as end-of-list / skip respectively. Only called from
/// [`CometS3CredentialBridge::fetch_policy_locations`] where the JVM dispatcher normalizes null
/// returns to `Collections.emptyList()`, but the extra null-check keeps the helper self-contained.
fn read_java_string_list(
    env: &mut jni::Env,
    list_obj: &JObject,
) -> Result<Vec<String>, ExecutionError> {
    if list_obj.is_null() {
        return Ok(Vec::new());
    }
    let list_class = env
        .find_class(JNIString::new("java/util/List"))
        .map_err(|e| ExecutionError::GeneralError(format!("find_class(List): {e}")))?;
    let size_method = env
        .get_method_id(&list_class, jni::jni_str!("size"), jni::jni_sig!("()I"))
        .map_err(|e| ExecutionError::GeneralError(format!("List.size: {e}")))?;
    let get_method = env
        .get_method_id(
            &list_class,
            jni::jni_str!("get"),
            jni::jni_sig!("(I)Ljava/lang/Object;"),
        )
        .map_err(|e| ExecutionError::GeneralError(format!("List.get: {e}")))?;

    let size: jint = unsafe {
        env.call_method_unchecked(
            list_obj,
            size_method,
            ReturnType::Primitive(Primitive::Int),
            &[],
        )
    }
    .and_then(|v| v.i())
    .map_err(|e| ExecutionError::GeneralError(format!("List.size call: {e}")))?;
    if size <= 0 {
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(size as usize);
    for i in 0..size {
        let elem = unsafe {
            env.call_method_unchecked(
                list_obj,
                get_method,
                ReturnType::Object,
                &[JValue::Int(i).as_jni()],
            )
        }
        .and_then(|v| v.l())
        .map_err(|e| ExecutionError::GeneralError(format!("List.get({i}) call: {e}")))?;
        if elem.is_null() {
            continue;
        }
        let jstr = unsafe { JString::from_raw(env, elem.into_raw()) };
        let s = jstr
            .try_to_string(env)
            .map_err(|e| ExecutionError::GeneralError(format!("try_to_string(List[{i}]): {e}")))?;
        out.push(s);
    }
    Ok(out)
}
