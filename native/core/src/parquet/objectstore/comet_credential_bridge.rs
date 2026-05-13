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

//! JNI bridge to the Java `CometCloudCredentialDispatcher` SPI for per-request AWS credentials.
//!
//! See `common/src/main/java/org/apache/comet/cloud/CometCloudCredentialDispatcher.java` for the
//! Java side and the architecture diagram. JNI handles are cached on `JVMClasses` next to all
//! the other Comet JNI bridges; this file holds only the Rust trait impls that delegate through.

use crate::execution::operators::ExecutionError;
use crate::jvm_bridge::{check_exception, JVMClasses};
use crate::JAVA_VM;
use async_trait::async_trait;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use jni::objects::{JFieldID, JObject, JString, JValue};
use jni::signature::{Primitive, ReturnType};
use log::{debug, warn};
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;
use once_cell::sync::OnceCell;
use reqsign_core::time::Timestamp;
use reqsign_core::{
    Context, Error as ReqsignError, ErrorKind as ReqsignErrorKind,
    ProvideCredential as IcebergProvideCredential,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Bound on opendal's credential cache when the Java provider returns `expirationEpochMillis = 0`
/// ("unknown"). Without this, opendal would hold the credential for the entire executor lifetime,
/// a silent footgun for Spark jobs that run for hours. Five minutes trades a small JNI-call
/// cadence for a tight staleness bound. Vendors that know the real expiry should set it.
const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

/// Cached "is a Java provider registered?" answer. Resolution is one JNI round-trip and the
/// result never changes within a JVM lifetime, so memoize.
static PROVIDER_REGISTERED: OnceCell<bool> = OnceCell::new();

/// True iff a `CometCloudCredentialProvider` was discovered on the JVM classpath. Used by
/// `s3.rs::create_store` and `iceberg_scan.rs` to decide whether to wire a [`CometCredentialBridge`]
/// in front of the default credential paths.
pub fn is_provider_registered() -> bool {
    *PROVIDER_REGISTERED.get_or_init(|| {
        // Unit tests construct stores without a JVM; treat that as "no provider registered" so we
        // don't trip `with_env`'s debug_assert. In production the JVM is always initialized before
        // any object_store is built.
        if JAVA_VM.get().is_none() {
            return false;
        }
        JVMClasses::with_env(|env| -> Result<bool, ExecutionError> {
            let dispatcher = &JVMClasses::get().comet_cloud_credential_dispatcher;
            let result = unsafe {
                env.call_static_method_unchecked(
                    &dispatcher.class,
                    dispatcher.method_is_provider_registered,
                    dispatcher.method_is_provider_registered_ret,
                    &[],
                )
            }
            .map_err(|e| {
                ExecutionError::GeneralError(format!("isProviderRegistered call failed: {e}"))
            })?;
            if let Some(exception) = check_exception(env)
                .map_err(|e| ExecutionError::GeneralError(format!("Exception check failed: {e}")))?
            {
                return Err(ExecutionError::GeneralError(format!(
                    "Java exception in isProviderRegistered: {exception}"
                )));
            }
            result.z().map_err(|e| {
                ExecutionError::GeneralError(format!("isProviderRegistered did not return Z: {e}"))
            })
        })
        .unwrap_or_else(|e| {
            debug!(
                "CometCloudCredentialDispatcher.isProviderRegistered failed; \
                 native S3 readers will use the default AWS credential chain: {e}"
            );
            false
        })
    })
}

/// Per-request credential provider that delegates to the Java SPI via JNI.
///
/// One instance is constructed per S3 store (per-URL in `create_store`) or per FileIO (the
/// metadata location, in `iceberg_scan.rs`). The `(bucket, path)` tuple is forwarded verbatim
/// on every credential fetch; the Java provider is free to return different credentials for
/// different paths.
#[derive(Debug)]
pub struct CometCredentialBridge {
    bucket: String,
    path: String,
    /// Latched once the bridge observes a credential without an expiry, so the warning that
    /// goes with [`DEFAULT_EXPIRY_WHEN_UNKNOWN`] only fires once per bridge instance instead of
    /// per request.
    warned_missing_expiry: AtomicBool,
}

impl CometCredentialBridge {
    pub fn new(bucket: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            path: path.into(),
            warned_missing_expiry: AtomicBool::new(false),
        }
    }

    /// Single JNI round-trip to the dispatcher; both async trait impls share this.
    fn fetch_raw(&self) -> Result<RawCredentials, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<RawCredentials, ExecutionError> {
            let dispatcher = &JVMClasses::get().comet_cloud_credential_dispatcher;

            let bucket_jstr = env
                .new_string(&self.bucket)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(bucket): {e}")))?;
            let path_jstr = env
                .new_string(&self.path)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(path): {e}")))?;

            let result = unsafe {
                env.call_static_method_unchecked(
                    &dispatcher.class,
                    dispatcher.method_get_credentials_for_path,
                    dispatcher.method_get_credentials_for_path_ret,
                    &[
                        JValue::from(&bucket_jstr).as_jni(),
                        JValue::from(&path_jstr).as_jni(),
                    ],
                )
            };

            if let Some(exception) = check_exception(env)
                .map_err(|e| ExecutionError::GeneralError(format!("Exception check failed: {e}")))?
            {
                return Err(ExecutionError::GeneralError(format!(
                    "Java exception in getCredentialsForPath: {exception}"
                )));
            }

            let creds_obj = result
                .map_err(|e| {
                    ExecutionError::GeneralError(format!("getCredentialsForPath JNI call: {e}"))
                })?
                .l()
                .map_err(|e| {
                    ExecutionError::GeneralError(format!(
                        "getCredentialsForPath did not return an object: {e}"
                    ))
                })?;

            if creds_obj.is_null() {
                return Err(ExecutionError::GeneralError(
                    "getCredentialsForPath returned null (contract violation)".to_string(),
                ));
            }

            Ok(RawCredentials {
                access_key_id: read_required_string(
                    env,
                    &creds_obj,
                    dispatcher.field_access_key_id,
                    "accessKeyId",
                )?,
                secret_access_key: read_required_string(
                    env,
                    &creds_obj,
                    dispatcher.field_secret_access_key,
                    "secretAccessKey",
                )?,
                session_token: read_optional_string(
                    env,
                    &creds_obj,
                    dispatcher.field_session_token,
                )?,
                expiration_epoch_millis: unsafe {
                    env.get_field_unchecked(
                        &creds_obj,
                        dispatcher.field_expiration_epoch_millis,
                        ReturnType::Primitive(Primitive::Long),
                    )
                }
                .map_err(|e| {
                    ExecutionError::GeneralError(format!("read expirationEpochMillis: {e}"))
                })?
                .j()
                .map_err(|e| {
                    ExecutionError::GeneralError(format!("expirationEpochMillis not a long: {e}"))
                })?,
            })
        })
    }
}

struct RawCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    /// Provider-supplied absolute expiry. `0` means the provider didn't say; callers translate
    /// that into a short fallback so opendal can't cache a stale credential indefinitely.
    expiration_epoch_millis: i64,
}

#[async_trait]
impl CredentialProvider for CometCredentialBridge {
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

impl IcebergProvideCredential for CometCredentialBridge {
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
            if !self.warned_missing_expiry.swap(true, Ordering::Relaxed) {
                warn!(
                    "CometCloudCredentialProvider returned credentials without expiration for \
                     bucket={} path={}; defaulting to {}s expiry to bound opendal caching",
                    self.bucket,
                    self.path,
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
        .map_err(|e| ExecutionError::GeneralError(format!("get_field_unchecked: {e}")))?
        .l()
        .map_err(|e| ExecutionError::GeneralError(format!("field was not an Object: {e}")))?;
    if value.is_null() {
        return Ok(None);
    }
    let jstr = unsafe { JString::from_raw(env, value.into_raw()) };
    jstr.try_to_string(env)
        .map(Some)
        .map_err(|e| ExecutionError::GeneralError(format!("try_to_string: {e}")))
}
