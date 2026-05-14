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
//! ```text
//!   JVM                                        Native (Rust)
//!   ---                                        -------------
//!
//!   ServiceLoader                              s3.rs (object_store)
//!   (one-time, at class-load)                  iceberg_scan.rs (opendal)
//!         |                                              |
//!         v                                              v
//!   CometS3CredentialDispatcher                 CometS3CredentialBridge
//!   (static singleton)                            impl object_store::CredentialProvider
//!         |      ^                                impl reqsign_core::ProvideCredential
//!         |      |                                       |
//!         |      +<---- JNI call -----------------+
//!         |         getCredentialsForPath(bucket, path, mode ordinal)
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
use crate::jvm_bridge::{jni_static_call, JVMClasses};
use crate::JAVA_VM;
use async_trait::async_trait;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use jni::objects::{JFieldID, JObject, JString};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jboolean, jint};
use log::{debug, warn};
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;
use once_cell::sync::OnceCell;
use reqsign_core::time::Timestamp;
use reqsign_core::{
    Context, Error as ReqsignError, ErrorKind as ReqsignErrorKind,
    ProvideCredential as IcebergProvideCredential,
};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Cap on opendal's credential cache when the provider does not report an expiry. Prevents the
/// executor from holding a stale credential for the entire job lifetime.
const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

static PROVIDER_REGISTERED: OnceCell<bool> = OnceCell::new();
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

/// True iff a `CometS3CredentialProvider` was discovered on the JVM classpath.
pub fn is_provider_registered() -> bool {
    *PROVIDER_REGISTERED.get_or_init(|| {
        // Unit tests construct stores without a JVM. Production init always precedes any store
        // construction, so the None branch only fires in tests.
        if JAVA_VM.get().is_none() {
            return false;
        }
        JVMClasses::with_env(|env| -> Result<bool, ExecutionError> {
            let registered: jboolean = unsafe {
                jni_static_call!(env,
                    comet_s3_credential_dispatcher.is_provider_registered() -> jboolean
                )?
            };
            Ok(registered)
        })
        .unwrap_or_else(|e| {
            debug!(
                "CometS3CredentialDispatcher.isProviderRegistered failed; native S3 readers \
                 will use the default AWS credential chain: {e}"
            );
            false
        })
    })
}

/// Per-request credential provider that delegates to the Java SPI via JNI. Constructed once per
/// S3 store or FileIO and forwards the same `(bucket, path, mode)` tuple on every fetch.
#[derive(Debug)]
pub struct CometS3CredentialBridge {
    bucket: String,
    path: String,
    mode: AccessMode,
}

impl CometS3CredentialBridge {
    pub fn new(bucket: impl Into<String>, path: impl Into<String>, mode: AccessMode) -> Self {
        Self {
            bucket: bucket.into(),
            path: path.into(),
            mode,
        }
    }

    /// Shared constructor for the s3.rs and iceberg_scan.rs call sites. Returns `None` when no
    /// provider is registered so callers can fall through to their default credential path.
    pub fn for_url(url: &Url, mode: AccessMode) -> Option<Self> {
        if !is_provider_registered() {
            return None;
        }
        let bucket = url.host_str()?.to_string();
        debug!("Routing S3 credentials for bucket {bucket} through CometS3CredentialProvider");
        Some(Self::new(bucket, url.path(), mode))
    }

    fn fetch_raw(&self) -> Result<RawCredentials, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<RawCredentials, ExecutionError> {
            let bucket = env
                .new_string(&self.bucket)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(bucket): {e}")))?;
            let path = env
                .new_string(&self.path)
                .map_err(|e| ExecutionError::GeneralError(format!("new_string(path): {e}")))?;
            let mode = self.mode as jint;

            let creds_obj: JObject = unsafe {
                jni_static_call!(env,
                    comet_s3_credential_dispatcher.get_credentials_for_path(
                        &bucket, &path, mode
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
