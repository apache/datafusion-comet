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
use crate::jvm_bridge::{jni_static_call, JVMClasses};
use async_trait::async_trait;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use jni::objects::{JFieldID, JObject, JString};
use jni::signature::{Primitive, ReturnType};
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
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Hadoop-style config key naming the vendor `CometS3CredentialProvider` FQCN. Per-bucket form is
/// `fs.s3a.bucket.<name>.comet.credential.provider.class`.
pub const PROVIDER_CLASS_PROPERTY: &str = "comet.credential.provider.class";

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

/// Resolve the configured provider class for the given bucket, applying the per-bucket override
/// before falling back to the global key. Returns the trimmed FQCN if non-empty.
pub fn lookup_provider_class<'a>(
    configs: &'a HashMap<String, String>,
    bucket: &str,
) -> Option<&'a str> {
    let per_bucket = format!("fs.s3a.bucket.{bucket}.{PROVIDER_CLASS_PROPERTY}");
    let value = configs.get(&per_bucket).or_else(|| {
        let global = format!("fs.s3a.{PROVIDER_CLASS_PROPERTY}");
        configs.get(&global)
    })?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Per-request credential provider that delegates to the Java SPI via JNI. Constructed once per
/// S3 store or FileIO and forwards the same `(class, bucket, path, mode)` tuple on every fetch.
#[derive(Debug)]
pub struct CometS3CredentialBridge {
    provider_class: String,
    bucket: String,
    path: String,
    mode: AccessMode,
}

impl CometS3CredentialBridge {
    pub fn new(
        provider_class: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
        mode: AccessMode,
    ) -> Self {
        Self {
            provider_class: provider_class.into(),
            bucket: bucket.into(),
            path: path.into(),
            mode,
        }
    }

    /// Shared constructor for the s3.rs and iceberg_scan.rs call sites. Returns `None` when no
    /// provider class is configured so callers can fall through to their default credential path.
    pub fn for_url(url: &Url, configs: &HashMap<String, String>, mode: AccessMode) -> Option<Self> {
        let bucket = url.host_str()?;
        let provider_class = lookup_provider_class(configs, bucket)?;
        Some(Self::new(provider_class, bucket, url.path(), mode))
    }

    fn fetch_raw(&self) -> Result<RawCredentials, ExecutionError> {
        JVMClasses::with_env(|env| -> Result<RawCredentials, ExecutionError> {
            let provider_class = env.new_string(&self.provider_class).map_err(|e| {
                ExecutionError::GeneralError(format!("new_string(provider_class): {e}"))
            })?;
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
                        &provider_class, &bucket, &path, mode
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
