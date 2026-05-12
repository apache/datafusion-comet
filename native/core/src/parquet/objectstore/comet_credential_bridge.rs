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
//! Java side and the architecture diagram.

use crate::execution::operators::ExecutionError;
use crate::jvm_bridge::{check_exception, JVMClasses};
use async_trait::async_trait;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use jni::objects::{JClass, JFieldID, JStaticMethodID, JString};
use jni::signature::{Primitive, ReturnType};
use jni::strings::JNIString;
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

/// Default expiration to attach when the Java provider returns
/// `CometCredentials.expirationEpochMillis == 0` ("unknown"). Without a non-None expiry, opendal
/// would cache the credential for the entire executor lifetime - a silent footgun for Spark
/// jobs that run for hours. Five minutes is short enough to limit blast radius and long enough
/// to avoid per-request JNI overhead. Vendors that want a different cadence should set
/// `expirationEpochMillis` on every returned POJO.
const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

const DISPATCHER_CLASS: &str = "org/apache/comet/cloud/CometCloudCredentialDispatcher";
const CREDENTIALS_CLASS: &str = "org/apache/comet/cloud/CometCredentials";

/// Process-lifetime cache of the JNI handles needed to call into the dispatcher.
///
/// ## Why static / process lifetime?
///
/// `JStaticMethodID` and `JFieldID` are tied to their owning `JClass`; the class itself must
/// remain reachable for the IDs to stay valid. Resolving the class and IDs requires a JNI
/// round-trip, so caching them once per executor avoids per-request overhead.
///
/// ## Bounded
///
/// One entry, populated exactly once. `OnceCell` enforces single initialization.
///
/// ## Credential refresh
///
/// This cache holds *no* credentials, only the JNI handles needed to invoke the Java provider.
/// Every `get_credential` call dispatches through JNI; the Java provider owns all token / STS
/// refresh logic. There is no Rust-side credential staleness window.
static BRIDGE_HANDLE: OnceCell<BridgeHandleState> = OnceCell::new();

enum BridgeHandleState {
    /// Java dispatcher reported a registered provider; cached handles are usable.
    Registered(BridgeHandle),
    /// Java dispatcher reported no provider, or initialization could not reach the JVM.
    /// Native callers should fall back to the default AWS credential chain.
    NotRegistered,
}

struct BridgeHandle {
    /// Used by `get_credential` to invoke the static dispatcher method.
    dispatcher_class: JClass<'static>,
    /// Kept alive so that the cached field IDs remain valid.
    _credentials_class: JClass<'static>,
    method_get_credentials: JStaticMethodID,
    field_access_key_id: JFieldID,
    field_secret_access_key: JFieldID,
    field_session_token: JFieldID,
    field_expiration_epoch_millis: JFieldID,
}

// SAFETY: The cached `JClass`, `JStaticMethodID`, and `JFieldID` are all global identifiers
// that may be used from any thread once acquired. JVMClasses applies the same reasoning to its
// own cached classes/methods.
unsafe impl Send for BridgeHandle {}
unsafe impl Sync for BridgeHandle {}

/// Attempt to initialize the JNI handles. Returns `Some(handle)` if the dispatcher reports a
/// registered provider, `None` otherwise. Cached after the first call.
fn get_handle() -> Option<&'static BridgeHandle> {
    let state = BRIDGE_HANDLE.get_or_init(init_handle);
    match state {
        BridgeHandleState::Registered(h) => Some(h),
        BridgeHandleState::NotRegistered => None,
    }
}

fn init_handle() -> BridgeHandleState {
    let result: Result<BridgeHandleState, ExecutionError> = JVMClasses::with_env(|env| {
        // SAFETY: Match the transmute trick in `JVMClasses::init` so that classes acquired here
        // can be cached for process lifetime.
        let env_static =
            unsafe { std::mem::transmute::<&mut jni::Env, &'static mut jni::Env>(env) };

        let dispatcher_class = env_static
            .find_class(JNIString::new(DISPATCHER_CLASS))
            .map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to find {DISPATCHER_CLASS}: {e}"))
            })?;

        let is_registered_method = env_static
            .get_static_method_id(
                JNIString::new(DISPATCHER_CLASS),
                jni::jni_str!("isProviderRegistered"),
                jni::jni_sig!("()Z"),
            )
            .map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to resolve isProviderRegistered: {e}"))
            })?;

        let registered = unsafe {
            env_static.call_static_method_unchecked(
                &dispatcher_class,
                is_registered_method,
                ReturnType::Primitive(Primitive::Boolean),
                &[],
            )
        }
        .map_err(|e| {
            ExecutionError::GeneralError(format!("isProviderRegistered call failed: {e}"))
        })?
        .z()
        .map_err(|e| {
            ExecutionError::GeneralError(format!(
                "isProviderRegistered did not return boolean: {e}"
            ))
        })?;

        if !registered {
            debug!("CometCloudCredentialDispatcher reports no registered provider");
            return Ok(BridgeHandleState::NotRegistered);
        }

        let method_get_credentials = env_static
            .get_static_method_id(
                JNIString::new(DISPATCHER_CLASS),
                jni::jni_str!("getCredentialsForPath"),
                jni::jni_sig!(
                    "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/comet/cloud/CometCredentials;"
                ),
            )
            .map_err(|e| {
                ExecutionError::GeneralError(format!(
                    "Failed to resolve getCredentialsForPath: {e}"
                ))
            })?;

        let credentials_class = env_static
            .find_class(JNIString::new(CREDENTIALS_CLASS))
            .map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to find {CREDENTIALS_CLASS}: {e}"))
            })?;

        let field_access_key_id = env_static
            .get_field_id(
                &credentials_class,
                jni::jni_str!("accessKeyId"),
                jni::jni_sig!("Ljava/lang/String;"),
            )
            .map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to resolve accessKeyId field: {e}"))
            })?;
        let field_secret_access_key = env_static
            .get_field_id(
                &credentials_class,
                jni::jni_str!("secretAccessKey"),
                jni::jni_sig!("Ljava/lang/String;"),
            )
            .map_err(|e| {
                ExecutionError::GeneralError(format!(
                    "Failed to resolve secretAccessKey field: {e}"
                ))
            })?;
        let field_session_token = env_static
            .get_field_id(
                &credentials_class,
                jni::jni_str!("sessionToken"),
                jni::jni_sig!("Ljava/lang/String;"),
            )
            .map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to resolve sessionToken field: {e}"))
            })?;
        let field_expiration_epoch_millis = env_static
            .get_field_id(
                &credentials_class,
                jni::jni_str!("expirationEpochMillis"),
                jni::jni_sig!("J"),
            )
            .map_err(|e| {
                ExecutionError::GeneralError(format!(
                    "Failed to resolve expirationEpochMillis field: {e}"
                ))
            })?;

        Ok(BridgeHandleState::Registered(BridgeHandle {
            dispatcher_class,
            _credentials_class: credentials_class,
            method_get_credentials,
            field_access_key_id,
            field_secret_access_key,
            field_session_token,
            field_expiration_epoch_millis,
        }))
    });

    match result {
        Ok(state) => {
            if matches!(state, BridgeHandleState::Registered(_)) {
                debug!("CometCredentialBridge initialized; will route credentials through JNI");
            }
            state
        }
        Err(e) => {
            // Initialization is best-effort. If the JVM isn't reachable or the dispatcher
            // class isn't on the classpath, fall back silently to the default chain.
            debug!("CometCredentialBridge unavailable, falling back to default chain: {e}");
            BridgeHandleState::NotRegistered
        }
    }
}

/// Returns true if a `CometCloudCredentialProvider` is registered on the JVM classpath.
/// Used by `s3.rs::create_store` to decide whether to construct a [`CometCredentialBridge`].
pub fn is_provider_registered() -> bool {
    get_handle().is_some()
}

/// Per-request credential provider that delegates to the Java SPI via JNI.
///
/// One instance is constructed per S3 store for the `object_store` path (per-URL in
/// `s3.rs::create_store`) or per FileIO for the iceberg-rust path. The Java provider receives
/// `(bucket, path)` on every credential fetch and is free to return different credentials per
/// path; on the iceberg-rust path the path is the table's metadata location, so credentials are
/// effectively per-table.
#[derive(Debug)]
pub struct CometCredentialBridge {
    bucket: String,
    path: String,
}

impl CometCredentialBridge {
    pub fn new(bucket: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            path: path.into(),
        }
    }

    /// Invoke the Java dispatcher and extract the three string fields off the returned POJO.
    /// Shared between the `object_store::CredentialProvider` and `reqsign_core::ProvideCredential`
    /// impls.
    fn fetch_raw(&self) -> Result<RawCredentials, ExecutionError> {
        let handle = get_handle().ok_or_else(|| {
            ExecutionError::GeneralError(
                "CometCredentialBridge invoked but no Java provider is registered".to_string(),
            )
        })?;

        JVMClasses::with_env(|env| -> Result<RawCredentials, ExecutionError> {
            let bucket_jstr = env.new_string(&self.bucket).map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to create bucket JString: {e}"))
            })?;
            let path_jstr = env.new_string(&self.path).map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to create path JString: {e}"))
            })?;

            let result = unsafe {
                env.call_static_method_unchecked(
                    &handle.dispatcher_class,
                    handle.method_get_credentials,
                    ReturnType::Object,
                    &[
                        jni::objects::JValue::from(&bucket_jstr).as_jni(),
                        jni::objects::JValue::from(&path_jstr).as_jni(),
                    ],
                )
            };

            if let Some(exception) = check_exception(env).map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to check Java exception: {e}"))
            })? {
                return Err(ExecutionError::GeneralError(format!(
                    "Java exception in CometCloudCredentialDispatcher.getCredentialsForPath: \
                     {exception}"
                )));
            }

            let creds_obj = result
                .map_err(|e| {
                    ExecutionError::GeneralError(format!(
                        "getCredentialsForPath JNI call failed: {e}"
                    ))
                })?
                .l()
                .map_err(|e| {
                    ExecutionError::GeneralError(format!(
                        "getCredentialsForPath did not return an object: {e}"
                    ))
                })?;

            if creds_obj.is_null() {
                return Err(ExecutionError::GeneralError(
                    "getCredentialsForPath returned null".to_string(),
                ));
            }

            let access_key_id =
                read_string_field(env, &creds_obj, handle.field_access_key_id, "accessKeyId")?
                    .ok_or_else(|| {
                        ExecutionError::GeneralError("accessKeyId was null".to_string())
                    })?;
            let secret_access_key = read_string_field(
                env,
                &creds_obj,
                handle.field_secret_access_key,
                "secretAccessKey",
            )?
            .ok_or_else(|| ExecutionError::GeneralError("secretAccessKey was null".to_string()))?;
            let session_token =
                read_string_field(env, &creds_obj, handle.field_session_token, "sessionToken")?;
            let expiration_epoch_millis = unsafe {
                env.get_field_unchecked(
                    &creds_obj,
                    handle.field_expiration_epoch_millis,
                    ReturnType::Primitive(Primitive::Long),
                )
            }
            .map_err(|e| {
                ExecutionError::GeneralError(format!("Failed to read expirationEpochMillis: {e}"))
            })?
            .j()
            .map_err(|e| {
                ExecutionError::GeneralError(format!("expirationEpochMillis was not a long: {e}"))
            })?;

            Ok(RawCredentials {
                access_key_id,
                secret_access_key,
                session_token,
                expiration_epoch_millis,
            })
        })
    }
}

struct RawCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    /// Provider-supplied absolute expiry. `0` means "unknown"; callers translate that into a
    /// short fallback so opendal cannot cache the credential for the entire executor lifetime.
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
            // Provider did not set an expiration. Opendal would otherwise cache this credential
            // for the entire executor lifetime; force a refresh after DEFAULT_EXPIRY_WHEN_UNKNOWN.
            warn!(
                "CometCloudCredentialProvider returned credentials with no expiration; \
                 defaulting to {}s expiry to bound opendal caching",
                DEFAULT_EXPIRY_WHEN_UNKNOWN.as_secs()
            );
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

/// Read a Java `String` instance field via cached `JFieldID`. Returns `Ok(None)` if the field
/// value is null (allowed for nullable fields like `sessionToken`).
fn read_string_field(
    env: &mut jni::Env,
    instance: &jni::objects::JObject,
    field_id: JFieldID,
    field_name: &str,
) -> Result<Option<String>, ExecutionError> {
    let value = unsafe { env.get_field_unchecked(instance, field_id, ReturnType::Object) }
        .map_err(|e| {
            ExecutionError::GeneralError(format!("Failed to read field {field_name}: {e}"))
        })?
        .l()
        .map_err(|e| {
            ExecutionError::GeneralError(format!("Field {field_name} was not an Object: {e}"))
        })?;

    if value.is_null() {
        return Ok(None);
    }
    let jstr = unsafe { JString::from_raw(env, value.into_raw()) };
    let s = jstr.try_to_string(env).map_err(|e| {
        ExecutionError::GeneralError(format!("Failed to read field {field_name} as String: {e}"))
    })?;
    Ok(Some(s))
}
