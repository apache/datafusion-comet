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

//! IRSA (EKS "IAM Roles for Service Accounts") web-identity credential provider for the native S3
//! paths.
//!
//! Why this exists: on EKS with IRSA the native reader assumes the app role by calling STS
//! `AssumeRoleWithWebIdentity`. Under a concurrent burst (many executors x many cores starting
//! together) STS throttles that call. opendal's default reqsign chain (used by the Iceberg path
//! when no Comet provider class is set) does NOT retry the throttle and silently downgrades to the
//! EC2/EKS node instance role, which lacks bucket access -> every read then fails with a hard S3
//! 403. See docs/source/contributor-guide/s3-credential-provider-design.md.
//!
//! This provider fixes all three parts of that failure:
//!   1. Retry on throttle. It builds an STS client from the AWS SDK's fully-resolved `SdkConfig`
//!      (`aws_config::defaults(...).load()`) with a raised `RetryConfig`, and calls
//!      `AssumeRoleWithWebIdentity` on it. Because the client comes from the resolved config, it
//!      honors region, FIPS, dual-stack and any profile/custom STS endpoint the SDK would --
//!      there is no hand-assembled config to drift. `max_attempts` is configurable.
//!   2. No silent downgrade. It only ever calls `AssumeRoleWithWebIdentity` -- there is no
//!      credential chain and no IMDS/instance-role fallback -- so a throttle that outlasts the
//!      retries surfaces as an error instead of a wrong-identity credential.
//!   3. Shared, jittered cache. One assumed-role credential is cached per process, keyed by
//!      identity (role_arn, token_file, region) and the resolved retry/refresh settings, and shared
//!      across all reader threads and scans that resolve to the same key. Refresh fires ahead of
//!      expiry by `min_ttl` plus a per-process random jitter so cluster-wide refreshes do not
//!      synchronize into another burst; a failed refresh is briefly remembered so a throttled burst
//!      costs one STS call rather than one per reader.
//!
//! The same struct is exposed as both `object_store::CredentialProvider` (raw Parquet path) and
//! reqsign's `ProvideCredential` (Iceberg via opendal / `CustomAwsCredentialLoader`), mirroring
//! `credential_bridge::CometS3CredentialBridge`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::future as creds_future;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::Credentials;
use iceberg_storage_opendal::AwsCredential as IcebergAwsCredential;
use object_store::aws::AwsCredential;
use object_store::CredentialProvider;
use rand::RngExt;
use reqsign_core::time::Timestamp;
use reqsign_core::{
    Context, Error as ReqsignError, ErrorKind as ReqsignErrorKind,
    ProvideCredential as IcebergProvideCredential,
};

use crate::cloud::s3::credential_bridge::DEFAULT_EXPIRY_WHEN_UNKNOWN;

/// EKS-projected env vars that signal IRSA is in effect. Both must be present.
const ENV_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const ENV_ROLE_ARN: &str = "AWS_ROLE_ARN";

/// Config keys read from the Iceberg catalog property bag. A non-`s3.`/`client.` prefix keeps them
/// from being forwarded into opendal's S3 config (see `iceberg_common::STORAGE_PROPERTY_PREFIXES`).
const KEY_ENABLED: &str = "comet.s3.credentials.webIdentity.enabled";
const KEY_MAX_ATTEMPTS: &str = "comet.s3.credentials.webIdentity.maxAttempts";
const KEY_MIN_TTL_SECS: &str = "comet.s3.credentials.webIdentity.minTtlSeconds";
const KEY_JITTER_SECS: &str = "comet.s3.credentials.webIdentity.refreshJitterSeconds";

const DEFAULT_ENABLED: bool = true;
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_MIN_TTL_SECS: u64 = 300;
const DEFAULT_JITTER_SECS: u64 = 60;

/// After a refresh exhausts its STS retries and fails, waiters within this window get the failure
/// without each firing their own assume-role call. Bounds STS pressure during a sustained throttle
/// (one call per entry per window instead of one per reader) while still letting the credential
/// recover shortly after. Kept short: the SDK has already spent its retry budget by the time we
/// record a failure.
const FAILURE_COOLDOWN: Duration = Duration::from_secs(1);

/// Detected IRSA identity plus the resolved tuning knobs. Cheap to clone; the expensive AWS SDK
/// provider lives in the process-wide `SharedEntry` keyed by `entry_key`.
#[derive(Clone, Debug)]
pub struct WebIdentityConfig {
    role_arn: String,
    token_file: String,
    /// From `AWS_REGION` / `AWS_DEFAULT_REGION`; only part of the cache key. The STS client's
    /// actual region (and endpoint) comes from the resolved `SdkConfig`.
    region: Option<String>,
    max_attempts: u32,
    min_ttl: Duration,
    max_jitter: Duration,
}

impl WebIdentityConfig {
    /// Returns a config only when IRSA is in effect (both env vars present) and the feature is
    /// enabled. `resolve` looks up a bare setting key (e.g. `KEY_MAX_ATTEMPTS`) in whichever config
    /// bag the caller owns -- the Iceberg catalog bag or the Parquet `fs.s3a.*` bag -- so the two
    /// scan paths share one detection routine without sharing a config-key scheme. Returns `None`
    /// when IRSA is not detected or the feature is disabled.
    pub fn detect_with<F>(resolve: F) -> Option<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let token_file = non_empty_env(ENV_TOKEN_FILE)?;
        let role_arn = non_empty_env(ENV_ROLE_ARN)?;
        if !parse_setting(resolve(KEY_ENABLED), DEFAULT_ENABLED) {
            return None;
        }
        Some(Self {
            role_arn,
            token_file,
            region: non_empty_env("AWS_REGION").or_else(|| non_empty_env("AWS_DEFAULT_REGION")),
            max_attempts: parse_u32(resolve(KEY_MAX_ATTEMPTS), DEFAULT_MAX_ATTEMPTS),
            min_ttl: Duration::from_secs(parse_setting(
                resolve(KEY_MIN_TTL_SECS),
                DEFAULT_MIN_TTL_SECS,
            )),
            max_jitter: Duration::from_secs(parse_setting(
                resolve(KEY_JITTER_SECS),
                DEFAULT_JITTER_SECS,
            )),
        })
    }

    fn entry_key(&self) -> EntryKey {
        EntryKey {
            role_arn: self.role_arn.clone(),
            token_file: self.token_file.clone(),
            region: self.region.clone(),
            max_attempts: self.max_attempts,
            min_ttl: self.min_ttl,
            max_jitter: self.max_jitter,
        }
    }
}

/// Process-wide cache key. A credential is shared per distinct identity AND resolved settings, so a
/// catalog that configures its own retry/refresh knobs gets its own entry with its own
/// configuration honored -- independent of which scan initializes first. Two callers with the same
/// identity and the same settings still share one entry (and one STS call).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EntryKey {
    role_arn: String,
    token_file: String,
    region: Option<String>,
    max_attempts: u32,
    min_ttl: Duration,
    max_jitter: Duration,
}

/// The shared, cached credential for one identity. `provider` is the AWS SDK web-identity provider
/// built once; `cached` holds the last credential; `refresh_jitter` is drawn once per process so
/// each executor refreshes at a slightly different time. `last_failure_at` coalesces a burst of
/// readers that hit a persistent throttle into a single STS call.
#[derive(Debug)]
struct SharedEntry {
    provider: Arc<dyn ProvideCredentials>,
    cached: RwLock<Option<Credentials>>,
    /// Single-flights refreshes so a burst of readers triggers exactly one STS call.
    refresh_lock: tokio::sync::Mutex<()>,
    /// When the last refresh failed. Waiters within `FAILURE_COOLDOWN` of this get the failure
    /// without re-calling STS, so a failed burst costs one call rather than one per reader.
    last_failure_at: RwLock<Option<Instant>>,
    min_ttl: Duration,
    refresh_jitter: Duration,
}

impl SharedEntry {
    /// Returns the cached credential if it is still fresh, i.e. it does not expire within
    /// `min_ttl + refresh_jitter`.
    fn fresh(&self) -> Option<Credentials> {
        let guard = self.cached.read().unwrap();
        let cred = guard.as_ref()?;
        match cred.expiry() {
            Some(expiry) => {
                if expiry <= SystemTime::now() + self.min_ttl + self.refresh_jitter {
                    None
                } else {
                    Some(cred.clone())
                }
            }
            // No expiry reported: keep it. Web-identity credentials normally carry one.
            None => Some(cred.clone()),
        }
    }

    /// `Some(error)` if a refresh failed within the last `FAILURE_COOLDOWN`, so callers can bail out
    /// instead of piling another assume-role call onto a throttled STS.
    fn in_failure_cooldown(&self) -> Option<String> {
        let at = (*self.last_failure_at.read().unwrap())?;
        (at.elapsed() < FAILURE_COOLDOWN).then(|| {
            "web-identity credential refresh failed recently; backing off before retrying STS"
                .to_string()
        })
    }

    /// Fetches a fresh credential, refreshing from STS at most once at a time. On a refresh error
    /// the error propagates -- we never fall back to a lower-privilege identity -- and is briefly
    /// remembered so concurrent waiters do not each re-issue the same throttled call.
    async fn credentials(&self) -> Result<Credentials, String> {
        if let Some(cred) = self.fresh() {
            return Ok(cred);
        }
        if let Some(err) = self.in_failure_cooldown() {
            return Err(err);
        }
        let _guard = self.refresh_lock.lock().await;
        // Re-check: another task may have refreshed (or just failed) while we waited on the lock.
        if let Some(cred) = self.fresh() {
            return Ok(cred);
        }
        if let Some(err) = self.in_failure_cooldown() {
            return Err(err);
        }
        match self.provider.provide_credentials().await {
            Ok(cred) => {
                *self.cached.write().unwrap() = Some(cred.clone());
                *self.last_failure_at.write().unwrap() = None;
                Ok(cred)
            }
            Err(e) => {
                *self.last_failure_at.write().unwrap() = Some(Instant::now());
                Err(format!("web-identity assume-role failed: {e}"))
            }
        }
    }
}

/// Registry of shared credential entries, one per identity, for the lifetime of the process.
///
/// Process lifetime is the right scope for the same reason as the region cache in `s3.rs`: each
/// executor is dedicated to one Spark application, and there is a bounded set of assumed roles per
/// job. Entries are never evicted; the map stays proportional to the number of distinct roles.
fn registry() -> &'static std::sync::Mutex<HashMap<EntryKey, Arc<SharedEntry>>> {
    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<EntryKey, Arc<SharedEntry>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Returns the shared entry for `cfg`, building the AWS SDK provider once if needed. The provider
/// is built outside the registry lock (it is async); a concurrent builder just loses the insert
/// race, which is harmless.
async fn shared_entry(cfg: &WebIdentityConfig) -> Arc<SharedEntry> {
    let key = cfg.entry_key();
    if let Some(entry) = registry().lock().unwrap().get(&key).cloned() {
        return entry;
    }

    let provider = build_provider(cfg).await;
    // Draw the refresh jitter once. subsec_nanos at build time differs across processes, so this
    // seeds a per-executor offset even before rand is consulted.
    let jitter = if cfg.max_jitter.is_zero() {
        Duration::ZERO
    } else {
        Duration::from_secs(rand::rng().random_range(0..=cfg.max_jitter.as_secs()))
    };
    let entry = Arc::new(SharedEntry {
        provider,
        cached: RwLock::new(None),
        refresh_lock: tokio::sync::Mutex::new(()),
        last_failure_at: RwLock::new(None),
        min_ttl: cfg.min_ttl,
        refresh_jitter: jitter,
    });

    let mut map = registry().lock().unwrap();
    Arc::clone(map.entry(key).or_insert(entry))
}

/// Builds the web-identity credential provider from the AWS SDK's fully-resolved config.
///
/// The key move: we load a real `SdkConfig` (`aws_config::defaults(...).load()`), which resolves
/// region, FIPS, dual-stack, the profile, and any custom/profile STS endpoint with the SDK's normal
/// environment-then-profile precedence, and build the STS client from it. Because the client is
/// built from the resolved config rather than a hand-assembled one, there is no per-setting copying
/// to keep in sync -- every endpoint/region knob the SDK understands is honored. We only ever call
/// `AssumeRoleWithWebIdentity`, so there is no IMDS/instance-role fallback to downgrade to, and the
/// raised `RetryConfig` gives the throttle its retries.
async fn build_provider(cfg: &WebIdentityConfig) -> Arc<dyn ProvideCredentials> {
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .retry_config(RetryConfig::standard().with_max_attempts(cfg.max_attempts))
        .load()
        .await;
    Arc::new(web_identity_provider_from(
        cfg,
        aws_sdk_sts::Client::new(&sdk),
    ))
}

/// Assembles the provider from an STS client. Split out so tests can supply a client built with an
/// in-memory HTTP stub while sharing the identity wiring with production.
fn web_identity_provider_from(
    cfg: &WebIdentityConfig,
    sts: aws_sdk_sts::Client,
) -> WebIdentityStsProvider {
    WebIdentityStsProvider {
        sts,
        role_arn: cfg.role_arn.clone(),
        token_file: cfg.token_file.clone(),
        session_name: session_name(),
    }
}

/// STS `AssumeRoleWithWebIdentity` session name. STS requires one; it is informational only, so a
/// stable prefix plus a timestamp keeps sessions distinguishable in CloudTrail.
fn session_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("comet-web-identity-{nanos}")
}

/// A web-identity-only credential provider: it reads the projected token and calls STS
/// `AssumeRoleWithWebIdentity` on `sts`, and does nothing else. No credential chain, so a throttle
/// that outlasts the STS client's retries returns an error rather than a lower-privilege identity.
#[derive(Debug)]
struct WebIdentityStsProvider {
    sts: aws_sdk_sts::Client,
    role_arn: String,
    token_file: String,
    session_name: String,
}

impl WebIdentityStsProvider {
    async fn resolve(&self) -> Result<Credentials, CredentialsError> {
        let token = std::fs::read_to_string(&self.token_file).map_err(|e| {
            CredentialsError::provider_error(format!(
                "reading web identity token file {}: {e}",
                self.token_file
            ))
        })?;
        let response = self
            .sts
            .assume_role_with_web_identity()
            .role_arn(&self.role_arn)
            .role_session_name(&self.session_name)
            .web_identity_token(token.trim())
            .send()
            .await
            .map_err(CredentialsError::provider_error)?;
        let creds = response.credentials().ok_or_else(|| {
            CredentialsError::provider_error(
                "STS AssumeRoleWithWebIdentity response had no credentials",
            )
        })?;
        let expiration = creds.expiration();
        let expiry = SystemTime::UNIX_EPOCH
            .checked_add(Duration::new(
                expiration.secs().max(0) as u64,
                expiration.subsec_nanos(),
            ))
            .ok_or_else(|| {
                CredentialsError::provider_error("STS credential expiry is out of range")
            })?;
        Ok(Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_string()),
            Some(expiry),
            "CometWebIdentity",
        ))
    }
}

impl ProvideCredentials for WebIdentityStsProvider {
    fn provide_credentials<'a>(&'a self) -> creds_future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        creds_future::ProvideCredentials::new(self.resolve())
    }
}

/// The credential provider handed to `object_store` (Parquet) and, via
/// `CustomAwsCredentialLoader`, to opendal (Iceberg). Holds only the cheap config plus a lazily
/// resolved handle to the process-wide shared entry, so the per-request path skips the registry
/// lock after the first fetch.
#[derive(Debug)]
pub struct WebIdentityCredentialProvider {
    config: WebIdentityConfig,
    entry: tokio::sync::OnceCell<Arc<SharedEntry>>,
}

impl WebIdentityCredentialProvider {
    pub fn new(config: WebIdentityConfig) -> Self {
        Self {
            config,
            entry: tokio::sync::OnceCell::new(),
        }
    }

    /// Resolves (once per provider) the shared entry for this identity. The entry itself is shared
    /// process-wide via the registry; this just memoizes the lookup so repeated fetches avoid the
    /// registry lock and the per-call `EntryKey` allocation.
    async fn entry(&self) -> &Arc<SharedEntry> {
        self.entry.get_or_init(|| shared_entry(&self.config)).await
    }
}

#[async_trait]
impl CredentialProvider for WebIdentityCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let cred =
            self.entry()
                .await
                .credentials()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: e.into(),
                })?;
        Ok(Arc::new(AwsCredential {
            key_id: cred.access_key_id().to_string(),
            secret_key: cred.secret_access_key().to_string(),
            token: cred.session_token().map(|s| s.to_string()),
        }))
    }
}

impl IcebergProvideCredential for WebIdentityCredentialProvider {
    type Credential = IcebergAwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let entry = self.entry().await;
        let cred = entry
            .credentials()
            .await
            .map_err(|e| ReqsignError::new(ReqsignErrorKind::CredentialInvalid, e))?;

        // Report the jittered refresh deadline (true expiry minus min_ttl minus jitter) as the
        // expiry opendal caches against, so opendal refreshes when our own cache would, and each
        // executor's refresh is spread out rather than synchronized.
        let expires_in = match cred.expiry() {
            Some(expiry) => {
                let deadline = expiry
                    .checked_sub(entry.min_ttl + entry.refresh_jitter)
                    .unwrap_or(expiry);
                Some(system_time_to_timestamp(deadline)?)
            }
            None => Some(Timestamp::now() + DEFAULT_EXPIRY_WHEN_UNKNOWN),
        };

        Ok(Some(IcebergAwsCredential {
            access_key_id: cred.access_key_id().to_string(),
            secret_access_key: cred.secret_access_key().to_string(),
            session_token: cred.session_token().map(|s| s.to_string()),
            expires_in,
        }))
    }
}

/// Decides whether the Comet web-identity provider should take over credential resolution. It does
/// so only when the caller has no explicit credentials configured and IRSA is detected; otherwise
/// the caller keeps its default chain. `resolve` reads a bare setting key (e.g. `KEY_MAX_ATTEMPTS`)
/// from whichever config bag the caller owns. Both scan paths share this one decision.
///
/// It also stands aside for any credential source the default chain ranks ahead of web-identity:
/// static credentials in the environment (`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`) or a
/// configured profile (`AWS_PROFILE`, or a shared credentials / config file). Both the AWS SDK
/// default chain (Parquet) and opendal/reqsign (Iceberg) resolve Environment -> Profile ->
/// WebIdentity, so taking over in those cases would silently switch identity from the user's chosen
/// source to the service-account role -- and would also drop profile-configured settings such as a
/// custom STS endpoint that a hand-built `ProviderConfig` cannot reconstruct here.
pub fn take_over_if_irsa<F>(
    explicit_credentials: bool,
    resolve: F,
) -> Option<WebIdentityCredentialProvider>
where
    F: Fn(&str) -> Option<String>,
{
    if explicit_credentials || explicit_env_credentials() || configured_profile() {
        return None;
    }
    WebIdentityConfig::detect_with(resolve).map(WebIdentityCredentialProvider::new)
}

/// True if explicit static credentials are present in the environment. These outrank web-identity
/// in every default chain, so the take-over must not shadow them.
fn explicit_env_credentials() -> bool {
    non_empty_env("AWS_ACCESS_KEY_ID").is_some() && non_empty_env("AWS_SECRET_ACCESS_KEY").is_some()
}

/// True if a profile is configured that the default chain would consult ahead of web-identity:
/// `AWS_PROFILE` is set, or a shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, else
/// `~/.aws/credentials`) or a config file (`AWS_CONFIG_FILE`, else `~/.aws/config`) exists. We defer
/// to the default chain in all of these because it resolves profile credentials AND
/// profile-configured settings (region, endpoint URLs, FIPS/dual-stack) that a hand-built
/// `ProviderConfig` cannot reconstruct here. Conservative by design: standing aside just falls back
/// to the pre-existing default-chain behavior, so it is never worse than before. On an EKS/IRSA pod
/// none of these are normally present, so the take-over still applies there.
fn configured_profile() -> bool {
    if non_empty_env("AWS_PROFILE").is_some() {
        return true;
    }
    let home = non_empty_env("HOME");
    let candidate = |env_key: &str, default_suffix: &str| {
        non_empty_env(env_key).or_else(|| home.as_ref().map(|h| format!("{h}{default_suffix}")))
    };
    [
        candidate("AWS_SHARED_CREDENTIALS_FILE", "/.aws/credentials"),
        candidate("AWS_CONFIG_FILE", "/.aws/config"),
    ]
    .into_iter()
    .flatten()
    .any(|path| Path::new(&path).exists())
}

fn system_time_to_timestamp(t: SystemTime) -> reqsign_core::Result<Timestamp> {
    let millis = t
        .duration_since(UNIX_EPOCH)
        .map_err(|e| {
            ReqsignError::new(
                ReqsignErrorKind::CredentialInvalid,
                format!("credential expiry precedes the unix epoch: {e}"),
            )
        })?
        .as_millis() as i64;
    Timestamp::from_millisecond(millis).map_err(|e| {
        ReqsignError::new(
            ReqsignErrorKind::CredentialInvalid,
            format!("invalid credential expiry {millis}: {e}"),
        )
    })
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Parses a setting value, falling back to `default`. Env fallback is intentionally omitted for
/// tunables: IRSA config travels in the config bag, and env is reserved for the IRSA signal itself.
fn parse_setting<T: std::str::FromStr>(value: Option<String>, default: T) -> T {
    value
        .and_then(|v| v.trim().parse::<T>().ok())
        .unwrap_or(default)
}

/// Like `parse_setting` but rejects zero (and negatives): a non-positive attempt budget makes no
/// sense, so it falls back to `default`.
fn parse_u32(value: Option<String>, default: u32) -> u32 {
    value
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::provider::error::CredentialsError;
    use aws_credential_types::provider::future as creds_future;
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_types::body::SdkBody;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Serializes the process-global env mutations the IRSA tests rely on. Poison-tolerant so one
    /// failing test does not cascade into confusing `PoisonError`s in the rest.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn clear_irsa_env() {
        std::env::remove_var(ENV_TOKEN_FILE);
        std::env::remove_var(ENV_ROLE_ARN);
    }

    const THROTTLE_XML: &str = concat!(
        r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">"#,
        r#"<Error><Type>Sender</Type><Code>Throttling</Code>"#,
        r#"<Message>Rate exceeded</Message></Error><RequestId>mock</RequestId></ErrorResponse>"#,
    );

    const SUCCESS_XML: &str = concat!(
        r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">"#,
        r#"<AssumeRoleWithWebIdentityResult><Credentials>"#,
        r#"<AccessKeyId>AKIDTEST</AccessKeyId>"#,
        r#"<SecretAccessKey>SECRETTEST</SecretAccessKey>"#,
        r#"<SessionToken>TOKENTEST</SessionToken>"#,
        r#"<Expiration>2999-01-01T00:00:00Z</Expiration>"#,
        r#"</Credentials>"#,
        r#"<SubjectFromWebIdentityToken>sub</SubjectFromWebIdentityToken>"#,
        r#"<AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/app/sess</Arn>"#,
        r#"<AssumedRoleId>ROLEID:sess</AssumedRoleId></AssumedRoleUser>"#,
        r#"<Provider>provider</Provider><Audience>aud</Audience>"#,
        r#"</AssumeRoleWithWebIdentityResult>"#,
        r#"<ResponseMetadata><RequestId>mock</RequestId></ResponseMetadata>"#,
        r#"</AssumeRoleWithWebIdentityResponse>"#,
    );

    /// A hand-rolled `HttpClient` that returns one canned STS response per attempt, in order, so the
    /// retry path can be exercised without a socket. A `200` yields the success document, anything
    /// else a `Throttling` error. Requests beyond the queue also throttle. It also records the URI
    /// of the last request so tests can assert which STS endpoint the client resolved.
    #[derive(Debug, Clone)]
    struct CannedStsClient {
        statuses: Arc<Mutex<VecDeque<u16>>>,
        requests: Arc<AtomicUsize>,
        last_uri: Arc<Mutex<Option<String>>>,
    }

    impl CannedStsClient {
        fn new(statuses: &[u16]) -> Self {
            Self {
                statuses: Arc::new(Mutex::new(statuses.iter().copied().collect())),
                requests: Arc::new(AtomicUsize::new(0)),
                last_uri: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl HttpConnector for CannedStsClient {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.requests.fetch_add(1, Ordering::SeqCst);
            *self.last_uri.lock().unwrap() = Some(request.uri().to_string());
            let status = self.statuses.lock().unwrap().pop_front().unwrap_or(400);
            let body = if status == 200 {
                SUCCESS_XML
            } else {
                THROTTLE_XML
            };
            let response = HttpResponse::try_from(
                http::Response::builder()
                    .status(status)
                    .body(SdkBody::from(body))
                    .unwrap(),
            )
            .unwrap();
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    impl HttpClient for CannedStsClient {
        fn http_connector(
            &self,
            _settings: &HttpConnectorSettings,
            _components: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    /// RAII IRSA env: writes the token file the AWS SDK reads and sets the identity env vars, then
    /// tears them down. `role_suffix` is unique per test so nothing collides. It also neutralizes
    /// the higher-precedence credential sources and endpoint knobs (env keys, profile, FIPS,
    /// dual-stack) so a test starts from a clean IRSA-only baseline regardless of the host
    /// environment; individual tests re-set the one variable they exercise.
    struct IrsaEnv {
        token_path: std::path::PathBuf,
    }

    impl IrsaEnv {
        fn set(role_suffix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let token_path =
                std::env::temp_dir().join(format!("comet-webid-{role_suffix}-{nanos}"));
            std::fs::write(&token_path, "dummy.web.identity.token").unwrap();
            std::env::set_var(ENV_TOKEN_FILE, &token_path);
            std::env::set_var(
                ENV_ROLE_ARN,
                format!("arn:aws:iam::123456789012:role/{role_suffix}"),
            );
            std::env::set_var("AWS_REGION", "us-east-1");
            // Clean baseline: no higher-precedence source and no endpoint mode unless a test opts in.
            for var in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
                "AWS_USE_FIPS_ENDPOINT",
                "AWS_USE_DUALSTACK_ENDPOINT",
            ] {
                std::env::remove_var(var);
            }
            // Point profile/config files at nonexistent paths so an ambient ~/.aws is ignored.
            let missing = std::env::temp_dir().join(format!("comet-webid-noprofile-{nanos}"));
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &missing);
            std::env::set_var("AWS_CONFIG_FILE", &missing);
            Self { token_path }
        }
    }

    impl Drop for IrsaEnv {
        fn drop(&mut self) {
            clear_irsa_env();
            for var in [
                "AWS_REGION",
                "AWS_SHARED_CREDENTIALS_FILE",
                "AWS_CONFIG_FILE",
                "AWS_PROFILE",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_USE_FIPS_ENDPOINT",
                "AWS_USE_DUALSTACK_ENDPOINT",
            ] {
                std::env::remove_var(var);
            }
            let _ = std::fs::remove_file(&self.token_path);
        }
    }

    /// Builds a `SharedEntry` whose STS calls are served by `http`, matching how production builds
    /// the provider (STS client from a loaded `SdkConfig`) but with the canned client attached. The
    /// identity comes from the `IrsaEnv` the test set, so the token file actually exists.
    async fn entry_with_http(max_attempts: u32, http: CannedStsClient) -> SharedEntry {
        let cfg = WebIdentityConfig {
            role_arn: non_empty_env(ENV_ROLE_ARN).expect("IrsaEnv sets the role arn"),
            token_file: non_empty_env(ENV_TOKEN_FILE).expect("IrsaEnv sets the token file"),
            region: Some("us-east-1".to_string()),
            max_attempts,
            min_ttl: Duration::from_secs(300),
            max_jitter: Duration::ZERO,
        };
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .retry_config(
                aws_config::retry::RetryConfig::standard().with_max_attempts(max_attempts),
            )
            .http_client(http)
            .load()
            .await;
        let provider = web_identity_provider_from(&cfg, aws_sdk_sts::Client::new(&sdk));
        SharedEntry {
            provider: Arc::new(provider),
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            last_failure_at: RwLock::new(None),
            min_ttl: cfg.min_ttl,
            refresh_jitter: Duration::ZERO,
        }
    }

    #[test]
    fn transient_sts_throttle_is_retried_then_succeeds() {
        let _guard = lock_env();
        let _env = IrsaEnv::set("retry-succeeds");
        let http = CannedStsClient::new(&[400, 400, 200]); // throttle twice, then succeed
        let requests = Arc::clone(&http.requests);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let credential = rt.block_on(async {
            let entry = entry_with_http(3, http).await;
            entry.credentials().await
        });
        let credential = credential.expect("retry must recover the transient throttle");
        assert_eq!(credential.access_key_id(), "AKIDTEST");
        assert_eq!(credential.session_token(), Some("TOKENTEST"));
        assert_eq!(
            requests.load(Ordering::SeqCst),
            3,
            "expected two throttled attempts then one success"
        );
    }

    #[test]
    fn persistent_sts_throttle_errors_without_downgrade() {
        let _guard = lock_env();
        let _env = IrsaEnv::set("always-throttled");
        let http = CannedStsClient::new(&[400, 400, 400]); // every attempt throttles
        let requests = Arc::clone(&http.requests);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let entry = entry_with_http(3, http).await;
            entry.credentials().await
        });
        assert!(
            result.is_err(),
            "a persistent throttle must surface as an error, never a downgraded credential"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            3,
            "retries must be bounded by maxAttempts"
        );
    }

    /// A stand-in for the AWS SDK provider that counts how many times it is asked to resolve, so
    /// tests can assert on caching and single-flighting without hitting STS. Each call sleeps
    /// briefly to widen the window in which concurrent callers overlap. When `fail` is set it
    /// always errors, standing in for a persistently throttled STS.
    #[derive(Debug)]
    struct CountingProvider {
        calls: Arc<AtomicUsize>,
        expiry: Option<SystemTime>,
        fail: bool,
    }

    impl ProvideCredentials for CountingProvider {
        fn provide_credentials<'a>(&'a self) -> creds_future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let calls = Arc::clone(&self.calls);
            let expiry = self.expiry;
            let fail = self.fail;
            creds_future::ProvideCredentials::new(async move {
                // Blocking sleep is fine here: waiters are parked on the async refresh lock, not on
                // this worker thread.
                std::thread::sleep(Duration::from_millis(20));
                calls.fetch_add(1, Ordering::SeqCst);
                if fail {
                    return Err(CredentialsError::not_loaded_no_source());
                }
                let mut builder = Credentials::builder()
                    .access_key_id("AKID")
                    .secret_access_key("SECRET")
                    .provider_name("counting");
                if let Some(exp) = expiry {
                    builder = builder.expiry(exp);
                }
                Ok(builder.build())
            })
        }
    }

    fn shared_entry_from(provider: CountingProvider, min_ttl: Duration) -> Arc<SharedEntry> {
        Arc::new(SharedEntry {
            provider: Arc::new(provider),
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            last_failure_at: RwLock::new(None),
            min_ttl,
            refresh_jitter: Duration::ZERO,
        })
    }

    fn entry_with(
        expiry: Option<SystemTime>,
        min_ttl: Duration,
    ) -> (Arc<SharedEntry>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let entry = shared_entry_from(
            CountingProvider {
                calls: Arc::clone(&calls),
                expiry,
                fail: false,
            },
            min_ttl,
        );
        (entry, calls)
    }

    fn failing_entry() -> (Arc<SharedEntry>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let entry = shared_entry_from(
            CountingProvider {
                calls: Arc::clone(&calls),
                expiry: None,
                fail: true,
            },
            Duration::from_secs(300),
        );
        (entry, calls)
    }

    #[test]
    fn detect_requires_both_env_vars_and_honors_toggle() {
        // These assertions share process env, so they live in one test to avoid racing another
        // test that mutates the same IRSA env vars.
        let _guard = lock_env();
        clear_irsa_env();
        let detect = |props: &HashMap<String, String>| {
            let props = props.clone();
            WebIdentityConfig::detect_with(move |key| props.get(key).cloned())
        };
        let props = HashMap::new();
        assert!(detect(&props).is_none());

        std::env::set_var(ENV_TOKEN_FILE, "/var/run/secrets/token");
        assert!(detect(&props).is_none(), "token file alone is not IRSA");

        std::env::set_var(ENV_ROLE_ARN, "arn:aws:iam::1:role/app");
        let cfg = detect(&props).expect("both vars present -> IRSA");
        assert_eq!(cfg.role_arn, "arn:aws:iam::1:role/app");
        assert_eq!(cfg.token_file, "/var/run/secrets/token");
        assert_eq!(cfg.max_attempts, DEFAULT_MAX_ATTEMPTS);

        // Same env, but the feature toggled off -> no take-over.
        let mut disabled = HashMap::new();
        disabled.insert(KEY_ENABLED.to_string(), "false".to_string());
        assert!(detect(&disabled).is_none());
        clear_irsa_env();
    }

    #[test]
    fn explicit_env_credentials_keep_precedence_over_irsa() {
        // With IRSA present AND explicit static env credentials set, the default chain would have
        // used the env credentials (Environment -> Profile -> WebIdentity). The take-over must
        // stand aside so it does not silently switch identity to the service-account role.
        let _guard = lock_env();
        let _env = IrsaEnv::set("env-precedence");

        // Clean IRSA baseline -> take over.
        assert!(
            take_over_if_irsa(false, |_| None).is_some(),
            "IRSA with no explicit credentials should take over"
        );

        // Explicit static env creds -> stand aside.
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
        assert!(
            take_over_if_irsa(false, |_| None).is_none(),
            "explicit env credentials must keep precedence over the IRSA take-over"
        );
    }

    #[test]
    fn configured_profile_keeps_precedence_over_irsa() {
        // A working profile is ranked before web-identity in the default chain, so the take-over
        // must defer to it rather than silently assuming the service-account role.
        let _guard = lock_env();
        let _env = IrsaEnv::set("profile-precedence");

        // Baseline neutralizes profile sources -> take over.
        assert!(
            take_over_if_irsa(false, |_| None).is_some(),
            "IRSA with no profile configured should take over"
        );

        // A selected profile -> stand aside.
        std::env::set_var("AWS_PROFILE", "second-account");
        assert!(
            take_over_if_irsa(false, |_| None).is_none(),
            "a configured profile must keep precedence over the IRSA take-over"
        );
    }

    #[test]
    fn config_file_profile_keeps_precedence_over_irsa() {
        // A default profile (or a profile-configured STS endpoint) can live in ~/.aws/config with
        // no AWS_PROFILE and no credentials file. The default chain honors it, and a hand-built
        // ProviderConfig cannot reconstruct a profile endpoint, so the take-over defers when a
        // config file is present.
        let _guard = lock_env();
        let _env = IrsaEnv::set("config-profile");

        // Baseline points AWS_CONFIG_FILE at a nonexistent path -> take over.
        assert!(
            take_over_if_irsa(false, |_| None).is_some(),
            "IRSA with no config file should take over"
        );

        // A config file with a profile-configured STS endpoint (the reviewer's trigger) -> stand
        // aside so the default chain can honor that endpoint.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_path = std::env::temp_dir().join(format!("comet-webid-config-{nanos}"));
        std::fs::write(
            &config_path,
            "[default]\nservices = private\n\n[services private]\nsts =\n  endpoint_url = https://sts.example.internal\n",
        )
        .unwrap();
        std::env::set_var("AWS_CONFIG_FILE", &config_path);
        let taken_over = take_over_if_irsa(false, |_| None).is_some();
        let _ = std::fs::remove_file(&config_path);
        assert!(
            !taken_over,
            "a config-file profile/endpoint must keep precedence over the IRSA take-over"
        );
    }

    #[test]
    fn forwards_fips_endpoint_setting() {
        // The default chain honors AWS_USE_FIPS_ENDPOINT; the take-over must too, or it would hit
        // the standard STS endpoint from a FIPS-restricted network.
        let _guard = lock_env();
        let _env = IrsaEnv::set("fips");
        std::env::set_var("AWS_USE_FIPS_ENDPOINT", "true");

        let http = CannedStsClient::new(&[200]);
        let last_uri = Arc::clone(&http.last_uri);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let entry = entry_with_http(3, http).await;
            entry.credentials().await.expect("mock STS returns success");
        });

        let uri = last_uri
            .lock()
            .unwrap()
            .clone()
            .expect("a request was made");
        assert!(
            uri.contains("sts-fips"),
            "expected a FIPS STS endpoint, got {uri}"
        );
    }

    #[test]
    fn fresh_credential_is_reused_without_refetching() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (entry, calls) = entry_with(
            Some(SystemTime::now() + Duration::from_secs(3600)),
            Duration::from_secs(300),
        );
        rt.block_on(async {
            entry.credentials().await.unwrap();
            entry.credentials().await.unwrap();
            entry.credentials().await.unwrap();
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a fresh credential must be served from cache"
        );
    }

    #[test]
    fn near_expiry_credential_is_refreshed() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        // Expires inside min_ttl, so it is always treated as stale and every call refreshes.
        let (entry, calls) = entry_with(
            Some(SystemTime::now() + Duration::from_secs(60)),
            Duration::from_secs(300),
        );
        rt.block_on(async {
            entry.credentials().await.unwrap();
            entry.credentials().await.unwrap();
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a credential expiring within min_ttl must be refreshed"
        );
    }

    #[test]
    fn concurrent_refresh_is_single_flighted() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (entry, calls) = entry_with(
            Some(SystemTime::now() + Duration::from_secs(3600)),
            Duration::from_secs(300),
        );
        rt.block_on(async {
            let futures = (0..8).map(|_| entry.credentials()).collect::<Vec<_>>();
            for result in futures::future::join_all(futures).await {
                result.unwrap();
            }
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a burst of readers must trigger exactly one STS call"
        );
    }

    #[test]
    fn concurrent_failed_refresh_is_coalesced() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (entry, calls) = failing_entry();
        rt.block_on(async {
            let futures = (0..8).map(|_| entry.credentials()).collect::<Vec<_>>();
            for result in futures::future::join_all(futures).await {
                assert!(
                    result.is_err(),
                    "a throttled refresh must surface as an error"
                );
            }
        });
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a failed burst must coalesce into one STS call, not one per reader"
        );
    }

    #[test]
    fn entry_key_includes_resolved_settings() {
        // Two callers with the same identity but different tuning must NOT share an entry, so each
        // catalog's configured retry/refresh knobs are honored regardless of init order.
        let base = WebIdentityConfig {
            role_arn: "arn:aws:iam::1:role/app".to_string(),
            token_file: "/token".to_string(),
            region: Some("us-east-1".to_string()),
            max_attempts: 5,
            min_ttl: Duration::from_secs(300),
            max_jitter: Duration::from_secs(60),
        };
        let mut more_attempts = base.clone();
        more_attempts.max_attempts = 8;
        assert_ne!(
            base.entry_key(),
            more_attempts.entry_key(),
            "different maxAttempts must not share a cache entry"
        );
        assert_eq!(
            base.entry_key(),
            base.clone().entry_key(),
            "identical identity and settings must share one entry"
        );
    }

    #[test]
    fn settings_parse_from_props_with_fallback() {
        let mut props = HashMap::new();
        props.insert(KEY_MAX_ATTEMPTS.to_string(), "9".to_string());
        props.insert(KEY_MIN_TTL_SECS.to_string(), "120".to_string());
        assert_eq!(
            parse_u32(props.get(KEY_MAX_ATTEMPTS).cloned(), DEFAULT_MAX_ATTEMPTS),
            9
        );
        assert_eq!(
            parse_setting::<u64>(props.get(KEY_MIN_TTL_SECS).cloned(), DEFAULT_MIN_TTL_SECS),
            120
        );
        // Zero and garbage fall back to the default.
        props.insert(KEY_MAX_ATTEMPTS.to_string(), "0".to_string());
        assert_eq!(
            parse_u32(props.get(KEY_MAX_ATTEMPTS).cloned(), DEFAULT_MAX_ATTEMPTS),
            DEFAULT_MAX_ATTEMPTS
        );
        assert_eq!(
            parse_setting::<u64>(props.get(KEY_JITTER_SECS).cloned(), DEFAULT_JITTER_SECS),
            DEFAULT_JITTER_SECS
        );
    }
}
