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
//!   1. Retry on throttle. It uses the AWS SDK `WebIdentityTokenCredentialsProvider`, whose STS
//!      client retries throttling with exponential backoff + jitter. `max_attempts` is
//!      configurable (default higher than the SDK's default of 3).
//!   2. No silent downgrade. The provider is web-identity ONLY -- there is no IMDS/instance-role
//!      fallback -- so a transient throttle surfaces as a retryable error instead of a
//!      wrong-identity credential.
//!   3. Shared, jittered cache. One assumed-role credential is cached per process, keyed by
//!      (role_arn, token_file, region), and shared across all reader threads and scans. Refresh
//!      fires ahead of expiry by `min_ttl` plus a per-process random jitter so cluster-wide
//!      refreshes do not synchronize into another burst.
//!
//! The same struct is exposed as both `object_store::CredentialProvider` (raw Parquet path) and
//! reqsign's `ProvideCredential` (Iceberg via opendal / `CustomAwsCredentialLoader`), mirroring
//! `credential_bridge::CometS3CredentialBridge`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_config::provider_config::ProviderConfig;
use aws_config::retry::RetryConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
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

/// Fallback expiry when a credential reports none. Web-identity credentials always carry an
/// expiry, so this only guards against a malformed STS response. Matches the bridge's bound.
const DEFAULT_EXPIRY_WHEN_UNKNOWN: Duration = Duration::from_secs(300);

/// Detected IRSA identity plus the resolved tuning knobs. Cheap to clone; the expensive AWS SDK
/// provider lives in the process-wide `SharedEntry` keyed by `entry_key`.
#[derive(Clone, Debug)]
pub struct WebIdentityConfig {
    role_arn: String,
    token_file: String,
    /// From `AWS_REGION` / `AWS_DEFAULT_REGION`; only part of the cache key. The actual region
    /// resolution is done by the AWS SDK's `with_default_region`.
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
        if !parse_bool(resolve(KEY_ENABLED), DEFAULT_ENABLED) {
            return None;
        }
        Some(Self {
            role_arn,
            token_file,
            region: non_empty_env("AWS_REGION").or_else(|| non_empty_env("AWS_DEFAULT_REGION")),
            max_attempts: parse_u32(resolve(KEY_MAX_ATTEMPTS), DEFAULT_MAX_ATTEMPTS),
            min_ttl: Duration::from_secs(parse_u64(
                resolve(KEY_MIN_TTL_SECS),
                DEFAULT_MIN_TTL_SECS,
            )),
            max_jitter: Duration::from_secs(parse_u64(
                resolve(KEY_JITTER_SECS),
                DEFAULT_JITTER_SECS,
            )),
        })
    }

    /// Convenience for the Iceberg path, whose catalog bag is a flat `HashMap` keyed by the bare
    /// setting names.
    pub fn detect(props: &HashMap<String, String>) -> Option<Self> {
        Self::detect_with(|key| props.get(key).cloned())
    }

    fn entry_key(&self) -> EntryKey {
        EntryKey {
            role_arn: self.role_arn.clone(),
            token_file: self.token_file.clone(),
            region: self.region.clone(),
        }
    }
}

/// Process-wide cache key. One assumed-role credential is shared per distinct identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EntryKey {
    role_arn: String,
    token_file: String,
    region: Option<String>,
}

/// The shared, cached credential for one identity. `provider` is the AWS SDK web-identity provider
/// built once; `cached` holds the last credential; `refresh_jitter` is drawn once per process so
/// each executor refreshes at a slightly different time.
#[derive(Debug)]
struct SharedEntry {
    provider: Arc<dyn ProvideCredentials>,
    cached: RwLock<Option<Credentials>>,
    /// Single-flights refreshes so a burst of readers triggers exactly one STS call.
    refresh_lock: tokio::sync::Mutex<()>,
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

    /// Fetches a fresh credential, refreshing from STS at most once at a time. On a refresh error
    /// the error propagates -- we never fall back to a lower-privilege identity.
    async fn credentials(&self) -> Result<Credentials, String> {
        if let Some(cred) = self.fresh() {
            return Ok(cred);
        }
        let _guard = self.refresh_lock.lock().await;
        // Re-check: another task may have refreshed while we waited on the lock.
        if let Some(cred) = self.fresh() {
            return Ok(cred);
        }
        let cred = self
            .provider
            .provide_credentials()
            .await
            .map_err(|e| format!("web-identity assume-role failed: {e}"))?;
        *self.cached.write().unwrap() = Some(cred.clone());
        Ok(cred)
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
        min_ttl: cfg.min_ttl,
        refresh_jitter: jitter,
    });

    let mut map = registry().lock().unwrap();
    Arc::clone(map.entry(key).or_insert(entry))
}

/// Builds a web-identity-only AWS SDK credential provider with STS retry raised to
/// `cfg.max_attempts`. No IMDS/instance-role fallback is wired in, so a throttle that outlasts the
/// retries errors instead of downgrading.
async fn build_provider(cfg: &WebIdentityConfig) -> Arc<dyn ProvideCredentials> {
    let provider_config = base_provider_config(cfg).await;
    let provider = WebIdentityTokenCredentialsProvider::builder()
        .configure(&provider_config)
        .build();
    Arc::new(provider)
}

/// The `ProviderConfig` shared by the production build and the tests: default region resolution
/// plus the raised STS retry budget. Tests attach an in-memory HTTP client to this so the retry
/// path can be exercised without a socket.
async fn base_provider_config(cfg: &WebIdentityConfig) -> ProviderConfig {
    ProviderConfig::with_default_region()
        .await
        .with_retry_config(RetryConfig::standard().with_max_attempts(cfg.max_attempts))
}

/// The credential provider handed to `object_store` (Parquet) and, via
/// `CustomAwsCredentialLoader`, to opendal (Iceberg). Holds only the cheap config; the shared
/// state lives in the process registry.
#[derive(Clone, Debug)]
pub struct WebIdentityCredentialProvider {
    config: WebIdentityConfig,
}

impl WebIdentityCredentialProvider {
    pub fn new(config: WebIdentityConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl CredentialProvider for WebIdentityCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let entry = shared_entry(&self.config).await;
        let cred = entry
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
        let entry = shared_entry(&self.config).await;
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
fn parse_bool(value: Option<String>, default: bool) -> bool {
    value
        .and_then(|v| v.trim().parse::<bool>().ok())
        .unwrap_or(default)
}

fn parse_u32(value: Option<String>, default: u32) -> u32 {
    value
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn parse_u64(value: Option<String>, default: u64) -> u64 {
    value
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
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
    /// else a `Throttling` error. Requests beyond the queue also throttle.
    #[derive(Debug, Clone)]
    struct CannedStsClient {
        statuses: Arc<Mutex<VecDeque<u16>>>,
        requests: Arc<AtomicUsize>,
    }

    impl CannedStsClient {
        fn new(statuses: &[u16]) -> Self {
            Self {
                statuses: Arc::new(Mutex::new(statuses.iter().copied().collect())),
                requests: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl HttpConnector for CannedStsClient {
        fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
            self.requests.fetch_add(1, Ordering::SeqCst);
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
    /// tears them down. `role_suffix` is unique per test so nothing collides.
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
            Self { token_path }
        }
    }

    impl Drop for IrsaEnv {
        fn drop(&mut self) {
            clear_irsa_env();
            std::env::remove_var("AWS_REGION");
            let _ = std::fs::remove_file(&self.token_path);
        }
    }

    /// Builds a `SharedEntry` whose STS calls are served by `http`, matching how production builds
    /// the provider (`base_provider_config`) but with the canned client attached.
    async fn entry_with_http(max_attempts: u32, http: CannedStsClient) -> SharedEntry {
        let cfg = WebIdentityConfig {
            role_arn: "arn:aws:iam::123456789012:role/test".to_string(),
            token_file: "unused-in-key".to_string(),
            region: Some("us-east-1".to_string()),
            max_attempts,
            min_ttl: Duration::from_secs(300),
            max_jitter: Duration::ZERO,
        };
        let provider_config = base_provider_config(&cfg).await.with_http_client(http);
        let provider = WebIdentityTokenCredentialsProvider::builder()
            .configure(&provider_config)
            .build();
        SharedEntry {
            provider: Arc::new(provider),
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
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
    /// briefly to widen the window in which concurrent callers overlap.
    #[derive(Debug)]
    struct CountingProvider {
        calls: Arc<AtomicUsize>,
        expiry: Option<SystemTime>,
    }

    impl ProvideCredentials for CountingProvider {
        fn provide_credentials<'a>(&'a self) -> creds_future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let calls = Arc::clone(&self.calls);
            let expiry = self.expiry;
            creds_future::ProvideCredentials::new(async move {
                // Blocking sleep is fine here: waiters are parked on the async refresh lock, not on
                // this worker thread.
                std::thread::sleep(Duration::from_millis(20));
                calls.fetch_add(1, Ordering::SeqCst);
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

    fn entry_with(
        expiry: Option<SystemTime>,
        min_ttl: Duration,
    ) -> (Arc<SharedEntry>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let entry = Arc::new(SharedEntry {
            provider: Arc::new(CountingProvider {
                calls: Arc::clone(&calls),
                expiry,
            }),
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            min_ttl,
            refresh_jitter: Duration::ZERO,
        });
        (entry, calls)
    }

    #[test]
    fn detect_requires_both_env_vars_and_honors_toggle() {
        // These assertions share process env, so they live in one test to avoid racing another
        // test that mutates the same IRSA env vars.
        let _guard = lock_env();
        clear_irsa_env();
        let props = HashMap::new();
        assert!(WebIdentityConfig::detect(&props).is_none());

        std::env::set_var(ENV_TOKEN_FILE, "/var/run/secrets/token");
        assert!(
            WebIdentityConfig::detect(&props).is_none(),
            "token file alone is not IRSA"
        );

        std::env::set_var(ENV_ROLE_ARN, "arn:aws:iam::1:role/app");
        let cfg = WebIdentityConfig::detect(&props).expect("both vars present -> IRSA");
        assert_eq!(cfg.role_arn, "arn:aws:iam::1:role/app");
        assert_eq!(cfg.token_file, "/var/run/secrets/token");
        assert_eq!(cfg.max_attempts, DEFAULT_MAX_ATTEMPTS);

        // Same env, but the feature toggled off -> no take-over.
        let mut disabled = HashMap::new();
        disabled.insert(KEY_ENABLED.to_string(), "false".to_string());
        assert!(WebIdentityConfig::detect(&disabled).is_none());
        clear_irsa_env();
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
    fn settings_parse_from_props_with_fallback() {
        let mut props = HashMap::new();
        props.insert(KEY_MAX_ATTEMPTS.to_string(), "9".to_string());
        props.insert(KEY_MIN_TTL_SECS.to_string(), "120".to_string());
        assert_eq!(
            parse_u32(props.get(KEY_MAX_ATTEMPTS).cloned(), DEFAULT_MAX_ATTEMPTS),
            9
        );
        assert_eq!(
            parse_u64(props.get(KEY_MIN_TTL_SECS).cloned(), DEFAULT_MIN_TTL_SECS),
            120
        );
        // Zero and garbage fall back to the default.
        props.insert(KEY_MAX_ATTEMPTS.to_string(), "0".to_string());
        assert_eq!(
            parse_u32(props.get(KEY_MAX_ATTEMPTS).cloned(), DEFAULT_MAX_ATTEMPTS),
            DEFAULT_MAX_ATTEMPTS
        );
        assert_eq!(
            parse_u64(props.get(KEY_JITTER_SECS).cloned(), DEFAULT_JITTER_SECS),
            DEFAULT_JITTER_SECS
        );
    }
}
