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

use log::{debug, error};
use std::collections::HashMap;
use std::sync::OnceLock;
use url::{Host, Url};

use crate::cloud::s3::credential_bridge::{AccessMode, CometS3CredentialBridge};
use crate::execution::jni_api::get_runtime;
use crate::parquet::objectstore::location_scoped::{
    LocationScopedObjectStore, LocationSource, LocationStoreFactory,
};
use async_trait::async_trait;
use aws_config::{
    default_provider::region::DefaultRegionChain,
    ecs::EcsCredentialsProvider,
    environment::EnvironmentVariableCredentialsProvider,
    identity::IdentityCache,
    imds::credentials::ImdsCredentialsProvider,
    meta::{credentials::CredentialsProviderChain, region::ProvideRegion},
    profile::{Profile, ProfileFileCredentialsProvider, ProfileFileRegionProvider, ProfileSet},
    provider_config::ProviderConfig,
    sts::AssumeRoleProvider,
    web_identity_token::WebIdentityTokenCredentialsProvider,
    BehaviorVersion, ConfigLoader, Region, SdkConfig,
};
use aws_credential_types::{
    provider::{error::CredentialsError, ProvideCredentials, SharedCredentialsProvider},
    Credentials,
};
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, AwsCredentialProvider},
    path::Path,
    CredentialProvider, ObjectStore, ObjectStoreScheme,
};
use std::error::Error;
use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

/// Creates an S3 object store using options specified as Hadoop S3A configurations.
///
/// When the configured `CometS3CredentialProvider` implements
/// `CometS3LocationScopedCredentialProvider`, the store is a [`LocationScopedObjectStore`] that
/// serves each of the provider's policy locations with its own credential.
///
/// # Arguments
///
/// * `url` - The URL of the S3 object to access.
/// * `configs` - The Hadoop S3A configurations to use for building the object store.
/// * `min_ttl` - Time buffer before credential expiry when refresh should be triggered.
///
/// # Returns
///
/// * `(Box<dyn ObjectStore>, Path)` - The object store and path of the S3 object store.
///
pub fn create_store(
    url: &Url,
    configs: &HashMap<String, String>,
    min_ttl: Duration,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    if scheme != ObjectStoreScheme::AmazonS3 {
        return Err(object_store::Error::Generic {
            store: "S3",
            source: format!("Scheme of URL is not S3: {url}").into(),
        });
    }
    let path = Path::parse(path)?;

    let bucket = url.host_str().ok_or_else(|| object_store::Error::Generic {
        store: "S3",
        source: "Missing bucket name in S3 URL".into(),
    })?;

    let credentials = match lookup_provider_class(configs, bucket) {
        Some(provider_class) => {
            // Parquet path: forward the full fs.s3a.* config subset so the SPI provider sees the
            // same config Spark would (e.g. the built-in adapters read fs.s3a.aws.credentials.provider
            // and any static keys a chain resolves through). Only built when a bridge is actually
            // configured. See s3-credential-provider-design.md.
            let forwarded_props = forward_catalog_properties(configs);
            // Fail rather than fall back to the default chain, which could resolve to the wrong
            // identity for a user who explicitly named a provider.
            let bridge = CometS3CredentialBridge::new(
                provider_class,
                bucket,
                bucket,
                url.path(),
                AccessMode::Read,
                &forwarded_props,
            )
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: format!("CometS3CredentialBridge init failed for {bucket}: {e}").into(),
            })?;
            let locations =
                bridge
                    .policy_locations()
                    .map_err(|e| object_store::Error::Generic {
                        store: "S3",
                        source: format!("Failed to get policy locations for {bucket}: {e}").into(),
                    })?;
            if let Some(locations) = locations {
                let template = S3StoreTemplate::new(url, configs, bucket)?;
                let store = location_scoped_store(template, bucket, bridge, locations)?;
                return Ok((Box::new(store), path));
            }
            S3Credentials::Provider(Arc::new(bridge))
        }
        None => {
            match get_runtime().block_on(build_credential_provider(configs, bucket, min_ttl))? {
                Some(provider) => S3Credentials::Provider(Arc::new(provider)),
                None => S3Credentials::SkipSignature,
            }
        }
    };

    let object_store = S3StoreTemplate::new(url, configs, bucket)?.build(credentials)?;

    Ok((Box::new(object_store), path))
}

/// How a store built from an [`S3StoreTemplate`] signs its requests.
enum S3Credentials {
    Provider(AwsCredentialProvider),
    SkipSignature,
}

/// Builder settings shared by every store for one bucket. Creating a template may block on a
/// region lookup; building a store from it does not, so location-scoped stores can be built from
/// async code on a Tokio worker.
struct S3StoreTemplate {
    url: String,
    region: Option<String>,
    s3_configs: HashMap<AmazonS3ConfigKey, String>,
}

impl S3StoreTemplate {
    fn new(
        url: &Url,
        configs: &HashMap<String, String>,
        bucket: &str,
    ) -> Result<Self, object_store::Error> {
        let s3_configs = extract_s3_config_options(configs, bucket);
        debug!("S3 configs for bucket {bucket}: {s3_configs:?}");

        // When using the default AWS S3 endpoint (no custom endpoint configured), a valid region
        // is required. If no region is explicitly configured, attempt to auto-resolve it by
        // making a HeadBucket request to determine the bucket's region.
        let region = if !s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint)
            && !s3_configs.contains_key(&AmazonS3ConfigKey::Region)
        {
            let region = get_runtime()
                .block_on(resolve_bucket_region(bucket))
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: format!(
                        "Failed to resolve region: {e}. If '{bucket}' is on a non-AWS S3-compatible \
                         service, set fs.s3a.endpoint (and optionally fs.s3a.endpoint.region, \
                         fs.s3a.path.style.access) or the per-bucket variants \
                         fs.s3a.bucket.{bucket}.endpoint[.region] so Comet skips the AWS HEAD probe."
                    )
                    .into(),
                })?;
            debug!("resolved region: {region:?}");
            Some(region)
        } else {
            None
        };

        Ok(Self {
            url: url.to_string(),
            region,
            s3_configs,
        })
    }

    fn build(&self, credentials: S3Credentials) -> Result<AmazonS3, object_store::Error> {
        let builder = AmazonS3Builder::new()
            .with_url(self.url.clone())
            .with_allow_http(true);
        let mut builder = match credentials {
            S3Credentials::Provider(provider) => builder.with_credentials(provider),
            S3Credentials::SkipSignature => builder.with_skip_signature(true),
        };
        if let Some(region) = &self.region {
            builder = builder.with_config(AmazonS3ConfigKey::Region, region.clone());
        }
        for (key, value) in &self.s3_configs {
            builder = builder.with_config(*key, value.clone());
        }
        builder.build()
    }
}

/// Builds the store for a `CometS3LocationScopedCredentialProvider`. `bridge` was created on this
/// thread, which registered the provider. It fetches the locations again after a 403 or a failure
/// to get a location's credential, and each location's bridge is derived from it on first use,
/// often on a Tokio worker, so every location shares the bucket's provider registration without
/// another `ensureInitialized` call.
fn location_scoped_store(
    template: S3StoreTemplate,
    bucket: &str,
    bridge: CometS3CredentialBridge,
    locations: Vec<String>,
) -> Result<LocationScopedObjectStore, object_store::Error> {
    let bridge = Arc::new(bridge);

    let source_bridge = Arc::clone(&bridge);
    let source_bucket = bucket.to_string();
    let source: LocationSource = Arc::new(move || {
        let locations =
            source_bridge
                .policy_locations()
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: format!("Failed to get policy locations for {source_bucket}: {e}")
                        .into(),
                })?;
        locations.ok_or_else(|| object_store::Error::Generic {
            store: "S3",
            source: format!("The provider for {source_bucket} stopped returning policy locations")
                .into(),
        })
    });

    let factory_bucket = bucket.to_string();
    let factory: LocationStoreFactory = Arc::new(move |credential_path: &str| {
        let location_bridge =
            bridge
                .for_path(credential_path)
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: format!(
                        "CometS3CredentialBridge init failed for {factory_bucket}: {e}"
                    )
                    .into(),
                })?;
        let store = template.build(S3Credentials::Provider(Arc::new(location_bridge)))?;
        Ok(Arc::new(store) as Arc<dyn ObjectStore>)
    });

    LocationScopedObjectStore::new(bucket.to_string(), locations, source, factory)
}

/// Process-wide cache of resolved S3 bucket regions, keyed by bucket name.
///
/// ## Why static / process lifetime?
///
/// See the equivalent rationale on `object_store_cache` in `parquet_support.rs`: the JNI
/// call site creates a new `RuntimeEnv` per file, leaving the executor process as the only
/// available scope for cross-call state.  In the standard Spark-on-Kubernetes deployment
/// model each executor is dedicated to a single application, so process and application
/// lifetimes are equivalent.
///
/// ## Unbounded size
///
/// A Spark job accesses a bounded, typically small set of S3 buckets, so the number of
/// entries stays proportional to the number of distinct buckets.  Entries are just
/// `(String, String)` pairs and the set does not grow beyond what the job actually touches.
///
/// ## Invalidation
///
/// An S3 bucket's region is permanently fixed at creation time and cannot change; no
/// invalidation is therefore needed.  This is what makes a static, never-evicting cache
/// safe here and on the equivalent region-resolution path inside the `object_store` crate.
fn region_cache() -> &'static RwLock<HashMap<String, String>> {
    static CACHE: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Get the bucket region using the [HeadBucket API]. This will fail if the bucket does not exist.
/// Results are cached per bucket to avoid redundant network calls.
///
/// [HeadBucket API]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
///
/// TODO this is copied from the object store crate and has been adapted as a workaround
/// for https://github.com/apache/arrow-rs-object-store/issues/479
pub async fn resolve_bucket_region(bucket: &str) -> Result<String, Box<dyn Error>> {
    // Check cache first
    if let Ok(cache) = region_cache().read() {
        if let Some(region) = cache.get(bucket) {
            debug!("Using cached region '{region}' for bucket '{bucket}'");
            return Ok(region.clone());
        }
    }

    let endpoint = format!("https://{bucket}.s3.amazonaws.com");
    let client = reqwest::Client::new();

    let response = client.head(&endpoint).send().await?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(Box::new(object_store::Error::Generic {
            store: "S3",
            source: format!("Bucket not found: {bucket}").into(),
        }));
    }

    let region = response
        .headers()
        .get("x-amz-bucket-region")
        .ok_or_else(|| {
            Box::new(object_store::Error::Generic {
                store: "S3",
                source: format!("Missing region for bucket: {bucket}").into(),
            })
        })?
        .to_str()?
        .to_string();

    // Cache the resolved region
    if let Ok(mut cache) = region_cache().write() {
        debug!("Caching region '{region}' for bucket '{bucket}'");
        cache.insert(bucket.to_string(), region.clone());
    }

    Ok(region)
}

/// Extracts S3 configuration options from Hadoop S3A configurations and returns them
/// as a HashMap of (AmazonS3ConfigKey, String) pairs that can be applied to an AmazonS3Builder.
///
/// # Arguments
///
/// * `configs` - The Hadoop S3A configurations to extract from.
/// * `bucket` - The bucket name to extract configurations for.
///
/// # Returns
///
/// * `HashMap<AmazonS3ConfigKey, String>` - The extracted S3 configuration options.
///
fn extract_s3_config_options(
    configs: &HashMap<String, String>,
    bucket: &str,
) -> HashMap<AmazonS3ConfigKey, String> {
    let mut s3_configs = HashMap::new();

    // Extract region configuration
    if let Some(region) = get_config_trimmed(configs, bucket, "endpoint.region") {
        s3_configs.insert(AmazonS3ConfigKey::Region, region.to_string());
    }

    // Hadoop defaults fs.s3a.path.style.access to false, which means virtual-hosted addressing,
    // and treats non-boolean text as that default. object_store expects the inverse flag.
    let path_style_access = get_config_trimmed(configs, bucket, "path.style.access")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let mut virtual_hosted_style_request = !path_style_access;

    // Extract endpoint configuration and shape it for the selected addressing style. The flag is
    // taken from the normalized result so the endpoint and the flag never disagree. A custom
    // endpoint decides the bucket-name rule by its own scheme inside normalize_endpoint; the
    // default AWS endpoint is HTTPS, so the rule applies to it here without dots.
    let custom_endpoint = get_config_trimmed(configs, bucket, "endpoint")
        .and_then(|endpoint| normalize_endpoint(endpoint, bucket, virtual_hosted_style_request));
    match custom_endpoint {
        Some(normalized) => {
            virtual_hosted_style_request = normalized.virtual_hosted_style_request;
            s3_configs.insert(AmazonS3ConfigKey::Endpoint, normalized.endpoint);
        }
        None => {
            if !is_virtual_hostable_bucket(bucket, false) {
                virtual_hosted_style_request = false;
            }
        }
    }
    s3_configs.insert(
        AmazonS3ConfigKey::VirtualHostedStyleRequest,
        virtual_hosted_style_request.to_string(),
    );

    // Extract request payer configuration
    if let Some(requester_pays) = get_config_trimmed(configs, bucket, "requester.pays.enabled") {
        let requester_pays_enabled = requester_pays.to_lowercase() == "true";
        s3_configs.insert(
            AmazonS3ConfigKey::RequestPayer,
            requester_pays_enabled.to_string(),
        );
    }

    s3_configs
}

/// Whether the AWS SDK would virtual-host `bucket`, following its `isVirtualHostableS3Bucket`
/// endpoint rule: 3 to 63 lowercase letters, digits and hyphens that start and end with a letter
/// or digit. Other names, such as mixed-case legacy buckets, are addressed path-style, since a
/// hostname is case-insensitive. The SDK allows dots only over plain HTTP, since a dotted host
/// falls outside S3's wildcard certificate, and then rejects an IPv4-shaped name and a dot or
/// hyphen next to another.
fn is_virtual_hostable_bucket(bucket: &str, allow_dots: bool) -> bool {
    let bytes = bucket.as_bytes();
    let edge = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let inner = |b: &u8| edge(b) || *b == b'-' || (allow_dots && *b == b'.');
    if !(3..=63).contains(&bytes.len())
        || !bytes.first().is_some_and(edge)
        || !bytes.last().is_some_and(edge)
        || !bytes.iter().all(inner)
    {
        return false;
    }
    let ipv4_shaped = bucket.split('.').count() == 4
        && bucket
            .split('.')
            .all(|label| label.bytes().all(|b| b.is_ascii_digit()));
    let separators_touch = bytes
        .windows(2)
        .any(|pair| pair.iter().all(|b| *b == b'.' || *b == b'-'));
    !(allow_dots && (ipv4_shaped || separators_touch))
}

/// An endpoint shaped for object_store together with the addressing mode it was shaped for.
#[derive(Debug, Clone, PartialEq)]
struct NormalizedEndpoint {
    endpoint: String,
    virtual_hosted_style_request: bool,
}

/// Shapes a Hadoop `fs.s3a.endpoint` value into the endpoint object_store expects: for
/// virtual-hosted requests the bucket becomes the leading host label (`scheme://bucket.host[:port]`),
/// while for path-style requests object_store appends `/bucket` itself so the value passes through.
fn normalize_endpoint(
    endpoint: &str,
    bucket: &str,
    virtual_hosted_style_request: bool,
) -> Option<NormalizedEndpoint> {
    if endpoint.is_empty() {
        return None;
    }

    // This is the default Hadoop S3A configuration. Explicitly specifying this endpoint will lead to HTTP
    // request failures when using object_store crate, so we ignore it and let object_store crate
    // use the default endpoint.
    if endpoint == "s3.amazonaws.com" {
        return None;
    }

    let endpoint = if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        format!("https://{endpoint}")
    } else {
        endpoint.to_string()
    };

    let path_style = |endpoint: String| {
        Some(NormalizedEndpoint {
            endpoint,
            virtual_hosted_style_request: false,
        })
    };
    if !virtual_hosted_style_request {
        return path_style(endpoint);
    }
    if !is_virtual_hostable_bucket(bucket, endpoint.starts_with("http://")) {
        return path_style(endpoint);
    }

    // Fall back to the endpoint as written when it cannot be parsed so object_store reports
    // the malformed value instead of a mangled one
    let Ok(url) = Url::parse(&endpoint) else {
        return path_style(endpoint);
    };
    // The AWS SDK endpoint rules address IP-literal hosts path-style since `bucket.127.0.0.1` is
    // not a valid host. Hadoop does not special-case `localhost`, so neither does this.
    let host = match url.host() {
        Some(Host::Domain(host)) => host,
        _ => return path_style(endpoint),
    };
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let path = url.path().trim_end_matches('/');
    Some(NormalizedEndpoint {
        endpoint: format!("{}://{bucket}.{host}{port}{path}", url.scheme()),
        virtual_hosted_style_request: true,
    })
}

/// Object store defaults the executor JVM resolves at plan creation and native applies to
/// every scan's options, so the native side reads the same files Hadoop does on that executor.
#[derive(Debug, Clone, Default)]
pub struct ExecutorObjectStoreDefaults {
    /// The credentials file Hadoop's profile provider reads with no `fs.s3a.auth.profile.file`
    /// configured, resolved against the executor JVM's `user.home` and environment.
    pub default_profile_file: Option<String>,
}

/// The key the executor JVM and the native provider share for that file.
pub const COMET_DEFAULT_PROFILE_FILE_KEY: &str = "fs.s3a.comet.default.profile.file";

impl ExecutorObjectStoreDefaults {
    /// Overlays the executor's defaults onto a scan's forwarded options. The executor value
    /// wins over anything the driver serialized, since only the executor knows its own home.
    pub fn apply(&self, options: &mut HashMap<String, String>) {
        if let Some(file) = &self.default_profile_file {
            options.insert(COMET_DEFAULT_PROFILE_FILE_KEY.to_string(), file.clone());
        }
    }
}

/// Applies the executor defaults registered on `session` (see `ExecutorObjectStoreDefaults`)
/// to a scan's forwarded object store options.
pub fn apply_executor_object_store_defaults(
    session: &datafusion::prelude::SessionContext,
    options: &mut HashMap<String, String>,
) {
    if let Some(defaults) = session
        .state()
        .config()
        .get_extension::<ExecutorObjectStoreDefaults>()
    {
        defaults.apply(options);
    }
}

/// The credentials file Hadoop's profile provider reads when none is configured:
/// `AWS_SHARED_CREDENTIALS_FILE` when set, otherwise `~/.aws/credentials`.
fn default_shared_credentials_file(env_override: Option<String>, home: Option<String>) -> String {
    env_override
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| format!("{}/.aws/credentials", home.unwrap_or_default()))
}

fn get_config<'a>(
    configs: &'a HashMap<String, String>,
    bucket: &str,
    property: &str,
) -> Option<&'a String> {
    let per_bucket_key = format!("fs.s3a.bucket.{bucket}.{property}");
    configs.get(&per_bucket_key).or_else(|| {
        let global_key = format!("fs.s3a.{property}");
        configs.get(&global_key)
    })
}

pub(super) fn get_config_trimmed<'a>(
    configs: &'a HashMap<String, String>,
    bucket: &str,
    property: &str,
) -> Option<&'a str> {
    get_config(configs, bucket, property).map(|s| s.trim())
}

/// Like [`get_config_trimmed`] but treats a blank value as unset.
fn get_non_empty_config(
    configs: &HashMap<String, String>,
    bucket: &str,
    property: &str,
) -> Option<String> {
    get_config_trimmed(configs, bucket, property)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Activation key (without `fs.s3a.` prefix) naming the vendor `CometS3CredentialProvider` FQCN.
/// Per-bucket override is honored via [`get_config_trimmed`].
const PROVIDER_CLASS_PROPERTY: &str = "comet.credential.provider.class";

fn lookup_provider_class<'a>(
    configs: &'a HashMap<String, String>,
    bucket: &str,
) -> Option<&'a str> {
    get_config_trimmed(configs, bucket, PROVIDER_CLASS_PROPERTY).filter(|s| !s.is_empty())
}

/// Builds the `catalog_properties` map forwarded to the SPI on the Parquet path: the full
/// `fs.s3a.*` subset. This matches the Iceberg path, which forwards its full property bag, so an
/// adapter delegating to Hadoop's provider construction sees exactly the config Spark would --
/// including the static keys a provider chain may resolve through. Stripping them would let a
/// chain like `SimpleAWSCredentialsProvider,customProvider` silently resolve through a different
/// entry than Spark, reading data as a different principal. These keys already cross JNI for the
/// non-adapter path (see `build_credential_provider`). HashMap equality is order-independent, so
/// the dispatcher instance-cache key stays stable regardless of iteration order.
fn forward_catalog_properties(configs: &HashMap<String, String>) -> HashMap<String, String> {
    configs
        .iter()
        .filter(|(k, _)| k.starts_with("fs.s3a."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

// Hadoop S3A credential provider constants
const HADOOP_IAM_INSTANCE: &str = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider";
const HADOOP_SIMPLE: &str = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
const HADOOP_TEMPORARY: &str = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
const HADOOP_ASSUMED_ROLE: &str = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";
const HADOOP_ANONYMOUS: &str = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";

// AWS SDK credential provider constants
const AWS_CONTAINER_CREDENTIALS: &str =
    "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider";
const AWS_CONTAINER_CREDENTIALS_V1: &str = "com.amazonaws.auth.ContainerCredentialsProvider";
const AWS_EC2_CONTAINER_CREDENTIALS: &str =
    "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper";
const AWS_INSTANCE_PROFILE: &str =
    "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider";
const AWS_INSTANCE_PROFILE_V1: &str = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
const AWS_ENVIRONMENT: &str =
    "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider";
const AWS_ENVIRONMENT_V1: &str = "com.amazonaws.auth.EnvironmentVariableCredentialsProvider";
const AWS_WEB_IDENTITY: &str =
    "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider";
const AWS_WEB_IDENTITY_V1: &str = "com.amazonaws.auth.WebIdentityTokenCredentialsProvider";
const AWS_PROFILE: &str = "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider";
const AWS_PROFILE_V1: &str = "com.amazonaws.auth.profile.ProfileCredentialsProvider";
const HADOOP_PROFILE: &str = "org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider";
const AWS_ANONYMOUS: &str = "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider";
const AWS_ANONYMOUS_V1: &str = "com.amazonaws.auth.AnonymousAWSCredentials";

/// Builds an AWS credential provider from the given configurations.
/// It first checks if the credential provider is anonymous, and if so, returns `None`.
/// Otherwise, it builds a [CachedAwsCredentialProvider] from the given configurations.
///
/// # Arguments
///
/// * `configs` - The Hadoop S3A configurations to use for building the credential provider.
/// * `bucket` - The bucket to build the credential provider for.
/// * `min_ttl` - Time buffer before credential expiry when refresh should be triggered.
///
/// # Returns
///
/// * `None` - If the credential provider is anonymous.
/// * `Some(CachedAwsCredentialProvider)` - If the credential provider is not anonymous.
///
async fn build_credential_provider(
    configs: &HashMap<String, String>,
    bucket: &str,
    min_ttl: Duration,
) -> Result<Option<CachedAwsCredentialProvider>, object_store::Error> {
    let aws_credential_provider_names =
        get_config_trimmed(configs, bucket, "aws.credentials.provider");
    let aws_credential_provider_names =
        aws_credential_provider_names.map_or(Vec::new(), |s| parse_credential_provider_names(s));
    if aws_credential_provider_names
        .iter()
        .any(|name| is_anonymous_credential_provider(name))
    {
        if aws_credential_provider_names.len() > 1 {
            return Err(object_store::Error::Generic {
                store: "S3",
                source:
                    "Anonymous credential provider cannot be mixed with other credential providers"
                        .into(),
            });
        }
        return Ok(None);
    }
    let provider_metadata = build_chained_aws_credential_provider_metadata(
        aws_credential_provider_names,
        configs,
        bucket,
    )?;
    debug!(
        "Credential providers for S3 bucket {}: {}",
        bucket,
        provider_metadata.simple_string()
    );
    let provider = provider_metadata.create_credential_provider().await?;
    Ok(Some(CachedAwsCredentialProvider::new(
        provider,
        provider_metadata,
        min_ttl,
    )))
}

fn parse_credential_provider_names(aws_credential_provider_names: &str) -> Vec<&str> {
    aws_credential_provider_names
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
}

fn is_anonymous_credential_provider(credential_provider_name: &str) -> bool {
    [HADOOP_ANONYMOUS, AWS_ANONYMOUS_V1, AWS_ANONYMOUS].contains(&credential_provider_name)
}

fn build_chained_aws_credential_provider_metadata(
    credential_provider_names: Vec<&str>,
    configs: &HashMap<String, String>,
    bucket: &str,
) -> Result<CredentialProviderMetadata, object_store::Error> {
    if credential_provider_names.is_empty() {
        // Use the default credential provider chain. This is actually more permissive than
        // the default Hadoop S3A FileSystem behavior, which only uses
        // TemporaryAWSCredentialsProvider, SimpleAWSCredentialsProvider,
        // EnvironmentVariableCredentialsProvider and IAMInstanceCredentialsProvider
        return Ok(CredentialProviderMetadata::Default);
    }

    // Safety: credential_provider_names is not empty, taking its first element is safe
    let provider_name = credential_provider_names[0];
    let provider_metadata = build_aws_credential_provider_metadata(provider_name, configs, bucket)?;
    if credential_provider_names.len() == 1 {
        // No need to chain the provider as there's only one provider
        return Ok(provider_metadata);
    }

    // More than one credential provider names were specified, we need to chain them together
    let mut metadata_vec = vec![provider_metadata];
    for provider_name in credential_provider_names[1..].iter() {
        let provider_metadata =
            build_aws_credential_provider_metadata(provider_name, configs, bucket)?;
        metadata_vec.push(provider_metadata);
    }

    Ok(CredentialProviderMetadata::Chain(metadata_vec))
}

fn build_aws_credential_provider_metadata(
    credential_provider_name: &str,
    configs: &HashMap<String, String>,
    bucket: &str,
) -> Result<CredentialProviderMetadata, object_store::Error> {
    match credential_provider_name {
        AWS_CONTAINER_CREDENTIALS
        | AWS_CONTAINER_CREDENTIALS_V1
        | AWS_EC2_CONTAINER_CREDENTIALS => Ok(CredentialProviderMetadata::Ecs),
        AWS_INSTANCE_PROFILE | AWS_INSTANCE_PROFILE_V1 => Ok(CredentialProviderMetadata::Imds),
        HADOOP_IAM_INSTANCE => Ok(CredentialProviderMetadata::Chain(vec![
            CredentialProviderMetadata::Ecs,
            CredentialProviderMetadata::Imds,
        ])),
        AWS_ENVIRONMENT_V1 | AWS_ENVIRONMENT => Ok(CredentialProviderMetadata::Environment),
        HADOOP_SIMPLE | HADOOP_TEMPORARY => {
            build_static_credential_provider_metadata(credential_provider_name, configs, bucket)
        }
        HADOOP_ASSUMED_ROLE => build_assume_role_credential_provider_metadata(configs, bucket),
        AWS_WEB_IDENTITY_V1 | AWS_WEB_IDENTITY => Ok(CredentialProviderMetadata::WebIdentity),
        // Only Hadoop's own provider reads the profile keys. Hadoop builds the SDK spellings
        // through the SDK's static constructor without its configuration, so applying the keys
        // to them here would authenticate the native side as a different identity.
        // With no configured file, Hadoop reads AWS_SHARED_CREDENTIALS_FILE or the JVM
        // user's ~/.aws/credentials; the JVM forwards that resolved path so both sides agree
        // even when the native process sees a different HOME.
        HADOOP_PROFILE => Ok(CredentialProviderMetadata::Profile {
            name: get_non_empty_config(configs, bucket, "auth.profile.name"),
            file: get_non_empty_config(configs, bucket, "auth.profile.file")
                .or_else(|| get_non_empty_config(configs, bucket, "comet.default.profile.file")),
            credentials_only: true,
        }),
        AWS_PROFILE_V1 | AWS_PROFILE => Ok(CredentialProviderMetadata::Profile {
            name: None,
            file: None,
            credentials_only: false,
        }),
        _ => Err(object_store::Error::Generic {
            store: "S3",
            source: format!("Unsupported credential provider: {credential_provider_name}").into(),
        }),
    }
}

fn build_static_credential_provider_metadata(
    credential_provider_name: &str,
    configs: &HashMap<String, String>,
    bucket: &str,
) -> Result<CredentialProviderMetadata, object_store::Error> {
    let access_key_id = get_config_trimmed(configs, bucket, "access.key");
    let secret_access_key = get_config_trimmed(configs, bucket, "secret.key");
    let session_token = if credential_provider_name == HADOOP_TEMPORARY {
        get_config_trimmed(configs, bucket, "session.token")
    } else {
        None
    };

    // Allow static credential provider creation even when access/secret keys are missing.
    // This maintains compatibility with Hadoop S3A FileSystem, whose default credential chain
    // includes TemporaryAWSCredentialsProvider. Missing credentials won't prevent other
    // providers in the chain from working - this provider will error only when accessed.
    let mut is_valid = access_key_id.is_some() && secret_access_key.is_some();
    if credential_provider_name == HADOOP_TEMPORARY {
        is_valid = is_valid && session_token.is_some();
    };

    Ok(CredentialProviderMetadata::Static {
        is_valid,
        access_key: access_key_id.unwrap_or("").to_string(),
        secret_key: secret_access_key.unwrap_or("").to_string(),
        session_token: session_token.map(|s| s.to_string()),
    })
}

fn build_assume_role_credential_provider_metadata(
    configs: &HashMap<String, String>,
    bucket: &str,
) -> Result<CredentialProviderMetadata, object_store::Error> {
    let base_provider_names =
        get_config_trimmed(configs, bucket, "assumed.role.credentials.provider")
            .map(|s| parse_credential_provider_names(s));
    let base_provider_names = if let Some(v) = base_provider_names {
        if v.iter().any(|name| is_anonymous_credential_provider(name)) {
            return Err(object_store::Error::Generic {
                store: "S3",
                source: "Anonymous credential provider cannot be used as assumed role credential provider".into(),
            });
        }
        v
    } else {
        // If credential provider for performing assume role operation is not specified, we'll use simple
        // credential provider first, and fallback to environment variable credential provider. This is the
        // same behavior as Hadoop S3A FileSystem.
        vec![HADOOP_SIMPLE, AWS_ENVIRONMENT]
    };

    let role_arn = get_config_trimmed(configs, bucket, "assumed.role.arn").ok_or(
        object_store::Error::Generic {
            store: "S3",
            source: "Missing required assume role ARN configuration".into(),
        },
    )?;
    let default_session_name = "comet-parquet-s3".to_string();
    let session_name = get_config_trimmed(configs, bucket, "assumed.role.session.name")
        .unwrap_or(&default_session_name);

    let base_provider_metadata =
        build_chained_aws_credential_provider_metadata(base_provider_names, configs, bucket)?;
    Ok(CredentialProviderMetadata::AssumeRole {
        role_arn: role_arn.to_string(),
        session_name: session_name.to_string(),
        base_provider_metadata: Box::new(base_provider_metadata),
    })
}

/// A caching wrapper around AWS credential providers that implements the object_store `CredentialProvider` trait.
///
/// This struct bridges AWS SDK credential providers (`ProvideCredentials`) with the object_store
/// crate's `CredentialProvider` trait, enabling seamless use of AWS credentials with object_store's
/// S3 implementation. It also provides credential caching to improve performance and reduce the
/// frequency of credential refresh operations. Many AWS credential providers (like IMDS, ECS, STS
/// assume role) involve network calls or complex authentication flows that can be expensive to
/// repeat constantly.
#[derive(Debug)]
struct CachedAwsCredentialProvider {
    /// The underlying AWS credential provider that this cache wraps.
    /// This can be any provider implementing `ProvideCredentials` (static, IMDS, ECS, assume role, etc.)
    provider: Arc<dyn ProvideCredentials>,

    /// Cache holding the most recently fetched credentials. [CredentialProvider] is required to be
    /// Send + Sync, so we have to use Arc + RwLock to make it thread-safe.
    cached: Arc<RwLock<Option<aws_credential_types::Credentials>>>,

    /// Time buffer before credential expiry when refresh should be triggered.
    /// For example, if set to 5 minutes, credentials will be refreshed when they have
    /// 5 minutes or less remaining before expiration. This prevents credential expiry
    /// during active operations.
    min_ttl: Duration,

    /// The metadata of the credential provider. Only present when running tests. This field is used
    /// to assert on the structure of the credential provider.
    #[cfg(test)]
    metadata: CredentialProviderMetadata,
}

impl CachedAwsCredentialProvider {
    #[allow(unused_variables)]
    fn new(
        credential_provider: Arc<dyn ProvideCredentials>,
        metadata: CredentialProviderMetadata,
        min_ttl: Duration,
    ) -> Self {
        Self {
            provider: credential_provider,
            cached: Arc::new(RwLock::new(None)),
            min_ttl,
            #[cfg(test)]
            metadata,
        }
    }

    #[cfg(test)]
    fn metadata(&self) -> CredentialProviderMetadata {
        self.metadata.clone()
    }

    fn fetch_credential(&self) -> Option<aws_credential_types::Credentials> {
        let locked = self.cached.read().unwrap();
        locked.as_ref().and_then(|cred| match cred.expiry() {
            Some(expiry) => {
                if expiry < SystemTime::now() + self.min_ttl {
                    None
                } else {
                    Some(cred.clone())
                }
            }
            None => Some(cred.clone()),
        })
    }

    async fn refresh_credential(&self) -> object_store::Result<aws_credential_types::Credentials> {
        let credentials = self.provider.provide_credentials().await.map_err(|e| {
            error!("Failed to retrieve credentials: {e:?}");
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        *self.cached.write().unwrap() = Some(credentials.clone());
        Ok(credentials)
    }
}

#[async_trait]
impl CredentialProvider for CachedAwsCredentialProvider {
    /// The type of credential returned by this provider
    type Credential = AwsCredential;

    /// Return a credential
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let credentials = match self.fetch_credential() {
            Some(cred) => cred,
            None => self.refresh_credential().await?,
        };
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(|s| s.to_string()),
        }))
    }
}

/// A custom AWS credential provider that holds static, pre-configured credentials.
///
/// This provider is used when the S3 credential configuration specifies static access keys,
/// such as when using Hadoop's `SimpleAWSCredentialsProvider` or `TemporaryAWSCredentialsProvider`.
/// Unlike dynamic credential providers (like IMDS or ECS), this provider returns the same
/// credentials every time without any external API calls.
#[derive(Debug)]
struct StaticCredentialProvider {
    is_valid: bool,
    cred: Credentials,
}

impl StaticCredentialProvider {
    fn new(is_valid: bool, ak: String, sk: String, token: Option<String>) -> Self {
        let mut builder = Credentials::builder()
            .access_key_id(ak)
            .secret_access_key(sk)
            .provider_name("AwsStaticCredentialProvider");
        if let Some(token) = token {
            builder = builder.session_token(token);
        }
        let cred = builder.build();
        Self { is_valid, cred }
    }
}

impl ProvideCredentials for StaticCredentialProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        if self.is_valid {
            aws_credential_types::provider::future::ProvideCredentials::ready(Ok(self.cred.clone()))
        } else {
            aws_credential_types::provider::future::ProvideCredentials::ready(Err(
                CredentialsError::not_loaded_no_source(),
            ))
        }
    }
}

/// Structural representation of credential provider types. It reflects the nested structure of the
/// credential providers, and can be used as blueprint to creating the actual credential providers.
/// We are defining this type because it is hard to assert on the structures of credential providers
/// using the `dyn ProvideCredentials` values directly. Please refer to the test cases for usages of
/// this type.
#[derive(Debug, Clone, PartialEq)]
enum CredentialProviderMetadata {
    Default,
    Ecs,
    Imds,
    Environment,
    WebIdentity,
    Profile {
        name: Option<String>,
        file: Option<String>,
        // Hadoop's ProfileAWSCredentialsProvider reads only the credentials file, while the
        // SDK spellings merge the SDK's config and credentials files.
        credentials_only: bool,
    },
    Static {
        is_valid: bool,
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
    AssumeRole {
        role_arn: String,
        session_name: String,
        base_provider_metadata: Box<CredentialProviderMetadata>,
    },
    Chain(Vec<CredentialProviderMetadata>),
}

impl CredentialProviderMetadata {
    fn name(&self) -> &'static str {
        match self {
            CredentialProviderMetadata::Default => "Default",
            CredentialProviderMetadata::Ecs => "Ecs",
            CredentialProviderMetadata::Imds => "Imds",
            CredentialProviderMetadata::Environment => "Environment",
            CredentialProviderMetadata::WebIdentity => "WebIdentity",
            CredentialProviderMetadata::Profile { .. } => "Profile",
            CredentialProviderMetadata::Static { .. } => "Static",
            CredentialProviderMetadata::AssumeRole { .. } => "AssumeRole",
            CredentialProviderMetadata::Chain(..) => "Chain",
        }
    }

    /// Return a simple name for the credential provider. Security sensitive informations are not included.
    /// This is useful for logging and debugging.
    fn simple_string(&self) -> String {
        match self {
            CredentialProviderMetadata::Default => "Default".to_string(),
            CredentialProviderMetadata::Ecs => "Ecs".to_string(),
            CredentialProviderMetadata::Imds => "Imds".to_string(),
            CredentialProviderMetadata::Environment => "Environment".to_string(),
            CredentialProviderMetadata::WebIdentity => "WebIdentity".to_string(),
            CredentialProviderMetadata::Profile { name, file, .. } => {
                let overrides: Vec<String> = [("name", name), ("file", file)]
                    .into_iter()
                    .filter_map(|(key, value)| value.as_ref().map(|v| format!("{key}: {v}")))
                    .collect();
                if overrides.is_empty() {
                    "Profile".to_string()
                } else {
                    format!("Profile({})", overrides.join(", "))
                }
            }
            CredentialProviderMetadata::Static { is_valid, .. } => {
                format!("Static(valid: {is_valid})")
            }
            CredentialProviderMetadata::AssumeRole {
                role_arn,
                session_name,
                base_provider_metadata,
            } => {
                format!(
                    "AssumeRole(role: {}, session: {}, base: {})",
                    role_arn,
                    session_name,
                    base_provider_metadata.simple_string()
                )
            }
            CredentialProviderMetadata::Chain(providers) => {
                let provider_strings: Vec<String> =
                    providers.iter().map(|p| p.simple_string()).collect();
                format!("Chain({})", provider_strings.join(" -> "))
            }
        }
    }
}

impl CredentialProviderMetadata {
    /// Create a credential provider from the metadata.
    ///
    /// Note: this function is not covered by tests. However, the implementation of this function is
    /// quite straightforward and should be easy to verify.
    async fn create_credential_provider(
        &self,
    ) -> Result<Arc<dyn ProvideCredentials>, object_store::Error> {
        match self {
            CredentialProviderMetadata::Default => {
                let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
                let credential_provider =
                    config
                        .credentials_provider()
                        .ok_or(object_store::Error::Generic {
                            store: "S3",
                            source: "Cannot get default credential provider chain".into(),
                        })?;
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::Ecs => {
                let credential_provider = EcsCredentialsProvider::builder().build();
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::Imds => {
                let credential_provider = ImdsCredentialsProvider::builder().build();
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::Environment => {
                let credential_provider = EnvironmentVariableCredentialsProvider::new();
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::WebIdentity => {
                let credential_provider = WebIdentityTokenCredentialsProvider::builder()
                    .configure(&ProviderConfig::with_default_region().await)
                    .build();
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::Profile {
                name,
                file,
                credentials_only,
            } => Ok(build_profile_provider(
                ProviderConfig::without_region(),
                &DefaultRegionChain::builder().build(),
                aws_config::defaults(BehaviorVersion::latest()),
                name.as_deref(),
                file.as_deref(),
                *credentials_only,
            )
            .await),
            CredentialProviderMetadata::Static {
                is_valid,
                access_key,
                secret_key,
                session_token,
            } => {
                let credential_provider = StaticCredentialProvider::new(
                    *is_valid,
                    access_key.clone(),
                    secret_key.clone(),
                    session_token.clone(),
                );
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::AssumeRole {
                role_arn,
                session_name,
                base_provider_metadata,
            } => {
                let base_provider =
                    Box::pin(base_provider_metadata.create_credential_provider()).await?;
                let credential_provider = AssumeRoleProvider::builder(role_arn)
                    .session_name(session_name)
                    .build_from_provider(base_provider)
                    .await;
                Ok(Arc::new(credential_provider))
            }
            CredentialProviderMetadata::Chain(metadata_vec) => {
                if metadata_vec.is_empty() {
                    return Err(object_store::Error::Generic {
                        store: "S3",
                        source: "Cannot create credential provider chain with empty providers"
                            .into(),
                    });
                }
                let mut chained_provider = CredentialsProviderChain::first_try(
                    metadata_vec[0].name(),
                    Box::pin(metadata_vec[0].create_credential_provider()).await?,
                );
                for metadata in metadata_vec[1..].iter() {
                    chained_provider = chained_provider.or_else(
                        metadata.name(),
                        Box::pin(metadata.create_credential_provider()).await?,
                    );
                }
                Ok(Arc::new(chained_provider))
            }
        }
    }
}

/// The STS region the SDK profile provider takes, on its regional host, when no region is found.
const STS_FALLBACK_REGION: &str = "us-east-1";

/// The region whose STS endpoint is the global https://sts.amazonaws.com, signed for us-east-1,
/// where the Java SDK sends a role profile's request when no region is found.
const STS_GLOBAL_REGION: &str = "aws-global";

/// Profile properties that make a profile resolve credentials other than its static keys.
const CREDENTIAL_PROPERTIES: [&str; 10] = [
    "role_arn",
    "credential_source",
    "web_identity_token_file",
    "credential_process",
    "login_session",
    "sso_session",
    "sso_account_id",
    "sso_region",
    "sso_role_name",
    "sso_start_url",
];

/// A role a profile assumes from its `source_profile`.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
struct ProfileRole {
    role_arn: String,
    external_id: Option<String>,
    session_name: Option<String>,
    region: Option<String>,
}

/// A role profile's chain: the profile whose credentials start it and the roles assumed from
/// them, outermost first.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
struct ProfileRoleChain {
    base: String,
    /// Whether the base is a web identity role, the only base that calls STS.
    base_needs_region: bool,
    roles: Vec<ProfileRole>,
}

fn has_only_static_keys(profile: &Profile) -> bool {
    profile.get("aws_access_key_id").is_some()
        && !CREDENTIAL_PROPERTIES
            .iter()
            .any(|property| profile.get(property).is_some())
}

/// Follows `role_arn` and `source_profile` from `selected` the way the SDK's profile provider
/// does (aws-config's profile/credentials/repr.rs). That provider assumes every role with one
/// STS region and offers no per-role endpoint, while Hadoop's Java SDK gives each role its own,
/// so the roles are assumed here instead. A chain this does not mirror returns the reason, to
/// stay on the SDK provider.
fn resolve_role_chain(profiles: &ProfileSet, selected: &str) -> Result<ProfileRoleChain, String> {
    let mut name = selected;
    let mut visited = Vec::new();
    let mut roles = Vec::new();
    loop {
        let profile = profiles
            .get_profile(name)
            .ok_or_else(|| format!("profile {name} is not defined"))?;
        if visited.contains(&name) {
            return Err(format!("profile {name} is in a source_profile cycle"));
        }
        visited.push(name);
        // The SDK takes a source profile's static keys ahead of its other settings, which the
        // base provider, reading the profile as its selected one, would not.
        if visited.len() > 1
            && profile.get("aws_access_key_id").is_some()
            && !has_only_static_keys(profile)
        {
            return Err(format!(
                "source profile {name} mixes keys with other credentials"
            ));
        }
        // A web identity role is the SDK's own base provider.
        let role_arn = profile
            .get("role_arn")
            .filter(|_| profile.get("web_identity_token_file").is_none());
        let Some(role_arn) = role_arn else {
            return Ok(ProfileRoleChain {
                base: name.to_string(),
                base_needs_region: profile.get("role_arn").is_some()
                    && profile.get("web_identity_token_file").is_some(),
                roles,
            });
        };
        match (
            profile.get("source_profile"),
            profile.get("credential_source"),
        ) {
            (Some(source), None) if source != name => {
                roles.push(ProfileRole {
                    role_arn: role_arn.to_string(),
                    external_id: profile.get("external_id").map(str::to_string),
                    session_name: profile.get("role_session_name").map(str::to_string),
                    region: profile.get("region").map(str::to_string),
                });
                name = source;
            }
            (_, Some(_)) => return Err(format!("profile {name} uses credential_source")),
            _ => return Err(format!("profile {name} has no other source_profile")),
        }
    }
}

/// The default chain's region, asked for at most once because the chain can probe IMDS.
async fn default_chain_region(
    default_region: &impl ProvideRegion,
    resolved: &mut Option<Option<Region>>,
) -> Option<Region> {
    if resolved.is_none() {
        *resolved = Some(default_region.region().await);
    }
    resolved.clone().flatten()
}

/// One role of a [`RoleChainProvider`], with the STS configuration for its region.
#[derive(Debug)]
struct RoleHop {
    role_arn: String,
    external_id: Option<String>,
    session_name: String,
    sts_config: SdkConfig,
}

/// Assumes each role in turn with the credentials of the one before it, starting from `base`,
/// the way the SDK's profile provider runs its chain (aws-config's profile/credentials/exec.rs),
/// so the stack does not grow with the chain's length.
#[derive(Debug)]
struct RoleChainProvider {
    base: ProfileFileCredentialsProvider,
    /// Innermost first.
    hops: Vec<RoleHop>,
}

impl RoleChainProvider {
    async fn credentials(&self) -> Result<Credentials, CredentialsError> {
        let mut credentials = self
            .base
            .provide_credentials()
            .await
            .map_err(CredentialsError::provider_error)?;
        for hop in &self.hops {
            let config = hop
                .sts_config
                .to_builder()
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build();
            let output = aws_sdk_sts::Client::new(&config)
                .assume_role()
                .role_arn(&hop.role_arn)
                .set_external_id(hop.external_id.clone())
                .role_session_name(&hop.session_name)
                .send()
                .await
                .map_err(CredentialsError::provider_error)?;
            let assumed = output.credentials().ok_or_else(|| {
                CredentialsError::provider_error("STS AssumeRole response had no credentials")
            })?;
            let expiry = SystemTime::try_from(*assumed.expiration()).map_err(|_| {
                CredentialsError::provider_error("STS credential expiry is out of range")
            })?;
            credentials = Credentials::new(
                assumed.access_key_id(),
                assumed.secret_access_key(),
                Some(assumed.session_token().to_string()),
                Some(expiry),
                "CometProfileRoleChain",
            );
        }
        Ok(credentials)
    }
}

impl ProvideCredentials for RoleChainProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(self.credentials())
    }
}

/// Builds a provider that assumes `chain`'s roles innermost first, each with its own STS client
/// in the role's region, else the default chain's, else the global endpoint signed for us-east-1,
/// as Hadoop's Java SDK does. The base keeps the SDK profile provider; a web identity base ignores
/// its profile region, as Java's StsWebIdentityCredentialsProvider does, and calls STS in the
/// default chain's region, else the global endpoint. Regions are resolved here, once.
async fn assume_profile_roles(
    chain: ProfileRoleChain,
    provider_config: ProviderConfig,
    default_region: &impl ProvideRegion,
    sts_config: ConfigLoader,
    profile_files: EnvConfigFiles,
) -> Arc<dyn ProvideCredentials> {
    let mut resolved = None;
    let base_region = if chain.base_needs_region {
        let region = default_chain_region(default_region, &mut resolved).await;
        Some(region.unwrap_or_else(|| Region::from_static(STS_GLOBAL_REGION)))
    } else {
        None
    };
    let base = ProfileFileCredentialsProvider::builder()
        .configure(&provider_config.with_region(base_region))
        .profile_name(chain.base)
        .profile_files(profile_files)
        .build();
    if chain.roles.is_empty() {
        return Arc::new(base);
    }
    // Hop settings other than the region come from the default config sources, as for Java's
    // StsClient.builder(). A hop with no region uses `aws-global`: sts.amazonaws.com signed for
    // us-east-1, unless FIPS, dual-stack or a configured endpoint URL apply.
    let sts_config = sts_config
        // Each hop sets its own region. Without one here, load() would resolve the default
        // region chain (which can probe IMDS) for a value every hop replaces.
        .region(Region::from_static(STS_GLOBAL_REGION))
        .no_credentials()
        .identity_cache(IdentityCache::no_cache())
        .load()
        .await;
    let mut hops = Vec::with_capacity(chain.roles.len());
    for role in chain.roles.into_iter().rev() {
        let region = match role.region {
            Some(region) => Some(Region::new(region)),
            None => default_chain_region(default_region, &mut resolved).await,
        };
        let region = region.unwrap_or_else(|| Region::from_static(STS_GLOBAL_REGION));
        // The SDK profile provider's default session name.
        let session_name = role.session_name.unwrap_or_else(|| {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            format!("assume-role-from-profile-{}", now.as_millis())
        });
        hops.push(RoleHop {
            role_arn: role.role_arn,
            external_id: role.external_id,
            session_name,
            sts_config: sts_config.to_builder().region(region).build(),
        });
    }
    Arc::new(RoleChainProvider { base, hops })
}

/// The role chain provider for a credentials-file profile whose chain [`resolve_role_chain`]
/// mirrors, or None, after logging why not, to stay on the SDK profile provider.
async fn role_chain_provider(
    provider_config: &ProviderConfig,
    default_region: &impl ProvideRegion,
    sts_config: ConfigLoader,
    name: Option<&str>,
    files: &EnvConfigFiles,
) -> Option<Arc<dyn ProvideCredentials>> {
    // The role chain is read once, when the provider is built, from the real filesystem and
    // environment, as the SDK profile provider reads them.
    let profiles = aws_config::profile::load(
        &Default::default(),
        &Default::default(),
        files,
        name.map(|name| name.to_string().into()),
    )
    .await;
    let chain = match profiles {
        Ok(profiles) => resolve_role_chain(&profiles, profiles.selected_profile()),
        Err(_) => Err("the credentials file did not load".to_string()),
    };
    match chain {
        Ok(chain) => Some(
            assume_profile_roles(
                chain,
                provider_config.clone(),
                default_region,
                sts_config,
                files.clone(),
            )
            .await,
        ),
        // The SDK provider reports any error when credentials are first requested.
        Err(reason) => {
            debug!("Profile credentials use the SDK profile provider: {reason}");
            None
        }
    }
}

/// The STS region of the SDK profile provider.
async fn sdk_profile_region(
    provider_config: &ProviderConfig,
    default_region: &impl ProvideRegion,
    name: Option<&str>,
    profile_files: Option<&EnvConfigFiles>,
    credentials_only: bool,
) -> Option<Region> {
    if !credentials_only {
        return default_region.region().await;
    }
    // The SDK provider covers only the chains the walk does not take. It keeps a single STS
    // region: the profile's own, else its `source_profile` chain's, else the default chain's,
    // else us-east-1 on the regional host. A file that fails to load yields no region here
    // and surfaces from the credentials provider.
    let mut region_provider = ProfileFileRegionProvider::builder().configure(provider_config);
    if let Some(name) = name {
        region_provider = region_provider.profile_name(name);
    }
    if let Some(files) = profile_files {
        region_provider = region_provider.profile_files(files.clone());
    }
    // The default chain can probe IMDS, so it runs only when the profile has no region.
    let region = match ProvideRegion::region(&region_provider.build()).await {
        Some(region) => region,
        None => default_region
            .region()
            .await
            .unwrap_or_else(|| Region::from_static(STS_FALLBACK_REGION)),
    };
    Some(region)
}

/// Builds the profile credentials provider on `provider_config`, with `default_region` standing
/// in for the SDK's default region chain and `sts_config` for the base of each STS client's
/// configuration.
async fn build_profile_provider(
    provider_config: ProviderConfig,
    default_region: &impl ProvideRegion,
    sts_config: ConfigLoader,
    name: Option<&str>,
    file: Option<&str>,
    credentials_only: bool,
) -> Arc<dyn ProvideCredentials> {
    // Hadoop's ProfileAWSCredentialsProvider loads the configured file, or the shared
    // credentials file, as a credentials-format file and reads nothing else, so a same-name
    // role profile in the SDK's config file never applies.
    let credentials_file = match (file, credentials_only) {
        (Some(file), _) => Some(file.to_string()),
        (None, true) => Some(default_shared_credentials_file(
            std::env::var("AWS_SHARED_CREDENTIALS_FILE").ok(),
            std::env::var("HOME").ok(),
        )),
        (None, false) => None,
    };
    let profile_files = credentials_file.map(|file| {
        EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Credentials, file)
            .build()
    });
    if let (true, Some(files)) = (credentials_only, &profile_files) {
        let provider =
            role_chain_provider(&provider_config, default_region, sts_config, name, files).await;
        if let Some(provider) = provider {
            return provider;
        }
    }
    let region = sdk_profile_region(
        &provider_config,
        default_region,
        name,
        profile_files.as_ref(),
        credentials_only,
    )
    .await;
    let provider_config = provider_config.with_region(region);
    let mut builder = ProfileFileCredentialsProvider::builder().configure(&provider_config);
    if let Some(name) = name {
        builder = builder.profile_name(name);
    }
    if let Some(files) = profile_files {
        builder = builder.profile_files(files);
    }
    Arc::new(builder.build())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};

    use super::*;
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_types::body::SdkBody;

    /// Test configuration builder for easier setup Hadoop configurations
    #[derive(Debug, Default)]
    struct TestConfigBuilder {
        configs: HashMap<String, String>,
    }

    impl TestConfigBuilder {
        fn new() -> Self {
            Self::default()
        }

        fn with_region(mut self, region: &str) -> Self {
            self.configs
                .insert("fs.s3a.endpoint.region".to_string(), region.to_string());
            self
        }

        fn with_credential_provider(mut self, provider: &str) -> Self {
            self.configs.insert(
                "fs.s3a.aws.credentials.provider".to_string(),
                provider.to_string(),
            );
            self
        }

        fn with_bucket_credential_provider(mut self, bucket: &str, provider: &str) -> Self {
            self.configs.insert(
                format!("fs.s3a.bucket.{bucket}.aws.credentials.provider"),
                provider.to_string(),
            );
            self
        }

        fn with_access_key(mut self, key: &str) -> Self {
            self.configs
                .insert("fs.s3a.access.key".to_string(), key.to_string());
            self
        }

        fn with_secret_key(mut self, key: &str) -> Self {
            self.configs
                .insert("fs.s3a.secret.key".to_string(), key.to_string());
            self
        }

        fn with_session_token(mut self, token: &str) -> Self {
            self.configs
                .insert("fs.s3a.session.token".to_string(), token.to_string());
            self
        }

        fn with_bucket_access_key(mut self, bucket: &str, key: &str) -> Self {
            self.configs.insert(
                format!("fs.s3a.bucket.{bucket}.access.key"),
                key.to_string(),
            );
            self
        }

        fn with_bucket_secret_key(mut self, bucket: &str, key: &str) -> Self {
            self.configs.insert(
                format!("fs.s3a.bucket.{bucket}.secret.key"),
                key.to_string(),
            );
            self
        }

        fn with_bucket_session_token(mut self, bucket: &str, token: &str) -> Self {
            self.configs.insert(
                format!("fs.s3a.bucket.{bucket}.session.token"),
                token.to_string(),
            );
            self
        }

        fn with_assume_role_arn(mut self, arn: &str) -> Self {
            self.configs
                .insert("fs.s3a.assumed.role.arn".to_string(), arn.to_string());
            self
        }

        fn with_assume_role_session_name(mut self, name: &str) -> Self {
            self.configs.insert(
                "fs.s3a.assumed.role.session.name".to_string(),
                name.to_string(),
            );
            self
        }

        fn with_assume_role_credentials_provider(mut self, provider: &str) -> Self {
            self.configs.insert(
                "fs.s3a.assumed.role.credentials.provider".to_string(),
                provider.to_string(),
            );
            self
        }

        fn with_property(mut self, property: &str, value: &str) -> Self {
            self.configs
                .insert(format!("fs.s3a.{property}"), value.to_string());
            self
        }

        fn with_bucket_property(mut self, bucket: &str, property: &str, value: &str) -> Self {
            self.configs.insert(
                format!("fs.s3a.bucket.{bucket}.{property}"),
                value.to_string(),
            );
            self
        }

        fn build(self) -> HashMap<String, String> {
            self.configs
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AWS credential providers and object_store call foreign functions
    fn test_create_store() {
        let url = Url::parse("s3a://test_bucket/comet/spark-warehouse/part-00000.snappy.parquet")
            .unwrap();
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ANONYMOUS)
            .with_region("us-east-1")
            .build();
        let (_object_store, path) = create_store(&url, &configs, Duration::from_secs(300)).unwrap();
        assert_eq!(
            path,
            Path::from("/comet/spark-warehouse/part-00000.snappy.parquet")
        );
    }

    /// A location-scoped store builds each location's store on first use, usually inside an async
    /// read on a Tokio worker, so building from a template must not block on the runtime. The
    /// template resolves the region when it is created; this bucket's region is already cached, so
    /// no request is made.
    #[test]
    fn builds_from_a_template_inside_the_runtime() {
        let bucket = "comet-template-test-bucket";
        region_cache()
            .write()
            .unwrap()
            .insert(bucket.to_string(), "us-west-2".to_string());
        let url = Url::parse(&format!("s3a://{bucket}/warehouse/sales/part-0.parquet")).unwrap();
        // With no endpoint or region configured, creating the template resolves the region.
        let template = S3StoreTemplate::new(&url, &HashMap::new(), bucket).unwrap();
        assert_eq!(template.region.as_deref(), Some("us-west-2"));

        let store = get_runtime().block_on(async { template.build(S3Credentials::SkipSignature) });
        assert!(store.is_ok(), "{:?}", store.err());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AWS credential providers and object_store call foreign functions
    fn test_create_store_with_custom_endpoint() {
        // object_store must accept the flag and endpoint pair in both addressing modes, and
        // create_store enables allow_http so an http endpoint is usable
        let url = Url::parse("s3a://test-bucket/comet/data.parquet").unwrap();
        for path_style_access in ["false", "true"] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(HADOOP_ANONYMOUS)
                .with_region("us-east-1")
                .with_property("endpoint", "http://minio.internal:9000")
                .with_property("path.style.access", path_style_access)
                .build();
            let (_object_store, path) =
                create_store(&url, &configs, Duration::from_secs(300)).unwrap();
            assert_eq!(path, Path::from("/comet/data.parquet"));
        }

        // An IP-literal endpoint must build without path.style.access being set
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ANONYMOUS)
            .with_region("us-east-1")
            .with_property("endpoint", "http://127.0.0.1:9000")
            .build();
        let (_object_store, path) = create_store(&url, &configs, Duration::from_secs(300)).unwrap();
        assert_eq!(path, Path::from("/comet/data.parquet"));
    }

    #[test]
    fn test_get_config_trimmed() {
        let configs = TestConfigBuilder::new()
            .with_access_key("test_key")
            .with_secret_key("  \n  test_secret_key\n  \n")
            .with_session_token("  \n  test_session_token\n  \n")
            .with_bucket_access_key("test-bucket", "test_bucket_key")
            .with_bucket_secret_key("test-bucket", "  \n  test_bucket_secret_key\n  \n")
            .with_bucket_session_token("test-bucket", "  \n  test_bucket_session_token\n  \n")
            .build();

        // bucket-specific keys
        let access_key = get_config_trimmed(&configs, "test-bucket", "access.key");
        assert_eq!(access_key, Some("test_bucket_key"));
        let secret_key = get_config_trimmed(&configs, "test-bucket", "secret.key");
        assert_eq!(secret_key, Some("test_bucket_secret_key"));
        let session_token = get_config_trimmed(&configs, "test-bucket", "session.token");
        assert_eq!(session_token, Some("test_bucket_session_token"));

        // global keys
        let access_key = get_config_trimmed(&configs, "test-bucket-2", "access.key");
        assert_eq!(access_key, Some("test_key"));
        let secret_key = get_config_trimmed(&configs, "test-bucket-2", "secret.key");
        assert_eq!(secret_key, Some("test_secret_key"));
        let session_token = get_config_trimmed(&configs, "test-bucket-2", "session.token");
        assert_eq!(session_token, Some("test_session_token"));
    }

    #[test]
    fn test_forward_catalog_properties_forwards_fs_s3a_subset() {
        let mut configs: HashMap<String, String> = HashMap::new();
        configs.insert(
            "fs.s3a.aws.credentials.provider".to_string(),
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain".to_string(),
        );
        configs.insert("fs.s3a.endpoint".to_string(), "s3.example.com".to_string());
        // The activation key itself must survive forwarding.
        configs.insert(
            format!("fs.s3a.{PROVIDER_CLASS_PROPERTY}"),
            "org.apache.comet.cloud.s3.HadoopS3ACredentialProviderAdapter".to_string(),
        );
        // Static keys are forwarded too: a Hadoop provider chain may resolve through them, and
        // stripping them would change which principal wins vs Spark.
        configs.insert("fs.s3a.access.key".to_string(), "AK".to_string());
        configs.insert("fs.s3a.secret.key".to_string(), "SK".to_string());
        configs.insert("fs.s3a.session.token".to_string(), "ST".to_string());
        configs.insert(
            "fs.s3a.bucket.b.secret.key".to_string(),
            "bucket-secret".to_string(),
        );
        // A non-fs.s3a key that can actually reach this bag (extractObjectStoreOptions also passes
        // the fs.comet.* scheme keys) is dropped: the adapters read fs.s3a.* only.
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            "blob".to_string(),
        );

        let forwarded = forward_catalog_properties(&configs);

        assert!(forwarded.contains_key("fs.s3a.aws.credentials.provider"));
        assert!(forwarded.contains_key("fs.s3a.endpoint"));
        assert!(forwarded.contains_key(&format!("fs.s3a.{PROVIDER_CLASS_PROPERTY}")));
        assert!(forwarded.contains_key("fs.s3a.access.key"));
        assert!(forwarded.contains_key("fs.s3a.secret.key"));
        assert!(forwarded.contains_key("fs.s3a.session.token"));
        assert!(forwarded.contains_key("fs.s3a.bucket.b.secret.key"));
        assert!(!forwarded.contains_key("fs.comet.s3Compliant.schemes"));
    }

    #[test]
    fn test_empty_per_bucket_provider_class_opts_out() {
        let adapter = "org.apache.comet.cloud.s3.HadoopS3ACredentialProviderAdapter";
        let mut configs: HashMap<String, String> = HashMap::new();
        configs.insert(
            format!("fs.s3a.{PROVIDER_CLASS_PROPERTY}"),
            adapter.to_string(),
        );

        // A bucket with no per-bucket override uses the globally configured adapter.
        assert_eq!(
            lookup_provider_class(&configs, "other-bucket"),
            Some(adapter)
        );

        // An empty per-bucket value opts that bucket out, even though the global adapter is set, so
        // the native reader resolves it directly (this is the documented anonymous opt-out).
        configs.insert(
            format!("fs.s3a.bucket.public-data.{PROVIDER_CLASS_PROPERTY}"),
            "".to_string(),
        );
        assert_eq!(lookup_provider_class(&configs, "public-data"), None);
    }

    #[test]
    fn test_parse_credential_provider_names() {
        let credential_provider_names = parse_credential_provider_names("");
        assert!(credential_provider_names.is_empty());

        let credential_provider_names = parse_credential_provider_names(HADOOP_ANONYMOUS);
        assert_eq!(credential_provider_names, vec![HADOOP_ANONYMOUS]);

        let aws_credential_provider_names =
            format!("{HADOOP_ANONYMOUS},{AWS_ENVIRONMENT},{AWS_ENVIRONMENT_V1}");
        let credential_provider_names =
            parse_credential_provider_names(&aws_credential_provider_names);
        assert_eq!(
            credential_provider_names,
            vec![HADOOP_ANONYMOUS, AWS_ENVIRONMENT, AWS_ENVIRONMENT_V1]
        );

        let aws_credential_provider_names =
            format!(" {HADOOP_ANONYMOUS}, {AWS_ENVIRONMENT},, {AWS_ENVIRONMENT_V1},");
        let credential_provider_names =
            parse_credential_provider_names(&aws_credential_provider_names);
        assert_eq!(
            credential_provider_names,
            vec![HADOOP_ANONYMOUS, AWS_ENVIRONMENT, AWS_ENVIRONMENT_V1]
        );

        let aws_credential_provider_names = format!(
            "\n  {HADOOP_ANONYMOUS},\n  {AWS_ENVIRONMENT},\n  , \n  {AWS_ENVIRONMENT_V1},\n"
        );
        let credential_provider_names =
            parse_credential_provider_names(&aws_credential_provider_names);
        assert_eq!(
            credential_provider_names,
            vec![HADOOP_ANONYMOUS, AWS_ENVIRONMENT, AWS_ENVIRONMENT_V1]
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_default_credential_provider() {
        let configs0 = TestConfigBuilder::new().build();
        let configs1 = TestConfigBuilder::new()
            .with_credential_provider("")
            .build();
        let configs2 = TestConfigBuilder::new()
            .with_credential_provider("\n  ,")
            .build();

        for configs in [configs0, configs1, configs2] {
            let result =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap();
            assert!(
                result.is_some(),
                "Should return a credential provider for default config"
            );
            assert_eq!(
                result.unwrap().metadata(),
                CredentialProviderMetadata::Default
            );
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_anonymous_credential_provider() {
        for provider_name in [HADOOP_ANONYMOUS, AWS_ANONYMOUS, AWS_ANONYMOUS_V1] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(provider_name)
                .build();

            let result =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap();
            assert!(result.is_none(), "Anonymous provider should return None");
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_mixed_anonymous_and_other_providers_error() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(&format!("{HADOOP_ANONYMOUS},{AWS_ENVIRONMENT}"))
            .build();

        let result =
            build_credential_provider(&configs, "test-bucket", Duration::from_secs(300)).await;
        assert!(
            result.is_err(),
            "Should error when mixing anonymous with other providers"
        );

        if let Err(e) = result {
            assert!(e
                .to_string()
                .contains("Anonymous credential provider cannot be mixed"));
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_simple_credential_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_SIMPLE)
            .with_access_key("test_access_key")
            .with_secret_key("test_secret_key")
            .with_session_token("test_session_token")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for simple credentials"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: true,
                access_key: "test_access_key".to_string(),
                secret_key: "test_secret_key".to_string(),
                session_token: None
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_temporary_credential_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_TEMPORARY)
            .with_access_key("test_access_key")
            .with_secret_key("test_secret_key")
            .with_session_token("test_session_token")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for temporary credentials"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: true,
                access_key: "test_access_key".to_string(),
                secret_key: "test_secret_key".to_string(),
                session_token: Some("test_session_token".to_string())
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_missing_access_key() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_SIMPLE)
            .with_secret_key("test_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return an invalid credential provider when access key is missing"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: false,
                access_key: "".to_string(),
                secret_key: "test_secret_key".to_string(),
                session_token: None
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_missing_secret_key() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_SIMPLE)
            .with_access_key("test_access_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return an invalid credential provider when secret key is missing"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: false,
                access_key: "test_access_key".to_string(),
                secret_key: "".to_string(),
                session_token: None
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_missing_session_token_for_temporary() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_TEMPORARY)
            .with_access_key("test_access_key")
            .with_secret_key("test_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return an invalid credential provider when session token is missing"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: false,
                access_key: "test_access_key".to_string(),
                secret_key: "test_secret_key".to_string(),
                session_token: None
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_bucket_specific_configuration() {
        let configs = TestConfigBuilder::new()
            .with_bucket_credential_provider("specific-bucket", HADOOP_SIMPLE)
            .with_bucket_access_key("specific-bucket", "bucket_access_key")
            .with_bucket_secret_key("specific-bucket", "bucket_secret_key")
            .build();

        let result =
            build_credential_provider(&configs, "specific-bucket", Duration::from_secs(300))
                .await
                .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for bucket-specific config"
        );

        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_SIMPLE)
            .with_access_key("test_access_key")
            .with_secret_key("test_secret_key")
            .with_bucket_credential_provider("specific-bucket", HADOOP_TEMPORARY)
            .with_bucket_access_key("specific-bucket", "bucket_access_key")
            .with_bucket_secret_key("specific-bucket", "bucket_secret_key")
            .with_bucket_session_token("specific-bucket", "bucket_session_token")
            .with_bucket_credential_provider("specific-bucket-2", HADOOP_TEMPORARY)
            .with_bucket_access_key("specific-bucket-2", "bucket_access_key_2")
            .with_bucket_secret_key("specific-bucket-2", "bucket_secret_key_2")
            .with_bucket_session_token("specific-bucket-2", "bucket_session_token_2")
            .build();

        let result =
            build_credential_provider(&configs, "specific-bucket", Duration::from_secs(300))
                .await
                .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for bucket-specific config"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: true,
                access_key: "bucket_access_key".to_string(),
                secret_key: "bucket_secret_key".to_string(),
                session_token: Some("bucket_session_token".to_string())
            }
        );

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for default config"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: true,
                access_key: "test_access_key".to_string(),
                secret_key: "test_secret_key".to_string(),
                session_token: None
            }
        );

        let result =
            build_credential_provider(&configs, "specific-bucket-2", Duration::from_secs(300))
                .await
                .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for bucket-specific config"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Static {
                is_valid: true,
                access_key: "bucket_access_key_2".to_string(),
                secret_key: "bucket_secret_key_2".to_string(),
                session_token: Some("bucket_session_token_2".to_string())
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_credential_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ASSUMED_ROLE)
            .with_assume_role_arn("arn:aws:iam::123456789012:role/test-role")
            .with_assume_role_session_name("test-session")
            .with_access_key("base_access_key")
            .with_secret_key("base_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for assume role"
        );
        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/test-role".to_string(),
                session_name: "test-session".to_string(),
                base_provider_metadata: Box::new(CredentialProviderMetadata::Chain(vec![
                    CredentialProviderMetadata::Static {
                        is_valid: true,
                        access_key: "base_access_key".to_string(),
                        secret_key: "base_secret_key".to_string(),
                        session_token: None
                    },
                    CredentialProviderMetadata::Environment,
                ]))
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_missing_arn_error() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ASSUMED_ROLE)
            .with_access_key("base_access_key")
            .with_secret_key("base_secret_key")
            .build();

        let result =
            build_credential_provider(&configs, "test-bucket", Duration::from_secs(300)).await;
        assert!(
            result.is_err(),
            "Should error when assume role ARN is missing"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_unsupported_credential_provider_error() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider("unsupported.provider.Class")
            .build();

        let result =
            build_credential_provider(&configs, "test-bucket", Duration::from_secs(300)).await;
        assert!(
            result.is_err(),
            "Should error for unsupported credential provider"
        );

        if let Err(e) = result {
            assert!(e.to_string().contains("Unsupported credential provider"));
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_environment_credential_provider() {
        for provider_name in [AWS_ENVIRONMENT, AWS_ENVIRONMENT_V1] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(provider_name)
                .build();

            let result =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap();
            assert!(result.is_some(), "Should return a credential provider");

            let test_provider = result.unwrap().metadata();
            assert_eq!(test_provider, CredentialProviderMetadata::Environment);
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_ecs_credential_provider() {
        for provider_name in [
            AWS_CONTAINER_CREDENTIALS,
            AWS_CONTAINER_CREDENTIALS_V1,
            AWS_EC2_CONTAINER_CREDENTIALS,
        ] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(provider_name)
                .build();

            let result =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap();
            assert!(result.is_some(), "Should return a credential provider");

            let test_provider = result.unwrap().metadata();
            assert_eq!(test_provider, CredentialProviderMetadata::Ecs);
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_imds_credential_provider() {
        for provider_name in [AWS_INSTANCE_PROFILE, AWS_INSTANCE_PROFILE_V1] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(provider_name)
                .build();

            let result =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap();
            assert!(result.is_some(), "Should return a credential provider");

            let test_provider = result.unwrap().metadata();
            assert_eq!(test_provider, CredentialProviderMetadata::Imds);
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_web_identity_credential_provider() {
        for provider_name in [AWS_WEB_IDENTITY, AWS_WEB_IDENTITY_V1] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(provider_name)
                .build();

            let result =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap();
            assert!(result.is_some(), "Should return a credential provider");

            let test_provider = result.unwrap().metadata();
            assert_eq!(test_provider, CredentialProviderMetadata::WebIdentity);
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_profile_credential_provider() {
        // (configured name, configured file, expected name, expected file)
        let cases = [
            (None, None, None, None),
            (Some("analytics"), None, Some("analytics"), None),
            (
                None,
                Some("/etc/aws/credentials"),
                None,
                Some("/etc/aws/credentials"),
            ),
            (
                Some("analytics"),
                Some("/etc/aws/credentials"),
                Some("analytics"),
                Some("/etc/aws/credentials"),
            ),
            // Empty and blank values are treated as unset, other values are trimmed
            (Some(""), Some("   "), None, None),
            (
                Some("  analytics  "),
                Some(" /etc/aws/credentials "),
                Some("analytics"),
                Some("/etc/aws/credentials"),
            ),
        ];
        for (name, file, expected_name, expected_file) in cases {
            {
                let provider_name = HADOOP_PROFILE;
                let mut builder = TestConfigBuilder::new().with_credential_provider(provider_name);
                if let Some(name) = name {
                    builder = builder.with_property("auth.profile.name", name);
                }
                if let Some(file) = file {
                    builder = builder.with_property("auth.profile.file", file);
                }
                let configs = builder.build();

                let result =
                    build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                        .await
                        .unwrap();
                let test_provider = result
                    .expect("Should return a credential provider")
                    .metadata();
                assert_eq!(
                    test_provider,
                    CredentialProviderMetadata::Profile {
                        name: expected_name.map(str::to_string),
                        file: expected_file.map(str::to_string),
                        credentials_only: true,
                    },
                    "provider {provider_name}, name {name:?}, file {file:?}"
                );
            }
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_sdk_profile_provider_spellings_ignore_the_profile_keys() {
        // Hadoop constructs these spellings without its configuration, so the native side must
        // resolve the SDK default profile too, even when the keys are set.
        for provider_name in [AWS_PROFILE, AWS_PROFILE_V1] {
            let configs = TestConfigBuilder::new()
                .with_credential_provider(provider_name)
                .with_property("auth.profile.name", "analytics")
                .with_property("auth.profile.file", "/etc/aws/credentials")
                .build();
            let test_provider =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap()
                    .expect("Should return a credential provider")
                    .metadata();
            assert_eq!(
                test_provider,
                CredentialProviderMetadata::Profile {
                    name: None,
                    file: None,
                    credentials_only: false,
                },
                "provider {provider_name}"
            );
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_profile_credential_provider_per_bucket_override() {
        // Each key is overridden independently, so a bucket can replace just the name or just
        // the file while the other key keeps its global value
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_PROFILE)
            .with_property("auth.profile.name", "global-profile")
            .with_property("auth.profile.file", "/etc/aws/global-credentials")
            .with_bucket_property("name-bucket", "auth.profile.name", "bucket-profile")
            .with_bucket_property(
                "file-bucket",
                "auth.profile.file",
                "/etc/aws/bucket-credentials",
            )
            .build();

        let cases = [
            (
                "name-bucket",
                "bucket-profile",
                "/etc/aws/global-credentials",
            ),
            (
                "file-bucket",
                "global-profile",
                "/etc/aws/bucket-credentials",
            ),
            (
                "other-bucket",
                "global-profile",
                "/etc/aws/global-credentials",
            ),
        ];
        for (bucket, expected_name, expected_file) in cases {
            let result = build_credential_provider(&configs, bucket, Duration::from_secs(300))
                .await
                .unwrap();
            let test_provider = result
                .expect("Should return a credential provider")
                .metadata();
            assert_eq!(
                test_provider,
                CredentialProviderMetadata::Profile {
                    name: Some(expected_name.to_string()),
                    file: Some(expected_file.to_string()),
                    credentials_only: true,
                },
                "bucket {bucket}"
            );
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_profile_credential_provider_in_chain() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(&format!(
                "{AWS_ENVIRONMENT},{HADOOP_PROFILE},{AWS_INSTANCE_PROFILE}"
            ))
            .with_property("auth.profile.name", "analytics")
            .with_property("auth.profile.file", "/etc/aws/credentials")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        let test_provider = result
            .expect("Should return a credential provider")
            .metadata();
        assert_eq!(
            test_provider,
            CredentialProviderMetadata::Chain(vec![
                CredentialProviderMetadata::Environment,
                CredentialProviderMetadata::Profile {
                    name: Some("analytics".to_string()),
                    file: Some("/etc/aws/credentials".to_string()),
                    credentials_only: true,
                },
                CredentialProviderMetadata::Imds,
            ])
        );
    }

    /// A synthetic `<action>Response` carrying `key` as the access key, so a test can tell which
    /// call's credentials signed the next one.
    fn synthetic_sts_response(action: &str, key: &str) -> String {
        format!(
            concat!(
                r#"<{action}Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/">"#,
                r#"<{action}Result><AssumedRoleUser>"#,
                r#"<AssumedRoleId>synthetic-role-id:session</AssumedRoleId>"#,
                r#"<Arn>arn:aws:sts::123456789012:assumed-role/synthetic/session</Arn>"#,
                r#"</AssumedRoleUser><Credentials>"#,
                r#"<AccessKeyId>{key}</AccessKeyId>"#,
                r#"<SecretAccessKey>synthetic-role-secret</SecretAccessKey>"#,
                r#"<SessionToken>synthetic-role-token</SessionToken>"#,
                r#"<Expiration>2999-01-01T00:00:00Z</Expiration>"#,
                r#"</Credentials></{action}Result>"#,
                r#"<ResponseMetadata><RequestId>synthetic</RequestId></ResponseMetadata>"#,
                r#"</{action}Response>"#,
            ),
            action = action,
            key = key,
        )
    }

    /// One request the in-memory STS received. Unsigned requests have no signing fields.
    #[derive(Debug, Clone)]
    struct StsCall {
        host: String,
        action: String,
        signing_key: Option<String>,
        signing_region: Option<String>,
        role_arn: Option<String>,
        external_id: Option<String>,
        session_name: Option<String>,
    }

    impl StsCall {
        fn parse(request: &HttpRequest) -> Self {
            let host = Url::parse(request.uri())
                .unwrap()
                .host_str()
                .unwrap()
                .to_string();
            // SigV4 scope: `Credential=<key>/<date>/<region>/sts/aws4_request, ...`
            let scope: Vec<String> = request
                .headers()
                .get("authorization")
                .and_then(|auth| auth.split("Credential=").nth(1))
                .and_then(|credential| credential.split(',').next())
                .map(|scope| scope.split('/').map(str::to_string).collect())
                .unwrap_or_default();
            let form: HashMap<String, String> =
                url::form_urlencoded::parse(request.body().bytes().unwrap_or_default())
                    .into_owned()
                    .collect();
            Self {
                host,
                action: form.get("Action").cloned().unwrap_or_default(),
                signing_key: scope.first().cloned(),
                signing_region: scope.get(2).cloned(),
                role_arn: form.get("RoleArn").cloned(),
                external_id: form.get("ExternalId").cloned(),
                session_name: form.get("RoleSessionName").cloned(),
            }
        }

        /// `<role> at <host> signed <region> by <key>`, naming the role by its ARN's last part.
        fn summary(&self) -> String {
            let role = self.role_arn.as_deref().unwrap_or_default();
            let role = role.rsplit('/').next().unwrap_or_default();
            match (&self.signing_region, &self.signing_key) {
                (Some(region), Some(key)) => {
                    format!("{role} at {} signed {region} by {key}", self.host)
                }
                _ => format!("{role} at {} unsigned", self.host),
            }
        }
    }

    /// An in-memory STS that answers every request with synthetic credentials and records each
    /// request, so no request leaves the test.
    #[derive(Debug, Clone, Default)]
    struct RecordingSts {
        calls: Arc<std::sync::Mutex<Vec<StsCall>>>,
    }

    impl RecordingSts {
        fn calls(&self) -> Vec<StsCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl HttpConnector for RecordingSts {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let call = StsCall::parse(&request);
            // Each role's credentials carry its name, so a request shows whose keys signed it.
            let key = if call.action == "AssumeRoleWithWebIdentity" {
                "synthetic-web-key".to_string()
            } else {
                let role = call.role_arn.as_deref().unwrap_or_default();
                format!("{}-key", role.rsplit('/').next().unwrap_or_default())
            };
            let body = synthetic_sts_response(&call.action, &key);
            self.calls.lock().unwrap().push(call);
            let response = http::Response::builder()
                .status(200)
                .body(SdkBody::from(body))
                .unwrap();
            HttpConnectorFuture::ready(Ok(response.try_into().unwrap()))
        }
    }

    impl HttpClient for RecordingSts {
        fn http_connector(
            &self,
            _settings: &HttpConnectorSettings,
            _components: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    /// A fixed region that counts how often it is asked for, standing in for the default chain.
    #[derive(Debug)]
    struct CountingRegion {
        region: Option<Region>,
        calls: AtomicUsize,
    }

    impl ProvideRegion for CountingRegion {
        fn region(&self) -> aws_config::meta::region::future::ProvideRegion<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            aws_config::meta::region::future::ProvideRegion::ready(self.region.clone())
        }
    }

    /// What the profile provider did: the access key or `error: ...`, the STS requests it sent,
    /// and how often it asked the default region chain.
    struct ProfileRun {
        outcome: String,
        calls: Vec<StsCall>,
        default_chain_calls: usize,
    }

    impl ProfileRun {
        /// The outcome and the STS requests, with any error reduced to `error`.
        fn summary(&self) -> String {
            let outcome = if self.outcome.starts_with("error") {
                "error"
            } else {
                self.outcome.as_str()
            };
            let calls: Vec<String> = self.calls.iter().map(StsCall::summary).collect();
            format!(
                "{outcome} via {calls:?}, chain consulted {}",
                self.default_chain_calls
            )
        }
    }

    /// Resolves credentials through `build_profile_provider` against the in-memory STS, with
    /// `default_region` standing in for the default region chain.
    async fn resolve_profile(
        name: Option<&str>,
        file: Option<&str>,
        default_region: Option<&str>,
        credentials_only: bool,
    ) -> ProfileRun {
        let sts = RecordingSts::default();
        let provider_config = ProviderConfig::without_region().with_http_client(sts.clone());
        let default_chain = CountingRegion {
            region: default_region.map(|region| Region::new(region.to_string())),
            calls: AtomicUsize::new(0),
        };
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .empty_test_environment()
            .http_client(sts.clone());
        let result = build_profile_provider(
            provider_config,
            &default_chain,
            sts_config,
            name,
            file,
            credentials_only,
        )
        .await
        .provide_credentials()
        .await;
        let outcome = match result {
            Ok(credentials) => credentials.access_key_id().to_string(),
            Err(e) => format!(
                "error: {}",
                aws_smithy_types::error::display::DisplayErrorContext(e)
            ),
        };
        ProfileRun {
            outcome,
            calls: sts.calls(),
            default_chain_calls: default_chain.calls.load(Ordering::SeqCst),
        }
    }

    /// Resolves the `analytics` profile of a credentials file holding `contents`.
    async fn resolve_analytics(
        contents: &str,
        default_region: Option<&str>,
        credentials_only: bool,
    ) -> ProfileRun {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials");
        std::fs::write(&credentials, contents).unwrap();
        resolve_profile(
            Some("analytics"),
            credentials.to_str(),
            default_region,
            credentials_only,
        )
        .await
    }

    fn region_line(region: Option<&str>) -> String {
        region
            .map(|region| format!("region = {region}\n"))
            .unwrap_or_default()
    }

    /// A credentials file whose `analytics` role, in us-west-2, is assumed through `hops - 1`
    /// roles in eu-central-1 from static keys.
    fn long_role_chain(hops: usize) -> String {
        let mut contents = String::new();
        for hop in 0..hops {
            let (name, region) = match hop {
                0 => ("analytics".to_string(), "us-west-2"),
                _ => (format!("hop{hop}"), "eu-central-1"),
            };
            let source = match hop + 1 {
                next if next == hops => "source".to_string(),
                next => format!("hop{next}"),
            };
            contents.push_str(&format!(
                "[{name}]\nrole_arn = arn:aws:iam::123456789012:role/{name}\n\
                 source_profile = {source}\nregion = {region}\n\n"
            ));
        }
        contents.push_str(
            "[source]\naws_access_key_id = synthetic-source-key\n\
             aws_secret_access_key = synthetic-source-secret\n",
        );
        contents
    }

    /// The run of a `long_role_chain`, innermost role first, each in its own region and signed
    /// with the keys of the role before it.
    fn long_role_chain_run(hops: usize) -> String {
        let name = |hop: usize| match hop {
            0 => "analytics".to_string(),
            _ => format!("hop{hop}"),
        };
        let calls: Vec<String> = (0..hops)
            .rev()
            .map(|hop| {
                let region = if hop == 0 {
                    "us-west-2"
                } else {
                    "eu-central-1"
                };
                let key = if hop + 1 == hops {
                    "synthetic-source-key".to_string()
                } else {
                    format!("{}-key", name(hop + 1))
                };
                format!(
                    "{} at sts.{region}.amazonaws.com signed {region} by {key}",
                    name(hop)
                )
            })
            .collect();
        format!("analytics-key via {calls:?}, chain consulted 0")
    }

    /// A credentials file whose `analytics` role is assumed with the `intermediate` role's
    /// credentials, which is assumed with the static `source` keys. The source's region is
    /// never an STS region.
    fn two_role_chain(outer_region: Option<&str>, intermediate_region: Option<&str>) -> String {
        format!(
            "[analytics]\nrole_arn = arn:aws:iam::123456789012:role/outer\n\
             source_profile = intermediate\n{}\n\
             [intermediate]\nrole_arn = arn:aws:iam::123456789012:role/intermediate\n\
             source_profile = source\n{}\n\
             [source]\nregion = eu-west-1\naws_access_key_id = synthetic-source-key\n\
             aws_secret_access_key = synthetic-source-secret\n",
            region_line(outer_region),
            region_line(intermediate_region)
        )
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_sends_sts_to_the_profile_region() {
        // An assume-role profile in the Hadoop-selected file, with static source keys. The Java
        // SDK that Hadoop calls takes the role profile's own region first, then its default
        // region chain, then the global endpoint signed for us-east-1; the source profile's
        // region is never used, and fs.s3a.endpoint.region configures the S3 client, not this
        // provider. The default chain is consulted only when the role profile has no region.
        // (role profile region, source profile region, default chain region,
        //  fs.s3a.endpoint.region, STS host, signing region, default chain consulted)
        let cases = [
            (
                Some("us-west-2"),
                None,
                None,
                None,
                "sts.us-west-2.amazonaws.com",
                "us-west-2",
                0,
            ),
            (
                Some("us-west-2"),
                None,
                Some("eu-central-1"),
                None,
                "sts.us-west-2.amazonaws.com",
                "us-west-2",
                0,
            ),
            (
                Some("us-west-2"),
                None,
                None,
                Some("ap-south-1"),
                "sts.us-west-2.amazonaws.com",
                "us-west-2",
                0,
            ),
            (
                None,
                None,
                Some("eu-central-1"),
                None,
                "sts.eu-central-1.amazonaws.com",
                "eu-central-1",
                1,
            ),
            (
                None,
                None,
                Some("eu-central-1"),
                Some("ap-south-1"),
                "sts.eu-central-1.amazonaws.com",
                "eu-central-1",
                1,
            ),
            (None, None, None, None, "sts.amazonaws.com", "us-east-1", 1),
            (
                None,
                Some("eu-west-1"),
                Some("eu-central-1"),
                None,
                "sts.eu-central-1.amazonaws.com",
                "eu-central-1",
                1,
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (
            profile_region,
            source_region,
            default_region,
            endpoint_region,
            expected_host,
            expected_signing_region,
            calls,
        ) in cases
        {
            let dir = tempfile::tempdir().unwrap();
            let credentials = dir.path().join("credentials");
            let (role_region, source_region_line) =
                (region_line(profile_region), region_line(source_region));
            std::fs::write(
                &credentials,
                format!(
                    "[analytics]\nrole_arn = arn:aws:iam::123456789012:role/synthetic\n\
                     source_profile = source\n{role_region}\n[source]\n{source_region_line}\
                     aws_access_key_id = synthetic-source-key\n\
                     aws_secret_access_key = synthetic-source-secret\n"
                ),
            )
            .unwrap();
            let mut builder = TestConfigBuilder::new()
                .with_credential_provider(HADOOP_PROFILE)
                .with_property("auth.profile.name", "analytics")
                .with_property("auth.profile.file", credentials.to_str().unwrap());
            if let Some(region) = endpoint_region {
                builder = builder.with_region(region);
            }
            let configs = builder.build();
            let metadata =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap()
                    .expect("Should return a credential provider")
                    .metadata();
            let CredentialProviderMetadata::Profile {
                name,
                file,
                credentials_only,
            } = metadata
            else {
                panic!("expected a profile provider, got {metadata:?}");
            };

            let run = resolve_profile(
                name.as_deref(),
                file.as_deref(),
                default_region,
                credentials_only,
            )
            .await;
            let case = format!(
                "{profile_region:?}, {source_region:?}, {default_region:?}, {endpoint_region:?}"
            );
            actual.push(format!("{case}: {}", run.summary()));
            expected.push(format!(
                "{case}: synthetic-key via [\"synthetic at {expected_host} signed \
                 {expected_signing_region} by synthetic-source-key\"], chain consulted {calls}"
            ));
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_resolves_each_role_region() {
        // Hadoop builds one STS client per role in the chain, each from that role's own region,
        // then the default chain, then the global endpoint signed for us-east-1. The inner role
        // is assumed first with the source keys and its credentials sign the outer request.
        let at = |role: &str, host: &str, region: &str, key: &str| {
            format!("{role} at {host} signed {region} by {key}")
        };
        let source = "synthetic-source-key";
        let role = "intermediate-key";
        // (outer role region, intermediate role region, default chain region,
        //  expected STS requests, default chain consulted)
        let cases = [
            (
                Some("us-west-2"),
                Some("eu-central-1"),
                None,
                vec![
                    at(
                        "intermediate",
                        "sts.eu-central-1.amazonaws.com",
                        "eu-central-1",
                        source,
                    ),
                    at("outer", "sts.us-west-2.amazonaws.com", "us-west-2", role),
                ],
                0,
            ),
            (
                Some("us-west-2"),
                None,
                Some("ap-south-1"),
                vec![
                    at(
                        "intermediate",
                        "sts.ap-south-1.amazonaws.com",
                        "ap-south-1",
                        source,
                    ),
                    at("outer", "sts.us-west-2.amazonaws.com", "us-west-2", role),
                ],
                1,
            ),
            (
                None,
                Some("eu-central-1"),
                None,
                vec![
                    at(
                        "intermediate",
                        "sts.eu-central-1.amazonaws.com",
                        "eu-central-1",
                        source,
                    ),
                    at("outer", "sts.amazonaws.com", "us-east-1", role),
                ],
                1,
            ),
            // Two roles without a region ask the default chain once.
            (
                None,
                None,
                Some("ap-south-1"),
                vec![
                    at(
                        "intermediate",
                        "sts.ap-south-1.amazonaws.com",
                        "ap-south-1",
                        source,
                    ),
                    at("outer", "sts.ap-south-1.amazonaws.com", "ap-south-1", role),
                ],
                1,
            ),
            (
                None,
                None,
                None,
                vec![
                    at("intermediate", "sts.amazonaws.com", "us-east-1", source),
                    at("outer", "sts.amazonaws.com", "us-east-1", role),
                ],
                1,
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (outer_region, intermediate_region, default_region, calls, consulted) in cases {
            let contents = two_role_chain(outer_region, intermediate_region);
            let run = resolve_analytics(&contents, default_region, true).await;
            let case = format!("{outer_region:?}, {intermediate_region:?}, {default_region:?}");
            actual.push(format!("{case}: {}", run.summary()));
            expected.push(format!(
                "{case}: outer-key via {calls:?}, chain consulted {consulted}"
            ));
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_forwards_each_role_parameters() {
        // Each role sends its own external_id and role_session_name; a role without a session
        // name gets the SDK's profile default.
        let contents = "[analytics]\nrole_arn = arn:aws:iam::123456789012:role/outer\n\
                        source_profile = intermediate\nregion = us-west-2\n\
                        external_id = synthetic-outer-id\n\
                        role_session_name = synthetic-outer-session\n\n\
                        [intermediate]\nrole_arn = arn:aws:iam::123456789012:role/intermediate\n\
                        source_profile = source\nregion = us-west-2\n\n\
                        [source]\naws_access_key_id = synthetic-source-key\n\
                        aws_secret_access_key = synthetic-source-secret\n";
        let run = resolve_analytics(contents, None, true).await;
        assert_eq!(run.outcome, "outer-key");
        let parameters: Vec<_> = run
            .calls
            .iter()
            .map(|call| {
                (
                    call.role_arn.clone().unwrap_or_default(),
                    call.external_id.clone(),
                    call.session_name.clone().unwrap_or_default(),
                )
            })
            .collect();
        let [(inner_arn, inner_id, inner_session), (outer_arn, outer_id, outer_session)] =
            parameters.as_slice()
        else {
            panic!("expected two STS requests, got {parameters:?}");
        };
        assert_eq!(inner_arn, "arn:aws:iam::123456789012:role/intermediate");
        assert_eq!(inner_id, &None);
        assert!(
            inner_session.starts_with("assume-role-from-profile-"),
            "{inner_session}"
        );
        assert_eq!(outer_arn, "arn:aws:iam::123456789012:role/outer");
        assert_eq!(outer_id.as_deref(), Some("synthetic-outer-id"));
        assert_eq!(outer_session, "synthetic-outer-session");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_base_profiles() {
        // The profile a role chain starts from keeps its own provider. Static keys and a
        // credential process need no STS region and never ask the default chain. A web identity
        // base ignores its profile region, as Java's StsWebIdentityCredentialsProvider does, and
        // calls STS in the default chain's region, else the global endpoint.
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "synthetic-web-token").unwrap();
        let process = "echo '{\"Version\": 1, \"AccessKeyId\": \"synthetic-process-key\", \
                       \"SecretAccessKey\": \"synthetic-process-secret\"}'";
        let outer = "[analytics]\nrole_arn = arn:aws:iam::123456789012:role/outer\n\
                     region = us-west-2\n";
        let web = format!(
            "role_arn = arn:aws:iam::123456789012:role/web\nweb_identity_token_file = {}\n\
             region = eu-central-1\n",
            token.display()
        );
        let web_then_outer = |web_host: &str, consulted: usize| {
            format!(
                "outer-key via [\"web at {web_host} unsigned\", \"outer at \
                 sts.us-west-2.amazonaws.com signed us-west-2 by synthetic-web-key\"], \
                 chain consulted {consulted}"
            )
        };
        // (credentials file, default chain region, expected run)
        let cases = [
            (
                "[analytics]\naws_access_key_id = synthetic-source-key\n\
                 aws_secret_access_key = synthetic-source-secret\n"
                    .to_string(),
                Some("eu-central-1"),
                "synthetic-source-key via [], chain consulted 0".to_string(),
            ),
            (
                format!("{outer}source_profile = process\n\n[process]\ncredential_process = {process}\n"),
                Some("eu-central-1"),
                "outer-key via [\"outer at sts.us-west-2.amazonaws.com signed \
                 us-west-2 by synthetic-process-key\"], chain consulted 0"
                    .to_string(),
            ),
            (
                format!("{outer}source_profile = web\n\n[web]\n{web}"),
                Some("ap-south-1"),
                web_then_outer("sts.ap-south-1.amazonaws.com", 1),
            ),
            (
                format!("{outer}source_profile = web\n\n[web]\n{web}"),
                None,
                web_then_outer("sts.amazonaws.com", 1),
            ),
            (
                format!("[analytics]\n{web}"),
                None,
                "synthetic-web-key via [\"web at sts.amazonaws.com unsigned\"], \
                 chain consulted 1"
                    .to_string(),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (contents, default_region, run) in cases {
            actual.push(
                resolve_analytics(&contents, default_region, true)
                    .await
                    .summary(),
            );
            expected.push(run);
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_unsupported_chains_keep_the_sdk_provider() {
        // Chains the walk does not take over stay on the SDK's own profile provider, so its
        // errors surface from provide_credentials and its single STS region applies.
        let keys = "aws_access_key_id = synthetic-source-key\n\
                    aws_secret_access_key = synthetic-source-secret\n";
        let role = |name: &str| format!("role_arn = arn:aws:iam::123456789012:role/{name}\n");
        // (credentials file, text the outcome contains, expected run)
        let cases = [
            (
                format!(
                    "[analytics]\n{}source_profile = loop\n\n[loop]\n{}source_profile = analytics\n",
                    role("outer"),
                    role("loop")
                ),
                "profile formed an infinite loop",
                "error via [], chain consulted 1".to_string(),
            ),
            (
                format!("[analytics]\n{}source_profile = absent\n", role("outer")),
                "profile `absent` was not defined",
                "error via [], chain consulted 1".to_string(),
            ),
            (
                format!(
                    "[analytics]\n{}source_profile = source\ncredential_source = Environment\n\
                     region = us-west-2\n\n[source]\n{keys}",
                    role("outer")
                ),
                "contained both source_profile and credential_source",
                "error via [], chain consulted 0".to_string(),
            ),
            // A self-referencing role is assumed with the profile's own keys.
            (
                format!(
                    "[analytics]\n{}source_profile = analytics\nregion = us-west-2\n{keys}",
                    role("outer")
                ),
                "outer-key",
                "outer-key via [\"outer at sts.us-west-2.amazonaws.com signed \
                 us-west-2 by synthetic-source-key\"], chain consulted 0"
                    .to_string(),
            ),
            // A source profile with keys uses them, even when it also names a role.
            (
                format!(
                    "[analytics]\n{}source_profile = mixed\nregion = us-west-2\n\n[mixed]\n{}\
                     source_profile = absent\nregion = eu-central-1\n{keys}",
                    role("outer"),
                    role("ignored")
                ),
                "outer-key",
                "outer-key via [\"outer at sts.us-west-2.amazonaws.com signed \
                 us-west-2 by synthetic-source-key\"], chain consulted 0"
                    .to_string(),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (contents, outcome, run) in cases {
            let result = resolve_analytics(&contents, None, true).await;
            let found = if result.outcome.contains(outcome) {
                outcome
            } else {
                &result.outcome
            };
            actual.push(format!("{} [{found}]", result.summary()));
            expected.push(format!("{run} [{outcome}]"));
        }
        assert_eq!(actual, expected);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    fn test_hadoop_profile_provider_long_chain_runs_on_a_small_stack() {
        // Each role is assumed in turn with the keys of the role before it, so the stack a chain
        // needs does not grow with its length.
        const STACK_BYTES: usize = 512 * 1024;
        const ROLES: usize = 32;
        let contents = long_role_chain(ROLES);
        let run = std::thread::Builder::new()
            .stack_size(STACK_BYTES)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(resolve_analytics(&contents, None, true))
                    .summary()
            })
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(run, long_role_chain_run(ROLES));
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_base_failure_kind() {
        // A base that fails under a role is a provider error, so it stops a provider list as the
        // SDK's error does; a profile with no roles keeps the SDK provider's own error.
        let no_credentials = "region = us-west-2\n";
        let cases = [
            format!(
                "[analytics]\nrole_arn = arn:aws:iam::123456789012:role/outer\n\
                 source_profile = source\n\n[source]\n{no_credentials}"
            ),
            format!("[analytics]\n{no_credentials}"),
        ];
        let mut kinds = Vec::new();
        for contents in cases {
            let dir = tempfile::tempdir().unwrap();
            let credentials = dir.path().join("credentials");
            std::fs::write(&credentials, contents).unwrap();
            let result = build_profile_provider(
                ProviderConfig::without_region(),
                &None::<Region>,
                aws_config::defaults(BehaviorVersion::latest()).empty_test_environment(),
                Some("analytics"),
                credentials.to_str(),
                true,
            )
            .await
            .provide_credentials()
            .await;
            kinds.push(match result {
                Err(CredentialsError::ProviderError(_)) => "provider error",
                Err(CredentialsError::CredentialsNotLoaded(_)) => "not loaded",
                _ => "other",
            });
        }
        assert_eq!(kinds, ["provider error", "not loaded"]);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_resolve_role_chain() {
        let keys = "aws_access_key_id = synthetic-source-key\n\
                    aws_secret_access_key = synthetic-source-secret\n";
        let arn = |name: &str| format!("arn:aws:iam::123456789012:role/{name}");
        let error = |reason: &str| Err(reason.to_string());
        // Reasons for staying on the SDK provider that the end-to-end tests above cannot tell
        // apart. (credentials file, expected reason)
        let cases = [
            (
                format!(
                    "[analytics]\nrole_arn = {}\ncredential_source = Environment\n",
                    arn("outer")
                ),
                error("profile analytics uses credential_source"),
            ),
            (
                format!(
                    "[analytics]\nrole_arn = {}\ncredential_process = synthetic\n",
                    arn("outer")
                ),
                error("profile analytics has no other source_profile"),
            ),
            (
                format!(
                    "[analytics]\nrole_arn = {}\nsource_profile = mixed\n\n[mixed]\n\
                     credential_process = synthetic\n{keys}",
                    arn("outer")
                ),
                error("source profile mixed mixes keys with other credentials"),
            ),
        ];
        for (contents, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let credentials = dir.path().join("credentials");
            std::fs::write(&credentials, &contents).unwrap();
            let files = EnvConfigFiles::builder()
                .with_file(EnvConfigFileKind::Credentials, credentials)
                .build();
            let profiles = aws_config::profile::load(
                &Default::default(),
                &Default::default(),
                &files,
                Some("analytics".into()),
            )
            .await
            .unwrap();
            assert_eq!(
                resolve_role_chain(&profiles, profiles.selected_profile()),
                expected,
                "{contents}"
            );
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_sdk_profile_provider_keeps_one_sts_region() {
        // The SDK spellings keep the SDK's own chain: every role is assumed in the default
        // chain's region, whatever the profiles say.
        let contents = two_role_chain(Some("us-west-2"), Some("eu-central-1"));
        let run = resolve_analytics(&contents, Some("ap-south-1"), false).await;
        assert_eq!(
            run.summary(),
            "outer-key via [\"intermediate at sts.ap-south-1.amazonaws.com signed \
             ap-south-1 by synthetic-source-key\", \"outer at sts.ap-south-1.amazonaws.com \
             signed ap-south-1 by intermediate-key\"], chain consulted 1"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_iam_instance_credential_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_IAM_INSTANCE)
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(result.is_some(), "Should return a credential provider");

        let test_provider = result.unwrap().metadata();
        assert_eq!(
            test_provider,
            CredentialProviderMetadata::Chain(vec![
                CredentialProviderMetadata::Ecs,
                CredentialProviderMetadata::Imds
            ])
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_chained_credential_providers() {
        // Test three providers in chain: Environment -> IMDS -> ECS
        let configs = TestConfigBuilder::new()
            .with_credential_provider(&format!(
                "{AWS_ENVIRONMENT},{AWS_INSTANCE_PROFILE},{AWS_CONTAINER_CREDENTIALS}"
            ))
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for complex chain"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Chain(vec![
                CredentialProviderMetadata::Environment,
                CredentialProviderMetadata::Imds,
                CredentialProviderMetadata::Ecs
            ])
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_static_environment_web_identity_chain() {
        // Test chaining static credentials -> environment -> web identity
        let configs = TestConfigBuilder::new()
            .with_credential_provider(&format!(
                "{HADOOP_SIMPLE},{AWS_ENVIRONMENT},{AWS_WEB_IDENTITY}"
            ))
            .with_access_key("chain_access_key")
            .with_secret_key("chain_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return a credential provider for static+env+web chain"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Chain(vec![
                CredentialProviderMetadata::Static {
                    is_valid: true,
                    access_key: "chain_access_key".to_string(),
                    secret_key: "chain_secret_key".to_string(),
                    session_token: None
                },
                CredentialProviderMetadata::Environment,
                CredentialProviderMetadata::WebIdentity
            ])
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_with_static_base_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ASSUMED_ROLE)
            .with_assume_role_arn("arn:aws:iam::123456789012:role/test-role")
            .with_assume_role_session_name("static-base-session")
            .with_assume_role_credentials_provider(HADOOP_TEMPORARY)
            .with_access_key("base_static_access_key")
            .with_secret_key("base_static_secret_key")
            .with_session_token("base_static_session_token")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return assume role provider with static base"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/test-role".to_string(),
                session_name: "static-base-session".to_string(),
                base_provider_metadata: Box::new(CredentialProviderMetadata::Static {
                    is_valid: true,
                    access_key: "base_static_access_key".to_string(),
                    secret_key: "base_static_secret_key".to_string(),
                    session_token: Some("base_static_session_token".to_string())
                })
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_with_web_identity_base_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ASSUMED_ROLE)
            .with_assume_role_arn("arn:aws:iam::123456789012:role/web-identity-role")
            .with_assume_role_session_name("web-identity-session")
            .with_assume_role_credentials_provider(AWS_WEB_IDENTITY)
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return assume role provider with web identity base"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/web-identity-role".to_string(),
                session_name: "web-identity-session".to_string(),
                base_provider_metadata: Box::new(CredentialProviderMetadata::WebIdentity)
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_with_chained_base_providers() {
        // Test assume role with multiple base providers: Static -> Environment -> IMDS
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ASSUMED_ROLE)
            .with_assume_role_arn("arn:aws:iam::123456789012:role/chained-role")
            .with_assume_role_session_name("chained-base-session")
            .with_assume_role_credentials_provider(&format!(
                "{HADOOP_SIMPLE},{AWS_ENVIRONMENT},{AWS_INSTANCE_PROFILE}"
            ))
            .with_access_key("chained_base_access_key")
            .with_secret_key("chained_base_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return assume role provider with chained base"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/chained-role".to_string(),
                session_name: "chained-base-session".to_string(),
                base_provider_metadata: Box::new(CredentialProviderMetadata::Chain(vec![
                    CredentialProviderMetadata::Static {
                        is_valid: true,
                        access_key: "chained_base_access_key".to_string(),
                        secret_key: "chained_base_secret_key".to_string(),
                        session_token: None
                    },
                    CredentialProviderMetadata::Environment,
                    CredentialProviderMetadata::Imds
                ]))
            }
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_chained_with_other_providers() {
        // Test assume role as first provider in a chain, followed by environment and IMDS
        let configs = TestConfigBuilder::new()
            .with_credential_provider(&format!(
                "  {HADOOP_ASSUMED_ROLE}\n,  {AWS_INSTANCE_PROFILE}\n"
            ))
            .with_assume_role_arn("arn:aws:iam::123456789012:role/first-in-chain")
            .with_assume_role_session_name("first-chain-session")
            .with_assume_role_credentials_provider(&format!(
                "  {AWS_WEB_IDENTITY}\n,  {HADOOP_TEMPORARY}\n,  {AWS_ENVIRONMENT}\n"
            ))
            .with_access_key("assume_role_base_key")
            .with_secret_key("assume_role_base_secret")
            .with_session_token("assume_role_base_token")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "Should return chained provider with assume role first"
        );

        assert_eq!(
            result.unwrap().metadata(),
            CredentialProviderMetadata::Chain(vec![
                CredentialProviderMetadata::AssumeRole {
                    role_arn: "arn:aws:iam::123456789012:role/first-in-chain".to_string(),
                    session_name: "first-chain-session".to_string(),
                    base_provider_metadata: Box::new(CredentialProviderMetadata::Chain(vec![
                        CredentialProviderMetadata::WebIdentity,
                        CredentialProviderMetadata::Static {
                            is_valid: true,
                            access_key: "assume_role_base_key".to_string(),
                            secret_key: "assume_role_base_secret".to_string(),
                            session_token: Some("assume_role_base_token".to_string())
                        },
                        CredentialProviderMetadata::Environment,
                    ]))
                },
                CredentialProviderMetadata::Imds
            ])
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_assume_role_with_anonymous_base_provider_error() {
        // Test that assume role with anonymous base provider fails
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_ASSUMED_ROLE)
            .with_assume_role_arn("arn:aws:iam::123456789012:role/should-fail")
            .with_assume_role_session_name("should-fail-session")
            .with_assume_role_credentials_provider(HADOOP_ANONYMOUS)
            .build();

        let result =
            build_credential_provider(&configs, "test-bucket", Duration::from_secs(300)).await;
        assert!(
            result.is_err(),
            "Should error when assume role uses anonymous base provider"
        );

        if let Err(e) = result {
            assert!(e.to_string().contains(
                "Anonymous credential provider cannot be used as assumed role credential provider"
            ));
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_get_credential_from_static_credential_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_SIMPLE)
            .with_access_key("test_access_key")
            .with_secret_key("test_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(result.is_some(), "Should return a credential provider");

        let test_provider = result.unwrap();
        let credential = test_provider.get_credential().await.unwrap();
        assert_eq!(credential.key_id, "test_access_key");
        assert_eq!(credential.secret_key, "test_secret_key");
        assert_eq!(credential.token, None);

        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_TEMPORARY)
            .with_access_key("test_access_key_2")
            .with_secret_key("test_secret_key_2")
            .with_session_token("test_session_token_2")
            .build();
        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(result.is_some(), "Should return a credential provider");

        let test_provider = result.unwrap();
        let credential = test_provider.get_credential().await.unwrap();
        assert_eq!(credential.key_id, "test_access_key_2");
        assert_eq!(credential.secret_key, "test_secret_key_2");
        assert_eq!(credential.token, Some("test_session_token_2".to_string()));
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_get_credential_from_invalid_static_credential_provider() {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(HADOOP_SIMPLE)
            .with_access_key("test_access_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(result.is_some(), "Should return a credential provider");

        let test_provider = result.unwrap();
        let result = test_provider.get_credential().await;
        assert!(result.is_err(), "Should return an error when getting credential from invalid static credential provider");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_invalid_static_credential_provider_should_not_prevent_other_providers_from_working(
    ) {
        let configs = TestConfigBuilder::new()
            .with_credential_provider(&format!("{HADOOP_TEMPORARY},{HADOOP_SIMPLE}"))
            .with_access_key("test_access_key")
            .with_secret_key("test_secret_key")
            .build();

        let result = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(result.is_some(), "Should return a credential provider");

        assert_eq!(
            result.as_ref().unwrap().metadata(),
            CredentialProviderMetadata::Chain(vec![
                CredentialProviderMetadata::Static {
                    is_valid: false,
                    access_key: "test_access_key".to_string(),
                    secret_key: "test_secret_key".to_string(),
                    session_token: None,
                },
                CredentialProviderMetadata::Static {
                    is_valid: true,
                    access_key: "test_access_key".to_string(),
                    secret_key: "test_secret_key".to_string(),
                    session_token: None,
                }
            ])
        );

        let test_provider = result.unwrap();

        for _ in 0..10 {
            let credential = test_provider.get_credential().await.unwrap();
            assert_eq!(credential.key_id, "test_access_key");
            assert_eq!(credential.secret_key, "test_secret_key");
        }
    }

    #[derive(Debug)]
    struct MockAwsCredentialProvider {
        counter: AtomicI32,
    }

    impl ProvideCredentials for MockAwsCredentialProvider {
        fn provide_credentials<'a>(
            &'a self,
        ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let cnt = self.counter.fetch_add(1, Ordering::SeqCst);
            let cred = Credentials::builder()
                .access_key_id(format!("test_access_key_{cnt}"))
                .secret_access_key(format!("test_secret_key_{cnt}"))
                .expiry(SystemTime::now() + Duration::from_secs(60))
                .provider_name("mock_provider")
                .build();
            aws_credential_types::provider::future::ProvideCredentials::ready(Ok(cred))
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_cached_credential_provider_refresh_credential() {
        let provider = Arc::new(MockAwsCredentialProvider {
            counter: AtomicI32::new(0),
        });

        // 60 seconds before expiry, the credential is always refreshed
        let cached_provider = CachedAwsCredentialProvider::new(
            provider,
            CredentialProviderMetadata::Default,
            Duration::from_secs(60),
        );
        for k in 0..3 {
            let credential = cached_provider.get_credential().await.unwrap();
            assert_eq!(credential.key_id, format!("test_access_key_{k}"));
            assert_eq!(credential.secret_key, format!("test_secret_key_{k}"));
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_cached_credential_provider_cache_credential() {
        let provider = Arc::new(MockAwsCredentialProvider {
            counter: AtomicI32::new(0),
        });

        // 10 seconds before expiry, the credential is not refreshed
        let cached_provider = CachedAwsCredentialProvider::new(
            provider,
            CredentialProviderMetadata::Default,
            Duration::from_secs(10),
        );
        for _ in 0..3 {
            let credential = cached_provider.get_credential().await.unwrap();
            assert_eq!(credential.key_id, "test_access_key_0");
            assert_eq!(credential.secret_key, "test_secret_key_0");
        }
    }

    #[test]
    fn test_extract_s3_config_options() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.s3a.endpoint.region".to_string(),
            "ap-northeast-1".to_string(),
        );
        configs.insert(
            "fs.s3a.requester.pays.enabled".to_string(),
            "true".to_string(),
        );
        let s3_configs = extract_s3_config_options(&configs, "test-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Region),
            Some(&"ap-northeast-1".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::RequestPayer),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_normalize_endpoint_virtual_hosted_style() {
        // Virtual-hosted addressing inserts the bucket as the leading host label. The scheme,
        // port and any path suffix are preserved and a trailing slash is dropped.
        let cases = [
            (
                "custom.endpoint.com",
                "https://test-bucket.custom.endpoint.com",
            ),
            (
                "http://custom.endpoint.com",
                "http://test-bucket.custom.endpoint.com",
            ),
            (
                "https://custom.endpoint.com/",
                "https://test-bucket.custom.endpoint.com",
            ),
            (
                "http://minio.internal:9000",
                "http://test-bucket.minio.internal:9000",
            ),
            (
                "https://custom.endpoint.com:8443/",
                "https://test-bucket.custom.endpoint.com:8443",
            ),
            (
                "https://custom.endpoint.com/path/to/resource",
                "https://test-bucket.custom.endpoint.com/path/to/resource",
            ),
            (
                "https://custom.endpoint.com/path/to/resource/",
                "https://test-bucket.custom.endpoint.com/path/to/resource",
            ),
            (
                "s3.us-west-2.amazonaws.com",
                "https://test-bucket.s3.us-west-2.amazonaws.com",
            ),
        ];
        for (endpoint, expected) in cases {
            assert_eq!(
                normalize_endpoint(endpoint, "test-bucket", true),
                Some(NormalizedEndpoint {
                    endpoint: expected.to_string(),
                    virtual_hosted_style_request: true,
                }),
                "endpoint {endpoint}"
            );
        }

        // A dotted bucket over HTTPS stays path-style, as the AWS SDK addresses it, since the
        // dotted host falls outside S3's wildcard certificate; over HTTP it is virtual-hosted.
        assert_eq!(
            normalize_endpoint("custom.endpoint.com", "my.dotted.bucket", true),
            Some(NormalizedEndpoint {
                endpoint: "https://custom.endpoint.com".to_string(),
                virtual_hosted_style_request: false,
            })
        );
        assert_eq!(
            normalize_endpoint("http://custom.endpoint.com", "my.dotted.bucket", true),
            Some(NormalizedEndpoint {
                endpoint: "http://my.dotted.bucket.custom.endpoint.com".to_string(),
                virtual_hosted_style_request: true,
            })
        );
    }

    #[test]
    fn test_extract_s3_config_dotted_bucket_stays_path_style_on_default_endpoint() {
        // No custom endpoint means the HTTPS AWS endpoint, where a dotted bucket must be
        // addressed path-style whatever the flag says.
        let configs = TestConfigBuilder::new().with_region("us-east-1").build();
        let s3_configs = extract_s3_config_options(&configs, "review.dotted.bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"false".to_string())
        );
        assert!(!s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint));

        let configs = TestConfigBuilder::new()
            .with_region("us-east-1")
            .with_property("endpoint", "https://s3.us-east-1.amazonaws.com")
            .build();
        let s3_configs = extract_s3_config_options(&configs, "review.dotted.bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"false".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Endpoint),
            Some(&"https://s3.us-east-1.amazonaws.com".to_string())
        );

        let s3_configs = extract_s3_config_options(&configs, "plainbucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"true".to_string())
        );

        // A custom HTTP endpoint keeps virtual hosting for a dotted bucket, as the SDK does,
        // since the certificate rule only applies to HTTPS.
        let configs = TestConfigBuilder::new()
            .with_property("endpoint", "http://storage.example.test")
            .build();
        let s3_configs = extract_s3_config_options(&configs, "review.dotted.bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"true".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Endpoint),
            Some(&"http://review.dotted.bucket.storage.example.test".to_string())
        );
    }

    /// The URL object_store sends a GET for `s3a://<bucket>/object` to under the options Comet
    /// derives from `configs`, read from a presigned URL so no request leaves the test.
    async fn final_url(bucket: &str, configs: &HashMap<String, String>) -> String {
        use object_store::signer::Signer;

        let mut builder = AmazonS3Builder::new()
            .with_url(format!("s3a://{bucket}/object"))
            .with_allow_http(true)
            .with_access_key_id("test_access_key")
            .with_secret_access_key("test_secret_key");
        for (key, value) in extract_s3_config_options(configs, bucket) {
            builder = builder.with_config(key, value);
        }
        let signed = match builder.build() {
            Ok(store) => {
                store
                    .signed_url(
                        reqwest::Method::GET,
                        &Path::from("object"),
                        Duration::from_secs(60),
                    )
                    .await
            }
            Err(e) => Err(e),
        };
        match signed {
            Ok(mut url) => {
                url.set_query(None);
                url.to_string()
            }
            Err(e) => format!("error: {e}"),
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // object_store calls foreign functions
    async fn test_bucket_the_sdk_cannot_virtual_host_stays_path_style() {
        // The AWS SDK virtual-hosts a bucket only when its name is a DNS label: 3 to 63
        // lowercase letters, digits and hyphens that start and end with a letter or digit.
        // Other names, such as the mixed-case ones US East accepted before March 2018, go
        // path-style, since a hostname would lowercase the bucket into a different one.
        let default_endpoint = TestConfigBuilder::new().with_region("us-east-1").build();
        let http_endpoint = TestConfigBuilder::new()
            .with_region("us-east-1")
            .with_property("endpoint", "http://storage.example.test")
            .build();
        let long = "a".repeat(64);
        let cases = [
            (
                &default_endpoint,
                "LegacyBucket",
                "https://s3.us-east-1.amazonaws.com/LegacyBucket/object".to_string(),
            ),
            (
                &default_endpoint,
                "legacy_bucket",
                "https://s3.us-east-1.amazonaws.com/legacy_bucket/object".to_string(),
            ),
            (
                &default_endpoint,
                "-legacy-bucket",
                "https://s3.us-east-1.amazonaws.com/-legacy-bucket/object".to_string(),
            ),
            (
                &default_endpoint,
                "legacy-bucket-",
                "https://s3.us-east-1.amazonaws.com/legacy-bucket-/object".to_string(),
            ),
            (
                &default_endpoint,
                long.as_str(),
                format!("https://s3.us-east-1.amazonaws.com/{long}/object"),
            ),
            (
                &default_endpoint,
                "legacy-bucket",
                "https://legacy-bucket.s3.us-east-1.amazonaws.com/object".to_string(),
            ),
            // Over plain HTTP the SDK also accepts dots, but not an IPv4-shaped name, a dot next
            // to a hyphen or an uppercase letter
            (
                &http_endpoint,
                "192.168.10.12",
                "http://storage.example.test/192.168.10.12/object".to_string(),
            ),
            (
                &http_endpoint,
                "legacy-.bucket",
                "http://storage.example.test/legacy-.bucket/object".to_string(),
            ),
            (
                &http_endpoint,
                "LegacyBucket",
                "http://storage.example.test/LegacyBucket/object".to_string(),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (configs, bucket, url) in cases {
            actual.push(format!("{bucket}: {}", final_url(bucket, configs).await));
            expected.push(format!("{bucket}: {url}"));
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // AWS credential providers call foreign functions
    async fn test_hadoop_profile_provider_takes_the_forwarded_default_file() {
        // With no configured file the JVM-resolved default applies; a configured file wins;
        // the SDK spellings ignore both.
        for (file, expected) in [
            (None, Some("/synthetic/jvm-home/.aws/credentials")),
            (Some("/etc/aws/credentials"), Some("/etc/aws/credentials")),
        ] {
            let mut builder = TestConfigBuilder::new()
                .with_credential_provider(HADOOP_PROFILE)
                .with_property(
                    "comet.default.profile.file",
                    "/synthetic/jvm-home/.aws/credentials",
                );
            if let Some(file) = file {
                builder = builder.with_property("auth.profile.file", file);
            }
            let configs = builder.build();
            let metadata =
                build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
                    .await
                    .unwrap()
                    .expect("Should return a credential provider")
                    .metadata();
            assert_eq!(
                metadata,
                CredentialProviderMetadata::Profile {
                    name: None,
                    file: expected.map(str::to_string),
                    credentials_only: true,
                }
            );
        }
        let configs = TestConfigBuilder::new()
            .with_credential_provider(AWS_PROFILE)
            .with_property(
                "comet.default.profile.file",
                "/synthetic/jvm-home/.aws/credentials",
            )
            .build();
        let metadata = build_credential_provider(&configs, "test-bucket", Duration::from_secs(300))
            .await
            .unwrap()
            .expect("Should return a credential provider")
            .metadata();
        assert_eq!(
            metadata,
            CredentialProviderMetadata::Profile {
                name: None,
                file: None,
                credentials_only: false,
            }
        );
    }

    #[test]
    fn test_executor_defaults_overlay_the_forwarded_options() {
        use datafusion::prelude::{SessionConfig, SessionContext};
        let mut options = HashMap::from([(
            COMET_DEFAULT_PROFILE_FILE_KEY.to_string(),
            "/synthetic/driver-home/.aws/credentials".to_string(),
        )]);
        // Without executor defaults registered, the options pass through untouched.
        apply_executor_object_store_defaults(&SessionContext::new(), &mut options);
        assert_eq!(
            options
                .get(COMET_DEFAULT_PROFILE_FILE_KEY)
                .map(String::as_str),
            Some("/synthetic/driver-home/.aws/credentials")
        );
        // The executor's own resolution wins over whatever the driver serialized.
        let config = SessionConfig::new().with_extension(Arc::new(ExecutorObjectStoreDefaults {
            default_profile_file: Some("/synthetic/executor-home/.aws/credentials".to_string()),
        }));
        apply_executor_object_store_defaults(
            &SessionContext::new_with_config(config),
            &mut options,
        );
        assert_eq!(
            options
                .get(COMET_DEFAULT_PROFILE_FILE_KEY)
                .map(String::as_str),
            Some("/synthetic/executor-home/.aws/credentials")
        );
        // An executor with nothing resolved leaves the options alone.
        let config =
            SessionConfig::new().with_extension(Arc::new(ExecutorObjectStoreDefaults::default()));
        apply_executor_object_store_defaults(
            &SessionContext::new_with_config(config),
            &mut options,
        );
        assert_eq!(options.len(), 1);
    }

    #[test]
    fn test_default_shared_credentials_file_matches_hadoop() {
        assert_eq!(
            default_shared_credentials_file(None, Some("/home/comet".to_string())),
            "/home/comet/.aws/credentials"
        );
        assert_eq!(
            default_shared_credentials_file(
                Some("/etc/aws/shared".to_string()),
                Some("/home/comet".to_string())
            ),
            "/etc/aws/shared"
        );
        assert_eq!(
            default_shared_credentials_file(
                Some("  ".to_string()),
                Some("/home/comet".to_string())
            ),
            "/home/comet/.aws/credentials"
        );
    }

    #[test]
    fn test_normalize_endpoint_path_style() {
        // Path-style leaves the endpoint as configured apart from the https default, since
        // object_store appends the bucket itself
        let cases = [
            ("custom.endpoint.com", "https://custom.endpoint.com"),
            ("http://custom.endpoint.com", "http://custom.endpoint.com"),
            (
                "https://custom.endpoint.com/",
                "https://custom.endpoint.com/",
            ),
            ("http://minio.internal:9000", "http://minio.internal:9000"),
            (
                "https://custom.endpoint.com:8443/",
                "https://custom.endpoint.com:8443/",
            ),
            (
                "https://custom.endpoint.com/path/to/resource",
                "https://custom.endpoint.com/path/to/resource",
            ),
            (
                "s3.us-west-2.amazonaws.com",
                "https://s3.us-west-2.amazonaws.com",
            ),
        ];
        for (endpoint, expected) in cases {
            assert_eq!(
                normalize_endpoint(endpoint, "test-bucket", false),
                Some(NormalizedEndpoint {
                    endpoint: expected.to_string(),
                    virtual_hosted_style_request: false,
                }),
                "endpoint {endpoint}"
            );
        }

        assert_eq!(
            normalize_endpoint("custom.endpoint.com", "my.dotted.bucket", false),
            Some(NormalizedEndpoint {
                endpoint: "https://custom.endpoint.com".to_string(),
                virtual_hosted_style_request: false,
            })
        );
    }

    #[test]
    fn test_normalize_endpoint_ip_host_forces_path_style() {
        // The AWS SDK endpoint rules address IP-literal hosts path-style whatever the
        // configuration says, since `bucket.127.0.0.1` is not a valid host
        let cases = [
            ("http://127.0.0.1:9000", "http://127.0.0.1:9000"),
            ("http://127.0.0.1", "http://127.0.0.1"),
            ("127.0.0.1:9000", "https://127.0.0.1:9000"),
            ("http://[::1]:9000", "http://[::1]:9000"),
            ("https://[::1]", "https://[::1]"),
            ("[::1]:9000", "https://[::1]:9000"),
        ];
        for (endpoint, expected) in cases {
            for virtual_hosted_style_request in [true, false] {
                assert_eq!(
                    normalize_endpoint(endpoint, "test-bucket", virtual_hosted_style_request),
                    Some(NormalizedEndpoint {
                        endpoint: expected.to_string(),
                        virtual_hosted_style_request: false,
                    }),
                    "endpoint {endpoint}, requested virtual-hosted {virtual_hosted_style_request}"
                );
            }
        }
    }

    #[test]
    fn test_normalize_endpoint_skips_default_aws_endpoint() {
        for virtual_hosted_style_request in [true, false] {
            assert_eq!(
                normalize_endpoint(
                    "s3.amazonaws.com",
                    "test-bucket",
                    virtual_hosted_style_request
                ),
                None
            );
            assert_eq!(
                normalize_endpoint("", "test-bucket", virtual_hosted_style_request),
                None
            );
        }
    }

    #[test]
    fn test_extract_s3_config_path_style_access() {
        // Hadoop defaults fs.s3a.path.style.access to false (virtual-hosted) and, like
        // Configuration.getBoolean, falls back to that default for non-boolean text
        let cases = [
            (None, "true", "https://test-bucket.custom.endpoint.com"),
            (
                Some("false"),
                "true",
                "https://test-bucket.custom.endpoint.com",
            ),
            (
                Some("yes"),
                "true",
                "https://test-bucket.custom.endpoint.com",
            ),
            (Some("true"), "false", "https://custom.endpoint.com"),
            (Some(" TRUE "), "false", "https://custom.endpoint.com"),
        ];
        for (path_style_access, expected_flag, expected_endpoint) in cases {
            let mut builder =
                TestConfigBuilder::new().with_property("endpoint", "custom.endpoint.com");
            if let Some(value) = path_style_access {
                builder = builder.with_property("path.style.access", value);
            }
            let s3_configs = extract_s3_config_options(&builder.build(), "test-bucket");
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
                Some(&expected_flag.to_string()),
                "path.style.access {path_style_access:?}"
            );
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::Endpoint),
                Some(&expected_endpoint.to_string()),
                "path.style.access {path_style_access:?}"
            );
        }
    }

    #[test]
    fn test_extract_s3_config_ip_endpoint_forces_path_style() {
        // With path.style.access unset an IP-literal endpoint stays path-style and the flag
        // handed to object_store agrees with the unchanged endpoint
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://127.0.0.1",
            "http://[::1]:9000",
            "http://[::1]",
        ] {
            let configs = TestConfigBuilder::new()
                .with_property("endpoint", endpoint)
                .build();
            let s3_configs = extract_s3_config_options(&configs, "test-bucket");
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
                Some(&"false".to_string()),
                "endpoint {endpoint}"
            );
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::Endpoint),
                Some(&endpoint.to_string()),
                "endpoint {endpoint}"
            );
        }
    }

    #[test]
    fn test_extract_s3_config_http_endpoint_keeps_scheme() {
        for (path_style_access, expected_endpoint) in [
            ("false", "http://test-bucket.minio.internal:9000"),
            ("true", "http://minio.internal:9000"),
        ] {
            let configs = TestConfigBuilder::new()
                .with_property("endpoint", "http://minio.internal:9000")
                .with_property("path.style.access", path_style_access)
                .build();
            let s3_configs = extract_s3_config_options(&configs, "test-bucket");
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::Endpoint),
                Some(&expected_endpoint.to_string()),
                "path.style.access {path_style_access}"
            );
        }
    }

    #[test]
    fn test_extract_s3_config_path_style_access_without_endpoint() {
        // The flag is always handed to object_store so the default AWS endpoint follows the
        // same addressing rule as a custom one
        let configs = TestConfigBuilder::new().with_region("us-east-1").build();
        let s3_configs = extract_s3_config_options(&configs, "test-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"true".to_string())
        );
        assert!(!s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint));

        let configs = TestConfigBuilder::new()
            .with_region("us-east-1")
            .with_property("path.style.access", "true")
            .build();
        let s3_configs = extract_s3_config_options(&configs, "test-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"false".to_string())
        );
        assert!(!s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint));
    }

    #[test]
    fn test_extract_s3_config_per_bucket_overrides() {
        // A bucket can override both the endpoint and the addressing flag, in either direction
        let configs = TestConfigBuilder::new()
            .with_property("endpoint", "global.endpoint.com")
            .with_property("path.style.access", "true")
            .with_bucket_property("vh-bucket", "endpoint", "http://bucket.endpoint.com:9000")
            .with_bucket_property("vh-bucket", "path.style.access", "false")
            .build();

        let s3_configs = extract_s3_config_options(&configs, "vh-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"true".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Endpoint),
            Some(&"http://vh-bucket.bucket.endpoint.com:9000".to_string())
        );

        let s3_configs = extract_s3_config_options(&configs, "other-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"false".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Endpoint),
            Some(&"https://global.endpoint.com".to_string())
        );

        let configs = TestConfigBuilder::new()
            .with_property("endpoint", "global.endpoint.com")
            .with_property("path.style.access", "false")
            .with_bucket_property("ps-bucket", "path.style.access", "true")
            .build();

        let s3_configs = extract_s3_config_options(&configs, "ps-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"false".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Endpoint),
            Some(&"https://global.endpoint.com".to_string())
        );

        let s3_configs = extract_s3_config_options(&configs, "other-bucket");
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some(&"true".to_string())
        );
        assert_eq!(
            s3_configs.get(&AmazonS3ConfigKey::Endpoint),
            Some(&"https://other-bucket.global.endpoint.com".to_string())
        );
    }

    #[test]
    fn test_extract_s3_config_ignore_default_endpoint() {
        for path_style_access in ["false", "true"] {
            let configs = TestConfigBuilder::new()
                .with_property("endpoint", "s3.amazonaws.com")
                .with_property("path.style.access", path_style_access)
                .build();
            let s3_configs = extract_s3_config_options(&configs, "test-bucket");
            assert!(!s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint));

            let configs = TestConfigBuilder::new()
                .with_property("endpoint", "")
                .with_property("path.style.access", path_style_access)
                .build();
            let s3_configs = extract_s3_config_options(&configs, "test-bucket");
            assert!(!s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint));
        }
    }

    #[test]
    fn test_credential_provider_metadata_simple_string() {
        // Test Static provider
        let static_metadata = CredentialProviderMetadata::Static {
            is_valid: true,
            access_key: "sensitive_key".to_string(),
            secret_key: "sensitive_secret".to_string(),
            session_token: Some("sensitive_token".to_string()),
        };
        assert_eq!(static_metadata.simple_string(), "Static(valid: true)");

        // Test AssumeRole provider
        let assume_role_metadata = CredentialProviderMetadata::AssumeRole {
            role_arn: "arn:aws:iam::123456789012:role/test-role".to_string(),
            session_name: "test-session".to_string(),
            base_provider_metadata: Box::new(CredentialProviderMetadata::Environment),
        };
        assert_eq!(
            assume_role_metadata.simple_string(),
            "AssumeRole(role: arn:aws:iam::123456789012:role/test-role, session: test-session, base: Environment)"
        );

        // Test Profile provider with and without overrides
        let profile_metadata = CredentialProviderMetadata::Profile {
            name: None,
            file: None,
            credentials_only: false,
        };
        assert_eq!(profile_metadata.simple_string(), "Profile");
        let profile_metadata = CredentialProviderMetadata::Profile {
            name: Some("analytics".to_string()),
            file: None,
            credentials_only: true,
        };
        assert_eq!(profile_metadata.simple_string(), "Profile(name: analytics)");
        let profile_metadata = CredentialProviderMetadata::Profile {
            name: Some("analytics".to_string()),
            file: Some("/etc/aws/credentials".to_string()),
            credentials_only: true,
        };
        assert_eq!(
            profile_metadata.simple_string(),
            "Profile(name: analytics, file: /etc/aws/credentials)"
        );

        // Test Chain provider
        let chain_metadata = CredentialProviderMetadata::Chain(vec![
            CredentialProviderMetadata::Static {
                is_valid: false,
                access_key: "key1".to_string(),
                secret_key: "secret1".to_string(),
                session_token: None,
            },
            CredentialProviderMetadata::Environment,
            CredentialProviderMetadata::Imds,
        ]);
        assert_eq!(
            chain_metadata.simple_string(),
            "Chain(Static(valid: false) -> Environment -> Imds)"
        );

        // Test nested AssumeRole with Chain base
        let nested_metadata = CredentialProviderMetadata::AssumeRole {
            role_arn: "arn:aws:iam::123456789012:role/nested-role".to_string(),
            session_name: "nested-session".to_string(),
            base_provider_metadata: Box::new(chain_metadata),
        };
        assert_eq!(
            nested_metadata.simple_string(),
            "AssumeRole(role: arn:aws:iam::123456789012:role/nested-role, session: nested-session, base: Chain(Static(valid: false) -> Environment -> Imds))"
        );
    }
}
