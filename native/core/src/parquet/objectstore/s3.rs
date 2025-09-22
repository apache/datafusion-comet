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
use url::Url;

use crate::execution::jni_api::get_runtime;
use async_trait::async_trait;
use aws_config::{
    ecs::EcsCredentialsProvider, environment::EnvironmentVariableCredentialsProvider,
    imds::credentials::ImdsCredentialsProvider, meta::credentials::CredentialsProviderChain,
    provider_config::ProviderConfig, sts::AssumeRoleProvider,
    web_identity_token::WebIdentityTokenCredentialsProvider, BehaviorVersion,
};
use aws_credential_types::{
    provider::{error::CredentialsError, ProvideCredentials},
    Credentials,
};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential},
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

    let mut builder = AmazonS3Builder::new()
        .with_url(url.to_string())
        .with_allow_http(true);
    let bucket = url.host_str().ok_or_else(|| object_store::Error::Generic {
        store: "S3",
        source: "Missing bucket name in S3 URL".into(),
    })?;

    let credential_provider =
        get_runtime().block_on(build_credential_provider(configs, bucket, min_ttl))?;
    builder = match credential_provider {
        Some(provider) => builder.with_credentials(Arc::new(provider)),
        None => builder.with_skip_signature(true),
    };

    let s3_configs = extract_s3_config_options(configs, bucket);
    debug!("S3 configs for bucket {bucket}: {s3_configs:?}");

    // When using the default AWS S3 endpoint (no custom endpoint configured), a valid region
    // is required. If no region is explicitly configured, attempt to auto-resolve it by
    // making a HeadBucket request to determine the bucket's region.
    if !s3_configs.contains_key(&AmazonS3ConfigKey::Endpoint)
        && !s3_configs.contains_key(&AmazonS3ConfigKey::Region)
    {
        let region = get_runtime()
            .block_on(resolve_bucket_region(bucket))
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: format!("Failed to resolve region: {e}").into(),
            })?;
        debug!("resolved region: {region:?}");
        builder = builder.with_config(AmazonS3ConfigKey::Region, region.to_string());
    }

    for (key, value) in s3_configs {
        builder = builder.with_config(key, value);
    }

    let object_store = builder.build()?;

    Ok((Box::new(object_store), path))
}

/// Get the bucket region using the [HeadBucket API]. This will fail if the bucket does not exist.
///
/// [HeadBucket API]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
pub async fn resolve_bucket_region(bucket: &str) -> Result<String, Box<dyn Error>> {
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
                source: format!("Bucket not found: {bucket}").into(),
            })
        })?
        .to_str()?
        .to_string();

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

    // Extract and handle path style access (virtual hosted style)
    let mut virtual_hosted_style_request = false;
    if let Some(path_style) = get_config_trimmed(configs, bucket, "path.style.access") {
        virtual_hosted_style_request = path_style.to_lowercase() == "true";
        s3_configs.insert(
            AmazonS3ConfigKey::VirtualHostedStyleRequest,
            virtual_hosted_style_request.to_string(),
        );
    }

    // Extract endpoint configuration and modify if virtual hosted style is enabled
    if let Some(endpoint) = get_config_trimmed(configs, bucket, "endpoint") {
        let normalized_endpoint =
            normalize_endpoint(endpoint, bucket, virtual_hosted_style_request);
        if let Some(endpoint) = normalized_endpoint {
            s3_configs.insert(AmazonS3ConfigKey::Endpoint, endpoint);
        }
    }

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

fn normalize_endpoint(
    endpoint: &str,
    bucket: &str,
    virtual_hosted_style_request: bool,
) -> Option<String> {
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

    if virtual_hosted_style_request {
        if endpoint.ends_with("/") {
            Some(format!("{endpoint}{bucket}"))
        } else {
            Some(format!("{endpoint}/{bucket}"))
        }
    } else {
        Some(endpoint) // Avoid extra to_string() call since endpoint is already a String
    }
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

fn get_config_trimmed<'a>(
    configs: &'a HashMap<String, String>,
    bucket: &str,
    property: &str,
) -> Option<&'a str> {
    get_config(configs, bucket, property).map(|s| s.trim())
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};

    use super::*;

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
    fn test_extract_s3_config_custom_endpoint() {
        let cases = vec![
            ("custom.endpoint.com", "https://custom.endpoint.com"),
            ("https://custom.endpoint.com", "https://custom.endpoint.com"),
            (
                "https://custom.endpoint.com/path/to/resource",
                "https://custom.endpoint.com/path/to/resource",
            ),
        ];
        for (endpoint, configured_endpoint) in cases {
            let mut configs = HashMap::new();
            configs.insert("fs.s3a.endpoint".to_string(), endpoint.to_string());
            let s3_configs = extract_s3_config_options(&configs, "test-bucket");
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::Endpoint),
                Some(&configured_endpoint.to_string())
            );
        }
    }

    #[test]
    fn test_extract_s3_config_custom_endpoint_with_virtual_hosted_style() {
        let cases = vec![
            (
                "custom.endpoint.com",
                "https://custom.endpoint.com/test-bucket",
            ),
            (
                "https://custom.endpoint.com",
                "https://custom.endpoint.com/test-bucket",
            ),
            (
                "https://custom.endpoint.com/",
                "https://custom.endpoint.com/test-bucket",
            ),
            (
                "https://custom.endpoint.com/path/to/resource",
                "https://custom.endpoint.com/path/to/resource/test-bucket",
            ),
            (
                "https://custom.endpoint.com/path/to/resource/",
                "https://custom.endpoint.com/path/to/resource/test-bucket",
            ),
        ];
        for (endpoint, configured_endpoint) in cases {
            let mut configs = HashMap::new();
            configs.insert("fs.s3a.endpoint".to_string(), endpoint.to_string());
            configs.insert("fs.s3a.path.style.access".to_string(), "true".to_string());
            let s3_configs = extract_s3_config_options(&configs, "test-bucket");
            assert_eq!(
                s3_configs.get(&AmazonS3ConfigKey::Endpoint),
                Some(&configured_endpoint.to_string())
            );
        }
    }

    #[test]
    fn test_extract_s3_config_ignore_default_endpoint() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.s3a.endpoint".to_string(),
            "s3.amazonaws.com".to_string(),
        );
        let s3_configs = extract_s3_config_options(&configs, "test-bucket");
        assert!(s3_configs.is_empty());

        configs.insert("fs.s3a.endpoint".to_string(), "".to_string());
        let s3_configs = extract_s3_config_options(&configs, "test-bucket");
        assert!(s3_configs.is_empty());
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
