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

//! Construct a `MicrosoftAzure` object store from the ABFS authentication the driver resolved.
//!
//! Comet's native scans run outside the JVM, so they cannot use Hadoop's `AzureBlobFileSystem`.
//! Which mechanism applies to an `abfs[s]://<container>@<account>/<path>` URL, and which values
//! it needs, is decided on the Spark driver by `AbfsAuthResolver`, which asks the classpath's
//! hadoop-azure the same questions `AzureBlobFileSystemStore.initializeClient` asks. Account-,
//! container- and globally-scoped keys, credential providers and `${...}` references are all
//! Hadoop's business there. This module trusts that answer and only maps it onto
//! `MicrosoftAzureBuilder`. The answer arrives in the object store options as `comet.azure.*`
//! markers plus, for a resolved mechanism, the values under their global Hadoop key names.
//!
//! `comet.azure.resolution` decides everything:
//!
//! - `resolved`: `MicrosoftAzureBuilder::new()` plus exactly the keys the mechanism owns.
//! - `none`: `MicrosoftAzureBuilder::from_env()`, the only path in Comet's own code that reads
//!   `AZURE_*` or `IDENTITY_ENDPOINT`. (object_store's IMDS provider still reads
//!   `IDENTITY_HEADER` at token-fetch time under a resolved managed identity; that is its
//!   behaviour, not a Comet lookup.)
//! - `declined`: an error naming the auth type and the provider class Hadoop configured.
//! - `error`: an error carrying the driver's exception class and message.
//! - missing or anything else: an error, so a plan that did not run the resolver cannot pass.
//!
//! `comet.azure.auth.type` is Hadoop's `AuthType` name, `comet.azure.oauth.provider.class` the
//! OAuth provider class, `comet.azure.sas.provider.class` a custom SAS provider and
//! `comet.azure.oauth.assertion.provider.class` a Workload Identity client assertion provider.
//! Under `resolved` each mechanism maps the keys its Hadoop provider constructor reads:
//!
//! | Mechanism | Hadoop key (global form) | `AzureConfigKey` |
//! | --- | --- | --- |
//! | `SharedKey` | `fs.azure.account.key` | `AccessKey` |
//! | `ClientCredsTokenProvider` | `fs.azure.account.oauth2.client.id` | `ClientId` |
//! |  | `fs.azure.account.oauth2.client.secret` | `ClientSecret` |
//! |  | `fs.azure.account.oauth2.client.endpoint` | `AuthorityId`, `AuthorityHost` |
//! | `MsiTokenProvider` | `fs.azure.account.oauth2.msi.endpoint` | `MsiEndpoint` |
//! |  | `fs.azure.account.oauth2.client.id` | `ClientId` (optional) |
//! |  | `fs.azure.account.oauth2.msi.tenant` | `AuthorityId` (optional) |
//! |  | `fs.azure.account.oauth2.msi.authority` | `AuthorityHost` (optional) |
//! | `WorkloadIdentityTokenProvider` | `fs.azure.account.oauth2.client.id` | `ClientId` |
//! |  | `fs.azure.account.oauth2.msi.tenant` | `AuthorityId` |
//! |  | `fs.azure.account.oauth2.token.file` | `FederatedTokenFile` |
//! |  | `fs.azure.account.oauth2.msi.authority` | `AuthorityHost` |
//! | `SAS` (fixed token) | `fs.azure.sas.fixed.token` | `SasKey` |
//!
//! An optional key that is absent or blank counts as unset, as Hadoop's `MsiTokenProvider` adds
//! the client id and tenant to its request only when they are non-empty. The MSI tenant and
//! authority are accepted but unused: object_store's IMDS provider reads only the client id and
//! endpoint. `RefreshTokenBasedTokenProvider` and `UserPasswordTokenProvider` have no
//! `object_store` counterpart and fail as unsupported. A value a mechanism needs but that is
//! absent (or, for the SAS token and the client credentials endpoint, unusable) is an error
//! naming the key; other `fs.azure.*` keys are ignored. Values are applied as forwarded (Hadoop
//! already trimmed and defaulted them), except that the trailing slash Hadoop appends to
//! `msi.authority` is removed because `object_store` joins `<authority host>/<tenant>/...`
//! itself. No error message or log line carries a value, only key names.

use log::debug;
use std::collections::HashMap;
use url::{Position, Url};

use object_store::{
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    path::Path,
    ObjectStore, ObjectStoreScheme,
};

const MARKER_RESOLUTION: &str = "comet.azure.resolution";
const MARKER_AUTH_TYPE: &str = "comet.azure.auth.type";
const MARKER_OAUTH_PROVIDER_CLASS: &str = "comet.azure.oauth.provider.class";
const MARKER_OAUTH_ASSERTION_PROVIDER_CLASS: &str = "comet.azure.oauth.assertion.provider.class";
const MARKER_SAS_PROVIDER_CLASS: &str = "comet.azure.sas.provider.class";
const MARKER_ERROR_CLASS: &str = "comet.azure.error.class";
const MARKER_ERROR_MESSAGE: &str = "comet.azure.error.message";

const AUTH_TYPE_SHARED_KEY: &str = "SharedKey";
const AUTH_TYPE_OAUTH: &str = "OAuth";
const AUTH_TYPE_SAS: &str = "SAS";

const PROVIDER_CLIENT_CREDS: &str = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";
const PROVIDER_MSI: &str = "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider";
const PROVIDER_WORKLOAD_IDENTITY: &str =
    "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider";
const PROVIDER_REFRESH_TOKEN: &str =
    "org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider";
const PROVIDER_USER_PASSWORD: &str =
    "org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider";

const HADOOP_KEY: &str = "fs.azure.account.key";
const HADOOP_OAUTH_CLIENT_ID: &str = "fs.azure.account.oauth2.client.id";
const HADOOP_OAUTH_CLIENT_SECRET: &str = "fs.azure.account.oauth2.client.secret";
const HADOOP_OAUTH_CLIENT_ENDPOINT: &str = "fs.azure.account.oauth2.client.endpoint";
const HADOOP_MSI_TENANT: &str = "fs.azure.account.oauth2.msi.tenant";
const HADOOP_MSI_ENDPOINT: &str = "fs.azure.account.oauth2.msi.endpoint";
const HADOOP_MSI_AUTHORITY: &str = "fs.azure.account.oauth2.msi.authority";
const HADOOP_WI_TOKEN_FILE: &str = "fs.azure.account.oauth2.token.file";
const HADOOP_SAS_FIXED_TOKEN: &str = "fs.azure.sas.fixed.token";

type Settings = Vec<(AzureConfigKey, String)>;

/// Build a `MicrosoftAzure` `ObjectStore` for `url` from the driver's resolved authentication.
///
/// `url` must use the `abfs[s]://` scheme; the returned `Path` is the URL's resource path
/// (container-relative), suitable for direct use with `ObjectStore::get`.
pub fn create_store(
    url: &Url,
    configs: &HashMap<String, String>,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    if scheme != ObjectStoreScheme::MicrosoftAzure {
        return Err(config_error(format!("Scheme of URL is not Azure: {url}")));
    }
    let path = Path::parse(path)?;
    let store = builder_for(url, configs)?.build()?;
    Ok((Box::new(store), path))
}

/// The builder the markers in `configs` call for. Only `none` touches the environment.
fn builder_for(
    url: &Url,
    configs: &HashMap<String, String>,
) -> Result<MicrosoftAzureBuilder, object_store::Error> {
    let marker = |key: &str| configs.get(key).map(String::as_str);
    match marker(MARKER_RESOLUTION) {
        Some("none") => {
            debug!("Azure authentication for {url}: none configured in Hadoop, reading AZURE_*");
            Ok(MicrosoftAzureBuilder::from_env().with_url(url.to_string()))
        }
        Some("resolved") => {
            let settings = resolved_settings(configs)?;
            debug!(
                "Azure authentication for {url}: {} with keys {:?}",
                marker(MARKER_AUTH_TYPE).unwrap_or_default(),
                settings.iter().map(|(k, _)| k.as_ref()).collect::<Vec<_>>()
            );
            let builder = MicrosoftAzureBuilder::new().with_url(url.to_string());
            Ok(settings
                .into_iter()
                .fold(builder, |b, (key, value)| b.with_config(key, value)))
        }
        Some("declined") => {
            let class = marker(MARKER_OAUTH_ASSERTION_PROVIDER_CLASS)
                .or_else(|| marker(MARKER_SAS_PROVIDER_CLASS))
                .or_else(|| marker(MARKER_OAUTH_PROVIDER_CLASS));
            Err(unsupported(
                marker(MARKER_AUTH_TYPE).unwrap_or("<unknown>"),
                class,
            ))
        }
        Some("error") => Err(config_error(format!(
            "Hadoop ABFS authentication failed on the driver for {}://{}: {}: {}",
            url.scheme(),
            &url[Position::BeforeUsername..Position::AfterPort],
            marker(MARKER_ERROR_CLASS).unwrap_or("<unknown>"),
            marker(MARKER_ERROR_MESSAGE).unwrap_or("<unknown>"),
        ))),
        Some(_) => Err(config_error(format!(
            "unknown `{MARKER_RESOLUTION}` value; the native ABFS scan needs the driver's Hadoop \
             resolution"
        ))),
        None => Err(config_error(format!(
            "`{MARKER_RESOLUTION}` is missing; the native ABFS scan needs the driver's Hadoop \
             resolution"
        ))),
    }
}

/// The builder settings for the mechanism the auth type and provider class markers name: the
/// keys its Hadoop provider constructor reads, and nothing else.
fn resolved_settings(configs: &HashMap<String, String>) -> Result<Settings, object_store::Error> {
    let marker = |key: &str| configs.get(key).map(String::as_str);
    let auth_type = marker(MARKER_AUTH_TYPE)
        .ok_or_else(|| config_error(format!("`{MARKER_AUTH_TYPE}` is missing")))?;
    if let Some(class) =
        marker(MARKER_OAUTH_ASSERTION_PROVIDER_CLASS).or_else(|| marker(MARKER_SAS_PROVIDER_CLASS))
    {
        return Err(unsupported(auth_type, Some(class)));
    }
    match (auth_type, marker(MARKER_OAUTH_PROVIDER_CLASS)) {
        (AUTH_TYPE_SHARED_KEY, _) => Ok(vec![(
            AzureConfigKey::AccessKey,
            required(configs, HADOOP_KEY)?,
        )]),
        (AUTH_TYPE_SAS, _) => {
            let token = required(configs, HADOOP_SAS_FIXED_TOKEN)?;
            if token.is_empty() {
                return Err(config_error(format!("`{HADOOP_SAS_FIXED_TOKEN}` is empty")));
            }
            Ok(vec![(AzureConfigKey::SasKey, token)])
        }
        (AUTH_TYPE_OAUTH, Some(PROVIDER_CLIENT_CREDS)) => client_creds_settings(configs),
        (AUTH_TYPE_OAUTH, Some(PROVIDER_MSI)) => msi_settings(configs),
        (AUTH_TYPE_OAUTH, Some(PROVIDER_WORKLOAD_IDENTITY)) => workload_identity_settings(configs),
        (AUTH_TYPE_OAUTH, Some(class @ (PROVIDER_REFRESH_TOKEN | PROVIDER_USER_PASSWORD))) => {
            Err(unsupported(auth_type, Some(class)))
        }
        (AUTH_TYPE_OAUTH, Some(_)) => Err(config_error(format!(
            "unknown `{MARKER_OAUTH_PROVIDER_CLASS}` for the native ABFS scan"
        ))),
        (AUTH_TYPE_OAUTH, None) => Err(config_error(format!(
            "`{MARKER_OAUTH_PROVIDER_CLASS}` is missing"
        ))),
        _ => Err(config_error(format!(
            "unknown `{MARKER_AUTH_TYPE}` for the native ABFS scan"
        ))),
    }
}

/// `ClientCredsTokenProvider` posts to the endpoint itself, so the endpoint supplies both the
/// tenant and the authority host; an endpoint without either is an error, not a default.
fn client_creds_settings(
    configs: &HashMap<String, String>,
) -> Result<Settings, object_store::Error> {
    let endpoint = required(configs, HADOOP_OAUTH_CLIENT_ENDPOINT)?;
    let (tenant, host) = oauth_endpoint_parts(&endpoint).ok_or_else(|| {
        config_error(format!(
            "`{HADOOP_OAUTH_CLIENT_ENDPOINT}` has no `scheme://host` origin or no tenant segment \
             before `oauth2`"
        ))
    })?;
    Ok(vec![
        (
            AzureConfigKey::ClientId,
            required(configs, HADOOP_OAUTH_CLIENT_ID)?,
        ),
        (
            AzureConfigKey::ClientSecret,
            required(configs, HADOOP_OAUTH_CLIENT_SECRET)?,
        ),
        (AzureConfigKey::AuthorityId, tenant),
        (AzureConfigKey::AuthorityHost, host),
    ])
}

/// `MsiTokenProvider`: the endpoint is required (Hadoop defaults it); the identity is optional.
fn msi_settings(configs: &HashMap<String, String>) -> Result<Settings, object_store::Error> {
    let mut out = vec![(
        AzureConfigKey::MsiEndpoint,
        required(configs, HADOOP_MSI_ENDPOINT)?,
    )];
    out.extend(optional(
        configs,
        HADOOP_OAUTH_CLIENT_ID,
        AzureConfigKey::ClientId,
    ));
    out.extend(optional(
        configs,
        HADOOP_MSI_TENANT,
        AzureConfigKey::AuthorityId,
    ));
    out.extend(optional(
        configs,
        HADOOP_MSI_AUTHORITY,
        AzureConfigKey::AuthorityHost,
    ));
    Ok(out)
}

/// `WorkloadIdentityTokenProvider`: every key is mandatory or defaulted in Hadoop.
fn workload_identity_settings(
    configs: &HashMap<String, String>,
) -> Result<Settings, object_store::Error> {
    Ok(vec![
        (
            AzureConfigKey::ClientId,
            required(configs, HADOOP_OAUTH_CLIENT_ID)?,
        ),
        (
            AzureConfigKey::AuthorityId,
            required(configs, HADOOP_MSI_TENANT)?,
        ),
        (
            AzureConfigKey::FederatedTokenFile,
            required(configs, HADOOP_WI_TOKEN_FILE)?,
        ),
        (
            AzureConfigKey::AuthorityHost,
            authority_host(&required(configs, HADOOP_MSI_AUTHORITY)?),
        ),
    ])
}

fn required(configs: &HashMap<String, String>, key: &str) -> Result<String, object_store::Error> {
    configs.get(key).cloned().ok_or_else(|| {
        config_error(format!(
            "`{key}` is missing from the resolved ABFS authentication"
        ))
    })
}

/// An optional key, where blank counts as unset (`MsiTokenProvider` sends a client id or tenant
/// only when it is non-empty, untrimmed). The authority loses Hadoop's trailing slash.
fn optional(
    configs: &HashMap<String, String>,
    key: &str,
    azure_key: AzureConfigKey,
) -> Option<(AzureConfigKey, String)> {
    let value = configs.get(key).filter(|v| !v.is_empty())?;
    let value = match azure_key {
        AzureConfigKey::AuthorityHost => authority_host(value),
        _ => value.clone(),
    };
    Some((azure_key, value))
}

/// Hadoop appends a slash; object_store joins `<authority host>/<tenant>/oauth2/v2.0/token`.
fn authority_host(authority: &str) -> String {
    authority.trim_end_matches('/').to_string()
}

fn config_error(message: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "MicrosoftAzure",
        source: message.into(),
    }
}

fn unsupported(auth_type: &str, class: Option<&str>) -> object_store::Error {
    let provider = class
        .map(|c| format!(" with provider {c}"))
        .unwrap_or_default();
    config_error(format!(
        "unsupported authentication mechanism for the native ABFS scan: auth type \
         {auth_type}{provider}"
    ))
}

/// The tenant id and authority host of an OAuth token endpoint like
/// `https://login.microsoftonline.com/<tenant>/oauth2/token`. The tenant is the path segment
/// before the last `oauth2`, or the first segment when no later segment is `oauth2`, so a
/// prefix that itself contains `oauth2` is kept. The authority host is the origin plus every
/// segment before the tenant, so object_store's `<authority host>/<tenant>/oauth2/v2.0/token`
/// keeps a proxy's path prefix. `None` when the endpoint has no tenant or no `scheme://host`
/// origin.
fn oauth_endpoint_parts(endpoint: &str) -> Option<(String, String)> {
    let parsed = Url::parse(endpoint).ok()?;
    let segments: Vec<&str> = parsed.path_segments()?.collect();
    let tenant_at = segments
        .iter()
        .rposition(|segment| *segment == "oauth2")
        .filter(|&at| at > 0)
        .map_or(0, |at| at - 1);
    let tenant = segments.get(tenant_at).filter(|t| !t.is_empty())?;
    let origin = parsed.origin();
    if !origin.is_tuple() {
        return None;
    }
    let mut host = origin.ascii_serialization();
    for segment in &segments[..tenant_at] {
        host.push('/');
        host.push_str(segment);
    }
    Some((tenant.to_string(), host))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody,
        HttpService,
    };
    use object_store::ClientOptions;
    use std::sync::Mutex;

    const URL: &str = "abfss://data@myacct.dfs.core.windows.net/path/file.parquet";
    /// Marks every credential value; no error may echo it.
    const SECRET: &str = "SECRET-VALUE-MARKER";
    /// A valid base64 account key, since `build()` decodes it.
    const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const DEFAULT_ENDPOINT: &str = "https://login.microsoftonline.com/hadoop-tenant/oauth2/token";
    const DEFAULT_MSI_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
    const DEFAULT_AUTHORITY: &str = "https://login.microsoftonline.com/";
    const PUBLIC_CLOUD: &str = "https://login.microsoftonline.com";
    const TOKEN_FILE: &str = "/var/run/secrets/azure/tokens/azure-identity-token";
    const ENV_KEY: &str = "AZURE_STORAGE_ACCOUNT_KEY";
    const ENV_CLIENT_ID: &str = "AZURE_CLIENT_ID";
    const UNSUPPORTED: &str = "unsupported authentication mechanism";

    /// Serializes the tests that mutate the process environment.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn url() -> Url {
        Url::parse(URL).unwrap()
    }

    type Configs = HashMap<String, String>;

    fn configs(pairs: &[(&str, &str)]) -> Configs {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn with(mut configs: Configs, key: &str, value: &str) -> Configs {
        configs.insert(key.into(), value.into());
        configs
    }

    fn without(mut configs: Configs, key: &str) -> Configs {
        configs.remove(key);
        configs
    }

    fn resolved(auth_type: &str, pairs: &[(&str, &str)]) -> Configs {
        let out = with(configs(pairs), MARKER_RESOLUTION, "resolved");
        with(out, MARKER_AUTH_TYPE, auth_type)
    }

    fn shared_key() -> Configs {
        resolved(AUTH_TYPE_SHARED_KEY, &[(HADOOP_KEY, SECRET)])
    }

    fn oauth(provider: &str, pairs: &[(&str, &str)]) -> Configs {
        with(
            resolved(AUTH_TYPE_OAUTH, pairs),
            MARKER_OAUTH_PROVIDER_CLASS,
            provider,
        )
    }

    fn client_creds() -> Configs {
        oauth(
            PROVIDER_CLIENT_CREDS,
            &[
                (HADOOP_OAUTH_CLIENT_ID, "hadoop-client"),
                (HADOOP_OAUTH_CLIENT_SECRET, SECRET),
                (HADOOP_OAUTH_CLIENT_ENDPOINT, DEFAULT_ENDPOINT),
            ],
        )
    }

    fn msi() -> Configs {
        oauth(
            PROVIDER_MSI,
            &[
                (HADOOP_MSI_ENDPOINT, DEFAULT_MSI_ENDPOINT),
                (HADOOP_MSI_AUTHORITY, DEFAULT_AUTHORITY),
            ],
        )
    }

    fn workload_identity() -> Configs {
        oauth(
            PROVIDER_WORKLOAD_IDENTITY,
            &[
                (HADOOP_OAUTH_CLIENT_ID, "hadoop-client"),
                (HADOOP_MSI_TENANT, "hadoop-tenant"),
                (HADOOP_WI_TOKEN_FILE, TOKEN_FILE),
                (HADOOP_MSI_AUTHORITY, DEFAULT_AUTHORITY),
            ],
        )
    }

    fn builder(configs: &Configs) -> MicrosoftAzureBuilder {
        builder_for(&url(), configs).expect("builder")
    }

    fn value(builder: &MicrosoftAzureBuilder, key: AzureConfigKey) -> Option<String> {
        builder.get_config_value(&key)
    }

    /// Every credential-bearing key the builder could receive, for "nothing else was set".
    const CREDENTIAL_KEYS: &[AzureConfigKey] = &[
        AzureConfigKey::AccessKey,
        AzureConfigKey::SasKey,
        AzureConfigKey::ClientId,
        AzureConfigKey::ClientSecret,
        AzureConfigKey::AuthorityId,
        AzureConfigKey::AuthorityHost,
        AzureConfigKey::MsiEndpoint,
        AzureConfigKey::FederatedTokenFile,
        AzureConfigKey::Token,
    ];

    fn assert_exactly(configs: &Configs, expected: &[(AzureConfigKey, &str)]) {
        let builder = builder(configs);
        for key in CREDENTIAL_KEYS {
            let want = expected.iter().find(|(k, _)| k == key).map(|(_, v)| *v);
            assert_eq!(value(&builder, *key).as_deref(), want, "{key:?}");
        }
    }

    /// The error `configs` produce, which must not echo the secret marker.
    fn err(configs: &Configs) -> String {
        let err = builder_for(&url(), configs)
            .err()
            .map(|e| e.to_string())
            .expect("an error");
        assert!(!err.contains(SECRET), "value echoed: {err}");
        err
    }

    #[test]
    fn shared_key_sets_only_the_access_key_and_ignores_other_hadoop_keys() {
        assert_exactly(&shared_key(), &[(AzureConfigKey::AccessKey, SECRET)]);
        let stray = with(shared_key(), HADOOP_SAS_FIXED_TOKEN, "sv=1&sig=x");
        let stray = with(stray, HADOOP_OAUTH_CLIENT_ID, "stray");
        let stray = with(stray, "fs.azure.account.hns.enabled", "true");
        assert_exactly(&stray, &[(AzureConfigKey::AccessKey, SECRET)]);
    }

    #[test]
    fn create_store_checks_the_scheme_and_returns_the_container_path() {
        let configs = with(shared_key(), HADOOP_KEY, KEY);
        let (_store, path) = create_store(&url(), &configs).expect("store builds");
        assert_eq!(path.as_ref(), "path/file.parquet");
        let s3 = Url::parse("s3://bucket/file.parquet").unwrap();
        let err = create_store(&s3, &configs).unwrap_err().to_string();
        assert!(err.contains("Scheme of URL is not Azure"), "{err}");
    }

    #[test]
    fn client_creds_take_tenant_and_authority_host_from_the_endpoint_alone() {
        assert_exactly(
            &client_creds(),
            &[
                (AzureConfigKey::ClientId, "hadoop-client"),
                (AzureConfigKey::ClientSecret, SECRET),
                (AzureConfigKey::AuthorityId, "hadoop-tenant"),
                (AzureConfigKey::AuthorityHost, PUBLIC_CLOUD),
            ],
        );
        // `msi.*` keys from a shared configuration are not read under this provider.
        let proxied = with(client_creds(), HADOOP_MSI_TENANT, "other-tenant");
        let proxied = with(
            proxied,
            HADOOP_MSI_AUTHORITY,
            "https://login.chinacloudapi.cn/",
        );
        let proxied = with(
            proxied,
            HADOOP_OAUTH_CLIENT_ENDPOINT,
            "https://auth-proxy.example:8443/aad/hadoop-tenant/oauth2/v2.0/token",
        );
        let builder = builder(&proxied);
        assert_eq!(
            value(&builder, AzureConfigKey::AuthorityId).as_deref(),
            Some("hadoop-tenant")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AuthorityHost).as_deref(),
            Some("https://auth-proxy.example:8443/aad")
        );
        // An endpoint without a tenant segment or without a `scheme://host` origin cannot
        // fall through to another credential or to the default authority host.
        for endpoint in [
            "https://auth-proxy.example/",
            "file:///hadoop-tenant/oauth2/token",
        ] {
            let err = err(&with(
                client_creds(),
                HADOOP_OAUTH_CLIENT_ENDPOINT,
                endpoint,
            ));
            assert!(
                err.contains(HADOOP_OAUTH_CLIENT_ENDPOINT),
                "{endpoint}: {err}"
            );
        }
    }

    /// Records the request URLs a store sends so the token request is observed offline.
    #[derive(Debug, Default, Clone)]
    struct RecordingConnector(std::sync::Arc<Mutex<Vec<String>>>);

    #[async_trait::async_trait]
    impl HttpService for RecordingConnector {
        async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            self.0.lock().unwrap().push(req.uri().to_string());
            Ok(http::Response::builder()
                .status(400)
                .body(HttpResponseBody::from(String::new()))
                .unwrap())
        }
    }

    impl HttpConnector for RecordingConnector {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    #[tokio::test]
    async fn client_creds_token_requests_go_to_the_endpoint_host() {
        // Hadoop posts to `client.endpoint` itself, so a proxy mounted under a path keeps that
        // path in the authority host and the tenant is the segment before the last `oauth2`.
        for (endpoint, token_url) in [
            (
                DEFAULT_ENDPOINT,
                "https://login.microsoftonline.com/hadoop-tenant/oauth2/v2.0/token",
            ),
            (
                "https://auth-proxy.example/gateway/oauth2/hadoop-tenant/oauth2/v2.0/token",
                "https://auth-proxy.example/gateway/oauth2/hadoop-tenant/oauth2/v2.0/token",
            ),
        ] {
            let configs = with(client_creds(), HADOOP_OAUTH_CLIENT_ENDPOINT, endpoint);
            let recorder = RecordingConnector::default();
            let store = builder(&configs)
                .with_http_connector(recorder.clone())
                .build()
                .expect("store builds");
            assert!(store.credentials().get_credential().await.is_err());
            let urls = recorder.0.lock().unwrap();
            assert_eq!(
                urls.first().map(String::as_str),
                Some(token_url),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn msi_sets_the_endpoint_with_an_optional_identity_and_authority() {
        assert_exactly(
            &msi(),
            &[
                (AzureConfigKey::MsiEndpoint, DEFAULT_MSI_ENDPOINT),
                (AzureConfigKey::AuthorityHost, PUBLIC_CLOUD),
            ],
        );
        let user_assigned = with(msi(), HADOOP_OAUTH_CLIENT_ID, "user-assigned");
        let user_assigned = with(user_assigned, HADOOP_MSI_TENANT, "hadoop-tenant");
        assert_exactly(
            &user_assigned,
            &[
                (AzureConfigKey::MsiEndpoint, DEFAULT_MSI_ENDPOINT),
                (AzureConfigKey::AuthorityHost, PUBLIC_CLOUD),
                (AzureConfigKey::ClientId, "user-assigned"),
                (AzureConfigKey::AuthorityId, "hadoop-tenant"),
            ],
        );
        // Hadoop forwards a blank client id or tenant as "" and sends neither; the authority is
        // defaulted in Hadoop but unused by object_store's IMDS provider, so it may be absent.
        let blank = with(msi(), HADOOP_OAUTH_CLIENT_ID, "");
        let blank = with(blank, HADOOP_MSI_TENANT, "");
        let blank = without(blank, HADOOP_MSI_AUTHORITY);
        assert_exactly(
            &blank,
            &[(AzureConfigKey::MsiEndpoint, DEFAULT_MSI_ENDPOINT)],
        );
    }

    #[test]
    fn workload_identity_sets_client_tenant_token_file_and_authority() {
        assert_exactly(
            &workload_identity(),
            &[
                (AzureConfigKey::ClientId, "hadoop-client"),
                (AzureConfigKey::AuthorityId, "hadoop-tenant"),
                (AzureConfigKey::FederatedTokenFile, TOKEN_FILE),
                (AzureConfigKey::AuthorityHost, PUBLIC_CLOUD),
            ],
        );
        // Hadoop appends a slash to the authority; object_store joins `<host>/<tenant>/...`.
        for (authority, host) in [
            (
                "https://login.chinacloudapi.cn/",
                "https://login.chinacloudapi.cn",
            ),
            (
                "https://login.chinacloudapi.cn",
                "https://login.chinacloudapi.cn",
            ),
            (
                "https://auth-proxy.example/aad/",
                "https://auth-proxy.example/aad",
            ),
        ] {
            let configs = with(workload_identity(), HADOOP_MSI_AUTHORITY, authority);
            assert_eq!(
                value(&builder(&configs), AzureConfigKey::AuthorityHost).as_deref(),
                Some(host),
                "{authority}"
            );
        }
    }

    #[test]
    fn fixed_sas_token_is_forwarded_verbatim() {
        for token in [
            "sv=2020-08-04&sig=xyz",
            "?sv=2020-08-04&sig=xyz",
            " sv=1&sig=x ",
        ] {
            let configs = resolved(AUTH_TYPE_SAS, &[(HADOOP_SAS_FIXED_TOKEN, token)]);
            assert_exactly(&configs, &[(AzureConfigKey::SasKey, token)]);
            // `split_sas` trims the leading `?`, so the store builds from either spelling.
            create_store(&url(), &configs).unwrap_or_else(|e| panic!("{token:?}: {e}"));
        }
        // An empty token would make object_store send unsigned requests.
        let err = err(&resolved(AUTH_TYPE_SAS, &[(HADOOP_SAS_FIXED_TOKEN, "")]));
        assert!(err.contains(HADOOP_SAS_FIXED_TOKEN), "{err}");
    }

    #[test]
    fn unsupported_mechanisms_fail_naming_the_auth_type_and_class() {
        let custom = "com.example.CustomProvider";
        // (resolution, auth type, marker carrying the class, class)
        let cases = [
            // Declined on the driver, where the class was never instantiated either.
            ("declined", "OAuth", MARKER_OAUTH_PROVIDER_CLASS, custom),
            (
                "declined",
                "OAuth",
                MARKER_OAUTH_ASSERTION_PROVIDER_CLASS,
                custom,
            ),
            ("declined", "SAS", MARKER_SAS_PROVIDER_CLASS, custom),
            ("declined", "Custom", "", ""),
            ("declined", "UserboundSASWithOAuth", "", ""),
            // Resolved by Hadoop to a provider without an object_store counterpart.
            (
                "resolved",
                "OAuth",
                MARKER_OAUTH_PROVIDER_CLASS,
                PROVIDER_REFRESH_TOKEN,
            ),
            (
                "resolved",
                "OAuth",
                MARKER_OAUTH_PROVIDER_CLASS,
                PROVIDER_USER_PASSWORD,
            ),
        ];
        for (resolution, auth_type, marker, class) in cases {
            let mut configs = configs(&[
                (MARKER_RESOLUTION, resolution),
                (MARKER_AUTH_TYPE, auth_type),
                (HADOOP_OAUTH_CLIENT_ID, SECRET),
            ]);
            if marker == MARKER_OAUTH_ASSERTION_PROVIDER_CLASS {
                // The assertion provider rides beside the Workload Identity class.
                configs = with(
                    configs,
                    MARKER_OAUTH_PROVIDER_CLASS,
                    PROVIDER_WORKLOAD_IDENTITY,
                );
            }
            if !marker.is_empty() {
                configs = with(configs, marker, class);
            }
            let err = err(&configs);
            assert!(err.contains(UNSUPPORTED), "{err}");
            assert!(err.contains(auth_type) && err.contains(class), "{err}");
        }
    }

    #[test]
    fn bad_or_missing_markers_fail_closed_naming_the_marker() {
        let cases = [
            (configs(&[(HADOOP_KEY, SECRET)]), MARKER_RESOLUTION),
            (
                with(shared_key(), MARKER_RESOLUTION, "Resolved"),
                MARKER_RESOLUTION,
            ),
            (without(shared_key(), MARKER_AUTH_TYPE), MARKER_AUTH_TYPE),
            (
                resolved("Kerberos", &[(HADOOP_KEY, SECRET)]),
                MARKER_AUTH_TYPE,
            ),
            (resolved(AUTH_TYPE_OAUTH, &[]), MARKER_OAUTH_PROVIDER_CLASS),
            (
                oauth("com.example.MyTokenProvider", &[]),
                MARKER_OAUTH_PROVIDER_CLASS,
            ),
        ];
        for (configs, marker) in cases {
            let err = err(&configs);
            assert!(err.contains(marker), "{marker}: {err}");
        }
    }

    #[test]
    fn a_required_key_missing_under_resolved_fails_naming_it() {
        let cases: Vec<(Configs, &[&str])> = vec![
            (shared_key(), &[HADOOP_KEY]),
            (
                resolved(AUTH_TYPE_SAS, &[(HADOOP_SAS_FIXED_TOKEN, SECRET)]),
                &[HADOOP_SAS_FIXED_TOKEN],
            ),
            (
                client_creds(),
                &[
                    HADOOP_OAUTH_CLIENT_ID,
                    HADOOP_OAUTH_CLIENT_SECRET,
                    HADOOP_OAUTH_CLIENT_ENDPOINT,
                ],
            ),
            (msi(), &[HADOOP_MSI_ENDPOINT]),
            (
                workload_identity(),
                &[
                    HADOOP_OAUTH_CLIENT_ID,
                    HADOOP_MSI_TENANT,
                    HADOOP_WI_TOKEN_FILE,
                    HADOOP_MSI_AUTHORITY,
                ],
            ),
        ];
        for (complete, required) in cases {
            builder(&complete);
            for key in required {
                let err = err(&without(complete.clone(), key));
                assert!(err.contains(key), "{key}: {err}");
            }
        }
    }

    #[test]
    fn error_marker_surfaces_the_driver_failure() {
        let configs = configs(&[
            (MARKER_RESOLUTION, "error"),
            (
                MARKER_ERROR_CLASS,
                "org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException",
            ),
            (MARKER_ERROR_MESSAGE, "Failure to initialize configuration"),
        ]);
        assert_eq!(
            err(&configs),
            "Generic MicrosoftAzure error: Hadoop ABFS authentication failed on the driver for \
             abfss://data@myacct.dfs.core.windows.net: \
             org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException: \
             Failure to initialize configuration"
        );
    }

    #[test]
    fn only_none_reads_the_environment() {
        let _guard = lock_env();
        std::env::set_var(ENV_KEY, KEY);
        std::env::set_var(ENV_CLIENT_ID, "ambient-client");
        let from_env = builder_for(&url(), &configs(&[(MARKER_RESOLUTION, "none")]));
        // Stray Hadoop values beside `none` are not read either.
        let from_env_with_stray = builder_for(
            &url(),
            &configs(&[(MARKER_RESOLUTION, "none"), (HADOOP_KEY, SECRET)]),
        );
        let sas = resolved(AUTH_TYPE_SAS, &[(HADOOP_SAS_FIXED_TOKEN, "sv=1&sig=x")]);
        let mechanisms = [
            ("shared_key", shared_key(), Some(SECRET), None),
            ("client_creds", client_creds(), None, Some("hadoop-client")),
            ("msi", msi(), None, None),
            (
                "workload_identity",
                workload_identity(),
                None,
                Some("hadoop-client"),
            ),
            ("sas", sas, None, None),
        ];
        let resolved: Vec<_> = mechanisms
            .iter()
            .map(|(name, configs, key, client)| {
                (*name, builder_for(&url(), configs), *key, *client)
            })
            .collect();
        let failing = [
            configs(&[
                (MARKER_RESOLUTION, "declined"),
                (MARKER_AUTH_TYPE, "Custom"),
            ]),
            configs(&[(MARKER_RESOLUTION, "error")]),
            configs(&[(HADOOP_KEY, SECRET)]),
        ];
        let results: Vec<Result<(), object_store::Error>> = failing
            .iter()
            .map(|configs| create_store(&url(), configs).map(|_| ()))
            .collect();
        std::env::remove_var(ENV_KEY);
        std::env::remove_var(ENV_CLIENT_ID);

        for builder in [from_env, from_env_with_stray] {
            let builder = builder.expect("builder");
            assert_eq!(
                value(&builder, AzureConfigKey::AccessKey).as_deref(),
                Some(KEY)
            );
            assert_eq!(
                value(&builder, AzureConfigKey::ClientId).as_deref(),
                Some("ambient-client")
            );
        }
        for (name, builder, key, client) in resolved {
            let builder = builder.expect(name);
            assert_eq!(
                value(&builder, AzureConfigKey::AccessKey).as_deref(),
                key,
                "{name}"
            );
            assert_eq!(
                value(&builder, AzureConfigKey::ClientId).as_deref(),
                client,
                "{name}"
            );
        }
        for (configs, result) in failing.iter().zip(results) {
            let err = result.err().map(|e| e.to_string()).expect("an error");
            assert!(!err.contains(KEY), "{configs:?}: {err}");
        }
    }

    #[test]
    fn oauth_endpoint_parts_split_tenant_and_authority_host() {
        fn parts(tenant: &'static str, host: &'static str) -> Option<(&'static str, &'static str)> {
            Some((tenant, host))
        }
        for (endpoint, expected) in [
            (
                "https://user:pass@auth-proxy.example/t/oauth2/token",
                parts("t", "https://auth-proxy.example"),
            ),
            (
                "https://auth-proxy.example:443/t/oauth2/token",
                parts("t", "https://auth-proxy.example"),
            ),
            (
                "https://auth-proxy.example:8443/t/oauth2/token",
                parts("t", "https://auth-proxy.example:8443"),
            ),
            (
                "https://[::1]:8443/t/oauth2/token",
                parts("t", "https://[::1]:8443"),
            ),
            (
                "https://auth-proxy.example/aad/t/oauth2/v2.0/token",
                parts("t", "https://auth-proxy.example/aad"),
            ),
            (
                "https://auth-proxy.example:8443/a/b/t/oauth2/v2.0/token",
                parts("t", "https://auth-proxy.example:8443/a/b"),
            ),
            // No `oauth2` after the first segment: the first segment is the tenant.
            (
                "https://auth-proxy.example/aad/t/token",
                parts("aad", "https://auth-proxy.example"),
            ),
            (
                "https://auth-proxy.example/oauth2/token",
                parts("oauth2", "https://auth-proxy.example"),
            ),
            ("https://h//t/oauth2/token", parts("t", "https://h/")),
            ("https://h/a//oauth2/token", None),
            (
                "https://h/aad/t/oauth2/v2.0/token?q=oauth2#f",
                parts("t", "https://h/aad"),
            ),
            (
                "https://h/a/oauth2/b/oauth2/token",
                parts("b", "https://h/a/oauth2"),
            ),
            (
                "https://h/u@evil.example/t/oauth2/token",
                parts("t", "https://h/u@evil.example"),
            ),
            // The URL parser resolves `%2e%2e` and reads `\` as `/`; the host stays `h`.
            ("https://h/a/%2e%2e/t/oauth2/token", parts("t", "https://h")),
            ("https://h/a\\b/t/oauth2/token", parts("t", "https://h/a/b")),
            (
                "https://h/oauth2/t/oauth2/token",
                parts("t", "https://h/oauth2"),
            ),
            // An opaque origin has no host to post to.
            ("file:///t/oauth2/token", None),
            ("https://auth-proxy.example/", None),
            ("urn:t:oauth2", None),
            ("not a url", None),
        ] {
            let actual = oauth_endpoint_parts(endpoint);
            assert_eq!(
                actual
                    .as_ref()
                    .map(|(tenant, host)| (tenant.as_str(), host.as_str())),
                expected,
                "{endpoint}"
            );
        }
    }
}
