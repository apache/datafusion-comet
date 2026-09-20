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

//! Construct a `MicrosoftAzure` object store from a Hadoop ABFS configuration.
//!
//! Comet's native scans run outside the JVM, so they bypass Hadoop's
//! `AzureBlobFileSystem` driver entirely. This module bridges the gap by translating the
//! Hadoop `fs.azure.*` configuration namespace (the same keys users already put in
//! `core-site.xml` or `spark.hadoop.*`) into the `object_store` crate's `AzureConfigKey`
//! options and applying them to a `MicrosoftAzureBuilder`.
//!
//! Only `abfs[s]://<container>@<account>.<endpoint-suffix>/<path>` URLs are supported —
//! the same URL shape Spark and Hadoop emit. `wasb[s]://` is not routed here because
//! `object_store::ObjectStoreScheme::parse` does not recognise it (it only accepts
//! `az | adl | azure | abfs | abfss`). The `az` / `azure` / `adl` schemes are also out of
//! scope: `object_store`'s `MicrosoftAzureBuilder::parse_url` treats the URL host as the
//! *container* for those schemes rather than the *account*, which does not match the
//! account-scoped Hadoop key layout that `NativeConfig` forwards.
//!
//! The Hadoop configuration is authoritative for authentication, matching the ABFS driver,
//! which reads no environment variables at all:
//!
//! 1. When the translated Hadoop keys include an auth mechanism (an account key, a SAS
//!    token, a client secret, a federated token file or an MSI endpoint), or the Hadoop
//!    `fs.azure.account.oauth.provider.type` names `MsiTokenProvider`, that mechanism alone
//!    determines the identity and no `AZURE_*` variable is consulted at all. Credentials
//!    are skipped, so an ambient `AZURE_STORAGE_TOKEN` or `AZURE_STORAGE_ACCOUNT_KEY` cannot
//!    outrank the configured identity and an AKS-injected `AZURE_FEDERATED_TOKEN_FILE`
//!    cannot turn a client-secret or MSI principal into workload identity. Transport
//!    settings such as `AZURE_ALLOW_HTTP`, `AZURE_PROXY_URL` or `AZURE_STORAGE_ENDPOINT`
//!    are skipped too, so the environment cannot redirect or intercept it either. A
//!    partial mechanism, such as a token file without a client id and tenant, is not
//!    completed from the environment; `object_store` then falls through its credential
//!    chain to the node's managed identity, so configure the full set in Hadoop.
//! 2. When the Hadoop keys name no mechanism (nothing, or only a client id / tenant /
//!    authority host), the `AZURE_*` variables are applied first and the Hadoop keys on
//!    top. This is what makes AKS Workload Identity work out of the box, including when
//!    Hadoop names the client id and tenant while the webhook supplies the token file.
//!    Transport settings from the environment apply only in this case, alongside the
//!    environment credentials.
//!
//! Within the Hadoop keys, the account-scoped variant
//! (`fs.azure.account.X.<account>.dfs.core.windows.net`) wins over the global one
//! (`fs.azure.account.X`), mirroring Hadoop ABFS's own `AbfsConfiguration` precedence.
//!
//! The translated keys cover the auth schemes that ABFS users actually configure:
//!
//! | Hadoop key (account-scoped suffix omitted)             | `AzureConfigKey`       |
//! | ------------------------------------------------------- | ---------------------- |
//! | `fs.azure.account.key`                                   | `AccessKey`            |
//! | `fs.azure.account.oauth2.client.id`                      | `ClientId`             |
//! | `fs.azure.account.oauth2.client.secret`                  | `ClientSecret`         |
//! | `fs.azure.account.oauth2.client.endpoint`                | `AuthorityId` (from URL) |
//! | `fs.azure.account.oauth2.msi.tenant`                     | `AuthorityId`          |
//! | `fs.azure.account.oauth2.msi.endpoint`                   | `MsiEndpoint`          |
//! | `fs.azure.account.oauth2.msi.authority`                  | `AuthorityHost`        |
//! | `fs.azure.account.oauth2.token.file`                     | `FederatedTokenFile`   |
//! | `fs.azure.sas.<container>.<account>`                     | `SasKey`               |
//!
//! Hadoop keys outside this table are not translated; the URL supplies the account and
//! container.

use log::debug;
use std::collections::HashMap;
use url::Url;

use object_store::{
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    path::Path,
    ObjectStore, ObjectStoreScheme,
};

const HADOOP_KEY: &str = "fs.azure.account.key";
const HADOOP_OAUTH_CLIENT_ID: &str = "fs.azure.account.oauth2.client.id";
const HADOOP_OAUTH_CLIENT_SECRET: &str = "fs.azure.account.oauth2.client.secret";
const HADOOP_OAUTH_CLIENT_ENDPOINT: &str = "fs.azure.account.oauth2.client.endpoint";
const HADOOP_MSI_TENANT: &str = "fs.azure.account.oauth2.msi.tenant";
const HADOOP_MSI_ENDPOINT: &str = "fs.azure.account.oauth2.msi.endpoint";
const HADOOP_MSI_AUTHORITY: &str = "fs.azure.account.oauth2.msi.authority";
const HADOOP_WI_TOKEN_FILE: &str = "fs.azure.account.oauth2.token.file";
const HADOOP_SAS_PREFIX: &str = "fs.azure.sas.";
const HADOOP_OAUTH_PROVIDER_TYPE: &str = "fs.azure.account.oauth.provider.type";
/// Simple class name of Hadoop's `org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider`.
const HADOOP_MSI_PROVIDER_CLASS: &str = "MsiTokenProvider";

const ENDPOINT_SUFFIXES: &[&str] = &["dfs.core.windows.net", "blob.core.windows.net"];

/// Environment variable object_store's `from_env` reads for the managed identity endpoint.
const MSI_ENDPOINT_ENV_KEY: &str = "IDENTITY_ENDPOINT";
const AZURE_ENV_PREFIX: &str = "AZURE_";

/// Build a `MicrosoftAzure` `ObjectStore` for `url` using `configs`.
///
/// `url` must use the `abfs[s]://` scheme; the returned `Path` is the URL's resource path
/// (container-relative), suitable for direct use with `ObjectStore::get`.
pub fn create_store(
    url: &Url,
    configs: &HashMap<String, String>,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    if scheme != ObjectStoreScheme::MicrosoftAzure {
        return Err(object_store::Error::Generic {
            store: "MicrosoftAzure",
            source: format!("Scheme of URL is not Azure: {url}").into(),
        });
    }
    let path = Path::parse(path)?;

    let account = extract_account(url);
    let container = extract_container(url);

    let translated = translate_hadoop_configs(configs, account.as_deref(), container.as_deref());
    debug!(
        "Azure configs for account={:?}, container={:?}: keys={:?}",
        account,
        container,
        translated
            .iter()
            .map(|(k, _)| k.as_ref())
            .collect::<Vec<_>>()
    );

    let provider_type =
        account_scoped_value(configs, HADOOP_OAUTH_PROVIDER_TYPE, account.as_deref());
    let store = build_builder(url, &translated, provider_type.as_deref(), env_pairs()).build()?;
    Ok((Box::new(store), path))
}

/// Process environment as UTF-8 `(key, value)` pairs, skipping entries that are not UTF-8.
fn env_pairs() -> impl Iterator<Item = (String, String)> {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.to_str()?.to_string(), v.to_str()?.to_string())))
}

/// Assemble the builder from the environment, the URL and the translated Hadoop keys.
///
/// When the Hadoop keys configure an auth mechanism the environment is not consulted at
/// all, so nothing ambient can outrank, combine with or redirect the configured identity.
/// Otherwise the environment is read the way `MicrosoftAzureBuilder::from_env` reads it.
/// `provider_type` is the resolved `fs.azure.account.oauth.provider.type`, if any.
fn build_builder(
    url: &Url,
    translated: &[(AzureConfigKey, String)],
    provider_type: Option<&str>,
    env: impl Iterator<Item = (String, String)>,
) -> MicrosoftAzureBuilder {
    let mut builder = MicrosoftAzureBuilder::new();
    if !hadoop_auth_present(translated, provider_type) {
        builder = apply_env(builder, env);
    }
    builder = builder.with_url(url.to_string());
    for (key, value) in translated {
        builder = builder.with_config(*key, value.clone());
    }
    builder
}

/// Whether the Hadoop configuration selects an authentication mechanism, either through a
/// translated credential key or through an `MsiTokenProvider` provider type, whose
/// endpoint Hadoop defaults to IMDS without any translated key.
///
/// `ClientId`, `AuthorityId` and `AuthorityHost` only name an identity and leave the token
/// source open, which keeps AKS Workload Identity working when Hadoop names the principal
/// and the webhook supplies `AZURE_FEDERATED_TOKEN_FILE`. A partial mechanism, such as a
/// token file alone, is not completed from the environment; Hadoop needs the full set too.
fn hadoop_auth_present(
    translated: &[(AzureConfigKey, String)],
    provider_type: Option<&str>,
) -> bool {
    let has_mechanism_key = translated.iter().any(|(key, _)| {
        matches!(
            key,
            AzureConfigKey::AccessKey
                | AzureConfigKey::SasKey
                | AzureConfigKey::FederatedTokenFile
                | AzureConfigKey::ClientSecret
                | AzureConfigKey::MsiEndpoint
        )
    });
    has_mechanism_key || provider_type.is_some_and(|p| p.ends_with(HADOOP_MSI_PROVIDER_CLASS))
}

/// Apply the environment to `builder` the way `MicrosoftAzureBuilder::from_env` does:
/// every `AZURE_*` variable that parses as an `AzureConfigKey`, then the MSI endpoint
/// variable, which is applied last so it wins over `AZURE_MSI_ENDPOINT` regardless of the
/// order the environment is iterated in.
fn apply_env(
    mut builder: MicrosoftAzureBuilder,
    env: impl Iterator<Item = (String, String)>,
) -> MicrosoftAzureBuilder {
    let mut msi_endpoint: Option<String> = None;
    for (key, value) in env {
        if key == MSI_ENDPOINT_ENV_KEY {
            msi_endpoint = Some(value);
            continue;
        }
        if !key.starts_with(AZURE_ENV_PREFIX) {
            continue;
        }
        if let Ok(config_key) = key.to_ascii_lowercase().parse::<AzureConfigKey>() {
            builder = builder.with_config(config_key, value);
        }
    }
    if let Some(endpoint) = msi_endpoint {
        builder = builder.with_msi_endpoint(endpoint);
    }
    builder
}

/// Translate a Hadoop ABFS configuration map into `(AzureConfigKey, value)` pairs.
///
/// `account` and `container` are extracted from the URL and used to resolve account-scoped
/// keys (`fs.azure.X.<account>.<endpoint-suffix>`) and the SAS namespace
/// (`fs.azure.sas.<container>.<account>`). Account-scoped keys win over global ones.
fn translate_hadoop_configs(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
) -> Vec<(AzureConfigKey, String)> {
    let mut out: Vec<(AzureConfigKey, String)> = Vec::new();

    let mappings: &[(&str, AzureConfigKey)] = &[
        (HADOOP_KEY, AzureConfigKey::AccessKey),
        (HADOOP_OAUTH_CLIENT_ID, AzureConfigKey::ClientId),
        (HADOOP_OAUTH_CLIENT_SECRET, AzureConfigKey::ClientSecret),
        (HADOOP_MSI_TENANT, AzureConfigKey::AuthorityId),
        (HADOOP_MSI_ENDPOINT, AzureConfigKey::MsiEndpoint),
        (HADOOP_MSI_AUTHORITY, AzureConfigKey::AuthorityHost),
        (HADOOP_WI_TOKEN_FILE, AzureConfigKey::FederatedTokenFile),
    ];

    for (hadoop_base, azure_key) in mappings {
        if let Some(value) = account_scoped_value(configs, hadoop_base, account) {
            out.push((*azure_key, value));
        }
    }

    // `fs.azure.account.oauth2.client.endpoint` is a full token URL of the form
    // `https://login.microsoftonline.com/<tenant>/oauth2/token`. object_store wants the
    // tenant id directly (`AuthorityId`), so extract it if AuthorityId hasn't already
    // been set from `fs.azure.account.oauth2.msi.tenant`.
    let has_authority_id = out
        .iter()
        .any(|(k, _)| matches!(k, AzureConfigKey::AuthorityId));
    if !has_authority_id {
        if let Some(endpoint) = account_scoped_value(configs, HADOOP_OAUTH_CLIENT_ENDPOINT, account)
        {
            if let Some(tenant) = tenant_from_oauth_endpoint(&endpoint) {
                out.push((AzureConfigKey::AuthorityId, tenant));
            }
        }
    }

    // SAS tokens are scoped to `fs.azure.sas.<container>.<account>[.<endpoint-suffix>]`.
    if let (Some(container), Some(account)) = (container, account) {
        if let Some(sas) = sas_value(configs, container, account) {
            out.push((AzureConfigKey::SasKey, sas));
        }
    }

    out
}

/// Look up `base_key`, preferring account-scoped variants.
///
/// Probes (in order): `<base>.<account>.<endpoint-suffix>`, `<base>.<account>`,
/// then the unscoped `<base>`. Returns the first hit.
fn account_scoped_value(
    configs: &HashMap<String, String>,
    base_key: &str,
    account: Option<&str>,
) -> Option<String> {
    if let Some(acc) = account {
        for suffix in ENDPOINT_SUFFIXES {
            let scoped = format!("{base_key}.{acc}.{suffix}");
            if let Some(v) = configs.get(&scoped) {
                return Some(v.clone());
            }
        }
        let bare = format!("{base_key}.{acc}");
        if let Some(v) = configs.get(&bare) {
            return Some(v.clone());
        }
    }
    configs.get(base_key).cloned()
}

/// Resolve the SAS token for `(container, account)`, accepting any of the
/// `fs.azure.sas.<container>.<account>[.<endpoint-suffix>]` variants.
fn sas_value(configs: &HashMap<String, String>, container: &str, account: &str) -> Option<String> {
    for suffix in ENDPOINT_SUFFIXES {
        let key = format!("{HADOOP_SAS_PREFIX}{container}.{account}.{suffix}");
        if let Some(v) = configs.get(&key) {
            return Some(v.clone());
        }
    }
    let bare = format!("{HADOOP_SAS_PREFIX}{container}.{account}");
    configs.get(&bare).cloned()
}

/// Extract the storage account name from an `abfs[s]://` URL.
///
/// ABFS hostnames are `<account>.<endpoint-suffix>` (e.g. `myacct.dfs.core.windows.net`),
/// so the account is the first label of the host.
fn extract_account(url: &Url) -> Option<String> {
    let host = url.host_str()?;
    host.split('.').next().map(str::to_string)
}

/// Extract the container name from an `abfs[s]://` URL.
///
/// ABFS encodes the container as the URL user-info (`container@account.dfs...`).
fn extract_container(url: &Url) -> Option<String> {
    let user = url.username();
    if user.is_empty() {
        return None;
    }
    Some(user.to_string())
}

/// Pull the tenant id out of an OAuth token endpoint like
/// `https://login.microsoftonline.com/<tenant>/oauth2/token`.
fn tenant_from_oauth_endpoint(endpoint: &str) -> Option<String> {
    let parsed = Url::parse(endpoint).ok()?;
    let mut segments = parsed.path_segments()?;
    let tenant = segments.next()?;
    if tenant.is_empty() {
        return None;
    }
    Some(tenant.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ClientConfigKey;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn extracts_account_and_container_from_abfss_url() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        assert_eq!(extract_account(&u).as_deref(), Some("myacct"));
        assert_eq!(extract_container(&u).as_deref(), Some("data"));
    }

    #[test]
    fn account_scoped_key_takes_precedence_over_global() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.account.oauth2.client.id".into(),
            "global-client".into(),
        );
        configs.insert(
            "fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net".into(),
            "scoped-client".into(),
        );
        assert_eq!(
            account_scoped_value(&configs, HADOOP_OAUTH_CLIENT_ID, Some("myacct")).as_deref(),
            Some("scoped-client"),
        );
    }

    #[test]
    fn account_scoped_lookup_falls_back_to_global() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.account.oauth2.client.id".into(),
            "global-client".into(),
        );
        assert_eq!(
            account_scoped_value(&configs, HADOOP_OAUTH_CLIENT_ID, Some("myacct")).as_deref(),
            Some("global-client"),
        );
    }

    #[test]
    fn translates_workload_identity_keys() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net".into(),
            "client-123".into(),
        );
        configs.insert(
            "fs.azure.account.oauth2.msi.tenant.myacct.dfs.core.windows.net".into(),
            "tenant-abc".into(),
        );
        configs.insert(
            "fs.azure.account.oauth2.token.file.myacct.dfs.core.windows.net".into(),
            "/var/run/secrets/azure/tokens/azure-identity-token".into(),
        );
        let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));

        let by_key: HashMap<_, _> = translated.into_iter().collect();
        assert_eq!(
            by_key.get(&AzureConfigKey::ClientId).map(String::as_str),
            Some("client-123")
        );
        assert_eq!(
            by_key.get(&AzureConfigKey::AuthorityId).map(String::as_str),
            Some("tenant-abc")
        );
        assert_eq!(
            by_key
                .get(&AzureConfigKey::FederatedTokenFile)
                .map(String::as_str),
            Some("/var/run/secrets/azure/tokens/azure-identity-token")
        );
    }

    #[test]
    fn translates_account_key() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.account.key.myacct.blob.core.windows.net".into(),
            "secret==".into(),
        );
        let translated = translate_hadoop_configs(&configs, Some("myacct"), None);
        let by_key: HashMap<_, _> = translated.into_iter().collect();
        assert_eq!(
            by_key.get(&AzureConfigKey::AccessKey).map(String::as_str),
            Some("secret==")
        );
    }

    #[test]
    fn translates_sas_token_for_container() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.sas.data.myacct.dfs.core.windows.net".into(),
            "sv=2020-08-04&sig=xyz".into(),
        );
        let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));
        let by_key: HashMap<_, _> = translated.into_iter().collect();
        assert_eq!(
            by_key.get(&AzureConfigKey::SasKey).map(String::as_str),
            Some("sv=2020-08-04&sig=xyz")
        );
    }

    #[test]
    fn derives_tenant_from_oauth_endpoint_when_msi_tenant_is_absent() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.account.oauth2.client.endpoint".into(),
            "https://login.microsoftonline.com/00000000-1111-2222-3333-444444444444/oauth2/token"
                .into(),
        );
        let translated = translate_hadoop_configs(&configs, Some("myacct"), None);
        let by_key: HashMap<_, _> = translated.into_iter().collect();
        assert_eq!(
            by_key.get(&AzureConfigKey::AuthorityId).map(String::as_str),
            Some("00000000-1111-2222-3333-444444444444")
        );
    }

    #[test]
    fn msi_tenant_wins_over_oauth_endpoint_tenant() {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.azure.account.oauth2.msi.tenant".into(),
            "from-msi".into(),
        );
        configs.insert(
            "fs.azure.account.oauth2.client.endpoint".into(),
            "https://login.microsoftonline.com/from-endpoint/oauth2/token".into(),
        );
        let translated = translate_hadoop_configs(&configs, Some("myacct"), None);
        let by_key: HashMap<_, _> = translated.into_iter().collect();
        assert_eq!(
            by_key.get(&AzureConfigKey::AuthorityId).map(String::as_str),
            Some("from-msi")
        );
    }

    #[test]
    fn create_store_succeeds_with_workload_identity_configs() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let mut configs = HashMap::new();
        // A typical Workload Identity setup: client id, tenant id, and federated token
        // file are all present in the Hadoop configuration.
        configs.insert(
            "fs.azure.account.oauth2.client.id".into(),
            "client-123".into(),
        );
        configs.insert(
            "fs.azure.account.oauth2.msi.tenant".into(),
            "tenant-abc".into(),
        );
        configs.insert(
            "fs.azure.account.oauth2.token.file".into(),
            "/var/run/secrets/azure/tokens/azure-identity-token".into(),
        );
        let (_store, path) = create_store(&u, &configs).expect("store builds");
        assert_eq!(path.as_ref(), "path/file.parquet");
    }

    #[test]
    fn create_store_succeeds_with_hadoop_account_key_only() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        // `build()` base64-decodes the account key, so this one must be valid base64.
        let configs = hadoop(&[(
            "fs.azure.account.key",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        )]);
        let (_store, path) = create_store(&u, &configs).expect("store builds");
        assert_eq!(path.as_ref(), "path/file.parquet");
    }

    #[test]
    fn create_store_rejects_non_azure_scheme() {
        let u = url("s3://bucket/file.parquet");
        let configs = HashMap::new();
        let err = create_store(&u, &configs).expect_err("must fail");
        assert!(
            format!("{err}").contains("Scheme of URL is not Azure"),
            "unexpected error: {err}"
        );
    }

    fn env_of(pairs: &[(&str, &str)]) -> impl Iterator<Item = (String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn hadoop(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn builder_for(
        hadoop: &HashMap<String, String>,
        env: &[(&str, &str)],
    ) -> MicrosoftAzureBuilder {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let translated = translate_hadoop_configs(hadoop, Some("myacct"), Some("data"));
        let provider = account_scoped_value(hadoop, HADOOP_OAUTH_PROVIDER_TYPE, Some("myacct"));
        build_builder(&u, &translated, provider.as_deref(), env_of(env))
    }

    fn value(builder: &MicrosoftAzureBuilder, key: AzureConfigKey) -> Option<String> {
        builder.get_config_value(&key)
    }

    const CLIENT_SECRET_PRINCIPAL: &[(&str, &str)] = &[
        ("fs.azure.account.oauth2.client.id", "hadoop-client"),
        ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
        ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
    ];

    #[test]
    fn env_bearer_token_is_ignored_when_hadoop_sets_account_key() {
        let configs = hadoop(&[("fs.azure.account.key", "secret==")]);
        let builder = builder_for(&configs, &[("AZURE_STORAGE_TOKEN", "ambient")]);
        assert_eq!(value(&builder, AzureConfigKey::Token), None);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("secret==")
        );
    }

    #[test]
    fn env_account_key_is_ignored_when_hadoop_sets_client_secret_principal() {
        let configs = hadoop(CLIENT_SECRET_PRINCIPAL);
        let builder = builder_for(&configs, &[("AZURE_STORAGE_ACCOUNT_KEY", "ambient==")]);
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientSecret).as_deref(),
            Some("hadoop-secret")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::ClientId).as_deref(),
            Some("hadoop-client")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AuthorityId).as_deref(),
            Some("hadoop-tenant")
        );
    }

    #[test]
    fn env_federated_token_file_is_ignored_when_hadoop_sets_client_secret_principal() {
        let configs = hadoop(CLIENT_SECRET_PRINCIPAL);
        let builder = builder_for(
            &configs,
            &[(
                "AZURE_FEDERATED_TOKEN_FILE",
                "/var/run/secrets/azure/tokens/token",
            )],
        );
        assert_eq!(value(&builder, AzureConfigKey::FederatedTokenFile), None);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientSecret).as_deref(),
            Some("hadoop-secret")
        );
    }

    #[test]
    fn env_workload_identity_is_used_when_hadoop_has_no_auth() {
        let configs = hadoop(&[]);
        let builder = builder_for(
            &configs,
            &[
                ("AZURE_CLIENT_ID", "env-client"),
                ("AZURE_TENANT_ID", "env-tenant"),
                (
                    "AZURE_FEDERATED_TOKEN_FILE",
                    "/var/run/secrets/azure/tokens/token",
                ),
            ],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::ClientId).as_deref(),
            Some("env-client")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AuthorityId).as_deref(),
            Some("env-tenant")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::FederatedTokenFile).as_deref(),
            Some("/var/run/secrets/azure/tokens/token")
        );
    }

    #[test]
    fn hadoop_client_id_and_tenant_still_accept_env_federated_token_file() {
        let configs = hadoop(&[
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ]);
        let builder = builder_for(
            &configs,
            &[
                ("AZURE_CLIENT_ID", "env-client"),
                (
                    "AZURE_FEDERATED_TOKEN_FILE",
                    "/var/run/secrets/azure/tokens/token",
                ),
            ],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::FederatedTokenFile).as_deref(),
            Some("/var/run/secrets/azure/tokens/token")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::ClientId).as_deref(),
            Some("hadoop-client")
        );
    }

    #[test]
    fn hadoop_token_file_alone_does_not_borrow_env_client_id_or_tenant() {
        let configs = hadoop(&[(
            "fs.azure.account.oauth2.token.file",
            "/var/run/secrets/azure/tokens/token",
        )]);
        let builder = builder_for(
            &configs,
            &[
                ("AZURE_CLIENT_ID", "env-client"),
                ("AZURE_TENANT_ID", "env-tenant"),
            ],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::FederatedTokenFile).as_deref(),
            Some("/var/run/secrets/azure/tokens/token")
        );
        assert_eq!(value(&builder, AzureConfigKey::ClientId), None);
        assert_eq!(value(&builder, AzureConfigKey::AuthorityId), None);
    }

    #[test]
    fn hadoop_msi_provider_type_counts_as_a_mechanism() {
        let configs = hadoop(&[
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
            ),
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ]);
        let builder = builder_for(
            &configs,
            &[(
                "AZURE_FEDERATED_TOKEN_FILE",
                "/var/run/secrets/azure/tokens/token",
            )],
        );
        assert_eq!(value(&builder, AzureConfigKey::FederatedTokenFile), None);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientId).as_deref(),
            Some("hadoop-client")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AuthorityId).as_deref(),
            Some("hadoop-tenant")
        );
    }

    #[test]
    fn hadoop_workload_identity_provider_type_still_accepts_env_token_file() {
        let configs = hadoop(&[
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
            ),
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ]);
        let builder = builder_for(
            &configs,
            &[(
                "AZURE_FEDERATED_TOKEN_FILE",
                "/var/run/secrets/azure/tokens/token",
            )],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::FederatedTokenFile).as_deref(),
            Some("/var/run/secrets/azure/tokens/token")
        );
    }

    #[test]
    fn identity_endpoint_env_wins_over_azure_msi_endpoint() {
        let configs = hadoop(&[]);
        let pairs = [
            (MSI_ENDPOINT_ENV_KEY, "http://identity.internal/msi"),
            ("AZURE_MSI_ENDPOINT", "http://azure.internal/msi"),
        ];
        let mut reversed = pairs;
        reversed.reverse();
        for env in [pairs, reversed] {
            let builder = builder_for(&configs, &env);
            assert_eq!(
                value(&builder, AzureConfigKey::MsiEndpoint).as_deref(),
                Some("http://identity.internal/msi"),
                "env order: {env:?}"
            );
        }
    }

    const TRANSPORT_ENV: &[(&str, &str)] = &[
        ("AZURE_ALLOW_HTTP", "true"),
        ("AZURE_PROXY_URL", "http://proxy.internal:3128"),
        ("AZURE_STORAGE_ENDPOINT", "http://127.0.0.1:10000/myacct"),
        ("AZURE_STORAGE_USE_EMULATOR", "true"),
    ];

    #[test]
    fn env_transport_options_are_ignored_when_hadoop_auth_present() {
        let configs = hadoop(&[("fs.azure.account.key", "secret==")]);
        let builder = builder_for(&configs, TRANSPORT_ENV);
        // `AllowHttp` and `UseEmulator` are boolean-backed and read back "false" when unset.
        assert_eq!(
            value(&builder, AzureConfigKey::Client(ClientConfigKey::AllowHttp)).as_deref(),
            Some("false")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::Client(ClientConfigKey::ProxyUrl)),
            None
        );
        assert_eq!(value(&builder, AzureConfigKey::Endpoint), None);
        assert_eq!(
            value(&builder, AzureConfigKey::UseEmulator).as_deref(),
            Some("false")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("secret==")
        );
    }

    #[test]
    fn env_transport_options_apply_when_hadoop_has_no_auth() {
        let configs = hadoop(&[]);
        let builder = builder_for(&configs, TRANSPORT_ENV);
        assert_eq!(
            value(&builder, AzureConfigKey::Client(ClientConfigKey::AllowHttp)).as_deref(),
            Some("true")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::Client(ClientConfigKey::ProxyUrl)).as_deref(),
            Some("http://proxy.internal:3128")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::Endpoint).as_deref(),
            Some("http://127.0.0.1:10000/myacct")
        );
    }

    #[test]
    fn env_msi_endpoint_is_applied_when_hadoop_has_no_auth() {
        let configs = hadoop(&[]);
        let builder = builder_for(
            &configs,
            &[(MSI_ENDPOINT_ENV_KEY, "http://169.254.169.254/msi")],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::MsiEndpoint).as_deref(),
            Some("http://169.254.169.254/msi")
        );
    }

    #[test]
    fn env_msi_endpoint_is_dropped_when_hadoop_auth_present() {
        let configs = hadoop(&[("fs.azure.account.key", "secret==")]);
        let builder = builder_for(
            &configs,
            &[(MSI_ENDPOINT_ENV_KEY, "http://169.254.169.254/msi")],
        );
        assert_eq!(value(&builder, AzureConfigKey::MsiEndpoint), None);
    }
}
