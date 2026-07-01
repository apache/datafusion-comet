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

//! Construct a `MicrosoftAzure` object store from a Hadoop ABFS/WASB configuration.
//!
//! Comet's native scans run outside the JVM, so they bypass Hadoop's
//! `AzureBlobFileSystem` driver entirely. This module bridges the gap by translating the
//! Hadoop `fs.azure.*` configuration namespace (the same keys users already put in
//! `core-site.xml` or `spark.hadoop.*`) into the `object_store` crate's `AzureConfigKey`
//! options, then layering them on top of any AZURE_* environment variables that AKS's
//! Workload Identity webhook injects.
//!
//! Supported authentication, in the priority order applied to the builder:
//!
//! 1. `MicrosoftAzureBuilder::from_env()` — picks up `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
//!    `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST`, `AZURE_STORAGE_*`, etc.
//!    This is what makes Workload Identity work out of the box in AKS pods.
//! 2. Account-scoped Hadoop keys (`fs.azure.account.X.<account>.dfs.core.windows.net`).
//! 3. Global Hadoop keys (`fs.azure.account.X`).
//!
//! Items 2 and 3 are forwarded via `MicrosoftAzureBuilder::with_config`, which overrides
//! whatever `from_env()` produced. The account-scoped variant wins over the global one,
//! mirroring Hadoop ABFS's own `AbfsConfiguration` precedence.
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
//! Anything beyond these falls through to whatever `from_env()` or the URL itself provided.

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

const ENDPOINT_SUFFIXES: &[&str] = &[
    "dfs.core.windows.net",
    "blob.core.windows.net",
];

/// Build a `MicrosoftAzure` `ObjectStore` for `url` using `configs`.
///
/// The returned `Path` is the URL's resource path (container-relative for ABFS / WASB,
/// container+key for `az://`), suitable for direct use with `ObjectStore::get`.
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

    // Start from the environment so AKS Workload Identity (AZURE_CLIENT_ID,
    // AZURE_TENANT_ID, AZURE_FEDERATED_TOKEN_FILE, AZURE_AUTHORITY_HOST) and any
    // explicit AZURE_STORAGE_* variables are honoured without further configuration.
    // `with_url` then fills in account/container from the URL itself.
    let mut builder = MicrosoftAzureBuilder::from_env().with_url(url.to_string());

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
    for (key, value) in translated {
        builder = builder.with_config(key, value);
    }

    let store = builder.build()?;
    Ok((Box::new(store), path))
}

/// Translate a Hadoop ABFS/WASB configuration map into `(AzureConfigKey, value)` pairs.
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

/// Extract the storage account name from an Azure URL.
///
/// Handles ABFS/WASB hostnames of the form `<account>.<endpoint-suffix>` and the
/// shorter `az://<account>/...` form.
fn extract_account(url: &Url) -> Option<String> {
    let host = url.host_str()?;
    match url.scheme() {
        "az" | "azure" | "adl" => Some(host.to_string()),
        _ => host.split('.').next().map(str::to_string),
    }
}

/// Extract the container name from an Azure URL.
///
/// ABFS/WASB encode the container as the URL user-info (`container@account.dfs...`);
/// `az://account/container/...` encodes it as the first path segment.
fn extract_container(url: &Url) -> Option<String> {
    let user = url.username();
    if !user.is_empty() {
        return Some(user.to_string());
    }
    match url.scheme() {
        "az" | "azure" | "adl" => url
            .path_segments()
            .and_then(|mut segs| segs.next())
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        _ => None,
    }
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
    fn extracts_account_and_container_from_az_url() {
        let u = url("az://myacct/data/path/file.parquet");
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
        // file are all present, and `from_env()` will additionally pick them up from the
        // AZURE_* env vars in production.
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
    fn create_store_rejects_non_azure_scheme() {
        let u = url("s3://bucket/file.parquet");
        let configs = HashMap::new();
        let err = create_store(&u, &configs).expect_err("must fail");
        assert!(
            format!("{err}").contains("Scheme of URL is not Azure"),
            "unexpected error: {err}"
        );
    }
}
