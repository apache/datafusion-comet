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
//!    partial mechanism, such as a token file or client secret without a client id and
//!    tenant, is an error rather than being completed from the environment: Hadoop rejects
//!    it too, and `object_store` would otherwise fall through its credential chain to the
//!    node's managed identity. A blank SAS token or credential value is an error for the
//!    same reason: `object_store` would send unsigned requests or fall through the chain.
//! 2. When the Hadoop keys name nothing at all, the `AZURE_*` variables are applied first
//!    and the Hadoop keys on top. This is what makes AKS Workload Identity work out of the
//!    box. Transport settings from the environment apply only in this case, alongside the
//!    environment credentials. When the Hadoop keys name only a principal (a client id,
//!    tenant or authority host, or the `WorkloadIdentityTokenProvider` class with the
//!    client id and tenant), only `AZURE_FEDERATED_TOKEN_FILE` is read, and only when
//!    `fs.azure.account.oauth2.token.file` is not set; no other variable is consulted, so
//!    an ambient account key or bearer token cannot outrank the named principal. A
//!    principal with no token file in either place is an error naming both sources, and
//!    a borrowed token file still needs the client id and tenant from Hadoop.
//! 3. An explicit `fs.azure.account.auth.type` is Hadoop choosing a mechanism, so it is
//!    validated against the translated keys: `SharedKey` needs the account key, `OAuth`
//!    needs a provider the scan can build (the MSI, Workload Identity or client
//!    credentials provider classes, or no class with a secret or token file), `SAS` needs
//!    a SAS token, `Custom` and any other value are errors. The auth type also decides
//!    which keys are read, as it does for Hadoop's `initializeClient`: only the selected
//!    mechanism's keys are translated and validated, so an unused global OAuth secret
//!    under an account-scoped `SharedKey`, or an account key under `OAuth`, is ignored
//!    rather than rejected or handed to the builder. With no auth type at all, Hadoop's
//!    `getAuthType` defaults to `SharedKey`, so an account key entry (blank or not)
//!    selects that mechanism the same way and only the key is read; a blank key is still
//!    an error. With neither an auth type nor a key, every mechanism's keys are read, as
//!    described in points 1 and 2. A provider class is validated whenever it is set,
//!    provided OAuth is the mechanism read (with or without an explicit auth type): MSI
//!    stands alone, Workload Identity needs the client id and tenant, client credentials
//!    need the secret, and any other class is an error. Keys that select a mechanism with
//!    no native counterpart (a SAS token provider class, an account key provider class, a
//!    refresh token, or a user name or password) are errors too. In every one of these
//!    cases the environment is not consulted either, so the scan can neither borrow an
//!    ambient identity nor fall back to managed identity in place of the one Hadoop was
//!    told to use.
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
//! | `fs.azure.sas.fixed.token`                               | `SasKey` (lower priority) |
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
const HADOOP_SAS_FIXED_TOKEN: &str = "fs.azure.sas.fixed.token";
const HADOOP_OAUTH_PROVIDER_TYPE: &str = "fs.azure.account.oauth.provider.type";
/// Simple class names of the `org.apache.hadoop.fs.azurebfs.oauth2` token providers the
/// native scan can satisfy.
const HADOOP_MSI_PROVIDER_CLASS: &str = "MsiTokenProvider";
const HADOOP_WI_PROVIDER_CLASS: &str = "WorkloadIdentityTokenProvider";
const HADOOP_CLIENT_CREDS_PROVIDER_CLASS: &str = "ClientCredsTokenProvider";
const HADOOP_AUTH_TYPE: &str = "fs.azure.account.auth.type";
/// Hadoop credential keys, the `AzureConfigKey` each translates to and the mechanism each
/// belongs to.
const HADOOP_CREDENTIAL_MAPPINGS: &[(&str, AzureConfigKey, AuthMechanism)] = &[
    (
        HADOOP_KEY,
        AzureConfigKey::AccessKey,
        AuthMechanism::SharedKey,
    ),
    (
        HADOOP_OAUTH_CLIENT_ID,
        AzureConfigKey::ClientId,
        AuthMechanism::OAuth,
    ),
    (
        HADOOP_OAUTH_CLIENT_SECRET,
        AzureConfigKey::ClientSecret,
        AuthMechanism::OAuth,
    ),
    (
        HADOOP_MSI_TENANT,
        AzureConfigKey::AuthorityId,
        AuthMechanism::OAuth,
    ),
    (
        HADOOP_MSI_ENDPOINT,
        AzureConfigKey::MsiEndpoint,
        AuthMechanism::OAuth,
    ),
    (
        HADOOP_MSI_AUTHORITY,
        AzureConfigKey::AuthorityHost,
        AuthMechanism::OAuth,
    ),
    (
        HADOOP_WI_TOKEN_FILE,
        AzureConfigKey::FederatedTokenFile,
        AuthMechanism::OAuth,
    ),
];
/// Hadoop keys that each select an auth mechanism with no native counterpart, and the
/// mechanism each belongs to.
const HADOOP_UNSUPPORTED_MECHANISM_KEYS: &[(&str, AuthMechanism)] = &[
    ("fs.azure.sas.token.provider.type", AuthMechanism::Sas),
    ("fs.azure.account.keyprovider", AuthMechanism::SharedKey),
    (
        "fs.azure.account.oauth2.refresh.token",
        AuthMechanism::OAuth,
    ),
    ("fs.azure.account.oauth2.user.name", AuthMechanism::OAuth),
    (
        "fs.azure.account.oauth2.user.password",
        AuthMechanism::OAuth,
    ),
];

/// The authentication mechanisms Hadoop's `fs.azure.account.auth.type` can select that
/// the native scan can build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthMechanism {
    SharedKey,
    OAuth,
    Sas,
}

/// The mechanism an explicit `fs.azure.account.auth.type` selects for the account, the way
/// Hadoop's `AbfsConfiguration.getAuthType` resolves it: the account-scoped key over the
/// global one. `None` when no auth type is set, or when it names something the scan cannot
/// build (`auth_type_problem` reports that).
fn explicit_mechanism(
    configs: &HashMap<String, String>,
    account: Option<&str>,
) -> Option<AuthMechanism> {
    let value = account_scoped_value(configs, HADOOP_AUTH_TYPE, account)?;
    let auth_type = value.trim();
    if auth_type.eq_ignore_ascii_case("SharedKey") {
        Some(AuthMechanism::SharedKey)
    } else if auth_type.eq_ignore_ascii_case("OAuth") {
        Some(AuthMechanism::OAuth)
    } else if auth_type.eq_ignore_ascii_case("SAS") {
        Some(AuthMechanism::Sas)
    } else {
        None
    }
}

/// The mechanism Hadoop selects for the account: the explicit auth type when one is set,
/// otherwise `SharedKey` when an account key entry is present, since that is the default
/// `AbfsConfiguration.getAuthType` falls back to. `None` when no auth type is set and no
/// key is present, in which case every Hadoop key is read. A blank key still counts as
/// present, so it is reported as blank rather than skipped in favour of another mechanism.
fn selected_mechanism(
    configs: &HashMap<String, String>,
    account: Option<&str>,
) -> Option<AuthMechanism> {
    explicit_mechanism(configs, account).or_else(|| {
        account_scoped_entry(configs, HADOOP_KEY, account).map(|_| AuthMechanism::SharedKey)
    })
}

/// Whether Hadoop reads the keys of `mechanism` for this account: every mechanism when
/// none is selected, otherwise only the selected one. Hadoop's
/// `AzureBlobFileSystemStore.initializeClient` reads only the selected mechanism's keys,
/// so an unused one (a global OAuth secret under an account-scoped `SharedKey`, or beside
/// an account key with no auth type) is neither translated nor validated.
fn mechanism_is_read(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    mechanism: AuthMechanism,
) -> bool {
    selected_mechanism(configs, account).is_none_or(|selected| selected == mechanism)
}

/// The OAuth provider class entry, but only while Hadoop is reading OAuth keys.
fn active_provider_class(
    configs: &HashMap<String, String>,
    account: Option<&str>,
) -> Option<(String, String)> {
    if !mechanism_is_read(configs, account, AuthMechanism::OAuth) {
        return None;
    }
    account_scoped_entry(configs, HADOOP_OAUTH_PROVIDER_TYPE, account)
}

const ENDPOINT_SUFFIXES: &[&str] = &["dfs.core.windows.net", "blob.core.windows.net"];

/// Environment variable object_store's `from_env` reads for the managed identity endpoint.
const MSI_ENDPOINT_ENV_KEY: &str = "IDENTITY_ENDPOINT";
const AZURE_ENV_PREFIX: &str = "AZURE_";
/// The one variable a named principal may borrow from the environment.
const ENV_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";

/// Build a `MicrosoftAzure` `ObjectStore` for `url` using `configs`.
///
/// `url` must use the `abfs[s]://` scheme; the returned `Path` is the URL's resource path
/// (container-relative), suitable for direct use with `ObjectStore::get`. Fails when the
/// Hadoop keys select a mechanism the native scan cannot build, or only part of one, rather
/// than building a store with a different identity (see the module docs).
pub fn create_store(
    url: &Url,
    configs: &HashMap<String, String>,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    create_store_with_env(url, configs, env_pairs())
}

/// `create_store` with the environment supplied by the caller. `create_store` is the only
/// caller that reads the process environment.
fn create_store_with_env(
    url: &Url,
    configs: &HashMap<String, String>,
    env: impl Iterator<Item = (String, String)>,
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

    let env: Vec<(String, String)> = env.collect();
    validate_translated(
        configs,
        &translated,
        account.as_deref(),
        container.as_deref(),
        env_token_file(&env).is_some(),
    )?;
    let store = build_builder(
        url,
        configs,
        account.as_deref(),
        container.as_deref(),
        &translated,
        env.into_iter(),
    )
    .build()?;
    Ok((Box::new(store), path))
}

fn config_error(message: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "MicrosoftAzure",
        source: message.into(),
    }
}

/// Reject a Hadoop configuration that `object_store` would silently build a different
/// identity from: a blank credential, an auth type or mechanism the native scan cannot
/// build, a named principal with no token file in Hadoop or the environment, or a client
/// secret or token file without the client id and tenant that complete it.
/// `has_env_token_file` says whether `AZURE_FEDERATED_TOKEN_FILE` is set.
fn validate_translated(
    configs: &HashMap<String, String>,
    translated: &[(AzureConfigKey, String)],
    account: Option<&str>,
    container: Option<&str>,
    has_env_token_file: bool,
) -> Result<(), object_store::Error> {
    let account_name = account.unwrap_or("<unknown>");
    let fail = |reason: String| {
        Err(config_error(format!(
            "Hadoop configuration for account {account_name}: {reason}"
        )))
    };
    if let Some(reason) = hadoop_problem(configs, account, container, translated) {
        return fail(reason);
    }
    let has = |wanted: AzureConfigKey| translated.iter().any(|(key, _)| *key == wanted);
    let borrows_env_token_file = env_policy(configs, account, container, translated)
        == EnvPolicy::TokenFileOnly
        && !has(AzureConfigKey::FederatedTokenFile);
    if borrows_env_token_file && !has_env_token_file {
        return fail(format!(
            "the principal named by the Hadoop keys needs a token file from \
             `{HADOOP_WI_TOKEN_FILE}` or `{ENV_FEDERATED_TOKEN_FILE}`"
        ));
    }
    let mechanism = if has(AzureConfigKey::ClientSecret) {
        HADOOP_OAUTH_CLIENT_SECRET
    } else if has(AzureConfigKey::FederatedTokenFile) {
        HADOOP_WI_TOKEN_FILE
    } else if borrows_env_token_file {
        ENV_FEDERATED_TOKEN_FILE
    } else {
        return Ok(());
    };
    let mut missing = Vec::new();
    if !has(AzureConfigKey::ClientId) {
        missing.push(format!("`{HADOOP_OAUTH_CLIENT_ID}`"));
    }
    if !has(AzureConfigKey::AuthorityId) {
        missing.push(format!(
            "`{HADOOP_MSI_TENANT}` or `{HADOOP_OAUTH_CLIENT_ENDPOINT}`"
        ));
    }
    if missing.is_empty() {
        return Ok(());
    }
    fail(format!(
        "`{mechanism}` also needs {}",
        missing.join(" and ")
    ))
}

/// Why the Hadoop keys cannot be built natively as configured, or `None` when they can:
/// a blank value first, then an explicit auth type the translated keys do not satisfy,
/// then a provider class they do not satisfy, then a key that selects a mechanism with no
/// native counterpart.
fn hadoop_problem(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
    translated: &[(AzureConfigKey, String)],
) -> Option<String> {
    blank_value_problem(configs, account, container)
        .or_else(|| auth_type_problem(configs, account, translated))
        .or_else(|| provider_class_problem(configs, account, translated))
        .or_else(|| unsupported_key_problem(configs, account))
}

/// A blank SAS token or credential value, named by the exact key that holds it.
///
/// Blank values are errors rather than absent, so a templated configuration that
/// substitutes an empty string fails loudly instead of silently using another credential.
fn blank_value_problem(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
) -> Option<String> {
    if let Some((key, value)) = active_sas_token(configs, account, container) {
        if value.trim().is_empty() {
            let fallback = if key.starts_with(HADOOP_SAS_FIXED_TOKEN) {
                String::new()
            } else {
                format!(
                    "; `{HADOOP_SAS_FIXED_TOKEN}` is not used as a fallback when a \
                     container-scoped SAS key is set"
                )
            };
            return Some(format!("`{key}` is blank{fallback}"));
        }
    }
    HADOOP_CREDENTIAL_MAPPINGS
        .iter()
        .filter(|(_, _, mechanism)| mechanism_is_read(configs, account, *mechanism))
        .find_map(|(base, _, _)| {
            account_scoped_entry(configs, base, account)
                .filter(|(_, value)| value.trim().is_empty())
                .map(|(key, _)| format!("`{key}` is blank"))
        })
}

/// Whether an explicit `fs.azure.account.auth.type` is one the translated keys satisfy.
///
/// Setting it is Hadoop choosing a mechanism, so it is validated rather than ignored:
/// `SharedKey` needs the account key, `OAuth` a provider the scan can build, `SAS` a SAS
/// token, `Custom` has no native counterpart and any other value is a typo.
fn auth_type_problem(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    translated: &[(AzureConfigKey, String)],
) -> Option<String> {
    let (key, value) = account_scoped_entry(configs, HADOOP_AUTH_TYPE, account)?;
    let auth_type = value.trim();
    if auth_type.is_empty() {
        return Some(format!("`{key}` is blank"));
    }
    let has = |wanted: AzureConfigKey| translated.iter().any(|(key, _)| *key == wanted);
    let setting = format!("`{key}={auth_type}`");
    if auth_type.eq_ignore_ascii_case("SharedKey") {
        return (!has(AzureConfigKey::AccessKey))
            .then(|| format!("{setting} needs `{HADOOP_KEY}`"));
    }
    if auth_type.eq_ignore_ascii_case("OAuth") {
        return oauth_problem(configs, account, translated, &setting);
    }
    if auth_type.eq_ignore_ascii_case("SAS") {
        return (!has(AzureConfigKey::SasKey)).then(|| {
            format!(
                "{setting} needs `{HADOOP_SAS_FIXED_TOKEN}`; a SAS token provider class is \
                 not supported by the native scan"
            )
        });
    }
    if auth_type.eq_ignore_ascii_case("Custom") {
        return Some(format!(
            "{setting} loads a custom token provider class, which the native scan does not \
             support"
        ));
    }
    Some(format!(
        "{setting} is not supported; the native scan supports `{HADOOP_AUTH_TYPE}` values \
         SharedKey, OAuth and SAS"
    ))
}

/// Whether `fs.azure.account.auth.type=OAuth` can be satisfied: through the provider class
/// when one is set, otherwise through a translated secret or token file.
fn oauth_problem(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    translated: &[(AzureConfigKey, String)],
    setting: &str,
) -> Option<String> {
    if active_provider_class(configs, account).is_some() {
        return provider_class_problem(configs, account, translated);
    }
    let has = |wanted: AzureConfigKey| translated.iter().any(|(key, _)| *key == wanted);
    if has(AzureConfigKey::ClientSecret) || has(AzureConfigKey::FederatedTokenFile) {
        return None;
    }
    Some(format!(
        "{setting} needs `{HADOOP_OAUTH_CLIENT_SECRET}` or `{HADOOP_WI_TOKEN_FILE}`"
    ))
}

/// Whether a `fs.azure.account.oauth.provider.type` class, validated whenever it is set
/// and OAuth is in use, names a token provider the native scan can
/// satisfy: MSI stands alone, Workload Identity needs the client id and tenant (the token
/// file may still come from `AZURE_FEDERATED_TOKEN_FILE`), client credentials need the
/// secret, and any other class has no native counterpart.
fn provider_class_problem(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    translated: &[(AzureConfigKey, String)],
) -> Option<String> {
    let (provider_key, class) = active_provider_class(configs, account)?;
    let class = class.trim();
    if class.is_empty() {
        return Some(format!("`{provider_key}` is blank"));
    }
    let has = |wanted: AzureConfigKey| translated.iter().any(|(key, _)| *key == wanted);
    let provider = format!("`{provider_key}={class}`");
    if is_provider_class(class, HADOOP_MSI_PROVIDER_CLASS) {
        return None;
    }
    if is_provider_class(class, HADOOP_WI_PROVIDER_CLASS) {
        return (!workload_identity_named(configs, account, translated)).then(|| {
            format!("{provider} needs `{HADOOP_OAUTH_CLIENT_ID}` and `{HADOOP_MSI_TENANT}`")
        });
    }
    if is_provider_class(class, HADOOP_CLIENT_CREDS_PROVIDER_CLASS) {
        return (!has(AzureConfigKey::ClientSecret))
            .then(|| format!("{provider} needs `{HADOOP_OAUTH_CLIENT_SECRET}`"));
    }
    Some(format!(
        "{provider} is not a token provider the native scan supports"
    ))
}

/// The first Hadoop key present that selects a mechanism with no native counterpart,
/// named exactly as the user set it.
fn unsupported_key_problem(
    configs: &HashMap<String, String>,
    account: Option<&str>,
) -> Option<String> {
    HADOOP_UNSUPPORTED_MECHANISM_KEYS
        .iter()
        .filter(|(_, mechanism)| mechanism_is_read(configs, account, *mechanism))
        .find_map(|(base, _)| {
            account_scoped_entry(configs, base, account).map(|(key, _)| {
                format!(
                    "`{key}` selects an authentication mechanism the native scan does not support"
                )
            })
        })
}

/// Whether `class` is the Hadoop token provider with `simple_name`: the bare simple name,
/// or a qualified name whose last segment is exactly it, so that a lookalike such as
/// `com.attacker.EvilWorkloadIdentityTokenProvider` does not pass as the real class.
fn is_provider_class(class: &str, simple_name: &str) -> bool {
    let class = class.trim();
    class == simple_name
        || class
            .strip_suffix(simple_name)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

/// Whether Hadoop names the Workload Identity provider and the principal, which leaves
/// only the token file open for `AZURE_FEDERATED_TOKEN_FILE` to supply.
fn workload_identity_named(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    translated: &[(AzureConfigKey, String)],
) -> bool {
    let has = |wanted: AzureConfigKey| translated.iter().any(|(key, _)| *key == wanted);
    active_provider_class(configs, account)
        .is_some_and(|(_, class)| is_provider_class(&class, HADOOP_WI_PROVIDER_CLASS))
        && has(AzureConfigKey::ClientId)
        && has(AzureConfigKey::AuthorityId)
}

/// Process environment as UTF-8 `(key, value)` pairs, skipping entries that are not UTF-8.
fn env_pairs() -> impl Iterator<Item = (String, String)> {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.to_str()?.to_string(), v.to_str()?.to_string())))
}

/// Assemble the builder from the environment, the URL and the translated Hadoop keys.
///
/// What the environment may contribute is decided by `env_policy`: everything when the
/// Hadoop keys name no identity, only the token file when they name a principal, and
/// nothing when they select a mechanism, so nothing ambient can outrank, combine with or
/// redirect the configured identity. `configs`, `account` and `container` resolve the
/// Hadoop keys that select a mechanism without translating to anything.
fn build_builder(
    url: &Url,
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
    translated: &[(AzureConfigKey, String)],
    env: impl Iterator<Item = (String, String)>,
) -> MicrosoftAzureBuilder {
    let mut builder = MicrosoftAzureBuilder::new();
    match env_policy(configs, account, container, translated) {
        EnvPolicy::All => builder = apply_env(builder, env),
        EnvPolicy::TokenFileOnly => builder = apply_env_token_file(builder, translated, env),
        EnvPolicy::Nothing => {}
    }
    builder = builder.with_url(url.to_string());
    for (key, value) in translated {
        builder = builder.with_config(*key, value.clone());
    }
    builder
}

/// How much of the environment the builder may read, decided by the Hadoop keys.
#[derive(Debug, PartialEq)]
enum EnvPolicy {
    /// Hadoop names neither an identity nor a mechanism: read it as `from_env` does.
    All,
    /// Hadoop names a principal but not its token source: read `AZURE_FEDERATED_TOKEN_FILE`
    /// only, which is what AKS Workload Identity injects.
    TokenFileOnly,
    /// Hadoop selects a mechanism, or a configuration the scan rejects: read nothing.
    Nothing,
}

/// Decide `EnvPolicy` from the Hadoop keys.
///
/// A translated credential key, a provider class (the MSI one stands alone, since Hadoop
/// defaults its endpoint to IMDS without any translated key), an explicit
/// `fs.azure.account.auth.type` or a rejected configuration selects the mechanism. A
/// client id, tenant or authority host, or the Workload Identity provider with the client
/// id and tenant (with or without `auth.type=OAuth`), names a principal and leaves only the
/// token file open. A partial mechanism, such as a token file alone, is never completed
/// from the environment.
fn env_policy(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
    translated: &[(AzureConfigKey, String)],
) -> EnvPolicy {
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
    // A provider class or an explicit auth type is Hadoop choosing a mechanism; only the
    // Workload Identity provider with the client id and tenant leaves the token file open.
    let has_provider_class =
        active_provider_class(configs, account).is_some_and(|(_, class)| !class.trim().is_empty());
    let has_auth_type = account_scoped_value(configs, HADOOP_AUTH_TYPE, account).is_some();
    let chooses_mechanism = (has_provider_class || has_auth_type)
        && !workload_identity_named(configs, account, translated);
    // `hadoop_problem` is checked here as well as in `validate_translated` so that
    // `build_builder` never borrows the environment even when called without validation.
    if has_mechanism_key
        || chooses_mechanism
        || hadoop_problem(configs, account, container, translated).is_some()
    {
        return EnvPolicy::Nothing;
    }
    // A named Workload Identity provider implies a translated client id and tenant, so
    // this covers it too.
    let names_identity = translated.iter().any(|(key, _)| {
        matches!(
            key,
            AzureConfigKey::ClientId | AzureConfigKey::AuthorityId | AzureConfigKey::AuthorityHost
        )
    });
    if names_identity {
        EnvPolicy::TokenFileOnly
    } else {
        EnvPolicy::All
    }
}

/// Borrow `AZURE_FEDERATED_TOKEN_FILE` alone, and only when Hadoop supplied no token file.
fn apply_env_token_file(
    builder: MicrosoftAzureBuilder,
    translated: &[(AzureConfigKey, String)],
    env: impl Iterator<Item = (String, String)>,
) -> MicrosoftAzureBuilder {
    let has_token_file = translated
        .iter()
        .any(|(key, _)| *key == AzureConfigKey::FederatedTokenFile);
    if has_token_file {
        return builder;
    }
    let env: Vec<(String, String)> = env.collect();
    match env_token_file(&env) {
        Some(file) => builder.with_config(AzureConfigKey::FederatedTokenFile, file),
        None => builder,
    }
}

/// The non-blank value of `AZURE_FEDERATED_TOKEN_FILE` in `env`, if any.
fn env_token_file(env: &[(String, String)]) -> Option<String> {
    env.iter()
        .find(|(key, _)| key == ENV_FEDERATED_TOKEN_FILE)
        .map(|(_, value)| value.clone())
        .filter(|value| !value.trim().is_empty())
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

    // A blank value is left out so it never reaches the builder; validation reports it
    // as an error before the store is built.
    for (hadoop_base, azure_key, mechanism) in HADOOP_CREDENTIAL_MAPPINGS {
        if !mechanism_is_read(configs, account, *mechanism) {
            continue;
        }
        if let Some(value) = account_scoped_value(configs, hadoop_base, account) {
            if !value.trim().is_empty() {
                out.push((*azure_key, value));
            }
        }
    }

    // `fs.azure.account.oauth2.client.endpoint` is a full token URL of the form
    // `https://login.microsoftonline.com/<tenant>/oauth2/token`. object_store wants the
    // tenant id directly (`AuthorityId`), so extract it if AuthorityId hasn't already
    // been set from `fs.azure.account.oauth2.msi.tenant`.
    let has_authority_id = out
        .iter()
        .any(|(k, _)| matches!(k, AzureConfigKey::AuthorityId));
    if !has_authority_id && mechanism_is_read(configs, account, AuthMechanism::OAuth) {
        if let Some(endpoint) = account_scoped_value(configs, HADOOP_OAUTH_CLIENT_ENDPOINT, account)
        {
            if let Some(tenant) = tenant_from_oauth_endpoint(&endpoint) {
                out.push((AzureConfigKey::AuthorityId, tenant));
            }
        }
    }

    // The container-scoped SAS token wins over the account-level fixed token. A blank
    // token is left out so it never reaches the builder; validation reports it as an
    // error before the store is built.
    if let Some((_, sas)) = active_sas_token(configs, account, container) {
        if !sas.trim().is_empty() {
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
    account_scoped_entry(configs, base_key, account).map(|(_, value)| value)
}

/// Like `account_scoped_value`, but also returns the configuration key that matched.
fn account_scoped_entry(
    configs: &HashMap<String, String>,
    base_key: &str,
    account: Option<&str>,
) -> Option<(String, String)> {
    let mut candidates = Vec::new();
    if let Some(acc) = account {
        for suffix in ENDPOINT_SUFFIXES {
            candidates.push(format!("{base_key}.{acc}.{suffix}"));
        }
        candidates.push(format!("{base_key}.{acc}"));
    }
    candidates.push(base_key.to_string());
    first_entry(configs, candidates)
}

/// The first `(key, value)` of `candidates` that is present in `configs`.
fn first_entry(
    configs: &HashMap<String, String>,
    candidates: Vec<String>,
) -> Option<(String, String)> {
    candidates
        .into_iter()
        .find_map(|key| configs.get(&key).map(|v| (key, v.clone())))
}

/// `sas_token`, but only while SAS is the selected mechanism.
fn active_sas_token(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
) -> Option<(String, String)> {
    if !mechanism_is_read(configs, account, AuthMechanism::Sas) {
        return None;
    }
    sas_token(configs, account, container)
}

/// Resolve the SAS token and the key it came from: the container-scoped
/// `fs.azure.sas.<container>.<account>[.<endpoint-suffix>]` variants first, then the
/// account-level `fs.azure.sas.fixed.token` that Hadoop's fixed-token provider reads.
fn sas_token(
    configs: &HashMap<String, String>,
    account: Option<&str>,
    container: Option<&str>,
) -> Option<(String, String)> {
    let container_scoped = match (container, account) {
        (Some(container), Some(account)) => {
            let mut candidates: Vec<String> = ENDPOINT_SUFFIXES
                .iter()
                .map(|suffix| format!("{HADOOP_SAS_PREFIX}{container}.{account}.{suffix}"))
                .collect();
            candidates.push(format!("{HADOOP_SAS_PREFIX}{container}.{account}"));
            first_entry(configs, candidates)
        }
        _ => None,
    };
    container_scoped.or_else(|| account_scoped_entry(configs, HADOOP_SAS_FIXED_TOKEN, account))
}

/// Extract the storage account name from an `abfs[s]://` URL.
///
/// ABFS hostnames are `<account>.<endpoint-suffix>` (e.g. `myacct.dfs.core.windows.net`),
/// so the account is the first label of the host. It is lowercased because DNS reaches
/// the same account whatever the case, while the `url` crate keeps the case as written
/// for the `abfs[s]` schemes and the account-scoped Hadoop keys are spelled in lowercase.
fn extract_account(url: &Url) -> Option<String> {
    let host = url.host_str()?;
    host.split('.').next().map(str::to_ascii_lowercase)
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
        let (_store, path) =
            create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
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
        let (_store, path) =
            create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
        assert_eq!(path.as_ref(), "path/file.parquet");
    }

    #[test]
    fn create_store_rejects_non_azure_scheme() {
        let u = url("s3://bucket/file.parquet");
        let configs = HashMap::new();
        let err = err_for(&u, &configs);
        assert!(
            err.contains("Scheme of URL is not Azure"),
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
        build_builder(
            &u,
            hadoop,
            Some("myacct"),
            Some("data"),
            &translated,
            env_of(env),
        )
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

    /// An environment that offers a token file next to credentials and transport settings
    /// that a named principal must not pick up.
    const AMBIENT_WITH_TOKEN_FILE: &[(&str, &str)] = &[
        ("AZURE_CLIENT_ID", "env-client"),
        ("AZURE_STORAGE_ACCOUNT_KEY", "ambient=="),
        ("AZURE_STORAGE_TOKEN", "ambient"),
        ("AZURE_ALLOW_HTTP", "true"),
        (
            "AZURE_FEDERATED_TOKEN_FILE",
            "/var/run/secrets/azure/tokens/token",
        ),
    ];

    /// Only the token file came from `AMBIENT_WITH_TOKEN_FILE`.
    fn assert_only_env_token_file(builder: &MicrosoftAzureBuilder, context: &str) {
        assert_eq!(
            value(builder, AzureConfigKey::FederatedTokenFile).as_deref(),
            Some("/var/run/secrets/azure/tokens/token"),
            "{context}"
        );
        assert_eq!(
            value(builder, AzureConfigKey::ClientId).as_deref(),
            Some("hadoop-client"),
            "{context}"
        );
        assert_eq!(value(builder, AzureConfigKey::AccessKey), None, "{context}");
        assert_eq!(value(builder, AzureConfigKey::Token), None, "{context}");
        assert_eq!(
            value(builder, AzureConfigKey::Client(ClientConfigKey::AllowHttp)).as_deref(),
            Some("false"),
            "{context}"
        );
    }

    #[test]
    fn hadoop_client_id_and_tenant_borrow_only_env_federated_token_file() {
        let configs = hadoop(&[
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ]);
        let builder = builder_for(&configs, AMBIENT_WITH_TOKEN_FILE);
        assert_only_env_token_file(&builder, "identity only");
    }

    #[test]
    fn workload_identity_provider_borrows_only_env_federated_token_file() {
        let base = [
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
            ),
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ];
        let with_auth_type = [
            base[0],
            base[1],
            base[2],
            ("fs.azure.account.auth.type", "OAuth"),
        ];
        for (context, pairs) in [
            ("provider only", &base[..]),
            ("auth.type=OAuth", &with_auth_type[..]),
        ] {
            let configs = hadoop(pairs);
            let builder = builder_for(&configs, AMBIENT_WITH_TOKEN_FILE);
            assert_only_env_token_file(&builder, context);
        }
    }

    #[test]
    fn hadoop_token_file_is_not_replaced_by_env_token_file() {
        let configs = hadoop(&[
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
            ),
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
            ("fs.azure.account.oauth2.token.file", "/etc/hadoop/token"),
        ]);
        let builder = builder_for(&configs, AMBIENT_WITH_TOKEN_FILE);
        assert_eq!(
            value(&builder, AzureConfigKey::FederatedTokenFile).as_deref(),
            Some("/etc/hadoop/token")
        );
    }

    const WORKLOAD_IDENTITY_PRINCIPAL: &[(&str, &str)] = &[
        (
            "fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
        ),
        ("fs.azure.account.oauth2.client.id", "hadoop-client"),
        ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
    ];

    #[test]
    fn create_store_rejects_named_principal_without_any_token_file() {
        let identity_only = &WORKLOAD_IDENTITY_PRINCIPAL[1..];
        for pairs in [WORKLOAD_IDENTITY_PRINCIPAL, identity_only] {
            let configs = hadoop(pairs);
            let err = err_of(&configs);
            assert!(
                err.contains("fs.azure.account.oauth2.token.file")
                    && err.contains("AZURE_FEDERATED_TOKEN_FILE"),
                "{pairs:?}: unexpected error: {err}"
            );
        }
    }

    #[test]
    fn create_store_accepts_named_principal_with_a_token_file_from_either_source() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let configs = hadoop(WORKLOAD_IDENTITY_PRINCIPAL);
        let env = [(
            "AZURE_FEDERATED_TOKEN_FILE",
            "/var/run/secrets/azure/tokens/token",
        )];
        create_store_with_env(&u, &configs, env_of(&env)).expect("env token file");
        let mut configs = configs;
        configs.insert(
            "fs.azure.account.oauth2.token.file".into(),
            "/etc/hadoop/token".into(),
        );
        create_store_with_env(&u, &configs, env_of(&[])).expect("hadoop token file");
    }

    #[test]
    fn env_token_file_for_client_id_alone_reports_the_missing_tenant() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let configs = hadoop(&[("fs.azure.account.oauth2.client.id", "hadoop-client")]);
        let env = [(
            "AZURE_FEDERATED_TOKEN_FILE",
            "/var/run/secrets/azure/tokens/token",
        )];
        let err = match create_store_with_env(&u, &configs, env_of(&env)) {
            Ok(_) => panic!("store built although the configuration must be rejected"),
            Err(err) => format!("{err}"),
        };
        assert!(
            err.contains(ERROR_PREFIX)
                && err.contains("AZURE_FEDERATED_TOKEN_FILE")
                && err.contains("fs.azure.account.oauth2.msi.tenant"),
            "unexpected error: {err}"
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

    const ERROR_PREFIX: &str = "Hadoop configuration for account myacct: ";

    /// The error for a rejected Hadoop configuration; every one carries `ERROR_PREFIX`.
    fn err_of(configs: &HashMap<String, String>) -> String {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let err = err_for(&u, configs);
        assert!(err.contains(ERROR_PREFIX), "missing prefix: {err}");
        err
    }

    /// The error `create_store` returns, without ever formatting a built store, whose
    /// `Debug` output includes the credential.
    fn err_for(u: &Url, configs: &HashMap<String, String>) -> String {
        match create_store_with_env(u, configs, env_of(&[])) {
            Ok(_) => panic!("store built although the configuration must be rejected"),
            Err(err) => format!("{err}"),
        }
    }

    /// Credential values must never leak into an error message.
    fn assert_hides(err: &str, secrets: &[&str]) {
        for secret in secrets {
            assert!(!err.contains(secret), "error leaks {secret:?}: {err}");
        }
    }

    #[test]
    fn create_store_rejects_client_secret_without_client_id_and_tenant() {
        let configs = hadoop(&[("fs.azure.account.oauth2.client.secret", "hadoop-secret")]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.client.id")
                && err.contains("fs.azure.account.oauth2.msi.tenant")
                && err.contains("myacct"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["hadoop-secret"]);
    }

    #[test]
    fn create_store_rejects_client_secret_with_client_id_but_no_tenant() {
        let configs = hadoop(&[
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.msi.tenant")
                && !err.contains("fs.azure.account.oauth2.client.id"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["hadoop-secret", "hadoop-client"]);
    }

    #[test]
    fn create_store_rejects_token_file_without_client_id_and_tenant() {
        let configs = hadoop(&[(
            "fs.azure.account.oauth2.token.file",
            "/var/run/secrets/azure/tokens/token",
        )]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.token.file")
                && err.contains("fs.azure.account.oauth2.client.id")
                && err.contains("fs.azure.account.oauth2.msi.tenant"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["/var/run/secrets/azure/tokens/token"]);
    }

    #[test]
    fn create_store_accepts_client_secret_with_tenant_from_oauth_endpoint() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let configs = hadoop(&[
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
            (
                "fs.azure.account.oauth2.client.endpoint",
                "https://login.microsoftonline.com/hadoop-tenant/oauth2/token",
            ),
        ]);
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    #[test]
    fn translates_fixed_sas_token() {
        for key in [
            "fs.azure.sas.fixed.token",
            "fs.azure.sas.fixed.token.myacct",
            "fs.azure.sas.fixed.token.myacct.dfs.core.windows.net",
        ] {
            let configs = hadoop(&[(key, "sv=2020-08-04&sig=fixed")]);
            let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));
            let by_key: HashMap<_, _> = translated.into_iter().collect();
            assert_eq!(
                by_key.get(&AzureConfigKey::SasKey).map(String::as_str),
                Some("sv=2020-08-04&sig=fixed"),
                "key: {key}"
            );
        }
    }

    #[test]
    fn container_scoped_sas_token_wins_over_fixed_token() {
        let configs = hadoop(&[
            ("fs.azure.sas.data.myacct", "sv=2020-08-04&sig=container"),
            (
                "fs.azure.sas.fixed.token.myacct.dfs.core.windows.net",
                "sv=2020-08-04&sig=fixed",
            ),
        ]);
        let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));
        let sas: Vec<_> = translated
            .iter()
            .filter(|(k, _)| *k == AzureConfigKey::SasKey)
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(sas, vec!["sv=2020-08-04&sig=container"]);
    }

    #[test]
    fn create_store_succeeds_with_auth_type_sas_and_fixed_token() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SAS"),
            ("fs.azure.sas.fixed.token", "sv=2020-08-04&sig=fixed"),
        ]);
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    /// Hadoop auth mechanisms with no native counterpart, as `(key, value)`.
    const UNSUPPORTED_MECHANISMS: &[(&str, &str)] = &[
        ("fs.azure.account.auth.type", "Custom"),
        ("fs.azure.account.auth.type", "SAS"),
        (
            "fs.azure.sas.token.provider.type",
            "com.example.SasProvider",
        ),
        ("fs.azure.account.keyprovider", "com.example.KeyProvider"),
        ("fs.azure.account.oauth2.refresh.token", "refresh-token"),
        ("fs.azure.account.oauth2.user.name", "alice"),
        ("fs.azure.account.oauth2.user.password", "hunter2"),
    ];

    #[test]
    fn create_store_rejects_unsupported_hadoop_mechanisms() {
        for (key, val) in UNSUPPORTED_MECHANISMS {
            for scoped in [
                key.to_string(),
                format!("{key}.myacct"),
                format!("{key}.myacct.dfs.core.windows.net"),
            ] {
                let configs = hadoop(&[(&scoped, val)]);
                let err = err_of(&configs);
                assert!(
                    err.contains(&scoped) && err.contains("myacct"),
                    "key {scoped}: unexpected error: {err}"
                );
                assert_hides(&err, &["hunter2", "refresh-token"]);
            }
        }
    }

    #[test]
    fn create_store_rejects_sas_provider_type_even_with_fixed_token() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SAS"),
            (
                "fs.azure.sas.token.provider.type",
                "com.example.SasProvider",
            ),
            ("fs.azure.sas.fixed.token", "sv=2020-08-04&sig=fixed"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.sas.token.provider.type"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["sv=2020-08-04&sig=fixed"]);
    }

    #[test]
    fn unsupported_hadoop_mechanism_does_not_borrow_env_credentials() {
        for (key, val) in UNSUPPORTED_MECHANISMS {
            let configs = hadoop(&[(key, val)]);
            let builder = builder_for(
                &configs,
                &[
                    ("AZURE_STORAGE_TOKEN", "ambient"),
                    ("AZURE_STORAGE_ACCOUNT_KEY", "ambient=="),
                ],
            );
            assert_eq!(value(&builder, AzureConfigKey::Token), None, "key: {key}");
            assert_eq!(
                value(&builder, AzureConfigKey::AccessKey),
                None,
                "key: {key}"
            );
        }
    }

    #[test]
    fn blank_sas_token_is_not_translated() {
        for key in [
            "fs.azure.sas.data.myacct.dfs.core.windows.net",
            "fs.azure.sas.fixed.token",
        ] {
            for blank in ["", "  "] {
                let configs = hadoop(&[(key, blank)]);
                let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));
                assert!(
                    !translated.iter().any(|(k, _)| *k == AzureConfigKey::SasKey),
                    "key {key}, value {blank:?}"
                );
            }
        }
    }

    #[test]
    fn create_store_rejects_blank_sas_token() {
        for key in [
            "fs.azure.sas.data.myacct.dfs.core.windows.net",
            "fs.azure.sas.data.myacct",
            "fs.azure.sas.fixed.token.myacct",
        ] {
            let configs = hadoop(&[(key, "  ")]);
            let err = err_of(&configs);
            assert!(err.contains(key), "key {key}: unexpected error: {err}");
        }
    }

    #[test]
    fn create_store_rejects_shared_key_auth_type_without_account_key() {
        for key in [
            "fs.azure.account.auth.type",
            "fs.azure.account.auth.type.myacct",
        ] {
            let configs = hadoop(&[(key, "SharedKey")]);
            let err = err_of(&configs);
            assert!(
                err.contains("fs.azure.account.key") && err.contains("myacct"),
                "key {key}: unexpected error: {err}"
            );
        }
    }

    #[test]
    fn create_store_accepts_shared_key_auth_type_with_account_key() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
        ]);
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    #[test]
    fn create_store_rejects_oauth_auth_type_with_unrecognised_provider() {
        for key in [
            "fs.azure.account.auth.type",
            "fs.azure.account.auth.type.myacct",
        ] {
            let configs = hadoop(&[
                (key, "OAuth"),
                (
                    "fs.azure.account.oauth.provider.type",
                    "com.example.CustomTokenProvider",
                ),
            ]);
            let err = err_of(&configs);
            assert!(
                err.contains("fs.azure.account.oauth.provider.type")
                    && err.contains("com.example.CustomTokenProvider")
                    && err.contains("myacct"),
                "key {key}: unexpected error: {err}"
            );
        }
    }

    #[test]
    fn create_store_rejects_oauth_auth_type_without_credentials() {
        let configs = hadoop(&[("fs.azure.account.auth.type", "OAuth")]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.client.secret")
                && err.contains("fs.azure.account.oauth2.token.file"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_store_accepts_oauth_auth_type_with_client_secret_principal() {
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let mut configs = hadoop(CLIENT_SECRET_PRINCIPAL);
        configs.insert("fs.azure.account.auth.type".into(), "OAuth".into());
        create_store_with_env(&u, &configs, env_of(&[]))
            .expect("store builds without a provider type");
        configs.insert(
            "fs.azure.account.oauth.provider.type".into(),
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider".into(),
        );
        create_store_with_env(&u, &configs, env_of(&[]))
            .expect("store builds with the client creds provider");
    }

    #[test]
    fn oauth_auth_type_with_workload_identity_provider_accepts_env_token_file() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "OAuth"),
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
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        let env = [(
            "AZURE_FEDERATED_TOKEN_FILE",
            "/var/run/secrets/azure/tokens/token",
        )];
        create_store_with_env(&u, &configs, env_of(&env)).expect("store builds");
    }

    #[test]
    fn create_store_rejects_unknown_auth_type() {
        for key in [
            "fs.azure.account.auth.type",
            "fs.azure.account.auth.type.myacct",
        ] {
            let configs = hadoop(&[(key, "Bogus")]);
            let err = err_of(&configs);
            assert!(
                err.contains(key)
                    && err.contains("Bogus")
                    && err.contains("the native scan supports")
                    && err.contains("SharedKey, OAuth and SAS")
                    && !err.contains("not one of"),
                "key {key}: unexpected error: {err}"
            );
        }
    }

    #[test]
    fn explicit_auth_type_does_not_borrow_env_credentials() {
        let cases: &[&[(&str, &str)]] = &[
            &[("fs.azure.account.auth.type", "SharedKey")],
            &[
                ("fs.azure.account.auth.type", "OAuth"),
                (
                    "fs.azure.account.oauth.provider.type",
                    "com.example.CustomTokenProvider",
                ),
            ],
            &[("fs.azure.account.auth.type", "Bogus")],
        ];
        for case in cases {
            let configs = hadoop(case);
            let builder = builder_for(
                &configs,
                &[
                    ("AZURE_STORAGE_ACCOUNT_KEY", "ambient=="),
                    ("AZURE_STORAGE_TOKEN", "ambient"),
                    (
                        "AZURE_FEDERATED_TOKEN_FILE",
                        "/var/run/secrets/azure/tokens/token",
                    ),
                ],
            );
            assert_eq!(value(&builder, AzureConfigKey::AccessKey), None, "{case:?}");
            assert_eq!(value(&builder, AzureConfigKey::Token), None, "{case:?}");
            assert_eq!(
                value(&builder, AzureConfigKey::FederatedTokenFile),
                None,
                "{case:?}"
            );
        }
    }

    #[test]
    fn mixed_case_host_resolves_account_scoped_key_and_borrows_no_env() {
        let u = url("abfss://data@MyAcct.dfs.core.windows.net/path/file.parquet");
        let configs = hadoop(&[(
            "fs.azure.account.key.myacct.dfs.core.windows.net",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        )]);
        let account = extract_account(&u);
        assert_eq!(account.as_deref(), Some("myacct"));
        let translated = translate_hadoop_configs(&configs, account.as_deref(), Some("data"));
        let builder = build_builder(
            &u,
            &configs,
            account.as_deref(),
            Some("data"),
            &translated,
            env_of(&[("AZURE_STORAGE_TOKEN", "ambient")]),
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        assert_eq!(value(&builder, AzureConfigKey::Token), None);
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    #[test]
    fn blank_container_sas_is_not_replaced_by_fixed_token() {
        let configs = hadoop(&[
            ("fs.azure.sas.data.myacct", "  "),
            ("fs.azure.sas.fixed.token", "sv=2020-08-04&sig=fixed"),
        ]);
        let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));
        assert!(!translated.iter().any(|(k, _)| *k == AzureConfigKey::SasKey));
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.sas.data.myacct")
                && err.contains("fs.azure.sas.fixed.token")
                && err.contains("fallback"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["sv=2020-08-04&sig=fixed"]);
    }

    #[test]
    fn blank_sas_is_reported_before_sas_auth_type() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SAS"),
            ("fs.azure.sas.data.myacct", ""),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.sas.data.myacct") && !err.contains("needs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn blank_credential_values_are_not_translated() {
        for key in [
            "fs.azure.account.key",
            "fs.azure.account.oauth2.client.id",
            "fs.azure.account.oauth2.client.secret",
            "fs.azure.account.oauth2.msi.tenant",
            "fs.azure.account.oauth2.token.file",
        ] {
            let configs = hadoop(&[(key, " ")]);
            let translated = translate_hadoop_configs(&configs, Some("myacct"), Some("data"));
            assert!(translated.is_empty(), "key {key}: {translated:?}");
        }
    }

    #[test]
    fn create_store_rejects_blank_credential_values() {
        for key in [
            "fs.azure.account.key",
            "fs.azure.account.oauth2.client.secret.myacct.dfs.core.windows.net",
        ] {
            let configs = hadoop(&[(key, "")]);
            let err = err_of(&configs);
            assert!(
                err.contains(key) && err.contains("blank"),
                "key {key}: unexpected error: {err}"
            );
        }
    }

    #[test]
    fn blank_client_id_with_secret_reports_the_blank_key() {
        let configs = hadoop(&[
            ("fs.azure.account.oauth2.client.id", ""),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.client.id")
                && err.contains("blank")
                && !err.contains("also needs"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["hadoop-secret"]);
    }

    #[test]
    fn account_scoped_lookup_prefers_dfs_then_blob_then_bare() {
        let mut configs = hadoop(&[
            (
                "fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net",
                "dfs",
            ),
            (
                "fs.azure.account.oauth2.client.id.myacct.blob.core.windows.net",
                "blob",
            ),
            ("fs.azure.account.oauth2.client.id.myacct", "bare"),
        ]);
        let lookup = |configs: &HashMap<String, String>| {
            account_scoped_value(configs, HADOOP_OAUTH_CLIENT_ID, Some("myacct"))
        };
        assert_eq!(lookup(&configs).as_deref(), Some("dfs"));
        configs.remove("fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net");
        assert_eq!(lookup(&configs).as_deref(), Some("blob"));
        configs.remove("fs.azure.account.oauth2.client.id.myacct.blob.core.windows.net");
        assert_eq!(lookup(&configs).as_deref(), Some("bare"));
    }

    #[test]
    fn hadoop_msi_provider_type_with_surrounding_whitespace_counts_as_a_mechanism() {
        let configs = hadoop(&[(
            "fs.azure.account.oauth.provider.type",
            " org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider\n",
        )]);
        let builder = builder_for(
            &configs,
            &[
                ("AZURE_STORAGE_ACCOUNT_KEY", "ambient=="),
                ("AZURE_STORAGE_TOKEN", "ambient"),
            ],
        );
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
        assert_eq!(value(&builder, AzureConfigKey::Token), None);
    }

    #[test]
    fn create_store_rejects_blank_auth_type_as_blank() {
        for key in [
            "fs.azure.account.auth.type",
            "fs.azure.account.auth.type.myacct",
        ] {
            for blank in ["", " \n"] {
                let configs = hadoop(&[(key, blank)]);
                let err = err_of(&configs);
                assert!(
                    err.contains(&format!("`{key}` is blank")) && !err.contains("supports"),
                    "key {key}, value {blank:?}: unexpected error: {err}"
                );
            }
        }
    }

    /// Provider classes set on their own, with what each error must name.
    const PROVIDER_CLASSES_ALONE: &[(&str, &str)] = &[
        (
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
            "fs.azure.account.oauth2.client.id",
        ),
        (
            " WorkloadIdentityTokenProvider\n",
            "fs.azure.account.oauth2.msi.tenant",
        ),
        (
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.secret",
        ),
        (
            "ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.secret",
        ),
        ("com.example.CustomTokenProvider", "not a token provider"),
    ];

    #[test]
    fn create_store_rejects_provider_class_alone() {
        for key in [
            "fs.azure.account.oauth.provider.type",
            "fs.azure.account.oauth.provider.type.myacct",
        ] {
            for (class, needs) in PROVIDER_CLASSES_ALONE {
                let configs = hadoop(&[(key, class)]);
                let err = err_of(&configs);
                assert!(
                    err.contains(key) && err.contains(class.trim()) && err.contains(needs),
                    "key {key}, class {class:?}: unexpected error: {err}"
                );
            }
            let configs = hadoop(&[(key, " ")]);
            let err = err_of(&configs);
            assert!(
                err.contains(&format!("`{key}` is blank")),
                "key {key}: unexpected error: {err}"
            );
        }
    }

    #[test]
    fn provider_class_alone_borrows_nothing_from_env() {
        let classes = PROVIDER_CLASSES_ALONE
            .iter()
            .map(|(class, _)| *class)
            .chain([" "]);
        for class in classes {
            let configs = hadoop(&[("fs.azure.account.oauth.provider.type", class)]);
            let builder = builder_for(
                &configs,
                &[
                    ("AZURE_STORAGE_ACCOUNT_KEY", "ambient=="),
                    ("AZURE_STORAGE_TOKEN", "ambient"),
                    ("AZURE_ALLOW_HTTP", "true"),
                    (MSI_ENDPOINT_ENV_KEY, "http://169.254.169.254/msi"),
                ],
            );
            assert_eq!(
                value(&builder, AzureConfigKey::AccessKey),
                None,
                "{class:?}"
            );
            assert_eq!(value(&builder, AzureConfigKey::Token), None, "{class:?}");
            assert_eq!(
                value(&builder, AzureConfigKey::Client(ClientConfigKey::AllowHttp)).as_deref(),
                Some("false"),
                "{class:?}"
            );
            assert_eq!(
                value(&builder, AzureConfigKey::MsiEndpoint),
                None,
                "{class:?}"
            );
        }
    }

    #[test]
    fn provider_class_must_match_the_simple_name_exactly() {
        let evil = "com.attacker.EvilWorkloadIdentityTokenProvider";
        let configs = hadoop(&[
            ("fs.azure.account.oauth.provider.type", evil),
            ("fs.azure.account.oauth2.client.id", "hadoop-client"),
            ("fs.azure.account.oauth2.msi.tenant", "hadoop-tenant"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth.provider.type") && err.contains(evil),
            "unexpected error: {err}"
        );
        let builder = builder_for(&configs, AMBIENT_WITH_TOKEN_FILE);
        assert_eq!(value(&builder, AzureConfigKey::FederatedTokenFile), None);
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);

        let configs = hadoop(&[(
            "fs.azure.account.oauth.provider.type",
            "XyzMsiTokenProvider",
        )]);
        let err = err_of(&configs);
        assert!(
            err.contains("XyzMsiTokenProvider") && err.contains("not a token provider"),
            "unexpected error: {err}"
        );

        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        for class in [
            "MsiTokenProvider",
            "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
        ] {
            let configs = hadoop(&[("fs.azure.account.oauth.provider.type", class)]);
            create_store_with_env(&u, &configs, env_of(&[])).expect(class);
        }
    }

    // An explicit `fs.azure.account.auth.type` (account-scoped wins over global) selects one
    // mechanism, and only that mechanism's keys are translated and validated. Keys of the
    // other mechanisms are skipped: never translated into the builder and never validated,
    // blank or not.

    #[test]
    fn explicit_shared_key_ignores_inactive_global_oauth_keys() {
        // A global `auth.type` of OAuth overridden by an account-scoped `SharedKey`: the
        // global OAuth provider class and client secret stay untouched although present.
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "OAuth"),
            (
                "fs.azure.account.auth.type.myacct.dfs.core.windows.net",
                "SharedKey",
            ),
            (
                "fs.azure.account.key.myacct.dfs.core.windows.net",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            ),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        assert_eq!(value(&builder, AzureConfigKey::ClientSecret), None);
        assert_eq!(value(&builder, AzureConfigKey::ClientId), None);
        assert_eq!(value(&builder, AzureConfigKey::AuthorityId), None);
    }

    #[test]
    fn explicit_oauth_ignores_inactive_account_key() {
        let mut configs = hadoop(CLIENT_SECRET_PRINCIPAL);
        configs.insert("fs.azure.account.auth.type".into(), "OAuth".into());
        configs.insert("fs.azure.account.key".into(), "secret==".into());
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientSecret).as_deref(),
            Some("hadoop-secret")
        );
        assert_eq!(
            value(&builder, AzureConfigKey::ClientId).as_deref(),
            Some("hadoop-client")
        );
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
    }

    #[test]
    fn explicit_oauth_still_rejects_incomplete_client_secret() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "OAuth"),
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            ),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.client.id"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["hadoop-secret"]);
    }

    #[test]
    fn explicit_shared_key_ignores_inactive_sas_provider_class() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            (
                "fs.azure.sas.token.provider.type",
                "com.example.SasProvider",
            ),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    #[test]
    fn explicit_shared_key_still_rejects_key_provider_class() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.account.keyprovider", "com.example.KeyProvider"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.keyprovider"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn explicit_sas_ignores_inactive_key_and_secret() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SAS"),
            ("fs.azure.sas.fixed.token", "sv=2020&sig=abc"),
            ("fs.azure.account.key", "secret=="),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::SasKey).as_deref(),
            Some("sv=2020&sig=abc")
        );
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
        assert_eq!(value(&builder, AzureConfigKey::ClientSecret), None);
    }

    #[test]
    fn explicit_shared_key_ignores_blank_inactive_secret() {
        // A blank value on a key of a mechanism that is not selected is no error. It is
        // never read, unlike a blank value on the selected mechanism's own key.
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.account.oauth2.client.secret", ""),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    #[test]
    fn explicit_shared_key_ignores_inactive_unsupported_provider_class() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            (
                "fs.azure.account.oauth.provider.type",
                "com.example.CustomTokenProvider",
            ),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");
    }

    #[test]
    fn default_shared_key_ignores_unused_global_oauth_secret() {
        // No auth type, an account-scoped key and a global client secret left over from
        // another account. Hadoop defaults to SharedKey and reads only the key, so the
        // secret is neither handed to the builder nor checked for a client id and tenant.
        let configs = hadoop(&[
            (
                "fs.azure.account.key.myacct.dfs.core.windows.net",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.account.oauth2.client.secret", "hadoop-secret"),
            (
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            ),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        assert_eq!(value(&builder, AzureConfigKey::ClientSecret), None);
        assert_eq!(value(&builder, AzureConfigKey::ClientId), None);
        assert_eq!(value(&builder, AzureConfigKey::AuthorityId), None);
    }

    #[test]
    fn default_shared_key_ignores_unused_sas_token_and_sas_provider_class() {
        let configs = hadoop(&[
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.sas.fixed.token", "sv=2020&sig=abc"),
            (
                "fs.azure.sas.token.provider.type",
                "com.example.SasProvider",
            ),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(value(&builder, AzureConfigKey::SasKey), None);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
    }

    #[test]
    fn default_shared_key_reads_nothing_from_the_environment() {
        let configs = hadoop(&[("fs.azure.account.key", "secret==")]);
        let builder = builder_for(
            &configs,
            &[
                ("AZURE_STORAGE_TOKEN", "ambient"),
                ("AZURE_CLIENT_ID", "ambient-client"),
                (
                    "AZURE_FEDERATED_TOKEN_FILE",
                    "/var/run/secrets/azure/tokens/token",
                ),
                (
                    "IDENTITY_ENDPOINT",
                    "http://169.254.169.254/metadata/identity",
                ),
            ],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("secret==")
        );
        assert_eq!(value(&builder, AzureConfigKey::Token), None);
        assert_eq!(value(&builder, AzureConfigKey::ClientId), None);
        assert_eq!(value(&builder, AzureConfigKey::FederatedTokenFile), None);
        assert_eq!(value(&builder, AzureConfigKey::MsiEndpoint), None);
    }

    #[test]
    fn default_shared_key_still_rejects_blank_account_key() {
        // A key entry selects SharedKey whatever its value, so a blank key is an error on
        // that mechanism rather than a fall-through to the complete OAuth principal.
        let mut pairs = vec![("fs.azure.account.key", "")];
        pairs.extend_from_slice(CLIENT_SECRET_PRINCIPAL);
        let configs = hadoop(&pairs);
        let err = err_of(&configs);
        assert!(
            err.contains("`fs.azure.account.key` is blank"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["hadoop-secret"]);
    }

    #[test]
    fn default_shared_key_ignores_blank_unused_secret() {
        // With a key and no auth type, an OAuth key is never read, so a blank one is no
        // error, just as under an explicit `SharedKey`.
        let configs = hadoop(&[
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.account.oauth2.client.secret", ""),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        assert_eq!(value(&builder, AzureConfigKey::ClientSecret), None);
    }

    #[test]
    fn default_shared_key_ignores_blank_unused_sas_token() {
        let configs = hadoop(&[
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.sas.fixed.token", ""),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(value(&builder, AzureConfigKey::SasKey), None);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
    }

    #[test]
    fn default_shared_key_still_rejects_key_provider_class() {
        let configs = hadoop(&[
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            ("fs.azure.account.keyprovider", "com.example.KeyProvider"),
        ]);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.keyprovider"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn no_key_and_no_auth_type_still_reads_every_mechanism() {
        let configs = hadoop(CLIENT_SECRET_PRINCIPAL);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        create_store_with_env(&u, &configs, env_of(&[])).expect("store builds");

        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientSecret).as_deref(),
            Some("hadoop-secret")
        );
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
    }

    #[test]
    fn explicit_oauth_scoped_over_global_shared_key_ignores_account_key() {
        let mut pairs = vec![
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.auth.type.myacct.dfs.core.windows.net",
                "OAuth",
            ),
            ("fs.azure.account.key", "secret=="),
        ];
        pairs.extend_from_slice(CLIENT_SECRET_PRINCIPAL);
        let configs = hadoop(&pairs);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        assert!(create_store_with_env(&u, &configs, env_of(&[])).is_ok());
        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientSecret).as_deref(),
            Some("hadoop-secret")
        );
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
    }

    #[test]
    fn explicit_sas_ignores_blank_inactive_account_key() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SAS"),
            ("fs.azure.sas.fixed.token", "sv=2020&sig=abc"),
            ("fs.azure.account.key", ""),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        assert!(create_store_with_env(&u, &configs, env_of(&[])).is_ok());
        let builder = builder_for(&configs, &[]);
        assert_eq!(
            value(&builder, AzureConfigKey::SasKey).as_deref(),
            Some("sv=2020&sig=abc")
        );
        assert_eq!(value(&builder, AzureConfigKey::AccessKey), None);
    }

    #[test]
    fn explicit_oauth_ignores_blank_inactive_sas_token() {
        let mut pairs = vec![
            ("fs.azure.account.auth.type", "OAuth"),
            ("fs.azure.sas.fixed.token", ""),
        ];
        pairs.extend_from_slice(CLIENT_SECRET_PRINCIPAL);
        let configs = hadoop(&pairs);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        assert!(create_store_with_env(&u, &configs, env_of(&[])).is_ok());
        let builder = builder_for(&configs, &[]);
        assert_eq!(value(&builder, AzureConfigKey::SasKey), None);
        assert_eq!(
            value(&builder, AzureConfigKey::ClientSecret).as_deref(),
            Some("hadoop-secret")
        );
    }

    #[test]
    fn explicit_shared_key_with_its_key_reads_nothing_from_the_environment() {
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            ("fs.azure.account.key", "secret=="),
        ]);
        let builder = builder_for(
            &configs,
            &[
                ("AZURE_STORAGE_TOKEN", "ambient"),
                ("AZURE_CLIENT_ID", "ambient-client"),
                (
                    "AZURE_FEDERATED_TOKEN_FILE",
                    "/var/run/secrets/azure/tokens/token",
                ),
                (
                    "IDENTITY_ENDPOINT",
                    "http://169.254.169.254/metadata/identity",
                ),
            ],
        );
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("secret==")
        );
        assert_eq!(value(&builder, AzureConfigKey::Token), None);
        assert_eq!(value(&builder, AzureConfigKey::ClientId), None);
        assert_eq!(value(&builder, AzureConfigKey::FederatedTokenFile), None);
        assert_eq!(value(&builder, AzureConfigKey::MsiEndpoint), None);
    }

    #[test]
    fn explicit_oauth_still_rejects_refresh_token_mechanism() {
        let mut pairs = vec![
            ("fs.azure.account.auth.type", "OAuth"),
            ("fs.azure.account.oauth2.refresh.token", "refresh-me"),
        ];
        pairs.extend_from_slice(CLIENT_SECRET_PRINCIPAL);
        let configs = hadoop(&pairs);
        let err = err_of(&configs);
        assert!(
            err.contains("fs.azure.account.oauth2.refresh.token"),
            "unexpected error: {err}"
        );
        assert_hides(&err, &["refresh-me", "hadoop-secret"]);
    }

    #[test]
    fn global_shared_key_ignores_inactive_account_scoped_secret() {
        // `build()` base64-decodes the account key, so this one must be valid base64.
        let configs = hadoop(&[
            ("fs.azure.account.auth.type", "SharedKey"),
            (
                "fs.azure.account.key",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            ),
            (
                "fs.azure.account.oauth2.client.secret.myacct.dfs.core.windows.net",
                "scoped-secret",
            ),
        ]);
        let u = url("abfss://data@myacct.dfs.core.windows.net/path/file.parquet");
        assert!(create_store_with_env(&u, &configs, env_of(&[])).is_ok());
        let builder = builder_for(&configs, &[]);
        assert_eq!(value(&builder, AzureConfigKey::ClientSecret), None);
        assert_eq!(
            value(&builder, AzureConfigKey::AccessKey).as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
    }
}
