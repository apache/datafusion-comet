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

//! S3-compatible filesystem alias handling.
//!
//! Operators mark extra schemes as `s3://` aliases via `fs.comet.s3Compliant.schemes`
//! (comma-separated, case-insensitive, opt-in). They route to `object_store::AmazonS3`, but
//! `ObjectStoreScheme::parse` does not recognize them.
//!
//! On the Parquet scan path, [`normalize_object_store_url`] rewrites those aliases (and `s3a`) to
//! `s3://bucket/key` so `prepare_object_store_with_configs`'s `scheme == "s3"` dispatch fires. Its
//! callers consume only `url.scheme()`/`url.path()`, so re-serialization through `url::Url` is
//! harmless.
//!
//! The Iceberg path never rewrites the recorded location string that iceberg-rust holds.
//! iceberg-rust matches positional/equality deletes by an exact string comparison of a delete
//! file's recorded `file_path` against the `data_file_path` Comet supplies, so rewriting that
//! string would desync the two and silently drop deletes. `iceberg_common::storage_factory_for`
//! routes an alias scheme to its S3 backend (via [`is_s3_compliant_alias_scheme`]), and
//! iceberg-storage-opendal's scheme-agnostic S3 operator opens a host-bearing `blob://bucket/key`
//! as-is (bucket from `Url::host_str`, key prefix from the path's own scheme).
//!
//! Some vendor blob filesystems record HOSTLESS locations (`blob:///bucket/key`, bucket as first
//! path segment) that operator cannot open. For alias scans, [`BlobHostPromotingS3Storage`] wraps
//! the S3 backend and promotes the bucket from the first path segment (via
//! [`promote_hostless_alias_url`]) ONLY at the open boundary. The string iceberg-rust matches
//! deletes against is still the raw recorded location, so deletes stay correct.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result as IcebergResult};
use iceberg_storage_opendal::{CustomAwsCredentialLoader, OpenDalStorageFactory};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::is_hdfs_scheme;

/// Rewrites `s3a` and the configured s3-compliant aliases to `s3://bucket/key`, promoting a missing
/// authority into the host. Non-alias schemes are returned unchanged. `s3a` routed through libhdfs
/// (`fs.comet.libhdfs.schemes`) is left alone.
pub(crate) fn normalize_object_store_url(
    url_str: &str,
    object_store_configs: &HashMap<String, String>,
) -> Result<Url, ExecutionError> {
    let url = Url::parse(url_str)
        .map_err(|e| ExecutionError::GeneralError(format!("Error parsing URL {url_str}: {e}")))?;
    if is_hdfs_scheme(&url, object_store_configs) {
        return Ok(url);
    }
    // Callers consume only `url.scheme()`/`url.path()`, so re-serialization through `url::Url` is
    // harmless here (the Iceberg path avoids this function precisely because it must preserve raw
    // bytes; it routes aliases by scheme in `storage_factory_for` instead).
    let scheme = url.scheme();
    if scheme != "s3a" && !is_s3_compliant_alias_scheme(scheme, object_store_configs) {
        return Ok(url);
    }
    rewrite_alias_to_s3(url)
}

/// True if `scheme` is a configured s3-compliant alias (`fs.comet.s3Compliant.schemes`,
/// comma-separated, case-insensitive; empty/unset means none). These route to `AmazonS3` but are
/// not recognized by `ObjectStoreScheme::parse`. `s3a` is excluded -- object_store knows it and
/// callers special-case it. The URL-spec special schemes (`file`, `http`, `https`, `ftp`, `ws`,
/// `wss`) are excluded too: they cannot be rewritten to `s3` (see the body). Shared by the Parquet
/// normalizer above and the Iceberg storage-factory gate (`iceberg_common::storage_factory_for`)
/// so both admit the same schemes.
pub(crate) fn is_s3_compliant_alias_scheme(
    scheme: &str,
    object_store_configs: &HashMap<String, String>,
) -> bool {
    const COMET_S3_COMPLIANT_SCHEMES_KEY: &str = "fs.comet.s3Compliant.schemes";
    // The URL spec's "special" schemes can never be an alias. `Url::set_scheme` refuses to rewrite
    // between a special scheme and a non-special one, so `rewrite_alias_to_s3` returns `Err` for
    // every URL of such a scheme -- and since `normalize_object_store_url` and its callers
    // propagate that error, a stray `file`/`https`/... in the user-typed list would turn every
    // matching scan into a hard failure instead of a clean fallback (a `file` entry would take down
    // every local Parquet scan). It is load-bearing for the Iceberg caller too, which never
    // rewrites: without it a listed `file` would be admitted by `is_s3_family_scheme` and routed to
    // S3 in `storage_factory_for`, misdirecting local reads. Excluded for the same reason `s3a` is.
    // These six are the spec-fixed WHATWG special set (mirrors `url`'s internal `SchemeType::from`).
    const URL_SPEC_SPECIAL_SCHEMES: [&str; 6] = ["file", "http", "https", "ftp", "ws", "wss"];
    if URL_SPEC_SPECIAL_SCHEMES
        .iter()
        .any(|s| s.eq_ignore_ascii_case(scheme))
    {
        return false;
    }
    match object_store_configs.get(COMET_S3_COMPLIANT_SCHEMES_KEY) {
        Some(schemes) => schemes
            .split(',')
            .any(|s| s.trim().eq_ignore_ascii_case(scheme)),
        None => false,
    }
}

/// Rewrites a parsed alias URL (`s3a` or a configured alias) to `s3://bucket/key`. When it has no
/// authority (empty-authority `blob:///bucket/key` or opaque `blob:/bucket/key`, both host=None)
/// the first path segment is promoted into the host. Defensive: object-store URLs normally have one.
fn rewrite_alias_to_s3(mut url: Url) -> Result<Url, ExecutionError> {
    let original = url.scheme().to_string();
    if url.host_str().is_none() {
        // host=None (empty-authority `blob:///bucket/key` or opaque `blob:/bucket/key`): lift the
        // first path segment into the host for `ObjectStoreScheme::parse`.
        let trimmed = url.path().trim_start_matches('/');
        let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
        if bucket.is_empty() {
            return Err(ExecutionError::GeneralError(format!(
                "{original}:// URL is missing bucket name: {url}"
            )));
        }
        return Url::parse(&format!("s3://{bucket}/{key}")).map_err(|e| {
            ExecutionError::GeneralError(format!("Could not normalize {original}:// URL: {e}"))
        });
    }
    // Host-bearing (`blob://bucket/key`): swap only the scheme. s3 and the aliases are all
    // non-special, so `set_scheme` succeeds and the host/path are preserved verbatim.
    url.set_scheme("s3").map_err(|_| {
        ExecutionError::GeneralError(format!("Could not convert scheme from {original} to s3"))
    })?;
    Ok(url)
}

/// Promote a HOSTLESS s3-compliant-alias location so the scheme-agnostic opendal S3 operator can
/// derive the bucket from the URL host. Some vendor blob filesystems (e.g. the Spark McQueen
/// connector) record data/delete file locations WITHOUT a URL host -- `blob:///<bucket>/<key>`
/// (empty authority) or `blob:/<bucket>/<key>` (opaque) -- keeping the bucket as the first path
/// segment. `iceberg-storage-opendal`'s S3 operator reads the bucket from `Url::host_str`, so such
/// a location fails with a missing-bucket error. This lifts the first path segment into the host
/// (yielding `s3://<bucket>/<key>`), reusing [`rewrite_alias_to_s3`].
///
/// Host-bearing locations (`blob://<bucket>/<key>`, `s3://...`) are returned unchanged. This runs
/// ONLY at the storage open boundary inside [`BlobHostPromotingS3Storage`], never on the raw
/// recorded string iceberg-rust matches positional/equality deletes against (the FileScanTask
/// `data_file_path` and the delete file's stored `file_path` column), so promotion cannot desync
/// the two and silently drop deletes.
pub(crate) fn promote_hostless_alias_url(path: &str) -> Result<Cow<'_, str>, ExecutionError> {
    let url = Url::parse(path)
        .map_err(|e| ExecutionError::GeneralError(format!("Error parsing URL {path}: {e}")))?;
    if url.host_str().is_some() {
        // Already addressable by host (e.g. blob://bucket/key): open as-is.
        return Ok(Cow::Borrowed(path));
    }
    Ok(Cow::Owned(rewrite_alias_to_s3(url)?.to_string()))
}

/// [`StorageFactory`] for hostless-capable s3-compliant-alias Iceberg scans
/// (`fs.comet.s3Compliant.schemes`, e.g. `blob`). It builds the crate's scheme-agnostic opendal S3
/// storage and wraps it in [`BlobHostPromotingS3Storage`] so a hostless alias location is opened by
/// promoting the bucket from the first path segment. Only alias schemes route here; plain
/// `s3`/`s3a` keep the unwrapped [`OpenDalStorageFactory::S3`] (see
/// `iceberg_common::storage_factory_for`).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlobHostPromotingS3StorageFactory {
    #[serde(skip)]
    customized_credential_load: Option<CustomAwsCredentialLoader>,
}

impl BlobHostPromotingS3StorageFactory {
    pub(crate) fn new(customized_credential_load: Option<CustomAwsCredentialLoader>) -> Self {
        Self {
            customized_credential_load,
        }
    }
}

#[typetag::serde(name = "CometBlobHostPromotingS3StorageFactory")]
impl StorageFactory for BlobHostPromotingS3StorageFactory {
    fn build(&self, config: &StorageConfig) -> IcebergResult<Arc<dyn Storage>> {
        Ok(Arc::new(BlobHostPromotingS3Storage {
            props: config.props().clone(),
            inner: OnceLock::new(),
            customized_credential_load: self.customized_credential_load.clone(),
        }))
    }
}

/// [`Storage`] that promotes hostless s3-compliant-alias locations (see
/// [`promote_hostless_alias_url`]) at the open boundary, then delegates to the crate's opendal S3
/// storage, which it builds lazily and caches. `props`/`customized_credential_load` mirror
/// [`OpenDalStorageFactory::S3`]'s inputs so the wrapped storage is exactly the one it would build.
/// Serde mirrors the crate's `OpenDalResolvingStorage` (props carried, operator cache and
/// credential loader skipped) only to satisfy the `Storage` typetag supertraits; Comet never
/// serializes storage during a scan.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlobHostPromotingS3Storage {
    props: HashMap<String, String>,
    #[serde(skip, default)]
    inner: OnceLock<Arc<dyn Storage>>,
    #[serde(skip)]
    customized_credential_load: Option<CustomAwsCredentialLoader>,
}

impl BlobHostPromotingS3Storage {
    /// Lazily build and cache the wrapped opendal S3 storage from the same inputs
    /// [`OpenDalStorageFactory::S3`] uses.
    fn inner(&self) -> IcebergResult<Arc<dyn Storage>> {
        if let Some(inner) = self.inner.get() {
            return Ok(Arc::clone(inner));
        }
        let built = OpenDalStorageFactory::S3 {
            customized_credential_load: self.customized_credential_load.clone(),
        }
        .build(&StorageConfig::from_props(self.props.clone()))?;
        let _ = self.inner.set(built);
        Ok(Arc::clone(self.inner.get().unwrap()))
    }

    /// Promote a hostless path, surfacing failures as an iceberg [`Error`].
    fn promote<'a>(&self, path: &'a str) -> IcebergResult<Cow<'a, str>> {
        promote_hostless_alias_url(path)
            .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))
    }
}

#[async_trait]
#[typetag::serde(name = "CometBlobHostPromotingS3Storage")]
impl Storage for BlobHostPromotingS3Storage {
    async fn exists(&self, path: &str) -> IcebergResult<bool> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.exists(promoted.as_ref()).await
    }

    async fn metadata(&self, path: &str) -> IcebergResult<FileMetadata> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.metadata(promoted.as_ref()).await
    }

    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.read(promoted.as_ref()).await
    }

    async fn reader(&self, path: &str) -> IcebergResult<Box<dyn FileRead>> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.reader(promoted.as_ref()).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> IcebergResult<()> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.write(promoted.as_ref(), bs).await
    }

    async fn writer(&self, path: &str) -> IcebergResult<Box<dyn FileWrite>> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.writer(promoted.as_ref()).await
    }

    async fn delete(&self, path: &str) -> IcebergResult<()> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.delete(promoted.as_ref()).await
    }

    async fn delete_prefix(&self, path: &str) -> IcebergResult<()> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.delete_prefix(promoted.as_ref()).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> IcebergResult<()> {
        // Not used by read scans; promote each path best-effort (a parse failure keeps the original
        // so the inner storage surfaces it) to stay correct if a caller ever deletes via an alias.
        let promoted = paths.map(|p| {
            // Compute an owned result first so `p`'s borrow ends before the fallback move.
            let rewritten = promote_hostless_alias_url(&p).map(Cow::into_owned);
            rewritten.unwrap_or(p)
        });
        self.inner()?.delete_stream(promoted.boxed()).await
    }

    fn new_input(&self, path: &str) -> IcebergResult<InputFile> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.new_input(promoted.as_ref())
    }

    fn new_output(&self, path: &str) -> IcebergResult<OutputFile> {
        let inner = self.inner()?;
        let promoted = self.promote(path)?;
        inner.new_output(promoted.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Config opting `blob` in as an s3-compliant alias (`fs.comet.s3Compliant.schemes=blob`).
    fn blob_alias_configs() -> HashMap<String, String> {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            "blob".to_string(),
        );
        configs
    }

    fn normalized(url: &str, configs: &HashMap<String, String>) -> String {
        normalize_object_store_url(url, configs)
            .unwrap()
            .as_str()
            .to_string()
    }

    #[test]
    fn test_normalize_object_store_url_rewrites_configured_alias() {
        // A configured alias (blob) is rewritten to canonical s3:// so
        // `prepare_object_store_with_configs` dispatches on `scheme == "s3"`.
        let configs = blob_alias_configs();
        assert_eq!(
            normalized("blob://bucket/key.parquet", &configs),
            "s3://bucket/key.parquet"
        );
    }

    #[test]
    fn test_normalize_object_store_url_alias_is_opt_in() {
        // Empty config => blob is NOT an alias and is returned unchanged.
        let empty = HashMap::new();
        assert_eq!(
            normalized("blob://bucket/key.parquet", &empty),
            "blob://bucket/key.parquet"
        );

        // The list is comma-separated, whitespace-trimmed, and case-insensitive.
        let mut configs = HashMap::new();
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            " cos , BLOB ".to_string(),
        );
        assert_eq!(
            normalized("blob://bucket/key.parquet", &configs),
            "s3://bucket/key.parquet"
        );
    }

    #[test]
    fn test_normalize_object_store_url_rewrites_s3a_always() {
        // s3a is rewritten to s3 regardless of the alias config (object_store dispatches on s3).
        let empty = HashMap::new();
        assert_eq!(
            normalized("s3a://bucket/key.parquet", &empty),
            "s3://bucket/key.parquet"
        );
    }

    #[test]
    fn test_normalize_object_store_url_passes_through_non_alias_schemes() {
        // s3:// and file:// are not aliases and round-trip unchanged.
        let configs = blob_alias_configs();
        let s3 = "s3://bucket/key.parquet";
        assert_eq!(normalized(s3, &configs), s3);
        let file = "file:///tmp/warehouse/db/t";
        assert_eq!(normalized(file, &configs), file);
    }

    #[test]
    fn test_normalize_object_store_url_promotes_authorityless_alias() {
        // The opaque single-slash `blob:/bucket/key` form (host=None) lifts the first path segment
        // into the host so `ObjectStoreScheme::parse` accepts the result.
        let configs = blob_alias_configs();
        assert_eq!(
            normalized("blob:/bucket/key.parquet", &configs),
            "s3://bucket/key.parquet"
        );
    }

    #[test]
    fn test_is_s3_compliant_alias_scheme_opt_in() {
        let empty = HashMap::new();
        assert!(!is_s3_compliant_alias_scheme("blob", &empty));

        let configs = blob_alias_configs();
        assert!(is_s3_compliant_alias_scheme("blob", &configs));
        // Case-insensitive; s3a is intentionally NOT reported here (callers special-case it).
        assert!(is_s3_compliant_alias_scheme("BLOB", &configs));
        assert!(!is_s3_compliant_alias_scheme("s3a", &configs));
    }

    #[test]
    fn test_is_s3_compliant_alias_scheme_excludes_url_special_schemes() {
        // A URL-spec special scheme is never an alias even if the user lists it: it cannot be
        // rewritten to `s3` (`Url::set_scheme` refuses it), so admitting it would turn a clean
        // fallback into a hard error. Guards against a stray `file`/`https` in the list.
        let mut configs = HashMap::new();
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            "file,http,https,ftp,ws,wss,blob".to_string(),
        );
        for special in ["file", "http", "https", "ftp", "ws", "wss"] {
            assert!(
                !is_s3_compliant_alias_scheme(special, &configs),
                "{special} must not be treated as an s3-compliant alias"
            );
        }
        // A non-special scheme in the same list is still honored.
        assert!(is_s3_compliant_alias_scheme("blob", &configs));
    }

    #[test]
    fn test_normalize_object_store_url_special_scheme_alias_falls_back() {
        // Listing `file` must not hard-error local scans: normalize returns the URL unchanged so the
        // caller falls back, rather than failing in `set_scheme`.
        let mut configs = HashMap::new();
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            "file".to_string(),
        );
        assert_eq!(
            normalized("file:///tmp/warehouse/db/t", &configs),
            "file:///tmp/warehouse/db/t"
        );
    }

    fn promoted(path: &str) -> String {
        promote_hostless_alias_url(path).unwrap().into_owned()
    }

    #[test]
    fn test_promote_hostless_alias_url_promotes_hostless_forms() {
        // Both hostless spellings (host=None) promote the first path segment into the host so the
        // opendal S3 operator finds the bucket: the empty-authority `blob:///bucket/key` form (the
        // McQueen default store) and the opaque single-slash `blob:/bucket/key` form java.net.URI
        // re-renders it to.
        assert_eq!(
            promoted("blob:///sparkinsightdev/tmp/warehouse/x.parquet"),
            "s3://sparkinsightdev/tmp/warehouse/x.parquet"
        );
        assert_eq!(
            promoted("blob:/bucket/a/b/c.parquet"),
            "s3://bucket/a/b/c.parquet"
        );
    }

    #[test]
    fn test_promote_hostless_alias_url_passthrough_when_host_present() {
        // Host-bearing locations are opened as-is: promotion runs only when the URL is hostless.
        for p in [
            "blob://bucket/key.parquet",
            "s3://bucket/key.parquet",
            "s3a://bucket/key.parquet",
        ] {
            assert_eq!(
                promoted(p),
                p,
                "host-bearing {p} must pass through unchanged"
            );
        }
    }

    #[test]
    fn test_promote_hostless_alias_url_requires_a_bucket_segment() {
        // A hostless URL with no promotable first segment errors, so the scan fails loudly rather
        // than opening a bucketless URL. (The JVM gate already declines these; defense in depth.)
        assert!(promote_hostless_alias_url("blob:///").is_err());
    }

    #[test]
    fn test_blob_host_promoting_factory_builds_storage() {
        // The factory constructs a storage without eagerly building the inner operator (lazy), so
        // this smoke test does not require any live backend.
        let factory = BlobHostPromotingS3StorageFactory::new(None);
        assert!(factory
            .build(&StorageConfig::from_props(HashMap::new()))
            .is_ok());
    }
}
