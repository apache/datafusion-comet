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
//! `s3://bucket/key`. Its callers consume only `url.scheme()`/`url.path()`, so re-serialization
//! through `url::Url` is harmless.
//!
//! The Iceberg path never rewrites the recorded location string that iceberg-rust holds, because
//! deletes are matched against it by exact string compare (see `planner.rs`'s `data_file_path`).
//! `iceberg_common::storage_factory_for` routes an alias scheme to its S3 backend (via
//! [`is_s3_compliant_alias_scheme`]), and iceberg-storage-opendal's scheme-agnostic S3 operator
//! opens a host-bearing `blob://bucket/key` as-is (bucket from `Url::host_str`).
//!
//! Some vendor blob filesystems record HOSTLESS locations (`blob:///bucket/key`, bucket as first
//! path segment) that operator cannot open. For alias scans, [`BlobHostPromotingS3Storage`] wraps
//! the S3 backend and promotes the bucket from the first path segment (via
//! [`promote_hostless_alias_url`]) at the open boundary.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

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
use crate::parquet::parquet_support::{is_hdfs_scheme, scheme_in_list};

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
    let scheme = url.scheme();
    if scheme != "s3a" && !is_s3_compliant_alias_scheme(scheme, object_store_configs) {
        return Ok(url);
    }
    rewrite_alias_to_s3(url)
}

/// True if `scheme` is a configured s3-compliant alias (`fs.comet.s3Compliant.schemes`; empty or
/// unset means none). These route to `AmazonS3` but are not recognized by
/// `ObjectStoreScheme::parse`. `s3a` is excluded -- object_store knows it and callers special-case
/// it. Shared by the Parquet normalizer above and the Iceberg storage-factory gate
/// (`iceberg_common::storage_factory_for`) so both admit the same schemes.
pub(crate) fn is_s3_compliant_alias_scheme(
    scheme: &str,
    object_store_configs: &HashMap<String, String>,
) -> bool {
    const COMET_S3_COMPLIANT_SCHEMES_KEY: &str = "fs.comet.s3Compliant.schemes";
    let configured = match object_store_configs.get(COMET_S3_COMPLIANT_SCHEMES_KEY) {
        Some(schemes) => schemes,
        None => return false,
    };
    // The WHATWG "special" schemes can never be an alias: `Url::set_scheme` refuses to rewrite
    // between a special and a non-special scheme, so admitting a stray `file`/`https` from the
    // user-typed list would turn every matching scan into a hard error instead of a clean
    // fallback. Load-bearing for the Iceberg caller too, which never rewrites: a listed `file`
    // would otherwise be routed to S3 and misdirect local reads.
    const URL_SPEC_SPECIAL_SCHEMES: [&str; 6] = ["file", "http", "https", "ftp", "ws", "wss"];
    if URL_SPEC_SPECIAL_SCHEMES
        .iter()
        .any(|s| s.eq_ignore_ascii_case(scheme))
    {
        return false;
    }
    scheme_in_list(configured, scheme)
}

/// Rewrites a parsed alias URL (`s3a` or a configured alias) to `s3://bucket/key`. When it has no
/// authority (empty-authority `blob:///bucket/key` or opaque `blob:/bucket/key`, both host=None)
/// the first path segment is promoted into the host. Defensive: object-store URLs normally have one.
fn rewrite_alias_to_s3(mut url: Url) -> Result<Url, ExecutionError> {
    if url.host_str().is_none() {
        let trimmed = url.path().trim_start_matches('/');
        let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
        if bucket.is_empty() {
            return Err(ExecutionError::GeneralError(format!(
                "{}:// URL is missing bucket name: {url}",
                url.scheme()
            )));
        }
        return Url::parse(&format!("s3://{bucket}/{key}")).map_err(|e| {
            ExecutionError::GeneralError(format!(
                "Could not normalize {}:// URL: {e}",
                url.scheme()
            ))
        });
    }
    // s3 and the aliases are all non-special, so `set_scheme` succeeds and the host/path are
    // preserved verbatim. On failure `url` is untouched, so it still reports the original scheme.
    if url.set_scheme("s3").is_err() {
        return Err(ExecutionError::GeneralError(format!(
            "Could not convert scheme from {} to s3",
            url.scheme()
        )));
    }
    Ok(url)
}

/// Promote a HOSTLESS s3-compliant-alias location so the scheme-agnostic opendal S3 operator can
/// derive the bucket from the URL host. Some vendor blob filesystems record data/delete file
/// locations WITHOUT a URL host -- `blob:///<bucket>/<key>` (empty authority) or
/// `blob:/<bucket>/<key>` (opaque) -- keeping the bucket as the first path segment, which the
/// operator fails to open with a missing-bucket error.
///
/// This runs ONLY at the storage open boundary inside [`BlobHostPromotingS3Storage`], never on the
/// raw recorded string iceberg-rust matches positional/equality deletes against, so promotion
/// cannot desync the two and silently drop deletes.
pub(crate) fn promote_hostless_alias_url(path: &str) -> Result<Cow<'_, str>, ExecutionError> {
    let url = Url::parse(path)
        .map_err(|e| ExecutionError::GeneralError(format!("Error parsing URL {path}: {e}")))?;
    if url.host_str().is_some() {
        return Ok(Cow::Borrowed(path));
    }
    Ok(Cow::Owned(rewrite_alias_to_s3(url)?.to_string()))
}

/// [`StorageFactory`] for hostless-capable s3-compliant-alias Iceberg scans
/// (`fs.comet.s3Compliant.schemes`, e.g. `blob`). Wraps [`OpenDalStorageFactory::S3`] in
/// [`BlobHostPromotingS3Storage`]. Only alias schemes route here; plain `s3`/`s3a` keep the
/// unwrapped factory (see `iceberg_common::storage_factory_for`).
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
        let inner = OpenDalStorageFactory::S3 {
            customized_credential_load: self.customized_credential_load.clone(),
        }
        .build(config)?;
        Ok(Arc::new(BlobHostPromotingS3Storage { inner }))
    }
}

/// [`Storage`] that promotes hostless s3-compliant-alias locations (see
/// [`promote_hostless_alias_url`]) at the open boundary, then delegates to the opendal S3 storage
/// [`OpenDalStorageFactory::S3`] built from the same [`StorageConfig`]. Serde exists only to
/// satisfy the `Storage` typetag supertraits; Comet never serializes storage during a scan.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlobHostPromotingS3Storage {
    inner: Arc<dyn Storage>,
}

impl BlobHostPromotingS3Storage {
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
        self.inner.exists(self.promote(path)?.as_ref()).await
    }

    async fn metadata(&self, path: &str) -> IcebergResult<FileMetadata> {
        self.inner.metadata(self.promote(path)?.as_ref()).await
    }

    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        self.inner.read(self.promote(path)?.as_ref()).await
    }

    async fn reader(&self, path: &str) -> IcebergResult<Box<dyn FileRead>> {
        self.inner.reader(self.promote(path)?.as_ref()).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> IcebergResult<()> {
        self.inner.write(self.promote(path)?.as_ref(), bs).await
    }

    async fn writer(&self, path: &str) -> IcebergResult<Box<dyn FileWrite>> {
        self.inner.writer(self.promote(path)?.as_ref()).await
    }

    async fn delete(&self, path: &str) -> IcebergResult<()> {
        self.inner.delete(self.promote(path)?.as_ref()).await
    }

    async fn delete_prefix(&self, path: &str) -> IcebergResult<()> {
        self.inner.delete_prefix(self.promote(path)?.as_ref()).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> IcebergResult<()> {
        // Not used by read scans; promote each path best-effort (a parse failure keeps the original
        // so the inner storage surfaces it) to stay correct if a caller ever deletes via an alias.
        let promoted = paths.map(|p| {
            // Compute an owned result first so `p`'s borrow ends before the fallback move.
            let rewritten = promote_hostless_alias_url(&p).map(Cow::into_owned);
            rewritten.unwrap_or(p)
        });
        self.inner.delete_stream(promoted.boxed()).await
    }

    fn new_input(&self, path: &str) -> IcebergResult<InputFile> {
        self.inner.new_input(self.promote(path)?.as_ref())
    }

    fn new_output(&self, path: &str) -> IcebergResult<OutputFile> {
        self.inner.new_output(self.promote(path)?.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalized(url: &str, configs: &HashMap<String, String>) -> String {
        normalize_object_store_url(url, configs)
            .unwrap()
            .as_str()
            .to_string()
    }

    /// Config opting the given comma-separated schemes in as s3-compliant aliases.
    fn configs_with(schemes: &str) -> HashMap<String, String> {
        let mut configs = HashMap::new();
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            schemes.to_string(),
        );
        configs
    }

    #[test]
    fn test_normalize_object_store_url() {
        let blob = configs_with("blob");
        let empty = HashMap::new();
        for (input, configs, expected) in [
            // Opt-in: blob is left alone until the user lists it, and the list is comma-separated,
            // whitespace-trimmed, and case-insensitive.
            (
                "blob://bucket/key.parquet",
                &empty,
                "blob://bucket/key.parquet",
            ),
            (
                "blob://bucket/key.parquet",
                &configs_with(" cos , BLOB "),
                "s3://bucket/key.parquet",
            ),
            // s3a is rewritten regardless of the alias config (object_store dispatches on s3).
            (
                "s3a://bucket/key.parquet",
                &empty,
                "s3://bucket/key.parquet",
            ),
            // Non-alias schemes round-trip unchanged.
            ("s3://bucket/key.parquet", &blob, "s3://bucket/key.parquet"),
            (
                "file:///tmp/warehouse/db/t",
                &blob,
                "file:///tmp/warehouse/db/t",
            ),
            // Listing a URL-spec special scheme must not hard-error local scans: the URL comes back
            // unchanged so the caller falls back, rather than failing in `set_scheme`.
            (
                "file:///tmp/warehouse/db/t",
                &configs_with("file"),
                "file:///tmp/warehouse/db/t",
            ),
            // Hostless alias: the first path segment is lifted into the host so
            // `ObjectStoreScheme::parse` accepts the result.
            ("blob:/bucket/key.parquet", &blob, "s3://bucket/key.parquet"),
        ] {
            assert_eq!(normalized(input, configs), expected, "input: {input}");
        }
    }

    #[test]
    fn test_is_s3_compliant_alias_scheme() {
        // Opt-in and case-insensitive; `s3a` is intentionally NOT reported (callers special-case
        // it). A URL-spec special scheme is never an alias even when the user lists it: it cannot
        // be rewritten to `s3` (`Url::set_scheme` refuses it), so admitting one would turn a clean
        // fallback into a hard error.
        let special = "file,http,https,ftp,ws,wss,blob";
        for (scheme, configured, expected) in [
            ("blob", "", false),
            ("blob", "blob", true),
            ("BLOB", "blob", true),
            ("s3a", "blob", false),
            ("file", special, false),
            ("http", special, false),
            ("https", special, false),
            ("ftp", special, false),
            ("ws", special, false),
            ("wss", special, false),
            // A non-special scheme in the same list is still honored.
            ("blob", special, true),
        ] {
            let configs = if configured.is_empty() {
                HashMap::new()
            } else {
                configs_with(configured)
            };
            assert_eq!(
                is_s3_compliant_alias_scheme(scheme, &configs),
                expected,
                "scheme {scheme} with list {configured:?}"
            );
        }
    }

    #[test]
    fn test_promote_hostless_alias_url() {
        // Hostless spellings (host=None) promote the first path segment into the host so the
        // opendal S3 operator finds the bucket: the empty-authority `blob:///bucket/key` form (the
        // vendor default store) and the opaque single-slash `blob:/bucket/key` form java.net.URI
        // re-renders it to. Host-bearing locations are opened as-is.
        for (input, expected) in [
            (
                "blob:///sparkinsightdev/tmp/warehouse/x.parquet",
                "s3://sparkinsightdev/tmp/warehouse/x.parquet",
            ),
            ("blob:/bucket/a/b/c.parquet", "s3://bucket/a/b/c.parquet"),
            ("blob://bucket/key.parquet", "blob://bucket/key.parquet"),
            ("s3://bucket/key.parquet", "s3://bucket/key.parquet"),
            ("s3a://bucket/key.parquet", "s3a://bucket/key.parquet"),
        ] {
            assert_eq!(
                promote_hostless_alias_url(input).unwrap(),
                expected,
                "input: {input}"
            );
        }
        // A hostless URL with no promotable first segment errors, so the scan fails loudly rather
        // than opening a bucketless URL. (The JVM gate already declines these; defense in depth.)
        assert!(promote_hostless_alias_url("blob:///").is_err());
    }
}
