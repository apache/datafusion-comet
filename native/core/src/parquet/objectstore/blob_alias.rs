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
//! Comet treats `blob://` and `s3a://` as aliases for `s3://` -- they route through the same
//! `object_store::AmazonS3` / `iceberg-storage-opendal::S3` code paths but the aliased forms are
//! not directly recognized by those crates. This module rewrites the URL shape (scheme +
//! `blob:/bucket/key` single-slash form that Java's `URI.toString()` produces from
//! `blob:///bucket/key` and Iceberg manifests store) so downstream code can treat everything as
//! canonical `s3://bucket/key`.
//!
//! The actual S3 store construction lives in [`super::s3`]; this module is only about URL shape
//! so all the alias code has one home instead of drifting across `parquet_support.rs`,
//! `execution/planner.rs`, and `execution/operators/iceberg_scan.rs`.

use std::collections::HashMap;

use url::Url;

use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::is_hdfs_scheme;

/// Parses `url_str` and rewrites `blob`/`s3a` schemes (and the awkward three-slash
/// `blob:///bucket/key` form that `ObjectStoreScheme::parse` rejects because host=None) to the
/// canonical `s3://bucket/key`. Non-alias schemes are returned unchanged.
///
/// `object_store_configs` is consulted only via `is_hdfs_scheme`: if the user routed `s3a`
/// through libhdfs via `fs.comet.libhdfs.schemes`, we must NOT rewrite it to `s3` -- HDFS
/// handling takes over.
pub(crate) fn normalize_object_store_url(
    url_str: &str,
    object_store_configs: &HashMap<String, String>,
) -> Result<Url, ExecutionError> {
    let mut url = Url::parse(url_str)
        .map_err(|e| ExecutionError::GeneralError(format!("Error parsing URL {url_str}: {e}")))?;
    if is_hdfs_scheme(&url, object_store_configs) {
        return Ok(url);
    }
    let scheme = url.scheme();
    if scheme != "s3a" && scheme != "blob" {
        return Ok(url);
    }
    let original = scheme.to_string();
    let needs_host_promotion = url.host_str().is_none();
    url.set_scheme("s3").map_err(|_| {
        ExecutionError::GeneralError(format!("Could not convert scheme from {original} to s3"))
    })?;
    if needs_host_promotion {
        // Some deployments emit `blob:///bucket/key` (three slashes, empty authority) or Java
        // collapses that to `blob:/bucket/key` (opaque form) in Iceberg manifests. In both,
        // `url::Url` reports host=None and path=`/bucket/key`, but `ObjectStoreScheme::parse`
        // requires a non-empty host. Lift the first path segment into the host.
        let trimmed = url.path().trim_start_matches('/').to_string();
        let (bucket, key) = match trimmed.split_once('/') {
            Some((b, k)) => (b.to_string(), k.to_string()),
            None => (trimmed, String::new()),
        };
        if bucket.is_empty() {
            return Err(ExecutionError::GeneralError(format!(
                "{original}:// URL is missing bucket name: {url}"
            )));
        }
        url = Url::parse(&format!("s3://{bucket}/{key}")).map_err(|e| {
            ExecutionError::GeneralError(format!("Could not normalize {original}:// URL: {e}"))
        })?;
    }
    Ok(url)
}

/// String-returning wrapper: iceberg-rust stores paths as owned strings on
/// `FileScanTask`/`FileScanTaskDeleteFile`, and iceberg-storage-opendal's `storage_factory_for`
/// picks the LocalFs backend for any path without `://` -- so a single-slash `blob:/...` path
/// lands in the fs backend and gets its first character stripped (`&path[1..]`), producing
/// errors like `path: lob:/...`. Rewrite before handing paths to iceberg-rust.
pub(crate) fn normalize_object_store_url_string(path: &str) -> Result<String, ExecutionError> {
    Ok(normalize_object_store_url(path, &HashMap::new())?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_object_store_url_string_blob_single_slash_form() {
        // Java's URI.toString() collapses `blob:///bucket/key` to `blob:/bucket/key` (opaque form
        // with a leading slash on the path), and that's exactly what Iceberg manifests store.
        // Iceberg-storage-opendal's `storage_factory_for` uses `path.contains("://")` to detect
        // the scheme, so a `blob:/...` path routes to the LocalFs backend and its `&path[1..]`
        // fallback strips the first char, producing `lob:/...` errors. Guard: this string helper
        // must produce the canonical `s3://bucket/key` before the path reaches iceberg-rust.
        let out = normalize_object_store_url_string(
            "blob:/mybucket/tmp/warehouse/db/test_table/data/part-0.parquet",
        )
        .expect("single-slash blob URL should normalize");
        assert_eq!(
            out,
            "s3://mybucket/tmp/warehouse/db/test_table/data/part-0.parquet"
        );

        // Two-slash form (authority present) also normalizes to s3://.
        let out = normalize_object_store_url_string("blob://bucket/key.parquet").unwrap();
        assert_eq!(out, "s3://bucket/key.parquet");

        // s3a shares the alias path and gets rewritten too.
        let out = normalize_object_store_url_string("s3a://bucket/key.parquet").unwrap();
        assert_eq!(out, "s3://bucket/key.parquet");

        // Non-alias schemes pass through unchanged so file:// / memory:/ Iceberg paths keep
        // working with iceberg-storage-opendal's Fs / Memory backends.
        let out = normalize_object_store_url_string("file:///tmp/warehouse/db/t").unwrap();
        assert_eq!(out, "file:///tmp/warehouse/db/t");
        let out = normalize_object_store_url_string("s3://bucket/key.parquet").unwrap();
        assert_eq!(out, "s3://bucket/key.parquet");
    }
}
