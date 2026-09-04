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

//! Helpers shared between the Iceberg scan and Iceberg write operators.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg_storage_opendal::{CustomAwsCredentialLoader, OpenDalStorageFactory};

use crate::cloud::s3::credential_bridge::{AccessMode, CometS3CredentialBridge};
use crate::parquet::objectstore::s3_blob_fs_support::{
    is_s3_compliant_alias_scheme, BlobHostPromotingS3StorageFactory,
};

/// Activation key for the `CometS3CredentialProvider` SPI, read from a catalog's `s3.*` property
/// bag.
const ICEBERG_PROVIDER_CLASS_PROPERTY: &str = "s3.comet.credential.provider.class";

/// Key prefixes forwarded to iceberg-rust's `FileIO`. The full unfiltered catalog bag (catalog
/// URI, OAuth tokens, credentials.uri, tenant-id, etc.) is kept upstream so
/// `CometS3CredentialBridge` can read whatever the vendor needs.
const STORAGE_PROPERTY_PREFIXES: &[&str] = &["s3.", "gcs.", "adls.", "client."];

/// Pick an OpenDAL storage backend from a URI's scheme. `file` (or no scheme) falls through to
/// the local file system. `memory` is used by the write path to assemble manifest bytes that
/// stay entirely in-process. For S3, the Comet credential bridge is wired in when a provider
/// class is configured; `access_mode` is forwarded to the JVM SPI so the read and write paths can
/// be granted different (e.g. read-only vs read-write) credentials.
pub(crate) fn storage_factory_for(
    path: &str,
    catalog_properties: &HashMap<String, String>,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<Arc<dyn StorageFactory>, DataFusionError> {
    let scheme = scheme_of(path);
    match scheme {
        // s3, s3a, and any opted-in s3-compliant alias (e.g. blob) all route to the S3 backend.
        // Host-bearing paths reach iceberg-storage-opendal's scheme-agnostic S3 operator RAW (it
        // takes the bucket from `Url::host_str`), so `blob://bucket/key` opens directly. An alias
        // additionally gets a wrapper that promotes a HOSTLESS `blob:///bucket/key` into the host
        // at the open boundary. Both leave the recorded string iceberg-rust matches deletes
        // against untouched -- see s3_blob_fs_support for that delete-safety rationale.
        s if is_s3_family_scheme(s, catalog_properties) => {
            let customized_credential_load =
                build_s3_credential_loader(path, catalog_properties, catalog_name, access_mode)?;
            if is_s3_compliant_alias_scheme(s, catalog_properties) {
                Ok(Arc::new(BlobHostPromotingS3StorageFactory::new(
                    customized_credential_load,
                )))
            } else {
                Ok(Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load,
                }))
            }
        }
        "file" => Ok(Arc::new(OpenDalStorageFactory::Fs)),
        "memory" => Ok(Arc::new(OpenDalStorageFactory::Memory)),
        "gs" => Ok(Arc::new(OpenDalStorageFactory::Gcs)),
        // Reads keep the OSS backend they have always had (CometScanRule admits `oss` scan
        // locations through HadoopFileIO). Writes fail closed: Comet does not forward `oss.*`
        // properties into the FileIO and no test covers the write path, so OSS-specific
        // endpoint/credential configuration could silently be dropped. The JVM write gate
        // already declines `oss` locations; this is the native-side backstop.
        "oss" => match access_mode {
            AccessMode::Read => Ok(Arc::new(OpenDalStorageFactory::Oss)),
            AccessMode::Write => Err(DataFusionError::Execution(
                "OSS is not supported for native Iceberg writes (oss.* properties are not \
                 forwarded to the native FileIO)"
                    .to_string(),
            )),
        },
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported storage scheme: {scheme}"
        ))),
    }
}

/// Build a `FileIO` whose storage scheme is inferred from `reference_path` and whose properties
/// come from the catalog. The reference path is the metadata location for reads or the data
/// location for writes — anything that carries the right URI scheme. `catalog_name` is the
/// credential dispatch key and `access_mode` is the access intent forwarded to the S3 credential
/// bridge, so the write path can request write-capable credentials.
pub(crate) fn load_file_io(
    catalog_properties: &HashMap<String, String>,
    reference_path: &str,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<FileIO, DataFusionError> {
    let factory = storage_factory_for(
        reference_path,
        catalog_properties,
        catalog_name,
        access_mode,
    )?;
    let mut file_io_builder = FileIOBuilder::new(factory);

    // Narrow to storage-prefix keys before forwarding to iceberg-rust's FileIO. The full
    // unfiltered bag (catalog URI, OAuth tokens, credentials.uri, tenant-id, etc.) is kept
    // upstream so CometS3CredentialBridge can read whatever the vendor needs.
    for (key, value) in catalog_properties {
        if STORAGE_PROPERTY_PREFIXES.iter().any(|p| key.starts_with(p)) {
            file_io_builder = file_io_builder.with_prop(key, value);
        }
    }

    // Object-store's AmazonS3Builder defaults the SigV4 region to `us-east-1` when unset;
    // iceberg-storage-opendal's S3 factory instead errors with `region is missing. Please
    // find it by S3::detect_region() or set them in env.` Non-AWS S3-compliant storage
    // services accept any region in the credential, so default to `us-east-1` -- but ONLY when
    // neither the catalog NOR the AWS environment supplies a region. opendal/reqsign reads
    // `AWS_REGION` / `AWS_DEFAULT_REGION`, so forcing `us-east-1` unconditionally would
    // override an `AWS_REGION=us-west-2` and break auth for AWS buckets outside us-east-1.
    // Both catalog key spellings iceberg-rust reads (`client.region` wins over `s3.region`).
    let region_forwarded = catalog_properties.contains_key("s3.region")
        || catalog_properties.contains_key("client.region")
        || env_region_present();
    // reference_path reaches here raw (see storage_factory_for), so alias schemes must match too.
    let scheme = scheme_of(reference_path);
    if !region_forwarded && is_s3_family_scheme(scheme, catalog_properties) {
        file_io_builder = file_io_builder.with_prop("s3.region", "us-east-1");
    }

    Ok(file_io_builder.build())
}

/// Wires the configured Comet credential provider into opendal's S3 service. `Ok(None)` means no
/// provider is configured (or the path carries no bucket) and opendal's default credential chain
/// applies. When a provider IS configured but fails to initialize, the failure mode depends on
/// the access intent: reads warn and fall back to the default chain (a wrong-credential read
/// fails on permissions), but writes fail closed -- silently switching which credentials perform
/// a write after the configured provider failed is not acceptable.
fn build_s3_credential_loader(
    reference_path: &str,
    catalog_properties: &HashMap<String, String>,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Result<Option<CustomAwsCredentialLoader>, DataFusionError> {
    let Ok(url) = url::Url::parse(reference_path) else {
        return Ok(None);
    };
    let Some(bucket) = url.host_str() else {
        return Ok(None);
    };
    let Some(provider_class) = catalog_properties
        .get(ICEBERG_PROVIDER_CLASS_PROPERTY)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    // Fall back to the bucket when the table has no catalog identity (e.g. HadoopTables loaded by
    // raw path).
    let dispatch_key: &str = if catalog_name.is_empty() {
        bucket
    } else {
        catalog_name
    };
    let bridge = CometS3CredentialBridge::new(
        provider_class,
        dispatch_key,
        bucket,
        url.path(),
        access_mode,
        catalog_properties,
    );
    match bridge {
        Ok(b) => Ok(Some(CustomAwsCredentialLoader::new(b))),
        Err(e) => match access_mode {
            AccessMode::Write => Err(DataFusionError::Execution(format!(
                "Configured S3 credential provider {provider_class} failed to initialize: {e}; \
                 refusing to write through the default opendal credential chain"
            ))),
            AccessMode::Read => {
                log::warn!(
                    "Failed to initialize CometS3CredentialBridge for {provider_class}: {e}; \
                     falling back to default opendal credential chain"
                );
                Ok(None)
            }
        },
    }
}

/// True if the AWS environment supplies a region (see the region defaulting in `load_file_io`).
fn env_region_present() -> bool {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.is_empty()))
}

/// Extracts the URI scheme, defaulting to `file` for schemeless local paths (e.g. `/tmp/x`). Both
/// `storage_factory_for` (backend routing) and `load_file_io` (region defaulting) classify by
/// scheme, so they share this to admit exactly the same set.
///
/// Splits on the first `:` (RFC 3986), NOT `://`: hostless vendor forms like `blob:/bucket/key`
/// (opaque, empty authority) carry no `://`, and treating them as `file` would route an
/// S3-compliant scan to the local filesystem. A `/` before the `:` means there is no scheme (the
/// `:` sits inside a path segment, e.g. `/tmp/a:b`), so those and truly schemeless paths default
/// to `file`.
fn scheme_of(path: &str) -> &str {
    match path.split_once(':') {
        Some((scheme, _)) if !scheme.is_empty() && !scheme.contains('/') => scheme,
        _ => "file",
    }
}

/// True if `scheme` routes to the S3 backend: `s3`/`s3a`, or any opted-in s3-compliant alias
/// (`fs.comet.s3Compliant.schemes`). Both the storage-factory selection and the region default use
/// this, so they admit the same schemes. The Scala `NativeConfig.isS3FamilyScheme` additionally
/// treats `s3n` as S3-family for bucket resolution; `s3n` is intentionally omitted here because
/// iceberg-rust's storage factory has no `s3n` backend and the Scala Iceberg scheme gate
/// (`isIcebergReadableScheme`) rejects `s3n` before a path ever reaches this operator.
fn is_s3_family_scheme(scheme: &str, catalog_properties: &HashMap<String, String>) -> bool {
    matches!(scheme, "s3" | "s3a") || is_s3_compliant_alias_scheme(scheme, catalog_properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn factory_result(path: &str, mode: AccessMode) -> Result<(), String> {
        storage_factory_for(path, &HashMap::new(), "test_cat", mode)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    #[test]
    fn oss_scheme_is_readable_but_not_writable() {
        // CometScanRule admits oss scan locations (through HadoopFileIO), so removing the read
        // arm would regress an existing native-scan capability; writes stay unsupported until
        // oss.* property forwarding exists and is tested.
        assert!(factory_result("oss://bucket/db/table", AccessMode::Read).is_ok());
        let err = factory_result("oss://bucket/db/table", AccessMode::Write).unwrap_err();
        assert!(err.contains("OSS"), "unexpected error: {err}");
    }

    #[test]
    fn common_schemes_resolve_for_both_modes() {
        for mode in [AccessMode::Read, AccessMode::Write] {
            assert!(factory_result("file:///tmp/x", mode).is_ok());
            assert!(factory_result("/tmp/no-scheme", mode).is_ok());
            assert!(factory_result("memory:manifest.avro", mode).is_ok());
            // No credential provider configured: the default chain applies in both modes.
            assert!(factory_result("s3://bucket/db/table", mode).is_ok());
            assert!(factory_result("gs://bucket/db/table", mode).is_ok());
        }
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        let err = factory_result("hdfs://nn/db/table", AccessMode::Read).unwrap_err();
        assert!(
            err.contains("Unsupported storage scheme"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn scheme_of_extracts_scheme_from_all_uri_forms() {
        // Host-bearing and hostless/opaque vendor forms must resolve to the same scheme, so an
        // S3-compliant scan is not misrouted to the local FS. `blob:/bucket/key` (single slash, the
        // hostless vendor form) regressed here: splitting on `://` returned `file`.
        assert_eq!(scheme_of("blob://bucket/key"), "blob");
        assert_eq!(scheme_of("blob:/bucket/key"), "blob");
        assert_eq!(scheme_of("s3://bucket/key"), "s3");
        assert_eq!(scheme_of("file:///tmp/x"), "file");
        // Schemeless and colon-in-path locals default to the local FS.
        assert_eq!(scheme_of("/tmp/no-scheme"), "file");
        assert_eq!(scheme_of("/tmp/a:b"), "file");
    }
}
