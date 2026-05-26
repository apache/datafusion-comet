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
    let scheme = if path.contains("://") {
        path.split("://").next().unwrap_or("file")
    } else {
        "file"
    };
    match scheme {
        "file" => Ok(Arc::new(OpenDalStorageFactory::Fs)),
        "memory" => Ok(Arc::new(OpenDalStorageFactory::Memory)),
        "s3" | "s3a" => {
            let customized_credential_load =
                build_s3_credential_loader(path, catalog_properties, catalog_name, access_mode);
            Ok(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load,
            }))
        }
        "gs" => Ok(Arc::new(OpenDalStorageFactory::Gcs)),
        "oss" => Ok(Arc::new(OpenDalStorageFactory::Oss)),
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

    Ok(file_io_builder.build())
}

/// Wires the configured Comet credential provider into opendal's S3 service, or returns `None`
/// so opendal falls back to its default credential chain.
fn build_s3_credential_loader(
    reference_path: &str,
    catalog_properties: &HashMap<String, String>,
    catalog_name: &str,
    access_mode: AccessMode,
) -> Option<CustomAwsCredentialLoader> {
    let url = url::Url::parse(reference_path).ok()?;
    let bucket = url.host_str()?;
    let provider_class = catalog_properties
        .get(ICEBERG_PROVIDER_CLASS_PROPERTY)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())?;
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
        Ok(b) => Some(CustomAwsCredentialLoader::new(b)),
        Err(e) => {
            log::warn!(
                "Failed to initialize CometS3CredentialBridge for {provider_class}: {e}; \
                 falling back to default opendal credential chain"
            );
            None
        }
    }
}
