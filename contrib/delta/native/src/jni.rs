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

//! Driver-side JNI entry point for Delta log replay.
//!
//! Exposes `Java_org_apache_comet_contrib_delta_Native_planDeltaScan`. The Scala driver
//! calls this once per query to ask kernel for the active file list at a
//! given snapshot version, then distributes the returned tasks across
//! Spark executors via Comet's usual split-mode serialization.

use jni::{
    objects::{JByteArray, JClass, JMap, JObject, JString},
    sys::{jbyteArray, jlong},
    Env, EnvUnowned,
};
use prost::Message;

use crate::scan::plan_delta_scan_with_predicate;
use crate::DeltaStorageConfig;
use datafusion_comet_jni_bridge::errors::{try_unwrap_or_throw, CometError, CometResult};
// Proto types now live in this contrib's own proto module (was core's
// datafusion_comet_proto::spark_operator).
use crate::proto::{DeltaPartitionValue, DeltaScanTask, DeltaScanTaskList};

/// `Java_org_apache_comet_contrib_delta_Native_planDeltaScan`.
///
/// # Arguments (JNI wire order)
/// 1. `table_url` — absolute URL or bare path of the Delta table root
/// 2. `snapshot_version` — `-1` for latest, otherwise the exact version
/// 3. `storage_options` — a `java.util.Map<String, String>` of cloud
///    credentials. **Phase 1 currently only consumes a small subset** (the
///    AWS / Azure keys listed in `DeltaStorageConfig`); unknown keys are
///    silently ignored. Full options-map plumbing lands with Phase 2.
///
/// # Returns
/// A Java `byte[]` containing a prost-encoded [`DeltaScanTaskList`]
/// message, or `null` on error (with a `CometNativeException` thrown on
/// the JVM side via `try_unwrap_or_throw`).
///
/// # Safety
/// Inherently unsafe because it dereferences raw JNI pointers.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_contrib_delta_Native_planDeltaScan(
    e: EnvUnowned,
    _class: JClass,
    table_url: JString,
    snapshot_version: jlong,
    storage_options: JObject,
    predicate_bytes: JByteArray,
    column_names: jni::objects::JObjectArray,
) -> jbyteArray {
    try_unwrap_or_throw(&e, |env| {
        let url_str: String = table_url.try_to_string(env)?;
        let version = if snapshot_version < 0 {
            None
        } else {
            Some(snapshot_version as u64)
        };
        let config = if storage_options.is_null() {
            DeltaStorageConfig::default()
        } else {
            let jmap: JMap<'_> = env.cast_local::<JMap>(storage_options)?;
            // TODO(contrib-delta): the rich Hadoop credential-provider chain (PR1 on
            // delta-kernel-phase-1, commit 461fa4f4) called into
            // `core::parquet::objectstore::s3::resolve_static_credentials` to walk
            // SimpleAWSCredentialsProvider / TemporaryAWSCredentialsProvider /
            // AssumedRoleCredentialProvider / IAMInstanceCredentialsProvider. That
            // helper lives in core and is not exposed through `comet-contrib-spi`.
            // For PR2 we'll either (a) move the helper into contrib-spi or a shared
            // leaf crate, or (b) re-implement a Delta-local credential resolver. The
            // local-fs regression doesn't hit this path so we defer for the validation
            // build; cloud-storage Delta tables will need this re-enabled before ship.
            extract_storage_config(env, &jmap)?
        };

        // Phase 2: read column names for BoundReference resolution.
        // storageOptions map carries Hadoop-style keys (fs.s3a.access.key,
        // fs.s3a.secret.key, fs.s3a.endpoint, fs.s3a.path.style.access,
        // fs.s3a.endpoint.region, fs.s3a.session.token) extracted by
        // NativeConfig.extractObjectStoreOptions on the Scala side.
        // extract_storage_config below maps these to kernel's DeltaStorageConfig.
        let col_names = read_string_array(env, &column_names)?;

        // Phase 2: deserialize the Catalyst predicate (if provided) for
        // kernel's stats-based file pruning. Empty bytes = no predicate.
        let _predicate_proto: Option<Vec<u8>> = if predicate_bytes.is_null() {
            None
        } else {
            let bytes = env.convert_byte_array(predicate_bytes)?;
            if bytes.is_empty() {
                None
            } else {
                Some(bytes)
            }
        };

        // Phase 2: translate Catalyst predicate proto to kernel Predicate for
        // stats-based file pruning during log replay. Pass column names for
        // BoundReference index-to-name resolution.
        let kernel_predicate = _predicate_proto.and_then(|bytes| {
            use prost::Message;
            match datafusion_comet_proto::spark_expression::Expr::decode(bytes.as_slice()) {
                Ok(expr) => Some(
                    crate::predicate::catalyst_to_kernel_predicate_with_names(
                        &expr, &col_names,
                    ),
                ),
                Err(e) => {
                    log::warn!(
                        "Failed to decode predicate for Delta file pruning: {e}; \
                         scanning all files"
                    );
                    None
                }
            }
        });

        let plan = plan_delta_scan_with_predicate(&url_str, &config, version, kernel_predicate)
            .map_err(|e| CometError::Internal(format!("delta_kernel log replay failed: {e}")))?;

        // Under column mapping, kernel returns partition_values keyed by the
        // PHYSICAL column name (e.g. `col-<uuid>`), but `partition_schema`
        // (and therefore `build_delta_partitioned_files`'s lookup) uses the
        // LOGICAL name. Build the inverse lookup so we can translate keys
        // back to logical names on the wire.
        let physical_to_logical: std::collections::HashMap<String, String> = plan
            .column_mappings
            .iter()
            .map(|(logical, physical)| (physical.clone(), logical.clone()))
            .collect();

        let tasks: Vec<DeltaScanTask> = plan
            .entries
            .into_iter()
            .map(|entry| DeltaScanTask {
                file_path: resolve_file_path(&url_str, &entry.path),
                file_size: entry.size as u64,
                record_count: entry.num_records,
                // Partition values are produced by kernel as an
                // unordered `HashMap<String, String>` per file. Translate
                // physical -> logical when a column mapping is present so
                // `build_delta_partitioned_files` can match by logical name.
                partition_values: entry
                    .partition_values
                    .into_iter()
                    .map(|(name, value)| {
                        let logical_name = physical_to_logical
                            .get(&name)
                            .cloned()
                            .unwrap_or(name);
                        DeltaPartitionValue {
                            name: logical_name,
                            value: Some(value),
                        }
                    })
                    .collect(),
                // Deletion-vector descriptor (path/offset/size) -- the executor's
                // `DeltaDvFilterExec` calls `kernel::DeletionVectorDescriptor::read`
                // to decode the bitmap on-task. The driver-side `plan_delta_scan`
                // no longer materialises the indexes (task #218 / Iceberg-style
                // refactor: per-scan-exec heap stays KB-scale regardless of DV size).
                dv: entry.dv_descriptor,
                // Row tracking: extracted from each scan-files RecordBatch's
                // `fileConstantValues.baseRowId` / `defaultRowCommitVersion` columns
                // in scan.rs (see `extract_row_tracking_for_selected`). Kernel's
                // `visit_scan_files` callback doesn't surface these; we read them
                // directly. `None` when the table doesn't have row tracking enabled.
                base_row_id: entry.base_row_id,
                default_row_commit_version: entry.default_row_commit_version,
                // Splitting is done on the Scala side just before serialization,
                // not here on the kernel-driver path. Leave unset.
                byte_range_start: None,
                byte_range_end: None,
                // kernel-driver path doesn't surface modification_time today; the
                // BatchFileIndex path (`buildTaskListFromAddFiles` on the Scala side)
                // does set it from AddFile.modificationTime. None here is fine for
                // tables read via kernel log replay -- callers that need
                // `_metadata.file_modification_time` get null (which is what Spark
                // would produce for unknown modification time anyway).
                modification_time: None,
            })
            .collect();

        let column_mappings: Vec<crate::proto::DeltaColumnMapping> = plan
            .column_mappings
            .into_iter()
            .map(
                |(logical, physical)| crate::proto::DeltaColumnMapping {
                    logical_name: logical,
                    physical_name: physical,
                },
            )
            .collect();

        let msg = DeltaScanTaskList {
            snapshot_version: plan.version,
            table_root: url_str,
            tasks,
            unsupported_features: plan.unsupported_features,
            column_mappings,
        };

        let bytes = msg.encode_to_vec();
        let result = env.byte_array_from_slice(&bytes)?;
        Ok(result.into_raw())
    })
}

/// Join `entry.path` (Delta add-action path, usually relative to the
/// table root) with `table_root` to yield an absolute URL the native-side
/// `build_delta_partitioned_files` can feed straight into
/// `object_store::path::Path::from_url_path`.
fn resolve_file_path(table_root: &str, relative: &str) -> String {
    // Fully-qualified paths (kernel surfaces these for some tables, e.g. after
    // MERGE, REPLACE, or SHALLOW CLONE) pass through untouched. Accept both
    // `file:///abs` (authority form) and `file:/abs` (Hadoop `Path.toUri` form,
    // which SHALLOW CLONE uses when it stores absolute paths in AddFile.path).
    if has_uri_scheme(relative) {
        return relative.to_string();
    }

    if table_root.ends_with('/') {
        format!("{table_root}{relative}")
    } else {
        format!("{table_root}/{relative}")
    }
}

/// True if `s` starts with a URI scheme — `^[A-Za-z][A-Za-z0-9+.-]*:` per RFC 3986.
/// We check the scheme only (not whether a `//` authority follows) because Hadoop's
/// `Path.toUri.toString` emits `file:/abs` (single slash) for local absolute paths
/// and Delta stores that form verbatim in AddFile.path for SHALLOW CLONE tables.
fn has_uri_scheme(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_alphabetic() {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate().skip(1) {
        if b == b':' {
            return i >= 1;
        }
        if !(b.is_ascii_alphanumeric() || b == b'+' || b == b'-' || b == b'.') {
            return false;
        }
    }
    false
}

/// Walk a `java.util.Map<String, String>` of storage options into a
/// [`DeltaStorageConfig`]. Checks both kernel-style keys (`aws_access_key_id`)
/// and Hadoop-style keys (`fs.s3a.access.key`) since Comet's
/// `NativeConfig.extractObjectStoreOptions` passes the latter.
///
/// Reads the JMap into a Rust `HashMap` and delegates to
/// [`delta_storage_config_from_map`] for the actual key mapping. Splitting
/// the JNI traversal from the mapping logic lets the mapping be unit-tested
/// without a JVM.
fn extract_storage_config(env: &mut Env, jmap: &JMap<'_>) -> CometResult<DeltaStorageConfig> {
    let m = jmap_to_hashmap(env, jmap)?;
    Ok(delta_storage_config_from_map(&m))
}

/// Pure (JVM-free) mapping from a generic options map to [`DeltaStorageConfig`].
///
/// Accepts kernel-style keys (`aws_access_key_id`, `azure_account_name`, ...)
/// AND Hadoop-style keys that Comet's `NativeConfig.extractObjectStoreOptions`
/// produces on the Scala side (`fs.s3a.access.key`, ...). The kernel-style key
/// wins when both are present.
///
/// Known gaps tracked by the credential audit:
///   - GCS (`gs://`) is unsupported -- no `gcp_*` fields, no
///     `fs.gs.*` translation; reads against GCS would fail at
///     `create_object_store` with `UnsupportedScheme`.
///   - Per-bucket S3 keys (`fs.s3a.bucket.<name>.*`) are not extracted;
///     multi-bucket setups fall back to global creds.
///   - Hadoop Azure connector keys (`fs.azure.account.key.<account>`,
///     OAuth client id/secret, MSI tokens, SAS tokens) are not bridged --
///     only the three kernel-style azure keys flow through.
///
/// These gaps are intentional for the initial cut: every active test path
/// uses local-fs, and the Iceberg-style key translation should land before
/// shipping cloud credentials in production. New cred shapes that surface
/// in tests should grow new branches here AND a corresponding entry in
/// [`tests::extract_storage_config_matrix`] below.
pub fn delta_storage_config_from_map(
    m: &std::collections::HashMap<String, String>,
) -> DeltaStorageConfig {
    let kernel_or_hadoop = |k1: &str, k2: &str| m.get(k1).or_else(|| m.get(k2)).cloned();
    DeltaStorageConfig {
        aws_access_key: kernel_or_hadoop("aws_access_key_id", "fs.s3a.access.key"),
        aws_secret_key: kernel_or_hadoop("aws_secret_access_key", "fs.s3a.secret.key"),
        aws_session_token: kernel_or_hadoop("aws_session_token", "fs.s3a.session.token"),
        aws_region: kernel_or_hadoop("aws_region", "fs.s3a.endpoint.region")
            .or_else(|| m.get("fs.s3a.region").cloned()),
        aws_endpoint: kernel_or_hadoop("aws_endpoint", "fs.s3a.endpoint"),
        aws_force_path_style: kernel_or_hadoop(
            "aws_force_path_style",
            "fs.s3a.path.style.access",
        )
        .map(|s| s == "true")
        .unwrap_or(false),
        azure_account_name: m.get("azure_account_name").cloned(),
        azure_access_key: m.get("azure_access_key").cloned(),
        azure_bearer_token: m.get("azure_bearer_token").cloned(),
    }
}

/// Read a Java `String[]` into a `Vec<String>`. Returns empty vec for null arrays.
fn read_string_array(env: &mut Env, arr: &jni::objects::JObjectArray) -> CometResult<Vec<String>> {
    if arr.is_null() {
        return Ok(Vec::new());
    }
    let len = arr.len(env)?;
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let obj = arr.get_element(env, i)?;
        // SAFETY: get_element returns a valid local JObject reference that we
        // immediately convert to JString. The array is String[], so the cast
        // is valid. The env lifetime outlives this scope.
        let jstr = unsafe { JString::from_raw(env, obj.into_raw()) };
        result.push(jstr.try_to_string(env)?);
    }
    Ok(result)
}

/// Iterate a `java.util.Map<String, String>` into a Rust `HashMap`. Used when we need to
/// pass the full Hadoop config map to a downstream consumer (e.g.,
/// `s3::resolve_static_credentials`) that walks its own provider chain.
///
/// Uses `env.cast_local::<JString>(...)` to safely downcast each key/value entry rather
/// than the `unsafe { JString::from_raw(..., into_raw()) }` shortcut used elsewhere in
/// this file -- the runtime cast performs the same JNI-side type check the JLS implies
/// for `Map<String, String>` but without the unchecked transmute.
fn jmap_to_hashmap(
    env: &mut Env,
    jmap: &JMap<'_>,
) -> CometResult<std::collections::HashMap<String, String>> {
    let mut out = std::collections::HashMap::new();
    jmap.iter(env).and_then(|mut iter| {
        while let Some(entry) = iter.next(env)? {
            let k = entry.key(env)?;
            let v = entry.value(env)?;
            let kstr: JString = env.cast_local::<JString>(k)?;
            let key = kstr.try_to_string(env)?;
            if !v.is_null() {
                let vstr: JString = env.cast_local::<JString>(v)?;
                let value = vstr.try_to_string(env)?;
                out.insert(key, value);
            }
        }
        Ok(())
    })?;
    Ok(out)
}

// Re-export the test helpers so the integration_tests module can verify
// `resolve_file_path` without exposing it in the public API surface.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_file_path_joins_with_slash() {
        assert_eq!(
            resolve_file_path("file:///tmp/t/", "part-0.parquet"),
            "file:///tmp/t/part-0.parquet"
        );
        assert_eq!(
            resolve_file_path("file:///tmp/t", "part-0.parquet"),
            "file:///tmp/t/part-0.parquet"
        );
    }

    #[test]
    fn resolve_file_path_passes_through_absolute() {
        assert_eq!(
            resolve_file_path("file:///tmp/t/", "s3://bucket/data/part-0.parquet"),
            "s3://bucket/data/part-0.parquet"
        );
    }

    /// Cred-audit regression: full matrix of storage-option keys that the
    /// Scala side (`NativeConfig.extractObjectStoreOptions` +
    /// `augmentWithResolvedAwsCredentials`) is allowed to send, mapped to
    /// the expected [`DeltaStorageConfig`] field values. When new
    /// translation branches land here, extend this matrix so a future
    /// silent drop is caught.
    #[test]
    fn extract_storage_config_matrix() {
        use std::collections::HashMap;
        // Case 1: Hadoop-style S3A keys (the Comet path produces these).
        let mut hadoop_s3 = HashMap::new();
        hadoop_s3.insert("fs.s3a.access.key".into(), "AK".into());
        hadoop_s3.insert("fs.s3a.secret.key".into(), "SK".into());
        hadoop_s3.insert("fs.s3a.session.token".into(), "TOK".into());
        hadoop_s3.insert("fs.s3a.endpoint.region".into(), "us-west-2".into());
        hadoop_s3.insert("fs.s3a.endpoint".into(), "https://s3.example".into());
        hadoop_s3.insert("fs.s3a.path.style.access".into(), "true".into());
        let cfg = delta_storage_config_from_map(&hadoop_s3);
        assert_eq!(cfg.aws_access_key.as_deref(), Some("AK"));
        assert_eq!(cfg.aws_secret_key.as_deref(), Some("SK"));
        assert_eq!(cfg.aws_session_token.as_deref(), Some("TOK"));
        assert_eq!(cfg.aws_region.as_deref(), Some("us-west-2"));
        assert_eq!(cfg.aws_endpoint.as_deref(), Some("https://s3.example"));
        assert!(cfg.aws_force_path_style);

        // Case 2: Kernel-style keys WIN when both forms are present.
        let mut both = HashMap::new();
        both.insert("aws_access_key_id".into(), "KERNEL_AK".into());
        both.insert("fs.s3a.access.key".into(), "HADOOP_AK".into());
        both.insert("aws_secret_access_key".into(), "KERNEL_SK".into());
        both.insert("fs.s3a.secret.key".into(), "HADOOP_SK".into());
        let cfg = delta_storage_config_from_map(&both);
        assert_eq!(cfg.aws_access_key.as_deref(), Some("KERNEL_AK"));
        assert_eq!(cfg.aws_secret_key.as_deref(), Some("KERNEL_SK"));

        // Case 3: `fs.s3a.region` is the deepest-fallback for region
        // (after both `aws_region` and `fs.s3a.endpoint.region`).
        let mut region_fallback = HashMap::new();
        region_fallback.insert("fs.s3a.region".into(), "eu-central-1".into());
        let cfg = delta_storage_config_from_map(&region_fallback);
        assert_eq!(cfg.aws_region.as_deref(), Some("eu-central-1"));

        // Case 4: Azure kernel-style keys.
        let mut azure = HashMap::new();
        azure.insert("azure_account_name".into(), "myacct".into());
        azure.insert("azure_access_key".into(), "AZKEY".into());
        azure.insert("azure_bearer_token".into(), "BEARER".into());
        let cfg = delta_storage_config_from_map(&azure);
        assert_eq!(cfg.azure_account_name.as_deref(), Some("myacct"));
        assert_eq!(cfg.azure_access_key.as_deref(), Some("AZKEY"));
        assert_eq!(cfg.azure_bearer_token.as_deref(), Some("BEARER"));

        // Case 5: empty map -> all defaults (no creds, force_path_style=false).
        let cfg = delta_storage_config_from_map(&HashMap::new());
        assert!(cfg.aws_access_key.is_none());
        assert!(cfg.aws_secret_key.is_none());
        assert!(cfg.aws_session_token.is_none());
        assert!(cfg.aws_region.is_none());
        assert!(cfg.aws_endpoint.is_none());
        assert!(!cfg.aws_force_path_style);
        assert!(cfg.azure_account_name.is_none());
    }

    /// Cred-audit gap markers. These document the credential shapes that
    /// the current implementation DOES NOT bridge through to native. Each
    /// asserts the missing-credential state explicitly so when a fix lands
    /// the failing assertion forces the gap entry to be removed (and a
    /// positive case added to `extract_storage_config_matrix`).
    #[test]
    fn extract_storage_config_known_gaps() {
        use std::collections::HashMap;
        // Gap 1: GCS service-account / OAuth keys aren't bridged. Hadoop
        // surfaces these under `fs.gs.*`; iceberg-rust uses `gcs.*`.
        // DeltaStorageConfig has no `gcp_*` fields at all -- reads against
        // `gs://` would fail at `create_object_store` with
        // UnsupportedScheme. Tracked in #183 follow-up.
        let mut gcs = HashMap::new();
        gcs.insert(
            "fs.gs.auth.service.account.json.keyfile".into(),
            "/tmp/key.json".into(),
        );
        gcs.insert("fs.gs.project.id".into(), "my-project".into());
        let cfg = delta_storage_config_from_map(&gcs);
        // Everything stays None -- the GCS keys are silently dropped.
        assert!(cfg.aws_access_key.is_none());
        assert!(cfg.azure_account_name.is_none());

        // Gap 2: per-bucket S3 keys (`fs.s3a.bucket.<name>.access.key`)
        // aren't bridged. Multi-bucket setups silently fall back to global
        // creds (or fail when none are set).
        let mut per_bucket = HashMap::new();
        per_bucket.insert(
            "fs.s3a.bucket.my-bucket.access.key".into(),
            "PERBKT_AK".into(),
        );
        let cfg = delta_storage_config_from_map(&per_bucket);
        assert!(
            cfg.aws_access_key.is_none(),
            "per-bucket S3 key was unexpectedly bridged; if intentional, \
             move this case into extract_storage_config_matrix"
        );

        // Gap 3: Hadoop's Azure connector key formats (storage-account key,
        // OAuth, MSI, SAS) aren't bridged -- only the three kernel-style
        // azure keys flow through.
        let mut hadoop_azure = HashMap::new();
        hadoop_azure.insert(
            "fs.azure.account.key.myacct.blob.core.windows.net".into(),
            "HADOOP_AZKEY".into(),
        );
        hadoop_azure.insert(
            "fs.azure.account.oauth2.client.id".into(),
            "CLIENT_ID".into(),
        );
        hadoop_azure.insert(
            "fs.azure.account.oauth2.client.secret".into(),
            "CLIENT_SECRET".into(),
        );
        let cfg = delta_storage_config_from_map(&hadoop_azure);
        assert!(cfg.azure_account_name.is_none());
        assert!(cfg.azure_access_key.is_none());
        assert!(cfg.azure_bearer_token.is_none());
    }

    #[test]
    fn resolve_file_path_passes_through_single_slash_file_uri() {
        // SHALLOW CLONE stores paths as Hadoop `Path.toUri.toString` which uses
        // single-slash form `file:/abs/...`. Must not be concat'd onto the clone root.
        assert_eq!(
            resolve_file_path(
                "file:/tmp/clonetable/",
                "file:/tmp/parquet_table/part-0.parquet"
            ),
            "file:/tmp/parquet_table/part-0.parquet"
        );
    }

    #[test]
    fn has_uri_scheme_matches_schemes() {
        assert!(has_uri_scheme("file:/abs"));
        assert!(has_uri_scheme("file:///abs"));
        assert!(has_uri_scheme("s3://bucket/k"));
        assert!(has_uri_scheme("hdfs://nn/path"));
        assert!(!has_uri_scheme("part-0.parquet"));
        assert!(!has_uri_scheme("/abs/path"));
        assert!(!has_uri_scheme("1bad:/scheme")); // must start with letter
        assert!(!has_uri_scheme(""));
    }
}
