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
//! Exposes `Java_org_apache_comet_Native_planDeltaScan`. The Scala driver
//! calls this once per query to ask kernel for the active file list at a
//! given snapshot version, then distributes the returned tasks across
//! Spark executors via Comet's usual split-mode serialization.

use jni::{
    objects::{JByteArray, JClass, JMap, JObject, JString},
    sys::{jbyteArray, jlong},
    Env, EnvUnowned,
};
use prost::Message;

use crate::delta::{scan::plan_delta_scan_with_predicate, DeltaStorageConfig};
use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use datafusion_comet_proto::spark_operator::{
    DeltaPartitionValue, DeltaScanTask, DeltaScanTaskList,
};

/// `Java_org_apache_comet_Native_planDeltaScan`.
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
pub unsafe extern "system" fn Java_org_apache_comet_Native_planDeltaScan(
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
                    crate::delta::predicate::catalyst_to_kernel_predicate_with_names(
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
                // Phase 3: the DV is already materialized into a sorted
                // `Vec<u64>` of deleted row indexes by `plan_delta_scan`
                // (which calls `DvInfo::get_row_indexes` on the driver).
                deleted_row_indexes: entry.deleted_row_indexes,
                // Row tracking: kernel 0.19.x doesn't yet surface baseRowId /
                // defaultRowCommitVersion on the ScanFile path (it's read during
                // log replay but consumed internally for TransformSpec). Leave
                // unset on the kernel plan path; the pre-materialised-index
                // path on the Scala side fills these in from AddFile when
                // rowTracking is enabled.
                base_row_id: None,
                default_row_commit_version: None,
                // Splitting is done on the Scala side just before serialization,
                // not here on the kernel-driver path. Leave unset.
                byte_range_start: None,
                byte_range_end: None,
            })
            .collect();

        let column_mappings: Vec<datafusion_comet_proto::spark_operator::DeltaColumnMapping> = plan
            .column_mappings
            .into_iter()
            .map(
                |(logical, physical)| datafusion_comet_proto::spark_operator::DeltaColumnMapping {
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
fn extract_storage_config(env: &mut Env, jmap: &JMap<'_>) -> CometResult<DeltaStorageConfig> {
    // Helper: try kernel key first, fall back to Hadoop key.
    let get = |env: &mut Env, k1: &str, k2: &str| -> CometResult<Option<String>> {
        let v = map_get_string(env, jmap, k1)?;
        if v.is_some() {
            return Ok(v);
        }
        map_get_string(env, jmap, k2)
    };

    Ok(DeltaStorageConfig {
        aws_access_key: get(env, "aws_access_key_id", "fs.s3a.access.key")?,
        aws_secret_key: get(env, "aws_secret_access_key", "fs.s3a.secret.key")?,
        aws_session_token: get(env, "aws_session_token", "fs.s3a.session.token")?,
        aws_region: get(env, "aws_region", "fs.s3a.endpoint.region")?.or(map_get_string(
            env,
            jmap,
            "fs.s3a.region",
        )?),
        aws_endpoint: get(env, "aws_endpoint", "fs.s3a.endpoint")?,
        aws_force_path_style: get(env, "aws_force_path_style", "fs.s3a.path.style.access")?
            .map(|s| s == "true")
            .unwrap_or(false),
        azure_account_name: map_get_string(env, jmap, "azure_account_name")?,
        azure_access_key: map_get_string(env, jmap, "azure_access_key")?,
        azure_bearer_token: map_get_string(env, jmap, "azure_bearer_token")?,
    })
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

/// `map.get(key)` for a `java.util.Map<String, String>` surfaced as a
/// `JMap`. Returns `None` if the key is absent or the value is `null`.
fn map_get_string(env: &mut Env, jmap: &JMap<'_>, key: &str) -> CometResult<Option<String>> {
    let key_obj = env.new_string(key)?;
    let key_jobj: JObject = key_obj.into();
    match jmap.get(env, &key_jobj)? {
        None => Ok(None),
        Some(value) => {
            // SAFETY: Map<String, String>::get always returns a String. The
            // JObject reference is valid because JMap::get returned it from the
            // current env frame. We consume the local ref via into_raw().
            let jstr = unsafe { JString::from_raw(env, value.into_raw()) };
            Ok(Some(jstr.try_to_string(env)?))
        }
    }
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
