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
    // The query's data-read schema as Delta schema JSON (`StructType.json`, carrying
    // `delta.columnMapping.physicalName` + `id` from the analysis-time or snapshot schema). Drives
    // `scan.with_schema(...)` so kernel resolves the physical names the query was PLANNED with
    // (correct under schema-change-since-analysis). Empty => full-table scan, no kernel schemas.
    projected_schema_json: JString,
) -> jbyteArray {
    try_unwrap_or_throw(&e, |env| {
        let url_str: String = table_url.try_to_string(env)?;
        let version = if snapshot_version < 0 {
            None
        } else {
            Some(snapshot_version as u64)
        };
        // S3 bucket (URL host) for per-bucket credential resolution; None for non-S3.
        let s3_bucket = url::Url::parse(&url_str)
            .ok()
            .filter(|u| matches!(u.scheme(), "s3" | "s3a"))
            .and_then(|u| u.host_str().map(|h| h.to_string()));
        let config = if storage_options.is_null() {
            DeltaStorageConfig::default()
        } else {
            let jmap: JMap<'_> = env.cast_local::<JMap>(storage_options)?;
            // S3 static keys (global + per-bucket) are bridged here; Azure / GCS go
            // through object_store::parse_url + ambient creds in `create_object_store`.
            // Residual (08-known-limitations.md A2e): Hadoop's explicit S3
            // credential-provider classes (`fs.s3a.aws.credentials.provider` =
            // AssumedRole / WebIdentity / ...) are not honored -- object_store's own
            // default chain (static keys here, else IMDS / ECS / env) is used instead.
            extract_storage_config(env, &jmap, s3_bucket.as_deref())?
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
                Ok(expr) => Some(crate::predicate::catalyst_to_kernel_predicate_with_names(
                    &expr, &col_names,
                )),
                Err(e) => {
                    log::warn!(
                        "Failed to decode predicate for Delta file pruning: {e}; \
                         scanning all files"
                    );
                    None
                }
            }
        });

        // The data-read schema (Delta JSON) for kernel's `with_schema`. Absent => full-table scan.
        let projected_schema_json = decode_jstring(env, &projected_schema_json)?;

        let plan = plan_delta_scan_with_predicate(
            &url_str,
            &config,
            version,
            kernel_predicate,
            projected_schema_json,
        )
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
                        let logical_name = physical_to_logical.get(&name).cloned().unwrap_or(name);
                        DeltaPartitionValue {
                            name: logical_name,
                            value: Some(value),
                        }
                    })
                    .collect(),
                // Deletion-vector descriptor (path/offset/size) -- the executor's
                // `DeltaSyntheticColumnsExec` calls `kernel::DeletionVectorDescriptor::read`
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
                // Surface the kernel-provided modification time (epoch millis) so
                // `_metadata.file_modification_time` matches Spark on the kernel-driver
                // path too. The value is already populated on `DeltaFileEntry` from the
                // scan-file metadata; the BatchFileIndex path sets it from
                // AddFile.modificationTime equivalently.
                modification_time: Some(entry.modification_time),
                // Kernel's fully-resolved physical->logical transform for this file (serde JSON),
                // extracted in `plan_delta_scan` from `scan_file.transform`. The executor applies it
                // via `transform_to_logical` (partition injection / column-mapping relabel /
                // row-tracking baked in). Empty = identity (plain pass-through).
                transform_json: entry.transform_json,
            })
            .collect();

        // `plan.column_mappings` is consumed above (partition-value key translation) and is NOT put
        // on the wire: column mapping is fully resolved by the kernel schemas shipped on
        // `DeltaScanCommon`, so the executor needs no `column_mappings`.

        let msg = DeltaScanTaskList {
            snapshot_version: plan.version,
            table_root: url_str,
            tasks,
            unsupported_features: plan.unsupported_features,
            // Kernel-built projected schemas (Arrow IPC) -- Scala copies these onto
            // `DeltaScanCommon.kernel_physical_schema` / `kernel_logical_schema`. Empty when no
            // projection was supplied.
            physical_schema: plan.physical_schema_ipc,
            logical_schema: plan.logical_schema_ipc,
        };

        let bytes = msg.encode_to_vec();
        let result = env.byte_array_from_slice(&bytes)?;
        Ok(result.into_raw())
    })
}

/// Schema-only companion to `planDeltaScan` for the batch-file-index read path (the file list comes
/// from Delta `AddFile`s on the Scala side; only kernel's resolved physical/logical schemas are
/// needed). Builds the snapshot at `snapshot_version` + a projected `Scan` and returns a
/// `DeltaScanTaskList` with ONLY `physical_schema` / `logical_schema` set (Arrow IPC, field-ids at
/// every nesting level). Returns an empty (no schemas) message when `projected_schema_ipc` is
/// absent/empty -- the executor then falls back to physicalising `required_schema`.
///
/// # Safety
/// Inherently unsafe because it dereferences raw JNI pointers.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_contrib_delta_Native_planDeltaReadSchemas(
    e: EnvUnowned,
    _class: JClass,
    table_url: JString,
    snapshot_version: jlong,
    storage_options: JObject,
    projected_schema_json: JString,
) -> jbyteArray {
    try_unwrap_or_throw(&e, |env| {
        let url_str: String = table_url.try_to_string(env)?;
        let version = if snapshot_version < 0 {
            None
        } else {
            Some(snapshot_version as u64)
        };
        let s3_bucket = url::Url::parse(&url_str)
            .ok()
            .filter(|u| matches!(u.scheme(), "s3" | "s3a"))
            .and_then(|u| u.host_str().map(|h| h.to_string()));
        let config = if storage_options.is_null() {
            DeltaStorageConfig::default()
        } else {
            let jmap: JMap<'_> = env.cast_local::<JMap>(storage_options)?;
            extract_storage_config(env, &jmap, s3_bucket.as_deref())?
        };

        // The data-read schema (Delta JSON) for kernel's `with_schema`. Empty => zero data columns
        // => no kernel schemas needed.
        let (physical_schema, logical_schema) = match decode_jstring(env, &projected_schema_json)? {
            Some(json) => crate::scan::plan_delta_read_schemas(&url_str, &config, version, json)
                .map_err(|e| {
                    CometError::Internal(format!("delta kernel read-schema build failed: {e}"))
                })?,
            None => (Vec::new(), Vec::new()),
        };

        let msg = DeltaScanTaskList {
            snapshot_version: version.unwrap_or(0),
            table_root: url_str,
            tasks: Vec::new(),
            unsupported_features: Vec::new(),
            physical_schema,
            logical_schema,
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

/// Walk a `java.util.Map<String, String>` of storage options into the S3
/// [`DeltaStorageConfig`]. `bucket` is the table's S3 bucket (URL host), used to
/// prefer per-bucket Hadoop keys over global ones; pass `None` for non-S3 tables.
///
/// Reads the JMap into a Rust `HashMap` and delegates to
/// [`delta_storage_config_from_map`] for the actual key mapping. Splitting
/// the JNI traversal from the mapping logic lets the mapping be unit-tested
/// without a JVM.
fn extract_storage_config(
    env: &mut Env,
    jmap: &JMap<'_>,
    bucket: Option<&str>,
) -> CometResult<DeltaStorageConfig> {
    let m = jmap_to_hashmap(env, jmap)?;
    Ok(delta_storage_config_from_map(&m, bucket))
}

/// Pure (JVM-free) mapping from a generic options map to the S3 [`DeltaStorageConfig`].
///
/// Only S3 credentials are bridged here -- accepting kernel-style keys
/// (`aws_access_key_id`, ...) AND the Hadoop-style keys Comet's
/// `NativeConfig.extractObjectStoreOptions` produces (`fs.s3a.access.key`, ...).
/// The kernel-style key wins when both are present. Per-bucket Hadoop keys
/// (`fs.s3a.bucket.<bucket>.<suffix>`) override the global `fs.s3a.<suffix>` when
/// `bucket` (the table's S3 bucket) is supplied -- matching Hadoop S3A / core Comet.
///
/// Azure and GCS are intentionally NOT bridged here: `create_object_store` builds
/// them via `object_store::parse_url`, which sources credentials from the ambient
/// environment (`AZURE_*` / `GOOGLE_*` / ADC / instance metadata), exactly as core
/// Comet's non-S3 read path does. So `fs.azure.*` / `fs.gs.*` keys are not consulted.
///
/// Residual (tracked in `08-known-limitations.md` A2e): Hadoop's explicit S3
/// credential-provider classes (`fs.s3a.aws.credentials.provider` =
/// AssumedRole / WebIdentity / ...) are not honored; object_store's own default
/// chain (static keys here, else IMDS / ECS / env) is used instead.
pub fn delta_storage_config_from_map(
    m: &std::collections::HashMap<String, String>,
    bucket: Option<&str>,
) -> DeltaStorageConfig {
    // A per-bucket Hadoop key (`fs.s3a.bucket.<bucket>.<suffix>`) overrides the
    // global `fs.s3a.<suffix>`.
    fn s3_hadoop(
        m: &std::collections::HashMap<String, String>,
        bucket: Option<&str>,
        suffix: &str,
    ) -> Option<String> {
        bucket
            .and_then(|b| m.get(&format!("fs.s3a.bucket.{b}.{suffix}")).cloned())
            .or_else(|| m.get(&format!("fs.s3a.{suffix}")).cloned())
    }
    // Kernel-style (object_store) key wins over the Hadoop form.
    let kernel_or_s3 = |kernel: &str, suffix: &str| {
        m.get(kernel)
            .cloned()
            .or_else(|| s3_hadoop(m, bucket, suffix))
    };
    DeltaStorageConfig {
        aws_access_key: kernel_or_s3("aws_access_key_id", "access.key"),
        aws_secret_key: kernel_or_s3("aws_secret_access_key", "secret.key"),
        aws_session_token: kernel_or_s3("aws_session_token", "session.token"),
        aws_region: m
            .get("aws_region")
            .cloned()
            .or_else(|| s3_hadoop(m, bucket, "endpoint.region"))
            .or_else(|| s3_hadoop(m, bucket, "region")),
        aws_endpoint: kernel_or_s3("aws_endpoint", "endpoint"),
        aws_force_path_style: m
            .get("aws_force_path_style")
            .cloned()
            .or_else(|| s3_hadoop(m, bucket, "path.style.access"))
            .map(|s| s == "true")
            .unwrap_or(false),
    }
}

/// Decode a Java `String` into `Option<String>`: `None` for null/empty.
fn decode_jstring(env: &mut Env, s: &JString) -> CometResult<Option<String>> {
    if s.is_null() {
        return Ok(None);
    }
    let v = s.try_to_string(env)?;
    Ok(if v.is_empty() { None } else { Some(v) })
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
        // Checked downcast (matches `jmap_to_hashmap`): the array is declared
        // `String[]`, but use the safe cast rather than `JString::from_raw` so an
        // unexpected element type surfaces as a clean error instead of UB.
        let jstr: JString = env.cast_local::<JString>(obj)?;
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
        let cfg = delta_storage_config_from_map(&hadoop_s3, None);
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
        let cfg = delta_storage_config_from_map(&both, None);
        assert_eq!(cfg.aws_access_key.as_deref(), Some("KERNEL_AK"));
        assert_eq!(cfg.aws_secret_key.as_deref(), Some("KERNEL_SK"));

        // Case 3: `fs.s3a.region` is the deepest-fallback for region
        // (after both `aws_region` and `fs.s3a.endpoint.region`).
        let mut region_fallback = HashMap::new();
        region_fallback.insert("fs.s3a.region".into(), "eu-central-1".into());
        let cfg = delta_storage_config_from_map(&region_fallback, None);
        assert_eq!(cfg.aws_region.as_deref(), Some("eu-central-1"));

        // Case 4: per-bucket S3 key overrides the global one for the table's bucket
        // (A2b). A different bucket -- or no bucket -- falls back to global.
        let mut per_bucket = HashMap::new();
        per_bucket.insert("fs.s3a.access.key".into(), "GLOBAL".into());
        per_bucket.insert("fs.s3a.bucket.my-bucket.access.key".into(), "PERBKT".into());
        assert_eq!(
            delta_storage_config_from_map(&per_bucket, Some("my-bucket"))
                .aws_access_key
                .as_deref(),
            Some("PERBKT")
        );
        assert_eq!(
            delta_storage_config_from_map(&per_bucket, Some("other"))
                .aws_access_key
                .as_deref(),
            Some("GLOBAL")
        );
        assert_eq!(
            delta_storage_config_from_map(&per_bucket, None)
                .aws_access_key
                .as_deref(),
            Some("GLOBAL")
        );

        // Case 5: empty map -> all defaults (no creds, force_path_style=false).
        let cfg = delta_storage_config_from_map(&HashMap::new(), None);
        assert!(cfg.aws_access_key.is_none());
        assert!(cfg.aws_secret_key.is_none());
        assert!(cfg.aws_session_token.is_none());
        assert!(cfg.aws_region.is_none());
        assert!(cfg.aws_endpoint.is_none());
        assert!(!cfg.aws_force_path_style);
    }

    /// Azure / GCS credentials are deliberately NOT carried in `DeltaStorageConfig`:
    /// `create_object_store` builds those via `object_store::parse_url` + ambient
    /// credentials (parity with core's non-S3 path). This guards against accidentally
    /// re-introducing config bridging that leaks `fs.azure.*` / `fs.gs.*` into the S3
    /// fields. (A2e residual -- Hadoop S3 credential-provider classes -- is documented
    /// in `08-known-limitations.md`, not asserted here.)
    #[test]
    fn azure_and_gcs_keys_do_not_leak_into_s3_config() {
        use std::collections::HashMap;
        let mut m = HashMap::new();
        m.insert(
            "fs.azure.account.key.myacct.dfs.core.windows.net".into(),
            "HADOOP_AZKEY".into(),
        );
        m.insert("fs.azure.account.oauth2.client.id".into(), "CLIENT".into());
        m.insert(
            "fs.gs.auth.service.account.json.keyfile".into(),
            "/tmp/key.json".into(),
        );
        let cfg = delta_storage_config_from_map(&m, None);
        assert!(cfg.aws_access_key.is_none());
        assert!(cfg.aws_secret_key.is_none());
        assert!(cfg.aws_endpoint.is_none());
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
