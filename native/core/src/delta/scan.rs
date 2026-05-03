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

//! Delta log replay: given a table URL, return the list of active parquet
//! files with partition values, record-count stats, and deletion-vector
//! flags.
//!
//! Ported from tantivy4java's `delta_reader/scan.rs`. The API is the
//! smallest possible surface that still proves end-to-end kernel
//! integration: `Snapshot::builder_for(url)` → `scan_builder().build()` →
//! `scan_metadata(&engine)` → `visit_scan_files(...)`.
//!
//! **Critical gotcha** preserved from the reference implementation: kernel
//! internally does `table_root.join("_delta_log/")`, and `Url::join` will
//! *replace* the last path segment if the base URL does not end in `/`. So
//! `normalize_url` always appends a trailing slash.

use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use delta_kernel::snapshot::Snapshot;

use super::engine::{create_engine, DeltaStorageConfig};
use super::error::{DeltaError, DeltaResult};

/// Metadata for a single active parquet file in a Delta table.
///
/// Plain Rust types only — no arrow / parquet / object_store types. This is
/// the boundary at which kernel's isolated dep subtree meets the rest of
/// Comet.
#[derive(Debug, Clone)]
pub struct DeltaFileEntry {
    /// Parquet file path, relative to the table root.
    pub path: String,
    /// File size in bytes.
    pub size: i64,
    /// Last-modified time as epoch millis.
    pub modification_time: i64,
    /// Record count from log stats, if known.
    pub num_records: Option<u64>,
    /// Partition column → value mapping from the add action.
    pub partition_values: HashMap<String, String>,
    /// Deleted row indexes materialized from the file's deletion vector by
    /// kernel on the driver. Empty vector means the file has no DV in use.
    /// Sorted ascending; indexes are 0-based into the file's physical parquet
    /// row space, matching `DvInfo::get_row_indexes` semantics.
    pub deleted_row_indexes: Vec<u64>,
}

impl DeltaFileEntry {
    /// True if this entry has a deletion vector in use.
    pub fn has_deletion_vector(&self) -> bool {
        !self.deleted_row_indexes.is_empty()
    }
}

/// Result of planning a Delta scan: the active file list plus the pinned
/// snapshot version plus a list of reader features that Comet's native path
/// doesn't yet handle. The Scala side uses the feature list to decide
/// whether to fall back to Spark's vanilla Delta reader.
#[derive(Debug, Clone)]
pub struct DeltaScanPlan {
    pub entries: Vec<DeltaFileEntry>,
    pub version: u64,
    pub unsupported_features: Vec<String>,
    /// Logical→physical column name mapping for column-mapped tables.
    /// Empty when column_mapping_mode is None.
    pub column_mappings: Vec<(String, String)>,
}

/// List every active parquet file in a Delta table at the given version.
///
/// Returns `(entries, actual_version)` where `actual_version` is the
/// snapshot version that was actually read — equal to `version` when
/// specified, or the latest version otherwise.
///
/// Thin wrapper around [`plan_delta_scan`] that drops the feature list.
/// New code should call `plan_delta_scan` directly so it can honor the
/// unsupported-feature gate.
pub fn list_delta_files(
    url_str: &str,
    config: &DeltaStorageConfig,
    version: Option<u64>,
) -> DeltaResult<(Vec<DeltaFileEntry>, u64)> {
    let plan = plan_delta_scan(url_str, config, version)?;
    Ok((plan.entries, plan.version))
}

/// Plan a Delta scan against the given URL + optional snapshot version.
///
/// This is the full-fat variant of [`list_delta_files`]: it also reports
/// which reader features are *in use* for this snapshot and NOT yet
/// supported by Comet's native path.
///
/// Feature detection blends two signals:
///   1. [`delta_kernel::snapshot::Snapshot::table_properties`] — the
///      protocol-level flags (`column_mapping_mode`, `enable_type_widening`,
///      `enable_row_tracking`).
///   2. The per-file `ScanFile::dv_info.has_vector()` flag — set to true
///      only when the specific file actually has a deletion vector attached.
///      This is tighter than the `enable_deletion_vectors` table property
///      because a DV-enabled table with no deletes yet is still safe for
///      Comet to read natively.
pub fn plan_delta_scan(
    url_str: &str,
    config: &DeltaStorageConfig,
    version: Option<u64>,
) -> DeltaResult<DeltaScanPlan> {
    plan_delta_scan_with_predicate(url_str, config, version, None)
}

pub fn plan_delta_scan_with_predicate(
    url_str: &str,
    config: &DeltaStorageConfig,
    version: Option<u64>,
    kernel_predicate: Option<delta_kernel::expressions::Predicate>,
) -> DeltaResult<DeltaScanPlan> {
    let url = normalize_url(url_str)?;
    let engine = create_engine(&url, config)?;

    let snapshot = {
        let mut builder = Snapshot::builder_for(url);
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        builder.build(&engine)?
    };
    let actual_version = snapshot.version();

    // Protocol-level feature gate. Collect the names of features we don't
    // yet handle so the Scala side can decide to fall back. Note that we
    // explicitly do NOT treat the following as fallback-worthy:
    //   - `change_data_feed`: only affects CDF queries, not regular reads
    //   - `in_commit_timestamps`: regular reads work fine
    //   - `iceberg_compat_v1/v2`: doesn't change Delta read correctness
    //   - `append_only`: write-side constraint, reads are unaffected
    let mut unsupported_features: Vec<String> = Vec::new();
    let props = snapshot.table_properties();
    // columnMapping is now handled by Phase 4 — no longer a fallback trigger.
    // typeWidening: DataFusion's parquet schema adapter handles widening reads
    // (parquet stores the file's original type; the adapter casts to the table's
    // current widened type at read time). Removed from the gate; verified by
    // TypeWidening{TableFeature,Metadata,...}Suite in the Delta regression.
    if props.enable_row_tracking == Some(true) {
        unsupported_features.push("rowTracking".to_string());
    }

    // Phase 4: extract logical→physical column name mapping from schema metadata.
    // For column_mapping_mode = id or name, each StructField carries a
    // `delta.columnMapping.physicalName` metadata entry that tells us what the
    // parquet file's column name actually is.
    let column_mappings: Vec<(String, String)> = if props.column_mapping_mode.is_some() {
        snapshot
            .schema()
            .fields()
            .filter_map(|field| {
                use delta_kernel::schema::{ColumnMetadataKey, MetadataValue};
                field
                    .metadata
                    .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                    .and_then(|v| match v {
                        MetadataValue::String(phys) => Some((field.name().clone(), phys.clone())),
                        _ => None,
                    })
            })
            .collect()
    } else {
        Vec::new()
    };

    // `Snapshot::build()` returns `Arc<Snapshot>`, and `scan_builder` consumes
    // it. Clone the Arc so we can still reach `table_root()` after building
    // the scan — we need the URL to materialize DVs below.
    let snapshot_arc: Arc<_> = snapshot;
    let table_root_url = snapshot_arc.table_root().clone();
    let mut scan_builder = Arc::clone(&snapshot_arc).scan_builder();
    if let Some(pred) = kernel_predicate {
        scan_builder = scan_builder.with_predicate(Arc::new(pred));
    }
    let scan = scan_builder.build()?;

    // Temporary collection that keeps the raw kernel `DvInfo` alongside the
    // rest of the metadata. We need the `DvInfo` to materialize the deleted
    // row indexes below; it doesn't escape this function.
    struct RawEntry {
        path: String,
        size: i64,
        modification_time: i64,
        num_records: Option<u64>,
        partition_values: HashMap<String, String>,
        dv_info: delta_kernel::scan::state::DvInfo,
    }

    let mut raw: Vec<RawEntry> = Vec::new();
    let scan_metadata = scan.scan_metadata(&engine)?;

    for meta_result in scan_metadata {
        let meta: delta_kernel::scan::ScanMetadata = meta_result?;
        raw = meta.visit_scan_files(
            raw,
            |acc: &mut Vec<RawEntry>, scan_file: delta_kernel::scan::state::ScanFile| {
                let num_records = scan_file.stats.as_ref().map(|s| s.num_records);
                acc.push(RawEntry {
                    path: scan_file.path,
                    size: scan_file.size,
                    modification_time: scan_file.modification_time,
                    num_records,
                    partition_values: scan_file.partition_values,
                    dv_info: scan_file.dv_info,
                });
            },
        )?;
    }

    // For each file that has a DV attached, ask kernel to materialize the
    // deleted row indexes. Kernel handles inline bitmaps, on-disk DV files,
    // and the various storage-type variants transparently. This runs on the
    // driver (same process that's building the scan plan), so we only pay
    // the DV-fetch latency once per query.
    //
    // Note: for very large tables (millions of files), this collects all
    // entries into memory before returning. Consider streaming/chunked
    // processing if driver OOM becomes an issue at extreme scale.
    let mut entries: Vec<DeltaFileEntry> = Vec::with_capacity(raw.len());
    for r in raw {
        let deleted_row_indexes = if r.dv_info.has_vector() {
            r.dv_info
                .get_row_indexes(&engine, &table_root_url)?
                .ok_or_else(|| {
                    DeltaError::Internal(format!(
                        "DV has_vector() true but get_row_indexes() returned None for {}",
                        r.path
                    ))
                })?
        } else {
            Vec::new()
        };
        entries.push(DeltaFileEntry {
            path: r.path,
            size: r.size,
            modification_time: r.modification_time,
            num_records: r.num_records,
            partition_values: r.partition_values,
            deleted_row_indexes,
        });
    }

    Ok(DeltaScanPlan {
        entries,
        version: actual_version,
        unsupported_features,
        column_mappings,
    })
}

/// Normalize a table URL so kernel's `table_root.join("_delta_log/")`
/// appends rather than replaces. Bare paths become `file://` URLs.
///
/// Accepts three shapes:
///   1. `s3://`, `s3a://`, `az://`, `azure://`, `abfs://`, `abfss://`,
///      `file://` — already-formed URLs, parsed directly.
///   2. `file:/Users/...` — Hadoop's `Path.toUri.toString` output, which
///      uses a *single* slash and is NOT a valid `Url::parse` input. We
///      rewrite this to `file://` before parsing.
///   3. Bare local paths — canonicalized and turned into `file://` via
///      `Url::from_directory_path`.
pub(crate) fn normalize_url(url_str: &str) -> DeltaResult<Url> {
    // Hadoop's java.net.URI.toString emits `file:/path/to/t` (one slash)
    // for local files. Rewrite into the `file:///path` form that
    // `Url::parse` understands.
    if url_str.starts_with("file:/") && !url_str.starts_with("file://") {
        let rewritten = format!("file://{}", &url_str["file:".len()..]);
        let mut url = Url::parse(&rewritten).map_err(|e| DeltaError::InvalidUrl {
            url: url_str.to_string(),
            source: e,
        })?;
        ensure_trailing_slash(&mut url);
        return Ok(url);
    }

    if url_str.starts_with("s3://")
        || url_str.starts_with("s3a://")
        || url_str.starts_with("az://")
        || url_str.starts_with("azure://")
        || url_str.starts_with("abfs://")
        || url_str.starts_with("abfss://")
        || url_str.starts_with("file://")
    {
        let mut url = Url::parse(url_str).map_err(|e| DeltaError::InvalidUrl {
            url: url_str.to_string(),
            source: e,
        })?;
        ensure_trailing_slash(&mut url);
        Ok(url)
    } else {
        let abs_path = std::path::Path::new(url_str).canonicalize().map_err(|e| {
            DeltaError::PathResolution {
                path: url_str.to_string(),
                source: e,
            }
        })?;
        Url::from_directory_path(&abs_path).map_err(|_| DeltaError::PathToUrl {
            path: abs_path.display().to_string(),
        })
    }
}

fn ensure_trailing_slash(url: &mut Url) {
    let path = url.path().to_string();
    if !path.ends_with('/') {
        url.set_path(&format!("{path}/"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url_trailing_slash() {
        let url = normalize_url("file:///tmp/my_table").unwrap();
        assert!(url.path().ends_with('/'), "URL should end with /: {url}");
        assert_eq!(url.as_str(), "file:///tmp/my_table/");

        let url = normalize_url("file:///tmp/my_table/").unwrap();
        assert_eq!(url.as_str(), "file:///tmp/my_table/");

        let url = normalize_url("s3://bucket/path/to/table").unwrap();
        assert!(url.path().ends_with('/'), "URL should end with /: {url}");
    }

    #[test]
    fn test_normalize_url_hadoop_single_slash_form() {
        // Hadoop's Path.toUri.toString produces `file:/path` (single slash),
        // not `file:///path`. Must be normalized to a Url::parse-able form.
        let url = normalize_url("file:/Users/alice/tmp/t").unwrap();
        assert_eq!(url.as_str(), "file:///Users/alice/tmp/t/");

        let url = normalize_url("file:/tmp/t/").unwrap();
        assert_eq!(url.as_str(), "file:///tmp/t/");
    }

    #[test]
    fn test_normalize_url_join_behavior() {
        // The critical invariant: joining `_delta_log/` onto a normalized
        // URL must *append*, not replace the last segment.
        let url = normalize_url("file:///tmp/my_table").unwrap();
        let log_url = url.join("_delta_log/").unwrap();
        assert_eq!(log_url.as_str(), "file:///tmp/my_table/_delta_log/");
    }

    #[test]
    fn test_list_delta_files_local() {
        // Hand-build a minimal Delta table in a tempdir: one protocol action,
        // one metadata action, one add action. No Parquet data needed —
        // we're exercising the log-replay path only.
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_delta");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        let commit0 = [
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}"#,
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":5000,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":50}"}}"#,
        ]
        .join("\n");
        std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();
        std::fs::write(table_dir.join("part-00000.parquet"), [0u8]).unwrap();

        let config = DeltaStorageConfig::default();
        let (entries, version) =
            list_delta_files(table_dir.to_str().unwrap(), &config, None).unwrap();

        assert_eq!(version, 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "part-00000.parquet");
        assert_eq!(entries[0].size, 5000);
        assert_eq!(entries[0].num_records, Some(50));
        assert!(!entries[0].has_deletion_vector());
    }
}
