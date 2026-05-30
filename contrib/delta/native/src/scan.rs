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

use super::engine::{get_or_create_engine, DeltaStorageConfig};
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
    /// Deletion-vector descriptor for this file, when one is in use. `None`
    /// when the file has no DV. Carries everything the EXECUTOR needs to read
    /// the DV bitmap on-task -- the driver no longer materialises the deleted
    /// row indexes (which could reach ~1 GB long[] for a 99 M-row DV on the
    /// 2 B-row "huge table delete" test). See task #218 / the Iceberg-style
    /// refactor: the driver ships KB-scale descriptors, the executor calls
    /// `kernel::DeletionVectorDescriptor::read` once per partition.
    pub dv_descriptor: Option<crate::proto::DeltaDvDescriptor>,
    /// `AddFile.baseRowId` for row-tracking-enabled tables. `None` when the
    /// table doesn't have row tracking. `row_id` for any row in this file is
    /// `base_row_id + physical_row_index`.
    pub base_row_id: Option<i64>,
    /// `AddFile.defaultRowCommitVersion` for row-tracking-enabled tables.
    /// `None` when the table doesn't have row tracking. Constant per file.
    pub default_row_commit_version: Option<i64>,
}

impl DeltaFileEntry {
    /// True if this entry has a deletion vector in use.
    pub fn has_deletion_vector(&self) -> bool {
        self.dv_descriptor.is_some()
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
    let engine = get_or_create_engine(&url, config)?;

    let snapshot = {
        let mut builder = Snapshot::builder_for(url);
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        builder.build(&*engine)?
    };
    let actual_version = snapshot.version();

    // Protocol-level feature gate. Collect the names of features we don't
    // yet handle so the Scala side can decide to fall back. Note that we
    // explicitly do NOT treat the following as fallback-worthy:
    //   - `change_data_feed`: only affects CDF queries, not regular reads
    //   - `in_commit_timestamps`: regular reads work fine
    //   - `iceberg_compat_v1/v2`: doesn't change Delta read correctness
    //   - `append_only`: write-side constraint, reads are unaffected
    let unsupported_features: Vec<String> = Vec::new();
    let props = snapshot.table_properties();
    // columnMapping is now handled by Phase 4 — no longer a fallback trigger.
    // typeWidening: DataFusion's parquet schema adapter handles widening reads
    // (parquet stores the file's original type; the adapter casts to the table's
    // current widened type at read time). Removed from the gate; verified by
    // TypeWidening{TableFeature,Metadata,...}Suite in the Delta regression.
    // rowTracking: tables with `enable_row_tracking=true` are scannable
    // natively. Queries that explicitly select `_metadata.row_id` /
    // `_metadata.row_commit_version` are handled in CometScanRule's
    // `applyRowTrackingRewrite` (it rewrites the scan to read the materialized
    // physical column, or declines when no materialized name is available).
    // No need to gate the whole table's scan path here.

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
    // it. Clone the Arc so we keep a stable handle through scan construction
    // (driver no longer needs `table_root()` here -- DV decode now happens on the
    // executor via `dv_reader::read_dv_indexes` -- but the Arc retention is still
    // wanted for any future post-scan-build kernel API that wants the snapshot).
    let snapshot_arc: Arc<_> = snapshot;
    let mut scan_builder = Arc::clone(&snapshot_arc).scan_builder();
    if let Some(pred) = kernel_predicate {
        scan_builder = scan_builder.with_predicate(Arc::new(pred));
    }
    let scan = scan_builder.build()?;

    // Per-row state extracted directly from each scan_metadata RecordBatch -- avoids
    // both `DvInfo` (whose `deletion_vector` field is `pub(crate)`) and the per-DV
    // `get_row_indexes` driver-side read that previously materialised a Vec<u64>.
    //
    // Two parallel vecs (indexed by visit order over SELECTED rows):
    //   - row_tracking: (baseRowId, defaultRowCommitVersion). `ScanFile` doesn't
    //     surface these; we extract from the `fileConstantValues` struct column.
    //   - dv_descriptors: per-row DV descriptor as a proto message. None = no DV.
    //     The executor calls `kernel::DeletionVectorDescriptor::read` on-task --
    //     the driver no longer holds the expanded indexes.
    //
    // Comet's native synthetic-columns exec uses base_row_id / default_row_commit_version
    // to synthesise Delta's logical `row_id` and `row_commit_version`.
    struct RawEntry {
        path: String,
        size: i64,
        modification_time: i64,
        num_records: Option<u64>,
        partition_values: HashMap<String, String>,
        dv_descriptor: Option<crate::proto::DeltaDvDescriptor>,
        base_row_id: Option<i64>,
        default_row_commit_version: Option<i64>,
    }

    // Kernel's `visit_scan_files` requires a `fn` callback (not `FnMut`), so any
    // per-call state must live in the `context` we pass in. Use a struct that carries
    // both the accumulator AND the per-row lookups for the current batch.
    struct RawEntryAcc {
        entries: Vec<RawEntry>,
        row_tracking: Vec<(Option<i64>, Option<i64>)>,
        dv_descriptors: Vec<Option<crate::proto::DeltaDvDescriptor>>,
        next_idx: usize,
    }
    let mut acc = RawEntryAcc {
        entries: Vec::new(),
        row_tracking: Vec::new(),
        dv_descriptors: Vec::new(),
        next_idx: 0,
    };
    let scan_metadata = scan.scan_metadata(&*engine)?;

    for meta_result in scan_metadata {
        let meta: delta_kernel::scan::ScanMetadata = meta_result?;
        // Pre-extract per-row state for the SELECTED rows in this batch. Kernel's
        // `visit_scan_files` walks selected rows in order; we build parallel vecs
        // indexed by visit order, so the callback pulls each row's values via a
        // shared counter.
        acc.row_tracking = extract_row_tracking_for_selected(&meta)?;
        acc.dv_descriptors = extract_dv_descriptors_for_selected(&meta)?;
        acc.next_idx = 0;
        acc = meta.visit_scan_files(
            acc,
            |acc: &mut RawEntryAcc, scan_file: delta_kernel::scan::state::ScanFile| {
                let num_records = scan_file.stats.as_ref().map(|s| s.num_records);
                let (base_row_id, default_row_commit_version) = acc
                    .row_tracking
                    .get(acc.next_idx)
                    .copied()
                    .unwrap_or((None, None));
                let dv_descriptor = acc
                    .dv_descriptors
                    .get(acc.next_idx)
                    .cloned()
                    .unwrap_or(None);
                acc.next_idx += 1;
                acc.entries.push(RawEntry {
                    path: scan_file.path,
                    size: scan_file.size,
                    modification_time: scan_file.modification_time,
                    num_records,
                    partition_values: scan_file.partition_values,
                    dv_descriptor,
                    base_row_id,
                    default_row_commit_version,
                });
            },
        )?;
    }
    let raw = acc.entries;

    // No more driver-side DV materialisation -- just forward the descriptor. The
    // executor (`dv_reader::read_dv_indexes` invoked from `DeltaDvFilterExec`)
    // reads + decodes the RoaringBitmap on-task. Pre-refactor this loop called
    // `DvInfo::get_row_indexes` and produced a `Vec<u64>` per file, which on the
    // 99 M-row "huge table delete" DV reached ~800 MB per scan exec (task #218).
    let mut entries: Vec<DeltaFileEntry> = Vec::with_capacity(raw.len());
    for r in raw {
        entries.push(DeltaFileEntry {
            path: r.path,
            size: r.size,
            modification_time: r.modification_time,
            num_records: r.num_records,
            partition_values: r.partition_values,
            dv_descriptor: r.dv_descriptor,
            base_row_id: r.base_row_id,
            default_row_commit_version: r.default_row_commit_version,
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

/// Extract `(baseRowId, defaultRowCommitVersion)` per SELECTED row from a `ScanMetadata`
/// batch's underlying `RecordBatch`. Kernel's `visit_scan_files` callback receives a
/// `ScanFile` that does NOT surface these row-tracking values; they live in the raw
/// `fileConstantValues` struct column on the underlying arrow batch.
///
/// `kernel/src/scan/log_replay.rs::SCAN_ROW_SCHEMA` defines the schema:
///   { path, size, modificationTime, stats, deletionVector,
///     fileConstantValues: { partitionValues, baseRowId, defaultRowCommitVersion, tags } }
/// So the fileConstantValues struct is the 6th top-level field (index 5), and within it
/// baseRowId is at field index 1 and defaultRowCommitVersion at field index 2.
///
/// Returns one `(Option<i64>, Option<i64>)` per SELECTED row, in visit_scan_files order.
/// Rows where row tracking isn't enabled have `(None, None)`.
fn extract_row_tracking_for_selected(
    meta: &delta_kernel::scan::ScanMetadata,
) -> DeltaResult<Vec<(Option<i64>, Option<i64>)>> {
    use delta_kernel::arrow::array::{Array, Int64Array, StructArray};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    let engine_data = meta.scan_files.data();
    let arrow = match engine_data.any_ref().downcast_ref::<ArrowEngineData>() {
        Some(a) => a,
        // Non-Arrow engine (shouldn't happen for our DefaultEngine path); return empty
        // so downstream sees (None, None) per row and the row-tracking decline gate
        // takes over.
        None => return Ok(Vec::new()),
    };
    let batch = arrow.record_batch();
    let total_rows = batch.num_rows();

    let file_constants = batch
        .column_by_name("fileConstantValues")
        .and_then(|c| c.as_any().downcast_ref::<StructArray>());
    let (base_arr, default_arr): (Option<&Int64Array>, Option<&Int64Array>) = match file_constants
    {
        Some(s) => (
            s.column_by_name("baseRowId")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>()),
            s.column_by_name("defaultRowCommitVersion")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>()),
        ),
        None => (None, None),
    };

    let sel = meta.scan_files.selection_vector();
    // FilteredEngineData::try_new asserts `sel.len() <= data.len()`; rows beyond
    // sel.len() are treated as not-selected. visit_scan_files visits only rows that ARE
    // selected, so any rows past sel.len() won't appear in the callback and our parallel
    // vec stays aligned. The explicit bound below makes the contract obvious.
    let bounded_rows = total_rows.min(sel.len());
    let mut out: Vec<(Option<i64>, Option<i64>)> =
        Vec::with_capacity(sel.iter().filter(|b| **b).count());
    for i in 0..bounded_rows {
        if !sel[i] {
            continue;
        }
        let b = base_arr.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) });
        let d = default_arr.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) });
        out.push((b, d));
    }
    Ok(out)
}

/// Per-row DV descriptors extracted from kernel's `scan_metadata.scan_files`
/// RecordBatch, indexed by SELECTED row position (parallel to row_tracking,
/// consumed in `visit_scan_files` order).
///
/// `None` for rows without a DV (no deletion vector attached to that AddFile).
///
/// We extract directly from the RecordBatch instead of via `ScanFile.dv_info`
/// because kernel 0.19's `DvInfo` only exposes `has_vector()` + `get_row_indexes()`
/// publicly (the descriptor itself is `pub(crate)`), and `get_row_indexes()`
/// materialises the full bitmap on the DRIVER -- which was the 1 GB long[]
/// retention bug we're fixing (task #218). Reading the fields from the kernel
/// scan_files schema directly lets us ship the descriptor instead of the indices.
///
/// Schema reference: `delta_kernel::scan::log_replay::SCAN_ROW_SCHEMA` includes a
/// `deletionVector` struct column with fields `storageType` (utf8),
/// `pathOrInlineDv` (utf8), `offset` (int32, nullable), `sizeInBytes` (int32),
/// `cardinality` (int64). When the AddFile has no DV the whole struct is null.
fn extract_dv_descriptors_for_selected(
    meta: &delta_kernel::scan::ScanMetadata,
) -> DeltaResult<Vec<Option<crate::proto::DeltaDvDescriptor>>> {
    use delta_kernel::arrow::array::{Array, Int32Array, Int64Array, StringArray, StructArray};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    let engine_data = meta.scan_files.data();
    let arrow = match engine_data.any_ref().downcast_ref::<ArrowEngineData>() {
        Some(a) => a,
        // Non-Arrow engine (shouldn't happen for our DefaultEngine path); return
        // empty so downstream sees None per row -- which matches the no-DV case.
        None => return Ok(Vec::new()),
    };
    let batch = arrow.record_batch();
    let total_rows = batch.num_rows();

    let dv_struct = batch
        .column_by_name("deletionVector")
        .and_then(|c| c.as_any().downcast_ref::<StructArray>());
    let (storage_arr, path_arr, offset_arr, size_arr, card_arr) = match dv_struct {
        Some(s) => (
            s.column_by_name("storageType")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            s.column_by_name("pathOrInlineDv")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            s.column_by_name("offset")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>()),
            s.column_by_name("sizeInBytes")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>()),
            s.column_by_name("cardinality")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>()),
        ),
        None => (None, None, None, None, None),
    };

    let sel = meta.scan_files.selection_vector();
    let bounded_rows = total_rows.min(sel.len());
    let mut out: Vec<Option<crate::proto::DeltaDvDescriptor>> =
        Vec::with_capacity(sel.iter().filter(|b| **b).count());
    for i in 0..bounded_rows {
        if !sel[i] {
            continue;
        }
        // A row has a DV iff the outer struct is non-null AND storageType is present
        // and non-null. Kernel materialises the entire struct as null when there's
        // no DV; the field-level null check is a belt-and-braces for engines that
        // might emit a non-null struct with null fields.
        let struct_null = dv_struct.map(|s| s.is_null(i)).unwrap_or(true);
        if struct_null {
            out.push(None);
            continue;
        }
        let storage_null = storage_arr.map(|a| a.is_null(i)).unwrap_or(true);
        if storage_null {
            out.push(None);
            continue;
        }
        let storage_type = storage_arr.unwrap().value(i).to_string();
        let path_or_inline_dv = path_arr.map(|a| a.value(i).to_string()).unwrap_or_default();
        // offset is `optional uint64` on the proto side -- preserve the null/non-null
        // distinction from the source (Delta inline DVs sometimes lack an offset).
        let offset = offset_arr.and_then(|a| {
            if a.is_null(i) {
                None
            } else {
                Some(a.value(i) as u64)
            }
        });
        let size_in_bytes = size_arr.map(|a| a.value(i) as u64).unwrap_or(0);
        let cardinality = card_arr.map(|a| a.value(i) as u64).unwrap_or(0);
        out.push(Some(crate::proto::DeltaDvDescriptor {
            storage_type,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
            // inline_bytes is reserved for a future optimisation where the driver
            // pre-decodes inline DVs (rare + already small). For now the executor
            // decodes via `path_or_inline_dv` (kernel's
            // `DeletionVectorDescriptor::read` handles all three storage types
            // uniformly when reconstructed with the original `path_or_inline_dv`).
            inline_bytes: Vec::new(),
        }));
    }
    Ok(out)
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
