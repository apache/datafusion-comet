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

//! Iceberg-style "kernel reads" path (Phase 1 of the kernel-read refactor, see
//! `contrib/delta/docs/10-iceberg-style-kernel-read.md`).
//!
//! Instead of reading via Comet's `ParquetSource` and re-applying deletion vectors,
//! synthetic columns, and column-mapping physicalisation in Comet, this reads each Delta
//! data file through `delta-kernel-rs`'s own primitives -- the same way `iceberg_scan.rs`
//! reads through `iceberg::arrow::ArrowReaderBuilder`. Kernel does the parquet read, applies
//! the physical->logical transform (column mapping incl. nested, partition injection,
//! row-tracking materialisation), and applies the deletion vector.
//!
//! This module is the per-file primitive. The `DeltaKernelScanExec` (DataFusion
//! `ExecutionPlan`) and the JNI/proto wiring are built on top of it (Phase 1b). It runs on
//! the executor from serializable per-file inputs (path / size / DV info / partition values /
//! schemas / transform) without reconstructing the kernel `Snapshot` -- no per-executor log
//! replay.

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::state::transform_to_logical;
use delta_kernel::schema::SchemaRef;
use delta_kernel::{DeltaResult, Engine, Error, ExpressionRef, FileMeta};
use url::Url;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use crate::arrow_bridge::{comet_schema_to_kernel, kernel_batch_to_comet};
use crate::dv_reader::{map_dv_error_to_datafusion, normalize_table_root, read_dv_indexes};
use crate::engine::{get_or_create_engine, DeltaStorageConfig};
use crate::proto::DeltaDvDescriptor;

/// Read one Delta data file via kernel and return Arrow `RecordBatch`es already in the table's
/// **logical** schema: column mapping resolved (including nested), partition columns injected,
/// row-tracking materialised, and deletion-vector rows dropped.
///
/// Mirrors the per-file body of `delta_kernel::scan::Scan::execute`, but driven by per-file
/// inputs the driver ships to the executor rather than by an in-process `Snapshot`.
///
/// `selection_vector` is the deletion-vector mask for this file (physical row order, `true` =
/// keep), already decoded -- the executor produces it from the serializable DV descriptor via
/// the existing `dv_reader`, NOT from a kernel `DvInfo`. (`DvInfo` is intentionally avoided: it
/// is not serializable and its `deletion_vector` field is `pub(crate)`, so it can't be rebuilt
/// executor-side -- see `scan.rs`.) `None` means no DV (keep all rows).
///
/// `transform` is the physical->logical expression (rebuilt executor-side via kernel's public
/// `Transform` API in Phase 1b; `None` for a plain table with no mapping/partitions/row-tracking).
#[allow(clippy::too_many_arguments)]
pub fn read_file_via_kernel(
    engine: &dyn Engine,
    table_root: &Url,
    file_path: &str,
    file_size: i64,
    mut selection_vector: Option<Vec<bool>>,
    transform: Option<ExpressionRef>,
    physical_schema: SchemaRef,
    logical_schema: SchemaRef,
) -> DeltaResult<Vec<RecordBatch>> {
    let file_url = table_root.join(file_path)?;

    let meta = FileMeta {
        location: file_url,
        last_modified: 0,
        size: file_size
            .try_into()
            .map_err(|_| Error::generic("negative Delta file size"))?,
    };

    // TODO(Phase 1b): thread the pushed predicate here once row-index support lands upstream
    // (kernel disables read-time predicate pushdown until then -- see kernel issue #860).
    let read_iter = engine
        .parquet_handler()
        .read_parquet_files(&[meta], physical_schema.clone(), None)?;

    let mut out: Vec<RecordBatch> = Vec::new();
    for read_result in read_iter {
        let read_result = read_result?;
        // Physical -> logical: column mapping (incl. nested), partition injection, row-tracking.
        // Kernel's expression evaluator does this; no Comet-side physicalisation.
        let logical =
            transform_to_logical(engine, read_result, &physical_schema, &logical_schema, transform.clone())?;
        let len = logical.len();

        // Split the file-wide selection vector: the first `len` flags belong to this batch,
        // the rest carry forward (same accounting as Scan::execute's `split_vector`).
        let this_sv: Option<Vec<bool>> = match selection_vector.as_mut() {
            Some(sv) if sv.len() >= len => {
                let rest = sv.split_off(len);
                Some(std::mem::replace(sv, rest))
            }
            Some(sv) => Some(std::mem::take(sv)),
            None => None,
        };

        let filtered = match this_sv {
            Some(sv) => logical.apply_selection_vector(sv)?,
            None => logical,
        };

        let arrow = filtered
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::generic("kernel read returned non-Arrow EngineData"))?;
        out.push(arrow.record_batch().clone());
    }

    Ok(out)
}

/// One Delta data file's worth of per-file inputs the driver ships to the executor: enough to
/// read + DV-filter the file via kernel without reconstructing the `Snapshot`.
#[derive(Debug, Clone)]
pub struct KernelScanFile {
    /// File URL. Absolute once kernel resolved `add.path` on the driver; a path relative to
    /// the table root also resolves (kernel `join`s it onto the root).
    pub path: String,
    /// File size in bytes (from the Add action).
    pub size: i64,
    /// Physical row count (`add.stats.numRecords`). Required when a DV applies -- the keep-mask
    /// is sized to it. `None` is fine when there's no DV.
    pub record_count: Option<i64>,
    /// Deletion-vector descriptor (absent = no DV). Decoded executor-side via `dv_reader`.
    pub dv: Option<DeltaDvDescriptor>,
}

/// Iceberg-style "kernel reads" scan operator (Phase 1b). Reads each Delta data file through
/// `delta-kernel-rs` (`read_file_via_kernel`) and bridges the arrow-57 result into Comet's
/// arrow-58 plan (`kernel_batch_to_comet`). Replaces the ParquetSource + DV-sweep + synthetic-
/// columns stack on the kernel-read path; built by core's `plan_delta_scan` when
/// `DeltaScanCommon.kernel_read` is set. Single output partition: partition 0 reads every file.
///
/// Phase 1b scope: no column mapping, no partition columns, no row-tracking (`transform = None`).
/// Those re-add a kernel `Transform`, rebuilt executor-side via kernel's public API, in Phase 1c.
pub struct DeltaKernelScanExec {
    /// Comet (arrow-58) output schema = the projected/required schema.
    output_schema: ArrowSchemaRef,
    /// Normalised table root (trailing slash) used to resolve relative DV paths + the engine.
    table_root: String,
    /// Storage credentials/options for the kernel engine.
    storage_config: DeltaStorageConfig,
    /// Files this scan reads (whole-table, single partition for now).
    files: Vec<KernelScanFile>,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaKernelScanExec {
    pub fn new(
        output_schema: ArrowSchemaRef,
        table_root: String,
        storage_config: DeltaStorageConfig,
        files: Vec<KernelScanFile>,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            output_schema,
            table_root,
            storage_config,
            files,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Read + DV-filter + bridge every file to arrow-58. Eager (collects into a `Vec`) -- Phase
    /// 1b proves correctness; lazy per-file streaming is a 1c hardening step. The blocking kernel
    /// reads run on the calling task thread, matching how Comet drives scan operators.
    fn read_all(&self) -> DFResult<Vec<arrow::array::RecordBatch>> {
        let table_root_url = normalize_table_root(&self.table_root)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan table root: {e}")))?;
        let engine = get_or_create_engine(&table_root_url, &self.storage_config)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan engine: {e}")))?;
        // Schema comes from the proto (already arrow-58), converted to kernel -- no Snapshot.
        let kernel_schema = comet_schema_to_kernel(&self.output_schema)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan schema: {e}")))?;

        let mut out: Vec<arrow::array::RecordBatch> = Vec::new();
        for file in &self.files {
            // Decode the DV (if any) into a keep-mask sized to the file's physical rows.
            let selection_vector = match &file.dv {
                Some(desc) => {
                    let deleted = read_dv_indexes(desc, &table_root_url)
                        .map_err(|e| map_dv_error_to_datafusion(e, desc))?;
                    let n = file.record_count.ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "kernel scan: file {} has a deletion vector but no record_count to size the mask",
                            file.path
                        ))
                    })? as usize;
                    let mut mask = vec![true; n];
                    for idx in deleted {
                        if (idx as usize) < n {
                            mask[idx as usize] = false;
                        }
                    }
                    Some(mask)
                }
                None => None,
            };

            let kernel_batches = read_file_via_kernel(
                engine.as_ref(),
                &table_root_url,
                &file.path,
                file.size,
                selection_vector,
                None, // Phase 1b: no column mapping / partitions / row-tracking
                kernel_schema.clone(),
                kernel_schema.clone(),
            )
            .map_err(|e| DataFusionError::Execution(format!("kernel read of {}: {e}", file.path)))?;

            for batch in &kernel_batches {
                out.push(kernel_batch_to_comet(batch).map_err(DataFusionError::from)?);
            }
        }
        Ok(out)
    }
}

impl fmt::Debug for DeltaKernelScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaKernelScanExec(files={})", self.files.len())
    }
}

impl DisplayAs for DeltaKernelScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaKernelScanExec: files={}", self.files.len())
    }
}

impl ExecutionPlan for DeltaKernelScanExec {
    fn name(&self) -> &str {
        "DeltaKernelScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "DeltaKernelScanExec is a leaf, got {} children",
                children.len()
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DeltaKernelScanExec has a single partition, got {partition}"
            )));
        }
        let batches = self.read_all()?;
        let schema = Arc::clone(&self.output_schema);
        let stream =
            futures::stream::iter(batches.into_iter().map(Ok::<arrow::array::RecordBatch, DataFusionError>));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{create_engine, DeltaStorageConfig};
    use crate::scan::normalize_url;
    use delta_kernel::arrow::array::{Array, Int64Array, RecordBatch};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::parquet::arrow::ArrowWriter;
    use delta_kernel::Snapshot;
    use std::sync::Arc;

    // Builds a minimal single-file Delta table in a tempdir -- a real `part-00000.parquet`
    // (id: long, rows 0..=6) plus a one-commit `_delta_log`. Mirrors the hand-built log in
    // `scan.rs::test_list_delta_files_local`, but writes a *real* parquet (that test only
    // exercised log listing, so it stubbed the file). The parquet is written with kernel's own
    // re-exported arrow/parquet (arrow-57) so there's a single arrow version end to end.
    fn build_single_file_table() -> (tempfile::TempDir, Url, i64) {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("kscan_table");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        // Real parquet: one nullable Int64 column "id" with values 0..=6.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0i64, 1, 2, 3, 4, 5, 6]))],
        )
        .unwrap();
        let parquet_path = table_dir.join("part-00000.parquet");
        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = std::fs::metadata(&parquet_path).unwrap().len() as i64;

        // Single commit: protocol + metadata (id: long) + add pointing at the real parquet.
        let commit0 = [
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string(),
            r#"{"metaData":{"id":"kscan-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}"#.to_string(),
            format!(
                r#"{{"add":{{"path":"part-00000.parquet","partitionValues":{{}},"size":{size},"modificationTime":1700000000000,"dataChange":true,"stats":"{{\"numRecords\":7}}"}}}}"#
            ),
        ]
        .join("\n");
        std::fs::write(delta_log.join("00000000000000000000.json"), commit0).unwrap();

        let url = normalize_url(table_dir.to_str().unwrap()).unwrap();
        (tmp, url, size)
    }

    // Drains the kernel SchemaRef + a freshly built engine for the table at `url`.
    fn engine_and_schema(url: &Url) -> (crate::engine::DeltaEngine, delta_kernel::schema::SchemaRef) {
        let engine = create_engine(url, &DeltaStorageConfig::default()).unwrap();
        let snapshot = Snapshot::builder_for(url.clone()).build(&engine).unwrap();
        let schema = snapshot.schema();
        (engine, schema)
    }

    fn collect_ids(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column should be Int64");
                (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    // No deletion vector: every row comes back, in order.
    #[test]
    fn read_file_via_kernel_no_dv_returns_all_rows() {
        let (_tmp, url, size) = build_single_file_table();
        let (engine, schema) = engine_and_schema(&url);

        let batches = read_file_via_kernel(
            &engine,
            &url,
            "part-00000.parquet",
            size,
            None, // no DV -> keep all rows
            None, // plain table -> identity transform
            schema.clone(),
            schema,
        )
        .unwrap();

        assert_eq!(collect_ids(&batches), vec![0, 1, 2, 3, 4, 5, 6]);
    }

    // Deletion vector applied as a decoded mask (the real executor-side shape: dv_reader hands
    // this primitive an already-decoded Vec<bool>, never a kernel DvInfo). `false` rows drop.
    #[test]
    fn read_file_via_kernel_applies_selection_vector() {
        let (_tmp, url, size) = build_single_file_table();
        let (engine, schema) = engine_and_schema(&url);

        // Keep 0,2,3,5,6; drop 1 and 4.
        let mask = vec![true, false, true, true, false, true, true];
        let batches = read_file_via_kernel(
            &engine,
            &url,
            "part-00000.parquet",
            size,
            Some(mask),
            None,
            schema.clone(),
            schema,
        )
        .unwrap();

        assert_eq!(collect_ids(&batches), vec![0, 2, 3, 5, 6]);
    }

    // Full native Phase 1b path end to end: DeltaKernelScanExec::execute drives the kernel read,
    // bridges arrow-57 -> arrow-58, and streams Comet-native batches. Proves the whole native
    // side (read loop + arrow bridge + schema bridge + stream) before any proto/JVM wiring.
    #[tokio::test]
    async fn kernel_scan_exec_reads_table_end_to_end() {
        use arrow::array::{Array as _, Int64Array as Int64Array58};
        use arrow::datatypes::{DataType as DataType58, Field as Field58, Schema as Schema58};

        let (_tmp, url, size) = build_single_file_table();

        // Output (arrow-58) schema = the projected required schema, exactly as the proto ships it.
        let output_schema =
            Arc::new(Schema58::new(vec![Field58::new("id", DataType58::Int64, true)]));
        let files = vec![KernelScanFile {
            path: "part-00000.parquet".to_string(),
            size,
            record_count: Some(7),
            dv: None,
        }];

        let exec = DeltaKernelScanExec::new(
            output_schema,
            url.as_str().to_string(),
            DeltaStorageConfig::default(),
            files,
        );

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // The streamed batches are genuine arrow-58 (Comet) RecordBatches with the right rows.
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let c = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array58>()
                    .expect("arrow-58 Int64 id column");
                (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }
}
