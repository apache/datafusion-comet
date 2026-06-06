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

use delta_kernel::arrow::array::{new_null_array, RecordBatch};
use delta_kernel::arrow::compute::cast as arrow_cast;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit as ArrowTimeUnit,
};
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use delta_kernel::parquet::arrow::ProjectionMask;
use delta_kernel::parquet::basic::Type as ParquetPhysicalType;
use delta_kernel::parquet::schema::types::SchemaDescriptor;
use delta_kernel::scan::state::transform_to_logical;
use delta_kernel::schema::{SchemaRef, StructType};
use delta_kernel::{DeltaResult, Engine, EngineData, Error, ExpressionRef};
use url::Url;

use std::any::Any;
use std::collections::HashSet;
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

use datafusion::common::ScalarValue;

use crate::dv_reader::{
    map_dv_error_to_datafusion, map_file_read_error, normalize_table_root, read_dv_indexes,
};
use crate::engine::{get_or_create_engine, DeltaStorageConfig};
use crate::planner::{parse_delta_partition_scalar, SessionTimezone};
use crate::proto::DeltaDvDescriptor;

/// Convert a Comet arrow `Schema` into a kernel `StructType` for `transform_to_logical`. Since
/// delta-kernel pins the same arrow version as Comet (arrow-58), this is kernel's own
/// `try_from_arrow` directly -- no cross-version FFI bridge, and no field-id remap (the read
/// matches columns by physical name, so kernel never consults `parquet.field.id`).
fn arrow_to_kernel_schema(schema: &ArrowSchema) -> DeltaResult<SchemaRef> {
    Ok(Arc::new(
        StructType::try_from_arrow(schema).map_err(Error::Arrow)?,
    ))
}

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
/// expression API; `None` for a plain table with no mapping/partitions/row-tracking).
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
    // `file_size` is currently unused: we read the whole object through the storage handler, which
    // sizes the request itself. Kept in the signature for callers / future range reads.
    let _ = file_size;

    // We read the parquet ourselves rather than via kernel's `parquet_handler().read_parquet_files`
    // because kernel reads with a default `ArrowReaderOptions`, leaving INT96 timestamps at
    // nanosecond resolution. Spark writes TIMESTAMP (LTZ) as INT96 by default, and i64 nanoseconds
    // overflow at ~year 2262, so any later timestamp reads back as garbage (9999-12-31 -> 1816).
    // Mirroring Comet's canonical parquet path (`coerce_int96="us"`), we supply a schema that types
    // INT96 columns as microseconds so the reader coerces them on read.
    let arrow_physical: ArrowSchema = physical_schema
        .as_ref()
        .try_into_arrow()
        .map_err(Error::Arrow)?;

    // Fetch the data file's bytes through kernel's storage layer (object_store under the hood). This
    // is the same sync entry point `dv_reader` uses, so it runs on the engine's background executor
    // and works for local / S3 / Azure alike without spinning up a nested tokio runtime.
    let mut bytes_iter = engine.storage_handler().read_files(vec![(file_url, None)])?;
    let data = bytes_iter
        .next()
        .ok_or_else(|| Error::generic(format!("no bytes read for Delta file {file_path}")))??;

    // Load metadata once, build a supplied schema with INT96 columns coerced to microseconds, and
    // rebuild the reader metadata against it (the DataFusion `coerce_int96_to_resolution` recipe).
    let base_meta = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new())?;
    let options = match coerce_int96_to_micros(base_meta.parquet_schema(), base_meta.schema()) {
        Some(supplied) => ArrowReaderOptions::new().with_schema(Arc::new(supplied)),
        None => ArrowReaderOptions::new(),
    };
    let reader_meta = ArrowReaderMetadata::try_new(base_meta.metadata().clone(), options)?;
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(data, reader_meta);

    // Project to just the physical (data) columns we need, matched by name. Column mapping has
    // already renamed `arrow_physical`'s fields to physical names, which equal the parquet column
    // names, so a name match reproduces kernel's column selection. (Nested column mapping is still
    // guarded to the old path -- see #47 -- so top-level name matching suffices here.)
    let mask = ProjectionMask::columns(
        builder.parquet_schema(),
        arrow_physical.fields().iter().map(|f| f.name().as_str()),
    );
    let reader = builder.with_projection(mask).build()?;

    let mut out: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        let batch = batch?;
        // Reorder the projected columns into `arrow_physical` order and reconcile types -- e.g. the
        // INT96 coercion yields `Timestamp(us, None)` while the table wants `Timestamp(us, "UTC")`,
        // a metadata-only relabel. Stands in for kernel's `fixup_parquet_read`.
        let physical_batch = align_batch_to_schema(batch, &arrow_physical)?;
        let physical_data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(physical_batch));

        // Physical -> logical: column mapping (incl. nested), partition injection, row-tracking.
        // Kernel's expression evaluator does this; no Comet-side physicalisation.
        let logical = transform_to_logical(
            engine,
            physical_data,
            &physical_schema,
            &logical_schema,
            transform.clone(),
        )?;
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

/// Build a supplied arrow schema that retypes INT96 columns as `Timestamp(Microsecond, ...)` so the
/// parquet reader coerces them on read instead of defaulting to nanoseconds (which overflows i64 for
/// timestamps after ~year 2262). Mirrors DataFusion's `coerce_int96_to_resolution` / Comet's
/// `coerce_int96="us"`, but for the arrow version kernel pins. Returns `None` when the file has no INT96
/// columns (nothing to override). Top-level only: nested column mapping is still guarded to the old
/// path (#47), so nested INT96 columns don't reach this path.
fn coerce_int96_to_micros(
    parquet_schema: &SchemaDescriptor,
    file_schema: &ArrowSchema,
) -> Option<ArrowSchema> {
    let int96_top: HashSet<&str> = parquet_schema
        .columns()
        .iter()
        .filter(|c| c.physical_type() == ParquetPhysicalType::INT96)
        .filter_map(|c| c.path().parts().first().map(String::as_str))
        .collect();
    if int96_top.is_empty() {
        return None;
    }
    let mut changed = false;
    let fields: Vec<Arc<ArrowField>> = file_schema
        .fields()
        .iter()
        .map(|f| {
            // Only retype a top-level primitive Timestamp column that originated as INT96.
            if int96_top.contains(f.name().as_str()) {
                if let ArrowDataType::Timestamp(unit, tz) = f.data_type() {
                    if *unit != ArrowTimeUnit::Microsecond {
                        changed = true;
                        return Arc::new(
                            ArrowField::new(
                                f.name(),
                                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, tz.clone()),
                                f.is_nullable(),
                            )
                            .with_metadata(f.metadata().clone()),
                        );
                    }
                }
            }
            f.clone()
        })
        .collect();
    changed.then(|| ArrowSchema::new_with_metadata(fields, file_schema.metadata().clone()))
}

/// Reorder `batch`'s columns to match `target` (by field name) and cast each to the target field
/// type. Stands in for kernel's `fixup_parquet_read`: the projected read returns columns in file
/// order with the parquet-inferred types (e.g. INT96 coerced to `Timestamp(us, None)`), and the
/// transform evaluator expects them in `physical_schema` order/types (e.g. `Timestamp(us, "UTC")`).
/// Timestamp tz relabels and other widening casts are metadata-/value-preserving here. A target
/// column absent from the file is synthesised as all-NULL -- Delta schema evolution reads a column
/// added in a later commit as NULL in the older data files that predate it (same as kernel's reader).
fn align_batch_to_schema(batch: RecordBatch, target: &ArrowSchema) -> DeltaResult<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let col = match batch.schema().index_of(field.name()) {
            Ok(idx) => {
                let col = batch.column(idx);
                if col.data_type() == field.data_type() {
                    Arc::clone(col)
                } else {
                    arrow_cast(col, field.data_type()).map_err(Error::Arrow)?
                }
            }
            // Schema evolution: this column was added after these rows were written -> read as NULL.
            Err(_) => new_null_array(field.data_type(), num_rows),
        };
        columns.push(col);
    }
    RecordBatch::try_new(Arc::new(target.clone()), columns).map_err(Error::Arrow)
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
    /// Partition values for this file: `(column name, value)` where value is `None` for a NULL
    /// partition. Empty for unpartitioned tables. These are injected as constant columns after
    /// the parquet read (partition columns aren't stored in the data files).
    pub partition_values: Vec<(String, Option<String>)>,
}

/// Iceberg-style "kernel reads" scan operator. Reads each Delta data file through
/// `delta-kernel-rs` (`read_file_via_kernel`); kernel and Comet share arrow-58, so the resulting
/// `RecordBatch`es drop straight into the Comet plan. Replaces the ParquetSource + DV-sweep +
/// synthetic-columns stack on the kernel-read path; built by core's `plan_delta_scan` when
/// `DeltaScanCommon.kernel_read` is set. Single output partition: partition 0 reads every file.
///
/// Column mapping (name- and id-mode, including nested struct fields): read with
/// `physical_schema` (physical names), then an identity transform relabels to `output_schema`
/// (logical names). The physical->logical rename is driven by the schema pair through kernel's
/// evaluator; partitions, row-tracking, and DVs are all handled here -- this is the sole Delta
/// read path (the old ParquetSource + DV-sweep stack was removed).
pub struct DeltaKernelScanExec {
    /// Comet (arrow-58) output schema = data columns (logical names) followed by partition
    /// columns, matching Spark's `requiredSchema ++ partitionSchema` file-scan output.
    output_schema: ArrowSchemaRef,
    /// Schema to read from parquet, in PHYSICAL names (data columns only -- partition columns
    /// aren't stored in the files). Equals `read_logical_schema` for plain tables; differs
    /// (renamed) under column mapping.
    physical_schema: ArrowSchemaRef,
    /// Logical schema the read + transform produces (data columns only, logical names). The
    /// transform relabels `physical_schema` -> this; partition columns are appended afterward to
    /// reach `output_schema`.
    read_logical_schema: ArrowSchemaRef,
    /// Whether a physical->logical transform must be applied (column mapping active). When false
    /// the read is pass-through.
    needs_transform: bool,
    /// Whether to apply the deletion vector here (drop deleted rows). `true` for a standalone read.
    /// `false` when a `DeltaSyntheticColumnsExec` wraps this exec and applies the DV itself (so it
    /// can also surface `is_row_deleted` / compute row positions against the full physical rows).
    apply_dv: bool,
    /// Partition columns (arrow-58), appended as constants after the read. Empty = unpartitioned.
    partition_schema: ArrowSchemaRef,
    /// Session timezone, for parsing TIMESTAMP partition values to the correct instant.
    session_timezone: String,
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
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        output_schema: ArrowSchemaRef,
        physical_schema: ArrowSchemaRef,
        read_logical_schema: ArrowSchemaRef,
        needs_transform: bool,
        apply_dv: bool,
        partition_schema: ArrowSchemaRef,
        session_timezone: String,
        table_root: String,
        storage_config: DeltaStorageConfig,
        files: Vec<KernelScanFile>,
    ) -> Self {
        // Single DataFusion partition: the Spark side (CometDeltaNativeScanExec) already splits
        // files across Spark partitions and injects each partition's file subset, consuming one
        // DataFusion partition per Spark partition. For DV / row-tracking tables it sets
        // `oneTaskPerPartition`, so each Spark partition (hence this exec) sees a single file --
        // which is what lets the wrapping DeltaSyntheticColumnsExec compute per-file row positions.
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            output_schema,
            physical_schema,
            read_logical_schema,
            needs_transform,
            apply_dv,
            partition_schema,
            session_timezone,
            table_root,
            storage_config,
            files,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Read + (optionally) DV-filter + bridge EVERY file given to this exec to arrow-58. A single
    /// DataFusion partition: the Spark side (CometDeltaNativeScanExec) splits files across Spark
    /// partitions and injects each partition's subset, so `self.files` already holds just this
    /// partition's files. For DV / row-tracking tables it injects one file per Spark partition, so
    /// the wrapping DeltaSyntheticColumnsExec computes row positions against a single file. Eager
    /// (collects into a `Vec`); the blocking kernel reads run on the calling task thread.
    fn read_all(&self) -> DFResult<Vec<arrow::array::RecordBatch>> {
        let table_root_url = normalize_table_root(&self.table_root)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan table root: {e}")))?;
        let engine = get_or_create_engine(&table_root_url, &self.storage_config)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan engine: {e}")))?;

        // Zero data columns (partition-only reads, e.g. `groupBy(partition).agg(count("*"))`):
        // nothing to read from parquet. Drive each file's row count from `record_count` (Delta
        // numRecords; parquet footer fallback), emit an empty-data batch of that length, and append
        // partition constants. When the DV is applied here it just shortens the count; a wrapping
        // DeltaSyntheticColumnsExec (apply_dv off) instead gets the full physical count and
        // drops/flags rows itself.
        if self.physical_schema.fields().is_empty() {
            let empty_schema = Arc::new(arrow::datatypes::Schema::empty());
            let mut out = Vec::new();
            for file in &self.files {
                let n = self.file_row_count(engine.as_ref(), &table_root_url, file)?;
                let live = match (&file.dv, self.apply_dv) {
                    (Some(desc), true) => {
                        let deleted = read_dv_indexes(desc, &table_root_url, &self.storage_config)
                            .map_err(|e| map_dv_error_to_datafusion(e, desc))?;
                        n - deleted.iter().filter(|&&i| (i as usize) < n).count()
                    }
                    _ => n,
                };
                let opts = arrow::array::RecordBatchOptions::new().with_row_count(Some(live));
                let batch = arrow::array::RecordBatch::try_new_with_options(
                    Arc::clone(&empty_schema),
                    vec![],
                    &opts,
                )
                .map_err(DataFusionError::from)?;
                out.push(self.append_partition_columns(batch, &file.partition_values)?);
            }
            return Ok(out);
        }

        // Schemas come from the proto (already arrow-58), converted to kernel -- no Snapshot.
        // `logical` carries the DATA logical names (partition columns are appended after the read,
        // not produced by it); `physical` the parquet (physical) names.
        let logical_schema = arrow_to_kernel_schema(&self.read_logical_schema)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan logical schema: {e}")))?;
        let physical_schema = arrow_to_kernel_schema(&self.physical_schema)
            .map_err(|e| DataFusionError::Execution(format!("kernel scan physical schema: {e}")))?;

        // Under column mapping the physical names differ from the logical ones; an identity
        // transform makes kernel's evaluator relabel physical -> logical via the schema pair.
        // Plain tables pass `None` (pure pass-through).
        let transform: Option<ExpressionRef> = if self.needs_transform {
            Some(Arc::new(delta_kernel::expressions::Expression::struct_patch(
                delta_kernel::expressions::ExpressionStructPatch::new_top_level(),
            )))
        } else {
            None
        };

        let mut out: Vec<arrow::array::RecordBatch> = Vec::new();
        for file in &self.files {
            // Decode the DV (if any) into a keep-mask, UNLESS apply_dv is off -- then all physical
            // rows pass through and a wrapping DeltaSyntheticColumnsExec drops/flags them.
            let selection_vector = match (&file.dv, self.apply_dv) {
                (Some(desc), true) => {
                    let deleted = read_dv_indexes(desc, &table_root_url, &self.storage_config)
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
                _ => None,
            };

            let kernel_batches = read_file_via_kernel(
                engine.as_ref(),
                &table_root_url,
                &file.path,
                file.size,
                selection_vector,
                transform.clone(),
                physical_schema.clone(),
                logical_schema.clone(),
            )
            .map_err(|e| map_file_read_error(e, &file.path))?;

            for batch in &kernel_batches {
                // Kernel pins the same arrow version as Comet (arrow-58), so the kernel batch IS a
                // Comet batch -- no bridge.
                let data_batch = batch.clone();
                out.push(self.append_partition_columns(data_batch, &file.partition_values)?);
            }
        }
        Ok(out)
    }

    /// Row count for a file with no data columns to read: Delta's `numRecords` stat when present,
    /// else the parquet footer's row count (always available, one metadata fetch via the storage
    /// handler). Used only by the zero-data-column path.
    fn file_row_count(
        &self,
        engine: &dyn Engine,
        table_root_url: &Url,
        file: &KernelScanFile,
    ) -> DFResult<usize> {
        if let Some(rc) = file.record_count {
            return Ok(rc.max(0) as usize);
        }
        let file_url = table_root_url.join(&file.path).map_err(|e| {
            DataFusionError::Execution(format!("kernel scan file url {}: {e}", file.path))
        })?;
        let mut bytes_iter = engine
            .storage_handler()
            .read_files(vec![(file_url, None)])
            .map_err(|e| map_file_read_error(e, &file.path))?;
        let data = bytes_iter
            .next()
            .ok_or_else(|| {
                DataFusionError::Execution(format!("no bytes read for Delta file {}", file.path))
            })?
            .map_err(|e| map_file_read_error(e, &file.path))?;
        let meta = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new())
            .map_err(|e| map_file_read_error(delta_kernel::Error::from(e), &file.path))?;
        Ok(meta.metadata().file_metadata().num_rows().max(0) as usize)
    }

    /// Append partition columns as constants (partition values are stored in the Add action, not
    /// the data file). Produces a batch matching `output_schema` (data ++ partition). No-op for
    /// unpartitioned tables.
    fn append_partition_columns(
        &self,
        data_batch: arrow::array::RecordBatch,
        partition_values: &[(String, Option<String>)],
    ) -> DFResult<arrow::array::RecordBatch> {
        if self.partition_schema.fields().is_empty() {
            return Ok(data_batch);
        }
        let num_rows = data_batch.num_rows();
        let parsed_tz = SessionTimezone::parse(&self.session_timezone);
        let mut columns = data_batch.columns().to_vec();
        for pf in self.partition_schema.fields() {
            let raw = partition_values
                .iter()
                .find(|(name, _)| name == pf.name())
                .and_then(|(_, v)| v.as_ref());
            let scalar = match raw {
                Some(v) => parse_delta_partition_scalar(
                    v,
                    pf.data_type(),
                    &parsed_tz,
                    &self.session_timezone,
                )
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "kernel scan partition value parse for '{}': {e}",
                        pf.name()
                    ))
                })?,
                // Absent or explicitly-NULL partition value -> a typed NULL constant.
                None => ScalarValue::try_from(pf.data_type())?,
            };
            columns.push(scalar.to_array_of_size(num_rows)?);
        }
        arrow::array::RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map_err(DataFusionError::from)
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
    use crate::engine::{get_or_create_engine, DeltaStorageConfig};
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
    // re-exported arrow/parquet (arrow-58, same as Comet) so there's a single arrow version end to end.
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

    // Drains the kernel SchemaRef + a shared engine for the table at `url`.
    fn engine_and_schema(
        url: &Url,
    ) -> (Arc<crate::engine::DeltaEngine>, delta_kernel::schema::SchemaRef) {
        let engine = get_or_create_engine(url, &DeltaStorageConfig::default()).unwrap();
        let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref()).unwrap();
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
            engine.as_ref(),
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
            engine.as_ref(),
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
    // and streams Comet-native batches. Proves the whole native
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
            partition_values: vec![],
        }];

        let exec = DeltaKernelScanExec::new(
            Arc::clone(&output_schema),
            Arc::clone(&output_schema), // plain table: physical == logical
            output_schema,              // read_logical == output (no partitions)
            false,                      // no transform
            true,                       // apply_dv
            Arc::new(Schema58::empty()),
            "UTC".to_string(),
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

    // Phase 1c (#44): DeltaKernelScanExec with top-level name-mode column mapping. The parquet
    // holds a physical name ("col-abc"); the exec reads with the physical schema, applies the
    // identity transform, and the output is relabeled to the logical name ("id"). Exercises the
    // full exec path (read + transform + arrow bridge) end to end.
    #[tokio::test]
    async fn kernel_scan_exec_column_mapping_renames() {
        use arrow::array::{Array as _, Int64Array as Int64_58};
        use arrow::datatypes::{DataType as DT58, Field as F58, Schema as S58};
        use delta_kernel::arrow::array::{Int64Array as Int64_57, RecordBatch as RB57};
        use delta_kernel::arrow::datatypes::{DataType as DT57, Field as F57, Schema as S57};

        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path();
        let parquet_path = table_dir.join("part-00000.parquet");
        let physical_arrow = Arc::new(S57::new(vec![F57::new("col-abc", DT57::Int64, true)]));
        let batch = RB57::try_new(
            physical_arrow.clone(),
            vec![Arc::new(Int64_57::from(vec![7i64, 8, 9]))],
        )
        .unwrap();
        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer =
            delta_kernel::parquet::arrow::ArrowWriter::try_new(file, physical_arrow, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = std::fs::metadata(&parquet_path).unwrap().len() as i64;

        let url = crate::scan::normalize_url(table_dir.to_str().unwrap()).unwrap();

        // output (logical) names "id"; physical (parquet) names "col-abc".
        let output = Arc::new(S58::new(vec![F58::new("id", DT58::Int64, true)]));
        let physical = Arc::new(S58::new(vec![F58::new("col-abc", DT58::Int64, true)]));
        let files = vec![KernelScanFile {
            path: "part-00000.parquet".to_string(),
            size,
            record_count: Some(3),
            dv: None,
            partition_values: vec![],
        }];

        let exec = DeltaKernelScanExec::new(
            Arc::clone(&output),
            physical,
            Arc::clone(&output), // read_logical == output (no partitions)
            true,                // column mapping -> needs transform
            true,                // apply_dv
            Arc::new(S58::empty()),
            "UTC".to_string(),
            url.as_str().to_string(),
            DeltaStorageConfig::default(),
            files,
        );
        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        assert_eq!(
            batches[0].schema().field(0).name(),
            "id",
            "column-mapped physical name should be relabeled to the logical name"
        );
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64_58>()
            .unwrap();
        assert_eq!(
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
    }

    // Phase 1c (#44 id-mode): the kernel-read path reads a column by its PHYSICAL name (the name
    // stored in the parquet file), then relabels it to the logical name via the transform. For an
    // id-mode table the driver supplies the physical name in `column_mappings` (delta_scan.rs renames
    // the read schema to `cm.physical_name`), so a name match reproduces what kernel's field-id
    // matcher used to do -- physical names are unique UUIDs, so matching by name is equivalent. The
    // parquet column carries a field id in its footer (as a real id-mode file would); this test
    // proves that field-id metadata on the schema doesn't disturb the name-based read + relabel.
    #[test]
    fn id_mode_reads_by_physical_name_then_relabels() {
        use crate::scan::normalize_url;
        use arrow::datatypes::{DataType as DT58, Field as F58, Schema as S58};
        use delta_kernel::arrow::array::{Array as _, Int64Array as Int64_57, RecordBatch as RB57};
        use delta_kernel::arrow::datatypes::{DataType as DT57, Field as F57, Schema as S57};
        use delta_kernel::expressions::{Expression, ExpressionStructPatch};
        use std::collections::HashMap;

        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path();
        let parquet_path = table_dir.join("part-00000.parquet");

        // Parquet column: physical name "col-xyz", field id 1 (written into the parquet footer).
        let mut pmd = HashMap::new();
        pmd.insert("PARQUET:field_id".to_string(), "1".to_string());
        let parquet_arrow =
            Arc::new(S57::new(vec![F57::new("col-xyz", DT57::Int64, true).with_metadata(pmd)]));
        let batch = RB57::try_new(
            parquet_arrow.clone(),
            vec![Arc::new(Int64_57::from(vec![100i64, 200, 300]))],
        )
        .unwrap();
        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer =
            delta_kernel::parquet::arrow::ArrowWriter::try_new(file, parquet_arrow, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = std::fs::metadata(&parquet_path).unwrap().len() as i64;

        let url = normalize_url(table_dir.to_str().unwrap()).unwrap();
        let engine = get_or_create_engine(&url, &DeltaStorageConfig::default()).unwrap();

        // Physical schema = the parquet's physical name "col-xyz" (+ its field id); logical schema =
        // the table's logical name "id". The identity transform relabels physical -> logical.
        let mut pmd58 = HashMap::new();
        pmd58.insert("PARQUET:field_id".to_string(), "1".to_string());
        let physical_arrow =
            S58::new(vec![F58::new("col-xyz", DT58::Int64, true).with_metadata(pmd58)]);
        let physical = arrow_to_kernel_schema(&physical_arrow).unwrap();
        let logical_arrow = S58::new(vec![F58::new("id", DT58::Int64, true)]);
        let logical = arrow_to_kernel_schema(&logical_arrow).unwrap();
        let transform = Arc::new(Expression::struct_patch(ExpressionStructPatch::new_top_level()));

        let batches = read_file_via_kernel(
            engine.as_ref(),
            &url,
            "part-00000.parquet",
            size,
            None,
            Some(transform),
            physical,
            logical,
        )
        .unwrap();

        // The column was read by physical name "col-xyz" and relabeled to logical "id"; the data is
        // the real values. Kernel shares Comet's arrow-58, so the returned batch is a Comet batch.
        assert_eq!(
            batches[0].schema().field(0).name(),
            "id",
            "expected physical 'col-xyz' to be relabeled to logical 'id'"
        );
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64_57>()
            .expect("name-matched column should be the real Int64 data, not a null column");
        assert_eq!(
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>(),
            vec![100, 200, 300]
        );
    }

    // INT96 coercion: Spark writes TIMESTAMP (LTZ) as parquet INT96, which arrow-rs reads as
    // Timestamp(Nanosecond) by default -- overflowing i64 for any instant after ~year 2262.
    // `coerce_int96_to_micros` retypes INT96 columns to microseconds so the reader coerces on read.
    #[test]
    fn coerce_int96_to_micros_retypes_only_int96_timestamps() {
        use delta_kernel::arrow::datatypes::{DataType as DT, Field as F, TimeUnit};
        use delta_kernel::parquet::basic::{Repetition, Type as PhysType};
        use delta_kernel::parquet::schema::types::Type as PType;

        // Parquet schema: one INT96 column "ts", one INT64 column "id".
        let ts = PType::primitive_type_builder("ts", PhysType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        let id = PType::primitive_type_builder("id", PhysType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        let message = PType::group_type_builder("schema")
            .with_fields(vec![Arc::new(id), Arc::new(ts)])
            .build()
            .unwrap();
        let descr = SchemaDescriptor::new(Arc::new(message));

        // arrow schema as arrow-rs infers it from that parquet: INT96 -> Timestamp(Nanosecond).
        let inferred = ArrowSchema::new(vec![
            F::new("id", DT::Int64, true),
            F::new("ts", DT::Timestamp(TimeUnit::Nanosecond, None), true),
        ]);

        let coerced = coerce_int96_to_micros(&descr, &inferred)
            .expect("a file with an INT96 column must produce a coerced schema");
        // The INT96 column is now microseconds; the plain Int64 column is untouched.
        assert_eq!(coerced.field(0).data_type(), &DT::Int64);
        assert_eq!(
            coerced.field(1).data_type(),
            &DT::Timestamp(TimeUnit::Microsecond, None)
        );

        // A schema with no INT96 columns needs no override.
        let no_int96 = SchemaDescriptor::new(Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    PType::primitive_type_builder("id", PhysType::INT64)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        ));
        let plain = ArrowSchema::new(vec![F::new("id", DT::Int64, true)]);
        assert!(coerce_int96_to_micros(&no_int96, &plain).is_none());
    }

    // `align_batch_to_schema` reorders columns by name and casts each to the target type -- e.g. the
    // tz relabel Timestamp(us, None) -> Timestamp(us, "UTC") that follows INT96 coercion.
    #[test]
    fn align_batch_to_schema_reorders_and_casts() {
        use delta_kernel::arrow::array::{
            Array as _, Int64Array, RecordBatch as RB57, TimestampMicrosecondArray,
        };
        use delta_kernel::arrow::datatypes::{DataType as DT, Field as F, Schema as S, TimeUnit};

        // Read batch: columns in FILE order [ts, id], ts has no timezone.
        let read_schema = Arc::new(S::new(vec![
            F::new("ts", DT::Timestamp(TimeUnit::Microsecond, None), true),
            F::new("id", DT::Int64, true),
        ]));
        let read_batch = RB57::try_new(
            read_schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![253402300799000000i64])),
                Arc::new(Int64Array::from(vec![7i64])),
            ],
        )
        .unwrap();

        // Target: [id, ts] order, ts wants a UTC timezone.
        let target = ArrowSchema::new(vec![
            F::new("id", DT::Int64, true),
            F::new(
                "ts",
                DT::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);

        let aligned = align_batch_to_schema(read_batch, &target).unwrap();
        assert_eq!(aligned.schema().field(0).name(), "id");
        assert_eq!(aligned.schema().field(1).name(), "ts");
        assert_eq!(
            aligned.schema().field(1).data_type(),
            &DT::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        // The extreme value survives intact (it would be garbage if read as nanoseconds).
        let ts = aligned
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 253402300799000000);
        let id = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.value(0), 7);
    }

    // Schema evolution: a column added in a later commit is absent from older data files. The read
    // (projected by name) returns no such column, and `align_batch_to_schema` must synthesise an
    // all-NULL column rather than error -- matching kernel's reader and Delta's read semantics.
    #[test]
    fn align_batch_to_schema_nullfills_missing_column() {
        use delta_kernel::arrow::array::{Array as _, Int64Array, RecordBatch as RB57};
        use delta_kernel::arrow::datatypes::{DataType as DT, Field as F, Schema as S};

        // Older file has only "id"; the table schema added "extra" later.
        let read_schema = Arc::new(S::new(vec![F::new("id", DT::Int64, true)]));
        let read_batch = RB57::try_new(read_schema, vec![Arc::new(Int64Array::from(vec![1i64, 2]))])
            .unwrap();
        let target = ArrowSchema::new(vec![
            F::new("id", DT::Int64, true),
            F::new("extra", DT::Utf8, true),
        ]);

        let aligned = align_batch_to_schema(read_batch, &target).unwrap();
        assert_eq!(aligned.num_columns(), 2);
        assert_eq!(aligned.schema().field(1).name(), "extra");
        let extra = aligned.column(1);
        assert_eq!(extra.len(), 2);
        assert_eq!(extra.null_count(), 2, "added column should read as all-NULL");
    }
}
