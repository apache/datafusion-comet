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

//! `OpStruct::DeltaScan` planner body, feature-gated behind `contrib-delta`.
//!
//! This is the thin bridge between core's plan-tree builder and the
//! [`comet_contrib_delta`] crate. It lives in core (rather than the contrib
//! crate) because it reaches into core's `pub(crate)` planner helpers
//! (`create_expr`, `init_datasource_exec`, `prepare_object_store_with_configs`,
//! `convert_spark_types_to_arrow_schema`) -- a `contrib -> core` dependency
//! would cycle with core's optional `contrib-delta` dep on contrib. Compiled
//! only under `--features contrib-delta`; default builds carry zero Delta surface.
//!
//! Delta-specific algorithmic pieces (DV filter exec wrapping, column-mapping
//! rename projection, partition value parsing, synthetic column emission, kernel
//! log replay, the `ignore_missing_files` FileSource decorator) all live in the
//! [`comet_contrib_delta`] crate proper.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::{DeltaScan, DeltaScanCommon, Operator};

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::convert_spark_types_to_arrow_schema;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::execution::spark_plan::SparkPlan;

/// Column-and-name pairs for a `ProjectionExec` (one entry per projected output column).
type ProjectionColumns = Vec<(Arc<dyn PhysicalExpr>, String)>;

/// Build a `DeltaKernelScanExec` for the "kernel reads" path. The per-file inputs come straight
/// off the existing proto (`DeltaScanTask` path/size/record_count/dv); no new proto message.
///
/// Phase 1b: plain tables. Phase 1c (#44): + top-level name-mode column mapping (read with the
/// physical names, then an identity transform relabels to logical via the schema pair). Still
/// guarded to the old path (and the driver leaves `kernel_read` false for them): partitions,
/// row-tracking / synthetic columns, metadata columns, id-mode mapping (`use_field_id`), and
/// nested-typed columns (nested physical rename isn't handled here yet).
fn plan_delta_kernel_scan(
    spark_plan: &Operator,
    scan: &DeltaScan,
    common: &DeltaScanCommon,
    required_schema: &SchemaRef,
) -> PlanCreationResult {
    use comet_contrib_delta::jni::delta_storage_config_from_map;
    use comet_contrib_delta::kernel_scan::{DeltaKernelScanExec, KernelScanFile};

    let partition_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.partition_schema.as_slice());

    // The proto `required_schema` already contains the requested data columns followed by the
    // partition columns (Spark's `requiredSchema ++ partitionFieldsForRequired`). Partition
    // columns aren't stored in the data files, so SPLIT `required_schema` by partition-column
    // name: read the data fields from parquet, inject the partition fields as constants, and the
    // exec reassembles them into `required_schema` order. (Splitting by name also covers the case
    // where a partition column appears among the data fields.)
    let partition_names: std::collections::HashSet<&str> = partition_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let data_fields: Vec<_> = required_schema
        .fields()
        .iter()
        .filter(|f| !partition_names.contains(f.name().as_str()))
        .cloned()
        .collect();
    let partition_fields: Vec<_> = required_schema
        .fields()
        .iter()
        .filter(|f| partition_names.contains(f.name().as_str()))
        .cloned()
        .collect();

    // Data-column schemas come from kernel's own `Scan` (the driver projected the snapshot schema,
    // built the scan, and shipped `scan.physical_schema()` / `scan.logical_schema()` as Arrow IPC --
    // physical names + field-ids resolved at EVERY nesting level). The read uses the physical schema
    // (kernel matches the file by field-id, then name) and relabels physical -> logical via this
    // exact schema pair (kernel's own `Scan::execute` mechanism), so there is NO Comet-side
    // physicalisation of column mapping. `append_partition_columns`' positional `arrow_cast` then
    // maps the logical-named read batch onto `required_schema`.
    //
    // Two cases need no schemas: a read with no data columns (e.g. `groupBy(partition).agg(count)`)
    // reads nothing from parquet (`DeltaKernelScanExec::read_all` drives the row count from
    // `record_count`). Otherwise the kernel schemas are required -- their absence is a driver bug,
    // not something to paper over by re-deriving them (which is what the old `physicalise_field`
    // path did, getting nested column mapping wrong).
    // Change Data Feed read: the executor reconstructs TableChanges + execute()s the whole version
    // range itself (single partition), so it ships no per-file tasks and uses no kernel data-column
    // schemas -- bypass the schema-selection below.
    let cdf: Option<(u64, Option<u64>)> = if common.cdf_read {
        Some((common.cdf_start_version, common.cdf_end_version))
    } else {
        None
    };

    let (physical_schema, read_logical_schema, needs_transform): (SchemaRef, SchemaRef, bool) =
        if cdf.is_some() || data_fields.is_empty() {
            let empty: SchemaRef = Arc::new(Schema::empty());
            (Arc::clone(&empty), empty, false)
        } else if !common.kernel_physical_schema.is_empty() {
            let decode = |bytes: &[u8], which: &str| -> Result<SchemaRef, ExecutionError> {
                arrow::ipc::convert::try_schema_from_ipc_buffer(bytes)
                    .map(Arc::new)
                    .map_err(|e| GeneralError(format!("decode kernel {which} schema IPC: {e}")))
            };
            let physical = decode(&common.kernel_physical_schema, "physical")?;
            let logical = decode(&common.kernel_logical_schema, "logical")?;
            // Column mapping is active iff the physical names diverge from the logical names; only
            // then does the transform need to relabel (plain tables read pass-through).
            let needs_transform = physical
                .fields()
                .iter()
                .zip(logical.fields().iter())
                .any(|(p, l)| p.name() != l.name());
            (physical, logical, needs_transform)
        } else {
            return Err(GeneralError(format!(
                "Delta kernel-read scan is missing kernel data-column schemas for {} data \
                 column(s); the driver must ship scan.physical_schema()/logical_schema() \
                 (planDeltaScan / planDeltaReadSchemas)",
                data_fields.len()
            )));
        };

    // Final output = `required_schema` (data ++ partition, in order). The exec injects exactly the
    // partition columns present in the output.
    let output_schema: SchemaRef = Arc::clone(required_schema);
    let partition_output_schema: SchemaRef = Arc::new(Schema::new(partition_fields));

    let object_store_options: HashMap<String, String> = common
        .object_store_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let files: Vec<KernelScanFile> = scan
        .tasks
        .iter()
        .map(|t| KernelScanFile {
            path: t.file_path.clone(),
            size: t.file_size as i64,
            record_count: t.record_count.map(|c| c as i64),
            dv: t.dv.clone(),
            partition_values: t
                .partition_values
                .iter()
                .map(|pv| (pv.name.clone(), pv.value.clone()))
                .collect(),
            transform_json: t.transform_json.clone(),
            base_row_id: t.base_row_id,
            default_row_commit_version: t.default_row_commit_version,
            modification_time: t.modification_time.unwrap_or(0),
            byte_range_start: t.byte_range_start.map(|v| v as i64),
            byte_range_end: t.byte_range_end.map(|v| v as i64),
        })
        .collect();

    let table_root = if scan.table_root.is_empty() {
        common.table_root.clone()
    } else {
        scan.table_root.clone()
    };

    // S3 bucket (URL host) for per-bucket credential resolution; None for non-S3.
    let s3_bucket = url::Url::parse(&table_root)
        .ok()
        .filter(|u| matches!(u.scheme(), "s3" | "s3a"))
        .and_then(|u| u.host_str().map(|h| h.to_string()));
    let storage_config = delta_storage_config_from_map(&object_store_options, s3_bucket.as_deref());

    // Synthetic columns (row_index / is_row_deleted / row_id / row_commit_version) and `_metadata.*`
    // are computed by the existing DeltaSyntheticColumnsExec stacked ON TOP of the kernel read --
    // exactly as the old path stacks it on the parquet read. When wrapped, the kernel exec does NOT
    // drop DV rows (apply_dv = false); the synthetic exec drops them or surfaces `is_row_deleted`,
    // computing row positions against the full physical rows. DeltaKernelScanExec is one partition
    // per file, so the per-partition vectors below align by file/task order.
    // Stage B/C: when the driver set `synthesize_in_worker`, DeltaKernelScanExec produces ALL
    // synthetic columns itself (row_index/row_id from kernel metadata columns in the read;
    // is_row_deleted by inverting the decoded DV; row_commit_version + Spark `_metadata.*` as per-file
    // constants) and assembles the FULL `output_schema` (= required_schema, synthetics included) by
    // name -- so the legacy DeltaSyntheticColumnsExec is NOT stacked on top.
    let synthesize = common.synthesize_in_worker;

    // In-worker synthesis is the only native path (#82): DeltaKernelScanExec produces ALL output
    // columns (data + partitions + row_index/row_id/is_row_deleted/row_commit_version/_metadata.*)
    // by name and applies the DV itself. The legacy DeltaSyntheticColumnsExec is removed; the driver
    // never sets `synthesize_in_worker=false` for a native read (it declines to Spark instead).
    let scan_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        Arc::new(DeltaKernelScanExec::new(
            output_schema,
            physical_schema,
            read_logical_schema,
            needs_transform,
            true, // apply_dv: the kernel scan always applies the DV now
            partition_output_schema,
            common.session_timezone.clone(),
            table_root.clone(),
            storage_config.clone(),
            files,
            synthesize,
            cdf,
        ));

    // Reorder to the user-visible layout when synthetics aren't already a suffix.
    let scan_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if common
        .final_output_indices
        .is_empty()
    {
        scan_exec
    } else {
        let wrapped_schema = scan_exec.schema();
        let n = wrapped_schema.fields().len();
        let projections: Result<ProjectionColumns, ExecutionError> = common
            .final_output_indices
            .iter()
            .map(|idx| {
                if *idx < 0 || (*idx as usize) >= n {
                    return Err(GeneralError(format!(
                        "final_output_indices entry {idx} out of range \
                             (wrapped schema has {n} fields)"
                    )));
                }
                let field = wrapped_schema.field(*idx as usize);
                let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), *idx as usize));
                Ok((col, field.name().clone()))
            })
            .collect();
        Arc::new(
            ProjectionExec::try_new(projections?, scan_exec)
                .map_err(|e| GeneralError(format!("final_output_indices ProjectionExec: {e}")))?,
        )
    };

    Ok((
        vec![],
        vec![],
        Arc::new(SparkPlan::new(spark_plan.plan_id, scan_exec, vec![])),
    ))
}

pub(crate) fn plan_delta_scan(
    // The kernel-read path doesn't need the planner (the old ParquetSource path used it to build
    // pushed-down filter exprs); kept in the signature for the dispatcher call site.
    _planner: &PhysicalPlanner,
    spark_plan: &Operator,
    scan: &DeltaScan,
) -> PlanCreationResult {
    let common = scan
        .common
        .as_ref()
        .ok_or_else(|| GeneralError("DeltaScan missing common data".into()))?;

    let required_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.required_schema.as_slice());

    // Iceberg-style "kernel reads" is the only path: each Delta file is read through
    // `DeltaKernelScanExec` (delta-kernel 0.24 / arrow-58). The legacy ParquetSource + DV-sweep +
    // synthetic-columns stack has been removed. If the driver leaves `kernel_read` false (the user
    // set `spark.comet.delta.kernelRead.enabled=false`), there is nothing to fall back to natively,
    // so we error and let Comet fall back to vanilla Spark for this scan.
    if !common.kernel_read {
        return Err(GeneralError(
            "DeltaScan: the legacy read path was removed; kernel-read is required \
             (spark.comet.delta.kernelRead.enabled is on by default)"
                .into(),
        ));
    }
    plan_delta_kernel_scan(spark_plan, scan, common, &required_schema)
}
