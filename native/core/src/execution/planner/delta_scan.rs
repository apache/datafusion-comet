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

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::{
    DeltaColumnMapping, DeltaScan, DeltaScanCommon, Operator,
};

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::convert_spark_types_to_arrow_schema;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::execution::spark_plan::SparkPlan;

/// Recursively rename a logical arrow field to its physical form using a Delta column-mapping tree
/// (`DeltaColumnMapping` with recursive `children`). Delta only renames struct fields, so structs are
/// matched by logical name and renamed; arrays and maps are descended through transparently. When
/// `rename_top` is false the field keeps its own (top-level) name and only its nested struct fields
/// are physicalised -- the shape the old read path's downstream expects (top-level rename happens via
/// its rename ProjectionExec). When true the field is physicalised at every level -- the kernel-read
/// `physical_schema`.
fn physicalise_field(
    field: &Arc<Field>,
    mapping: Option<&DeltaColumnMapping>,
    rename_top: bool,
) -> Arc<Field> {
    let name = match (rename_top, mapping) {
        (true, Some(m)) if !m.physical_name.is_empty() => m.physical_name.clone(),
        _ => field.name().clone(),
    };
    let children = mapping.map(|m| m.children.as_slice()).unwrap_or(&[]);
    let dt = physicalise_type(field.data_type(), children);
    Arc::new(Field::new(name, dt, field.is_nullable()).with_metadata(field.metadata().clone()))
}

/// Physicalise a data type's nested struct field names using the column-mapping children. Structs
/// match children by logical name; lists/maps descend transparently (the element / key / value
/// wrapper field names are never renamed -- Delta column-maps only struct fields).
fn physicalise_type(dt: &DataType, child_mappings: &[DeltaColumnMapping]) -> DataType {
    use datafusion::arrow::datatypes::Fields;
    match dt {
        DataType::Struct(fields) => {
            let by_logical: HashMap<&str, &DeltaColumnMapping> = child_mappings
                .iter()
                .map(|m| (m.logical_name.as_str(), m))
                .collect();
            let new: Vec<Arc<Field>> = fields
                .iter()
                .map(|f| physicalise_field(f, by_logical.get(f.name().as_str()).copied(), true))
                .collect();
            DataType::Struct(Fields::from(new))
        }
        DataType::List(elem) => DataType::List(descend_elem(elem, child_mappings)),
        DataType::LargeList(elem) => DataType::LargeList(descend_elem(elem, child_mappings)),
        DataType::Map(entry, sorted) => {
            // entry is struct<key, value>; pass the child mappings down through the key/value wrapper
            // (matched by name inside whichever is a struct) without renaming the wrapper fields.
            if let DataType::Struct(kv) = entry.data_type() {
                let new_kv: Vec<Arc<Field>> =
                    kv.iter().map(|f| descend_elem(f, child_mappings)).collect();
                let new_entry = Field::new(
                    entry.name(),
                    DataType::Struct(Fields::from(new_kv)),
                    entry.is_nullable(),
                )
                .with_metadata(entry.metadata().clone());
                DataType::Map(Arc::new(new_entry), *sorted)
            } else {
                dt.clone()
            }
        }
        _ => dt.clone(),
    }
}

/// A list element / map key-or-value: keep the wrapper field's own name, physicalise its nested type.
fn descend_elem(elem: &Arc<Field>, child_mappings: &[DeltaColumnMapping]) -> Arc<Field> {
    Arc::new(
        Field::new(
            elem.name(),
            physicalise_type(elem.data_type(), child_mappings),
            elem.is_nullable(),
        )
        .with_metadata(elem.metadata().clone()),
    )
}

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

    // A read with no data columns (e.g. `groupBy(partition).agg(count("*"))`) reads nothing from
    // parquet; `DeltaKernelScanExec::read_all` drives the per-file row count from `record_count`
    // (the parquet footer as a fallback) and emits empty-data batches of that length.

    // Data columns are read from parquet. `read_logical_schema` is the pure-logical schema the
    // read produces after relabeling (the kernel transform's output); `physical_schema` is the same
    // columns with physical names at EVERY nesting level, derived from the recursive column-mapping
    // tree. The kernel-read primitive reads by physical name, then an identity transform relabels
    // physical -> logical (recursively, including nested struct fields -- proven by the
    // `spike_nested_struct_*` tests).
    let mapping_by_name: HashMap<&str, &DeltaColumnMapping> = common
        .column_mappings
        .iter()
        .map(|cm| (cm.logical_name.as_str(), cm))
        .collect();
    let read_logical_schema: SchemaRef = Arc::new(Schema::new(data_fields.clone()));
    let physical_schema: SchemaRef = if mapping_by_name.is_empty() {
        Arc::clone(&read_logical_schema)
    } else {
        let fields: Vec<_> = data_fields
            .iter()
            .map(|f| physicalise_field(f, mapping_by_name.get(f.name().as_str()).copied(), true))
            .collect();
        Arc::new(Schema::new(fields))
    };
    let needs_transform = !mapping_by_name.is_empty();

    // Final output = `required_schema` (data ++ partition, in order). The exec injects exactly the
    // partition columns present in the output.
    let output_schema: SchemaRef = Arc::clone(required_schema);
    let partition_output_schema: SchemaRef = Arc::new(Schema::new(partition_fields));

    let object_store_options: HashMap<String, String> = common
        .object_store_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let storage_config = delta_storage_config_from_map(&object_store_options);

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
        })
        .collect();

    let table_root = if scan.table_root.is_empty() {
        common.table_root.clone()
    } else {
        scan.table_root.clone()
    };

    // Synthetic columns (row_index / is_row_deleted / row_id / row_commit_version) and `_metadata.*`
    // are computed by the existing DeltaSyntheticColumnsExec stacked ON TOP of the kernel read --
    // exactly as the old path stacks it on the parquet read. When wrapped, the kernel exec does NOT
    // drop DV rows (apply_dv = false); the synthetic exec drops them or surfaces `is_row_deleted`,
    // computing row positions against the full physical rows. DeltaKernelScanExec is one partition
    // per file, so the per-partition vectors below align by file/task order.
    let need_synthetics = common.emit_row_index
        || common.emit_is_row_deleted
        || common.emit_row_id
        || common.emit_row_commit_version
        || !common.metadata_column_names.is_empty();

    let kernel_exec = Arc::new(DeltaKernelScanExec::new(
        output_schema,
        physical_schema,
        read_logical_schema,
        needs_transform,
        !need_synthetics, // apply_dv here only when the synthetic exec isn't doing it
        partition_output_schema,
        common.session_timezone.clone(),
        table_root.clone(),
        storage_config,
        files,
    ));

    let scan_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if need_synthetics {
        use comet_contrib_delta::synthetic_columns::{
            DeltaSyntheticColumnsExec, TaskMetadata, ROW_INDEX_COLUMN_NAME,
        };
        let has_dv = scan.tasks.iter().any(|t| t.dv.is_some());
        let drop_deleted = has_dv && !common.emit_is_row_deleted;
        // Only pass real DV descriptors when the exec flags or drops; else `None`s (never decode).
        let dvs_for_exec: Vec<Option<comet_contrib_delta::proto::DeltaDvDescriptor>> =
            if common.emit_is_row_deleted || drop_deleted {
                scan.tasks.iter().map(|t| t.dv.clone()).collect()
            } else {
                vec![None; scan.tasks.len()]
            };
        let base_row_ids: Vec<Option<i64>> = scan.tasks.iter().map(|t| t.base_row_id).collect();
        let default_commit_versions: Vec<Option<i64>> = scan
            .tasks
            .iter()
            .map(|t| t.default_row_commit_version)
            .collect();
        let task_metadata: Vec<TaskMetadata> = scan
            .tasks
            .iter()
            .map(|t| TaskMetadata {
                file_path: Some(t.file_path.clone()),
                file_size: Some(t.file_size as i64),
                byte_range_start: t.byte_range_start.map(|v| v as i64),
                byte_range_end: t.byte_range_end.map(|v| v as i64),
                modification_time_millis: t.modification_time,
                base_row_id: t.base_row_id,
                default_row_commit_version: t.default_row_commit_version,
            })
            .collect();
        let row_index_alias = if common.row_index_column_alias.is_empty() {
            ROW_INDEX_COLUMN_NAME
        } else {
            common.row_index_column_alias.as_str()
        };
        let synth_root = comet_contrib_delta::dv_reader::normalize_table_root(&table_root)
            .map_err(|e| GeneralError(format!("DeltaScan table_root: {e}")))?;
        Arc::new(
            DeltaSyntheticColumnsExec::new(
                kernel_exec,
                dvs_for_exec,
                synth_root,
                base_row_ids,
                default_commit_versions,
                common.emit_row_index,
                common.emit_is_row_deleted,
                common.emit_row_id,
                common.emit_row_commit_version,
                drop_deleted,
                row_index_alias,
                common.metadata_column_names.clone(),
                task_metadata,
            )
            .map_err(|e| GeneralError(format!("DeltaSyntheticColumnsExec: {e}")))?,
        )
    } else {
        kernel_exec
    };

    // Reorder to the user-visible layout when synthetics aren't already a suffix.
    let scan_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        if common.final_output_indices.is_empty() {
            scan_exec
        } else {
            let wrapped_schema = scan_exec.schema();
            let n = wrapped_schema.fields().len();
            let projections: Result<Vec<(Arc<dyn PhysicalExpr>, String)>, ExecutionError> = common
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
                    let col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(field.name(), *idx as usize));
                    Ok((col, field.name().clone()))
                })
                .collect();
            Arc::new(
                ProjectionExec::try_new(projections?, scan_exec).map_err(|e| {
                    GeneralError(format!("final_output_indices ProjectionExec: {e}"))
                })?,
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
