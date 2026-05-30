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

use comet_contrib_delta::planner::{build_delta_partitioned_files, ColumnMappingFilterRewriter};
use comet_contrib_delta::{DeltaDvFilterExec, IgnoreMissingFileSource};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::{DeltaScan, Operator};

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::convert_spark_types_to_arrow_schema;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::execution::spark_plan::SparkPlan;
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::prepare_object_store_with_configs;

pub(crate) fn plan_delta_scan(
    planner: &PhysicalPlanner,
    spark_plan: &Operator,
    scan: &DeltaScan,
) -> PlanCreationResult {
    let common = scan
        .common
        .as_ref()
        .ok_or_else(|| GeneralError("DeltaScan missing common data".into()))?;

    let required_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
    let mut data_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.data_schema.as_slice());
    let partition_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.partition_schema.as_slice());

    // Column mapping: substitute physical names into data_schema so ParquetSource
    // projects by the names actually in the file. A rename projection on top maps
    // physical names back to the logical names upstream operators expect.
    let logical_to_physical: HashMap<String, String> = common
        .column_mappings
        .iter()
        .map(|cm| (cm.logical_name.clone(), cm.physical_name.clone()))
        .collect();
    let has_column_mapping = !logical_to_physical.is_empty();
    if has_column_mapping {
        let new_fields: Vec<_> = data_schema
            .fields()
            .iter()
            .map(|f| {
                if let Some(physical) = logical_to_physical.get(f.name()) {
                    Arc::new(Field::new(physical, f.data_type().clone(), f.is_nullable()))
                } else {
                    Arc::clone(f)
                }
            })
            .collect();
        data_schema = Arc::new(Schema::new(new_fields));
    }
    let projection_vector: Vec<usize> = common
        .projection_vector
        .iter()
        .map(|offset| *offset as usize)
        .collect();

    // Empty-partition fast path.
    if scan.tasks.is_empty() {
        return Ok((
            vec![],
            vec![],
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                Arc::new(EmptyExec::new(required_schema)),
                vec![],
            )),
        ));
    }

    // Build pushed-down data filters, rewriting Column refs to physical names when
    // column mapping is active.
    let data_filters: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = common
        .data_filters
        .iter()
        .map(|expr| {
            let filter = planner
                .create_expr(expr, Arc::clone(&required_schema))
                .map_err(|e| GeneralError(format!("DeltaScan filter: {e}")))?;
            if has_column_mapping {
                let mut rewriter = ColumnMappingFilterRewriter {
                    logical_to_physical: &logical_to_physical,
                    data_schema: &data_schema,
                };
                filter
                    .rewrite(&mut rewriter)
                    .data()
                    .map_err(|e| GeneralError(format!("ColumnMappingFilterRewriter: {e}")))
            } else {
                Ok(filter)
            }
        })
        .collect();

    let object_store_options: HashMap<String, String> = common
        .object_store_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Build PartitionedFiles. Kernel has already resolved each file path to an
    // absolute URL on the driver, so we thread them straight through. Delta stores
    // TIMESTAMP partition values in the JVM default TZ; pass the session TZ so
    // partition-value parsing produces the correct instant.
    let files = build_delta_partitioned_files(
        &scan.tasks,
        partition_schema.as_ref(),
        common.session_timezone.as_str(),
    )
    .map_err(GeneralError)?;

    // Split files by DV presence -- each DV'd file becomes its own FileGroup so the
    // DeltaDvFilterExec's per-partition mapping is 1:1 with one physical parquet
    // file. All non-DV files go in a single combined group.
    //
    // EXCEPT when ANY synthetic column is emitted: the per-partition row offset
    // counter in DeltaSyntheticColumnsExec doesn't reset across files within a
    // FileGroup, and every synthetic we emit depends on per-file row position
    // (row_index is per-file by definition; is_row_deleted uses a per-file DV;
    // row_id = baseRowId + physical_row_index is per-file; row_commit_version is
    // per-file constant). So when ANY emit is on, give each file its own group
    // regardless of DV presence so the per-file lookup is well-defined.
    // When metadata columns are requested they're per-file constants too, so
    // need_per_file_groups must include that case to keep partition-index =
    // file-index alignment in DeltaSyntheticColumnsExec.
    let need_per_file_groups = common.emit_row_index
        || common.emit_is_row_deleted
        || common.emit_row_id
        || common.emit_row_commit_version
        || !common.metadata_column_names.is_empty();
    let mut file_groups: Vec<Vec<PartitionedFile>> = Vec::new();
    // Per-group DV descriptor (None = no DV). Lazily decoded on the executor
    // by DeltaDvFilterExec / DeltaSyntheticColumnsExec via dv_reader::read_dv_indexes
    // -- this is the per-file/per-group analog of Iceberg's delete_files_idx.
    let mut dv_descriptors_per_group: Vec<Option<comet_contrib_delta::proto::DeltaDvDescriptor>> =
        Vec::new();
    let mut base_row_ids_per_group: Vec<Option<i64>> = Vec::new();
    let mut default_commit_versions_per_group: Vec<Option<i64>> = Vec::new();
    let mut task_metadata_per_group: Vec<comet_contrib_delta::synthetic_columns::TaskMetadata> =
        Vec::new();
    let mut non_dv_files: Vec<PartitionedFile> = Vec::new();
    for (file, task) in files.into_iter().zip(scan.tasks.iter()) {
        if task.dv.is_some() || need_per_file_groups {
            file_groups.push(vec![file]);
            dv_descriptors_per_group.push(task.dv.clone());
            base_row_ids_per_group.push(task.base_row_id);
            default_commit_versions_per_group.push(task.default_row_commit_version);
            task_metadata_per_group.push(comet_contrib_delta::synthetic_columns::TaskMetadata {
                file_path: Some(task.file_path.clone()),
                file_size: Some(task.file_size as i64),
                byte_range_start: task.byte_range_start.map(|v| v as i64),
                byte_range_end: task.byte_range_end.map(|v| v as i64),
                modification_time_millis: task.modification_time,
                base_row_id: task.base_row_id,
                default_row_commit_version: task.default_row_commit_version,
            });
        } else {
            non_dv_files.push(file);
        }
    }
    if !non_dv_files.is_empty() {
        file_groups.push(non_dv_files);
        dv_descriptors_per_group.push(None);
        base_row_ids_per_group.push(None);
        default_commit_versions_per_group.push(None);
        task_metadata_per_group
            .push(comet_contrib_delta::synthetic_columns::TaskMetadata::default());
    }

    // Pick any one file to register the object store (they all share the same root).
    let one_file = scan
        .tasks
        .first()
        .map(|t| t.file_path.clone())
        .ok_or_else(|| GeneralError("DeltaScan has no tasks after split-mode injection".into()))?;
    let url = url::Url::parse(&one_file)
        .map_err(|e| GeneralError(format!("DeltaScan invalid file URL: {e}")))?;
    let (object_store_url, _root_path) = prepare_object_store_with_configs(
        planner.session_ctx().runtime_env(),
        url.to_string(),
        &object_store_options,
    )
    .map_err(|e| GeneralError(format!("prepare_object_store_with_configs: {e}")))?;

    // Filter pushdown is incompatible with any synthetic that is derived from a
    // row's PHYSICAL position in the file. DeltaSyntheticColumnsStream uses a
    // running `current_row_offset` to compute:
    //   - `is_row_deleted`  (membership in `deleted_row_indexes`),
    //   - `row_index`       (the offset itself), and
    //   - `row_id`          (base_row_id + offset, when unmaterialised).
    // All three assume the parquet reader returns EVERY row in physical order.
    // With data-filter pushdown the reader skips non-matching rows, decoupling
    // `current_row_offset` from the true parquet row_index, so these synthetics
    // are computed against the wrong positions (e.g. `id >= 50` pushed down
    // yielded row_id 0..49 for ids 50..99). Suppress data-filter pushdown
    // whenever one of them is emitted; Spark's outer Filter still applies the
    // predicates correctly, just without parquet pruning. `row_commit_version`
    // is a per-file constant (not position-derived) and partition filters prune
    // file groups before this point, so neither is affected.
    let suppress_pushdown =
        common.emit_is_row_deleted || common.emit_row_index || common.emit_row_id;
    let data_filters_for_parquet = if suppress_pushdown {
        Vec::new()
    } else {
        data_filters?
    };
    let delta_exec = init_datasource_exec(
        Arc::clone(&required_schema),
        Some(data_schema),
        Some(partition_schema),
        object_store_url,
        file_groups,
        Some(projection_vector),
        Some(data_filters_for_parquet),
        None, // default_values
        common.session_timezone.as_str(),
        common.case_sensitive,
        false, // return_null_struct_if_all_fields_missing
        false, // allow_type_promotion (Delta resolves an exact schema; no implicit widening)
        planner.session_ctx(),
        false, // encryption_enabled (Delta tables we natively support are unencrypted)
        common.use_field_id,
        false, // ignore_missing_field_id
    )?;

    // Honour Spark's `spark.sql.files.ignoreMissingFiles` by wrapping the scan's
    // FileSource so its FileOpener swallows object-store NotFound errors as empty
    // streams. Done here (not in core's init_datasource_exec) so the decorator
    // lives entirely in the Delta contrib -- core carries no Delta-specific knob.
    let delta_exec = if common.ignore_missing_files {
        let fsc = delta_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .ok_or_else(|| {
                GeneralError("DeltaScan: expected FileScanConfig from init_datasource_exec".into())
            })?;
        let wrapped = IgnoreMissingFileSource::new(Arc::clone(fsc.file_source()));
        let rebuilt = FileScanConfigBuilder::from(fsc.clone())
            .with_source(wrapped)
            .build();
        DataSourceExec::from_data_source(rebuilt)
    } else {
        delta_exec
    };

    // Three mutually-exclusive wrap modes based on what the surrounding plan asks
    // for:
    //  - Delta synthetic columns requested (row_index and/or is_row_deleted): wrap
    //    with DeltaSyntheticColumnsExec which keeps all rows and APPENDS the
    //    columns. The outer Delta plan (typically UPDATE/DELETE/MERGE) decides
    //    what to do with the deletion flag.
    //  - DV present and no synthetics: wrap with DeltaDvFilterExec which DROPS
    //    deleted rows inline (standard read path).
    //  - Neither: pass through (avoids per-batch overhead).
    let need_synthetics = common.emit_row_index
        || common.emit_is_row_deleted
        || common.emit_row_id
        || common.emit_row_commit_version
        || !common.metadata_column_names.is_empty();

    // Column-mapping rename has to happen BEFORE synthetic emission so that the
    // synthetic exec sees logical column names in its input schema (matching what
    // its build_output_schema expects) and so that the (stripped) `required_schema`
    // we use here for the rename match isn't compared against a schema that already
    // has synthetics appended. Synthetic columns have FIXED names
    // (`__delta_internal_*`, `row_id`, `row_commit_version`) and aren't subject to
    // CM-name physical renames -- so it's correct to apply the rename to the
    // parquet output BEFORE the append.
    let delta_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> = delta_exec;
    let scan_out = delta_exec.schema();
    let needs_rename = has_column_mapping
        && required_schema.fields().len() == scan_out.fields().len()
        && required_schema
            .fields()
            .iter()
            .zip(scan_out.fields().iter())
            .any(|(req, phys)| req.name() != phys.name());
    let after_rename: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if needs_rename {
        let phys_to_logical: HashMap<&str, &str> = scan_out
            .fields()
            .iter()
            .zip(required_schema.fields().iter())
            .map(|(phys, req)| (phys.name().as_str(), req.name().as_str()))
            .collect();
        let projections: Vec<(Arc<dyn PhysicalExpr>, String)> = scan_out
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, phys_field)| {
                let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(phys_field.name(), idx));
                let alias = phys_to_logical
                    .get(phys_field.name().as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| phys_field.name().clone());
                (col, alias)
            })
            .collect();
        Arc::new(
            ProjectionExec::try_new(projections, delta_exec)
                .map_err(|e| GeneralError(format!("rename ProjectionExec: {e}")))?,
        )
    } else {
        delta_exec
    };

    // After CM-name rename: apply synthetic emission and/or DV filter.
    //   - When synthetics are emitted: chain `synthetic` first (so row_index is
    //     populated with original-file offsets) then DV filter (which uses its own
    //     row counter to drop deleted rows; emitted columns ride along).
    //   - When `emit_is_row_deleted` is on, the upstream (UPDATE/DELETE/MERGE
    //     writer) consumes the flag itself; DON'T filter here -- the writer needs
    //     to see every row to decide what to do.
    //   - When only DV filtering is needed (no synthetic emission): use
    //     `DeltaDvFilterExec` directly.
    let has_dv = dv_descriptors_per_group.iter().any(Option::is_some);
    // Resolve the table-root URL once (kernel needs trailing slash for DV path joins).
    // scan.table_root is the authoritative driver-supplied value; fall back to deriving
    // from the first file's URL only on legacy/empty input to keep older proto payloads
    // working through a single build cycle. After cutover the fallback can drop.
    let table_root_for_dv = if has_dv {
        let raw = if !scan.table_root.is_empty() {
            scan.table_root.clone()
        } else {
            scan.tasks
                .first()
                .map(|t| t.file_path.clone())
                .unwrap_or_default()
        };
        Some(
            comet_contrib_delta::dv_reader::normalize_table_root(&raw)
                .map_err(|e| GeneralError(format!("DeltaScan table_root: {e}")))?,
        )
    } else {
        None
    };
    let after_synthetics: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if need_synthetics {
        let row_index_alias = if common.row_index_column_alias.is_empty() {
            comet_contrib_delta::synthetic_columns::ROW_INDEX_COLUMN_NAME
        } else {
            common.row_index_column_alias.as_str()
        };
        // SyntheticColumnsExec only consults the DV when emit_is_row_deleted is on;
        // otherwise pass `None`s so it never decodes (matches the pre-refactor behaviour
        // where callers shipped `vec![Vec::new(); n]` for the synthetic-only path).
        let synthetic_dvs: Vec<Option<comet_contrib_delta::proto::DeltaDvDescriptor>> =
            if common.emit_is_row_deleted {
                dv_descriptors_per_group.clone()
            } else {
                vec![None; dv_descriptors_per_group.len()]
            };
        // table_root for synthetics: only meaningful when emit_is_row_deleted; use the
        // resolved one if we computed it, else a placeholder (never consulted).
        let synth_root = table_root_for_dv
            .clone()
            .unwrap_or_else(|| url::Url::parse("file:///").expect("static URL"));
        let synth: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
            comet_contrib_delta::synthetic_columns::DeltaSyntheticColumnsExec::new(
                after_rename,
                synthetic_dvs,
                synth_root,
                base_row_ids_per_group,
                default_commit_versions_per_group,
                common.emit_row_index,
                common.emit_is_row_deleted,
                common.emit_row_id,
                common.emit_row_commit_version,
                row_index_alias,
                common.metadata_column_names.clone(),
                task_metadata_per_group,
            )
            .map_err(|e| GeneralError(format!("DeltaSyntheticColumnsExec: {e}")))?,
        );
        // Apply DV filter on top of synthetic emission, EXCEPT when the upstream
        // is consuming is_row_deleted -- then it needs every row.
        if has_dv && !common.emit_is_row_deleted {
            let root = table_root_for_dv
                .clone()
                .expect("table_root_for_dv set when has_dv");
            Arc::new(
                DeltaDvFilterExec::new(synth, dv_descriptors_per_group, root)
                    .map_err(|e| GeneralError(format!("DeltaDvFilterExec: {e}")))?,
            )
        } else {
            synth
        }
    } else if has_dv {
        let root = table_root_for_dv.expect("table_root_for_dv set when has_dv");
        Arc::new(
            DeltaDvFilterExec::new(after_rename, dv_descriptors_per_group, root)
                .map_err(|e| GeneralError(format!("DeltaDvFilterExec: {e}")))?,
        )
    } else {
        after_rename
    };

    // If synthetic columns aren't a suffix of the user-visible required_schema,
    // `final_output_indices` is set and we project to reorder. Each entry is an
    // index into the wrapped exec's output schema (parquet columns first, then
    // appended synthetics in the canonical row_index/is_row_deleted/row_id/
    // row_commit_version order). Empty => already in the right order.
    let with_rename: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        if !common.final_output_indices.is_empty() {
            let wrapped_schema = after_synthetics.schema();
            let projections: Vec<(Arc<dyn PhysicalExpr>, String)> = common
                .final_output_indices
                .iter()
                .map(|idx| {
                    let i = *idx as usize;
                    let field = wrapped_schema.field(i);
                    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), i));
                    (col, field.name().clone())
                })
                .collect();
            Arc::new(
                ProjectionExec::try_new(projections, after_synthetics)
                    .map_err(|e| GeneralError(format!("final reorder ProjectionExec: {e}")))?,
            )
        } else {
            after_synthetics
        };

    Ok((
        vec![],
        vec![],
        Arc::new(SparkPlan::new(spark_plan.plan_id, with_rename, vec![])),
    ))
}
