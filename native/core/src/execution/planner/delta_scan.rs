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
use comet_contrib_delta::IgnoreMissingFileSource;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::{DeltaScan, DeltaScanCommon, Operator};

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::convert_spark_types_to_arrow_schema;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::execution::spark_plan::SparkPlan;
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::prepare_object_store_with_configs;

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

    let has_nested = required_schema.fields().iter().any(|f| {
        matches!(
            f.data_type(),
            DataType::Struct(_)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Map(_, _)
        )
    });
    if !common.partition_schema.is_empty()
        || common.emit_row_index
        || common.emit_is_row_deleted
        || common.emit_row_id
        || common.emit_row_commit_version
        || !common.metadata_column_names.is_empty()
        || common.use_field_id
        || has_nested
    {
        return Err(GeneralError(
            "DeltaScan.kernel_read supports plain + top-level name-mode column-mapped tables only \
             (no partitions, row-tracking, metadata columns, id-mode mapping, or nested types yet)"
                .into(),
        ));
    }

    // Column mapping: read each file by its PHYSICAL column name, then relabel to logical via the
    // identity transform in DeltaKernelScanExec. `required_schema` already carries logical names;
    // derive the physical read schema by renaming each top-level field to its physical name.
    let logical_to_physical: HashMap<String, String> = common
        .column_mappings
        .iter()
        .map(|cm| (cm.logical_name.clone(), cm.physical_name.clone()))
        .collect();
    let physical_schema: SchemaRef = if logical_to_physical.is_empty() {
        Arc::clone(required_schema)
    } else {
        let fields: Vec<_> = required_schema
            .fields()
            .iter()
            .map(|f| match logical_to_physical.get(f.name()) {
                Some(physical) => {
                    Arc::new(Field::new(physical, f.data_type().clone(), f.is_nullable()))
                }
                None => Arc::clone(f),
            })
            .collect();
        Arc::new(Schema::new(fields))
    };
    let needs_transform = !logical_to_physical.is_empty();

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
        })
        .collect();

    let table_root = if scan.table_root.is_empty() {
        common.table_root.clone()
    } else {
        scan.table_root.clone()
    };

    let exec = Arc::new(DeltaKernelScanExec::new(
        Arc::clone(required_schema),
        physical_schema,
        needs_transform,
        table_root,
        storage_config,
        files,
    ));
    Ok((
        vec![],
        vec![],
        Arc::new(SparkPlan::new(spark_plan.plan_id, exec, vec![])),
    ))
}

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

    // Iceberg-style "kernel reads" path (Phase 1b). When the driver sets `kernel_read`, route to
    // `DeltaKernelScanExec` -- kernel reads each file + applies the DV, and the result is bridged
    // into this arrow plan -- instead of the ParquetSource + DV-sweep + synthetic-columns stack
    // below. Old path stays the default.
    if common.kernel_read {
        return plan_delta_kernel_scan(spark_plan, scan, common, &required_schema);
    }

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

    // Empty-partition fast path. The EmptyExec must carry the SAME output schema a populated
    // partition produces (synthetic columns appended + reordered), NOT the stripped
    // `required_schema`. Otherwise, when DPP prunes all tasks from one partition slot while
    // sibling slots retain data (buildPerPartitionBytes keeps the empty group to hold
    // numPartitions stable), this partition would emit N-k columns while siblings emit N --
    // a per-partition schema divergence within one RDD for any synthetic-emitting Delta scan.
    if scan.tasks.is_empty() {
        let row_index_alias = if common.row_index_column_alias.is_empty() {
            comet_contrib_delta::synthetic_columns::ROW_INDEX_COLUMN_NAME
        } else {
            common.row_index_column_alias.as_str()
        };
        let output_schema = empty_scan_output_schema(
            &required_schema,
            common.emit_row_index,
            common.emit_is_row_deleted,
            common.emit_row_id,
            common.emit_row_commit_version,
            row_index_alias,
            &common.metadata_column_names,
            &common.final_output_indices,
        )?;
        return Ok((
            vec![],
            vec![],
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                Arc::new(EmptyExec::new(output_schema)),
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
    // DV-sweep exec's per-partition mapping is 1:1 with one physical parquet
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
    // by DeltaSyntheticColumnsExec via dv_reader::read_dv_indexes
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
        // allow_type_promotion: Delta's TypeWidening feature leaves pre-widening files in their
        // original (narrower) physical parquet type and relies on read-time promotion to the
        // table's current widened type (e.g. INT32->INT64, FLOAT->DOUBLE, INT32->DOUBLE). Delta
        // only ever permits *widening* conversions, so the promotion is always lossless and safe.
        // It's also inert outside the widening case: a non-widened Delta table never presents a
        // physical type narrower than its logical type, so the flag only ever engages on a genuine
        // type-widening read -- enabling it unconditionally for Delta scans changes nothing else.
        // With this false, the schema adapter rejects exactly those three pairs, failing
        // TypeWidening{...}Suite's non-partitioned data-column reads (partition columns are read
        // from metadata strings via parse_delta_partition_scalar, so they were unaffected).
        true,
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

    // What to wrap the parquet scan with, based on what the surrounding plan asks for.
    // One DeltaSyntheticColumnsExec (the unified DV-sweep exec) covers every case:
    //  - synthetic columns requested (row_index / is_row_deleted / row_id / metadata):
    //    it APPENDS the columns; the outer Delta plan (UPDATE/DELETE/MERGE) decides what
    //    to do with the deletion flag.
    //  - DV present whose deletions aren't surfaced upward: it DROPS deleted rows inline
    //    (standard read path; the same exec with drop_deleted = true).
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

    // After CM-name rename: build the unified DV-sweep exec (below). Synthetic columns
    // are computed on physical row positions first, then -- when the DV's deletions are
    // not being surfaced via is_row_deleted -- the deleted rows are dropped in the same
    // sweep. When `emit_is_row_deleted` is on, the upstream UPDATE/DELETE/MERGE writer
    // consumes the flag, so rows are NOT dropped here (it must see every row).
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
    let after_synthetics: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        if need_synthetics || has_dv {
            // Unified DV-sweep exec (absorbs the former DeltaDvFilterExec). It appends any
            // requested synthetic columns AND, when there's a DV whose deletions are not
            // being surfaced upward (`!emit_is_row_deleted`), drops the deleted rows in the
            // same running-offset sweep. The three former wirings collapse here:
            //   - synthetics + DV + !is_row_deleted -> emit flags + drop (was synthetic
            //     emission with a DeltaDvFilterExec stacked on top)
            //   - synthetics + (is_row_deleted or no DV) -> emit flags, no drop
            //   - no synthetics + DV -> no emit flags + drop (was DeltaDvFilterExec alone)
            // Synthetic columns are computed on physical positions before the drop, so a
            // surviving row keeps the same row_index / row_id it had when the two execs
            // were stacked.
            let drop_deleted = has_dv && !common.emit_is_row_deleted;
            let row_index_alias = if common.row_index_column_alias.is_empty() {
                comet_contrib_delta::synthetic_columns::ROW_INDEX_COLUMN_NAME
            } else {
                common.row_index_column_alias.as_str()
            };
            // The exec needs the real DV descriptors whenever it flags
            // (`emit_is_row_deleted`) or drops; otherwise pass `None`s so it never decodes.
            let dvs_for_exec: Vec<Option<comet_contrib_delta::proto::DeltaDvDescriptor>> =
                if common.emit_is_row_deleted || drop_deleted {
                    dv_descriptors_per_group.clone()
                } else {
                    vec![None; dv_descriptors_per_group.len()]
                };
            // table_root is only consulted when the exec actually decodes a DV; use the
            // resolved one if we computed it, else a never-consulted placeholder.
            let synth_root = table_root_for_dv
                .unwrap_or_else(|| url::Url::parse("file:///").expect("static URL"));
            Arc::new(
                comet_contrib_delta::synthetic_columns::DeltaSyntheticColumnsExec::new(
                    after_rename,
                    dvs_for_exec,
                    synth_root,
                    base_row_ids_per_group,
                    default_commit_versions_per_group,
                    common.emit_row_index,
                    common.emit_is_row_deleted,
                    common.emit_row_id,
                    common.emit_row_commit_version,
                    drop_deleted,
                    row_index_alias,
                    common.metadata_column_names.clone(),
                    task_metadata_per_group,
                )
                .map_err(|e| GeneralError(format!("DeltaSyntheticColumnsExec: {e}")))?,
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
            let n = wrapped_schema.fields().len();
            let projections: Vec<(Arc<dyn PhysicalExpr>, String)> = common
                .final_output_indices
                .iter()
                .map(|idx| {
                    // `final_output_indices` is wire data (int32); a negative or
                    // out-of-range value would otherwise panic in `Schema::field`.
                    if *idx < 0 || (*idx as usize) >= n {
                        return Err(GeneralError(format!(
                            "final_output_indices entry {idx} out of range \
                             (wrapped schema has {n} fields)"
                        )));
                    }
                    let i = *idx as usize;
                    let field = wrapped_schema.field(i);
                    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), i));
                    Ok((col, field.name().clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;
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

/// Output schema a Delta scan partition produces: the (logical) `required_schema` with any
/// synthetic columns appended -- via the SAME `build_output_schema` the populated path uses --
/// then reordered by `final_output_indices`. The empty-partition fast path uses this so a
/// DPP-pruned partition emits the same schema as its populated siblings (see the call site).
#[allow(clippy::too_many_arguments)]
fn empty_scan_output_schema(
    required_schema: &SchemaRef,
    emit_row_index: bool,
    emit_is_row_deleted: bool,
    emit_row_id: bool,
    emit_row_commit_version: bool,
    row_index_alias: &str,
    metadata_column_names: &[String],
    final_output_indices: &[i32],
) -> Result<SchemaRef, ExecutionError> {
    let need_synthetics = emit_row_index
        || emit_is_row_deleted
        || emit_row_id
        || emit_row_commit_version
        || !metadata_column_names.is_empty();
    if !need_synthetics {
        // No synthetics -> the populated path emits `required_schema` (no reorder), so the
        // empty partition's stripped schema already matches. (final_output_indices is only
        // set when synthetics break the required-schema suffix ordering.)
        return Ok(Arc::clone(required_schema));
    }
    let post_synth = comet_contrib_delta::synthetic_columns::build_output_schema(
        required_schema,
        emit_row_index,
        emit_is_row_deleted,
        emit_row_id,
        emit_row_commit_version,
        row_index_alias,
        metadata_column_names,
    );
    if final_output_indices.is_empty() {
        return Ok(post_synth);
    }
    let n = post_synth.fields().len();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(final_output_indices.len());
    for idx in final_output_indices {
        if *idx < 0 || (*idx as usize) >= n {
            return Err(GeneralError(format!(
                "final_output_indices entry {idx} out of range \
                 (post-synthesis schema has {n} fields)"
            )));
        }
        fields.push(Arc::clone(&post_synth.fields()[*idx as usize]));
    }
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_contrib_delta::synthetic_columns::ROW_INDEX_COLUMN_NAME;
    use datafusion::arrow::datatypes::DataType;

    fn req(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64, false))
                .collect::<Vec<_>>(),
        ))
    }

    // The empty-partition fast path must emit the synthetic columns a populated partition
    // emits -- not the stripped required schema -- or a DPP-pruned partition slot would
    // diverge from its siblings within one RDD.
    #[test]
    fn empty_scan_schema_appends_synthetics() {
        let schema = empty_scan_output_schema(
            &req(&["id", "name"]),
            true, // emit_row_index
            false,
            false,
            false,
            ROW_INDEX_COLUMN_NAME,
            &[],
            &[],
        )
        .unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "name", ROW_INDEX_COLUMN_NAME],
            "empty-partition schema must append the row_index synthetic"
        );
    }

    #[test]
    fn empty_scan_schema_without_synthetics_is_required() {
        let schema =
            empty_scan_output_schema(&req(&["id", "name"]), false, false, false, false, "x", &[], &[])
                .unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn empty_scan_schema_applies_reorder() {
        // post-synth = [id, row_index]; final_output_indices [1,0] -> [row_index, id].
        let schema = empty_scan_output_schema(
            &req(&["id"]),
            true,
            false,
            false,
            false,
            ROW_INDEX_COLUMN_NAME,
            &[],
            &[1, 0],
        )
        .unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec![ROW_INDEX_COLUMN_NAME, "id"]);
    }

    #[test]
    fn empty_scan_schema_rejects_out_of_range_reorder() {
        let err = empty_scan_output_schema(
            &req(&["id"]),
            true,
            false,
            false,
            false,
            ROW_INDEX_COLUMN_NAME,
            &[],
            &[5],
        );
        assert!(err.is_err(), "out-of-range final_output_indices must error, not panic");
    }
}
