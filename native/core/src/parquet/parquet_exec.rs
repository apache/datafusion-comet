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

use crate::execution::operators::ExecutionError;
use crate::parquet::eager_page_index_reader_factory::EagerPageIndexReaderFactory;
use crate::parquet::encryption_support::{CometEncryptionConfig, ENCRYPTION_FACTORY_ID};
use crate::parquet::name_fold::fold_schema_names;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use arrow::datatypes::{Field, SchemaRef};
use datafusion::config::{ParquetOptions, TableParquetOptions};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_comet_spark_expr::EvalMode;
use datafusion_datasource::TableSchema;
use std::collections::HashMap;
use std::sync::Arc;

/// Initializes a DataSourceExec plan with a ParquetSource for Comet's native Parquet scan.
///
///   `required_schema`: Schema to be projected by the scan.
///
///   `data_schema`: Schema of the underlying data. It is optional and, if provided, is used
/// instead of `required_schema` to initialize the file scan
///
///   `partition_schema` and `partition_fields` are optional. If `partition_schema` is specified,
/// then `partition_fields` must also be specified
///
///   `object_store_url`: Url to read data from
///
///   `file_groups`: A collection of groups of `PartitionedFiles` that are to be read by the scan
///
///   `projection_vector`: A vector of the indexes in the schema of the fields to be projected
///
///   `data_filters`: Any predicate that must be applied to the data returned by the scan. If
/// specified, then `data_schema` must also be specified.
#[allow(clippy::too_many_arguments)]
pub(crate) fn init_datasource_exec(
    required_schema: SchemaRef,
    data_schema: Option<SchemaRef>,
    partition_schema: Option<SchemaRef>,
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<Vec<PartitionedFile>>,
    projection_vector: Option<Vec<usize>>,
    data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    default_values: Option<HashMap<Column, ScalarValue>>,
    session_timezone: &str,
    case_sensitive: bool,
    return_null_struct_if_all_fields_missing: bool,
    allow_type_promotion: bool,
    allow_timestamp_ltz_to_ntz: bool,
    session_ctx: &Arc<SessionContext>,
    encryption_enabled: bool,
    use_field_id: bool,
    ignore_missing_field_id: bool,
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    // Computed once and reused below for `try_pushdown_filters`. `copied_config()` clones only
    // `SessionConfig` (an `Arc<ConfigOptions>` plus a small extensions map); `SessionContext::
    // state()` would clone the whole `SessionState`, including the function registries (~250+
    // UDFs) and optimizer rules, on every scan init.
    let session_config = session_ctx.copied_config();
    let (table_parquet_options, mut spark_parquet_options) = get_options(
        session_timezone,
        case_sensitive,
        return_null_struct_if_all_fields_missing,
        allow_type_promotion,
        allow_timestamp_ltz_to_ntz,
        &object_store_url,
        encryption_enabled,
        &session_config.options().execution.parquet,
    );
    spark_parquet_options.use_field_id = use_field_id;
    spark_parquet_options.ignore_missing_field_id = ignore_missing_field_id;
    // Spark can discard filtered-out values before timestamp conversion using statistics,
    // dictionary, and row-level filters. Comet cannot mirror every pruning path, so applying
    // checked conversion in a filtered scan can fail on values Spark never reads. Preserve the
    // existing safe cast for filtered scans and use checked conversion only when every value is
    // necessarily read.
    spark_parquet_options.checked_timestamp_overflow = !data_filters
        .as_ref()
        .is_some_and(|filters| !filters.is_empty());

    // Determine the schema and projection to use for ParquetSource.
    // When data_schema is provided, use it as the base schema so DataFusion knows the full
    // file schema. Compute a projection vector to select only the required columns.
    let (base_schema, projection) = match (&data_schema, &projection_vector) {
        (Some(schema), Some(proj)) => (Arc::clone(schema), Some(proj.clone())),
        (Some(schema), None) => {
            // Compute projection: map required_schema field names to data_schema indices.
            // This is needed for schema pruning when the data_schema has more columns than
            // the required_schema.
            // Fold the data and required field names once (the same JVM `toLowerCase(Locale.ROOT)`
            // fold the schema adapter uses), then match on the folded names so this plan-time
            // projection stays consistent with the adapter's case-insensitive remap.
            let data_folded = fold_schema_names(schema, case_sensitive);
            let required_folded = fold_schema_names(&required_schema, case_sensitive);
            let projection: Vec<usize> = required_folded
                .iter()
                .filter_map(|req| data_folded.iter().position(|d| d == req))
                .collect();
            // Only use data_schema + projection when all required fields were found by name.
            // When some fields can't be matched (e.g., Parquet field ID mapping where names
            // differ between required and data schemas), fall back to using required_schema
            // directly with no projection.
            if projection.len() == required_schema.fields().len() {
                (Arc::clone(schema), Some(projection))
            } else {
                (Arc::clone(&required_schema), None)
            }
        }
        _ => (Arc::clone(&required_schema), None),
    };
    let partition_fields: Vec<_> = partition_schema
        .iter()
        .flat_map(|s| s.fields().iter())
        .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), f.is_nullable())) as _)
        .collect();
    let table_schema =
        TableSchema::from_file_schema(base_schema).with_table_partition_cols(partition_fields);

    let mut parquet_source = ParquetSource::new(table_schema)
        .with_table_parquet_options(table_parquet_options)
        .with_metadata_size_hint(512 * 1024); // Same as DataFusion's default

    if encryption_enabled {
        parquet_source = parquet_source.with_encryption_factory(
            session_ctx
                .runtime_env()
                .parquet_encryption_factory(ENCRYPTION_FACTORY_ID)?,
        );
    }

    // DataFusion's opener requests `PageIndexPolicy::Skip` on the initial metadata load and
    // defers loading the page index until row-group pruning shows it is still needed
    // (apache/datafusion#22857). That deferred load bypasses `FileMetadataCache` entirely, so on
    // a predicate that never resolves to fully-matched by row-group statistics alone (e.g. `IS
    // NOT NULL` on a column whose row groups don't carry `null_count` stats, as with TPC-DS
    // `store_sales`), the page index is re-fetched, uncached, on every open (comet#3978).
    // `EagerPageIndexReaderFactory` forces the page index to load on the first fetch and be
    // cached with the footer, at the cost of losing the skip's benefit when it would have
    // applied. Filed upstream as apache/datafusion#23978; revert this once that's fixed.
    //
    // TODO: metadata I/O is invisible in metrics. `fetch_metadata` reads via `ObjectStore::get_ranges`,
    // bypassing the `get_bytes` path where `bytes_scanned` is counted. A byte-counting ObjectStore
    // wrapper would surface it.
    let runtime_env = session_ctx.runtime_env();
    let store = runtime_env.object_store(&object_store_url)?;
    let metadata_cache = runtime_env.cache_manager.get_file_metadata_cache();
    parquet_source = parquet_source.with_parquet_file_reader_factory(Arc::new(
        EagerPageIndexReaderFactory::new(store, metadata_cache),
    ));

    // Route data filters through `try_pushdown_filters` rather than calling
    // `with_predicate` directly. This is the contract DataFusion's optimizer
    // uses, and it correctly classifies any filter ParquetSource cannot
    // evaluate as a `RowFilter` (e.g. virtual-column refs like Parquet
    // `row_number`) as `PushedDown::No`. The predicate still flows into
    // `source.predicate` for row-group / page-index / bloom-filter pruning
    // even when `datafusion.execution.parquet.pushdown_filters=false`; that
    // config only gates per-row `RowFilter` evaluation. We discard
    // `propagation.parent_pushdown_result` because Spark's Filter above the
    // scan re-evaluates every dataFilter, so No-classified filters stay
    // correct without us inserting a FilterExec here.
    let file_source: Arc<dyn FileSource> = match data_filters {
        Some(filters) if !filters.is_empty() => {
            let propagation =
                parquet_source.try_pushdown_filters(filters, session_config.options())?;
            // `updated_node` is `None` when every filter classified as `No`
            // (nothing pushable); keep the unmodified source in that case.
            propagation
                .updated_node
                .unwrap_or_else(|| Arc::new(parquet_source))
        }
        _ => Arc::new(parquet_source),
    };

    let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
        SparkPhysicalExprAdapterFactory::new(spark_parquet_options, default_values),
    );

    let file_groups = file_groups
        .iter()
        .map(|files| FileGroup::new(files.clone()))
        .collect();

    let mut file_scan_config_builder = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(file_groups)
        .with_expr_adapter(Some(expr_adapter_factory));

    if let Some(projection) = projection {
        file_scan_config_builder =
            file_scan_config_builder.with_projection_indices(Some(projection))?;
    }

    let file_scan_config = file_scan_config_builder.build();

    let data_source_exec = Arc::new(DataSourceExec::new(Arc::new(file_scan_config)));

    Ok(data_source_exec)
}

#[allow(clippy::too_many_arguments)]
fn get_options(
    session_timezone: &str,
    case_sensitive: bool,
    return_null_struct_if_all_fields_missing: bool,
    allow_type_promotion: bool,
    allow_timestamp_ltz_to_ntz: bool,
    object_store_url: &ObjectStoreUrl,
    encryption_enabled: bool,
    session_parquet_options: &ParquetOptions,
) -> (TableParquetOptions, SparkParquetOptions) {
    let mut table_parquet_options = TableParquetOptions::new();

    // Reader options that reach `ParquetSource`'s actual per-file execution path
    // (`ParquetMorselizer`, built from `table_parquet_options.global`) rather than only
    // `ParquetFormat::infer_schema`'s schema-inference path, which Comet never calls (Comet
    // always supplies its own schema, translated from Spark's catalog). Seeded from the
    // session so `spark.comet.datafusion.execution.parquet.*` (behind
    // `spark.comet.exec.respectDataFusionConfigs`) and `spark.comet.parquet.
    // rowFilterPushdown.enabled` actually take effect on the native scan (#4990); a fresh
    // `TableParquetOptions::new()` ignored the session entirely.
    //
    // Deliberately an allowlist, not `table_parquet_options.global = session_parquet_options.
    // clone()`: a new DataFusion reader option should be silently ignored here until someone
    // checks whether it is safe to pass through for Spark semantics (as `coerce_int96`/
    // `coerce_int96_tz` below are not), not silently active. Recheck this list against
    // `ParquetOptions` whenever the DataFusion dependency version changes.
    table_parquet_options.global.pushdown_filters = session_parquet_options.pushdown_filters;
    table_parquet_options.global.reorder_filters = session_parquet_options.reorder_filters;
    table_parquet_options.global.force_filter_selections =
        session_parquet_options.force_filter_selections;
    table_parquet_options.global.bloom_filter_on_read =
        session_parquet_options.bloom_filter_on_read;
    table_parquet_options.global.max_predicate_cache_size =
        session_parquet_options.max_predicate_cache_size;
    // Only gates whether the page index is used for row-group/page pruning
    // (datafusion's `enable_page_index` check around `page_pruning_predicate`). It does not
    // stop the index from being fetched: `EagerPageIndexReaderFactory` always forces
    // `PageIndexPolicy::Optional` on unencrypted files regardless of the requested policy, so
    // disabling this reduces pruning, not read I/O.
    table_parquet_options.global.enable_page_index = session_parquet_options.enable_page_index;
    table_parquet_options.global.pruning = session_parquet_options.pruning;

    // Hardcoded for Spark compatibility, not session-overridable.
    table_parquet_options.global.coerce_int96 = Some("us".to_string());
    // INT96 columns encode UTC-adjusted instants; attaching the UTC timezone
    // preserves that signal at the Arrow level so the schema adapter can
    // distinguish INT96-derived TimestampLTZ from a true TimestampNTZ source
    // and apply the pre-Spark-4 SPARK-36182 rejection (#4219).
    table_parquet_options.global.coerce_int96_tz = Some("UTC".to_string());
    let mut spark_parquet_options =
        SparkParquetOptions::new(EvalMode::Legacy, session_timezone, false);
    spark_parquet_options.allow_cast_unsigned_ints = true;
    spark_parquet_options.case_sensitive = case_sensitive;
    spark_parquet_options.return_null_struct_if_all_fields_missing =
        return_null_struct_if_all_fields_missing;
    spark_parquet_options.allow_type_promotion = allow_type_promotion;
    spark_parquet_options.allow_timestamp_ltz_to_ntz = allow_timestamp_ltz_to_ntz;

    if encryption_enabled {
        table_parquet_options.crypto.configure_factory(
            ENCRYPTION_FACTORY_ID,
            &CometEncryptionConfig {
                uri_base: object_store_url.to_string(),
            },
        );
    }

    (table_parquet_options, spark_parquet_options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_comet_spark_expr::test_common::file_util::get_temp_filename;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::fs::File;

    // Regression test for #4990: a fresh `TableParquetOptions::new()` ignored session-level
    // `datafusion.execution.parquet.*` settings entirely, so `spark.comet.datafusion.
    // execution.parquet.*` (behind `respectDataFusionConfigs`) and `spark.comet.parquet.
    // rowFilterPushdown.enabled` had no effect on the native scan.
    #[test]
    fn seeds_allowlisted_fields_from_session_but_protects_spark_compat_fields() {
        let session_parquet_options = ParquetOptions {
            pushdown_filters: true,
            reorder_filters: true,
            force_filter_selections: true,
            bloom_filter_on_read: false,
            max_predicate_cache_size: Some(0),
            enable_page_index: false,
            pruning: false,
            // An escape-hatch attempt to override Spark-compat fields; must not take effect.
            coerce_int96: None,
            coerce_int96_tz: None,
            ..Default::default()
        };

        let (table_parquet_options, _) = get_options(
            "UTC",
            true,
            false,
            false,
            false,
            &ObjectStoreUrl::local_filesystem(),
            false,
            &session_parquet_options,
        );

        let global = &table_parquet_options.global;
        assert!(global.pushdown_filters);
        assert!(global.reorder_filters);
        assert!(global.force_filter_selections);
        assert!(!global.bloom_filter_on_read);
        assert_eq!(global.max_predicate_cache_size, Some(0));
        assert!(!global.enable_page_index);
        assert!(!global.pruning);

        // Spark-compat fields stay hardcoded regardless of the session.
        assert_eq!(global.coerce_int96, Some("us".to_string()));
        assert_eq!(global.coerce_int96_tz, Some("UTC".to_string()));
    }

    // Regression test for #3978: DataFusion's opener requests `PageIndexPolicy::Skip` on the
    // initial metadata load and only loads the page index later, on demand, when row-group
    // pruning shows it is still needed (apache/datafusion#22857). That on-demand load bypasses
    // `FileMetadataCache` entirely, so on a predicate that never resolves to fully-matched (e.g.
    // non-null `IS NOT NULL` join keys without row-group `null_count` stats), the page index is
    // re-fetched, uncached, on every open. `EagerPageIndexReaderFactory` ignores the requested
    // policy and always loads the page index into the cache on the first fetch, so it asserts
    // present here even though this scan has no pruning predicate to request it.
    #[tokio::test]
    async fn caches_full_metadata_with_page_index() {
        // A file with a page index (page-level statistics and a small data page row limit) so we can
        // assert the cached metadata includes it rather than lacking it because none was written.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from((0..1000).collect::<Vec<i32>>()))],
        )
        .unwrap();

        let filename = get_temp_filename()
            .as_path()
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(100)
            .build();
        let file = File::create(&filename).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let partitioned_file = PartitionedFile::from_path(filename).unwrap();
        let location = partitioned_file.object_meta.location.clone();

        let session_ctx = Arc::new(SessionContext::new());
        let scan = init_datasource_exec(
            Arc::clone(&schema),
            None,
            None,
            ObjectStoreUrl::local_filesystem(),
            vec![vec![partitioned_file]],
            None,
            None,
            None,
            "UTC",
            true,
            false,
            false,
            false,
            &session_ctx,
            false,
            false,
            false,
        )
        .unwrap();

        // Drain the scan so the reader opens the file and populates the cache.
        let mut stream = scan.execute(0, session_ctx.task_ctx()).unwrap();
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }

        // The per-task RuntimeEnv metadata cache holds this file's metadata with the page index
        // (column + offset index) loaded, even though this scan pushed no pruning predicate.
        let cache = session_ctx
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache();
        let entry = cache
            .get(&location)
            .expect("file metadata should be cached");
        let parquet_meta = entry
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .expect("cached entry should hold Parquet metadata")
            .parquet_metadata();
        assert!(
            parquet_meta.column_index().is_some() && parquet_meta.offset_index().is_some(),
            "cached metadata must include the page index"
        );
    }
}
