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
use crate::parquet::encryption_support::{CometEncryptionConfig, ENCRYPTION_FACTORY_ID};
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use arrow::datatypes::{Field, SchemaRef};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
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
    let (table_parquet_options, mut spark_parquet_options) = get_options(
        session_timezone,
        case_sensitive,
        return_null_struct_if_all_fields_missing,
        allow_type_promotion,
        allow_timestamp_ltz_to_ntz,
        &object_store_url,
        encryption_enabled,
    );
    spark_parquet_options.use_field_id = use_field_id;
    spark_parquet_options.ignore_missing_field_id = ignore_missing_field_id;

    // Determine the schema and projection to use for ParquetSource.
    // When data_schema is provided, use it as the base schema so DataFusion knows the full
    // file schema. Compute a projection vector to select only the required columns.
    let (base_schema, projection) = match (&data_schema, &projection_vector) {
        (Some(schema), Some(proj)) => (Arc::clone(schema), Some(proj.clone())),
        (Some(schema), None) => {
            // Compute projection: map required_schema field names to data_schema indices.
            // This is needed for schema pruning when the data_schema has more columns than
            // the required_schema.
            let projection: Vec<usize> = required_schema
                .fields()
                .iter()
                .filter_map(|req_field| {
                    schema.fields().iter().position(|data_field| {
                        if case_sensitive {
                            data_field.name() == req_field.name()
                        } else {
                            data_field.name().to_lowercase() == req_field.name().to_lowercase()
                        }
                    })
                })
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

    // DataFusion's metadata-caching reader factory: loads each file's full metadata (including the
    // page index) once into the per-task RuntimeEnv cache (bounded LRU, `metadata_cache_limit`) and
    // reuses it across that file's row-group splits, so the opener does not re-fetch the page index.
    //
    // TODO: metadata I/O is invisible in metrics. `fetch_metadata` reads via `ObjectStore::get_ranges`,
    // bypassing the `get_bytes` path where `bytes_scanned` is counted. A byte-counting ObjectStore
    // wrapper would surface it.
    let runtime_env = session_ctx.runtime_env();
    let store = runtime_env.object_store(&object_store_url)?;
    let metadata_cache = runtime_env.cache_manager.get_file_metadata_cache();
    parquet_source = parquet_source.with_parquet_file_reader_factory(Arc::new(
        CachedParquetFileReaderFactory::new(store, metadata_cache),
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
            let state = session_ctx.state();
            let propagation =
                parquet_source.try_pushdown_filters(filters, state.config_options())?;
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

fn get_options(
    session_timezone: &str,
    case_sensitive: bool,
    return_null_struct_if_all_fields_missing: bool,
    allow_type_promotion: bool,
    allow_timestamp_ltz_to_ntz: bool,
    object_store_url: &ObjectStoreUrl,
    encryption_enabled: bool,
) -> (TableParquetOptions, SparkParquetOptions) {
    let mut table_parquet_options = TableParquetOptions::new();
    table_parquet_options.global.pushdown_filters = true;
    table_parquet_options.global.reorder_filters = true;
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

    // Regression test for #3978: the scan's reader factory must load the full
    // Parquet metadata, including the page index, into the per-task RuntimeEnv
    // metadata cache. The previous hand-rolled factory cached only the footer,
    // so DataFusion re-fetched the page index on every open of the same file.
    #[tokio::test]
    async fn caches_full_metadata_with_page_index() {
        // Write a file with a page index: page-level statistics and a small data
        // page row limit so the column and offset indexes span multiple pages.
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

        // The per-task RuntimeEnv metadata cache must now hold this file's
        // metadata with the page index (column + offset index) loaded.
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
