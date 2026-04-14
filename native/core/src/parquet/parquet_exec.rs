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

use crate::execution::jni_api::get_runtime;
use crate::execution::operators::ExecutionError;
use crate::parquet::encryption_support::{CometEncryptionConfig, ENCRYPTION_FACTORY_ID};
use crate::parquet::parquet_read_cached_factory::CachingParquetReaderFactory;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use arrow::datatypes::{Field, SchemaRef};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_comet_spark_expr::EvalMode;
use datafusion_datasource::TableSchema;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::RowGroupMetaData;
use std::collections::HashMap;
use std::sync::Arc;

/// Initializes a DataSourceExec plan with a ParquetSource. This may be used by either the
/// `native_datafusion` scan or the `native_iceberg_compat` scan.
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
    session_ctx: &Arc<SessionContext>,
    encryption_enabled: bool,
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    let (table_parquet_options, spark_parquet_options) = get_options(
        session_timezone,
        case_sensitive,
        &object_store_url,
        encryption_enabled,
    );

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

    let mut parquet_source =
        ParquetSource::new(table_schema).with_table_parquet_options(table_parquet_options);

    // Create a conjunctive form of the vector because ParquetExecBuilder takes
    // a single expression
    if let Some(data_filters) = data_filters {
        let cnf_data_filters = data_filters.clone().into_iter().reduce(|left, right| {
            Arc::new(BinaryExpr::new(
                left,
                datafusion::logical_expr::Operator::And,
                right,
            ))
        });

        if let Some(filter) = cnf_data_filters {
            parquet_source = parquet_source.with_predicate(filter);
        }
    }

    if encryption_enabled {
        parquet_source = parquet_source.with_encryption_factory(
            session_ctx
                .runtime_env()
                .parquet_encryption_factory(ENCRYPTION_FACTORY_ID)?,
        );
    }

    // Use caching reader factory to avoid redundant footer reads across partitions
    let store = session_ctx.runtime_env().object_store(&object_store_url)?;
    parquet_source = parquet_source
        .with_parquet_file_reader_factory(Arc::new(CachingParquetReaderFactory::new(
            Arc::clone(&store),
        )));

    // Apply midpoint-based row group pruning to match Spark/parquet-mr behavior.
    // DataFusion's built-in prune_by_range uses start-offset which can disagree with
    // Spark's midpoint-based assignment, causing some tasks to read no data while
    // others read too much. We replace the range with an explicit ParquetAccessPlan.
    let file_groups = apply_midpoint_row_group_pruning(file_groups, &store)?;

    let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
        SparkPhysicalExprAdapterFactory::new(spark_parquet_options, default_values),
    );

    let file_source: Arc<dyn FileSource> = Arc::new(parquet_source);

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

/// Compute the midpoint offset of a row group, matching the algorithm used by
/// Spark (parquet-mr) and parquet-rs to assign row groups to file splits.
///
/// The midpoint is: min(data_page_offset, dictionary_page_offset) + compressed_size / 2
///
/// A row group belongs to a split if its midpoint falls within [split_start, split_end).
fn get_row_group_midpoint(rg: &RowGroupMetaData) -> i64 {
    let col = rg.column(0);
    let mut offset = col.data_page_offset();
    if let Some(dict_offset) = col.dictionary_page_offset() {
        if dict_offset < offset {
            offset = dict_offset;
        }
    }
    offset + rg.compressed_size() / 2
}

/// For each PartitionedFile that has a byte range, read the Parquet footer and compute
/// which row groups belong to this split using the midpoint algorithm (matching Spark/parquet-mr).
/// Replace the byte range with an explicit ParquetAccessPlan so that DataFusion's
/// `prune_by_range` (which uses a different algorithm) is bypassed.
fn apply_midpoint_row_group_pruning(
    file_groups: Vec<Vec<PartitionedFile>>,
    store: &Arc<dyn ObjectStore>,
) -> Result<Vec<Vec<PartitionedFile>>, ExecutionError> {
    let has_ranges = file_groups
        .iter()
        .any(|group| group.iter().any(|f| f.range.is_some()));
    if !has_ranges {
        return Ok(file_groups);
    }

    let rt = get_runtime();

    let mut result = Vec::with_capacity(file_groups.len());
    for group in file_groups {
        let mut new_group = Vec::with_capacity(group.len());
        for mut file in group {
            if let Some(range) = file.range.take() {
                let metadata = rt.block_on(async {
                    let mut reader =
                        ParquetObjectReader::new(Arc::clone(store), file.object_meta.location.clone())
                            .with_file_size(file.object_meta.size);
                    reader.get_metadata(None).await
                })
                .map_err(|e| {
                    ExecutionError::GeneralError(format!(
                        "Failed to read Parquet metadata for {}: {e}",
                        file.object_meta.location
                    ))
                })?;

                let num_row_groups = metadata.num_row_groups();
                let mut access_plan = ParquetAccessPlan::new_none(num_row_groups);

                for i in 0..num_row_groups {
                    let midpoint = get_row_group_midpoint(metadata.row_group(i));
                    if midpoint >= range.start && midpoint < range.end {
                        access_plan.scan(i);
                    }
                }

                file.extensions = Some(Arc::new(access_plan));
            }
            new_group.push(file);
        }
        result.push(new_group);
    }

    Ok(result)
}

fn get_options(
    session_timezone: &str,
    case_sensitive: bool,
    object_store_url: &ObjectStoreUrl,
    encryption_enabled: bool,
) -> (TableParquetOptions, SparkParquetOptions) {
    let mut table_parquet_options = TableParquetOptions::new();
    table_parquet_options.global.pushdown_filters = true;
    table_parquet_options.global.reorder_filters = true;
    table_parquet_options.global.coerce_int96 = Some("us".to_string());
    let mut spark_parquet_options =
        SparkParquetOptions::new(EvalMode::Legacy, session_timezone, false);
    spark_parquet_options.allow_cast_unsigned_ints = true;
    spark_parquet_options.case_sensitive = case_sensitive;

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

/// Wraps a `SendableRecordBatchStream` to print each batch as it flows through.
/// Returns a new `SendableRecordBatchStream` that yields the same batches.
pub fn dbg_batch_stream(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    use futures::StreamExt;
    let schema = stream.schema();
    let printing_stream = stream.map(|batch_result| {
        match &batch_result {
            Ok(batch) => {
                dbg!(batch, batch.schema());
                for (col_idx, column) in batch.columns().iter().enumerate() {
                    dbg!(col_idx, column, column.nulls());
                }
            }
            Err(e) => {
                println!("batch error: {:?}", e);
            }
        }
        batch_result
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, printing_stream))
}
