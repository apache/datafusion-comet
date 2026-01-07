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
use arrow::datatypes::{Field, SchemaRef};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_comet_spark_expr::EvalMode;
use datafusion_datasource::TableSchema;
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
    session_timezone: &str,
    case_sensitive: bool,
    session_ctx: &Arc<SessionContext>,
    encryption_enabled: bool,
) -> Result<Arc<DataSourceExec>, ExecutionError> {
    let (table_parquet_options, _) = get_options(
        session_timezone,
        case_sensitive,
        &object_store_url,
        encryption_enabled,
    );

    // Determine the schema to use for ParquetSource
    let table_schema = if let Some(ref data_schema) = data_schema {
        if let Some(ref partition_schema) = partition_schema {
            let partition_fields: Vec<_> = partition_schema
                .fields()
                .iter()
                .map(|f| {
                    Arc::new(Field::new(f.name(), f.data_type().clone(), f.is_nullable())) as _
                })
                .collect();
            TableSchema::new(Arc::clone(data_schema), partition_fields)
        } else {
            TableSchema::from_file_schema(Arc::clone(data_schema))
        }
    } else {
        TableSchema::from_file_schema(Arc::clone(&required_schema))
    };

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

    let file_source = Arc::new(parquet_source) as Arc<dyn FileSource>;

    let file_groups = file_groups
        .iter()
        .map(|files| FileGroup::new(files.clone()))
        .collect();

    let mut file_scan_config_builder =
        FileScanConfigBuilder::new(object_store_url, file_source).with_file_groups(file_groups);

    if let Some(projection_vector) = projection_vector {
        file_scan_config_builder =
            file_scan_config_builder.with_projection_indices(Some(projection_vector))?;
    }

    let file_scan_config = file_scan_config_builder.build();

    Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan_config))))
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
