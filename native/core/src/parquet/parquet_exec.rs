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
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkSchemaAdapterFactory;
use arrow_schema::{Field, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::EvalMode;
use datafusion_common::config::TableParquetOptions;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_physical_expr::expressions::BinaryExpr;
use itertools::Itertools;
use object_store::path::Path;
use object_store::{parse_url, ObjectStore};
use std::sync::Arc;
use url::Url;

#[allow(clippy::too_many_arguments)]
pub(crate) fn init_parquet_exec(
    required_schema: SchemaRef,
    data_schema: Option<SchemaRef>,
    partition_schema: Option<SchemaRef>,
    partition_fields: Option<Vec<Field>>,
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<Vec<PartitionedFile>>,
    projection_vector: Option<Vec<usize>>,
    data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    // file_scan_config: FileScanConfig,
    session_timezone: &str,
) -> Result<ParquetExec, ExecutionError> {
    let file_scan_config = match (data_schema, projection_vector, partition_fields) {
        (Some(data_schema), Some(projection_vector), Some(partition_fields)) => {
            get_file_config(data_schema, partition_schema, file_groups, object_store_url)
                .with_projection(Some(projection_vector))
                .with_table_partition_cols(partition_fields)
        }
        _ => get_file_config(
            required_schema,
            partition_schema,
            file_groups,
            object_store_url,
        ),
    };

    let (table_parquet_options, spark_parquet_options) = get_options(session_timezone);

    let mut builder = ParquetExecBuilder::new(file_scan_config)
        .with_table_parquet_options(table_parquet_options)
        .with_schema_adapter_factory(Arc::new(SparkSchemaAdapterFactory::new(
            spark_parquet_options,
        )));

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
            builder = builder.with_predicate(filter);
        }
    }

    Ok(builder.build())
}

// Mirrors object_store::parse::parse_url for the hdfs object store
// fn parse_hdfs_url(url: &Url) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
#[cfg(feature = "hdfs")]
fn parse_hdfs_url(url: &Url) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    match datafusion_comet_objectstore_hdfs::object_store::hdfs::HadoopFileSystem::new(url.as_ref())
    {
        Some(object_store) => {
            let path = object_store.get_path(url.as_str());
            Ok((Box::new(object_store), path))
        }
        _ => {
            return Err(object_store::Error::Generic {
                store: "HadoopFileSystem",
                source: Box::new(datafusion_comet_objectstore_hdfs::object_store::hdfs::HadoopFileSystem::HdfsErr::Generic(
                    "Could not create hdfs object store".to_string(),
                )),
            });
        }
    }
}
#[cfg(not(feature = "hdfs"))]
fn parse_hdfs_url(_url: &Url) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    Err(object_store::Error::Generic {
        store: "HadoopFileSystem",
        source: "Hdfs support is not enabled in this build".into(),
    })
}

// parses the url, registers the object store, and returns a tuple of the object store url and object store path
pub(crate) fn prepare_object_store(
    runtime_env: Arc<RuntimeEnv>,
    url: String,
) -> Result<(ObjectStoreUrl, Path), ExecutionError> {
    let mut url = Url::parse(url.as_str()).unwrap();
    let mut scheme = url.scheme();
    if scheme == "s3a" {
        scheme = "s3";
        url.set_scheme("s3")
            .expect("Could not convert scheme from s3a to s3");
    }
    let url_key = format!(
        "{}://{}",
        scheme,
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    );

    let (object_store, object_store_path): (Box<dyn ObjectStore>, Path) = if scheme == "hdfs" {
        match parse_hdfs_url(&url) {
            Ok(r) => r,
            Err(e) => return Err(ExecutionError::GeneralError(e.to_string())),
        }
    } else {
        match parse_url(&url) {
            Ok(r) => r,
            Err(e) => return Err(ExecutionError::GeneralError(e.to_string())),
        }
    };
    let object_store_url = ObjectStoreUrl::parse(url_key.clone())?;
    // runtime_env.register_object_store(object_store_url.as_ref(), Arc::from(object_store));
    runtime_env.register_object_store(&url, Arc::from(object_store));
    Ok((object_store_url, object_store_path))
}

pub(crate) fn get_file_groups_single_file(
    path: &Path,
    file_size: u64,
    start: i64,
    length: i64,
) -> Vec<Vec<PartitionedFile>> {
    let mut partitioned_file = PartitionedFile::new_with_range(
        String::new(), // Dummy file path. We will override this with our path so that url encoding does not occur
        file_size,
        start,
        start + length,
    );
    partitioned_file.object_meta.location = (*path).clone();
    vec![vec![partitioned_file]]
}

fn get_options(session_timezone: &str) -> (TableParquetOptions, SparkParquetOptions) {
    let mut table_parquet_options = TableParquetOptions::new();
    // TODO: Maybe these are configs?
    table_parquet_options.global.pushdown_filters = true;
    table_parquet_options.global.reorder_filters = true;
    let mut spark_parquet_options =
        SparkParquetOptions::new(EvalMode::Legacy, session_timezone, false);
    spark_parquet_options.allow_cast_unsigned_ints = true;
    (table_parquet_options, spark_parquet_options)
}

fn get_file_config(
    schema: SchemaRef,
    partition_schema: Option<SchemaRef>,
    file_groups: Vec<Vec<PartitionedFile>>,
    object_store_url: ObjectStoreUrl,
) -> FileScanConfig {
    match partition_schema {
        Some(partition_schema) => {
            let partition_fields: Vec<Field> = partition_schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                })
                .collect_vec();
            FileScanConfig::new(object_store_url, Arc::clone(&schema))
                .with_file_groups(file_groups)
                .with_table_partition_cols(partition_fields)
        }
        _ => {
            FileScanConfig::new(object_store_url, Arc::clone(&schema)).with_file_groups(file_groups)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::operators::ExecutionError;
    use crate::parquet::parquet_exec::prepare_object_store;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use object_store::path::Path;
    use std::sync::Arc;
    use url::Url;

    #[test]
    #[cfg(not(feature = "hdfs"))]
    fn test_prepare_object_store() {
        let local_file_system_url = "file:///comet/spark-warehouse/part-00000.snappy.parquet";
        let s3_url = "s3a://test_bucket/comet/spark-warehouse/part-00000.snappy.parquet";
        let hdfs_url = "hdfs://localhost:8020/comet/spark-warehouse/part-00000.snappy.parquet";

        let all_urls = [local_file_system_url, s3_url, hdfs_url];
        let expected: Vec<Result<(ObjectStoreUrl, Path), ExecutionError>> = vec![
            Ok((
                ObjectStoreUrl::parse("file://").unwrap(),
                Path::from("/comet/spark-warehouse/part-00000.snappy.parquet"),
            )),
            Ok((
                ObjectStoreUrl::parse("s3://test_bucket").unwrap(),
                Path::from("/comet/spark-warehouse/part-00000.snappy.parquet"),
            )),
            Err(ExecutionError::GeneralError(
                "Generic HadoopFileSystem error: Hdfs support is not enabled in this build"
                    .parse()
                    .unwrap(),
            )),
        ];

        for (i, url_str) in all_urls.iter().enumerate() {
            let url = &Url::parse(url_str).unwrap();
            let res = prepare_object_store(Arc::new(RuntimeEnv::default()), url.to_string());

            let expected = expected.get(i).unwrap();
            match expected {
                Ok((o, p)) => {
                    let (r_o, r_p) = res.unwrap();
                    assert_eq!(r_o, *o);
                    assert_eq!(r_p, *p);
                }
                Err(e) => {
                    assert!(res.is_err());
                    let Err(res_e) = res else {
                        panic!("test failed")
                    };
                    assert_eq!(e.to_string(), res_e.to_string())
                }
            }
        }
    }
}
