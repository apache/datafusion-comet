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

//! A scan partition is read through one object store, so planning must reject a partition whose
//! files resolve to different stores, and a CSV scan must build the store of its own partition.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::physical_plan::CsvSource;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_expression::{data_type::DataTypeId, DataType as SparkDataType};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, CsvOptions, CsvScan, Operator, SparkFilePartition, SparkPartitionedFile,
    SparkStructField,
};
use datafusion_datasource::source::DataSourceExec;

use crate::execution::planner::PhysicalPlanner;

/// S3 options that build a store without network access: anonymous credentials and a region.
fn offline_s3_options() -> HashMap<String, String> {
    HashMap::from([
        (
            "fs.s3a.aws.credentials.provider".to_string(),
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string(),
        ),
        (
            "fs.s3a.endpoint.region".to_string(),
            "us-east-1".to_string(),
        ),
    ])
}

fn file(path: &str) -> SparkPartitionedFile {
    SparkPartitionedFile {
        file_path: path.to_string(),
        start: 0,
        length: 16,
        file_size: 16,
        partition_values: vec![],
    }
}

fn partition(paths: &[&str]) -> SparkFilePartition {
    SparkFilePartition {
        partitioned_file: paths.iter().map(|p| file(p)).collect(),
    }
}

fn partitioned_files_error(paths: &[&str], options: &HashMap<String, String>) -> Option<String> {
    PhysicalPlanner::default()
        .get_partitioned_files(&partition(paths), options)
        .err()
        .map(|e| e.to_string())
}

#[test]
fn partition_with_files_in_two_buckets_is_rejected() {
    let error = partitioned_files_error(
        &["s3a://bucket-a/t/f1.parquet", "s3a://bucket-b/t/f2.parquet"],
        &offline_s3_options(),
    )
    .expect("a partition spanning two buckets must not plan");
    assert!(
        error.contains("s3://bucket-a") && error.contains("s3://bucket-b"),
        "error should name both stores: {error}"
    );
}

#[test]
fn partition_with_files_of_two_schemes_is_rejected() {
    let error = partitioned_files_error(
        &["s3a://bucket-a/t/f1.parquet", "gs://bucket-b/t/f2.parquet"],
        &offline_s3_options(),
    )
    .expect("a partition spanning s3 and gcs must not plan");
    assert!(
        error.contains("s3://bucket-a") && error.contains("gs://bucket-b"),
        "error should name both stores: {error}"
    );
}

#[test]
fn partition_with_one_store_under_different_spellings_is_accepted() {
    // s3 and s3a resolve to the same store, so they may share a partition.
    assert_eq!(
        partitioned_files_error(
            &["s3a://bucket-a/t/f1.parquet", "s3://bucket-a/t/f2.parquet"],
            &offline_s3_options(),
        ),
        None
    );
}

#[test]
fn partition_mixing_libhdfs_and_native_reads_of_one_bucket_is_rejected() {
    // With s3 routed through libhdfs, s3:// and s3a:// share a key but not a backend.
    let mut options = offline_s3_options();
    options.insert("fs.comet.libhdfs.schemes".to_string(), "s3".to_string());
    let error = partitioned_files_error(
        &["s3://bucket-a/t/f1.parquet", "s3a://bucket-a/t/f2.parquet"],
        &options,
    )
    .expect("a partition mixing libhdfs and native stores must not plan");
    assert!(
        error.contains("libhdfs"),
        "error should name the backend: {error}"
    );
}

fn string_field(name: &str) -> SparkStructField {
    SparkStructField {
        name: name.to_string(),
        data_type: Some(SparkDataType {
            type_id: DataTypeId::String as i32,
            type_info: None,
        }),
        nullable: true,
        metadata: HashMap::new(),
    }
}

fn csv_scan(file_partitions: Vec<SparkFilePartition>) -> Operator {
    Operator {
        op_struct: Some(OpStruct::CsvScan(CsvScan {
            data_schema: vec![string_field("c")],
            partition_schema: vec![],
            projection_vector: vec![0],
            file_partitions,
            object_store_options: offline_s3_options(),
            csv_options: Some(CsvOptions {
                has_header: false,
                delimiter: ",".to_string(),
                quote: "\"".to_string(),
                escape: "\\".to_string(),
                comment: None,
                terminator: "\n".to_string(),
                truncated_rows: false,
            }),
        })),
        ..Default::default()
    }
}

/// Plans `operator` as Spark partition `partition` and returns the scan's object store URL.
fn csv_object_store_url(operator: &Operator, partition: i32) -> Result<String, String> {
    let planner = PhysicalPlanner::new(Arc::new(SessionContext::new()), partition);
    let (_, _, plan) = planner
        .create_plan(operator, &mut vec![], 2)
        .map_err(|e| e.to_string())?;
    let exec = plan
        .native_plan
        .downcast_ref::<DataSourceExec>()
        .expect("CSV scan plans a DataSourceExec");
    let (config, _) = exec
        .downcast_to_file_source::<CsvSource>()
        .expect("CSV scan reads CSV files");
    Ok(config.object_store_url.as_str().to_string())
}

#[test]
fn csv_scan_reads_each_partition_through_its_own_store() {
    let operator = csv_scan(vec![
        partition(&["s3a://bucket-a/t/a.csv"]),
        partition(&["s3a://bucket-b/t/b.csv"]),
    ]);
    let first = csv_object_store_url(&operator, 0).unwrap();
    let second = csv_object_store_url(&operator, 1).unwrap();
    assert!(
        first.ends_with("://bucket-a/"),
        "partition 0 store: {first}"
    );
    assert!(
        second.ends_with("://bucket-b/"),
        "partition 1 store: {second}"
    );
}

#[tokio::test]
async fn csv_scan_with_an_empty_partition_reads_no_rows() {
    let operator = csv_scan(vec![partition(&[])]);
    assert_eq!(csv_object_store_url(&operator, 0).unwrap(), "file:///");

    let planner = PhysicalPlanner::new(Arc::new(SessionContext::new()), 0);
    let (_, _, plan) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
    let schema = plan.native_plan.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "c");
    let batches = collect(
        Arc::clone(&plan.native_plan),
        planner.session_ctx().task_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[test]
fn csv_scan_rejects_a_partition_index_outside_the_serialized_partitions() {
    let operator = csv_scan(vec![partition(&["s3a://bucket-a/t/a.csv"])]);
    for index in [1, -1] {
        let error = csv_object_store_url(&operator, index).unwrap_err();
        assert!(
            error.contains(&format!("partition {index}")),
            "unexpected error: {error}"
        );
    }
}
