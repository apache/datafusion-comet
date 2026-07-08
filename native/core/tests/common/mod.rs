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
// Shared helpers for Ballista offload tests: write a tiny Parquet file and build
// Comet `Operator` proto blocks (`NativeScan` leaf, `Scan` leaf) used to assemble
// offload descriptors without standing up a real cluster.
//
// `write_test_parquet` / `build_native_scan_proto` are copied verbatim from
// `ballista_distributed.rs` so multiple test binaries can share them (each
// `tests/*.rs` file is its own crate, so this lives in `tests/common/mod.rs` and
// is pulled in via `mod common;`).

#![allow(dead_code)]

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use prost::Message;

use datafusion_comet_proto::spark_expression::{data_type::DataTypeId, DataType};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, NativeScan, NativeScanCommon, Operator, Scan, SparkFilePartition,
    SparkPartitionedFile, SparkStructField,
};

/// Write a tiny Parquet file with a single int32 column `a` = [1..=5].
pub fn write_test_parquet(path: &std::path::Path) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )?;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Build a Comet `Operator` proto: a single `NativeScan` over `parquet_path`.
pub fn build_native_scan_proto(parquet_path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let int32 = DataType {
        type_id: DataTypeId::Int32 as i32,
        type_info: None,
    };
    let field_a = SparkStructField {
        name: "a".to_string(),
        data_type: Some(int32),
        nullable: true,
        metadata: Default::default(),
    };
    let common = NativeScanCommon {
        required_schema: vec![field_a.clone()],
        data_schema: vec![field_a],
        projection_vector: vec![0],
        session_timezone: "UTC".to_string(),
        source: "comet-ffi-ballista-test".to_string(),
        ..Default::default()
    };
    let file_size = std::fs::metadata(parquet_path)?.len() as i64;
    let partitioned_file = SparkPartitionedFile {
        file_path: format!("file://{}", parquet_path.display()),
        start: 0,
        length: file_size,
        file_size,
        partition_values: vec![],
    };
    let native_scan = NativeScan {
        common: Some(common),
        file_partition: Some(SparkFilePartition {
            partitioned_file: vec![partitioned_file],
        }),
    };
    let op = Operator {
        children: vec![],
        plan_id: 0,
        op_struct: Some(OpStruct::NativeScan(native_scan)),
    };
    Ok(op.encode_to_vec())
}

/// Build a Comet `Operator` proto: a single `Scan` (#100) leaf over a one-column
/// Int32 schema named `a`. This is the shape a DAG **consumer** fragment's block
/// must have — a childless `Scan` leaf that `build_offload_plan`'s hash
/// `RepartitionExec` child feeds — as opposed to a `NativeScan` block (which reads
/// Parquet directly and has zero `Scan` leaves, i.e. is only valid as a producer
/// with no inputs).
pub fn build_scan_leaf_block_proto() -> Vec<u8> {
    let int32 = DataType {
        type_id: DataTypeId::Int32 as i32,
        type_info: None,
    };
    let scan = Scan {
        fields: vec![int32],
        source: "comet-offload-dag-test".to_string(),
    };
    let op = Operator {
        children: vec![],
        plan_id: 0,
        op_struct: Some(OpStruct::Scan(scan)),
    };
    op.encode_to_vec()
}
