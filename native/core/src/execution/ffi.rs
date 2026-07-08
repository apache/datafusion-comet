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

//! Exposes a Comet native plan across a `datafusion-ffi` boundary so that a
//! process compiled against a different DataFusion version (e.g. a Ballista
//! executor) can execute it without linking Comet's Rust crates.
//!
//! The input is a serialized Comet `Operator` plan whose leaves are
//! `NativeScan` (native Parquet), so no JVM-fed inputs are required.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_operator::Operator;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use prost::Message;
use tokio::runtime::Handle;

use super::planner::PhysicalPlanner;

/// Decode a Comet `Operator` protobuf, build the native DataFusion plan with
/// Comet's own planner, and wrap the root as an `FFI_ExecutionPlan`.
///
/// `runtime` is the Tokio runtime handle the foreign consumer should use to
/// drive async execution across the boundary.
pub fn comet_ffi_plan_from_proto(
    proto_bytes: &[u8],
    runtime: Option<Handle>,
) -> Result<FFI_ExecutionPlan, String> {
    let op = Operator::decode(proto_bytes)
        .map_err(|e| format!("failed to decode Comet Operator proto: {e}"))?;

    // A fresh `SessionContext` means object-store configuration comes only
    // from the proto's `object_store_options`, not from any ambient session.
    // That's sufficient for local `file://` scans; remote object stores
    // (S3, GCS, etc.) will need this revisited to plumb their config through.
    let session_ctx = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(session_ctx, 0);

    // NativeScan leaves read Parquet directly, so no JVM input sources are needed.
    let mut inputs = Vec::new();
    let (_scans, _shuffle_scans, spark_plan) = planner
        .create_plan(&op, &mut inputs, 1)
        .map_err(|e| format!("failed to build native plan: {e}"))?;

    let plan: Arc<dyn ExecutionPlan> = Arc::clone(&spark_plan.native_plan);
    Ok(FFI_ExecutionPlan::new(plan, runtime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion_comet_proto::spark_expression::{data_type::DataTypeId, DataType};
    use datafusion_comet_proto::spark_operator::{
        operator::OpStruct, NativeScan, NativeScanCommon, SparkFilePartition, SparkPartitionedFile,
        SparkStructField,
    };
    use datafusion_ffi::execution_plan::ForeignExecutionPlan;
    use futures::StreamExt;

    /// Write a tiny Parquet file with a single int32 column `a` = [1..=5].
    fn write_test_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Build a Comet `Operator` proto: a single `NativeScan` over `parquet_path`.
    fn build_native_scan_proto(parquet_path: &std::path::Path) -> Vec<u8> {
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
            source: "comet-ffi-test".to_string(),
            ..Default::default()
        };
        let file_size = std::fs::metadata(parquet_path).unwrap().len() as i64;
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
        op.encode_to_vec()
    }

    #[tokio::test]
    async fn ffi_export_executes_native_scan() {
        let dir = tempfile::tempdir().unwrap();
        let parquet_path = dir.path().join("ffi_export_test.parquet");
        write_test_parquet(&parquet_path);

        let proto = build_native_scan_proto(&parquet_path);

        let ffi_plan = comet_ffi_plan_from_proto(&proto, Handle::try_current().ok())
            .expect("failed to build FFI plan from proto");

        // Wrap via `ForeignExecutionPlan` to force the real FFI vtable path,
        // rather than datafusion-ffi's same-library short-circuit.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ForeignExecutionPlan::try_from(ffi_plan)
                .expect("failed to wrap FFI plan as ForeignExecutionPlan"),
        );

        let session_ctx = SessionContext::new();
        let mut stream = plan
            .execute(0, session_ctx.task_ctx())
            .expect("failed to execute plan");

        let mut total = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("failed to read batch");
            total += batch.num_rows();
        }

        assert_eq!(total, 5);
    }
}
