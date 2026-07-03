// Proves a Comet-FFI leaf survives Ballista's physical-plan serialization.
//
// Ballista ships each stage's physical plan to executors as protobuf via a
// PhysicalExtensionCodec. Here we take a plan `CoalesceBatchesExec(CometScanExec)`,
// serialize the WHOLE tree with datafusion-proto + our CometPhysicalCodec exactly
// as Ballista would, then deserialize it in a fresh context (simulating the
// executor) and execute it. The Comet leaf travels as proto bytes and is rebuilt
// on the far side by re-running Comet's planner over FFI.

#![cfg(feature = "ballista")]

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
// `CoalesceBatchesExec` is deprecated upstream in favor of arrow-rs's
// `BatchCoalescer`, but it's still a real, functional standard DataFusion
// operator, which is exactly what this test needs on top of the Comet leaf.
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;

use comet::execution::ballista::{CometPhysicalCodec, CometScanExec};
use datafusion_comet_proto::spark_expression::{data_type::DataTypeId, DataType};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, NativeScan, NativeScanCommon, Operator, SparkFilePartition,
    SparkPartitionedFile, SparkStructField,
};

/// Write a tiny Parquet file with a single int32 column `a` = [1..=5].
fn write_test_parquet(path: &std::path::Path) -> anyhow::Result<()> {
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
fn build_native_scan_proto(parquet_path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
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

#[allow(deprecated)]
#[tokio::test(flavor = "multi_thread")]
async fn comet_leaf_survives_ballista_codec() -> anyhow::Result<()> {
    let parquet_path = std::env::temp_dir().join("comet_ffi_ballista_codec_roundtrip.parquet");
    write_test_parquet(&parquet_path)?;
    let proto = build_native_scan_proto(&parquet_path)?;

    // Build a plan with a standard DataFusion operator on top of the Comet leaf.
    let comet_scan: Arc<dyn ExecutionPlan> = Arc::new(CometScanExec::try_new(proto)?);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceBatchesExec::new(comet_scan, 8192));
    println!(
        "original plan:\n{}",
        displayable(plan.as_ref()).indent(false)
    );

    // --- Encode (scheduler side) ---
    let codec = CometPhysicalCodec::default();
    let node = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&plan), &codec)?;
    let bytes = node.encode_to_vec();
    println!("serialized physical plan: {} bytes", bytes.len());

    // --- Ship bytes, decode in a fresh context (executor side) ---
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let node2 = PhysicalPlanNode::decode(&bytes[..])?;
    let plan2 = node2.try_into_physical_plan(task_ctx.as_ref(), &codec)?;
    println!(
        "reconstructed plan (executor side):\n{}",
        displayable(plan2.as_ref()).indent(false)
    );

    // --- Execute the reconstructed plan ---
    let mut stream = plan2.execute(0, task_ctx)?;
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
    }
    println!("\nTOTAL ROWS AFTER CODEC ROUND-TRIP: {total_rows}");
    assert_eq!(total_rows, 5, "expected 5 rows");
    Ok(())
}
