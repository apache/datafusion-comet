// Distributes a Comet FFI scan across a real (in-process) Ballista cluster.
//
// A `CometTableProvider` exposes the Comet `NativeScan` as a SQL table. The
// query `GROUP BY a` forces a hash repartition, which Ballista turns into a
// shuffle boundary -> two stages. Stage 1 (the Comet scan + partial aggregate)
// is serialized and shipped to the executor via our codecs; the Comet leaf is
// rebuilt there by re-running Comet's planner over FFI. This proves Ballista
// distributes Comet work end to end.
//
// Starts an in-process Ballista scheduler + executors, so it is heavier and
// slower than a unit test. Run explicitly:
//   cargo test -p datafusion-comet --features ballista --test ballista_distributed -- --ignored

#![cfg(feature = "ballista")]

use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionConfig, SessionContext};

use comet::execution::ballista::{CometLogicalCodec, CometPhysicalCodec, CometTableProvider};
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
    use prost::Message;
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

#[ignore = "starts an in-process Ballista cluster; run explicitly"]
#[tokio::test]
async fn comet_scan_distributed_with_shuffle() -> anyhow::Result<()> {
    let parquet = std::env::temp_dir().join("comet_ffi_ballista_distributed.parquet");
    write_test_parquet(&parquet)?;
    let proto = build_native_scan_proto(&parquet)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Int32,
        true,
    )]));

    // In-process Ballista cluster with our Comet codecs registered on both the
    // scheduler and executor sides (they flow via SessionConfig).
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_standalone_parallelism(2)
        .with_ballista_physical_extension_codec(Arc::new(CometPhysicalCodec::default()))
        .with_ballista_logical_extension_codec(Arc::new(CometLogicalCodec::default()));
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await?;

    ctx.register_table("comet_t", Arc::new(CometTableProvider::new(proto, schema)))?;

    let sql = "SELECT a, count(*) AS c FROM comet_t GROUP BY a ORDER BY a";
    println!("distributed query: {sql}\n");
    let df = ctx.sql(sql).await?;
    println!("logical plan:\n{}\n", df.logical_plan().display_indent());

    let results = df.collect().await?;
    println!("{}", pretty_format_batches(&results)?);
    let groups: usize = results.iter().map(|b| b.num_rows()).sum();
    println!("\nGROUP BY produced {groups} groups");
    assert_eq!(groups, 5, "expected 5 groups (a = 1..=5)");
    println!("PASS: Comet FFI scan distributed by Ballista (with shuffle) — correct results");
    Ok(())
}
