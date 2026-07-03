// Proves the driver-side offload result boundary: a Comet `Operator` proto is
// run on in-process standalone Ballista and its result is exported over the
// Arrow C Data Interface into *caller-allocated* FFI structs, exactly as the
// JVM boundary does (Arrow Java allocates the structs, native writes into them,
// Arrow Java imports them). Here the "caller" is this Rust test standing in for
// the JVM: it allocates the FFI structs, calls `submit_and_export`, then
// re-imports via `from_ffi` and asserts 5 rows come back.
//
//   cargo test -p datafusion-comet --features ballista --test ballista_ffi_roundtrip -- --ignored --nocapture

#![cfg(feature = "ballista")]

use std::sync::Arc;

use datafusion::arrow::array::{make_array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::parquet::arrow::ArrowWriter;

use comet::execution::ballista::submit_and_export;
use datafusion_comet_proto::spark_expression::{data_type::DataTypeId, DataType};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, NativeScan, NativeScanCommon, Operator, SparkFilePartition,
    SparkPartitionedFile, SparkStructField,
};

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
        source: "comet-ffi-ballista-jvm-spike".to_string(),
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
#[test]
fn offload_proto_and_import_over_c_data_interface() -> anyhow::Result<()> {
    let parquet = std::env::temp_dir().join("comet_ffi_ballista_jvm_spike.parquet");
    write_test_parquet(&parquet)?;
    let proto = build_native_scan_proto(&parquet)?;

    // Stand in for the JVM: allocate one (array, schema) FFI struct per column.
    // Arrow Java's ArrowArray.allocateNew / ArrowSchema.allocateNew produce the
    // exact same C Data structs; we hand their addresses to native code.
    const NUM_COLS: usize = 1;
    let mut arrays: Vec<FFI_ArrowArray> = (0..NUM_COLS).map(|_| FFI_ArrowArray::empty()).collect();
    let mut schemas: Vec<FFI_ArrowSchema> =
        (0..NUM_COLS).map(|_| FFI_ArrowSchema::empty()).collect();
    let array_addrs: Vec<i64> = arrays
        .iter_mut()
        .map(|a| a as *mut FFI_ArrowArray as i64)
        .collect();
    let schema_addrs: Vec<i64> = schemas
        .iter_mut()
        .map(|s| s as *mut FFI_ArrowSchema as i64)
        .collect();

    // JVM → native → in-process Ballista → export into the caller structs.
    let num_rows = unsafe { submit_and_export(&proto, &array_addrs, &schema_addrs) }
        .map_err(anyhow::Error::msg)?;
    assert_eq!(num_rows, 5, "expected 5 rows back from Ballista");

    // JVM side: import the exported structs (mirrors Arrow Java ArrowImporter).
    for i in 0..NUM_COLS {
        let array = std::mem::replace(&mut arrays[i], FFI_ArrowArray::empty());
        let schema = std::mem::replace(&mut schemas[i], FFI_ArrowSchema::empty());
        let data = unsafe { from_ffi(array, &schema) }?;
        let imported = make_array(data);
        assert_eq!(imported.len(), 5, "imported column {i} should have 5 rows");
        let ints = imported
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column a should be Int32");
        let values: Vec<i32> = ints.values().to_vec();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    println!(
        "PASS: proto -> standalone Ballista -> {num_rows} rows exported and re-imported over the \
         Arrow C Data Interface (the JVM boundary mechanism)"
    );
    Ok(())
}
