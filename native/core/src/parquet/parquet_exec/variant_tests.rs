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

use super::*;
use arrow::datatypes::i256;
use arrow::{
    array::{Array, ArrayRef, BinaryArray, Date64Array, Decimal256Array, StructArray},
    datatypes::{DataType, Fields, Schema},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::test_common::file_util::get_temp_filename;
use futures::StreamExt;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, ArrowWriter},
    basic::{LogicalType, Repetition, Type as PhysicalType},
    data_type::{
        ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArray,
        FixedLenByteArrayType,
    },
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::{Type as ParquetType, TypePtr},
    variant::{Variant, VariantArray, VariantBuilder, VariantDecimal4},
};
use std::{fs::File, path::PathBuf};
fn required_variant_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ])),
        false,
    )
    .with_extension_type(VariantType)]))
}

async fn write_and_scan_shredded_variant(
    typed_value: ArrayRef,
    coerce_types: bool,
) -> VariantArray {
    let mut builder = VariantBuilder::new();
    builder.append_value(Variant::Null);
    let (metadata, _) = builder.finish();
    let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata.as_slice())]));
    let physical: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("typed_value", typed_value.data_type().clone(), false),
            ]),
            vec![metadata, typed_value],
            None,
        )
        .unwrap(),
    );
    let file_schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        physical.data_type().clone(),
        false,
    )
    .with_extension_type(VariantType)]));
    let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![physical]).unwrap();

    let filename = get_temp_filename();
    let file = File::create(&filename).unwrap();
    let properties = WriterProperties::builder()
        .set_coerce_types(coerce_types)
        .build();
    let mut writer = ArrowWriter::try_new_with_options(
        file,
        file_schema,
        ArrowWriterOptions::new().with_properties(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    scan_variant_file(filename).await
}

fn write_variant_typed_value<T: ParquetDataType>(typed_value: TypePtr, values: &[T::T]) -> PathBuf {
    let filename = get_temp_filename();
    let file = File::create(&filename).unwrap();
    let metadata = Arc::new(
        ParquetType::primitive_type_builder("metadata", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    );
    let variant = Arc::new(
        ParquetType::group_type_builder("v")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::variant(None)))
            .with_fields(vec![metadata, typed_value])
            .build()
            .unwrap(),
    );
    let schema = Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec![variant])
            .build()
            .unwrap(),
    );
    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    let mut builder = VariantBuilder::new();
    builder.append_value(Variant::Null);
    let (metadata, _) = builder.finish();
    let metadata = (0..values.len())
        .map(|_| ByteArray::from(metadata.clone()))
        .collect::<Vec<_>>();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<ByteArrayType>()
        .write_batch(&metadata, None, None)
        .unwrap();
    column.close().unwrap();

    let mut column = row_group.next_column().unwrap().unwrap();
    column.typed::<T>().write_batch(values, None, None).unwrap();
    column.close().unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    filename
}

async fn scan_variant_file(filename: PathBuf) -> VariantArray {
    let partitioned_file =
        PartitionedFile::from_path(filename.to_string_lossy().into_owned()).unwrap();
    let session_ctx = Arc::new(SessionContext::new());
    let scan = init_datasource_exec(
        required_variant_schema(),
        None,
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
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
        false,
    )
    .unwrap();
    let mut stream = scan.execute(0, session_ctx.task_ctx()).unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert!(stream.next().await.is_none());
    VariantArray::try_new(batch.column(0).as_ref()).unwrap()
}

#[tokio::test]
async fn unread_variant_does_not_override_arrow_schema_hint() {
    let variant = required_variant_schema().field(0).clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date64, false),
        variant,
    ]));
    let mut builder = VariantBuilder::new();
    builder.append_value(Variant::Null);
    let (metadata, value) = builder.finish();
    let physical = StructArray::new(
        match schema.field(1).data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(BinaryArray::from(vec![value.as_slice()])),
            Arc::new(BinaryArray::from(vec![metadata.as_slice()])),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date64Array::from(vec![86_400_000])),
            Arc::new(physical),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let required = Arc::new(Schema::new(vec![Field::new("d", DataType::Date64, false)]));
    let session = Arc::new(SessionContext::new());
    let scan = init_datasource_exec(
        required,
        None,
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![vec![PartitionedFile::from_path(
            file.path().to_string_lossy().into_owned(),
        )
        .unwrap()]],
        None,
        None,
        None,
        "UTC",
        true,
        false,
        false,
        false,
        &session,
        false,
        false,
        false,
        false,
    )
    .unwrap();
    let mut stream = scan.execute(0, session.task_ctx()).unwrap();
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(output.column(0).data_type(), &DataType::Date64);
    assert_eq!(output.num_columns(), 1);
}

#[test]
fn encrypted_projected_variant_is_rejected_before_reader_creation() {
    let session = Arc::new(SessionContext::new());
    let result = init_datasource_exec(
        required_variant_schema(),
        None,
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![],
        None,
        None,
        None,
        "UTC",
        true,
        false,
        false,
        false,
        &session,
        true,
        false,
        false,
        false,
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("requires Spark fallback"));
}

#[tokio::test]
async fn variant_scan_uses_parquet_physical_types_instead_of_arrow_schema_hints() {
    let decimal: ArrayRef = Arc::new(
        Decimal256Array::from(vec![i256::from_i128(123)])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let output = write_and_scan_shredded_variant(decimal, false).await;
    assert_eq!(
        output.value(0),
        Variant::Decimal4(VariantDecimal4::try_new(123, 2).unwrap())
    );

    let date64: ArrayRef = Arc::new(Date64Array::from(vec![86_400_000]));
    let output = write_and_scan_shredded_variant(Arc::clone(&date64), false).await;
    assert_eq!(output.value(0).as_int64(), Some(86_400_000));

    let output = write_and_scan_shredded_variant(date64, true).await;
    let Variant::Date(date) = output.value(0) else {
        panic!("expected DATE-annotated physical value")
    };
    assert_eq!(date.to_string(), "1970-01-02");
}

#[tokio::test]
async fn variant_scan_preserves_parquet_enum_string_and_binary_semantics() {
    for (logical_type, expected_string) in [
        (Some(LogicalType::Enum), true),
        (Some(LogicalType::String), true),
        (None, false),
    ] {
        let typed_value = Arc::new(
            ParquetType::primitive_type_builder("typed_value", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(logical_type)
                .build()
                .unwrap(),
        );
        let filename = write_variant_typed_value::<ByteArrayType>(
            typed_value,
            &[ByteArray::from(b"red".to_vec())],
        );

        let output = scan_variant_file(filename).await;
        if expected_string {
            assert_eq!(output.value(0).as_string(), Some("red"));
        } else {
            assert_eq!(output.value(0), Variant::Binary(b"red"));
        }
    }
}

#[tokio::test]
async fn variant_scan_reads_wide_physical_decimal_as_decimal128() {
    for width in [17, 32] {
        let values = [123_i128, -123_i128]
            .into_iter()
            .map(|value| {
                let mut bytes = vec![if value.is_negative() { 0xff } else { 0 }; width];
                bytes[width - 16..].copy_from_slice(&value.to_be_bytes());
                FixedLenByteArray::from(bytes)
            })
            .collect::<Vec<_>>();
        let typed_value = Arc::new(
            ParquetType::primitive_type_builder("typed_value", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::decimal(2, 38)))
                .with_length(width as i32)
                .with_precision(38)
                .with_scale(2)
                .build()
                .unwrap(),
        );
        let filename = write_variant_typed_value::<FixedLenByteArrayType>(typed_value, &values);

        let output = scan_variant_file(filename).await;
        for (index, value) in [123, -123].into_iter().enumerate() {
            assert_eq!(
                output.value(index),
                Variant::Decimal4(VariantDecimal4::try_new(value, 2).unwrap())
            );
        }
    }
}

/// The two-field storage Spark writes for a Variant, declared without the Variant marker, which is
/// the shape of a hand-written `struct<value binary, metadata binary>` read schema.
fn plain_variant_storage() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ]))
}

/// The relation data schema `CometNativeScan` sends: every root, with `v` as a plain struct.
fn id_and_plain_variant_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("v", plain_variant_storage(), true),
    ]))
}

/// Writes `id INT` next to a VARIANT-annotated `v`. With `rows == 0` the file has no row group.
fn write_id_and_annotated_variant(rows: usize) -> PathBuf {
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("v", plain_variant_storage(), true).with_extension_type(VariantType),
    ]));
    let filename = get_temp_filename();
    let mut writer = ArrowWriter::try_new_with_options(
        File::create(&filename).unwrap(),
        Arc::clone(&file_schema),
        ArrowWriterOptions::new().with_skip_arrow_metadata(true),
    )
    .unwrap();
    if rows > 0 {
        let DataType::Struct(storage_fields) = plain_variant_storage() else {
            unreachable!()
        };
        let variant = StructArray::new(
            storage_fields,
            vec![
                Arc::new(BinaryArray::from(vec![Some(&[12u8, 1u8][..])])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![Some(&[1u8, 0u8, 0u8][..])])) as ArrayRef,
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            file_schema,
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(variant) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    filename
}

/// Scans through `init_datasource_exec` the way `CometNativeScan` drives it: the full data
/// schema, the Spark read schema, and a projection into the data schema. Returns the row count or
/// the error the scan raised.
async fn scan_with_read_schema(
    filename: PathBuf,
    required_schema: SchemaRef,
    projection: Vec<usize>,
) -> Result<usize, datafusion::common::DataFusionError> {
    let partitioned_file =
        PartitionedFile::from_path(filename.to_string_lossy().into_owned()).unwrap();
    let session_ctx = Arc::new(SessionContext::new());
    let scan = init_datasource_exec(
        required_schema,
        Some(id_and_plain_variant_schema()),
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![vec![partitioned_file]],
        Some(projection),
        None,
        None,
        "UTC",
        false,
        false,
        false,
        false,
        &session_ctx,
        false,
        false,
        false,
        false,
    )
    .unwrap();
    let mut stream = scan.execute(0, session_ctx.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    Ok(rows)
}

fn read_schema_id_only() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]))
}

fn assert_variant_annotation_rejected(result: Result<usize, datafusion::common::DataFusionError>) {
    let err = result.expect_err("reading the annotated `v` as a plain struct must be rejected");
    assert!(
        err.to_string().contains("_LEGACY_ERROR_TEMP_3071"),
        "unexpected error: {err}"
    );
}

/// Spark's schema pruning keeps unrequested roots in the relation data schema, so `v` reaches the
/// native scan even when only `id` is read. The check must follow the read schema, not the data
/// schema, or selecting `id` alone fails on a column the query never touches.
#[tokio::test]
async fn annotated_root_outside_the_read_schema_is_not_rejected() {
    let rows = scan_with_read_schema(
        write_id_and_annotated_variant(1),
        read_schema_id_only(),
        vec![0],
    )
    .await
    .expect("`v` is not in the read schema, so its annotation must not be checked");
    assert_eq!(rows, 1);
}

/// Once `v` is part of the read schema, the plain struct request is rejected as Spark rejects it.
#[tokio::test]
async fn annotated_root_inside_the_read_schema_is_rejected() {
    assert_variant_annotation_rejected(
        scan_with_read_schema(
            write_id_and_annotated_variant(1),
            id_and_plain_variant_schema(),
            vec![0, 1],
        )
        .await,
    );
}

/// Scoping the check to the read schema must not move it to row groups: a requested annotated root
/// in a file with no row group is still rejected, matching Spark's schema-conversion-time check.
#[tokio::test]
async fn requested_annotated_root_is_rejected_on_an_empty_file() {
    assert_variant_annotation_rejected(
        scan_with_read_schema(
            write_id_and_annotated_variant(0),
            id_and_plain_variant_schema(),
            vec![0, 1],
        )
        .await,
    );
}

/// The empty-file counterpart of `annotated_root_outside_the_read_schema_is_not_rejected`.
#[tokio::test]
async fn unrequested_annotated_root_is_not_rejected_on_an_empty_file() {
    let rows = scan_with_read_schema(
        write_id_and_annotated_variant(0),
        read_schema_id_only(),
        vec![0],
    )
    .await
    .expect("`v` is not in the read schema, so its annotation must not be checked");
    assert_eq!(rows, 0);
}
