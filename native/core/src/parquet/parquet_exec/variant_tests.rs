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
    variant::{Variant, VariantArray, VariantBuilder, VariantDecimal16},
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
        "",
        "",
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
        "",
        "",
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
        "",
        "",
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
        Variant::Decimal16(VariantDecimal16::try_new(123, 2).unwrap())
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
                Variant::Decimal16(VariantDecimal16::try_new(value, 2).unwrap())
            );
        }
    }
}
