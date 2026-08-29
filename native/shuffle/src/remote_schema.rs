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

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::error::Result;

/// Check a remotely fetched batch against the logical types declared by Spark before any cast
/// or JVM Arrow import. Arrow buffer validation alone cannot detect a valid but incorrect IPC
/// type, and a general cast can silently change values or turn them into nulls.
///
/// Native shuffle writers align their input with Spark's types before partitioning; row writers
/// build those types directly. Row writers may use top-level Int32-keyed string/binary
/// dictionaries, but disable dictionaries inside containers. Do not accept other dictionary
/// layouts just because Arrow can cast them: the JVM importer does not support all of them.
///
/// Nested nullability and metadata are normalized separately. List element and map entries names
/// are synthetic (for example, Rust uses `item` while the JVM uses `element`), but struct member
/// names and order must agree: Arrow's struct cast can otherwise reorder values by name.
/// Map entries and keys must remain non-nullable, as required by the JVM Arrow importer.
pub fn validate_remote_schema(batch: &RecordBatch, expected_types: &[DataType]) -> Result<()> {
    if batch.num_columns() != expected_types.len() {
        return Err(DataFusionError::Execution(format!(
            "Shuffle block column count mismatch: got {} but expected {}",
            batch.num_columns(),
            expected_types.len()
        )));
    }

    for (index, (column, expected)) in batch.columns().iter().zip(expected_types).enumerate() {
        let actual = column.data_type();
        let value_type = match actual {
            DataType::Dictionary(keys, values)
                if keys.as_ref() == &DataType::Int32
                    && matches!(values.as_ref(), DataType::Utf8 | DataType::Binary) =>
            {
                values.as_ref()
            }
            _ => actual,
        };
        if !same_logical_type(value_type, expected) {
            return Err(DataFusionError::Execution(format!(
                "Shuffle block type mismatch at column {index}: got {actual} but expected {expected}"
            )));
        }
    }
    Ok(())
}

fn same_logical_type(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::List(a), DataType::List(e))
        | (DataType::LargeList(a), DataType::LargeList(e)) => {
            same_logical_type(a.data_type(), e.data_type())
        }
        (DataType::FixedSizeList(a, a_len), DataType::FixedSizeList(e, e_len)) => {
            a_len == e_len && same_logical_type(a.data_type(), e.data_type())
        }
        (DataType::Map(a, a_sorted), DataType::Map(e, e_sorted)) => {
            !a.is_nullable()
                && matches!(a.data_type(), DataType::Struct(fields)
                    if fields.len() == 2 && !fields[0].is_nullable())
                && a_sorted == e_sorted
                && same_logical_type(a.data_type(), e.data_type())
        }
        (DataType::Struct(a), DataType::Struct(e)) => {
            a.len() == e.len()
                && a.iter().zip(e.iter()).all(|(a, e)| {
                    a.name() == e.name() && same_logical_type(a.data_type(), e.data_type())
                })
        }
        // Do not broaden this to can_cast_types: signedness, width, temporal units/timezones,
        // decimal precision/scale, and string/binary conversions change Spark's logical type.
        _ => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use super::validate_remote_schema;
    use crate::read_ipc_compressed_validated;
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BinaryDictionaryBuilder, Decimal128Array, Int32Array,
        ListArray, MapArray, RecordBatch, StringArray, StringDictionaryBuilder, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, IntervalUnit, Schema, TimeUnit};
    use arrow::ipc::writer::StreamWriter;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn batch_of_type(data_type: DataType) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "wire_name",
            data_type,
            true,
        )])))
    }

    fn list(data_type: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(data_type, true)))
    }

    fn structure(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect(),
        )
    }

    fn map(value_type: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value_type, true),
                ])),
                false,
            )),
            false,
        )
    }

    fn dictionary(key_type: DataType, value_type: DataType) -> DataType {
        DataType::Dictionary(Box::new(key_type), Box::new(value_type))
    }

    fn roundtrip(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, column)| Field::new(format!("wire_{i}"), column.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut bytes = b"NONE".to_vec();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        read_ipc_compressed_validated(&bytes).unwrap()
    }

    #[test]
    fn column_count_and_empty_schema_are_checked() {
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        validate_remote_schema(&empty, &[]).unwrap();
        for expected in [vec![], vec![DataType::Int32, DataType::Int32]] {
            let error = validate_remote_schema(&batch_of_type(DataType::Int32), &expected)
                .unwrap_err()
                .to_string();
            assert!(error.contains("column count mismatch"), "{error}");
        }
    }

    #[test]
    fn incompatible_scalar_logical_types_are_rejected() {
        use DataType::*;
        let utc = Some("UTC".into());
        for (actual, expected) in [
            (UInt32, Int32),
            (Int16, Int32),
            (Int64, Int32),
            (Int32, Float32),
            (Float32, Float64),
            (Float64, Int64),
            (Utf8, Binary),
            (LargeUtf8, Utf8),
            (Utf8View, Utf8),
            (BinaryView, Binary),
            (LargeBinary, Binary),
            (Null, Int32),
            (Date32, Int32),
            (Date64, Date32),
            (
                Timestamp(TimeUnit::Millisecond, utc.clone()),
                Timestamp(TimeUnit::Microsecond, utc.clone()),
            ),
            (
                Timestamp(TimeUnit::Microsecond, None),
                Timestamp(TimeUnit::Microsecond, utc.clone()),
            ),
            (
                Timestamp(TimeUnit::Microsecond, utc.clone()),
                Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into())),
                Timestamp(TimeUnit::Microsecond, utc),
            ),
            (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)),
            (
                Duration(TimeUnit::Millisecond),
                Duration(TimeUnit::Microsecond),
            ),
            (Decimal128(12, 2), Decimal128(12, 3)),
            (Decimal128(12, 2), Decimal128(10, 2)),
            (Decimal64(12, 2), Decimal128(12, 2)),
        ] {
            let error = validate_remote_schema(
                &batch_of_type(actual.clone()),
                std::slice::from_ref(&expected),
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("type mismatch at column 0"), "{error}");
            assert!(error.contains(&actual.to_string()), "{error}");
            assert!(error.contains(&expected.to_string()), "{error}");
        }
    }

    #[test]
    fn nested_logical_type_and_shape_changes_are_rejected() {
        use DataType::*;
        for (actual, expected) in [
            (list(UInt32), list(Int32)),
            (
                structure(&[("payload", list(UInt32))]),
                structure(&[("payload", list(Int32))]),
            ),
            (map(UInt32), map(Int32)),
            (structure(&[("a", Int32)]), structure(&[("b", Int32)])),
            (
                structure(&[("a", Int32), ("b", Int32)]),
                structure(&[("b", Int32), ("a", Int32)]),
            ),
            (
                structure(&[("a", Int32), ("b", Int32)]),
                structure(&[("a", Int32)]),
            ),
            (
                LargeList(Arc::new(Field::new_list_field(Int32, true))),
                list(Int32),
            ),
            (
                FixedSizeList(Arc::new(Field::new_list_field(Int32, true)), 2),
                FixedSizeList(Arc::new(Field::new_list_field(Int32, true)), 3),
            ),
        ] {
            let error = validate_remote_schema(&batch_of_type(actual), &[expected])
                .unwrap_err()
                .to_string();
            assert!(error.contains("type mismatch"), "{error}");
        }
    }

    #[test]
    fn unsupported_or_wrongly_typed_dictionaries_are_rejected() {
        use DataType::*;
        for (actual, expected) in [
            (dictionary(Int32, UInt32), Int32),
            (dictionary(Int32, Utf8), Int32),
            (dictionary(Int32, Int32), Int32),
            (dictionary(Int32, Null), Null),
            (
                dictionary(Int32, Interval(IntervalUnit::MonthDayNano)),
                Interval(IntervalUnit::MonthDayNano),
            ),
            (dictionary(Int8, Utf8), Utf8),
            (dictionary(UInt32, Utf8), Utf8),
            (dictionary(Int32, list(Utf8)), list(Utf8)),
            (dictionary(Int32, dictionary(Int32, Utf8)), Utf8),
            (list(dictionary(Int32, Utf8)), list(Utf8)),
        ] {
            let error = validate_remote_schema(&batch_of_type(actual), &[expected])
                .unwrap_err()
                .to_string();
            assert!(error.contains("type mismatch"), "{error}");
        }
    }

    #[test]
    fn map_keys_must_remain_non_nullable() {
        let actual = DataType::Map(
            Arc::new(Field::new(
                "entries",
                structure(&[("key", DataType::Utf8), ("value", DataType::Int32)]),
                false,
            )),
            false,
        );
        let error = validate_remote_schema(&batch_of_type(actual), &[map(DataType::Int32)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("type mismatch"), "{error}");
    }

    #[test]
    fn ordinary_and_dictionary_values_survive_validation() {
        let mut strings = StringDictionaryBuilder::<Int32Type>::new();
        strings.append_value("hello");
        strings.append_null();
        strings.append_value("hello");
        let mut binaries = BinaryDictionaryBuilder::<Int32Type>::new();
        binaries.append_value(b"\0\xff");
        binaries.append_null();
        binaries.append_value(b"\0\xff");
        let decimals = Decimal128Array::from(vec![Some(-12345), None, Some(999999999999)])
            .with_precision_and_scale(12, 2)
            .unwrap();
        let timestamps =
            TimestampMicrosecondArray::from(vec![Some(-1), None, Some(1727398000000001)])
                .with_timezone("UTC");
        let batch = roundtrip(vec![
            Arc::new(Int32Array::from(vec![Some(-1), None, Some(i32::MAX)])),
            Arc::new(strings.finish()),
            Arc::new(binaries.finish()),
            Arc::new(decimals.clone()),
            Arc::new(timestamps.clone()),
        ]);
        validate_remote_schema(
            &batch,
            &[
                DataType::Int32,
                DataType::Utf8,
                DataType::Binary,
                decimals.data_type().clone(),
                timestamps.data_type().clone(),
            ],
        )
        .unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(-1), None, Some(i32::MAX)])
        );
        let strings = arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        assert_eq!(
            strings.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("hello"), None, Some("hello")])
        );
        let binaries = arrow::compute::cast(batch.column(2), &DataType::Binary).unwrap();
        assert_eq!(
            binaries.as_any().downcast_ref::<BinaryArray>().unwrap(),
            &BinaryArray::from(vec![Some(&b"\0\xff"[..]), None, Some(&b"\0\xff"[..])])
        );
        assert_eq!(batch.column(3).to_data(), decimals.to_data());
        assert_eq!(batch.column(4).to_data(), timestamps.to_data());
    }

    #[test]
    fn map_entry_names_and_null_values_are_compatible() {
        let fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let keys = StringArray::from(vec!["one", "two"]);
        let values = Int32Array::from(vec![Some(1), None]);
        let entries = StructArray::new(
            fields.clone(),
            vec![Arc::new(keys.clone()), Arc::new(values.clone())],
            None,
        );
        let column = MapArray::new(
            Arc::new(Field::new("map_entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        );
        let batch = roundtrip(vec![Arc::new(column)]);
        let expected = map(DataType::Int32);
        validate_remote_schema(&batch, std::slice::from_ref(&expected)).unwrap();
        let reconciled = arrow::compute::cast(batch.column(0), &expected).unwrap();
        let result = reconciled.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(result.value_offsets(), &[0, 2]);
        assert_eq!(result.keys().to_data(), keys.to_data());
        assert_eq!(result.values().to_data(), values.to_data());
    }

    #[test]
    fn nested_nullability_metadata_and_synthetic_names_are_compatible() {
        let fields = Fields::from(vec![Field::new("id", DataType::Int32, false)]);
        let values = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1, -1, 3]))],
            None,
        ));
        let column = Arc::new(ListArray::new(
            Arc::new(Field::new("element", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            values,
            None,
        ));
        let batch = roundtrip(vec![column]);
        let expected = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int32, true)
                .with_metadata(HashMap::from([(
                    "parquet.field.id".to_owned(),
                    "1".to_owned(),
                )]))])),
            true,
        )));
        validate_remote_schema(&batch, std::slice::from_ref(&expected)).unwrap();
        let reconciled = arrow::compute::cast(batch.column(0), &expected).unwrap();
        let list = reconciled.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_offsets(), &[0, 2, 3]);
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            values
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, -1, 3]
        );
        // The existing normalization also supports narrowing nullability when there are no nulls.
        validate_remote_schema(
            &RecordBatch::try_from_iter([("normalized", reconciled)]).unwrap(),
            &[batch.column(0).data_type().clone()],
        )
        .unwrap();
    }
}
