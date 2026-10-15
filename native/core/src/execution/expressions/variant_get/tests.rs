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
use arrow::{
    array::{BinaryArray, StructArray},
    buffer::NullBuffer,
    datatypes::{
        Decimal128Type, Field, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
    },
};
use datafusion::physical_expr::expressions::Column;
use parquet::variant::{VariantBuilder, VariantDecimal16};

fn input(rows: &[Option<Variant<'_, '_>>]) -> ArrayRef {
    let mut values = Vec::new();
    let mut metadata = Vec::new();
    for row in rows {
        let mut builder = VariantBuilder::new();
        builder.append_value(row.clone().unwrap_or(Variant::Null));
        let (m, v) = builder.finish();
        metadata.push(m);
        values.push(v);
    }
    Arc::new(StructArray::new(
        vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]
        .into(),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                values.iter().map(Vec::as_slice),
            )),
            Arc::new(BinaryArray::from_iter_values(
                metadata.iter().map(Vec::as_slice),
            )),
        ],
        Some(NullBuffer::from(
            rows.iter().map(Option::is_some).collect::<Vec<_>>(),
        )),
    ))
}

fn expression(target: DataType, fail_on_error: bool) -> VariantGet {
    VariantGet {
        child: Arc::new(Column::new("v", 0)),
        path: vec![],
        path_sql: "$".into(),
        target_sql: target.to_string(),
        target,
        fail_on_error,
        size_limit: 16 * 1024 * 1024,
        cast_options: SparkCastOptions::new_with_version(
            EvalMode::Try,
            "America/Los_Angeles",
            true,
            true,
        ),
    }
}

#[test]
fn scalar_casts_and_nulls() {
    let data = input(&[
        Some(Variant::Int8(12)),
        Some(Variant::String("-4")),
        Some(Variant::BooleanTrue),
        Some(Variant::Double(4.9)),
        Some(Variant::Null),
        None,
        Some(Variant::String("bad")),
        Some(Variant::Int64(i64::MAX)),
    ]);
    let result = expression(DataType::Int32, false)
        .evaluate_array(&data)
        .unwrap();
    assert_eq!(
        result
            .as_primitive::<Int32Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(12), Some(-4), Some(1), Some(4), None, None, None, None]
    );
    let error = expression(DataType::Int32, true)
        .evaluate_array(&data)
        .unwrap_err()
        .to_string();
    assert!(error.contains("INVALID_VARIANT_CAST"), "{error}");
}

#[test]
fn floating_point_bigint_boundaries() {
    let boundary = 9223372036854775808.0_f64;
    let valid = input(&[
        Some(Variant::Double(boundary)),
        Some(Variant::Float(boundary as f32)),
        Some(Variant::Double(-boundary)),
        Some(Variant::Float(-boundary as f32)),
        Some(Variant::Double(boundary.next_down())),
        Some(Variant::Double((-boundary).next_up())),
        None,
        Some(Variant::Null),
    ]);
    for strict in [false, true] {
        let result = expression(DataType::Int64, strict)
            .evaluate_array(&valid)
            .unwrap();
        assert_eq!(
            result
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![
                Some(i64::MAX),
                Some(i64::MAX),
                Some(i64::MIN),
                Some(i64::MIN),
                Some(9223372036854774784),
                Some(-9223372036854774784),
                None,
                None
            ]
        );
    }
    for value in [
        Variant::Double(boundary.next_up()),
        Variant::Double((-boundary).next_down()),
        Variant::Float((boundary as f32).next_up()),
        Variant::Float((-boundary as f32).next_down()),
        Variant::Double(f64::NAN),
        Variant::Double(f64::INFINITY),
        Variant::Float(f32::NEG_INFINITY),
    ] {
        let invalid = input(&[Some(value)]);
        assert!(expression(DataType::Int64, false)
            .evaluate_array(&invalid)
            .unwrap()
            .is_null(0));
        assert!(expression(DataType::Int64, true)
            .evaluate_array(&invalid)
            .unwrap_err()
            .to_string()
            .contains("INVALID_VARIANT_CAST"));
    }
}

#[test]
fn variant_timestamp_overflow_is_checked() {
    let data = input(&[
        Some(Variant::Int64(1)),
        Some(Variant::Int64(i64::MAX)),
        Some(Variant::Decimal16(
            VariantDecimal16::try_new(-1234567, 6).unwrap(),
        )),
        Some(Variant::Decimal16(
            VariantDecimal16::try_new(i128::MAX / 10, 0).unwrap(),
        )),
    ]);
    let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let result = expression(target.clone(), false)
        .evaluate_array(&data)
        .unwrap();
    assert_eq!(
        result
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1_000_000), None, Some(-1_234_567), None]
    );
    assert!(expression(target, true)
        .evaluate_array(&data)
        .unwrap_err()
        .to_string()
        .contains("INVALID_VARIANT_CAST"));
}

#[test]
fn scalar_cast_matrix_rejects_arrow_only_conversions() {
    let data = input(&[
        Some(Variant::Int64(1)),
        Some(Variant::String("hello")),
        Some(Variant::Binary(b"bytes")),
    ]);
    let result = expression(DataType::Binary, false)
        .evaluate_array(&data)
        .unwrap();
    assert_eq!(
        result.as_binary::<i32>().iter().collect::<Vec<_>>(),
        vec![None, Some(b"hello".as_slice()), Some(b"bytes".as_slice())]
    );
    let bool_input = input(&[Some(Variant::BooleanTrue)]);
    let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    assert!(expression(target, false)
        .evaluate_array(&bool_input)
        .unwrap()
        .is_null(0));
}

#[test]
fn timestamp_numeric_and_boolean_decimal_casts() {
    let values = [-1_500_001_i64, 1_500_001, i64::MAX].map(|micros| {
        let mut bytes = vec![12 << 2];
        bytes.extend(micros.to_le_bytes());
        bytes
    });
    let data: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]
        .into(),
        vec![
            Arc::new(BinaryArray::from_iter_values(values.iter())),
            Arc::new(BinaryArray::from_iter_values([&[1u8]; 3])),
        ],
        None,
    ));
    let ints = expression(DataType::Int32, false)
        .evaluate_array(&data)
        .unwrap();
    assert_eq!(
        ints.as_primitive::<Int32Type>().iter().collect::<Vec<_>>(),
        vec![Some(-2), Some(1), None]
    );
    let doubles = expression(DataType::Float64, false)
        .evaluate_array(&data)
        .unwrap();
    assert_eq!(doubles.as_primitive::<Float64Type>().value(0), -1.500001);
    let decimals = expression(DataType::Decimal128(22, 6), false)
        .evaluate_array(&data)
        .unwrap();
    assert_eq!(
        decimals
            .as_primitive::<Decimal128Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-1_500_001), Some(1_500_001), Some(i64::MAX as i128)]
    );
    let bools = input(&[Some(Variant::BooleanTrue), Some(Variant::BooleanFalse)]);
    let result = expression(DataType::Decimal128(3, 1), false)
        .evaluate_array(&bools)
        .unwrap();
    assert_eq!(
        result
            .as_primitive::<Decimal128Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(10), Some(0)]
    );
}

#[test]
fn unknown_primitive_and_constructor_limits_are_not_missing_values() {
    let mut expr = expression(DataType::Int32, false);
    expr.path = vec![PathSegment::Key("missing".into())];
    assert!(expr
        .extract(&[1], &[17 << 2])
        .unwrap_err()
        .to_string()
        .contains("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT"));
    assert!(expr
        .extract(&vec![1; 16 * 1024 * 1024 + 1], &[0])
        .unwrap_err()
        .to_string()
        .contains("VARIANT_CONSTRUCTOR_SIZE_LIMIT"));
    // Unused dictionary contents are deliberately not validated for primitive getters.
    expr.path.clear();
    assert_eq!(
        expr.extract(&[1], &[12, 5]).unwrap(),
        Some([12, 5].as_slice())
    );
    assert_eq!(
        expr.scalar_for_cast(&[12, 5]).unwrap(),
        Some(ScalarValue::Int64(Some(5)))
    );
}

#[test]
fn empty_keys_and_unsorted_metadata_use_shallow_traversal() {
    // Spark's insertion-ordered dictionary has repeated offsets for the empty key.
    let metadata = [1, 3, 0, 1, 1, 2, b'z', b'a'];
    // Object fields are sorted by key: empty, a, z. Values are 1, 2, 3.
    let value = [2, 3, 1, 2, 0, 0, 2, 4, 6, 12, 1, 12, 2, 12, 3];
    let mut expr = expression(DataType::Int32, true);
    expr.path = vec![PathSegment::Key("".into())];
    assert_eq!(
        expr.extract(&metadata, &value).unwrap().unwrap(),
        &[12, 1, 12, 2, 12, 3]
    );
    expr.path = vec![PathSegment::Key("missing".into())];
    assert!(expr.extract(&metadata, &value).unwrap().is_none());
}

#[test]
fn duplicate_large_object_keys_use_sparks_binary_search_midpoint() {
    let metadata = [1, 1, 0, 1, b'x'];
    let mut value = vec![2, 32];
    value.extend([0; 32]);
    value.extend((0..=32).map(|i| i * 2));
    value.extend((0..32).flat_map(|i| [12, i]));
    let mut expr = expression(DataType::Int32, true);
    expr.path = vec![PathSegment::Key("x".into())];
    let selected = expr.extract(&metadata, &value).unwrap().unwrap();
    assert_eq!(
        expr.scalar_for_cast(selected).unwrap(),
        Some(ScalarValue::Int64(Some(15)))
    );
}

#[test]
fn wrapped_metadata_address_can_resolve_an_empty_key() {
    let metadata = [193, 254, 255, 255, 63, 0, 0, 0, 0, 0, 0, 0, 0];
    let value = [2, 1, 0, 0, 1, 0];
    let mut expr = expression(DataType::Int32, true);
    expr.path = vec![PathSegment::Key("".into())];
    assert_eq!(
        expr.extract(&metadata, &value).unwrap(),
        Some([0].as_slice())
    );
}

#[test]
fn wrapped_array_addresses_use_the_original_value_buffer() {
    let value = [31, 255, 255, 255, 63, 0, 0, 0, 0];
    let mut expr = expression(DataType::Int32, true);
    expr.path = vec![PathSegment::Index(0)];
    assert_eq!(
        expr.extract(&[1], &value).unwrap(),
        Some(value[5..].as_ref())
    );
    // Put the same wrapped array after a normal outer array header.
    let mut nested = vec![3, 1, 0, 9];
    nested.extend(value);
    expr.path.push(PathSegment::Index(0));
    assert_eq!(
        expr.extract(&[1], &nested).unwrap(),
        Some(nested[9..].as_ref())
    );
}

#[test]
fn raw_temporal_values_preserve_full_spark_range() {
    for days in [i32::MIN, i32::MAX] {
        let mut bytes = vec![11 << 2];
        bytes.extend(days.to_le_bytes());
        assert_eq!(
            expression(DataType::Date32, true)
                .scalar_for_cast(&bytes)
                .unwrap(),
            Some(ScalarValue::Date32(Some(days)))
        );
    }
    for micros in [i64::MIN, i64::MAX] {
        for (tag, zone) in [(12, Some("UTC".into())), (13, None)] {
            let mut bytes = vec![tag << 2];
            bytes.extend(micros.to_le_bytes());
            let target = DataType::Timestamp(TimeUnit::Microsecond, zone.clone());
            assert_eq!(
                expression(target, true).scalar_for_cast(&bytes).unwrap(),
                Some(ScalarValue::TimestampMicrosecond(Some(micros), zone))
            );
        }
    }
}

#[test]
fn field_identity_and_malformed_data() {
    let data = input(&[Some(Variant::Int32(1))]);
    let proto = spark_expression::VariantGet {
        datatype: Some(spark_expression::DataType {
            type_id: 3,
            type_info: None,
        }),
        timezone: "UTC".into(),
        size_limit: 16 * 1024 * 1024,
        ..Default::default()
    };
    let ordinary = Schema::new(vec![Field::new("v", data.data_type().clone(), true)]);
    assert!(VariantGet::try_new(Arc::new(Column::new("v", 0)), &proto, &ordinary).is_err());
    let schema = Arc::new(Schema::new(vec![ordinary
        .field(0)
        .clone()
        .with_extension_type(VariantType)]));
    let expr = VariantGet::try_new(Arc::new(Column::new("v", 0)), &proto, &schema).unwrap();
    let malformed: ArrayRef = Arc::new(StructArray::new(
        data.as_struct().fields().clone(),
        vec![
            Arc::new(BinaryArray::from(vec![b"".as_slice()])),
            Arc::new(BinaryArray::from(vec![[1u8, 0, 0].as_slice()])),
        ],
        None,
    ));
    let batch = RecordBatch::try_new(schema, vec![malformed]).unwrap();
    assert!(expr
        .evaluate(&batch)
        .unwrap_err()
        .to_string()
        .contains("MALFORMED_VARIANT"));
}

#[test]
fn variant_literals_preserve_identity_and_parent_null_masking() {
    use arrow::record_batch::RecordBatchOptions;
    use datafusion::physical_expr::expressions::Literal;

    for row in [Some(Variant::Int8(7)), None] {
        let data = input(&[row]);
        let field =
            Field::new("v", data.data_type().clone(), true).with_extension_type(VariantType);
        let literal = Arc::new(Literal::new_with_metadata(
            ScalarValue::Struct(Arc::new(data.as_struct().clone())),
            Some(field.metadata().into()),
        ));
        let proto = spark_expression::VariantGet {
            datatype: Some(spark_expression::DataType {
                type_id: 3,
                type_info: None,
            }),
            timezone: "UTC".into(),
            size_limit: 16 * 1024 * 1024,
            ..Default::default()
        };
        let schema = Arc::new(Schema::empty());
        let expr = VariantGet::try_new(literal, &proto, &schema).unwrap();
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let result = expr.evaluate(&batch).unwrap().into_array(1).unwrap();
        assert_eq!(
            result
                .as_primitive::<Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            if data.is_null(0) {
                vec![None]
            } else {
                vec![Some(7)]
            }
        );
    }
}
