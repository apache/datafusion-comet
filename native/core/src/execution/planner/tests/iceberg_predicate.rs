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

use std::sync::Arc;

use datafusion_comet_proto::spark_operator::{
    iceberg_literal::Value, iceberg_predicate::Node, IcebergBinaryPredicate, IcebergDecimal,
    IcebergLiteral, IcebergLogicalPredicate, IcebergPredicate, IcebergPredicateOperator as Op,
    IcebergSetPredicate,
};
use iceberg::expr::{Bind, Reference};
use iceberg::spec::{NestedField, PrimitiveLiteral, PrimitiveType, Schema, Type};

use crate::execution::planner::{iceberg_literal_to_datum, iceberg_predicate_to_predicate};

fn decimal_literal(unscaled: Vec<u8>, precision: i32, scale: i32) -> IcebergLiteral {
    IcebergLiteral {
        is_null: false,
        value: Some(Value::DecimalVal(IcebergDecimal {
            unscaled,
            precision,
            scale,
        })),
    }
}

fn decimal_schema(precision: u32, scale: u32) -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "amount",
                Type::Primitive(PrimitiveType::Decimal { precision, scale }),
            ))])
            .build()
            .unwrap(),
    )
}

fn binary(literal: IcebergLiteral, op: Op) -> IcebergPredicate {
    IcebergPredicate {
        node: Some(Node::Binary(IcebergBinaryPredicate {
            column: "amount".to_string(),
            op: op as i32,
            value: Some(literal),
        })),
    }
}

// Keep these cases in sync with Scala's predicateLiteralToProto. Byte and Short use IntVal;
// UUID, Fixed and Binary are gated on the Scala side and are not predicate literal outputs.
fn predicate_literal_cases() -> Vec<(Value, PrimitiveType, PrimitiveLiteral)> {
    vec![
        (
            Value::BoolVal(true),
            PrimitiveType::Boolean,
            PrimitiveLiteral::Boolean(true),
        ),
        (
            Value::BoolVal(false),
            PrimitiveType::Boolean,
            PrimitiveLiteral::Boolean(false),
        ),
        (
            Value::IntVal(i32::MIN),
            PrimitiveType::Int,
            PrimitiveLiteral::Int(i32::MIN),
        ),
        (
            Value::IntVal(i32::MAX),
            PrimitiveType::Int,
            PrimitiveLiteral::Int(i32::MAX),
        ),
        (
            Value::LongVal(i64::MIN),
            PrimitiveType::Long,
            PrimitiveLiteral::Long(i64::MIN),
        ),
        (
            Value::LongVal(i64::MAX),
            PrimitiveType::Long,
            PrimitiveLiteral::Long(i64::MAX),
        ),
        (
            Value::FloatVal(-1.25),
            PrimitiveType::Float,
            PrimitiveLiteral::Float((-1.25).into()),
        ),
        (
            Value::FloatVal(-0.0),
            PrimitiveType::Float,
            PrimitiveLiteral::Float((-0.0).into()),
        ),
        (
            Value::DoubleVal(123.125),
            PrimitiveType::Double,
            PrimitiveLiteral::Double(123.125.into()),
        ),
        (
            Value::DoubleVal(-0.0),
            PrimitiveType::Double,
            PrimitiveLiteral::Double((-0.0).into()),
        ),
        (
            Value::DateVal(-1),
            PrimitiveType::Date,
            PrimitiveLiteral::Int(-1),
        ),
        (
            Value::DateVal(20000),
            PrimitiveType::Date,
            PrimitiveLiteral::Int(20000),
        ),
        (
            Value::TimestampVal(-1),
            PrimitiveType::Timestamp,
            PrimitiveLiteral::Long(-1),
        ),
        (
            Value::TimestampVal(1_234_567_890_123_456),
            PrimitiveType::Timestamp,
            PrimitiveLiteral::Long(1_234_567_890_123_456),
        ),
        (
            Value::TimestampTzVal(-1),
            PrimitiveType::Timestamptz,
            PrimitiveLiteral::Long(-1),
        ),
        (
            Value::TimestampTzVal(1_234_567_890_123_456),
            PrimitiveType::Timestamptz,
            PrimitiveLiteral::Long(1_234_567_890_123_456),
        ),
        (
            Value::StringVal(String::new()),
            PrimitiveType::String,
            PrimitiveLiteral::String(String::new()),
        ),
        (
            Value::StringVal("Comet 小數\0".to_string()),
            PrimitiveType::String,
            PrimitiveLiteral::String("Comet 小數\0".to_string()),
        ),
        (
            Value::DecimalVal(IcebergDecimal {
                unscaled: vec![0xcf, 0xc7],
                precision: 9,
                scale: 2,
            }),
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            PrimitiveLiteral::Int128(-12345),
        ),
    ]
}

#[test]
fn every_scala_predicate_literal_decodes_to_the_expected_type_and_value() {
    for (value, expected_type, expected_value) in predicate_literal_cases() {
        let literal = IcebergLiteral {
            is_null: false,
            value: Some(value),
        };
        let datum = iceberg_literal_to_datum(&literal).expect("Scala-emitted literal must decode");
        assert_eq!(datum.data_type(), &expected_type, "{literal:?}");
        assert_eq!(datum.literal(), &expected_value, "{literal:?}");

        let mut null_literal = literal;
        null_literal.is_null = true;
        assert!(iceberg_literal_to_datum(&null_literal).is_none());
    }
}

#[test]
fn every_scala_predicate_literal_preserves_both_and_or_children_and_binds() {
    for (value, field_type, _) in predicate_literal_cases() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([
                    Arc::new(NestedField::optional(
                        1,
                        "amount",
                        Type::Primitive(field_type.clone()),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "other",
                        Type::Primitive(field_type),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let literal = IcebergLiteral {
            is_null: false,
            value: Some(value),
        };
        let datum = iceberg_literal_to_datum(&literal).expect("Scala-emitted literal must decode");
        let left = Reference::new("amount").equal_to(datum.clone());
        let right = Reference::new("other").not_equal_to(datum);
        let left_proto = binary(literal.clone(), Op::Eq);
        let right_proto = IcebergPredicate {
            node: Some(Node::Binary(IcebergBinaryPredicate {
                column: "other".to_string(),
                op: Op::NotEq as i32,
                value: Some(literal.clone()),
            })),
        };
        assert_eq!(
            iceberg_predicate_to_predicate(&left_proto),
            Some(left.clone())
        );
        assert_eq!(
            iceberg_predicate_to_predicate(&right_proto),
            Some(right.clone())
        );
        let logical = IcebergLogicalPredicate {
            left: Some(Box::new(left_proto)),
            right: Some(Box::new(right_proto)),
        };
        for (node, expected) in [
            (
                Node::And(Box::new(logical.clone())),
                left.clone().and(right.clone()),
            ),
            (Node::Or(Box::new(logical)), left.or(right)),
        ] {
            let proto = IcebergPredicate { node: Some(node) };
            let decoded = iceberg_predicate_to_predicate(&proto)
                .expect("both children of a Scala-emitted logical node must convert");
            // Check the full predicate, not just Some: dropping either child or swapping the
            // logical operator must fail, even if the remaining predicate can still bind.
            assert_eq!(decoded, expected, "{literal:?}");
            decoded
                .bind(Arc::clone(&schema), true)
                .expect("literal types must bind without coercion");
        }
    }
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "Iceberg residual logical node with an unconvertible child")]
fn combine_logical_asserts_when_a_child_did_not_convert() {
    use iceberg::expr::Predicate;

    let predicate = Reference::new("amount").equal_to(iceberg::spec::Datum::int(7));
    crate::execution::planner::combine_logical(Some(predicate), None, Predicate::and);
}

#[test]
fn decimal_literals_preserve_precision_scale_and_coefficient_and_bind() {
    for precision in [1, 9, 18, 38] {
        let max = 10_i128.pow(precision) - 1;
        for scale in [0, 2.min(precision), precision] {
            let schema = decimal_schema(precision, scale);
            // Include actual 38-digit coefficients, both signs, zero, and trailing zeros.
            let mut coefficients = vec![0, 1, -1, max, -max];
            if precision >= 9 {
                coefficients.extend([1200, -1200]);
            }
            for unscaled in coefficients {
                let literal = decimal_literal(
                    unscaled.to_be_bytes().to_vec(),
                    precision as i32,
                    scale as i32,
                );
                let datum = iceberg_literal_to_datum(&literal).expect("valid decimal literal");
                assert_eq!(
                    datum.data_type(),
                    &PrimitiveType::Decimal { precision, scale },
                    "coefficient={unscaled}"
                );
                assert_eq!(datum.literal(), &PrimitiveLiteral::Int128(unscaled));

                for op in [
                    Op::Eq,
                    Op::NotEq,
                    Op::LessThan,
                    Op::LessThanOrEq,
                    Op::GreaterThan,
                    Op::GreaterThanOrEq,
                ] {
                    let predicate = iceberg_predicate_to_predicate(&binary(literal.clone(), op))
                        .expect("decimal comparison must convert");
                    predicate
                        .rewrite_not()
                        .bind(Arc::clone(&schema), true)
                        .expect("decimal comparison must bind, not silently lose pushdown");
                }
            }
        }
    }
}

#[test]
fn decimal_literals_decode_java_signed_big_endian_bytes() {
    // BigInteger.toByteArray() emits a minimal signed representation, including a leading
    // sign byte for positive values whose highest data bit is set.
    for (bytes, expected) in [
        (vec![0], 0),
        (vec![0x7f], 127),
        (vec![0, 0x80], 128),
        (vec![0x80], -128),
        (vec![0xff, 0x7f], -129),
        (vec![0xff], -1),
        (vec![0xcf, 0xc7], -12345),
    ] {
        let literal = decimal_literal(bytes.clone(), 9, 2);
        let datum = iceberg_literal_to_datum(&literal).expect("valid signed decimal bytes");
        assert_eq!(
            datum.literal(),
            &PrimitiveLiteral::Int128(expected),
            "{bytes:?}"
        );
    }
}

#[test]
fn decimal_literals_decline_invalid_precision_or_scale() {
    for (precision, scale) in [
        (0, 0),
        (-1, 0),
        (39, 2),
        (i32::MAX, 0),
        (9, -1),
        (9, i32::MIN),
        (9, 10),
        (9, i32::MAX),
    ] {
        let literal = decimal_literal(vec![1], precision, scale);
        assert!(
            iceberg_literal_to_datum(&literal).is_none(),
            "precision={precision}, scale={scale}"
        );
    }
}

#[test]
fn decimal_literals_decline_invalid_bytes_or_precision_overflow() {
    for literal in [
        decimal_literal(vec![], 9, 2),
        decimal_literal(vec![0; 17], 38, 2),
        decimal_literal(vec![0xff; 17], 38, 2),
        decimal_literal(i128::MAX.to_be_bytes().to_vec(), 38, 2),
        decimal_literal(i128::MIN.to_be_bytes().to_vec(), 38, 2),
        decimal_literal(1000_i128.to_be_bytes().to_vec(), 3, 2),
        decimal_literal((-1000_i128).to_be_bytes().to_vec(), 3, 2),
    ] {
        assert!(iceberg_literal_to_datum(&literal).is_none(), "{literal:?}");
    }
}

#[test]
fn decimal_literals_decline_null_and_missing_values() {
    let mut literal = decimal_literal(vec![1], 9, 2);
    literal.is_null = true;
    assert!(iceberg_literal_to_datum(&literal).is_none());
    assert!(iceberg_literal_to_datum(&IcebergLiteral::default()).is_none());
}

#[test]
fn decimal_in_and_logical_predicates_convert_and_bind() {
    let literals = [100_i128, -500, 10_i128.pow(38) - 1]
        .map(|value| decimal_literal(value.to_be_bytes().to_vec(), 38, 2));
    let in_predicate = IcebergPredicate {
        node: Some(Node::Set(IcebergSetPredicate {
            column: "amount".to_string(),
            op: Op::In as i32,
            values: literals.to_vec(),
        })),
    };
    let logical = IcebergLogicalPredicate {
        left: Some(Box::new(binary(literals[0].clone(), Op::GreaterThan))),
        right: Some(Box::new(binary(literals[2].clone(), Op::LessThan))),
    };
    for node in [
        in_predicate.node.unwrap(),
        Node::And(Box::new(logical.clone())),
        Node::Or(Box::new(logical)),
        Node::Not(Box::new(binary(literals[1].clone(), Op::Eq))),
    ] {
        let proto = IcebergPredicate { node: Some(node) };
        let predicate = iceberg_predicate_to_predicate(&proto).expect("decimal residual converts");
        predicate
            .rewrite_not()
            .bind(decimal_schema(38, 2), true)
            .expect("complete decimal residual binds");
    }
}

#[test]
fn decimal_in_declines_the_entire_set_if_a_literal_is_invalid() {
    let valid = decimal_literal(vec![100], 9, 2);
    let invalid = decimal_literal(vec![1], 9, -1);
    for values in [vec![valid.clone(), invalid.clone()], vec![invalid, valid]] {
        let proto = IcebergPredicate {
            node: Some(Node::Set(IcebergSetPredicate {
                column: "amount".to_string(),
                op: Op::In as i32,
                values,
            })),
        };
        assert!(iceberg_predicate_to_predicate(&proto).is_none());
    }
}

// This controlled reader comparison isolates row-group statistics from page selection and
// row filtering. Both reads apply the same predicate to the same projected decimal column;
// only row-group filtering changes. There is no manifest/file planning or payload projection.
#[tokio::test]
async fn decimal_row_group_pruning_reduces_reads_with_page_selection_disabled() {
    use std::collections::HashMap;
    use std::fs::File;

    use arrow::array::Decimal128Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use futures::{StreamExt, TryStreamExt};
    use iceberg::arrow::ArrowReaderBuilder;
    use iceberg::io::FileIO;
    use iceberg::scan::FileScanTask;
    use iceberg::spec::DataFileFormat;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Type as PhysicalType;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let directory = tempfile::tempdir().unwrap();
    let rows = 65536_i128;
    for precision in [9, 18, 38] {
        let factor = if precision == 38 { 10_i128.pow(33) } else { 1 };
        let values: Vec<i128> = (0..rows).map(|i| (i - rows / 2) * factor).collect();
        let array = Decimal128Array::from(values)
            .with_precision_and_scale(precision as u8, 2)
            .unwrap();
        let field =
            Field::new("amount", DataType::Decimal128(precision as u8, 2), false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
            );
        let schema = Arc::new(ArrowSchema::new(vec![field]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap();
        let path = directory
            .path()
            .join(format!("decimal-{precision}.parquet"));
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(4096))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let footer = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        assert_eq!(footer.metadata().num_row_groups(), 16);
        let expected_physical = match precision {
            9 => PhysicalType::INT32,
            18 => PhysicalType::INT64,
            38 => PhysicalType::FIXED_LEN_BYTE_ARRAY,
            _ => unreachable!(),
        };
        for group in footer.metadata().row_groups() {
            assert_eq!(group.num_rows(), 4096);
            assert_eq!(group.column(0).column_type(), expected_physical);
            assert!(group.column(0).statistics().is_some());
        }

        let iceberg_schema = decimal_schema(precision, 2);
        let lower = iceberg_predicate_to_predicate(&binary(
            decimal_literal((100 * factor).to_be_bytes().to_vec(), precision as i32, 2),
            Op::GreaterThanOrEq,
        ))
        .unwrap();
        let upper = iceberg_predicate_to_predicate(&binary(
            decimal_literal((110 * factor).to_be_bytes().to_vec(), precision as i32, 2),
            Op::LessThanOrEq,
        ))
        .unwrap();
        let bound = lower
            .and(upper)
            .bind(Arc::clone(&iceberg_schema), true)
            .unwrap();
        let size = path.metadata().unwrap().len();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(size)
            .with_start(0)
            .with_length(size)
            .with_record_count(Some(rows as u64))
            .with_data_file_path(path.to_str().unwrap().to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema)
            .with_project_field_ids(vec![1])
            .with_predicate(Some(bound))
            .with_case_sensitive(true)
            .build()
            .unwrap();
        let mut read_bytes = Vec::new();
        for filtering in [false, true] {
            let reader =
                ArrowReaderBuilder::new(FileIO::new_with_fs(), iceberg::Runtime::current())
                    .with_row_selection_enabled(false)
                    .with_row_group_filtering_enabled(filtering)
                    .with_bloom_filter_enabled(false)
                    .with_metadata_size_hint(8192)
                    .build();
            let scan = reader
                .read(futures::stream::iter([Ok(task.clone())]).boxed())
                .unwrap();
            let metrics = scan.metrics().clone();
            let batches = scan.stream().try_collect::<Vec<_>>().await.unwrap();
            let mut actual: Vec<i128> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            actual.sort_unstable();
            assert_eq!(actual, (100..=110).map(|v| v * factor).collect::<Vec<_>>());
            read_bytes.push(metrics.bytes_read());
        }
        eprintln!(
            "decimal({precision},2) row-group pruning: unpruned={}, pruned={}",
            read_bytes[0], read_bytes[1]
        );
        assert!(
            read_bytes[1] > 0 && read_bytes[1] * 4 < read_bytes[0],
            "{read_bytes:?}"
        );
    }
}
