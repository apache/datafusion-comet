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

use arrow::array::{Array, Int32Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_comet_spark_expr::{GetStructField, ScalarSubqueryConsumer};
use datafusion_functions::string::octet_length::OctetLengthFunc;

#[test]
fn large_scalar_struct_field_is_consumed_before_broadcast() {
    let bytes = 262_144;
    let fields = vec![Field::new("s", DataType::Utf8, true)].into();
    let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
        fields,
        vec![Arc::new(StringArray::from(vec!["x".repeat(bytes)]))],
        None,
    )));
    let field = Arc::new(GetStructField::new(Arc::new(Literal::new(scalar)), 0));
    let consumer = Arc::new(ScalarFunctionExpr::new(
        "octet_length",
        Arc::new(ScalarUDF::new_from_impl(OctetLengthFunc::new())),
        vec![field],
        Arc::new(Field::new("length", DataType::Int32, true)),
        Arc::new(ConfigOptions::default()),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..8192))],
    )
    .unwrap();
    let result = ScalarSubqueryConsumer::wrap(consumer)
        .evaluate(&batch)
        .unwrap();
    let ColumnarValue::Array(result) = result else {
        panic!("expected a batch-sized result array");
    };
    assert_eq!(result.len(), batch.num_rows());
    assert_eq!(result.as_ref(), &Int32Array::from(vec![bytes as i32; 8192]));
}

fn field(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
    let fields = vec![Field::new("value", value.data_type(), true)].into();
    Arc::new(GetStructField::new(
        Arc::new(Literal::new(ScalarValue::Struct(Arc::new(
            StructArray::new(fields, vec![value.to_array().unwrap()], None),
        )))),
        0,
    ))
}

fn batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))],
    )
    .unwrap()
}

fn udf(
    name: &str,
    args: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
    volatility: datafusion::logical_expr::Volatility,
    fun: datafusion::logical_expr::ScalarFunctionImplementation,
) -> Arc<dyn PhysicalExpr> {
    let input_types = args
        .iter()
        .map(|e| e.data_type(batch(0).schema_ref()).unwrap())
        .collect();
    Arc::new(ScalarFunctionExpr::new(
        name,
        Arc::new(datafusion::logical_expr::create_udf(
            name,
            input_types,
            data_type.clone(),
            volatility,
            fun,
        )),
        args,
        Arc::new(Field::new(name, data_type, true)),
        Arc::new(ConfigOptions::default()),
    ))
}

#[test]
fn consumers_see_one_row_including_nested_functions() {
    use datafusion::logical_expr::Volatility;
    use std::sync::atomic::{AtomicUsize, Ordering};
    for (bytes, rows) in [(32768, 1024), (262144, 8192)] {
        for value in [
            ScalarValue::Utf8(Some("x".repeat(bytes))),
            ScalarValue::Binary(Some(vec![42; bytes])),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&calls);
            let inner = udf(
                "checked_identity",
                vec![field(value.clone())],
                value.data_type(),
                Volatility::Immutable,
                Arc::new(move |args| {
                    counter.fetch_add(1, Ordering::Relaxed);
                    let ColumnarValue::Array(a) = &args[0] else {
                        panic!("expected array argument");
                    };
                    assert_eq!(a.len(), 1, "the large field must not be broadcast first");
                    Ok(args[0].clone())
                }),
            );
            let inner = ScalarSubqueryConsumer::wrap(inner);
            let outer = Arc::new(ScalarFunctionExpr::new(
                "octet_length",
                Arc::new(ScalarUDF::new_from_impl(OctetLengthFunc::new())),
                vec![inner],
                Arc::new(Field::new("length", DataType::Int32, true)),
                Arc::new(ConfigOptions::default()),
            ));
            let expr = ScalarSubqueryConsumer::wrap(outer);
            assert_eq!(
                calls.load(Ordering::Relaxed),
                0,
                "planning must not evaluate functions"
            );
            let output = expr
                .evaluate(&batch(rows))
                .unwrap()
                .into_array(rows)
                .unwrap();
            assert_eq!(output.as_ref(), &Int32Array::from(vec![bytes as i32; rows]));
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            let empty = expr.evaluate(&batch(0)).unwrap().into_array(0).unwrap();
            assert_eq!(empty.len(), 0);
            assert_eq!(empty.data_type(), &DataType::Int32);
            assert_eq!(
                calls.load(Ordering::Relaxed),
                1,
                "empty batches must not evaluate the body"
            );
        }
    }
}

#[test]
fn null_short_circuit_skips_failing_argument() {
    use datafusion::logical_expr::Volatility;
    use datafusion::physical_expr::expressions::CastExpr;
    use datafusion_comet_spark_expr::NullShortCircuit;
    let invalid: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        Arc::new(Literal::new(ScalarValue::Utf8(Some("invalid".into())))),
        DataType::Int32,
        Some(arrow::compute::CastOptions {
            safe: false,
            ..Default::default()
        }),
    ));
    let inner = udf(
        "null_intolerant",
        vec![field(ScalarValue::Utf8(None)), invalid],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|_| panic!("all-NULL first argument must skip the function")),
    );
    let expr =
        ScalarSubqueryConsumer::wrap(NullShortCircuit::wrap(inner, batch(0).schema_ref()).unwrap());
    assert!(expr.is::<ScalarSubqueryConsumer>());
    for rows in [0, 1, 8192] {
        let output = expr
            .evaluate(&batch(rows))
            .unwrap()
            .into_array(rows)
            .unwrap();
        assert_eq!(output.len(), rows);
        assert_eq!(output.null_count(), rows);
        assert_eq!(output.data_type(), &DataType::Utf8);
    }
}

#[test]
fn stateful_consumers_are_rejected_and_columns_keep_row_values() {
    use datafusion::logical_expr::Volatility;
    use datafusion::physical_expr::expressions::Column;
    for volatility in [Volatility::Volatile, Volatility::Stable] {
        let original = udf(
            "stateful",
            vec![field(ScalarValue::Utf8(Some("x".into())))],
            DataType::Utf8,
            volatility,
            Arc::new(|args| Ok(args[0].clone())),
        );
        let expr = ScalarSubqueryConsumer::wrap(Arc::clone(&original));
        assert!(Arc::ptr_eq(&expr, &original));
    }
    let original = udf(
        "row_dependent",
        vec![
            field(ScalarValue::Utf8(Some("x".into()))),
            Arc::new(Column::new("id", 0)),
        ],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[1].clone())),
    );
    let expr = ScalarSubqueryConsumer::wrap(Arc::clone(&original));
    assert!(expr.is::<ScalarSubqueryConsumer>());
    let output = expr.evaluate(&batch(17)).unwrap().into_array(17).unwrap();
    assert_eq!(output.as_ref(), batch(17).column(0).as_ref());
    let literal = Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    assert!(Arc::ptr_eq(
        &ScalarSubqueryConsumer::wrap(Arc::clone(&literal)),
        &literal
    ));
}

#[test]
fn rewritten_consumer_rechecks_row_dependencies() {
    use datafusion::logical_expr::Volatility;
    use datafusion::physical_expr::expressions::Column;
    let original = udf(
        "identity",
        vec![field(ScalarValue::Int64(Some(42)))],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[0].clone())),
    );
    let wrapped = ScalarSubqueryConsumer::wrap(Arc::clone(&original));
    let replaced = original
        .with_new_children(vec![Arc::new(Column::new("id", 0))])
        .unwrap();
    let rewritten = wrapped.with_new_children(vec![replaced]).unwrap();
    assert!(!rewritten.is::<ScalarSubqueryConsumer>());
    let output = rewritten
        .evaluate(&batch(17))
        .unwrap()
        .into_array(17)
        .unwrap();
    assert_eq!(output.as_ref(), batch(17).column(0).as_ref());
}

#[test]
fn genuinely_oversized_final_output_is_an_error_not_a_panic() {
    use datafusion::logical_expr::Volatility;
    for value in [
        ScalarValue::Utf8(Some("x".repeat(262144))),
        ScalarValue::Binary(Some(vec![42; 262144])),
    ] {
        let original = udf(
            "identity",
            vec![field(value.clone())],
            value.data_type(),
            Volatility::Immutable,
            Arc::new(|args| Ok(args[0].clone())),
        );
        let err = ScalarSubqueryConsumer::wrap(original)
            .evaluate(&batch(8192))
            .unwrap_err();
        assert!(err.to_string().contains("32-bit offset limit"), "{err}");
    }
}

fn octet(child: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(ScalarFunctionExpr::new(
        "octet_length",
        Arc::new(ScalarUDF::new_from_impl(OctetLengthFunc::new())),
        vec![child],
        Arc::new(Field::new("length", DataType::Int32, true)),
        Arc::new(ConfigOptions::default()),
    ))
}

#[test]
fn row_dependent_conditionals_reduce_bounded_inputs_in_row_order() {
    use datafusion::logical_expr::{Operator, Volatility};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion_comet_spark_expr::IfExpr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    // Cross chunk boundaries with all-true, mixed and all-false branches, including NULL.
    for threshold in [0, 4097, 8201] {
        for otherwise in [ScalarValue::Utf8(Some("".into())), ScalarValue::Utf8(None)] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = calls.clone();
            let checked = udf(
                "bounded_input",
                vec![field(ScalarValue::Utf8(Some("x".repeat(262144))))],
                DataType::Utf8,
                Volatility::Immutable,
                Arc::new(move |args| {
                    counter.fetch_add(1, Ordering::Relaxed);
                    let ColumnarValue::Array(a) = &args[0] else {
                        panic!("array required")
                    };
                    let a = a.as_any().downcast_ref::<StringArray>().unwrap();
                    assert!(a.value_data().len() <= 1024 * 1024);
                    Ok(args[0].clone())
                }),
            );
            let predicate = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)),
                Operator::GtEq,
                Arc::new(Literal::new(ScalarValue::Int64(Some(threshold)))),
            ));
            let expr = ScalarSubqueryConsumer::wrap(octet(Arc::new(IfExpr::new(
                predicate,
                checked,
                Arc::new(Literal::new(otherwise.clone())),
            ))));
            // Exercise a nonzero-offset input batch as well as adapter-created slices.
            let input = batch(8201).slice(9, 8192);
            let output = expr.evaluate(&input).unwrap().into_array(8192).unwrap();
            let expected = Int32Array::from_iter((9..8201).map(|id| {
                if id >= threshold {
                    Some(262144)
                } else if otherwise.is_null() {
                    None
                } else {
                    Some(0)
                }
            }));
            assert_eq!(output.as_ref(), &expected);
            assert_eq!(calls.load(Ordering::Relaxed) == 0, threshold == 8201);
        }
    }
}

#[test]
fn direct_comparison_of_large_fields_is_reduced_before_broadcast() {
    use arrow::array::BooleanArray;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;
    let comparison = Arc::new(BinaryExpr::new(
        field(ScalarValue::Utf8(Some("x".repeat(262144)))),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some("x".into())))),
    ));
    let output = ScalarSubqueryConsumer::wrap(comparison)
        .evaluate(&batch(8192))
        .unwrap()
        .into_array(8192)
        .unwrap();
    assert_eq!(output.as_ref(), &BooleanArray::from(vec![false; 8192]));
}

#[test]
fn ordinary_struct_columns_are_not_scalar_subquery_consumers() {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(GetStructField::new(
            Arc::new(Column::new("struct_column", 0)),
            0,
        )),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some("x".repeat(262144))))),
    ));
    assert!(Arc::ptr_eq(
        &ScalarSubqueryConsumer::wrap(expr.clone()),
        &expr
    ));
}

#[test]
fn unary_and_pattern_consumers_reduce_large_fields() {
    use arrow::array::BooleanArray;
    use datafusion::physical_expr::expressions::{IsNotNullExpr, IsNullExpr, LikeExpr, NotExpr};
    use datafusion_comet_spark_expr::RLike;
    let input = field(ScalarValue::Utf8(Some("x".repeat(262144))));
    let cases: Vec<(Arc<dyn PhysicalExpr>, bool)> = vec![
        (Arc::new(IsNullExpr::new(input.clone())), false),
        (Arc::new(IsNotNullExpr::new(input.clone())), true),
        (
            Arc::new(NotExpr::new(Arc::new(IsNullExpr::new(input.clone())))),
            true,
        ),
        (
            Arc::new(LikeExpr::new(
                false,
                false,
                input.clone(),
                Arc::new(Literal::new(ScalarValue::Utf8(Some("x%".into())))),
            )),
            true,
        ),
        (Arc::new(RLike::try_new(input, "^x").unwrap()), true),
    ];
    for (expr, expected) in cases {
        let output = ScalarSubqueryConsumer::wrap(expr)
            .evaluate(&batch(8192))
            .unwrap()
            .into_array(8192)
            .unwrap();
        assert_eq!(output.as_ref(), &BooleanArray::from(vec![expected; 8192]));
    }
}

#[test]
fn in_list_reduces_large_fields_and_preserves_nulls() {
    use arrow::array::BooleanArray;
    use datafusion::physical_expr::expressions::in_list;
    for value in [
        ScalarValue::Utf8(Some("x".repeat(262144))),
        ScalarValue::Utf8(None),
    ] {
        let expr = in_list(
            field(value.clone()),
            vec![
                Arc::new(Literal::new(ScalarValue::Utf8(Some("x".into())))),
                Arc::new(Literal::new(ScalarValue::Utf8(Some("y".into())))),
            ],
            &false,
            batch(0).schema_ref(),
        )
        .unwrap();
        let output = ScalarSubqueryConsumer::wrap(expr)
            .evaluate(&batch(8192))
            .unwrap()
            .into_array(8192)
            .unwrap();
        let expected = if value.is_null() { None } else { Some(false) };
        assert_eq!(output.as_ref(), &BooleanArray::from(vec![expected; 8192]));
    }
}

#[test]
fn nested_struct_json_is_consumed_without_repeating_the_field() {
    use datafusion_comet_spark_expr::{CreateNamedStruct, ToJson};
    let structure = Arc::new(CreateNamedStruct::new(
        vec![field(ScalarValue::Utf8(Some("x".repeat(262144))))],
        vec!["s".into()],
    ));
    let json = Arc::new(ToJson::new(structure, "UTC", true));
    let output = ScalarSubqueryConsumer::wrap(octet(json))
        .evaluate(&batch(8192))
        .unwrap()
        .into_array(8192)
        .unwrap();
    assert_eq!(output.as_ref(), &Int32Array::from(vec![262152; 8192]));
}

#[test]
fn unused_wide_sibling_does_not_split_numeric_consumers() {
    use datafusion::logical_expr::Volatility;
    use datafusion::physical_expr::expressions::Column;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let structure = Arc::new(StructArray::new(
        vec![
            Field::new("n", DataType::Int64, false),
            Field::new("unused", DataType::Utf8, false),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)])),
        ],
        None,
    ));
    for nested in [false, true] {
        let literal: Arc<dyn PhysicalExpr> = if nested {
            let outer = StructArray::new(
                vec![Field::new("nested", structure.data_type().clone(), false)].into(),
                vec![structure.clone()],
                None,
            );
            Arc::new(GetStructField::new(
                Arc::new(Literal::new(ScalarValue::Struct(Arc::new(outer)))),
                0,
            ))
        } else {
            Arc::new(Literal::new(ScalarValue::Struct(structure.clone())))
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let expr = udf(
            "numeric_identity",
            vec![
                Arc::new(GetStructField::new(literal, 0)),
                Arc::new(Column::new("id", 0)),
            ],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(move |args| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(args[1].clone())
            }),
        );
        let output = ScalarSubqueryConsumer::wrap(expr)
            .evaluate(&batch(8192))
            .unwrap()
            .into_array(8192)
            .unwrap();
        assert_eq!(output.as_ref(), batch(8192).column(0).as_ref());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn literal_field_chain_preserves_each_parent_null_and_empty_shape() {
    use arrow::buffer::NullBuffer;
    for outer_null in [false, true] {
        for inner_null in [false, true] {
            let inner = Arc::new(StructArray::new(
                vec![Field::new("n", DataType::Int64, false)].into(),
                vec![Arc::new(Int64Array::from(vec![42]))],
                inner_null.then(|| NullBuffer::new_null(1)),
            ));
            let outer = StructArray::new(
                vec![Field::new("nested", inner.data_type().clone(), true)].into(),
                vec![inner],
                outer_null.then(|| NullBuffer::new_null(1)),
            );
            let expr = GetStructField::new(
                Arc::new(GetStructField::new(
                    Arc::new(Literal::new(ScalarValue::Struct(Arc::new(outer)))),
                    0,
                )),
                0,
            );
            for rows in [0, 1, 17] {
                let ColumnarValue::Array(output) = expr.evaluate(&batch(rows)).unwrap() else {
                    panic!("array required")
                };
                let expected = if outer_null || inner_null {
                    None
                } else {
                    Some(42)
                };
                assert_eq!(output.as_ref(), &Int64Array::from(vec![expected; rows]));
            }
        }
    }
}
