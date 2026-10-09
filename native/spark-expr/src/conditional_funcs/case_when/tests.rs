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
use crate::{spark_comparison, FloatOperands};
use arrow::array::{
    BinaryArray, Int32Array, Int64Array, LargeStringArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{Field, FieldRef, TimeUnit, TimestampMicrosecondType};
use datafusion::physical_expr::expressions::{col, lit};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

fn value_types() -> Vec<DataType> {
    vec![
        DataType::Int32,
        DataType::Int64,
        DataType::Float64,
        DataType::Decimal128(12, 3),
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Boolean,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
    ]
}

fn random_values(data_type: &DataType, len: usize, nulls: f64, rng: &mut StdRng) -> ArrayRef {
    let ints: Int64Array = (0..len)
        .map(|_| (!rng.random_bool(nulls)).then(|| rng.random_range(-9i64..9)))
        .collect();
    // Lengths either side of `SLACK`, some multi-byte
    let text = |v: i64| {
        let pad = if v % 2 == 0 { "é" } else { "x" };
        format!("{}{v}", pad.repeat(3 * v.unsigned_abs() as usize))
    };
    match data_type {
        DataType::Boolean => cast(&ints, data_type).unwrap(),
        DataType::Date32 => cast(&cast(&ints, &DataType::Int32).unwrap(), data_type).unwrap(),
        DataType::Utf8 => Arc::new(ints.iter().map(|v| v.map(text)).collect::<StringArray>()),
        DataType::LargeUtf8 => Arc::new(
            ints.iter()
                .map(|v| v.map(text))
                .collect::<LargeStringArray>(),
        ),
        DataType::Binary => Arc::new(
            ints.iter()
                .map(|v| v.map(|v| text(v).into_bytes()))
                .collect::<BinaryArray>(),
        ),
        _ => cast(&ints, data_type).unwrap(),
    }
}

/// The branches and ELSE of a CASE.
type CaseShape = (Vec<WhenThen>, Option<Arc<dyn PhysicalExpr>>);

/// How the predicate columns are laid out.
#[derive(Clone, Copy, Debug)]
enum Predicates {
    Random,
    WithNulls,
    AllTrue,
    AllFalse,
    /// Each predicate is true for one contiguous run of rows
    Runs,
}

fn random_predicate(len: usize, layout: Predicates, rng: &mut StdRng) -> ArrayRef {
    let (start, end) = (rng.random_range(0..len), rng.random_range(0..len));
    let values: BooleanBuffer = (0..len)
        .map(|row| match layout {
            Predicates::Random | Predicates::WithNulls => rng.random_bool(0.4),
            Predicates::AllTrue => true,
            Predicates::AllFalse => false,
            Predicates::Runs => start.min(end) <= row && row < start.max(end),
        })
        .collect();
    // A comparison kernel leaves whatever it computed under a NULL, so the value bits under
    // these are random too, and a NULL must not match even where its bit is set
    let nulls = matches!(layout, Predicates::WithNulls)
        .then(|| NullBuffer::from_iter((0..len).map(|_| !rng.random_bool(0.3))));
    Arc::new(BooleanArray::new(values, nulls))
}

/// Predicate columns `p0..p2` and value columns `v0..v2` of `data_type`.
fn test_batch(
    data_type: &DataType,
    len: usize,
    nulls: f64,
    layout: Predicates,
    rng: &mut StdRng,
) -> RecordBatch {
    let mut fields = vec![];
    let mut columns = vec![];
    for i in 0..3 {
        fields.push(Field::new(format!("p{i}"), DataType::Boolean, true));
        columns.push(random_predicate(len, layout, rng));
    }
    for i in 0..3 {
        fields.push(Field::new(format!("v{i}"), data_type.clone(), true));
        columns.push(random_values(data_type, len, nulls, rng));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// The first valid value in `values`, as a literal.
fn first_valid(values: &ArrayRef) -> Option<ScalarValue> {
    (0..values.len())
        .find(|&i| values.is_valid(i))
        .map(|i| ScalarValue::try_from_array(values, i).unwrap())
}

/// Evaluates the CASE with both `CaseWhenExpr` and DataFusion's `CaseExpr`, checks they agree,
/// and returns whether `CaseWhenExpr` evaluated it eagerly.
fn check_against_case_expr(
    batch: &RecordBatch,
    when_then: Vec<WhenThen>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> bool {
    let expected = CaseExpr::try_new(None, when_then.clone(), else_expr.clone())
        .unwrap()
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let expr = CaseWhenExpr::try_new(when_then, else_expr).unwrap();
    let actual = expr
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    assert_eq!(actual.data_type(), expected.data_type(), "{expr}");
    assert_eq!(actual.as_ref(), expected.as_ref(), "{expr}");
    expr.eager.get().unwrap().is_some()
}

#[test]
fn eager_evaluation_matches_case_expr() {
    let mut rng = StdRng::seed_from_u64(42);
    let layouts = [
        Predicates::Random,
        Predicates::WithNulls,
        Predicates::AllTrue,
        Predicates::AllFalse,
        Predicates::Runs,
    ];
    for data_type in value_types() {
        for nulls in [0.0, 0.2, 0.9] {
            for layout in layouts {
                // Longer than a word of the bitmap, and not a multiple of it
                let batch = test_batch(&data_type, 300, nulls, layout, &mut rng);
                for batch in [batch.clone(), batch.slice(7, 250)] {
                    let schema = batch.schema();
                    let c = |name: &str| col(name, &schema).unwrap();
                    let null = ScalarValue::try_new_null(&data_type).unwrap();
                    let literal = first_valid(batch.column(3)).unwrap_or(null.clone());
                    let shapes: Vec<CaseShape> = vec![
                        (vec![(c("p0"), c("v0"))], Some(c("v1"))),
                        (vec![(c("p0"), c("v0"))], None),
                        (
                            vec![
                                (c("p0"), c("v0")),
                                (c("p1"), c("v1")),
                                (c("p2"), lit(literal.clone())),
                            ],
                            Some(c("v2")),
                        ),
                        (vec![(c("p0"), c("v0")), (c("p1"), lit(null.clone()))], None),
                        (
                            vec![(c("p0"), lit(literal.clone())), (c("p1"), lit(null))],
                            Some(lit(literal.clone())),
                        ),
                    ];
                    for (when_then, else_expr) in shapes {
                        let eager = check_against_case_expr(&batch, when_then, else_expr);
                        assert!(eager, "{data_type} should be evaluated eagerly");
                    }
                }
            }
        }
    }
}

#[test]
fn nested_else_is_flattened() {
    let mut rng = StdRng::seed_from_u64(7);
    for data_type in [DataType::Int64, DataType::Utf8, DataType::Boolean] {
        let batch = test_batch(&data_type, 300, 0.2, Predicates::WithNulls, &mut rng);
        let schema = batch.schema();
        let c = |name: &str| col(name, &schema).unwrap();
        let literal = first_valid(batch.column(3)).unwrap();
        // IF(p0, v0, IF(p1, v1, CASE WHEN p2 THEN literal ELSE v2 END))
        let inner: Arc<dyn PhysicalExpr> = Arc::new(
            CaseWhenExpr::try_new(vec![(c("p2"), lit(literal.clone()))], Some(c("v2"))).unwrap(),
        );
        let middle: Arc<dyn PhysicalExpr> = Arc::new(IfExpr::new(c("p1"), c("v1"), inner));
        let expr = CaseWhenExpr::try_new(vec![(c("p0"), c("v0"))], Some(middle)).unwrap();
        assert_eq!(expr.flat_when_then.len(), 3);

        let reference = CaseExpr::try_new(
            None,
            vec![(c("p0"), c("v0"))],
            Some(Arc::new(
                CaseExpr::try_new(
                    None,
                    vec![(c("p1"), c("v1"))],
                    Some(Arc::new(
                        CaseExpr::try_new(None, vec![(c("p2"), lit(literal))], Some(c("v2")))
                            .unwrap(),
                    )),
                )
                .unwrap(),
            )),
        )
        .unwrap();
        let expected = reference.evaluate(&batch).unwrap().into_array(300).unwrap();
        let actual = expr.evaluate(&batch).unwrap().into_array(300).unwrap();
        assert_eq!(actual.as_ref(), expected.as_ref(), "{data_type}");
        assert!(expr.eager.get().unwrap().is_some());
    }
}

fn int_batch(a: Vec<Option<i64>>, b: Vec<Option<i64>>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
    )
    .unwrap()
}

fn binary(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, op, right))
}

#[test]
fn branch_that_can_fail_is_evaluated_lazily() {
    // CASE WHEN b != 0 THEN a / b ELSE 0 END: integer division fails for b = 0, which the
    // WHEN excludes
    let batch = int_batch(
        vec![Some(10), Some(20), Some(30), None],
        vec![Some(2), Some(0), Some(3), Some(0)],
    );
    let schema = batch.schema();
    let (a, b) = (col("a", &schema).unwrap(), col("b", &schema).unwrap());
    let when_then = vec![(
        binary(Arc::clone(&b), Operator::NotEq, lit(0i64)),
        binary(a, Operator::Divide, b),
    )];
    let eager = check_against_case_expr(&batch, when_then, Some(lit(0i64)));
    assert!(!eager);
}

#[test]
fn later_when_that_can_fail_is_evaluated_lazily() {
    // CASE WHEN b = 0 THEN -1 WHEN a / b > 5 THEN 1 ELSE 0 END: the second WHEN only sees the
    // rows where b is not 0
    let batch = int_batch(
        vec![Some(10), Some(20), Some(30), Some(1)],
        vec![Some(2), Some(0), Some(3), Some(0)],
    );
    let schema = batch.schema();
    let (a, b) = (col("a", &schema).unwrap(), col("b", &schema).unwrap());
    let when_then = vec![
        (binary(Arc::clone(&b), Operator::Eq, lit(0i64)), lit(-1i64)),
        (
            binary(binary(a, Operator::Divide, b), Operator::Gt, lit(5i64)),
            lit(1i64),
        ),
    ];
    let eager = check_against_case_expr(&batch, when_then, Some(lit(0i64)));
    assert!(!eager);
}

#[test]
fn first_when_can_fail_and_still_be_eager() {
    // Spark evaluates the first WHEN for every row, so it may be anything
    let batch = int_batch(vec![Some(10), Some(-20)], vec![Some(2), Some(5)]);
    let schema = batch.schema();
    let (a, b) = (col("a", &schema).unwrap(), col("b", &schema).unwrap());
    let when_then = vec![(
        binary(
            binary(Arc::clone(&a), Operator::Divide, b),
            Operator::Gt,
            lit(0i64),
        ),
        Arc::clone(&a),
    )];
    let eager = check_against_case_expr(&batch, when_then, Some(lit(0i64)));
    assert!(eager);
}

#[test]
fn infallible_expressions() {
    let list_of = |item: DataType| DataType::List(Arc::new(Field::new("item", item, true)));
    let schema = Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("j", DataType::Int32, true),
        Field::new("d", DataType::Float64, true),
        Field::new("e", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("dec", DataType::Decimal128(10, 2), true),
        Field::new("l", list_of(DataType::Float64), true),
        Field::new("m", list_of(DataType::Float64), true),
        Field::new(
            "dictionary",
            list_of(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Float64),
            )),
            true,
        ),
    ]);
    let c = |name: &str| col(name, &schema).unwrap();
    let cast = |e: Arc<dyn PhysicalExpr>, to: DataType| -> Arc<dyn PhysicalExpr> {
        Arc::new(Cast::new(
            e,
            to,
            SparkCastOptions::new_without_timezone(EvalMode::Ansi, false),
            None,
            None,
        ))
    };
    let infallible = |e: Arc<dyn PhysicalExpr>| is_infallible(&e, &schema);
    // A comparison as the planner builds it, which follows Spark's ordering for floats
    let compare = |left: &str, op: Operator, right: Arc<dyn PhysicalExpr>| {
        spark_comparison(c(left), op, right, &schema, FloatOperands::Normalize).unwrap()
    };

    assert!(infallible(c("i")));
    assert!(infallible(lit("x")));
    assert!(infallible(binary(c("i"), Operator::Lt, lit(0i64))));
    assert!(infallible(binary(c("s"), Operator::Eq, lit("x"))));
    assert!(infallible(binary(c("i"), Operator::Plus, c("i"))));
    assert!(infallible(binary(c("d"), Operator::Divide, c("d"))));
    assert!(infallible(binary(c("d"), Operator::Modulo, c("d"))));
    assert!(infallible(Arc::new(NotExpr::new(binary(
        c("i"),
        Operator::Lt,
        lit(0i64)
    )))));
    assert!(infallible(Arc::new(IsNullExpr::new(c("s")))));
    assert!(infallible(cast(c("j"), DataType::Int64)));
    assert!(infallible(cast(c("i"), DataType::Float64)));
    assert!(infallible(Arc::new(IfExpr::new(
        binary(c("i"), Operator::Lt, lit(0i64)),
        c("i"),
        lit(0i64)
    ))));

    assert!(infallible(Arc::new(NormalizeNaNAndZero::new(
        DataType::Float64,
        c("d")
    ))));
    for op in [Operator::Lt, Operator::Eq, Operator::IsNotDistinctFrom] {
        assert!(infallible(compare("d", op, lit(1.5))), "d {op} 1.5");
        assert!(infallible(compare("d", op, c("e"))), "d {op} e");
    }
    for op in [Operator::Lt, Operator::IsNotDistinctFrom] {
        assert!(infallible(compare("l", op, c("m"))), "l {op} m");
    }

    // Only a boolean can be negated, and only a float normalized
    assert!(!infallible(Arc::new(NotExpr::new(c("i")))));
    assert!(!infallible(Arc::new(NormalizeNaNAndZero::new(
        DataType::Int64,
        c("i")
    ))));
    // A nested comparison fails on a dictionary-encoded leaf against a plain one
    assert!(!infallible(compare("l", Operator::Lt, c("dictionary"))));
    // Integer division and remainder fail on a zero divisor
    assert!(!infallible(binary(c("i"), Operator::Divide, c("i"))));
    assert!(!infallible(binary(c("i"), Operator::Modulo, c("i"))));
    // Checked arithmetic fails on overflow
    let checked: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(c("i"), Operator::Plus, c("i")).with_fail_on_overflow(true));
    assert!(!infallible(checked));
    // Decimal arithmetic can overflow
    assert!(!infallible(binary(c("dec"), Operator::Plus, c("dec"))));
    // Narrowing and parsing casts can fail
    assert!(!infallible(cast(c("i"), DataType::Int32)));
    assert!(!infallible(cast(c("s"), DataType::Int32)));
    // So can anything built from a part that can fail
    assert!(!infallible(binary(
        binary(c("i"), Operator::Divide, c("i")),
        Operator::Lt,
        lit(0i64)
    )));
    assert!(!infallible(Arc::new(IfExpr::new(
        binary(c("i"), Operator::Lt, lit(0i64)),
        binary(c("i"), Operator::Divide, c("i")),
        lit(0i64)
    ))));
}

#[test]
fn result_of_one_branch_is_returned_as_is() {
    let batch = int_batch(vec![Some(1), Some(2)], vec![Some(1), Some(2)]);
    let schema = batch.schema();
    let a = col("a", &schema).unwrap();
    let scalar = |expr: CaseWhenExpr| match expr.evaluate(&batch).unwrap() {
        ColumnarValue::Scalar(v) => v,
        other => panic!("expected a scalar, got {other:?}"),
    };
    // Every row chooses THEN, a literal
    let expr = CaseWhenExpr::try_new(
        vec![(binary(Arc::clone(&a), Operator::Gt, lit(0i64)), lit(7i64))],
        Some(Arc::clone(&a)),
    )
    .unwrap();
    assert_eq!(scalar(expr), ScalarValue::Int64(Some(7)));
    // No row chooses a branch and there is no ELSE
    let expr = CaseWhenExpr::try_new(
        vec![(binary(Arc::clone(&a), Operator::Lt, lit(0i64)), a)],
        None,
    )
    .unwrap();
    assert_eq!(scalar(expr), ScalarValue::Int64(None));
}

#[test]
fn empty_batch() {
    let batch = int_batch(vec![], vec![]);
    let schema = batch.schema();
    let a = col("a", &schema).unwrap();
    let b = col("b", &schema).unwrap();
    let when_then = vec![(binary(Arc::clone(&a), Operator::Lt, lit(0i64)), a)];
    check_against_case_expr(&batch, when_then, Some(b));
}

#[test]
fn nullable_follows_spark() {
    let schema = Schema::new(vec![
        Field::new("p", DataType::Boolean, false),
        Field::new("a", DataType::Int64, false),
        Field::new("n", DataType::Int64, true),
    ]);
    let c = |name: &str| col(name, &schema).unwrap();
    let nullable = |when_then: Vec<WhenThen>, else_expr: Option<Arc<dyn PhysicalExpr>>| {
        CaseWhenExpr::try_new(when_then, else_expr)
            .unwrap()
            .nullable(&schema)
            .unwrap()
    };
    assert!(!nullable(vec![(c("p"), c("a"))], Some(lit(0i64))));
    assert!(nullable(vec![(c("p"), c("a"))], None));
    assert!(nullable(vec![(c("p"), c("n"))], Some(lit(0i64))));
    assert!(nullable(vec![(c("p"), c("a"))], Some(c("n"))));
    assert!(nullable(
        vec![(c("p"), c("a"))],
        Some(lit(ScalarValue::Int64(None)))
    ));
}

#[test]
fn create_case_when_casts_only_differing_branches() {
    let schema = Schema::new(vec![
        Field::new("p", DataType::Boolean, true),
        Field::new("i", DataType::Int32, true),
        Field::new("l", DataType::Int64, true),
    ]);
    let c = |name: &str| col(name, &schema).unwrap();
    let expr = create_case_when(
        vec![(c("p"), c("l")), (c("p"), c("i"))],
        Some(c("l")),
        &schema,
    )
    .unwrap();
    let children = expr.children();
    assert!(children[1].is::<Column>());
    let cast = children[3].downcast_ref::<Cast>().unwrap();
    assert_eq!(cast.data_type, DataType::Int64);
    assert!(children[4].is::<Column>());

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
        ],
    )
    .unwrap();
    let result = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
    assert_eq!(
        result.as_ref(),
        &Int64Array::from(vec![Some(10), None, Some(30)]) as &dyn Array
    );
}

/// CASE branches that share a Spark timestamp type but carry different Arrow timezone labels
/// are reconciled by relabelling them, which used to panic because the cast had no timezone.
/// Like IF, CASE retains the first THEN's label when present.
#[test]
fn case_reconciles_timestamp_timezone_labels() {
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    for then_label in [Some("Etc/UTC"), None] {
        let then_type = DataType::Timestamp(TimeUnit::Microsecond, then_label.map(Into::into));
        let expected_type = if then_label.is_some() {
            then_type.clone()
        } else {
            utc.clone()
        };
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("t", then_type, true),
            Field::new("e", utc.clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2]).with_timezone_opt(then_label)),
                Arc::new(TimestampMicrosecondArray::from(vec![10, 20]).with_timezone("UTC")),
            ],
        )
        .unwrap();
        let c = |name: &str| col(name, &schema).unwrap();
        let case = create_case_when(vec![(c("b"), c("t"))], Some(c("e")), &schema).unwrap();
        let result = case.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(case.data_type(&schema).unwrap(), expected_type);
        assert_eq!(result.data_type(), &expected_type, "{then_label:?}");
        let values = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(values.values().to_vec(), vec![1, 20], "{then_label:?}");
    }
}

/// Evaluates a conditional over `batch`, whose first row takes THEN and second row ELSE,
/// and over each row alone, checking that every result has the type the expression reports.
/// Returns the result for the whole batch.
fn evaluate_conditional(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> ArrayRef {
    let data_type = expr.data_type(&batch.schema()).unwrap();
    // Both branches, only THEN, and only ELSE, whose array `IfExpr` returns unchanged
    for (offset, len) in [(0, 2), (0, 1), (1, 1)] {
        let result = expr.evaluate(&batch.slice(offset, len)).unwrap();
        let result = result.into_array(len).unwrap();
        assert_eq!(
            result.data_type(),
            &data_type,
            "{expr} over rows {offset}..+{len}"
        );
    }
    expr.evaluate(batch).unwrap().into_array(2).unwrap()
}

/// Spark matches struct fields by position even when their case-distinct names are reordered.
/// Name-based union pairs the first INT with the second DOUBLE and silently widens it.
#[test]
fn conditional_branches_reconcile_case_variant_fields_positionally() {
    use arrow::array::Float64Array;

    let then_fields: arrow::datatypes::Fields = vec![
        Field::new("x", DataType::Int32, true),
        Field::new("X", DataType::Float64, false),
    ]
    .into();
    let else_fields: arrow::datatypes::Fields = vec![
        Field::new("X", DataType::Int32, false),
        Field::new("x", DataType::Float64, true),
    ]
    .into();
    let expected_fields: arrow::datatypes::Fields = vec![
        Field::new("x", DataType::Int32, true),
        Field::new("X", DataType::Float64, true),
    ]
    .into();
    let schema = Schema::new(vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("t", DataType::Struct(then_fields.clone()), false),
        Field::new("e", DataType::Struct(else_fields.clone()), false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(StructArray::new(
                then_fields,
                vec![
                    Arc::new(Int32Array::from(vec![Some(7), None])),
                    Arc::new(Float64Array::from(vec![5.5, 5.5])),
                ],
                None,
            )),
            Arc::new(StructArray::new(
                else_fields,
                vec![
                    Arc::new(Int32Array::from(vec![0, 0])),
                    Arc::new(Float64Array::from(vec![Some(9.5), None])),
                ],
                None,
            )),
        ],
    )
    .unwrap();
    let c = |name: &str| col(name, &schema).unwrap();
    let expected = StructArray::new(
        expected_fields,
        vec![
            Arc::new(Int32Array::from(vec![7, 0])),
            Arc::new(Float64Array::from(vec![Some(5.5), None])),
        ],
        None,
    );
    for expr in [
        create_if_expr(c("b"), c("t"), c("e"), &schema).unwrap(),
        create_case_when(vec![(c("b"), c("t"))], Some(c("e")), &schema).unwrap(),
    ] {
        assert_eq!(expr.data_type(&schema).unwrap(), *expected.data_type());
        let result = evaluate_conditional(&expr, &batch);
        assert_eq!(result.as_ref(), &expected, "{expr}");
    }
}

/// IF branches that share a Spark timestamp type but carry different Arrow timezone labels are
/// reconciled by relabelling one of them, as CASE branches are. The result has the THEN
/// branch's label, which `IfExpr` reported before the ELSE branch was cast, unless it has none.
/// Every case ends up labelled UTC, like every `TimestampType` value in a native plan.
#[test]
fn if_reconciles_timestamp_timezone_labels() {
    let timestamp =
        |label: Option<&str>| DataType::Timestamp(TimeUnit::Microsecond, label.map(Into::into));
    // The THEN branch's label, the ELSE branch's label, and the result's
    for (then_label, else_label, label) in [
        (None, Some("UTC"), Some("UTC")),
        (Some("UTC"), Some("Etc/UTC"), Some("UTC")),
        (Some("UTC"), None, Some("UTC")),
    ] {
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("t", timestamp(then_label), true),
            Field::new("e", timestamp(else_label), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2]).with_timezone_opt(then_label)),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![10, 20]).with_timezone_opt(else_label),
                ),
            ],
        )
        .unwrap();
        let c = |name: &str| col(name, &schema).unwrap();
        let expr = create_if_expr(c("b"), c("t"), c("e"), &schema).unwrap();
        let labels = format!("{then_label:?} {else_label:?}");
        assert_eq!(
            expr.data_type(&schema).unwrap(),
            timestamp(label),
            "{labels}"
        );
        let result = evaluate_conditional(&expr, &batch);
        let values = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(values.values().to_vec(), vec![1, 20], "{labels}");
    }
}

/// Conditional branches whose struct field differs only in nullability or in the case of its
/// name, which Spark adds no cast for. The result's field can be NULL if either branch's can,
/// and has the THEN branch's name, as in Spark's `If.dataType` and `CaseWhen.dataType`.
#[test]
fn conditional_branches_reconcile_struct_field_nullability_and_names() {
    let field = |name: &str, nullable: bool| Arc::new(Field::new(name, DataType::Int32, nullable));
    let struct_type = |field: &FieldRef| DataType::Struct(vec![Arc::clone(field)].into());
    let column = |field: &FieldRef, values: Vec<Option<i32>>| -> ArrayRef {
        Arc::new(StructArray::new(
            vec![Arc::clone(field)].into(),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
            None,
        ))
    };
    // The THEN branch's field, the ELSE branch's field, and the result's
    for (then_field, else_field, result_field) in [
        (field("x", false), field("x", true), field("x", true)),
        (field("x", true), field("x", false), field("x", true)),
        (field("x", true), field("X", true), field("x", true)),
        (field("X", false), field("x", true), field("X", true)),
    ] {
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("t", struct_type(&then_field), true),
            Field::new("e", struct_type(&else_field), true),
        ]);
        // A NULL field in the row that takes the ELSE branch, where it can be NULL
        let else_values = if else_field.is_nullable() {
            vec![Some(10), None]
        } else {
            vec![Some(10), Some(20)]
        };
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                column(&then_field, vec![Some(1), Some(2)]),
                column(&else_field, else_values.clone()),
            ],
        )
        .unwrap();
        let c = |name: &str| col(name, &schema).unwrap();
        let fields = format!("{then_field:?} {else_field:?}");
        let expected = column(&result_field, vec![Some(1), else_values[1]]);
        for expr in [
            create_if_expr(c("b"), c("t"), c("e"), &schema).unwrap(),
            create_case_when(vec![(c("b"), c("t"))], Some(c("e")), &schema).unwrap(),
        ] {
            assert_eq!(
                expr.data_type(&schema).unwrap(),
                struct_type(&result_field),
                "{fields} {expr}"
            );
            let result = evaluate_conditional(&expr, &batch);
            assert_eq!(result.as_ref(), expected.as_ref(), "{fields} {expr}");
        }
    }
}

#[test]
fn many_branches() {
    let len = 1000;
    let values: Int64Array = (0..len as i64).map(Some).collect();
    let schema = Schema::new(vec![Field::new("v", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
    let v = col("v", &schema).unwrap();
    // 300 branches, each matching the rows below its bound
    let when_then = (0..300)
        .map(|i| {
            let when = binary(Arc::clone(&v), Operator::Lt, lit(3 * i as i64));
            (when, lit(format!("b{i}")))
        })
        .collect();
    assert!(check_against_case_expr(
        &batch,
        when_then,
        Some(lit("rest"))
    ));
}

#[test]
fn timestamp_keeps_its_time_zone() {
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into()));
    let values =
        TimestampMicrosecondArray::from(vec![Some(1), Some(2), None]).with_timezone("+05:00");
    let schema = Schema::new(vec![
        Field::new("p", DataType::Boolean, true),
        Field::new("t", data_type.clone(), true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])),
            Arc::new(values),
        ],
    )
    .unwrap();
    let c = |name: &str| col(name, &schema).unwrap();
    let null = lit(ScalarValue::try_new_null(&data_type).unwrap());
    assert!(check_against_case_expr(
        &batch,
        vec![(c("p"), c("t"))],
        Some(null)
    ));
}
