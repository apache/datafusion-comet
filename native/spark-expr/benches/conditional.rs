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

//! Benchmarks for `CASE WHEN` and `IF`, built the way Comet's planner builds them.
//!
//! The first eight shapes are the queries in `CometConditionalExpressionBenchmark`. The rest cover
//! patterns that are common in TPC-H and TPC-DS, the null guard Comet puts around a divisor, and a
//! branch that can fail, which must still be evaluated only for the rows that choose it.

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, IsNotNullExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{
    create_case_when, Cast, EvalMode, IfExpr, NormalizeNaNAndZero, SparkCastOptions,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

type Expr = Arc<dyn PhysicalExpr>;

/// Columns shaped like the ones in `CometConditionalExpressionBenchmark`:
/// `c1` a random long, `c2` an int in `0..100`, `c3` another random long, and `c4` / `c5` short
/// and `c6` / `c7` long strings. `null_density` applies to every column. With `sorted`, `c1` and
/// `c2` ascend through the batch, so every predicate over them selects one contiguous run of rows.
fn make_batch(null_density: f32, sorted: bool) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(42);
    let mut c1: Vec<i64> = (0..NUM_ROWS).map(|_| rng.random::<i64>()).collect();
    let mut c2: Vec<i32> = (0..NUM_ROWS).map(|_| rng.random_range(0..100)).collect();
    if sorted {
        c1.sort_unstable();
        c2.sort_unstable();
    }
    let c3: Vec<i64> = (0..NUM_ROWS).map(|_| rng.random::<i64>()).collect();
    let mut null = |v: bool| {
        if rng.random::<f32>() < null_density {
            false
        } else {
            v
        }
    };
    let valid: Vec<Vec<bool>> = (0..7)
        .map(|_| (0..NUM_ROWS).map(|_| null(true)).collect())
        .collect();
    let opt = |col: usize, i: usize| valid[col][i];

    let c1: Int64Array = (0..NUM_ROWS).map(|i| opt(0, i).then_some(c1[i])).collect();
    let c2: Int32Array = (0..NUM_ROWS).map(|i| opt(1, i).then_some(c2[i])).collect();
    let c3: Int64Array = (0..NUM_ROWS).map(|i| opt(2, i).then_some(c3[i])).collect();
    let short = |col: usize, tag: &str| -> StringArray {
        (0..NUM_ROWS)
            .map(|i| opt(col, i).then(|| format!("{tag}{}", i * 7919 % 100_000)))
            .collect()
    };
    let long = |col: usize, tag: &str| -> StringArray {
        (0..NUM_ROWS)
            .map(|i| opt(col, i).then(|| format!("{tag}{i}-").repeat(12)))
            .collect()
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(c1),
        Arc::new(c2),
        Arc::new(c3),
        Arc::new(short(3, "s")),
        Arc::new(short(4, "t")),
        Arc::new(long(5, "long value ")),
        Arc::new(long(6, "other value ")),
    ];
    RecordBatch::try_new(Arc::new(schema()), columns).unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Int64, true),
        Field::new("c4", DataType::Utf8, true),
        Field::new("c5", DataType::Utf8, true),
        Field::new("c6", DataType::Utf8, true),
        Field::new("c7", DataType::Utf8, true),
    ])
}

fn col(name: &str) -> Expr {
    let index = schema().index_of(name).unwrap();
    Arc::new(Column::new(name, index))
}

fn lit(value: impl Into<ScalarValue>) -> Expr {
    Arc::new(Literal::new(value.into()))
}

fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
    Arc::new(BinaryExpr::new(left, op, right))
}

/// A Spark `Cast` as the planner serializes it for a `CAST` in the query.
fn spark_cast(child: Expr, data_type: DataType) -> Expr {
    Arc::new(Cast::new(
        child,
        data_type,
        SparkCastOptions::new(EvalMode::Legacy, "America/Los_Angeles", false),
        None,
        None,
    ))
}

fn if_expr(predicate: Expr, if_true: Expr, if_false: Expr) -> Expr {
    Arc::new(IfExpr::new(predicate, if_true, if_false))
}

/// `CASE WHEN`, built the way the planner builds it.
fn case_when(when_then: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Expr {
    create_case_when(when_then, else_expr, &schema()).unwrap()
}

/// `c2 < bound` for bounds 10, 20, ..., one per branch.
fn c2_below(bound: i32) -> Expr {
    binary(col("c2"), Operator::Lt, lit(bound))
}

/// `c1 < 0`, true for about half the rows in a random order.
fn c1_negative() -> Expr {
    binary(col("c1"), Operator::Lt, lit(0i64))
}

fn case_literal_3_branches() -> Expr {
    case_when(
        vec![
            (c1_negative(), lit("<0")),
            (binary(col("c1"), Operator::Eq, lit(0i64)), lit("=0")),
        ],
        Some(lit(">0")),
    )
}

fn case_literal_10_branches() -> Expr {
    let when_then = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
        .iter()
        .enumerate()
        .map(|(i, v)| (c2_below(10 * (i as i32 + 1)), lit(*v)))
        .collect();
    case_when(when_then, Some(lit("j")))
}

fn case_column_3_branches() -> Expr {
    case_when(
        vec![
            (c1_negative(), col("c3")),
            (binary(col("c1"), Operator::Eq, lit(0i64)), col("c1")),
        ],
        Some(binary(col("c3"), Operator::Plus, col("c1"))),
    )
}

/// The 10-branch column case is a DOUBLE, because `c1 / 2` is. Spark casts every other branch to
/// DOUBLE, and Comet's legacy-mode divide guards the divisor with `IF(d = 0, NULL, d)`.
fn case_column_10_branches() -> Expr {
    let double = |e: Expr| spark_cast(e, DataType::Float64);
    let long = |e: Expr| spark_cast(e, DataType::Int64);
    let two = || -> Expr {
        Arc::new(NormalizeNaNAndZero::new(
            DataType::Float64,
            lit(ScalarValue::Float64(Some(2.0))),
        ))
    };
    let divisor = if_expr(
        binary(two(), Operator::Eq, lit(0.0f64)),
        lit(ScalarValue::Float64(None)),
        two(),
    );
    let values = vec![
        double(col("c1")),
        double(col("c3")),
        double(binary(col("c1"), Operator::Plus, col("c3"))),
        double(binary(col("c1"), Operator::Minus, long(col("c2")))),
        double(binary(col("c3"), Operator::Multiply, lit(2i64))),
        binary(double(col("c1")), Operator::Divide, divisor),
        double(binary(long(col("c2")), Operator::Plus, col("c3"))),
        double(binary(col("c1"), Operator::Multiply, long(col("c2")))),
        double(binary(col("c3"), Operator::Minus, col("c1"))),
        double(binary(
            binary(col("c1"), Operator::Plus, long(col("c2"))),
            Operator::Plus,
            col("c3"),
        )),
    ];
    let mut values = values.into_iter();
    let when_then = (1..10)
        .map(|i| (c2_below(10 * i), values.next().unwrap()))
        .collect();
    case_when(when_then, values.next())
}

fn if_literal() -> Expr {
    if_expr(c1_negative(), lit("<0"), lit(">=0"))
}

fn if_column() -> Expr {
    if_expr(
        c1_negative(),
        col("c3"),
        binary(col("c1"), Operator::Plus, col("c3")),
    )
}

fn nested_if_literal() -> Expr {
    if_expr(
        c2_below(25),
        lit("a"),
        if_expr(
            c2_below(50),
            lit("b"),
            if_expr(c2_below(75), lit("c"), lit("d")),
        ),
    )
}

fn nested_if_column() -> Expr {
    if_expr(
        c2_below(25),
        col("c1"),
        if_expr(
            c2_below(50),
            col("c3"),
            if_expr(
                c2_below(75),
                binary(col("c1"), Operator::Plus, col("c3")),
                binary(col("c3"), Operator::Multiply, lit(2i64)),
            ),
        ),
    )
}

/// `sum(CASE WHEN ... THEN 1 ELSE 0 END)`, as in TPC-H q12.
fn case_int_literals() -> Expr {
    case_when(vec![(c2_below(50), lit(1i32))], Some(lit(0i32)))
}

/// `sum(CASE WHEN ... THEN col ELSE NULL END)`, as in TPC-DS q2 and q43.
fn case_column_or_null() -> Expr {
    case_when(vec![(c2_below(50), col("c3"))], None)
}

/// The guard Comet's legacy-mode divide puts around a divisor that is not a literal.
fn divisor_guard() -> Expr {
    if_expr(
        binary(col("c2"), Operator::Eq, lit(0i32)),
        lit(ScalarValue::Int32(None)),
        col("c2"),
    )
}

/// `coalesce(c1, c3)`, which Comet serializes as a `CASE WHEN`.
fn coalesce() -> Expr {
    case_when(
        vec![(Arc::new(IsNotNullExpr::new(col("c1"))), col("c1"))],
        Some(col("c3")),
    )
}

fn if_short_strings() -> Expr {
    if_expr(c1_negative(), col("c4"), col("c5"))
}

fn if_long_strings() -> Expr {
    if_expr(c1_negative(), col("c6"), col("c7"))
}

fn if_boolean() -> Expr {
    if_expr(
        c1_negative(),
        c2_below(50),
        binary(col("c2"), Operator::Gt, lit(90i32)),
    )
}

/// A branch value that can fail: integer division errors on a zero divisor, so it must only be
/// evaluated for the rows whose divisor the WHEN has checked.
fn case_fallible_branch() -> Expr {
    let c2_long = spark_cast(col("c2"), DataType::Int64);
    case_when(
        vec![(
            binary(col("c2"), Operator::NotEq, lit(0i32)),
            binary(col("c1"), Operator::Divide, c2_long),
        )],
        Some(lit(0i64)),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let no_nulls = make_batch(0.0, false);
    let sparse_nulls = make_batch(0.1, false);
    let dense_nulls = make_batch(0.9, false);
    let sorted = make_batch(0.0, true);

    let mut group = c.benchmark_group("conditional");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    let mut bench = |name: &str, expr: Expr, batch: &RecordBatch| {
        group.bench_function(name, |b| {
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        });
    };

    bench(
        "case literal 3 branches",
        case_literal_3_branches(),
        &no_nulls,
    );
    bench(
        "case literal 10 branches",
        case_literal_10_branches(),
        &no_nulls,
    );
    bench(
        "case column 3 branches",
        case_column_3_branches(),
        &no_nulls,
    );
    bench(
        "case column 10 branches",
        case_column_10_branches(),
        &no_nulls,
    );
    bench("if literal", if_literal(), &no_nulls);
    bench("if column", if_column(), &no_nulls);
    bench("nested if literal", nested_if_literal(), &no_nulls);
    bench("nested if column", nested_if_column(), &no_nulls);

    bench("case int literals", case_int_literals(), &no_nulls);
    bench("case column or null", case_column_or_null(), &no_nulls);
    bench("divisor guard", divisor_guard(), &no_nulls);
    bench("coalesce, sparse nulls", coalesce(), &sparse_nulls);
    bench("coalesce, dense nulls", coalesce(), &dense_nulls);
    bench("if short strings", if_short_strings(), &no_nulls);
    bench("if long strings", if_long_strings(), &no_nulls);
    bench("if boolean", if_boolean(), &no_nulls);
    bench("case fallible branch", case_fallible_branch(), &no_nulls);

    bench("if column, sparse nulls", if_column(), &sparse_nulls);
    bench("if column, dense nulls", if_column(), &dense_nulls);
    bench(
        "if short strings, sparse nulls",
        if_short_strings(),
        &sparse_nulls,
    );
    bench(
        "if short strings, dense nulls",
        if_short_strings(),
        &dense_nulls,
    );
    bench(
        "case column 3 branches, dense nulls",
        case_column_3_branches(),
        &dense_nulls,
    );

    bench("if column, sorted", if_column(), &sorted);
    bench("if short strings, sorted", if_short_strings(), &sorted);
    bench(
        "case literal 10 branches, sorted",
        case_literal_10_branches(),
        &sorted,
    );
    bench(
        "case column 10 branches, sorted",
        case_column_10_branches(),
        &sorted,
    );
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
