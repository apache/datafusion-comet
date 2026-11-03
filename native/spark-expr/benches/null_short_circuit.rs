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
//! Benchmarks for ANSI arithmetic whose second operand can fail.
//!
//! Each shape runs as `eager`, evaluating both operands over the whole batch, and as `lazy`, the
//! way Comet plans it now, evaluating the second operand only for the rows where the first one is
//! not NULL. The data is valid in every row, so both forms succeed, and the difference is the cost
//! of skipping the rows Spark skips.

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::prelude::SessionContext;
use datafusion_comet_spark_expr::{
    create_comet_physical_fun_with_eval_mode, Cast, CheckOverflow, EvalMode, NullShortCircuit,
    SparkCastOptions,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

const DECIMAL: DataType = DataType::Decimal128(12, 2);

type Expr = Arc<dyn PhysicalExpr>;

/// An expression: its name, and how to build it
type Shape = (&'static str, fn() -> Expr);

/// `a` a long with `null_density` of it NULL, `p` the same as a decimal, and `b`, `c`, `s`, a long
/// written as a string, and `d`, a decimal, never NULL.
fn make_batch(null_density: f32) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(42);
    let a: Int64Array = (0..NUM_ROWS)
        .map(|_| (rng.random::<f32>() >= null_density).then(|| rng.random_range(0..1000)))
        .collect();
    let b: Int64Array = (0..NUM_ROWS)
        .map(|_| Some(rng.random_range(0..1000)))
        .collect();
    let c: Int64Array = (0..NUM_ROWS)
        .map(|_| Some(rng.random_range(0..1000)))
        .collect();
    let s: StringArray = (0..NUM_ROWS)
        .map(|_| Some(rng.random_range(0..1_000_000).to_string()))
        .collect();
    let p = a
        .iter()
        .map(|v| v.map(i128::from))
        .collect::<Decimal128Array>()
        .with_data_type(DECIMAL);
    let d = (0..NUM_ROWS)
        .map(|_| Some(rng.random_range(0..100)))
        .collect::<Decimal128Array>()
        .with_data_type(DECIMAL);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(a),
        Arc::new(b),
        Arc::new(c),
        Arc::new(s),
        Arc::new(p),
        Arc::new(d),
    ];
    RecordBatch::try_new(Arc::new(schema()), columns).unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("p", DECIMAL, true),
        Field::new("d", DECIMAL, false),
    ])
}

fn column(name: &str, index: usize) -> Expr {
    Arc::new(Column::new(name, index))
}

/// An ANSI `checked_add`, `checked_sub` or `checked_mul` over longs, as Comet plans `+`, `-` and `*`
fn checked(name: &'static str, left: Expr, right: Expr) -> Expr {
    let ctx = SessionContext::new();
    let fun = create_comet_physical_fun_with_eval_mode(
        name,
        DataType::Int64,
        &ctx.state(),
        None,
        EvalMode::Ansi,
    )
    .unwrap();
    Arc::new(ScalarFunctionExpr::new(
        name,
        fun,
        vec![left, right],
        Arc::new(Field::new(name, DataType::Int64, true)),
        Arc::new(ConfigOptions::default()),
    ))
}

/// `a + CAST(s AS BIGINT)`, whose right operand fails if `s` is not a number
fn add_parsing_cast() -> Expr {
    let cast = Arc::new(Cast::new(
        column("s", 3),
        DataType::Int64,
        SparkCastOptions::new_without_timezone(EvalMode::Ansi),
        None,
        None,
    ));
    checked("checked_add", column("a", 0), cast)
}

/// `a * (b - c)`, whose right operand fails if the subtraction overflows
fn mul_nested_sub() -> Expr {
    let sub = checked("checked_sub", column("b", 1), column("c", 2));
    checked("checked_mul", column("a", 0), sub)
}

/// `p * (1 - d)` over decimals, as in TPC-H Q1, whose right operand Comet plans as a
/// `CheckOverflow` that fails under ANSI if the subtraction overflows
fn decimal_mul_nested_sub() -> Expr {
    let one = Arc::new(Literal::new(ScalarValue::Decimal128(Some(100), 12, 2)));
    let sub: Expr = Arc::new(BinaryExpr::new(one, Operator::Minus, column("d", 5)));
    let sub_type = sub.data_type(&schema()).unwrap();
    let checked = Arc::new(CheckOverflow::new(sub, sub_type, true, None, None));
    Arc::new(BinaryExpr::new(column("p", 4), Operator::Multiply, checked))
}

fn bench_arithmetic(c: &mut Criterion) {
    let schema = schema();
    let mut group = c.benchmark_group("null_short_circuit");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));

    let shapes: [Shape; 3] = [
        ("add_parsing_cast", add_parsing_cast),
        ("mul_nested_sub", mul_nested_sub),
        ("decimal_mul_nested_sub", decimal_mul_nested_sub),
    ];
    for (name, shape) in shapes {
        for null_percent in [0, 10, 50, 70, 90] {
            let batch = make_batch(null_percent as f32 / 100.0);
            let lazy = NullShortCircuit::wrap(shape(), &schema).unwrap();
            assert!(lazy.is::<NullShortCircuit>());
            for (form, expr) in [("eager", shape()), ("lazy", lazy)] {
                group.bench_with_input(
                    BenchmarkId::new(format!("{name}/{form}"), format!("nulls_{null_percent}")),
                    &batch,
                    |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_arithmetic);
criterion_main!(benches);
