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

//! Benchmarks for `AND`, `OR` and `IN` whose later operands can fail under ANSI mode.
//!
//! Each shape runs as `eager`, the way a plain `BinaryExpr` or DataFusion `InListExpr` evaluates
//! every operand over the whole batch, and as `lazy`, the way Comet plans it now. The data is
//! valid in every row, so both forms succeed and the difference is the cost of evaluating the
//! right operand only for the rows the left one has not decided, which `pending` sets for `AND`:
//! the percentage of rows where the left operand is true.

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{in_list, lit, BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{spark_in_list, Cast, EvalMode, LazyLogical, SparkCastOptions};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

type Expr = Arc<dyn PhysicalExpr>;

/// A right operand: its name, and how to build it
type Right = (&'static str, fn() -> Expr);

/// `g` an int in `0..100`, `n` a long in `0..1000`, and `s` an integer written as a string, with
/// `null_density` of each column NULL.
fn make_batch(null_density: f32) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(42);
    let valid = |rng: &mut StdRng| rng.random::<f32>() >= null_density;
    let g: Int32Array = (0..NUM_ROWS)
        .map(|_| valid(&mut rng).then(|| rng.random_range(0..100)))
        .collect();
    let n: Int64Array = (0..NUM_ROWS)
        .map(|_| valid(&mut rng).then(|| rng.random_range(0..1000)))
        .collect();
    let s: StringArray = (0..NUM_ROWS)
        .map(|_| valid(&mut rng).then(|| rng.random_range(0..1_000_000).to_string()))
        .collect();
    let columns: Vec<ArrayRef> = vec![Arc::new(g), Arc::new(n), Arc::new(s)];
    RecordBatch::try_new(Arc::new(schema()), columns).unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("g", DataType::Int32, true),
        Field::new("n", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ])
}

fn column(name: &str, index: usize) -> Expr {
    Arc::new(Column::new(name, index))
}

fn ansi_cast(child: Expr, to: DataType) -> Expr {
    Arc::new(Cast::new(
        child,
        to,
        SparkCastOptions::new_without_timezone(EvalMode::Ansi),
        None,
        None,
    ))
}

/// `CAST(n AS INT) > 1`, which fails if `n` overflows an int
fn narrowing_cast() -> Expr {
    let cast = ansi_cast(column("n", 1), DataType::Int32);
    Arc::new(BinaryExpr::new(cast, Operator::Gt, lit(1)))
}

/// `CAST(s AS INT) > 1`, which fails if `s` is not an integer
fn parsing_cast() -> Expr {
    let cast = ansi_cast(column("s", 2), DataType::Int32);
    Arc::new(BinaryExpr::new(cast, Operator::Gt, lit(1)))
}

fn g_cmp(op: Operator, value: i32) -> Expr {
    Arc::new(BinaryExpr::new(column("g", 0), op, lit(value)))
}

fn bench_logical(c: &mut Criterion) {
    let schema = schema();
    let mut group = c.benchmark_group("lazy_logical");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));

    for (nulls, null_density) in [("no_nulls", 0.0), ("with_nulls", 0.1)] {
        let batch = make_batch(null_density);
        let rights: [Right; 2] = [
            ("narrowing_cast", narrowing_cast),
            ("parsing_cast", parsing_cast),
        ];
        for (right_name, right) in rights {
            for pending in [0, 10, 50, 90, 100] {
                // `g < pending` is true for about `pending` percent of the rows
                let left = || g_cmp(Operator::Lt, pending);
                let id = format!("and_{right_name}_{nulls}");
                let eager: Expr = Arc::new(BinaryExpr::new(left(), Operator::And, right()));
                let lazy = LazyLogical::wrap(left(), Operator::And, right(), &schema).unwrap();
                for (form, expr) in [("eager", eager), ("lazy", lazy)] {
                    group.bench_with_input(
                        BenchmarkId::new(format!("{id}/{form}"), format!("pending_{pending}")),
                        &batch,
                        |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
                    );
                }
            }
        }
    }

    // OR is the mirror image: the right operand is pending where the left one is not true
    let batch = make_batch(0.0);
    let left = || g_cmp(Operator::GtEq, 50);
    let eager: Expr = Arc::new(BinaryExpr::new(left(), Operator::Or, parsing_cast()));
    let lazy = LazyLogical::wrap(left(), Operator::Or, parsing_cast(), &schema).unwrap();
    for (form, expr) in [("eager", eager), ("lazy", lazy)] {
        group.bench_with_input(
            BenchmarkId::new(format!("or_parsing_cast_no_nulls/{form}"), "pending_50"),
            &batch,
            |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
        );
    }
    group.finish();
}

fn bench_in(c: &mut Criterion) {
    let schema = schema();
    let batch = make_batch(0.0);
    let mut group = c.benchmark_group("lazy_in");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));

    for candidates in [2, 8, 64] {
        // `g IN (0, 1, ..., CAST(s AS INT))`: the literals match `candidates - 1` percent of rows
        let list = |fallible: bool| -> Vec<Expr> {
            let mut list: Vec<Expr> = (0..candidates - 1).map(lit).collect();
            list.push(if fallible {
                ansi_cast(column("s", 2), DataType::Int32)
            } else {
                lit(-1)
            });
            list
        };
        let value = || column("g", 0);
        // DataFusion evaluates every candidate over the whole batch
        let eager = in_list(value(), list(true), &false, &schema).unwrap();
        // Comet evaluates the cast only for the rows no literal matched
        let lazy = spark_in_list(value(), list(true), false, &schema).unwrap();
        // All candidates are constants, so nothing is lazy and DataFusion's static filter is used
        let constants = spark_in_list(value(), list(false), false, &schema).unwrap();
        for (form, expr) in [
            ("eager", eager),
            ("lazy", lazy),
            ("all_constants", constants),
        ] {
            group.bench_with_input(
                BenchmarkId::new(format!("in_{form}"), format!("candidates_{candidates}")),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
            );
        }
    }

    // A value that parses a string is dear to evaluate again for every candidate, so it keeps
    // DataFusion's `IN`, which evaluates it once
    for candidates in [8, 64] {
        let list = || -> Vec<Expr> {
            (0..candidates)
                .map(|_| ansi_cast(column("n", 1), DataType::Int32))
                .collect()
        };
        let value = || -> Expr {
            Arc::new(Cast::new(
                column("s", 2),
                DataType::Int32,
                SparkCastOptions::new_without_timezone(EvalMode::Try),
                None,
                None,
            ))
        };
        let eager = in_list(value(), list(), &false, &schema).unwrap();
        let lazy = spark_in_list(value(), list(), false, &schema).unwrap();
        for (form, expr) in [("eager", eager), ("lazy", lazy)] {
            group.bench_with_input(
                BenchmarkId::new(
                    format!("in_parsing_value_{form}"),
                    format!("candidates_{candidates}"),
                ),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_logical, bench_in);
criterion_main!(benches);
