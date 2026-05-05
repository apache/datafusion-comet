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

//! Benchmarks comparing the old Cast->BinaryExpr->Cast chain vs the fused WideDecimalBinaryExpr
//! for Decimal128 arithmetic that requires wider intermediate precision.

use arrow::array::builder::Decimal128Builder;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{
    Cast, EvalMode, SparkCastOptions, WideDecimalBinaryExpr, WideDecimalOp,
};
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

/// Build a RecordBatch with two Decimal128 columns.
fn make_decimal_batch(p1: u8, s1: i8, p2: u8, s2: i8) -> RecordBatch {
    let mut left = Decimal128Builder::new();
    let mut right = Decimal128Builder::new();
    for i in 0..BATCH_SIZE as i128 {
        left.append_value(123456789012345_i128 + i * 1000);
        right.append_value(987654321098765_i128 - i * 1000);
    }
    let left = left.finish().with_data_type(DataType::Decimal128(p1, s1));
    let right = right.finish().with_data_type(DataType::Decimal128(p2, s2));
    let schema = Schema::new(vec![
        Field::new("left", DataType::Decimal128(p1, s1), false),
        Field::new("right", DataType::Decimal128(p2, s2), false),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(left), Arc::new(right)]).unwrap()
}

/// Old approach: Cast(Decimal128->Decimal256) both sides, BinaryExpr, Cast(Decimal256->Decimal128).
fn build_old_expr(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    op: Operator,
    out_type: DataType,
) -> Arc<dyn PhysicalExpr> {
    let left_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("left", 0));
    let right_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("right", 1));
    let cast_opts = SparkCastOptions::new_without_timezone(EvalMode::Legacy, false);
    let left_cast = Arc::new(Cast::new(
        left_col,
        DataType::Decimal256(p1, s1),
        cast_opts.clone(),
        None,
        None,
    ));
    let right_cast = Arc::new(Cast::new(
        right_col,
        DataType::Decimal256(p2, s2),
        cast_opts.clone(),
        None,
        None,
    ));
    let binary = Arc::new(BinaryExpr::new(left_cast, op, right_cast));
    Arc::new(Cast::new(binary, out_type, cast_opts, None, None))
}

/// New approach: single fused WideDecimalBinaryExpr.
fn build_new_expr(op: WideDecimalOp, p_out: u8, s_out: i8) -> Arc<dyn PhysicalExpr> {
    let left_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("left", 0));
    let right_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("right", 1));
    Arc::new(WideDecimalBinaryExpr::new(
        left_col,
        right_col,
        op,
        p_out,
        s_out,
        EvalMode::Legacy,
    ))
}

fn bench_case(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    batch: &RecordBatch,
    old_expr: &Arc<dyn PhysicalExpr>,
    new_expr: &Arc<dyn PhysicalExpr>,
) {
    group.bench_with_input(BenchmarkId::new("old", name), batch, |b, batch| {
        b.iter(|| old_expr.evaluate(batch).unwrap());
    });
    group.bench_with_input(BenchmarkId::new("fused", name), batch, |b, batch| {
        b.iter(|| new_expr.evaluate(batch).unwrap());
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_decimal");

    // Case 1: Add with same scale - Decimal128(38,10) + Decimal128(38,10) -> Decimal128(38,10)
    // Triggers wide path because max(s1,s2) + max(p1-s1, p2-s2) = 10 + 28 = 38 >= 38
    {
        let batch = make_decimal_batch(38, 10, 38, 10);
        let old = build_old_expr(38, 10, 38, 10, Operator::Plus, DataType::Decimal128(38, 10));
        let new = build_new_expr(WideDecimalOp::Add, 38, 10);
        bench_case(&mut group, "add_same_scale", &batch, &old, &new);
    }

    // Case 2: Add with different scales - Decimal128(38,6) + Decimal128(38,4) -> Decimal128(38,6)
    {
        let batch = make_decimal_batch(38, 6, 38, 4);
        let old = build_old_expr(38, 6, 38, 4, Operator::Plus, DataType::Decimal128(38, 6));
        let new = build_new_expr(WideDecimalOp::Add, 38, 6);
        bench_case(&mut group, "add_diff_scale", &batch, &old, &new);
    }

    // Case 3: Multiply - Decimal128(20,10) * Decimal128(20,10) -> Decimal128(38,6)
    // Triggers wide path because p1 + p2 = 40 >= 38
    {
        let batch = make_decimal_batch(20, 10, 20, 10);
        let old = build_old_expr(
            20,
            10,
            20,
            10,
            Operator::Multiply,
            DataType::Decimal128(38, 6),
        );
        let new = build_new_expr(WideDecimalOp::Multiply, 38, 6);
        bench_case(&mut group, "multiply", &batch, &old, &new);
    }

    // Case 4: Subtract with same scale - Decimal128(38,18) - Decimal128(38,18) -> Decimal128(38,18)
    {
        let batch = make_decimal_batch(38, 18, 38, 18);
        let old = build_old_expr(
            38,
            18,
            38,
            18,
            Operator::Minus,
            DataType::Decimal128(38, 18),
        );
        let new = build_new_expr(WideDecimalOp::Subtract, 38, 18);
        bench_case(&mut group, "subtract", &batch, &old, &new);
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
