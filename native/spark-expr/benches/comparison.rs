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

//! Benchmarks for comparison expressions (eq, eq_null_safe, lt, is_null, etc.)

use arrow::array::builder::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, IsNotNullExpr, IsNullExpr};
use datafusion::physical_expr::PhysicalExpr;
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

/// Create a batch with two int64 columns with a specified null percentage
fn create_int64_batch(null_pct: usize) -> RecordBatch {
    let mut c1 = Int64Builder::with_capacity(BATCH_SIZE);
    let mut c2 = Int64Builder::with_capacity(BATCH_SIZE);

    for i in 0..BATCH_SIZE {
        if null_pct > 0 && i % (100 / null_pct) == 0 {
            c1.append_null();
        } else {
            c1.append_value(i as i64);
        }
        if null_pct > 0 && (i + 1) % (100 / null_pct) == 0 {
            c2.append_null();
        } else {
            c2.append_value((i as i64) % 1000);
        }
    }

    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int64, null_pct > 0),
        Field::new("c2", DataType::Int64, null_pct > 0),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(c1.finish()), Arc::new(c2.finish())],
    )
    .unwrap()
}

/// Create a batch with two string columns with a specified null percentage
fn create_string_batch(null_pct: usize) -> RecordBatch {
    let mut c1 = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20);
    let mut c2 = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 20);

    for i in 0..BATCH_SIZE {
        if null_pct > 0 && i % (100 / null_pct) == 0 {
            c1.append_null();
        } else {
            c1.append_value(format!("string value {}", i));
        }
        if null_pct > 0 && (i + 1) % (100 / null_pct) == 0 {
            c2.append_null();
        } else {
            c2.append_value(format!("string value {}", i % 1000));
        }
    }

    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, null_pct > 0),
        Field::new("c2", DataType::Utf8, null_pct > 0),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(c1.finish()), Arc::new(c2.finish())],
    )
    .unwrap()
}

fn bench_int64_equality(c: &mut Criterion) {
    let mut group = c.benchmark_group("int64_equality");

    for null_pct in [0, 10, 50] {
        let batch = create_int64_batch(null_pct);

        // Regular equality: c1 = c2
        let eq_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::Eq,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("eq", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(eq_expr.evaluate(black_box(batch)).unwrap()));
            },
        );

        // Null-safe equality: c1 <=> c2 (IsNotDistinctFrom)
        let eq_null_safe_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::IsNotDistinctFrom,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("eq_null_safe", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(eq_null_safe_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_int64_less_than(c: &mut Criterion) {
    let mut group = c.benchmark_group("int64_less_than");

    for null_pct in [0, 10, 50] {
        let batch = create_int64_batch(null_pct);

        // Less than: c1 < c2
        let lt_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::Lt,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("lt", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(lt_expr.evaluate(black_box(batch)).unwrap()));
            },
        );

        // Less than or equal: c1 <= c2
        let lteq_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::LtEq,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("lt_eq", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(lteq_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_string_equality(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_equality");

    for null_pct in [0, 10, 50] {
        let batch = create_string_batch(null_pct);

        // Regular equality: c1 = c2
        let eq_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::Eq,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("eq", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(eq_expr.evaluate(black_box(batch)).unwrap()));
            },
        );

        // Null-safe equality: c1 <=> c2 (IsNotDistinctFrom)
        let eq_null_safe_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::IsNotDistinctFrom,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("eq_null_safe", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(eq_null_safe_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_string_less_than(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_less_than");

    for null_pct in [0, 10, 50] {
        let batch = create_string_batch(null_pct);

        // Less than: c1 < c2
        let lt_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::Lt,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("lt", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(lt_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_is_null(c: &mut Criterion) {
    let mut group = c.benchmark_group("is_null");

    for null_pct in [0, 10, 50] {
        let batch = create_int64_batch(null_pct);

        // IS NULL check
        let is_null_expr = Arc::new(IsNullExpr::new(make_col("c1", 0)));

        group.bench_with_input(
            BenchmarkId::new("int64_is_null", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(is_null_expr.evaluate(black_box(batch)).unwrap()));
            },
        );

        // IS NOT NULL check
        let is_not_null_expr = Arc::new(IsNotNullExpr::new(make_col("c1", 0)));

        group.bench_with_input(
            BenchmarkId::new("int64_is_not_null", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(is_not_null_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    // Also benchmark string is_null
    for null_pct in [0, 10, 50] {
        let batch = create_string_batch(null_pct);

        let is_null_expr = Arc::new(IsNullExpr::new(make_col("c1", 0)));

        group.bench_with_input(
            BenchmarkId::new("string_is_null", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(is_null_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_not_equal(c: &mut Criterion) {
    let mut group = c.benchmark_group("not_equal");

    for null_pct in [0, 10, 50] {
        let batch = create_int64_batch(null_pct);

        // Not equal: c1 != c2
        let neq_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::NotEq,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("int64_neq", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(neq_expr.evaluate(black_box(batch)).unwrap()));
            },
        );

        // Null-safe not equal: c1 IS DISTINCT FROM c2
        let neq_null_safe_expr = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::IsDistinctFrom,
            make_col("c2", 1),
        ));

        group.bench_with_input(
            BenchmarkId::new("int64_neq_null_safe", format!("null_{}pct", null_pct)),
            &batch,
            |b, batch| {
                b.iter(|| black_box(neq_null_safe_expr.evaluate(black_box(batch)).unwrap()));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_int64_equality,
    bench_int64_less_than,
    bench_string_equality,
    bench_string_less_than,
    bench_is_null,
    bench_not_equal,
);
criterion_main!(benches);
