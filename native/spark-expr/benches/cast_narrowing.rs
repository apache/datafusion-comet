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

use arrow::array::{Array, Decimal128Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

fn f64_batch(size: usize) -> RecordBatch {
    // Small in-range values so narrowing to i8 does not overflow.
    let a: Float64Array = (0..size)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                Some((i % 100) as f64)
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap()
}

fn dec_batch(size: usize) -> RecordBatch {
    let a: Decimal128Array = (0..size)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                Some((i % 100) as i128 * 100)
            }
        })
        .collect::<Decimal128Array>()
        .with_precision_and_scale(10, 2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        a.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap()
}

fn cast(to: DataType, mode: EvalMode) -> Cast {
    Cast::new(
        Arc::new(Column::new("a", 0)),
        to,
        SparkCastOptions::new_without_timezone(mode, false),
        None,
        None,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let f = f64_batch(size);
    let d = dec_batch(size);

    let f_i8 = cast(DataType::Int8, EvalMode::Legacy);
    let f_i32 = cast(DataType::Int32, EvalMode::Legacy);
    let f_i32_ansi = cast(DataType::Int32, EvalMode::Ansi);
    let d_i8 = cast(DataType::Int8, EvalMode::Legacy);
    let d_i32 = cast(DataType::Int32, EvalMode::Legacy);

    c.bench_function("cast_narrowing: f64 -> i8", |b| {
        b.iter(|| black_box(f_i8.evaluate(black_box(&f)).unwrap()))
    });
    c.bench_function("cast_narrowing: f64 -> i32", |b| {
        b.iter(|| black_box(f_i32.evaluate(black_box(&f)).unwrap()))
    });
    c.bench_function("cast_narrowing: f64 -> i32 ansi", |b| {
        b.iter(|| black_box(f_i32_ansi.evaluate(black_box(&f)).unwrap()))
    });
    c.bench_function("cast_narrowing: dec -> i8", |b| {
        b.iter(|| black_box(d_i8.evaluate(black_box(&d)).unwrap()))
    });
    c.bench_function("cast_narrowing: dec -> i32", |b| {
        b.iter(|| black_box(d_i32.evaluate(black_box(&d)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
