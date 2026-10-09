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

use arrow::array::{Float32Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

fn f64_batch(size: usize, null_every: usize, big: bool) -> RecordBatch {
    let a: Float64Array = (0..size)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else if big {
                // value * 10^4 overflows Decimal128(15, 4).
                Some(1.0e12 + i as f64)
            } else {
                Some((i % 100_000) as f64 * 0.5)
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap()
}

fn f32_batch(size: usize) -> RecordBatch {
    let a: Float32Array = (0..size)
        .map(|i| Some((i % 100_000) as f32 * 0.5))
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));
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
    let dec = DataType::Decimal128(15, 4);

    let f64_no_nulls = f64_batch(size, 0, false);
    let f64_nulls = f64_batch(size, 10, false);
    let f64_big = f64_batch(size, 0, true);
    let f32 = f32_batch(size);

    let c_legacy = cast(dec.clone(), EvalMode::Legacy);
    let c_ansi = cast(dec.clone(), EvalMode::Ansi);

    c.bench_function("cast_float_to_decimal: f64 -> dec(15,4)", |b| {
        b.iter(|| black_box(c_legacy.evaluate(black_box(&f64_no_nulls)).unwrap()))
    });
    c.bench_function("cast_float_to_decimal: f64 -> dec(15,4), nulls", |b| {
        b.iter(|| black_box(c_legacy.evaluate(black_box(&f64_nulls)).unwrap()))
    });
    c.bench_function("cast_float_to_decimal: f32 -> dec(15,4)", |b| {
        b.iter(|| black_box(c_legacy.evaluate(black_box(&f32)).unwrap()))
    });
    c.bench_function("cast_float_to_decimal: f64 -> dec(15,4) ansi", |b| {
        b.iter(|| black_box(c_ansi.evaluate(black_box(&f64_no_nulls)).unwrap()))
    });
    c.bench_function("cast_float_to_decimal: f64 -> dec(15,4) overflow", |b| {
        b.iter(|| black_box(c_legacy.evaluate(black_box(&f64_big)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
