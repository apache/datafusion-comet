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

use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

fn i32_batch(size: usize, null_every: usize) -> RecordBatch {
    let a: Int32Array = (0..size)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                Some((i as i32) % 100_000)
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap()
}

fn i64_batch(size: usize, big: bool) -> RecordBatch {
    let a: Int64Array = (0..size)
        .map(|i| {
            if big {
                // Large enough that value * 10^4 overflows Decimal128(15, 4).
                Some(1_000_000_000_000_000_i64 + i as i64)
            } else {
                Some((i as i64) % 100_000)
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap()
}

fn cast(col: &str, to: DataType, mode: EvalMode) -> Cast {
    Cast::new(
        Arc::new(Column::new(col, 0)),
        to,
        SparkCastOptions::new_without_timezone(mode, false),
        None,
        None,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let dec_15_4 = DataType::Decimal128(15, 4);
    let dec_38_4 = DataType::Decimal128(38, 4);

    let i32_no_nulls = i32_batch(size, 0);
    let i32_nulls = i32_batch(size, 10);
    let i64_small = i64_batch(size, false);
    let i64_big = i64_batch(size, true);

    let c_i32 = cast("a", dec_15_4.clone(), EvalMode::Legacy);
    let c_i32_ansi = cast("a", dec_15_4.clone(), EvalMode::Ansi);
    let c_i64 = cast("a", dec_38_4, EvalMode::Legacy);
    let c_i64_overflow = cast("a", dec_15_4, EvalMode::Legacy);

    c.bench_function("cast_int_to_decimal: i32 -> dec(15,4)", |b| {
        b.iter(|| black_box(c_i32.evaluate(black_box(&i32_no_nulls)).unwrap()))
    });
    c.bench_function("cast_int_to_decimal: i32 -> dec(15,4), nulls", |b| {
        b.iter(|| black_box(c_i32.evaluate(black_box(&i32_nulls)).unwrap()))
    });
    c.bench_function("cast_int_to_decimal: i64 -> dec(38,4)", |b| {
        b.iter(|| black_box(c_i64.evaluate(black_box(&i64_small)).unwrap()))
    });
    c.bench_function("cast_int_to_decimal: i32 -> dec(15,4) ansi", |b| {
        b.iter(|| black_box(c_i32_ansi.evaluate(black_box(&i32_no_nulls)).unwrap()))
    });
    c.bench_function("cast_int_to_decimal: i64 -> dec(15,4) overflow", |b| {
        b.iter(|| black_box(c_i64_overflow.evaluate(black_box(&i64_big)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
