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

use arrow::array::{Decimal128Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

const NUM_ROWS: usize = 4096;

fn create_decimal_batch(precision: u8, scale: i8, unscaled: impl Fn(usize) -> i128) -> RecordBatch {
    let values: Vec<Option<i128>> = (0..NUM_ROWS)
        .map(|i| if i % 10 == 9 { None } else { Some(unscaled(i)) })
        .collect();
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Decimal128(precision, scale),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn cast_to_utf8() -> Cast {
    Cast::new(
        Arc::new(Column::new("a", 0)),
        DataType::Utf8,
        SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
        None,
        None,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    // plain notation, fraction shorter than the integer part
    let small_scale = create_decimal_batch(18, 2, |i| (i as i128 * 7919) - 1_000_000);
    // plain notation, leading zeroes in the fraction
    let leading_zeroes = create_decimal_batch(18, 6, |i| (i as i128 % 97) + 1);
    // wide coefficients spanning more than one 64-bit digit group
    let wide = create_decimal_batch(38, 10, |i| {
        i128::from(i as i64 + 1) * 1_234_567_890_123_456_789i128 * 1_000_000_007i128
    });
    // scientific notation (adjusted exponent < -6)
    let scientific = create_decimal_batch(38, 30, |i| (i as i128 % 13) + 1);

    let cast = cast_to_utf8();

    let mut group = c.benchmark_group("cast_decimal_to_string");
    group.bench_function("decimal128_scale_2", |b| {
        b.iter(|| cast.evaluate(&small_scale).unwrap());
    });
    group.bench_function("decimal128_leading_zeroes", |b| {
        b.iter(|| cast.evaluate(&leading_zeroes).unwrap());
    });
    group.bench_function("decimal128_wide", |b| {
        b.iter(|| cast.evaluate(&wide).unwrap());
    });
    group.bench_function("decimal128_scientific", |b| {
        b.iter(|| cast.evaluate(&scientific).unwrap());
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
