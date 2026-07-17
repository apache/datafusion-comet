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

use arrow::array::{Array, Decimal128Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::DecimalRescaleCheckOverflow;
use std::hint::black_box;
use std::sync::Arc;

const IN_PRECISION: u8 = 38;
const IN_SCALE: i8 = 2;
// A value that overflows the narrow output precision used in the overflow shapes.
const OVERFLOW_VALUE: i128 = 1_000_000_000_000;

/// Build a Decimal128(38, 2) column of `size` rows.
/// - `null_every`: null every Nth row (0 = none).
/// - `overflow_every`: make every Nth value large enough to overflow the narrow output (0 = none).
fn build(size: usize, null_every: usize, overflow_every: usize) -> RecordBatch {
    let arr: Decimal128Array = (0..size)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else if overflow_every != 0 && i % overflow_every == 0 {
                Some(OVERFLOW_VALUE)
            } else {
                Some((i as i128 % 100_000) * 100)
            }
        })
        .collect::<Decimal128Array>()
        .with_precision_and_scale(IN_PRECISION, IN_SCALE)
        .unwrap();
    let schema = Schema::new(vec![Field::new("col", arr.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let col = || Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>;

    // Widen scale 2 -> 6 into a roomy precision: the common decimal-cast case, no overflow.
    let scale_up = DecimalRescaleCheckOverflow::new(col(), IN_SCALE, 38, 6, false);
    // Narrow scale 2 -> 0 (HALF_UP rounding), roomy precision, no overflow.
    let scale_down = DecimalRescaleCheckOverflow::new(col(), IN_SCALE, 38, 0, false);
    // Scale up into a narrow precision so large values overflow.
    let narrow = DecimalRescaleCheckOverflow::new(col(), IN_SCALE, 12, 6, false);
    // ANSI widening, no overflow.
    let ansi = DecimalRescaleCheckOverflow::new(col(), IN_SCALE, 38, 6, true);

    let no_overflow = build(size, 0, 0);
    let no_overflow_nulls = build(size, 17, 0);
    let sparse_overflow = build(size, 0, 17);
    let dense_overflow = build(size, 0, 2);

    c.bench_function("decimal_rescale: scale up, no overflow", |b| {
        b.iter(|| black_box(scale_up.evaluate(black_box(&no_overflow)).unwrap()))
    });
    c.bench_function("decimal_rescale: scale up, no overflow, nulls", |b| {
        b.iter(|| black_box(scale_up.evaluate(black_box(&no_overflow_nulls)).unwrap()))
    });
    c.bench_function("decimal_rescale: scale down, no overflow", |b| {
        b.iter(|| black_box(scale_down.evaluate(black_box(&no_overflow)).unwrap()))
    });
    c.bench_function("decimal_rescale: sparse overflow", |b| {
        b.iter(|| black_box(narrow.evaluate(black_box(&sparse_overflow)).unwrap()))
    });
    c.bench_function("decimal_rescale: dense overflow", |b| {
        b.iter(|| black_box(narrow.evaluate(black_box(&dense_overflow)).unwrap()))
    });
    c.bench_function("decimal_rescale: ansi scale up, no overflow", |b| {
        b.iter(|| black_box(ansi.evaluate(black_box(&no_overflow)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
