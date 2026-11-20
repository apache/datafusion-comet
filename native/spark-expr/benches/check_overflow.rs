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
use arrow::buffer::NullBuffer;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion_comet_spark_expr::CheckOverflow;
use std::hint::black_box;
use std::sync::Arc;

// Input arithmetic result is a wide Decimal128(38, 2); CheckOverflow narrows it to the
// declared Decimal128(10, 2), so any value with |v| >= 10^10 overflows.
const IN_PRECISION: u8 = 38;
const TARGET_PRECISION: u8 = 10;
const SCALE: i8 = 2;
const OVERFLOW_VALUE: i128 = 1_000_000_000_000; // 10^12, does not fit precision 10

/// Build a Decimal128(38, 2) column of `size` rows.
/// - `null_every`: null every Nth row (0 = no nulls).
/// - `overflow_every`: make every Nth value overflow the target precision (0 = none).
fn build(size: usize, null_every: usize, overflow_every: usize) -> RecordBatch {
    let values: Decimal128Array = (0..size)
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
        .with_precision_and_scale(IN_PRECISION, SCALE)
        .unwrap();

    let schema = Schema::new(vec![Field::new("d", values.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values)]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let target = DataType::Decimal128(TARGET_PRECISION, SCALE);
    let col = || Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>;

    // Non-ANSI (fail_on_error = false): overflowing values become null.
    let legacy = CheckOverflow::new(col(), target.clone(), false, None, None);
    // ANSI (fail_on_error = true): overflow raises; only benched on non-overflowing data.
    let ansi = CheckOverflow::new(col(), target.clone(), true, None, None);

    // The common TPC-DS shape: decimal arithmetic result that fits the declared precision.
    let no_overflow = build(size, 0, 0);
    let no_overflow_nulls = build(size, 17, 0);
    let no_overflow_dense_nulls = build(size, 2, 0);
    let sparse_overflow = build(size, 0, 17);
    let dense_overflow = build(size, 0, 2);

    c.bench_function("check_overflow: no overflow", |b| {
        b.iter(|| black_box(legacy.evaluate(black_box(&no_overflow)).unwrap()))
    });
    c.bench_function("check_overflow: no overflow, with nulls", |b| {
        b.iter(|| black_box(legacy.evaluate(black_box(&no_overflow_nulls)).unwrap()))
    });
    c.bench_function("check_overflow: no overflow, dense nulls", |b| {
        b.iter(|| {
            black_box(
                legacy
                    .evaluate(black_box(&no_overflow_dense_nulls))
                    .unwrap(),
            )
        })
    });
    c.bench_function("check_overflow: sparse overflow", |b| {
        b.iter(|| black_box(legacy.evaluate(black_box(&sparse_overflow)).unwrap()))
    });
    c.bench_function("check_overflow: dense overflow", |b| {
        b.iter(|| black_box(legacy.evaluate(black_box(&dense_overflow)).unwrap()))
    });
    c.bench_function("check_overflow: ansi no overflow", |b| {
        b.iter(|| black_box(ansi.evaluate(black_box(&no_overflow)).unwrap()))
    });
    c.bench_function("check_overflow: ansi no overflow, with nulls", |b| {
        b.iter(|| black_box(ansi.evaluate(black_box(&no_overflow_nulls)).unwrap()))
    });

    // One overflow in the last row, so the scan reads the whole batch before the slow path runs.
    // `sparse overflow` and `dense overflow` overflow at row 0, where the scan stops.
    let head = build(size - 1, 0, 0);
    let last_row_overflow = concat_batches(&head.schema(), [&head, &build(1, 0, 1)]).unwrap();
    c.bench_function("check_overflow: last row overflow", |b| {
        b.iter(|| black_box(legacy.evaluate(black_box(&last_row_overflow)).unwrap()))
    });

    // The `with nulls` shape, except that its last row is a null slot holding a value that
    // doesn't fit. A scan over every slot reaches it at the end of the batch and has to find
    // that the slot is null, though nothing overflows.
    let null_slot_overflow = {
        let nulls = build(size, 17, 0);
        let nulls = nulls
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let mut values = nulls.values().to_vec();
        values[size - 1] = OVERFLOW_VALUE;
        let mut validity: Vec<bool> = (0..size).map(|i| nulls.is_valid(i)).collect();
        validity[size - 1] = false;
        let array = Decimal128Array::new(values.into(), Some(NullBuffer::from(validity)))
            .with_precision_and_scale(IN_PRECISION, SCALE)
            .unwrap();
        let schema = Schema::new(vec![Field::new("d", array.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
    };
    c.bench_function("check_overflow: no overflow, null slot out of range", |b| {
        b.iter(|| black_box(legacy.evaluate(black_box(&null_slot_overflow)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
