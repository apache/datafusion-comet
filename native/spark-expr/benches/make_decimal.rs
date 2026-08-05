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

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_make_decimal;
use std::hint::black_box;
use std::sync::Arc;

// Target Decimal128(18, 2): the widest precision DecimalAggregates produces for MakeDecimal.
// At precision 18 every i64 fits, so every value in these benches is in the no-overflow
// common path.
const NO_OVERFLOW_PRECISION: u8 = 18;
const NO_OVERFLOW_SCALE: i8 = 2;

// Narrow target used only for the overflow shapes. Values with |v| >= 1000 do not fit.
const OVERFLOW_PRECISION: u8 = 3;
const OVERFLOW_SCALE: i8 = 0;
const OVERFLOW_VALUE: i64 = 1000;

/// Build an Int64 column of `rows` rows, with every `null_every`-th row null
/// (`null_every == 0` means no nulls). Values always fit Decimal128(18, 2).
fn create_int64_array(rows: usize, null_every: usize) -> ArrayRef {
    let arr: Int64Array = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                Some((i as i64 % 100_000) * 100)
            }
        })
        .collect();
    Arc::new(arr)
}

/// Build an Int64 column where every row overflows Decimal128(3, 0).
fn create_all_overflow_array(rows: usize) -> ArrayRef {
    let arr: Int64Array = (0..rows).map(|_| Some(OVERFLOW_VALUE)).collect();
    Arc::new(arr)
}

/// Build an Int64 column where only the last row overflows Decimal128(3, 0).
fn create_last_row_overflow_array(rows: usize) -> ArrayRef {
    let arr: Int64Array = (0..rows)
        .map(|i| {
            if i + 1 == rows {
                Some(OVERFLOW_VALUE)
            } else {
                Some((i as i64 % 999) + 1)
            }
        })
        .collect();
    Arc::new(arr)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let no_overflow_target = DataType::Decimal128(NO_OVERFLOW_PRECISION, NO_OVERFLOW_SCALE);
    let overflow_target = DataType::Decimal128(OVERFLOW_PRECISION, OVERFLOW_SCALE);

    let no_nulls = create_int64_array(rows, 0);
    let sparse_nulls = create_int64_array(rows, 10);
    let dense_nulls = create_int64_array(rows, 2);
    let all_overflow = create_all_overflow_array(rows);
    let last_overflow = create_last_row_overflow_array(rows);

    // Success / non-ANSI paths: unwrap like the neighbouring expression benches.
    // Two-arg helpers match `unscaled_value.rs` (name + array; target/`fail_on_error` captured).
    {
        let target = no_overflow_target.clone();
        let mut bench = |name: &str, arr: &ArrayRef| {
            let args = vec![ColumnarValue::Array(Arc::clone(arr))];
            let target = target.clone();
            c.bench_function(name, move |b| {
                b.iter(|| {
                    black_box(
                        spark_make_decimal(black_box(&args), black_box(&target), false).unwrap(),
                    )
                })
            });
        };
        bench("spark_make_decimal: no nulls", &no_nulls);
        bench("spark_make_decimal: sparse nulls", &sparse_nulls);
        bench("spark_make_decimal: dense nulls", &dense_nulls);
    }
    {
        let target = no_overflow_target.clone();
        let mut bench = |name: &str, arr: &ArrayRef| {
            let args = vec![ColumnarValue::Array(Arc::clone(arr))];
            let target = target.clone();
            c.bench_function(name, move |b| {
                b.iter(|| {
                    black_box(
                        spark_make_decimal(black_box(&args), black_box(&target), true).unwrap(),
                    )
                })
            });
        };
        bench("spark_make_decimal: ansi no nulls", &no_nulls);
        bench("spark_make_decimal: ansi sparse nulls", &sparse_nulls);
        bench("spark_make_decimal: ansi dense nulls", &dense_nulls);
    }
    {
        // Non-ANSI overflow: short-circuiting scan then `null_if_overflow_precision`.
        let target = overflow_target.clone();
        let mut bench = |name: &str, arr: &ArrayRef| {
            let args = vec![ColumnarValue::Array(Arc::clone(arr))];
            let target = target.clone();
            c.bench_function(name, move |b| {
                b.iter(|| {
                    black_box(
                        spark_make_decimal(black_box(&args), black_box(&target), false).unwrap(),
                    )
                })
            });
        };
        bench("spark_make_decimal: overflow all rows", &all_overflow);
        bench("spark_make_decimal: overflow last row", &last_overflow);
    }

    // ANSI overflow raises; black-box the Result (same idea as check_overflow only
    // unwrapping non-error paths, but here the error path itself is the shape under test).
    let ansi_last_args = vec![ColumnarValue::Array(Arc::clone(&last_overflow))];
    let ansi_last_target = overflow_target.clone();
    c.bench_function("spark_make_decimal: ansi overflow last row", |b| {
        b.iter(|| {
            black_box(spark_make_decimal(
                black_box(&ansi_last_args),
                black_box(&ansi_last_target),
                true,
            ))
        })
    });

    let ansi_all_args = vec![ColumnarValue::Array(Arc::clone(&all_overflow))];
    c.bench_function("spark_make_decimal: ansi overflow all rows", |b| {
        b.iter(|| {
            black_box(spark_make_decimal(
                black_box(&ansi_all_args),
                black_box(&overflow_target),
                true,
            ))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
