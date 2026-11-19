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

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{checked_add, checked_div, checked_mul, checked_sub, EvalMode};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

/// Deterministic pseudo-random generator so the benchmark inputs are stable across runs.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn i32_array(seed: u64, nulls: bool) -> ArrayRef {
    let mut state = seed;
    let values: Vec<Option<i32>> = (0..NUM_ROWS)
        .map(|_| {
            let v = (next(&mut state) % 100_000) as i32 - 50_000;
            if nulls && next(&mut state).is_multiple_of(10) {
                None
            } else {
                Some(v)
            }
        })
        .collect();
    Arc::new(Int32Array::from(values))
}

fn i64_array(seed: u64, nulls: bool) -> ArrayRef {
    let mut state = seed;
    let values: Vec<Option<i64>> = (0..NUM_ROWS)
        .map(|_| {
            let v = (next(&mut state) % 1_000_000) as i64 - 500_000;
            if nulls && next(&mut state).is_multiple_of(10) {
                None
            } else {
                Some(v)
            }
        })
        .collect();
    Arc::new(Int64Array::from(values))
}

fn f64_array(seed: u64, nulls: bool) -> ArrayRef {
    let mut state = seed;
    let values: Vec<Option<f64>> = (0..NUM_ROWS)
        .map(|_| {
            let v = (next(&mut state) % 1_000_000) as f64 / 7.0 + 1.0;
            if nulls && next(&mut state).is_multiple_of(10) {
                None
            } else {
                Some(v)
            }
        })
        .collect();
    Arc::new(Float64Array::from(values))
}

type ToArray = fn(Vec<i64>, Option<NullBuffer>) -> ArrayRef;

#[derive(Clone, Copy)]
enum Overflow {
    None,
    OnePerBatch,
    /// About 1% of rows.
    Random,
    /// Under every null slot of the left operand, which has 10% nulls.
    UnderNulls,
}

/// Values in [-1000, 1000], with `max` at the rows that overflow, so that both `+` and `*`
/// overflow there. Returns the left and right operands.
fn try_operands(
    overflow: Overflow,
    nulls: bool,
    max: i64,
    to_array: ToArray,
) -> (ArrayRef, ArrayRef) {
    let mut state = 0x1234_5678;
    let mut side = |left: bool| {
        let mut values = Vec::with_capacity(NUM_ROWS);
        let mut valid = Vec::with_capacity(NUM_ROWS);
        for row in 0..NUM_ROWS {
            let null = nulls && left && next(&mut state).is_multiple_of(10);
            let overflows = match overflow {
                Overflow::None => false,
                Overflow::OnePerBatch => row == NUM_ROWS / 2,
                Overflow::Random => next(&mut state).is_multiple_of(100),
                Overflow::UnderNulls => null,
            };
            values.push(if overflows {
                max
            } else {
                (next(&mut state) % 2001) as i64 - 1000
            });
            valid.push(!null);
        }
        to_array(values, nulls.then(|| NullBuffer::from(valid)))
    };
    let left = side(true);
    (left, side(false))
}

fn try_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("checked_arithmetic_try");
    let types: [(&str, DataType, i64, ToArray); 2] = [
        ("i32", DataType::Int32, i32::MAX as i64, |v, n| {
            Arc::new(Int32Array::new(
                v.into_iter().map(|x| x as i32).collect(),
                n,
            ))
        }),
        ("i64", DataType::Int64, i64::MAX, |v, n| {
            Arc::new(Int64Array::new(v.into(), n))
        }),
    ];
    for (ty, data_type, max, to_array) in types {
        for (shape, overflow, nulls) in [
            ("no_overflow_no_nulls", Overflow::None, false),
            ("no_overflow_with_nulls", Overflow::None, true),
            ("one_overflow", Overflow::OnePerBatch, true),
            ("random_overflow", Overflow::Random, true),
            ("overflow_under_nulls", Overflow::UnderNulls, true),
        ] {
            let (l, r) = try_operands(overflow, nulls, max, to_array);
            let args = [ColumnarValue::Array(l), ColumnarValue::Array(r)];
            for (op, f) in [
                ("add", checked_add as CheckedFn),
                ("mul", checked_mul as CheckedFn),
            ] {
                group.bench_function(format!("try_{op}_{ty}_{shape}"), |b| {
                    b.iter(|| black_box(f(black_box(&args), &data_type, EvalMode::Try)))
                });
            }
        }

        let (l, r) = try_operands(Overflow::None, true, max, to_array);
        let scalar = ScalarValue::Int64(Some(42)).cast_to(&data_type).unwrap();
        let args = [
            ColumnarValue::Array(Arc::clone(&l)),
            ColumnarValue::Scalar(scalar),
        ];
        group.bench_function(format!("try_add_{ty}_scalar_right_with_nulls"), |b| {
            b.iter(|| black_box(checked_add(black_box(&args), &data_type, EvalMode::Try)))
        });

        // try_add(try_add(a, b), c): the second call reads what the first left in null slots.
        let args = [
            ColumnarValue::Array(l),
            ColumnarValue::Array(Arc::clone(&r)),
        ];
        group.bench_function(format!("try_add_{ty}_chained_with_nulls"), |b| {
            b.iter(|| {
                let sum = checked_add(black_box(&args), &data_type, EvalMode::Try).unwrap();
                let args = [sum, ColumnarValue::Array(Arc::clone(&r))];
                black_box(checked_add(&args, &data_type, EvalMode::Try))
            })
        });
    }
    group.finish();
}

type CheckedFn = fn(
    &[ColumnarValue],
    &DataType,
    EvalMode,
) -> Result<ColumnarValue, datafusion::common::DataFusionError>;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("checked_arithmetic");

    for (label, nulls) in [("no_nulls", false), ("with_nulls", true)] {
        let i32_args = [
            ColumnarValue::Array(i32_array(0x1234_5678, nulls)),
            ColumnarValue::Array(i32_array(0x9e37_79b9, nulls)),
        ];
        let i64_args = [
            ColumnarValue::Array(i64_array(0x1234_5678, nulls)),
            ColumnarValue::Array(i64_array(0x9e37_79b9, nulls)),
        ];
        let f64_args = [
            ColumnarValue::Array(f64_array(0x1234_5678, nulls)),
            ColumnarValue::Array(f64_array(0x9e37_79b9, nulls)),
        ];

        let i32_scalar_right_args = [
            ColumnarValue::Array(i32_array(0x1234_5678, nulls)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(42))),
        ];

        let i32_scalar_left_args = [
            ColumnarValue::Scalar(ScalarValue::Int32(Some(42))),
            ColumnarValue::Array(i32_array(0x1234_5678, nulls)),
        ];

        let i64_scalar_args = [
            ColumnarValue::Array(i64_array(0x1234_5678, nulls)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
        ];

        group.bench_function(format!("checked_add_i32_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_add(
                    black_box(&i32_args),
                    &DataType::Int32,
                    EvalMode::Ansi,
                ))
            })
        });

        group.bench_function(format!("checked_add_i32_try_{label}"), |b| {
            b.iter(|| {
                black_box(checked_add(
                    black_box(&i32_args),
                    &DataType::Int32,
                    EvalMode::Try,
                ))
            })
        });

        group.bench_function(format!("checked_sub_i64_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_sub(
                    black_box(&i64_args),
                    &DataType::Int64,
                    EvalMode::Ansi,
                ))
            })
        });

        group.bench_function(format!("checked_mul_i64_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_mul(
                    black_box(&i64_args),
                    &DataType::Int64,
                    EvalMode::Ansi,
                ))
            })
        });

        group.bench_function(format!("checked_div_f64_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_div(
                    black_box(&f64_args),
                    &DataType::Float64,
                    EvalMode::Ansi,
                ))
            })
        });

        group.bench_function(format!("checked_add_i32_scalar_right_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_add(
                    black_box(&i32_scalar_right_args),
                    &DataType::Int32,
                    EvalMode::Ansi,
                ))
            })
        });

        group.bench_function(format!("checked_add_i32_scalar_left_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_add(
                    black_box(&i32_scalar_left_args),
                    &DataType::Int32,
                    EvalMode::Ansi,
                ))
            })
        });

        group.bench_function(format!("checked_mul_i64_scalar_ansi_{label}"), |b| {
            b.iter(|| {
                black_box(checked_mul(
                    black_box(&i64_scalar_args),
                    &DataType::Int64,
                    EvalMode::Ansi,
                ))
            })
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark, try_benchmarks);
criterion_main!(benches);
