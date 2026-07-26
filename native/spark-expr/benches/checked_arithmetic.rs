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
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
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
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
