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

use arrow::array::{ArrayRef, Float64Array};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_pow;
use std::hint::black_box;
use std::sync::Arc;

/// Build a Float64 column of `rows` rows, with every `null_every`-th row null
/// (`null_every == 0` means no nulls). Values stay in [0.5, 5.0] so `powf` is finite.
fn create_f64_array(rows: usize, null_every: usize) -> ArrayRef {
    let arr: Float64Array = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                Some(0.5 + ((i % 10) as f64) * 0.5)
            }
        })
        .collect();
    Arc::new(arr)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let no_nulls_a = create_f64_array(rows, 0);
    let no_nulls_b = create_f64_array(rows, 0);
    let sparse_a = create_f64_array(rows, 10);
    let sparse_b = create_f64_array(rows, 10);
    let dense_a = create_f64_array(rows, 2);
    let dense_b = create_f64_array(rows, 2);

    // Array/array: exercises `binary` over spark_powf.
    let mut bench_arr_arr = |name: &str, a: &ArrayRef, b: &ArrayRef| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(a)),
            ColumnarValue::Array(Arc::clone(b)),
        ];
        c.bench_function(name, move |bencher| {
            bencher.iter(|| black_box(spark_pow(black_box(&args)).unwrap()))
        });
    };
    bench_arr_arr("spark_pow: array/array no nulls", &no_nulls_a, &no_nulls_b);
    bench_arr_arr("spark_pow: array/array sparse nulls", &sparse_a, &sparse_b);
    bench_arr_arr("spark_pow: array/array dense nulls", &dense_a, &dense_b);

    // Scalar/array: exercises `unary` with the base captured.
    let mut bench_scalar_arr = |name: &str, exp: &ArrayRef| {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(2.5))),
            ColumnarValue::Array(Arc::clone(exp)),
        ];
        c.bench_function(name, move |bencher| {
            bencher.iter(|| black_box(spark_pow(black_box(&args)).unwrap()))
        });
    };
    bench_scalar_arr("spark_pow: scalar/array no nulls", &no_nulls_b);
    bench_scalar_arr("spark_pow: scalar/array sparse nulls", &sparse_b);
    bench_scalar_arr("spark_pow: scalar/array dense nulls", &dense_b);

    // Array/scalar: exercises `unary` with the exponent captured.
    let mut bench_arr_scalar = |name: &str, base: &ArrayRef| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(base)),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.0))),
        ];
        c.bench_function(name, move |bencher| {
            bencher.iter(|| black_box(spark_pow(black_box(&args)).unwrap()))
        });
    };
    bench_arr_scalar("spark_pow: array/scalar no nulls", &no_nulls_a);
    bench_arr_scalar("spark_pow: array/scalar sparse nulls", &sparse_a);
    bench_arr_scalar("spark_pow: array/scalar dense nulls", &dense_a);

    // Null-scalar short-circuit: whole output is null, no work per row.
    let null_scalar_args = vec![
        ColumnarValue::Scalar(ScalarValue::Float64(None)),
        ColumnarValue::Array(Arc::clone(&no_nulls_b)),
    ];
    c.bench_function("spark_pow: null scalar short-circuit", |b| {
        b.iter(|| black_box(spark_pow(black_box(&null_scalar_args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
