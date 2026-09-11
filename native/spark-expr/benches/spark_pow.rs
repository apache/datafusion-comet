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

use arrow::array::{ArrayRef, Datum, Float64Array, Scalar};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::numeric::add;
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

/// Build a Float64 column of `rows` rows with approximately `null_pct`% nulls striped
/// across the batch (`i % 100 < null_pct` marks a null). Payload in null slots is
/// the default 0. Meant as input for a downstream Arrow op (like `add`) whose
/// output is what feeds `spark_pow`.
fn create_f64_array_with_null_pct(rows: usize, null_pct: usize) -> ArrayRef {
    let arr: Float64Array = (0..rows)
        .map(|i| {
            if null_pct != 0 && i % 100 < null_pct {
                None
            } else {
                Some(0.5 + ((i % 10) as f64) * 0.5)
            }
        })
        .collect();
    Arc::new(arr)
}

/// Build a Float64 column whose null slots still carry a real (non-zero) payload,
/// simulating the output of `a + 2.5D` where the addition preserves null bits but
/// writes a real value into every slot. `null_pct` is the target fraction of null rows
/// in `0..=100`. Nulls are striped across the batch modulo 100
/// (`i % 100 < null_pct` marks a null).
fn create_f64_array_with_payload_in_nulls(rows: usize, null_pct: usize) -> ArrayRef {
    let values: Vec<f64> = (0..rows).map(|i| 2.5 + ((i % 10) as f64) * 0.1).collect();
    let nulls = if null_pct == 0 {
        None
    } else {
        Some(NullBuffer::from(
            (0..rows)
                .map(|i| i % 100 >= null_pct)
                .collect::<Vec<bool>>(),
        ))
    };
    Arc::new(Float64Array::new(values.into(), nulls))
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

    // Composed-nullable: models `pow(a + 2.5D, b)` where null slots carry a real
    // payload (Arrow arithmetic preserves the null bit but overwrites the value).
    // Sweeps null density to locate the crossover between the raw-buffer kernels
    // (unary/binary) and a null-skipping path.
    for null_pct in [50usize, 70, 80, 90, 99] {
        let a = create_f64_array_with_payload_in_nulls(rows, null_pct);
        let b = create_f64_array_with_payload_in_nulls(rows, null_pct);

        let arr_arr_args = vec![
            ColumnarValue::Array(Arc::clone(&a)),
            ColumnarValue::Array(Arc::clone(&b)),
        ];
        c.bench_function(
            &format!("spark_pow: array/array composed nulls {null_pct}%"),
            move |bencher| bencher.iter(|| black_box(spark_pow(black_box(&arr_arr_args)).unwrap())),
        );

        let a = create_f64_array_with_payload_in_nulls(rows, null_pct);
        let scalar_arr_args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(2.5))),
            ColumnarValue::Array(Arc::clone(&a)),
        ];
        c.bench_function(
            &format!("spark_pow: scalar/array composed nulls {null_pct}%"),
            move |bencher| {
                bencher.iter(|| black_box(spark_pow(black_box(&scalar_arr_args)).unwrap()))
            },
        );

        let a = create_f64_array_with_payload_in_nulls(rows, null_pct);
        let arr_scalar_args = vec![
            ColumnarValue::Array(Arc::clone(&a)),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.0))),
        ];
        c.bench_function(
            &format!("spark_pow: array/scalar composed nulls {null_pct}%"),
            move |bencher| {
                bencher.iter(|| black_box(spark_pow(black_box(&arr_scalar_args)).unwrap()))
            },
        );
    }

    // End-to-end pipeline. The `add` step preserves null bits but overwrites the
    // underlying payload, which is the exact shape the reviewer flagged for a null-skipping
    // kernel. Timing includes both the Arrow `add` and the `spark_pow` call so it reflects
    // the real query cost, not a pre-materialised intermediate. Two shapes:
    //   1. `pow(a + 2.5D, 3)` — array/scalar dispatch (`pow_array_scalar_null_aware`)
    //   2. `pow(a + 2.5D, b)` — array/array dispatch (`pow_binary_null_aware`), with
    //      nullable `a` and a non-null array of finite fractional exponents.
    let exp_arg = ColumnarValue::Scalar(ScalarValue::Float64(Some(3.0)));
    let two_point_five: Arc<dyn Datum> = Arc::new(Scalar::new(Float64Array::from(vec![2.5])));
    let fractional_exponents: ArrayRef = Arc::new(Float64Array::from(
        (0..rows)
            .map(|i| 1.25 + (i % 10) as f64 * 0.1)
            .collect::<Vec<_>>(),
    ));
    for null_pct in [50usize, 70, 80, 90, 99] {
        let base: ArrayRef = create_f64_array_with_null_pct(rows, null_pct);
        let scalar = Arc::clone(&two_point_five);
        let exp = exp_arg.clone();
        c.bench_function(
            &format!("spark_pow: pipeline pow(a + 2.5D, 3) nulls {null_pct}%"),
            move |bencher| {
                bencher.iter(|| {
                    let composed =
                        add(black_box(&base.as_ref()), black_box(scalar.as_ref())).unwrap();
                    let args = [ColumnarValue::Array(composed), exp.clone()];
                    black_box(spark_pow(black_box(&args)).unwrap())
                })
            },
        );

        let base: ArrayRef = create_f64_array_with_null_pct(rows, null_pct);
        let scalar = Arc::clone(&two_point_five);
        let exp = Arc::clone(&fractional_exponents);
        c.bench_function(
            &format!("spark_pow: pipeline pow(a + 2.5D, b) nulls {null_pct}%"),
            move |bencher| {
                bencher.iter(|| {
                    let composed_base =
                        add(black_box(&base.as_ref()), black_box(scalar.as_ref())).unwrap();
                    let args = [
                        ColumnarValue::Array(composed_base),
                        ColumnarValue::Array(Arc::clone(&exp)),
                    ];
                    black_box(spark_pow(black_box(&args)).unwrap())
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
