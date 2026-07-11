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
use datafusion_comet_spark_expr::spark_log;
use std::hint::black_box;
use std::sync::Arc;

/// Build a `Float64Array` of `n` positive values with every 10th row null.
fn positive_f64(n: usize) -> ArrayRef {
    let vals: Vec<Option<f64>> = (0..n)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                Some(1.0 + (i as f64) * 0.5)
            }
        })
        .collect();
    Arc::new(Float64Array::from(vals))
}

fn criterion_benchmark(c: &mut Criterion) {
    let n = 8192;
    let column = positive_f64(n);

    // log(base_literal, value_column) — the common Spark shape where the base
    // is a constant, exercising the scalar-base / array-value path.
    c.bench_function("spark_log: scalar base, array value", |b| {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(10.0))),
            ColumnarValue::Array(Arc::clone(&column)),
        ];
        b.iter(|| black_box(spark_log(black_box(&args)).unwrap()))
    });

    // log(base_column, value_literal) — the array-base / scalar-value path.
    c.bench_function("spark_log: array base, scalar value", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&column)),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(100.0))),
        ];
        b.iter(|| black_box(spark_log(black_box(&args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
