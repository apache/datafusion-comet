// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{ArrayRef, Int32Array, StringArray};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_levenshtein;
use std::hint::black_box;
use std::sync::Arc;

fn create_string_arrays(rows: usize) -> (ArrayRef, ArrayRef) {
    let left_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_datafusion_comet_{}", i % 100))
        .collect();
    let right_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_comet_expr_{}", (i + 5) % 100))
        .collect();

    let left_array = StringArray::from(
        left_strings
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>(),
    );
    let right_array = StringArray::from(
        right_strings
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>(),
    );

    (
        Arc::new(left_array) as ArrayRef,
        Arc::new(right_array) as ArrayRef,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let (left, right) = create_string_arrays(rows);

    c.bench_function("spark_levenshtein: 2 arguments (no threshold)", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&left)),
            ColumnarValue::Array(Arc::clone(&right)),
        ];
        b.iter(|| black_box(spark_levenshtein(black_box(&args)).unwrap()))
    });

    let threshold = Int32Array::from(vec![10; rows]);
    c.bench_function("spark_levenshtein: 3 arguments (with threshold)", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&left)),
            ColumnarValue::Array(Arc::clone(&right)),
            ColumnarValue::Array(Arc::new(threshold.clone()) as ArrayRef),
        ];
        b.iter(|| black_box(spark_levenshtein(black_box(&args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
