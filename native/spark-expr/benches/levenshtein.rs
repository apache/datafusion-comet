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

use arrow::array::{ArrayRef, Int32Array, StringArray};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_levenshtein;
use std::hint::black_box;
use std::sync::Arc;

/// ASCII dataset: exercises the `is_ascii()` fast path (byte-level DP).
fn create_ascii_arrays(rows: usize) -> (ArrayRef, ArrayRef) {
    let left_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_datafusion_comet_{}", i % 100))
        .collect();
    let right_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_comet_expr_{}", (i + 5) % 100))
        .collect();

    (
        Arc::new(StringArray::from(
            left_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            right_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )) as ArrayRef,
    )
}

/// Non-ASCII dataset: exercises the Unicode `chars()` fallback and pays the
/// full-string `is_ascii()` scan before falling through. Both sides are
/// non-ASCII so neither short-circuits into the byte path.
fn create_non_ascii_arrays(rows: usize) -> (ArrayRef, ArrayRef) {
    let left_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_данные_融合_{}", i % 100))
        .collect();
    let right_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_café_données_{}", (i + 5) % 100))
        .collect();

    (
        Arc::new(StringArray::from(
            left_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            right_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )) as ArrayRef,
    )
}

/// Mixed dataset: one side ASCII, the other non-ASCII. `s.is_ascii() && t.is_ascii()`
/// short-circuits on the second operand, so the fast path is skipped after only
/// scanning `s`. This is the cheapest way for a mixed column to miss the byte path.
fn create_mixed_arrays(rows: usize) -> (ArrayRef, ArrayRef) {
    let left_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_datafusion_comet_{}", i % 100))
        .collect();
    let right_strings: Vec<String> = (0..rows)
        .map(|i| format!("apache_café_données_{}", (i + 5) % 100))
        .collect();

    (
        Arc::new(StringArray::from(
            left_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            right_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )) as ArrayRef,
    )
}

/// Shared harness: times the 2-argument and 3-argument (threshold) forms for
/// one dataset, so every dataset is measured identically.
fn bench_pair(c: &mut Criterion, label: &str, left: ArrayRef, right: ArrayRef, rows: usize) {
    c.bench_function(&format!("spark_levenshtein: {label} (no threshold)"), |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&left)),
            ColumnarValue::Array(Arc::clone(&right)),
        ];
        b.iter(|| black_box(spark_levenshtein(black_box(&args)).unwrap()))
    });

    let threshold = Int32Array::from(vec![10; rows]);
    c.bench_function(&format!("spark_levenshtein: {label} (with threshold)"), |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&left)),
            ColumnarValue::Array(Arc::clone(&right)),
            ColumnarValue::Array(Arc::new(threshold.clone()) as ArrayRef),
        ];
        b.iter(|| black_box(spark_levenshtein(black_box(&args)).unwrap()))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;

    let (ascii_left, ascii_right) = create_ascii_arrays(rows);
    bench_pair(c, "ascii", ascii_left, ascii_right, rows);

    let (non_ascii_left, non_ascii_right) = create_non_ascii_arrays(rows);
    bench_pair(c, "non-ascii", non_ascii_left, non_ascii_right, rows);

    let (mixed_left, mixed_right) = create_mixed_arrays(rows);
    bench_pair(c, "mixed", mixed_left, mixed_right, rows);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
