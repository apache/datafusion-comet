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

use arrow::array::builder::StringBuilder;
use arrow::array::ArrayRef;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_lpad, spark_rpad};
use std::hint::black_box;
use std::sync::Arc;

fn create_string_array(size: usize) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..size {
        if i % 10 == 0 {
            builder.append_null();
        } else {
            builder.append_value(format!("string{}", i % 100));
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let string_array = create_string_array(size);

    // lpad with default padding (space)
    c.bench_function("spark_lpad: default padding", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
        ];
        b.iter(|| black_box(spark_lpad(black_box(&args)).unwrap()))
    });

    // lpad with custom padding character
    c.bench_function("spark_lpad: custom padding", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("*".to_string()))),
        ];
        b.iter(|| black_box(spark_lpad(black_box(&args)).unwrap()))
    });

    // rpad with default padding (space)
    c.bench_function("spark_rpad: default padding", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
        ];
        b.iter(|| black_box(spark_rpad(black_box(&args)).unwrap()))
    });

    // rpad with custom padding character
    c.bench_function("spark_rpad: custom padding", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("*".to_string()))),
        ];
        b.iter(|| black_box(spark_rpad(black_box(&args)).unwrap()))
    });

    // lpad with multi-character padding string
    c.bench_function("spark_lpad: multi-char padding", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
        ];
        b.iter(|| black_box(spark_lpad(black_box(&args)).unwrap()))
    });

    // rpad with multi-character padding string
    c.bench_function("spark_rpad: multi-char padding", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
        ];
        b.iter(|| black_box(spark_rpad(black_box(&args)).unwrap()))
    });

    // lpad with truncation (target length shorter than some strings)
    c.bench_function("spark_lpad: with truncation", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
        ];
        b.iter(|| black_box(spark_lpad(black_box(&args)).unwrap()))
    });

    // rpad with truncation (target length shorter than some strings)
    c.bench_function("spark_rpad: with truncation", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
        ];
        b.iter(|| black_box(spark_rpad(black_box(&args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
