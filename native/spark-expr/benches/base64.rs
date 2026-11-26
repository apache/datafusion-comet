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

use arrow::array::builder::BinaryBuilder;
use arrow::array::ArrayRef;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_base64;
use std::hint::black_box;
use std::sync::Arc;

/// Build a binary array with a mix of short values and longer values that
/// exercise the CRLF line-wrapping path when chunking is enabled.
fn create_binary_array(size: usize, value_len: usize) -> ArrayRef {
    let mut builder = BinaryBuilder::new();
    for i in 0..size {
        if i % 10 == 0 {
            builder.append_null();
        } else {
            let byte = (i % 251) as u8;
            let value = vec![byte; value_len];
            builder.append_value(&value);
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    // Short values (encode to <= 76 chars, no wrapping).
    let short = create_binary_array(size, 16);
    // Long values (encode to several wrapped lines when chunking).
    let long = create_binary_array(size, 200);

    c.bench_function("spark_base64: short, unchunked", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&short)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
        ];
        b.iter(|| black_box(spark_base64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_base64: short, chunked", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&short)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
        ];
        b.iter(|| black_box(spark_base64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_base64: long, unchunked", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&long)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
        ];
        b.iter(|| black_box(spark_base64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_base64: long, chunked", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&long)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
        ];
        b.iter(|| black_box(spark_base64(black_box(&args)).unwrap()))
    });

    let mut group = c.benchmark_group("base64_output_size");
    for (name, value_len, null_every) in [
        ("large", 4096, 10),
        ("line_boundary", 58, 2),
        ("variable", 512, 10),
    ] {
        let mut builder = BinaryBuilder::new();
        let mut state = 42u64;
        for i in 0..size {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            if i % null_every == 0 {
                builder.append_null();
            } else {
                let len = if name == "variable" {
                    (state >> 32) as usize % value_len
                } else {
                    value_len
                };
                builder.append_value(vec![b'a'; len]);
            }
        }
        let input: ArrayRef = Arc::new(builder.finish());
        for chunk in [false, true] {
            let args = [
                ColumnarValue::Array(Arc::clone(&input)),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(chunk))),
            ];
            group.bench_function(BenchmarkId::new(name, chunk), |b| {
                b.iter(|| black_box(spark_base64(black_box(&args)).unwrap()))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
