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
use datafusion_comet_spark_expr::spark_regexp_extract;
use std::hint::black_box;
use std::sync::Arc;

fn create_string_array(size: usize) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..size {
        if i % 10 == 0 {
            builder.append_null();
        } else {
            builder.append_value(format!("{:03}-{:03}", i % 1000, (i * 7) % 1000));
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let string_array = create_string_array(size);

    // Extract the first capture group of a two-group pattern (the common case).
    c.bench_function("spark_regexp_extract: group 1", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r"(\d+)-(\d+)".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ];
        b.iter(|| black_box(spark_regexp_extract(black_box(&args)).unwrap()))
    });

    // Extract the whole match (group 0).
    c.bench_function("spark_regexp_extract: group 0", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r"(\d+)-(\d+)".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
        ];
        b.iter(|| black_box(spark_regexp_extract(black_box(&args)).unwrap()))
    });

    // Extract the second capture group.
    c.bench_function("spark_regexp_extract: group 2", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&string_array)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r"(\d+)-(\d+)".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        b.iter(|| black_box(spark_regexp_extract(black_box(&args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
