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

use arrow::array::StringArray;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion_comet_spark_expr::spark_regexp_extract_all;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 4096;

fn subjects() -> ColumnarValue {
    let values: Vec<Option<String>> = (0..NUM_ROWS)
        .map(|i| match i % 8 {
            0 => None,
            1 => Some("no digits at all in this row".to_string()),
            2 => Some(format!("{}-{}", i, i + 1)),
            _ => Some(format!(
                "{}-{}, {}-{} and {}-{}",
                i,
                i + 1,
                i * 3,
                i * 3 + 7,
                i * 11,
                i * 11 + 13
            )),
        })
        .collect();
    ColumnarValue::Array(Arc::new(StringArray::from(values)))
}

fn args(subjects: &ColumnarValue, pattern: &str, idx: i32) -> Vec<ColumnarValue> {
    vec![
        subjects.clone(),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.to_string()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(idx))),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let subjects = subjects();
    let mut group = c.benchmark_group("regexp_extract_all");

    let capture_group = args(&subjects, r"(\d+)-(\d+)", 1);
    group.bench_function("capture_group", |b| {
        b.iter(|| black_box(spark_regexp_extract_all(black_box(&capture_group)).unwrap()))
    });

    let whole_match = args(&subjects, r"(\d+)-(\d+)", 0);
    group.bench_function("whole_match", |b| {
        b.iter(|| black_box(spark_regexp_extract_all(black_box(&whole_match)).unwrap()))
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
