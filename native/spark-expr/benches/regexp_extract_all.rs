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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_regexp_extract_all, PatternCache};
use std::hint::black_box;

#[path = "common/mod.rs"]
mod common;
use common::{string_array, NULL_RATIOS, ROW_COUNTS};

const INPUT: &str =
    "datafusion has datafusion-python, datafusion-comet, datafusion-java as sub projects";
const PATTERN: &str = r"(\w+)-(\w+)";

/// Short rows with several matches each, so the per-match cost dominates.
const DIGITS_INPUT: &str = "123-456-789-123";
const DIGITS_PATTERN: &str = r"(\d+)";
const DIGITS_ROWS: usize = 8_192;

/// 8 KB rows made of short runs of `a` separated by a single `b`, so a single row carries
/// thousands of matches and the cost of walking a long haystack dominates.
const LONG_ROW_BYTES: usize = 8_192;
const LONG_PATTERN: &str = r"(a+)";
const LONG_ROWS: usize = 512;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_regexp_extract_all");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let args = vec![
                ColumnarValue::Array(string_array(rows, null_ratio, |_| INPUT.to_string())),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(PATTERN.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| {
                    // One cache per benchmark input mirrors one cache per planned expression.
                    let cache = PatternCache::new();
                    b.iter(|| black_box(spark_regexp_extract_all(black_box(args), &cache).unwrap()))
                },
            );
        }
    }

    let digits_args = vec![
        ColumnarValue::Array(string_array(DIGITS_ROWS, 0.0, |_| DIGITS_INPUT.to_string())),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(DIGITS_PATTERN.to_string()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
    ];
    group.bench_with_input(
        BenchmarkId::from_parameter(format!("digits/{DIGITS_ROWS}")),
        &digits_args,
        |b, args| {
            // One cache per benchmark input mirrors one cache per planned expression.
            let cache = PatternCache::new();
            b.iter(|| black_box(spark_regexp_extract_all(black_box(args), &cache).unwrap()))
        },
    );

    let long_row = "aaab".repeat(LONG_ROW_BYTES / 4);
    let long_args = vec![
        ColumnarValue::Array(string_array(LONG_ROWS, 0.0, |_| long_row.clone())),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(LONG_PATTERN.to_string()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
    ];
    group.bench_with_input(
        BenchmarkId::from_parameter(format!("long_8kb/{LONG_ROWS}")),
        &long_args,
        |b, args| {
            // One cache per benchmark input mirrors one cache per planned expression.
            let cache = PatternCache::new();
            b.iter(|| black_box(spark_regexp_extract_all(black_box(args), &cache).unwrap()))
        },
    );
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
