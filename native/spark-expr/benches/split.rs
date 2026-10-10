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
use datafusion_comet_spark_expr::{spark_split, spark_split_sql};
use std::hint::black_box;

#[path = "common/mod.rs"]
mod common;
use common::{string_array, NULL_RATIOS, ROW_COUNTS};

// The generator takes the row index so the payload varies per row. Repeating a
// single constant across the batch would let the branch predictor and the regex
// engine see input that real batches never look like.

fn csv(i: usize) -> String {
    format!("field1_{i},field2_{i},field3_{i},field4_{i},field5_{i}")
}

fn csv_trailing(i: usize) -> String {
    format!("data_{i},,,,")
}

fn multi_char(i: usize) -> String {
    format!("field1_{i}::field2_{i}::field3_{i}::field4_{i}")
}

fn whitespace(i: usize) -> String {
    format!("word1_{i}   word2_{i} \t  word3_{i}    word4_{i}")
}

/// The three pattern classes `is_regex_literal` routes between. `literal_char`
/// and `literal_multi_char` must stay off the regex DFA; `regex` must stay on it.
/// Together these are what separates the fast-path claim from "we stopped doing
/// work we were never supposed to do".
fn shapes() -> [(&'static str, &'static str, fn(usize) -> String); 3] {
    [
        ("literal_char", ",", csv),
        ("literal_multi_char", "::", multi_char),
        ("regex", r"\s+", whitespace),
    ]
}

fn utf8(s: &str) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_string())))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut split_group = c.benchmark_group("spark_split");

    for rows in ROW_COUNTS {
        for (null_ratio, null_tag) in NULL_RATIOS {
            // Default limit, one row per pattern class.
            for (label, pattern, gen) in shapes() {
                let args = vec![
                    ColumnarValue::Array(string_array(rows, null_ratio, gen)),
                    utf8(pattern),
                ];
                split_group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{label}/default_limit/{rows}/{null_tag}")),
                    &args,
                    |b, args| b.iter(|| black_box(spark_split(black_box(args)).unwrap())),
                );
            }

            // The limit axis, on one shared input so the two rows are a true A/B.
            // `limit == 0` is remapped to -1 before the helpers run, so both rows
            // execute identical code and must land within noise of each other. A
            // gap here means the remap does not cover this input path.
            for limit in [0, -1] {
                let args = vec![
                    ColumnarValue::Array(string_array(rows, null_ratio, csv_trailing)),
                    utf8(","),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(limit))),
                ];
                split_group.bench_with_input(
                    BenchmarkId::from_parameter(format!(
                        "literal_char/limit_{limit}/{rows}/{null_tag}"
                    )),
                    &args,
                    |b, args| b.iter(|| black_box(spark_split(black_box(args)).unwrap())),
                );
            }
        }
    }
    split_group.finish();

    let mut split_sql_group = c.benchmark_group("spark_split_sql");

    for rows in ROW_COUNTS {
        for (null_ratio, null_tag) in NULL_RATIOS {
            // Multi-char literal delimiter: exercises the path where the match
            // advances by more than one byte.
            let args = vec![
                ColumnarValue::Array(string_array(rows, null_ratio, multi_char)),
                utf8("::"),
            ];
            split_sql_group.bench_with_input(
                BenchmarkId::from_parameter(format!("literal_multi_char/{rows}/{null_tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_split_sql(black_box(args)).unwrap())),
            );

            // Empty delimiter on the scalar branch: the behavior sunchao flagged
            // in review. Correctness is pinned by the split_part / split_sql tests;
            // this row only makes sure the fixed branch is not a performance cliff.
            let args = vec![
                ColumnarValue::Array(string_array(rows, null_ratio, csv)),
                utf8(""),
            ];
            split_sql_group.bench_with_input(
                BenchmarkId::from_parameter(format!("empty_delimiter/{rows}/{null_tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_split_sql(black_box(args)).unwrap())),
            );
        }
    }
    split_sql_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
