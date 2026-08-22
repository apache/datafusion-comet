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
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion_comet_spark_expr::spark_split;
use std::hint::black_box;

use std::sync::Arc;

fn generate_string_array(num_rows: usize, pattern_type: &str) -> Arc<StringArray> {
    let mut builder = arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 64);

    for i in 0..num_rows {
        if i % 20 == 0 {
            builder.append_null();
            continue;
        }

        match pattern_type {
            "csv" => builder.append_value(format!(
                "field1_{i},field2_{i},field3_{i},field4_{i},field5_{i}"
            )),
            "whitespace_regex" => {
                builder.append_value(format!("word1_{i}   word2_{i} \t  word3_{i}    word4_{i}"))
            }
            "trailing_delimiters" => builder.append_value(format!("data_{i},,,,")),
            _ => unreachable!(),
        }
    }

    Arc::new(builder.finish())
}

fn bench_spark_split(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_split");
    let batch_sizes = [1024, 8192];

    for &size in &batch_sizes {
        group.throughput(Throughput::Elements(size as u64));

        {
            let array = generate_string_array(size, "csv");
            let args = vec![
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
            ];

            group.bench_with_input(
                BenchmarkId::new("literal_char_default_limit", size),
                &args,
                |b, args| {
                    b.iter(|| {
                        black_box(spark_split(black_box(args)).unwrap());
                    });
                },
            );
        }

        {
            let array = generate_string_array(size, "whitespace_regex");
            let args = vec![
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(r"\s".to_string()))),
            ];

            group.bench_with_input(BenchmarkId::new("regex_pattern", size), &args, |b, args| {
                b.iter(|| {
                    black_box(spark_split(black_box(args)).unwrap());
                });
            });
        }

        {
            let array = generate_string_array(size, "trailing_delimiters");
            let args = vec![
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
            ];

            group.bench_with_input(
                BenchmarkId::new("literal_char_limit_0", size),
                &args,
                |b, args| {
                    b.iter(|| {
                        black_box(spark_split(black_box(args)).unwrap());
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_spark_split);
criterion_main!(benches);
