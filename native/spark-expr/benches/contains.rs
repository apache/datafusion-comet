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

use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkContains;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{string_array, NULL_RATIOS, ROW_COUNTS};

/// Scalar used as the haystack in the scalar/array shape. The matching needle
/// values below are chosen so the scalar/array shape performs real work.
const HAYSTACK_SCALAR: &str = "datafusion-comet";

/// Scalar used as the needle in the array/scalar shape.
const NEEDLE_SCALAR: &str = "comet";

fn build_args(
    haystack: ColumnarValue,
    needle: ColumnarValue,
    number_rows: usize,
) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args: vec![haystack, needle],
        arg_fields: vec![],
        number_rows,
        return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkContains::new();

    // ------------------------------------------------------------------
    // Shape 1: array haystack vs scalar needle (`contains_array_scalar`).
    // This path already used a scalar representation on `main`; included as a
    // regression control since this PR touches it incidentally.
    // ------------------------------------------------------------------
    let mut group = c.benchmark_group("spark_contains/array_scalar");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let haystack = string_array(rows, null_ratio, |_| "datafusion-comet".to_string());
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &haystack,
                |b, haystack| {
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(build_args(
                                ColumnarValue::Array(Arc::clone(haystack)),
                                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                    NEEDLE_SCALAR.to_string(),
                                ))),
                                haystack.len(),
                            ))
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }
    group.finish();

    // ------------------------------------------------------------------
    // Shape 2: scalar haystack vs array needle (`contains_scalar_array`).
    // This is the path the PR actually optimizes (it replaced
    // `to_array_of_size(N)` with an O(1) broadcast), so it must be measured.
    // The needle array is varied per row so the kernel does non-trivial work.
    // ------------------------------------------------------------------
    let mut group = c.benchmark_group("spark_contains/scalar_array");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let needle = string_array(rows, null_ratio, |i| {
                if i % 2 == 0 {
                    "comet".to_string()
                } else {
                    format!("comet-{i}")
                }
            });
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &needle,
                |b, needle| {
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(build_args(
                                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                    HAYSTACK_SCALAR.to_string(),
                                ))),
                                ColumnarValue::Array(Arc::clone(needle)),
                                needle.len(),
                            ))
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }
    group.finish();

    // ------------------------------------------------------------------
    // Shape 3: array haystack vs array needle (`arrow_contains` directly).
    // Regression control for the straight-through path the PR does not touch.
    // ------------------------------------------------------------------
    let mut group = c.benchmark_group("spark_contains/array_array");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let haystack = string_array(rows, null_ratio, |_| "datafusion-comet".to_string());
            let needle = string_array(rows, null_ratio, |_| "comet".to_string());
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &(haystack, needle),
                |b, (haystack, needle)| {
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(build_args(
                                ColumnarValue::Array(Arc::clone(haystack)),
                                ColumnarValue::Array(Arc::clone(needle)),
                                haystack.len(),
                            ))
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
