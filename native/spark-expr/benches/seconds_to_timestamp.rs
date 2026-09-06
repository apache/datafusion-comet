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

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, TimeUnit};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkSecondsToTimestamp;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{f32_array, f64_array, i32_array, i64_array, NULL_RATIOS, ROW_COUNTS};

fn inputs(rows: usize, null_ratio: f64) -> Vec<(&'static str, ArrayRef)> {
    vec![
        ("int32", i32_array(rows, null_ratio, |i| (i % 100_000) as i32)),
        ("int64", i64_array(rows, null_ratio, |i| (i % 100_000) as i64)),
        ("float32", f32_array(rows, null_ratio, |i| (i % 100_000) as f32)),
        ("float64", f64_array(rows, null_ratio, |i| (i % 100_000) as f64)),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkSecondsToTimestamp::new();
    let mut group = c.benchmark_group("seconds_to_timestamp");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            for (ty, arr) in inputs(rows, null_ratio) {
                let args = vec![ColumnarValue::Array(arr)];
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{ty}/{rows}/{tag}")),
                    &args,
                    |b, args| {
                        b.iter(|| {
                            black_box(
                                udf.invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: vec![],
                                    number_rows: rows,
                                    return_field: Arc::new(Field::new(
                                        "result",
                                        DataType::Timestamp(TimeUnit::Microsecond, None),
                                        true,
                                    )),
                                    config_options: Arc::new(ConfigOptions::default()),
                                })
                                .unwrap(),
                            )
                        })
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
