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

use arrow::array::Decimal128Array;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkMakeTime;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{i32_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkMakeTime::new();
    let mut group = c.benchmark_group("make_time");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            // seconds is Decimal(16, 6) (micros). Build valid 0-59 values so the kernel runs its
            // success path; nulls are exercised via the hour/minute columns.
            let secs = Decimal128Array::from_iter_values(
                (0..rows).map(|i| ((i % 60) as i128) * 1_000_000),
            )
            .with_precision_and_scale(16, 6)
            .unwrap();
            let args = vec![
                ColumnarValue::Array(i32_array(rows, null_ratio, |i| (i % 24) as i32)),
                ColumnarValue::Array(i32_array(rows, null_ratio, |i| (i % 60) as i32)),
                ColumnarValue::Array(Arc::new(secs)),
            ];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| {
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![],
                                number_rows: rows,
                                return_field: Arc::new(Field::new("result", DataType::Null, true)),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
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
