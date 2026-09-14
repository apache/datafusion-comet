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
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{SparkHour, SparkMinute, SparkSecond};
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{timestamp_micros_array, NULL_RATIOS, ROW_COUNTS};

const TZ: &str = "America/Los_Angeles";
const BASE_MICROS: i64 = 1_600_000_000_000_000;

fn criterion_benchmark(c: &mut Criterion) {
    // hour/minute/second all route through extract_date_part; sweep the three parts and both the
    // timezone-aware (stored UTC, shifted to session tz) and TimestampNTZ (extracted directly) paths.
    let parts: Vec<(&str, Box<dyn ScalarUDFImpl>)> = vec![
        ("hour", Box::new(SparkHour::new(TZ.to_string()))),
        ("minute", Box::new(SparkMinute::new(TZ.to_string()))),
        ("second", Box::new(SparkSecond::new(TZ.to_string()))),
    ];
    for (part, udf) in &parts {
        let mut group = c.benchmark_group(*part);
        for (tz, tz_tag) in [(Some("UTC"), "tz"), (None, "ntz")] {
            for rows in ROW_COUNTS {
                for (null_ratio, tag) in NULL_RATIOS {
                    let arr = timestamp_micros_array(rows, null_ratio, tz, |i| {
                        BASE_MICROS + (i as i64) * 3_600_000_000
                    });
                    let args = vec![ColumnarValue::Array(arr)];
                    group.bench_with_input(
                        BenchmarkId::from_parameter(format!("{tz_tag}/{rows}/{tag}")),
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
                                            DataType::Int32,
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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
