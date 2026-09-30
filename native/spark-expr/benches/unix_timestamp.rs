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

use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkUnixTimestamp;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{timestamp_micros_array, NULL_RATIOS, ROW_COUNTS};

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn criterion_benchmark(c: &mut Criterion) {
    let return_field = Arc::new(Field::new("unix_timestamp", DataType::Int64, true));

    let mut group = c.benchmark_group("unix_timestamp");
    // TimestampNTZ skips timezone resolution (plain integer division); a tz-aware input runs
    // through `array_with_timezone` first. Bench both so a regression in either path shows up.
    for (variant, tz_input, udf_tz) in [
        ("ntz", None, "UTC"),
        (
            "tz_aware",
            Some("America/Los_Angeles"),
            "America/Los_Angeles",
        ),
    ] {
        let udf = SparkUnixTimestamp::new(udf_tz.to_string());
        for rows in ROW_COUNTS {
            for (null_ratio, tag) in NULL_RATIOS {
                let ts = timestamp_micros_array(rows, null_ratio, tz_input, |i| {
                    (i as i64) * MICROS_PER_DAY
                });
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{variant}/{rows}/{tag}")),
                    &ts,
                    |b, ts| {
                        b.iter(|| {
                            let args = ScalarFunctionArgs {
                                args: vec![ColumnarValue::Array(Arc::clone(ts))],
                                number_rows: ts.len(),
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::new(ConfigOptions::default()),
                                arg_fields: vec![],
                            };
                            black_box(udf.invoke_with_args(args).unwrap())
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
