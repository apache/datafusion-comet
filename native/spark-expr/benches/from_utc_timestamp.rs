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

//! `from_utc_timestamp` is a datafusion-spark passthrough. Comet's serde wires it directly, so the
//! kernel itself is upstream; this bench guards against regressions in the version Comet pins and
//! is the same shape as the (composed) `convert_timezone` cost.

use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::datetime::from_utc_timestamp::SparkFromUtcTimestamp;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{timestamp_micros_array, NULL_RATIOS, ROW_COUNTS};

const TZ: &str = "America/Los_Angeles";
const MICROS_PER_DAY: i64 = 86_400_000_000;

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkFromUtcTimestamp::new();
    let return_field = Arc::new(Field::new(
        "from_utc_timestamp",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        true,
    ));

    let mut group = c.benchmark_group("from_utc_timestamp");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            // Input is TimestampNTZ (Timestamp(Microsecond, None)); the timezone arrives as the
            // second, scalar argument.
            let ts =
                timestamp_micros_array(rows, null_ratio, None, |i| (i as i64) * MICROS_PER_DAY);
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &ts,
                |b, ts| {
                    b.iter(|| {
                        let args = ScalarFunctionArgs {
                            args: vec![
                                ColumnarValue::Array(Arc::clone(ts)),
                                ColumnarValue::Scalar(ScalarValue::Utf8(Some(TZ.to_string()))),
                            ],
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
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
