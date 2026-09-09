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

use arrow::array::{ArrayRef, DictionaryArray, Int32Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Int32Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{SparkHour, SparkMinute, SparkSecond};
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{is_null, timestamp_micros_array, NULL_RATIOS, ROW_COUNTS};

const TZ: &str = "America/Los_Angeles";
/// A zero-offset session zone. Only in such a session is a timezone-aware timestamp eligible for
/// the integer fast path, so both branches of the dispatch need a session zone of their own.
const UTC_TZ: &str = "UTC";
const SMALL_ROWS: usize = 8_192;

/// `NULL_RATIOS` covers none / sparse / all. Dense is added because that is where a kernel which
/// evaluates every slot can lose to arrow's `unary_opt`, which visits only valid indices.
const DENSE_NULLS: (f64, &str) = (0.875, "dense");

/// Instants either side of the epoch, roughly 1875 to 2064 at 8192 rows. Pre-epoch values are the
/// ones that matter: `-1` us is 1969-12-31 23:59:59.999999, so truncating toward zero instead of
/// dividing Euclidean-style would give the wrong field.
fn spanning_epoch(i: usize, rows: usize) -> i64 {
    (i as i64 - (rows as i64) / 2) * 730_000_000_000 + 12_345_678
}

fn udfs(session_tz: &str) -> Vec<(&'static str, Box<dyn ScalarUDFImpl>)> {
    vec![
        ("hour", Box::new(SparkHour::new(session_tz.to_string()))),
        ("minute", Box::new(SparkMinute::new(session_tz.to_string()))),
        ("second", Box::new(SparkSecond::new(session_tz.to_string()))),
    ]
}

fn args_of(arr: ArrayRef) -> Vec<ColumnarValue> {
    vec![ColumnarValue::Array(arr)]
}

fn run(udf: &dyn ScalarUDFImpl, args: &[ColumnarValue], rows: usize) {
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("result", DataType::Int32, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

/// A dictionary-encoded timestamp column, the shape a partition column arrives in. Dictionaries
/// deliberately stay on the general path, so this is also a control for code the change does not
/// touch.
fn dict_timestamps(rows: usize, cardinality: usize, null_ratio: f64) -> ArrayRef {
    let values = Arc::new(TimestampMicrosecondArray::from(
        (0..cardinality)
            .map(|i| spanning_epoch(i * 97, cardinality * 97))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let keys: Int32Array = (0..rows)
        .map(|i| {
            if is_null(i, null_ratio) {
                None
            } else {
                Some((i % cardinality) as i32)
            }
        })
        .collect();
    Arc::new(DictionaryArray::<Int32Type>::new(keys, values))
}

fn criterion_benchmark(c: &mut Criterion) {
    // hour/minute/second all route through extract_date_part. The dispatch picks an integer fast
    // path when no timezone offset applies -- TimestampNTZ, or a timezone-aware value in a
    // zero-offset session -- and otherwise shifts to the session zone and reads a calendar
    // datetime. Sweep both session zones so each branch is measured.
    for (session_tz, session_tag) in [(TZ, "la_session"), (UTC_TZ, "utc_session")] {
        for (part, udf) in &udfs(session_tz) {
            let mut group = c.benchmark_group(*part);

            for (tz, tz_tag) in [(Some("UTC"), "tz"), (None, "ntz")] {
                // The full row sweep runs for the session zone the benchmark shipped with; the
                // second session zone and the dictionary shapes stay at one size to keep the
                // matrix affordable.
                let sizes: &[usize] = if session_tz == TZ {
                    &ROW_COUNTS
                } else {
                    &[SMALL_ROWS]
                };
                for &rows in sizes {
                    for (null_ratio, tag) in NULL_RATIOS
                        .iter()
                        .copied()
                        .chain(std::iter::once(DENSE_NULLS))
                    {
                        let arr = timestamp_micros_array(rows, null_ratio, tz, |i| {
                            spanning_epoch(i, rows)
                        });
                        let args = args_of(arr);
                        group.bench_with_input(
                            BenchmarkId::from_parameter(format!(
                                "{session_tag}/{tz_tag}/{rows}/{tag}"
                            )),
                            &args,
                            |b, args| b.iter(|| run(udf.as_ref(), args, rows)),
                        );
                    }
                }
            }

            for cardinality in [8usize, 1024] {
                for (null_ratio, tag) in NULL_RATIOS
                    .iter()
                    .copied()
                    .chain(std::iter::once(DENSE_NULLS))
                {
                    let args = args_of(dict_timestamps(SMALL_ROWS, cardinality, null_ratio));
                    group.bench_with_input(
                        BenchmarkId::from_parameter(format!(
                            "{session_tag}/dict{cardinality}/{SMALL_ROWS}/{tag}"
                        )),
                        &args,
                        |b, args| b.iter(|| run(udf.as_ref(), args, SMALL_ROWS)),
                    );
                }
            }

            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
