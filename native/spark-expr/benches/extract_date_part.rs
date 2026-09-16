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

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, TimestampMicrosecondArray};
use arrow::compute::cast;
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
/// the integer fast path, so each branch of the dispatch needs a session zone of its own.
const UTC_TZ: &str = "UTC";
const SMALL_ROWS: usize = 8_192;

/// `NULL_RATIOS` covers none / sparse / all. Dense is added because that is where a kernel which
/// evaluates every slot can lose to arrow's `unary_opt`, which visits only valid indices.
const DENSE_NULLS: (f64, &str) = (0.875, "dense");

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Instants either side of the epoch, roughly 1875 to 2064 at 8192 rows. Pre-epoch values are the
/// ones that matter: `-1` us is 1969-12-31 23:59:59.999999, so truncating toward zero instead of
/// dividing Euclidean-style would give the wrong field.
fn spanning_epoch(i: usize, rows: usize) -> i64 {
    (i as i64 - (rows as i64) / 2) * 730_000_000_000 + 12_345_678
}

fn hour_of_day(micros: i64) -> i32 {
    (micros.rem_euclid(MICROS_PER_DAY) / 3_600_000_000) as i32
}
fn minute_of_hour(micros: i64) -> i32 {
    micros.div_euclid(60_000_000).rem_euclid(60) as i32
}
fn second_of_minute(micros: i64) -> i32 {
    micros.div_euclid(1_000_000).rem_euclid(60) as i32
}

struct Part {
    name: &'static str,
    kernel: fn(i64) -> i32,
    max: i32,
}

const PARTS: [Part; 3] = [
    Part {
        name: "hour",
        kernel: hour_of_day,
        max: 23,
    },
    Part {
        name: "minute",
        kernel: minute_of_hour,
        max: 59,
    },
    Part {
        name: "second",
        kernel: second_of_minute,
        max: 59,
    },
];

fn udf_for(part: &str, session_tz: &str) -> Box<dyn ScalarUDFImpl> {
    match part {
        "hour" => Box::new(SparkHour::new(session_tz.to_string())),
        "minute" => Box::new(SparkMinute::new(session_tz.to_string())),
        _ => Box::new(SparkSecond::new(session_tz.to_string())),
    }
}

fn invoke(udf: &dyn ScalarUDFImpl, args: &[ColumnarValue], rows: usize) -> ArrayRef {
    udf.invoke_with_args(ScalarFunctionArgs {
        args: args.to_vec(),
        arg_fields: vec![],
        number_rows: rows,
        return_field: Arc::new(Field::new("result", DataType::Int32, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
    .to_array(rows)
    .unwrap()
}

/// A dictionary result stays dictionary-encoded; flatten so every shape is compared the same way.
fn as_int32(array: &ArrayRef) -> Int32Array {
    let flat = if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        cast(array, &DataType::Int32).unwrap()
    } else {
        Arc::clone(array)
    };
    flat.as_any().downcast_ref::<Int32Array>().unwrap().clone()
}

/// Validates a shape before it is timed. Every shape gets a null-aware check: the result must be
/// null exactly where the input is, and every non-null value must be inside the field's range.
/// Shapes eligible for the fast path additionally get an exact oracle -- the field recomputed
/// from the generator, independent of arrow -- since no session offset applies to them.
fn check(
    part: &Part,
    array: &ArrayRef,
    udf: &dyn ScalarUDFImpl,
    rows: usize,
    oracle: Option<&dyn Fn(usize) -> Option<i32>>,
) {
    let out = as_int32(&invoke(
        udf,
        &[ColumnarValue::Array(Arc::clone(array))],
        rows,
    ));
    let input_nulls = array.logical_nulls();
    assert_eq!(out.len(), rows, "{}: length", part.name);
    for i in 0..rows {
        let input_null = input_nulls.as_ref().is_some_and(|n| n.is_null(i));
        assert_eq!(
            out.is_null(i),
            input_null,
            "{}: null flag at row {i} ({:?})",
            part.name,
            array.data_type()
        );
        if !input_null {
            let v = out.value(i);
            assert!(
                (0..=part.max).contains(&v),
                "{}: value {v} out of range at row {i}",
                part.name
            );
            if let Some(f) = oracle {
                assert_eq!(Some(v), f(i), "{}: value at row {i}", part.name);
            }
        }
    }
}

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

fn null_ratios() -> impl Iterator<Item = (f64, &'static str)> {
    NULL_RATIOS
        .iter()
        .copied()
        .chain(std::iter::once(DENSE_NULLS))
}

/// The stride approximation in `common::is_null` has to actually produce the density each shape
/// claims, or a "dense" case silently becomes a second all-null case.
fn assert_density(rows: usize, null_ratio: f64, tag: &str) {
    let nulls = (0..rows).filter(|&i| is_null(i, null_ratio)).count();
    let expected = (rows as f64 * null_ratio).round() as usize;
    assert!(
        nulls.abs_diff(expected) <= rows / 100,
        "{tag}: expected about {expected} nulls of {rows}, generator produced {nulls}"
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    // hour/minute/second all route through extract_date_part. The dispatch takes an integer fast
    // path when no timezone offset applies -- TimestampNTZ, or a timezone-aware value in a
    // zero-offset session -- and otherwise shifts to the session zone and reads a calendar
    // datetime. Both session zones are swept so each branch is measured.
    for (session_tz, session_tag) in [(TZ, "la_session"), (UTC_TZ, "utc_session")] {
        for part in &PARTS {
            let udf = udf_for(part.name, session_tz);
            let mut group = c.benchmark_group(part.name);

            for (tz, tz_tag) in [(Some("UTC"), "tz"), (None, "ntz")] {
                // The full row sweep stays on the session zone the benchmark shipped with; the
                // second zone and the dictionary shapes use one size to bound the matrix.
                let sizes: &[usize] = if session_tz == TZ {
                    &ROW_COUNTS
                } else {
                    &[SMALL_ROWS]
                };
                // No offset applies to NTZ in any session, nor to a stored UTC instant read in a
                // zero-offset session, so those shapes have an exact oracle.
                let eligible = tz.is_none() || session_tz == UTC_TZ;
                for &rows in sizes {
                    for (null_ratio, tag) in null_ratios() {
                        assert_density(rows, null_ratio, tag);
                        let arr = timestamp_micros_array(rows, null_ratio, tz, |i| {
                            spanning_epoch(i, rows)
                        });
                        let oracle = |i: usize| {
                            if is_null(i, null_ratio) {
                                None
                            } else {
                                Some((part.kernel)(spanning_epoch(i, rows)))
                            }
                        };
                        check(
                            part,
                            &arr,
                            udf.as_ref(),
                            rows,
                            if eligible { Some(&oracle) } else { None },
                        );
                        let args = vec![ColumnarValue::Array(arr)];
                        group.bench_with_input(
                            BenchmarkId::from_parameter(format!(
                                "{session_tag}/{tz_tag}/{rows}/{tag}"
                            )),
                            &args,
                            |b, args| {
                                b.iter(|| black_box(invoke(udf.as_ref(), args, rows)));
                            },
                        );
                    }
                }
            }

            for cardinality in [8usize, 1024] {
                for (null_ratio, tag) in null_ratios() {
                    let arr = dict_timestamps(SMALL_ROWS, cardinality, null_ratio);
                    // Dictionaries stay on the general path in every session, so there is no
                    // arithmetic oracle; the null-aware and range checks still apply.
                    check(part, &arr, udf.as_ref(), SMALL_ROWS, None);
                    let args = vec![ColumnarValue::Array(arr)];
                    group.bench_with_input(
                        BenchmarkId::from_parameter(format!(
                            "{session_tag}/dict{cardinality}/{SMALL_ROWS}/{tag}"
                        )),
                        &args,
                        |b, args| {
                            b.iter(|| black_box(invoke(udf.as_ref(), args, SMALL_ROWS)));
                        },
                    );
                }
            }

            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
