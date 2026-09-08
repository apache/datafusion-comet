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

//! `hour` / `minute` / `second` over microsecond timestamps.
//!
//! `SparkHour` and friends call arrow's `date_part`, which builds a datetime per row. For
//! `TimestampNTZ`, and for a timezone-aware timestamp in a UTC session, the clock fields are a
//! pure function of the stored microseconds. The `America/Los_Angeles` arm is the case such a
//! fast path could not serve, and is here as a reference point, not as an A/B pair.

use arrow::array::{Array, ArrayRef, Int32Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Int32Type, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{SparkHour, SparkMinute, SparkSecond};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8_192;
const NULL_STRIDE: usize = 8;
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Microsecond timestamps spread over about a decade either side of the epoch, so that the
/// negative-instant path (where truncation toward zero would give the wrong answer) is covered.
fn timestamps(nulls: bool) -> TimestampMicrosecondArray {
    (0..ROWS)
        .map(|i| {
            if nulls && i.is_multiple_of(NULL_STRIDE) {
                None
            } else {
                Some((i as i64).wrapping_mul(2_923_477_211) - 150_000_000_000_000)
            }
        })
        .collect()
}

// ---- proposed integer kernels -------------------------------------------------------------

#[inline]
fn hour_of_day(micros: i64) -> i32 {
    (micros.rem_euclid(MICROS_PER_DAY) / 3_600_000_000) as i32
}

#[inline]
fn minute_of_hour(micros: i64) -> i32 {
    micros.div_euclid(60_000_000).rem_euclid(60) as i32
}

#[inline]
fn second_of_minute(micros: i64) -> i32 {
    micros.div_euclid(1_000_000).rem_euclid(60) as i32
}

fn invoke(udf: &dyn ScalarUDFImpl, array: &ArrayRef) -> ArrayRef {
    let arg_fields = vec![Arc::new(Field::new("ts", array.data_type().clone(), true))];
    let return_type = udf.return_type(&[array.data_type().clone()]).unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        arg_fields,
        number_rows: ROWS,
        return_field: Arc::new(Field::new(udf.name(), return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
    .to_array(ROWS)
    .unwrap()
}

/// Confirms the integer kernel reproduces `date_part` exactly for the shapes it would replace.
fn assert_equivalent(array: &TimestampMicrosecondArray, dyn_array: &ArrayRef, tz: &str) {
    for (udf, f) in [
        (
            Box::new(SparkHour::new(tz.to_string())) as Box<dyn ScalarUDFImpl>,
            hour_of_day as fn(i64) -> i32,
        ),
        (Box::new(SparkMinute::new(tz.to_string())), minute_of_hour),
        (Box::new(SparkSecond::new(tz.to_string())), second_of_minute),
    ] {
        let current = invoke(udf.as_ref(), dyn_array);
        let kernel: Int32Array = array.unary::<_, Int32Type>(f);
        assert_eq!(
            current.as_any().downcast_ref::<Int32Array>().unwrap(),
            &kernel,
            "{} mismatch for tz {tz} on {:?}",
            udf.name(),
            dyn_array.data_type()
        );
    }
}

/// Benches one field for one input shape. The kernel is passed as a generic `F` and called
/// directly, so it inlines and vectorizes the way a real kernel would; routing it through a
/// `black_box`ed function pointer instead would measure an indirect call per element.
fn bench_field<F>(
    c: &mut Criterion,
    group: String,
    udf: &dyn ScalarUDFImpl,
    dyn_array: &ArrayRef,
    base: &TimestampMicrosecondArray,
    kernel: Option<F>,
) where
    F: Fn(i64) -> i32 + Copy,
{
    let mut g = c.benchmark_group(group);
    g.throughput(Throughput::Elements(ROWS as u64));
    g.bench_function("date_part", |b| {
        b.iter(|| black_box(invoke(udf, black_box(dyn_array))))
    });
    if let Some(k) = kernel {
        g.bench_function("integer_kernel", |b| {
            b.iter(|| {
                let out: Int32Array = black_box(base).unary::<_, Int32Type>(k);
                black_box(out)
            })
        });
    }
    g.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    for (nulls, null_tag) in [(false, "no_nulls"), (true, "sparse_nulls")] {
        let base = timestamps(nulls);

        // (shape tag, session timezone, array, replaceable by the integer kernel)
        let shapes: Vec<(&str, &str, ArrayRef, bool)> = vec![
            ("ntz", "America/Los_Angeles", Arc::new(base.clone()), true),
            (
                "utc_session",
                "UTC",
                Arc::new(base.clone().with_timezone("UTC")),
                true,
            ),
            (
                "la_session",
                "America/Los_Angeles",
                Arc::new(base.clone().with_timezone("UTC")),
                false,
            ),
        ];

        for (shape, tz, dyn_array, comparable) in shapes {
            if comparable {
                assert_equivalent(&base, &dyn_array, tz);
            }
            bench_field(
                c,
                format!("hour/{shape}/{null_tag}"),
                &SparkHour::new(tz.to_string()),
                &dyn_array,
                &base,
                comparable.then_some(hour_of_day),
            );
            bench_field(
                c,
                format!("minute/{shape}/{null_tag}"),
                &SparkMinute::new(tz.to_string()),
                &dyn_array,
                &base,
                comparable.then_some(minute_of_hour),
            );
            bench_field(
                c,
                format!("second/{shape}/{null_tag}"),
                &SparkSecond::new(tz.to_string()),
                &dyn_array,
                &base,
                comparable.then_some(second_of_minute),
            );
        }
    }

    // Sanity: the type the fast path keys off is still what we think it is.
    assert_eq!(
        timestamps(false).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
