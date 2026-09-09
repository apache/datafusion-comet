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

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Int32Type, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{SparkHour, SparkMinute, SparkSecond};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8_192;
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Null density of an input shape. The general path's `unary_opt` only visits valid slots while
/// the fast path's `unary` visits every slot, so the dense and all-null shapes are where a
/// replacement like this is most likely to backfire.
#[derive(Clone, Copy)]
enum Nulls {
    None,
    Sparse,
    Dense,
    All,
}

impl Nulls {
    fn tag(self) -> &'static str {
        match self {
            Nulls::None => "no_nulls",
            Nulls::Sparse => "sparse_nulls",
            Nulls::Dense => "dense_nulls",
            Nulls::All => "all_nulls",
        }
    }

    fn is_null(self, i: usize) -> bool {
        match self {
            Nulls::None => false,
            Nulls::Sparse => i.is_multiple_of(8), // 12.5%
            Nulls::Dense => !i.is_multiple_of(8), // 87.5%
            Nulls::All => true,
        }
    }
}

/// A microsecond instant for row `i`, walking from roughly 1875 to 2065 so that both negative
/// and positive instants are covered. Negative instants are the ones that matter: truncation
/// toward zero would give the wrong field there, and `-1` us is 1969-12-31 23:59:59.999999.
/// The odd offset keeps values off exact second and hour boundaries.
fn instant(i: usize) -> i64 {
    (i as i64 - (ROWS as i64) / 2) * 730_000_000_000 + 12_345_678
}

fn timestamps(nulls: Nulls) -> TimestampMicrosecondArray {
    (0..ROWS)
        .map(|i| {
            if nulls.is_null(i) {
                None
            } else {
                Some(instant(i))
            }
        })
        .collect()
}

/// A dictionary-encoded timestamp column. The fast path deliberately does not cover dictionaries,
/// so this shape exercises the retained general path and acts as an untouched-code control.
fn dict_timestamps(cardinality: usize, nulls: Nulls) -> DictionaryArray<Int32Type> {
    let values = Arc::new(TimestampMicrosecondArray::from(
        (0..cardinality)
            .map(|i| instant(i * 97))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let keys: Int32Array = (0..ROWS)
        .map(|i| {
            if nulls.is_null(i) {
                None
            } else {
                Some((i % cardinality) as i32)
            }
        })
        .collect();
    DictionaryArray::<Int32Type>::new(keys, values)
}

// ---- the arithmetic the fast path uses ------------------------------------------------------

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
    let rows = array.len();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        arg_fields: vec![Arc::new(Field::new("ts", array.data_type().clone(), true))],
        number_rows: rows,
        return_field: Arc::new(Field::new(udf.name(), DataType::Int32, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
    .to_array(rows)
    .unwrap()
}

/// Checks the UDF against the plain arithmetic for a shape where no offset applies. This holds
/// on both sides of the change -- before it the UDF reached the same answer through `date_part`
/// -- so it validates the baseline run as well as the head run.
fn assert_matches_arithmetic(base: &TimestampMicrosecondArray, array: &ArrayRef, tz: &str) {
    for (udf, f) in [
        (
            Box::new(SparkHour::new(tz.to_string())) as Box<dyn ScalarUDFImpl>,
            hour_of_day as fn(i64) -> i32,
        ),
        (Box::new(SparkMinute::new(tz.to_string())), minute_of_hour),
        (Box::new(SparkSecond::new(tz.to_string())), second_of_minute),
    ] {
        let got = invoke(udf.as_ref(), array);
        let got = got.as_any().downcast_ref::<Int32Array>().unwrap();
        let want: Int32Array = base.unary(f);
        assert_eq!(
            got.null_count(),
            want.null_count(),
            "{} null count",
            udf.name()
        );
        for i in 0..got.len() {
            assert_eq!(
                got.is_null(i),
                want.is_null(i),
                "{} null at {i}",
                udf.name()
            );
        }
        assert_eq!(got, &want, "{} values", udf.name());
    }
}

fn bench_shape(c: &mut Criterion, shape: &str, tz: &str, array: ArrayRef) {
    for (name, udf) in [
        (
            "hour",
            Box::new(SparkHour::new(tz.to_string())) as Box<dyn ScalarUDFImpl>,
        ),
        ("minute", Box::new(SparkMinute::new(tz.to_string()))),
        ("second", Box::new(SparkSecond::new(tz.to_string()))),
    ] {
        let mut g = c.benchmark_group(format!("{name}/{shape}"));
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_function("udf", |b| {
            b.iter(|| black_box(invoke(black_box(udf.as_ref()), black_box(&array))))
        });
        g.finish();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    for nulls in [Nulls::None, Nulls::Sparse, Nulls::Dense, Nulls::All] {
        let base = timestamps(nulls);
        let tag = nulls.tag();

        // TimestampNTZ: eligible for the fast path whatever the session zone.
        let ntz: ArrayRef = Arc::new(base.clone());
        assert_matches_arithmetic(&base, &ntz, "America/Los_Angeles");
        bench_shape(c, &format!("ntz/{tag}"), "America/Los_Angeles", ntz);

        // Timezone-aware in a UTC session: eligible, since the stored value is the UTC instant.
        let utc: ArrayRef = Arc::new(base.clone().with_timezone("UTC"));
        assert_matches_arithmetic(&base, &utc, "UTC");
        bench_shape(c, &format!("utc_session/{tag}"), "UTC", utc);

        // An offset session zone keeps the general path: untouched-code control.
        let la: ArrayRef = Arc::new(base.clone().with_timezone("UTC"));
        bench_shape(c, &format!("la_session/{tag}"), "America/Los_Angeles", la);
    }

    // Dictionaries are deliberately outside the fast path: another untouched-code control.
    for cardinality in [8usize, 1024] {
        for nulls in [Nulls::None, Nulls::Sparse, Nulls::Dense] {
            bench_shape(
                c,
                &format!("dict{cardinality}/{}", nulls.tag()),
                "UTC",
                Arc::new(dict_timestamps(cardinality, nulls)),
            );
        }
    }

    // Sanity: the type the fast path keys off is still what we think it is.
    assert_eq!(
        timestamps(Nulls::None).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
