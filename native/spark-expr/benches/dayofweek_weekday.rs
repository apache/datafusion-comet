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

//! `dayofweek` / `weekday` over `Date32`, comparing the chain Comet serializes today against a
//! direct integer kernel on the epoch day.
//!
//! The `datepart_*` arms reproduce the chain the serde emitted before this change:
//! `datepart('dow', child) + 1` for `dayofweek` and `datepart('isodow', child) - 1` for
//! `weekday`. The `native_*` arms invoke the kernels the serde emits now. Both arms run in the
//! same process, so this is a direct comparison rather than a cross-run baseline (the native
//! kernels do not exist on `main`, so a saved baseline could not build them). `datepart` resolves to DataFusion's `date_part`, which for
//! `Date32` runs `unary_opt(|d| date32_to_datetime(d).map(..))` -- a `NaiveDateTime` per row plus
//! a recomputed null mask -- and the `+ 1` / `- 1` is a second pass over the result.

use arrow::array::{Array, ArrayRef, Date32Array, DictionaryArray, Int32Array, Scalar};
use arrow::compute::kernels::numeric::{add_wrapping, sub_wrapping};
use arrow::compute::{cast, date_part, DatePart};
use arrow::datatypes::{DataType, Field, Int32Type};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{SparkDayOfWeek, SparkWeekDay};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8_192;

/// Null density of an input shape. The old path's `unary_opt` only visits valid slots while the
/// kernel's `unary` visits every slot, so the dense and all-null shapes are where a replacement
/// like this is most likely to backfire.
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

/// Epoch days spread over roughly 1970..2050, the range a date column actually holds.
fn dates(nulls: Nulls) -> Date32Array {
    (0..ROWS)
        .map(|i| {
            if nulls.is_null(i) {
                None
            } else {
                Some((i as i32).wrapping_mul(3) % 29_220)
            }
        })
        .collect()
}

/// A dictionary-encoded date column, the shape a partition column arrives in from a Parquet
/// scan. Cardinality matters: the old path ran the calendar conversion over the dictionary
/// *values* only and rewrapped the keys, so a low-cardinality column did very little work.
fn dict_dates(cardinality: usize, nulls: Nulls) -> DictionaryArray<Int32Type> {
    let values = Arc::new(Date32Array::from(
        (0..cardinality)
            .map(|i| (i as i32).wrapping_mul(37) % 29_220)
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

// ---- the chain the serde emitted before this change ------------------------------------------

/// The serde emitted `Add(Cast(datepart('dow', child), Int32), 1)`. The cast came *first*: it was
/// an identity no-op for a plain array, but it is what unpacked a dictionary result, and arrow's
/// arithmetic kernels reject `Dictionary(Int32, Int32) + Int32` outright, so the order matters.
fn current_dayofweek(array: &ArrayRef) -> ArrayRef {
    let part = date_part(array.as_ref(), DatePart::DayOfWeekSunday0).unwrap();
    let unpacked = cast(&part, &DataType::Int32).unwrap();
    add_wrapping(&unpacked, &Scalar::new(Int32Array::from(vec![1]))).unwrap()
}

fn current_weekday(array: &ArrayRef) -> ArrayRef {
    let part = date_part(array.as_ref(), DatePart::DayOfWeekMonday1).unwrap();
    let unpacked = cast(&part, &DataType::Int32).unwrap();
    sub_wrapping(&unpacked, &Scalar::new(Int32Array::from(vec![1]))).unwrap()
}

// ---- native kernels, as the serde emits them now ---------------------------------------------

fn invoke(udf: &dyn ScalarUDFImpl, array: &ArrayRef) -> ArrayRef {
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        arg_fields: vec![Arc::new(Field::new("d", array.data_type().clone(), true))],
        number_rows: ROWS,
        return_field: Arc::new(Field::new(udf.name(), DataType::Int32, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
    .to_array(ROWS)
    .unwrap()
}

/// Both arms must agree, values *and* null buffer, before any timing is meaningful.
fn assert_equivalent(array: &ArrayRef) {
    for (current, native) in [
        (
            current_dayofweek(array),
            invoke(&SparkDayOfWeek::new(), array),
        ),
        (current_weekday(array), invoke(&SparkWeekDay::new(), array)),
    ] {
        let a = current.as_any().downcast_ref::<Int32Array>().unwrap();
        let b = native.as_any().downcast_ref::<Int32Array>().unwrap();
        // Compare nullability logically: the old path materialises an all-valid null buffer
        // where the kernel leaves `None`, which is the same thing to every consumer.
        assert_eq!(a.null_count(), b.null_count(), "null counts differ");
        for i in 0..a.len() {
            assert_eq!(a.is_null(i), b.is_null(i), "null flag differs at row {i}");
        }
        assert_eq!(a, b, "values differ");
    }
}

fn bench_shape(c: &mut Criterion, shape: &str, array: ArrayRef) {
    assert_equivalent(&array);

    let mut group = c.benchmark_group(format!("dayofweek/{shape}"));
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("datepart_dow_plus_one", |b| {
        b.iter(|| black_box(current_dayofweek(black_box(&array))))
    });
    let dow = SparkDayOfWeek::new();
    group.bench_function("native_spark_dayofweek", |b| {
        b.iter(|| black_box(invoke(black_box(&dow), black_box(&array))))
    });
    group.finish();

    let mut group = c.benchmark_group(format!("weekday/{shape}"));
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("datepart_isodow_minus_one", |b| {
        b.iter(|| black_box(current_weekday(black_box(&array))))
    });
    let wd = SparkWeekDay::new();
    group.bench_function("native_spark_weekday", |b| {
        b.iter(|| black_box(invoke(black_box(&wd), black_box(&array))))
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    for nulls in [Nulls::None, Nulls::Sparse, Nulls::Dense, Nulls::All] {
        bench_shape(c, &format!("flat/{}", nulls.tag()), Arc::new(dates(nulls)));
    }
    // Low cardinality is the case the old path handled cheaply: it converted 8 distinct dates
    // and rewrapped the keys. High cardinality approaches one conversion per row.
    for cardinality in [8usize, 1024] {
        for nulls in [Nulls::None, Nulls::Sparse, Nulls::Dense] {
            bench_shape(
                c,
                &format!("dict{cardinality}/{}", nulls.tag()),
                Arc::new(dict_dates(cardinality, nulls)),
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
