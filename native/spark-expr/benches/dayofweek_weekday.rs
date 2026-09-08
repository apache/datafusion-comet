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

use arrow::array::{Array, ArrayRef, Date32Array, Int32Array, Scalar};
use arrow::compute::kernels::numeric::{add_wrapping, sub_wrapping};
use arrow::compute::{date_part, DatePart};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{SparkDayOfWeek, SparkWeekDay};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8_192;
const NULL_STRIDE: usize = 8;

/// Epoch days spread over roughly 1970..2050, the range a date column actually holds.
fn dates(nulls: bool) -> Date32Array {
    (0..ROWS)
        .map(|i| {
            if nulls && i.is_multiple_of(NULL_STRIDE) {
                None
            } else {
                Some((i as i32).wrapping_mul(3) % 29_220)
            }
        })
        .collect()
}

// ---- current path -------------------------------------------------------------------------

fn current_dayofweek(array: &ArrayRef) -> ArrayRef {
    let part = date_part(array.as_ref(), DatePart::DayOfWeekSunday0).unwrap();
    add_wrapping(&part, &Scalar::new(Int32Array::from(vec![1]))).unwrap()
}

fn current_weekday(array: &ArrayRef) -> ArrayRef {
    let part = date_part(array.as_ref(), DatePart::DayOfWeekMonday1).unwrap();
    sub_wrapping(&part, &Scalar::new(Int32Array::from(vec![1]))).unwrap()
}

// ---- native kernels, as the serde now emits them --------------------------------------------

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

/// The benchmark is only meaningful if both arms agree, so check before timing.
fn assert_equivalent(array: &Date32Array) {
    let dyn_array: ArrayRef = Arc::new(array.clone());
    for (current, native) in [
        (
            current_dayofweek(&dyn_array),
            invoke(&SparkDayOfWeek::new(), &dyn_array),
        ),
        (
            current_weekday(&dyn_array),
            invoke(&SparkWeekDay::new(), &dyn_array),
        ),
    ] {
        assert_eq!(
            current.as_any().downcast_ref::<Int32Array>().unwrap(),
            native.as_any().downcast_ref::<Int32Array>().unwrap(),
        );
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    for (nulls, null_tag) in [(false, "no_nulls"), (true, "sparse_nulls")] {
        let array = dates(nulls);
        assert_equivalent(&array);
        let dyn_array: ArrayRef = Arc::new(array);

        let mut group = c.benchmark_group(format!("dayofweek/{null_tag}"));
        group.throughput(Throughput::Elements(ROWS as u64));
        group.bench_function("datepart_dow_plus_one", |b| {
            b.iter(|| black_box(current_dayofweek(black_box(&dyn_array))))
        });
        let dow = SparkDayOfWeek::new();
        group.bench_function("native_spark_dayofweek", |b| {
            b.iter(|| black_box(invoke(black_box(&dow), black_box(&dyn_array))))
        });
        group.finish();

        let mut group = c.benchmark_group(format!("weekday/{null_tag}"));
        group.throughput(Throughput::Elements(ROWS as u64));
        group.bench_function("datepart_isodow_minus_one", |b| {
            b.iter(|| black_box(current_weekday(black_box(&dyn_array))))
        });
        let wd = SparkWeekDay::new();
        group.bench_function("native_spark_weekday", |b| {
            b.iter(|| black_box(invoke(black_box(&wd), black_box(&dyn_array))))
        });
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
