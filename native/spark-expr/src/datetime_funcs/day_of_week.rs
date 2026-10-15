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

//! Spark's `dayofweek` and `weekday`, computed straight from the epoch day.
//!
//! Both fields are a single modulo of the `Date32` value, so there is no need to build a calendar
//! date per row. Previously the serde emitted `datepart('dow'|'isodow', child)` followed by a
//! `+ 1` / `- 1` arithmetic node; `date_part` reconstructs a `NaiveDateTime` for every row and the
//! arithmetic node walks the result a second time.

use arrow::array::{Array, ArrayRef, AsArray, Date32Array, Int32Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Date32Type, Int32Type};
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Spark `DayOfWeek`: Sunday = 1, Monday = 2, ..., Saturday = 7.
///
/// Epoch day 0 (1970-01-01) is a Thursday, so `+ 4` moves Sunday to 0 before the shift to
/// 1-based. Widening to `i64` first keeps `i32::MAX` from overflowing.
#[inline]
fn day_of_week(days: i32) -> i32 {
    ((i64::from(days) + 4).rem_euclid(7) + 1) as i32
}

/// Spark `WeekDay`: Monday = 0, Tuesday = 1, ..., Sunday = 6.
#[inline]
fn week_day(days: i32) -> i32 {
    (i64::from(days) + 3).rem_euclid(7) as i32
}

/// Applies `kernel` to the dates.
///
/// `unary` evaluates every slot and vectorizes; `unary_opt` visits only valid indices but costs
/// more per element. The path this replaces used `unary_opt`, so an almost entirely null batch
/// was nearly free there and `unary` would regress it. Skipping only pays off once most of the
/// batch is null -- at 87.5% nulls `unary` was still ahead -- so switch on density rather than on
/// the mere presence of a null, which would give up most of the win on a lightly-null column.
///
/// `kernel` is generic rather than a `fn` pointer on purpose: a pointer forces an indirect call
/// per element inside `unary`, which blocks inlining and vectorization and costs roughly 2.5x.
#[inline]
fn map_dates<F: Fn(i32) -> i32 + Copy>(dates: &Date32Array, kernel: F) -> Int32Array {
    if dates.null_count() * 2 <= dates.len() {
        dates.unary::<_, Int32Type>(kernel)
    } else {
        dates.unary_opt::<_, Int32Type>(|d| Some(kernel(d)))
    }
}

macro_rules! epoch_day_extractor {
    ($struct_name:ident, $fn_name:expr, $kernel:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    // Spark types the child as DateType; a dictionary-encoded date column is
                    // unpacked below.
                    signature: Signature::any(1, Volatility::Immutable),
                }
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int32)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let [date] = take_function_args(self.name(), args.args)?;
                let array = date.into_array(args.number_rows)?;

                // A date partition column arrives dictionary-encoded from the Parquet scan. Map
                // the dictionary *values* and rewrap the keys, then unpack, which is what the
                // previous `Add(Cast(datepart(..), Int32), 1)` chain did: the calendar work is
                // proportional to the cardinality, not the row count. Decoding to Date32 first
                // would make a low-cardinality column markedly more expensive than before.
                if matches!(array.data_type(), DataType::Dictionary(_, _)) {
                    let dict = array.as_any_dictionary();
                    let values =
                        dict.values()
                            .as_primitive_opt::<Date32Type>()
                            .ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "{} expects a date, got {}",
                                    $fn_name,
                                    array.data_type()
                                ))
                            })?;
                    let mapped = Arc::new(map_dates(values, $kernel)) as ArrayRef;
                    let rewrapped = dict.with_values(mapped);
                    return Ok(ColumnarValue::Array(cast(&rewrapped, &DataType::Int32)?));
                }

                let dates = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{} expects a date, got {}",
                            $fn_name,
                            array.data_type()
                        ))
                    })?;

                Ok(ColumnarValue::Array(Arc::new(map_dates(dates, $kernel))))
            }
        }
    };
}

epoch_day_extractor!(
    SparkDayOfWeek,
    "spark_dayofweek",
    day_of_week,
    "Spark `dayofweek(date)`: Sunday = 1 through Saturday = 7."
);
epoch_day_extractor!(
    SparkWeekDay,
    "spark_weekday",
    week_day,
    "Spark `weekday(date)`: Monday = 0 through Sunday = 6."
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::DictionaryArray;
    use arrow::datatypes::Field;
    use datafusion::common::ScalarValue;
    use datafusion::config::ConfigOptions;

    fn invoke(udf: &dyn ScalarUDFImpl, array: ArrayRef) -> Int32Array {
        let rows = array.len();
        let arg_fields = vec![Arc::new(Field::new("d", array.data_type().clone(), true))];
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields,
                number_rows: rows,
                return_field: Arc::new(Field::new(udf.name(), DataType::Int32, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .to_array(rows)
            .unwrap();
        result
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone()
    }

    /// 1970-01-01 is a Thursday, so the week either side of the epoch pins both numberings.
    #[test]
    fn known_weekdays_around_the_epoch() {
        // days -4..=3 spans Sunday 1969-12-28 through Sunday 1970-01-04.
        let dates: Date32Array = (-4..=3).collect::<Vec<i32>>().into();
        let array: ArrayRef = Arc::new(dates);
        assert_eq!(
            invoke(&SparkDayOfWeek::new(), Arc::clone(&array)).values(),
            // Sun Mon Tue Wed Thu Fri Sat Sun
            &[1, 2, 3, 4, 5, 6, 7, 1]
        );
        assert_eq!(
            invoke(&SparkWeekDay::new(), array).values(),
            // Sun=6, Mon=0 ... Sat=5, Sun=6
            &[6, 0, 1, 2, 3, 4, 5, 6]
        );
    }

    /// Nulls must pass through in place, and the value under a null must not matter.
    #[test]
    fn nulls_are_preserved() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), None, Some(1), None]));
        let out = invoke(&SparkDayOfWeek::new(), array);
        assert_eq!(out.null_count(), 2);
        assert!(out.is_null(1) && out.is_null(3));
        assert_eq!(out.value(0), 5);
        assert_eq!(out.value(2), 6);
    }

    /// The whole `i32` domain must work: these are far outside chrono's representable range,
    /// where the previous `date_part` path produced NULL.
    #[test]
    fn full_i32_domain() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![i32::MIN, i32::MAX]));
        let dow = invoke(&SparkDayOfWeek::new(), Arc::clone(&array));
        let wd = invoke(&SparkWeekDay::new(), array);
        for i in 0..2 {
            assert!((1..=7).contains(&dow.value(i)), "dayofweek out of range");
            assert!((0..=6).contains(&wd.value(i)), "weekday out of range");
            // The two numberings must agree: dayofweek 1 (Sunday) is weekday 6.
            assert_eq!((dow.value(i) + 5) % 7, wd.value(i));
        }
    }

    /// The two numberings agree with each other across a full 400-year Gregorian cycle.
    #[test]
    fn numberings_agree_over_a_full_cycle() {
        for days in -146_097..=146_097 {
            assert_eq!((day_of_week(days) + 5) % 7, week_day(days), "day {days}");
        }
    }

    /// A dictionary-encoded date column (what a Parquet partition column looks like) unpacks to a
    /// plain Int32 result, matching what the old `datepart` + cast chain produced.
    #[test]
    fn dictionary_input_unpacks() {
        let values = Arc::new(Date32Array::from(vec![0, 1])) as ArrayRef;
        let keys = Int32Array::from(vec![0, 1, 0]);
        let dict = DictionaryArray::<Int32Type>::new(keys, values);
        let out = invoke(&SparkDayOfWeek::new(), Arc::new(dict));
        assert_eq!(out.values(), &[5, 6, 5]);
    }

    #[test]
    fn rejects_non_date_input() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let err = SparkDayOfWeek::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::clone(&array))],
                arg_fields: vec![Arc::new(Field::new("d", DataType::Int32, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("spark_dayofweek", DataType::Int32, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap_err();
        assert!(err.to_string().contains("expects a date"), "{err}");
    }

    /// Guard against the scalar path silently producing a wrong-length result.
    #[test]
    fn scalar_input_is_supported() {
        let out = SparkDayOfWeek::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Date32(Some(0)))],
                arg_fields: vec![Arc::new(Field::new("d", DataType::Date32, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("spark_dayofweek", DataType::Int32, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .to_array(1)
            .unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            5
        );
    }
}
