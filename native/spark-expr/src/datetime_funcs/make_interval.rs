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

use crate::arithmetic_overflow_error;
use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, Int64Array, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::collections::HashMap;
use std::sync::Arc;

const CALENDAR_INTERVAL_STRUCT_KEY: &str = "SPARK::calendarInterval::struct";
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_MINUTE: i64 = 60_000_000;

pub fn calendar_interval_type() -> DataType {
    let months = Field::new("months", DataType::Int32, false).with_metadata(HashMap::from([(
        CALENDAR_INTERVAL_STRUCT_KEY.to_string(),
        "true".to_string(),
    )]));
    DataType::Struct(Fields::from(vec![
        months,
        Field::new("days", DataType::Int32, false),
        Field::new("microseconds", DataType::Int64, false),
    ]))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    signature: Signature,
    fail_on_error: bool,
}

impl SparkMakeInterval {
    pub fn new(fail_on_error: bool) -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Int32,
                    DataType::Decimal128(18, 6),
                ],
                Volatility::Immutable,
            ),
            fail_on_error,
        }
    }
}

fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    minutes: i32,
    seconds_micros: i128,
) -> std::result::Result<(i32, i32, i64), &'static str> {
    let months = years
        .checked_mul(12)
        .and_then(|years| years.checked_add(months))
        .ok_or("integer")?;
    let days = weeks
        .checked_mul(7)
        .and_then(|weeks| weeks.checked_add(days))
        .ok_or("integer")?;
    let micros = i64::try_from(seconds_micros)
        .ok()
        .and_then(|seconds| {
            i64::from(hours)
                .checked_mul(MICROS_PER_HOUR)
                .and_then(|hours| seconds.checked_add(hours))
        })
        .and_then(|micros| {
            i64::from(minutes)
                .checked_mul(MICROS_PER_MINUTE)
                .and_then(|minutes| micros.checked_add(minutes))
        })
        .ok_or("long")?;
    Ok((months, days, micros))
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn name(&self) -> &str {
        "make_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(calendar_interval_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let arrays = args
            .args
            .into_iter()
            .map(|arg| arg.into_array(number_rows))
            .collect::<Result<Vec<_>>>()?;
        let years = arrays[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Int32 years".into())
            })?;
        let months = arrays[1]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Int32 months".into())
            })?;
        let weeks = arrays[2]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Int32 weeks".into())
            })?;
        let days = arrays[3]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Int32 days".into())
            })?;
        let hours = arrays[4]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Int32 hours".into())
            })?;
        let minutes = arrays[5]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Int32 minutes".into())
            })?;
        let seconds = arrays[6]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_interval: expected Decimal128 seconds".into())
            })?;

        let mut result_months = Vec::with_capacity(years.len());
        let mut result_days = Vec::with_capacity(years.len());
        let mut result_micros = Vec::with_capacity(years.len());
        let mut valid = Vec::with_capacity(years.len());

        for i in 0..years.len() {
            if arrays.iter().any(|array| array.is_null(i)) {
                result_months.push(0);
                result_days.push(0);
                result_micros.push(0);
                valid.push(false);
                continue;
            }

            match make_interval(
                years.value(i),
                months.value(i),
                weeks.value(i),
                days.value(i),
                hours.value(i),
                minutes.value(i),
                seconds.value(i),
            ) {
                Ok((months, days, micros)) => {
                    result_months.push(months);
                    result_days.push(days);
                    result_micros.push(micros);
                    valid.push(true);
                }
                Err(from_type) if self.fail_on_error => {
                    return Err(arithmetic_overflow_error(from_type).into());
                }
                Err(_) => {
                    result_months.push(0);
                    result_days.push(0);
                    result_micros.push(0);
                    valid.push(false);
                }
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(result_months)),
            Arc::new(Int32Array::from(result_days)),
            Arc::new(Int64Array::from(result_micros)),
        ];
        let DataType::Struct(fields) = calendar_interval_type() else {
            unreachable!()
        };
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            fields,
            columns,
            Some(NullBuffer::from(valid)),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_spark_microsecond_range_and_overflow() {
        assert_eq!(
            make_interval(1, 2, 3, 4, 2_562_048, 0, 123_456_789_012_123_456),
            Ok((14, 25, 132_680_161_812_123_456))
        );
        assert_eq!(make_interval(i32::MAX, 0, 0, 0, 0, 0, 0), Err("integer"));
        assert_eq!(
            make_interval(0, 0, 0, 0, i32::MAX, i32::MAX, i128::MAX),
            Err("long")
        );
    }
}
