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

use arrow::array::{Array, Decimal128Array, Int32Array, Time64NanosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

const MICROS_PER_SECOND: i128 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeTime {
    signature: Signature,
}

impl SparkMakeTime {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl Default for SparkMakeTime {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts hours, minutes, and fractional seconds (Decimal(16,6)) to nanoseconds from midnight.
/// Returns an error for invalid inputs (matching Spark's always-throw behavior).
fn make_time(hours: i32, minutes: i32, secs_and_micros_unscaled: i128) -> Result<i64> {
    let full_secs = secs_and_micros_unscaled.div_euclid(MICROS_PER_SECOND);
    let frac_micros = secs_and_micros_unscaled.rem_euclid(MICROS_PER_SECOND);

    if full_secs > i32::MAX as i128 || full_secs < 0 {
        return Err(DataFusionError::Execution(format!(
            "Invalid value for SecondOfMinute (valid values 0 - 59): {}",
            secs_and_micros_unscaled / MICROS_PER_SECOND
        )));
    }

    let secs = full_secs as i32;
    let nanos = (frac_micros as i64) * NANOS_PER_MICRO;

    if !(0..=23).contains(&hours) {
        return Err(DataFusionError::Execution(format!(
            "Invalid value for HourOfDay (valid values 0 - 23): {hours}"
        )));
    }
    if !(0..=59).contains(&minutes) {
        return Err(DataFusionError::Execution(format!(
            "Invalid value for MinuteOfHour (valid values 0 - 59): {minutes}"
        )));
    }
    if !(0..=59).contains(&secs) {
        return Err(DataFusionError::Execution(format!(
            "Invalid value for SecondOfMinute (valid values 0 - 59): {secs}"
        )));
    }

    let total_nanos = hours as i64 * 3_600 * NANOS_PER_SECOND
        + minutes as i64 * 60 * NANOS_PER_SECOND
        + secs as i64 * NANOS_PER_SECOND
        + nanos;

    Ok(total_nanos)
}

impl ScalarUDFImpl for SparkMakeTime {
    fn name(&self) -> &str {
        "make_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Nanosecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [hours, minutes, secs_and_micros] = take_function_args(self.name(), args.args)?;

        let num_rows = [&hours, &minutes, &secs_and_micros]
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

        let hours_arr = hours.into_array(num_rows)?;
        let minutes_arr = minutes.into_array(num_rows)?;
        let secs_arr = secs_and_micros.into_array(num_rows)?;

        let hours_arr = cast_to_int32(&hours_arr)?;
        let minutes_arr = cast_to_int32(&minutes_arr)?;

        let hours_array = hours_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_time: failed to cast hours to Int32".to_string())
            })?;

        let minutes_array = minutes_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_time: failed to cast minutes to Int32".to_string())
            })?;

        let secs_array = secs_arr
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "make_time: expected Decimal128 for seconds argument".to_string(),
                )
            })?;

        let len = hours_array.len();
        let mut builder = Time64NanosecondArray::builder(len);

        for i in 0..len {
            if hours_array.is_null(i) || minutes_array.is_null(i) || secs_array.is_null(i) {
                builder.append_null();
            } else {
                let h = hours_array.value(i);
                let m = minutes_array.value(i);
                let s = secs_array.value(i);

                let nanos = make_time(h, m, s)?;
                builder.append_value(nanos);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn cast_to_int32(arr: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    if arr.data_type() == &DataType::Int32 {
        Ok(Arc::clone(arr))
    } else {
        cast(arr.as_ref(), &DataType::Int32)
            .map_err(|e| DataFusionError::Execution(format!("Failed to cast to Int32: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_time_valid() {
        // Midnight
        assert_eq!(make_time(0, 0, 0).unwrap(), 0);
        // 1 hour
        assert_eq!(make_time(1, 0, 0).unwrap(), 3_600_000_000_000);
        // 1 minute
        assert_eq!(make_time(0, 1, 0).unwrap(), 60_000_000_000);
        // 1 second (unscaled: 1_000_000)
        assert_eq!(make_time(0, 0, 1_000_000).unwrap(), 1_000_000_000);
        // 1.5 seconds (unscaled: 1_500_000)
        assert_eq!(make_time(0, 0, 1_500_000).unwrap(), 1_500_000_000);
        // 23:59:59.999999 (unscaled: 59_999_999)
        assert_eq!(make_time(23, 59, 59_999_999).unwrap(), 86_399_999_999_000);
        // 12:30:45.123456 (unscaled: 45_123_456)
        assert_eq!(
            make_time(12, 30, 45_123_456).unwrap(),
            12 * 3_600_000_000_000 + 30 * 60_000_000_000 + 45_123_456_000
        );
    }

    #[test]
    fn test_make_time_invalid_hours() {
        assert!(make_time(24, 0, 0).is_err());
        assert!(make_time(25, 0, 0).is_err());
        assert!(make_time(-1, 0, 0).is_err());
    }

    #[test]
    fn test_make_time_invalid_minutes() {
        assert!(make_time(0, 60, 0).is_err());
        assert!(make_time(0, -1, 0).is_err());
    }

    #[test]
    fn test_make_time_invalid_seconds() {
        // 60 seconds (unscaled: 60_000_000)
        assert!(make_time(0, 0, 60_000_000).is_err());
        // 100.5 seconds (unscaled: 100_500_000)
        assert!(make_time(0, 0, 100_500_000).is_err());
        // negative seconds (unscaled: -1_000_000)
        assert!(make_time(0, 0, -1_000_000).is_err());
    }

    #[test]
    fn test_make_time_overflow_seconds() {
        // Very large value that overflows i32
        let large = (i32::MAX as i128 + 1) * MICROS_PER_SECOND;
        assert!(make_time(0, 0, large).is_err());
    }
}
