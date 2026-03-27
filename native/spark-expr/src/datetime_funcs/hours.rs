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

//! Spark-compatible `hours` V2 partition transform.
//!
//! Computes the number of hours since the Unix epoch (1970-01-01 00:00:00 UTC).
//!
//! - For `Timestamp(Microsecond, Some(tz))`: applies timezone offset before computing.
//! - For `Timestamp(Microsecond, None)` (NTZ): uses raw microseconds directly.

use arrow::array::cast::as_primitive_array;
use arrow::array::types::TimestampMicrosecondType;
use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, TimeUnit::Microsecond};
use arrow::temporal_conversions::as_datetime;
use chrono::{Offset, TimeZone};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::{any::Any, fmt::Debug, sync::Arc};

use crate::timezone::Tz;

const MICROS_PER_HOUR: i64 = 3_600_000_000;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHoursTransform {
    signature: Signature,
    timezone: String,
}

impl SparkHoursTransform {
    pub fn new(timezone: String) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            timezone,
        }
    }
}

impl ScalarUDFImpl for SparkHoursTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hours_transform"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args: [ColumnarValue; 1] = args.args.try_into().map_err(|_| {
            internal_datafusion_err!("hours_transform expects exactly one argument")
        })?;

        match args {
            [ColumnarValue::Array(array)] => {
                let ts_array = as_primitive_array::<TimestampMicrosecondType>(&array);
                let result: Int32Array = match array.data_type() {
                    DataType::Timestamp(Microsecond, Some(_)) => {
                        let tz: Tz = self.timezone.parse().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to parse timezone '{}': {}",
                                self.timezone, e
                            ))
                        })?;
                        arrow::compute::kernels::arity::try_unary(ts_array, |micros| {
                            let dt = as_datetime::<TimestampMicrosecondType>(micros)
                                .ok_or_else(|| {
                                    DataFusionError::Execution(format!(
                                        "Cannot convert {micros} to datetime"
                                    ))
                                })?;
                            let offset_secs = tz.offset_from_utc_datetime(&dt).fix().local_minus_utc() as i64;
                            let local_micros = micros + offset_secs * 1_000_000;
                            Ok(local_micros.div_euclid(MICROS_PER_HOUR) as i32)
                        })?
                    }
                    DataType::Timestamp(Microsecond, None) => {
                        arrow::compute::kernels::arity::unary(ts_array, |micros| {
                            micros.div_euclid(MICROS_PER_HOUR) as i32
                        })
                    }
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "hours_transform does not support input type: {:?}",
                            other
                        )));
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "hours_transform(scalar) should be folded on Spark JVM side.".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn test_hours_transform_utc() {
        let udf = SparkHoursTransform::new("UTC".to_string());
        // 2023-10-01 14:30:00 UTC = 1696171800 seconds = 1696171800000000 micros
        // Expected hours since epoch = 1696171800000000 / 3600000000 = 471158
        let ts = TimestampMicrosecondArray::from(vec![Some(1_696_171_800_000_000i64)])
            .with_timezone("UTC");
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 471158);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_ntz() {
        let udf = SparkHoursTransform::new("UTC".to_string());
        // Same timestamp but NTZ (no timezone on array)
        let ts = TimestampMicrosecondArray::from(vec![Some(1_696_171_800_000_000i64)]);
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 471158);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_negative_epoch() {
        let udf = SparkHoursTransform::new("UTC".to_string());
        // 1969-12-31 23:30:00 UTC = -1800 seconds = -1800000000 micros
        // Expected: div_euclid(-1800000000, 3600000000) = -1
        let ts =
            TimestampMicrosecondArray::from(vec![Some(-1_800_000_000i64)]).with_timezone("UTC");
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), -1);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_null() {
        let udf = SparkHoursTransform::new("UTC".to_string());
        let ts = TimestampMicrosecondArray::from(vec![None as Option<i64>]).with_timezone("UTC");
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert!(int_arr.is_null(0));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_epoch_zero() {
        let udf = SparkHoursTransform::new("UTC".to_string());
        let ts = TimestampMicrosecondArray::from(vec![Some(0i64)]).with_timezone("UTC");
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 0);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_non_utc_timezone() {
        // Asia/Tokyo is UTC+9. For a UTC timestamp of 1970-01-01 00:00:00 UTC (micros=0),
        // local time = 1970-01-01 09:00:00 JST, so local hours since epoch = 9.
        let udf = SparkHoursTransform::new("Asia/Tokyo".to_string());
        let ts = TimestampMicrosecondArray::from(vec![Some(0i64)]).with_timezone("Asia/Tokyo");
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 9);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_ntz_ignores_timezone() {
        // NTZ with micros=0 should always return 0, regardless of the timezone
        // string stored in the UDF (proving the NTZ path ignores timezone).
        let udf = SparkHoursTransform::new("Asia/Tokyo".to_string());
        let ts = TimestampMicrosecondArray::from(vec![Some(0i64)]); // No timezone on array
        let return_field = Arc::new(Field::new("hours_transform", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(ts))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 0); // NOT 9, because NTZ ignores timezone
            }
            _ => panic!("Expected array"),
        }
    }
}
