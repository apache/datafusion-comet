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
//! Both `TimestampType` and `TimestampNTZType` are computationally identical. They
//! extract the absolute hours since the epoch by directly dividing the microsecond
//! value by the number of microseconds in an hour, ignoring session timezone offsets.

use arrow::array::cast::as_primitive_array;
use arrow::array::types::TimestampMicrosecondType;
use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, TimeUnit::Microsecond};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::integer::div_floor;
use std::{any::Any, fmt::Debug, sync::Arc};

const MICROS_PER_HOUR: i64 = 3_600_000_000;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHoursTransform {
    signature: Signature,
}

impl SparkHoursTransform {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SparkHoursTransform {
    fn default() -> Self {
        Self::new()
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
                let result: Int32Array = match array.data_type() {
                    DataType::Timestamp(Microsecond, _) => {
                        let ts_array = as_primitive_array::<TimestampMicrosecondType>(&array);
                        arrow::compute::kernels::arity::unary(ts_array, |micros| {
                            div_floor(micros, MICROS_PER_HOUR) as i32
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
        let udf = SparkHoursTransform::new();
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
        let udf = SparkHoursTransform::new();
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
        let udf = SparkHoursTransform::new();
        // 1969-12-31 23:30:00 UTC = -1800 seconds = -1800000000 micros
        // Expected: floor_div(-1800000000, 3600000000) = -1
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
        let udf = SparkHoursTransform::new();
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
        let udf = SparkHoursTransform::new();
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
        // Spark's Hours partition transform evaluates absolute hours since epoch. Thus, a UTC
        // timestamp of 1970-01-01 00:00:00 UTC (micros=0) maps to 0 hours, even if the
        // timestamp array itself contains timezone metadata like Asia/Tokyo.
        let udf = SparkHoursTransform::new();
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
                assert_eq!(int_arr.value(0), 0);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_hours_transform_ntz_ignores_timezone() {
        // NTZ with micros=0 always returns 0 because NTZ is pure wall-clock time.
        // There is no timezone offset logic applied to either TimestampType or NTZ.
        let udf = SparkHoursTransform::new();
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
