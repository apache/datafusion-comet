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

use crate::utils::array_with_timezone;
use arrow::array::{Array, AsArray, PrimitiveArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int64Type, TimeUnit::Microsecond};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::integer::div_floor;
use std::{any::Any, fmt::Debug, sync::Arc};

const MICROS_PER_SECOND: i64 = 1_000_000;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixTimestamp {
    signature: Signature,
    aliases: Vec<String>,
    timezone: String,
}

impl SparkUnixTimestamp {
    pub fn new(timezone: String) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
            timezone,
        }
    }
}

impl ScalarUDFImpl for SparkUnixTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unix_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Dictionary(_, _) => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64))
            }
            _ => DataType::Int64,
        })
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args: [ColumnarValue; 1] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("unix_timestamp expects exactly one argument"))?;

        match args {
            [ColumnarValue::Array(array)] => match array.data_type() {
                DataType::Timestamp(_, _) => {
                    let is_utc = self.timezone == "UTC";
                    let array = if is_utc
                        && matches!(array.data_type(), DataType::Timestamp(Microsecond, Some(tz)) if tz.as_ref() == "UTC")
                    {
                        array
                    } else {
                        array_with_timezone(
                            array,
                            self.timezone.clone(),
                            Some(&DataType::Timestamp(Microsecond, Some("UTC".into()))),
                        )?
                    };

                    let timestamp_array =
                        array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();

                    let result: PrimitiveArray<Int64Type> = if timestamp_array.null_count() == 0 {
                        timestamp_array
                            .values()
                            .iter()
                            .map(|&micros| micros / MICROS_PER_SECOND)
                            .collect()
                    } else {
                        timestamp_array
                            .iter()
                            .map(|v| v.map(|micros| div_floor(micros, MICROS_PER_SECOND)))
                            .collect()
                    };

                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                DataType::Date32 => {
                    let timestamp_array = cast(&array, &DataType::Timestamp(Microsecond, None))?;

                    let is_utc = self.timezone == "UTC";
                    let array = if is_utc {
                        timestamp_array
                    } else {
                        array_with_timezone(
                            timestamp_array,
                            self.timezone.clone(),
                            Some(&DataType::Timestamp(Microsecond, Some("UTC".into()))),
                        )?
                    };

                    let timestamp_array =
                        array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();

                    let result: PrimitiveArray<Int64Type> = if timestamp_array.null_count() == 0 {
                        timestamp_array
                            .values()
                            .iter()
                            .map(|&micros| micros / MICROS_PER_SECOND)
                            .collect()
                    } else {
                        timestamp_array
                            .iter()
                            .map(|v| v.map(|micros| div_floor(micros, MICROS_PER_SECOND)))
                            .collect()
                    };

                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                _ => Err(DataFusionError::Execution(format!(
                    "unix_timestamp does not support input type: {:?}",
                    array.data_type()
                ))),
            },
            _ => Err(DataFusionError::Execution(
                "unix_timestamp(scalar) should be fold in Spark JVM side.".to_string(),
            )),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, TimestampMicrosecondArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn test_unix_timestamp_from_timestamp() {
        // Test with known timestamp value
        // 2020-01-01 00:00:00 UTC = 1577836800 seconds = 1577836800000000 microseconds
        let input = TimestampMicrosecondArray::from(vec![Some(1577836800000000)]);
        let udf = SparkUnixTimestamp::new("UTC".to_string());

        let return_field = Arc::new(Field::new("unix_timestamp", DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };

        let result = udf.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let int64_array = result_array.as_primitive::<arrow::datatypes::Int64Type>();
            assert_eq!(int64_array.value(0), 1577836800);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_unix_timestamp_from_date() {
        // Test with Date32
        // Date32(18262) = 2020-01-01 = 1577836800 seconds
        let input = Date32Array::from(vec![Some(18262)]);
        let udf = SparkUnixTimestamp::new("UTC".to_string());

        let return_field = Arc::new(Field::new("unix_timestamp", DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };

        let result = udf.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let int64_array = result_array.as_primitive::<arrow::datatypes::Int64Type>();
            assert_eq!(int64_array.value(0), 1577836800);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_unix_timestamp_with_nulls() {
        let input = TimestampMicrosecondArray::from(vec![Some(1577836800000000), None]);
        let udf = SparkUnixTimestamp::new("UTC".to_string());

        let return_field = Arc::new(Field::new("unix_timestamp", DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            number_rows: 2,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };

        let result = udf.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let int64_array = result_array.as_primitive::<arrow::datatypes::Int64Type>();
            assert_eq!(int64_array.value(0), 1577836800);
            assert!(int64_array.is_null(1));
        } else {
            panic!("Expected array result");
        }
    }
}
