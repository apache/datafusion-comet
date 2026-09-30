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

use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
use arrow::compute::try_unary;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{utils::take_function_args, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

const MICROS_PER_SECOND: i64 = 1_000_000;

/// The result is Spark's `TimestampType`, which is `Timestamp(Microsecond, "UTC")` everywhere in a
/// native plan. Without the label, downstream kernels would read the result as `TimestampNTZType`
/// wall-clock time, and Arrow would reject comparing it with other timestamps.
const TIMEZONE: &str = "UTC";

/// Spark-compatible seconds_to_timestamp (timestamp_seconds) function.
/// Converts seconds since Unix epoch to a timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSecondsToTimestamp {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkSecondsToTimestamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Float32]),
                    TypeSignature::Exact(vec![DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["timestamp_seconds".to_string()],
        }
    }
}

impl Default for SparkSecondsToTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkSecondsToTimestamp {
    fn name(&self) -> &str {
        "seconds_to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(TIMEZONE.into()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [seconds] = take_function_args(self.name(), args.args)?;

        match seconds {
            ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(Arc::new(
                seconds_to_micros(arr.as_ref())?.with_timezone(TIMEZONE),
            ))),
            ColumnarValue::Scalar(scalar) => {
                let ts_micros = match &scalar {
                    ScalarValue::Int32(Some(s)) => Some((*s as i64) * MICROS_PER_SECOND),
                    ScalarValue::Int64(Some(s)) => {
                        Some(s.checked_mul(MICROS_PER_SECOND).ok_or_else(|| {
                            DataFusionError::ArrowError(
                                Box::new(arrow::error::ArrowError::ComputeError(
                                    "long overflow".to_string(),
                                )),
                                None,
                            )
                        })?)
                    }
                    ScalarValue::Float32(Some(s)) => {
                        let s = *s as f64;
                        if s.is_nan() || s.is_infinite() {
                            None
                        } else {
                            Some((s * (MICROS_PER_SECOND as f64)) as i64)
                        }
                    }
                    ScalarValue::Float64(Some(s)) => {
                        if s.is_nan() || s.is_infinite() {
                            None
                        } else {
                            Some((s * (MICROS_PER_SECOND as f64)) as i64)
                        }
                    }
                    ScalarValue::Int32(None)
                    | ScalarValue::Int64(None)
                    | ScalarValue::Float32(None)
                    | ScalarValue::Float64(None)
                    | ScalarValue::Null => None,
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "seconds_to_timestamp expects numeric scalar input, got {:?}",
                            scalar.data_type()
                        )))
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    ts_micros,
                    Some(TIMEZONE.into()),
                )))
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Converts an array of seconds to microseconds. The caller labels the result with [`TIMEZONE`].
fn seconds_to_micros(arr: &dyn Array) -> Result<TimestampMicrosecondArray> {
    // Handle Int32 input — no overflow possible since i32 * 1_000_000 fits in i64
    if let Some(int_array) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(try_unary(
            int_array,
            |s| Ok((s as i64) * MICROS_PER_SECOND),
        )?);
    }

    // Handle Int64 input — error on overflow to match Spark's Math.multiplyExact
    if let Some(int_array) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(try_unary(int_array, |s| {
            s.checked_mul(MICROS_PER_SECOND)
                .ok_or_else(|| arrow::error::ArrowError::ComputeError("long overflow".to_string()))
        })?);
    }

    // Handle Float32 input — cast to f64 and use Float64 path
    if let Some(float_array) = arr.as_any().downcast_ref::<Float32Array>() {
        return Ok(float_array
            .iter()
            .map(|opt| {
                opt.and_then(|s| {
                    let s = s as f64;
                    if s.is_nan() || s.is_infinite() {
                        None
                    } else {
                        Some((s * (MICROS_PER_SECOND as f64)) as i64)
                    }
                })
            })
            .collect());
    }

    // Handle Float64 input — NaN and Infinity return null per Spark behavior
    if let Some(float_array) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok(float_array
            .iter()
            .map(|opt| {
                opt.and_then(|s| {
                    if s.is_nan() || s.is_infinite() {
                        None
                    } else {
                        Some((s * (MICROS_PER_SECOND as f64)) as i64)
                    }
                })
            })
            .collect());
    }

    Err(DataFusionError::Execution(format!(
        "seconds_to_timestamp expects Int32, Int64, Float32 or Float64 input, got {:?}",
        arr.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, AsArray};
    use arrow::datatypes::{Field, TimestampMicrosecondType};
    use datafusion::config::ConfigOptions;

    fn utc_timestamp() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    fn invoke(arg: ColumnarValue, number_rows: usize) -> ColumnarValue {
        let udf = SparkSecondsToTimestamp::new();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![arg],
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("result", utc_timestamp(), true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    }

    /// Every input type yields the Arrow type `return_type` declares.
    #[test]
    fn arrays_are_labelled_utc() {
        let udf = SparkSecondsToTimestamp::new();
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1800), None])),
            Arc::new(Int64Array::from(vec![Some(1800), None])),
            Arc::new(Float32Array::from(vec![Some(1800.0), None])),
            Arc::new(Float64Array::from(vec![Some(1800.0), None])),
        ];
        for input in inputs {
            let input_type = input.data_type().clone();
            let declared = udf.return_type(std::slice::from_ref(&input_type)).unwrap();
            assert_eq!(declared, utc_timestamp(), "{input_type}");
            let ColumnarValue::Array(result) = invoke(ColumnarValue::Array(input), 2) else {
                panic!("expected an array for {input_type}");
            };
            assert_eq!(result.data_type(), &declared, "{input_type}");
            let result = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(result.value(0), 1_800_000_000, "{input_type}");
            assert!(result.is_null(1), "{input_type}");
        }
    }

    #[test]
    fn scalars_are_labelled_utc() {
        for (input, expected) in [
            (ScalarValue::Int32(Some(1800)), Some(1_800_000_000)),
            (ScalarValue::Int64(Some(1800)), Some(1_800_000_000)),
            (ScalarValue::Float64(Some(1800.5)), Some(1_800_500_000)),
            (ScalarValue::Int64(None), None),
        ] {
            let ColumnarValue::Scalar(result) = invoke(ColumnarValue::Scalar(input.clone()), 1)
            else {
                panic!("expected a scalar for {input:?}");
            };
            assert_eq!(
                result,
                ScalarValue::TimestampMicrosecond(expected, Some("UTC".into())),
                "{input:?}"
            );
        }
    }
}
