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

use arrow::array::{Array, Date32Array, Int32Array};
use arrow::compute::kernels::arity::binary;
use arrow::datatypes::DataType;
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible date_diff function.
/// Returns the number of days from startDate to endDate (endDate - startDate).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateDiff {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkDateDiff {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Date32],
                Volatility::Immutable,
            ),
            aliases: vec!["datediff".to_string()],
        }
    }
}

impl Default for SparkDateDiff {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkDateDiff {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_diff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [end_date, start_date] = take_function_args(self.name(), args.args)?;

        // Determine the batch size from array arguments (scalars have no inherent size)
        let num_rows = [&end_date, &start_date]
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

        // Convert scalars to arrays for uniform processing, using the correct batch size
        let end_arr = end_date.into_array(num_rows)?;
        let start_arr = start_date.into_array(num_rows)?;

        let end_date_array = end_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("date_diff expects Date32Array for end_date".to_string())
            })?;

        let start_date_array = start_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "date_diff expects Date32Array for start_date".to_string(),
                )
            })?;

        // Date32 stores days since epoch, so difference is just subtraction. Use wrapping_sub
        // to match Spark, whose JVM int subtraction wraps on overflow; a plain `i32 -` would
        // panic in debug builds on extreme inputs.
        let result: Int32Array = binary(end_date_array, start_date_array, |end, start| {
            end.wrapping_sub(start)
        })?;

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn date_diff(end: i32, start: i32) -> i32 {
        let udf = SparkDateDiff::new();
        let return_field = Arc::new(Field::new("date_diff", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Date32Array::from(vec![Some(end)]))),
                ColumnarValue::Array(Arc::new(Date32Array::from(vec![Some(start)]))),
            ],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            _ => panic!("expected array result"),
        }
    }

    #[test]
    fn test_date_diff_basic() {
        // 2020-01-02 (18263) minus 2020-01-01 (18262) = 1 day
        assert_eq!(date_diff(18263, 18262), 1);
        assert_eq!(date_diff(18262, 18263), -1);
    }

    #[test]
    fn test_date_diff_wraps_on_overflow() {
        // Extreme inputs overflow i32; Spark's JVM int subtraction wraps rather than panicking.
        assert_eq!(
            date_diff(i32::MAX, i32::MIN),
            i32::MAX.wrapping_sub(i32::MIN)
        );
        assert_eq!(
            date_diff(i32::MIN, i32::MAX),
            i32::MIN.wrapping_sub(i32::MAX)
        );
    }
}
