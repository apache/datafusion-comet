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
use arrow::datatypes::DataType;
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible date_from_unix_date function.
/// Converts an integer representing days since Unix epoch (1970-01-01) to a Date32 value.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateFromUnixDate {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkDateFromUnixDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl Default for SparkDateFromUnixDate {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkDateFromUnixDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_from_unix_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [unix_date] = take_function_args(self.name(), args.args)?;
        match unix_date {
            ColumnarValue::Array(arr) => {
                let int_array = arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "date_from_unix_date expects Int32Array input".to_string(),
                        )
                    })?;

                // Date32 and Int32 both represent days since epoch, so we can directly
                // reinterpret the values. The only operation needed is creating a Date32Array
                // from the same underlying i32 values.
                let date_array = Date32Array::new(
                    int_array.values().clone(),
                    int_array.nulls().cloned(),
                );

                Ok(ColumnarValue::Array(Arc::new(date_array)))
            }
            ColumnarValue::Scalar(scalar) => {
                // Handle scalar case by converting to single-element array and back
                let arr = scalar.to_array()?;
                let int_array = arr.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "date_from_unix_date expects Int32 scalar input".to_string(),
                    )
                })?;

                let date_array =
                    Date32Array::new(int_array.values().clone(), int_array.nulls().cloned());

                Ok(ColumnarValue::Array(Arc::new(date_array)))
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
