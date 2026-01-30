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

        // Convert scalars to arrays for uniform processing
        let end_arr = end_date.into_array(1)?;
        let start_arr = start_date.into_array(1)?;

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

        // Date32 stores days since epoch, so difference is just subtraction
        let result: Int32Array =
            binary(end_date_array, start_date_array, |end, start| end - start)?;

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
