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

use arrow::compute::cast_with_options;
use arrow::datatypes::DataType;
use datafusion::common::{format::DEFAULT_CAST_OPTIONS, utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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
            ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(cast_with_options(
                arr.as_ref(),
                &DataType::Date32,
                &DEFAULT_CAST_OPTIONS,
            )?)),
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar(scalar.cast_to(&DataType::Date32)?))
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
