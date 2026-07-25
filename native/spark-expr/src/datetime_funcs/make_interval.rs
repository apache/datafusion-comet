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
use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval as DataFusionMakeInterval;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    inner: DataFusionMakeInterval,
    fail_on_error: bool,
}

impl SparkMakeInterval {
    pub fn new(fail_on_error: bool) -> Self {
        Self {
            inner: DataFusionMakeInterval::new(),
            fail_on_error,
        }
    }
}

impl Default for SparkMakeInterval {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let inputs = if self.fail_on_error && !args.args.is_empty() {
            Some(ColumnarValue::values_to_arrays(&args.args)?)
        } else {
            None
        };
        let result = self.inner.invoke_with_args(args)?;

        if let Some(inputs) = inputs {
            let overflow = match &result {
                ColumnarValue::Array(values) => (0..values.len())
                    .any(|i| values.is_null(i) && inputs.iter().all(|a| !a.is_null(i))),
                ColumnarValue::Scalar(value) => {
                    value.is_null() && inputs.iter().all(|a| !a.is_null(0))
                }
            };
            if overflow {
                return Err(arithmetic_overflow_error("interval").into());
            }
        }

        Ok(result)
    }
}
