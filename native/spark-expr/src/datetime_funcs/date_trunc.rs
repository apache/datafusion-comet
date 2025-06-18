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

use arrow::datatypes::DataType;
use datafusion::common::{utils::take_function_args, DataFusionError, Result, ScalarValue::Utf8};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;

use crate::kernels::temporal::{date_trunc_array_fmt_dyn, date_trunc_dyn};

#[derive(Debug)]
pub struct SparkDateTrunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
            aliases: vec![],
        }
    }
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date, format] = take_function_args(self.name(), args.args)?;
        match (date, format) {
            (ColumnarValue::Array(date), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = date_trunc_dyn(&date, format)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Array(date), ColumnarValue::Array(formats)) => {
                let result = date_trunc_array_fmt_dyn(&date, &formats)?;
                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid input to function DateTrunc. Expected (PrimitiveArray<Date32>, Scalar) or \
                    (PrimitiveArray<Date32>, StringArray)".to_string(),
            )),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
