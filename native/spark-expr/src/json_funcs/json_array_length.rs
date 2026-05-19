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

use std::any::Any;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::common::cast::as_string_array;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonArrayLength {
    signature: Signature,
}

impl Default for JsonArrayLength {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonArrayLength {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonArrayLength {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_json_array_length(&args.args)
    }
}

fn spark_json_array_length(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return exec_err!("json_array_length function takes exactly one argument");
    }
    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = spark_json_array_length_array(array)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let result = spark_json_array_length_scalar(scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

fn spark_json_array_length_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = as_string_array(array)?;
            Int32Array;
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function `json_array_length`")
        }
    }
}

fn spark_json_array_length_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    unimplemented!()
}
