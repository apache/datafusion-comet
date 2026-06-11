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

use arrow::array::{Array, ArrayRef, Int32Builder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::cast::as_generic_string_array;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use std::any::Any;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::Deserializer;
use std::fmt;
use std::sync::Arc;

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
            signature: Signature::variadic(
                vec![DataType::Utf8, DataType::LargeUtf8],
                Volatility::Immutable,
            ),
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
        DataType::Utf8 => spark_json_array_length_array_inner::<i32>(array),
        DataType::LargeUtf8 => spark_json_array_length_array_inner::<i64>(array),
        other => {
            exec_err!("Unsupported data type {other:?} for function `json_array_length`")
        }
    }
}

fn spark_json_array_length_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(value) => spark_json_array_length_scalar_inner(value),
        ScalarValue::LargeUtf8(value) => spark_json_array_length_scalar_inner(value),
        other => {
            exec_err!("Unsupported data type {other:?} for function `json_array_length`")
        }
    }
}

fn spark_json_array_length_scalar_inner(json_str: &Option<String>) -> Result<ScalarValue> {
    let array_length = json_str
        .clone()
        .and_then(|json_str| get_json_array_length(&json_str));
    Ok(ScalarValue::Int32(array_length))
}

fn spark_json_array_length_array_inner<T: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<T>(array)?;
    let mut builder = Int32Builder::with_capacity(str_array.len());
    for row_idx in 0..str_array.len() {
        if str_array.is_null(row_idx) {
            builder.append_null();
        } else {
            let json_str = str_array.value(row_idx);
            if let Some(json_array_length) = get_json_array_length(json_str) {
                builder.append_value(json_array_length);
            } else {
                builder.append_null()
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

struct ArrayItemCounter;

impl<'de> Visitor<'de> for ArrayItemCounter {
    type Value = i32;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut len = 0i32;
        while seq.next_element::<IgnoredAny>()?.is_some() {
            len += 1;
        }
        Ok(len)
    }
}

fn get_json_array_length(json: &str) -> Option<i32> {
    let mut deserializer = serde_json::Deserializer::from_str(json);
    deserializer.deserialize_seq(ArrayItemCounter).ok()
}
