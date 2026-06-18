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

use arrow::array::{ArrayRef, OffsetSizeTrait, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::cast::{as_generic_string_array, as_string_view_array};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark `Empty2Null`: returns NULL if the string is NULL or empty (`""`),
/// otherwise returns the string itself. Used in partitioned writes to ensure
/// that an empty string ends up in `__HIVE_DEFAULT_PARTITION__`, just like NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkEmpty2Null {
    signature: Signature,
}

impl Default for SparkEmpty2Null {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkEmpty2Null {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkEmpty2Null {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "empty2Null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_empty_to_null(&args.args)
    }
}

fn spark_empty_to_null(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return exec_err!("spark empty2Null takes exactly one argument");
    }
    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = spark_empty_to_null_array(&array)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let result = spark_empty_to_null_scalar(&scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

fn spark_empty_to_null_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => spark_empty_to_null_string_array::<i32>(array),
        DataType::LargeUtf8 => spark_empty_to_null_string_array::<i64>(array),
        DataType::Utf8View => spark_empty_to_null_string_view(array),
        other => {
            exec_err!("unsupported data type {other:?} for function `empty2Null`")
        }
    }
}

fn spark_empty_to_null_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(v) => Ok(ScalarValue::Utf8(v.clone().filter(|s| !s.is_empty()))),
        ScalarValue::LargeUtf8(v) => {
            Ok(ScalarValue::LargeUtf8(v.clone().filter(|s| !s.is_empty())))
        }
        ScalarValue::Utf8View(v) => Ok(ScalarValue::Utf8View(v.clone().filter(|s| !s.is_empty()))),
        other => {
            exec_err!("unsupported data type {other:?} for function `empty2Null`")
        }
    }
}

fn spark_empty_to_null_string_array<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<O>(array)?;
    let result = str_array
        .iter()
        .map(|opt| opt.filter(|s| !s.is_empty()))
        .collect::<StringArray>();
    Ok(Arc::new(result))
}

fn spark_empty_to_null_string_view(str_view: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_string_view_array(str_view)?;
    let result = str_array
        .iter()
        .map(|opt| opt.filter(|s| !s.is_empty()))
        .collect::<StringArray>();
    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};

    #[test]
    fn test_empty_to_null_utf8() {
        let input = Arc::new(StringArray::from(vec![
            Some("a"),
            Some(""),
            None,
            Some("b"),
        ])) as ArrayRef;
        let result = spark_empty_to_null_array(&input).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "a");
        assert!(result.is_null(1)); // "" -> null
        assert!(result.is_null(2)); // null -> null
        assert_eq!(result.value(3), "b");
    }
}

