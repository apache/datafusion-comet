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

use arrow::array::{AsArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::{any::Any, sync::Arc};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStringReplace {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkStringReplace {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStringReplace {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkStringReplace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 3] = take_function_args(self.name(), args.args)?;
        spark_string_replace(&args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn spark_string_replace(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let src = arrays[0].as_string::<i32>();
    let search = arrays[1].as_string::<i32>();
    let replace = arrays[2].as_string::<i32>();

    let result: StringArray = src
        .iter()
        .zip(search.iter())
        .zip(replace.iter())
        .map(|((s, search), replace)| match (s, search, replace) {
            (Some(s), Some(search), Some(replace)) => {
                Some(spark_replace_string(s, search, replace))
            }
            _ => None,
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn spark_replace_string(src: &str, search: &str, replace: &str) -> String {
    if search.is_empty() {
        let mut result = String::with_capacity(src.len() + replace.len() * (src.len() + 1));
        result.push_str(replace);
        for c in src.chars() {
            result.push(c);
            result.push_str(replace);
        }
        result
    } else {
        src.replace(search, replace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use datafusion::common::ScalarValue;

    #[test]
    fn test_empty_search_string() {
        let src = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            None,
        ])));
        let search = ColumnarValue::Scalar(ScalarValue::Utf8(Some("".to_string())));
        let replace = ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".to_string())));

        match spark_string_replace(&[src, search, replace]) {
            Ok(ColumnarValue::Array(result)) => {
                let string_arr = result.as_string::<i32>();
                assert_eq!(string_arr.value(0), "xhxexlxlxox");
                assert_eq!(string_arr.value(1), "xwxoxrxlxdx");
                assert!(string_arr.is_null(2));
            }
            _ => unreachable!(),
        }
    }
}
