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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{
        DataType,
        DataType::{Int64, Utf8},
    },
};

use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{cast::as_int64_array, exec_err, DataFusionError, Result, ScalarValue};

/// Returns the ASCII character having the binary equivalent to the input expression.
/// E.g., chr(65) = 'A'.
/// Compatible with Apache Spark's Chr function
pub fn spark_chr(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let chr_func = ChrFunc::default();
    chr_func.invoke(args)
}

pub fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let integer_array = as_int64_array(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = integer_array
        .iter()
        .map(|integer: Option<i64>| {
            integer
                .map(|integer| match core::char::from_u32(integer as u32) {
                    Some(integer) => Ok(integer.to_string()),
                    None => {
                        exec_err!("requested character too large for encoding.")
                    }
                })
                .transpose()
        })
        .collect::<Result<StringArray>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

#[derive(Debug)]
pub struct ChrFunc {
    signature: Signature,
}

impl Default for ChrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ChrFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ChrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "chr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        handle_chr_fn(args)
    }
}

fn handle_chr_fn(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = args[0].clone();
    match array {
        ColumnarValue::Array(array) => {
            let array = chr(&[array])?;
            Ok(ColumnarValue::Array(array))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
            match core::char::from_u32(value as u32) {
                Some(ch) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(ch.to_string())))),
                None => exec_err!("requested character too large for encoding."),
            }
        }
        ColumnarValue::Scalar(ScalarValue::Int64(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        _ => exec_err!("The argument must be an Int64 array or scalar."),
    }
}
