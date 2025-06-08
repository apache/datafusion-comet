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

#![allow(deprecated)]

use arrow::compute::{like_dyn, like_utf8_scalar_dyn};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue::Utf8;
use datafusion::common::{exec_err, internal_datafusion_err, Result};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct SparkLike {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkLike {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkLike {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkLike {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "like"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 2] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("like expects exactly two arguments"))?;
        let array = match args {
            // array (op) scalar
            [ColumnarValue::Array(array), ColumnarValue::Scalar(Utf8(Some(string)))] => {
                like_utf8_scalar_dyn(&array, string.as_str())
            }
            [ColumnarValue::Array(_), ColumnarValue::Scalar(other)] => {
                return exec_err!("Should be String but got: {:?}", other)
            }
            // array (op) array
            [ColumnarValue::Array(array1), ColumnarValue::Array(array2)] => {
                like_dyn(&array1, &array2)
            }
            _ => return exec_err!("Predicate on two literals should be folded at Spark"),
        }?;
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}
