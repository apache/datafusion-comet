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
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_spark::function::string::concat_ws::SparkConcatWs;

/// Adapt SparkConcatWs to runtime scalars, including non-foldable scalar subqueries.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct CometConcatWs(SparkConcatWs);

impl ScalarUDFImpl for CometConcatWs {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.0.return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.0.coerce_types(arg_types)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("concat_ws requires a separator");
        }
        let all_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        // The upstream kernel expands scalars to one row, independently of number_rows.
        // Evaluate once and let DataFusion broadcast the scalar to the enclosing batch.
        if all_scalar {
            args.number_rows = 1;
        }
        match (all_scalar, self.0.invoke_with_args(args)?) {
            (true, ColumnarValue::Array(array)) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&array, 0)?,
            )),
            (_, result) => Ok(result),
        }
    }
}
