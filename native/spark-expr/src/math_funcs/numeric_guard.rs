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

use arrow::array::{Array, ArrowNativeTypeOp, BooleanArray};
use arrow::compute::nullif;
use arrow::datatypes::{DataType, Schema};
use arrow::downcast_primitive_array;
use datafusion::common::{exec_err, utils::take_function_args, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::{
    expressions::{lit, Literal},
    PhysicalExpr, ScalarFunctionExpr,
};
use std::sync::{Arc, LazyLock};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) enum NumericGuard {
    NullIfZero,
    NullIfNonPositive,
}

impl NumericGuard {
    // Compare the native values so both signed zeros match and NaNs pass through.
    fn invalid<T: ArrowNativeTypeOp>(&self, value: T) -> bool {
        match self {
            Self::NullIfZero => value == T::ZERO,
            Self::NullIfNonPositive => value <= T::ZERO,
        }
    }

    fn mask(&self, input: &dyn Array) -> Result<BooleanArray> {
        downcast_primitive_array!(
            input => Ok(BooleanArray::from_unary(input, |x| self.invalid(x))),
            other => exec_err!("{} expected a numeric argument, got {other}", self.name()),
        )
    }

    fn scalar(&self, value: &ScalarValue) -> Result<ScalarValue> {
        if self.mask(value.to_array()?.as_ref())?.has_true() {
            ScalarValue::try_new_null(&value.data_type())
        } else {
            Ok(value.clone())
        }
    }
}

impl ScalarUDFImpl for NumericGuard {
    fn name(&self) -> &str {
        match self {
            Self::NullIfZero => "spark_null_if_zero",
            Self::NullIfNonPositive => "spark_null_if_non_positive",
        }
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::numeric(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = take_function_args(self.name(), arg_types)?;
        Ok(data_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), &args.args)?;
        match value {
            ColumnarValue::Scalar(scalar) => self.scalar(scalar).map(ColumnarValue::Scalar),
            ColumnarValue::Array(array) => {
                let mask = self.mask(array.as_ref())?;
                if mask.has_true() {
                    Ok(ColumnarValue::Array(nullif(array.as_ref(), &mask)?))
                } else {
                    Ok(value.clone())
                }
            }
        }
    }
}

pub fn create_null_if_zero_expr(
    expression: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if !expression.data_type(input_schema)?.is_numeric() {
        return Ok(expression);
    }
    let guard = NumericGuard::NullIfZero;
    if let Some(literal) = expression.downcast_ref::<Literal>() {
        return Ok(lit(guard.scalar(literal.value())?));
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::new_from_impl(guard)),
        vec![expression],
        input_schema,
        Arc::default(),
    )?))
}
