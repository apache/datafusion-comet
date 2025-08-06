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

use crate::{create_comet_physical_fun, IfExpr};
use crate::{divide_by_zero_error, Cast, EvalMode, SparkCastOptions};
use arrow::compute::kernels::numeric::rem;
use arrow::datatypes::*;
use datafusion::common::{exec_err, internal_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::expressions::{lit, BinaryExpr};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr_common::datum::{apply, apply_cmp_for_nested};
use datafusion::{
    logical_expr::{ColumnarValue, Operator},
    physical_expr::PhysicalExpr,
};
use std::cmp::max;
use std::sync::Arc;

/// Spark-compliant modulo function. If `fail_on_error` is true, then this function computes modulo
/// in ANSI mode and returns an error on division by zero, otherwise it returns `NULL` for such
/// cases.
pub fn spark_modulo(args: &[ColumnarValue], fail_on_error: bool) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("modulo expects exactly two arguments");
    }

    let lhs = &args[0];
    let rhs = &args[1];

    let left_data_type = lhs.data_type();
    let right_data_type = rhs.data_type();

    if left_data_type.is_nested() {
        if right_data_type != left_data_type {
            return internal_err!("Type mismatch for spark modulo operation");
        }
        return apply_cmp_for_nested(Operator::Modulo, lhs, rhs);
    }

    match apply(lhs, rhs, rem) {
        Ok(result) => Ok(result),
        Err(e) if e.to_string().contains("Divide by zero") && fail_on_error => {
            // Return Spark-compliant divide by zero error.
            Err(divide_by_zero_error().into())
        }
        Err(e) => Err(e),
    }
}

pub fn create_modulo_expr(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    input_schema: SchemaRef,
    fail_on_error: bool,
    registry: &dyn FunctionRegistry,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    // For non-ANSI mode, wrap the right expression such that any zero value is replaced with `NULL`
    // to prevent divide by zero error.
    let right_non_ansi_safe = if !fail_on_error {
        null_if_zero_primitive(right, &input_schema)?
    } else {
        right
    };

    // If the data type is `Decimal128` and the (scale + integral part) exceeds the maximum allowed
    // for `Decimal128`, then cast both operands to `Decimal256` before creating the modulo scalar
    // expression, otherwise, create the modulo scalar expression directly.
    match (
        left.data_type(&input_schema),
        right_non_ansi_safe.data_type(&input_schema),
    ) {
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2)))
            if max(s1, s2) as u8 + max(p1 - s1 as u8, p2 - s2 as u8) > DECIMAL128_MAX_PRECISION =>
        {
            let left_256 = Arc::new(Cast::new(
                left,
                DataType::Decimal256(p1, s1),
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            ));
            let right_256 = Arc::new(Cast::new(
                right_non_ansi_safe,
                DataType::Decimal256(p2, s2),
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            ));

            let modulo_scalar_func = create_modulo_scalar_function(
                left_256,
                right_256,
                &data_type,
                registry,
                fail_on_error,
            )?;

            Ok(Arc::new(Cast::new(
                modulo_scalar_func,
                data_type,
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            )))
        }
        _ => create_modulo_scalar_function(
            left,
            right_non_ansi_safe,
            &data_type,
            registry,
            fail_on_error,
        ),
    }
}

fn null_if_zero_primitive(
    expression: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let expr_data_type = expression.data_type(input_schema)?;

    if is_primitive_datatype(&expr_data_type) {
        let zero = match expr_data_type {
            DataType::Int8 => ScalarValue::Int8(Some(0)),
            DataType::Int16 => ScalarValue::Int16(Some(0)),
            DataType::Int32 => ScalarValue::Int32(Some(0)),
            DataType::Int64 => ScalarValue::Int64(Some(0)),
            DataType::UInt8 => ScalarValue::UInt8(Some(0)),
            DataType::UInt16 => ScalarValue::UInt16(Some(0)),
            DataType::UInt32 => ScalarValue::UInt32(Some(0)),
            DataType::UInt64 => ScalarValue::UInt64(Some(0)),
            DataType::Float32 => ScalarValue::Float32(Some(0.0)),
            DataType::Float64 => ScalarValue::Float64(Some(0.0)),
            DataType::Decimal128(s, p) => ScalarValue::Decimal128(Some(0), s, p),
            DataType::Decimal256(s, p) => ScalarValue::Decimal256(Some(i256::from(0)), s, p),
            _ => return Ok(expression),
        };

        // Create an expression like - `if (eval(expr) == Literal(0)) then NULL else eval(expr)`.
        // This expression evaluates to null for rows with zero values to prevent divide by zero
        // error.
        let eq_expr = Arc::new(BinaryExpr::new(
            Arc::<dyn PhysicalExpr>::clone(&expression),
            Operator::Eq,
            lit(zero),
        ));
        let null_literal = lit(ScalarValue::try_new_null(&expr_data_type)?);
        let if_expr = Arc::new(IfExpr::new(eq_expr, null_literal, expression));
        Ok(if_expr)
    } else {
        Ok(expression)
    }
}

fn is_primitive_datatype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn create_modulo_scalar_function(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    data_type: &DataType,
    registry: &dyn FunctionRegistry,
    fail_on_error: bool,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let func_name = "spark_modulo";
    let modulo_expr =
        create_comet_physical_fun(func_name, data_type.clone(), registry, Some(fail_on_error))?;
    Ok(Arc::new(ScalarFunctionExpr::new(
        func_name,
        modulo_expr,
        vec![left, right],
        Arc::new(Field::new(func_name, data_type.clone(), true)),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, Decimal128Array, Decimal128Builder, Int32Array, PrimitiveArray,
        RecordBatch,
    };
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::prelude::SessionContext;

    fn with_fail_on_error<F: Fn(bool)>(test_fn: F) {
        for fail_on_error in [true, false] {
            test_fn(fail_on_error);
        }
    }

    pub fn verify_result<T>(
        expr: Arc<dyn PhysicalExpr>,
        batch: RecordBatch,
        should_fail: bool,
        expected_result: Option<Arc<PrimitiveArray<T>>>,
    ) where
        T: ArrowPrimitiveType,
    {
        let actual_result = expr.evaluate(&batch);

        if should_fail {
            match actual_result {
                Err(error) => {
                    assert!(
                        error
                            .to_string()
                            .contains("[DIVIDE_BY_ZERO] Division by zero"),
                        "Error message did not match. Actual message: {error}"
                    );
                }
                Ok(value) => {
                    panic!("Expected error, but got: {value:?}");
                }
            }
        } else {
            match (actual_result, expected_result) {
                (Ok(ColumnarValue::Array(ref actual)), Some(expected)) => {
                    assert_eq!(actual.len(), expected.len(), "Array length mismatch");

                    let actual_arr = actual.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                    let expected_arr = expected
                        .as_any()
                        .downcast_ref::<PrimitiveArray<T>>()
                        .unwrap();

                    for i in 0..actual_arr.len() {
                        assert_eq!(
                            actual_arr.is_null(i),
                            expected_arr.is_null(i),
                            "Nullity mismatch at index {i}"
                        );
                        if !actual_arr.is_null(i) {
                            let actual_value = actual_arr.value(i);
                            let expected_value = expected_arr.value(i);
                            assert_eq!(
                                actual_value, expected_value,
                                "Mismatch at index {i}, actual {actual_value:?}, expected {expected_value:?}"
                            );
                        }
                    }
                }
                (actual, expected) => {
                    panic!("Actual: {actual:?}, expected: {expected:?}");
                }
            }
        }
    }

    #[test]
    fn test_modulo_basic_int() {
        with_fail_on_error(|fail_on_error| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]));

            let a_array = Arc::new(Int32Array::from(vec![3, 2, i32::MIN]));
            let b_array = Arc::new(Int32Array::from(vec![1, 5, -1]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a_array, b_array]).unwrap();

            let left_expr = Arc::new(Column::new("a", 0));
            let right_expr = Arc::new(Column::new("b", 1));

            let session_ctx = SessionContext::new();
            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Int32,
                schema,
                fail_on_error,
                &session_ctx.state(),
            )
            .unwrap();

            // This test case should not fail as there is no division by zero.
            let should_fail = false;
            let expected_result = Arc::new(Int32Array::from(vec![0, 2, 0]));
            verify_result(modulo_expr, batch, should_fail, Some(expected_result));
        })
    }

    #[test]
    fn test_modulo_basic_decimal() {
        with_fail_on_error(|fail_on_error| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Decimal128(18, 4), false),
                Field::new("b", DataType::Decimal128(18, 4), false),
            ]));

            let mut a_builder =
                Decimal128Builder::with_capacity(2).with_data_type(DataType::Decimal128(18, 4));
            a_builder.append_value(3000000000000000000);
            a_builder.append_value(2000000000000000000);
            let a_array: ArrayRef = Arc::new(a_builder.finish());

            let mut b_builder =
                Decimal128Builder::with_capacity(2).with_data_type(DataType::Decimal128(18, 4));
            b_builder.append_value(1000000000000000000);
            b_builder.append_value(5000000000000000000);
            let b_array: ArrayRef = Arc::new(b_builder.finish());

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a_array, b_array]).unwrap();

            let left_expr = Arc::new(Column::new("a", 0));
            let right_expr = Arc::new(Column::new("b", 1));

            let session_ctx = SessionContext::new();
            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Decimal128(18, 4),
                schema,
                fail_on_error,
                &session_ctx.state(),
            )
            .unwrap();

            // This test case should not fail as there is no division by zero.
            let should_fail = false;
            let expected_result = Arc::new(Decimal128Array::from(vec![
                Some(0),
                Some(2000000000000000000),
            ]));
            verify_result(modulo_expr, batch, should_fail, Some(expected_result));
        })
    }

    #[test]
    fn test_modulo_divide_by_zero_int() {
        with_fail_on_error(|fail_on_error| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]));

            let a_array = Arc::new(Int32Array::from(vec![3]));
            let b_array = Arc::new(Int32Array::from(vec![0]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a_array, b_array]).unwrap();

            let left_expr = Arc::new(Column::new("a", 0));
            let right_expr = Arc::new(Column::new("b", 1));

            let session_ctx = SessionContext::new();
            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Int32,
                schema,
                fail_on_error,
                &session_ctx.state(),
            )
            .unwrap();

            // Expected result in non-ANSI mode.
            let expected_result = Arc::new(Int32Array::from(vec![None]));
            verify_result(modulo_expr, batch, fail_on_error, Some(expected_result));
        })
    }

    #[test]
    fn test_division_by_zero_with_complex_int_expr() {
        with_fail_on_error(|fail_on_error| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ]));

            let a_array = Arc::new(Int32Array::from(vec![3, 0]));
            let b_array = Arc::new(Int32Array::from(vec![2, 4]));
            let c_array = Arc::new(Int32Array::from(vec![4, 5]));
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![a_array, b_array, c_array]).unwrap();

            let left_expr = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Divide,
                Arc::new(Column::new("b", 1)),
            ));
            let right_expr = Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
                Operator::Divide,
                Arc::new(Column::new("c", 2)),
            ));

            // Computes modulo of (a / b) % (0 / c).
            let session_ctx = SessionContext::new();
            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Int32,
                schema,
                fail_on_error,
                &session_ctx.state(),
            )
            .unwrap();

            // Expected result in non-ANSI mode.
            let expected_result = Arc::new(Int32Array::from(vec![None, None]));
            verify_result(modulo_expr, batch, fail_on_error, Some(expected_result));
        })
    }
}
