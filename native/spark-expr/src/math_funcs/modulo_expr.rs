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
use crate::{remainder_by_zero_error, Cast, EvalMode, SparkCastOptions};
use arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use arrow::compute::kernels::arity::try_binary;
use arrow::compute::kernels::numeric::rem;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use datafusion::common::{
    exec_err, internal_err, not_impl_err, DataFusionError, Result, ScalarValue,
};
use datafusion::config::ConfigOptions;
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

    // Arrow's `rem` kernel only signals `DivideByZero` for integer and decimal types. For
    // floating point it uses `mod_wrapping`, so `x % 0.0` silently yields NaN. Under ANSI
    // mode we compute the float remainder with `mod_checked` instead, which reports
    // `DivideByZero` for a zero divisor.
    if fail_on_error
        && left_data_type == right_data_type
        && matches!(right_data_type, DataType::Float32 | DataType::Float64)
    {
        return checked_float_modulo(lhs, rhs, &right_data_type);
    }

    match apply(lhs, rhs, rem) {
        Ok(result) => Ok(result),
        Err(e) if e.to_string().contains("Divide by zero") && fail_on_error => {
            // Return Spark-compliant remainder by zero error.
            Err(remainder_by_zero_error().into())
        }
        Err(e) => Err(e),
    }
}

/// Computes floating point remainder in ANSI mode using `mod_checked`, which raises
/// `DivideByZero` when the divisor is zero. `-0.0` triggers the error too, since
/// `mod_checked` compares the divisor against zero with IEEE 754 equality — matching
/// Spark, which also treats `-0.0` as a zero divisor.
fn checked_float_modulo(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    data_type: &DataType,
) -> Result<ColumnarValue> {
    let (left, right): (ArrayRef, ArrayRef) = match (lhs, rhs) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };

    let result: ArrayRef = match data_type {
        DataType::Float32 => Arc::new(checked_modulo_kernel::<Float32Type>(&left, &right)?),
        DataType::Float64 => Arc::new(checked_modulo_kernel::<Float64Type>(&left, &right)?),
        _ => return not_impl_err!("checked_float_modulo doesn't support {data_type}"),
    };

    if matches!(
        (lhs, rhs),
        (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
    ) {
        // Preserve `apply`'s behavior of returning a scalar when both operands are scalars.
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

/// `try_binary` only invokes the closure for rows that are valid in both arrays, so rows
/// where either operand is null produce a null result without raising — matching Spark's
/// row-wise semantics.
fn checked_modulo_kernel<T: ArrowPrimitiveType>(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<PrimitiveArray<T>> {
    try_binary::<_, _, _, T>(lhs.as_primitive::<T>(), rhs.as_primitive::<T>(), |l, r| {
        l.mod_checked(r)
    })
    .map_err(|e| match e {
        ArrowError::DivideByZero => remainder_by_zero_error().into(),
        other => DataFusionError::from(other),
    })
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
                None,
                None,
            ));
            let right_256 = Arc::new(Cast::new(
                right_non_ansi_safe,
                DataType::Decimal256(p2, s2),
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
                None,
                None,
            ));

            // The UDF's return type must match what Arrow's rem function will actually return.
            // Since we're operating on Decimal256 inputs, rem will return Decimal256.
            let decimal256_return_type = match &data_type {
                DataType::Decimal128(p, s) => DataType::Decimal256(*p, *s),
                other => other.clone(),
            };
            let modulo_scalar_func = create_modulo_scalar_function(
                left_256,
                right_256,
                &decimal256_return_type,
                registry,
                fail_on_error,
            )?;

            Ok(Arc::new(Cast::new(
                modulo_scalar_func,
                data_type,
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
                None,
                None,
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
        Arc::new(ConfigOptions::default()),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, Decimal128Array, Decimal128Builder, Float32Array, Float64Array,
        Int32Array, PrimitiveArray, RecordBatch,
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
                            .contains("[REMAINDER_BY_ZERO] Remainder by zero"),
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
                            // `is_eq` uses arrow's total ordering, under which `NaN` equals
                            // `NaN`. Plain `==` would fail for the NaN results that a
                            // non-zero divisor produces from NaN or infinite dividends.
                            assert!(
                                actual_value.is_eq(expected_value),
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

    fn run_float_modulo<T: ArrowPrimitiveType>(
        data_type: DataType,
        lhs: ArrayRef,
        rhs: ArrayRef,
        fail_on_error: bool,
        should_fail: bool,
        expected: Option<Arc<PrimitiveArray<T>>>,
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", data_type.clone(), true),
            Field::new("b", data_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![lhs, rhs]).unwrap();

        let session_ctx = SessionContext::new();
        let modulo_expr = create_modulo_expr(
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            data_type,
            schema,
            fail_on_error,
            &session_ctx.state(),
        )
        .unwrap();

        verify_result(modulo_expr, batch, should_fail, expected);
    }

    #[test]
    fn test_modulo_divide_by_zero_float64_ansi() {
        // ANSI mode with a zero divisor must raise REMAINDER_BY_ZERO for Float64.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            Arc::new(Float64Array::from(vec![Some(0.0)])),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_divide_by_negative_zero_float64_ansi() {
        // `-0.0` is equal to `0.0` under IEEE 754, and Spark treats it as a zero divisor.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            Arc::new(Float64Array::from(vec![Some(-0.0)])),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_divide_by_zero_float32_ansi() {
        run_float_modulo::<Float32Type>(
            DataType::Float32,
            Arc::new(Float32Array::from(vec![Some(1.0_f32)])),
            Arc::new(Float32Array::from(vec![Some(0.0_f32)])),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_divide_by_zero_float64_non_ansi() {
        // Non-ANSI mode preserves the existing behavior: the divisor is nulled out and the
        // result is null.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            Arc::new(Float64Array::from(vec![Some(0.0)])),
            false,
            false,
            Some(Arc::new(Float64Array::from(vec![None]))),
        );
    }

    #[test]
    fn test_modulo_null_dividend_zero_divisor_float64_ansi() {
        // A null dividend must not raise even when the divisor is zero — Spark returns null
        // whenever either operand is null.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(Float64Array::from(vec![Some(0.0)])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![None]))),
        );
    }

    #[test]
    fn test_modulo_mixed_null_and_zero_divisor_float64_ansi() {
        // Row 0: null lhs paired with zero divisor -> null result, no error.
        // Row 1: non-null lhs paired with zero divisor -> must raise.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![None, Some(1.0)])),
            Arc::new(Float64Array::from(vec![Some(0.0), Some(0.0)])),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_all_null_lhs_zero_scalar_divisor_float64_ansi() {
        // Every lhs row is null, so the zero scalar divisor cannot pair with a non-null
        // dividend and no error should be raised.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![None, None])),
            Arc::new(Float64Array::from(vec![Some(0.0), Some(0.0)])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![None, None]))),
        );
    }

    /// Evaluates `a % <literal>` where `a` is a Float64 column, so `spark_modulo` receives
    /// `(Array, Scalar)` rather than the `(Array, Array)` pair that `run_float_modulo`
    /// produces.
    fn run_float_modulo_with_literal_divisor(
        lhs: ArrayRef,
        divisor: ScalarValue,
        fail_on_error: bool,
        should_fail: bool,
        expected: Option<Arc<Float64Array>>,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![lhs]).unwrap();

        let session_ctx = SessionContext::new();
        let modulo_expr = create_modulo_expr(
            Arc::new(Column::new("a", 0)),
            Arc::new(Literal::new(divisor)),
            DataType::Float64,
            schema,
            fail_on_error,
            &session_ctx.state(),
        )
        .unwrap();

        verify_result(modulo_expr, batch, should_fail, expected);
    }

    #[test]
    fn test_modulo_literal_zero_divisor_float64_ansi() {
        // `(Array, Scalar)` operands with a zero literal divisor must raise.
        run_float_modulo_with_literal_divisor(
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
            ScalarValue::Float64(Some(0.0)),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_literal_negative_zero_divisor_float64_ansi() {
        run_float_modulo_with_literal_divisor(
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            ScalarValue::Float64(Some(-0.0)),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_all_null_lhs_literal_zero_divisor_float64_ansi() {
        // Every dividend is null, so no row pairs a non-null dividend with the zero
        // literal divisor and no error should be raised.
        run_float_modulo_with_literal_divisor(
            Arc::new(Float64Array::from(vec![None, None])),
            ScalarValue::Float64(Some(0.0)),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![None, None]))),
        );
    }

    #[test]
    fn test_modulo_literal_non_zero_divisor_float64() {
        with_fail_on_error(|fail_on_error| {
            run_float_modulo_with_literal_divisor(
                Arc::new(Float64Array::from(vec![Some(5.0), None])),
                ScalarValue::Float64(Some(2.0)),
                fail_on_error,
                false,
                Some(Arc::new(Float64Array::from(vec![Some(1.0), None]))),
            );
        })
    }

    /// Evaluates `<literal> % b` where `b` is a Float64 column, covering the
    /// `(Scalar, Array)` operand pair.
    fn run_float_modulo_with_literal_dividend(
        dividend: ScalarValue,
        rhs: ArrayRef,
        fail_on_error: bool,
        should_fail: bool,
        expected: Option<Arc<Float64Array>>,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![rhs]).unwrap();

        let session_ctx = SessionContext::new();
        let modulo_expr = create_modulo_expr(
            Arc::new(Literal::new(dividend)),
            Arc::new(Column::new("b", 0)),
            DataType::Float64,
            schema,
            fail_on_error,
            &session_ctx.state(),
        )
        .unwrap();

        verify_result(modulo_expr, batch, should_fail, expected);
    }

    #[test]
    fn test_modulo_literal_dividend_zero_divisor_float64_ansi() {
        // `(Scalar, Array)` operands: row 1 pairs the non-null literal dividend with a
        // zero divisor and must raise.
        run_float_modulo_with_literal_dividend(
            ScalarValue::Float64(Some(1.0)),
            Arc::new(Float64Array::from(vec![Some(2.0), Some(0.0)])),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_null_literal_dividend_zero_divisor_float64_ansi() {
        // A null literal dividend must not raise even though the divisor is zero.
        run_float_modulo_with_literal_dividend(
            ScalarValue::Float64(None),
            Arc::new(Float64Array::from(vec![Some(0.0)])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![None]))),
        );
    }

    #[test]
    fn test_modulo_both_literals_zero_divisor_float64_ansi() {
        // `(Scalar, Scalar)` operands. Spark folds fully-literal expressions before
        // execution, so this pair only reaches `spark_modulo` via a plan built directly,
        // but the branch must still raise rather than return NaN.
        let session_ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef],
        )
        .unwrap();

        let modulo_expr = create_modulo_expr(
            Arc::new(Literal::new(ScalarValue::Float64(Some(1.0)))),
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.0)))),
            DataType::Float64,
            schema,
            true,
            &session_ctx.state(),
        )
        .unwrap();

        verify_result::<Float64Type>(modulo_expr, batch, true, None);
    }

    #[test]
    fn test_modulo_both_literals_non_zero_divisor_float64_ansi() {
        // `(Scalar, Scalar)` operands must still evaluate to a scalar, not a one-row array.
        let session_ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])) as ArrayRef],
        )
        .unwrap();

        let modulo_expr = create_modulo_expr(
            Arc::new(Literal::new(ScalarValue::Float64(Some(5.0)))),
            Arc::new(Literal::new(ScalarValue::Float64(Some(2.0)))),
            DataType::Float64,
            schema,
            true,
            &session_ctx.state(),
        )
        .unwrap();

        match modulo_expr.evaluate(&batch).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => assert_eq!(v, 1.0),
            other => panic!("Expected a Float64 scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_modulo_special_dividends_zero_divisor_float64_ansi() {
        // Spark's `DivModLike.eval` only inspects the divisor when deciding whether to
        // raise, so NaN, +/-Infinity and 0.0 dividends must all throw rather than
        // producing NaN.
        for dividend in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.0] {
            run_float_modulo::<Float64Type>(
                DataType::Float64,
                Arc::new(Float64Array::from(vec![Some(dividend)])),
                Arc::new(Float64Array::from(vec![Some(0.0)])),
                true,
                true,
                None,
            );
            // Same dividends against a literal zero divisor, i.e. the `Array/Scalar` pair.
            run_float_modulo_with_literal_divisor(
                Arc::new(Float64Array::from(vec![Some(dividend)])),
                ScalarValue::Float64(Some(0.0)),
                true,
                true,
                None,
            );
        }
    }

    #[test]
    fn test_modulo_special_dividends_non_zero_divisor_float64_ansi() {
        // The same dividends must not raise when the divisor is non-zero. `x % 2.0` is
        // NaN for NaN and for both infinities, and preserves the sign of a zero dividend.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(5.0),
            ])),
            Arc::new(Float64Array::from(vec![Some(2.0); 5])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::NAN),
                Some(f64::NAN),
                Some(0.0),
                Some(1.0),
            ]))),
        );
    }

    #[test]
    fn test_modulo_mixed_batch_float64_ansi() {
        // A batch mixing non-zero divisors, a zero divisor with a non-null dividend, and a
        // zero divisor with a null dividend must raise, because of the middle row.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(3.0),
                None,
                Some(5.0),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(2.0),
                Some(0.0),
                Some(0.0),
                Some(1.5),
            ])),
            true,
            true,
            None,
        );
    }

    #[test]
    fn test_modulo_mixed_batch_zero_divisors_only_null_dividends_float64_ansi() {
        // Every zero divisor in this batch pairs with a null dividend, so no row can raise
        // and the non-zero rows must still compute.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![
                Some(5.0),
                None,
                Some(7.0),
                None,
                Some(9.0),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(2.0),
                Some(0.0),
                Some(4.0),
                Some(-0.0),
                Some(2.0),
            ])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(3.0),
                None,
                Some(1.0),
            ]))),
        );
    }

    #[test]
    fn test_modulo_null_divisor_float64_ansi() {
        // A null divisor is not a zero divisor: Spark returns null without raising.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
            Arc::new(Float64Array::from(vec![None, Some(2.0)])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![None, Some(0.0)]))),
        );
    }

    #[test]
    fn test_modulo_nan_divisor_float64_ansi() {
        // A NaN divisor is not zero either, so `x % NaN` is NaN rather than an error.
        run_float_modulo::<Float64Type>(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            Arc::new(Float64Array::from(vec![Some(f64::NAN)])),
            true,
            false,
            Some(Arc::new(Float64Array::from(vec![Some(f64::NAN)]))),
        );
    }
}
