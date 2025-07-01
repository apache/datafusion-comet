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

use crate::IfExpr;
use crate::{divide_by_zero_error, Cast, EvalMode, SparkCastOptions};
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::binary::BinaryTypeCoercer;
use datafusion::logical_expr::sort_properties::ExprProperties;
use datafusion::logical_expr::statistics::Distribution::Gaussian;
use datafusion::logical_expr::statistics::{
    combine_gaussians, new_generic_from_binary_op, Distribution,
};
use datafusion::logical_expr_common::interval_arithmetic::apply_operator;
use datafusion::physical_expr::expressions::{lit, BinaryExpr};
use datafusion::physical_expr::intervals::cp_solver::propagate_arithmetic;
use datafusion::physical_expr_common::datum::{apply, apply_cmp_for_nested};
use datafusion::{
    logical_expr::{interval_arithmetic::Interval, ColumnarValue, Operator},
    physical_expr::PhysicalExpr,
};
use std::cmp::max;
use std::hash::Hash;
use std::{any::Any, sync::Arc};

#[derive(Debug, Eq)]
pub struct ModuloExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,

    // Specifies whether the modulo expression should fail on error. If true, the modulo expression
    // will be ANSI-compliant.
    fail_on_error: bool,
}

impl PartialEq for ModuloExpr {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left)
            && self.right.eq(&other.right)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}
impl Hash for ModuloExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
        self.fail_on_error.hash(state);
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

        // Create an expression: if (expression == 0) then null else expression.
        // This expression evaluates to null for rows with zero values to prevent divide by zero
        // error.
        let eq_expr = Arc::new(BinaryExpr::new(Arc::<dyn PhysicalExpr>::clone(&expression), Operator::Eq, lit(zero)));
        let null_literal = lit(ScalarValue::try_new_null(&expr_data_type)?);
        let exp = Arc::new(IfExpr::new(eq_expr, null_literal, expression));
        Ok(exp)
    } else {
        Ok(expression)
    }
}

pub fn create_modulo_expr(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    input_schema: SchemaRef,
    fail_on_error: bool,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    // For non-ANSI mode, wrap the right expression with null-if-zero logic
    let wrapped_right = if !fail_on_error {
        null_if_zero_primitive(right, &input_schema)?
    } else {
        right
    };

    // If the data type is Decimal128 and the precision/scale exceeds the maximum allowed for
    // Decimal256, then cast both operands to Decimal256 before creating the modulo expression,
    // otherwise, create the modulo expression directly.
    match (
        left.data_type(&input_schema),
        wrapped_right.data_type(&input_schema),
    ) {
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2)))
            if max(s1, s2) as u8 + max(p1 - s1 as u8, p2 - s2 as u8) > DECIMAL128_MAX_PRECISION =>
        {
            let left = Arc::new(Cast::new(
                left,
                DataType::Decimal256(p1, s1),
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            ));
            let right = Arc::new(Cast::new(
                wrapped_right,
                DataType::Decimal256(p2, s2),
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            ));
            let child = Arc::new(ModuloExpr::new(left, right, fail_on_error));
            Ok(Arc::new(Cast::new(
                child,
                data_type,
                SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            )))
        }
        _ => Ok(Arc::new(ModuloExpr::new(
            left,
            wrapped_right,
            fail_on_error,
        ))),
    }
}

impl ModuloExpr {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        fail_on_error: bool,
    ) -> Self {
        Self {
            left,
            right,
            fail_on_error,
        }
    }
}

impl std::fmt::Display for ModuloExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({})", self.left.as_ref())?;
        write!(f, " {} ", &Operator::Modulo)?;
        write!(f, "({})", self.right.as_ref())?;
        Ok(())
    }
}

impl PhysicalExpr for ModuloExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        BinaryTypeCoercer::new(
            &self.left.data_type(input_schema)?,
            &Operator::Modulo,
            &self.right.data_type(input_schema)?,
        )
        .get_result_type()
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        use arrow::compute::kernels::numeric::*;

        let lhs = self.left.evaluate(batch)?;
        let rhs = self.right.evaluate(batch)?;

        let left_data_type = lhs.data_type();
        let right_data_type = rhs.data_type();

        if left_data_type.is_nested() {
            if right_data_type != left_data_type {
                return internal_err!("type mismatch for modulo operation");
            }
            return apply_cmp_for_nested(Operator::Modulo, &lhs, &rhs);
        }

        match apply(&lhs, &rhs, rem) {
            Ok(result) => Ok(result),
            Err(e) if e.to_string().contains("Divide by zero") && self.fail_on_error => {
                // Return Spark-compliant divide by zero error.
                Err(divide_by_zero_error().into())
            }
            Err(e) => Err(e),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ModuloExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.fail_on_error,
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        let left_interval = children[0];
        let right_interval = children[1];
        apply_operator(&Operator::Modulo, left_interval, right_interval)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let left_interval = children[0];
        let right_interval = children[1];
        Ok(
            propagate_arithmetic(&Operator::Modulo, interval, left_interval, right_interval)?
                .map(|(left, right)| vec![left, right]),
        )
    }

    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        let (left, right) = (children[0], children[1]);

        if let (Gaussian(left), Gaussian(right)) = (left, right) {
            if let Some(result) = combine_gaussians(&Operator::Modulo, left, right)? {
                return Ok(Gaussian(result));
            }
        }

        // Fall back to an unknown distribution with only summary statistics.
        new_generic_from_binary_op(&Operator::Modulo, left, right)
    }

    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties::new_unknown())
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.left.as_ref().fmt_sql(f)?;
        write!(f, " {} ", &Operator::Modulo)?;
        self.right.as_ref().fmt_sql(f)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::physical_expr::expressions::{Column, Literal};

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
                        "Error message did not match. Actual message: {}",
                        error
                    );
                }
                Ok(value) => {
                    panic!("Expected error, but got: {:?}", value);
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
                            "Nullity mismatch at index {}",
                            i
                        );
                        if !actual_arr.is_null(i) {
                            let actual_value = actual_arr.value(i);
                            let expected_value = expected_arr.value(i);
                            assert_eq!(
                                actual_value, expected_value,
                                "Mismatch at index {}, actual {:?}, expected {:?}",
                                i, actual_value, expected_value
                            );
                        }
                    }
                }
                (actual, expected) => {
                    panic!("Actual: {:?}, expected: {:?}", actual, expected);
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
            let batch = RecordBatch::try_new(schema.clone(), vec![a_array, b_array]).unwrap();

            let left_expr = Arc::new(Column::new("a", 0));
            let right_expr = Arc::new(Column::new("b", 1));

            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Int32,
                schema,
                fail_on_error,
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

            let batch = RecordBatch::try_new(schema.clone(), vec![a_array, b_array]).unwrap();

            let left_expr = Arc::new(Column::new("a", 0));
            let right_expr = Arc::new(Column::new("b", 1));

            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Decimal128(18, 4),
                schema,
                fail_on_error,
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
            let batch = RecordBatch::try_new(schema.clone(), vec![a_array, b_array]).unwrap();

            let left_expr = Arc::new(Column::new("a", 0));
            let right_expr = Arc::new(Column::new("b", 1));

            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Int32,
                schema,
                fail_on_error,
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
                RecordBatch::try_new(schema.clone(), vec![a_array, b_array, c_array]).unwrap();

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
            let modulo_expr = create_modulo_expr(
                left_expr,
                right_expr,
                DataType::Int32,
                schema,
                fail_on_error,
            )
            .unwrap();

            // Expected result in non-ANSI mode.
            let expected_result = Arc::new(Int32Array::from(vec![None, None]));
            verify_result(modulo_expr, batch, fail_on_error, Some(expected_result));
        })
    }
}
