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

//! Converts Spark physical plan to DataFusion physical plan

use std::{str::FromStr, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    common::DataFusionError,
    logical_expr::{BuiltinScalarFunction, Operator as DataFusionOperator},
    physical_expr::{
        expressions::{BinaryExpr, Column, IsNotNullExpr, Literal as DataFusionLiteral},
        functions::create_physical_expr,
        PhysicalExpr, PhysicalSortExpr,
    },
    physical_plan::{
        aggregates::{AggregateMode as DFAggregateMode, PhysicalGroupBy},
        filter::FilterExec,
        limit::LocalLimitExec,
        projection::ProjectionExec,
        sorts::sort::SortExec,
        ExecutionPlan, Partitioning,
    },
};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::{
    execution_props::ExecutionProps,
    expressions::{
        CaseExpr, CastExpr, Count, InListExpr, IsNullExpr, Max, Min, NegativeExpr, NotExpr, Sum,
    },
    AggregateExpr, ScalarFunctionExpr,
};
use itertools::Itertools;
use num::{BigInt, ToPrimitive};

use crate::{
    errors::ExpressionError,
    execution::{
        datafusion::{
            expressions::{
                avg::Avg,
                avg_decimal::AvgDecimal,
                bitwise_not::BitwiseNotExpr,
                cast::Cast,
                checkoverflow::CheckOverflow,
                if_expr::IfExpr,
                scalar_funcs::create_comet_physical_fun,
                strings::{Contains, EndsWith, Like, StartsWith, StringSpaceExec, SubstringExec},
                subquery::Subquery,
                sum_decimal::SumDecimal,
                temporal::{DateTruncExec, HourExec, MinuteExec, SecondExec, TimestampTruncExec},
                NormalizeNaNAndZero,
            },
            operators::expand::CometExpandExec,
        },
        operators::{CopyExec, ExecutionError, InputBatch, ScanExec},
        serde::to_arrow_datatype,
        spark_expression,
        spark_expression::{
            agg_expr::ExprStruct as AggExprStruct, expr::ExprStruct, literal::Value, AggExpr, Expr,
            ScalarFunc,
        },
        spark_operator::{operator::OpStruct, Operator},
        spark_partitioning::{partitioning::PartitioningStruct, Partitioning as SparkPartitioning},
    },
};

// For clippy error on type_complexity.
type ExecResult<T> = Result<T, ExecutionError>;
type PhyAggResult = Result<Vec<Arc<dyn AggregateExpr>>, ExecutionError>;
type PhyExprResult = Result<Vec<(Arc<dyn PhysicalExpr>, String)>, ExecutionError>;
type PartitionPhyExprResult = Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError>;

/// The query planner for converting Spark query plans to DataFusion query plans.
pub struct PhysicalPlanner {
    // The execution context id of this planner.
    exec_context_id: i64,
    execution_props: ExecutionProps,
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalPlanner {
    pub fn new() -> Self {
        let execution_props = ExecutionProps::new();
        Self {
            exec_context_id: -1,
            execution_props,
        }
    }

    pub fn with_exec_id(self, exec_context_id: i64) -> Self {
        Self {
            exec_context_id,
            execution_props: self.execution_props,
        }
    }

    /// Create a DataFusion physical expression from Spark physical expression
    fn create_expr(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::Add(expr) => self.create_binary_expr(
                expr.left.as_ref().unwrap(),
                expr.right.as_ref().unwrap(),
                expr.return_type.as_ref(),
                DataFusionOperator::Plus,
                input_schema,
            ),
            ExprStruct::Subtract(expr) => self.create_binary_expr(
                expr.left.as_ref().unwrap(),
                expr.right.as_ref().unwrap(),
                expr.return_type.as_ref(),
                DataFusionOperator::Minus,
                input_schema,
            ),
            ExprStruct::Multiply(expr) => self.create_binary_expr(
                expr.left.as_ref().unwrap(),
                expr.right.as_ref().unwrap(),
                expr.return_type.as_ref(),
                DataFusionOperator::Multiply,
                input_schema,
            ),
            ExprStruct::Divide(expr) => self.create_binary_expr(
                expr.left.as_ref().unwrap(),
                expr.right.as_ref().unwrap(),
                expr.return_type.as_ref(),
                DataFusionOperator::Divide,
                input_schema,
            ),
            ExprStruct::Remainder(expr) => self.create_binary_expr(
                expr.left.as_ref().unwrap(),
                expr.right.as_ref().unwrap(),
                expr.return_type.as_ref(),
                DataFusionOperator::Modulo,
                input_schema,
            ),
            ExprStruct::Eq(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Eq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Neq(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::NotEq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Gt(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Gt;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::GtEq(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::GtEq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Lt(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Lt;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::LtEq(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::LtEq;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Bound(bound) => {
                let idx = bound.index as usize;
                let column_name = format!("col_{}", idx);
                Ok(Arc::new(Column::new(&column_name, idx)))
            }
            ExprStruct::IsNotNull(is_notnull) => {
                let child = self.create_expr(is_notnull.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IsNotNullExpr::new(child)))
            }
            ExprStruct::IsNull(is_null) => {
                let child = self.create_expr(is_null.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IsNullExpr::new(child)))
            }
            ExprStruct::And(and) => {
                let left = self.create_expr(and.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(and.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::And;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Or(or) => {
                let left = self.create_expr(or.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(or.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::Or;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Literal(literal) => {
                let data_type = to_arrow_datatype(literal.datatype.as_ref().unwrap());
                let scalar_value = if literal.is_null {
                    match data_type {
                        DataType::Boolean => ScalarValue::Boolean(None),
                        DataType::Int8 => ScalarValue::Int8(None),
                        DataType::Int16 => ScalarValue::Int16(None),
                        DataType::Int32 => ScalarValue::Int32(None),
                        DataType::Int64 => ScalarValue::Int64(None),
                        DataType::Float32 => ScalarValue::Float32(None),
                        DataType::Float64 => ScalarValue::Float64(None),
                        DataType::Utf8 => ScalarValue::Utf8(None),
                        DataType::Date32 => ScalarValue::Date32(None),
                        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                            ScalarValue::TimestampMicrosecond(None, timezone)
                        }
                        DataType::Binary => ScalarValue::Binary(None),
                        DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s),
                        DataType::Null => ScalarValue::Null,
                        dt => {
                            return Err(ExecutionError::GeneralError(format!(
                                "{:?} is not supported in Comet",
                                dt
                            )))
                        }
                    }
                } else {
                    match literal.value.as_ref().unwrap() {
                        Value::BoolVal(value) => ScalarValue::Boolean(Some(*value)),
                        Value::ByteVal(value) => ScalarValue::Int8(Some(*value as i8)),
                        Value::ShortVal(value) => ScalarValue::Int16(Some(*value as i16)),
                        Value::IntVal(value) => match data_type {
                            DataType::Int32 => ScalarValue::Int32(Some(*value)),
                            DataType::Date32 => ScalarValue::Date32(Some(*value)),
                            dt => {
                                return Err(ExecutionError::GeneralError(format!(
                                    "Expected either 'Int32' or 'Date32' for IntVal, but found {:?}",
                                    dt
                                )))
                            }
                        },
                        Value::LongVal(value) => match data_type {
                            DataType::Int64 => ScalarValue::Int64(Some(*value)),
                            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                                ScalarValue::TimestampMicrosecond(Some(*value), None)
                            }
                            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                                ScalarValue::TimestampMicrosecond(Some(*value), Some(tz))
                            }
                            dt => {
                                return Err(ExecutionError::GeneralError(format!(
                                    "Expected either 'Int64' or 'Timestamp' for LongVal, but found {:?}",
                                    dt
                                )))
                            }
                        },
                        Value::FloatVal(value) => ScalarValue::Float32(Some(*value)),
                        Value::DoubleVal(value) => ScalarValue::Float64(Some(*value)),
                        Value::StringVal(value) => ScalarValue::Utf8(Some(value.clone())),
                        Value::BytesVal(value) => ScalarValue::Binary(Some(value.clone())),
                        Value::DecimalVal(value) => {
                            let big_integer = BigInt::from_signed_bytes_be(value);
                            let integer = big_integer.to_i128().ok_or_else(|| {
                                ExecutionError::GeneralError(format!(
                                    "Cannot parse {:?} as i128 for Decimal literal",
                                    big_integer
                                ))
                            })?;

                            match data_type {
                                DataType::Decimal128(p, s) => {
                                    ScalarValue::Decimal128(Some(integer), p, s)
                                }
                                dt => {
                                    return Err(ExecutionError::GeneralError(format!(
                                        "Decimal literal's data type should be Decimal128 but got {:?}",
                                        dt
                                    )))
                                }
                            }
                        }
                    }
                };
                Ok(Arc::new(DataFusionLiteral::new(scalar_value)))
            }
            ExprStruct::Cast(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let timezone = expr.timezone.clone();
                Ok(Arc::new(Cast::new(child, datatype, timezone)))
            }
            ExprStruct::Hour(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let timezone = expr.timezone.clone();

                Ok(Arc::new(HourExec::new(child, timezone)))
            }
            ExprStruct::Minute(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let timezone = expr.timezone.clone();

                Ok(Arc::new(MinuteExec::new(child, timezone)))
            }
            ExprStruct::Second(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let timezone = expr.timezone.clone();

                Ok(Arc::new(SecondExec::new(child, timezone)))
            }
            ExprStruct::TruncDate(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema.clone())?;
                let format = self.create_expr(expr.format.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(DateTruncExec::new(child, format)))
            }
            ExprStruct::TruncTimestamp(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema.clone())?;
                let format = self.create_expr(expr.format.as_ref().unwrap(), input_schema)?;
                let timezone = expr.timezone.clone();

                Ok(Arc::new(TimestampTruncExec::new(child, format, timezone)))
            }
            ExprStruct::Substring(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                // Spark Substring's start is 1-based when start > 0
                let start = expr.start - i32::from(expr.start > 0);
                let len = expr.len;

                Ok(Arc::new(SubstringExec::new(
                    child,
                    start as i64,
                    len as u64,
                )))
            }
            ExprStruct::StringSpace(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(StringSpaceExec::new(child)))
            }
            ExprStruct::Contains(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(Contains::new(left, right)))
            }
            ExprStruct::StartsWith(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(StartsWith::new(left, right)))
            }
            ExprStruct::EndsWith(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(EndsWith::new(left, right)))
            }
            ExprStruct::Like(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

                Ok(Arc::new(Like::new(left, right)))
            }
            ExprStruct::CheckOverflow(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let fail_on_error = expr.fail_on_error;

                Ok(Arc::new(CheckOverflow::new(
                    child,
                    data_type,
                    fail_on_error,
                )))
            }
            ExprStruct::ScalarFunc(expr) => self.create_scalar_function_expr(expr, input_schema),
            ExprStruct::EqNullSafe(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::IsNotDistinctFrom;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::NeqNullSafe(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::IsDistinctFrom;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseAnd(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseAnd;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseNot(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(BitwiseNotExpr::new(child)))
            }
            ExprStruct::BitwiseOr(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseOr;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseXor(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseXor;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseShiftRight(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseShiftRight;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::BitwiseShiftLeft(expr) => {
                let left = self.create_expr(expr.left.as_ref().unwrap(), input_schema.clone())?;
                let right = self.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                let op = DataFusionOperator::BitwiseShiftLeft;
                Ok(Arc::new(BinaryExpr::new(left, op, right)))
            }
            ExprStruct::Abs(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema.clone())?;
                let args = vec![child];
                let expr = create_physical_expr(
                    &BuiltinScalarFunction::Abs,
                    &args,
                    &input_schema,
                    &self.execution_props,
                )?;
                Ok(expr)
            }
            ExprStruct::CaseWhen(case_when) => {
                let when_then_pairs = case_when
                    .when
                    .iter()
                    .map(|x| self.create_expr(x, input_schema.clone()).unwrap())
                    .zip(
                        case_when
                            .then
                            .iter()
                            .map(|then| self.create_expr(then, input_schema.clone()).unwrap()),
                    )
                    .collect::<Vec<_>>();

                let else_phy_expr = match &case_when.else_expr {
                    None => None,
                    Some(_) => {
                        Some(self.create_expr(case_when.else_expr.as_ref().unwrap(), input_schema)?)
                    }
                };
                Ok(Arc::new(CaseExpr::try_new(
                    None,
                    when_then_pairs,
                    else_phy_expr,
                )?))
            }
            ExprStruct::In(expr) => {
                let value =
                    self.create_expr(expr.in_value.as_ref().unwrap(), input_schema.clone())?;
                let list = expr
                    .lists
                    .iter()
                    .map(|x| self.create_expr(x, input_schema.clone()).unwrap())
                    .collect::<Vec<_>>();
                Ok(Arc::new(InListExpr::new(value, list, expr.negated, None)))
            }
            ExprStruct::If(expr) => {
                let if_expr =
                    self.create_expr(expr.if_expr.as_ref().unwrap(), input_schema.clone())?;
                let true_expr =
                    self.create_expr(expr.true_expr.as_ref().unwrap(), input_schema.clone())?;
                let false_expr =
                    self.create_expr(expr.false_expr.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(IfExpr::new(if_expr, true_expr, false_expr)))
            }
            ExprStruct::Not(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(NotExpr::new(child)))
            }
            ExprStruct::Negative(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(NegativeExpr::new(child)))
            }
            ExprStruct::NormalizeNanAndZero(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(NormalizeNaNAndZero::new(data_type, child)))
            }
            ExprStruct::Subquery(expr) => {
                let id = expr.id;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(Subquery::new(self.exec_context_id, id, data_type)))
            }
            expr => Err(ExecutionError::GeneralError(format!(
                "Not implemented: {:?}",
                expr
            ))),
        }
    }

    /// Create a DataFusion physical sort expression from Spark physical expression
    fn create_sort_expr<'a>(
        &'a self,
        spark_expr: &'a Expr,
        input_schema: SchemaRef,
    ) -> Result<PhysicalSortExpr, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            ExprStruct::SortOrder(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                let descending = expr.direction == 1;
                let nulls_first = expr.null_ordering == 0;

                let options = SortOptions {
                    descending,
                    nulls_first,
                };

                Ok(PhysicalSortExpr {
                    expr: child,
                    options,
                })
            }
            expr => Err(ExecutionError::GeneralError(format!(
                "{:?} isn't a SortOrder",
                expr
            ))),
        }
    }

    fn create_binary_expr(
        &self,
        left: &Expr,
        right: &Expr,
        return_type: Option<&spark_expression::DataType>,
        op: DataFusionOperator,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let left = self.create_expr(left, input_schema.clone())?;
        let right = self.create_expr(right, input_schema.clone())?;
        match (
            op,
            left.data_type(&input_schema),
            right.data_type(&input_schema),
        ) {
            (
                DataFusionOperator::Plus
                | DataFusionOperator::Minus
                | DataFusionOperator::Multiply
                | DataFusionOperator::Modulo,
                Ok(DataType::Decimal128(p1, s1)),
                Ok(DataType::Decimal128(p2, s2)),
            ) => {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
                // For some Decimal128 operations, we need wider internal digits.
                // Cast left and right to Decimal256 and cast the result back to Decimal128
                let left = Arc::new(Cast::new_without_timezone(
                    left,
                    DataType::Decimal256(p1, s1),
                ));
                let right = Arc::new(Cast::new_without_timezone(
                    right,
                    DataType::Decimal256(p2, s2),
                ));
                let child = Arc::new(BinaryExpr::new(left, op, right));
                Ok(Arc::new(Cast::new_without_timezone(child, data_type)))
            }
            (
                DataFusionOperator::Divide,
                Ok(DataType::Decimal128(_p1, _s1)),
                Ok(DataType::Decimal128(_p2, _s2)),
            ) => {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
                let fun_expr = create_comet_physical_fun(
                    "decimal_div",
                    &self.execution_props,
                    data_type.clone(),
                )?;
                Ok(Arc::new(ScalarFunctionExpr::new(
                    "decimal_div",
                    fun_expr,
                    vec![left, right],
                    data_type,
                    None,
                )))
            }
            _ => Ok(Arc::new(BinaryExpr::new(left, op, right))),
        }
    }

    /// Create a DataFusion physical plan from Spark physical plan.
    ///
    /// Note that we need `input_batches` parameter because we need to know the exact schema (not
    /// only data type but also dictionary-encoding) at `ScanExec`s. It is because some DataFusion
    /// operators, e.g., `ProjectionExec`, gets child operator schema during initialization and
    /// uses it later for `RecordBatch`. We may be able to get rid of it once `RecordBatch`
    /// relaxes schema check.
    ///
    /// Note that we return created `Scan`s which will be kept at JNI API. JNI calls will use it to
    /// feed in new input batch from Spark JVM side.
    pub fn create_plan<'a>(
        &'a self,
        spark_plan: &'a Operator,
        input_batches: &mut Vec<InputBatch>,
    ) -> Result<(Vec<ScanExec>, Arc<dyn ExecutionPlan>), ExecutionError> {
        let children = &spark_plan.children;
        match spark_plan.op_struct.as_ref().unwrap() {
            OpStruct::Projection(project) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], input_batches)?;
                let exprs: PhyExprResult = project
                    .project_list
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, child.schema())
                            .map(|r| (r, format!("col_{}", idx)))
                    })
                    .collect();
                Ok((scans, Arc::new(ProjectionExec::try_new(exprs?, child)?)))
            }
            OpStruct::Filter(filter) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], input_batches)?;
                let predicate =
                    self.create_expr(filter.predicate.as_ref().unwrap(), child.schema())?;

                Ok((scans, Arc::new(FilterExec::try_new(predicate, child)?)))
            }
            OpStruct::HashAgg(agg) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], input_batches)?;

                let group_exprs: PhyExprResult = agg
                    .grouping_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, child.schema())
                            .map(|r| (r, format!("col_{}", idx)))
                    })
                    .collect();
                let group_by = PhysicalGroupBy::new_single(group_exprs?);
                let schema = child.schema();

                let mode = if agg.mode == 0 {
                    DFAggregateMode::Partial
                } else {
                    DFAggregateMode::Final
                };

                let agg_exprs: PhyAggResult = agg
                    .agg_exprs
                    .iter()
                    .map(|expr| self.create_agg_expr(expr, schema.clone()))
                    .collect();

                let num_agg = agg.agg_exprs.len();
                let aggregate = Arc::new(
                    datafusion::physical_plan::aggregates::AggregateExec::try_new(
                        mode,
                        group_by,
                        agg_exprs?,
                        vec![None; num_agg], // no filter expressions
                        vec![None; num_agg], // no order by expressions
                        child.clone(),
                        schema.clone(),
                    )?,
                );
                let result_exprs: PhyExprResult = agg
                    .result_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, child.schema())
                            .map(|r| (r, format!("col_{}", idx)))
                    })
                    .collect();

                let exec: Arc<dyn ExecutionPlan> = if agg.result_exprs.is_empty() {
                    aggregate
                } else {
                    // For final aggregation, DF's hash aggregate exec doesn't support Spark's
                    // aggregate result expressions like `COUNT(col) + 1`, but instead relying
                    // on additional `ProjectionExec` to handle the case. Therefore, here we'll
                    // add a projection node on top of the aggregate node.
                    //
                    // Note that `result_exprs` should only be set for final aggregation on the
                    // Spark side.
                    Arc::new(ProjectionExec::try_new(result_exprs?, aggregate)?)
                };

                Ok((scans, exec))
            }
            OpStruct::Limit(limit) => {
                assert!(children.len() == 1);
                let num = limit.limit;
                let (scans, child) = self.create_plan(&children[0], input_batches)?;

                Ok((scans, Arc::new(LocalLimitExec::new(child, num as usize))))
            }
            OpStruct::Sort(sort) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], input_batches)?;

                let exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = sort
                    .sort_orders
                    .iter()
                    .map(|expr| self.create_sort_expr(expr, child.schema()))
                    .collect();

                let fetch = sort.fetch.map(|num| num as usize);

                let copy_exec = Arc::new(CopyExec::new(child));

                Ok((
                    scans,
                    Arc::new(SortExec::new(exprs?, copy_exec).with_fetch(fetch)),
                ))
            }
            OpStruct::Scan(scan) => {
                let fields = scan.fields.iter().map(to_arrow_datatype).collect_vec();
                if input_batches.is_empty() {
                    return Err(ExecutionError::GeneralError(
                        "No input batch for scan".to_string(),
                    ));
                }
                // Consumes the first input batch source for the scan
                let input_batch = input_batches.remove(0);

                // The `ScanExec` operator will take actual arrays from Spark during execution
                let scan = ScanExec::new(input_batch, fields);
                Ok((vec![scan.clone()], Arc::new(scan)))
            }
            OpStruct::Expand(expand) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], input_batches)?;

                let mut projections = vec![];
                let mut projection = vec![];

                expand.project_list.iter().try_for_each(|expr| {
                    let expr = self.create_expr(expr, child.schema())?;
                    projection.push(expr);

                    if projection.len() == expand.num_expr_per_project as usize {
                        projections.push(projection.clone());
                        projection = vec![];
                    }

                    Ok::<(), ExecutionError>(())
                })?;

                assert!(
                    !projections.is_empty(),
                    "Expand should have at least one projection"
                );

                let datatypes = projections[0]
                    .iter()
                    .map(|expr| expr.data_type(&child.schema()))
                    .collect::<Result<Vec<DataType>, _>>()?;
                let fields: Vec<Field> = datatypes
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| Field::new(format!("col_{}", idx), dt.clone(), true))
                    .collect();
                let schema = Arc::new(Schema::new(fields));

                Ok((
                    scans,
                    Arc::new(CometExpandExec::new(projections, child, schema)),
                ))
            }
        }
    }

    /// Create a DataFusion physical aggregate expression from Spark physical aggregate expression
    fn create_agg_expr(
        &self,
        spark_expr: &AggExpr,
        schema: SchemaRef,
    ) -> Result<Arc<dyn AggregateExpr>, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            AggExprStruct::Count(expr) => {
                let child = self.create_expr(&expr.children[0], schema)?;
                Ok(Arc::new(Count::new(child, "count", DataType::Int64)))
            }
            AggExprStruct::Min(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(Min::new(child, "min", datatype)))
            }
            AggExprStruct::Max(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(Max::new(child, "max", datatype)))
            }
            AggExprStruct::Sum(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema.clone())?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());

                match datatype {
                    DataType::Decimal128(_, _) => {
                        Ok(Arc::new(SumDecimal::new("sum", child, datatype)))
                    }
                    _ => {
                        // cast to the result data type of SUM if necessary, we should not expect
                        // a cast failure since it should have already been checked at Spark side
                        let child = Arc::new(CastExpr::new(child, datatype.clone(), None));
                        Ok(Arc::new(Sum::new(child, "sum", datatype)))
                    }
                }
            }
            AggExprStruct::Avg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema.clone())?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let input_datatype = to_arrow_datatype(expr.sum_datatype.as_ref().unwrap());
                match datatype {
                    DataType::Decimal128(_, _) => Ok(Arc::new(AvgDecimal::new(
                        child,
                        "avg",
                        datatype,
                        input_datatype,
                    ))),
                    _ => {
                        // cast to the result data type of AVG if the result data type is different
                        // from the input type, e.g. AVG(Int32). We should not expect a cast
                        // failure since it should have already been checked at Spark side.
                        let child = Arc::new(CastExpr::new(child, datatype.clone(), None));
                        Ok(Arc::new(Avg::new(child, "avg", datatype)))
                    }
                }
            }
        }
    }

    /// Create a DataFusion physical partitioning from Spark physical partitioning
    fn create_partitioning(
        &self,
        spark_partitioning: &SparkPartitioning,
        input_schema: SchemaRef,
    ) -> Result<Partitioning, ExecutionError> {
        match spark_partitioning.partitioning_struct.as_ref().unwrap() {
            PartitioningStruct::HashPartition(hash_partition) => {
                let exprs: PartitionPhyExprResult = hash_partition
                    .hash_expression
                    .iter()
                    .map(|x| self.create_expr(x, input_schema.clone()))
                    .collect();
                Ok(Partitioning::Hash(
                    exprs?,
                    hash_partition.num_partitions as usize,
                ))
            }
            PartitioningStruct::SinglePartition(_) => Ok(Partitioning::UnknownPartitioning(1)),
        }
    }

    fn create_scalar_function_expr(
        &self,
        expr: &ScalarFunc,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let args = expr
            .args
            .iter()
            .map(|x| self.create_expr(x, input_schema.clone()).unwrap())
            .collect::<Vec<_>>();

        let fun_name = &expr.func;
        let input_expr_types = args
            .iter()
            .map(|x| x.data_type(input_schema.as_ref()).unwrap())
            .collect::<Vec<_>>();
        let data_type = match expr.return_type.as_ref().map(to_arrow_datatype) {
            Some(t) => t,
            None => {
                // If no data type is provided from Spark, we'll use DF's return type from the
                // scalar function
                // Note this assumes the `fun_name` is a defined function in DF. Otherwise, it'll
                // throw error.
                let fun = &BuiltinScalarFunction::from_str(fun_name)?;
                fun.return_type(&input_expr_types)?
            }
        };
        let fun_expr =
            create_comet_physical_fun(fun_name, &self.execution_props, data_type.clone())?;

        let scalar_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            fun_name,
            fun_expr,
            args.to_vec(),
            data_type,
            None,
        ));

        Ok(scalar_expr)
    }
}

impl From<DataFusionError> for ExecutionError {
    fn from(value: DataFusionError) -> Self {
        ExecutionError::DataFusionError(value.to_string())
    }
}

impl From<ExecutionError> for DataFusionError {
    fn from(value: ExecutionError) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}

impl From<ExpressionError> for DataFusionError {
    fn from(value: ExpressionError) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, task::Poll};

    use futures::{poll, StreamExt};

    use arrow_array::{DictionaryArray, Int32Array, StringArray};
    use arrow_schema::DataType;
    use datafusion::{physical_plan::common::collect, prelude::SessionContext};
    use tokio::sync::mpsc;

    use crate::execution::{
        datafusion::planner::PhysicalPlanner,
        operators::InputBatch,
        spark_expression::{self, literal},
        spark_operator,
    };

    use spark_expression::expr::ExprStruct::*;
    use spark_operator::{operator::OpStruct, Operator};

    #[test]
    fn test_unpack_dictionary_primitive() {
        let op_scan = Operator {
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![spark_expression::DataType {
                    type_id: 3, // Int32
                    type_info: None,
                }],
            })),
        };

        let op = create_filter(op_scan, 3);
        let planner = PhysicalPlanner::new();
        let row_count = 100;

        // Create a dictionary array with 100 values, and use it as input to the execution.
        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = Int32Array::from(vec![0, 1, 2, 3]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);
        let mut input_batches = vec![input_batch];

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut input_batches).unwrap();

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            // Create a dictionary array with 100 values, and use it as input to the execution.
            let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
            let values = Int32Array::from(vec![0, 1, 2, 3]);
            let input_array = DictionaryArray::new(keys, Arc::new(values));
            let input_batch1 = InputBatch::Batch(vec![Arc::new(input_array)], row_count);
            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), row_count / 4);
                        // dictionary should be unpacked
                        assert!(matches!(batch.column(0).data_type(), DataType::Int32));
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    const STRING_TYPE_ID: i32 = 7;

    #[test]
    fn test_unpack_dictionary_string() {
        let op_scan = Operator {
            children: vec![],
            op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                fields: vec![spark_expression::DataType {
                    type_id: STRING_TYPE_ID, // String
                    type_info: None,
                }],
            })),
        };

        let lit = spark_expression::Literal {
            value: Some(literal::Value::StringVal("foo".to_string())),
            datatype: Some(spark_expression::DataType {
                type_id: STRING_TYPE_ID,
                type_info: None,
            }),
            is_null: false,
        };

        let op = create_filter_literal(op_scan, STRING_TYPE_ID, lit);
        let planner = PhysicalPlanner::new();

        let row_count = 100;

        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = StringArray::from(vec!["foo", "bar", "hello", "comet"]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);
        let mut input_batches = vec![input_batch];

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut input_batches).unwrap();

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = datafusion_plan.execute(0, task_ctx).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        // Separate thread to send the EOF signal once we've processed the only input batch
        runtime.spawn(async move {
            // Create a dictionary array with 100 values, and use it as input to the execution.
            let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
            let values = StringArray::from(vec!["foo", "bar", "hello", "comet"]);
            let input_array = DictionaryArray::new(keys, Arc::new(values));
            let input_batch1 = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

            let input_batch2 = InputBatch::EOF;

            let batches = vec![input_batch1, input_batch2];

            for batch in batches.into_iter() {
                tx.send(batch).await.unwrap();
            }
        });

        runtime.block_on(async move {
            loop {
                let batch = rx.recv().await.unwrap();
                scans[0].set_input_batch(batch);
                match poll!(stream.next()) {
                    Poll::Ready(Some(batch)) => {
                        assert!(batch.is_ok(), "got error {}", batch.unwrap_err());
                        let batch = batch.unwrap();
                        assert_eq!(batch.num_rows(), row_count / 4);
                        // string/binary should still be packed with dictionary
                        assert!(matches!(
                            batch.column(0).data_type(),
                            DataType::Dictionary(_, _)
                        ));
                    }
                    Poll::Ready(None) => {
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    #[tokio::test()]
    #[allow(clippy::field_reassign_with_default)]
    async fn to_datafusion_filter() {
        let op_scan = spark_operator::Operator {
            children: vec![],
            op_struct: Some(spark_operator::operator::OpStruct::Scan(
                spark_operator::Scan {
                    fields: vec![spark_expression::DataType {
                        type_id: 3,
                        type_info: None,
                    }],
                },
            )),
        };

        let op = create_filter(op_scan, 0);
        let planner = PhysicalPlanner::new();

        let mut input_batches = vec![InputBatch::EOF];
        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut input_batches).unwrap();

        let scan = &mut scans[0];
        scan.set_input_batch(InputBatch::EOF);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let stream = datafusion_plan.execute(0, task_ctx.clone()).unwrap();
        let output = collect(stream).await.unwrap();
        assert!(output.is_empty());
    }

    // Creates a filter operator which takes an `Int32Array` and selects rows that are equal to
    // `value`.
    fn create_filter(child_op: spark_operator::Operator, value: i32) -> spark_operator::Operator {
        let lit = spark_expression::Literal {
            value: Some(literal::Value::IntVal(value)),
            datatype: Some(spark_expression::DataType {
                type_id: 3,
                type_info: None,
            }),
            is_null: false,
        };

        create_filter_literal(child_op, 3, lit)
    }

    fn create_filter_literal(
        child_op: spark_operator::Operator,
        type_id: i32,
        lit: spark_expression::Literal,
    ) -> spark_operator::Operator {
        let mut expr = spark_expression::Expr::default();

        let mut left = spark_expression::Expr::default();
        left.expr_struct = Some(Bound(spark_expression::BoundReference {
            index: 0,
            datatype: Some(spark_expression::DataType {
                type_id,
                type_info: None,
            }),
        }));
        let mut right = spark_expression::Expr::default();
        right.expr_struct = Some(Literal(lit));

        expr.expr_struct = Some(Eq(Box::new(spark_expression::Equal {
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        })));

        let mut op = spark_operator::Operator::default();
        op.children = vec![child_op];
        op.op_struct = Some(OpStruct::Filter(spark_operator::Filter {
            predicate: Some(expr),
        }));
        op
    }
}
