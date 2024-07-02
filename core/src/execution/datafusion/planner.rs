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

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef},
    common::DataFusionError,
    execution::FunctionRegistry,
    logical_expr::Operator as DataFusionOperator,
    physical_expr::{
        execution_props::ExecutionProps,
        expressions::{
            in_list, BinaryExpr, BitAnd, BitOr, BitXor, CaseExpr, CastExpr, Column, Count,
            FirstValue, IsNotNullExpr, IsNullExpr, LastValue, Literal as DataFusionLiteral, Max,
            Min, NotExpr, Sum,
        },
        AggregateExpr, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    },
    physical_optimizer::join_selection::swap_hash_join,
    physical_plan::{
        aggregates::{AggregateMode as DFAggregateMode, PhysicalGroupBy},
        filter::FilterExec,
        joins::{utils::JoinFilter, HashJoinExec, PartitionMode, SortMergeJoinExec},
        limit::LocalLimitExec,
        projection::ProjectionExec,
        sorts::sort::SortExec,
        ExecutionPlan, Partitioning,
    },
    prelude::SessionContext,
};
use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    JoinType as DFJoinType, ScalarValue,
};
use datafusion_expr::ScalarUDF;
use datafusion_physical_expr_common::aggregate::create_aggregate_expr;
use itertools::Itertools;
use jni::objects::GlobalRef;
use num::{BigInt, ToPrimitive};

use crate::{
    errors::ExpressionError,
    execution::{
        datafusion::{
            expressions::{
                avg::Avg,
                avg_decimal::AvgDecimal,
                bitwise_not::BitwiseNotExpr,
                bloom_filter_might_contain::BloomFilterMightContain,
                cast::Cast,
                checkoverflow::CheckOverflow,
                correlation::Correlation,
                covariance::Covariance,
                if_expr::IfExpr,
                negative,
                scalar_funcs::create_comet_physical_fun,
                stats::StatsType,
                stddev::Stddev,
                strings::{Contains, EndsWith, Like, StartsWith, StringSpaceExec, SubstringExec},
                subquery::Subquery,
                sum_decimal::SumDecimal,
                temporal::{DateTruncExec, HourExec, MinuteExec, SecondExec, TimestampTruncExec},
                unbound::UnboundColumn,
                variance::Variance,
                NormalizeNaNAndZero,
            },
            operators::expand::CometExpandExec,
            shuffle_writer::ShuffleWriterExec,
        },
        operators::{CopyExec, ExecutionError, ScanExec},
        serde::to_arrow_datatype,
        spark_expression,
        spark_expression::{
            agg_expr::ExprStruct as AggExprStruct, expr::ExprStruct, literal::Value, AggExpr, Expr,
            ScalarFunc,
        },
        spark_operator::{operator::OpStruct, BuildSide, JoinType, Operator},
        spark_partitioning::{partitioning::PartitioningStruct, Partitioning as SparkPartitioning},
    },
};

use super::expressions::{abs::CometAbsFunc, create_named_struct::CreateNamedStruct, EvalMode};

// For clippy error on type_complexity.
type ExecResult<T> = Result<T, ExecutionError>;
type PhyAggResult = Result<Vec<Arc<dyn AggregateExpr>>, ExecutionError>;
type PhyExprResult = Result<Vec<(Arc<dyn PhysicalExpr>, String)>, ExecutionError>;
type PartitionPhyExprResult = Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError>;

struct JoinParameters {
    pub left: Arc<dyn ExecutionPlan>,
    pub right: Arc<dyn ExecutionPlan>,
    pub join_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    pub join_filter: Option<JoinFilter>,
    pub join_type: DFJoinType,
}

pub const TEST_EXEC_CONTEXT_ID: i64 = -1;

/// The query planner for converting Spark query plans to DataFusion query plans.
pub struct PhysicalPlanner {
    // The execution context id of this planner.
    exec_context_id: i64,
    execution_props: ExecutionProps,
    session_ctx: Arc<SessionContext>,
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        let session_ctx = Arc::new(SessionContext::new());
        let execution_props = ExecutionProps::new();
        Self {
            exec_context_id: TEST_EXEC_CONTEXT_ID,
            execution_props,
            session_ctx,
        }
    }
}

impl PhysicalPlanner {
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        let execution_props = ExecutionProps::new();
        Self {
            exec_context_id: TEST_EXEC_CONTEXT_ID,
            execution_props,
            session_ctx,
        }
    }

    pub fn with_exec_id(self, exec_context_id: i64) -> Self {
        Self {
            exec_context_id,
            execution_props: self.execution_props,
            session_ctx: self.session_ctx.clone(),
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
                if idx >= input_schema.fields().len() {
                    return Err(ExecutionError::GeneralError(format!(
                        "Column index {} is out of bound. Schema: {}",
                        idx, input_schema
                    )));
                }
                let field = input_schema.field(idx);
                Ok(Arc::new(Column::new(field.name().as_str(), idx)))
            }
            ExprStruct::Unbound(unbound) => {
                let data_type = to_arrow_datatype(unbound.datatype.as_ref().unwrap());
                Ok(Arc::new(UnboundColumn::new(
                    unbound.name.as_str(),
                    data_type,
                )))
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
                let eval_mode = expr.eval_mode.try_into()?;

                Ok(Arc::new(Cast::new(child, datatype, eval_mode, timezone)))
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
                // substring negative len is treated as 0 in Spark
                let len = std::cmp::max(expr.len, 0);

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
                let return_type = child.data_type(&input_schema)?;
                let args = vec![child];
                let eval_mode = expr.eval_mode.try_into()?;
                let comet_abs = Arc::new(ScalarUDF::new_from_impl(CometAbsFunc::new(
                    eval_mode,
                    return_type.to_string(),
                )?));
                let expr = ScalarFunctionExpr::new("abs", comet_abs, args, return_type);
                Ok(Arc::new(expr))
            }
            ExprStruct::CaseWhen(case_when) => {
                let when_then_pairs = case_when
                    .when
                    .iter()
                    .map(|x| self.create_expr(x, input_schema.clone()))
                    .zip(
                        case_when
                            .then
                            .iter()
                            .map(|then| self.create_expr(then, input_schema.clone())),
                    )
                    .try_fold(Vec::new(), |mut acc, (a, b)| {
                        acc.push((a?, b?));
                        Ok::<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>, ExecutionError>(
                            acc,
                        )
                    })?;

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
                    .map(|x| self.create_expr(x, input_schema.clone()))
                    .collect::<Result<Vec<_>, _>>()?;

                in_list(value, list, &expr.negated, input_schema.as_ref()).map_err(|e| e.into())
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
            ExprStruct::UnaryMinus(expr) => {
                let child: Arc<dyn PhysicalExpr> =
                    self.create_expr(expr.child.as_ref().unwrap(), input_schema.clone())?;
                let result = negative::create_negate_expr(child, expr.fail_on_error);
                result.map_err(|e| ExecutionError::GeneralError(e.to_string()))
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
            ExprStruct::BloomFilterMightContain(expr) => {
                let bloom_filter_expr =
                    self.create_expr(expr.bloom_filter.as_ref().unwrap(), input_schema.clone())?;
                let value_expr = self.create_expr(expr.value.as_ref().unwrap(), input_schema)?;
                Ok(Arc::new(BloomFilterMightContain::try_new(
                    bloom_filter_expr,
                    value_expr,
                )?))
            }
            ExprStruct::CreateNamedStruct(expr) => {
                let values = expr
                    .values
                    .iter()
                    .map(|expr| self.create_expr(expr, input_schema.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(CreateNamedStruct::new(values, data_type)))
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
                    EvalMode::Legacy,
                ));
                let right = Arc::new(Cast::new_without_timezone(
                    right,
                    DataType::Decimal256(p2, s2),
                    EvalMode::Legacy,
                ));
                let child = Arc::new(BinaryExpr::new(left, op, right));
                Ok(Arc::new(Cast::new_without_timezone(
                    child,
                    data_type,
                    EvalMode::Legacy,
                )))
            }
            (
                DataFusionOperator::Divide,
                Ok(DataType::Decimal128(_p1, _s1)),
                Ok(DataType::Decimal128(_p2, _s2)),
            ) => {
                let data_type = return_type.map(to_arrow_datatype).unwrap();
                let fun_expr = create_comet_physical_fun(
                    "decimal_div",
                    data_type.clone(),
                    &self.session_ctx.state(),
                )?;
                Ok(Arc::new(ScalarFunctionExpr::new(
                    "decimal_div",
                    fun_expr,
                    vec![left, right],
                    data_type,
                )))
            }
            _ => Ok(Arc::new(BinaryExpr::new(left, op, right))),
        }
    }

    /// Create a DataFusion physical plan from Spark physical plan.
    ///
    /// `inputs` is a vector of input source IDs. It is used to create `ScanExec`s. Each `ScanExec`
    /// will be assigned a unique ID from `inputs` and the ID will be used to identify the input
    /// source at JNI API.
    ///
    /// Note that `ScanExec` will pull initial input batch during initialization. It is because we
    /// need to know the exact schema (not only data type but also dictionary-encoding) at
    /// `ScanExec`s. It is because some DataFusion operators, e.g., `ProjectionExec`, gets child
    /// operator schema during initialization and uses it later for `RecordBatch`. We may be
    /// able to get rid of it once `RecordBatch` relaxes schema check.
    ///
    /// Note that we return created `Scan`s which will be kept at JNI API. JNI calls will use it to
    /// feed in new input batch from Spark JVM side.
    pub fn create_plan<'a>(
        &'a self,
        spark_plan: &'a Operator,
        inputs: &mut Vec<Arc<GlobalRef>>,
    ) -> Result<(Vec<ScanExec>, Arc<dyn ExecutionPlan>), ExecutionError> {
        let children = &spark_plan.children;
        match spark_plan.op_struct.as_ref().unwrap() {
            OpStruct::Projection(project) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], inputs)?;
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
                let (scans, child) = self.create_plan(&children[0], inputs)?;
                let predicate =
                    self.create_expr(filter.predicate.as_ref().unwrap(), child.schema())?;

                Ok((scans, Arc::new(FilterExec::try_new(predicate, child)?)))
            }
            OpStruct::HashAgg(agg) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], inputs)?;

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
                        child.clone(),
                        schema.clone(),
                    )?,
                );
                let result_exprs: PhyExprResult = agg
                    .result_exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        self.create_expr(expr, aggregate.schema())
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
                let (scans, child) = self.create_plan(&children[0], inputs)?;

                Ok((scans, Arc::new(LocalLimitExec::new(child, num as usize))))
            }
            OpStruct::Sort(sort) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], inputs)?;

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

                // If it is not test execution context for unit test, we should have at least one
                // input source
                if self.exec_context_id != TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                    return Err(ExecutionError::GeneralError(
                        "No input for scan".to_string(),
                    ));
                }

                // Consumes the first input source for the scan
                let input_source =
                    if self.exec_context_id == TEST_EXEC_CONTEXT_ID && inputs.is_empty() {
                        // For unit test, we will set input batch to scan directly by `set_input_batch`.
                        None
                    } else {
                        Some(inputs.remove(0))
                    };

                // The `ScanExec` operator will take actual arrays from Spark during execution
                let scan = ScanExec::new(self.exec_context_id, input_source, fields)?;
                Ok((vec![scan.clone()], Arc::new(scan)))
            }
            OpStruct::ShuffleWriter(writer) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], inputs)?;

                let partitioning = self
                    .create_partitioning(writer.partitioning.as_ref().unwrap(), child.schema())?;

                Ok((
                    scans,
                    Arc::new(ShuffleWriterExec::try_new(
                        child,
                        partitioning,
                        writer.output_data_file.clone(),
                        writer.output_index_file.clone(),
                    )?),
                ))
            }
            OpStruct::Expand(expand) => {
                assert!(children.len() == 1);
                let (scans, child) = self.create_plan(&children[0], inputs)?;

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

                // `Expand` operator keeps the input batch and expands it to multiple output
                // batches. However, `ScanExec` will reuse input arrays for the next
                // input batch. Therefore, we need to copy the input batch to avoid
                // the data corruption. Note that we only need to copy the input batch
                // if the child operator is `ScanExec`, because other operators after `ScanExec`
                // will create new arrays for the output batch.
                let child = if child.as_any().downcast_ref::<ScanExec>().is_some() {
                    Arc::new(CopyExec::new(child))
                } else {
                    child
                };

                Ok((
                    scans,
                    Arc::new(CometExpandExec::new(projections, child, schema)),
                ))
            }
            OpStruct::SortMergeJoin(join) => {
                let (join_params, scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &join.left_join_keys,
                    &join.right_join_keys,
                    join.join_type,
                    &None,
                )?;

                let sort_options = join
                    .sort_options
                    .iter()
                    .map(|sort_option| {
                        let sort_expr = self
                            .create_sort_expr(sort_option, join_params.left.schema())
                            .unwrap();
                        SortOptions {
                            descending: sort_expr.options.descending,
                            nulls_first: sort_expr.options.nulls_first,
                        }
                    })
                    .collect();

                let join = Arc::new(SortMergeJoinExec::try_new(
                    join_params.left,
                    join_params.right,
                    join_params.join_on,
                    join_params.join_filter,
                    join_params.join_type,
                    sort_options,
                    // null doesn't equal to null in Spark join key. If the join key is
                    // `EqualNullSafe`, Spark will rewrite it during planning.
                    false,
                )?);

                Ok((scans, join))
            }
            OpStruct::HashJoin(join) => {
                let (join_params, scans) = self.parse_join_parameters(
                    inputs,
                    children,
                    &join.left_join_keys,
                    &join.right_join_keys,
                    join.join_type,
                    &join.condition,
                )?;
                let hash_join = Arc::new(HashJoinExec::try_new(
                    join_params.left,
                    join_params.right,
                    join_params.join_on,
                    join_params.join_filter,
                    &join_params.join_type,
                    None,
                    PartitionMode::Partitioned,
                    // null doesn't equal to null in Spark join key. If the join key is
                    // `EqualNullSafe`, Spark will rewrite it during planning.
                    false,
                )?);

                // If the hash join is build right, we need to swap the left and right
                let hash_join = if join.build_side == BuildSide::BuildLeft as i32 {
                    hash_join
                } else {
                    swap_hash_join(hash_join.as_ref(), PartitionMode::Partitioned)?
                };

                Ok((scans, hash_join))
            }
        }
    }

    fn parse_join_parameters(
        &self,
        inputs: &mut Vec<Arc<GlobalRef>>,
        children: &[Operator],
        left_join_keys: &[Expr],
        right_join_keys: &[Expr],
        join_type: i32,
        condition: &Option<Expr>,
    ) -> Result<(JoinParameters, Vec<ScanExec>), ExecutionError> {
        assert!(children.len() == 2);
        let (mut left_scans, left) = self.create_plan(&children[0], inputs)?;
        let (mut right_scans, right) = self.create_plan(&children[1], inputs)?;

        left_scans.append(&mut right_scans);

        let left_join_exprs: Vec<_> = left_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, left.schema()))
            .collect::<Result<Vec<_>, _>>()?;
        let right_join_exprs: Vec<_> = right_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, right.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        let join_on = left_join_exprs
            .into_iter()
            .zip(right_join_exprs)
            .collect::<Vec<_>>();

        let join_type = match join_type.try_into() {
            Ok(JoinType::Inner) => DFJoinType::Inner,
            Ok(JoinType::LeftOuter) => DFJoinType::Left,
            Ok(JoinType::RightOuter) => DFJoinType::Right,
            Ok(JoinType::FullOuter) => DFJoinType::Full,
            Ok(JoinType::LeftSemi) => DFJoinType::LeftSemi,
            Ok(JoinType::RightSemi) => DFJoinType::RightSemi,
            Ok(JoinType::LeftAnti) => DFJoinType::LeftAnti,
            Ok(JoinType::RightAnti) => DFJoinType::RightAnti,
            Err(_) => {
                return Err(ExecutionError::GeneralError(format!(
                    "Unsupported join type: {:?}",
                    join_type
                )));
            }
        };

        // Handle join filter as DataFusion `JoinFilter` struct
        let join_filter = if let Some(expr) = condition {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields();
            let right_fields = right_schema.fields();
            let all_fields: Vec<_> = left_fields
                .into_iter()
                .chain(right_fields)
                .cloned()
                .collect();
            let full_schema = Arc::new(Schema::new(all_fields));

            // Because we cast dictionary array to array in scan operator,
            // we need to change dictionary type to data type for join filter expression.
            let fields: Vec<_> = full_schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => Arc::new(Field::new(
                        f.name(),
                        val_type.as_ref().clone(),
                        f.is_nullable(),
                    )),
                    _ => f.clone(),
                })
                .collect();

            let full_schema = Arc::new(Schema::new(fields));

            let physical_expr = self.create_expr(expr, full_schema)?;
            let (left_field_indices, right_field_indices) =
                expr_to_columns(&physical_expr, left_fields.len(), right_fields.len())?;
            let column_indices = JoinFilter::build_column_indices(
                left_field_indices.clone(),
                right_field_indices.clone(),
            );

            let filter_fields: Vec<Field> = left_field_indices
                .clone()
                .into_iter()
                .map(|i| left.schema().field(i).clone())
                .chain(
                    right_field_indices
                        .clone()
                        .into_iter()
                        .map(|i| right.schema().field(i).clone()),
                )
                // Because we cast dictionary array to array in scan operator,
                // we need to change dictionary type to data type for join filter expression.
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => {
                        Field::new(f.name(), val_type.as_ref().clone(), f.is_nullable())
                    }
                    _ => f.clone(),
                })
                .collect_vec();

            let filter_schema = Schema::new_with_metadata(filter_fields, HashMap::new());

            // Rewrite the physical expression to use the new column indices.
            // DataFusion's join filter is bound to intermediate schema which contains
            // only the fields used in the filter expression. But the Spark's join filter
            // expression is bound to the full schema. We need to rewrite the physical
            // expression to use the new column indices.
            let rewritten_physical_expr = rewrite_physical_expr(
                physical_expr,
                left_schema.fields.len(),
                right_schema.fields.len(),
                &left_field_indices,
                &right_field_indices,
            )?;

            Some(JoinFilter::new(
                rewritten_physical_expr,
                column_indices,
                filter_schema,
            ))
        } else {
            None
        };

        // DataFusion Join operators keep the input batch internally. We need
        // to copy the input batch to avoid the data corruption from reusing the input
        // batch.
        let left = if can_reuse_input_batch(&left) {
            Arc::new(CopyExec::new(left))
        } else {
            left
        };

        let right = if can_reuse_input_batch(&right) {
            Arc::new(CopyExec::new(right))
        } else {
            right
        };

        Ok((
            JoinParameters {
                left,
                right,
                join_on,
                join_type,
                join_filter,
            },
            left_scans,
        ))
    }

    /// Create a DataFusion physical aggregate expression from Spark physical aggregate expression
    fn create_agg_expr(
        &self,
        spark_expr: &AggExpr,
        schema: SchemaRef,
    ) -> Result<Arc<dyn AggregateExpr>, ExecutionError> {
        match spark_expr.expr_struct.as_ref().unwrap() {
            AggExprStruct::Count(expr) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, schema.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(Count::new_with_multiple_exprs(
                    children,
                    "count",
                    DataType::Int64,
                )))
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
            AggExprStruct::First(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema.clone())?;
                let func = datafusion_expr::AggregateUDF::new_from_impl(FirstValue::new());

                create_aggregate_expr(&func, &[child], &[], &[], &schema, "first", false, false)
                    .map_err(|e| e.into())
            }
            AggExprStruct::Last(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema.clone())?;
                let func = datafusion_expr::AggregateUDF::new_from_impl(LastValue::new());

                create_aggregate_expr(&func, &[child], &[], &[], &schema, "last", false, false)
                    .map_err(|e| e.into())
            }
            AggExprStruct::BitAndAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(BitAnd::new(child, "bit_and", datatype)))
            }
            AggExprStruct::BitOrAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(BitOr::new(child, "bit_or", datatype)))
            }
            AggExprStruct::BitXorAgg(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema)?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(BitXor::new(child, "bit_xor", datatype)))
            }
            AggExprStruct::Covariance(expr) => {
                let child1 = self.create_expr(expr.child1.as_ref().unwrap(), schema.clone())?;
                let child2 = self.create_expr(expr.child2.as_ref().unwrap(), schema.clone())?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => Ok(Arc::new(Covariance::new(
                        child1,
                        child2,
                        "covariance",
                        datatype,
                        StatsType::Sample,
                        expr.null_on_divide_by_zero,
                    ))),
                    1 => Ok(Arc::new(Covariance::new(
                        child1,
                        child2,
                        "covariance_pop",
                        datatype,
                        StatsType::Population,
                        expr.null_on_divide_by_zero,
                    ))),
                    stats_type => Err(ExecutionError::GeneralError(format!(
                        "Unknown StatisticsType {:?} for Variance",
                        stats_type
                    ))),
                }
            }
            AggExprStruct::Variance(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema.clone())?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => Ok(Arc::new(Variance::new(
                        child,
                        "variance",
                        datatype,
                        StatsType::Sample,
                        expr.null_on_divide_by_zero,
                    ))),
                    1 => Ok(Arc::new(Variance::new(
                        child,
                        "variance_pop",
                        datatype,
                        StatsType::Population,
                        expr.null_on_divide_by_zero,
                    ))),
                    stats_type => Err(ExecutionError::GeneralError(format!(
                        "Unknown StatisticsType {:?} for Variance",
                        stats_type
                    ))),
                }
            }
            AggExprStruct::Stddev(expr) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), schema.clone())?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match expr.stats_type {
                    0 => Ok(Arc::new(Stddev::new(
                        child,
                        "stddev",
                        datatype,
                        StatsType::Sample,
                        expr.null_on_divide_by_zero,
                    ))),
                    1 => Ok(Arc::new(Stddev::new(
                        child,
                        "stddev_pop",
                        datatype,
                        StatsType::Population,
                        expr.null_on_divide_by_zero,
                    ))),
                    stats_type => Err(ExecutionError::GeneralError(format!(
                        "Unknown StatisticsType {:?} for stddev",
                        stats_type
                    ))),
                }
            }
            AggExprStruct::Correlation(expr) => {
                let child1 = self.create_expr(expr.child1.as_ref().unwrap(), schema.clone())?;
                let child2 = self.create_expr(expr.child2.as_ref().unwrap(), schema.clone())?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                Ok(Arc::new(Correlation::new(
                    child1,
                    child2,
                    "correlation",
                    datatype,
                    expr.null_on_divide_by_zero,
                )))
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
            .map(|x| self.create_expr(x, input_schema.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let fun_name = &expr.func;
        let input_expr_types = args
            .iter()
            .map(|x| x.data_type(input_schema.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        let data_type = match expr.return_type.as_ref().map(to_arrow_datatype) {
            Some(t) => t,
            None => self
                .session_ctx
                .udf(fun_name)?
                .inner()
                .return_type(&input_expr_types)?,
        };

        let fun_expr =
            create_comet_physical_fun(fun_name, data_type.clone(), &self.session_ctx.state())?;

        let scalar_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            fun_name,
            fun_expr,
            args.to_vec(),
            data_type,
        ));

        Ok(scalar_expr)
    }
}

impl From<DataFusionError> for ExecutionError {
    fn from(value: DataFusionError) -> Self {
        ExecutionError::DataFusionError(value.message().to_string())
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

/// Returns true if given operator can return input array as output array without
/// modification. This is used to determine if we need to copy the input batch to avoid
/// data corruption from reusing the input batch.
fn can_reuse_input_batch(op: &Arc<dyn ExecutionPlan>) -> bool {
    op.as_any().downcast_ref::<ScanExec>().is_some()
        || op.as_any().downcast_ref::<LocalLimitExec>().is_some()
        || op.as_any().downcast_ref::<ProjectionExec>().is_some()
        || op.as_any().downcast_ref::<FilterExec>().is_some()
}

/// Collects the indices of the columns in the input schema that are used in the expression
/// and returns them as a pair of vectors, one for the left side and one for the right side.
fn expr_to_columns(
    expr: &Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
) -> Result<(Vec<usize>, Vec<usize>), ExecutionError> {
    let mut left_field_indices: Vec<usize> = vec![];
    let mut right_field_indices: Vec<usize> = vec![];

    expr.apply(&mut |expr: &Arc<dyn PhysicalExpr>| {
        Ok({
            if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                if column.index() > left_field_len + right_field_len {
                    return Err(DataFusionError::Internal(format!(
                        "Column index {} out of range",
                        column.index()
                    )));
                } else if column.index() < left_field_len {
                    left_field_indices.push(column.index());
                } else {
                    right_field_indices.push(column.index() - left_field_len);
                }
            }
            TreeNodeRecursion::Continue
        })
    })?;

    left_field_indices.sort();
    right_field_indices.sort();

    Ok((left_field_indices, right_field_indices))
}

/// A physical join filter rewritter which rewrites the column indices in the expression
/// to use the new column indices. See `rewrite_physical_expr`.
struct JoinFilterRewriter<'a> {
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &'a [usize],
    right_field_indices: &'a [usize],
}

impl JoinFilterRewriter<'_> {
    fn new<'a>(
        left_field_len: usize,
        right_field_len: usize,
        left_field_indices: &'a [usize],
        right_field_indices: &'a [usize],
    ) -> JoinFilterRewriter<'a> {
        JoinFilterRewriter {
            left_field_len,
            right_field_len,
            left_field_indices,
            right_field_indices,
        }
    }
}

impl TreeNodeRewriter for JoinFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if column.index() < self.left_field_len {
                // left side
                let new_index = self
                    .left_field_indices
                    .iter()
                    .position(|&x| x == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in left field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index,
                ))))
            } else if column.index() < self.left_field_len + self.right_field_len {
                // right side
                let new_index = self
                    .right_field_indices
                    .iter()
                    .position(|&x| x + self.left_field_len == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in right field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index + self.left_field_indices.len(),
                ))))
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Column index {} out of range",
                    column.index()
                )));
            }
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Rewrites the physical expression to use the new column indices.
/// This is necessary when the physical expression is used in a join filter, as the column
/// indices are different from the original schema.
fn rewrite_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &[usize],
    right_field_indices: &[usize],
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    let mut rewriter = JoinFilterRewriter::new(
        left_field_len,
        right_field_len,
        left_field_indices,
        right_field_indices,
    );

    Ok(expr.rewrite(&mut rewriter).data()?)
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

    use crate::execution::operators::ExecutionError;
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
        let planner = PhysicalPlanner::default();
        let row_count = 100;

        // Create a dictionary array with 100 values, and use it as input to the execution.
        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = Int32Array::from(vec![0, 1, 2, 3]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut vec![]).unwrap();
        scans[0].set_input_batch(input_batch);

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
        let planner = PhysicalPlanner::default();

        let row_count = 100;

        let keys = Int32Array::new((0..(row_count as i32)).map(|n| n % 4).collect(), None);
        let values = StringArray::from(vec!["foo", "bar", "hello", "comet"]);
        let input_array = DictionaryArray::new(keys, Arc::new(values));
        let input_batch = InputBatch::Batch(vec![Arc::new(input_array)], row_count);

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut vec![]).unwrap();

        // Scan's schema is determined by the input batch, so we need to set it before execution.
        scans[0].set_input_batch(input_batch);

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
        let planner = PhysicalPlanner::default();

        let (mut scans, datafusion_plan) = planner.create_plan(&op, &mut vec![]).unwrap();

        let scan = &mut scans[0];
        scan.set_input_batch(InputBatch::EOF);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let stream = datafusion_plan.execute(0, task_ctx.clone()).unwrap();
        let output = collect(stream).await.unwrap();
        assert!(output.is_empty());
    }

    #[tokio::test()]
    async fn from_datafusion_error_to_comet() {
        let err_msg = "exec error";
        let err = datafusion_common::DataFusionError::Execution(err_msg.to_string());
        let comet_err: ExecutionError = err.into();
        assert_eq!(comet_err.to_string(), "Error from DataFusion: exec error.");
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
