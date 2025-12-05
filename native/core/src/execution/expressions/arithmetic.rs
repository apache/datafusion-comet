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

//! Arithmetic expression builders

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::{create_modulo_expr, create_negate_expr, EvalMode};

use crate::execution::{
    expressions::extract_expr,
    operators::ExecutionError,
    planner::{
        from_protobuf_eval_mode, traits::ExpressionBuilder, BinaryExprOptions, PhysicalPlanner,
    },
};

/// Builder for Add expressions
pub struct AddBuilder;

impl ExpressionBuilder for AddBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Add);
        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
        planner.create_binary_expr(
            expr.left.as_ref().unwrap(),
            expr.right.as_ref().unwrap(),
            expr.return_type.as_ref(),
            DataFusionOperator::Plus,
            input_schema,
            eval_mode,
        )
    }
}

/// Builder for Subtract expressions
pub struct SubtractBuilder;

impl ExpressionBuilder for SubtractBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Subtract);
        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
        planner.create_binary_expr(
            expr.left.as_ref().unwrap(),
            expr.right.as_ref().unwrap(),
            expr.return_type.as_ref(),
            DataFusionOperator::Minus,
            input_schema,
            eval_mode,
        )
    }
}

/// Builder for Multiply expressions
pub struct MultiplyBuilder;

impl ExpressionBuilder for MultiplyBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Multiply);
        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
        planner.create_binary_expr(
            expr.left.as_ref().unwrap(),
            expr.right.as_ref().unwrap(),
            expr.return_type.as_ref(),
            DataFusionOperator::Multiply,
            input_schema,
            eval_mode,
        )
    }
}

/// Builder for Divide expressions
pub struct DivideBuilder;

impl ExpressionBuilder for DivideBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Divide);
        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
        planner.create_binary_expr(
            expr.left.as_ref().unwrap(),
            expr.right.as_ref().unwrap(),
            expr.return_type.as_ref(),
            DataFusionOperator::Divide,
            input_schema,
            eval_mode,
        )
    }
}

/// Builder for IntegralDivide expressions
pub struct IntegralDivideBuilder;

impl ExpressionBuilder for IntegralDivideBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, IntegralDivide);
        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
        planner.create_binary_expr_with_options(
            expr.left.as_ref().unwrap(),
            expr.right.as_ref().unwrap(),
            expr.return_type.as_ref(),
            DataFusionOperator::Divide,
            input_schema,
            BinaryExprOptions {
                is_integral_div: true,
            },
            eval_mode,
        )
    }
}

/// Builder for Remainder expressions
pub struct RemainderBuilder;

impl ExpressionBuilder for RemainderBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Remainder);
        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), Arc::clone(&input_schema))?;

        let result = create_modulo_expr(
            left,
            right,
            expr.return_type
                .as_ref()
                .map(crate::execution::serde::to_arrow_datatype)
                .unwrap(),
            input_schema,
            eval_mode == EvalMode::Ansi,
            &planner.session_ctx().state(),
        );
        result.map_err(|e| ExecutionError::GeneralError(e.to_string()))
    }
}

/// Builder for UnaryMinus expressions
pub struct UnaryMinusBuilder;

impl ExpressionBuilder for UnaryMinusBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, UnaryMinus);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
        let result = create_negate_expr(child, expr.fail_on_error);
        result.map_err(|e| ExecutionError::GeneralError(e.to_string()))
    }
}
