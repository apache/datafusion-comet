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

//! String expression builders

use std::cmp::max;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::{LikeExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::{FromJson, RLike, SubstringExpr};

use crate::execution::{
    expressions::extract_expr,
    operators::ExecutionError,
    planner::{expression_registry::ExpressionBuilder, PhysicalPlanner},
    serde::to_arrow_datatype,
};

/// Builder for Substring expressions
pub struct SubstringBuilder;

impl ExpressionBuilder for SubstringBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Substring);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
        // Spark Substring's start is 1-based when start > 0
        let start = expr.start - i32::from(expr.start > 0);
        // substring negative len is treated as 0 in Spark
        let len = max(expr.len, 0);

        Ok(Arc::new(SubstringExpr::new(
            child,
            start as i64,
            len as u64,
        )))
    }
}

/// Builder for Like expressions
pub struct LikeBuilder;

impl ExpressionBuilder for LikeBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Like);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

        Ok(Arc::new(LikeExpr::new(false, false, left, right)))
    }
}

/// Builder for Rlike (regex like) expressions
pub struct RlikeBuilder;

impl ExpressionBuilder for RlikeBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Rlike);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

        match right.as_any().downcast_ref::<Literal>().unwrap().value() {
            ScalarValue::Utf8(Some(pattern)) => Ok(Arc::new(RLike::try_new(left, pattern)?)),
            _ => Err(ExecutionError::GeneralError(
                "RLike only supports scalar patterns".to_string(),
            )),
        }
    }
}

pub struct FromJsonBuilder;

impl ExpressionBuilder for FromJsonBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, FromJson);
        let child = planner.create_expr(
            expr.child.as_ref().ok_or_else(|| {
                ExecutionError::GeneralError("FromJson missing child".to_string())
            })?,
            input_schema,
        )?;
        let schema =
            to_arrow_datatype(expr.schema.as_ref().ok_or_else(|| {
                ExecutionError::GeneralError("FromJson missing schema".to_string())
            })?);
        Ok(Arc::new(FromJson::new(child, schema, &expr.timezone)))
    }
}
