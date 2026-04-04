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

//! Array expression builders

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::{ArrayExistsExpr, LambdaVariableExpr};

use crate::execution::operators::ExecutionError;
use crate::execution::planner::expression_registry::ExpressionBuilder;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::serde::to_arrow_datatype;
use crate::extract_expr;

pub struct ArrayExistsBuilder;

impl ExpressionBuilder for ArrayExistsBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, ArrayExists);
        let array_expr =
            planner.create_expr(expr.array.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let lambda_body = planner.create_expr(expr.lambda_body.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(ArrayExistsExpr::new(
            array_expr,
            lambda_body,
            expr.follow_three_valued_logic,
        )))
    }
}

pub struct LambdaVariableBuilder;

impl ExpressionBuilder for LambdaVariableBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        _input_schema: SchemaRef,
        _planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, LambdaVariable);
        let data_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
        Ok(Arc::new(LambdaVariableExpr::new(data_type)))
    }
}
