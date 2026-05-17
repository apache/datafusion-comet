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

use crate::execution::operators::ExecutionError;
use crate::execution::planner::expression_registry::ExpressionBuilder;
use crate::execution::planner::PhysicalPlanner;
use crate::extract_expr;
use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::{RandExpr, RandnExpr};
use std::sync::Arc;

pub struct RandBuilder;

impl ExpressionBuilder for RandBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        _input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Rand);
        let seed = expr.seed.wrapping_add(planner.partition().into());
        Ok(Arc::new(RandExpr::new(seed)))
    }
}

pub struct RandnBuilder;

impl ExpressionBuilder for RandnBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        _input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Randn);
        let seed = expr.seed.wrapping_add(planner.partition().into());
        Ok(Arc::new(RandnExpr::new(seed)))
    }
}
