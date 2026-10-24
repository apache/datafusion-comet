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

//! Null check expression builders

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::AssertNotNull;

use crate::execution::{
    operators::ExecutionError,
    planner::{expression_registry::ExpressionBuilder, PhysicalPlanner},
};
use crate::extract_expr;
use datafusion::physical_expr::expressions::{IsNotNullExpr, IsNullExpr};

use crate::unary_expr_builder;

/// Macro to define all null check builders at once
macro_rules! define_null_check_builders {
    () => {
        unary_expr_builder!(IsNullBuilder, IsNull, IsNullExpr::new);
        unary_expr_builder!(IsNotNullBuilder, IsNotNull, IsNotNullExpr::new);
    };
}

define_null_check_builders!();

pub struct AssertNotNullBuilder;

impl ExpressionBuilder for AssertNotNullBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, AssertNotNull);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(AssertNotNull::new(
            child,
            expr.walked_type_path.clone(),
        )))
    }
}
