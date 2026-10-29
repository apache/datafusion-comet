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

//! Logical expression builders

use crate::execution::operators::ExecutionError;
use crate::execution::planner::expression_registry::ExpressionBuilder;
use crate::execution::planner::PhysicalPlanner;
use crate::{extract_expr, unary_expr_builder};
use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::LazyLogical;
use std::sync::Arc;

/// Builds `AND` and `OR`, which Spark evaluates for the right operand only on the rows that the
/// left one has not decided.
macro_rules! lazy_logical_builder {
    ($builder_name:ident, $expr_type:ident, $operator:expr) => {
        pub struct $builder_name;

        impl ExpressionBuilder for $builder_name {
            fn build(
                &self,
                spark_expr: &Expr,
                input_schema: SchemaRef,
                planner: &PhysicalPlanner,
            ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
                let expr = extract_expr!(spark_expr, $expr_type);
                let left =
                    planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
                let right =
                    planner.create_expr(expr.right.as_ref().unwrap(), Arc::clone(&input_schema))?;
                Ok(LazyLogical::wrap(left, $operator, right, &input_schema)?)
            }
        }
    };
}

lazy_logical_builder!(AndBuilder, And, DataFusionOperator::And);
lazy_logical_builder!(OrBuilder, Or, DataFusionOperator::Or);
unary_expr_builder!(NotBuilder, Not, NotExpr::new);
