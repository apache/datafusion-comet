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

//! Core macros for the modular planner framework

/// Macro to extract a specific expression variant, panicking if called with wrong type.
/// This should be used in expression builders where the registry guarantees the correct
/// expression type has been routed to the builder.
#[macro_export]
macro_rules! extract_expr {
    ($spark_expr:expr, $variant:ident) => {
        match $spark_expr
            .expr_struct
            .as_ref()
            .expect("expression struct must be present")
        {
            datafusion_comet_proto::spark_expression::expr::ExprStruct::$variant(expr) => expr,
            other => panic!(
                "{} builder called with wrong expression type: {:?}",
                stringify!($variant),
                other
            ),
        }
    };
}

/// Macro to extract a specific operator variant, panicking if called with wrong type.
/// This should be used in operator builders where the registry guarantees the correct
/// operator type has been routed to the builder.
#[macro_export]
macro_rules! extract_op {
    ($spark_operator:expr, $variant:ident) => {
        match $spark_operator
            .op_struct
            .as_ref()
            .expect("operator struct must be present")
        {
            datafusion_comet_proto::spark_operator::operator::OpStruct::$variant(op) => op,
            other => panic!(
                "{} builder called with wrong operator type: {:?}",
                stringify!($variant),
                other
            ),
        }
    };
}

/// Macro to generate binary expression builders with minimal boilerplate
#[macro_export]
macro_rules! binary_expr_builder {
    ($builder_name:ident, $expr_type:ident, $operator:expr) => {
        pub struct $builder_name;

        impl $crate::execution::planner::expression_registry::ExpressionBuilder for $builder_name {
            fn build(
                &self,
                spark_expr: &datafusion_comet_proto::spark_expression::Expr,
                input_schema: arrow::datatypes::SchemaRef,
                planner: &$crate::execution::planner::PhysicalPlanner,
            ) -> Result<
                std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                $crate::execution::operators::ExecutionError,
            > {
                let expr = $crate::extract_expr!(spark_expr, $expr_type);
                let left = planner.create_expr(
                    expr.left.as_ref().unwrap(),
                    std::sync::Arc::clone(&input_schema),
                )?;
                let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
                Ok(std::sync::Arc::new(
                    datafusion::physical_expr::expressions::BinaryExpr::new(left, $operator, right),
                ))
            }
        }
    };
}

/// Macro to generate unary expression builders
#[macro_export]
macro_rules! unary_expr_builder {
    ($builder_name:ident, $expr_type:ident, $expr_constructor:expr) => {
        pub struct $builder_name;

        impl $crate::execution::planner::expression_registry::ExpressionBuilder for $builder_name {
            fn build(
                &self,
                spark_expr: &datafusion_comet_proto::spark_expression::Expr,
                input_schema: arrow::datatypes::SchemaRef,
                planner: &$crate::execution::planner::PhysicalPlanner,
            ) -> Result<
                std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                $crate::execution::operators::ExecutionError,
            > {
                let expr = $crate::extract_expr!(spark_expr, $expr_type);
                let child = planner.create_expr(expr.child.as_ref().unwrap(), input_schema)?;
                Ok(std::sync::Arc::new($expr_constructor(child)))
            }
        }
    };
}
