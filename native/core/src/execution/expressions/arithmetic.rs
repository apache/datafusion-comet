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

use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{QueryContext, SparkError, SparkErrorWithContext};

/// Wrapper expression that catches and wraps SparkError with QueryContext
/// for binary arithmetic operations.
#[derive(Debug)]
pub struct CheckedBinaryExpr {
    /// The underlying physical expression (typically a ScalarFunctionExpr)
    child: Arc<dyn PhysicalExpr>,
    /// Optional query context to attach to errors
    query_context: Option<Arc<QueryContext>>,
}

impl CheckedBinaryExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, query_context: Option<Arc<QueryContext>>) -> Self {
        Self {
            child,
            query_context,
        }
    }
}

impl Display for CheckedBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CheckedBinaryExpr({})", self.child)
    }
}

impl PartialEq for CheckedBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for CheckedBinaryExpr {}

impl PartialEq<dyn Any> for CheckedBinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self.eq(x))
            .unwrap_or(false)
    }
}

impl Hash for CheckedBinaryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for CheckedBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.child.fmt_sql(f)
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let result = self.child.evaluate(batch);

        // If there's an error and we have query_context, wrap it
        match result {
            Err(DataFusionError::External(e)) if self.query_context.is_some() => {
                if let Some(spark_err) = e.downcast_ref::<SparkError>() {
                    let wrapped = SparkErrorWithContext::with_context(
                        spark_err.clone(),
                        Arc::clone(self.query_context.as_ref().unwrap()),
                    );
                    Err(DataFusionError::External(Box::new(wrapped)))
                } else {
                    Err(DataFusionError::External(e))
                }
            }
            other => other,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(CheckedBinaryExpr::new(
                Arc::clone(&children[0]),
                self.query_context.clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "CheckedBinaryExpr should have exactly one child".to_string(),
            )),
        }
    }
}

/// Macro to generate arithmetic expression builders that need eval_mode handling
#[macro_export]
macro_rules! arithmetic_expr_builder {
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
                let eval_mode =
                    $crate::execution::planner::from_protobuf_eval_mode(expr.eval_mode)?;
                planner.create_binary_expr(
                    spark_expr, // Pass the full spark_expr for query_context lookup
                    expr.left.as_ref().unwrap(),
                    expr.right.as_ref().unwrap(),
                    expr.return_type.as_ref(),
                    $operator,
                    input_schema,
                    eval_mode,
                )
            }
        }
    };
}

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::{create_modulo_expr, create_negate_expr, EvalMode};

use crate::execution::{
    expressions::extract_expr,
    operators::ExecutionError,
    planner::{
        expression_registry::ExpressionBuilder, from_protobuf_eval_mode, BinaryExprOptions,
        PhysicalPlanner,
    },
};

/// Macro to define basic arithmetic builders that use eval_mode
macro_rules! define_basic_arithmetic_builders {
    ($(($builder:ident, $expr_type:ident, $op:expr)),* $(,)?) => {
        $(
            arithmetic_expr_builder!($builder, $expr_type, $op);
        )*
    };
}

define_basic_arithmetic_builders![
    (AddBuilder, Add, DataFusionOperator::Plus),
    (SubtractBuilder, Subtract, DataFusionOperator::Minus),
    (MultiplyBuilder, Multiply, DataFusionOperator::Multiply),
    (DivideBuilder, Divide, DataFusionOperator::Divide),
];

/// Builder for IntegralDivide expressions (requires special options)
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
            spark_expr, // Pass the full spark_expr for query_context lookup
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

/// Builder for Remainder expressions (uses special modulo function)
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

/// Builder for UnaryMinus expressions (uses special negate function)
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
