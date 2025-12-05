//! Comparison expression builders

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion::physical_expr::{expressions::BinaryExpr, PhysicalExpr};
use datafusion_comet_proto::spark_expression::{expr::ExprStruct, Expr};

use crate::execution::{
    operators::ExecutionError,
    planner::{traits::ExpressionBuilder, PhysicalPlanner},
};

/// Helper function to create binary comparison expressions
fn create_binary_comparison_expr(
    spark_expr: &Expr,
    input_schema: SchemaRef,
    planner: &PhysicalPlanner,
    operator: DataFusionOperator,
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    // Extract left and right from the appropriate comparison expression
    let (left_expr, right_expr) = match &spark_expr.expr_struct {
        Some(ExprStruct::Eq(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::Neq(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::Lt(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::LtEq(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::Gt(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::GtEq(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::EqNullSafe(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(ExprStruct::NeqNullSafe(expr)) => (expr.left.as_ref(), expr.right.as_ref()),
        _ => {
            panic!("create_binary_comparison_expr called with non-comparison expression");
        }
    };

    let left = planner.create_expr(left_expr.unwrap(), Arc::clone(&input_schema))?;
    let right = planner.create_expr(right_expr.unwrap(), input_schema)?;
    Ok(Arc::new(BinaryExpr::new(left, operator, right)))
}

/// Builder for Eq expressions
pub struct EqBuilder;

impl ExpressionBuilder for EqBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::Eq)
    }
}

/// Builder for Neq expressions
pub struct NeqBuilder;

impl ExpressionBuilder for NeqBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::NotEq)
    }
}

/// Builder for Lt expressions
pub struct LtBuilder;

impl ExpressionBuilder for LtBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::Lt)
    }
}

/// Builder for LtEq expressions
pub struct LtEqBuilder;

impl ExpressionBuilder for LtEqBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::LtEq)
    }
}

/// Builder for Gt expressions
pub struct GtBuilder;

impl ExpressionBuilder for GtBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::Gt)
    }
}

/// Builder for GtEq expressions
pub struct GtEqBuilder;

impl ExpressionBuilder for GtEqBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::GtEq)
    }
}

/// Builder for EqNullSafe expressions
pub struct EqNullSafeBuilder;

impl ExpressionBuilder for EqNullSafeBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::IsNotDistinctFrom,
        )
    }
}

/// Builder for NeqNullSafe expressions
pub struct NeqNullSafeBuilder;

impl ExpressionBuilder for NeqNullSafeBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_comparison_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::IsDistinctFrom,
        )
    }
}
