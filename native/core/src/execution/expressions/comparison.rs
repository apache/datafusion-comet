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
            return Err(ExecutionError::GeneralError(
                "Expected comparison expression".to_string(),
            ))
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
        if let Some(ExprStruct::Eq(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::Eq)
        } else {
            Err(ExecutionError::GeneralError(
                "Expected Eq expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::Neq(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(
                spark_expr,
                input_schema,
                planner,
                DataFusionOperator::NotEq,
            )
        } else {
            Err(ExecutionError::GeneralError(
                "Expected Neq expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::Lt(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::Lt)
        } else {
            Err(ExecutionError::GeneralError(
                "Expected Lt expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::LtEq(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(
                spark_expr,
                input_schema,
                planner,
                DataFusionOperator::LtEq,
            )
        } else {
            Err(ExecutionError::GeneralError(
                "Expected LtEq expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::Gt(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(spark_expr, input_schema, planner, DataFusionOperator::Gt)
        } else {
            Err(ExecutionError::GeneralError(
                "Expected Gt expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::GtEq(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(
                spark_expr,
                input_schema,
                planner,
                DataFusionOperator::GtEq,
            )
        } else {
            Err(ExecutionError::GeneralError(
                "Expected GtEq expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::EqNullSafe(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(
                spark_expr,
                input_schema,
                planner,
                DataFusionOperator::IsNotDistinctFrom,
            )
        } else {
            Err(ExecutionError::GeneralError(
                "Expected EqNullSafe expression".to_string(),
            ))
        }
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
        if let Some(ExprStruct::NeqNullSafe(_)) = &spark_expr.expr_struct {
            create_binary_comparison_expr(
                spark_expr,
                input_schema,
                planner,
                DataFusionOperator::IsDistinctFrom,
            )
        } else {
            Err(ExecutionError::GeneralError(
                "Expected NeqNullSafe expression".to_string(),
            ))
        }
    }
}
