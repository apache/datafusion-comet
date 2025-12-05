//! Bitwise expression builders

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion::physical_expr::{expressions::BinaryExpr, PhysicalExpr};
use datafusion_comet_proto::spark_expression::Expr;

use crate::execution::{
    operators::ExecutionError,
    planner::{traits::ExpressionBuilder, PhysicalPlanner},
};

/// Helper function to create binary bitwise expressions
fn create_binary_bitwise_expr(
    spark_expr: &Expr,
    input_schema: SchemaRef,
    planner: &PhysicalPlanner,
    operator: DataFusionOperator,
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    // Extract left and right from the appropriate bitwise expression
    let (left_expr, right_expr) = match &spark_expr.expr_struct {
        Some(datafusion_comet_proto::spark_expression::expr::ExprStruct::BitwiseAnd(expr)) => {
            (expr.left.as_ref(), expr.right.as_ref())
        }
        Some(datafusion_comet_proto::spark_expression::expr::ExprStruct::BitwiseOr(expr)) => {
            (expr.left.as_ref(), expr.right.as_ref())
        }
        Some(datafusion_comet_proto::spark_expression::expr::ExprStruct::BitwiseXor(expr)) => {
            (expr.left.as_ref(), expr.right.as_ref())
        }
        Some(datafusion_comet_proto::spark_expression::expr::ExprStruct::BitwiseShiftLeft(
            expr,
        )) => (expr.left.as_ref(), expr.right.as_ref()),
        Some(datafusion_comet_proto::spark_expression::expr::ExprStruct::BitwiseShiftRight(
            expr,
        )) => (expr.left.as_ref(), expr.right.as_ref()),
        _ => {
            panic!("create_binary_bitwise_expr called with non-bitwise expression");
        }
    };

    let left = planner.create_expr(left_expr.unwrap(), Arc::clone(&input_schema))?;
    let right = planner.create_expr(right_expr.unwrap(), input_schema)?;
    Ok(Arc::new(BinaryExpr::new(left, operator, right)))
}

/// Builder for BitwiseAnd expressions
pub struct BitwiseAndBuilder;

impl ExpressionBuilder for BitwiseAndBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_bitwise_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::BitwiseAnd,
        )
    }
}

/// Builder for BitwiseOr expressions
pub struct BitwiseOrBuilder;

impl ExpressionBuilder for BitwiseOrBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_bitwise_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::BitwiseOr,
        )
    }
}

/// Builder for BitwiseXor expressions
pub struct BitwiseXorBuilder;

impl ExpressionBuilder for BitwiseXorBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_bitwise_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::BitwiseXor,
        )
    }
}

/// Builder for BitwiseShiftLeft expressions
pub struct BitwiseShiftLeftBuilder;

impl ExpressionBuilder for BitwiseShiftLeftBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_bitwise_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::BitwiseShiftLeft,
        )
    }
}

/// Builder for BitwiseShiftRight expressions
pub struct BitwiseShiftRightBuilder;

impl ExpressionBuilder for BitwiseShiftRightBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        create_binary_bitwise_expr(
            spark_expr,
            input_schema,
            planner,
            DataFusionOperator::BitwiseShiftRight,
        )
    }
}
