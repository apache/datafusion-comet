//! Bitwise expression builders

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion::physical_expr::{expressions::BinaryExpr, PhysicalExpr};
use datafusion_comet_proto::spark_expression::Expr;

use crate::execution::{
    expressions::extract_expr,
    operators::ExecutionError,
    planner::{traits::ExpressionBuilder, PhysicalPlanner},
};

/// Builder for BitwiseAnd expressions
pub struct BitwiseAndBuilder;

impl ExpressionBuilder for BitwiseAndBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, BitwiseAnd);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        let op = DataFusionOperator::BitwiseAnd;
        Ok(Arc::new(BinaryExpr::new(left, op, right)))
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
        let expr = extract_expr!(spark_expr, BitwiseOr);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        let op = DataFusionOperator::BitwiseOr;
        Ok(Arc::new(BinaryExpr::new(left, op, right)))
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
        let expr = extract_expr!(spark_expr, BitwiseXor);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        let op = DataFusionOperator::BitwiseXor;
        Ok(Arc::new(BinaryExpr::new(left, op, right)))
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
        let expr = extract_expr!(spark_expr, BitwiseShiftLeft);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        let op = DataFusionOperator::BitwiseShiftLeft;
        Ok(Arc::new(BinaryExpr::new(left, op, right)))
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
        let expr = extract_expr!(spark_expr, BitwiseShiftRight);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        let op = DataFusionOperator::BitwiseShiftRight;
        Ok(Arc::new(BinaryExpr::new(left, op, right)))
    }
}
