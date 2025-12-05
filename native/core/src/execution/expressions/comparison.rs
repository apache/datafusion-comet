//! Comparison expression builders

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

/// Builder for Eq expressions
pub struct EqBuilder;

impl ExpressionBuilder for EqBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Eq);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::Eq,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, Neq);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::NotEq,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, Lt);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::Lt,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, LtEq);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::LtEq,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, Gt);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::Gt,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, GtEq);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::GtEq,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, EqNullSafe);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::IsNotDistinctFrom,
            right,
        )))
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
        let expr = extract_expr!(spark_expr, NeqNullSafe);
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;
        Ok(Arc::new(BinaryExpr::new(
            left,
            DataFusionOperator::IsDistinctFrom,
            right,
        )))
    }
}
