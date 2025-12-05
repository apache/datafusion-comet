//! Logical expression builders

use datafusion::logical_expr::Operator as DataFusionOperator;
use datafusion::physical_expr::expressions::NotExpr;

use crate::{binary_expr_builder, unary_expr_builder};

/// Macro to define all logical builders at once
macro_rules! define_logical_builders {
    () => {
        binary_expr_builder!(AndBuilder, And, DataFusionOperator::And);
        binary_expr_builder!(OrBuilder, Or, DataFusionOperator::Or);
        unary_expr_builder!(NotBuilder, Not, NotExpr::new);
    };
}

define_logical_builders!();
