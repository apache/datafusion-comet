//! Null check expression builders

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
