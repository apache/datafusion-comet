//! Comparison expression builders

use datafusion::logical_expr::Operator as DataFusionOperator;

use crate::binary_expr_builder;

/// Macro to define all comparison builders at once
macro_rules! define_comparison_builders {
    ($(($builder:ident, $expr_type:ident, $op:expr)),* $(,)?) => {
        $(
            binary_expr_builder!($builder, $expr_type, $op);
        )*
    };
}

define_comparison_builders![
    (EqBuilder, Eq, DataFusionOperator::Eq),
    (NeqBuilder, Neq, DataFusionOperator::NotEq),
    (LtBuilder, Lt, DataFusionOperator::Lt),
    (LtEqBuilder, LtEq, DataFusionOperator::LtEq),
    (GtBuilder, Gt, DataFusionOperator::Gt),
    (GtEqBuilder, GtEq, DataFusionOperator::GtEq),
    (
        EqNullSafeBuilder,
        EqNullSafe,
        DataFusionOperator::IsNotDistinctFrom
    ),
    (
        NeqNullSafeBuilder,
        NeqNullSafe,
        DataFusionOperator::IsDistinctFrom
    ),
];
