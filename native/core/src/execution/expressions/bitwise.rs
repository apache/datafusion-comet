//! Bitwise expression builders

use datafusion::logical_expr::Operator as DataFusionOperator;

use crate::binary_expr_builder;

/// Macro to define all bitwise builders at once
macro_rules! define_bitwise_builders {
    ($(($builder:ident, $expr_type:ident, $op:expr)),* $(,)?) => {
        $(
            binary_expr_builder!($builder, $expr_type, $op);
        )*
    };
}

define_bitwise_builders![
    (
        BitwiseAndBuilder,
        BitwiseAnd,
        DataFusionOperator::BitwiseAnd
    ),
    (BitwiseOrBuilder, BitwiseOr, DataFusionOperator::BitwiseOr),
    (
        BitwiseXorBuilder,
        BitwiseXor,
        DataFusionOperator::BitwiseXor
    ),
    (
        BitwiseShiftLeftBuilder,
        BitwiseShiftLeft,
        DataFusionOperator::BitwiseShiftLeft
    ),
    (
        BitwiseShiftRightBuilder,
        BitwiseShiftRight,
        DataFusionOperator::BitwiseShiftRight
    ),
];
