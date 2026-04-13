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

//! Translates Catalyst-proto `Expr` to delta-kernel `Predicate` for
//! stats-based file pruning.
//!
//! Only the subset of operators kernel understands is translated.
//! Anything else becomes `Predicate::unknown()`, which disables data
//! skipping for that subtree but is never incorrect.

use delta_kernel::expressions::{Expression, Predicate};
use datafusion_comet_proto::spark_expression::{self, expr::ExprStruct, literal, Expr};

/// Translate with column name resolution for BoundReferences.
pub fn catalyst_to_kernel_predicate_with_names(
    expr: &Expr,
    column_names: &[String],
) -> Predicate {
    translate_predicate(expr, column_names)
}

/// Try to translate a Catalyst-proto `Expr` into a kernel `Predicate`
/// (without column name resolution — BoundReferences become Unknown).
pub fn catalyst_to_kernel_predicate(expr: &Expr) -> Predicate {
    translate_predicate(expr, &[])
}

fn translate_predicate(expr: &Expr, names: &[String]) -> Predicate {
    let to_expr = |e: &Expr| catalyst_to_kernel_expression_with_names(e, names);
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::IsNull(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::is_null(to_expr(child)),
            None => Predicate::unknown("missing_child"),
        },
        Some(ExprStruct::IsNotNull(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::is_not_null(to_expr(child)),
            None => Predicate::unknown("missing_child"),
        },
        Some(ExprStruct::Eq(binary)) => binary_pred_n(
            |a, b| Predicate::eq(a, b),
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Neq(binary)) => binary_pred_n(
            |a, b| Predicate::ne(a, b),
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Lt(binary)) => binary_pred_n(
            |a, b| Predicate::lt(a, b),
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::LtEq(binary)) => binary_pred_n(
            |a, b| Predicate::le(a, b),
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Gt(binary)) => binary_pred_n(
            |a, b| Predicate::gt(a, b),
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::GtEq(binary)) => binary_pred_n(
            |a, b| Predicate::ge(a, b),
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::And(binary)) => {
            match (binary.left.as_deref(), binary.right.as_deref()) {
                (Some(l), Some(r)) => {
                    Predicate::and(translate_predicate(l, names), translate_predicate(r, names))
                }
                _ => Predicate::unknown("and_missing_child"),
            }
        }
        Some(ExprStruct::Or(binary)) => {
            match (binary.left.as_deref(), binary.right.as_deref()) {
                (Some(l), Some(r)) => {
                    Predicate::or(translate_predicate(l, names), translate_predicate(r, names))
                }
                _ => Predicate::unknown("or_missing_child"),
            }
        }
        Some(ExprStruct::Not(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::not(translate_predicate(child, names)),
            None => Predicate::unknown("not_missing_child"),
        },
        _ => Predicate::unknown("unsupported_catalyst_expr"),
    }
}

fn binary_pred_n(
    builder: impl Fn(Expression, Expression) -> Predicate,
    left: Option<&Expr>,
    right: Option<&Expr>,
    names: &[String],
) -> Predicate {
    match (left, right) {
        (Some(l), Some(r)) => builder(
            catalyst_to_kernel_expression_with_names(l, names),
            catalyst_to_kernel_expression_with_names(r, names),
        ),
        _ => Predicate::unknown("binary_missing_child"),
    }
}

/// Translate a Catalyst-proto `Expr` into a kernel value `Expression`.
///
/// `column_names` maps BoundReference indices to column names. When
/// empty, BoundReferences become unknown expressions (disabling file
/// skipping for that sub-expression but never producing wrong results).
pub fn catalyst_to_kernel_expression_with_names(
    expr: &Expr,
    column_names: &[String],
) -> Expression {
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::Bound(bound)) => {
            let idx = bound.index as usize;
            if idx < column_names.len() {
                Expression::column([column_names[idx].as_str()])
            } else {
                Expression::unknown("bound_ref_out_of_range")
            }
        }
        Some(ExprStruct::Literal(lit)) => catalyst_literal_to_kernel(lit),
        _ => Expression::unknown("unsupported_expr_operand"),
    }
}

fn catalyst_literal_to_kernel(lit: &spark_expression::Literal) -> Expression {
    match &lit.value {
        Some(literal::Value::BoolVal(b)) => Expression::literal(*b),
        Some(literal::Value::ByteVal(v)) => Expression::literal(*v as i32),
        Some(literal::Value::ShortVal(v)) => Expression::literal(*v as i32),
        Some(literal::Value::IntVal(v)) => Expression::literal(*v),
        Some(literal::Value::LongVal(v)) => Expression::literal(*v),
        Some(literal::Value::FloatVal(v)) => Expression::literal(*v),
        Some(literal::Value::DoubleVal(v)) => Expression::literal(*v),
        Some(literal::Value::StringVal(s)) => Expression::literal(s.as_str()),
        _ => Expression::null_literal(delta_kernel::schema::DataType::STRING),
    }
}

