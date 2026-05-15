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
//! Supported operators: =, !=, <, <=, >, >=, AND, OR, NOT, IS NULL,
//! IS NOT NULL, IN (including NOT IN). Cast wrappers are unwrapped
//! (kernel stats don't need type coercion). Anything else becomes
//! `Predicate::unknown()`, which disables data skipping for that
//! subtree but is never incorrect.

use datafusion_comet_proto::spark_expression::{self, expr::ExprStruct, literal, Expr};
use delta_kernel::expressions::{ArrayData, BinaryPredicateOp, Expression, Predicate, Scalar};
use delta_kernel::schema::{ArrayType, DataType};

/// Translate with column name resolution for BoundReferences.
pub fn catalyst_to_kernel_predicate_with_names(expr: &Expr, column_names: &[String]) -> Predicate {
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
            Predicate::eq,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Neq(binary)) => binary_pred_n(
            Predicate::ne,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Lt(binary)) => binary_pred_n(
            Predicate::lt,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::LtEq(binary)) => binary_pred_n(
            Predicate::le,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::Gt(binary)) => binary_pred_n(
            Predicate::gt,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::GtEq(binary)) => binary_pred_n(
            Predicate::ge,
            binary.left.as_deref(),
            binary.right.as_deref(),
            names,
        ),
        Some(ExprStruct::And(binary)) => match (binary.left.as_deref(), binary.right.as_deref()) {
            (Some(l), Some(r)) => {
                Predicate::and(translate_predicate(l, names), translate_predicate(r, names))
            }
            _ => Predicate::unknown("and_missing_child"),
        },
        Some(ExprStruct::Or(binary)) => match (binary.left.as_deref(), binary.right.as_deref()) {
            (Some(l), Some(r)) => {
                Predicate::or(translate_predicate(l, names), translate_predicate(r, names))
            }
            _ => Predicate::unknown("or_missing_child"),
        },
        Some(ExprStruct::Not(unary)) => match unary.child.as_deref() {
            Some(child) => Predicate::not(translate_predicate(child, names)),
            None => Predicate::unknown("not_missing_child"),
        },
        Some(ExprStruct::In(in_expr)) => translate_in(in_expr, names),
        // Unwrap Cast: kernel stats don't need type coercion, pass child through
        Some(ExprStruct::Cast(cast)) => match cast.child.as_deref() {
            Some(child) => translate_predicate(child, names),
            None => Predicate::unknown("cast_missing_child"),
        },
        _ => Predicate::unknown("unsupported_catalyst_expr"),
    }
}

fn translate_in(in_expr: &spark_expression::In, names: &[String]) -> Predicate {
    let value = match in_expr.in_value.as_deref() {
        Some(v) => catalyst_to_kernel_expression_with_names(v, names),
        None => return Predicate::unknown("in_missing_value"),
    };

    let scalars: Vec<Scalar> = in_expr
        .lists
        .iter()
        .filter_map(catalyst_literal_to_scalar)
        .collect();

    if scalars.is_empty() {
        return Predicate::unknown("in_no_literal_values");
    }

    let kernel_type = scalar_to_kernel_type(&scalars[0]);
    let array_data = match ArrayData::try_new(ArrayType::new(kernel_type, true), scalars) {
        Ok(ad) => ad,
        Err(_) => return Predicate::unknown("in_array_type_mismatch"),
    };
    let array = Expression::literal(Scalar::Array(array_data));

    let pred = Predicate::binary(BinaryPredicateOp::In, value, array);
    if in_expr.negated {
        Predicate::not(pred)
    } else {
        pred
    }
}

fn scalar_to_kernel_type(s: &Scalar) -> DataType {
    match s {
        Scalar::Boolean(_) => DataType::BOOLEAN,
        Scalar::Byte(_) => DataType::BYTE,
        Scalar::Short(_) => DataType::SHORT,
        Scalar::Integer(_) => DataType::INTEGER,
        Scalar::Long(_) => DataType::LONG,
        Scalar::Float(_) => DataType::FLOAT,
        Scalar::Double(_) => DataType::DOUBLE,
        Scalar::String(_) => DataType::STRING,
        _ => DataType::STRING,
    }
}

fn catalyst_literal_to_scalar(expr: &Expr) -> Option<Scalar> {
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::Literal(lit)) => match &lit.value {
            Some(literal::Value::BoolVal(b)) => Some(Scalar::Boolean(*b)),
            Some(literal::Value::ByteVal(v)) => Some(Scalar::Byte(*v as i8)),
            Some(literal::Value::ShortVal(v)) => Some(Scalar::Short(*v as i16)),
            Some(literal::Value::IntVal(v)) => Some(Scalar::Integer(*v)),
            Some(literal::Value::LongVal(v)) => Some(Scalar::Long(*v)),
            Some(literal::Value::FloatVal(v)) => Some(Scalar::Float(*v)),
            Some(literal::Value::DoubleVal(v)) => Some(Scalar::Double(*v)),
            Some(literal::Value::StringVal(s)) => Some(Scalar::String(s.clone())),
            _ => None,
        },
        _ => None,
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
        // Unwrap Cast: pass child expression through for kernel stats evaluation
        Some(ExprStruct::Cast(cast)) => match cast.child.as_deref() {
            Some(child) => catalyst_to_kernel_expression_with_names(child, column_names),
            None => Expression::unknown("cast_missing_child"),
        },
        _ => Expression::unknown("unsupported_expr_operand"),
    }
}

fn catalyst_literal_to_kernel(lit: &spark_expression::Literal) -> Expression {
    match &lit.value {
        Some(literal::Value::BoolVal(b)) => Expression::literal(*b),
        Some(literal::Value::ByteVal(v)) => Expression::literal(*v),
        Some(literal::Value::ShortVal(v)) => Expression::literal(*v),
        Some(literal::Value::IntVal(v)) => Expression::literal(*v),
        Some(literal::Value::LongVal(v)) => Expression::literal(*v),
        Some(literal::Value::FloatVal(v)) => Expression::literal(*v),
        Some(literal::Value::DoubleVal(v)) => Expression::literal(*v),
        Some(literal::Value::StringVal(s)) => Expression::literal(s.as_str()),
        _ => Expression::null_literal(DataType::STRING),
    }
}
