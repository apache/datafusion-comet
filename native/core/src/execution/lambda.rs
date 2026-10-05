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

//! Lambda Function Support and Physical Adapters for Comet.
//!
//! This module provides the infrastructure required to execute Spark higher-order functions
//! (such as `array_filter`) within the native DataFusion runtime:
//!
//! 1. **Scope Management (`LambdaScope`):**
//!    Tracks nested lambda parameter bindings by their Spark `expr_id`, preventing name
//!    shadowing or collisions across nested higher-order invocations.
//!
//! 2. **Empty Batch Runtime Guard (`EmptyBatchGuardExpr`):**
//!    Wraps the lambda body to early-exit with an empty array when `batch.num_rows() == 0`.
//!    This preserves Spark's ANSI guarantee that predicates are never evaluated on empty arrays.
//!
//! 3. **Strict Short-Circuiting (`ShortCircuitBinaryExpr`):**
//!    Replaces standard DataFusion `BinaryExpr` for `AND` / `OR` inside lambda bodies.
//!    Enforces SQL Three-Valued Logic (3VL) by evaluating the RHS strictly on rows requiring it
//!    via `evaluate_selection`, preventing unwanted side-effects and runtime errors on skipped elements.

use std::cell::RefCell;
use std::collections::HashMap;

use arrow::datatypes::FieldRef;
use datafusion::common::{DataFusionError, Result};

use arrow::array::{Array, BooleanArray, BooleanBuilder};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// Maps Spark `exprId` -> (column index in the extended body schema, field).
pub(crate) type LambdaScope = HashMap<i64, (usize, FieldRef)>;

/// A stack of lambda variable scopes, innermost last.
/// Planning is single-threaded per planner, so `RefCell` is sufficient to manage
/// the stack of scopes during the recursive planning process.
#[derive(Default)]
pub(crate) struct LambdaScopes {
    stack: RefCell<Vec<LambdaScope>>,
}

impl LambdaScopes {
    /// Resolve a lambda variable by Spark `exprId`, searching innermost
    /// scope first.
    pub(crate) fn resolve_variable(&self, expr_id: i64) -> Option<(usize, FieldRef)> {
        self.stack
            .borrow()
            .iter()
            .rev()
            .find_map(|s| s.get(&expr_id).cloned())
    }

    /// Push `scope`, run `f`, pop unconditionally. The pop happens on both
    /// the `Ok` and `Err` paths — this replaces the earlier RAII guard.
    pub(crate) fn with_scope<T, E>(
        &self,
        scope: LambdaScope,
        f: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        self.stack.borrow_mut().push(scope);
        let out = f();
        self.stack.borrow_mut().pop();
        out
    }
}

/// An expression adapter that short-circuits evaluation on empty batches (0 rows),
/// returning an empty array without evaluating the inner expression.
/// This prevents runtime errors (such as division by zero in ANSI mode) when
/// Spark's short-circuit semantics guarantee the lambda predicate is never invoked
/// for empty arrays.
#[derive(Debug)]
pub struct EmptyBatchGuardExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl PartialEq for EmptyBatchGuardExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for EmptyBatchGuardExpr {}

impl Hash for EmptyBatchGuardExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.dyn_hash(state);
    }
}

impl EmptyBatchGuardExpr {
    pub fn new(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { inner }
    }
}

impl Display for EmptyBatchGuardExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EmptyBatchGuard({})", self.inner)
    }
}

impl PhysicalExpr for EmptyBatchGuardExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if batch.num_rows() == 0 {
            let dt = self.inner.data_type(&batch.schema())?;
            return Ok(ColumnarValue::Array(arrow::array::new_empty_array(&dt)));
        }
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(EmptyBatchGuardExpr::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "empty_batch_guard(")?;
        self.inner.fmt_sql(f)?;
        write!(f, ")")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShortCircuitBinaryOp {
    And,
    Or,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A physical expression for AND / OR that enforces Spark-compatible strict
/// per-element short-circuiting in lambda bodies.
///
/// Unlike DataFusion's standard `BinaryExpr` (which evaluates unmasked batches
/// if > 20% of rows need the RHS), this expression evaluates the RHS only
/// on elements that strictly require it according to SQL Three-Valued Logic (3VL):
/// - AND: evaluates RHS only when LHS is TRUE or NULL (skips when LHS is FALSE).
/// - OR:  evaluates RHS only when LHS is FALSE or NULL (skips when LHS is TRUE).
///
/// If no rows require the RHS, evaluation is skipped entirely, protecting stateful
/// expressions (such as `monotonically_increasing_id` and `rand`) and fallible operations
/// (such as division by zero or out-of-bounds indexing in ANSI mode).
#[derive(Debug)]
pub struct ShortCircuitBinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: ShortCircuitBinaryOp,
}

impl ShortCircuitBinaryExpr {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        op: ShortCircuitBinaryOp,
    ) -> Self {
        Self { left, right, op }
    }

    pub fn left(&self) -> &Arc<dyn PhysicalExpr> {
        &self.left
    }

    pub fn right(&self) -> &Arc<dyn PhysicalExpr> {
        &self.right
    }

    pub fn op(&self) -> ShortCircuitBinaryOp {
        self.op
    }
}

impl Display for ShortCircuitBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op_str = match self.op {
            ShortCircuitBinaryOp::And => "SHORT_CIRCUIT_AND",
            ShortCircuitBinaryOp::Or => "SHORT_CIRCUIT_OR",
            ShortCircuitBinaryOp::Eq => "SHORT_CIRCUIT_EQ",
            ShortCircuitBinaryOp::NotEq => "SHORT_CIRCUIT_NOT_EQ",
            ShortCircuitBinaryOp::Lt => "SHORT_CIRCUIT_LT",
            ShortCircuitBinaryOp::LtEq => "SHORT_CIRCUIT_LT_EQ",
            ShortCircuitBinaryOp::Gt => "SHORT_CIRCUIT_GT",
            ShortCircuitBinaryOp::GtEq => "SHORT_CIRCUIT_GT_EQ",
        };
        write!(f, "({} {} {})", self.left, op_str, self.right)
    }
}

impl PartialEq for ShortCircuitBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.op.eq(&other.op) && self.left().eq(other.left()) && self.right().eq(other.right())
    }
}

impl Eq for ShortCircuitBinaryExpr {}

impl Hash for ShortCircuitBinaryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        self.left.dyn_hash(state);
        self.right.dyn_hash(state);
    }
}

impl PhysicalExpr for ShortCircuitBinaryExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(ColumnarValue::Array(arrow::array::new_empty_array(
                &DataType::Boolean,
            )));
        }

        // 1. Evaluate LHS
        let lhs_val = self.left.evaluate(batch)?;
        let lhs_arr = lhs_val.into_array(num_rows)?;

        // 2. Build selection mask for RHS
        let mut selection_builder = BooleanBuilder::with_capacity(num_rows);
        match self.op {
            ShortCircuitBinaryOp::And => {
                let lhs_bool =
                    lhs_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "LHS of AND must be BooleanArray".to_string(),
                            )
                        })?;
                for i in 0..num_rows {
                    selection_builder.append_value(lhs_bool.is_null(i) || lhs_bool.value(i));
                }
            }
            ShortCircuitBinaryOp::Or => {
                let lhs_bool =
                    lhs_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("LHS of OR must be BooleanArray".to_string())
                        })?;
                for i in 0..num_rows {
                    selection_builder.append_value(lhs_bool.is_null(i) || !lhs_bool.value(i));
                }
            }

            ShortCircuitBinaryOp::Eq
            | ShortCircuitBinaryOp::NotEq
            | ShortCircuitBinaryOp::Lt
            | ShortCircuitBinaryOp::LtEq
            | ShortCircuitBinaryOp::Gt
            | ShortCircuitBinaryOp::GtEq => {
                for i in 0..num_rows {
                    selection_builder.append_value(!lhs_arr.is_null(i));
                }
            }
        }
        let selection_mask = selection_builder.finish();
        let true_count = selection_mask.true_count();

        if true_count == 0 {
            return match self.op {
                ShortCircuitBinaryOp::And | ShortCircuitBinaryOp::Or => {
                    Ok(ColumnarValue::Array(Arc::new(lhs_arr)))
                }
                _ => {
                    // Все строки LHS - NULL -> все результаты сравнения - NULL
                    Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                        num_rows,
                    ))))
                }
            };
        }

        let rhs_val = if true_count == num_rows {
            self.right.evaluate(batch)?
        } else {
            self.right.evaluate_selection(batch, &selection_mask)?
        };
        let rhs_arr = rhs_val.into_array(num_rows)?;

        // 4. Combine LHS and RHS
        let result = match self.op {
            ShortCircuitBinaryOp::And => {
                let lhs_bool = lhs_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let rhs_bool = rhs_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                arrow::compute::and_kleene(lhs_bool, rhs_bool)?
            }
            ShortCircuitBinaryOp::Or => {
                let lhs_bool = lhs_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let rhs_bool = rhs_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                arrow::compute::or_kleene(lhs_bool, rhs_bool)?
            }
            ShortCircuitBinaryOp::Eq => eq(&lhs_arr, &rhs_arr)?,
            ShortCircuitBinaryOp::NotEq => neq(&lhs_arr, &rhs_arr)?,
            ShortCircuitBinaryOp::Lt => lt(&lhs_arr, &rhs_arr)?,
            ShortCircuitBinaryOp::LtEq => lt_eq(&lhs_arr, &rhs_arr)?,
            ShortCircuitBinaryOp::Gt => gt(&lhs_arr, &rhs_arr)?,
            ShortCircuitBinaryOp::GtEq => gt_eq(&lhs_arr, &rhs_arr)?,
        };

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 2 {
            return Err(DataFusionError::Internal(
                "ShortCircuitBinaryExpr expects exactly 2 children".to_string(),
            ));
        }
        Ok(Arc::new(ShortCircuitBinaryExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.op,
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op_str = match self.op {
            ShortCircuitBinaryOp::And => "AND",
            ShortCircuitBinaryOp::Or => "OR",
            ShortCircuitBinaryOp::Eq => "=",
            ShortCircuitBinaryOp::NotEq => "!=",
            ShortCircuitBinaryOp::Lt => "<",
            ShortCircuitBinaryOp::LtEq => "<=",
            ShortCircuitBinaryOp::Gt => ">",
            ShortCircuitBinaryOp::GtEq => ">=",
        };
        write!(f, "(")?;
        self.left.fmt_sql(f)?;
        write!(f, " {} ", op_str)?;
        self.right.fmt_sql(f)?;
        write!(f, ")")
    }
}

/// Recursively replaces `BinaryExpr` with `Operator::And` and `Operator::Or`
/// in the physical expression tree with `ShortCircuitBinaryExpr`.
pub fn rewrite_short_circuit_binary(expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        let op = match binary.op() {
            Operator::And => Some(ShortCircuitBinaryOp::And),
            Operator::Or => Some(ShortCircuitBinaryOp::Or),
            Operator::Eq => Some(ShortCircuitBinaryOp::Eq),
            Operator::NotEq => Some(ShortCircuitBinaryOp::NotEq),
            Operator::Lt => Some(ShortCircuitBinaryOp::Lt),
            Operator::LtEq => Some(ShortCircuitBinaryOp::LtEq),
            Operator::Gt => Some(ShortCircuitBinaryOp::Gt),
            Operator::GtEq => Some(ShortCircuitBinaryOp::GtEq),
            _ => None,
        };
        if let Some(short_circuit_op) = op {
            let left = rewrite_short_circuit_binary(Arc::clone(binary.left()))?;
            let right = rewrite_short_circuit_binary(Arc::clone(binary.right()))?;
            return Ok(Arc::new(ShortCircuitBinaryExpr::new(
                left,
                right,
                short_circuit_op,
            )));
        }
    }

    let children = expr.children();
    if children.is_empty() {
        Ok(expr)
    } else {
        let new_children = children
            .into_iter()
            .map(|c| rewrite_short_circuit_binary(Arc::clone(c)))
            .collect::<Result<Vec<_>, _>>()?;
        expr.with_new_children(new_children)
    }
}
