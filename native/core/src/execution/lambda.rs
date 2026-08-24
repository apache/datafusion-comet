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

//! Helpers for planning DataFusion higher-order functions (HOFs) coming
//! from Spark.
//!
//! The planner needs three things that don't belong in `planner.rs`:
//! 1. A stack of *lambda scopes* so nested `NamedLambdaVariable`s resolve
//!    by Spark `exprId` (immune to name shadowing / column collisions).
//! 2. A drop-guard that pops a scope on any exit path (`?`, panic-safe).
//! 3. A tiny `PhysicalExpr` wrapper that keeps *unused* lambda parameters
//!    visible in `children()` so `LambdaExpr::new`'s projection compaction
//!    stays consistent with the runtime batch layout.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{FieldRef, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::expressions::LambdaVariable;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use std::collections::HashSet;

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

/// Wrap a lambda body so that params which are *unused* in the body still
/// appear in `children()`.
///
/// Why: `LambdaExpr::new` compacts referenced `LambdaVariable` indices to
/// `0..k` while HOF `evaluate` builds the lambda batch as
/// `[projected env columns] ++ [ALL params]`. An unreferenced param would
/// otherwise let a nested lambda's variable get compacted into its runtime
/// slot, tripping DataFusion's exact field check.
///
/// Public helper: prefer [`pin_unused_params`] over constructing this
/// directly.
#[derive(Debug)]
pub(crate) struct LambdaParamsCapture {
    body: Arc<dyn PhysicalExpr>,
    unused_params: Vec<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Display for LambdaParamsCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.body)
    }
}

// `Arc<dyn PhysicalExpr>` blocks derive (see rust-lang/rust#78808).
// Equality/hash must cover *both* fields: two captures with the same body
// but different pinned params yield different projections and are not
// interchangeable under CSE / plan-equality.
impl PartialEq for LambdaParamsCapture {
    fn eq(&self, other: &Self) -> bool {
        self.body.eq(&other.body) && self.unused_params == other.unused_params
    }
}

impl Eq for LambdaParamsCapture {}

impl std::hash::Hash for LambdaParamsCapture {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.body.hash(state);
        self.unused_params.hash(state);
    }
}

impl PhysicalExpr for LambdaParamsCapture {
    fn data_type(&self, s: &Schema) -> Result<arrow::datatypes::DataType> {
        self.body.data_type(s)
    }

    fn nullable(&self, s: &Schema) -> Result<bool> {
        self.body.nullable(s)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Params are never evaluated — they exist only for the index scan
        // in `LambdaExpr::new` and the `transform_down` remapping.
        self.body.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut c = Vec::with_capacity(1 + self.unused_params.len());
        c.push(&self.body);
        c.extend(self.unused_params.iter());
        c
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let (body, params) = children.split_first().ok_or_else(|| {
            DataFusionError::Internal("LambdaParamsCapture requires children".into())
        })?;
        Ok(Arc::new(Self {
            body: Arc::clone(body),
            unused_params: params.to_vec(),
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.body.fmt_sql(f)
    }
}

/// Wrap `body` in a [`LambdaParamsCapture`] if there are params which the
/// body never references. Returns `body` unchanged otherwise.
///
/// `params` is `(column_index, field)` for every declared lambda param.
pub(crate) fn pin_unused_params(
    body: Arc<dyn PhysicalExpr>,
    params: &[(usize, FieldRef)],
) -> Arc<dyn PhysicalExpr> {
    let mut used_indices = HashSet::new();

    let _ = body.apply(|e| {
        if let Some(v) = e.downcast_ref::<LambdaVariable>() {
            used_indices.insert(v.index());
        }
        Ok(TreeNodeRecursion::Continue)
    });

    let unused_params: Vec<Arc<dyn PhysicalExpr>> = params
        .iter()
        .filter(|(idx, _)| !used_indices.contains(idx))
        .map(|(idx, field)| {
            Arc::new(LambdaVariable::new(*idx, Arc::clone(field))) as Arc<dyn PhysicalExpr>
        })
        .collect();

    if unused_params.is_empty() {
        body
    } else {
        Arc::new(LambdaParamsCapture {
            body,
            unused_params,
        })
    }
}
