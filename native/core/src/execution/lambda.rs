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
//! 2. A stack of scopes, popped on the `Ok`/`Err` paths of `with_scope`.
//!    Not unwind-safe: a panic unwinds through the whole planner and is
//!    caught at the JNI boundary (`try_unwrap_or_throw`), which tears down
//!    the planner and its scope stack together, so no stale scope survives.
//! 3. A tiny `PhysicalExpr` wrapper that keeps *unused* lambda parameters
//!    visible in `children()` so `LambdaExpr::new`'s projection compaction
//!    stays consistent with the runtime batch layout.

use std::cell::RefCell;
use std::collections::HashMap;

use arrow::datatypes::FieldRef;
use datafusion::common::Result;

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
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
