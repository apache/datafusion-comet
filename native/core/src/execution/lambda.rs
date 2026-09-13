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

use arrow::datatypes::FieldRef;
use datafusion::common::Result;

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
