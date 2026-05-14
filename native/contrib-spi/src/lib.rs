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

//! Thin SPI crate shared between Comet's core and every contrib crate.
//!
//! Both core (`datafusion-comet`) and individual contribs (`comet-contrib-example`,
//! eventually `comet-contrib-delta`) depend on THIS crate, NOT on each other. This avoids
//! a cyclic dependency: core wires contribs in via Cargo feature flags, and contribs need
//! the SPI types to implement the trait. With the SPI in a third crate, the dependency
//! graph is a DAG.
//!
//! Surface:
//!   * [`ContribOperatorPlanner`] — the trait contribs implement.
//!   * [`register_contrib_planner`] / [`lookup_contrib_planner_by_kind`] —
//!     process-wide registry, expected to be populated from a contrib's `#[ctor]`.
//!   * [`registered_contrib_kinds`] — diagnostics.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use datafusion::physical_plan::ExecutionPlan;

/// Implemented by each contrib. Called from core's planner when an `OpStruct::ContribOp`
/// with the contrib's `kind` is encountered.
///
/// The contract is intentionally minimal:
///   * `payload` is the raw bytes from `ContribOp.payload`. The contrib decodes it into
///     whatever proto / serde format it uses internally; core never inspects.
///   * `children` is the list of already-built native children (in spark-plan child
///     order). The contrib uses these to build its `ExecutionPlan` if it needs child
///     inputs.
///   * The returned `Arc<dyn ExecutionPlan>` is the contrib's operator. Core wraps it
///     into a `SparkPlan` and threads it through the rest of the plan tree.
///
/// Implementations MUST be `Send + Sync` and idempotent — the same `(payload, children)`
/// must always produce a functionally equivalent plan, so core can cache or re-plan.
pub trait ContribOperatorPlanner: Send + Sync {
    fn plan(
        &self,
        payload: &[u8],
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError>;
}

/// Error type returned by [`ContribOperatorPlanner::plan`]. Kept distinct from core's
/// `ExecutionError` so this crate stays free of core's dependency tree. Core converts
/// `ContribError` into its own `ExecutionError` at the dispatch site.
#[derive(Debug)]
pub enum ContribError {
    /// Generic failure. Use this for cases that don't fit the more specific variants.
    Plan(String),
    /// The contrib received a payload it couldn't decode (wrong proto schema, missing
    /// required field, etc.).
    BadPayload(String),
    /// The contrib received a child count it can't handle (e.g. a binary operator wired
    /// to one child).
    WrongChildCount {
        expected: &'static str,
        actual: usize,
    },
}

impl std::fmt::Display for ContribError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContribError::Plan(msg) => write!(f, "{msg}"),
            ContribError::BadPayload(msg) => write!(f, "bad payload: {msg}"),
            ContribError::WrongChildCount { expected, actual } => {
                write!(f, "wrong child count: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for ContribError {}

/// Process-wide registry of contrib operator planners, keyed by `ContribOp.kind`.
fn registry() -> &'static RwLock<HashMap<String, Arc<dyn ContribOperatorPlanner>>> {
    static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn ContribOperatorPlanner>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register a contrib operator planner under the given `kind` identifier. Last-write-wins
/// on duplicates (logged as a warning). Thread-safe; intended to be called from a
/// contrib's `#[ctor]` constructor at library-init time.
pub fn register_contrib_planner(
    kind: impl Into<String>,
    planner: Arc<dyn ContribOperatorPlanner>,
) {
    let kind = kind.into();
    let mut guard = registry()
        .write()
        .expect("contrib planner registry poisoned");
    if guard.contains_key(&kind) {
        log::warn!(
            "register_contrib_planner: replacing existing planner for kind={kind:?}; \
             second registration usually indicates a misconfigured test harness"
        );
    }
    guard.insert(kind, planner);
}

/// Look up the contrib planner registered for `kind`, or `None` if no contrib is loaded
/// for that operator. Core's dispatcher uses this to route `OpStruct::ContribOp` payloads.
pub fn lookup_contrib_planner_by_kind(kind: &str) -> Option<Arc<dyn ContribOperatorPlanner>> {
    let guard = registry()
        .read()
        .expect("contrib planner registry poisoned");
    guard.get(kind).cloned()
}

/// Return a snapshot of all registered contrib kinds, for diagnostics and tests.
pub fn registered_contrib_kinds() -> Vec<String> {
    let guard = registry()
        .read()
        .expect("contrib planner registry poisoned");
    let mut kinds: Vec<String> = guard.keys().cloned().collect();
    kinds.sort();
    kinds
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::Arc;

    struct AlwaysEmpty;
    impl ContribOperatorPlanner for AlwaysEmpty {
        fn plan(
            &self,
            _payload: &[u8],
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
            Ok(Arc::new(EmptyExec::new(Arc::new(
                datafusion::arrow::datatypes::Schema::empty(),
            ))))
        }
    }

    #[test]
    fn register_and_lookup() {
        register_contrib_planner("test-spi-kind-a", Arc::new(AlwaysEmpty));
        register_contrib_planner("test-spi-kind-b", Arc::new(AlwaysEmpty));
        assert!(lookup_contrib_planner_by_kind("test-spi-kind-a").is_some());
        assert!(lookup_contrib_planner_by_kind("test-spi-kind-b").is_some());
        assert!(lookup_contrib_planner_by_kind("test-spi-kind-c").is_none());
        let kinds = registered_contrib_kinds();
        assert!(kinds.contains(&"test-spi-kind-a".to_string()));
        assert!(kinds.contains(&"test-spi-kind-b".to_string()));
    }
}
