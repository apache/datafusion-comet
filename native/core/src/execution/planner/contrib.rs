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

//! Registry for contrib operator planners.
//!
//! Contribs are extension crates that ship Spark plan operators living outside core (Delta,
//! example, future Hudi/DeltaSharing, etc.). They link into core's cdylib as Cargo `rlib`
//! dependencies enabled via core's Cargo feature flags (e.g. `contrib-delta`,
//! `contrib-example`). At library-init time (typically via `#[ctor]` in the contrib crate),
//! each contrib calls [`register_contrib_planner`] with a stable `kind` string and an
//! [`OperatorBuilder`] implementation. Core's `OpStruct::ContribOp` dispatcher arm then
//! looks up the planner by `kind` and delegates plan construction to it.
//!
//! See `docs/contrib-delta-migration-plan.md` for the broader architecture.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use super::operator_registry::OperatorBuilder;

/// Process-wide registry of contrib operator planners, keyed by `ContribOp.kind`.
///
/// Implemented as an `OnceLock<RwLock<...>>` so:
///   * The OnceLock makes lazy first-touch initialisation thread-safe.
///   * The inner RwLock allows multiple contribs to register concurrently at lib-init time
///     (e.g. independent `#[ctor]` invocations) without blocking subsequent reads.
///
/// Registration is cheap and happens once per contrib per process; lookups are read-mostly.
fn registry() -> &'static RwLock<HashMap<String, Arc<dyn OperatorBuilder>>> {
    static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn OperatorBuilder>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register a contrib operator planner under the given `kind` identifier.
///
/// `kind` must match the value the contrib's JVM-side serde writes into the
/// `ContribOp.kind` proto field. Convention: lowercase-hyphenated, prefixed by the
/// contrib's short name (e.g. `delta-scan`, `example-constant-scan`).
///
/// If a planner is already registered for `kind`, this REPLACES it and logs a warning.
/// Last-write-wins lets test harnesses re-register without restarting the JVM, and
/// production contribs only ever register once per process.
///
/// Thread-safe; intended to be called from a contrib's `#[ctor]` at library init.
pub fn register_contrib_planner(kind: impl Into<String>, planner: Arc<dyn OperatorBuilder>) {
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
/// for that operator. The native dispatcher arm in `planner.rs` uses this to route
/// `OpStruct::ContribOp` payloads.
pub fn lookup_contrib_planner_by_kind(kind: &str) -> Option<Arc<dyn OperatorBuilder>> {
    let guard = registry()
        .read()
        .expect("contrib planner registry poisoned");
    guard.get(kind).cloned()
}

/// Return a snapshot of all registered contrib kinds. Useful for diagnostics and tests.
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
    use crate::execution::operators::{ExecutionError, ScanExec, ShuffleScanExec};
    use crate::execution::planner::PhysicalPlanner;
    use crate::execution::spark_plan::SparkPlan;
    use datafusion_comet_proto::spark_operator::Operator;
    use jni::objects::{Global, JObject};

    /// Trivial test planner that returns a not-implemented error. We don't need a real
    /// ExecutionPlan to validate the registry; only identity-by-kind matters.
    struct NoopBuilder(&'static str);
    impl OperatorBuilder for NoopBuilder {
        fn build(
            &self,
            _spark_plan: &Operator,
            _inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
            _partition_count: usize,
            _planner: &PhysicalPlanner,
        ) -> Result<(Vec<ScanExec>, Vec<ShuffleScanExec>, Arc<SparkPlan>), ExecutionError> {
            Err(ExecutionError::GeneralError(format!(
                "NoopBuilder({}) -- registry round-trip ok",
                self.0
            )))
        }
    }

    #[test]
    fn register_and_lookup_round_trips_by_kind() {
        register_contrib_planner("test-kind-a", Arc::new(NoopBuilder("a")));
        register_contrib_planner("test-kind-b", Arc::new(NoopBuilder("b")));

        assert!(lookup_contrib_planner_by_kind("test-kind-a").is_some());
        assert!(lookup_contrib_planner_by_kind("test-kind-b").is_some());
        assert!(lookup_contrib_planner_by_kind("test-kind-c").is_none());

        let kinds = registered_contrib_kinds();
        assert!(kinds.contains(&"test-kind-a".to_string()));
        assert!(kinds.contains(&"test-kind-b".to_string()));
    }

    #[test]
    fn registering_existing_kind_replaces() {
        register_contrib_planner("test-replace-kind", Arc::new(NoopBuilder("first")));
        // Second registration should not panic; replaces silently (with a warn-level log).
        register_contrib_planner("test-replace-kind", Arc::new(NoopBuilder("second")));
        assert!(lookup_contrib_planner_by_kind("test-replace-kind").is_some());
    }
}
