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
//! `comet-contrib-delta`, ...) depend on THIS crate, NOT on each other. This avoids a
//! cyclic dependency: core wires contribs in via Cargo feature flags, and contribs need
//! the SPI types to implement the trait. With the SPI in a third crate, the dependency
//! graph is a DAG.
//!
//! Surface:
//!   * [`ContribOperatorPlanner`]   -- the trait contribs implement.
//!   * [`ContribPlannerContext`]    -- the trait core implements; gives contribs access
//!                                     to the parquet exec builder, expression planner,
//!                                     object-store registration, and session context.
//!   * [`ParquetDatasourceParams`]  -- argument bundle for the parquet exec builder.
//!   * [`register_contrib_planner`] / [`lookup_contrib_planner_by_kind`] --
//!                                     process-wide registry, expected to be populated
//!                                     from a contrib's `#[ctor]`.
//!   * [`registered_contrib_kinds`] -- diagnostics.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::ScalarValue,
    datasource::listing::PartitionedFile,
    execution::{context::SessionContext, object_store::ObjectStoreUrl},
    physical_expr::PhysicalExpr,
    physical_plan::{expressions::Column, ExecutionPlan},
};
use datafusion_comet_proto::{spark_expression, spark_operator};

/// Implemented by each contrib. Called from core's planner when an `OpStruct::ContribOp`
/// with the contrib's `kind` is encountered.
///
/// The contract is intentionally minimal:
///   * `ctx` is a handle to core-side planner services (parquet exec builder,
///     expression planner, object-store registration, session context). Contribs reach
///     into core through this trait rather than depending on core directly, which keeps
///     the dependency graph acyclic.
///   * `payload` is the raw bytes from `ContribOp.payload`. The contrib decodes it into
///     whatever proto / serde format it uses internally; core never inspects.
///   * `children` is the list of already-built native children (in spark-plan child
///     order). The contrib uses these to build its `ExecutionPlan` if it needs child
///     inputs.
///   * The returned `Arc<dyn ExecutionPlan>` is the contrib's operator. Core wraps it
///     into a `SparkPlan` and threads it through the rest of the plan tree.
///
/// Implementations MUST be `Send + Sync` and idempotent -- the same `(payload, children)`
/// must always produce a functionally equivalent plan, so core can cache or re-plan.
pub trait ContribOperatorPlanner: Send + Sync {
    fn plan(
        &self,
        ctx: &dyn ContribPlannerContext,
        payload: &[u8],
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError>;
}

/// Argument bundle for [`ContribPlannerContext::build_parquet_datasource_exec`]. Mirrors
/// core's internal `init_datasource_exec` signature one-to-one, so the trait method is a
/// thin forward.
///
/// `#[non_exhaustive]` so adding fields in future is a minor SemVer bump, not a break.
/// Contribs construct via [`ParquetDatasourceParams::new`] (required fields only) +
/// `with_*` builder setters; never by struct-literal syntax.
#[non_exhaustive]
pub struct ParquetDatasourceParams<'a> {
    pub required_schema: SchemaRef,
    pub data_schema: Option<SchemaRef>,
    pub partition_schema: Option<SchemaRef>,
    pub object_store_url: ObjectStoreUrl,
    pub file_groups: Vec<Vec<PartitionedFile>>,
    pub projection_vector: Option<Vec<usize>>,
    pub data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    pub default_values: Option<HashMap<Column, ScalarValue>>,
    pub session_timezone: &'a str,
    pub case_sensitive: bool,
    pub return_null_struct_if_all_fields_missing: bool,
    pub encryption_enabled: bool,
    pub use_field_id: bool,
    pub ignore_missing_field_id: bool,
}

impl<'a> ParquetDatasourceParams<'a> {
    /// Minimal constructor with the parameters every parquet scan needs. All `Option`s
    /// default to `None`, all `bool`s to `false`, and `session_timezone` to `"UTC"`. Use
    /// the `with_*` setters to populate the rest.
    pub fn new(
        required_schema: SchemaRef,
        object_store_url: ObjectStoreUrl,
        file_groups: Vec<Vec<PartitionedFile>>,
    ) -> Self {
        Self {
            required_schema,
            data_schema: None,
            partition_schema: None,
            object_store_url,
            file_groups,
            projection_vector: None,
            data_filters: None,
            default_values: None,
            session_timezone: "UTC",
            case_sensitive: false,
            return_null_struct_if_all_fields_missing: false,
            encryption_enabled: false,
            use_field_id: false,
            ignore_missing_field_id: false,
        }
    }

    pub fn with_data_schema(mut self, schema: SchemaRef) -> Self {
        self.data_schema = Some(schema);
        self
    }
    pub fn with_partition_schema(mut self, schema: SchemaRef) -> Self {
        self.partition_schema = Some(schema);
        self
    }
    pub fn with_projection_vector(mut self, projection: Vec<usize>) -> Self {
        self.projection_vector = Some(projection);
        self
    }
    pub fn with_data_filters(mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        self.data_filters = Some(filters);
        self
    }
    pub fn with_default_values(mut self, values: HashMap<Column, ScalarValue>) -> Self {
        self.default_values = Some(values);
        self
    }
    pub fn with_session_timezone(mut self, tz: &'a str) -> Self {
        self.session_timezone = tz;
        self
    }
    pub fn with_case_sensitive(mut self, b: bool) -> Self {
        self.case_sensitive = b;
        self
    }
    pub fn with_return_null_struct_if_all_fields_missing(mut self, b: bool) -> Self {
        self.return_null_struct_if_all_fields_missing = b;
        self
    }
    pub fn with_encryption_enabled(mut self, b: bool) -> Self {
        self.encryption_enabled = b;
        self
    }
    pub fn with_use_field_id(mut self, b: bool) -> Self {
        self.use_field_id = b;
        self
    }
    pub fn with_ignore_missing_field_id(mut self, b: bool) -> Self {
        self.ignore_missing_field_id = b;
        self
    }
}

/// Planner services exposed by core to contribs. Core implements this trait against its
/// `PhysicalPlanner` + `SessionContext`; contribs receive a `&dyn ContribPlannerContext`
/// in their [`ContribOperatorPlanner::plan`] call and reach into core through it.
///
/// All trait methods are infallible at the trait-bound level but return `ContribError`
/// for runtime failures, so contribs can propagate without converting between error
/// types.
// Note: no `Send + Sync` bound -- `&dyn ContribPlannerContext` is only held for the
// duration of a synchronous `plan()` call, so it doesn't need to cross threads. The
// natural core-side impl borrows the `PhysicalPlanner` (which carries JNI handles that
// aren't `Send`), and adding the bound here would force an awkward `Arc<Mutex<...>>`
// dance for no gain.
pub trait ContribPlannerContext {
    /// The session context the plan is being built under. Contribs need this to register
    /// object stores on `runtime_env()` and to read session-level configs (timezone,
    /// case sensitivity, etc) that aren't already on `ParquetDatasourceParams`.
    fn session_ctx(&self) -> &Arc<SessionContext>;

    /// Convert a Catalyst-side Spark expression proto into a DataFusion `PhysicalExpr`
    /// against the given input schema. Used by file-scan contribs to convert data
    /// filters from their proto-side `Expr` form into the typed `PhysicalExpr`s that
    /// `ParquetSource` consumes.
    fn build_physical_expr(
        &self,
        expr: &spark_expression::Expr,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ContribError>;

    /// Convert a slice of Spark struct fields (the proto representation of a Spark
    /// schema) into an Arrow `SchemaRef`. This is a pure proto-to-arrow conversion --
    /// no side effects, no session state.
    fn convert_spark_schema(&self, fields: &[spark_operator::SparkStructField]) -> SchemaRef;

    /// Register an object store on the runtime env for the given URL's scheme + bucket,
    /// using `object_store_configs` for credentials / endpoint overrides. Returns
    /// `(ObjectStoreUrl, Path)`: the URL the contrib attaches to its `PartitionedFile`s,
    /// and the canonical path within that store (caller may discard if not needed --
    /// most file-scan contribs use it to set `partitioned_file.object_meta.location`).
    fn prepare_object_store(
        &self,
        any_file_url: String,
        object_store_configs: &HashMap<String, String>,
    ) -> Result<(ObjectStoreUrl, object_store::path::Path), ContribError>;

    /// Build a `DataSourceExec` over Comet's tuned `ParquetSource`. This is the single
    /// most important method on the trait -- every file-scan contrib (Delta, Iceberg)
    /// goes through here so the contrib doesn't have to rebuild Comet's parquet plumbing.
    fn build_parquet_datasource_exec(
        &self,
        params: ParquetDatasourceParams<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError>;
}

/// Error type returned by [`ContribOperatorPlanner::plan`] and the trait methods on
/// [`ContribPlannerContext`]. Kept distinct from core's `ExecutionError` so this crate
/// stays free of core's dependency tree. Core converts `ContribError` into its own
/// `ExecutionError` at the dispatch site.
///
/// `#[non_exhaustive]` so adding variants in the future is a minor SemVer bump, not a
/// break. Pattern matchers in contribs MUST include a wildcard arm.
#[non_exhaustive]
#[derive(Debug)]
pub enum ContribError {
    /// Generic failure. Use this for cases that don't fit the more specific variants.
    Plan(String),
    /// The contrib received a payload it couldn't decode (wrong proto schema, missing
    /// required field, etc.).
    BadPayload(String),
    /// The contrib received a child count it can't handle. `expected` is a free-form
    /// human description, conventionally a phrase like `"exactly 1"` or `"0 or 1"` so
    /// the error message reads `wrong child count: expected exactly 1, got 2`.
    WrongChildCount { expected: String, actual: usize },
}

impl std::fmt::Display for ContribError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContribError::Plan(msg) => write!(f, "{msg}"),
            ContribError::BadPayload(msg) => write!(f, "bad payload: {msg}"),
            ContribError::WrongChildCount { expected, actual } => {
                write!(f, "wrong child count: expected {expected}, got {actual}")
            }
            // Wildcard arm so the match stays exhaustive after future #[non_exhaustive]
            // additions. Reached only by `_` constructors that don't exist today.
            #[allow(unreachable_patterns)]
            _ => write!(f, "unknown contrib error"),
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

/// RAII guard that registers a planner for the lifetime of the guard and removes it on
/// drop. Use in tests that want a planner registered without polluting the process
/// registry for other tests running in parallel.
///
/// Not `Send` because dropping it requires the registry write lock; tests using this
/// guard should mark themselves `#[serial_test::serial]` if they assert on
/// `registered_contrib_kinds()` (whose snapshot is affected by other threads' guards).
#[cfg(any(test, feature = "test-utils"))]
pub struct ScopedContribPlannerRegistration {
    kind: String,
    previous: Option<Arc<dyn ContribOperatorPlanner>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl ScopedContribPlannerRegistration {
    /// Install `planner` under `kind` for the lifetime of the returned guard. The
    /// previously-registered planner (if any) is restored on drop.
    pub fn new(kind: impl Into<String>, planner: Arc<dyn ContribOperatorPlanner>) -> Self {
        let kind = kind.into();
        let mut guard = registry()
            .write()
            .expect("contrib planner registry poisoned");
        let previous = guard.insert(kind.clone(), planner);
        Self { kind, previous }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for ScopedContribPlannerRegistration {
    fn drop(&mut self) {
        let mut guard = registry()
            .write()
            .expect("contrib planner registry poisoned");
        match self.previous.take() {
            Some(prev) => {
                guard.insert(self.kind.clone(), prev);
            }
            None => {
                guard.remove(&self.kind);
            }
        }
    }
}

/// Clear the registry. **Test-only escape hatch.** Use [`ScopedContribPlannerRegistration`]
/// instead in any test that runs in parallel with other registry users -- this function
/// removes the entries every other concurrent test depends on.
#[cfg(any(test, feature = "test-utils"))]
pub fn _clear_for_test() {
    let mut guard = registry()
        .write()
        .expect("contrib planner registry poisoned");
    guard.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::Arc;

    struct AlwaysEmpty;
    impl ContribOperatorPlanner for AlwaysEmpty {
        fn plan(
            &self,
            _ctx: &dyn ContribPlannerContext,
            _payload: &[u8],
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
            Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
        }
    }

    // Use globally-unique kinds so concurrent tests in the same binary don't collide
    // through the process-wide registry. The `_test_` prefix is reserved for unit tests.

    #[test]
    fn unknown_kind_returns_none() {
        // Independent of any registrations: a kind no one ever registers stays None.
        let probe = "_test_definitely_unregistered_a8f3c1e";
        assert!(lookup_contrib_planner_by_kind(probe).is_none());
    }

    #[test]
    fn scoped_registration_round_trip() {
        let kind = "_test_scoped_registration_a";
        assert!(lookup_contrib_planner_by_kind(kind).is_none());
        {
            let _guard = ScopedContribPlannerRegistration::new(kind, Arc::new(AlwaysEmpty));
            assert!(lookup_contrib_planner_by_kind(kind).is_some());
        }
        // Dropping the guard removes the entry.
        assert!(lookup_contrib_planner_by_kind(kind).is_none());
    }

    #[test]
    fn scoped_registration_restores_previous() {
        let kind = "_test_scoped_registration_b";
        let _outer =
            ScopedContribPlannerRegistration::new(kind, Arc::new(AlwaysEmpty));
        {
            // Inner guard temporarily replaces the outer planner; drop restores outer.
            let _inner =
                ScopedContribPlannerRegistration::new(kind, Arc::new(AlwaysEmpty));
            assert!(lookup_contrib_planner_by_kind(kind).is_some());
        }
        assert!(lookup_contrib_planner_by_kind(kind).is_some());
    }

    #[test]
    fn parquet_datasource_params_constructor_defaults() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::object_store::ObjectStoreUrl;

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let url = ObjectStoreUrl::parse("file://").unwrap();
        let p = ParquetDatasourceParams::new(Arc::clone(&schema), url, vec![]);

        assert_eq!(p.required_schema.fields().len(), 1);
        assert!(p.data_schema.is_none());
        assert!(p.partition_schema.is_none());
        assert!(p.projection_vector.is_none());
        assert!(p.data_filters.is_none());
        assert!(p.default_values.is_none());
        assert_eq!(p.session_timezone, "UTC");
        assert!(!p.case_sensitive);
        assert!(!p.return_null_struct_if_all_fields_missing);
        assert!(!p.encryption_enabled);
        assert!(!p.use_field_id);
        assert!(!p.ignore_missing_field_id);
    }

    #[test]
    fn parquet_datasource_params_setters_apply() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::object_store::ObjectStoreUrl;

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let url = ObjectStoreUrl::parse("file://").unwrap();
        let p = ParquetDatasourceParams::new(Arc::clone(&schema), url, vec![])
            .with_data_schema(Arc::clone(&schema))
            .with_session_timezone("America/Los_Angeles")
            .with_case_sensitive(true)
            .with_use_field_id(true)
            .with_ignore_missing_field_id(true)
            .with_encryption_enabled(true);

        // Distinguishable bool tuple: a swap in `init_datasource_exec`'s arg order
        // would fail this assertion in core's planner::contrib tests.
        assert_eq!(p.session_timezone, "America/Los_Angeles");
        assert!(p.case_sensitive);
        assert!(!p.return_null_struct_if_all_fields_missing);
        assert!(p.encryption_enabled);
        assert!(p.use_field_id);
        assert!(p.ignore_missing_field_id);
        assert!(p.data_schema.is_some());
    }

    #[test]
    fn contrib_error_display_preserves_variant_info() {
        // The dispatcher wraps `e` via Display: `format!("contrib planner {kind:?}: {e}")`.
        // These cases assert each variant's discriminating info survives that path.
        let plan = ContribError::Plan("plan-context-message".into()).to_string();
        assert!(plan.contains("plan-context-message"));

        let bad = ContribError::BadPayload("decoding failed at field 7".into()).to_string();
        assert!(bad.starts_with("bad payload: "));
        assert!(bad.contains("decoding failed at field 7"));

        let wcc = ContribError::WrongChildCount {
            expected: "exactly 1".into(),
            actual: 3,
        }
        .to_string();
        assert!(wcc.contains("exactly 1"));
        assert!(wcc.contains("got 3"));
    }

    #[test]
    fn registered_contrib_kinds_reflects_current_state() {
        let kind = "_test_kinds_snapshot_only";
        let _guard = ScopedContribPlannerRegistration::new(kind, Arc::new(AlwaysEmpty));
        let kinds = registered_contrib_kinds();
        assert!(
            kinds.iter().any(|k| k == kind),
            "expected snapshot to include {kind:?}, got {kinds:?}"
        );
    }
}
