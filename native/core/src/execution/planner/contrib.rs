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

//! Re-exports + core-side `ContribPlannerContext` adapter.
//!
//! The SPI trait + registry live in the standalone `comet-contrib-spi` crate so both
//! core and contribs can depend on them without forming a dependency cycle (core links
//! contribs via Cargo feature flags, contribs need the SPI types). This module:
//!
//!   1. re-exports the parts of the SPI core itself imports, so existing
//!      `crate::execution::planner::contrib::...` paths keep resolving;
//!   2. provides `CorePlannerContext`, a thin adapter that lets a `&PhysicalPlanner` be
//!      passed to contribs as a `&dyn ContribPlannerContext`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_proto::{spark_expression, spark_operator};

pub use comet_contrib_spi::lookup_contrib_planner_by_kind;
#[allow(unused_imports)] // surfaced for tests + diagnostics
pub use comet_contrib_spi::{
    register_contrib_planner, registered_contrib_kinds, ContribError, ContribOperatorPlanner,
    ContribPlannerContext, ParquetDatasourceParams,
};

use crate::execution::planner::PhysicalPlanner;
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::prepare_object_store_with_configs;

/// Adapter that exposes a `&PhysicalPlanner` (plus the session_ctx it carries) as a
/// `ContribPlannerContext`. Construction is cheap -- just borrows the planner. The
/// dispatcher creates one per ContribOp arm.
pub(crate) struct CorePlannerContext<'a> {
    pub(crate) planner: &'a PhysicalPlanner,
}

impl ContribPlannerContext for CorePlannerContext<'_> {
    fn session_ctx(&self) -> &Arc<SessionContext> {
        self.planner.session_ctx()
    }

    fn build_physical_expr(
        &self,
        expr: &spark_expression::Expr,
        input_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ContribError> {
        self.planner
            .create_expr(expr, input_schema)
            .map_err(|e| ContribError::Plan(format!("create_expr: {e}")))
    }

    fn convert_spark_schema(&self, fields: &[spark_operator::SparkStructField]) -> SchemaRef {
        super::convert_spark_types_to_arrow_schema(fields)
    }

    fn prepare_object_store(
        &self,
        url: String,
        configs: &HashMap<String, String>,
    ) -> Result<(ObjectStoreUrl, object_store::path::Path), ContribError> {
        prepare_object_store_with_configs(self.planner.session_ctx().runtime_env(), url, configs)
            .map_err(|e| ContribError::Plan(format!("prepare_object_store_with_configs: {e}")))
    }

    fn build_parquet_datasource_exec(
        &self,
        params: ParquetDatasourceParams,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        init_datasource_exec(
            params.required_schema,
            params.data_schema,
            params.partition_schema,
            params.object_store_url,
            params.file_groups,
            params.projection_vector,
            params.data_filters,
            params.default_values,
            &params.session_timezone,
            params.case_sensitive,
            params.return_null_struct_if_all_fields_missing,
            self.planner.session_ctx(),
            params.encryption_enabled,
            params.use_field_id,
            params.ignore_missing_field_id,
        )
        .map(|e| e as Arc<dyn ExecutionPlan>)
        .map_err(|e| ContribError::Plan(format!("init_datasource_exec: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::planner::PhysicalPlanner;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::object_store::ObjectStoreUrl;

    /// Production-build assertion: when no contrib feature is enabled, the registry
    /// must be empty. Catches an accidental re-introduction of a contrib into core's
    /// `default = [...]` feature set. Compiled out under any active contrib feature
    /// (the test binary always links its crate's dependencies, so the assertion would
    /// be wrong under those flags).
    ///
    /// MAINTENANCE: when adding a new `contrib-<name>` feature to `native/core/Cargo.toml`,
    /// extend the `not(any(...))` predicate below with the new feature name so the
    /// canary still compiles under that contrib's standalone CI matrix entry.
    #[cfg(not(any(feature = "contrib-example")))]
    #[test]
    fn production_build_has_no_contrib_planners_registered() {
        // Direct read through the SPI's public API. This test is the canary for
        // the contributor-guide claim that release builds carry zero contrib surface.
        let kinds = comet_contrib_spi::registered_contrib_kinds();
        assert!(
            kinds.is_empty(),
            "default cdylib leaked contrib planners: {kinds:?}. \
             Check native/core/Cargo.toml's `default = [...]` for contrib features."
        );
    }

    #[test]
    fn core_planner_context_builds_parquet_exec_with_expected_schema() {
        // Smoke test for the adapter: build a minimal DataSourceExec through the SPI
        // trait method and verify the schema flowed through. Catches a coarse class of
        // bugs where init_datasource_exec call-site args go out of order -- a swap that
        // sent `required_schema` into the `data_schema` slot would produce a different
        // output schema.
        let session_ctx = Arc::new(SessionContext::new());
        let planner = PhysicalPlanner::new(Arc::clone(&session_ctx), 0);
        let ctx = CorePlannerContext { planner: &planner };

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let url = ObjectStoreUrl::parse("file://").unwrap();
        let params = ParquetDatasourceParams::new(Arc::clone(&schema), url, vec![])
            .with_session_timezone("UTC")
            .with_case_sensitive(true);

        let exec = ctx
            .build_parquet_datasource_exec(params)
            .expect("adapter should build a DataSourceExec");

        // The exec's reported schema must equal the required_schema we passed in.
        let out_schema = exec.schema();
        assert_eq!(out_schema.fields().len(), 2);
        assert_eq!(out_schema.field(0).name(), "id");
        assert_eq!(out_schema.field(1).name(), "name");
    }

    #[test]
    fn core_planner_context_session_ctx_round_trip() {
        let session_ctx = Arc::new(SessionContext::new());
        let planner = PhysicalPlanner::new(Arc::clone(&session_ctx), 0);
        let ctx = CorePlannerContext { planner: &planner };
        // Arc identity check -- the contrib gets back the same SessionContext core was
        // built with, not a copy.
        assert!(Arc::ptr_eq(ctx.session_ctx(), &session_ctx));
    }

    #[test]
    fn core_planner_context_converts_empty_schema() {
        let session_ctx = Arc::new(SessionContext::new());
        let planner = PhysicalPlanner::new(Arc::clone(&session_ctx), 0);
        let ctx = CorePlannerContext { planner: &planner };
        let schema = ctx.convert_spark_schema(&[]);
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn core_planner_context_encryption_flag_reaches_init_datasource_exec() {
        // Cross-crate positional-arg-swap guard. `init_datasource_exec` takes five `bool`
        // parameters in a row (case_sensitive, return_null_struct_..., encryption_enabled,
        // use_field_id, ignore_missing_field_id); a swap of two of them at the call site
        // in `build_parquet_datasource_exec` would compile fine and break silently. We
        // exploit the asymmetry that `encryption_enabled=true` triggers an encryption-
        // factory lookup that fails when no factory is registered, while every other
        // bool being `true` keeps the call succeeding. So:
        //   * Default (all bools false) -> Ok
        //   * Same call with `encryption_enabled=true` -> Err on factory lookup
        // If a swap accidentally routed e.g. `use_field_id` into the encryption slot, the
        // "default" variant below would fail (because use_field_id is true here in the
        // params struct, and the swapped slot would now enable encryption).
        //
        // SCOPE: this test catches swaps that involve the `encryption_enabled` slot.
        // Swaps among the other four bools (case_sensitive / return_null_... /
        // use_field_id / ignore_missing_field_id) are NOT caught -- the two witnesses
        // below either set all four to true (witness #1) or all four to false
        // (witness #2), so a permutation among them is invisible. Adding a new bool to
        // ParquetDatasourceParams / init_datasource_exec should be accompanied by a new
        // asymmetry witness that exercises THAT new flag.
        let session_ctx = Arc::new(SessionContext::new());
        let planner = PhysicalPlanner::new(Arc::clone(&session_ctx), 0);
        let ctx = CorePlannerContext { planner: &planner };

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let url = ObjectStoreUrl::parse("file://").unwrap();

        // Witness #1: all five bools `true` EXCEPT encryption_enabled. Must succeed --
        // confirms case_sensitive / use_field_id / etc. are NOT routed into the
        // encryption slot.
        let no_encryption = ParquetDatasourceParams::new(Arc::clone(&schema), url.clone(), vec![])
            .with_case_sensitive(true)
            .with_return_null_struct_if_all_fields_missing(true)
            .with_use_field_id(true)
            .with_ignore_missing_field_id(true)
            .with_encryption_enabled(false);
        ctx.build_parquet_datasource_exec(no_encryption)
            .expect("encryption_enabled=false must not trigger factory lookup");

        // Witness #2: only encryption_enabled is true. Must fail with the encryption-factory
        // not-found error. Confirms encryption_enabled actually reaches the encryption slot.
        let with_encryption =
            ParquetDatasourceParams::new(Arc::clone(&schema), url, vec![]).with_encryption_enabled(true);
        let err = ctx
            .build_parquet_datasource_exec(with_encryption)
            .expect_err("encryption_enabled=true should fail without a factory");
        let msg = format!("{err}");
        assert!(
            msg.contains("encryption") || msg.contains("Encryption") || msg.contains("factory"),
            "expected encryption-factory error, got: {msg}"
        );
    }
}
