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
        params: ParquetDatasourceParams<'_>,
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
            params.session_timezone,
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
}
