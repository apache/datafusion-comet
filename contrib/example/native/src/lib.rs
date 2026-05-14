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

//! Worked reference implementation of a Comet contrib extension.
//!
//! Demonstrates two patterns future contribs will follow:
//!
//!   1. **Dispatch wiring** -- registers a `ContribOperatorPlanner` against a stable
//!      `kind` string at lib-init time via `#[ctor::ctor]`. The planner is called from
//!      core's `OpStruct::ContribOp` dispatcher with the contrib's payload bytes.
//!
//!   2. **Proto layer** -- the contrib has its own `proto/` directory with its own
//!      `.proto` schema (`example_op.proto`). `build.rs` runs `prost-build` over it;
//!      generated Rust types live under `src/generated/` (gitignored). The planner
//!      decodes the payload via `prost::Message::decode` -- the same way real contribs
//!      (Delta etc.) will.
//!
//! Two planner kinds are registered:
//!
//!   * `example-no-op`            -- returns a sentinel error. Tests use this to verify
//!                                   the dispatch chain end-to-end.
//!   * `example-constant-scan`    -- decodes an `ExampleConstantScan` payload, returns
//!                                   an `EmptyExec` sized by the payload's `row_count`.
//!                                   Real contribs (Delta) follow the same pattern,
//!                                   just with their own message and operator.
//!
//! The whole crate is gated by `native/core/Cargo.toml`'s `contrib-example` feature flag.
//! Build core without that feature (`cargo build --no-default-features`) and zero bytes
//! of this crate end up in `libcomet`.

use std::sync::Arc;

use comet_contrib_spi::{
    register_contrib_planner, ContribError, ContribOperatorPlanner, ContribPlannerContext,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use prost::Message;

/// Generated Rust types for the contrib's proto schema. `build.rs` writes the module
/// here at compile time; `src/generated/` is gitignored.
pub mod proto {
    include!(concat!("generated/", "comet.contrib.example.rs"));
}

/// Sentinel kind used by tests to verify dispatch reaches this contrib at all.
pub const EXAMPLE_NO_OP_KIND: &str = "example-no-op";

/// Kind for the proto-decoding constant-scan planner. Demonstrates the
/// proto-decode-and-build path real contribs will use.
pub const EXAMPLE_CONSTANT_SCAN_KIND: &str = "example-constant-scan";

/// A planner that intentionally does no plan-building work. Returns a sentinel error so
/// dispatch tests can assert the message reaches this code path. The payload is ignored;
/// children are ignored.
struct NoOpPlanner;

impl ContribOperatorPlanner for NoOpPlanner {
    fn plan(
        &self,
        _ctx: &dyn ContribPlannerContext,
        _payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        Err(ContribError::Plan(format!(
            "comet-contrib-example: NoOpPlanner reached for kind={EXAMPLE_NO_OP_KIND:?}; \
             this is the expected sentinel for SPI dispatch tests"
        )))
    }
}

/// Decodes the payload as an `ExampleConstantScan` proto and returns an `EmptyExec`
/// with a schema-less output. Real contribs use the same decode-then-build pattern --
/// they just decode richer messages and return richer execs.
struct ConstantScanPlanner;

impl ContribOperatorPlanner for ConstantScanPlanner {
    fn plan(
        &self,
        _ctx: &dyn ContribPlannerContext,
        payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        let msg = proto::ExampleConstantScan::decode(payload).map_err(|e| {
            ContribError::BadPayload(format!(
                "ExampleConstantScan: decode failed: {e}"
            ))
        })?;
        log::info!(
            "comet-contrib-example: ConstantScanPlanner produces {} synthetic rows",
            msg.row_count
        );
        // For the worked example we don't actually populate rows -- EmptyExec is fine to
        // demonstrate the build path. Real contribs return their domain-specific exec
        // (Delta returns the file scan + DV filter wrap).
        Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
    }
}

/// Registers all of the example contrib's planners against the contrib registry at
/// library-init time. `#[ctor::ctor]` runs this constructor before
/// `main`/`JNI_OnLoad`. Comet's `libcomet` cdylib is the single library the JVM loads;
/// this constructor runs during that one library's init.
#[ctor::ctor]
fn register() {
    log::info!(
        "comet-contrib-example: registering ContribOperatorPlanners \
         (no-op={EXAMPLE_NO_OP_KIND:?}, constant-scan={EXAMPLE_CONSTANT_SCAN_KIND:?})"
    );
    register_contrib_planner(EXAMPLE_NO_OP_KIND, Arc::new(NoOpPlanner));
    register_contrib_planner(EXAMPLE_CONSTANT_SCAN_KIND, Arc::new(ConstantScanPlanner));
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_contrib_spi::{lookup_contrib_planner_by_kind, ParquetDatasourceParams};
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion_comet_proto::{spark_expression, spark_operator};
    use std::collections::HashMap;

    /// Minimal `ContribPlannerContext` for unit-testing contrib planners that don't
    /// actually need to build a parquet exec. All methods that the tests don't exercise
    /// panic if invoked.
    struct TestCtx {
        ctx: Arc<SessionContext>,
    }
    impl ContribPlannerContext for TestCtx {
        fn session_ctx(&self) -> &Arc<SessionContext> {
            &self.ctx
        }
        fn build_physical_expr(
            &self,
            _expr: &spark_expression::Expr,
            _input_schema: SchemaRef,
        ) -> Result<Arc<dyn PhysicalExpr>, ContribError> {
            unimplemented!("TestCtx: build_physical_expr not used by this test")
        }
        fn convert_spark_schema(
            &self,
            _fields: &[spark_operator::SparkStructField],
        ) -> SchemaRef {
            unimplemented!("TestCtx: convert_spark_schema not used by this test")
        }
        fn prepare_object_store(
            &self,
            _url: String,
            _configs: &HashMap<String, String>,
        ) -> Result<ObjectStoreUrl, ContribError> {
            unimplemented!("TestCtx: prepare_object_store not used by this test")
        }
        fn build_parquet_datasource_exec(
            &self,
            _params: ParquetDatasourceParams<'_>,
        ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
            unimplemented!("TestCtx: build_parquet_datasource_exec not used by this test")
        }
    }

    fn test_ctx() -> TestCtx {
        TestCtx {
            ctx: Arc::new(SessionContext::new()),
        }
    }

    #[test]
    fn ctor_registers_both_planners() {
        // The #[ctor] above runs at process-init time for test binaries too.
        assert!(lookup_contrib_planner_by_kind(EXAMPLE_NO_OP_KIND).is_some());
        assert!(lookup_contrib_planner_by_kind(EXAMPLE_CONSTANT_SCAN_KIND).is_some());
    }

    #[test]
    fn constant_scan_decodes_payload_and_builds() {
        let payload = proto::ExampleConstantScan { row_count: 42 }.encode_to_vec();
        let planner = ConstantScanPlanner;
        let ctx = test_ctx();
        let plan = planner.plan(&ctx, &payload, vec![]).expect("decode + build");
        assert!(plan.schema().fields().is_empty());
    }

    #[test]
    fn constant_scan_surfaces_bad_payload() {
        let planner = ConstantScanPlanner;
        let ctx = test_ctx();
        let bad = b"not a valid proto";
        let err = planner
            .plan(&ctx, bad, vec![])
            .expect_err("garbage should fail decode");
        match err {
            ContribError::BadPayload(_) => {}
            other => panic!("expected BadPayload, got {other:?}"),
        }
    }
}
