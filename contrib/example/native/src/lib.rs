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
//! Registers a single `ContribOperatorPlanner` under `kind = "example-no-op"`. The
//! planner is intentionally trivial: it returns a clear `ContribError::Plan` so tests can
//! verify the full dispatch chain (JVM serde → ContribOp envelope → JNI → native planner
//! → contrib registry → this planner) without needing to actually execute anything.
//!
//! Real contribs (Delta, Hudi, etc.) replace `NoOpPlanner::plan` with a function that
//! decodes the contrib's own proto message from `payload` and constructs an
//! `ExecutionPlan` for the contrib's native operator.
//!
//! The whole crate is gated by `native/core/Cargo.toml`'s `contrib-example` feature flag.
//! Build core without that feature (`cargo build --no-default-features`) and zero bytes
//! of this crate end up in `libcomet`.

use std::sync::Arc;

use comet_contrib_spi::{
    register_contrib_planner, ContribError, ContribOperatorPlanner,
};
use datafusion::physical_plan::ExecutionPlan;

/// Stable identifier the example registers under. The Scala side writes this same string
/// into `ContribOp.kind` when building a payload for the example operator. Convention:
/// `<contrib-short-name>-<operator-short-name>`.
pub const EXAMPLE_NO_OP_KIND: &str = "example-no-op";

/// A planner that intentionally does no plan-building work. It exists only to prove the
/// dispatch chain is wired up correctly: tests construct an Operator with this kind, ship
/// it through JNI, and assert that the returned error mentions this string.
struct NoOpPlanner;

impl ContribOperatorPlanner for NoOpPlanner {
    fn plan(
        &self,
        _payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        Err(ContribError::Plan(format!(
            "comet-contrib-example: NoOpPlanner reached for kind={EXAMPLE_NO_OP_KIND:?}; \
             this is the expected sentinel for SPI dispatch tests"
        )))
    }
}

/// Registers `NoOpPlanner` against `EXAMPLE_NO_OP_KIND` at library-init time. Called by
/// the linker before `main`/`JNI_OnLoad` because of `#[ctor::ctor]`. Comet's main
/// `libcomet` is what gets loaded by the JVM; this constructor runs during its init.
#[ctor::ctor]
fn register() {
    log::info!(
        "comet-contrib-example: registering ContribOperatorPlanner kind={EXAMPLE_NO_OP_KIND:?}"
    );
    register_contrib_planner(EXAMPLE_NO_OP_KIND, Arc::new(NoOpPlanner));
}
