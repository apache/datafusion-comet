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

//! Exposes a Comet native plan across a `datafusion-ffi` boundary so that a
//! process compiled against a different DataFusion version (e.g. a Ballista
//! executor) can execute it without linking Comet's Rust crates.
//!
//! The input is a serialized Comet `Operator` plan whose leaves are
//! `NativeScan` (native Parquet), so no JVM-fed inputs are required.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_operator::Operator;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use prost::Message;
use tokio::runtime::Handle;

use super::planner::PhysicalPlanner;

/// Decode a Comet `Operator` protobuf, build the native DataFusion plan with
/// Comet's own planner, and wrap the root as an `FFI_ExecutionPlan`.
///
/// `runtime` is the Tokio runtime handle the foreign consumer should use to
/// drive async execution across the boundary.
pub fn comet_ffi_plan_from_proto(
    proto_bytes: &[u8],
    runtime: Option<Handle>,
) -> Result<FFI_ExecutionPlan, String> {
    let op = Operator::decode(proto_bytes)
        .map_err(|e| format!("failed to decode Comet Operator proto: {e}"))?;

    let session_ctx = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(session_ctx, 0);

    // NativeScan leaves read Parquet directly, so no JVM input sources are needed.
    let mut inputs = Vec::new();
    let (_scans, _shuffle_scans, spark_plan) = planner
        .create_plan(&op, &mut inputs, 1)
        .map_err(|e| format!("failed to build native plan: {e}"))?;

    let plan: Arc<dyn ExecutionPlan> = Arc::clone(&spark_plan.native_plan);
    Ok(FFI_ExecutionPlan::new(plan, runtime))
}
