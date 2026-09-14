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

//! Delta handler for the generic `OpStruct::ContribScan` dispatcher, feature-gated behind
//! `contrib-delta`.
//!
//! Thin bridge between core's plan-tree builder and the [`comet_contrib_delta`] crate. ALL
//! Delta-specific scan planning lives in `comet_contrib_delta::planner::plan_delta_scan`; this
//! shim only computes the requested/partition Arrow schemas (core owns the proto -> arrow type
//! converter, used across the planner) and wraps the contrib's `ExecutionPlan` in a `SparkPlan`.
//! It lives in core (not the contrib crate) because the converter is core's `pub(crate)` and a
//! `contrib -> core` dependency would cycle with core's optional `contrib-delta` dep. Compiled
//! only under `--features contrib-delta`; default builds carry zero Delta surface.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_comet_proto::spark_operator::{ContribScan, DeltaScan, Operator};
use prost::Message;

use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::convert_spark_types_to_arrow_schema;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::execution::spark_plan::SparkPlan;

/// Type name the Delta contrib claims within the `ContribScan` envelope. The JVM packs the
/// `DeltaScan` with a `type_url` of `type.googleapis.com/comet.contrib.delta.DeltaScan`, and we
/// recognise our messages by this suffix. Kept local to the contrib module so core's dispatcher
/// names no Delta type.
///
/// Deliberately namespaced to the contrib rather than to the message's *current* proto package
/// (`spark.spark_operator`, core's): the `DeltaScan` definitions still live in core's
/// `operator.proto` only because a contrib proto build pipeline does not exist yet, and are meant
/// to move under `contrib/delta/proto`. Keying dispatch on a contrib-owned name makes that
/// relocation invisible here and on the wire -- the identifier reflects who owns the message, not
/// where the `.proto` file happens to sit today.
const DELTA_SCAN_TYPE_NAME: &str = "comet.contrib.delta.DeltaScan";

/// Contrib entry point for the `OpStruct::ContribScan` dispatcher. Returns `Some(result)` when the
/// envelope carries a Delta scan this build handles, or `None` when the `type_url` belongs to some
/// other contrib -- letting core fall through to its "no contrib handles this" error. Decoding and
/// all Delta-specific knowledge live here, not in core's dispatcher.
pub(crate) fn try_plan_contrib_scan(
    planner: &PhysicalPlanner,
    spark_plan: &Operator,
    contrib: &ContribScan,
) -> Option<PlanCreationResult> {
    if !contrib.type_url.ends_with(DELTA_SCAN_TYPE_NAME) {
        return None;
    }
    Some(
        DeltaScan::decode(contrib.value.as_slice())
            .map_err(|e| GeneralError(format!("Failed to decode DeltaScan from contrib_scan: {e}")))
            .and_then(|scan| plan_delta_scan(planner, spark_plan, &scan)),
    )
}

pub(crate) fn plan_delta_scan(
    // The kernel-read path doesn't need the planner (the old ParquetSource path used it to build
    // pushed-down filter exprs); kept in the signature for the dispatcher call site.
    _planner: &PhysicalPlanner,
    spark_plan: &Operator,
    scan: &DeltaScan,
) -> PlanCreationResult {
    let common = scan
        .common
        .as_ref()
        .ok_or_else(|| GeneralError("DeltaScan missing common data".into()))?;

    // Core owns the proto -> arrow schema converter (`to_arrow_datatype`, used across the planner),
    // so compute the requested + partition schemas here and hand them to the contrib, which owns all
    // Delta-specific scan planning (#77).
    let required_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
    let partition_schema: SchemaRef =
        convert_spark_types_to_arrow_schema(common.partition_schema.as_slice());

    let exec = comet_contrib_delta::planner::plan_delta_scan(
        scan,
        common,
        &required_schema,
        &partition_schema,
    )
    .map_err(|e| GeneralError(format!("{e}")))?;

    Ok((
        vec![],
        vec![],
        Arc::new(SparkPlan::new(spark_plan.plan_id, exec, vec![])),
    ))
}
