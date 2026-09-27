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

use std::sync::Arc;

use datafusion_comet_proto::spark_operator::{ContribScan, LanceScan, Operator};
use prost::Message;

use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::{
    convert_spark_types_to_arrow_schema, PhysicalPlanner, PlanCreationResult,
};
use crate::execution::spark_plan::SparkPlan;

const LANCE_SCAN_TYPE_NAME: &str = "comet.contrib.lance.LanceScan";

pub(crate) fn try_plan_contrib_scan(
    _planner: &PhysicalPlanner,
    spark_plan: &Operator,
    contrib: &ContribScan,
) -> Option<PlanCreationResult> {
    if !contrib.type_url.ends_with(LANCE_SCAN_TYPE_NAME) {
        return None;
    }

    Some(
        LanceScan::decode(contrib.value.as_slice())
            .map_err(|e| GeneralError(format!("Failed to decode LanceScan: {e}")))
            .and_then(|scan| plan_lance_scan(spark_plan, &scan)),
    )
}

fn plan_lance_scan(spark_plan: &Operator, scan: &LanceScan) -> PlanCreationResult {
    let common = scan
        .common
        .as_ref()
        .ok_or_else(|| GeneralError("LanceScan missing common data".into()))?;
    let partition = scan
        .partition
        .as_ref()
        .ok_or_else(|| GeneralError("LanceScan missing partition data".into()))?;
    let output_schema = convert_spark_types_to_arrow_schema(common.projected_schema.as_slice());

    let exec = comet_contrib_lance::planner::plan_lance_scan(common, partition, &output_schema)
        .map_err(|e| GeneralError(e.to_string()))?;

    Ok((
        vec![],
        vec![],
        Arc::new(SparkPlan::new(spark_plan.plan_id, exec, vec![])),
    ))
}
