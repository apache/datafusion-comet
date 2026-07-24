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

//! Delta scan planning entry point.
//!
//! BUILD-GATE STUB: matches the exact signature core's `planner::delta_scan` shim calls
//! (`plan_delta_scan(&DeltaScan, &DeltaScanCommon, &SchemaRef, &SchemaRef)`), but returns a
//! clean `DataFusionError::NotImplemented` instead of building a scan. The shim maps that error
//! to a `GeneralError`, which surfaces on the JVM side as a fallback so the query runs on vanilla
//! Spark. The real kernel-read implementation replaces this in a later unit.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;

use datafusion_comet_proto::spark_operator::{DeltaScan, DeltaScanCommon};

/// Plan a native Delta scan. Stub: not yet implemented (build-gate unit).
///
/// The signature mirrors the real implementation so core's shim and the dispatcher arm compile
/// unchanged when the read path lands. `required_schema` / `partition_schema` are computed by core
/// (which owns the proto -> arrow converter) and handed in.
pub fn plan_delta_scan(
    _scan: &DeltaScan,
    _common: &DeltaScanCommon,
    _required_schema: &SchemaRef,
    _partition_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    Err(DataFusionError::NotImplemented(
        "contrib-delta native Delta read path is not yet implemented (build-gate stub)".to_string(),
    ))
}
