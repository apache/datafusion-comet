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

//! Metrics for the Grace Hash Join operator.

use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};

/// Production metrics for the Grace Hash Join operator.
pub(super) struct GraceHashJoinMetrics {
    /// Baseline metrics (output rows, elapsed compute)
    pub(super) baseline: BaselineMetrics,
    /// Time spent partitioning the build side
    pub(super) build_time: Time,
    /// Time spent partitioning the probe side
    pub(super) probe_time: Time,
    /// Number of spill events
    pub(super) spill_count: Count,
    /// Total bytes spilled to disk
    pub(super) spilled_bytes: Count,
    /// Number of build-side input rows
    pub(super) build_input_rows: Count,
    /// Number of build-side input batches
    pub(super) build_input_batches: Count,
    /// Number of probe-side input rows
    pub(super) input_rows: Count,
    /// Number of probe-side input batches
    pub(super) input_batches: Count,
    /// Number of output batches
    pub(super) output_batches: Count,
    /// Time spent in per-partition joins
    pub(super) join_time: Time,
}

impl GraceHashJoinMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            build_time: MetricBuilder::new(metrics).subset_time("build_time", partition),
            probe_time: MetricBuilder::new(metrics).subset_time("probe_time", partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            build_input_rows: MetricBuilder::new(metrics).counter("build_input_rows", partition),
            build_input_batches: MetricBuilder::new(metrics)
                .counter("build_input_batches", partition),
            input_rows: MetricBuilder::new(metrics).counter("input_rows", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
            output_batches: MetricBuilder::new(metrics).counter("output_batches", partition),
            join_time: MetricBuilder::new(metrics).subset_time("join_time", partition),
        }
    }
}
