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

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};

/// Metrics for CometSortMergeJoinExec, matching CometMetricNode.scala definitions.
#[derive(Debug, Clone)]
pub(super) struct SortMergeJoinMetrics {
    pub input_rows: Count,
    pub input_batches: Count,
    pub output_rows: Count,
    pub output_batches: Count,
    pub join_time: Time,
    pub peak_mem_used: Gauge,
    pub spill_count: Count,
    pub spilled_bytes: Count,
    pub spilled_rows: Count,
}

impl SortMergeJoinMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            input_rows: MetricBuilder::new(metrics).counter("input_rows", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            output_batches: MetricBuilder::new(metrics).counter("output_batches", partition),
            join_time: MetricBuilder::new(metrics).subset_time("join_time", partition),
            peak_mem_used: MetricBuilder::new(metrics).gauge("peak_mem_used", partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            spilled_rows: MetricBuilder::new(metrics).counter("spilled_rows", partition),
        }
    }

    pub fn update_peak_mem(&self, current_mem: usize) {
        if current_mem > self.peak_mem_used.value() {
            self.peak_mem_used.set(current_mem);
        }
    }
}
