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
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};

pub(super) struct ShufflePartitionerMetrics {
    /// metrics
    pub(super) baseline: BaselineMetrics,

    /// Time to perform repartitioning
    pub(super) repart_time: Time,

    /// Time encoding batches to IPC format
    pub(super) encode_time: Time,

    /// Time spent writing to disk. Maps to "shuffleWriteTime" in Spark SQL Metrics.
    pub(super) write_time: Time,

    /// Number of input batches
    pub(super) input_batches: Count,

    /// count of spills during the execution of the operator
    pub(super) spill_count: Count,

    /// total spilled bytes during the execution of the operator
    pub(super) spilled_bytes: Count,

    /// The original size of spilled data. Different to `spilled_bytes` because of compression.
    pub(super) data_size: Count,
}

impl ShufflePartitionerMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            repart_time: MetricBuilder::new(metrics).subset_time("repart_time", partition),
            encode_time: MetricBuilder::new(metrics).subset_time("encode_time", partition),
            write_time: MetricBuilder::new(metrics).subset_time("write_time", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            data_size: MetricBuilder::new(metrics).counter("data_size", partition),
        }
    }
}
