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

//! Delta Lake deletion-vector filter operator.
//!
//! Wraps a child `ExecutionPlan` (produced by `init_datasource_exec` over the
//! list of Delta parquet files) and applies Delta deletion vectors at the
//! batch level. One `Vec<u64>` of deleted row indexes per partition drives
//! the filter.
//!
//! Design notes:
//!
//!   - **One file per partition.** The planner match arm places each DV'd
//!     file in its own `FileGroup`, so when this operator sees partition
//!     `i`, it knows the full set of rows that `ParquetSource` is going to
//!     emit for that partition is exactly the physical rows of one file
//!     in physical order. That's the only assumption we rely on for the
//!     "subtract deleted indexes by tracking a running row offset" strategy
//!     to be correct.
//!
//!   - **Indexes are pre-materialized.** Kernel already turned the DV
//!     (inline bitmap / on-disk file / UUID reference) into a sorted
//!     `Vec<u64>` on the driver via `DvInfo::get_row_indexes`. That's what
//!     `plan_delta_scan` returns. We don't touch DV bytes on the executor
//!     side at all.
//!
//!   - **Filter uses arrow `filter_record_batch`.** Builds a per-batch
//!     `BooleanArray` mask where `true` means "keep". One mask per batch,
//!     allocated fresh — the batch sizes are small and allocation overhead
//!     is negligible compared with decoding parquet.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

/// Execution-plan wrapper that applies per-partition deletion-vector filters
/// to the output of a child parquet scan.
///
/// `deleted_row_indexes_by_partition[i]` is the sorted list of physical row
/// indexes to drop from partition `i`'s output. An empty vec means "no DV
/// for this partition — pass through untouched".
#[derive(Debug)]
pub struct DeltaDvFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// One entry per output partition. Length must match the input's
    /// partition count.
    deleted_row_indexes_by_partition: Vec<Vec<u64>>,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaDvFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_row_indexes_by_partition: Vec<Vec<u64>>,
    ) -> DFResult<Self> {
        let input_props = input.properties();
        let num_partitions = input_props.output_partitioning().partition_count();
        if deleted_row_indexes_by_partition.len() != num_partitions {
            return Err(DataFusionError::Internal(format!(
                "DeltaDvFilterExec: got {} DV entries for {} partitions",
                deleted_row_indexes_by_partition.len(),
                num_partitions
            )));
        }
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input_props.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            deleted_row_indexes_by_partition,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for DeltaDvFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let total_dv: usize = self
            .deleted_row_indexes_by_partition
            .iter()
            .map(|v| v.len())
            .sum();
        let dv_partitions = self
            .deleted_row_indexes_by_partition
            .iter()
            .filter(|v| !v.is_empty())
            .count();
        write!(
            f,
            "DeltaDvFilterExec: {dv_partitions} partitions with DVs, \
             {total_dv} total deleted rows"
        )
    }
}

impl ExecutionPlan for DeltaDvFilterExec {
    fn name(&self) -> &str {
        "DeltaDvFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DeltaDvFilterExec takes exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.deleted_row_indexes_by_partition.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let child_stream = self.input.execute(partition, context)?;
        let deleted = self
            .deleted_row_indexes_by_partition
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let metrics = DeltaDvFilterMetrics::new(&self.metrics, partition);
        metrics.num_deleted.add(deleted.len());
        Ok(Box::pin(DeltaDvFilterStream {
            inner: child_stream,
            deleted,
            current_row_offset: 0,
            next_delete_idx: 0,
            schema: self.input.schema(),
            baseline_metrics: metrics.baseline,
            rows_dropped_metric: metrics.rows_dropped,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct DeltaDvFilterMetrics {
    baseline: BaselineMetrics,
    num_deleted: Count,
    rows_dropped: Count,
}

impl DeltaDvFilterMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            num_deleted: MetricBuilder::new(metrics).counter("dv_rows_scheduled_delete", partition),
            rows_dropped: MetricBuilder::new(metrics).counter("dv_rows_dropped", partition),
        }
    }
}

struct DeltaDvFilterStream {
    inner: SendableRecordBatchStream,
    /// Sorted deleted row indexes for this partition.
    deleted: Vec<u64>,
    /// Physical row offset into the file that the NEXT batch starts at.
    current_row_offset: u64,
    /// Index into `deleted` of the first entry that hasn't been applied yet.
    /// `deleted[..next_delete_idx]` are all strictly less than
    /// `current_row_offset`.
    next_delete_idx: usize,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    rows_dropped_metric: Count,
}

impl DeltaDvFilterStream {
    /// Drop rows from `batch` whose physical row index is in the DV. Returns
    /// the filtered batch (possibly empty) and advances `current_row_offset`.
    fn apply(&mut self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let batch_rows = batch.num_rows() as u64;
        if batch_rows == 0 || self.deleted.is_empty() {
            self.current_row_offset += batch_rows;
            return Ok(batch);
        }

        let batch_start = self.current_row_offset;
        let batch_end = batch_start + batch_rows;

        // Fast-path: if no remaining deletes fall into this batch's row
        // range, pass it through untouched.
        if self.next_delete_idx >= self.deleted.len()
            || self.deleted[self.next_delete_idx] >= batch_end
        {
            self.current_row_offset = batch_end;
            return Ok(batch);
        }

        // Build the keep-mask. Walk forward through `deleted` popping entries
        // that fall inside [batch_start, batch_end).
        let mut mask_buf: Vec<bool> = vec![true; batch_rows as usize];
        let mut dropped: usize = 0;
        // Loop is safe: next_delete_idx < deleted.len() is checked by the while
        // condition, and deleted is sorted ascending by the kernel contract.
        while self.next_delete_idx < self.deleted.len() {
            let d = self.deleted[self.next_delete_idx];
            if d >= batch_end {
                break;
            }
            if d < batch_start {
                return Err(DataFusionError::Internal(format!(
                    "DV index {d} predates batch start {batch_start}"
                )));
            }
            let local = (d - batch_start) as usize;
            if local < mask_buf.len() && mask_buf[local] {
                mask_buf[local] = false;
                dropped += 1;
            }
            self.next_delete_idx += 1;
        }

        self.current_row_offset = batch_end;
        self.rows_dropped_metric.add(dropped);

        if dropped == 0 {
            return Ok(batch);
        }
        let mask = BooleanArray::from(mask_buf);
        filter_record_batch(&batch, &mask).map_err(DataFusionError::from)
    }
}

impl Stream for DeltaDvFilterStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        let result = match poll {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.apply(batch))),
            other => other,
        };
        self.baseline_metrics.record_poll(result)
    }
}

impl RecordBatchStream for DeltaDvFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
