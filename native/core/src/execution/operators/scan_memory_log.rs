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

//! A transparent wrapper around a scan ExecutionPlan that logs memory stats
//! when the scan stream reaches EOF. Uses jemalloc stats (when available)
//! to track how much memory was allocated during the scan phase.
//!
//! Captures three jemalloc snapshots:
//! 1. Before `child.execute()` — baseline
//! 2. After `child.execute()` — cost of stream setup (file opens, metadata reads)
//! 3. On stream EOF — total cost of the entire scan including all data reads
//!
//! Note: jemalloc `allocated` is process-wide, so concurrent partitions will
//! see each other's allocations. The deltas are approximate but still useful
//! for identifying which partitions drive memory growth.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use std::any::Any;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

/// Global counters to aggregate stats across all partitions.
/// These are process-wide — multiple concurrent queries will aggregate together.
static TOTAL_DELTA_SUM: AtomicUsize = AtomicUsize::new(0);
static MAX_PEAK: AtomicUsize = AtomicUsize::new(0);
static COMPLETED_PARTITIONS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_ROWS_SUM: AtomicUsize = AtomicUsize::new(0);

/// Read jemalloc `stats::allocated` (bytes currently allocated by the application).
/// Returns 0 if jemalloc feature is not enabled.
#[cfg(feature = "jemalloc")]
fn jemalloc_allocated() -> usize {
    use tikv_jemalloc_ctl::{epoch, stats};
    epoch::advance().ok();
    stats::allocated::read().unwrap_or(0)
}

#[cfg(not(feature = "jemalloc"))]
fn jemalloc_allocated() -> usize {
    0
}

/// Wraps a child ExecutionPlan and logs memory stats when the child's stream
/// reaches EOF. Passes through all batches unchanged.
///
/// `spark_partition` is the actual Spark partition index (not the DataFusion
/// partition, which is always 0 in Comet's per-partition execution model).
#[derive(Debug)]
pub struct ScanMemoryLogExec {
    child: Arc<dyn ExecutionPlan>,
    spark_partition: i32,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl ScanMemoryLogExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, spark_partition: i32) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(child.schema()),
            child.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            child,
            spark_partition,
            cache,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }
}

impl DisplayAs for ScanMemoryLogExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ScanMemoryLogExec: spark_partition={}",
                    self.spark_partition
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for ScanMemoryLogExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ScanMemoryLogExec::new(
            children.remove(0),
            self.spark_partition,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Snapshot before child.execute() — baseline
        let allocated_before_execute = jemalloc_allocated();

        let child_stream = self.child.execute(partition, Arc::clone(&context))?;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(ScanMemoryLogStream {
            child: child_stream,
            context,
            spark_partition: self.spark_partition,
            logged: false,
            baseline_metrics,
            allocated_before_scan: allocated_before_execute,
            peak_allocated: allocated_before_execute,
            batch_count: 0,
            total_rows: 0,
        }))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn name(&self) -> &str {
        "ScanMemoryLogExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct ScanMemoryLogStream {
    child: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    spark_partition: i32,
    logged: bool,
    baseline_metrics: BaselineMetrics,
    /// jemalloc allocated bytes captured before child.execute()
    allocated_before_scan: usize,
    /// High-water mark of jemalloc allocated bytes seen during polling
    peak_allocated: usize,
    /// Number of batches consumed through this stream
    batch_count: usize,
    /// Total rows consumed through this stream
    total_rows: usize,
}

impl Stream for ScanMemoryLogStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.child).poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(batch))) => {
                self.batch_count += 1;
                self.total_rows += batch.num_rows();
                self.baseline_metrics.record_output(batch.num_rows());
                // Track peak allocation across all batches
                let current = jemalloc_allocated();
                if current > self.peak_allocated {
                    self.peak_allocated = current;
                }
            }
            Poll::Ready(None) if !self.logged => {
                self.logged = true;
                let pool = self.context.memory_pool();
                let allocated_at_eof = jemalloc_allocated();
                let total_delta =
                    allocated_at_eof.saturating_sub(self.allocated_before_scan);
                let peak_delta =
                    self.peak_allocated.saturating_sub(self.allocated_before_scan);

                // Accumulate into global counters
                TOTAL_DELTA_SUM.fetch_add(total_delta, Ordering::Relaxed);
                TOTAL_ROWS_SUM.fetch_add(self.total_rows, Ordering::Relaxed);
                // Update max peak (atomic CAS loop)
                let mut current_max = MAX_PEAK.load(Ordering::Relaxed);
                while self.peak_allocated > current_max {
                    match MAX_PEAK.compare_exchange_weak(
                        current_max,
                        self.peak_allocated,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(actual) => current_max = actual,
                    }
                }
                let completed = COMPLETED_PARTITIONS.fetch_add(1, Ordering::Relaxed) + 1;

                log::info!(
                    "ScanMemoryLogExec spark_partition={}: scan complete, \
                     batches={}, rows={}, \
                     memory_pool_reserved={}, \
                     jemalloc_allocated: before={}, at_eof={}, peak={}, \
                     total_delta={}, peak_delta={} | \
                     aggregate: completed_partitions={}, sum_total_delta={}, \
                     max_peak={}, sum_rows={}",
                    self.spark_partition,
                    self.batch_count,
                    self.total_rows,
                    pool.reserved(),
                    self.allocated_before_scan,
                    allocated_at_eof,
                    self.peak_allocated,
                    total_delta,
                    peak_delta,
                    completed,
                    TOTAL_DELTA_SUM.load(Ordering::Relaxed),
                    MAX_PEAK.load(Ordering::Relaxed),
                    TOTAL_ROWS_SUM.load(Ordering::Relaxed),
                );
            }
            _ => {}
        }
        result
    }
}

impl RecordBatchStream for ScanMemoryLogStream {
    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }
}
