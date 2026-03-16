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

//! A transparent wrapper around a scan ExecutionPlan that tracks per-partition
//! memory allocation using jemalloc's per-thread counters.
//!
//! Uses `thread.allocatedp` / `thread.deallocatedp` which are thread-local
//! monotonic counters. By measuring deltas around each `poll_next()` call,
//! we get accurate per-partition allocation/deallocation numbers even with
//! concurrent partitions on different threads.
//!
//! A summary line is logged once when all partitions have completed.

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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

// --- Global aggregation counters ---

/// Sum of per-thread allocated bytes across all partitions
static AGG_THREAD_ALLOCATED: AtomicU64 = AtomicU64::new(0);
/// Sum of per-thread deallocated bytes across all partitions
static AGG_THREAD_DEALLOCATED: AtomicU64 = AtomicU64::new(0);
/// Sum of bytes_scanned across all partitions
static AGG_BYTES_SCANNED: AtomicU64 = AtomicU64::new(0);
/// Total rows across all partitions
static AGG_TOTAL_ROWS: AtomicU64 = AtomicU64::new(0);
/// Number of partitions that have completed
static AGG_COMPLETED_PARTITIONS: AtomicU64 = AtomicU64::new(0);

/// Per-thread allocated/deallocated from jemalloc.
#[cfg(feature = "jemalloc")]
fn jemalloc_thread_stats() -> (u64, u64) {
    use tikv_jemalloc_ctl::thread;
    let allocated = thread::allocatedp::read().map(|p| p.get()).unwrap_or(0);
    let deallocated = thread::deallocatedp::read().map(|p| p.get()).unwrap_or(0);
    (allocated, deallocated)
}

#[cfg(not(feature = "jemalloc"))]
fn jemalloc_thread_stats() -> (u64, u64) {
    (0, 0)
}

/// Wraps a child ExecutionPlan and tracks memory allocated/deallocated
/// during scan execution using jemalloc per-thread counters.
#[derive(Debug)]
pub struct ScanMemoryLogExec {
    child: Arc<dyn ExecutionPlan>,
    spark_partition: i32,
    num_partitions: usize,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl ScanMemoryLogExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, spark_partition: i32, num_partitions: usize) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(child.schema()),
            child.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            child,
            spark_partition,
            num_partitions,
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
            self.num_partitions,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // On partition 0: reset aggregates for this scan wave
        if self.spark_partition == 0 {
            AGG_THREAD_ALLOCATED.store(0, Ordering::Relaxed);
            AGG_THREAD_DEALLOCATED.store(0, Ordering::Relaxed);
            AGG_BYTES_SCANNED.store(0, Ordering::Relaxed);
            AGG_TOTAL_ROWS.store(0, Ordering::Relaxed);
            AGG_COMPLETED_PARTITIONS.store(0, Ordering::Relaxed);
        }

        // Capture thread-local counters before execute()
        let (thread_alloc_before, thread_dealloc_before) = jemalloc_thread_stats();

        let child_stream = self.child.execute(partition, Arc::clone(&context))?;

        // Capture after execute() to measure setup cost
        let (thread_alloc_after, thread_dealloc_after) = jemalloc_thread_stats();
        let execute_allocated = thread_alloc_after - thread_alloc_before;
        let execute_deallocated = thread_dealloc_after - thread_dealloc_before;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(ScanMemoryLogStream {
            child: child_stream,
            child_plan: Arc::clone(&self.child),
            context,
            spark_partition: self.spark_partition,
            num_partitions: self.num_partitions,
            logged: false,
            baseline_metrics,
            thread_total_allocated: execute_allocated,
            thread_total_deallocated: execute_deallocated,
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
    child_plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    spark_partition: i32,
    num_partitions: usize,
    logged: bool,
    baseline_metrics: BaselineMetrics,
    thread_total_allocated: u64,
    thread_total_deallocated: u64,
    batch_count: usize,
    total_rows: usize,
}

impl Stream for ScanMemoryLogStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (alloc_before, dealloc_before) = jemalloc_thread_stats();

        let result = Pin::new(&mut self.child).poll_next(cx);

        let (alloc_after, dealloc_after) = jemalloc_thread_stats();
        self.thread_total_allocated += alloc_after - alloc_before;
        self.thread_total_deallocated += dealloc_after - dealloc_before;

        match &result {
            Poll::Ready(Some(Ok(batch))) => {
                self.batch_count += 1;
                self.total_rows += batch.num_rows();
                self.baseline_metrics.record_output(batch.num_rows());
            }
            Poll::Ready(None) if !self.logged => {
                self.logged = true;
                let net = self.thread_total_allocated as i64
                    - self.thread_total_deallocated as i64;

                let bytes_scanned = self
                    .child_plan
                    .metrics()
                    .map(|m| {
                        m.iter()
                            .filter(|metric| metric.value().name() == "bytes_scanned")
                            .map(|metric| metric.value().as_usize())
                            .sum::<usize>()
                    })
                    .unwrap_or(0);

                // Per-partition log line (thread-level stats only)
                log::info!(
                    "ScanMemoryLogExec spark_partition={}: scan complete, \
                     batches={}, rows={}, bytes_scanned={}, \
                     memory_pool_reserved={}, \
                     thread: allocated={}, deallocated={}, net={}",
                    self.spark_partition,
                    self.batch_count,
                    self.total_rows,
                    bytes_scanned,
                    self.context.memory_pool().reserved(),
                    self.thread_total_allocated,
                    self.thread_total_deallocated,
                    net,
                );

                // Accumulate into global aggregates
                AGG_THREAD_ALLOCATED
                    .fetch_add(self.thread_total_allocated, Ordering::Relaxed);
                AGG_THREAD_DEALLOCATED
                    .fetch_add(self.thread_total_deallocated, Ordering::Relaxed);
                AGG_BYTES_SCANNED.fetch_add(bytes_scanned as u64, Ordering::Relaxed);
                AGG_TOTAL_ROWS.fetch_add(self.total_rows as u64, Ordering::Relaxed);
                let completed =
                    AGG_COMPLETED_PARTITIONS.fetch_add(1, Ordering::Relaxed) + 1;

                // Log summary only once when all partitions are done
                if completed == self.num_partitions as u64 {
                    let agg_alloc = AGG_THREAD_ALLOCATED.load(Ordering::Relaxed);
                    let agg_dealloc = AGG_THREAD_DEALLOCATED.load(Ordering::Relaxed);
                    log::info!(
                        "ScanMemoryLogExec SUMMARY (partitions={}): \
                         total_rows={}, total_bytes_scanned={}, \
                         sum_thread_allocated={}, sum_thread_deallocated={}, \
                         sum_thread_net={}",
                        completed,
                        AGG_TOTAL_ROWS.load(Ordering::Relaxed),
                        AGG_BYTES_SCANNED.load(Ordering::Relaxed),
                        agg_alloc,
                        agg_dealloc,
                        agg_alloc as i64 - agg_dealloc as i64,
                    );
                }
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
