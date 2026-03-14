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

/// Global counters to aggregate stats across all partitions.
static AGG_THREAD_ALLOCATED: AtomicU64 = AtomicU64::new(0);
static AGG_THREAD_DEALLOCATED: AtomicU64 = AtomicU64::new(0);
static AGG_PEAK_PROCESS_ALLOCATED: AtomicU64 = AtomicU64::new(0);
static AGG_COMPLETED_PARTITIONS: AtomicU64 = AtomicU64::new(0);
static AGG_TOTAL_ROWS: AtomicU64 = AtomicU64::new(0);

/// Per-thread allocated/deallocated from jemalloc.
/// These are monotonically increasing counters scoped to the calling thread.
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

/// Process-wide jemalloc allocated bytes.
#[cfg(feature = "jemalloc")]
fn jemalloc_process_allocated() -> u64 {
    use tikv_jemalloc_ctl::{epoch, stats};
    epoch::advance().ok();
    stats::allocated::read().unwrap_or(0) as u64
}

#[cfg(not(feature = "jemalloc"))]
fn jemalloc_process_allocated() -> u64 {
    0
}

fn update_atomic_max(atomic: &AtomicU64, value: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while value > current {
        match atomic.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

/// Wraps a child ExecutionPlan and tracks memory allocated/deallocated
/// during scan execution using jemalloc per-thread counters.
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
        // Capture thread-local counters before execute()
        let (thread_alloc_before, thread_dealloc_before) = jemalloc_thread_stats();

        let child_stream = self.child.execute(partition, Arc::clone(&context))?;

        // Capture after execute() to measure setup cost
        let (thread_alloc_after, thread_dealloc_after) = jemalloc_thread_stats();
        let execute_allocated = thread_alloc_after - thread_alloc_before;
        let execute_deallocated = thread_dealloc_after - thread_dealloc_before;

        log::info!(
            "ScanMemoryLogExec spark_partition={}: execute() setup: \
             thread_allocated={}, thread_deallocated={}, net={}",
            self.spark_partition,
            execute_allocated,
            execute_deallocated,
            execute_allocated as i64 - execute_deallocated as i64,
        );

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(ScanMemoryLogStream {
            child: child_stream,
            context,
            spark_partition: self.spark_partition,
            logged: false,
            baseline_metrics,
            // Accumulate total thread-level alloc/dealloc across all polls
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
    context: Arc<TaskContext>,
    spark_partition: i32,
    logged: bool,
    baseline_metrics: BaselineMetrics,
    /// Accumulated bytes allocated on the thread during this scan
    thread_total_allocated: u64,
    /// Accumulated bytes deallocated on the thread during this scan
    thread_total_deallocated: u64,
    batch_count: usize,
    total_rows: usize,
}

impl Stream for ScanMemoryLogStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Snapshot thread counters before polling child
        let (alloc_before, dealloc_before) = jemalloc_thread_stats();

        let result = Pin::new(&mut self.child).poll_next(cx);

        // Snapshot after — delta is what this poll allocated/freed
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
                let pool = self.context.memory_pool();
                let process_allocated = jemalloc_process_allocated();
                let net = self.thread_total_allocated as i64
                    - self.thread_total_deallocated as i64;

                // Accumulate into global aggregates
                AGG_THREAD_ALLOCATED
                    .fetch_add(self.thread_total_allocated, Ordering::Relaxed);
                AGG_THREAD_DEALLOCATED
                    .fetch_add(self.thread_total_deallocated, Ordering::Relaxed);
                update_atomic_max(&AGG_PEAK_PROCESS_ALLOCATED, process_allocated);
                AGG_TOTAL_ROWS.fetch_add(self.total_rows as u64, Ordering::Relaxed);
                let completed =
                    AGG_COMPLETED_PARTITIONS.fetch_add(1, Ordering::Relaxed) + 1;

                log::info!(
                    "ScanMemoryLogExec spark_partition={}: scan complete, \
                     batches={}, rows={}, \
                     memory_pool_reserved={}, \
                     thread: allocated={}, deallocated={}, net={}, \
                     process_allocated={} | \
                     aggregate(n={}): thread_allocated={}, thread_deallocated={}, \
                     thread_net={}, max_process_allocated={}, total_rows={}",
                    self.spark_partition,
                    self.batch_count,
                    self.total_rows,
                    pool.reserved(),
                    self.thread_total_allocated,
                    self.thread_total_deallocated,
                    net,
                    process_allocated,
                    completed,
                    AGG_THREAD_ALLOCATED.load(Ordering::Relaxed),
                    AGG_THREAD_DEALLOCATED.load(Ordering::Relaxed),
                    AGG_THREAD_ALLOCATED.load(Ordering::Relaxed) as i64
                        - AGG_THREAD_DEALLOCATED.load(Ordering::Relaxed) as i64,
                    AGG_PEAK_PROCESS_ALLOCATED.load(Ordering::Relaxed),
                    AGG_TOTAL_ROWS.load(Ordering::Relaxed),
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
