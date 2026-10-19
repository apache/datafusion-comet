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

//! Query-scoped execution, independent of Spark tasks and JNI.
//!
//! A caller supplies a fresh, fully planned graph for each execution and receives one
//! result stream. The caller owns the Tokio runtime and must keep it running until
//! cleanup finishes. Spark admission and JNI live outside this crate.

pub mod handle;

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream, ExecutionPlan, ExecutionPlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::future::poll_fn;
use futures::{FutureExt, Stream, StreamExt};
use tokio::task::JoinHandle;

/// Batches each root partition may run ahead of the one being delivered, when results are
/// delivered in partition order.
const PARTITION_BUFFER: usize = 2;

/// One execution's graph and resources. Consumed when execution starts; not a plan cache.
pub struct LocalQuery {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    /// Delivers the partitions in order, with at most this many running at once.
    in_partition_order: Option<usize>,
}

impl LocalQuery {
    /// Takes ownership of a fresh graph and its query-scoped context.
    ///
    /// The planner must satisfy operator distribution and ordering requirements. It must not
    /// reuse stateful operator instances from another query or attempt.
    pub fn new(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Self {
        Self {
            plan,
            context,
            in_partition_order: None,
        }
    }

    /// Delivers the root partitions one after another, in partition order, as Spark returns
    /// the partitions of a result. Up to `max_running` partitions run at once, each up to a few
    /// batches ahead, and the next one starts when the first is delivered, as Spark runs at most
    /// as many tasks as it has cores. A partition's operators register their memory consumers
    /// when it starts, so this also bounds how many share the memory pool. Only for roots whose
    /// partitions do not share an exchange: a partition that waits to be delivered would
    /// otherwise leave its exchange buffering for the others.
    pub fn in_partition_order(mut self, max_running: usize) -> Self {
        self.in_partition_order = Some(max_running.max(1));
        self
    }

    /// Starts all root partitions and exposes one streaming result.
    ///
    /// By default the partitions are coalesced as they produce batches, which does not
    /// preserve their order. A single-partition root preserves its ordering either way. No
    /// result-wide collection is performed here.
    pub fn execute(self) -> Result<LocalQueryStream> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            DataFusionError::Execution("LocalQuery requires an active Tokio runtime".into())
        })?;
        let schema = self.plan.schema();
        let stream = if let Some(max_running) = self.in_partition_order {
            in_partition_order(&self.plan, &self.context, max_running)?
        } else {
            execute_stream(Arc::clone(&self.plan), Arc::clone(&self.context))?
        };
        Ok(LocalQueryStream {
            stream: Some(stream),
            query: Some(self),
            schema,
        })
    }
}

fn in_partition_order(
    plan: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
    max_running: usize,
) -> Result<SendableRecordBatchStream> {
    let mut order = PartitionOrder {
        plan: Arc::clone(plan),
        context: Arc::clone(context),
        runtime: tokio::runtime::Handle::current(),
        next: 0,
        max_running,
        running: VecDeque::new(),
    };
    // Start the first partitions now, so that a failure to start them fails here.
    order.start_partitions()?;
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        plan.schema(),
        order,
    )))
}

/// One root partition running on its own Tokio task, a few batches ahead of delivery.
struct Partition {
    batches: tokio::sync::mpsc::Receiver<Result<RecordBatch>>,
    /// Set once the task has sent the stream's last batch. The channel closes whenever the
    /// task ends, and a runtime that shuts down cancels its tasks, so only this tells that
    /// every batch arrived.
    ended: Arc<AtomicBool>,
    /// The partition's stream. The task locks it only while polling it, so dropping the
    /// partition can take it whenever the task is waiting.
    stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    task: JoinHandle<()>,
}

impl Partition {
    fn spawn(runtime: &tokio::runtime::Handle, stream: SendableRecordBatchStream) -> Self {
        let (sender, batches) = tokio::sync::mpsc::channel(PARTITION_BUFFER);
        let ended = Arc::new(AtomicBool::new(false));
        let stream = Arc::new(Mutex::new(Some(stream)));
        let (task_ended, polled) = (Arc::clone(&ended), Arc::clone(&stream));
        let task = runtime.spawn(async move {
            let result = AssertUnwindSafe(async {
                // Reserve before polling, so at most `PARTITION_BUFFER` batches are held.
                while let Ok(permit) = sender.reserve().await {
                    let next = poll_fn(|cx| match lock(&polled).as_mut() {
                        Some(stream) => stream.poll_next_unpin(cx),
                        None => Poll::Ready(None),
                    })
                    .await;
                    match next {
                        Some(Ok(batch)) => permit.send(Ok(batch)),
                        Some(Err(error)) => return permit.send(Err(error)),
                        None => {
                            // A taken stream also ends here, but then nobody reads the flag.
                            task_ended.store(true, Ordering::Release);
                            return;
                        }
                    }
                }
            })
            .catch_unwind()
            .await;
            if result.is_err() {
                let _ = sender
                    .send(Err(DataFusionError::Execution(
                        "Local query partition panicked".into(),
                    )))
                    .await;
            }
        });
        Self {
            batches,
            ended,
            stream,
            task,
        }
    }
}

impl Drop for Partition {
    /// Drops the partition's stream on the calling thread, since cancelling the task only takes
    /// effect once a Tokio worker is free. See `QueryHandle::cancel`.
    fn drop(&mut self) {
        self.task.abort();
        let stream = lock(&self.stream).take();
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| drop(stream)));
    }
}

/// Delivers the partitions in turn. A partition's error is reported when its turn comes, as
/// Spark's `executeTake` evaluates a later partition only when it needs that partition's rows:
/// a consumer that stops early never sees the failure of a partition it did not reach.
struct PartitionOrder {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    runtime: tokio::runtime::Handle,
    /// The first partition not started yet.
    next: usize,
    max_running: usize,
    /// Started partitions, the first of which is being delivered.
    running: VecDeque<Partition>,
}

impl PartitionOrder {
    fn start_partitions(&mut self) -> Result<()> {
        let partitions = self.plan.output_partitioning().partition_count();
        while self.running.len() < self.max_running && self.next < partitions {
            let stream = self.plan.execute(self.next, Arc::clone(&self.context))?;
            self.running
                .push_back(Partition::spawn(&self.runtime, stream));
            self.next += 1;
        }
        Ok(())
    }

    fn fail(&mut self, error: DataFusionError) -> Poll<Option<Result<RecordBatch>>> {
        self.running.clear();
        self.next = usize::MAX;
        Poll::Ready(Some(Err(error)))
    }
}

impl Stream for PartitionOrder {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Err(error) = self.start_partitions() {
                return self.fail(error);
            }
            let Some(partition) = self.running.front_mut() else {
                return Poll::Ready(None);
            };
            match partition.batches.poll_recv(cx) {
                Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                Poll::Ready(Some(Err(error))) => return self.fail(error),
                Poll::Ready(None) if partition.ended.load(Ordering::Acquire) => {
                    self.running.pop_front();
                }
                Poll::Ready(None) => {
                    return self.fail(DataFusionError::Execution(
                        "Local query partition stopped without completing".into(),
                    ))
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Locks `mutex`, ignoring poisoning: the guarded values stay consistent across a panic.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Owns the graph until EOF, the first error, cancellation, or drop.
///
/// Cancellation requests teardown by dropping DataFusion streams. Spawned tasks finish
/// aborting asynchronously on the caller's runtime; this is not a join barrier.
pub struct LocalQueryStream {
    // Drop streams before releasing our graph/context references.
    stream: Option<SendableRecordBatchStream>,
    query: Option<LocalQuery>,
    schema: SchemaRef,
}

impl LocalQueryStream {
    /// Stops result delivery and requests cancellation of the whole native execution.
    /// Calling this again, or polling afterwards, is harmless.
    pub fn cancel(&mut self) {
        self.stream = None;
        self.query = None;
    }
}

impl Stream for LocalQueryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        let result = stream.as_mut().poll_next(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.cancel();
        }
        result
    }
}

impl RecordBatchStream for LocalQueryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
