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
//! A caller supplies a fresh, fully planned graph for each execution. All partitions
//! execute the same operator instances, including the channels in RepartitionExec.
//! The caller owns the Tokio runtime and must keep it running until cleanup finishes.
//! Spark admission and JNI live outside this crate.

pub mod handle;
pub mod range;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{
    execute_stream, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;

/// One execution's graph and resources. Consumed when execution starts; not a plan cache.
pub struct LocalQuery {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
}

impl LocalQuery {
    /// Takes ownership of a fresh graph and its query-scoped context.
    ///
    /// The planner must satisfy operator distribution and ordering requirements. It
    /// must not reuse stateful operator instances from another query or attempt.
    /// The context must not borrow a Spark task's memory pool or input iterators.
    pub fn new(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Self {
        Self { plan, context }
    }

    /// Starts all root partitions and exposes one unordered, streaming result.
    ///
    /// DataFusion's coalescer polls partitions concurrently; consumers do not have
    /// to schedule one Spark task per partition. A single-partition root preserves
    /// its ordering. This boundary does not preserve partition IDs or establish a
    /// global sort order. No result-wide collection is performed here.
    pub fn execute(self) -> Result<LocalQueryStream> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            DataFusionError::Execution("LocalQuery requires an active Tokio runtime".into())
        })?;
        let schema = self.plan.schema();
        let stream = execute_stream(Arc::clone(&self.plan), Arc::clone(&self.context))?;
        Ok(LocalQueryStream {
            stream: Some(stream),
            query: Some(self),
            schema,
        })
    }
}

/// Owns the graph until EOF, the first error, cancellation, or drop.
///
/// Cancellation requests teardown by dropping DataFusion streams. Spawned tasks
/// finish aborting asynchronously on the caller's runtime; this is not a join barrier.
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
