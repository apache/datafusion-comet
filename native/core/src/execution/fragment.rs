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

//! Builds and drives a Comet plan fragment whose input-leaf `Scan` operators are
//! fed by native DataFusion [`SendableRecordBatchStream`]s (e.g. a Ballista
//! shuffle reader), rather than by JVM-exported Arrow streams.
//!
//! Unlike [`super::ffi`], this path stays entirely in-process: there is no
//! `datafusion-ffi` boundary, because the consumer (a Ballista executor) and
//! Comet resolve to the same DataFusion build, so `ExecutionPlan`s and
//! `SendableRecordBatchStream`s are shared directly.
//!
//! `PhysicalPlanner::create_plan` builds a `Scan` (op #100) leaf with no input
//! source and returns a handle to it. Its executable clone shares this handle's
//! `batch` slot (an `Arc`), so [`super::operators::ScanExec::set_native_input`]
//! injects the child stream into the handle and [`NativeFragmentStream`] drives
//! `get_next_batch` on it — mirroring the JVM busy-poll in `jni_api` — to make
//! child batches flow through the fragment.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties, RecordBatchStream};
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_operator::Operator;
use futures::{Stream, StreamExt};
use prost::Message;

use super::operators::ScanExec;
use super::planner::PhysicalPlanner;

/// A non-`TEST_EXEC_CONTEXT_ID` execution-context id for natively-fed scans. No
/// JVM context is involved on this path (the input is a native stream, not an
/// `ArrowArrayStream`), so the concrete value is immaterial as long as it is not
/// the test sentinel, which would make `pull_next` short-circuit to EOF.
const NATIVE_FRAGMENT_EXEC_ID: i64 = 0;

/// Decode the Comet `Operator` proto and build the DataFusion plan with Comet's
/// planner, returning the input-leaf `Scan` handles (in encounter order) and the
/// fragment root. The default (test) `exec_context_id` is used so the `Scan` op
/// builds without consuming a JVM input; native inputs are injected afterwards
/// via [`ScanExec::set_native_input`].
fn plan_from_proto(proto_bytes: &[u8]) -> Result<(Vec<ScanExec>, Arc<dyn ExecutionPlan>), String> {
    let op = Operator::decode(proto_bytes)
        .map_err(|e| format!("failed to decode Operator proto: {e}"))?;

    // A fresh `SessionContext` means configuration comes only from the proto,
    // not from any ambient session (see `super::ffi`).
    let session_ctx = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(session_ctx, 0);

    let mut jvm_inputs = Vec::new();
    let (scans, _shuffle_scans, spark_plan) = planner
        .create_plan(&op, &mut jvm_inputs, 1)
        .map_err(|e| format!("failed to build native plan: {e}"))?;

    Ok((scans, Arc::clone(&spark_plan.native_plan)))
}

/// The `PlanProperties` (schema, partitioning, ordering) of the fragment root,
/// used to establish a `CometFragmentExec`'s schema/properties at construction
/// time without executing it or requiring the child streams.
pub fn native_fragment_plan_properties(proto_bytes: &[u8]) -> Result<Arc<PlanProperties>, String> {
    let (_scans, root) = plan_from_proto(proto_bytes)?;
    Ok(Arc::clone(root.properties()))
}

/// Build the Comet fragment described by `proto_bytes`, feeding its input-leaf
/// `Scan` operators from `inputs` (one stream per leaf, in encounter order), and
/// return the fragment root's output stream. Executing that stream drives the
/// child streams through the fragment.
pub fn build_native_fragment(
    proto_bytes: &[u8],
    task_ctx: Arc<TaskContext>,
    inputs: Vec<SendableRecordBatchStream>,
) -> Result<SendableRecordBatchStream, String> {
    let (mut scans, root) = plan_from_proto(proto_bytes)?;

    if scans.len() != inputs.len() {
        return Err(format!(
            "Comet fragment has {} Scan input leaves but {} child streams were provided",
            scans.len(),
            inputs.len()
        ));
    }

    // Inject each child stream into the matching `Scan` handle. The handle shares
    // its `batch` slot with the executable leaf, so pulling here delivers batches
    // to the plan node.
    for (scan, input) in scans.iter_mut().zip(inputs) {
        scan.set_native_input(NATIVE_FRAGMENT_EXEC_ID, input);
    }

    // The Comet fragment is internally single-partition; execute its root at
    // partition 0. The child streams were already obtained for the desired output
    // partition by the caller.
    let root_stream = root
        .execute(0, task_ctx)
        .map_err(|e| format!("failed to execute Comet fragment root: {e}"))?;
    let schema = root_stream.schema();

    Ok(Box::pin(NativeFragmentStream {
        root: root_stream,
        scans,
        schema,
    }))
}

/// Streams the fragment root while pumping its `Scan` leaves. When the root
/// yields `Pending` because a leaf's `batch` slot is empty, that leaf handle is
/// asked to pull its next batch and the root is re-polled — the same interleaving
/// `jni_api` performs for JVM-fed scans, but with native child streams.
///
/// Crucially, the root is only re-polled after new input is actually fed into a
/// leaf. If the root returns `Pending` while every leaf already has a batch
/// pending consumption (or there are no leaves at all — e.g. a childless
/// `NativeScan` fragment reading Parquet directly), the root is genuinely pending
/// on its own async work and has registered a waker on `cx`; we return
/// `Poll::Pending` and let that waker reschedule us, rather than hot-spinning the
/// worker thread on every async-I/O `Pending`.
struct NativeFragmentStream {
    root: SendableRecordBatchStream,
    scans: Vec<ScanExec>,
    schema: SchemaRef,
}

impl Stream for NativeFragmentStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.root.poll_next_unpin(cx) {
                Poll::Ready(item) => return Poll::Ready(item),
                Poll::Pending => {
                    // Feed only leaves whose `batch` slot is empty; `get_next_batch`
                    // blocks until it delivers a batch (or EOF), so a fed leaf then
                    // holds input and re-polling the root makes progress. Track
                    // whether we fed anything this iteration.
                    let mut fed_new_input = false;
                    for scan in this.scans.iter_mut() {
                        // Peek the slot without holding the lock into `get_next_batch`
                        // (which takes it again). A slot that is already `Some` needs
                        // no feeding; a contended `try_lock` is treated as "not empty"
                        // (nothing to do this round).
                        let needs_input = scan
                            .batch
                            .try_lock()
                            .map(|slot| slot.is_none())
                            .unwrap_or(false);
                        if needs_input {
                            if let Err(e) = scan.get_next_batch() {
                                return Poll::Ready(Some(Err(DataFusionError::Execution(
                                    format!("Comet fragment scan input error: {e}"),
                                ))));
                            }
                            fed_new_input = true;
                        }
                    }
                    // Nothing new to feed: the root is pending on its own async work
                    // and its waker (registered on `cx` above) will reschedule us.
                    if !fed_new_input {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for NativeFragmentStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
