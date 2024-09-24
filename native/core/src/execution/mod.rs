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

//! PoC of vectorization execution through JNI to Rust.
pub mod datafusion;
pub mod jni_api;

pub mod kernels; // for benchmarking

mod metrics;
pub mod operators;
pub mod serde;
pub mod shuffle;
pub(crate) mod sort;

use ::datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
pub use datafusion_comet_spark_expr::timezone;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
pub(crate) mod utils;

mod memory_pool;
pub use memory_pool::*;

#[derive(Debug)]
pub struct DebugExec {
    child: Arc<dyn ExecutionPlan>,
}

impl DebugExec {
    pub fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

impl DisplayAs for DebugExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DebugExec")
    }
}

impl ExecutionPlan for DebugExec {
    fn name(&self) -> &str {
        "DebugExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.child.execute(partition, context)?;
        Ok(Box::pin(DebugStream {
            name: self.child.name().to_owned(),
            child_stream: stream,
        }))
    }
}

pub struct DebugStream {
    name: String,
    child_stream: SendableRecordBatchStream,
}

impl Stream for DebugStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.child_stream.poll_next_unpin(cx).map(|x| match x {
            Some(Err(e)) => {
                println!("{}.poll_next() returned an error: {}", self.name, e);
                panic!("{}.poll_next() returned an error: {}", self.name, e);
                // Some(Err(e))
            }
            other => other,
        })
    }
}

impl RecordBatchStream for DebugStream {
    fn schema(&self) -> SchemaRef {
        self.child_stream.schema()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
