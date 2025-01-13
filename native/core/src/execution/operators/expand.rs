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

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use futures::{Stream, StreamExt};
use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A Comet native operator that expands a single row into multiple rows. This behaves as same as
/// Spark Expand operator.
#[derive(Debug)]
pub struct ExpandExec {
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl ExpandExec {
    /// Create a new ExpandExec
    pub fn new(
        projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            projections,
            child,
            schema,
            cache,
        }
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometExpandExec")?;
                write!(f, "Projections: [")?;
                for projection in &self.projections {
                    write!(f, "[")?;
                    for expr in projection {
                        write!(f, "{}, ", expr)?;
                    }
                    write!(f, "], ")?;
                }
                write!(f, "]")?;

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for ExpandExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let new_expand = ExpandExec::new(
            self.projections.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&self.schema),
        );
        Ok(Arc::new(new_expand))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let expand_stream = ExpandStream::new(
            self.projections.clone(),
            child_stream,
            Arc::clone(&self.schema),
        );
        Ok(Box::pin(expand_stream))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn name(&self) -> &str {
        "CometExpandExec"
    }
}

pub struct ExpandStream {
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    current_index: i32,
    max_index: i32,
    current_batch: Option<RecordBatch>,
}

impl ExpandStream {
    /// Create a new ExpandStream
    pub fn new(
        projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        child_stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        let max_index = projections.len() as i32;
        Self {
            projections,
            child_stream,
            schema,
            current_index: -1,
            max_index,
            current_batch: None,
        }
    }

    fn expand(
        &self,
        batch: &RecordBatch,
        projection: &[Arc<dyn PhysicalExpr>],
    ) -> Result<RecordBatch, DataFusionError> {
        let mut columns = vec![];

        projection.iter().try_for_each(|expr| {
            let column = expr.evaluate(batch)?;
            columns.push(column.into_array(batch.num_rows())?);

            Ok::<(), DataFusionError>(())
        })?;

        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), columns, &options)
            .map_err(|e| e.into())
    }
}

impl Stream for ExpandStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_index == -1 {
            let next = self.child_stream.poll_next_unpin(cx);
            match next {
                Poll::Ready(Some(Ok(batch))) => {
                    self.current_batch = Some(batch);
                    self.current_index = 0;
                }
                other => return other,
            }
        }
        assert!(self.current_batch.is_some());

        let projection = &self.projections[self.current_index as usize];
        let batch = self.expand(self.current_batch.as_ref().unwrap(), projection);

        self.current_index += 1;

        if self.current_index == self.max_index {
            self.current_index = -1;
            self.current_batch = None;
        }
        Poll::Ready(Some(batch))
    }
}

impl RecordBatchStream for ExpandStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
