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

use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use datafusion::{execution::TaskContext, physical_expr::*, physical_plan::*};
use datafusion_common::{DataFusionError, Result as DataFusionResult};

use super::copy_or_cast_array;

/// An utility execution node which makes deep copies of input batches.
///
/// In certain scenarios like sort, DF execution nodes only make shallow copy of input batches.
/// This could cause issues for Comet, since we re-use column vectors across different batches.
/// For those scenarios, this can be used as an adapter node.
#[derive(Debug)]
pub struct CopyExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl CopyExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let fields: Vec<Field> = input
            .schema()
            .fields
            .iter()
            .map(|f: &FieldRef| match f.data_type() {
                DataType::Dictionary(_, value_type) => {
                    Field::new(f.name(), value_type.as_ref().clone(), f.is_nullable())
                }
                _ => f.as_ref().clone(),
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));

        Self { input, schema }
    }
}

impl DisplayAs for CopyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CopyExec")
            }
        }
    }
}

impl ExecutionPlan for CopyExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.input.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = self.input.clone();
        let new_input = input.with_new_children(children)?;
        Ok(Arc::new(CopyExec {
            input: new_input,
            schema: self.schema.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let child_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(CopyStream::new(self.schema(), child_stream)))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        self.input.statistics()
    }
}

struct CopyStream {
    schema: SchemaRef,
    child_stream: SendableRecordBatchStream,
}

impl CopyStream {
    fn new(schema: SchemaRef, child_stream: SendableRecordBatchStream) -> Self {
        Self {
            schema,
            child_stream,
        }
    }

    // TODO: replace copy_or_cast_array with copy_array if upstream sort kernel fixes
    // dictionary array sorting issue.
    fn copy(&self, batch: RecordBatch) -> DataFusionResult<RecordBatch> {
        let vectors = batch
            .columns()
            .iter()
            .map(|v| copy_or_cast_array(v))
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        RecordBatch::try_new(self.schema.clone(), vectors).map_err(DataFusionError::ArrowError)
    }
}

impl Stream for CopyStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.child_stream.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.copy(batch)),
            other => other,
        })
    }
}

impl RecordBatchStream for CopyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
