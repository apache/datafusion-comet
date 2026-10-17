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

//! The inputs of the inner joins of [`super::SpillingHashJoinExec`].

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use parking_lot::Mutex;

/// A leaf that hands one stream, made before the plan runs, to the first `execute`. The operator
/// feeds each inner `HashJoinExec` this way, from memory, from a spill file or from the probe
/// input.
pub(super) struct StreamSourceExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl StreamSourceExec {
    pub(super) fn new_arc(stream: SendableRecordBatchStream) -> Arc<dyn ExecutionPlan> {
        let schema = stream.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            stream: Mutex::new(Some(stream)),
            properties,
        })
    }
}

impl Debug for StreamSourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSourceExec").finish_non_exhaustive()
    }
}

impl DisplayAs for StreamSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamSourceExec")
    }
}

impl ExecutionPlan for StreamSourceExec {
    fn name(&self) -> &str {
        "StreamSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("StreamSourceExec has no children")
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.stream
            .lock()
            .take()
            .ok_or_else(|| internal_datafusion_err!("StreamSourceExec can only be executed once"))
    }
}

/// A stream of batches held in memory.
pub(super) fn batches_stream(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok)),
    ))
}

/// The name of the column of probe row numbers that the chunked fallback appends.
const ROW_NUMBER: &str = "__comet_probe_row";

/// `schema` with a column of row numbers appended.
pub(super) fn with_row_number_field(schema: &Schema) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(ROW_NUMBER, DataType::UInt64, false)));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Appends to each batch the numbers of its rows within the stream, starting at 0.
pub(super) fn with_row_numbers(
    input: SendableRecordBatchStream,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let mut next = 0u64;
    let output_schema = Arc::clone(&schema);
    let stream = input.map(move |batch| {
        let batch = batch?;
        let rows = batch.num_rows() as u64;
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(UInt64Array::from_iter_values(next..next + rows)));
        next += rows;
        Ok(RecordBatch::try_new(Arc::clone(&output_schema), columns)?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
