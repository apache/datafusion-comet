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

use arrow::compute::{cast_with_options, CastOptions};
use futures::{Stream, StreamExt};
use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::array::{
    downcast_dictionary_array, make_array, Array, ArrayRef, MutableArrayData, RecordBatch,
    RecordBatchOptions,
};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion::common::{arrow_datafusion_err, DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::{execution::TaskContext, physical_expr::*, physical_plan::*};

/// An utility execution node which makes deep copies of input batches.
///
/// In certain scenarios like sort, DF execution nodes only make shallow copy of input batches.
/// This could cause issues for Comet, since we re-use column vectors across different batches.
/// For those scenarios, this can be used as an adapter node.
#[derive(Debug)]
pub struct CopyExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    mode: CopyMode,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CopyMode {
    /// Perform a deep copy and also unpack dictionaries
    UnpackOrDeepCopy,
    /// Perform a clone and also unpack dictionaries
    UnpackOrClone,
}

impl CopyExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, mode: CopyMode) -> Self {
        // change schema to remove dictionary types because CopyExec always unpacks
        // dictionaries

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

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::default(),
            mode,
        }
    }
}

impl DisplayAs for CopyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CopyExec [{:?}]", self.mode)
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for CopyExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = Arc::clone(&self.input);
        let new_input = input.with_new_children(children)?;
        Ok(Arc::new(CopyExec {
            input: new_input,
            schema: Arc::clone(&self.schema),
            cache: self.cache.clone(),
            metrics: self.metrics.clone(),
            mode: self.mode.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let child_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(CopyStream::new(
            self,
            self.schema(),
            child_stream,
            partition,
            self.mode.clone(),
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        self.input.statistics()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn name(&self) -> &str {
        "CopyExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct CopyStream {
    schema: SchemaRef,
    child_stream: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    mode: CopyMode,
}

impl CopyStream {
    fn new(
        exec: &CopyExec,
        schema: SchemaRef,
        child_stream: SendableRecordBatchStream,
        partition: usize,
        mode: CopyMode,
    ) -> Self {
        Self {
            schema,
            child_stream,
            baseline_metrics: BaselineMetrics::new(&exec.metrics, partition),
            mode,
        }
    }

    // TODO: replace copy_or_cast_array with copy_array if upstream sort kernel fixes
    // dictionary array sorting issue.
    fn copy(&self, batch: RecordBatch) -> DataFusionResult<RecordBatch> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        let vectors = batch
            .columns()
            .iter()
            .map(|v| copy_or_unpack_array(v, &self.mode))
            .collect::<Result<Vec<ArrayRef>, _>>()?;

        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let maybe_batch =
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), vectors, &options)
                .map_err(|e| arrow_datafusion_err!(e));
        timer.stop();
        self.baseline_metrics.record_output(batch.num_rows());
        maybe_batch
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
        Arc::clone(&self.schema)
    }
}

/// Copy an Arrow Array
fn copy_array(array: &dyn Array) -> ArrayRef {
    let capacity = array.len();
    let data = array.to_data();

    let mut mutable = MutableArrayData::new(vec![&data], false, capacity);

    mutable.extend(0, 0, capacity);

    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        let copied_dict = make_array(mutable.freeze());
        let ref_copied_dict = &copied_dict;

        downcast_dictionary_array!(
            ref_copied_dict => {
                // Copying dictionary value array
                let values = ref_copied_dict.values();
                let data = values.to_data();

                let mut mutable = MutableArrayData::new(vec![&data], false, values.len());
                mutable.extend(0, 0, values.len());

                let copied_dict = ref_copied_dict.with_values(make_array(mutable.freeze()));
                Arc::new(copied_dict)
            }
            t => unreachable!("Should not reach here: {}", t)
        )
    } else {
        make_array(mutable.freeze())
    }
}

/// Copy an Arrow Array or cast to primitive type if it is a dictionary array.
/// This is used for `CopyExec` to copy/cast the input array. If the input array
/// is a dictionary array, we will cast the dictionary array to primitive type
/// (i.e., unpack the dictionary array) and copy the primitive array. If the input
/// array is a primitive array, we simply copy the array.
pub(crate) fn copy_or_unpack_array(
    array: &Arc<dyn Array>,
    mode: &CopyMode,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            let options = CastOptions::default();
            // We need to copy the array after `cast` because arrow-rs `take` kernel which is used
            // to unpack dictionary array might reuse the input array's null buffer.
            Ok(copy_array(&cast_with_options(
                array,
                value_type.as_ref(),
                &options,
            )?))
        }
        _ => {
            if mode == &CopyMode::UnpackOrDeepCopy {
                Ok(copy_array(array))
            } else {
                Ok(Arc::clone(array))
            }
        }
    }
}
