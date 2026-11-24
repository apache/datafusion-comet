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

//! An operator that bounds the rows per batch its parent receives.

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::take_record_batch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SplitMetrics};
use datafusion::physical_plan::stream::{BatchSplitStream, RecordBatchStreamAdapter};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::Arc;

/// Passes its input through in batches of at most `batch_size` rows, slicing wider ones without
/// copying. The planner puts it under an operator whose expressions expand a large string or
/// binary scalar to the batch's length, so that the expanded value fits Arrow's 32-bit offsets
/// whatever batch size the operators below settle on.
///
/// A compacting split copies each slice into buffers of its own instead. A sort buffers what it
/// receives and reserves each batch's whole buffers, so slices of one batch would each be
/// reserved as the batch (<https://github.com/apache/datafusion/issues/25804>).
#[derive(Debug)]
pub struct BatchSplitExec {
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
    compact: bool,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl BatchSplitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, batch_size: usize) -> Self {
        // Slicing keeps every row in place, so the input's ordering and partitioning stand.
        let cache = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            batch_size: batch_size.max(1),
            compact: false,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// A split that copies each slice it cuts into buffers of its own.
    pub fn compacting(input: Arc<dyn ExecutionPlan>, batch_size: usize) -> Self {
        Self {
            compact: true,
            ..Self::new(input, batch_size)
        }
    }

    pub fn is_compacting(&self) -> bool {
        self.compact
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl DisplayAs for BatchSplitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometBatchSplitExec: batch_size={}", self.batch_size)?;
                if self.compact {
                    write!(f, ", compact")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for BatchSplitExec {
    fn name(&self) -> &str {
        "CometBatchSplitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        let input = Arc::clone(&children[0]);
        Ok(Arc::new(if self.compact {
            BatchSplitExec::compacting(input, self.batch_size)
        } else {
            BatchSplitExec::new(input, self.batch_size)
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        if !self.compact {
            return Ok(Box::pin(BatchSplitStream::new(
                input,
                self.batch_size,
                SplitMetrics::new(&self.metrics, partition),
            )));
        }
        let batch_size = self.batch_size;
        let schema = input.schema();
        // A batch within the size passes through as it came; a wider one is copied out in pieces.
        let compacted = input.flat_map(move |batch| {
            let pieces: Vec<Result<RecordBatch>> = match batch {
                Ok(batch) if batch.num_rows() > batch_size => (0..batch.num_rows())
                    .step_by(batch_size)
                    .map(|start| {
                        let end = (start + batch_size).min(batch.num_rows());
                        let indices = UInt32Array::from_iter_values(start as u32..end as u32);
                        Ok(take_record_batch(&batch, &indices)?)
                    })
                    .collect(),
                other => vec![other],
            };
            futures::stream::iter(pieces)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, compacted)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    fn batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    /// The rows of each batch the operator emits for `batches`, split at `batch_size`.
    async fn split(batches: Vec<RecordBatch>, batch_size: usize) -> Vec<Vec<i32>> {
        emitted(batches, batch_size, false)
            .await
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    async fn emitted(
        batches: Vec<RecordBatch>,
        batch_size: usize,
        compact: bool,
    ) -> Vec<RecordBatch> {
        let schema = batches[0].schema();
        let config = MemorySourceConfig::try_new(&[batches], schema, None).unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(config)));
        let exec = Arc::new(if compact {
            BatchSplitExec::compacting(input, batch_size)
        } else {
            BatchSplitExec::new(input, batch_size)
        });
        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            out.push(batch.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn splits_a_wider_batch_and_passes_narrower_ones_through() {
        let out = split(vec![batch((0..10).collect()), batch(vec![10, 11])], 4).await;
        assert_eq!(
            out,
            vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9], vec![10, 11]]
        );
    }

    #[tokio::test]
    async fn a_compacting_split_copies_each_slice_into_its_own_buffers() {
        let wide = batch((0..10).collect());
        let wide_bytes = wide.get_array_memory_size();
        let out = emitted(vec![wide, batch(vec![10, 11])], 4, true).await;
        let rows: Vec<Vec<i32>> = out
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            rows,
            vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9], vec![10, 11]]
        );
        // Each piece of the wide batch holds its own rows, not the batch's buffers.
        for piece in &out[..3] {
            assert!(piece.get_array_memory_size() < wide_bytes);
        }
    }
}
