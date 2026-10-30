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

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SplitMetrics};
use datafusion::physical_plan::stream::BatchSplitStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use std::fmt::Formatter;
use std::sync::Arc;

/// Passes its input through in batches of at most `batch_size` rows, slicing wider ones without
/// copying. The planner puts it under an operator whose expressions expand a large string or
/// binary scalar to the batch's length, so that the expanded value fits Arrow's 32-bit offsets
/// whatever batch size the operators below settle on.
#[derive(Debug)]
pub struct BatchSplitExec {
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
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
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl DisplayAs for BatchSplitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometBatchSplitExec: batch_size={}", self.batch_size)
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
        Ok(Arc::new(BatchSplitExec::new(
            Arc::clone(&children[0]),
            self.batch_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(BatchSplitStream::new(
            input,
            self.batch_size,
            SplitMetrics::new(&self.metrics, partition),
        )))
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
        let schema = batches[0].schema();
        let config = MemorySourceConfig::try_new(&[batches], schema, None).unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(config)));
        let exec = Arc::new(BatchSplitExec::new(input, batch_size));
        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            out.push(column.values().to_vec());
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
}
