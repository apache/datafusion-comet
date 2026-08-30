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

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SplitMetrics};
use datafusion::physical_plan::stream::BatchSplitStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

/// Splits oversized batches emitted by an input plan using the runtime batch size.
///
/// DataFusion 54.1.0's `UnnestExec` can emit more rows than the configured batch size. This
/// wrapper is a temporary downstream boundary for Comet's explode path until Comet upgrades to a
/// DataFusion version containing https://github.com/apache/datafusion/pull/24384.
#[derive(Debug)]
pub struct BatchSplitExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl BatchSplitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            cache: Arc::clone(input.properties()),
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for BatchSplitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometBatchSplitExec")
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "CometBatchSplitExec expects one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(BatchSplitStream::new(
            input,
            batch_size,
            SplitMetrics::new(&self.metrics, partition),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut metrics = self.metrics.clone_inner();

        // BatchSplitExec is a transparent execution wrapper. Preserve metrics produced by the
        // wrapped UnnestExec so the Spark explode node can continue reporting its original SQL
        // metrics.
        if let Some(input_metrics) = self.input.metrics() {
            for metric in input_metrics.iter() {
                metrics.push(metric.to_owned());
            }
        }

        Some(metrics)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{AsArray, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::Partitioning;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::StreamExt;

    /// Emits one batch regardless of the session `batch_size`. MemorySource/DataSourceExec
    /// would split first, so BatchSplitExec would only pass through and `batches_split`
    /// would stay 0.
    #[derive(Debug)]
    struct SingleBatchExec {
        batch: RecordBatch,
        cache: Arc<PlanProperties>,
    }

    impl SingleBatchExec {
        fn new(batch: RecordBatch) -> Self {
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(batch.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { batch, cache }
        }
    }

    impl DisplayAs for SingleBatchExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "SingleBatchExec")
                }
                DisplayFormatType::TreeRender => unimplemented!(),
            }
        }
    }

    impl ExecutionPlan for SingleBatchExec {
        fn name(&self) -> &str {
            "SingleBatchExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return internal_err!("SingleBatchExec expects no children");
            }
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let batch = self.batch.clone();
            let schema = batch.schema();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::once(async move { Ok(batch) }),
            )))
        }
    }

    #[tokio::test]
    async fn splits_batches_at_the_runtime_batch_size_and_preserves_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();
        // Limit records output_rows on the wrapped child so forwarding can be checked.
        let input: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            Arc::new(SingleBatchExec::new(input_batch)),
            0,
            None,
        ));
        let split = BatchSplitExec::new(input);

        let config = SessionConfig::new().with_batch_size(4);
        let state = SessionStateBuilder::new().with_config(config).build();
        let context = SessionContext::new_with_state(state);
        let mut stream = split.execute(0, context.task_ctx()).unwrap();

        let mut batch_sizes = vec![];
        let mut values = vec![];
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            batch_sizes.push(batch.num_rows());
            let column = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>();
            values.extend(column.values().iter().copied());
        }

        assert_eq!(batch_sizes, vec![4, 4, 2]);
        assert_eq!(values, (0..10).collect::<Vec<_>>());

        let metrics = split.metrics().unwrap().aggregate_by_name();
        let output_rows = metrics
            .iter()
            .find(|metric| metric.value().name() == "output_rows")
            .expect("wrapped input output_rows metric should be forwarded");
        assert_eq!(output_rows.value().as_usize(), 10);

        let batches_split = metrics
            .iter()
            .find(|metric| metric.value().name() == "batches_split")
            .expect("batches_split metric should be present");
        assert_eq!(batches_split.value().as_usize(), 3);
    }
}
