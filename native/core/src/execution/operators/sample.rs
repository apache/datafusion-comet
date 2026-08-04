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

use arrow::array::BooleanArray;
use arrow::buffer::BooleanBuffer;
use arrow::compute::filter_record_batch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_comet_spark_expr::BernoulliCellSampler;
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::Arc;

/// A Comet native operator matching Spark's `SampleExec` for the case where sampling is performed
/// without replacement. Rows are selected by `BernoulliCellSampler`, which reproduces Spark's
/// per-row draw sequence.
#[derive(Debug)]
pub struct SampleExec {
    input: Arc<dyn ExecutionPlan>,
    lower_bound: f64,
    upper_bound: f64,
    /// Seed with the partition index already applied.
    seed: i64,
    cache: Arc<PlanProperties>,
}

impl SampleExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        lower_bound: f64,
        upper_bound: f64,
        seed: i64,
    ) -> Self {
        // Sampling only removes rows, so any ordering and partitioning of the input is preserved.
        let cache = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            lower_bound,
            upper_bound,
            seed,
            cache,
        }
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CometSampleExec: range=[{}, {}), seed={}",
                    self.lower_bound, self.upper_bound, self.seed
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for SampleExec {
    fn name(&self) -> &str {
        "CometSampleExec"
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
        Ok(Arc::new(SampleExec::new(
            Arc::clone(&children[0]),
            self.lower_bound,
            self.upper_bound,
            self.seed,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        // The generator advances across batch boundaries, so a single sampler is used for the
        // whole stream rather than one per batch.
        let mut sampler = BernoulliCellSampler::new(self.lower_bound, self.upper_bound, self.seed);
        let stream = input.map(move |batch| {
            let batch = batch?;
            // `collect_bool` invokes the closure once per row, in order, which is what keeps the
            // draw sequence identical to Spark's.
            let mask = BooleanBuffer::collect_bool(batch.num_rows(), |_| sampler.sample());
            Ok(filter_record_batch(&batch, &BooleanArray::new(mask, None))?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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

    fn batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    async fn collect_sample(batches: Vec<RecordBatch>, lb: f64, ub: f64, seed: i64) -> Vec<i32> {
        let schema = batches[0].schema();
        let config = MemorySourceConfig::try_new(&[batches], schema, None).unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(config)));
        let sample = Arc::new(SampleExec::new(input, lb, ub, seed));
        let ctx = SessionContext::new();
        let mut stream = sample.execute(0, ctx.task_ctx()).unwrap();
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend(column.values().iter().copied());
        }
        values
    }

    /// The generator must not restart at each batch, so splitting the input into batches cannot
    /// change which rows are selected.
    #[tokio::test]
    async fn test_batching_does_not_affect_selection() {
        let single = collect_sample(vec![batch((0..300).collect())], 0.0, 0.4, 1234).await;
        let split = collect_sample(
            vec![
                batch((0..100).collect()),
                batch((100..200).collect()),
                batch((200..300).collect()),
            ],
            0.0,
            0.4,
            1234,
        )
        .await;
        assert_eq!(single, split);
        assert!(!single.is_empty() && single.len() < 300);
    }

    /// Empty batches consume no draws from the generator, so interleaving them with non-empty
    /// batches cannot change which rows are selected.
    #[tokio::test]
    async fn test_empty_batches_do_not_affect_selection() {
        let without_empty = collect_sample(
            vec![batch((0..100).collect()), batch((100..200).collect())],
            0.0,
            0.4,
            42,
        )
        .await;
        let with_empty = collect_sample(
            vec![
                batch(vec![]),
                batch((0..100).collect()),
                batch(vec![]),
                batch((100..200).collect()),
                batch(vec![]),
            ],
            0.0,
            0.4,
            42,
        )
        .await;
        assert_eq!(without_empty, with_empty);
        assert!(!without_empty.is_empty());
    }

    /// Complementary ranges must partition the input, which is the property `randomSplit` needs.
    #[tokio::test]
    async fn test_complementary_ranges_partition_input() {
        let input = vec![batch((0..500).collect())];
        let lower = collect_sample(input.clone(), 0.0, 0.25, 99).await;
        let upper = collect_sample(input, 0.25, 1.0, 99).await;
        let mut all = [lower, upper].concat();
        all.sort();
        assert_eq!(all, (0..500).collect::<Vec<i32>>());
    }
}
