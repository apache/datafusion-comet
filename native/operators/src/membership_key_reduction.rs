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

//! Keep the first occurrence of each integer key in a duplicate-heavy shuffle batch.
//! A left-semi join only needs to know whether a right-side key exists.

use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{downcast_integer_array, Array, PrimitiveArray, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::{DataFusionError, HashSet, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

const PREFIX_ROWS: usize = 8192;
const MIN_BATCH_ROWS: usize = 1024;
const MAX_BATCH_ROWS: usize = 64 * 1024;

#[derive(Debug)]
pub struct MembershipKeyReductionExec {
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl MembershipKeyReductionExec {
    /// The join planner supplies a fresh shuffle batch containing one signed integer key.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for MembershipKeyReductionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometMembershipKeyReductionExec")
    }
}

impl ExecutionPlan for MembershipKeyReductionExec {
    fn name(&self) -> &str {
        "CometMembershipKeyReductionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children.try_into().map_err(|_| {
            DataFusionError::Internal("Membership key reduction requires one child".into())
        })?;
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let input_rows =
            MetricBuilder::new(&self.metrics).counter("right_key_reduction_input_rows", partition);
        let output_rows =
            MetricBuilder::new(&self.metrics).counter("right_key_reduction_output_rows", partition);
        let bypass_rows =
            MetricBuilder::new(&self.metrics).counter("right_key_reduction_bypass_rows", partition);
        let time =
            MetricBuilder::new(&self.metrics).subset_time("right_key_reduction_time", partition);
        let mut seen = 0usize;
        let mut min_rows = MIN_BATCH_ROWS;
        let stream = input.map(move |batch| {
            let batch = batch?;
            let rows = batch.num_rows();
            input_rows.add(rows);
            // Avoid the setup cost on short partitions, including a single large batch.
            let attempt = seen >= PREFIX_ROWS && (min_rows..=MAX_BATCH_ROWS).contains(&rows);
            seen = seen.saturating_add(rows);
            if attempt {
                match try_reduce_batch(&batch, &context, &time) {
                    Ok(Some(reduced)) => {
                        output_rows.add(reduced.num_rows());
                        return Ok(reduced);
                    }
                    // A short shuffle frame may have too few repeats while a later full
                    // batch is worth reducing. Retry only at the next larger batch size.
                    Ok(None) => min_rows = (rows + 1).next_power_of_two(),
                    Err(e) if matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) => {
                        min_rows = MAX_BATCH_ROWS + 1;
                    }
                    Err(e) => return Err(e),
                }
            }
            bypass_rows.add(rows);
            output_rows.add(rows);
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn try_reduce_batch(
    batch: &RecordBatch,
    context: &TaskContext,
    time: &Time,
) -> Result<Option<RecordBatch>> {
    let _timer = time.timer();
    let column = batch.column(0).as_ref();
    downcast_integer_array!(column => reduce_integer(column, batch, context),
        data_type => datafusion::common::internal_err!("Expected an integer membership key, got {data_type}"))
}

fn reduce_integer<T: ArrowPrimitiveType>(
    column: &PrimitiveArray<T>,
    batch: &RecordBatch,
    context: &TaskContext,
) -> Result<Option<RecordBatch>>
where
    T::Native: Eq + Hash,
{
    let rows = column.len();
    let limit = rows.div_ceil(3);
    let reservation = MemoryConsumer::new("MembershipKeyReduction batch")
        .with_can_spill(true)
        .register(context.memory_pool());
    // The first key past the limit is inserted before it triggers bypass. Budget the
    // Arrow value and validity buffers, scratch indices, and array/buffer headers.
    let table_bytes = estimate_memory_size::<Option<T::Native>>(limit + 1, 1024)?;
    reservation.try_grow(
        table_bytes + limit * (size_of::<u32>() + size_of::<T::Native>()) + limit.div_ceil(8),
    )?;

    let indices = if column.null_count() == rows
        || (column.null_count() == 0 && column.values()[1..] == column.values()[..rows - 1])
    {
        vec![0]
    } else {
        let mut seen = HashSet::with_capacity(limit + 1);
        let mut indices = Vec::with_capacity(limit);
        for (row, value) in column.iter().enumerate() {
            if seen.insert(value) {
                if indices.len() == limit {
                    return Ok(None);
                }
                indices.push(row as u32);
            }
        }
        indices
    };
    Ok(Some(take_record_batch(batch, &UInt32Array::from(indices))?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn batch(values: impl Iterator<Item = i32>) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int32Array::from_iter_values(values)) as ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn keeps_distinct_nullable_keys_in_input_order() {
        let keys = [None, Some(i32::MIN), Some(0), Some(-1)];
        let input = RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int32Array::from_iter(
                (0..1025).map(|i| keys[i % keys.len()]),
            )) as ArrayRef,
        )])
        .unwrap()
        .slice(1, 1024);
        let reduced = try_reduce_batch(&input, &SessionContext::new().task_ctx(), &Time::new())
            .unwrap()
            .unwrap();
        assert_eq!(reduced, input.slice(0, keys.len()));
    }

    #[test]
    fn keeps_a_different_key_at_the_end_of_an_otherwise_constant_batch() {
        let context = SessionContext::new().task_ctx();
        for last in [0, 1] {
            let input = batch((0..1024).map(|i| if i == 1023 { last } else { 0 }));
            let output = try_reduce_batch(&input, &context, &Time::new())
                .unwrap()
                .unwrap();
            assert_eq!(output, batch(0..=last));
        }
    }

    #[tokio::test]
    async fn short_frames_do_not_disable_larger_batches() {
        let prefix = batch(0..PREFIX_ROWS as i32);
        let hot = batch((0..2048).map(|i| i % 8));
        for (limit, attempted) in [(1, hot.clone()), (usize::MAX, batch(0..2048))] {
            let pool = Arc::new(GreedyMemoryPool::new(limit));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                .build_arc()
                .unwrap();
            let full = batch((0..8192).map(|i| (i * 1031) % 2048));
            let input = vec![prefix.clone(), attempted, hot.clone(), full.clone()];
            let source = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&input),
                prefix.schema(),
                None,
            )
            .unwrap();
            let reducer = MembershipKeyReductionExec::new(source);
            let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
            let output = collect(reducer.execute(0, ctx).unwrap()).await.unwrap();
            assert_eq!(output[..3], input[..3]);
            assert_eq!(
                output[3],
                if limit == 1 {
                    full
                } else {
                    full.slice(0, 2048)
                }
            );
            assert_eq!(pool.reserved(), 0);
        }
    }
}
