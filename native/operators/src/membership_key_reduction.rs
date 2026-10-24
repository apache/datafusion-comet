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

//! Keep the first occurrence of each complete integer row in a duplicate-heavy shuffle batch.
//! A left-semi join only needs to know whether a right-side key exists.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{make_comparator, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::{create_hashes, RandomState};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
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
    /// The join planner supplies a fresh shuffle batch of signed integer columns. Equal
    /// rows must produce the same membership key without side effects.
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
        let mut bypass = false;
        let stream = input.map(move |batch| {
            let batch = batch?;
            let rows = batch.num_rows();
            input_rows.add(rows);
            // Avoid the setup cost on short partitions, including a single large batch.
            let attempt = !bypass && seen >= PREFIX_ROWS && rows >= MIN_BATCH_ROWS;
            seen = seen.saturating_add(rows);
            if attempt {
                match try_reduce_batch(&batch, &context, &time) {
                    Ok(Some(reduced)) => {
                        output_rows.add(reduced.num_rows());
                        return Ok(reduced);
                    }
                    Ok(None) => {}
                    Err(e) if matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) => {}
                    Err(e) => return Err(e),
                }
                bypass = true;
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
    let rows = batch.num_rows();
    if rows > MAX_BATCH_ROWS {
        return Ok(None);
    }

    let max_distinct = rows.div_ceil(3);
    let slots = rows.next_power_of_two();
    let row_width: usize = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|field| field.data_type().primitive_width().unwrap())
        .sum();
    // Reserve before allocating. Arrow take materializes at most max_distinct values
    // per column. Allow one validity bitmap and 1 KiB per column for the comparator,
    // array and buffer headers, pointer vectors and alignment.
    let scratch_bytes = rows * size_of::<u64>() + (slots + max_distinct) * size_of::<u32>();
    let output_bytes =
        row_width * max_distinct + batch.num_columns() * (max_distinct.div_ceil(8) + 1024);
    let reservation = MemoryConsumer::new("MembershipKeyReduction batch")
        .with_can_spill(true)
        .register(context.memory_pool());
    reservation.try_grow(scratch_bytes + output_bytes)?;

    let mut hashes = vec![0; rows];
    create_hashes(batch.columns(), &RandomState::with_seed(0), &mut hashes)?;
    let comparators = batch
        .columns()
        .iter()
        .map(|column| make_comparator(column.as_ref(), column.as_ref(), Default::default()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut table = vec![u32::MAX; slots];
    let mut indices = Vec::with_capacity(max_distinct);
    // DataFusion skips null integers when hashing. Different null placements can have
    // identical hashes, so bound total collision probes to one extra probe per input row.
    let mut remaining_collisions = rows;
    for (row, &hash) in hashes.iter().enumerate() {
        let mut slot = hash as usize & (slots - 1);
        loop {
            let previous = table[slot];
            if previous == u32::MAX {
                if indices.len() == max_distinct {
                    return Ok(None);
                }
                table[slot] = row as u32;
                indices.push(row as u32);
                break;
            }
            let previous = previous as usize;
            if hashes[previous] == hash && comparators.iter().all(|cmp| cmp(previous, row).is_eq())
            {
                break;
            }
            if remaining_collisions == 0 {
                return Ok(None);
            }
            remaining_collisions -= 1;
            slot = (slot + 1) & (slots - 1);
        }
    }

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
    fn keeps_complete_nullable_keys_in_input_order() {
        // The two null placements can hash alike. Only equal tuples may be removed.
        let keys = [
            (None, Some(-1)),
            (Some(-1), None),
            (None, None),
            (Some(i32::MIN), Some(-1)),
            (Some(0), Some(0)),
        ];
        let input = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(Int32Array::from_iter(
                    (0..1025).map(|i| keys[i % keys.len()].0),
                )) as ArrayRef,
            ),
            (
                "b",
                Arc::new(Int32Array::from_iter(
                    (0..1025).map(|i| keys[i % keys.len()].1),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
        .slice(1, 1024);
        let reduced = try_reduce_batch(&input, &SessionContext::new().task_ctx(), &Time::new())
            .unwrap()
            .unwrap();
        assert_eq!(reduced, input.slice(0, keys.len()));
    }

    #[test]
    fn reduces_four_copies_of_each_key() {
        let input = batch((0..8192).map(|i| (i * 1031) % 2048));
        let reduced = try_reduce_batch(&input, &SessionContext::new().task_ctx(), &Time::new())
            .unwrap()
            .unwrap();
        assert_eq!(reduced, input.slice(0, 2048));
    }

    #[tokio::test]
    async fn bypasses_the_rest_of_the_partition_after_a_refusal() {
        let prefix = batch(0..PREFIX_ROWS as i32);
        let hot = batch((0..2048).map(|i| i % 8));
        for (limit, attempted) in [(1, hot.clone()), (usize::MAX, batch(0..2048))] {
            let pool = Arc::new(GreedyMemoryPool::new(limit));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                .build_arc()
                .unwrap();
            let input = vec![prefix.clone(), attempted, hot.clone()];
            let source = MemorySourceConfig::try_new_exec(
                std::slice::from_ref(&input),
                prefix.schema(),
                None,
            )
            .unwrap();
            let reducer = MembershipKeyReductionExec::new(source);
            let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
            let output = collect(reducer.execute(0, ctx).unwrap()).await.unwrap();
            assert_eq!(output, input);
            assert_eq!(pool.reserved(), 0);
        }
    }
}
