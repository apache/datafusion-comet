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
use arrow::compute::{take_record_batch, BatchCoalescer};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, HashSet, Result};
use datafusion::execution::{async_try_stream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};

const PREFIX_ROWS: usize = 8192;
const MIN_BATCH_ROWS: usize = 1024;
const MAX_BATCH_ROWS: usize = 64 * 1024;
const MIN_OUTPUT_ROWS: usize = 128;

#[derive(Debug)]
pub struct MembershipKeyReductionExec {
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl MembershipKeyReductionExec {
    /// The join planner supplies a single signed integer key column.
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
        let input = self.input.execute(partition, context)?;
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
        let mut batches = input.map(move |batch| -> Result<_> {
            let batch = batch?;
            let rows = batch.num_rows();
            input_rows.add(rows);
            // Avoid building a hash table on short partitions, including a single large batch.
            let attempt = seen >= PREFIX_ROWS && (min_rows..=MAX_BATCH_ROWS).contains(&rows);
            seen = seen.saturating_add(rows);
            if attempt {
                let _timer = time.timer();
                if let Some(reduced) = try_reduce_batch(&batch)? {
                    return Ok((reduced.num_rows() < MIN_OUTPUT_ROWS, reduced));
                }
                // A larger shuffle frame may still be worth reducing. With uniform
                // frame sizes the rest of the partition goes straight to the sort.
                min_rows = (rows + 1).next_power_of_two();
            }
            bypass_rows.add(rows);
            Ok((false, batch))
        });
        // The sort accounts for Arrow buffers, but each small RecordBatch also has
        // metadata. Pack tiny reductions so this overhead stays small on long partitions.
        let mut small = BatchCoalescer::new(self.schema(), MIN_OUTPUT_ROWS);
        let stream = async_try_stream(|mut out| async move {
            while let Some((pack, batch)) = batches.try_next().await? {
                let forward = if pack {
                    small.push_batch(batch)?;
                    None
                } else {
                    small.finish_buffered_batch()?;
                    Some(batch)
                };
                for batch in [small.next_completed_batch(), forward]
                    .into_iter()
                    .flatten()
                {
                    out.emit(batch).await;
                }
            }
            small.finish_buffered_batch()?;
            if let Some(batch) = small.next_completed_batch() {
                out.emit(batch).await;
            }
            Ok(())
        })
        .inspect_ok(move |batch| output_rows.add(batch.num_rows()));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn try_reduce_batch(batch: &RecordBatch) -> Result<Option<RecordBatch>> {
    let column = batch.column(0).as_ref();
    downcast_integer_array!(column => reduce_integer(column, batch), _ => Ok(None))
}

fn reduce_integer<T: ArrowPrimitiveType>(
    column: &PrimitiveArray<T>,
    batch: &RecordBatch,
) -> Result<Option<RecordBatch>>
where
    T::Native: Eq + Hash,
{
    let rows = column.len();
    // A low-cardinality shuffle can put just one key in a partition.
    let constant = column.null_count() == rows
        || (column.null_count() == 0 && column.values()[1..] == column.values()[..rows - 1]);
    let indices = if constant {
        vec![0]
    } else {
        // This scratch is bounded by MAX_BATCH_ROWS and lives only for this synchronous
        // batch, like Arrow's take below. A separate Spark reservation can spill a JVM
        // sorter even when the native sort would spill its own input instead.
        let limit = rows.div_ceil(3);
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
    use arrow::datatypes::{Int16Type, Int32Type, Int64Type, Int8Type};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::SessionContext;

    fn batch(values: impl Iterator<Item = i32>) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int32Array::from_iter_values(values)) as ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn keeps_distinct_nullable_keys_in_input_order() {
        fn check<T: ArrowPrimitiveType>(min: T::Native)
        where
            T::Native: From<i8>,
        {
            let keys = [None, Some(min), Some(0_i8.into()), Some((-1_i8).into())];
            let column = PrimitiveArray::<T>::from_iter(keys.into_iter().cycle().take(1025));
            let input = RecordBatch::try_from_iter([("k", Arc::new(column) as ArrayRef)])
                .unwrap()
                .slice(1, 1024);
            let reduced = try_reduce_batch(&input).unwrap().unwrap();
            assert_eq!(reduced, input.slice(0, keys.len()));
        }

        check::<Int8Type>(i8::MIN);
        check::<Int16Type>(i16::MIN);
        check::<Int32Type>(i32::MIN);
        check::<Int64Type>(i64::MIN);
    }

    #[test]
    fn keeps_a_different_key_at_the_end_of_an_otherwise_constant_batch() {
        for last in [0, 1] {
            let input = batch((0..1024).map(|i| if i == 1023 { last } else { 0 }));
            let output = try_reduce_batch(&input).unwrap().unwrap();
            assert_eq!(output, batch(0..=last));
        }
    }

    #[tokio::test]
    async fn packs_tiny_reductions_before_a_bypassed_batch_and_at_end_of_input() {
        let prefix = batch(0..PREFIX_ROWS as i32);
        let bypass = batch(0..(MIN_BATCH_ROWS / 2) as i32);
        let constant = |key| batch(std::iter::repeat_n(key, MIN_BATCH_ROWS));
        let target = MIN_OUTPUT_ROWS as i32;
        let mut input = vec![prefix.clone()];
        input.extend((0..=target * 2).map(constant));
        input.extend([bypass.clone(), constant(-1)]);
        let source = MemorySourceConfig::try_new_exec(&[input], prefix.schema(), None).unwrap();
        let reducer = MembershipKeyReductionExec::new(source);
        let output = collect(
            reducer
                .execute(0, SessionContext::new().task_ctx())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            output,
            [
                prefix,
                batch(0..target),
                batch(target..target * 2),
                batch(std::iter::once(target * 2)),
                bypass.clone(),
                batch(std::iter::once(-1)),
            ]
        );
        assert!(Arc::ptr_eq(output[4].column(0), bypass.column(0)));
    }

    #[tokio::test]
    async fn short_frames_do_not_disable_larger_batches() {
        let prefix = batch(0..PREFIX_ROWS as i32);
        let unique = batch(0..2048);
        let hot = batch((0..2048).map(|i| i % 8));
        let full = batch((0..8192).map(|i| (i * 1031) % 2048));
        let input = vec![prefix, unique, hot, full.clone()];
        let source =
            MemorySourceConfig::try_new_exec(std::slice::from_ref(&input), full.schema(), None)
                .unwrap();
        let reducer = MembershipKeyReductionExec::new(source);
        let output = collect(
            reducer
                .execute(0, SessionContext::new().task_ctx())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(output[..3], input[..3]);
        assert_eq!(output[3], full.slice(0, 2048));
    }
}
