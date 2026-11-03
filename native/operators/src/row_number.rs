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

//! Batch-at-a-time ROW_NUMBER for input sorted by its normalised partition keys.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{partition, take, SortOptions};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{exec_err, internal_err, plan_err, Result, ScalarValue, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::functions_window::row_number::RowNumber;
use datafusion::physical_expr::window::StandardWindowExpr;
use datafusion::physical_expr::{OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, ChildrenPropertiesMode, ReplaceChildrenOptions,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    InputOrderMode, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// Generates every ROW_NUMBER result in one pass over each input batch.
///
/// The unexecuted window supplies the schema, ordering, distribution and statistics contracts.
/// Input must use `InputOrderMode::Sorted` so each partition is contiguous. Its expressions must
/// all be the built-in ROW_NUMBER over the same partition and sort keys.
/// The caller must normalise floating-point partition keys before building the window, just as
/// it does for ordinary native windows. Other window functions keep their existing executor.
///
/// This avoids the per-partition overhead tracked in
/// <https://github.com/apache/datafusion/issues/26541>. Reassess this executor when upstream
/// changes provide equivalent results, memory accounting and performance.
#[derive(Debug)]
pub struct RowNumberExec {
    window: BoundedWindowAggExec,
    metrics: ExecutionPlanMetricsSet,
}

impl RowNumberExec {
    pub fn supports(window: &BoundedWindowAggExec) -> bool {
        if window.input_order_mode != InputOrderMode::Sorted {
            return false;
        }
        let Some(first) = window.window_expr().first() else {
            return false;
        };
        window.window_expr().iter().all(|expr| {
            let Some(standard) = expr.as_any().downcast_ref::<StandardWindowExpr>() else {
                return false;
            };
            let Some(udf) = standard
                .get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
            else {
                return false;
            };
            udf.fun().inner().downcast_ref::<RowNumber>().is_some()
                && expr.expressions().is_empty()
                && expr.partition_by() == first.partition_by()
                && expr.order_by() == first.order_by()
        })
    }

    pub fn try_new(window: BoundedWindowAggExec) -> Result<Self> {
        if !Self::supports(&window) {
            return plan_err!(
                "RowNumberExec requires InputOrderMode::Sorted and only built-in ROW_NUMBER \
                 expressions with matching keys"
            );
        }
        Ok(Self {
            window,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for RowNumberExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometRowNumberExec: ")?;
        self.window.fmt_as(t, f)
    }
}

impl ExecutionPlan for RowNumberExec {
    fn name(&self) -> &str {
        "CometRowNumberExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.window.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.window.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.window.apply_expressions(f)
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.window.required_input_ordering()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.window
            .input_distribution_requirements()
            .into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.window.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let replaced = Arc::new(self.window.clone()).replace_children(children, options)?;
        let Some(window) = replaced.downcast_ref::<BoundedWindowAggExec>() else {
            return internal_err!("ROW_NUMBER child replacement changed its window type");
        };
        Ok(Arc::new(Self::try_new(window.clone())?))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_by = self.window.window_expr()[0].partition_by();
        if self.window.partition_by_sort_keys()?.len() != partition_by.len() {
            return exec_err!("All partition by columns should have an ordering in Sorted mode.");
        }
        let input = self
            .window
            .input()
            .execute(partition, Arc::clone(&context))?;
        let partition_by = partition_by.to_vec();
        let output_count = self.window.window_expr().len();
        let schema = self.schema();
        let state = RowNumberState {
            key: vec![],
            rank: 0,
            reservation: MemoryConsumer::new("ROW_NUMBER partition key")
                .register(context.memory_pool()),
        };
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = futures::stream::try_unfold(
            (input, state, metrics, partition_by),
            move |(mut input, mut state, metrics, partition_by)| {
                let schema = Arc::clone(&schema);
                async move {
                    let Some(batch) = input.next().await else {
                        return Ok(None);
                    };
                    let batch = batch?;
                    let output = {
                        let _timer = metrics.elapsed_compute().timer();
                        state.evaluate(batch, &partition_by, schema, output_count)?
                    };
                    (&output).record_output(&metrics);
                    Ok(Some((output, (input, state, metrics, partition_by))))
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.window.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.window.statistics_from_inputs(input_stats, args)
    }
}

struct RowNumberState {
    key: Vec<ArrayRef>,
    rank: u64,
    reservation: MemoryReservation,
}

impl RowNumberState {
    fn evaluate(
        &mut self,
        batch: RecordBatch,
        partition_by: &[Arc<dyn PhysicalExpr>],
        schema: SchemaRef,
        output_count: usize,
    ) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let mut ranks = Vec::with_capacity(rows);
        if rows != 0 {
            let keys = partition_by
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(rows))
                .collect::<Result<Vec<_>>>()?;
            let mut continues = keys.len() == self.key.len();
            for (key, previous) in keys.iter().zip(&self.key) {
                let compare = arrow::array::make_comparator(
                    previous.as_ref(),
                    key.as_ref(),
                    SortOptions::default(),
                )?;
                continues &= compare(0, 0).is_eq();
            }
            let ranges = if keys.is_empty() {
                std::iter::once(0..rows).collect::<Vec<_>>()
            } else {
                partition(&keys)?.ranges()
            };
            for range in &ranges {
                let start = if range.start == 0 && continues {
                    self.rank
                } else {
                    0
                };
                ranks.extend((1..=range.len() as u64).map(|offset| start.wrapping_add(offset)));
            }
            self.rank = *ranks.last().unwrap();
            if !keys.is_empty() && (!continues || ranges.len() != 1) {
                // Drop the old key before copying its replacement, but retain the reservation
                // so only the change in retained bytes adjusts the pool.
                self.key = vec![];
                let index = UInt64Array::from(vec![(rows - 1) as u64]);
                let key = keys
                    .iter()
                    .map(|array| {
                        if matches!(
                            array.data_type(),
                            DataType::Dictionary(_, _) | DataType::Utf8View | DataType::BinaryView
                        ) {
                            // A view or dictionary take retains its source buffers. Rebuild
                            // the logical value so unrelated bytes do not stay alive.
                            ScalarValue::try_from_array(array.as_ref(), rows - 1)?.to_array()
                        } else {
                            take(array.as_ref(), &index, None).map_err(Into::into)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                // Nested views and dictionaries can still share backing buffers after take.
                // Charge the buffers retained, rather than assuming a one-row key is small.
                let size = key
                    .iter()
                    .map(|array| array.get_array_memory_size())
                    .sum::<usize>()
                    + key.capacity() * std::mem::size_of::<ArrayRef>();
                self.reservation.try_resize(size)?;
                self.key = key;
            }
        }
        let ranks: ArrayRef = Arc::new(UInt64Array::from(ranks));
        let columns = batch
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::repeat_n(ranks, output_count))
            .collect();
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

#[cfg(test)]
mod tests;
