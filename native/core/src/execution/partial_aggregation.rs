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

//! Singleton-state conversion and operator-local partial aggregation policy.
//!
//! Conversion support does not establish that reassociating an aggregate is safe.
//! The planner must qualify semantics before enabling DataFusion's adaptive bypass.

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{new_empty_array, Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::{GroupsAccumulatorAdapter, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::execution::merge_as_partial::MergeAsPartialUDF;

const SINGLETON_CHUNK_ROWS: usize = 1024;

/// Requested policy retained while the base session disables unqualified partials.
#[derive(Debug, Clone)]
pub(crate) struct PartialAggregationConfig {
    pub probe_ratio_threshold: f64,
    pub native_shuffle: bool,
    pub allow_numerical_differences: bool,
}

/// Preserve the original expression as the factory, including its argument fields,
/// evaluation mode and state schema. PartialMerge already consumes and forwards
/// intermediate states, so it does not need a singleton-state wrapper.
pub(crate) fn wrap_aggregate_expr(
    expr: Arc<AggregateFunctionExpr>,
    schema: SchemaRef,
) -> Result<Arc<AggregateFunctionExpr>> {
    if expr
        .fun()
        .inner()
        .downcast_ref::<MergeAsPartialUDF>()
        .is_some()
    {
        return Ok(expr);
    }
    let wrapper = SingletonStateUDF {
        state_fields: expr.state_fields()?,
        inner: Arc::clone(&expr),
    };
    Ok(Arc::new(
        AggregateExprBuilder::new(
            Arc::new(AggregateUDF::new_from_impl(wrapper)),
            expr.expressions(),
        )
        .schema(schema)
        .alias(expr.name())
        .order_by(expr.order_bys().to_vec())
        .with_ignore_nulls(expr.ignore_nulls())
        .with_distinct(expr.is_distinct())
        .with_reversed(expr.is_reversed())
        .build()?,
    ))
}

#[derive(Debug)]
struct SingletonStateUDF {
    inner: Arc<AggregateFunctionExpr>,
    state_fields: Vec<FieldRef>,
}

impl PartialEq for SingletonStateUDF {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for SingletonStateUDF {}

impl Hash for SingletonStateUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.inner).hash(state);
    }
}

impl AggregateUDFImpl for SingletonStateUDF {
    fn name(&self) -> &str {
        self.inner.fun().name()
    }

    fn signature(&self) -> &Signature {
        self.inner.fun().signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.inner.field().data_type().clone())
    }

    fn return_field(&self, _arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(self.inner.field())
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.fun().coerce_types(arg_types)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(self.state_fields.clone())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.create_accumulator()
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SingletonStateGroupsAccumulator {
            inner: create_groups_accumulator(&self.inner)?,
            factory: Arc::clone(&self.inner),
            state_fields: self.state_fields.clone(),
        }))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.fun().order_sensitivity()
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.fun().is_descending()
    }
}

fn create_groups_accumulator(
    expr: &Arc<AggregateFunctionExpr>,
) -> Result<Box<dyn GroupsAccumulator>> {
    if expr.groups_accumulator_supported() {
        expr.create_groups_accumulator()
    } else {
        let expr = Arc::clone(expr);
        Ok(Box::new(GroupsAccumulatorAdapter::new(move || {
            expr.create_accumulator()
        })))
    }
}

struct SingletonStateGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    factory: Arc<AggregateFunctionExpr>,
    state_fields: Vec<FieldRef>,
}

impl SingletonStateGroupsAccumulator {
    fn singleton_states(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
        rows: usize,
    ) -> Result<Vec<ArrayRef>> {
        if rows == 0 {
            return Ok(self
                .state_fields
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect());
        }

        // Only output arrays grow with the input batch. Temporary group state and
        // identity indices are bounded, including for oversized upstream batches.
        let indices: Vec<usize> = (0..rows.min(SINGLETON_CHUNK_ROWS)).collect();
        let mut columns = vec![Vec::new(); self.state_fields.len()];
        for offset in (0..rows).step_by(SINGLETON_CHUNK_ROWS) {
            let len = (rows - offset).min(SINGLETON_CHUNK_ROWS);
            let values: Vec<ArrayRef> = values.iter().map(|a| a.slice(offset, len)).collect();
            let filter = opt_filter.map(|a| a.slice(offset, len));
            let mut accumulator = create_groups_accumulator(&self.factory)?;
            accumulator.update_batch(&values, &indices[..len], filter.as_ref(), len)?;
            let states = accumulator.state(EmitTo::All)?;
            for (column, state) in columns.iter_mut().zip(states) {
                column.push(state);
            }
        }
        columns
            .into_iter()
            .map(|mut chunks| {
                if chunks.len() == 1 {
                    Ok(chunks.pop().unwrap())
                } else {
                    let arrays: Vec<&dyn Array> = chunks.iter().map(|a| a.as_ref()).collect();
                    Ok(concat(&arrays)?)
                }
            })
            .collect()
    }
}

impl GroupsAccumulator for SingletonStateGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(states, group_indices, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.inner.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let rows = values
            .first()
            .map(|a| a.len())
            .or_else(|| opt_filter.map(Array::len))
            .ok_or_else(|| {
                DataFusionError::Internal("Singleton conversion requires an input array".into())
            })?;
        if !self.factory.groups_accumulator_supported() {
            // The scalar adapter's generic converter buffers ScalarValues for
            // the whole batch. Use bounded groups of scalar accumulators instead.
            self.singleton_states(values, opt_filter, rows)
        } else {
            match self.inner.convert_to_state(values, opt_filter) {
                Err(DataFusionError::NotImplemented(_)) => {
                    self.singleton_states(values, opt_filter, rows)
                }
                result => result,
            }
        }
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[derive(Debug)]
struct OriginalInputContext(Arc<TaskContext>);

/// Applies policy to exactly one aggregate. Its child receives the unchanged
/// incoming context, allowing nested aggregates to decide independently.
#[derive(Debug)]
pub(crate) struct PartialAggregationExec {
    aggregate: Arc<dyn ExecutionPlan>,
    eligible: bool,
    reason: &'static str,
    metrics: ExecutionPlanMetricsSet,
}

impl PartialAggregationExec {
    pub(crate) fn try_new(
        aggregate: Arc<dyn ExecutionPlan>,
        eligible: bool,
        reason: &'static str,
    ) -> Result<Self> {
        let children = aggregate.children();
        if children.len() != 1 {
            return internal_err!("Partial aggregation policy requires one aggregate input");
        }
        let input = Arc::new(RestoreInputContextExec {
            input: Arc::clone(children[0]),
        });
        let aggregate = aggregate.replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )?;
        Ok(Self {
            aggregate,
            eligible,
            reason,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for PartialAggregationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometPartialAggregationExec: {}", self.reason)
    }
}

impl ExecutionPlan for PartialAggregationExec {
    fn name(&self) -> &str {
        "CometPartialAggregationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.aggregate.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.aggregate]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("Partial aggregation policy requires one child");
        }
        Ok(Arc::new(Self {
            aggregate: children.remove(0),
            eligible: self.eligible,
            reason: self.reason,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut config = context.session_config().clone();
        let requested = config
            .get_extension::<PartialAggregationConfig>()
            .map(|policy| policy.probe_ratio_threshold)
            .unwrap_or(
                config
                    .options()
                    .execution
                    .skip_partial_aggregation_probe_ratio_threshold,
            );
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_ratio_threshold =
            if self.eligible { requested } else { 1.1 };
        config.set_extension(Arc::new(OriginalInputContext(Arc::clone(&context))));
        let local = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        MetricBuilder::new(&self.metrics)
            .counter(
                if self.eligible {
                    "partial_bypass_eligible_partitions"
                } else {
                    "partial_bypass_ineligible_partitions"
                },
                partition,
            )
            .add(1);
        let input = self.aggregate.execute(partition, local)?;
        if !self.eligible || requested >= 1.0 {
            return Ok(input);
        }

        // Partial aggregation can emit short batches under memory pressure as well as
        // during bypass. Bound retained states and admit space for Arrow's concatenated
        // output before retaining them. Full or oversized inputs stay zero-copy.
        let batch_size = context.session_config().batch_size();
        let memory_pool = Arc::clone(context.memory_pool());
        let time = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let state = PartialOutputBuffer {
            input,
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            reservation: None,
            pending: None,
            finished: false,
        };
        let stream = futures::stream::try_unfold(state, move |mut state| {
            let time = time.clone();
            let memory_pool = Arc::clone(&memory_pool);
            async move {
                loop {
                    if state.finished {
                        return Ok(None);
                    }
                    let next = match state.pending.take() {
                        Some(batch) => Some(Ok(batch)),
                        None => state.input.next().await,
                    };
                    let _timer = time.timer();
                    let Some(batch) = next else {
                        state.finished = true;
                        return if state.batches.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some((state.flush()?, state)))
                        };
                    };
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let bytes = batch.get_array_memory_size();
                    if batch.num_rows() >= batch_size || bytes > MAX_PARTIAL_OUTPUT_BYTES {
                        if state.batches.is_empty() {
                            return Ok(Some((batch, state)));
                        }
                        state.pending = Some(batch);
                        return Ok(Some((state.flush()?, state)));
                    }
                    let combined_bytes = state.bytes + bytes;
                    if combined_bytes > MAX_PARTIAL_OUTPUT_BYTES {
                        state.pending = Some(batch);
                        return Ok(Some((state.flush()?, state)));
                    }
                    let reservation = state.reservation.get_or_insert_with(|| {
                        MemoryConsumer::new("CometPartialOutput").register(&memory_pool)
                    });
                    // Inputs remain owned while concat_batches allocates its output.
                    if let Err(error) = reservation.try_resize(combined_bytes * 2) {
                        if !matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
                            return Err(error);
                        }
                        if state.batches.is_empty() {
                            state.reservation = None;
                            return Ok(Some((batch, state)));
                        }
                        state.pending = Some(batch);
                        return Ok(Some((state.flush()?, state)));
                    }
                    state.rows += batch.num_rows();
                    state.bytes = combined_bytes;
                    state.batches.push(batch);
                    if state.rows >= batch_size {
                        return Ok(Some((state.flush()?, state)));
                    }
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut metrics = self.aggregate.metrics().unwrap_or_default();
        for metric in self.metrics.clone_inner().iter() {
            metrics.push(Arc::clone(metric));
        }
        Some(metrics)
    }
}

// A row is an entire intermediate state, which can be a large list. The byte cap
// bounds a group even with an unbounded pool; admission reserves input plus output.
const MAX_PARTIAL_OUTPUT_BYTES: usize = 8 * 1024 * 1024;

struct PartialOutputBuffer {
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    reservation: Option<MemoryReservation>,
    // One already-read input waits while the preceding group is handed downstream.
    pending: Option<RecordBatch>,
    finished: bool,
}

impl PartialOutputBuffer {
    fn flush(&mut self) -> Result<RecordBatch> {
        let mut batches = std::mem::take(&mut self.batches);
        let output = if batches.len() == 1 {
            batches.pop().unwrap()
        } else {
            concat_batches(&self.input.schema(), &batches)?
        };
        drop(batches);
        self.rows = 0;
        self.bytes = 0;
        // The downstream operator now owns and accounts for this output.
        self.reservation = None;
        Ok(output)
    }
}

#[derive(Debug)]
struct RestoreInputContextExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for RestoreInputContextExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometPartialAggregationInput")
    }
}

impl ExecutionPlan for RestoreInputContextExec {
    fn name(&self) -> &str {
        "CometPartialAggregationInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("Partial aggregation input requires one child");
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let original = context
            .session_config()
            .get_extension::<OriginalInputContext>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Partial aggregation input has no original context".into(),
                )
            })?;
        self.input.execute(partition, Arc::clone(&original.0))
    }
}

#[cfg(test)]
mod tests;
