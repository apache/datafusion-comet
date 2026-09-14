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

//! Connect a hash join's completed build domain to its probe input.
//!
//! Comet does not run DataFusion's physical optimizer, which normally connects
//! dynamic-filter producers and consumers. This targeted wiring filters probe
//! batches and lets a direct Parquet reader use the same live predicate for
//! pruning. The original join verifies matches, including hash collisions.
//! This leaves Spark's operator tree and partitioning intact and does
//! not cross Spark exchanges or JVM/Arrow boundaries.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, JoinType, NullEquality, Result, ScalarValue, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{
    lit, BinaryExpr, Column, DynamicFilterPhysicalExpr, IsNotNullExpr,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    apply_expression_roots, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::CometFilterExec;

/// A task-local consumer of DataFusion's build-side runtime filter.
#[derive(Debug)]
pub(crate) struct DynamicFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    metrics: ExecutionPlanMetricsSet,
}

impl DynamicFilterExec {
    fn new(input: Arc<dyn ExecutionPlan>, predicate: Arc<DynamicFilterPhysicalExpr>) -> Self {
        Self {
            input,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for DynamicFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometDynamicFilterExec")
    }
}

impl ExecutionPlan for DynamicFilterExec {
    fn name(&self) -> &str {
        "CometDynamicFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // Removing rows preserves the input's schema, ordering and partitioning.
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([Arc::clone(&self.predicate) as Arc<dyn PhysicalExpr>], f)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
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

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometDynamicFilterExec requires one child");
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            Arc::clone(&self.predicate),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        // HashJoinExec resets its producer on reexecution. Never retain a previous
        // build's domain in the consumer. A reset plan safely bypasses filtering;
        // ordinary Spark task attempts each construct a fresh, connected plan.
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            self.predicate.children().into_iter().cloned().collect(),
            lit(true),
        ));
        Ok(Arc::new(Self::new(Arc::clone(&self.input), predicate)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let children = self.predicate.children();
        let [key] = children.as_slice() else {
            return internal_err!("CometDynamicFilterExec requires one join-key column");
        };
        let Some(key) = key.downcast_ref::<Column>() else {
            return internal_err!("CometDynamicFilterExec requires a direct join-key column");
        };
        let key_index = key.index();
        let predicate = Arc::clone(&self.predicate)
            .with_new_children(vec![Arc::new(Column::new(key.name(), 0))])?;
        let input = self.input.execute(partition, context)?;
        let evaluated =
            MetricBuilder::new(&self.metrics).counter("dynamic_filter_rows_evaluated", partition);
        let pruned =
            MetricBuilder::new(&self.metrics).counter("dynamic_filter_rows_pruned", partition);
        let bypassed =
            MetricBuilder::new(&self.metrics).counter("dynamic_filter_rows_bypassed", partition);
        // Only dedicated metrics: merging this helper into the Spark join must not
        // add its input/output counts or elapsed time to the join's existing metrics.
        let eval_time =
            MetricBuilder::new(&self.metrics).subset_time("dynamic_filter_eval_time", partition);
        let stream = input.map(move |batch| {
            let batch = batch?;
            let _timer = eval_time.timer();
            // AND may prefilter its input before evaluating hash membership. A
            // zero-copy key projection keeps payload columns out of that temporary
            // batch. The remapped expression still observes live producer updates.
            let key_batch = batch.project(&[key_index])?;
            match predicate.evaluate(&key_batch)? {
                // DataFusion leaves this placeholder unchanged until the complete
                // build is available, or if it declines to populate the filter.
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                    bypassed.add(batch.num_rows());
                    Ok(batch)
                }
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => {
                    evaluated.add(batch.num_rows());
                    pruned.add(batch.num_rows());
                    Ok(batch.slice(0, 0))
                }
                ColumnarValue::Array(mask) => {
                    let filtered = filter_record_batch(&batch, as_boolean_array(&mask)?)?;
                    evaluated.add(batch.num_rows());
                    pruned.add(batch.num_rows() - filtered.num_rows());
                    Ok(filtered)
                }
                _ => internal_err!("Join dynamic filter must evaluate to a Boolean"),
            }
        });
        // Return even empty batches. Each poll consumes at most one input batch,
        // so a selective filter cannot drain a ready input in an unbounded loop.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A permanent plan must not own a completed join's filter or build accumulator:
/// those can retain the hash map after its stream-owned reservation is released.
/// Keep an unexecuted template here and create the producer and consumer together
/// for each stream. Only their metric handles are retained by the Spark plan.
#[derive(Debug)]
pub(crate) struct DynamicFilterJoinExec {
    template: HashJoinExec,
    config: ConfigOptions,
    metrics: ExecutionPlanMetricsSet,
}

/// Per-execution join state. The permanent plan keeps no live filter; this value
/// records whether this execution also connected its filter to the Parquet reader.
struct RuntimeDynamicFilterJoin {
    join: HashJoinExec,
    reader_filter_attached: bool,
}

/// Recognize only direct-column null checks joined by AND, without evaluating
/// or changing the predicate. Every accepted leaf is deterministic, infallible,
/// and only discards rows, so reader pruning cannot suppress expression errors
/// or alter stateful evaluation. All other expressions remain a boundary.
fn is_direct_column_null_checks(predicate: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>() {
        return binary.op() == &Operator::And
            && is_direct_column_null_checks(binary.left())
            && is_direct_column_null_checks(binary.right());
    }
    predicate
        .downcast_ref::<IsNotNullExpr>()
        .is_some_and(|is_not_null| is_not_null.arg().is::<Column>())
}

fn try_attach_parquet_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Filtering before a fetch can change which rows are selected by its limit.
    if input.fetch().is_some() {
        log::debug!("Join dynamic filter reader pushdown skipped: probe has a fetch limit");
        return Ok(None);
    }
    // Spark inserts IS NOT NULL residuals above equijoin inputs, including AND
    // chains of inferred null checks. A reader predicate can cross those direct
    // checks because both operations only discard rows. Keep every other filter
    // as a boundary: reader pruning would change which rows reach stateful
    // expressions and can suppress expression errors.
    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        if filter.has_projection() {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: probe FilterExec has a projection"
            );
            return Ok(None);
        }
        if !is_direct_column_null_checks(filter.predicate()) {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: probe filter is not direct column IS NOT NULL checks"
            );
            return Ok(None);
        }
        let Some(reader) =
            try_attach_parquet_reader_filter(filter.input(), Arc::clone(&predicate), config)?
        else {
            return Ok(None);
        };
        return match filter.with_execution_input(reader) {
            Ok(updated) => Ok(Some(updated)),
            Err(error) => {
                log::debug!(
                    "Join dynamic filter reader pushdown skipped: probe filter rebuild failed: {error}"
                );
                Ok(None)
            }
        };
    }
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        log::debug!(
            "Join dynamic filter reader pushdown skipped: probe root is {}",
            input.name()
        );
        return Ok(None);
    };
    if scan.downcast_to_file_source::<ParquetSource>().is_none() {
        log::debug!("Join dynamic filter reader pushdown skipped: probe is not Parquet");
        return Ok(None);
    }

    let predicate: Arc<dyn PhysicalExpr> = predicate;
    let propagation = match scan
        .data_source()
        .try_pushdown_filters(vec![predicate], config)
    {
        Ok(propagation) => propagation,
        Err(error) => {
            log::debug!(
                "Join dynamic filter reader pushdown skipped: predicate remapping failed: {error}"
            );
            return Ok(None);
        }
    };
    let Some(data_source) = propagation.updated_node else {
        log::debug!("Join dynamic filter reader pushdown skipped: Parquet declined the predicate");
        return Ok(None);
    };
    Ok(Some(Arc::new(scan.clone().with_data_source(data_source))))
}

impl DynamicFilterJoinExec {
    /// Return no wrapper when the join cannot safely use a runtime filter.
    pub(crate) fn try_new(join: &HashJoinExec, config: &ConfigOptions) -> Result<Option<Self>> {
        if let Some(reason) = ineligible_reason(join, config)? {
            log::debug!("Join dynamic filter skipped: {reason}");
            return Ok(None);
        }
        Ok(Some(Self::new(join, config.clone())?))
    }

    fn new(join: &HashJoinExec, config: ConfigOptions) -> Result<Self> {
        Ok(Self {
            template: join.builder().reset_state().build()?,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn build_runtime_join(&self) -> Result<RuntimeDynamicFilterJoin> {
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&self.template.on()[0].1)],
            lit(true),
        ));
        let reader = try_attach_parquet_reader_filter(
            self.template.right(),
            Arc::clone(&predicate),
            &self.config,
        )?;
        let reader_filter_attached = reader.is_some();
        let consumer = Arc::new(DynamicFilterExec {
            input: reader.unwrap_or_else(|| Arc::clone(self.template.right())),
            predicate: Arc::clone(&predicate),
            metrics: self.metrics.clone(),
        });
        // In particular, do not share CollectLeft's cached build future with the
        // template, another execution, or a reset plan.
        let join = self
            .template
            .builder()
            .reset_state()
            .with_new_children(vec![Arc::clone(self.template.left()), consumer])?
            .build()?
            .with_dynamic_filter_expr(predicate)?;
        Ok(RuntimeDynamicFilterJoin {
            join,
            reader_filter_attached,
        })
    }

    fn execute_runtime_join(
        &self,
        join: HashJoinExec,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // DataFusion can materialize one IN-list literal per build row,
        // despite admitting the list by packed-array bytes and distinct-key count.
        // Avoid that unreserved allocation. Map membership
        // reuses the join's already-reserved hash table and preserves duplicates.
        let mut config = context.session_config().clone();
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_size = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_distinct_values = 0;
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        let result = join.execute(partition, context);
        // HashJoinExec registers its metrics synchronously in execute(). Keep the
        // live counters, including on error, without retaining the producer plan.
        for metric in join.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        drop(join);
        let input = result?;
        // Drop execution state at EOF or error even if the caller retains the
        // exhausted stream. Dropping a pending stream also drops all of its state.
        let stream = futures::stream::unfold(Some(input), |input| async move {
            let mut input = input?;
            let batch = input.next().await?;
            let remaining = if batch.is_ok() { Some(input) } else { None };
            Some((batch, remaining))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for DynamicFilterJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometDynamicFilterJoinExec: ")?;
        self.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for DynamicFilterJoinExec {
    fn name(&self) -> &str {
        "CometDynamicFilterJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.template.properties()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.template.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.template.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.template.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.template.apply_expressions(f)
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

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = self
            .template
            .builder()
            .reset_state()
            .with_new_children(children)?
            .build()?;
        match Self::try_new(&join, &self.config)? {
            Some(wrapper) => Ok(Arc::new(wrapper)),
            None => Ok(Arc::new(join)),
        }
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(&self.template, self.config.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let runtime = self.build_runtime_join()?;
        let attachment_metric = if runtime.reader_filter_attached {
            "dynamic_filter_reader_filters_attached"
        } else {
            "dynamic_filter_reader_filters_skipped"
        };
        MetricBuilder::new(&self.metrics)
            .counter(attachment_metric, partition)
            .add(1);
        self.execute_runtime_join(runtime.join, partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.template.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.template.statistics_from_inputs(input_stats, args)
    }
}

fn ineligible_reason(join: &HashJoinExec, config: &ConfigOptions) -> Result<Option<&'static str>> {
    if !config.optimizer.enable_dynamic_filter_pushdown
        || !config.optimizer.enable_join_dynamic_filter_pushdown
    {
        return Ok(Some("disabled by DataFusion session options"));
    }
    if join.join_type() != &JoinType::Inner
        || join.null_equality() != NullEquality::NullEqualsNothing
    {
        return Ok(Some("only ordinary inner equijoins are supported"));
    }
    if !matches!(
        join.partition_mode(),
        PartitionMode::Partitioned | PartitionMode::CollectLeft
    ) {
        return Ok(Some("unresolved hash join partition mode"));
    }
    if config.optimizer.preserve_file_partitions > 0
        && matches!(join.partition_mode(), PartitionMode::Partitioned)
    {
        return Ok(Some("DataFusion preserve_file_partitions is enabled"));
    }
    // Spark, not DataFusion, routes rows across tasks. Restrict the filter to the
    // single native partition executed by this Spark task: no shared domains or
    // assumptions about DataFusion's repartition hash across Spark partitions.
    if join.left().output_partitioning().partition_count() != 1
        || join.right().output_partitioning().partition_count() != 1
    {
        return Ok(Some("requires one native partition per input"));
    }
    let [(build_key, probe_key)] = join.on() else {
        return Ok(Some("requires one join key"));
    };
    if !build_key.is::<Column>() || !probe_key.is::<Column>() {
        return Ok(Some("computed join keys are not supported"));
    }
    let build_type = build_key.data_type(join.left().schema().as_ref())?;
    let probe_type = probe_key.data_type(join.right().schema().as_ref())?;
    if build_type != probe_type
        || !matches!(
            build_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    {
        return Ok(Some("requires matching signed integer keys"));
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
