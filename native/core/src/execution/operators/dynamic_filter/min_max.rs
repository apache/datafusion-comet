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

//! Connect one partial MIN/MAX aggregate's improving bound to its Parquet reader.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, Statistics};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::min_max::{Max, Min};
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::{OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::parquet_reader::{parquet_file_source, try_attach_parquet_reader_filter};

/// Own an unexecuted aggregate template and metric handles. Each execution owns
/// a fresh aggregate, accumulator bounds, and reader predicate; none of that live
/// state is retained by the template or shared with another Spark task.
#[derive(Debug)]
pub(crate) struct MinMaxReaderFilterExec {
    template: AggregateExec,
    config: ConfigOptions,
    metrics: ExecutionPlanMetricsSet,
}

struct RuntimeAggregate {
    aggregate: Arc<dyn ExecutionPlan>,
    reader_filter_attached: bool,
}

impl MinMaxReaderFilterExec {
    // Spark planner integration is a separate step of apache/datafusion-comet#5774.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn try_new(
        aggregate: &AggregateExec,
        config: &ConfigOptions,
    ) -> Result<Option<Self>> {
        if !config.optimizer.enable_dynamic_filter_pushdown
            || !config.optimizer.enable_aggregate_dynamic_filter_pushdown
            || aggregate.mode() != &AggregateMode::Partial
            || !aggregate.group_expr().is_true_no_grouping()
            || aggregate.limit_options().is_some()
            || aggregate.filter_expr().iter().any(Option::is_some)
            || aggregate.input().output_partitioning().partition_count() != 1
            || aggregate.input().fetch().is_some()
        {
            return Ok(None);
        }
        let [expression] = aggregate.aggr_expr() else {
            return Ok(None);
        };
        let function = expression.fun().inner();
        if !(function.is::<Min>() || function.is::<Max>())
            || expression.is_distinct()
            || !expression.order_bys().is_empty()
        {
            return Ok(None);
        }
        let arguments = expression.expressions();
        let [key] = arguments.as_slice() else {
            return Ok(None);
        };
        if !key.is::<Column>()
            || !matches!(
                key.data_type(aggregate.input().schema().as_ref())?,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        {
            return Ok(None);
        }
        // Keep expressions, residuals, limits, and native/JVM boundaries out of
        // this first path. The shared helper still handles reader column mapping
        // and per-file schema guards once this direct scan is accepted.
        let Some(scan) = aggregate.input().downcast_ref::<DataSourceExec>() else {
            return Ok(None);
        };
        let Some((_, source)) = parquet_file_source(scan) else {
            return Ok(None);
        };
        if source.filter().is_some()
            || source
                .projection()
                .is_some_and(|projection| projection.iter().any(|expr| !expr.expr.is::<Column>()))
        {
            // Pruning before a computed scan projection could change its stateful
            // evaluation or suppress errors, even for an unselected output column.
            return Ok(None);
        }
        Ok(Some(Self::new(aggregate, config.clone())?))
    }

    fn new(aggregate: &AggregateExec, config: ConfigOptions) -> Result<Self> {
        Ok(Self {
            template: Self::fresh_aggregate(aggregate, Arc::clone(aggregate.input()))?,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn fresh_aggregate(
        template: &AggregateExec,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<AggregateExec> {
        // AggregateExec::clone/replace_children keep shared accumulator bounds.
        // Replacing just its dynamic expression would leave those bounds stale.
        AggregateExec::try_new(
            *template.mode(),
            template.group_expr().clone(),
            template.aggr_expr().to_vec(),
            template.filter_expr().to_vec(),
            input,
            template.input_schema(),
        )
    }

    fn build_runtime_aggregate(&self) -> Result<RuntimeAggregate> {
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(Self::fresh_aggregate(
            &self.template,
            Arc::clone(self.template.input()),
        )?);
        let predicate = aggregate
            .dynamic_expressions_produced()
            .into_iter()
            .find_map(|expr| {
                (expr as Arc<dyn std::any::Any + Send + Sync>)
                    .downcast::<DynamicFilterPhysicalExpr>()
                    .ok()
            });
        let Some(predicate) = predicate else {
            return Ok(RuntimeAggregate {
                aggregate,
                reader_filter_attached: false,
            });
        };
        let reader =
            try_attach_parquet_reader_filter(self.template.input(), predicate, &self.config)?;
        let reader_filter_attached = reader.is_some();
        // Replacement preserves the fresh aggregate's predicate and accumulator
        // bounds, so the reader observes exactly the state the aggregate updates.
        let aggregate = match reader {
            Some(reader) => aggregate.replace_children(
                vec![reader],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            None => aggregate,
        };
        Ok(RuntimeAggregate {
            aggregate,
            reader_filter_attached,
        })
    }

    fn execute_runtime_aggregate(
        &self,
        aggregate: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let result = aggregate.execute(partition, context);
        // The ungrouped stream registers its counters in execute(). Retain only
        // their handles, including on error, so the runtime state can be dropped.
        for metric in aggregate.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        drop(aggregate);
        let input = result?;
        // Also release state at EOF/error if the caller retains the spent stream.
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

impl DisplayAs for MinMaxReaderFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometMinMaxReaderFilterExec: ")?;
        self.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for MinMaxReaderFilterExec {
    fn name(&self) -> &str {
        "CometMinMaxReaderFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.template.properties()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.template.input_distribution_requirements()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.template.required_input_ordering()
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
        // The template's unused predicate is not a permanent-plan consumer.
        datafusion::physical_plan::apply_expression_roots(
            self.template.aggr_expr()[0].expressions(),
            f,
        )
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
            return internal_err!("CometMinMaxReaderFilterExec requires one child");
        }
        let aggregate = Self::fresh_aggregate(&self.template, children.remove(0))?;
        match Self::try_new(&aggregate, &self.config)? {
            Some(wrapper) => Ok(Arc::new(wrapper)),
            None => Ok(Arc::new(aggregate)),
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
        let runtime = self.build_runtime_aggregate()?;
        let attachment_metric = if runtime.reader_filter_attached {
            "dynamic_filter_minmax_filters_attached"
        } else {
            "dynamic_filter_minmax_filters_skipped"
        };
        MetricBuilder::new(&self.metrics)
            .counter(attachment_metric, partition)
            .add(1);
        self.execute_runtime_aggregate(runtime.aggregate, partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.template.cardinality_effect()
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

#[cfg(test)]
mod tests {
    use super::super::parquet_reader::concrete_parquet_source;
    use super::*;
    use crate::parquet::parquet_exec::init_datasource_exec;
    use crate::parquet::parquet_support::ObjectStoreBackend;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::compute::cast;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{lit, CastExpr, IsNotNullExpr};
    use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    fn session() -> Arc<SessionContext> {
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(100)
            .with_parquet_page_index_pruning(false);
        // Measure row-group pruning independently of decoder filtering and page indexes.
        config.options_mut().execution.parquet.pushdown_filters = false;
        Arc::new(SessionContext::new_with_config(config))
    }

    fn aggregate(input: Arc<dyn ExecutionPlan>, is_max: bool) -> AggregateExec {
        let expr = AggregateExprBuilder::new(
            if is_max { max_udaf() } else { min_udaf() },
            vec![Arc::new(Column::new("key", 0))],
        )
        .schema(input.schema())
        .alias("extremum")
        .build()
        .unwrap();
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![Arc::new(expr)],
            vec![None],
            Arc::clone(&input),
            input.schema(),
        )
        .unwrap()
    }

    fn wrapper(aggregate: &AggregateExec) -> MinMaxReaderFilterExec {
        MinMaxReaderFilterExec::try_new(aggregate, &ConfigOptions::default())
            .unwrap()
            .expect("eligible MIN/MAX")
    }

    fn result(batches: &[RecordBatch]) -> Option<i32> {
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let batch = batches.iter().find(|batch| batch.num_rows() == 1).unwrap();
        let column = cast(batch.column(0), &DataType::Int32).unwrap();
        column
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .next()
            .unwrap()
    }

    fn metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
        plan.metrics()
            .and_then(|metrics| match name {
                "output_rows" => metrics.output_rows(),
                _ => metrics.sum_by_name(name).map(|value| value.as_usize()),
            })
            .unwrap_or(0)
    }

    fn produced_filter(aggregate: &dyn ExecutionPlan) -> Arc<DynamicFilterPhysicalExpr> {
        let mut expressions = aggregate.dynamic_expressions_produced();
        assert_eq!(expressions.len(), 1);
        (expressions.pop().unwrap() as Arc<dyn std::any::Any + Send + Sync>)
            .downcast::<DynamicFilterPhysicalExpr>()
            .unwrap()
    }

    #[tokio::test]
    async fn signed_integer_extrema_preserve_empty_null_and_duplicate_results() {
        let session = session();
        for key_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for values in [
                vec![],
                vec![None; 4],
                vec![None, Some(-8), Some(4), Some(-8), None],
            ] {
                for is_max in [false, true] {
                    let expected = if is_max {
                        values.iter().flatten().max()
                    } else {
                        values.iter().flatten().min()
                    }
                    .copied();
                    let (_file, scan) = parquet_input(values.clone(), &key_type, &session, 2);
                    let plain = aggregate(scan, is_max);
                    let filtered = Arc::new(wrapper(&plain));
                    let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
                        .await
                        .unwrap();
                    let baseline = collect(Arc::new(plain), session.task_ctx()).await.unwrap();
                    assert_eq!(result(&actual), expected, "{key_type:?}, max={is_max}");
                    assert_eq!(result(&actual), result(&baseline));
                    assert_eq!(
                        metric(filtered.as_ref(), "dynamic_filter_minmax_filters_attached"),
                        1
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn live_bounds_prune_remapped_row_groups_and_keep_scan_metrics_on_the_scan() {
        let session = session();
        for is_max in [false, true] {
            for favorable in [false, true] {
                // MIN improves 200 -> 100 -> 0; MAX uses the negated layout.
                let mut starts = if favorable {
                    vec![200, 100, 150, 0, 50, 300]
                } else {
                    vec![500, 400, 300, 200, 100, 0]
                };
                let sign = if is_max { -1 } else { 1 };
                let values: Vec<_> = starts
                    .drain(..)
                    .flat_map(|start| start..start + 100)
                    .map(|value| Some(sign * value))
                    .collect();
                let (_file, scan) = parquet_input(values.clone(), &DataType::Int32, &session, 100);
                let filtered = Arc::new(wrapper(&aggregate(Arc::clone(&scan) as _, is_max)));
                let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
                    .await
                    .unwrap();
                let (_baseline_file, baseline_scan) =
                    parquet_input(values, &DataType::Int32, &session, 100);
                let baseline = collect(
                    Arc::new(aggregate(Arc::clone(&baseline_scan) as _, is_max)),
                    session.task_ctx(),
                )
                .await
                .unwrap();
                assert_eq!(result(&actual), result(&baseline));
                assert_eq!(metric(baseline_scan.as_ref(), "output_rows"), 600);
                assert_eq!(
                    metric(baseline_scan.as_ref(), "row_groups_pruned_dynamic_filter"),
                    0
                );
                assert_eq!(metric(filtered.as_ref(), "output_rows"), 1);
                assert_eq!(
                    metric(filtered.as_ref(), "dynamic_filter_minmax_filters_attached"),
                    1
                );
                assert!(filtered
                    .metrics()
                    .unwrap()
                    .sum_by_name("bytes_scanned")
                    .is_none());
                if favorable {
                    assert!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter") >= 3);
                    assert!(metric(scan.as_ref(), "output_rows") <= 300);
                    assert!(
                        metric(scan.as_ref(), "bytes_scanned")
                            < metric(baseline_scan.as_ref(), "bytes_scanned")
                    );
                } else {
                    assert_eq!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"), 0);
                    assert_eq!(metric(scan.as_ref(), "output_rows"), 600);
                }
            }
        }
    }

    #[tokio::test]
    async fn each_execution_resets_both_the_predicate_and_accumulator_bounds() {
        let session = session();
        let (_file, scan) = parquet_input(
            vec![Some(4), Some(3), Some(2), Some(1)],
            &DataType::Int32,
            &session,
            2,
        );
        let plan = Arc::new(wrapper(&aggregate(scan, false)));
        let first = plan.build_runtime_aggregate().unwrap();
        let second = plan.build_runtime_aggregate().unwrap();
        let first_filter = produced_filter(first.aggregate.as_ref());
        let second_filter = produced_filter(second.aggregate.as_ref());
        assert_ne!(first_filter.expression_id(), second_filter.expression_id());
        for (runtime, predicate) in [(first, first_filter), (second, second_filter)] {
            assert_eq!(predicate.snapshot_generation(), 1);
            let rows = collect(runtime.aggregate, session.task_ctx())
                .await
                .unwrap();
            assert_eq!(result(&rows), Some(1));
            // A fresh predicate with stale shared bounds would never publish this bound.
            assert!(predicate.snapshot_generation() > 1);
        }
        for execution in [
            Arc::clone(&plan) as Arc<dyn ExecutionPlan>,
            Arc::clone(&plan) as _,
            Arc::clone(&plan).reset_state().unwrap(),
        ] {
            assert_eq!(
                result(&collect(execution, session.task_ctx()).await.unwrap()),
                Some(1)
            );
        }
        let (_replacement_file, replacement) = parquet_input(
            vec![Some(104), Some(103), Some(102), Some(101)],
            &DataType::Int32,
            &session,
            2,
        );
        let replaced = plan
            .replace_children(
                vec![replacement],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        assert_eq!(
            result(&collect(replaced, session.task_ctx()).await.unwrap()),
            Some(101)
        );
    }

    #[tokio::test]
    async fn completed_or_cancelled_streams_release_the_runtime_predicate() {
        let session = session();
        let (_file, scan) = parquet_input(
            vec![Some(4), Some(3), Some(2), Some(1)],
            &DataType::Int32,
            &session,
            2,
        );
        let plan = wrapper(&aggregate(scan, false));
        for complete in [false, true] {
            let runtime = plan.build_runtime_aggregate().unwrap();
            let weak = Arc::downgrade(&produced_filter(runtime.aggregate.as_ref()));
            let mut stream = plan
                .execute_runtime_aggregate(runtime.aggregate, 0, session.task_ctx())
                .unwrap();
            if complete {
                while let Some(batch) = stream.next().await {
                    batch.unwrap();
                }
                assert!(
                    weak.upgrade().is_none(),
                    "retaining a spent stream must not retain its bound"
                );
            }
            drop(stream);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn computed_scan_projections_are_rejected() {
        let session = session();
        let (_file, scan) = parquet_input(vec![Some(1), Some(2)], &DataType::Int32, &session, 1);
        let (file_config, source) = parquet_file_source(&scan).unwrap();
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let computed: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(Arc::clone(&column), DataType::Int64, None));
        for projection in [
            // The aggregate still sees a direct Column, but the scan computes it.
            vec![ProjectionExpr::new(Arc::clone(&computed), "key")],
            // Even an unused computed output must not be skipped by pruning.
            vec![
                ProjectionExpr::new(Arc::clone(&column), "key"),
                ProjectionExpr::new(computed, "computed"),
            ],
        ] {
            let mut config = file_config.clone();
            config.file_source = source
                .try_pushdown_projection(&ProjectionExprs::new(projection))
                .unwrap()
                .unwrap();
            let scan = Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config)));
            assert!(MinMaxReaderFilterExec::try_new(
                &aggregate(scan, false),
                &ConfigOptions::default(),
            )
            .unwrap()
            .is_none());
        }
    }

    #[test]
    fn unsupported_aggregate_shapes_and_reader_boundaries_are_rejected() {
        let session = session();
        let (_file, scan) = parquet_input(vec![Some(1), Some(2)], &DataType::Int32, &session, 1);
        let plain = aggregate(Arc::clone(&scan) as _, false);
        let config = ConfigOptions::default();
        let rejected = |aggregate: AggregateExec| {
            assert!(MinMaxReaderFilterExec::try_new(&aggregate, &config)
                .unwrap()
                .is_none());
        };
        for mode in [AggregateMode::Final, AggregateMode::Single] {
            rejected(
                AggregateExec::try_new(
                    mode,
                    PhysicalGroupBy::default(),
                    plain.aggr_expr().to_vec(),
                    vec![None],
                    Arc::clone(&scan) as _,
                    scan.schema(),
                )
                .unwrap(),
            );
        }
        rejected(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new("key", 0)),
                    "key".to_owned(),
                )]),
                plain.aggr_expr().to_vec(),
                vec![None],
                Arc::clone(&scan) as _,
                scan.schema(),
            )
            .unwrap(),
        );
        rejected(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                plain.aggr_expr().to_vec(),
                vec![Some(lit(true))],
                Arc::clone(&scan) as _,
                scan.schema(),
            )
            .unwrap(),
        );
        let sum = AggregateExprBuilder::new(
            sum_udaf(),
            vec![Arc::new(CastExpr::new(
                Arc::new(Column::new("key", 0)),
                DataType::Int64,
                None,
            ))],
        )
        .schema(scan.schema())
        .alias("sum")
        .build()
        .unwrap();
        rejected(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                vec![Arc::clone(&plain.aggr_expr()[0]), Arc::new(sum)],
                vec![None, None],
                Arc::clone(&scan) as _,
                scan.schema(),
            )
            .unwrap(),
        );
        for distinct in [false, true] {
            let argument: Arc<dyn PhysicalExpr> = if distinct {
                Arc::new(Column::new("key", 0))
            } else {
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("key", 0)),
                    DataType::Int64,
                    None,
                ))
            };
            let expr = AggregateExprBuilder::new(min_udaf(), vec![argument])
                .schema(scan.schema())
                .alias("min")
                .with_distinct(distinct)
                .build()
                .unwrap();
            rejected(
                AggregateExec::try_new(
                    AggregateMode::Partial,
                    PhysicalGroupBy::default(),
                    vec![Arc::new(expr)],
                    vec![None],
                    Arc::clone(&scan) as _,
                    scan.schema(),
                )
                .unwrap(),
            );
        }
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::clone(&column), "key".into())],
                Arc::clone(&scan) as _,
            )
            .unwrap(),
        );
        rejected(aggregate(projection, false));
        let filter = Arc::new(
            FilterExec::try_new(Arc::new(IsNotNullExpr::new(column)), Arc::clone(&scan) as _)
                .unwrap(),
        );
        rejected(aggregate(filter, false));
        rejected(aggregate(scan.with_fetch(Some(1)).unwrap(), false));
        for key_type in [DataType::UInt32, DataType::Float64] {
            let (_file, other) = parquet_input(vec![Some(1), Some(2)], &key_type, &session, 1);
            rejected(aggregate(other, false));
        }
        for aggregate_enabled in [false, true] {
            let mut disabled = config.clone();
            disabled.optimizer.enable_dynamic_filter_pushdown = aggregate_enabled;
            disabled.optimizer.enable_aggregate_dynamic_filter_pushdown = !aggregate_enabled;
            assert!(MinMaxReaderFilterExec::try_new(&plain, &disabled)
                .unwrap()
                .is_none());
        }
        let input =
            RecordBatch::try_new(scan.schema(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let memory = MemorySourceConfig::try_new_exec(&[vec![input]], scan.schema(), None).unwrap();
        rejected(aggregate(memory, false));
        let (file_config, _, _) = concrete_parquet_source(&scan).unwrap();
        let mut partitioned = file_config.clone();
        partitioned
            .file_groups
            .push(partitioned.file_groups[0].clone());
        rejected(aggregate(
            Arc::new(
                scan.as_ref()
                    .clone()
                    .with_data_source(Arc::new(partitioned)),
            ),
            false,
        ));
    }

    #[tokio::test]
    async fn unavailable_reader_attachment_keeps_the_ordinary_aggregate() {
        let session = session();
        let (_file, scan) = parquet_input(vec![Some(-3), Some(5)], &DataType::Int32, &session, 1);
        let (config, _, _) = concrete_parquet_source(&scan).unwrap();
        let mut config = config.clone();
        config.file_groups = config
            .file_groups
            .iter()
            .map(|group| {
                group
                    .files()
                    .iter()
                    .cloned()
                    .map(|file| {
                        file.with_statistics(Arc::new(Statistics::new_unknown(&scan.schema())))
                    })
                    .collect::<Vec<_>>()
                    .into()
            })
            .collect();
        let scan = Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config)));
        let plan = Arc::new(wrapper(&aggregate(scan, false)));
        let rows = collect(Arc::clone(&plan) as _, session.task_ctx())
            .await
            .unwrap();
        assert_eq!(result(&rows), Some(-3));
        assert_eq!(
            metric(plan.as_ref(), "dynamic_filter_minmax_filters_skipped"),
            1
        );
        assert_eq!(
            metric(plan.as_ref(), "dynamic_filter_minmax_filters_attached"),
            0
        );
    }

    #[tokio::test]
    async fn missing_null_counts_do_not_hide_later_extrema() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::metadata::ParquetMetaDataWriter;
        use parquet::file::statistics::Statistics as ParquetStatistics;
        use parquet::file::writer::TrackedWrite;
        use std::io::Write;
        let session = session();
        for is_max in [false, true] {
            let sign = if is_max { -1 } else { 1 };
            let (file, _) = parquet_input(
                vec![None, Some(10 * sign), None, Some(-10 * sign)],
                &DataType::Int32,
                &session,
                2,
            );
            let original = std::fs::read(file.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
            let mut builder = reader.metadata().as_ref().clone().into_builder();
            let groups = builder
                .take_row_groups()
                .into_iter()
                .map(|group| {
                    let mut columns = group.columns().to_vec();
                    let Some(ParquetStatistics::Int32(stats)) = columns[1].statistics() else {
                        panic!("INT32 statistics expected")
                    };
                    let replacement = ParquetStatistics::int32(
                        stats.min_opt().copied(),
                        stats.max_opt().copied(),
                        None,
                        None,
                        false,
                    );
                    columns[1] = columns[1]
                        .clone()
                        .into_builder()
                        .set_statistics(replacement)
                        .build()
                        .unwrap();
                    group
                        .into_builder()
                        .set_column_metadata(columns)
                        .build()
                        .unwrap()
                })
                .collect();
            let metadata = builder.set_row_groups(groups).build();
            let mut output = TrackedWrite::new(file.reopen().unwrap());
            output.write_all(&original).unwrap();
            ParquetMetaDataWriter::new_with_tracked(output, &metadata)
                .finish()
                .unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new("payload", DataType::Int32, false),
                Field::new("key", DataType::Int32, true),
            ]));
            let scan = parquet_scan(&[&file], schema, vec![1], None, false, &session);
            let plain = aggregate(scan, is_max);
            let filtered = Arc::new(wrapper(&plain));
            assert_eq!(
                result(&collect(filtered, session.task_ctx()).await.unwrap()),
                Some(-10 * sign)
            );
            assert_eq!(
                result(&collect(Arc::new(plain), session.task_ctx()).await.unwrap()),
                Some(-10 * sign)
            );
        }
    }

    fn parquet_input(
        values: Vec<Option<i32>>,
        key_type: &DataType,
        session: &Arc<SessionContext>,
        group_size: usize,
    ) -> (tempfile::NamedTempFile, Arc<DataSourceExec>) {
        let key = cast(&Int32Array::from(values), key_type).unwrap();
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Int32, false),
            Field::new("key", key_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..key.len() as i32)) as ArrayRef,
                key,
            ],
        )
        .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_size))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ArrowWriter::try_new(
            file.reopen().unwrap(),
            Arc::clone(&file_schema),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();
        assert_eq!(
            metadata.num_row_groups(),
            batch.num_rows().div_ceil(group_size)
        );
        let scan = parquet_scan(&[&file], file_schema, vec![1], None, false, session);
        (file, scan)
    }

    fn parquet_scan(
        files: &[&tempfile::NamedTempFile],
        file_schema: Arc<Schema>,
        projection: Vec<usize>,
        data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
        allow_type_promotion: bool,
        session: &Arc<SessionContext>,
    ) -> Arc<DataSourceExec> {
        init_datasource_exec(
            Arc::new(file_schema.project(&projection).unwrap()),
            Some(file_schema),
            None,
            ObjectStoreUrl::local_filesystem(),
            ObjectStoreBackend::Local,
            vec![files
                .iter()
                .map(|file| {
                    PartitionedFile::from_path(file.path().to_string_lossy().into_owned()).unwrap()
                })
                .collect()],
            Some(projection),
            data_filters,
            None,
            "UTC",
            true,
            false,
            allow_type_promotion,
            false,
            session,
            false,
            false,
            false,
            false,
            "CORRECTED",
            "CORRECTED",
        )
        .unwrap()
    }
}
