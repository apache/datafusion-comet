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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use datafusion::physical_plan::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::array::{
    make_array, Array, ArrayRef, BooleanArray, MutableArrayData, RecordBatchOptions,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::stats::Precision;
use datafusion::common::{
    internal_err, plan_err, project_schema, DataFusionError, Result, ScalarValue,
};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::intervals::utils::check_support;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{
    analyze, split_conjunction, AcrossPartitions, AnalysisContext, ConstExpr, ExprBoundaries,
    PhysicalExpr,
};
use datafusion::physical_plan::common::can_project;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use futures::stream::{Stream, StreamExt};
use log::trace;

/// This is a copy of DataFusion's FilterExec with one modification to ensure that input
/// batches are never passed through unchanged. The changes are between the comments
/// `BEGIN Comet change` and `END Comet change`.
#[derive(Debug, Clone)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Selectivity for statistics. 0 = no rows, 100 = all rows
    default_selectivity: u8,
    /// Properties equivalence properties, partitioning, etc.
    cache: PlanProperties,
    /// The projection indices of the columns in the output schema of join
    projection: Option<Vec<usize>>,
}

impl FilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => {
                let default_selectivity = 20;
                let cache =
                    Self::compute_properties(&input, &predicate, default_selectivity, None)?;
                Ok(Self {
                    predicate,
                    input: Arc::clone(&input),
                    metrics: ExecutionPlanMetricsSet::new(),
                    default_selectivity,
                    cache,
                    projection: None,
                })
            }
            other => {
                plan_err!("Filter predicate must return BOOLEAN values, got {other:?}")
            }
        }
    }

    pub fn with_default_selectivity(
        mut self,
        default_selectivity: u8,
    ) -> Result<Self, DataFusionError> {
        if default_selectivity > 100 {
            return plan_err!(
                "Default filter selectivity value needs to be less than or equal to 100"
            );
        }
        self.default_selectivity = default_selectivity;
        Ok(self)
    }

    /// Return new instance of [FilterExec] with the given projection.
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        //  Check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;

        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };

        let cache = Self::compute_properties(
            &self.input,
            &self.predicate,
            self.default_selectivity,
            projection.as_ref(),
        )?;
        Ok(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache,
            projection,
        })
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The default selectivity
    pub fn default_selectivity(&self) -> u8 {
        self.default_selectivity
    }

    /// Projection
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Calculates `Statistics` for `FilterExec`, by applying selectivity (either default, or estimated) to input statistics.
    fn statistics_helper(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
    ) -> Result<Statistics> {
        let input_stats = input.statistics()?;
        let schema = input.schema();
        if !check_support(predicate, &schema) {
            let selectivity = default_selectivity as f64 / 100.0;
            let mut stats = input_stats.to_inexact();
            stats.num_rows = stats.num_rows.with_estimated_selectivity(selectivity);
            stats.total_byte_size = stats
                .total_byte_size
                .with_estimated_selectivity(selectivity);
            return Ok(stats);
        }

        let num_rows = input_stats.num_rows;
        let total_byte_size = input_stats.total_byte_size;
        let input_analysis_ctx =
            AnalysisContext::try_from_statistics(&input.schema(), &input_stats.column_statistics)?;

        let analysis_ctx = analyze(predicate, input_analysis_ctx, &schema)?;

        // Estimate (inexact) selectivity of predicate
        let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
        let num_rows = num_rows.with_estimated_selectivity(selectivity);
        let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

        let column_statistics =
            collect_new_statistics(&input_stats.column_statistics, analysis_ctx.boundaries);
        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    fn extend_constants(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
    ) -> Vec<ConstExpr> {
        let mut res_constants = Vec::new();
        let input_eqs = input.equivalence_properties();

        let conjunctions = split_conjunction(predicate);
        for conjunction in conjunctions {
            if let Some(binary) = conjunction.as_any().downcast_ref::<BinaryExpr>() {
                if binary.op() == &Operator::Eq {
                    // Filter evaluates to single value for all partitions
                    if input_eqs.is_expr_constant(binary.left()) {
                        let (expr, across_parts) = (
                            binary.right(),
                            input_eqs.get_expr_constant_value(binary.right()),
                        );
                        res_constants.push(
                            ConstExpr::new(Arc::clone(expr)).with_across_partitions(across_parts),
                        );
                    } else if input_eqs.is_expr_constant(binary.right()) {
                        let (expr, across_parts) = (
                            binary.left(),
                            input_eqs.get_expr_constant_value(binary.left()),
                        );
                        res_constants.push(
                            ConstExpr::new(Arc::clone(expr)).with_across_partitions(across_parts),
                        );
                    }
                }
            }
        }
        res_constants
    }
    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Combine the equal predicates with the input equivalence properties
        // to construct the equivalence properties:
        let stats = Self::statistics_helper(input, predicate, default_selectivity)?;
        let mut eq_properties = input.equivalence_properties().clone();
        let (equal_pairs, _) = collect_columns_from_predicate(predicate);
        for (lhs, rhs) in equal_pairs {
            eq_properties.add_equal_conditions(lhs, rhs)?
        }
        // Add the columns that have only one viable value (singleton) after
        // filtering to constants.
        let constants = collect_columns(predicate)
            .into_iter()
            .filter(|column| stats.column_statistics[column.index()].is_singleton())
            .map(|column| {
                let value = stats.column_statistics[column.index()]
                    .min_value
                    .get_value();
                let expr = Arc::new(column) as _;
                ConstExpr::new(expr)
                    .with_across_partitions(AcrossPartitions::Uniform(value.cloned()))
            });
        // This is for statistics
        eq_properties = eq_properties.with_constants(constants);
        // This is for logical constant (for example: a = '1', then a could be marked as a constant)
        // to do: how to deal with multiple situation to represent = (for example c1 between 0 and 0)
        eq_properties = eq_properties.with_constants(Self::extend_constants(input, predicate));

        let mut output_partitioning = input.output_partitioning().clone();
        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            let schema = eq_properties.schema();
            let projection_mapping = ProjectionMapping::from_indices(projection, schema)?;
            let out_schema = project_schema(schema, Some(projection))?;
            output_partitioning = output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl DisplayAs for FilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_projections = if let Some(projection) = self.projection.as_ref() {
                    format!(
                        ", projection=[{}]",
                        projection
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.input.schema().fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                write!(
                    f,
                    "CometFilterExec: {}{}",
                    self.predicate, display_projections
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &'static str {
        "CometFilterExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        FilterExec::try_new(Arc::clone(&self.predicate), children.swap_remove(0))
            .and_then(|e| {
                let selectivity = e.default_selectivity();
                e.with_default_selectivity(selectivity)
            })
            .and_then(|e| e.with_projection(self.projection().cloned()))
            .map(|e| Arc::new(e) as _)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(FilterExecStream {
            schema: self.schema(),
            predicate: Arc::clone(&self.predicate),
            input: self.input.execute(partition, context)?,
            baseline_metrics,
            projection: self.projection.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The output statistics of a filtering operation can be estimated if the
    /// predicate's selectivity value can be determined for the incoming data.
    fn statistics(&self) -> Result<Statistics> {
        let stats =
            Self::statistics_helper(&self.input, self.predicate(), self.default_selectivity)?;
        Ok(stats.project(self.projection.as_ref()))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

/// This function ensures that all bounds in the `ExprBoundaries` vector are
/// converted to closed bounds. If a lower/upper bound is initially open, it
/// is adjusted by using the next/previous value for its data type to convert
/// it into a closed bound.
fn collect_new_statistics(
    input_column_stats: &[ColumnStatistics],
    analysis_boundaries: Vec<ExprBoundaries>,
) -> Vec<ColumnStatistics> {
    analysis_boundaries
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                ExprBoundaries {
                    interval,
                    distinct_count,
                    ..
                },
            )| {
                let Some(interval) = interval else {
                    // If the interval is `None`, we can say that there are no rows:
                    return ColumnStatistics {
                        null_count: Precision::Exact(0),
                        max_value: Precision::Exact(ScalarValue::Null),
                        min_value: Precision::Exact(ScalarValue::Null),
                        sum_value: Precision::Exact(ScalarValue::Null),
                        distinct_count: Precision::Exact(0),
                    };
                };
                let (lower, upper) = interval.into_bounds();
                let (min_value, max_value) = if lower.eq(&upper) {
                    (Precision::Exact(lower), Precision::Exact(upper))
                } else {
                    (Precision::Inexact(lower), Precision::Inexact(upper))
                };
                ColumnStatistics {
                    null_count: input_column_stats[idx].null_count.to_inexact(),
                    max_value,
                    min_value,
                    sum_value: Precision::Absent,
                    distinct_count: distinct_count.to_inexact(),
                }
            },
        )
        .collect()
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct FilterExecStream {
    /// Output schema after the projection
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// Runtime metrics recording
    baseline_metrics: BaselineMetrics,
    /// The projection indices of the columns in the input schema
    projection: Option<Vec<usize>>,
}

fn filter_and_project(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projection: Option<&Vec<usize>>,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(match (as_boolean_array(&array), projection) {
                // Apply filter array to record batch
                (Ok(filter_array), None) => comet_filter_record_batch(batch, filter_array)?,
                (Ok(filter_array), Some(projection)) => {
                    let projected_columns = projection
                        .iter()
                        .map(|i| Arc::clone(batch.column(*i)))
                        .collect();
                    let projected_batch =
                        RecordBatch::try_new(Arc::clone(output_schema), projected_columns)?;
                    comet_filter_record_batch(&projected_batch, filter_array)?
                }
                (Err(_), _) => {
                    return internal_err!("Cannot create filter_array from non-boolean predicates");
                }
            })
        })
}

// BEGIN Comet changes
// `FilterExec` could modify input batch or return input batch without change. Instead of always
// adding `CopyExec` on top of it, we only copy input batch for the special case.
pub fn comet_filter_record_batch(
    record_batch: &RecordBatch,
    predicate: &BooleanArray,
) -> std::result::Result<RecordBatch, ArrowError> {
    if predicate.true_count() == record_batch.num_rows() {
        // special case where we just make an exact copy
        let arrays: Vec<ArrayRef> = record_batch
            .columns()
            .iter()
            .map(|array| {
                let capacity = array.len();
                let data = array.to_data();
                let mut mutable = MutableArrayData::new(vec![&data], false, capacity);
                mutable.extend(0, 0, capacity);
                make_array(mutable.freeze())
            })
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(record_batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::clone(&record_batch.schema()), arrays, &options)
    } else {
        filter_record_batch(record_batch, predicate)
    }
}
// END Comet changes

impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = self.baseline_metrics.elapsed_compute().timer();
                    let filtered_batch = filter_and_project(
                        &batch,
                        &self.predicate,
                        self.projection.as_ref(),
                        &self.schema,
                    )?;
                    timer.done();
                    // Skip entirely filtered batches
                    if filtered_batch.num_rows() == 0 {
                        continue;
                    }
                    poll = Poll::Ready(Some(Ok(filtered_batch)));
                    break;
                }
                value => {
                    poll = Poll::Ready(value);
                    break;
                }
            }
        }
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for FilterExecStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Return the equals Column-Pairs and Non-equals Column-Pairs
fn collect_columns_from_predicate(predicate: &Arc<dyn PhysicalExpr>) -> EqualAndNonEqual {
    let mut eq_predicate_columns = Vec::<PhysicalExprPairRef>::new();
    let mut ne_predicate_columns = Vec::<PhysicalExprPairRef>::new();

    let predicates = split_conjunction(predicate);
    predicates.into_iter().for_each(|p| {
        if let Some(binary) = p.as_any().downcast_ref::<BinaryExpr>() {
            match binary.op() {
                Operator::Eq => eq_predicate_columns.push((binary.left(), binary.right())),
                Operator::NotEq => ne_predicate_columns.push((binary.left(), binary.right())),
                _ => {}
            }
        }
    });

    (eq_predicate_columns, ne_predicate_columns)
}

/// Pair of `Arc<dyn PhysicalExpr>`s
pub type PhysicalExprPairRef<'a> = (&'a Arc<dyn PhysicalExpr>, &'a Arc<dyn PhysicalExpr>);

/// The equals Column-Pairs and Non-equals Column-Pairs in the Predicates
pub type EqualAndNonEqual<'a> = (Vec<PhysicalExprPairRef<'a>>, Vec<PhysicalExprPairRef<'a>>);
