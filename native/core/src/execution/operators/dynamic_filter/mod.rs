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

//! Runtime-filter wiring and shared filtering of decoded batches.

mod join;
mod parquet_reader;

pub(crate) use join::DynamicFilterJoinExec;

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    apply_expression_roots, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

/// A task-local consumer of a live runtime predicate.
#[derive(Debug)]
pub(crate) struct DynamicFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    metrics: ExecutionPlanMetricsSet,
    metric_prefix: &'static str,
}

impl DynamicFilterExec {
    pub(super) fn new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<DynamicFilterPhysicalExpr>,
        metrics: ExecutionPlanMetricsSet,
        metric_prefix: &'static str,
    ) -> Self {
        Self {
            input,
            predicate,
            metrics,
            metric_prefix,
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
            ExecutionPlanMetricsSet::new(),
            self.metric_prefix,
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
        Ok(Arc::new(Self::new(
            Arc::clone(&self.input),
            predicate,
            ExecutionPlanMetricsSet::new(),
            self.metric_prefix,
        )))
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
        let evaluated = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_rows_evaluated", self.metric_prefix), partition);
        let pruned = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_rows_pruned", self.metric_prefix), partition);
        let bypassed = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_rows_bypassed", self.metric_prefix), partition);
        // Only dedicated metrics: merging this helper into the Spark join must not
        // add its input/output counts or elapsed time to the join's existing metrics.
        let eval_time = MetricBuilder::new(&self.metrics)
            .subset_time(format!("{}_eval_time", self.metric_prefix), partition);
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

#[cfg(test)]
mod tests;
