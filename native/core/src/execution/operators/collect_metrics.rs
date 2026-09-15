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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

#[derive(Debug)]
pub struct CollectMetricsExec {
    metric_name: String,
    agg_exprs: Vec<AggregateFunctionExpr>,
    metric_exprs: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CollectMetricsExec {
    pub fn new(
        metric_name: String,
        agg_exprs: Vec<AggregateFunctionExpr>,
        metric_exprs: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            metric_name,
            agg_exprs,
            metric_exprs,
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn metric_name(&self) -> &str {
        &self.metric_name
    }

    pub fn agg_exprs(&self) -> &[AggregateFunctionExpr] {
        &self.agg_exprs
    }

    pub fn metric_exprs(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.metric_exprs
    }
}

impl DisplayAs for CollectMetricsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CometCollectMetricsExec: metric_name={}, agg_exprs={:?}, metric_exprs={:?}",
            self.metric_name, self.agg_exprs, self.metric_exprs
        )
    }
}

impl ExecutionPlan for CollectMetricsExec {
    fn name(&self) -> &str {
        "CometCollectMetricsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "CometCollectMetricsExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(CollectMetricsExec::new(
            self.metric_name.clone(),
            self.agg_exprs.clone(),
            self.metric_exprs.clone(),
            children[0].clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        let accumulators: Vec<Box<dyn Accumulator + Send>> = self
            .agg_exprs
            .iter()
            .map(|agg| {
                agg.create_accumulator()
                    .map(|acc| acc as Box<dyn Accumulator + Send>)
            })
            .collect::<Result<Vec<_>>>()?;

        let stream: CollectMetricsStream = CollectMetricsStream::new(
            self.metric_name.clone(),
            self.agg_exprs.clone(),
            accumulators,
            self.metric_exprs.clone(),
            input_stream,
        );

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct CollectMetricsStream {
    metric_name: String,
    agg_exprs: Vec<AggregateFunctionExpr>,
    accumulators: Vec<Box<dyn Accumulator + Send>>,
    metric_exprs: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    is_finished: bool,
}

impl CollectMetricsStream {
    fn new(
        metric_name: String,
        agg_exprs: Vec<AggregateFunctionExpr>,
        accumulators: Vec<Box<dyn Accumulator + Send>>,
        metric_exprs: Vec<Arc<dyn PhysicalExpr>>,
        input: SendableRecordBatchStream,
    ) -> Self {
        let schema = input.schema();
        Self {
            metric_name,
            agg_exprs,
            accumulators,
            metric_exprs,
            input,
            schema,
            is_finished: false,
        }
    }

    fn process_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for (agg_expr, acc) in self.agg_exprs.iter().zip(self.accumulators.iter_mut()) {
            let args = agg_expr
                .expressions()
                .iter()
                .map(|e| e.evaluate(batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            acc.update_batch(&args)?;
        }
        Ok(())
    }

    fn finalize_metrics(&mut self) -> Result<RecordBatch> {
        let mut scalar_values = Vec::with_capacity(self.accumulators.len());
        for acc in &mut self.accumulators {
            scalar_values.push(acc.evaluate()?);
        }

        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(scalar_values.len());
        let mut agg_fields: Vec<Field> = Vec::with_capacity(scalar_values.len());

        for (i, val) in scalar_values.into_iter().enumerate() {
            let array = val.to_array()?;
            let field = Field::new(format!("agg_{i}"), array.data_type().clone(), true);
            agg_arrays.push(array);
            agg_fields.push(field);
        }

        let agg_schema = Arc::new(Schema::new(agg_fields));
        let agg_batch = RecordBatch::try_new(agg_schema, agg_arrays)?;

        let mut final_arrays: Vec<ArrayRef> = Vec::with_capacity(self.metric_exprs.len());
        let mut final_fields: Vec<Field> = Vec::with_capacity(self.metric_exprs.len());

        for (i, expr) in self.metric_exprs.iter().enumerate() {
            let col = expr.evaluate(&agg_batch)?.into_array(1)?;
            let field = Field::new(format!("metric_{i}"), col.data_type().clone(), true);
            final_arrays.push(col);
            final_fields.push(field);
        }

        let final_schema = Arc::new(Schema::new(final_fields));
        RecordBatch::try_new(final_schema, final_arrays).map_err(DataFusionError::from)
    }
}

impl Stream for CollectMetricsStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx);
        match ready!(poll) {
            Some(Ok(batch)) => {
                if let Err(e) = self.process_batch(&batch) {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => {
                if !self.is_finished {
                    self.is_finished = true;
                    if let Err(e) = self.finalize_metrics() {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for CollectMetricsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
