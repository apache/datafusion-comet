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

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream,
};

use futures::StreamExt;

use super::stream::{Driver, JoinSpec};

/// Settings of [`SpillingHashJoinExec`].
#[derive(Debug, Clone, Default)]
pub struct SpillingHashJoinConfig {
    /// Spill once the build side holds this many bytes in memory, even if the memory pool would
    /// grant more. None spills only when the pool refuses. Tests set it low to run every join
    /// through the spilling path.
    pub max_build_buffer_bytes: Option<usize>,
}

/// A shuffled hash join that spills to disk instead of failing when its build side does not fit
/// in memory. See the [module documentation](super).
///
/// It displays as the `HashJoinExec` it runs, so that native plans read the same whether or not
/// a join spills. Its metrics show whether it did.
#[derive(Debug)]
pub struct SpillingHashJoinExec {
    spec: Arc<JoinSpec>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SpillingHashJoinExec {
    /// Runs `join` with spilling, or returns None when `join` is not a join this operator runs:
    /// it must be a partitioned join with no embedded projection, limit or dynamic filter, and
    /// must not be null-aware, whose result depends on every build row at once.
    pub fn try_new(join: &HashJoinExec, config: SpillingHashJoinConfig) -> Result<Option<Self>> {
        if join.mode != PartitionMode::Partitioned
            || join.null_aware
            || join.projection.is_some()
            || join.fetch().is_some()
            || !join.dynamic_expressions_produced().is_empty()
        {
            return Ok(None);
        }
        let template = join.builder().reset_state().build()?;
        // Rows of spilled buckets are emitted after the others, so no input order survives, and
        // the output is not declared ordered.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(template.schema()),
            template.properties().output_partitioning().clone(),
            EmissionType::Both,
            Boundedness::Bounded,
        ));
        Ok(Some(Self {
            spec: Arc::new(JoinSpec { template, config }),
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn with_template(&self, template: HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
        match Self::try_new(&template, self.spec.config.clone())? {
            Some(join) => Ok(Arc::new(join)),
            None => Ok(Arc::new(template)),
        }
    }
}

impl DisplayAs for SpillingHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.spec.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for SpillingHashJoinExec {
    fn name(&self) -> &str {
        "HashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.spec.template.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.spec.template.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.spec.template.apply_expressions(f)
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
        let template = self
            .spec
            .template
            .builder()
            .reset_state()
            .with_new_children(children)?
            .build()?;
        self.with_template(template)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let template = self.spec.template.builder().reset_state().build()?;
        self.with_template(template)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let build = self
            .spec
            .template
            .left()
            .execute(partition, Arc::clone(&context))?;
        let probe = self
            .spec
            .template
            .right()
            .execute(partition, Arc::clone(&context))?;
        // The operator counts its own input and output, as a plain `HashJoinExec` would. The inner
        // joins see only some of the rows, some of them more than once, and the chunked
        // fallback emits rows that no inner join emits.
        let metrics = MetricBuilder::new(&self.metrics);
        let build = counted(
            build,
            metrics.clone().counter("build_input_rows", partition),
            metrics.clone().counter("build_input_batches", partition),
        );
        let probe = counted(
            probe,
            metrics.clone().counter("input_rows", partition),
            metrics.clone().counter("input_batches", partition),
        );
        let output_rows = metrics.clone().output_rows(partition);
        let output_batches = metrics.output_batches(partition);
        let driver = Driver::try_new(
            Arc::clone(&self.spec),
            partition,
            context,
            self.metrics.clone(),
            build,
            probe,
        )?;
        let stream = futures::stream::try_unfold(driver, move |mut driver| {
            let (output_rows, output_batches) = (output_rows.clone(), output_batches.clone());
            async move {
                let batch = driver.next().await?;
                Ok(batch.map(|batch| {
                    output_rows.add(batch.num_rows());
                    output_batches.add(1);
                    (batch, driver)
                }))
            }
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

/// Counts the rows and batches of `input`.
fn counted(
    input: SendableRecordBatchStream,
    rows: Count,
    batches: Count,
) -> SendableRecordBatchStream {
    let schema = input.schema();
    let stream = input.inspect(move |batch| {
        if let Ok(batch) = batch {
            rows.add(batch.num_rows());
            batches.add(1);
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
