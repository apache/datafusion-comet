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

//! An input that declares the order its rows arrive in.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};

/// Passes `input` through unchanged and declares to DataFusion the order its rows arrive in.
///
/// Spark's plan knows the order of an input that enters the native plan from the JVM, a relation
/// sorted before it was cached for instance, but the `ScanExec` it arrives through declares none,
/// and DataFusion's hash join keeps the unmatched probe rows of a right outer join in place only
/// when the probe input declares an order. Otherwise it moves them after the matched rows of each
/// batch, which loses an order Spark has already dropped a sort for. The planner puts this only
/// on that join's probe input, with the order Spark reports for it, and only when the rows reach
/// that input from the JVM through operators that keep their order. The join keeps the order in
/// its own output, so the operators above it in the native plan see it too: an aggregate grouped
/// by the key runs in sorted, streaming mode, a sort aggregate takes the order as already
/// satisfied, and a top-k sort whose keys start with it stops reading early. A wrong order would
/// therefore give wrong groups or wrong rows, not only rows out of order.
#[derive(Debug)]
pub struct SortedInputExec {
    input: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    cache: Arc<PlanProperties>,
}

impl SortedInputExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, ordering: LexOrdering) -> Self {
        let input_properties = input.properties();
        // Start from the input's own properties, so the constants and equivalence classes it
        // knows of stay visible to the operators above.
        let mut equivalence_properties = input.equivalence_properties().clone();
        equivalence_properties.add_ordering(ordering.iter().cloned());
        let cache = Arc::new(PlanProperties::new(
            equivalence_properties,
            input_properties.output_partitioning().clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Self {
            input,
            ordering,
            cache,
        }
    }
}

impl DisplayAs for SortedInputExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SortedInputExec: ordering=[{}]", self.ordering)
    }
}

impl ExecutionPlan for SortedInputExec {
    fn name(&self) -> &str {
        "SortedInputExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.ordering.iter().map(|sort| Arc::clone(&sort.expr)), f)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// The rows pass through, so the input's metrics are this operator's, and a range whose root
    /// this wraps still reports its output rows.
    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("SortedInputExec requires one child");
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.ordering.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
