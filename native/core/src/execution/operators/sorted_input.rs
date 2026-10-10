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
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

/// Passes `input` through unchanged and declares to DataFusion the order its rows arrive in.
///
/// Spark's plan knows the order of an input that enters the native plan from the JVM, a relation
/// sorted before it was cached for instance, but the `ScanExec` it arrives through declares none,
/// and DataFusion's hash join keeps the unmatched probe rows of a right outer join in place only
/// when the probe input declares an order. Otherwise it moves them after the matched rows of each
/// batch, which loses an order Spark has already dropped a sort for. The planner puts this below
/// that join's probe side with the order Spark reports for it, and nowhere else: an order that
/// the whole native plan could see would also change how DataFusion's aggregates group.
#[derive(Debug)]
pub struct SortedInputExec {
    input: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    cache: Arc<PlanProperties>,
}

impl SortedInputExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, ordering: LexOrdering) -> Self {
        let input_properties = input.properties();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(input.schema(), [ordering.clone()]),
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
