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

//! An operator that runs its input with a smaller session batch size.

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use std::fmt::Formatter;
use std::sync::Arc;

/// Executes its input, and everything below it, with the session batch size lowered to
/// `batch_size`, and passes its batches through unchanged.
///
/// A join evaluates its filter over the row pairs it matched, up to the session batch size of
/// them at a time, and one input row can match many. A split under the join cannot bound that, so
/// the planner puts this above a join whose filter expands a large string or binary scalar. It
/// owns no metrics: the join's are reported as its own.
#[derive(Debug)]
pub struct BatchSizeScopeExec {
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
}

impl BatchSizeScopeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, batch_size: usize) -> Self {
        Self {
            input,
            batch_size: batch_size.max(1),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl DisplayAs for BatchSizeScopeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometBatchSizeScopeExec: batch_size={}", self.batch_size)
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for BatchSizeScopeExec {
    fn name(&self) -> &str {
        "CometBatchSizeScopeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        Ok(Arc::new(BatchSizeScopeExec::new(
            Arc::clone(&children[0]),
            self.batch_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let config = context
            .session_config()
            .clone()
            .with_batch_size(self.batch_size);
        let scoped = TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        );
        self.input.execute(partition, Arc::new(scoped))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Mutex;

    /// Records the session batch size it is executed with, as a join reads it.
    #[derive(Debug)]
    struct BatchSizeProbe {
        input: Arc<dyn ExecutionPlan>,
        seen: Arc<Mutex<Option<usize>>>,
    }

    impl DisplayAs for BatchSizeProbe {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "BatchSizeProbe")
        }
    }

    impl ExecutionPlan for BatchSizeProbe {
        fn name(&self) -> &str {
            "BatchSizeProbe"
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

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            *self.seen.lock().unwrap() = Some(context.session_config().batch_size());
            self.input.execute(partition, context)
        }
    }

    #[test]
    fn runs_its_input_at_the_scoped_batch_size() {
        let seen = Arc::new(Mutex::new(None));
        let probe = Arc::new(BatchSizeProbe {
            input: Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
            seen: Arc::clone(&seen),
        });
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(16));
        BatchSizeScopeExec::new(probe, 4)
            .execute(0, ctx.task_ctx())
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), Some(4));
        assert_eq!(ctx.task_ctx().session_config().batch_size(), 16);
    }
}
