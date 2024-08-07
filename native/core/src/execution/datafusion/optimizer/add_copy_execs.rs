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

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use std::sync::Arc;

use crate::execution::datafusion::operators::expand::CometExpandExec;
use crate::execution::operators::{CopyExec, ScanExec};

/// Physical optimize rule to wrap operators in a CopyExec if the operators re-use input batches
pub struct AddCopyExecs {}

impl Default for AddCopyExecs {
    fn default() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AddCopyExecs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if can_reuse_input_batch(&plan) {
                Ok(Transformed::yes(Arc::new(CopyExec::new(plan))))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "add_copy_execs"
    }

    fn schema_check(&self) -> bool {
        // CopyExec currently unpacks dictionaries, therefore changes the schema
        false
    }
}

/// Returns true if given operator can cache batches
fn caches_batches(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<CometExpandExec>()
        || plan.as_any().is::<SortExec>()
        || plan.as_any().is::<HashJoinExec>()
        || plan.as_any().is::<SortMergeJoinExec>()
}

/// Returns true if given operator can return input array as output array without
/// modification. This is used to determine if we need to copy the input batch to avoid
/// data corruption from reusing the input batch.
fn can_reuse_input_batch(op: &Arc<dyn ExecutionPlan>) -> bool {
    op.as_any().is::<ScanExec>()
        || op.as_any().is::<LocalLimitExec>()
        || op.as_any().is::<ProjectionExec>()
        || op.as_any().is::<FilterExec>()
}
