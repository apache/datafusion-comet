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
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::utils::{build_join_schema, check_join_is_valid, JoinFilter};
use datafusion::physical_plan::joins::JoinOn;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::common::NullEquality;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::JoinType;

/// A Comet-specific sort merge join operator that replaces DataFusion's
/// `SortMergeJoinExec` with Spark-compatible semantics.
#[derive(Debug)]
pub(crate) struct CometSortMergeJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_on: JoinOn,
    join_filter: Option<JoinFilter>,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CometSortMergeJoinExec {
    /// Create a new `CometSortMergeJoinExec`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: JoinOn,
        join_filter: Option<JoinFilter>,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        check_join_is_valid(&left_schema, &right_schema, &join_on)?;

        let (schema, _column_indices) =
            build_join_schema(&left_schema, &right_schema, &join_type);
        let schema = Arc::new(schema);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(left.properties().output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            left,
            right,
            join_on,
            join_filter,
            join_type,
            sort_options,
            null_equality,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::default(),
        })
    }
}

impl DisplayAs for CometSortMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometSortMergeJoinExec: join_type={:?}", self.join_type)
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for CometSortMergeJoinExec {
    fn name(&self) -> &str {
        "CometSortMergeJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CometSortMergeJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.join_on.clone(),
            self.join_filter.clone(),
            self.join_type,
            self.sort_options.clone(),
            self.null_equality,
        )?))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("SortMergeJoinStream not yet implemented")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
