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

//! Lazy native range for the initial Spark bridge. No JVM input iterator is used.

use std::fmt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    expressions::Column, EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};

/// These admission limits bound task and batch overhead in the experimental bridge.
pub const MAX_PARTITIONS: usize = 1024;
pub const MAX_BATCH_SIZE: usize = 65536;
pub const MAX_COLUMNS: usize = 1024;

#[derive(Debug)]
struct RangeExec {
    start: i128,
    step: i128,
    count: i128,
    partitions: usize,
    batch_size: usize,
    properties: Arc<PlanProperties>,
}

pub fn range_plan(
    start: i64,
    end: i64,
    step: i64,
    partitions: usize,
    batch_size: usize,
    columns: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if step == 0
        || !(1..=MAX_PARTITIONS).contains(&partitions)
        || !(1..=MAX_BATCH_SIZE).contains(&batch_size)
        || !(1..=MAX_COLUMNS).contains(&columns)
    {
        return Err(DataFusionError::Plan(
            "Invalid local range parameters".into(),
        ));
    }
    let (start, end, step) = (i128::from(start), i128::from(end), i128::from(step));
    let distance = if step > 0 { end - start } else { start - end };
    let count = if distance <= 0 {
        0
    } else {
        (distance - 1) / step.abs() + 1
    };
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let options = SortOptions {
        descending: step < 0,
        nulls_first: step > 0,
    };
    let source_order = PhysicalSortExpr {
        expr: Arc::new(Column::new("id", 0)),
        options,
    };
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new_with_orderings(schema, [vec![source_order]]),
        Partitioning::UnknownPartitioning(partitions),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ));
    let input = Arc::new(RangeExec {
        start,
        step,
        count,
        partitions,
        batch_size,
        properties,
    });
    let expressions: Vec<_> = (0..columns)
        .map(|i| {
            (
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
                format!("column_{i}"),
            )
        })
        .collect();
    let projection = Arc::new(ProjectionExec::try_new(expressions, input)?);
    // Spark may already have eliminated a redundant sort using Range's ordering.
    // Preserve it across partitions instead of exposing an unordered coalescer.
    let ordering = LexOrdering::new([PhysicalSortExpr {
        expr: Arc::new(Column::new("column_0", 0)),
        options,
    }])
    .ok_or_else(|| DataFusionError::Plan("Missing range ordering".into()))?;
    Ok(Arc::new(SortPreservingMergeExec::new(ordering, projection)))
}

impl DisplayAs for RangeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LocalRangeExec")
    }
}

impl ExecutionPlan for RangeExec {
    fn name(&self) -> &'static str {
        "LocalRangeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("Range has no children".into()));
        }
        Ok(self)
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
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition >= self.partitions {
            return Err(DataFusionError::Execution(
                "Invalid local range partition".into(),
            ));
        }
        // i128 handles Long.MinValue, Long.MaxValue and negative steps without overflow.
        let begin = self.count * partition as i128 / self.partitions as i128;
        let end = self.count * (partition + 1) as i128 / self.partitions as i128;
        let (start, step, batch_size) = (self.start, self.step, self.batch_size as i128);
        let schema = self.schema();
        let stream_schema = Arc::clone(&schema);
        let stream = futures::stream::unfold(begin, move |offset| {
            let schema = Arc::clone(&stream_schema);
            async move {
                if offset >= end {
                    return None;
                }
                let next = (offset + batch_size).min(end);
                let array =
                    Int64Array::from_iter_values((offset..next).map(|i| (start + step * i) as i64));
                let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).map_err(Into::into);
                Some((batch, next))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
