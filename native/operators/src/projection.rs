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

//! A DataFusion projection backed by the shared [MetricOwningExec] adapter.

use std::sync::Arc;

use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::metric_owning::{MetricOwningExec, UnaryExec};

pub type CometProjectionExec = MetricOwningExec<ProjectionExec>;

impl UnaryExec for ProjectionExec {
    type Settings = ();

    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        ProjectionExec::input(self)
    }
}

impl CometProjectionExec {
    pub fn projection(&self) -> &ProjectionExec {
        self.inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::stats::Precision;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::common::Result;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};

    fn projection() -> Result<ProjectionExec> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let partitions = [1..11, 11..41]
            .into_iter()
            .map(|values| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from_iter_values(values))],
                )
                .map(|batch| vec![batch])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let input = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        ProjectionExec::try_new(
            vec![
                (
                    binary(col("key", &schema)?, Operator::Plus, lit(5i32), &schema)?,
                    "offset".into(),
                ),
                (col("key", &schema)?, "key".into()),
            ],
            input,
        )
    }

    #[test]
    fn expression_visitor_preserves_roots_and_recursion() -> Result<()> {
        let wrapper = CometProjectionExec::from_datafusion(projection()?);
        assert_eq!(wrapper.projection().expr()[0].expr.children().len(), 2);
        for recursion in [TreeNodeRecursion::Continue, TreeNodeRecursion::Stop] {
            let mut visits = 0;
            let result = wrapper.apply_expressions(&mut |expression| {
                assert!(Arc::ptr_eq(
                    expression,
                    &wrapper.projection().expr()[visits].expr
                ));
                visits += 1;
                Ok(recursion)
            })?;
            assert_eq!(
                visits,
                if recursion == TreeNodeRecursion::Stop {
                    1
                } else {
                    2
                }
            );
            assert_eq!(result, recursion);
        }
        Ok(())
    }

    #[test]
    fn statistics_match_datafusion_for_each_partition() -> Result<()> {
        let projection = projection()?;
        let wrapper = CometProjectionExec::from_datafusion(projection.clone());
        let context = StatisticsContext::new();
        let mut row_counts = Vec::new();
        for partition in [None, Some(0), Some(1)] {
            let args = StatisticsArgs::new().with_partition(partition);
            let expected = context.compute(&projection, &args)?;
            let actual = context.compute(&wrapper, &args)?;
            assert_eq!(actual, expected);
            assert_ne!(actual.num_rows, Precision::Absent);
            row_counts.push(actual.num_rows);
        }
        assert_ne!(row_counts[0], row_counts[1]);
        assert_ne!(row_counts[1], row_counts[2]);
        Ok(())
    }
}
