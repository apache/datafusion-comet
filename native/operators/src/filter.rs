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

//! A DataFusion filter backed by the shared [MetricOwningExec] adapter.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::metric_owning::{MetricOwningExec, UnaryExec};

pub type CometFilterExec = MetricOwningExec<FilterExec>;

/// Comet settings that a filter keeps through every rebuild.
#[derive(Clone, Debug, Default)]
pub struct FilterSettings {
    allow_runtime_filter_pushdown: bool,
}

impl UnaryExec for FilterExec {
    type Settings = FilterSettings;

    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        FilterExec::input(self)
    }
}

impl CometFilterExec {
    pub fn with_runtime_filter_pushdown(mut self, allowed: bool) -> Self {
        self.settings_mut().allow_runtime_filter_pushdown = allowed;
        self
    }

    pub fn allows_runtime_filter_pushdown(&self) -> bool {
        self.settings().allow_runtime_filter_pushdown
    }

    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        self.inner().predicate()
    }

    pub fn has_projection(&self) -> bool {
        self.inner().projection().is_some()
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
    use datafusion::physical_plan::execution_plan::{
        ChildrenPropertiesMode, ReplaceChildrenOptions,
    };
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};

    fn filter() -> Result<FilterExec> {
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
        let predicate = binary(col("key", &schema)?, Operator::Gt, lit(5i32), &schema)?;
        FilterExec::try_new(predicate, input)
    }

    #[test]
    fn runtime_filter_permission_survives_rebuilding() -> Result<()> {
        assert!(!CometFilterExec::from_datafusion(filter()?).allows_runtime_filter_pushdown());
        for allowed in [false, true] {
            let filter = Arc::new(
                CometFilterExec::from_datafusion(filter()?).with_runtime_filter_pushdown(allowed),
            );
            for rebuilt in [
                filter.with_execution_input(Arc::clone(filter.input()))?,
                Arc::clone(&filter).replace_children(
                    vec![Arc::clone(filter.input())],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?,
                filter.with_fetch(Some(1)).unwrap(),
            ] {
                let rebuilt = rebuilt.downcast_ref::<CometFilterExec>().unwrap();
                assert_eq!(rebuilt.allows_runtime_filter_pushdown(), allowed);
                assert!(Arc::ptr_eq(rebuilt.predicate(), filter.predicate()));
            }
        }
        Ok(())
    }

    #[test]
    fn expression_visitor_preserves_predicate_root_and_recursion() -> Result<()> {
        let filter = CometFilterExec::from_datafusion(filter()?);
        assert_eq!(filter.predicate().children().len(), 2);

        for recursion in [TreeNodeRecursion::Continue, TreeNodeRecursion::Stop] {
            let mut visits = 0;
            let result = filter.apply_expressions(&mut |expression| {
                visits += 1;
                assert!(Arc::ptr_eq(expression, filter.predicate()));
                Ok(recursion)
            })?;
            assert_eq!(visits, 1);
            assert_eq!(result, recursion);
        }
        Ok(())
    }

    #[test]
    fn statistics_match_datafusion_for_each_partition() -> Result<()> {
        let filter = filter()?;
        let wrapper = CometFilterExec::from_datafusion(filter.clone());
        let context = StatisticsContext::new();
        let mut row_counts = Vec::new();

        for partition in [None, Some(0), Some(1)] {
            let args = StatisticsArgs::new().with_partition(partition);
            let expected = context.compute(&filter, &args)?;
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
