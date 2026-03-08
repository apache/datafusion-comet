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

//! Execution plan for semi/anti sort-merge joins.
//!
//! Ported from Apache DataFusion.

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use super::stream::SemiAntiSortMergeJoinStream;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::joins::utils::{
    build_join_schema, check_join_is_valid, JoinFilter, JoinOn, JoinOnRef,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion::common::{
    assert_eq_or_internal_err, plan_err, JoinSide, JoinType, NullEquality, Result,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::equivalence::join_equivalence_properties;
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};

/// Sort-merge join operator specialized for semi/anti joins.
///
/// # Motivation
///
/// The general-purpose `SortMergeJoinExec` handles semi/anti joins by
/// materializing `(outer, inner)` row pairs, applying a filter, then using a
/// "corrected filter mask" to deduplicate. Semi/anti joins only need a boolean
/// per outer row (does a match exist?), not pairs. The pair-based approach
/// incurs unnecessary memory allocation and intermediate batches.
///
/// This operator instead tracks matches with a per-outer-batch bitset,
/// avoiding all pair materialization.
///
/// Supports: `LeftSemi`, `LeftAnti`, `RightSemi`, `RightAnti`.
///
/// # Algorithm
///
/// Both inputs must be sorted by the join keys. The stream performs a merge
/// scan across the two sorted inputs. At each step:
///
/// - **outer < inner**: Skip the outer key group (no match exists).
/// - **outer > inner**: Skip the inner key group.
/// - **outer == inner**: Process the match.
///
/// **Without filter**: All outer rows in the key group are marked as matched.
///
/// **With filter**: The inner key group is buffered. For each buffered inner
/// row, the filter is evaluated against the outer key group as a batch.
/// Results are OR'd into the matched bitset. Short-circuits when all outer
/// rows in the group are matched.
///
/// On emit:
///   Semi  -> filter_record_batch(outer_batch, &matched)
///   Anti  -> filter_record_batch(outer_batch, &NOT(matched))
#[derive(Debug, Clone)]
pub struct SemiAntiSortMergeJoinExec {
    pub left: Arc<dyn ExecutionPlan>,
    pub right: Arc<dyn ExecutionPlan>,
    pub on: JoinOn,
    pub filter: Option<JoinFilter>,
    pub join_type: JoinType,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    left_sort_exprs: LexOrdering,
    right_sort_exprs: LexOrdering,
    pub sort_options: Vec<SortOptions>,
    pub null_equality: NullEquality,
    cache: PlanProperties,
}

impl SemiAntiSortMergeJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<Self> {
        if !matches!(
            join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
        ) {
            return plan_err!(
                "SemiAntiSortMergeJoinExec only supports semi/anti joins, got {:?}",
                join_type
            );
        }

        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        if sort_options.len() != on.len() {
            return plan_err!(
                "Expected number of sort options: {}, actual: {}",
                on.len(),
                sort_options.len()
            );
        }

        let (left_sort_exprs, right_sort_exprs): (Vec<_>, Vec<_>) = on
            .iter()
            .zip(sort_options.iter())
            .map(|((l, r), sort_op)| {
                let left = PhysicalSortExpr {
                    expr: Arc::clone(l),
                    options: *sort_op,
                };
                let right = PhysicalSortExpr {
                    expr: Arc::clone(r),
                    options: *sort_op,
                };
                (left, right)
            })
            .unzip();

        let Some(left_sort_exprs) = LexOrdering::new(left_sort_exprs) else {
            return plan_err!(
                "SemiAntiSortMergeJoinExec requires valid sort expressions for its left side"
            );
        };
        let Some(right_sort_exprs) = LexOrdering::new(right_sort_exprs) else {
            return plan_err!(
                "SemiAntiSortMergeJoinExec requires valid sort expressions for its right side"
            );
        };

        let schema = Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);
        let cache = Self::compute_properties(&left, &right, Arc::clone(&schema), join_type, &on)?;

        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            left_sort_exprs,
            right_sort_exprs,
            sort_options,
            null_equality,
            cache,
        })
    }

    /// The outer (probe) side: Left for LeftSemi/LeftAnti, Right for RightSemi/RightAnti.
    pub fn probe_side(join_type: &JoinType) -> JoinSide {
        match join_type {
            JoinType::RightSemi | JoinType::RightAnti => JoinSide::Right,
            _ => JoinSide::Left,
        }
    }

    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => vec![true, false],
            JoinType::RightSemi | JoinType::RightAnti => vec![false, true],
            _ => vec![false, false],
        }
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        join_on: JoinOnRef,
    ) -> Result<PlanProperties> {
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side(&join_type)),
            join_on,
        )?;
        let output_partitioning = symmetric_join_output_partitioning(left, right, &join_type)?;
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([left, right]),
        ))
    }
}

/// Inlined from `datafusion_physical_plan::execution_plan::boundedness_from_children`
/// which is `pub(crate)` in DF 52.2.0.
fn boundedness_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
) -> Boundedness {
    let mut unbounded_with_finite_mem = false;
    for child in children {
        match child.boundedness() {
            Boundedness::Unbounded {
                requires_infinite_memory: true,
            } => {
                return Boundedness::Unbounded {
                    requires_infinite_memory: true,
                };
            }
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            } => {
                unbounded_with_finite_mem = true;
            }
            Boundedness::Bounded => {}
        }
    }
    if unbounded_with_finite_mem {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    } else {
        Boundedness::Bounded
    }
}

/// Inlined from `datafusion_physical_plan::joins::utils::symmetric_join_output_partitioning`
/// which is `pub(crate)` in DF 52.2.0.
fn symmetric_join_output_partitioning(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
) -> Result<Partitioning> {
    let left_partitioning = left.output_partitioning();
    let right_partitioning = right.output_partitioning();
    let result = match join_type {
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            left_partitioning.clone()
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            right_partitioning.clone()
        }
        _ => Partitioning::UnknownPartitioning(right_partitioning.partition_count()),
    };
    Ok(result)
}

impl DisplayAs for SemiAntiSortMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({c1}, {c2})"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "{}: join_type={:?}, on=[{}]{}",
                    Self::static_name(),
                    self.join_type,
                    on,
                    self.filter.as_ref().map_or_else(
                        || "".to_string(),
                        |filt| format!(", filter={}", filt.expression())
                    ),
                )
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| {
                        format!("({} = {})", fmt_sql(c1.as_ref()), fmt_sql(c2.as_ref()))
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                writeln!(f, "join_type={:?}", self.join_type)?;
                writeln!(f, "on={on}")?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for SemiAntiSortMergeJoinExec {
    fn name(&self) -> &'static str {
        "SemiAntiSortMergeJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();
        vec![
            Distribution::HashPartitioned(left_expr),
            Distribution::HashPartitioned(right_expr),
        ]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            Some(OrderingRequirements::from(self.left_sort_exprs.clone())),
            Some(OrderingRequirements::from(self.right_sort_exprs.clone())),
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(Self::try_new(
                Arc::clone(left),
                Arc::clone(right),
                self.on.clone(),
                self.filter.clone(),
                self.join_type,
                self.sort_options.clone(),
                self.null_equality,
            )?)),
            _ => datafusion::common::internal_err!(
                "SemiAntiSortMergeJoinExec wrong number of children"
            ),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        assert_eq_or_internal_err!(
            left_partitions,
            right_partitions,
            "Invalid SemiAntiSortMergeJoinExec, partition count mismatch \
             {left_partitions}!={right_partitions}"
        );

        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();

        let (outer, inner, on_outer, on_inner) =
            if Self::probe_side(&self.join_type) == JoinSide::Left {
                (
                    Arc::clone(&self.left),
                    Arc::clone(&self.right),
                    on_left,
                    on_right,
                )
            } else {
                (
                    Arc::clone(&self.right),
                    Arc::clone(&self.left),
                    on_right,
                    on_left,
                )
            };

        let outer = outer.execute(partition, Arc::clone(&context))?;
        let inner = inner.execute(partition, Arc::clone(&context))?;
        let batch_size = context.session_config().batch_size();

        Ok(Box::pin(SemiAntiSortMergeJoinStream::try_new(
            Arc::clone(&self.schema),
            self.sort_options.clone(),
            self.null_equality,
            outer,
            inner,
            on_outer,
            on_inner,
            self.filter.clone(),
            self.join_type,
            batch_size,
            partition,
            &self.metrics,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}
