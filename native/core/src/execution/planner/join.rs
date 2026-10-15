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

//! Builders for the native join operators (hash join, sort-merge join, and broadcast nested loop
//! join), and the join-key and join-filter planning they share.

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    arrow::compute::SortOptions,
    common::{
        tree_node::{
            Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
        },
        DataFusionError, JoinType as DFJoinType, NullEquality,
    },
    config::ConfigOptions,
    physical_expr::{
        expressions::{CastExpr, Column},
        PhysicalExpr,
    },
    physical_plan::{
        filter::FilterExec,
        joins::{
            utils::JoinFilter, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
        },
        projection::ProjectionExec,
        ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
    },
};
use datafusion_comet_operators::{CometFilterExec, CometProjectionExec};
use datafusion_comet_proto::{
    spark_expression::Expr,
    spark_operator::{BuildSide, JoinType, Operator},
};
use itertools::Itertools;
use jni::objects::{Global, JObject};

use super::{operator_registry::OperatorBuilder, PhysicalPlanner, PlanCreationResult};
use crate::{
    execution::{
        operators::{DynamicFilterJoinExec, ExecutionError, ScanExec, ShuffleScanExec},
        spark_plan::SparkPlan,
    },
    extract_op,
};

use ExecutionError::GeneralError;

struct JoinParameters {
    pub left: Arc<SparkPlan>,
    pub right: Arc<SparkPlan>,
    pub join_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    pub join_filter: Option<JoinFilter>,
    pub join_type: DFJoinType,
}

/// Builder for `HashJoin`.
pub struct HashJoinBuilder;

impl OperatorBuilder for HashJoinBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let join = extract_op!(spark_plan, HashJoin);
        let children = &spark_plan.children;

        let (mut join_params, scans, shuffle_scans) = planner.parse_join_parameters(
            inputs,
            children,
            &join.left_join_keys,
            &join.right_join_keys,
            join.join_type,
            &join.condition,
            partition_count,
        )?;

        // Reader attachment and early placement recursively replace probe-side
        // filters and projections. Keep metrics owned by their Spark nodes.
        if join.dynamic_filter_enabled && !join.null_aware_anti_join {
            let probe = if join.build_side == BuildSide::BuildLeft as i32 {
                &mut join_params.right
            } else {
                &mut join_params.left
            };
            *probe = PhysicalPlanner::prepare_probe_for_runtime_filters(Arc::clone(probe))?;
        }

        let left = Arc::clone(&join_params.left.native_plan);
        let right = Arc::clone(&join_params.right.native_plan);

        // Null-aware anti-join must run in CollectLeft mode. In Partitioned mode
        // each partition only sees per-partition null/emptiness state, which can
        // produce wrong NOT IN results across partitions. DataFusion's JoinSelection
        // rewrites null-aware joins to CollectLeft for this reason, but Comet
        // executes the physical plan directly so we must pick the mode here.
        let partition_mode = if join.null_aware_anti_join {
            PartitionMode::CollectLeft
        } else {
            PartitionMode::Partitioned
        };

        let hash_join = Arc::new(HashJoinExec::try_new(
            left,
            right,
            join_params.join_on,
            join_params.join_filter,
            &join_params.join_type,
            None,
            partition_mode,
            // null doesn't equal to null in Spark join key. If the join key is
            // `EqualNullSafe`, Spark will rewrite it during planning.
            NullEquality::NullEqualsNothing,
            join.null_aware_anti_join,
        )?);

        // If the hash join is build right, we need to swap the left and right.
        // Exception: null-aware anti-join requires LeftAnti + build-right semantics
        // (which matches DataFusion's default), and swap_inputs would turn LeftAnti
        // into RightAnti, which DataFusion rejects with null_aware=true.
        if join.build_side == BuildSide::BuildLeft as i32 || join.null_aware_anti_join {
            let hash_join = PhysicalPlanner::apply_join_dynamic_filter(
                hash_join,
                join.dynamic_filter_enabled && !join.null_aware_anti_join,
                planner.session_ctx.copied_config().options(),
            )?;
            Ok((
                scans,
                shuffle_scans,
                Arc::new(SparkPlan::new(
                    spark_plan.plan_id,
                    hash_join,
                    vec![join_params.left, join_params.right],
                )),
            ))
        } else {
            let swapped_hash_join = hash_join.as_ref().swap_inputs(PartitionMode::Partitioned)?;
            let swapped_hash_join = PhysicalPlanner::apply_join_dynamic_filter(
                swapped_hash_join,
                join.dynamic_filter_enabled,
                planner.session_ctx.copied_config().options(),
            )?;

            let mut additional_native_plans = vec![];
            if swapped_hash_join.is::<ProjectionExec>() {
                // a projection was added to the hash join
                additional_native_plans.push(Arc::clone(swapped_hash_join.children()[0]));
            }

            Ok((
                scans,
                shuffle_scans,
                Arc::new(SparkPlan::new_with_additional(
                    spark_plan.plan_id,
                    swapped_hash_join,
                    vec![join_params.left, join_params.right],
                    additional_native_plans,
                )),
            ))
        }
    }
}

/// Builder for `SortMergeJoin`.
pub struct SortMergeJoinBuilder;

impl OperatorBuilder for SortMergeJoinBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let join = extract_op!(spark_plan, SortMergeJoin);
        let children = &spark_plan.children;

        let (join_params, scans, shuffle_scans) = planner.parse_join_parameters(
            inputs,
            children,
            &join.left_join_keys,
            &join.right_join_keys,
            join.join_type,
            &join.condition,
            partition_count,
        )?;

        let sort_options = join
            .sort_options
            .iter()
            .map(|sort_option| {
                let sort_expr = planner
                    .create_sort_expr(sort_option, join_params.left.schema())
                    .unwrap();
                SortOptions {
                    descending: sort_expr.options.descending,
                    nulls_first: sort_expr.options.nulls_first,
                }
            })
            .collect();

        let left = Arc::clone(&join_params.left.native_plan);
        let right = Arc::clone(&join_params.right.native_plan);

        let left_schema = left.schema();
        let right_schema = right.schema();
        let join_on = join_params
            .join_on
            .into_iter()
            .map(|(l, r)| {
                Ok((
                    strip_timestamp_tz(l, left_schema.as_ref())?,
                    strip_timestamp_tz(r, right_schema.as_ref())?,
                ))
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        let join = Arc::new(SortMergeJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            join_on,
            join_params.join_filter,
            join_params.join_type,
            sort_options,
            // null doesn't equal to null in Spark join key. If the join key is
            // `EqualNullSafe`, Spark will rewrite it during planning.
            NullEquality::NullEqualsNothing,
        )?);

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                join,
                vec![
                    Arc::clone(&join_params.left),
                    Arc::clone(&join_params.right),
                ],
            )),
        ))
    }
}

/// Builder for `BroadcastNestedLoopJoin`.
pub struct BroadcastNestedLoopJoinBuilder;

impl OperatorBuilder for BroadcastNestedLoopJoinBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let bnlj = extract_op!(spark_plan, BroadcastNestedLoopJoin);
        let children = &spark_plan.children;

        let (join_params, scans, shuffle_scans) = planner.parse_join_parameters(
            inputs,
            children,
            &[],
            &[],
            bnlj.join_type,
            &bnlj.condition,
            partition_count,
        )?;

        let left = Arc::clone(&join_params.left.native_plan);
        let right = Arc::clone(&join_params.right.native_plan);

        let nested_loop_join = Arc::new(NestedLoopJoinExec::try_new(
            left,
            right,
            join_params.join_filter,
            &join_params.join_type,
            None,
        )?);

        if bnlj.build_side == BuildSide::BuildRight as i32 {
            let swapped_join = nested_loop_join.as_ref().swap_inputs()?;
            let mut additional_native_plans = vec![];
            if swapped_join.is::<ProjectionExec>() {
                additional_native_plans.push(Arc::clone(swapped_join.children()[0]));
            }
            Ok((
                scans,
                shuffle_scans,
                Arc::new(SparkPlan::new_with_additional(
                    spark_plan.plan_id,
                    swapped_join,
                    vec![join_params.left, join_params.right],
                    additional_native_plans,
                )),
            ))
        } else {
            Ok((
                scans,
                shuffle_scans,
                Arc::new(SparkPlan::new(
                    spark_plan.plan_id,
                    nested_loop_join,
                    vec![join_params.left, join_params.right],
                )),
            ))
        }
    }
}

impl PhysicalPlanner {
    /// Keep Spark metric identities for the small set of nodes whose children
    /// runtime-filter placement can replace. Stop at native/Spark tree boundaries.
    fn prepare_probe_for_runtime_filters(
        plan: Arc<SparkPlan>,
    ) -> Result<Arc<SparkPlan>, ExecutionError> {
        let mut native: Arc<dyn ExecutionPlan> =
            if let Some(filter) = plan.native_plan.downcast_ref::<FilterExec>() {
                Arc::new(CometFilterExec::from_datafusion(filter.clone()))
            } else if let Some(projection) = plan.native_plan.downcast_ref::<ProjectionExec>() {
                Arc::new(CometProjectionExec::from_datafusion(projection.clone()))
            } else {
                return Ok(plan);
            };
        let mut prepared = plan.as_ref().clone();
        if let ([child], [only]) = (plan.children.as_slice(), native.children().as_slice()) {
            if Arc::ptr_eq(only, &child.native_plan) {
                let child = Self::prepare_probe_for_runtime_filters(Arc::clone(child))?;
                native = native.replace_children(
                    vec![Arc::clone(&child.native_plan)],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?;
                prepared.children = vec![child];
            }
        }
        prepared.native_plan = native;
        Ok(Arc::new(prepared))
    }

    /// Attach after choosing the final build side, including the projection emitted
    /// by `swap_inputs`, without running DataFusion's physical optimizer.
    pub(crate) fn apply_join_dynamic_filter(
        plan: Arc<dyn ExecutionPlan>,
        enabled: bool,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        if !enabled {
            return Ok(plan);
        }
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            let child =
                Self::apply_join_dynamic_filter(Arc::clone(projection.input()), enabled, config)?;
            return if !Arc::ptr_eq(&child, projection.input()) {
                Ok(plan.replace_children(
                    vec![child],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?)
            } else {
                Ok(plan)
            };
        }
        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
            log::debug!(
                "Join dynamic filter skipped: unexpected plan shape {}",
                plan.name()
            );
            return Ok(plan);
        };
        match DynamicFilterJoinExec::try_new(join, config)? {
            Some(wrapper) => Ok(Arc::new(wrapper)),
            None => Ok(plan),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_join_parameters(
        &self,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        children: &[Operator],
        left_join_keys: &[Expr],
        right_join_keys: &[Expr],
        join_type: i32,
        condition: &Option<Expr>,
        partition_count: usize,
    ) -> Result<(JoinParameters, Vec<ScanExec>, Vec<ShuffleScanExec>), ExecutionError> {
        assert_eq!(children.len(), 2);
        let (mut left_scans, mut left_shuffle_scans, left) =
            self.create_plan(&children[0], inputs, partition_count)?;
        let (mut right_scans, mut right_shuffle_scans, right) =
            self.create_plan(&children[1], inputs, partition_count)?;

        left_scans.append(&mut right_scans);
        left_shuffle_scans.append(&mut right_shuffle_scans);

        let left_join_exprs: Vec<_> = left_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, left.schema()))
            .collect::<Result<Vec<_>, _>>()?;
        let right_join_exprs: Vec<_> = right_join_keys
            .iter()
            .map(|expr| self.create_expr(expr, right.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        let join_on = left_join_exprs
            .into_iter()
            .zip(right_join_exprs)
            .collect::<Vec<_>>();

        let join_type = match join_type.try_into() {
            Ok(JoinType::Inner) => DFJoinType::Inner,
            Ok(JoinType::LeftOuter) => DFJoinType::Left,
            Ok(JoinType::RightOuter) => DFJoinType::Right,
            Ok(JoinType::FullOuter) => DFJoinType::Full,
            Ok(JoinType::LeftSemi) => DFJoinType::LeftSemi,
            Ok(JoinType::LeftAnti) => DFJoinType::LeftAnti,
            Ok(JoinType::Existence) => DFJoinType::LeftMark,
            Err(_) => {
                return Err(GeneralError(format!(
                    "Unsupported join type: {join_type:?}"
                )));
            }
        };

        // Handle join filter as DataFusion `JoinFilter` struct
        let join_filter = if let Some(expr) = condition {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields();
            let right_fields = right_schema.fields();
            let all_fields: Vec<_> = left_fields
                .into_iter()
                .chain(right_fields)
                .cloned()
                .collect();
            let full_schema = Arc::new(Schema::new(all_fields));

            // Because we cast dictionary array to array in scan operator,
            // we need to change dictionary type to data type for join filter expression.
            let fields: Vec<_> = full_schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => Arc::new(Field::new(
                        f.name(),
                        val_type.as_ref().clone(),
                        f.is_nullable(),
                    )),
                    _ => Arc::clone(f),
                })
                .collect();

            let full_schema = Arc::new(Schema::new(fields));

            let physical_expr = self.create_expr(expr, full_schema)?;
            let (left_field_indices, right_field_indices) =
                expr_to_columns(&physical_expr, left_fields.len(), right_fields.len())?;
            let column_indices = JoinFilter::build_column_indices(
                left_field_indices.clone(),
                right_field_indices.clone(),
            );

            let filter_fields: Vec<Field> = left_field_indices
                .clone()
                .into_iter()
                .map(|i| left.schema().field(i).clone())
                .chain(
                    right_field_indices
                        .clone()
                        .into_iter()
                        .map(|i| right.schema().field(i).clone()),
                )
                // Because we cast dictionary array to array in scan operator,
                // we need to change dictionary type to data type for join filter expression.
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, val_type) => {
                        Field::new(f.name(), val_type.as_ref().clone(), f.is_nullable())
                    }
                    _ => f.clone(),
                })
                .collect_vec();

            let filter_schema = Schema::new_with_metadata(filter_fields, HashMap::new());

            // Rewrite the physical expression to use the new column indices.
            // DataFusion's join filter is bound to intermediate schema which contains
            // only the fields used in the filter expression. But the Spark's join filter
            // expression is bound to the full schema. We need to rewrite the physical
            // expression to use the new column indices.
            let rewritten_physical_expr = rewrite_physical_expr(
                physical_expr,
                left_schema.fields.len(),
                right_schema.fields.len(),
                &left_field_indices,
                &right_field_indices,
            )?;

            Some(JoinFilter::new(
                rewritten_physical_expr,
                column_indices,
                filter_schema.into(),
            ))
        } else {
            None
        };

        Ok((
            JoinParameters {
                left: Arc::clone(&left),
                right: Arc::clone(&right),
                join_on,
                join_type,
                join_filter,
            },
            left_scans,
            left_shuffle_scans,
        ))
    }
}

/// If `expr` evaluates to `Timestamp(_, Some(_))` against `schema`, wrap it in a
/// metadata-only cast to `Timestamp(_, None)`. This is required because
/// DataFusion's `SortMergeJoinExec` comparator only supports timezone-less
/// timestamp types, while Spark's `TimestampType` serializes as
/// `Timestamp(µs, "UTC")`. The cast preserves ordering on the same time unit.
fn strip_timestamp_tz(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    match expr.data_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => Ok(Arc::new(CastExpr::new(
            expr,
            DataType::Timestamp(unit, None),
            None,
        ))),
        _ => Ok(expr),
    }
}

/// Collects the indices of the columns in the input schema that are used in the expression
/// and returns them as a pair of vectors, one for the left side and one for the right side.
fn expr_to_columns(
    expr: &Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
) -> Result<(Vec<usize>, Vec<usize>), ExecutionError> {
    let mut left_field_indices: Vec<usize> = vec![];
    let mut right_field_indices: Vec<usize> = vec![];

    expr.apply(&mut |expr: &Arc<dyn PhysicalExpr>| {
        Ok({
            if let Some(column) = expr.downcast_ref::<Column>() {
                if column.index() > left_field_len + right_field_len {
                    return Err(DataFusionError::Internal(format!(
                        "Column index {} out of range",
                        column.index()
                    )));
                } else if column.index() < left_field_len {
                    left_field_indices.push(column.index());
                } else {
                    right_field_indices.push(column.index() - left_field_len);
                }
            }
            TreeNodeRecursion::Continue
        })
    })?;

    left_field_indices.sort();
    right_field_indices.sort();

    Ok((left_field_indices, right_field_indices))
}

/// A physical join filter rewritter which rewrites the column indices in the expression
/// to use the new column indices. See `rewrite_physical_expr`.
struct JoinFilterRewriter<'a> {
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &'a [usize],
    right_field_indices: &'a [usize],
}

impl JoinFilterRewriter<'_> {
    fn new<'a>(
        left_field_len: usize,
        right_field_len: usize,
        left_field_indices: &'a [usize],
        right_field_indices: &'a [usize],
    ) -> JoinFilterRewriter<'a> {
        JoinFilterRewriter {
            left_field_len,
            right_field_len,
            left_field_indices,
            right_field_indices,
        }
    }
}

impl TreeNodeRewriter for JoinFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> datafusion::common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.downcast_ref::<Column>() {
            if column.index() < self.left_field_len {
                // left side
                let new_index = self
                    .left_field_indices
                    .iter()
                    .position(|&x| x == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in left field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index,
                ))))
            } else if column.index() < self.left_field_len + self.right_field_len {
                // right side
                let new_index = self
                    .right_field_indices
                    .iter()
                    .position(|&x| x + self.left_field_len == column.index())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Column index {} not found in right field indices",
                            column.index()
                        ))
                    })?;
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    new_index + self.left_field_indices.len(),
                ))))
            } else {
                Err(DataFusionError::Internal(format!(
                    "Column index {} out of range",
                    column.index()
                )))
            }
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Rewrites the physical expression to use the new column indices.
/// This is necessary when the physical expression is used in a join filter, as the column
/// indices are different from the original schema.
fn rewrite_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
    left_field_len: usize,
    right_field_len: usize,
    left_field_indices: &[usize],
    right_field_indices: &[usize],
) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
    let mut rewriter = JoinFilterRewriter::new(
        left_field_len,
        right_field_len,
        left_field_indices,
        right_field_indices,
    );

    Ok(expr.rewrite(&mut rewriter).data()?)
}
