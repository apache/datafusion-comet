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

//! Local graph construction. Reuse Comet scan/expression builders without per-task plan execution.

use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion::common::{JoinType, NullEquality};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    filter::FilterExec, projection::ProjectionExec, union::UnionExec, ExecutionPlan,
};
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_comet_local::LocalQuery;
use datafusion_comet_proto::local::{LocalAggregate, LocalJoin, LocalOutput};
use datafusion_comet_proto::spark_operator::{operator::OpStruct, Operator, SparkFilePartition};
use prost::Message;

use crate::execution::operators::ExecutionError;
use crate::execution::planner::PhysicalPlanner;
use crate::parquet::parquet_support::CometObjectStoreRegistry;

pub(super) struct QuerySettings<'a> {
    pub terminal: &'a [u8],
    pub aggregate: &'a [u8],
    pub memory_limit: usize,
    pub spill_enabled: bool,
}

pub(super) fn parquet_query(
    bytes: &[u8],
    partitions: &[Vec<u8>],
    batch_size: usize,
    columns: usize,
    row_filter_pushdown: bool,
    settings: QuerySettings<'_>,
) -> Result<LocalQuery, ExecutionError> {
    let root = Operator::decode(bytes)?;
    let groups = partitions
        .iter()
        .map(|b| SparkFilePartition::decode(b.as_slice()))
        .collect::<Result<Vec<_>, _>>()?;
    let context = query_context(batch_size, groups.len(), row_filter_pushdown, &settings)?;
    let planner = PhysicalPlanner::new(Arc::clone(&context), 0).with_sql_text_pool(&root);
    let plan = build(&root, &groups, &planner)?;
    let plan = if settings.aggregate.is_empty() {
        plan
    } else {
        aggregate_plan(plan, &LocalAggregate::decode(settings.aggregate)?, &planner)?
    };
    let plan = output_plan(plan, settings.terminal, &planner)?;
    if plan.schema().fields().len() != columns {
        return Err(ExecutionError::GeneralError(
            "Local output schema width mismatch".into(),
        ));
    }
    size_sorters(&context, plan.as_ref(), settings.memory_limit);
    Ok(LocalQuery::new(plan, context.task_ctx()))
}

fn query_context(
    batch_size: usize,
    partitions: usize,
    row_filter_pushdown: bool,
    settings: &QuerySettings<'_>,
) -> Result<Arc<SessionContext>, ExecutionError> {
    let mut config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(partitions.max(1));
    config.options_mut().execution.parquet.pushdown_filters = row_filter_pushdown;
    config.options_mut().execution.parquet.reorder_filters = row_filter_pushdown;
    // Registry and configuration are query-owned. Never inherit another query's credentials.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(settings.memory_limit)))
        .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
            if settings.spill_enabled {
                DiskManagerMode::OsTmpDirectory
            } else {
                DiskManagerMode::Disabled
            },
        ))
        .with_object_store_registry(Arc::new(CometObjectStoreRegistry::default()))
        .build()?;
    let context = Arc::new(SessionContext::new_with_config_rt(
        config,
        Arc::new(runtime),
    ));
    Ok(context)
}

/// Sizes DataFusion's sort settings by the number of sorters that share the query budget.
/// Must run after the whole graph is built and before its task context is created.
fn size_sorters(context: &SessionContext, plan: &dyn ExecutionPlan, memory_limit: usize) {
    fn sorters(plan: &dyn ExecutionPlan) -> usize {
        let own = match plan.downcast_ref::<SortExec>() {
            Some(sort) if sort.preserve_partitioning() => {
                sort.input().output_partitioning().partition_count()
            }
            Some(_) => 1,
            None => 0,
        };
        own + plan
            .children()
            .into_iter()
            .map(|child| sorters(child.as_ref()))
            .sum::<usize>()
    }
    let sorters = sorters(plan);
    if sorters == 0 {
        return;
    }
    let share = memory_limit / sorters;
    let state = context.state_ref();
    let mut state = state.write();
    let execution = &mut state.config_mut().options_mut().execution;
    // Every sorter reserves this much for its final merge before sorting. With the default
    // 10 MiB, a few concurrent sorters can exhaust a small query budget up front.
    execution.sort_spill_reservation_bytes =
        (share / 4).min(execution.sort_spill_reservation_bytes);
    // Workaround for DataFusion 55.1's ExternalSorter, fixed upstream in DataFusion 56.0.0:
    // before spilling, it frees its merge reservation and merges buffered batches with a new,
    // empty, unspillable reservation. Once spillable sorters fill the fair pool, that merge
    // cannot grow and the query fails instead of spilling. A sorter spills once its buffered
    // batches reach its fair share, so a threshold of one share makes it sort them in place
    // instead of merging. This costs unaccounted transient copies and slower multi-column
    // sorts that fit in memory. Remove this override after upgrading to DataFusion 56.0.0;
    // `multi_column_sorts_spill_under_a_shared_budget` must still pass without it.
    execution.sort_in_place_threshold_bytes = share.max(execution.sort_in_place_threshold_bytes);
}

pub(super) fn join_query(
    bytes: &[u8],
    batch_size: usize,
    columns: usize,
    row_filter_pushdown: bool,
    settings: QuerySettings<'_>,
) -> Result<LocalQuery, ExecutionError> {
    let join = LocalJoin::decode(bytes)?;
    if join.left_files.len() > 1024 || join.right_files.len() > 1024 {
        return Err(ExecutionError::GeneralError(
            "Too many local join file groups".into(),
        ));
    }
    let context = query_context(
        batch_size,
        join.partitions as usize,
        row_filter_pushdown,
        &settings,
    )?;
    let invalid = || ExecutionError::GeneralError("Missing local join input".into());
    let left = join.left.as_ref().ok_or_else(invalid)?;
    let right = join.right.as_ref().ok_or_else(invalid)?;
    // Each input retains its own SQL text pool; neither borrows the other's scan metadata.
    let left_planner = PhysicalPlanner::new(Arc::clone(&context), 0).with_sql_text_pool(left);
    let right_planner = PhysicalPlanner::new(Arc::clone(&context), 0).with_sql_text_pool(right);
    let left = build(left, &join.left_files, &left_planner)?;
    let right = build(right, &join.right_files, &right_planner)?;
    let planner = PhysicalPlanner::new(Arc::clone(&context), 0);
    let plan = join_plan(left, right, &join, &planner)?;
    let plan = output_plan(plan, settings.terminal, &planner)?;
    if plan.schema().fields().len() != columns {
        return Err(ExecutionError::GeneralError(
            "Local join output schema width mismatch".into(),
        ));
    }
    size_sorters(&context, plan.as_ref(), settings.memory_limit);
    Ok(LocalQuery::new(plan, context.task_ctx()))
}

fn join_plan(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join: &LocalJoin,
    planner: &PhysicalPlanner,
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    use datafusion_comet_proto::spark_operator::JoinType as SparkJoinType;
    if !(1..=1024).contains(&join.partitions)
        || join.left_keys.is_empty()
        || join.left_keys.len() != join.right_keys.len()
        || join.result.is_empty()
    {
        return Err(ExecutionError::GeneralError("Invalid local join".into()));
    }
    let kind = match SparkJoinType::try_from(join.join_type) {
        Ok(SparkJoinType::Inner) => JoinType::Inner,
        Ok(SparkJoinType::LeftOuter) => JoinType::Left,
        Ok(SparkJoinType::RightOuter) => JoinType::Right,
        Ok(SparkJoinType::FullOuter) => JoinType::Full,
        Ok(SparkJoinType::LeftSemi) => JoinType::LeftSemi,
        Ok(SparkJoinType::LeftAnti) => JoinType::LeftAnti,
        Err(_) => {
            return Err(ExecutionError::GeneralError(
                "Invalid local join type".into(),
            ))
        }
    };
    let left_keys = join
        .left_keys
        .iter()
        .map(|e| planner.create_expr(e, left.schema()))
        .collect::<Result<Vec<_>, _>>()?;
    let right_keys = join
        .right_keys
        .iter()
        .map(|e| planner.create_expr(e, right.schema()))
        .collect::<Result<Vec<_>, _>>()?;
    // Both exchanges use the same DataFusion hash implementation and partition count.
    // Spark's partition IDs and hash algorithm never cross this boundary.
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(left_keys.clone(), join.partitions as usize),
    )?);
    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(right_keys.clone(), join.partitions as usize),
    )?);
    let on = left_keys.into_iter().zip(right_keys).collect();
    let hash = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &kind,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )?;
    // swap_inputs restores Spark's logical output order with a projection when needed.
    let plan: Arc<dyn ExecutionPlan> = if join.build_right {
        hash.swap_inputs(PartitionMode::Partitioned)?
    } else {
        Arc::new(hash)
    };
    let result = join
        .result
        .iter()
        .enumerate()
        .map(|(i, e)| {
            planner
                .create_expr(e, plan.schema())
                .map(|expr| (expr, format!("col_{i}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(ProjectionExec::try_new(result, plan)?))
}

fn output_plan(
    input: Arc<dyn ExecutionPlan>,
    bytes: &[u8],
    planner: &PhysicalPlanner,
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    if bytes.is_empty() {
        return Ok(input);
    }
    let output = LocalOutput::decode(bytes)?;
    let skip = usize::try_from(output.skip)
        .map_err(|_| ExecutionError::GeneralError("Local offset overflow".into()))?;
    let fetch = output
        .fetch
        .map(usize::try_from)
        .transpose()
        .map_err(|_| ExecutionError::GeneralError("Local fetch overflow".into()))?;
    let top = fetch
        .map(|n| {
            n.checked_add(skip)
                .ok_or_else(|| ExecutionError::GeneralError("Local Top-K overflow".into()))
        })
        .transpose()?;
    let expressions = output
        .orders
        .iter()
        .map(|order| {
            let child = order
                .child
                .as_ref()
                .ok_or_else(|| ExecutionError::GeneralError("Missing local sort key".into()))?;
            Ok(PhysicalSortExpr {
                expr: planner.create_expr(child, input.schema())?,
                options: SortOptions {
                    descending: order.descending,
                    nulls_first: order.nulls_first,
                },
            })
        })
        .collect::<Result<Vec<_>, ExecutionError>>()?;
    // Sort each input partition in parallel, then merge the sorted runs into one ordered
    // partition. Never send independently sorted partitions through the unordered result
    // coalescer: that would lose the global order. An unordered limit just gathers.
    let mut plan: Arc<dyn ExecutionPlan> = match LexOrdering::new(expressions) {
        Some(ordering) if input.output_partitioning().partition_count() > 1 => {
            let sorted = Arc::new(
                SortExec::new(ordering.clone(), input)
                    .with_preserve_partitioning(true)
                    .with_fetch(top),
            );
            Arc::new(SortPreservingMergeExec::new(ordering, sorted).with_fetch(top))
        }
        Some(ordering) => Arc::new(SortExec::new(ordering, input).with_fetch(top)),
        None => Arc::new(CoalescePartitionsExec::new(input)),
    };
    if fetch.is_some() || skip > 0 {
        plan = Arc::new(GlobalLimitExec::new(plan, skip, fetch));
    }
    if !output.result.is_empty() {
        let result = output
            .result
            .iter()
            .enumerate()
            .map(|(i, e)| {
                planner
                    .create_expr(e, plan.schema())
                    .map(|expr| (expr, format!("col_{i}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        plan = Arc::new(ProjectionExec::try_new(result, plan)?);
    }
    Ok(plan)
}

fn build(
    op: &Operator,
    groups: &[SparkFilePartition],
    planner: &PhysicalPlanner,
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    let invalid = || ExecutionError::GeneralError("Invalid local Parquet plan".into());
    match op.op_struct.as_ref().ok_or_else(invalid)? {
        OpStruct::NativeScan(scan) if op.children.is_empty() => {
            let common = scan.common.as_ref().ok_or_else(invalid)?;
            if common.encryption_enabled || !common.object_store_options.is_empty() {
                return Err(ExecutionError::GeneralError(
                    "Local scan requires unencrypted local files".into(),
                ));
            }
            let empty = SparkFilePartition::default();
            let groups = if groups.is_empty() {
                std::slice::from_ref(&empty)
            } else {
                groups
            };
            let mut plans = Vec::with_capacity(groups.len());
            for group in groups {
                if group.partitioned_file.iter().any(|file| {
                    url::Url::parse(&file.file_path).map_or(true, |url| url.scheme() != "file")
                }) {
                    return Err(invalid());
                }
                let mut scan = scan.clone();
                scan.file_partition = Some(group.clone());
                let mut leaf = op.clone();
                leaf.op_struct = Some(OpStruct::NativeScan(scan));
                let (inputs, shuffles, plan) = planner.create_plan(&leaf, &mut vec![], 1)?;
                if !inputs.is_empty() || !shuffles.is_empty() {
                    return Err(invalid());
                }
                plans.push(Arc::clone(&plan.native_plan));
            }
            if plans.len() == 1 {
                Ok(plans.remove(0))
            } else {
                Ok(UnionExec::try_new(plans)?)
            }
        }
        OpStruct::Projection(project)
            if op.children.len() == 1 && !project.project_list.is_empty() =>
        {
            let child = build(&op.children[0], groups, planner)?;
            let expressions = project
                .project_list
                .iter()
                .enumerate()
                .map(|(i, expression)| {
                    planner
                        .create_expr(expression, child.schema())
                        .map(|e| (e, format!("col_{i}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(ProjectionExec::try_new(expressions, child)?))
        }
        OpStruct::Filter(filter) if op.children.len() == 1 => {
            let child = build(&op.children[0], groups, planner)?;
            let predicate = planner.create_expr(
                filter.predicate.as_ref().ok_or_else(invalid)?,
                child.schema(),
            )?;
            Ok(Arc::new(FilterExec::try_new(predicate, child)?))
        }
        _ => Err(invalid()),
    }
}

fn aggregate_plan(
    input: Arc<dyn ExecutionPlan>,
    aggregate: &LocalAggregate,
    planner: &PhysicalPlanner,
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    use datafusion_comet_proto::spark_expression::agg_expr::ExprStruct;
    if aggregate.partitions == 0
        || aggregate.partitions > 1024
        || aggregate.result.is_empty()
        || aggregate.aggregates.is_empty()
        || aggregate.aggregates.iter().any(|a| {
            !matches!(
                a.expr_struct,
                Some(ExprStruct::Count(_) | ExprStruct::Min(_) | ExprStruct::Max(_))
            )
        })
    {
        return Err(ExecutionError::GeneralError(
            "Invalid local aggregate".into(),
        ));
    }
    let schema = input.schema();
    let grouping = aggregate
        .grouping
        .iter()
        .enumerate()
        .map(|(i, e)| {
            planner
                .create_expr(e, Arc::clone(&schema))
                .map(|expr| (expr, format!("group_{i}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expressions = aggregate
        .aggregates
        .iter()
        .map(|e| {
            planner
                .create_agg_expr(e, Arc::clone(&schema))
                .map(Arc::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let filters = aggregate
        .aggregates
        .iter()
        .map(|e| {
            e.filter
                .as_ref()
                .map(|f| planner.create_expr(f, Arc::clone(&schema)))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let group_by = PhysicalGroupBy::new_single(grouping);
    let gather = group_by.is_empty() || aggregate.partitions == 1;
    // Pre-aggregate each input partition so only partial states cross the exchange. Partial
    // buffers and DataFusion's hash buckets stay internal to this graph; no Spark consumer
    // observes them. A single input partition that needs no exchange aggregates directly.
    let plan: Arc<dyn ExecutionPlan> =
        if gather && input.output_partitioning().partition_count() == 1 {
            Arc::new(AggregateExec::try_new(
                AggregateMode::Single,
                group_by,
                expressions,
                filters,
                input,
                schema,
            )?)
        } else {
            let partial = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                expressions.clone(),
                filters,
                input,
                Arc::clone(&schema),
            )?);
            let final_group_by = group_by.as_final();
            let final_filters = vec![None; expressions.len()];
            let (mode, input): (_, Arc<dyn ExecutionPlan>) = if gather {
                (
                    AggregateMode::Final,
                    Arc::new(CoalescePartitionsExec::new(partial)),
                )
            } else {
                let keys = final_group_by
                    .expr()
                    .iter()
                    .map(|(e, _)| Arc::clone(e))
                    .collect();
                (
                    AggregateMode::FinalPartitioned,
                    Arc::new(RepartitionExec::try_new(
                        partial,
                        Partitioning::Hash(keys, aggregate.partitions as usize),
                    )?),
                )
            };
            Arc::new(AggregateExec::try_new(
                mode,
                final_group_by,
                expressions,
                final_filters,
                input,
                schema,
            )?)
        };
    let results = aggregate
        .result
        .iter()
        .enumerate()
        .map(|(i, e)| {
            planner
                .create_expr(e, plan.schema())
                .map(|expr| (expr, format!("col_{i}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(ProjectionExec::try_new(results, plan)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::physical_plan::{accept, collect, ExecutionPlanVisitor};
    use datafusion_comet_proto::spark_expression::{
        agg_expr, expr::ExprStruct, AggExpr, BoundReference, Count, Expr,
    };

    fn bound(index: i32) -> Expr {
        Expr {
            expr_struct: Some(ExprStruct::Bound(BoundReference {
                index,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn complete_aggregate_spills_and_releases_query_reservations() {
        spill_case(1, 2 * 1024 * 1024, false).await;
    }

    #[tokio::test]
    async fn shared_repartition_and_aggregate_spill_on_one_worker() {
        spill_case(7, 8 * 1024 * 1024, false).await;
    }

    #[tokio::test]
    async fn early_drop_releases_spilled_aggregate_and_repartition() {
        spill_case(7, 8 * 1024 * 1024, true).await;
    }

    async fn spill_case(partitions: u32, budget: usize, early: bool) {
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(budget));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build()
            .unwrap();
        let context = Arc::new(SessionContext::new_with_config_rt(
            SessionConfig::new().with_batch_size(1024),
            Arc::new(runtime),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batches: Vec<_> = (0..200)
            .map(|chunk| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(
                        (0..1024).map(|i| chunk * 1024 + i),
                    ))],
                )
                .unwrap()
            })
            .collect();
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let message = LocalAggregate {
            grouping: vec![bound(0)],
            aggregates: vec![AggExpr {
                expr_struct: Some(agg_expr::ExprStruct::Count(Count {
                    children: vec![bound(0)],
                })),
                ..Default::default()
            }],
            result: vec![bound(0), bound(1)],
            partitions,
        };
        let plan = aggregate_plan(
            input,
            &message,
            &PhysicalPlanner::new(Arc::clone(&context), 0),
        )
        .unwrap();
        let batches = if early {
            use futures::StreamExt;
            let mut stream =
                datafusion::physical_plan::execute_stream(Arc::clone(&plan), context.task_ctx())
                    .unwrap();
            let batch = tokio::time::timeout(std::time::Duration::from_secs(30), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            drop(stream);
            vec![batch]
        } else {
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                collect(Arc::clone(&plan), context.task_ctx()),
            )
            .await
            .unwrap()
            .unwrap()
        };
        let mut keys = std::collections::BTreeSet::new();
        for batch in batches {
            let key = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(keys.insert(key.value(row)));
                assert_eq!(count.value(row), 1);
            }
        }
        if early {
            assert!(!keys.is_empty() && keys.len() < 200 * 1024);
        } else {
            assert_eq!(keys.len(), 200 * 1024);
        }
        struct Spills(usize);
        impl ExecutionPlanVisitor for Spills {
            type Error = std::convert::Infallible;
            fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
                if let Some(metrics) = plan.metrics() {
                    if let Some(value) = metrics.spilled_bytes() {
                        self.0 += value;
                    }
                }
                Ok(true)
            }
        }
        let mut spills = Spills(0);
        accept(plan.as_ref(), &mut spills).unwrap();
        assert!(spills.0 > 0, "test must actually spill");
        drop(plan);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.reserved() != 0 || context.runtime_env().disk_manager.used_disk_space() != 0
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn partitioned_join_one_worker_releases_both_exchanges() {
        join_case(false, false).await;
    }

    #[tokio::test]
    async fn partitioned_join_early_drop_releases_both_exchanges() {
        join_case(true, false).await;
    }

    #[tokio::test]
    async fn partitioned_join_resource_error_releases_both_exchanges() {
        join_case(false, true).await;
    }

    async fn join_case(early: bool, fail: bool) {
        let context = query_context(
            128,
            7,
            false,
            &QuerySettings {
                aggregate: &[],
                terminal: &[],
                memory_limit: if fail { 1 } else { 32 * 1024 * 1024 },
                spill_enabled: true,
            },
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batches: Vec<_> = (0..64)
            .map(|chunk| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(
                        (0..1024).map(|i| chunk * 1024 + i),
                    ))],
                )
                .unwrap()
            })
            .collect();
        let left = MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&batches),
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        let right = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let plan = join_plan(
            left,
            right,
            &LocalJoin {
                left_keys: vec![bound(0)],
                right_keys: vec![bound(0)],
                partitions: 7,
                build_right: true,
                result: vec![bound(0), bound(1)],
                ..Default::default()
            },
            &PhysicalPlanner::new(Arc::clone(&context), 0),
        )
        .unwrap();
        let output = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            if early {
                use futures::StreamExt;
                let mut stream = datafusion::physical_plan::execute_stream(
                    Arc::clone(&plan),
                    context.task_ctx(),
                )?;
                let batch = stream.next().await.unwrap()?;
                drop(stream);
                Ok(vec![batch])
            } else {
                collect(Arc::clone(&plan), context.task_ctx()).await
            }
        })
        .await
        .unwrap();
        if fail {
            let error = output.unwrap_err().to_string();
            assert!(
                error.contains("Resources exhausted") || error.contains("memory"),
                "{error}"
            );
        } else {
            let mut keys = std::collections::BTreeSet::new();
            for batch in output.unwrap() {
                let left = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let right = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert_eq!(left.value(row), right.value(row));
                    assert!(keys.insert(left.value(row)));
                }
            }
            if early {
                assert!(!keys.is_empty() && keys.len() < 65536);
            } else {
                assert_eq!(keys.len(), 65536);
            }
        }
        drop(plan);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while context.runtime_env().memory_pool.reserved() != 0
                || context.runtime_env().disk_manager.used_disk_space() != 0
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn global_sort_spills_and_releases_query_resources() {
        sort_case(false, false).await;
    }

    #[tokio::test]
    async fn dropping_global_sort_releases_spill_files() {
        sort_case(true, false).await;
    }

    #[tokio::test]
    async fn top_k_applies_offset_once_across_input_partitions() {
        sort_case(false, true).await;
    }

    async fn sort_case(early: bool, top: bool) {
        use datafusion_comet_proto::local::LocalSort;
        let context = query_context(
            1024,
            7,
            false,
            &QuerySettings {
                aggregate: &[],
                terminal: &[],
                memory_limit: 16 * 1024 * 1024,
                spill_enabled: true,
            },
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let mut partitions = vec![vec![]; 7];
        let total = 256 * 4096;
        for chunk in 0..256 {
            partitions[chunk % 7].push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(
                        (0..4096).map(|i| (total - 1 - (chunk * 4096 + i)) as i64),
                    ))],
                )
                .unwrap(),
            );
        }
        let input = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
        let message = LocalOutput {
            orders: vec![LocalSort {
                child: Some(bound(0)),
                descending: false,
                nulls_first: true,
            }],
            skip: if top { 9 } else { 0 },
            fetch: if top { Some(13) } else { None },
            result: vec![],
        };
        let plan = output_plan(
            input,
            &message.encode_to_vec(),
            &PhysicalPlanner::new(Arc::clone(&context), 0),
        )
        .unwrap();
        size_sorters(&context, plan.as_ref(), 16 * 1024 * 1024);
        assert_eq!(plan.properties().output_partitioning().partition_count(), 1);
        let batches = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            if early {
                use futures::StreamExt;
                let mut stream = datafusion::physical_plan::execute_stream(
                    Arc::clone(&plan),
                    context.task_ctx(),
                )?;
                let first = stream.next().await.unwrap()?;
                drop(stream);
                Ok(vec![first])
            } else {
                collect(Arc::clone(&plan), context.task_ctx()).await
            }
        })
        .await
        .unwrap()
        .unwrap();
        let mut next = if top { 9 } else { 0 };
        for batch in batches {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(values.value(row), next);
                next += 1;
            }
        }
        if early {
            assert!(next > 0 && next < total as i64);
        } else {
            assert_eq!(next, if top { 22 } else { total as i64 });
        }
        if !top {
            struct Spills(usize);
            impl ExecutionPlanVisitor for Spills {
                type Error = std::convert::Infallible;
                fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
                    if let Some(metrics) = plan.metrics() {
                        self.0 += metrics.spilled_bytes().unwrap_or_default();
                    }
                    Ok(true)
                }
            }
            let mut spills = Spills(0);
            accept(plan.as_ref(), &mut spills).unwrap();
            assert!(spills.0 > 0, "sort test must actually spill");
        }
        drop(plan);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while context.runtime_env().memory_pool.reserved() != 0
                || context.runtime_env().disk_manager.used_disk_space() != 0
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn concurrent_queries_keep_spill_and_reservation_budgets_isolated() {
        for _ in 0..3 {
            tokio::join!(
                sort_case(false, false),
                sort_case(true, false),
                join_case(false, true)
            );
        }
    }

    /// Several sorters with a two-column key share a budget far below the data size and run
    /// concurrently, so the pool fills with their spillable batches before they spill. With
    /// DataFusion 55.1 this fails with an `ExternalSorterMerge` allocation error unless
    /// `size_sorters` raises the in-place sort threshold. Keep this test when that override is
    /// removed after upgrading to DataFusion 56.0.0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_column_sorts_spill_under_a_shared_budget() {
        use arrow::array::Array;
        use datafusion_comet_proto::local::LocalSort;
        let budget = 8 * 1024 * 1024;
        let context = query_context(
            4096,
            1,
            false,
            &QuerySettings {
                aggregate: &[],
                terminal: &[],
                memory_limit: budget,
                spill_enabled: true,
            },
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("rank", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let (sorters, rows) = (4, 1 << 20);
        let mut partitions = vec![vec![]; sorters];
        for (chunk, start) in (0..rows).step_by(4096).enumerate() {
            let ids: Vec<i64> = (start..start + 4096).collect();
            partitions[chunk % sorters].push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(
                            ids.iter().map(|id| id * 37 % 1_000_003),
                        )),
                        Arc::new(Int64Array::from(ids)),
                    ],
                )
                .unwrap(),
            );
        }
        let input = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
        let order = |index| LocalSort {
            child: Some(bound(index)),
            descending: false,
            nulls_first: true,
        };
        let message = LocalOutput {
            orders: vec![order(0), order(1)],
            skip: 0,
            fetch: None,
            result: vec![],
        };
        let plan = output_plan(
            input,
            &message.encode_to_vec(),
            &PhysicalPlanner::new(Arc::clone(&context), 0),
        )
        .unwrap();
        size_sorters(&context, plan.as_ref(), budget);
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            collect(Arc::clone(&plan), context.task_ctx()),
        )
        .await
        .unwrap()
        .unwrap();
        let mut previous = None;
        let mut count = 0;
        for batch in &batches {
            let column = |i: usize| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            };
            let (rank, id) = (column(0), column(1));
            for row in 0..rank.len() {
                let key = (rank.value(row), id.value(row));
                assert!(previous.is_none_or(|p| p < key), "out of order at {key:?}");
                previous = Some(key);
            }
            count += rank.len();
        }
        assert_eq!(count, rows as usize);
        struct Spills(usize);
        impl ExecutionPlanVisitor for Spills {
            type Error = std::convert::Infallible;
            fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
                if let Some(metrics) = plan.metrics() {
                    self.0 += metrics.spilled_bytes().unwrap_or_default();
                }
                Ok(true)
            }
        }
        let mut spills = Spills(0);
        accept(plan.as_ref(), &mut spills).unwrap();
        assert!(spills.0 > 0, "the shared budget must force spilling");
        drop(plan);
        drop(batches);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while context.runtime_env().memory_pool.reserved() != 0
                || context.runtime_env().disk_manager.used_disk_space() != 0
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
