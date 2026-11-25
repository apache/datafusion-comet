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

//! Adaptive join planning for Comet fragments.
//!
//! A stage's joins are inside its Comet block, so Ballista's adaptive join rules see the stage
//! as a leaf, and Spark has already placed an exchange on each side of every shuffled join. When
//! a join's build side turns out small, broadcasting it is not enough: the probe side would still
//! be shuffled whole to reach the join. These rules move the join into the stage that produces
//! the probe side instead, with the build side broadcast to it, so the probe side's exchange
//! carries the join's output rather than all of its input.
//!
//! That only pays off if the probe side's stage hasn't run yet when the build side's size is
//! known. [`HoldProbeStages`] holds it back before it launches, where Spark's estimates say the
//! measurement could change the plan, and [`BroadcastIntoProbe`] decides once the build side's
//! stage finishes: it moves the join where the build side measured small, and otherwise releases
//! the probe side's stage as it was. Holding serialises the two stages, so like Ballista's own
//! build-side staging it uses Ballista's broadcast threshold, minimum probe ratio and maximum
//! estimate multiple.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_scheduler::state::adaptive::{AdaptiveRule, AdaptiveRules, ExchangeExec};
use comet::execution::native_executors::{
    partition_descriptor, partition_plans, CometFragmentExec,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, Result};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions, StatisticsArgs,
};
use datafusion_comet_proto::spark_expression::expr::ExprStruct;
use datafusion_comet_proto::spark_expression::{BoundReference, DataType, Expr};
use datafusion_comet_proto::spark_native_executors::Fragment;
use datafusion_comet_proto::spark_operator::operator::OpStruct;
use datafusion_comet_proto::spark_operator::{
    BuildSide, HashJoin, JoinType, Operator, Projection, ShuffleScan, Sort,
};
use log::{debug, info};
use prost::Message;

/// Whether `consumer` reads only one input that isn't read whole, so that how that input's rows
/// are placed in partitions matters only to operators above a column that places them.
fn only_partitioned_input(consumer: &CometFragmentExec) -> bool {
    consumer
        .fragment()
        .broadcast
        .iter()
        .filter(|broadcast| !**broadcast)
        .count()
        == 1
}

/// The rules Comet adds to Ballista's adaptive planning.
#[derive(Debug, Default)]
pub struct CometAdaptiveRules;

impl AdaptiveRules for CometAdaptiveRules {
    /// Both are replan rules: Ballista runs an embedder's preparation rules before it places the
    /// stage boundaries, and these rules read the exchanges on a stage's inputs. The first replan
    /// runs when the job is planned, before any stage launches, so [`HoldProbeStages`] sees every
    /// probe side's stage in time.
    fn replan_rules(&self, plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![
            Arc::new(HoldProbeStages),
            Arc::new(BroadcastIntoProbe { plan_ids }),
        ]
    }
}

/// A join in a stage's block that can run in its probe side's stage instead, with its build side
/// broadcast to that stage.
///
/// It is a hash join or a sort-merge join with keys that are columns, whose inputs are both input
/// leaves of the block (a sort-merge join's through the sorts Spark put above them). The build
/// side is one the join can read whole in every partition: the right side of an inner, left
/// outer, left semi or left anti join, or the left side of an inner or right outer join. A
/// sort-merge join has no build side of its own, so an inner one is a candidate both ways round.
///
/// Moved below the probe side's exchange the join's output keeps the probe side's columns first
/// (a join built on the left is followed by a projection that puts them there), so the
/// exchange's keys still name the same columns.
struct MovableJoin {
    /// The child indexes from the block's root to the join.
    path: Vec<usize>,
    /// The stage inputs that the join's probe and build leaves read.
    probe_input: usize,
    build_input: usize,
    /// Whether the build side is the join's left child.
    build_left: bool,
}

/// The parts of a hash or sort-merge join that [`MovableJoin`] needs.
struct JoinParts<'a> {
    join_type: i32,
    left_keys: &'a [Expr],
    right_keys: &'a [Expr],
    condition: Option<&'a Expr>,
    /// For a hash join, its build side; a sort-merge join has none.
    build_left: Option<bool>,
    sort_merge: bool,
}

fn join_parts(op: &Operator) -> Option<JoinParts<'_>> {
    match &op.op_struct {
        Some(OpStruct::HashJoin(join)) if !join.null_aware_anti_join => Some(JoinParts {
            join_type: join.join_type,
            left_keys: &join.left_join_keys,
            right_keys: &join.right_join_keys,
            condition: join.condition.as_ref(),
            build_left: Some(join.build_side == BuildSide::BuildLeft as i32),
            sort_merge: false,
        }),
        Some(OpStruct::SortMergeJoin(join)) => Some(JoinParts {
            join_type: join.join_type,
            left_keys: &join.left_join_keys,
            right_keys: &join.right_join_keys,
            condition: join.condition.as_ref(),
            build_left: None,
            sort_merge: true,
        }),
        _ => None,
    }
}

/// Whether a join of `join_type` can read its build side whole in every partition.
fn can_broadcast(join_type: i32, build_left: bool) -> bool {
    let types: &[JoinType] = if build_left {
        &[JoinType::Inner, JoinType::RightOuter]
    } else {
        &[
            JoinType::Inner,
            JoinType::LeftOuter,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ]
    };
    types.iter().any(|t| *t as i32 == join_type)
}

/// Whether a join of `join_type` outputs its right side's columns.
fn outputs_right(join_type: i32) -> bool {
    join_type != JoinType::LeftSemi as i32 && join_type != JoinType::LeftAnti as i32
}

/// The input leaf a join reads as one of its sides: `op` itself, or for a sort-merge join the
/// leaf under the sort Spark placed above it.
fn side_leaf(op: &Operator, sort_merge: bool) -> Option<&Operator> {
    if is_input_leaf(op) {
        return Some(op);
    }
    match &op.op_struct {
        Some(OpStruct::Sort(sort))
            if sort_merge
                && sort.fetch.is_none()
                && sort.skip.is_none()
                && op.children.len() == 1
                && is_input_leaf(&op.children[0]) =>
        {
            Some(&op.children[0])
        }
        _ => None,
    }
}

/// The probe side of a join: an input leaf under the filters and projections that only pick
/// columns which Spark plans between joins, and for a sort-merge join under its sort. They move
/// into the probe side's stage with the join.
struct ProbeSide<'a> {
    leaf: &'a Operator,
    /// The filters and projections from the join down to the leaf, without a sort-merge join's
    /// sort.
    chain: Vec<&'a Operator>,
    /// For each column the chain outputs, the column of the leaf it is.
    columns: Vec<usize>,
}

fn probe_side(op: &Operator, sort_merge: bool) -> Option<ProbeSide<'_>> {
    let mut op = op;
    if let Some(OpStruct::Sort(sort)) = &op.op_struct {
        if !sort_merge || sort.fetch.is_some() || sort.skip.is_some() || op.children.len() != 1 {
            return None;
        }
        op = &op.children[0];
    }
    let mut chain = vec![];
    while !is_input_leaf(op) {
        let passes = op.children.len() == 1
            && match &op.op_struct {
                Some(OpStruct::Filter(_)) => true,
                Some(OpStruct::Projection(projection)) => projection
                    .project_list
                    .iter()
                    .all(|expr| matches!(expr.expr_struct, Some(ExprStruct::Bound(_)))),
                _ => false,
            };
        if !passes {
            return None;
        }
        chain.push(op);
        op = &op.children[0];
    }
    let mut columns: Vec<usize> = (0..leaf_fields(op).len()).collect();
    for link in chain.iter().rev() {
        if let Some(OpStruct::Projection(projection)) = &link.op_struct {
            columns = projection
                .project_list
                .iter()
                .map(|expr| match &expr.expr_struct {
                    Some(ExprStruct::Bound(bound)) => columns.get(bound.index as usize).copied(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
        }
    }
    Some(ProbeSide {
        leaf: op,
        chain,
        columns,
    })
}

/// Where the keys of `exchange`, a hash exchange of a stage's output, are in the output of a
/// join moved into that stage, whose probe side outputs the stage's columns `columns` first:
/// `None` when the probe side drops a key.
fn moved_keys(exchange: &ExchangeExec, columns: &[usize]) -> Option<Vec<usize>> {
    let Some(Partitioning::Hash(keys, _)) = exchange.partitioning() else {
        return None;
    };
    keys.iter()
        .map(|key| {
            let index = key.downcast_ref::<Column>()?.index();
            columns.iter().position(|column| *column == index)
        })
        .collect()
}

/// The joins of `block` that [`MovableJoin`] describes.
fn movable_joins(block: &Operator) -> Vec<MovableJoin> {
    fn walk(
        op: &Operator,
        path: &mut Vec<usize>,
        leaves: &mut usize,
        found: &mut Vec<MovableJoin>,
    ) {
        if is_input_leaf(op) {
            *leaves += 1;
            return;
        }
        if let Some(parts) = join_parts(op) {
            let movable_shape = parts
                .left_keys
                .iter()
                .chain(parts.right_keys)
                .all(|key| matches!(key.expr_struct, Some(ExprStruct::Bound(_))))
                && op.children.len() == 2;
            if movable_shape {
                let left_input = *leaves;
                let right_input = *leaves + 1;
                let sides = match parts.build_left {
                    Some(build_left) => vec![build_left],
                    None => vec![false, true],
                };
                for build_left in sides {
                    let (probe_child, build_child) = if build_left {
                        (&op.children[1], &op.children[0])
                    } else {
                        (&op.children[0], &op.children[1])
                    };
                    if can_broadcast(parts.join_type, build_left)
                        && probe_side(probe_child, parts.sort_merge).is_some()
                        && side_leaf(build_child, parts.sort_merge).is_some()
                    {
                        let (probe_input, build_input) = if build_left {
                            (right_input, left_input)
                        } else {
                            (left_input, right_input)
                        };
                        found.push(MovableJoin {
                            path: path.clone(),
                            probe_input,
                            build_input,
                            build_left,
                        });
                    }
                }
            }
        }
        for (index, child) in op.children.iter().enumerate() {
            path.push(index);
            walk(child, path, leaves, found);
            path.pop();
        }
    }
    let mut found = vec![];
    walk(block, &mut vec![], &mut 0, &mut found);
    found
}

/// Whether `op` reads one of its stage's inputs. Comet's planner feeds them to these leaves in
/// the order a depth-first walk meets them.
fn is_input_leaf(op: &Operator) -> bool {
    matches!(
        op.op_struct,
        Some(OpStruct::Scan(_)) | Some(OpStruct::ShuffleScan(_))
    )
}

fn leaf_fields(op: &Operator) -> &[datafusion_comet_proto::spark_expression::DataType] {
    match &op.op_struct {
        Some(OpStruct::Scan(scan)) => &scan.fields,
        Some(OpStruct::ShuffleScan(scan)) => &scan.fields,
        _ => &[],
    }
}

fn node_at<'a>(block: &'a Operator, path: &[usize]) -> &'a Operator {
    path.iter().fold(block, |op, index| &op.children[*index])
}

/// A stage's block, or `None` for a stage that reads files, whose partitions each run their own
/// copy of it.
fn block_of(fragment: &CometFragmentExec) -> Result<Option<Operator>> {
    let block = &fragment.fragment().block;
    if block.is_empty() {
        return Ok(None);
    }
    Ok(Some(Operator::decode(block.as_slice()).map_err(|e| {
        internal_datafusion_err!("Cannot decode a stage's block: {e}")
    })?))
}

/// A stage input's exchange, when it is a hash shuffle of another stage's output.
fn hash_exchange(input: &Arc<dyn ExecutionPlan>) -> Option<(&ExchangeExec, &CometFragmentExec)> {
    let exchange = input.downcast_ref::<ExchangeExec>()?;
    if exchange.is_broadcast() || !matches!(exchange.partitioning(), Some(Partitioning::Hash(..))) {
        return None;
    }
    let producer = exchange.input().downcast_ref::<CometFragmentExec>()?;
    Some((exchange, producer))
}

/// A join's build side: the exchange that carries it to the consumer and the stage that produces
/// it, or no exchange for a stage the consumer reads whole and runs inline, as Ballista plans a
/// small broadcast that Spark chose.
struct BuildInput<'a> {
    exchange: Option<&'a ExchangeExec>,
    stage: &'a CometFragmentExec,
}

impl BuildInput<'_> {
    /// Whether every partition of the consumer reads it whole already.
    fn is_broadcast(&self) -> bool {
        self.exchange.is_none_or(|exchange| exchange.is_broadcast())
    }

    /// Whether it can be read: an exchange's stage has finished. An inline stage runs where it
    /// is read.
    fn is_ready(&self) -> bool {
        self.exchange
            .is_none_or(|exchange| exchange.shuffle_created())
    }
}

/// The build side `input` carries: a hash exchange, a broadcast exchange, or an inline stage read
/// whole, through any `CoalescePartitionsExec` that gathers it into one partition.
fn build_input(input: &Arc<dyn ExecutionPlan>) -> Option<BuildInput<'_>> {
    if let Some((exchange, stage)) = hash_exchange(input) {
        return Some(BuildInput {
            exchange: Some(exchange),
            stage,
        });
    }
    let exchange = input.downcast_ref::<ExchangeExec>();
    if exchange.is_some_and(|exchange| !exchange.is_broadcast()) {
        return None;
    }
    let mut producer = exchange.map_or(input, |exchange| exchange.input());
    while let Some(gathered) = producer.downcast_ref::<CoalescePartitionsExec>() {
        producer = gathered.input();
    }
    Some(BuildInput {
        exchange,
        stage: producer.downcast_ref::<CometFragmentExec>()?,
    })
}

fn ballista_config(config: &ConfigOptions) -> BallistaConfig {
    config
        .extensions
        .get::<BallistaConfig>()
        .cloned()
        .unwrap_or_default()
}

/// Holds back the probe side's stage of each [`MovableJoin`] whose build side Spark estimates
/// close enough to the broadcast threshold that measuring it could make it a broadcast, and
/// whose probe side Spark estimates large enough that not shuffling it would pay for the wait.
///
/// Spark's estimate of a filtered relation ignores the filter, so a build side under a selective
/// filter is often far smaller than its estimate.
///
/// It runs on every replan, so it holds only a probe side's stage that hasn't launched, and only
/// while the build side's stage hasn't finished. Once it has, [`BroadcastIntoProbe`] decides the
/// join, and a probe side's stage it releases is not held again.
#[derive(Debug)]
pub struct HoldProbeStages;

impl PhysicalOptimizerRule for HoldProbeStages {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = ballista_config(config);
        let threshold = config.broadcast_join_threshold_bytes() as u64;
        if threshold == 0 || !config.adaptive_join_enabled() {
            return Ok(plan);
        }
        let max_build =
            threshold.saturating_mul(config.stage_build_side_max_estimate_multiple() as u64);
        let min_probe_ratio = config.stage_build_side_min_probe_ratio() as u64;
        Ok(plan
            .transform_up(|node| {
                let Some(fragment) = node.downcast_ref::<CometFragmentExec>() else {
                    return Ok(Transformed::no(node));
                };
                let Some(block) = block_of(fragment)? else {
                    return Ok(Transformed::no(node));
                };
                let inputs = &fragment.children()[1..];
                let mut held = inputs
                    .iter()
                    .map(|input| Arc::clone(*input))
                    .collect::<Vec<_>>();
                let mut changed = false;
                // Each candidate with its estimates, the smallest build side first, so that an
                // inner sort-merge join, a candidate both ways round, holds the larger side.
                let mut candidates = vec![];
                for join in movable_joins(&block) {
                    let (Some((_, probe_stage)), Some((_, build_stage))) = (
                        inputs
                            .get(join.probe_input)
                            .copied()
                            .and_then(hash_exchange),
                        inputs
                            .get(join.build_input)
                            .copied()
                            .and_then(hash_exchange),
                    ) else {
                        continue;
                    };
                    let (Some(probe_bytes), Some(build_bytes)) = (
                        probe_stage.fragment().estimated_output_bytes,
                        build_stage.fragment().estimated_output_bytes,
                    ) else {
                        continue;
                    };
                    candidates.push((join, probe_bytes, build_bytes));
                }
                candidates.sort_by_key(|(_, _, build_bytes)| *build_bytes);
                let mut decided_paths: Vec<Vec<usize>> = vec![];
                for (join, probe_bytes, build_bytes) in candidates {
                    if decided_paths.contains(&join.path) {
                        continue;
                    }
                    let (Some((probe, _)), Some((build, _))) = (
                        held.get(join.probe_input).and_then(hash_exchange),
                        held.get(join.build_input).and_then(hash_exchange),
                    ) else {
                        continue;
                    };
                    // A join already held either way round is BroadcastIntoProbe's to decide.
                    if probe.is_inactive_stage() || build.is_inactive_stage() {
                        decided_paths.push(join.path.clone());
                        continue;
                    }
                    if probe.shuffle_created()
                        || build.shuffle_created()
                        || probe.stage_id().is_some()
                        || build_bytes > max_build
                        || probe_bytes < build_bytes.saturating_mul(min_probe_ratio)
                    {
                        continue;
                    }
                    info!(
                        "Holding back a join's probe side (estimated {probe_bytes} bytes) until \
                         its build side (estimated {build_bytes} bytes) is measured"
                    );
                    let probe = probe.with_inactive_stage(true);
                    held[join.probe_input] = Arc::new(probe);
                    decided_paths.push(join.path.clone());
                    changed = true;
                }
                if !changed {
                    return Ok(Transformed::no(node));
                }
                let children = std::iter::once(Arc::clone(fragment.children()[0]))
                    .chain(held)
                    .collect();
                Ok(Transformed::yes(with_children(&node, children)?))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "HoldProbeStages"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Decides each join whose probe side [`HoldProbeStages`] held back, once its build side's stage
/// has finished: a build side under the broadcast threshold is broadcast to the probe side's
/// stage, which runs the join, and otherwise the probe side's stage is released unchanged.
#[derive(Debug)]
pub struct BroadcastIntoProbe {
    plan_ids: Arc<AtomicUsize>,
}

impl PhysicalOptimizerRule for BroadcastIntoProbe {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = ballista_config(config);
        let threshold_bytes = config.broadcast_join_threshold_bytes();
        let threshold_rows = config.broadcast_join_threshold_rows();
        Ok(plan
            .transform_up(|node| {
                let mut node = node;
                let mut changed = false;
                // Each decision changes the block, so find the joins again after each.
                while let Some(decided) = self.decide_one(&node, threshold_bytes, threshold_rows)? {
                    node = decided;
                    changed = true;
                }
                Ok(if changed {
                    Transformed::yes(node)
                } else {
                    Transformed::no(node)
                })
            })?
            .data)
    }

    fn name(&self) -> &str {
        "BroadcastIntoProbe"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl BroadcastIntoProbe {
    /// `node` with one held join decided, or `None` when it has none whose build side is ready.
    fn decide_one(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        threshold_bytes: usize,
        threshold_rows: usize,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(fragment) = node.downcast_ref::<CometFragmentExec>() else {
            return Ok(None);
        };
        let Some(block) = block_of(fragment)? else {
            return Ok(None);
        };
        let inputs = &fragment.children()[1..];
        let joins = movable_joins(&block);
        debug!(
            "BroadcastIntoProbe: {} candidate joins over inputs {:?}",
            joins.len(),
            inputs.iter().map(|input| input.name()).collect::<Vec<_>>()
        );
        for join in joins {
            let (Some((probe, _)), Some(build)) = (
                inputs
                    .get(join.probe_input)
                    .copied()
                    .and_then(hash_exchange),
                inputs.get(join.build_input).copied().and_then(build_input),
            ) else {
                debug!(
                    "BroadcastIntoProbe: join at {:?}: probe input {} or build input {} is not \
                     one it can move",
                    join.path, join.probe_input, join.build_input
                );
                continue;
            };
            debug!(
                "BroadcastIntoProbe: join at {:?}: build ready {}, broadcast {}, probe held {}, \
                 launched {}",
                join.path,
                build.is_ready(),
                build.is_broadcast(),
                probe.is_inactive_stage(),
                probe.stage_id().is_some() || probe.shuffle_created()
            );
            if !build.is_ready() {
                continue;
            }
            // The probe side's exchange must still find its keys in the moved join's output.
            let join_op = node_at(&block, &join.path);
            let probe_child = &join_op.children[if join.build_left { 1 } else { 0 }];
            // When the probe side drops a key, nothing above it can rely on rows being placed by
            // it, unless the consumer meets them with another partitioned input.
            let keeps_keys = join_parts(join_op)
                .and_then(|parts| probe_side(probe_child, parts.sort_merge))
                .is_some_and(|side| {
                    moved_keys(probe, &side.columns).is_some() || only_partitioned_input(fragment)
                });
            // A join Spark already broadcast moves into its probe side's stage without being
            // measured, as Ballista plans a broadcast join inside its probe side's stage, when
            // it filters or looks up the probe side's rows rather than adding rows of its own:
            // the exchange then carries the join's output instead of all of the probe side. A
            // probe side's stage that is held for another join stays as it is, since moving this
            // one would release it.
            if build.is_broadcast() {
                let join_type = join_parts(node_at(&block, &join.path)).map(|p| p.join_type);
                let filters = join_type.is_some_and(|t| {
                    [JoinType::Inner, JoinType::LeftSemi, JoinType::LeftAnti]
                        .iter()
                        .any(|kind| *kind as i32 == t)
                });
                if !filters
                    || !keeps_keys
                    || probe.is_inactive_stage()
                    || probe.shuffle_created()
                    || probe.stage_id().is_some()
                {
                    continue;
                }
                info!("Moving a broadcast join into its probe side's stage");
                return Ok(Some(self.broadcast_into_probe(fragment, &block, &join)?));
            }
            let Some(build) = build.exchange else {
                continue;
            };
            // A probe side's stage that hasn't launched, because it waits for stages of its own,
            // can take the join too, held or not. Only a held one is released if the build side
            // measures large.
            let held = probe.is_inactive_stage();
            if !held && (probe.shuffle_created() || probe.stage_id().is_some()) {
                continue;
            }
            let stats = StatisticsContext::new().compute(build, &StatisticsArgs::new())?;
            let small = match (
                stats.total_byte_size.get_value(),
                stats.num_rows.get_value(),
            ) {
                (Some(bytes), _) => *bytes <= threshold_bytes,
                (None, Some(rows)) => *rows <= threshold_rows,
                (None, None) => false,
            };
            let small = small && keeps_keys;
            if !held && !small {
                continue;
            }
            info!(
                "A {} join's build side measured {:?} bytes and {:?} rows: {}",
                if held { "held" } else { "pending" },
                stats.total_byte_size,
                stats.num_rows,
                if small {
                    "broadcasting it to the probe side's stage"
                } else {
                    "shuffling the probe side"
                }
            );
            let decided = if small {
                self.broadcast_into_probe(fragment, &block, &join)?
            } else {
                let mut children = fragment
                    .children()
                    .into_iter()
                    .map(Arc::clone)
                    .collect::<Vec<_>>();
                children[1 + join.probe_input] = Arc::new(probe.with_inactive_stage(false));
                with_children(node, children)?
            };
            return Ok(Some(decided));
        }
        Ok(None)
    }

    /// `consumer` with `join` moved into the stage that produces its probe side, and its build
    /// side broadcast to that stage.
    fn broadcast_into_probe(
        &self,
        consumer: &CometFragmentExec,
        block: &Operator,
        join: &MovableJoin,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inputs = &consumer.children()[1..];
        let (probe_exchange, probe_stage) = inputs
            .get(join.probe_input)
            .copied()
            .and_then(hash_exchange)
            .ok_or_else(|| internal_datafusion_err!("The join's probe side is not an exchange"))?;
        let BuildInput {
            exchange: build,
            stage: build_stage,
        } = inputs
            .get(join.build_input)
            .copied()
            .and_then(build_input)
            .ok_or_else(|| internal_datafusion_err!("The join's build side is not an exchange"))?;
        let join_op = node_at(block, &join.path);
        let parts = join_parts(join_op)
            .ok_or_else(|| internal_datafusion_err!("The moved join is not a join"))?;
        let (probe_index, build_index) = if join.build_left { (1, 0) } else { (0, 1) };
        let probe = probe_side(&join_op.children[probe_index], parts.sort_merge)
            .ok_or_else(|| internal_datafusion_err!("The moved join's probe side is not a leaf"))?;
        let build_leaf = side_leaf(&join_op.children[build_index], parts.sort_merge)
            .ok_or_else(|| internal_datafusion_err!("The moved join's build side is not a leaf"))?;
        let probe_fields = probe
            .columns
            .iter()
            .map(|column| leaf_fields(probe.leaf)[*column].clone())
            .collect::<Vec<_>>();
        let build_fields = leaf_fields(build_leaf).to_vec();
        // Semi and anti joins, which are built on the right, output only the probe side.
        let with_build = outputs_right(parts.join_type);
        // The probe side's stage runs it as a hash join, whatever the consumer ran.
        let hash_join = match &join_op.op_struct {
            Some(OpStruct::HashJoin(hash_join)) => hash_join.clone(),
            _ => HashJoin {
                left_join_keys: parts.left_keys.to_vec(),
                right_join_keys: parts.right_keys.to_vec(),
                join_type: parts.join_type,
                condition: parts.condition.cloned(),
                build_side: if join.build_left {
                    BuildSide::BuildLeft
                } else {
                    BuildSide::BuildRight
                } as i32,
                ..Default::default()
            },
        };
        // Built on the left, the join outputs the build side's columns first, so a projection
        // puts the probe side's first, and the consumer puts them back.
        let probe_first: Vec<usize> = if join.build_left {
            (build_fields.len()..build_fields.len() + probe_fields.len())
                .chain(0..build_fields.len())
                .collect()
        } else {
            vec![]
        };
        let original_order: Vec<usize> = if join.build_left {
            (probe_fields.len()..probe_fields.len() + build_fields.len())
                .chain(0..probe_fields.len())
                .collect()
        } else {
            vec![]
        };
        let join_output: Vec<DataType> = if join.build_left {
            build_fields.iter().chain(&probe_fields).cloned().collect()
        } else if with_build {
            probe_fields.iter().chain(&build_fields).cloned().collect()
        } else {
            probe_fields.clone()
        };

        // The probe side's stage runs the join on its output, reading the build side whole as
        // an input. The join becomes the root of the stage's block, so it takes over the
        // block's pool of SQL texts.
        let into_probe = |root: Operator| -> Operator {
            let mut root = root;
            let pool = std::mem::take(&mut root.sql_text_pool);
            for link in probe.chain.iter().rev() {
                root = Operator {
                    children: vec![root],
                    ..(*link).clone()
                };
            }
            let children = if join.build_left {
                vec![build_leaf.clone(), root]
            } else {
                vec![root, build_leaf.clone()]
            };
            let mut moved = Operator {
                children,
                plan_id: join_op.plan_id,
                sql_text_pool: vec![],
                op_struct: Some(OpStruct::HashJoin(hash_join.clone())),
            };
            if join.build_left {
                moved = projection(moved, &probe_first, &join_output, join_op.plan_id);
            }
            moved.sql_text_pool = pool;
            moved
        };
        let probe_fragment = probe_stage.fragment();
        let block_of_probe = match block_of(probe_stage)? {
            Some(root) => into_probe(root).encode_to_vec(),
            None => vec![],
        };
        let descriptor = Arc::clone(probe_stage.children()[0]);
        let own_plans = partition_plans(&descriptor)?;
        let descriptor = if own_plans.is_empty() {
            descriptor
        } else {
            let plans = own_plans
                .iter()
                .map(|plan| {
                    let root = Operator::decode(plan.as_slice()).map_err(|e| {
                        internal_datafusion_err!("Cannot decode a partition's block: {e}")
                    })?;
                    Ok(into_probe(root).encode_to_vec())
                })
                .collect::<Result<Vec<_>>>()?;
            partition_descriptor(plans.len(), &plans)?
        };
        let mut output_schema = probe
            .columns
            .iter()
            .map(|column| probe_fragment.output_schema[*column].clone())
            .collect::<Vec<_>>();
        if with_build {
            // An outer join's build side is null where the probe side has no match.
            let outer = parts.join_type != JoinType::Inner as i32;
            output_schema.extend(build_stage.fragment().output_schema.iter().map(|field| {
                let mut field = field.clone();
                field.nullable |= outer;
                field
            }));
        }
        // The build side's leaf is the first input of a join built on the left, and the last
        // otherwise, as the leaves of the block read them.
        let build_read: Arc<dyn ExecutionPlan> = match build {
            Some(build) if !build.is_broadcast() => {
                Arc::new(build.to_broadcast(self.plan_ids.fetch_add(1, Ordering::Relaxed)))
            }
            _ => Arc::clone(inputs[join.build_input]),
        };
        let probe_inputs = probe_stage.children()[1..]
            .iter()
            .map(|child| Arc::clone(*child))
            .collect::<Vec<_>>();
        let mut children = vec![descriptor];
        let mut broadcast = vec![];
        if join.build_left {
            children.push(Arc::clone(&build_read));
            broadcast.push(true);
        }
        children.extend(probe_inputs);
        broadcast.extend(probe_fragment.broadcast.iter().copied());
        if !join.build_left {
            children.push(build_read);
            broadcast.push(true);
        }
        let joined = Fragment {
            block: block_of_probe,
            output_schema,
            broadcast,
            conf: probe_fragment.conf.clone(),
            // At most the probe side, so that a join above it can be held for it in turn.
            estimated_output_bytes: probe_fragment.estimated_output_bytes,
            // It feeds an exchange, so nothing merges its partitions.
            output_ordering: vec![],
        };
        let joined: Arc<dyn ExecutionPlan> =
            Arc::new(CometFragmentExec::try_new(Arc::new(joined), children)?);
        // The exchange partitions the joined rows on the same columns, wherever the probe side's
        // projections put them.
        // When the probe side drops a key, which only a consumer with no other partitioned input
        // allows, the rows may go anywhere: they are placed by the first column.
        let keys = match moved_keys(probe_exchange, &probe.columns) {
            Some(keys) => keys,
            None if only_partitioned_input(consumer) => vec![0],
            None => {
                return Err(internal_datafusion_err!(
                    "The moved join drops an exchange key"
                ))
            }
        };
        let exchange = if probe.columns.iter().copied().eq(0..probe.columns.len()) {
            with_children(inputs[join.probe_input], vec![joined])?
                .downcast_ref::<ExchangeExec>()
                .ok_or_else(|| internal_datafusion_err!("An exchange rebuilt as something else"))?
                .with_inactive_stage(false)
        } else {
            let Some(Partitioning::Hash(_, count)) = probe_exchange.partitioning() else {
                return Err(internal_datafusion_err!(
                    "The probe side's exchange is not a hash"
                ));
            };
            let schema = joined.schema();
            let keys = keys
                .iter()
                .map(|index| {
                    Arc::new(Column::new(schema.field(*index).name(), *index))
                        as Arc<dyn PhysicalExpr>
                })
                .collect();
            ExchangeExec::new(
                joined,
                Some(Partitioning::Hash(keys, *count)),
                probe_exchange.plan_id(),
            )
        };

        // The consumer reads the join's output where it read the join's probe side, and no
        // longer reads the build side. A sort-merge join's output was sorted on its keys, which
        // an operator above it may rely on, so the consumer sorts the join's output where one
        // does.
        let mut fields = probe_fields.clone();
        if with_build {
            fields.extend(build_fields.iter().cloned());
        }
        let mut replacement = Operator {
            children: vec![],
            plan_id: join_op.plan_id,
            sql_text_pool: vec![],
            op_struct: Some(OpStruct::ShuffleScan(ShuffleScan {
                fields,
                source: "a join moved into the probe side's stage".to_string(),
            })),
        };
        if join.build_left {
            let probe_first_output: Vec<DataType> =
                probe_fields.iter().chain(&build_fields).cloned().collect();
            replacement = projection(
                replacement,
                &original_order,
                &probe_first_output,
                join_op.plan_id,
            );
        }
        if let Some(OpStruct::SortMergeJoin(smj)) = &join_op.op_struct {
            if needs_order(
                block,
                &join.path,
                !consumer.fragment().output_ordering.is_empty(),
            ) {
                let sort_orders = if parts.join_type == JoinType::RightOuter as i32 {
                    // Sorted on its right keys, which follow the left side's columns.
                    let shift = if join.build_left {
                        build_fields.len()
                    } else {
                        probe_fields.len()
                    };
                    smj.sort_options
                        .iter()
                        .zip(&smj.right_join_keys)
                        .map(|(order, key)| with_sort_child(order, shifted(key, shift)))
                        .collect()
                } else {
                    smj.sort_options.clone()
                };
                replacement = Operator {
                    children: vec![replacement],
                    plan_id: join_op.plan_id,
                    sql_text_pool: vec![],
                    op_struct: Some(OpStruct::Sort(Sort {
                        sort_orders,
                        ..Default::default()
                    })),
                };
            }
        }
        if join.path.is_empty() {
            replacement.sql_text_pool = join_op.sql_text_pool.clone();
        }
        let mut consumer_block = block.clone();
        replace_at(&mut consumer_block, &join.path, replacement);
        let mut consumer_broadcast = consumer.fragment().broadcast.clone();
        consumer_broadcast.remove(join.build_input);
        let consumer_fragment = Fragment {
            block: consumer_block.encode_to_vec(),
            broadcast: consumer_broadcast,
            ..consumer.fragment().as_ref().clone()
        };
        let mut children = consumer
            .children()
            .into_iter()
            .map(Arc::clone)
            .collect::<Vec<_>>();
        children[1 + join.probe_input] = Arc::new(exchange);
        children.remove(1 + join.build_input);
        Ok(Arc::new(CometFragmentExec::try_new(
            Arc::new(consumer_fragment),
            children,
        )?))
    }
}

/// `child`'s columns at `columns`, of the types `types` lists for each column of `child`.
fn projection(child: Operator, columns: &[usize], types: &[DataType], plan_id: u32) -> Operator {
    Operator {
        children: vec![child],
        plan_id,
        sql_text_pool: vec![],
        op_struct: Some(OpStruct::Projection(Projection {
            project_list: columns
                .iter()
                .map(|index| bound_expr(*index, types[*index].clone()))
                .collect(),
        })),
    }
}

fn bound_expr(index: usize, datatype: DataType) -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::Bound(BoundReference {
            index: index as i32,
            datatype: Some(datatype),
        })),
        ..Default::default()
    }
}

/// `key`, a column, as the column `shift` places further right.
fn shifted(key: &Expr, shift: usize) -> Expr {
    let mut key = key.clone();
    if let Some(ExprStruct::Bound(bound)) = &mut key.expr_struct {
        bound.index += shift as i32;
    }
    key
}

/// `order`, a sort order, sorting on `child` instead.
fn with_sort_child(order: &Expr, child: Expr) -> Expr {
    let mut order = order.clone();
    if let Some(ExprStruct::SortOrder(sort_order)) = &mut order.expr_struct {
        sort_order.child = Some(Box::new(child));
    }
    order
}

/// Whether an operator above the join at `path` in `block` relies on the join's output being
/// sorted, as a sort-merge join's is. `merged` says whether the stage's sorted partitions are
/// merged, which relies on the block's output order.
fn needs_order(block: &Operator, path: &[usize], merged: bool) -> bool {
    for depth in (0..path.len()).rev() {
        match &node_at(block, &path[..depth]).op_struct {
            Some(OpStruct::Sort(_)) | Some(OpStruct::HashAgg(_)) => return false,
            Some(OpStruct::SortMergeJoin(_))
            | Some(OpStruct::Window(_))
            | Some(OpStruct::Limit(_))
            | Some(OpStruct::WindowGroupLimit(_)) => return true,
            Some(OpStruct::HashJoin(join)) if !join.streamed_sort_orders.is_empty() => return true,
            _ => {}
        }
    }
    merged
}

fn replace_at(block: &mut Operator, path: &[usize], replacement: Operator) {
    let target = path
        .iter()
        .fold(block, |op, index| &mut op.children[*index]);
    *target = replacement;
}

fn with_children(
    plan: &Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Arc::clone(plan).replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}
