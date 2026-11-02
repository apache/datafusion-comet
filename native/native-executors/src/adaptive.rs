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
//! known. [`HoldProbeStages`] holds it back before anything runs, where Spark's estimates say the
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
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions, StatisticsArgs,
};
use datafusion_comet_proto::spark_expression::expr::ExprStruct;
use datafusion_comet_proto::spark_native_executors::Fragment;
use datafusion_comet_proto::spark_operator::operator::OpStruct;
use datafusion_comet_proto::spark_operator::{BuildSide, JoinType, Operator, ShuffleScan};
use log::info;
use prost::Message;

/// The rules Comet adds to Ballista's adaptive planning.
#[derive(Debug, Default)]
pub struct CometAdaptiveRules;

impl AdaptiveRules for CometAdaptiveRules {
    fn preparation_rules(&self, _plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![Arc::new(HoldProbeStages)]
    }

    fn replan_rules(&self, plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![Arc::new(BroadcastIntoProbe { plan_ids })]
    }
}

/// A join in a stage's block that can run in its probe side's stage instead.
///
/// It is an inner hash join built on the right, with no condition and with keys that are
/// columns, whose inputs are both input leaves of the block. Moved below the probe side's
/// exchange its output keeps the probe side's columns first, so the exchange's keys still name
/// the same columns.
struct MovableJoin {
    /// The child indexes from the block's root to the join.
    path: Vec<usize>,
    /// The stage inputs that the join's probe (left) and build (right) leaves read.
    probe_input: usize,
    build_input: usize,
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
        if let Some(OpStruct::HashJoin(join)) = &op.op_struct {
            let movable = join.join_type == JoinType::Inner as i32
                && join.build_side == BuildSide::BuildRight as i32
                && join.condition.is_none()
                && !join.null_aware_anti_join
                && join
                    .left_join_keys
                    .iter()
                    .chain(&join.right_join_keys)
                    .all(|key| matches!(key.expr_struct, Some(ExprStruct::Bound(_))))
                && op.children.len() == 2
                && op.children.iter().all(is_input_leaf);
            if movable {
                found.push(MovableJoin {
                    path: path.clone(),
                    probe_input: *leaves,
                    build_input: *leaves + 1,
                });
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
                for join in movable_joins(&block) {
                    let (Some((probe, probe_stage)), Some((build, build_stage))) = (
                        hash_exchange(inputs[join.probe_input]),
                        hash_exchange(inputs[join.build_input]),
                    ) else {
                        continue;
                    };
                    let (Some(probe_bytes), Some(build_bytes)) = (
                        probe_stage.fragment().estimated_output_bytes,
                        build_stage.fragment().estimated_output_bytes,
                    ) else {
                        continue;
                    };
                    if probe.shuffle_created()
                        || build.shuffle_created()
                        || probe.is_inactive_stage()
                        || build_bytes > max_build
                        || probe_bytes < build_bytes.saturating_mul(min_probe_ratio)
                    {
                        continue;
                    }
                    info!(
                        "Holding back a join's probe side (estimated {probe_bytes} bytes) until \
                         its build side (estimated {build_bytes} bytes) is measured"
                    );
                    held[join.probe_input] = Arc::new(probe.with_inactive_stage(true));
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
        for join in movable_joins(&block) {
            let (Some((probe, _)), Some((build, _))) = (
                hash_exchange(inputs[join.probe_input]),
                hash_exchange(inputs[join.build_input]),
            ) else {
                continue;
            };
            if !probe.is_inactive_stage() || !build.shuffle_created() {
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
            info!(
                "A held join's build side measured {:?} bytes and {:?} rows: {}",
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
        let (_, probe_stage) = hash_exchange(inputs[join.probe_input])
            .ok_or_else(|| internal_datafusion_err!("The join's probe side is not an exchange"))?;
        let (build, build_stage) = hash_exchange(inputs[join.build_input])
            .ok_or_else(|| internal_datafusion_err!("The join's build side is not an exchange"))?;
        let join_op = node_at(block, &join.path);
        let build_leaf = &join_op.children[1];

        // The probe side's stage runs the join on its output, reading the build side whole as
        // its last input. The join becomes the root of the stage's block, so it takes over the
        // block's pool of SQL texts.
        let into_probe = |root: Operator| -> Operator {
            let mut root = root;
            let mut moved = join_op.clone();
            moved.sql_text_pool = std::mem::take(&mut root.sql_text_pool);
            moved.children = vec![root, build_leaf.clone()];
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
        let mut broadcast = probe_fragment.broadcast.clone();
        broadcast.push(true);
        let mut output_schema = probe_fragment.output_schema.clone();
        output_schema.extend(build_stage.fragment().output_schema.iter().cloned());
        let joined = Fragment {
            block: block_of_probe,
            output_schema,
            broadcast,
            conf: probe_fragment.conf.clone(),
            estimated_output_bytes: None,
        };
        let mut children = vec![descriptor];
        children.extend(
            probe_stage.children()[1..]
                .iter()
                .map(|child| Arc::clone(*child)),
        );
        children.push(Arc::new(
            build.to_broadcast(self.plan_ids.fetch_add(1, Ordering::Relaxed)),
        ));
        let joined: Arc<dyn ExecutionPlan> =
            Arc::new(CometFragmentExec::try_new(Arc::new(joined), children)?);
        let exchange = with_children(inputs[join.probe_input], vec![joined])?;
        let exchange = exchange
            .downcast_ref::<ExchangeExec>()
            .ok_or_else(|| internal_datafusion_err!("An exchange rebuilt as something else"))?
            .with_inactive_stage(false);

        // The consumer reads the join's output where it read the join's probe side, and no
        // longer reads the build side.
        let mut fields = leaf_fields(&join_op.children[0]).to_vec();
        fields.extend(leaf_fields(build_leaf).iter().cloned());
        let leaf = Operator {
            children: vec![],
            plan_id: join_op.plan_id,
            sql_text_pool: if join.path.is_empty() {
                join_op.sql_text_pool.clone()
            } else {
                vec![]
            },
            op_struct: Some(OpStruct::ShuffleScan(ShuffleScan {
                fields,
                source: "a join moved into the probe side's stage".to_string(),
            })),
        };
        let mut consumer_block = block.clone();
        replace_at(&mut consumer_block, &join.path, leaf);
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
