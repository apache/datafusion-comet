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

//! Stage-attempt scoped DataFusion trees, using unmodified upstream operators.
//! Inputs are task-local; each shared partition executes at most once.

use super::operators::{ExecutionError, ScanExec};
use super::planner::PhysicalPlanner;
use super::spark_plan::SparkPlan;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{internal_err, tree_node::TreeNodeRecursion, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_expression::{agg_expr, expr::ExprStruct, AggExpr, Expr};
use datafusion_comet_proto::spark_operator::{operator::OpStruct, Operator};
use futures::{Stream, StreamExt};
use jni::objects::{Global, JObject};
use parking_lot::Mutex;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::task::{Context, Poll};

// The registry owns only weak references: the last task drops the physical tree and metrics.
// An executor has no reliable stage-completion callback, so idle gaps deliberately end reuse.
static PHYSICAL_PLANS: LazyLock<ScopedPlans> = LazyLock::new(ScopedPlans::default);

#[derive(Default)]
struct ScopedPlans {
    entries: Mutex<HashMap<Vec<u8>, Weak<SharedPipeline>>>,
}

impl ScopedPlans {
    fn get_or_build(
        &self,
        key: &[u8],
        build: impl FnOnce() -> std::result::Result<Arc<SharedPipeline>, ExecutionError>,
    ) -> std::result::Result<Arc<SharedPipeline>, ExecutionError> {
        let mut entries = self.entries.lock();
        entries.retain(|_, plan| plan.strong_count() != 0);
        if let Some(plan) = entries.get(key).and_then(Weak::upgrade) {
            return Ok(plan);
        }
        // First construction is serialized; failures never become resident entries.
        let plan = build()?;
        if entries.len() < 64
            && key.len() <= 8 * 1024 * 1024
            && entries.keys().map(Vec::len).sum::<usize>() + key.len() <= 8 * 1024 * 1024
        {
            entries.insert(key.to_vec(), Arc::downgrade(&plan));
        }
        Ok(plan)
    }
}

pub(super) fn clear() {
    PHYSICAL_PLANS.entries.lock().clear();
}

/// JVM scope includes driver-generated block identity, stage ID and stage attempt.
pub(super) fn scoped_key(scope: &[u8], key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(8 + scope.len() + key.len());
    result.extend_from_slice(&(scope.len() as u64).to_le_bytes());
    result.extend_from_slice(scope);
    result.extend_from_slice(key);
    result
}

/// Length-prefix every field so distinct plans/configurations cannot alias. Configuration order
/// is immaterial. Partition index and attempt identity intentionally do not participate: admitted
/// expressions cannot depend on either. task_cpus also participates because it sets the session
/// target_partitions independently of serialized Spark config. Resources arrive through binding.
pub(super) fn cache_key(
    bytes: &[u8],
    config: &HashMap<String, String>,
    batch_size: i32,
    partition_count: i32,
    task_cpus: i64,
) -> Vec<u8> {
    fn append(key: &mut Vec<u8>, bytes: &[u8]) {
        key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        key.extend_from_slice(bytes);
    }
    let mut key = Vec::new();
    append(&mut key, bytes);
    key.extend_from_slice(&batch_size.to_le_bytes());
    key.extend_from_slice(&partition_count.to_le_bytes());
    key.extend_from_slice(&task_cpus.to_le_bytes());
    let mut entries: Vec<_> = config.iter().collect();
    entries.sort_unstable();
    for (name, value) in entries {
        append(&mut key, name.as_bytes());
        append(&mut key, value.as_bytes());
    }
    key
}

pub(super) fn cache_bytes<'a>(plan: &Operator, original: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
    fn has_files(plan: &Operator) -> bool {
        matches!(plan.op_struct, Some(OpStruct::NativeScan(_)))
            || plan.children.iter().any(has_files)
    }
    if has_files(plan) {
        std::borrow::Cow::Owned(template_bytes(plan))
    } else {
        std::borrow::Cow::Borrowed(original)
    }
}

/// Legacy file-list normalization. Native scans are currently rejected by admission,
/// so this does not expand the set of trees eligible for sharing.
pub(super) fn template_bytes(plan: &Operator) -> Vec<u8> {
    fn normalize(plan: &mut Operator) {
        if let Some(OpStruct::NativeScan(scan)) = plan.op_struct.as_mut() {
            scan.file_partition = None;
        }
        for child in &mut plan.children {
            normalize(child);
        }
    }
    let mut template = plan.clone();
    normalize(&mut template);
    template.encode_to_vec()
}

// Preserve the planner's input_plan push order: children are planned left-to-right, including
// parse_join_parameters. HashJoin may swap physical children afterwards; convert_tree maps each
// original input Arc to this pre-swap slot, so binding must keep the protobuf child order.
fn input_definitions<'a>(plan: &'a Operator, result: &mut Vec<&'a Operator>) {
    if matches!(
        plan.op_struct,
        Some(OpStruct::Scan(_) | OpStruct::NativeScan(_))
    ) {
        result.push(plan);
    } else {
        for child in &plan.children {
            input_definitions(child, result);
        }
    }
}

pub(super) fn get_or_build(
    key: &[u8],
    plan: &Operator,
    session: &Arc<SessionContext>,
    partition_count: usize,
) -> std::result::Result<Arc<SharedPipeline>, ExecutionError> {
    PHYSICAL_PLANS.get_or_build(key, || {
        SharedPipeline::build_partitions(plan, session, partition_count)
    })
}

pub(super) fn supports(plan: &Operator) -> bool {
    match plan.op_struct.as_ref() {
        Some(OpStruct::Scan(_)) => plan.children.is_empty(),
        Some(OpStruct::NativeScan(_)) => false,
        Some(OpStruct::Projection(project)) => {
            plan.children.len() == 1
                && project.project_list.iter().all(supports_expr)
                && supports(&plan.children[0])
        }
        Some(OpStruct::Filter(filter)) => {
            plan.children.len() == 1
                && filter.predicate.as_ref().is_some_and(supports_expr)
                && supports(&plan.children[0])
        }
        Some(OpStruct::HashJoin(join)) => {
            plan.children.len() == 2
                && !join.dynamic_filter_enabled
                && !join.null_aware_anti_join
                && join.left_join_keys.iter().all(supports_expr)
                && join.right_join_keys.iter().all(supports_expr)
                && join.condition.as_ref().is_none_or(supports_expr)
                && plan.children.iter().all(supports)
        }
        Some(OpStruct::Sort(sort)) => {
            plan.children.len() == 1
                && !sort.sort_orders.is_empty()
                && sort.fetch.is_none()
                && sort.skip.is_none_or(|n| n == 0)
                && sort.sort_orders.iter().all(supports_sort_order)
                && supports(&plan.children[0])
        }
        Some(OpStruct::HashAgg(agg)) => {
            plan.children.len() == 1
                && agg.grouping_exprs.iter().all(supports_expr)
                && agg.agg_exprs.iter().all(supports_aggregate)
                && supports(&plan.children[0])
        }
        _ => false,
    }
}

fn supports_sort_order(expr: &Expr) -> bool {
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::SortOrder(order)) => order.child.as_deref().is_some_and(supports_expr),
        _ => false,
    }
}

// DISTINCT is lowered by Spark to grouping/deduplication stages before serialization; AggExpr
// has no distinct flag. expr_modes changes how buffers are consumed, never which functions or
// child expressions are admitted here. The original planner supplies the merge definitions.
fn supports_aggregate(expr: &AggExpr) -> bool {
    use agg_expr::ExprStruct::*;
    expr.filter.as_ref().is_none_or(supports_expr)
        && match expr.expr_struct.as_ref() {
            Some(Count(e)) => !e.children.is_empty() && e.children.iter().all(supports_expr),
            Some(Sum(e)) => e.child.as_ref().is_some_and(supports_expr),
            Some(Avg(e)) => e.child.as_ref().is_some_and(supports_expr),
            Some(Min(e)) => e.child.as_ref().is_some_and(supports_expr),
            Some(Max(e)) => e.child.as_ref().is_some_and(supports_expr),
            _ => false,
        }
}

fn supports_expr(expr: &Expr) -> bool {
    match expr.expr_struct.as_ref() {
        Some(ExprStruct::Bound(_) | ExprStruct::Literal(_)) => true,
        Some(ExprStruct::Add(e) | ExprStruct::Subtract(e) | ExprStruct::Multiply(e)) => {
            e.left.as_deref().is_some_and(supports_expr)
                && e.right.as_deref().is_some_and(supports_expr)
        }
        Some(
            ExprStruct::Eq(e)
            | ExprStruct::Neq(e)
            | ExprStruct::Gt(e)
            | ExprStruct::GtEq(e)
            | ExprStruct::Lt(e)
            | ExprStruct::LtEq(e)
            | ExprStruct::And(e)
            | ExprStruct::Or(e),
        ) => {
            e.left.as_deref().is_some_and(supports_expr)
                && e.right.as_deref().is_some_and(supports_expr)
        }
        Some(ExprStruct::IsNull(e) | ExprStruct::IsNotNull(e) | ExprStruct::Not(e)) => {
            e.child.as_deref().is_some_and(supports_expr)
        }
        // In particular: RNGs, partition ID, subqueries, UDFs and unreviewed scalar functions.
        _ => false,
    }
}

#[derive(Debug)]
pub(super) struct SharedPipeline {
    pub root: Arc<SparkPlan>,
    scan_definitions: Vec<Operator>,
    identity: Arc<()>,
    partition_count: usize,
    claimed_partitions: Mutex<HashSet<usize>>,
}

type BoundAttempt = (Vec<ScanExec>, Arc<AttemptState>);

impl SharedPipeline {
    /// Never release a claim: upstream metrics persist until the tree is dropped.
    /// A repeated partition must execute on an ordinary private plan instead.
    fn try_claim_partition(&self, partition: usize) -> bool {
        partition < self.partition_count && self.claimed_partitions.lock().insert(partition)
    }

    #[cfg(test)]
    fn build(
        plan: &Operator,
        session: &Arc<SessionContext>,
    ) -> std::result::Result<Arc<Self>, ExecutionError> {
        Self::build_partitions(plan, session, 1)
    }

    fn build_partitions(
        plan: &Operator,
        session: &Arc<SessionContext>,
        partition_count: usize,
    ) -> std::result::Result<Arc<Self>, ExecutionError> {
        if partition_count == 0 || !supports(plan) {
            return Err(ExecutionError::GeneralError(
                "Unsupported shared native pipeline".into(),
            ));
        }
        // TEST_EXEC_CONTEXT_ID is the planner's default. No task inputs/context are imported.
        let input_plans = Arc::new(Mutex::new(Vec::new()));
        let planner = PhysicalPlanner::new(Arc::clone(session), 0)
            .with_sql_text_pool(plan)
            .with_input_plans(Arc::clone(&input_plans));
        let (_, _, original) = planner.create_plan(plan, &mut vec![], 1)?;
        let identity = Arc::new(());
        let mut mapping = Vec::new();
        convert_tree(
            &original.native_plan,
            &identity,
            &input_plans.lock(),
            &mut mapping,
            partition_count,
        )?;
        let root = convert_spark_tree(&original, &mapping)?;
        let mut definitions = Vec::new();
        input_definitions(plan, &mut definitions);
        let scan_definitions = definitions
            .into_iter()
            .map(|p| {
                let mut p = p.clone();
                if let Some(OpStruct::NativeScan(scan)) = p.op_struct.as_mut() {
                    scan.file_partition = None;
                }
                p
            })
            .collect();
        Ok(Arc::new(Self {
            root,
            scan_definitions,
            identity,
            partition_count,
            claimed_partitions: Mutex::new(HashSet::new()),
        }))
    }

    #[cfg(test)]
    fn bind(
        self: &Arc<Self>,
        planner: &PhysicalPlanner,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
    ) -> std::result::Result<(Vec<ScanExec>, Arc<AttemptState>), ExecutionError> {
        self.bind_definitions(
            planner,
            inputs,
            &self.scan_definitions.iter().collect::<Vec<_>>(),
            0,
        )
    }

    pub fn try_bind_plan(
        self: &Arc<Self>,
        planner: &PhysicalPlanner,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        task_plan: &Operator,
    ) -> std::result::Result<Option<BoundAttempt>, ExecutionError> {
        if !self.try_claim_partition(planner.partition() as usize) {
            return Ok(None);
        }
        self.bind_plan(planner, inputs, task_plan).map(Some)
    }

    fn bind_plan(
        self: &Arc<Self>,
        planner: &PhysicalPlanner,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        task_plan: &Operator,
    ) -> std::result::Result<(Vec<ScanExec>, Arc<AttemptState>), ExecutionError> {
        let mut definitions = Vec::new();
        input_definitions(task_plan, &mut definitions);
        self.bind_definitions(planner, inputs, &definitions, planner.partition() as usize)
    }

    fn bind_definitions(
        self: &Arc<Self>,
        planner: &PhysicalPlanner,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        definitions: &[&Operator],
        partition: usize,
    ) -> std::result::Result<(Vec<ScanExec>, Arc<AttemptState>), ExecutionError> {
        if partition >= self.partition_count || definitions.len() != self.scan_definitions.len() {
            return Err(ExecutionError::GeneralError(
                "Shared input binding count mismatch".into(),
            ));
        }
        let mut scans = Vec::new();
        let mut bound_inputs = Vec::new();
        for definition in definitions {
            let (jvm_scans, _, input) = planner.create_plan(definition, inputs, 1)?;
            scans.extend(jvm_scans);
            bound_inputs.push(Arc::clone(&input.native_plan));
        }
        let attempt = Arc::new(AttemptState {
            inputs: bound_inputs,
            _owner: Arc::clone(self),
            identity: Arc::clone(&self.identity),
            started: (0..definitions.len())
                .map(|_| AtomicBool::new(false))
                .collect(),
            partition,
        });
        Ok((scans, attempt))
    }
}

/// Owned by one Spark task attempt. The shared tree contains no input readers,
/// memory pools or TaskContext belonging to an attempt. Metrics are partition-labelled.
#[derive(Debug)]
pub(crate) struct AttemptState {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    _owner: Arc<SharedPipeline>,
    identity: Arc<()>,
    started: Vec<AtomicBool>,
    partition: usize,
}

impl AttemptState {
    pub fn partition(&self) -> usize {
        self.partition
    }

    pub fn task_context(self: &Arc<Self>, session: &SessionContext) -> Arc<TaskContext> {
        let context = TaskContext::from(session);
        let config = context
            .session_config()
            .clone()
            .with_extension(Arc::clone(self));
        Arc::new(context.with_session_config(config))
    }

    pub fn metrics_for(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<MetricsSet> {
        if let Some(input) = plan.downcast_ref::<SharedInputExec>() {
            if !Arc::ptr_eq(&self.identity, &input.identity) {
                return None;
            }
            self.inputs[input.index].metrics()
        } else {
            plan.metrics().map(|metrics| {
                let mut selected = MetricsSet::new();
                for metric in metrics
                    .iter()
                    .filter(|m| m.partition() == Some(self.partition))
                {
                    selected.push(Arc::clone(metric));
                }
                selected
            })
        }
    }
}

/// The only replacement node: obtain this attempt's reader through TaskContext.
/// All operators above it are real, shared DataFusion ExecutionPlan instances.
#[derive(Debug)]
struct SharedInputExec {
    index: usize,
    identity: Arc<()>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for SharedInputExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SharedInputExec: slot={}", self.index)
    }
}

impl ExecutionPlan for SharedInputExec {
    fn name(&self) -> &str {
        "SharedInputExec"
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Cannot add children to a shared input")
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let attempt = context
            .session_config()
            .get_extension::<AttemptState>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "Missing shared pipeline attempt binding".into(),
                )
            })?;
        if !Arc::ptr_eq(&self.identity, &attempt.identity) {
            return internal_err!("Shared pipeline attempt belongs to a different plan");
        }
        if partition != attempt.partition {
            return internal_err!("Shared input partition does not match task binding");
        }
        if attempt.started[self.index].swap(true, Ordering::AcqRel) {
            return internal_err!("Shared pipeline attempt was already executed");
        }
        let stream = attempt.inputs[self.index].execute(0, context)?;
        // Keep the binding alive across asynchronous polling without retaining it
        // in any shared operator. Cancellation drops the stream before its binding.
        Ok(Box::pin(BoundInputStream {
            stream,
            _attempt: attempt,
        }))
    }
}

struct BoundInputStream {
    stream: SendableRecordBatchStream,
    _attempt: Arc<AttemptState>,
}
impl Stream for BoundInputStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}
impl RecordBatchStream for BoundInputStream {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

type PlanMapping = Vec<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)>;

fn convert_tree(
    plan: &Arc<dyn ExecutionPlan>,
    identity: &Arc<()>,
    bound_inputs: &[Arc<dyn ExecutionPlan>],
    mapping: &mut PlanMapping,
    partition_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let shared: Arc<dyn ExecutionPlan> =
        if let Some(index) = bound_inputs.iter().position(|p| Arc::ptr_eq(p, plan)) {
            Arc::new(SharedInputExec {
                index,
                identity: Arc::clone(identity),
                properties: Arc::new(
                    plan.properties()
                        .as_ref()
                        .clone()
                        .with_partitioning(Partitioning::UnknownPartitioning(partition_count)),
                ),
            })
        } else {
            let children = plan
                .children()
                .into_iter()
                .map(|child| convert_tree(child, identity, bound_inputs, mapping, partition_count))
                .collect::<Result<Vec<_>>>()?;
            // Rebuild only during cache construction to install shared input leaves.
            // No constructor, reset_state or operator clone runs on task cache hits.
            let rebuilt = Arc::clone(plan).replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
            if let Some(sort) = rebuilt.downcast_ref::<SortExec>() {
                if sort.fetch().is_some() {
                    return internal_err!("Top-K is not admitted");
                }
                Arc::new(sort.clone().with_preserve_partitioning(true))
            } else if let Some(aggregate) = rebuilt.downcast_ref::<AggregateExec>() {
                if *aggregate.mode() == AggregateMode::Final && partition_count > 1 {
                    // Each Spark task consumes its own already-shuffled partition.
                    Arc::new(AggregateExec::try_new(
                        AggregateMode::FinalPartitioned,
                        aggregate.group_expr().clone(),
                        aggregate.aggr_expr().to_vec(),
                        aggregate.filter_expr().to_vec(),
                        Arc::clone(aggregate.input()),
                        aggregate.input_schema(),
                    )?)
                } else {
                    rebuilt
                }
            } else if let Some(join) = rebuilt.downcast_ref::<HashJoinExec>() {
                if *join.partition_mode() != PartitionMode::Partitioned {
                    return internal_err!("Only partitioned joins can share execution trees");
                }
                rebuilt
            } else if rebuilt.is::<ProjectionExec>() || rebuilt.is::<FilterExec>() {
                rebuilt
            } else {
                return internal_err!("Unexpected operator in shared tree: {}", rebuilt.name());
            }
        };
    mapping.push((Arc::clone(plan), Arc::clone(&shared)));
    Ok(shared)
}

fn convert_spark_tree(plan: &Arc<SparkPlan>, mapping: &PlanMapping) -> Result<Arc<SparkPlan>> {
    let lookup = |old: &Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
        mapping
            .iter()
            .find(|(from, _)| Arc::ptr_eq(from, old))
            .map(|(_, to)| Arc::clone(to))
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "Missing shared pipeline metric node".into(),
                )
            })
    };
    Ok(Arc::new(SparkPlan::new_with_additional(
        plan.plan_id,
        lookup(&plan.native_plan)?,
        plan.children
            .iter()
            .map(|c| convert_spark_tree(c, mapping))
            .collect::<Result<_>>()?,
        plan.additional_native_plans
            .iter()
            .map(lookup)
            .collect::<Result<_>>()?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::metrics::utils::to_native_metric_node_with;
    use crate::execution::operators::InputBatch;
    use arrow::array::Int64Array;
    use datafusion_comet_proto::spark_expression::{literal::Value, Literal};
    use datafusion_comet_proto::spark_expression::{
        BinaryExpr, BoundReference, DataType, EmptyExpr, MathExpr,
    };
    use datafusion_comet_proto::spark_operator::{Filter, Projection, Scan};
    use futures::FutureExt;
    use std::sync::{atomic::AtomicUsize, Barrier};

    fn datatype() -> DataType {
        DataType {
            type_id: 4,
            type_info: None,
        }
    }
    fn expr(kind: ExprStruct) -> Expr {
        Expr {
            expr_struct: Some(kind),
            ..Default::default()
        }
    }
    fn column() -> Expr {
        expr(ExprStruct::Bound(BoundReference {
            index: 0,
            datatype: Some(datatype()),
        }))
    }
    fn literal(value: i64) -> Expr {
        expr(ExprStruct::Literal(Literal {
            value: Some(Value::LongVal(value)),
            datatype: Some(datatype()),
            is_null: false,
        }))
    }
    fn pipeline() -> Operator {
        Operator {
            plan_id: 3,
            op_struct: Some(OpStruct::Projection(Projection {
                project_list: vec![expr(ExprStruct::Add(Box::new(MathExpr {
                    left: Some(Box::new(column())),
                    right: Some(Box::new(literal(10))),
                    return_type: Some(datatype()),
                    ..Default::default()
                })))],
            })),
            children: vec![Operator {
                plan_id: 2,
                op_struct: Some(OpStruct::Filter(Filter {
                    predicate: Some(expr(ExprStruct::Gt(Box::new(BinaryExpr {
                        left: Some(Box::new(column())),
                        right: Some(Box::new(literal(0))),
                    })))),
                })),
                children: vec![Operator {
                    plan_id: 1,
                    op_struct: Some(OpStruct::Scan(Scan {
                        fields: vec![datatype()],
                        source: "shared-test".into(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }
    fn feed(scan: &mut ScanExec, values: Vec<Option<i64>>) {
        let len = values.len();
        scan.set_input_batch(InputBatch::Batch(
            vec![Arc::new(Int64Array::from(values))],
            len,
        ));
    }
    fn next(stream: &mut SendableRecordBatchStream) -> Vec<i64> {
        let batch = stream
            .next()
            .now_or_never()
            .expect("must not wait for another task")
            .unwrap()
            .unwrap();
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn concurrent_first_touch_builds_one_physical_tree_without_retaining_sessions() {
        let cache = Arc::new(ScopedPlans::default());
        let builds = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (cache, builds, barrier) = (
                    Arc::clone(&cache),
                    Arc::clone(&builds),
                    Arc::clone(&barrier),
                );
                std::thread::spawn(move || {
                    let session = Arc::new(SessionContext::new());
                    let weak = Arc::downgrade(&session);
                    barrier.wait();
                    let shared = cache
                        .get_or_build(b"same", || {
                            builds.fetch_add(1, Ordering::SeqCst);
                            SharedPipeline::build(&pipeline(), &session)
                        })
                        .unwrap();
                    drop(session);
                    assert!(weak.upgrade().is_none());
                    shared
                })
            })
            .collect();
        let plans: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        for plan in &plans {
            assert!(Arc::ptr_eq(
                &plans[0].root.native_plan,
                &plan.root.native_plan
            ));
        }
    }

    #[tokio::test]
    async fn interleaved_attempts_cancellation_retry_and_metrics_are_isolated() {
        let session = Arc::new(SessionContext::new());
        let shared = SharedPipeline::build(&pipeline(), &session).unwrap();
        let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
        let (mut scans_a, a) = shared.bind(&planner, &mut vec![]).unwrap();
        let retry_b = SharedPipeline::build(&pipeline(), &session).unwrap();
        let (mut scans_b, b) = retry_b.bind(&planner, &mut vec![]).unwrap();
        let mut stream_a = shared
            .root
            .native_plan
            .execute(0, a.task_context(&session))
            .unwrap();
        let stream_b = retry_b
            .root
            .native_plan
            .execute(0, b.task_context(&session))
            .unwrap();
        feed(&mut scans_a[0], vec![None, Some(-1), Some(1), Some(2)]);
        feed(&mut scans_b[0], vec![Some(8)]);
        assert!(stream_a.next().now_or_never().is_none());
        let weak = Arc::downgrade(&a);
        drop(stream_a);
        drop(scans_a);
        drop(a);
        assert!(weak.upgrade().is_none());
        let retry_c = SharedPipeline::build(&pipeline(), &session).unwrap();
        let (mut scans_c, c) = retry_c.bind(&planner, &mut vec![]).unwrap();
        let stream_c = retry_c
            .root
            .native_plan
            .execute(0, c.task_context(&session))
            .unwrap();
        feed(&mut scans_c[0], vec![Some(30), Some(31)]);
        let (out_b, out_c) = tokio::join!(
            drain_inputs(scans_b, stream_b),
            drain_inputs(scans_c, stream_c)
        );
        assert_eq!(rows(&out_b), vec![vec![Some(18)]]);
        assert_eq!(rows(&out_c), vec![vec![Some(40)], vec![Some(41)]]);
        assert_eq!(
            b.metrics_for(&retry_b.root.native_plan)
                .unwrap()
                .output_rows(),
            Some(1)
        );
        assert_eq!(
            c.metrics_for(&retry_c.root.native_plan)
                .unwrap()
                .output_rows(),
            Some(2)
        );
    }

    #[test]
    fn empty_and_null_batches_and_zero_column_projection_preserve_rows() {
        let session = Arc::new(SessionContext::new());
        let mut definition = pipeline();
        let Some(OpStruct::Projection(project)) = definition.op_struct.as_mut() else {
            unreachable!()
        };
        project.project_list.clear();
        let shared = SharedPipeline::build(&definition, &session).unwrap();
        let planner = PhysicalPlanner::new(Arc::clone(&session), 99);
        let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
        let mut stream = shared
            .root
            .native_plan
            .execute(0, attempt.task_context(&session))
            .unwrap();
        for batch in [vec![], vec![None, Some(-1)]] {
            feed(&mut scans[0], batch);
            assert!(stream.next().now_or_never().is_none());
        }
        feed(&mut scans[0], vec![Some(2), Some(3)]);
        assert!(stream.next().now_or_never().is_none());
        scans[0].set_input_batch(InputBatch::EOF);
        let batch = stream.next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!((batch.num_columns(), batch.num_rows()), (0, 2));
        scans[0].set_input_batch(InputBatch::EOF);
        assert!(stream.next().now_or_never().unwrap().is_none());
    }

    #[test]
    fn bindings_reject_wrong_tree_missing_context_and_duplicate_execution() {
        let session = Arc::new(SessionContext::new());
        let shared = SharedPipeline::build(&pipeline(), &session).unwrap();
        let other = SharedPipeline::build(&pipeline(), &session).unwrap();
        let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
        let (_, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
        assert!(shared
            .root
            .native_plan
            .execute(0, session.task_ctx())
            .is_err());
        assert!(shared
            .root
            .native_plan
            .execute(1, attempt.task_context(&session))
            .is_err());
        assert!(other
            .root
            .native_plan
            .execute(0, attempt.task_context(&session))
            .is_err());
        let _stream = shared
            .root
            .native_plan
            .execute(0, attempt.task_context(&session))
            .unwrap();
        assert!(shared
            .root
            .native_plan
            .execute(0, attempt.task_context(&session))
            .is_err());
    }

    #[test]
    fn admission_is_recursive_and_configuration_is_part_of_identity() {
        assert!(supports(&pipeline()));
        for stateful in [
            ExprStruct::SparkPartitionId(EmptyExpr {}),
            ExprStruct::MonotonicallyIncreasingId(EmptyExpr {}),
        ] {
            let mut plan = pipeline();
            let Some(OpStruct::Projection(project)) = plan.op_struct.as_mut() else {
                unreachable!()
            };
            let Some(ExprStruct::Add(add)) = project.project_list[0].expr_struct.as_mut() else {
                unreachable!()
            };
            add.left = Some(Box::new(expr(stateful)));
            assert!(!supports(&plan));
        }
        let mut plan = pipeline();
        plan.children.push(plan.children[0].clone());
        assert!(!supports(&plan));
        assert!(!supports(&Operator::default()));
        let config: HashMap<String, String> = HashMap::from([
            ("timezone".into(), "UTC".into()),
            ("ansi".into(), "true".into()),
        ]);
        let reversed = config.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let key = cache_key(b"plan", &config, 100, 8, 1);
        assert_eq!(key, cache_key(b"plan", &reversed, 100, 8, 1));
        assert_ne!(key, cache_key(b"plan", &config, 101, 8, 1));
        assert_ne!(key, cache_key(b"plan", &config, 100, 9, 1));
        assert_ne!(key, cache_key(b"plan", &config, 100, 8, 2));
        assert_ne!(key, cache_key(b"other", &config, 100, 8, 1));
        assert_ne!(key, cache_key(b"plan", &HashMap::new(), 100, 8, 1));
    }
    #[test]
    fn stage_scope_and_partition_claims_isolate_retries() {
        let cache = ScopedPlans::default();
        let session = Arc::new(SessionContext::new());
        let build = || SharedPipeline::build_partitions(&pipeline(), &session, 4);
        let key = scoped_key(b"block-a:stage-1:attempt-0", b"plan");
        let first = cache.get_or_build(&key, build).unwrap();
        let same = cache
            .get_or_build(&key, || panic!("same active scope"))
            .unwrap();
        assert!(Arc::ptr_eq(&first, &same));
        assert!(first.try_claim_partition(0));
        assert!(same.try_claim_partition(2));
        assert!(!same.try_claim_partition(0));
        assert!(!same.try_claim_partition(4));
        for scope in [
            b"block-a:stage-1:attempt-1".as_slice(),
            b"block-b:stage-1:attempt-0",
            b"block-a:stage-2:attempt-0",
        ] {
            let other = cache
                .get_or_build(&scoped_key(scope, b"plan"), build)
                .unwrap();
            assert!(!Arc::ptr_eq(&first, &other));
            assert!(other.try_claim_partition(0));
        }
        let planner = PhysicalPlanner::new(Arc::clone(&session), 1);
        assert!(first
            .try_bind_plan(&planner, &mut vec![], &pipeline())
            .unwrap()
            .is_some());
        assert!(same
            .try_bind_plan(&planner, &mut vec![], &pipeline())
            .unwrap()
            .is_none());
        // Even an overlapping retry may execute privately without affecting the first tree.
        let private = build().unwrap();
        assert!(!Arc::ptr_eq(
            &first.root.native_plan,
            &private.root.native_plan
        ));
        assert!(private.try_claim_partition(0));
    }

    #[test]
    fn collapsed_spark_nodes_do_not_double_count_output_rows() {
        let session = Arc::new(SessionContext::new());
        let mut definition = pipeline();
        definition.plan_id = definition.children[0].plan_id;
        let shared = SharedPipeline::build(&definition, &session).unwrap();
        assert_eq!(shared.root.additional_native_plans.len(), 1);
        assert_eq!(shared.root.children[0].plan_id, 1);
        let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
        let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
        let mut stream = shared
            .root
            .native_plan
            .execute(0, attempt.task_context(&session))
            .unwrap();
        feed(&mut scans[0], vec![Some(-1), Some(2), Some(3)]);
        assert!(stream.next().now_or_never().is_none());
        scans[0].set_input_batch(InputBatch::EOF);
        assert_eq!(next(&mut stream), vec![12, 13]);
        let metrics =
            to_native_metric_node_with(&shared.root, &|p| attempt.metrics_for(p)).unwrap();
        assert_eq!(metrics.metrics["output_rows"], 2);
        assert_eq!(metrics.metrics["selectivity_total"], 3);
        assert_eq!(metrics.children[0].metrics["output_rows"], 3);
    }
    fn scan() -> Operator {
        pipeline().children[0].children[0].clone()
    }
    fn sort_plan(fetch: Option<i32>, skip: Option<i32>) -> Operator {
        use datafusion_comet_proto::spark_expression::SortOrder;
        use datafusion_comet_proto::spark_operator::Sort;
        Operator {
            plan_id: 10,
            children: vec![scan()],
            op_struct: Some(OpStruct::Sort(Sort {
                sort_orders: vec![expr(ExprStruct::SortOrder(Box::new(SortOrder {
                    child: Some(Box::new(column())),
                    direction: 0,
                    null_ordering: 1,
                })))],
                fetch,
                skip,
            })),
            ..Default::default()
        }
    }
    fn aggregate_plan() -> Operator {
        use datafusion_comet_proto::spark_expression::{Count, Sum};
        use datafusion_comet_proto::spark_operator::HashAggregate;
        Operator {
            plan_id: 11,
            children: vec![scan()],
            op_struct: Some(OpStruct::HashAgg(HashAggregate {
                grouping_exprs: vec![column()],
                agg_exprs: vec![
                    AggExpr {
                        expr_struct: Some(agg_expr::ExprStruct::Count(Count {
                            children: vec![column()],
                        })),
                        ..Default::default()
                    },
                    AggExpr {
                        expr_struct: Some(agg_expr::ExprStruct::Sum(Sum {
                            child: Some(column()),
                            datatype: Some(datatype()),
                            eval_mode: 0,
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })),
            ..Default::default()
        }
    }
    fn join_plan(build_side: i32, join_type: i32) -> Operator {
        use datafusion_comet_proto::spark_operator::HashJoin;
        Operator {
            plan_id: 12,
            children: vec![
                scan(),
                Operator {
                    plan_id: 2,
                    ..scan()
                },
            ],
            op_struct: Some(OpStruct::HashJoin(HashJoin {
                left_join_keys: vec![column()],
                right_join_keys: vec![column()],
                join_type,
                build_side,
                ..Default::default()
            })),
            ..Default::default()
        }
    }
    fn rows(batches: &[RecordBatch]) -> Vec<Vec<Option<i64>>> {
        use arrow::array::Array;
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|c| {
                            let a = c.as_any().downcast_ref::<Int64Array>().unwrap();
                            (!a.is_null(row)).then(|| a.value(row))
                        })
                        .collect(),
                );
            }
        }
        rows.sort();
        rows
    }
    async fn drain_inputs(
        mut scans: Vec<ScanExec>,
        mut stream: SendableRecordBatchStream,
    ) -> Vec<RecordBatch> {
        // Mimic JNI's pull-on-Pending driver: mocked ScanStream does not wake a task when
        // its slot is filled. Feed each consumed input independently, including build-right.
        let mut ended = vec![false; scans.len()];
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let mut batches = Vec::new();
            loop {
                let next = futures::future::poll_fn(|cx| match stream.poll_next_unpin(cx) {
                    Poll::Pending => {
                        for (i, scan) in scans.iter_mut().enumerate() {
                            if !ended[i] && scan.batch.lock().unwrap().is_none() {
                                scan.set_input_batch(InputBatch::EOF);
                                ended[i] = true;
                            }
                        }
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    ready => ready,
                })
                .await;
                match next {
                    Some(batch) => batches.push(batch.unwrap()),
                    None => break,
                }
            }
            batches
        })
        .await;
        result.expect("attempt must finish without other partitions")
    }

    #[tokio::test]
    async fn full_sort_and_cancellation_have_private_state() {
        for (fetch, skip) in [(None, None)] {
            let session = Arc::new(SessionContext::new());
            let shared = SharedPipeline::build(&sort_plan(fetch, skip), &session).unwrap();
            let planner = PhysicalPlanner::new(Arc::clone(&session), 7);
            // Cancel with input buffered, before EOF. A later attempt must not inherit TopK's
            // threshold, memory reservations, completion state or metrics.
            let (mut scans, cancelled) = shared.bind(&planner, &mut vec![]).unwrap();
            feed(&mut scans[0], vec![Some(-100), Some(-200)]);
            let mut stream = shared
                .root
                .native_plan
                .execute(0, cancelled.task_context(&session))
                .unwrap();
            assert!(stream.next().now_or_never().is_none());
            let weak = Arc::downgrade(&cancelled);
            drop(stream);
            drop(cancelled);
            drop(scans);
            assert!(weak.upgrade().is_none());
            assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
            for values in [
                vec![Some(9), Some(3), Some(7), None],
                vec![Some(100), Some(200)],
            ] {
                let shared = SharedPipeline::build(&sort_plan(fetch, skip), &session).unwrap();
                let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
                feed(&mut scans[0], values.clone());
                let stream = shared
                    .root
                    .native_plan
                    .execute(0, attempt.task_context(&session))
                    .unwrap();
                let batches = drain_inputs(scans, stream).await;
                let mut expected = values;
                expected.sort_by_key(|v| (v.is_none(), *v));
                if let Some(fetch) = fetch {
                    expected.truncate(fetch as usize);
                }
                let expected: Vec<_> = expected
                    .into_iter()
                    .skip(skip.unwrap_or(0) as usize)
                    .collect();
                let actual: Vec<_> = batches
                    .iter()
                    .flat_map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                    })
                    .collect();
                assert_eq!(actual, expected);
                assert_eq!(
                    attempt
                        .metrics_for(&shared.root.native_plan)
                        .unwrap()
                        .output_rows(),
                    Some(actual.len())
                );
                assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
            }
        }
    }

    #[test]
    fn aggregate_modes_do_not_bypass_function_or_expression_admission() {
        use datafusion_comet_proto::spark_expression::First;
        // DISTINCT rewrites can emit uniform stages or mixed Partial/PartialMerge stages.
        for (mode, expr_modes) in [(0, vec![]), (1, vec![]), (2, vec![2, 2]), (0, vec![0, 2])] {
            let mut definition = aggregate_plan();
            let Some(OpStruct::HashAgg(agg)) = definition.op_struct.as_mut() else {
                unreachable!()
            };
            agg.mode = mode;
            agg.expr_modes = expr_modes;
            assert!(supports(&definition));
            let mut unsupported_function = definition.clone();
            let Some(OpStruct::HashAgg(agg)) = unsupported_function.op_struct.as_mut() else {
                unreachable!()
            };
            agg.agg_exprs[0].expr_struct = Some(agg_expr::ExprStruct::First(First {
                child: Some(column()),
                datatype: Some(datatype()),
                ignore_nulls: false,
            }));
            assert!(!supports(&unsupported_function));
            let Some(OpStruct::HashAgg(agg)) = definition.op_struct.as_mut() else {
                unreachable!()
            };
            agg.agg_exprs[0].filter = Some(expr(ExprStruct::SparkPartitionId(EmptyExpr {})));
            assert!(!supports(&definition));
        }
    }

    // Historical audit showing why repeated partition execution accumulates metrics.
    // Production bindings prohibit this reuse; the audit deliberately bypasses that guard.
    #[derive(Debug)]
    struct AuditInput {
        index: usize,
        identity: Arc<()>,
        properties: Arc<PlanProperties>,
    }

    impl DisplayAs for AuditInput {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "AuditInput({})", self.index)
        }
    }

    impl ExecutionPlan for AuditInput {
        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn name(&self) -> &str {
            "AuditInput"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert!(children.is_empty());
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            assert!(partition < 2);
            let attempt = context
                .session_config()
                .get_extension::<AttemptState>()
                .unwrap();
            assert!(Arc::ptr_eq(&self.identity, &attempt.identity));
            // The DataFusion partition selects the execution lane, whereas the
            // task's JVM input still has exactly one local partition.
            attempt.inputs[self.index].execute(0, context)
        }
    }

    fn audit_tree(
        plan: &Arc<dyn ExecutionPlan>,
        inputs: &[Arc<dyn ExecutionPlan>],
        identity: &Arc<()>,
    ) -> Arc<dyn ExecutionPlan> {
        if let Some(index) = inputs.iter().position(|p| Arc::ptr_eq(p, plan)) {
            return Arc::new(AuditInput {
                index,
                identity: Arc::clone(identity),
                properties: Arc::new(plan.properties().as_ref().clone().with_partitioning(
                    datafusion::physical_plan::Partitioning::UnknownPartitioning(2),
                )),
            });
        }
        let children = plan
            .children()
            .into_iter()
            .map(|p| audit_tree(p, inputs, identity))
            .collect();
        let rewritten = Arc::clone(plan)
            .replace_children(
                children,
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )
            .unwrap();
        if let Some(sort) = rewritten.downcast_ref::<SortExec>() {
            assert!(sort.fetch().is_none());
            // Spark already defines each task's sort input; this is a local sort
            // on each lane, not a new executor-wide global sort.
            Arc::new(sort.clone().with_preserve_partitioning(true))
        } else {
            rewritten
        }
    }

    fn audit_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
        assert!(!plan.is::<SharedInputExec>());
        let mut nodes = vec![Arc::clone(plan)];
        for child in plan.children() {
            nodes.extend(audit_nodes(child));
        }
        nodes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actual_datafusion_tree_is_reused_across_partitions_and_attempt_waves() {
        let mut global = aggregate_plan();
        if let Some(OpStruct::HashAgg(agg)) = &mut global.op_struct {
            agg.grouping_exprs.clear();
            // Wide aggregation: compiled expressions and AggregateExec itself
            // must both survive all executions of the shared root.
            agg.agg_exprs = vec![agg.agg_exprs[1].clone(); 64];
        }
        let mut cases = vec![pipeline(), aggregate_plan(), global, sort_plan(None, None)];
        for side in 0..=1 {
            for kind in 0..=5 {
                cases.push(join_plan(side, kind));
            }
        }
        for definition in cases {
            let session = Arc::new(SessionContext::new());
            let bindings = SharedPipeline::build(&definition, &session).unwrap();
            let input_plans = Arc::new(Mutex::new(Vec::new()));
            let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
            let builder = PhysicalPlanner::new(Arc::clone(&session), 0)
                .with_input_plans(Arc::clone(&input_plans));
            let (_, _, original) = builder.create_plan(&definition, &mut vec![], 1).unwrap();
            let root = audit_tree(
                &original.native_plan,
                &input_plans.lock(),
                &bindings.identity,
            );
            assert!(!root.is::<SharedInputExec>());
            if matches!(definition.op_struct, Some(OpStruct::HashAgg(_))) {
                assert!(root.is::<AggregateExec>());
            }
            let original_nodes = audit_nodes(&root);
            let mut cumulative_rows = [0; 2];
            let mut previous_metric_count = 0;
            for wave in 0..2 {
                let start = Arc::new(tokio::sync::Barrier::new(2));
                let mut executions = Vec::new();
                for partition in 0..2 {
                    let (mut scans, attempt) = bindings.bind(&planner, &mut vec![]).unwrap();
                    let (mut reference_scans, _, reference) =
                        planner.create_plan(&definition, &mut vec![], 1).unwrap();
                    for (i, scan) in scans.iter_mut().enumerate() {
                        let base = 100 * wave + 10 * partition as i64;
                        let values = vec![None, Some(base + i as i64), Some(base + 1)];
                        feed(scan, values.clone());
                        feed(&mut reference_scans[i], values);
                    }
                    let stream = root
                        .execute(partition, attempt.task_context(&session))
                        .unwrap();
                    let reference_stream = reference
                        .native_plan
                        .execute(0, session.task_ctx())
                        .unwrap();
                    let start = Arc::clone(&start);
                    executions.push(tokio::spawn(async move {
                        start.wait().await;
                        let (actual, expected) = tokio::join!(
                            drain_inputs(scans, stream),
                            drain_inputs(reference_scans, reference_stream)
                        );
                        assert_eq!(rows(&actual), rows(&expected));
                        let weak = Arc::downgrade(&attempt);
                        drop(attempt);
                        assert!(weak.upgrade().is_none());
                        actual.iter().map(RecordBatch::num_rows).sum::<usize>()
                    }));
                }
                for (partition, execution) in executions.into_iter().enumerate() {
                    cumulative_rows[partition] += execution.await.unwrap();
                }
                let current_nodes = audit_nodes(&root);
                assert_eq!(current_nodes.len(), original_nodes.len());
                for (original, current) in original_nodes.iter().zip(&current_nodes) {
                    assert!(Arc::ptr_eq(original, current));
                }
                let metrics = root.metrics().unwrap();
                for (partition, expected) in cumulative_rows.iter().enumerate() {
                    let mut selected = MetricsSet::new();
                    for metric in metrics.iter().filter(|m| m.partition() == Some(partition)) {
                        selected.push(Arc::clone(metric));
                    }
                    assert_eq!(selected.output_rows(), Some(*expected));
                }
                // Demonstrate the remaining integration gap: reusing a partition
                // appends metrics, even after its stream and attempt are gone.
                let metric_count = metrics.iter().count();
                assert!(metric_count > previous_metric_count);
                previous_metric_count = metric_count;
                assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
            }
        }
    }

    fn physical_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
        let mut nodes = vec![Arc::clone(plan)];
        for child in plan.children() {
            nodes.extend(physical_nodes(child));
        }
        nodes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_tree_shares_real_nodes_with_partition_metrics() {
        let mut wide = aggregate_plan();
        if let Some(OpStruct::HashAgg(a)) = &mut wide.op_struct {
            a.grouping_exprs.clear();
            a.agg_exprs = vec![a.agg_exprs[1].clone(); 64];
        }
        let mut definitions = vec![pipeline(), aggregate_plan(), wide, sort_plan(None, None)];
        for side in 0..=1 {
            for kind in 0..=5 {
                definitions.push(join_plan(side, kind));
            }
        }
        for definition in definitions {
            let session = Arc::new(SessionContext::new());
            let shared = SharedPipeline::build_partitions(&definition, &session, 4).unwrap();
            let original_nodes = physical_nodes(&shared.root.native_plan);
            if matches!(definition.op_struct, Some(OpStruct::HashAgg(_))) {
                assert!(shared.root.native_plan.is::<AggregateExec>());
            }
            // Production claims each partition once. Retry/repeated-partition behavior
            // is covered by stage_scope_and_partition_claims_isolate_retries.
            let mut executions = Vec::new();
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            // Partitions 1 and 3 never execute here; operators must not wait for them.
            for (attempt_no, partition) in [0, 2].into_iter().enumerate() {
                let planner = PhysicalPlanner::new(Arc::clone(&session), partition);
                let (mut scans, attempt) = shared
                    .try_bind_plan(&planner, &mut vec![], &definition)
                    .unwrap()
                    .unwrap();
                let (mut private_scans, _, private) =
                    planner.create_plan(&definition, &mut vec![], 1).unwrap();
                for (index, scan) in scans.iter_mut().enumerate() {
                    let base = attempt_no as i64 * 10;
                    let values = vec![None, Some(base + index as i64), Some(base + 1)];
                    feed(scan, values.clone());
                    feed(&mut private_scans[index], values);
                }
                let stream = shared
                    .root
                    .native_plan
                    .execute(partition as usize, attempt.task_context(&session))
                    .unwrap();
                let private_stream = private.native_plan.execute(0, session.task_ctx()).unwrap();
                let root = Arc::clone(&shared.root.native_plan);
                let barrier = Arc::clone(&barrier);
                executions.push(tokio::spawn(async move {
                    barrier.wait().await;
                    let (actual, expected) = tokio::join!(
                        drain_inputs(scans, stream),
                        drain_inputs(private_scans, private_stream)
                    );
                    assert_eq!(rows(&actual), rows(&expected));
                    assert_eq!(
                        attempt.metrics_for(&root).unwrap().output_rows(),
                        Some(actual.iter().map(RecordBatch::num_rows).sum())
                    );
                    let weak = Arc::downgrade(&attempt);
                    drop(attempt);
                    assert!(weak.upgrade().is_none());
                }));
            }
            for execution in executions {
                execution.await.unwrap();
            }
            let current = physical_nodes(&shared.root.native_plan);
            assert_eq!(original_nodes.len(), current.len());
            for (before, after) in original_nodes.iter().zip(current) {
                assert!(Arc::ptr_eq(before, &after));
            }
            assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn aggregate_stream_variants_resolve_attempt_metrics() {
        for migration in [false, true] {
            let mut config = datafusion::prelude::SessionConfig::new();
            config.options_mut().execution.enable_migration_aggregate = migration;
            let session = Arc::new(SessionContext::new_with_config(config));
            for ordered in [false, true] {
                let mut definition = aggregate_plan();
                if ordered {
                    definition.children[0] = sort_plan(None, None);
                }
                let shared = SharedPipeline::build(&definition, &session).unwrap();
                let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
                let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
                let (mut private_scans, _, private) =
                    planner.create_plan(&definition, &mut vec![], 1).unwrap();
                let values = vec![Some(3), Some(1), Some(3), None];
                feed(&mut scans[0], values.clone());
                feed(&mut private_scans[0], values);
                let stream = shared
                    .root
                    .native_plan
                    .execute(0, attempt.task_context(&session))
                    .unwrap();
                let reference = private.native_plan.execute(0, session.task_ctx()).unwrap();
                let (actual, expected) = tokio::join!(
                    drain_inputs(scans, stream),
                    drain_inputs(private_scans, reference)
                );
                assert_eq!(rows(&actual), rows(&expected));
                assert_eq!(
                    attempt
                        .metrics_for(&shared.root.native_plan)
                        .unwrap()
                        .output_rows(),
                    Some(3)
                );

                assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
            }
        }
    }

    fn min_max_plan(grouped: bool) -> Operator {
        use datafusion_comet_proto::spark_expression::{Max, Min};
        let mut definition = aggregate_plan();
        let Some(OpStruct::HashAgg(a)) = &mut definition.op_struct else {
            unreachable!()
        };
        if !grouped {
            a.grouping_exprs.clear();
        }
        a.agg_exprs = vec![
            AggExpr {
                expr_struct: Some(agg_expr::ExprStruct::Min(Min {
                    child: Some(column()),
                    datatype: Some(datatype()),
                })),
                ..Default::default()
            },
            AggExpr {
                expr_struct: Some(agg_expr::ExprStruct::Max(Max {
                    child: Some(column()),
                    datatype: Some(datatype()),
                })),
                ..Default::default()
            },
        ];
        definition
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn min_max_share_real_operators_across_partitions() {
        // Disjoint bounds, all-null and empty inputs detect accidental reuse of
        // another partition's accumulator. Final/merge buffers are both i64.
        for grouped in [false, true] {
            for mode in 0..=2 {
                for filtered in [false, true] {
                    // Spark applies aggregate filters before merging state buffers.
                    if filtered && mode != 0 {
                        continue;
                    }
                    let session = Arc::new(SessionContext::new());
                    let mut definition = min_max_plan(grouped);
                    let Some(OpStruct::HashAgg(a)) = &mut definition.op_struct else {
                        unreachable!()
                    };
                    a.mode = mode;
                    a.initial_input_buffer_offset = i32::from(grouped);
                    if filtered {
                        for aggregate in &mut a.agg_exprs {
                            aggregate.filter = Some(expr(ExprStruct::Gt(Box::new(BinaryExpr {
                                left: Some(Box::new(column())),
                                right: Some(Box::new(literal(-50))),
                            }))));
                        }
                    }
                    let width = if mode == 0 {
                        1
                    } else {
                        2 + usize::from(grouped)
                    };
                    let Some(OpStruct::Scan(scan)) = &mut definition.children[0].op_struct else {
                        unreachable!()
                    };
                    scan.fields = vec![datatype(); width];
                    assert!(supports(&definition));
                    let shared =
                        SharedPipeline::build_partitions(&definition, &session, 5).unwrap();
                    let original = physical_nodes(&shared.root.native_plan);
                    assert!(original[0].is::<AggregateExec>());
                    let mut executions = Vec::new();
                    let barrier = Arc::new(tokio::sync::Barrier::new(4));
                    for (partition, values) in [
                        (0, vec![Some(-100), Some(-10), None]),
                        (2, vec![Some(20), Some(200), None]),
                        (3, vec![None, None]),
                        (4, vec![]),
                    ] {
                        let planner = PhysicalPlanner::new(Arc::clone(&session), partition);
                        let (mut scans, attempt) = shared
                            .try_bind_plan(&planner, &mut vec![], &definition)
                            .unwrap()
                            .unwrap();
                        let (mut private_scans, _, private) =
                            planner.create_plan(&definition, &mut vec![], 1).unwrap();
                        let columns: Vec<Arc<dyn arrow::array::Array>> = (0..width)
                            .map(|_| {
                                Arc::new(Int64Array::from(values.clone()))
                                    as Arc<dyn arrow::array::Array>
                            })
                            .collect();
                        scans[0].set_input_batch(InputBatch::Batch(columns.clone(), values.len()));
                        private_scans[0].set_input_batch(InputBatch::Batch(columns, values.len()));
                        let root = Arc::clone(&shared.root.native_plan);
                        let stream = root
                            .execute(partition as usize, attempt.task_context(&session))
                            .unwrap();
                        let reference = private.native_plan.execute(0, session.task_ctx()).unwrap();
                        let barrier = Arc::clone(&barrier);
                        executions.push(tokio::spawn(async move {
                            barrier.wait().await;
                            let (actual, expected) = tokio::join!(
                                drain_inputs(scans, stream),
                                drain_inputs(private_scans, reference)
                            );
                            assert_eq!(rows(&actual), rows(&expected));
                            assert_eq!(
                                attempt.metrics_for(&root).unwrap().output_rows(),
                                Some(actual.iter().map(RecordBatch::num_rows).sum())
                            );
                        }));
                    }
                    for execution in executions {
                        execution.await.unwrap();
                    }
                    for (before, after) in original
                        .iter()
                        .zip(physical_nodes(&shared.root.native_plan))
                    {
                        assert!(Arc::ptr_eq(before, &after));
                    }
                    assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn min_max_dynamic_bounds_do_not_replace_partition_accumulators() {
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use datafusion::physical_expr::expressions::Column;
        let session = Arc::new(SessionContext::new());
        let definition = min_max_plan(false);
        let mut shared = SharedPipeline::build_partitions(&definition, &session, 3).unwrap();
        let aggregate = shared
            .root
            .native_plan
            .downcast_ref::<AggregateExec>()
            .unwrap();
        // Comet normally wraps MIN/MAX arguments in CastExpr. Construct direct
        // Column arguments so upstream really creates its plan-owned dynamic filter.
        let expressions = aggregate
            .aggr_expr()
            .iter()
            .map(|a| {
                Arc::new(
                    AggregateExprBuilder::new(
                        Arc::new(a.fun().clone()),
                        vec![Arc::new(Column::new(
                            aggregate.input_schema().field(0).name(),
                            0,
                        ))],
                    )
                    .schema(aggregate.input_schema())
                    .alias(a.name())
                    .build()
                    .unwrap(),
                )
            })
            .collect();
        let input = Arc::clone(aggregate.input());
        let raw: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                aggregate.group_expr().clone(),
                expressions,
                vec![None, None],
                Arc::clone(&input),
                aggregate.input_schema(),
            )
            .unwrap(),
        );
        assert_eq!(raw.dynamic_expressions_produced().len(), 1);
        let converted = convert_tree(&raw, &shared.identity, &[input], &mut vec![], 3).unwrap();
        assert_eq!(converted.dynamic_expressions_produced().len(), 1);
        let owner = Arc::get_mut(&mut shared).unwrap();
        Arc::get_mut(&mut owner.root).unwrap().native_plan = converted;
        let root = Arc::clone(&shared.root.native_plan);
        let dynamic = root.dynamic_expressions_produced();
        let initial_filter = dynamic[0].to_string();
        // First establish tighter bounds, then execute a disjoint partition on
        // exactly the same node. The second result must retain its own extrema.
        for (partition, values, expected) in [
            (0, vec![Some(-100), Some(200)], vec![Some(-100), Some(200)]),
            (2, vec![Some(20), Some(30)], vec![Some(20), Some(30)]),
        ] {
            let planner = PhysicalPlanner::new(Arc::clone(&session), partition);
            let (mut scans, attempt) = shared
                .try_bind_plan(&planner, &mut vec![], &definition)
                .unwrap()
                .unwrap();
            feed(&mut scans[0], values);
            let stream = root
                .execute(partition as usize, attempt.task_context(&session))
                .unwrap();
            assert_eq!(rows(&drain_inputs(scans, stream).await), vec![expected]);
            assert_ne!(dynamic[0].to_string(), initial_filter);
            assert_eq!(attempt.metrics_for(&root).unwrap().output_rows(), Some(1));
            assert!(Arc::ptr_eq(&root, &shared.root.native_plan));
        }
        assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn average_stream_metrics_and_state_are_attempt_local() {
        let session = Arc::new(SessionContext::new());
        let mut definition = aggregate_plan();
        if let Some(OpStruct::HashAgg(a)) = &mut definition.op_struct {
            a.grouping_exprs.clear();
            a.agg_exprs = vec![AggExpr {
                expr_struct: Some(agg_expr::ExprStruct::Avg(
                    datafusion_comet_proto::spark_expression::Avg {
                        child: Some(column()),
                        datatype: Some(DataType {
                            type_id: 6,
                            type_info: None,
                        }),
                        sum_datatype: Some(DataType {
                            type_id: 6,
                            type_info: None,
                        }),
                        eval_mode: 0,
                    },
                )),
                ..Default::default()
            }];
        }
        let shared = SharedPipeline::build_partitions(&definition, &session, 2).unwrap();
        for partition in [0, 1] {
            let planner = PhysicalPlanner::new(Arc::clone(&session), partition);
            let (mut scans, attempt) = shared
                .bind_plan(&planner, &mut vec![], &definition)
                .unwrap();
            let (mut private_scans, _, private) =
                planner.create_plan(&definition, &mut vec![], 1).unwrap();
            let values = vec![None, Some(2 + partition as i64), Some(4)];
            feed(&mut scans[0], values.clone());
            feed(&mut private_scans[0], values);
            let stream = shared
                .root
                .native_plan
                .execute(partition as usize, attempt.task_context(&session))
                .unwrap();
            let ordinary = private.native_plan.execute(0, session.task_ctx()).unwrap();
            let (actual, expected) = tokio::join!(
                drain_inputs(scans, stream),
                drain_inputs(private_scans, ordinary)
            );
            assert_eq!(actual, expected);
            assert_eq!(
                attempt
                    .metrics_for(&shared.root.native_plan)
                    .unwrap()
                    .output_rows(),
                Some(1)
            );
        }
    }

    #[tokio::test]
    async fn final_and_partial_merge_aggregates_share_the_tree() {
        use datafusion_comet_proto::spark_operator::AggregateMode as ProtoMode;
        let session = Arc::new(SessionContext::new());
        let mut partial = aggregate_plan();
        if let Some(OpStruct::HashAgg(a)) = &mut partial.op_struct {
            // COUNT has one i64 buffer, simplifying explicit merge inputs.
            a.agg_exprs.truncate(1);
        }
        for mode in [ProtoMode::Final, ProtoMode::PartialMerge] {
            let mut definition = partial.clone();
            if let Some(OpStruct::HashAgg(a)) = &mut definition.op_struct {
                a.mode = mode as i32;
                a.initial_input_buffer_offset = 1;
            }
            if let Some(OpStruct::Scan(scan)) = &mut definition.children[0].op_struct {
                scan.fields = vec![datatype(), datatype()];
            }
            let shared = SharedPipeline::build_partitions(&definition, &session, 3).unwrap();
            for partition in [0, 2] {
                let planner = PhysicalPlanner::new(Arc::clone(&session), partition);
                let (mut scans, attempt) = shared
                    .bind_plan(&planner, &mut vec![], &definition)
                    .unwrap();
                let (mut reference_scans, _, reference) =
                    planner.create_plan(&definition, &mut vec![], 1).unwrap();
                let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
                    Arc::new(Int64Array::from(vec![1, 1, 2])),
                    Arc::new(Int64Array::from(vec![2, 3, 4])),
                ];
                scans[0].set_input_batch(InputBatch::Batch(columns.clone(), 3));
                reference_scans[0].set_input_batch(InputBatch::Batch(columns, 3));
                let stream = shared
                    .root
                    .native_plan
                    .execute(partition as usize, attempt.task_context(&session))
                    .unwrap();
                let ordinary = reference
                    .native_plan
                    .execute(0, session.task_ctx())
                    .unwrap();
                let (actual, expected) = tokio::join!(
                    drain_inputs(scans, stream),
                    drain_inputs(reference_scans, ordinary)
                );
                assert_eq!(rows(&actual), rows(&expected));
                assert_eq!(
                    rows(&actual),
                    vec![vec![Some(1), Some(5)], vec![Some(2), Some(4)]]
                );
                assert_eq!(
                    attempt
                        .metrics_for(&shared.root.native_plan)
                        .unwrap()
                        .output_rows(),
                    Some(2)
                );
            }
        }
    }

    #[tokio::test]
    async fn completed_scopes_release_tree_and_metrics() {
        let cache = ScopedPlans::default();
        let session = Arc::new(SessionContext::new());
        for value in 0..2000 {
            let shared = cache
                .get_or_build(b"scope", || {
                    SharedPipeline::build(&aggregate_plan(), &session)
                })
                .unwrap();
            assert!(shared.try_claim_partition(0));
            let weak = Arc::downgrade(&shared);
            let root = Arc::downgrade(&shared.root.native_plan);
            let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
            let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
            feed(&mut scans[0], vec![Some(value)]);
            let stream = shared
                .root
                .native_plan
                .execute(0, attempt.task_context(&session))
                .unwrap();
            drop(shared);
            assert!(weak.upgrade().is_some());
            let output = drain_inputs(scans, stream).await;
            assert_eq!(rows(&output).len(), 1);
            assert_eq!(
                attempt
                    .metrics_for(&root.upgrade().unwrap())
                    .unwrap()
                    .output_rows(),
                Some(1)
            );
            drop(attempt);
            assert!(weak.upgrade().is_none());
            assert!(root.upgrade().is_none());
        }
        assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn grouped_aggregate_accumulators_and_metrics_are_attempt_local() {
        let session = Arc::new(SessionContext::new());
        let shared = SharedPipeline::build_partitions(&aggregate_plan(), &session, 3).unwrap();
        for (partition, values) in [
            vec![Some(2), Some(2), None],
            vec![Some(8), Some(8), Some(8)],
            vec![],
        ]
        .into_iter()
        .enumerate()
        {
            let planner = PhysicalPlanner::new(Arc::clone(&session), partition as i32);
            let (mut scans, attempt) = shared
                .try_bind_plan(&planner, &mut vec![], &aggregate_plan())
                .unwrap()
                .unwrap();
            feed(&mut scans[0], values.clone());
            let stream = shared
                .root
                .native_plan
                .execute(partition, attempt.task_context(&session))
                .unwrap();
            let batches = drain_inputs(scans, stream).await;
            let (mut ordinary_scans, _, ordinary) = planner
                .create_plan(&aggregate_plan(), &mut vec![], 1)
                .unwrap();
            feed(&mut ordinary_scans[0], values);
            let reference = drain_inputs(
                ordinary_scans,
                ordinary.native_plan.execute(0, session.task_ctx()).unwrap(),
            )
            .await;
            assert_eq!(rows(&batches), rows(&reference));
            assert_eq!(
                attempt
                    .metrics_for(&shared.root.native_plan)
                    .unwrap()
                    .output_rows(),
                Some(rows(&batches).len())
            );
            let weak = Arc::downgrade(&attempt);
            drop(attempt);
            assert!(weak.upgrade().is_none());
            assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn hash_join_build_sides_join_types_and_overlapping_attempts() {
        for side in [0, 1] {
            for kind in 0..=5 {
                let session = Arc::new(SessionContext::new());
                let definition = join_plan(side, kind);
                let shared = SharedPipeline::build_partitions(&definition, &session, 2).unwrap();
                let mut executions = Vec::new();
                for (partition, base) in [0, 100].into_iter().enumerate() {
                    let planner = PhysicalPlanner::new(Arc::clone(&session), partition as i32);
                    let left = vec![None, Some(base + 1), Some(base + 2), Some(base + 2)];
                    let right = vec![Some(base + 2), Some(base + 3), None];
                    let (mut scans, attempt) = shared
                        .try_bind_plan(&planner, &mut vec![], &definition)
                        .unwrap()
                        .unwrap();
                    feed(&mut scans[0], left.clone());
                    feed(&mut scans[1], right.clone());
                    let stream = shared
                        .root
                        .native_plan
                        .execute(partition, attempt.task_context(&session))
                        .unwrap();
                    let (mut ordinary_scans, _, ordinary) =
                        planner.create_plan(&definition, &mut vec![], 1).unwrap();
                    feed(&mut ordinary_scans[0], left);
                    feed(&mut ordinary_scans[1], right);
                    let ordinary_stream =
                        ordinary.native_plan.execute(0, session.task_ctx()).unwrap();
                    executions.push(async move {
                        let (actual, expected) = tokio::join!(
                            drain_inputs(scans, stream),
                            drain_inputs(ordinary_scans, ordinary_stream)
                        );
                        assert_eq!(rows(&actual), rows(&expected));
                        (attempt, actual)
                    });
                }
                let a = executions.remove(0);
                let b = executions.remove(0);
                let ((a, out_a), (b, out_b)) = tokio::join!(a, b);
                assert_eq!(
                    a.metrics_for(&shared.root.native_plan)
                        .unwrap()
                        .output_rows(),
                    Some(rows(&out_a).len())
                );
                assert_eq!(
                    b.metrics_for(&shared.root.native_plan)
                        .unwrap()
                        .output_rows(),
                    Some(rows(&out_b).len())
                );
                let weak_a = Arc::downgrade(&a);
                let weak_b = Arc::downgrade(&b);
                drop(a);
                drop(b);
                assert!(weak_a.upgrade().is_none() && weak_b.upgrade().is_none());
                assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
            }
        }
    }
    fn native_file_plan(path: &std::path::Path) -> Operator {
        use datafusion_comet_proto::spark_operator::{
            NativeScan, NativeScanCommon, SparkFilePartition, SparkPartitionedFile,
            SparkStructField,
        };
        let field = SparkStructField {
            name: "id".into(),
            data_type: Some(datatype()),
            nullable: true,
            ..Default::default()
        };
        let size = std::fs::metadata(path).unwrap().len() as i64;
        let scan = Operator {
            plan_id: 1,
            op_struct: Some(OpStruct::NativeScan(NativeScan {
                common: Some(NativeScanCommon {
                    required_schema: vec![field.clone()],
                    data_schema: vec![field],
                    projection_vector: vec![0],
                    session_timezone: "UTC".into(),
                    case_sensitive: true,
                    ..Default::default()
                }),
                file_partition: Some(SparkFilePartition {
                    partitioned_file: vec![SparkPartitionedFile {
                        file_path: format!("file://{}", path.display()),
                        start: 0,
                        length: size,
                        file_size: size,
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut plan = pipeline();
        plan.children[0].children[0] = scan;
        plan
    }

    #[test]
    fn unsupported_stateful_paths_fall_back_as_whole_blocks() {
        assert!(!supports(&sort_plan(Some(2), None)));
        assert!(!supports(&sort_plan(Some(3), Some(1))));
        let file = tempfile::NamedTempFile::new().unwrap();
        let native = native_file_plan(file.path());
        assert!(!supports(&native));
        assert!(SharedPipeline::build(&native, &Arc::new(SessionContext::new())).is_err());
    }

    #[tokio::test]
    async fn sort_spills_with_attempt_memory_pool_and_releases_reservations() {
        use datafusion::execution::{config::SessionConfig, runtime_env::RuntimeEnvBuilder};
        let config = SessionConfig::new().with_batch_size(128);
        let reservation = config.options().execution.sort_spill_reservation_bytes;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(reservation + 12288, 1.0)
            .build_arc()
            .unwrap();
        let session = Arc::new(SessionContext::new_with_config_rt(config, runtime));
        for _ in 0..2 {
            let shared = SharedPipeline::build(&sort_plan(None, None), &session).unwrap();
            let planner = PhysicalPlanner::new(Arc::clone(&session), 7);
            let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
            let mut stream = shared
                .root
                .native_plan
                .execute(0, attempt.task_context(&session))
                .unwrap();
            let mut remaining = 100;
            let mut ended = false;
            let mut actual = Vec::new();
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                loop {
                    let batch = futures::future::poll_fn(|cx| match stream.poll_next_unpin(cx) {
                        Poll::Pending => {
                            if !ended && scans[0].batch.lock().unwrap().is_none() {
                                if remaining > 0 {
                                    feed(&mut scans[0], (0..100).rev().map(Some).collect());
                                    remaining -= 1;
                                } else {
                                    scans[0].set_input_batch(InputBatch::EOF);
                                    ended = true;
                                }
                            }
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        ready => ready,
                    })
                    .await;
                    match batch {
                        Some(batch) => actual.extend(
                            batch
                                .unwrap()
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .copied(),
                        ),
                        None => break,
                    }
                }
            })
            .await
            .unwrap();
            drop(stream);
            assert_eq!(actual.len(), 10000);
            assert!(actual.windows(2).all(|w| w[0] <= w[1]));
            let metrics = attempt.metrics_for(&shared.root.native_plan).unwrap();
            assert_eq!(metrics.output_rows(), Some(10000));
            assert!(metrics.spill_count().unwrap_or(0) > 0);
            assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
        }
    }
    #[tokio::test]
    async fn cancelling_join_and_aggregate_drops_attempt_and_memory() {
        for definition in [join_plan(0, 0), join_plan(1, 0), aggregate_plan()] {
            let session = Arc::new(SessionContext::new());
            let shared = SharedPipeline::build(&definition, &session).unwrap();
            let planner = PhysicalPlanner::new(Arc::clone(&session), 7);
            let (mut scans, attempt) = shared.bind(&planner, &mut vec![]).unwrap();
            for scan in &mut scans {
                feed(scan, vec![Some(1), Some(2)]);
            }
            let mut stream = shared
                .root
                .native_plan
                .execute(0, attempt.task_context(&session))
                .unwrap();
            assert!(stream.next().now_or_never().is_none());
            let weak = Arc::downgrade(&attempt);
            drop(stream);
            drop(scans);
            drop(attempt);
            assert!(
                weak.upgrade().is_none(),
                "execution kernel retained cancelled attempt"
            );
            assert_eq!(session.runtime_env().memory_pool.reserved(), 0);
        }
    }
}
