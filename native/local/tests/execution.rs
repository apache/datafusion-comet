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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_comet_local::LocalQuery;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::test::exec::{BlockingExec, ErrorExec, MockExec};
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(10);
const INPUTS: usize = 4;
const ROWS_PER_INPUT: usize = 512;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn context() -> Arc<TaskContext> {
    Arc::new(TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(4)))
}

fn batch(ids: impl Iterator<Item = i64>) -> RecordBatch {
    let ids: Vec<_> = ids.collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids.iter().map(|id| id % 17))),
            Arc::new(Int64Array::from(ids)),
        ],
    )
    .unwrap()
}

fn input() -> Arc<dyn ExecutionPlan> {
    let inputs = (0..INPUTS)
        .map(|partition| {
            let batches = (0..ROWS_PER_INPUT)
                .step_by(4)
                .map(|offset| {
                    let start = (partition * ROWS_PER_INPUT + offset) as i64;
                    Ok(batch(start..start + 4))
                })
                .collect();
            Arc::new(MockExec::new(batches, schema())) as Arc<dyn ExecutionPlan>
        })
        .collect();
    UnionExec::try_new(inputs).unwrap()
}

fn repartition(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, partitioning).unwrap())
}

#[derive(Debug, Default)]
struct Observations {
    executions: Mutex<Vec<usize>>,
    key_partitions: Mutex<BTreeMap<i64, BTreeSet<usize>>>,
}

/// Records actual partition execution and routing without changing the data or scheduling.
#[derive(Debug)]
struct ObserveExec {
    input: Arc<dyn ExecutionPlan>,
    observations: Arc<Observations>,
}

impl DisplayAs for ObserveExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ObserveExec")
    }
}

impl ExecutionPlan for ObserveExec {
    fn name(&self) -> &'static str {
        "ObserveExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            observations: Arc::clone(&self.observations),
        }))
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

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.observations.executions.lock().unwrap().push(partition);
        let observations = Arc::clone(&self.observations);
        let stream = self.input.execute(partition, ctx)?.map_ok(move |batch| {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mut locations = observations.key_partitions.lock().unwrap();
            for key in keys.values() {
                locations.entry(*key).or_default().insert(partition);
            }
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn observe(input: Arc<dyn ExecutionPlan>) -> (Arc<dyn ExecutionPlan>, Arc<Observations>) {
    let observations = Arc::new(Observations::default());
    (
        Arc::new(ObserveExec {
            input,
            observations: Arc::clone(&observations),
        }),
        observations,
    )
}

fn assert_executed_once(observations: &Observations, partitions: usize) {
    let mut executed = observations.executions.lock().unwrap().clone();
    executed.sort_unstable();
    assert_eq!(executed, (0..partitions).collect::<Vec<_>>());
}

async fn wait_released<T: ?Sized>(refs: &Weak<T>) {
    timeout(DEADLINE, async {
        while refs.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("execution retained a graph, context, or input stream after teardown");
}

async fn wait_started(observations: &Observations, partitions: usize) {
    timeout(DEADLINE, async {
        while observations.executions.lock().unwrap().len() < partitions {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("input partitions did not start");
    assert_executed_once(observations, partitions);
}

async fn check_exchange(hash: bool, nested: bool) {
    let (input, source_observations) = observe(input());
    let partitioning = if hash {
        Partitioning::Hash(vec![Arc::new(Column::new("key", 0))], 7)
    } else {
        Partitioning::RoundRobinBatch(7)
    };
    let (mut plan, exchange_observations) = observe(repartition(input, partitioning));
    if nested {
        plan = repartition(plan, Partitioning::RoundRobinBatch(11));
    }
    let plan_ref = Arc::downgrade(&plan);
    let ctx = context();
    let ctx_ref = Arc::downgrade(&ctx);
    let pool = Arc::clone(ctx.memory_pool());
    let mut stream = LocalQuery::new(plan, ctx).execute().unwrap();
    let batches: Vec<RecordBatch> = timeout(DEADLINE, stream.by_ref().try_collect())
        .await
        .expect("repartition stalled")
        .unwrap();
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..(INPUTS * ROWS_PER_INPUT) as i64).collect::<Vec<_>>()
    );
    assert_executed_once(&source_observations, INPUTS);
    assert_executed_once(&exchange_observations, 7);
    if hash {
        let locations = exchange_observations.key_partitions.lock().unwrap();
        assert_eq!(locations.len(), 17);
        assert!(locations.values().all(|partitions| partitions.len() == 1));
    }
    // EOF releases resources even if the JVM would retain its exhausted iterator.
    assert!(stream.next().await.is_none());
    assert_eq!(stream.schema(), schema());
    wait_released(&plan_ref).await;
    wait_released(&ctx_ref).await;
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn round_robin_more_partitions_than_workers() {
    check_exchange(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hash_routes_all_input_partitions_through_one_exchange() {
    check_exchange(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_exchanges_more_partitions_than_workers() {
    check_exchange(true, true).await;
}

#[tokio::test]
async fn single_partition_preserves_order() {
    let expected = batch(0..12);
    let plan = Arc::new(MockExec::new(vec![Ok(expected.clone())], schema()));
    let batches: Vec<RecordBatch> = LocalQuery::new(plan, context())
        .execute()
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches, vec![expected]);
}

#[tokio::test]
async fn empty_result_finishes_with_schema() {
    for partitions in [0, 1, 7] {
        let plan = Arc::new(EmptyExec::new(schema()).with_partitions(partitions));
        let mut stream = LocalQuery::new(plan, context()).execute().unwrap();
        assert!(timeout(DEADLINE, stream.next()).await.unwrap().is_none());
        assert_eq!(stream.schema(), schema());
    }
}

#[tokio::test]
async fn cancel_pending_exchange_releases_inputs() {
    let input = BlockingExec::new(schema(), 4);
    let refs = input.refs();
    let (input, observations) = observe(Arc::new(input));
    let plan = repartition(input, Partitioning::RoundRobinBatch(7));
    let mut stream = LocalQuery::new(plan, context()).execute().unwrap();
    // Prove cancellation tears down running streams, not just an unstarted graph.
    wait_started(&observations, 4).await;
    // Drive the execution to Pending, then cancel without waiting for a batch.
    assert!(futures::poll!(stream.next()).is_pending());
    stream.cancel();
    stream.cancel();
    assert!(stream.next().await.is_none());
    wait_released(&refs).await;
}

fn partially_blocked_input(fail: bool) -> (Arc<dyn ExecutionPlan>, Weak<()>, Arc<Observations>) {
    let blocking = BlockingExec::new(schema(), 3);
    let refs = blocking.refs();
    let (blocking, observations) = observe(Arc::new(blocking));
    let value = if fail {
        Err(DataFusionError::Execution("injected source failure".into()))
    } else {
        Ok(batch(0..4))
    };
    let input = UnionExec::try_new(vec![
        Arc::new(MockExec::new(vec![value], schema()).with_unknown_statistics()),
        blocking,
    ])
    .unwrap();
    (
        repartition(input, Partitioning::RoundRobinBatch(7)),
        refs,
        observations,
    )
}

#[tokio::test]
async fn drop_after_first_batch_aborts_sibling_partitions() {
    let (plan, refs, observations) = partially_blocked_input(false);
    let mut stream = LocalQuery::new(plan, context()).execute().unwrap();
    wait_started(&observations, 3).await;
    timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(stream);
    wait_released(&refs).await;
}

#[tokio::test]
async fn first_error_cancels_siblings_and_is_terminal() {
    let (plan, refs, observations) = partially_blocked_input(true);
    let ctx = context();
    let ctx_ref = Arc::downgrade(&ctx);
    let pool = Arc::clone(ctx.memory_pool());
    let mut stream = LocalQuery::new(plan, ctx).execute().unwrap();
    wait_started(&observations, 3).await;
    let error = timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("injected source failure"));
    assert!(stream.next().await.is_none());
    wait_released(&refs).await;
    wait_released(&ctx_ref).await;
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn global_limit_stops_exchange_without_draining_inputs() {
    let (input, refs, observations) = partially_blocked_input(false);
    let plan = Arc::new(GlobalLimitExec::new(
        Arc::new(CoalescePartitionsExec::new(input)),
        0,
        Some(1),
    ));
    let mut stream = LocalQuery::new(plan, context()).execute().unwrap();
    wait_started(&observations, 3).await;
    let batches: Vec<RecordBatch> = timeout(DEADLINE, stream.by_ref().try_collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    wait_released(&refs).await;
}

#[tokio::test]
async fn cancelling_one_query_does_not_cancel_another() {
    let blocking_input = BlockingExec::new(schema(), 4);
    let refs = blocking_input.refs();
    let (blocking_input, observations) = observe(Arc::new(blocking_input));
    let mut blocked = LocalQuery::new(
        repartition(blocking_input, Partitioning::RoundRobinBatch(7)),
        context(),
    )
    .execute()
    .unwrap();
    wait_started(&observations, 4).await;
    let other = LocalQuery::new(input(), context()).execute().unwrap();
    blocked.cancel();
    let batches: Vec<RecordBatch> = timeout(DEADLINE, other.try_collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        INPUTS * ROWS_PER_INPUT
    );
    wait_released(&refs).await;
}

#[tokio::test]
async fn startup_error_releases_query() {
    let plan = Arc::new(ErrorExec::new());
    let refs = Arc::downgrade(&plan);
    let error = LocalQuery::new(plan, context()).execute().err().unwrap();
    assert!(error.to_string().contains("ErrorExec"));
    wait_released(&refs).await;
}

#[test]
fn missing_runtime_is_an_error_not_a_panic() {
    let plan = Arc::new(EmptyExec::new(schema()));
    let refs = Arc::downgrade(&plan);
    let error = LocalQuery::new(plan, context()).execute().err().unwrap();
    assert!(error.to_string().contains("active Tokio runtime"));
    assert_eq!(refs.strong_count(), 0);
}
