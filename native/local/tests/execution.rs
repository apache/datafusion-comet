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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::test::exec::{BlockingExec, ErrorExec, MockExec};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_comet_local::LocalQuery;
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

#[derive(Debug, Default)]
struct Observations {
    executions: Mutex<Vec<usize>>,
    batches: Mutex<BTreeMap<usize, usize>>,
}

/// Records actual partition execution without changing the data or scheduling.
#[derive(Debug)]
struct ObserveExec {
    input: Arc<dyn ExecutionPlan>,
    observations: Arc<Observations>,
    /// How long starting a partition blocks its caller, as a memory consumer's registration
    /// can while the pool waits for Spark.
    start_delay: Duration,
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
            start_delay: self.start_delay,
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
        std::thread::sleep(self.start_delay);
        self.observations.executions.lock().unwrap().push(partition);
        let observations = Arc::clone(&self.observations);
        let stream = self.input.execute(partition, ctx)?.map_ok(move |batch| {
            *observations
                .batches
                .lock()
                .unwrap()
                .entry(partition)
                .or_default() += 1;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn observe(input: Arc<dyn ExecutionPlan>) -> (Arc<dyn ExecutionPlan>, Arc<Observations>) {
    observe_starting_in(input, Duration::ZERO)
}

fn observe_starting_in(
    input: Arc<dyn ExecutionPlan>,
    start_delay: Duration,
) -> (Arc<dyn ExecutionPlan>, Arc<Observations>) {
    let observations = Arc::new(Observations::default());
    (
        Arc::new(ObserveExec {
            input,
            observations: Arc::clone(&observations),
            start_delay,
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

#[tokio::test]
async fn single_partition_preserves_order() {
    let expected = batch(0..12);
    let plan = Arc::new(MockExec::new(vec![Ok(expected.clone())], schema()));
    let batches: Vec<RecordBatch> = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
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
        let mut stream = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
            .execute()
            .unwrap();
        assert!(timeout(DEADLINE, stream.next()).await.unwrap().is_none());
        assert_eq!(stream.schema(), schema());
    }
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
    (input, refs, observations)
}

#[tokio::test]
async fn drop_after_first_batch_aborts_sibling_partitions() {
    let (plan, refs, observations) = partially_blocked_input(false);
    let mut stream = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
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
    let mut stream = LocalQuery::new(plan, ctx, usize::MAX, usize::MAX)
        .execute()
        .unwrap();
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
async fn cancelling_one_query_does_not_cancel_another() {
    let blocking_input = BlockingExec::new(schema(), 4);
    let refs = blocking_input.refs();
    let (blocking_input, observations) = observe(Arc::new(blocking_input));
    let mut blocked = LocalQuery::new(blocking_input, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
    wait_started(&observations, 4).await;
    let other = LocalQuery::new(input(), context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
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
async fn startup_error_is_reported_in_its_turn_and_releases_query() {
    let plan = Arc::new(ErrorExec::new());
    let refs = Arc::downgrade(&plan);
    let mut stream = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
    let error = timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("ErrorExec"));
    drop(stream);
    wait_released(&refs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_start_that_waits_does_not_hold_up_the_reader() {
    let (plan, observations) = observe_starting_in(ordered_input(2, 1), Duration::from_millis(500));
    let started = Instant::now();
    let mut stream = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
    assert!(started.elapsed() < Duration::from_millis(250));
    let batches: Vec<RecordBatch> = timeout(DEADLINE, stream.try_collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batches.len(), 2);
    assert_executed_once(&observations, 2);
}

#[test]
fn missing_runtime_is_an_error_not_a_panic() {
    let plan = Arc::new(EmptyExec::new(schema()));
    let refs = Arc::downgrade(&plan);
    let error = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
        .execute()
        .err()
        .unwrap();
    assert!(error.to_string().contains("active Tokio runtime"));
    assert_eq!(refs.strong_count(), 0);
}

fn ordered_input(partitions: usize, batches: usize) -> Arc<dyn ExecutionPlan> {
    let plans = (0..partitions)
        .map(|partition| {
            let base = (partition * batches * 4) as i64;
            let values = (0..batches)
                .map(|i| Ok(batch(base + i as i64 * 4..base + i as i64 * 4 + 4)))
                .collect();
            Arc::new(MockExec::new(values, schema())) as Arc<dyn ExecutionPlan>
        })
        .collect();
    UnionExec::try_new(plans).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_order_delivers_partitions_in_turn() {
    let plan = ordered_input(7, 9);
    let batches: Vec<RecordBatch> = timeout(
        DEADLINE,
        LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
            .execute()
            .unwrap()
            .try_collect(),
    )
    .await
    .unwrap()
    .unwrap();
    let values: Vec<i64> = batches
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
    assert_eq!(values, (0..7 * 9 * 4).collect::<Vec<_>>());
}

fn failing() -> Arc<dyn ExecutionPlan> {
    Arc::new(MockExec::new(
        vec![Err(DataFusionError::Execution(
            "injected late failure".into(),
        ))],
        schema(),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_order_reports_a_later_failure_in_its_turn() {
    let plan = UnionExec::try_new(vec![ordered_input(1, 3), failing()]).unwrap();
    let mut stream = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
    // The earlier partition's batches come first, even though the later one failed at once.
    for _ in 0..3 {
        timeout(DEADLINE, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    let error = timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("injected late failure"));
    assert!(stream.next().await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_order_never_reports_the_failure_of_a_partition_not_reached() {
    let blocking = BlockingExec::new(schema(), 1);
    let refs = blocking.refs();
    let plan = UnionExec::try_new(vec![Arc::new(blocking), failing()]).unwrap();
    let mut stream = LocalQuery::new(plan, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
    // A consumer that stops in the first partition, as a limit does, sees no error.
    assert!(timeout(Duration::from_millis(200), stream.next())
        .await
        .is_err());
    stream.cancel();
    assert!(stream.next().await.is_none());
    wait_released(&refs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_order_cancel_releases_running_partitions() {
    let blocking = BlockingExec::new(schema(), 3);
    let refs = blocking.refs();
    let (blocking, observations) = observe(Arc::new(blocking));
    let mut stream = LocalQuery::new(blocking, context(), usize::MAX, usize::MAX)
        .execute()
        .unwrap();
    wait_started(&observations, 3).await;
    assert!(futures::poll!(stream.next()).is_pending());
    stream.cancel();
    assert!(stream.next().await.is_none());
    wait_released(&refs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_order_runs_at_most_the_given_number_of_partitions() {
    let (plan, observations) = observe(ordered_input(6, 3));
    let executed = || observations.executions.lock().unwrap().clone();
    let mut stream = LocalQuery::new(plan, context(), 2, usize::MAX)
        .execute()
        .unwrap();
    // The first two partitions start with the query; the rest wait for their turn.
    wait_started(&observations, 2).await;
    for _ in 0..3 {
        timeout(DEADLINE, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert_eq!(executed(), vec![0, 1]);
    // Finishing the first partition starts the third.
    timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    wait_started(&observations, 3).await;
    let rest: Vec<RecordBatch> = timeout(DEADLINE, stream.try_collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rest.len(), 6 * 3 - 4);
    assert_executed_once(&observations, 6);
}

/// A first partition that never delivers, and a second one of ten batches.
fn behind_a_blocked_partition() -> (Arc<dyn ExecutionPlan>, Arc<Observations>) {
    let blocking = Arc::new(BlockingExec::new(schema(), 1));
    observe(UnionExec::try_new(vec![blocking, ordered_input(1, 10)]).unwrap())
}

fn produced(observations: &Observations, partition: usize) -> usize {
    let batches = observations.batches.lock().unwrap();
    batches.get(&partition).copied().unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_order_runs_later_partitions_ahead_within_the_limit() {
    let (plan, observations) = behind_a_blocked_partition();
    let mut stream = LocalQuery::new(plan, context(), 2, usize::MAX)
        .execute()
        .unwrap();
    // The second partition runs to its end while the first has delivered nothing.
    timeout(DEADLINE, async {
        while produced(&observations, 1) < 10 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the second partition did not run ahead");
    stream.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_order_holds_a_few_batches_beyond_the_limit() {
    let (plan, observations) = behind_a_blocked_partition();
    let mut stream = LocalQuery::new(plan, context(), 2, 0).execute().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(produced(&observations, 1), 2);
    stream.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_order_follows_a_changing_running_limit() {
    let (plan, observations) = observe(ordered_input(6, 1));
    let limit = Arc::new(AtomicUsize::new(1));
    let current = Arc::clone(&limit);
    let mut stream = LocalQuery::new(plan, context(), 3, usize::MAX)
        .with_running_limit(Arc::new(move || current.load(Ordering::Acquire)))
        .execute()
        .unwrap();
    let executed = || observations.executions.lock().unwrap().len();
    wait_started(&observations, 1).await;
    // A limit of zero still runs one partition at a time.
    limit.store(0, Ordering::Release);
    for partition in 0..2 {
        timeout(DEADLINE, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // The next partition starts only once this one ends.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(executed(), partition + 1, "started beyond the limit");
    }
    // A higher limit applies to the partitions started from then on.
    limit.store(usize::MAX, Ordering::Release);
    let rest: Vec<RecordBatch> = timeout(DEADLINE, stream.try_collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rest.len(), 4);
    assert_executed_once(&observations, 6);
}
