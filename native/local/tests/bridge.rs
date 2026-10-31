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

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::test::exec::{BlockingExec, ErrorExec, MockExec, PanicExec};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_local::handle::{QueryHandle, QueryPoll};
use datafusion_comet_local::LocalQuery;
use tokio::runtime::Builder;

fn runtime() -> tokio::runtime::Runtime {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// One partition per `(start, end)` range, each split into batches of `rows` values.
fn ranges(ranges: &[(i64, i64)], rows: i64) -> Arc<dyn ExecutionPlan> {
    let partitions = ranges
        .iter()
        .map(|&(first, last)| {
            let batches = (first..last)
                .step_by(rows as usize)
                .map(|start| {
                    let end = (start + rows).min(last);
                    Ok(RecordBatch::try_new(
                        schema(),
                        vec![Arc::new(Int64Array::from_iter_values(start..end))],
                    )
                    .unwrap())
                })
                .collect();
            Arc::new(MockExec::new(batches, schema())) as Arc<dyn ExecutionPlan>
        })
        .collect();
    UnionExec::try_new(partitions).unwrap()
}

fn values(batch: &RecordBatch) -> &[i64] {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
}

#[test]
fn partition_order_is_preserved_through_the_handle() {
    let runtime = runtime();
    let plan = ranges(&[(0, 10), (10, 10), (10, 25), (25, 26)], 3);
    let refs = Arc::downgrade(&plan);
    let query = QueryHandle::start(
        LocalQuery::new(
            plan,
            Arc::new(TaskContext::default()),
            usize::MAX,
            usize::MAX,
        ),
        runtime.handle(),
    )
    .unwrap();
    let mut actual = vec![];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        match query.poll(Duration::from_millis(20)).unwrap() {
            QueryPoll::Batch(batch) => actual.extend_from_slice(values(&batch)),
            QueryPoll::Pending => (),
            QueryPoll::Finished => break,
        }
    }
    assert_eq!(actual, (0..26).collect::<Vec<_>>());
    drop(query);
    while refs.strong_count() > 0 {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}

#[test]
fn pending_is_distinct_from_eof_and_drop_releases_graph() {
    let runtime = runtime();
    let plan = BlockingExec::new(schema(), 4);
    let refs = plan.refs();
    let query = QueryHandle::start(
        LocalQuery::new(
            Arc::new(plan),
            Arc::new(TaskContext::default()),
            usize::MAX,
            usize::MAX,
        ),
        runtime.handle(),
    )
    .unwrap();
    assert!(matches!(
        query.poll(Duration::from_millis(20)).unwrap(),
        QueryPoll::Pending
    ));
    assert!(
        refs.strong_count() > 1,
        "pending streams should have started"
    );
    drop(query);
    let deadline = Instant::now() + Duration::from_secs(5);
    while refs.strong_count() > 0 {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}

#[test]
fn worker_panic_is_an_error_not_successful_eof() {
    let runtime = runtime();
    let plan = Arc::new(PanicExec::new(schema(), 2));
    let query = QueryHandle::start(
        LocalQuery::new(
            plan,
            Arc::new(TaskContext::default()),
            usize::MAX,
            usize::MAX,
        ),
        runtime.handle(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        match query.poll(Duration::from_millis(20)) {
            Err(error) => {
                assert!(error.to_string().contains("panic"));
                break;
            }
            Ok(QueryPoll::Pending | QueryPoll::Batch(_)) => (),
            Ok(QueryPoll::Finished) => panic!("worker failure was lost"),
        }
    }
}

#[test]
fn startup_failure_is_returned() {
    let runtime = runtime();
    let query = QueryHandle::start(
        LocalQuery::new(
            Arc::new(ErrorExec::new()),
            Arc::new(TaskContext::default()),
            usize::MAX,
            usize::MAX,
        ),
        runtime.handle(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match query.poll(Duration::from_millis(50)) {
            Err(error) => {
                assert!(error.to_string().contains("ErrorExec"));
                break;
            }
            Ok(QueryPoll::Pending) => assert!(Instant::now() < deadline, "no startup failure"),
            Ok(_) => panic!("startup failure was lost"),
        }
    }
}

#[test]
fn runtime_shutdown_is_an_error_not_successful_eof() {
    let runtime = runtime();
    let query = QueryHandle::start(
        LocalQuery::new(
            Arc::new(BlockingExec::new(schema(), 4)),
            Arc::new(TaskContext::default()),
            usize::MAX,
            usize::MAX,
        ),
        runtime.handle(),
    )
    .unwrap();
    assert!(matches!(
        query.poll(Duration::from_millis(20)).unwrap(),
        QueryPoll::Pending
    ));
    drop(runtime);
    match query.poll(Duration::from_millis(50)) {
        Err(error) => assert!(error.to_string().contains("without completing")),
        Ok(_) => panic!("runtime shutdown must not look like successful EOF"),
    }
}

#[test]
fn backpressured_query_does_not_starve_other_queries_and_cancel_releases_graph() {
    let runtime = runtime();
    for _ in 0..12 {
        let stalled = ranges(&[(0, 100_000), (100_000, 200_000)], 5);
        let refs = Arc::downgrade(&stalled);
        let stalled = QueryHandle::start(
            LocalQuery::new(stalled, Arc::new(TaskContext::default()), usize::MAX, 0),
            runtime.handle(),
        )
        .unwrap();
        // Leave it unread, with its partitions at their read-ahead bound, while a second query
        // makes progress.
        let healthy = QueryHandle::start(
            LocalQuery::new(
                ranges(&[(100, 150), (150, 200), (200, 237)], 3),
                Arc::new(TaskContext::default()),
                usize::MAX,
                usize::MAX,
            ),
            runtime.handle(),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut next = 100;
        loop {
            assert!(
                Instant::now() < deadline,
                "a stalled consumer starved another query"
            );
            match healthy.poll(Duration::from_millis(20)).unwrap() {
                QueryPoll::Batch(batch) => {
                    for value in values(&batch) {
                        assert_eq!(*value, next);
                        next += 1;
                    }
                }
                QueryPoll::Pending => (),
                QueryPoll::Finished => break,
            }
        }
        assert_eq!(next, 237);
        stalled.cancel();
        assert!(stalled.poll(Duration::ZERO).is_err());
        drop(stalled);
        drop(healthy);
        while refs.strong_count() != 0 {
            assert!(Instant::now() < deadline, "cancelled graph remained live");
            std::thread::yield_now();
        }
    }
}

#[test]
fn shutdown_after_partial_output_never_silently_truncates_the_result() {
    let runtime = runtime();
    let query = QueryHandle::start(
        // No read-ahead, so the partition holds at most two batches ahead of delivery.
        LocalQuery::new(
            ranges(&[(0, 100_000)], 5),
            Arc::new(TaskContext::default()),
            usize::MAX,
            0,
        ),
        runtime.handle(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let first = loop {
        assert!(Instant::now() < deadline);
        match query.poll(Duration::from_millis(20)).unwrap() {
            QueryPoll::Batch(batch) => break batch,
            QueryPoll::Pending => (),
            QueryPoll::Finished => panic!("large fixture ended"),
        }
    };
    drop(runtime);
    assert_eq!(values(&first)[0], 0);
    let mut queued = 0;
    loop {
        assert!(Instant::now() < deadline);
        match query.poll(Duration::from_millis(20)) {
            Ok(QueryPoll::Batch(_)) => {
                queued += 1;
                assert!(
                    queued <= 2,
                    "the partition held more than two batches ahead"
                );
            }
            Ok(QueryPoll::Pending) => (),
            Ok(QueryPoll::Finished) => panic!("partial output was reported as complete"),
            Err(error) => {
                assert!(error.to_string().contains("without completing"));
                break;
            }
        }
    }
}

#[test]
fn cancel_releases_the_graph_while_every_worker_is_busy() {
    let runtime = runtime();
    let plan = BlockingExec::new(schema(), 3);
    let refs = plan.refs();
    let query = QueryHandle::start(
        LocalQuery::new(
            Arc::new(plan),
            Arc::new(TaskContext::default()),
            usize::MAX,
            usize::MAX,
        ),
        runtime.handle(),
    )
    .unwrap();
    assert!(matches!(
        query.poll(Duration::from_millis(20)).unwrap(),
        QueryPoll::Pending
    ));
    // Occupy the runtime's only worker, as Spark's acquireMemory can, so that no cancelled
    // task gets to run.
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let (started, running) = std::sync::mpsc::channel::<()>();
    runtime.spawn(async move {
        started.send(()).unwrap();
        let _ = blocked.recv();
    });
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    query.cancel();
    assert_eq!(
        refs.strong_count(),
        0,
        "cancel must drop the graph on the calling thread"
    );
    release.send(()).unwrap();
}
