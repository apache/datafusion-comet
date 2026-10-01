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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_comet_local::handle::{QueryHandle, QueryPoll};
use datafusion_comet_local::range::range_plan;
use datafusion_comet_local::LocalQuery;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::test::exec::{BlockingExec, ErrorExec, PanicExec};
use tokio::runtime::Builder;

fn runtime() -> tokio::runtime::Runtime {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn range_partitions_projection_and_long_boundaries() {
    let runtime = runtime();
    for (start, end, step, mut expected) in [
        (0, 10, 3, vec![0, 3, 6, 9]),
        (10, 0, -3, vec![10, 7, 4, 1]),
        (5, 5, 1, vec![]),
        (0, 10, -1, vec![]),
        (
            i64::MIN,
            i64::MAX,
            i64::MAX,
            vec![i64::MIN, -1, i64::MAX - 1],
        ),
        (i64::MAX, i64::MIN, i64::MIN, vec![i64::MAX, -1]),
    ] {
        let plan = range_plan(start, end, step, 7, 2, 2).unwrap();
        let refs = Arc::downgrade(&plan);
        let query = QueryHandle::start(
            LocalQuery::new(
                plan,
                Arc::new(
                    TaskContext::default()
                        .with_session_config(SessionConfig::new().with_batch_size(2)),
                ),
            ),
            runtime.handle(),
        )
        .unwrap();
        let mut actual: Vec<i64> = vec![];
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline);
            match query.poll(Duration::from_millis(20)).unwrap() {
                QueryPoll::Batch(batch) => {
                    assert!(batch.num_rows() <= 2);
                    assert_eq!(batch.column(0), batch.column(1));
                    actual.extend(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values(),
                    );
                }
                QueryPoll::Pending => (),
                QueryPoll::Finished => break,
            }
        }
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        drop(query);
        while refs.strong_count() > 0 {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
}

#[test]
fn invalid_range_parameters_are_rejected_before_execution() {
    for (step, partitions, batch, columns) in [
        (0, 1, 1, 1),
        (1, 0, 1, 1),
        (1, 1025, 1, 1),
        (1, 1, 0, 1),
        (1, 1, 65537, 1),
        (1, 1, 1, 0),
        (1, 1, 1, 1025),
    ] {
        assert!(range_plan(0, 10, step, partitions, batch, columns).is_err());
    }
}

#[test]
fn pending_is_distinct_from_eof_and_drop_releases_graph() {
    let runtime = runtime();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let plan = BlockingExec::new(schema, 4);
    let refs = plan.refs();
    let query = QueryHandle::start(
        LocalQuery::new(Arc::new(plan), Arc::new(TaskContext::default())),
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
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let plan = Arc::new(PanicExec::new(schema, 2));
    let query = QueryHandle::start(
        LocalQuery::new(plan, Arc::new(TaskContext::default())),
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
    let plan = Arc::new(ErrorExec::new());
    assert!(QueryHandle::start(
        LocalQuery::new(plan, Arc::new(TaskContext::default())),
        runtime.handle()
    )
    .is_err());
}

#[test]
fn runtime_shutdown_is_an_error_not_successful_eof() {
    let runtime = runtime();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = QueryHandle::start(
        LocalQuery::new(
            Arc::new(BlockingExec::new(schema, 4)),
            Arc::new(TaskContext::default()),
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
        let stalled = range_plan(0, i64::MAX, 1, 7, 5, 1).unwrap();
        let refs = Arc::downgrade(&stalled);
        let stalled = QueryHandle::start(
            LocalQuery::new(stalled, Arc::new(TaskContext::default())),
            runtime.handle(),
        )
        .unwrap();
        // Leave its one-batch result channel unread while a second query makes progress.
        let healthy = QueryHandle::start(
            LocalQuery::new(
                range_plan(100, 237, 1, 7, 3, 1).unwrap(),
                Arc::new(TaskContext::default()),
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
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        assert_eq!(values.value(i), next);
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
        LocalQuery::new(
            range_plan(0, i64::MAX, 1, 7, 5, 1).unwrap(),
            Arc::new(TaskContext::default()),
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
            QueryPoll::Finished => panic!("unbounded fixture ended"),
        }
    };
    drop(runtime);
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    let mut queued = 0;
    loop {
        assert!(Instant::now() < deadline);
        match query.poll(Duration::from_millis(20)) {
            Ok(QueryPoll::Batch(_)) => {
                queued += 1;
                assert!(queued <= 1, "handoff exceeded its one-batch bound");
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
