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

use super::*;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use futures::StreamExt;

#[tokio::test]
async fn executions_resets_and_child_replacement_do_not_reuse_thresholds() {
    let session = session(2);
    let (_file, scan) = parquet_input(
        vec![Some(4), Some(3), Some(2), Some(1)],
        &DataType::Int32,
        &session,
        2,
    );
    let plan = Arc::new(wrapper(&sort(scan, 2, SortOptions::default()), &session));
    let first = plan.build_runtime_sort().unwrap();
    let second = plan.build_runtime_sort().unwrap();
    let first_filter = produced_filter(&first.sort);
    let second_filter = produced_filter(&second.sort);
    assert_ne!(first_filter.expression_id(), second_filter.expression_id());
    let actual = collect(Arc::new(first.sort), session.task_ctx())
        .await
        .unwrap();
    assert_eq!(keys(&actual), vec![Some(1), Some(2)]);
    assert!(first_filter.snapshot_generation() > 1);
    assert_eq!(second_filter.snapshot_generation(), 1);
    assert_eq!(
        keys(
            &collect(Arc::new(second.sort), session.task_ctx())
                .await
                .unwrap()
        ),
        vec![Some(1), Some(2)],
    );
    for execution in [
        Arc::clone(&plan) as Arc<dyn ExecutionPlan>,
        Arc::clone(&plan) as Arc<dyn ExecutionPlan>,
        Arc::clone(&plan).reset_state().unwrap(),
    ] {
        assert_eq!(
            keys(&collect(execution, session.task_ctx()).await.unwrap()),
            vec![Some(1), Some(2)]
        );
    }
    let (left, right) = tokio::join!(
        collect(Arc::clone(&plan) as _, session.task_ctx()),
        collect(Arc::clone(&plan) as _, session.task_ctx()),
    );
    assert_eq!(keys(&left.unwrap()), vec![Some(1), Some(2)]);
    assert_eq!(keys(&right.unwrap()), vec![Some(1), Some(2)]);

    let (_file, replacement) = parquet_input(
        vec![Some(104), Some(103), Some(102), Some(101)],
        &DataType::Int32,
        &session,
        2,
    );
    let replaced = plan
        .replace_children(
            vec![replacement],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    assert!(replaced.is::<TopKReaderFilterExec>());
    assert_eq!(
        keys(&collect(replaced, session.task_ctx()).await.unwrap()),
        vec![Some(101), Some(102)],
        "a previous threshold of 2 must not discard the replacement's rows",
    );
}

#[tokio::test]
async fn replacing_child_rechecks_supported_key_type() {
    let session = session(2);
    let plan = Arc::new(wrapper(
        &sort(
            memory_input(vec![Some(1), Some(2)], &DataType::Int32),
            1,
            SortOptions::default(),
        ),
        &session,
    ));
    for (key_type, eligible) in [(DataType::Int64, true), (DataType::Float64, false)] {
        let replaced = Arc::clone(&plan)
            .replace_children(
                vec![memory_input(vec![Some(101), Some(102)], &key_type)],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        assert_eq!(replaced.is::<TopKReaderFilterExec>(), eligible);
        assert_eq!(
            keys(&collect(replaced, session.task_ctx()).await.unwrap()),
            vec![Some(101)]
        );
    }
}

#[tokio::test]
async fn dropping_runtime_stream_releases_predicate_and_sort_memory() {
    let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as _)
        .build_arc()
        .unwrap();
    let session =
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(2), runtime);
    let plan = wrapper(
        &sort(
            memory_input((0..20).rev().map(Some).collect(), &DataType::Int32),
            10,
            SortOptions::default(),
        ),
        &session,
    );
    let runtime = plan.build_runtime_sort().unwrap();
    let predicate = Arc::downgrade(&produced_filter(&runtime.sort));
    let mut stream = plan
        .execute_runtime_sort(runtime.sort, 0, session.task_ctx())
        .unwrap();
    assert!(stream.next().await.unwrap().unwrap().num_rows() > 0);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    assert!(
        predicate.upgrade().is_none(),
        "completed executions must not stay on the template"
    );
    assert!(plan.dynamic_expressions_produced().is_empty());
}

#[derive(Debug, Clone, Copy)]
enum Ending {
    Eof,
    Error,
    Pending,
}

#[derive(Debug)]
struct ControlledInput {
    batch: RecordBatch,
    ending: Ending,
}

impl datafusion::physical_plan::streaming::PartitionStream for ControlledInput {
    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        self.batch.schema_ref()
    }

    fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let tail: futures::stream::BoxStream<'static, Result<RecordBatch>> = match self.ending {
            Ending::Eof => futures::stream::empty().boxed(),
            Ending::Error => futures::stream::once(async {
                datafusion::common::exec_err!("controlled input failure")
            })
            .boxed(),
            Ending::Pending => futures::stream::pending().boxed(),
        };
        Box::pin(RecordBatchStreamAdapter::new(
            self.batch.schema(),
            futures::stream::iter(vec![Ok(self.batch.clone())]).chain(tail),
        ))
    }
}

#[tokio::test]
async fn eof_error_and_pending_cancellation_release_execution_state() {
    use datafusion::physical_plan::streaming::StreamingTableExec;
    use futures::FutureExt;

    for ending in [Ending::Eof, Ending::Error, Ending::Pending] {
        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as _)
            .build_arc()
            .unwrap();
        let session =
            SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(10), runtime);
        let batch = batch((0..100).rev().map(Some).collect(), &DataType::Int32);
        let input = StreamingTableExec::try_new(
            batch.schema(),
            vec![Arc::new(ControlledInput { batch, ending })],
            None,
            [],
            false,
            None,
        )
        .unwrap();
        let plan = wrapper(&sort(Arc::new(input), 10, SortOptions::default()), &session);
        let runtime = plan.build_runtime_sort().unwrap();
        assert!(!runtime.reader_filter_attached);
        let predicate = Arc::downgrade(&produced_filter(&runtime.sort));
        let mut stream = plan
            .execute_runtime_sort(runtime.sort, 0, session.task_ctx())
            .unwrap();
        match ending {
            Ending::Eof => {
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    batches.push(batch.unwrap());
                }
                assert_eq!(keys(&batches), (0..10).map(Some).collect::<Vec<_>>());
            }
            Ending::Error => {
                let error = stream.next().await.unwrap().unwrap_err();
                assert!(error.to_string().contains("controlled input failure"));
            }
            Ending::Pending => {
                assert!(stream.next().now_or_never().is_none());
                assert!(pool.reserved() > 0, "the pending heap must hold memory");
                assert!(predicate.upgrade().is_some());
                drop(stream);
                assert_eq!(pool.reserved(), 0);
                assert!(predicate.upgrade().is_none());
                continue;
            }
        }
        // Keep the terminated stream and permanent plan alive during these checks.
        assert_eq!(pool.reserved(), 0, "{ending:?}");
        assert!(predicate.upgrade().is_none(), "{ending:?}");
        assert!(stream.next().await.is_none());
    }
}
