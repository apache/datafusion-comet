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

//! Delivery to blocking callers, with cancellation that does not wait for a reader's wait.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use futures::StreamExt;
use tokio::runtime::Handle;

use crate::{lock, LocalQuery, LocalQueryStream};

pub enum QueryPoll {
    Batch(RecordBatch),
    Pending,
    Finished,
}

/// A started query, read by one blocking caller at a time. The reader polls the query's stream
/// on its own thread, so a batch reaches it straight from its partition's task, and waits by
/// parking rather than on a Tokio timer, which needs a free worker. Cancelling or dropping the
/// handle drops the query's stream on the calling thread and unread batches with it. The runtime
/// must remain alive until DataFusion finishes aborting the tasks its operators spawn.
pub struct QueryHandle {
    /// The query's stream. A reader holds it for the whole of a poll; `cancel` wakes the reader
    /// and so waits only for a poll of the stream it is already in.
    stream: Mutex<Option<LocalQueryStream>>,
    /// The thread of the reader in a poll, which `cancel` wakes.
    reader: Mutex<Option<Thread>>,
    cancelled: AtomicBool,
    runtime: Handle,
}

/// Wakes a reader parked in `QueryHandle::poll`.
struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

impl QueryHandle {
    pub fn start(query: LocalQuery, runtime: &Handle) -> Result<Self> {
        let stream = {
            let _guard = runtime.enter();
            query.execute()?
        };
        Ok(Self {
            stream: Mutex::new(Some(stream)),
            reader: Mutex::new(None),
            cancelled: AtomicBool::new(false),
            runtime: runtime.clone(),
        })
    }

    /// A bounded wait lets a JVM caller check Spark task interruption between polls.
    /// Only one reader may call this at a time.
    pub fn poll(&self, wait: Duration) -> Result<QueryPoll> {
        let mut stream = match self.stream.try_lock() {
            Ok(stream) => stream,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) if self.cancelled.load(Ordering::Acquire) => {
                return Err(cancelled())
            }
            Err(TryLockError::WouldBlock) => {
                return Err(DataFusionError::Execution(
                    "Concurrent local query result readers".into(),
                ))
            }
        };
        *lock(&self.reader) = Some(std::thread::current());
        let result = self.poll_stream(&mut stream, wait);
        *lock(&self.reader) = None;
        result
    }

    fn poll_stream(
        &self,
        stream: &mut Option<LocalQueryStream>,
        wait: Duration,
    ) -> Result<QueryPoll> {
        let deadline = Instant::now() + wait;
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        // Starting a partition executes its operators, which may spawn tasks.
        let _guard = self.runtime.enter();
        loop {
            // Checked after the reader is registered, so a cancel either sees the reader and
            // wakes it, or is seen here.
            if self.cancelled.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let Some(query) = stream.as_mut() else {
                return Err(cancelled());
            };
            // A panic must not unwind into the JNI caller.
            let next =
                std::panic::catch_unwind(AssertUnwindSafe(|| query.poll_next_unpin(&mut cx)))
                    .map_err(|_| DataFusionError::Execution("Local query panicked".into()))?;
            match next {
                Poll::Ready(Some(batch)) => return batch.map(QueryPoll::Batch),
                Poll::Ready(None) => return Ok(QueryPoll::Finished),
                Poll::Pending => (),
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(QueryPoll::Pending);
            }
            std::thread::park_timeout(deadline - now);
        }
    }

    /// Stops the query and drops its stream on the calling thread, rather than leaving that to
    /// a Tokio task: a task only runs once a worker is free, and every worker can be waiting,
    /// for instance in Spark's `acquireMemory` for memory this stream holds.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(reader) = lock(&self.reader).as_ref() {
            reader.unpark();
        }
        let stream = lock(&self.stream).take();
        // A panic while dropping must not unwind into the JNI caller.
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| drop(stream)));
    }
}

impl Drop for QueryHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn cancelled() -> DataFusionError {
    DataFusionError::Execution("Local query cancelled".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::test::exec::BlockingExec;

    #[test]
    fn cancel_wakes_a_waiting_reader() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let plan = BlockingExec::new(Arc::new(Schema::empty()), 2);
        let refs = plan.refs();
        let query = Arc::new(
            QueryHandle::start(
                LocalQuery::new(
                    Arc::new(plan),
                    Arc::new(TaskContext::default()),
                    usize::MAX,
                    usize::MAX,
                ),
                runtime.handle(),
            )
            .unwrap(),
        );
        let reader = Arc::clone(&query);
        let (sender, receiver) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            sender.send(reader.poll(Duration::from_secs(60))).unwrap();
        });
        while lock(&query.reader).is_none() {
            std::thread::yield_now();
        }
        assert!(query.poll(Duration::ZERO).is_err(), "a second reader");
        let started = Instant::now();
        query.cancel();
        let result = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(result.err().unwrap().to_string().contains("cancelled"));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(refs.strong_count(), 0);
        thread.join().unwrap();
    }
}
