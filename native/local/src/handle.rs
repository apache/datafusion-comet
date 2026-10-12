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

//! Bounded handoff to blocking callers, with cancellation independent of the reader lock.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use futures::future::poll_fn;
use futures::{FutureExt, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::{lock, LocalQuery, LocalQueryStream};

struct Delivery {
    batch: Result<RecordBatch>,
    _permit: OwnedSemaphorePermit,
}

pub enum QueryPoll {
    Batch(RecordBatch),
    Pending,
    Finished,
}

/// Holds at most one queued batch, plus the batch owned by the external consumer.
/// Cancelling or dropping the handle drops the query's stream on the calling thread and
/// unread batches with it. The runtime must remain alive until DataFusion finishes aborting
/// the tasks its operators spawn.
pub struct QueryHandle {
    receiver: Mutex<mpsc::Receiver<Delivery>>,
    /// The query's stream. The producer locks it only while polling it, so `cancel` can take it
    /// whenever the producer is waiting.
    stream: Arc<Mutex<Option<LocalQueryStream>>>,
    producer: JoinHandle<()>,
    cancelled: AtomicBool,
    completed: Arc<AtomicBool>,
}

impl QueryHandle {
    pub fn start(query: LocalQuery, runtime: &Handle) -> Result<Self> {
        let stream = {
            let _guard = runtime.enter();
            query.execute()?
        };
        let (sender, receiver) = mpsc::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let worker_completed = Arc::clone(&completed);
        let stream = Arc::new(Mutex::new(Some(stream)));
        let polled = Arc::clone(&stream);
        let producer = runtime.spawn(async move {
            let capacity = Arc::new(Semaphore::new(1));
            let result = AssertUnwindSafe(async {
                loop {
                    let permit = Arc::clone(&capacity)
                        .acquire_owned()
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    let next = poll_fn(|cx| match lock(&polled).as_mut() {
                        Some(stream) => stream.poll_next_unpin(cx).map(Some),
                        // Cancelled: the stream was taken, so this is not completion.
                        None => Poll::Ready(None),
                    })
                    .await;
                    let Some(next) = next else {
                        return Ok(false);
                    };
                    match next {
                        Some(batch) => {
                            let failed = batch.is_err();
                            if sender
                                .send(Delivery {
                                    batch,
                                    _permit: permit,
                                })
                                .is_err()
                                || failed
                            {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                Ok::<_, DataFusionError>(true)
            })
            .catch_unwind()
            .await;
            let error = match result {
                Ok(Ok(true)) => None,
                Ok(Ok(false)) => return,
                Ok(Err(e)) => Some(e),
                Err(_) => Some(DataFusionError::Execution(
                    "Local query worker panicked".into(),
                )),
            };
            if let Some(error) = error {
                if let Ok(permit) = capacity.acquire_owned().await {
                    let _ = sender.send(Delivery {
                        batch: Err(error),
                        _permit: permit,
                    });
                }
            }
            // Publish before dropping sender. Runtime shutdown/abort skips this store;
            // channel disconnection alone must never turn a truncated result into EOF.
            worker_completed.store(true, Ordering::Release);
        });
        Ok(Self {
            receiver: Mutex::new(receiver),
            stream,
            producer,
            cancelled: AtomicBool::new(false),
            completed,
        })
    }

    /// A bounded wait lets a JVM caller check Spark task interruption between polls.
    /// Only one reader may call this at a time. Cancellation never needs this lock.
    pub fn poll(&self, wait: Duration) -> Result<QueryPoll> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution("Local query cancelled".into()));
        }
        let receiver = self.receiver.try_lock().map_err(|_| {
            DataFusionError::Execution("Concurrent local query result readers".into())
        })?;
        let delivery = receiver.recv_timeout(wait);
        if self.cancelled.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution("Local query cancelled".into()));
        }
        match delivery {
            Ok(delivery) => delivery.batch.map(QueryPoll::Batch),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(QueryPoll::Pending),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if self.completed.load(Ordering::Acquire) {
                    Ok(QueryPoll::Finished)
                } else {
                    Err(DataFusionError::Execution(
                        "Local query worker stopped without completing".into(),
                    ))
                }
            }
        }
    }

    /// Stops the query and drops its stream on the calling thread, rather than leaving that to
    /// the cancelled producer: a cancellation only runs once a Tokio worker is free, and every
    /// worker can be waiting, for instance in Spark's `acquireMemory` for memory this stream
    /// holds. Taking the stream waits only for a poll the producer is already in.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.producer.abort();
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::test::exec::BlockingExec;

    #[test]
    fn cancel_interrupts_a_blocked_reader_without_its_mutex() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let plan = Arc::new(BlockingExec::new(Arc::new(Schema::empty()), 2));
        let query = Arc::new(
            QueryHandle::start(
                LocalQuery::new(plan, Arc::new(TaskContext::default())),
                runtime.handle(),
            )
            .unwrap(),
        );
        let reader = Arc::clone(&query);
        let (started_tx, started_rx) = mpsc::channel();
        let (sender, receiver) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            // Hold exactly the receiver lock used by poll. A handshake avoids racing
            // poll's try_lock with a test thread inspecting that same lock.
            let incoming = reader.receiver.lock().unwrap();
            started_tx.send(()).unwrap();
            let result = incoming.recv_timeout(Duration::from_secs(60));
            sender
                .send(matches!(result, Err(mpsc::RecvTimeoutError::Disconnected)))
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(query.poll(Duration::ZERO).is_err());
        query.cancel();
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        thread.join().unwrap();
    }
}
