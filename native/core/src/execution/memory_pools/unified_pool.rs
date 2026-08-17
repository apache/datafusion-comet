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

use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use crate::execution::memory_pools::spark_client::SparkMemoryClient;
use datafusion::{
    common::{resources_datafusion_err, DataFusionError},
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use log::warn;

/// A DataFusion `MemoryPool` implementation for Comet that delegates to
/// Spark's off-heap executor memory pool via JNI by calling
/// [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometUnifiedMemoryPool {
    client: SparkMemoryClient,
    used: AtomicUsize,
}

impl Debug for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CometUnifiedMemoryPool")
            .field("used", &self.used.load(Relaxed))
            .finish()
    }
}

impl CometUnifiedMemoryPool {
    pub fn new(client: SparkMemoryClient) -> CometUnifiedMemoryPool {
        Self {
            client,
            used: AtomicUsize::new(0),
        }
    }

    fn task_attempt_id(&self) -> i64 {
        self.client.task_attempt_id()
    }
}

impl Drop for CometUnifiedMemoryPool {
    fn drop(&mut self) {
        self.client.log_stats(self.name());
        let used = self.used.load(Relaxed);
        if used != 0 {
            warn!(
                "Task {} dropped CometUnifiedMemoryPool with {used} bytes still reserved",
                self.task_attempt_id()
            );
        }
    }
}

impl Display for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "CometUnifiedMemoryPool(used={})",
            self.used.load(Relaxed)
        )
    }
}

impl MemoryPool for CometUnifiedMemoryPool {
    fn name(&self) -> &str {
        "CometUnifiedMemoryPool"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional).unwrap();
    }

    fn shrink(&self, _: &MemoryReservation, size: usize) {
        if let Err(e) = self.client.release(size) {
            panic!(
                "Task {} failed to return {size} bytes to Spark: {e:?}",
                self.task_attempt_id()
            );
        }
        if let Err(prev) = self
            .used
            .fetch_update(Relaxed, Relaxed, |old| old.checked_sub(size))
        {
            panic!(
                "Task {} overflow when releasing {size} of {prev} bytes",
                self.task_attempt_id()
            );
        }
    }

    fn try_grow(&self, _: &MemoryReservation, additional: usize) -> Result<(), DataFusionError> {
        if additional > 0 {
            let acquired = self.client.acquire(additional)?;
            // If the number of bytes we acquired is less than the requested, return an error,
            // and hopefully will trigger spilling from the caller side.
            if acquired < additional as i64 {
                // Release the acquired bytes before throwing error
                self.client.release(acquired as usize)?;

                return Err(resources_datafusion_err!(
                    "Task {} failed to acquire {} bytes, only got {}. Reserved: {}",
                    self.task_attempt_id(),
                    additional,
                    acquired,
                    self.reserved()
                ));
            }
            if let Err(prev) = self
                .used
                .fetch_update(Relaxed, Relaxed, |old| old.checked_add(acquired as usize))
            {
                return Err(resources_datafusion_err!(
                    "Task {} failed to acquire {} bytes due to overflow. Reserved: {}",
                    self.task_attempt_id(),
                    additional,
                    prev
                ));
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::memory_pools::spark_client::tests::TestMemoryBackend;
    use datafusion::execution::memory_pool::MemoryConsumer;
    use std::sync::Arc;

    /// Returns the pool both as a trait object, for registering consumers, and
    /// as the concrete type, for inspecting the statistics it recorded.
    fn pool(
        capacity: usize,
    ) -> (
        Arc<dyn MemoryPool>,
        Arc<CometUnifiedMemoryPool>,
        Arc<TestMemoryBackend>,
    ) {
        let backend = Arc::new(TestMemoryBackend::new(capacity));
        let client = SparkMemoryClient::with_backend(Arc::clone(&backend) as _, 1);
        let pool = Arc::new(CometUnifiedMemoryPool::new(client));
        (Arc::clone(&pool) as _, pool, backend)
    }

    #[test]
    fn grow_and_shrink_track_spark_grants() {
        let (pool, unified, backend) = pool(1024);
        let reservation = MemoryConsumer::new("test").register(&pool);

        reservation.try_grow(100).unwrap();
        assert_eq!(pool.reserved(), 100);
        assert_eq!(backend.granted(), 100);

        reservation.try_grow(200).unwrap();
        assert_eq!(pool.reserved(), 300);
        assert_eq!(backend.granted(), 300);

        reservation.shrink(300);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(backend.granted(), 0);

        let stats = unified.client.stats();
        assert_eq!(stats.acquire_calls(), 2);
        assert_eq!(stats.release_calls(), 1);
    }

    #[test]
    fn short_grant_is_returned_to_spark_and_reported_as_an_error() {
        let (pool, _unified, backend) = pool(100);
        let reservation = MemoryConsumer::new("test").register(&pool);

        let err = reservation.try_grow(150).unwrap_err();
        assert!(
            err.to_string().contains("failed to acquire 150 bytes"),
            "unexpected error: {err}"
        );
        // The partial grant must not be retained, otherwise the pool would hold
        // memory that no reservation accounts for.
        assert_eq!(backend.granted(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn every_grow_and_shrink_reaches_spark() {
        // Records the current behaviour that issue #5383 is about: one round-trip
        // per grow and per shrink, with no batching or hysteresis.
        let (pool, unified, _) = pool(1024);
        let reservation = MemoryConsumer::new("test").register(&pool);

        for _ in 0..10 {
            reservation.try_grow(10).unwrap();
            reservation.shrink(10);
        }

        let stats = unified.client.stats();
        assert_eq!(stats.acquire_calls(), 10);
        assert_eq!(stats.release_calls(), 10);
    }
}
