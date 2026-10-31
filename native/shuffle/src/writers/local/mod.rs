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

pub(crate) mod local_partition_writer;
mod spill;

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::MemoryReservation;

const FALLBACK_BUFFER_SIZE: usize = 8 * 1024;

/// Whether `error` is the pool refusing memory. Other errors, such as an exception Spark threw
/// while spilling another consumer, must fail the task.
fn is_refusal(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
}

/// Returns the capacity to allocate. A refused size falls back to a small buffer charged with
/// `grow`, so the task carries on and the reservation still matches what is held.
fn reserve_buffer(
    reservation: &MemoryReservation,
    size: usize,
) -> datafusion::common::Result<usize> {
    match reservation.try_grow(size) {
        Ok(()) => Ok(size),
        Err(error) if is_refusal(&error) => {
            let fallback = size.min(FALLBACK_BUFFER_SIZE);
            reservation.grow(fallback);
            Ok(fallback)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) mod erroring_pool {
    use datafusion::common::DataFusionError;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, MemoryReservation};
    use std::fmt;

    /// A greedy pool whose `try_grow` fails with `error()` for requests of `fail_at` bytes or more.
    #[derive(Debug)]
    pub(crate) struct ErroringPool {
        inner: GreedyMemoryPool,
        fail_at: usize,
        error: fn() -> DataFusionError,
    }

    impl ErroringPool {
        pub(crate) fn new(limit: usize, fail_at: usize, error: fn() -> DataFusionError) -> Self {
            Self {
                inner: GreedyMemoryPool::new(limit),
                fail_at,
                error,
            }
        }
    }

    /// What a Spark-backed pool returns when Spark throws.
    pub(crate) fn external() -> DataFusionError {
        DataFusionError::External(Box::new(std::io::Error::other("spill of a peer failed")))
    }

    /// A refusal under a context, so the check has to look at the root.
    pub(crate) fn refusal() -> DataFusionError {
        DataFusionError::ResourcesExhausted("refused".to_string()).context("wrapped")
    }

    impl fmt::Display for ErroringPool {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "erroring")
        }
    }

    impl MemoryPool for ErroringPool {
        fn name(&self) -> &str {
            "erroring"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional)
        }

        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.inner.shrink(reservation, shrink)
        }

        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> datafusion::common::Result<()> {
            if additional >= self.fail_at {
                return Err((self.error)());
            }
            self.inner.try_grow(reservation, additional)
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::erroring_pool::{external, refusal, ErroringPool};
    use super::*;
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
    use std::sync::Arc;

    #[test]
    fn reserve_buffer_falls_back_only_on_refusal() {
        let size = 1 << 20;
        let pool: Arc<dyn MemoryPool> = Arc::new(ErroringPool::new(1 << 30, size, refusal));
        let reservation = MemoryConsumer::new("test").register(&pool);
        assert_eq!(
            reserve_buffer(&reservation, size).unwrap(),
            FALLBACK_BUFFER_SIZE
        );
        assert_eq!(reservation.size(), FALLBACK_BUFFER_SIZE);
    }

    #[test]
    fn reserve_buffer_propagates_other_pool_errors() {
        let size = 1 << 20;
        let pool: Arc<dyn MemoryPool> = Arc::new(ErroringPool::new(1 << 30, size, external));
        let reservation = MemoryConsumer::new("test").register(&pool);
        let error = reserve_buffer(&reservation, size).expect_err("an exception is not a refusal");
        assert!(
            matches!(error, DataFusionError::External(_)),
            "unexpected error: {error}"
        );
        assert_eq!((reservation.size(), pool.reserved()), (0, 0));
    }
}
