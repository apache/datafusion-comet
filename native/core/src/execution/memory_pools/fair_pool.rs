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
    collections::HashMap,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    sync::Arc,
};

use jni::objects::{Global, JObject};

use super::spark_memory::SparkMemory;
use datafusion::common::resources_err;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::{
    common::DataFusionError,
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use parking_lot::Mutex;

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometFairMemoryPool {
    spark: SparkMemory,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
}

struct CometFairPoolState {
    used: usize,
    /// Bytes held by each registered consumer, keyed by [`MemoryConsumer::id`]. The sibling
    /// reservations that `new_empty()`, `split()` and `take()` create belong to the same consumer,
    /// so they count against one share. The pool keeps these totals itself because
    /// `reservation.size()` covers only one reservation, and DataFusion updates it after calling
    /// `try_grow` but before calling `shrink`.
    consumers: HashMap<usize, usize>,
}

impl CometFairPoolState {
    /// The bytes held by `reservation`'s consumer across all of its reservations.
    fn consumer_used(&mut self, reservation: &MemoryReservation) -> &mut usize {
        self.consumers
            .get_mut(&reservation.consumer().id())
            .expect("reservation's consumer is not registered with the pool")
    }
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("used", &state.used)
            .field("num", &state.consumers.len())
            .field("overcommit", &self.spark.overcommit())
            .finish()
    }
}

impl CometFairMemoryPool {
    pub fn new(
        task_memory_manager_handle: Arc<Global<JObject<'static>>>,
        pool_size: usize,
        task_attempt_id: i64,
    ) -> CometFairMemoryPool {
        Self::with_spark(
            SparkMemory::new(task_memory_manager_handle, task_attempt_id),
            pool_size,
        )
    }

    fn with_spark(spark: SparkMemory, pool_size: usize) -> CometFairMemoryPool {
        Self {
            spark,
            pool_size,
            state: Mutex::new(CometFairPoolState {
                used: 0,
                consumers: HashMap::new(),
            }),
        }
    }
}

impl Display for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        write!(
            f,
            "CometFairMemoryPool(pool_size={}, used={}, num={}, overcommit={})",
            self.pool_size,
            state.used,
            state.consumers.len(),
            self.spark.overcommit()
        )
    }
}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.state.lock().consumers.insert(consumer.id(), 0);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        // DataFusion unregisters a consumer after its last reservation has dropped and released
        // its bytes. If a release panicked, this runs while unwinding, so it must not panic too.
        self.state.lock().consumers.remove(&consumer.id());
    }

    /// Records memory that already exists, so it must not fail and ignores the fair and pool
    /// limits.
    /// See [`SparkMemory`].
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        if additional == 0 {
            return;
        }
        let mut state = self.state.lock();
        self.spark.acquire(additional);
        state.used = state.used.saturating_add(additional);
        let consumer_used = state.consumer_used(reservation);
        *consumer_used = consumer_used.saturating_add(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let mut state = self.state.lock();
            let consumer_used = *state.consumer_used(reservation);
            if consumer_used < subtractive {
                panic!(
                    "Failed to release {subtractive} bytes where only {consumer_used} bytes tracked for the consumer"
                )
            }
            self.spark
                .release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            state.used = state.used.checked_sub(subtractive).unwrap();
            *state.consumer_used(reservation) -= subtractive;
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            let mut state = self.state.lock();
            let num = state.consumers.len();
            let limit = self
                .pool_size
                .checked_div(num)
                .expect("overflow in checked_div");
            let consumer_used = *state.consumer_used(reservation);
            if limit < consumer_used.saturating_add(additional) {
                return resources_err!(
                    "Failed to acquire {additional} bytes where this consumer already holds {consumer_used} bytes and the fair limit is {limit} bytes, {num} registered ({} bytes overcommitted)",
                    self.spark.overcommit()
                );
            }
            // The shares alone do not bound the pool's total, because a consumer keeps what it
            // reserved before another consumer registered.
            let used = state.used;
            if self.pool_size < used.saturating_add(additional) {
                return resources_err!(
                    "Failed to acquire {additional} bytes where {used} bytes already reserved ({} bytes overcommitted) and the pool limit is {} bytes",
                    self.spark.overcommit(),
                    self.pool_size
                );
            }

            // A partial grant is handed back and refused, which triggers spilling in the caller.
            if let Err(refusal) = self.spark.try_acquire(additional)? {
                return resources_err!(
                    "Failed to acquire {} bytes plus {} bytes overcommitted, only got {} bytes. Reserved: {} bytes",
                    additional,
                    refusal.overcommit,
                    refusal.granted,
                    state.used
                );
            }
            state.used = state
                .used
                .checked_add(additional)
                .expect("overflow in checked_add");
            *state.consumer_used(reservation) += additional;
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }
}

#[cfg(test)]
mod tests {
    use super::super::spark_memory::fake::FakeSpark;
    use super::*;

    #[test]
    fn grow_past_the_fair_limit_is_recorded_and_refuses_the_next_try_grow() {
        let fake = FakeSpark::with(100);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let reservation = MemoryConsumer::new("smj").register(&pool);

        // Past both the fair limit and what Spark will grant.
        reservation.grow(150);
        assert_eq!(pool.reserved(), 150);
        assert_eq!(fake.held(), 100);
        assert!(reservation.try_grow(1).is_err());

        drop(reservation);
        assert_eq!(pool.reserved(), 0);
        // Spark gets back exactly the 100 bytes it granted.
        assert_eq!(fake.released(), vec![100]);
    }

    #[test]
    fn each_consumer_is_limited_to_its_own_share() {
        // Spark grants everything, so only the pool's own checks refuse.
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let first = MemoryConsumer::new("first").register(&pool);
        let second = MemoryConsumer::new("second").register(&pool);

        // Each consumer's share is 50 bytes, whatever the other one holds.
        first.try_grow(40).unwrap();
        second.try_grow(20).unwrap();
        first.try_grow(10).unwrap();
        assert!(first.try_grow(1).is_err());
        second.try_grow(30).unwrap();
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
        assert_eq!(fake.held(), 100);
    }

    #[test]
    fn a_consumer_registered_late_is_limited_by_the_pool_total() {
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 90));
        let first = MemoryConsumer::new("first").register(&pool);
        // Alone, the first consumer's share is the whole pool.
        first.try_grow(60).unwrap();

        // A second consumer halves both shares, but the first keeps the 60 bytes it holds.
        let second = MemoryConsumer::new("second").register(&pool);
        assert!(first.try_grow(1).is_err());
        second.try_grow(30).unwrap();
        // The second consumer is 15 bytes under its share, but the pool is full.
        assert!(second.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 90);

        // Once the first consumer releases memory, the second can use the rest of its share.
        first.shrink(30);
        second.try_grow(15).unwrap();
        assert!(second.try_grow(1).is_err());

        // Unregistering the second consumer gives the first the whole pool again.
        drop(second);
        first.try_grow(60).unwrap();
        assert_eq!(pool.reserved(), 90);
        assert_eq!(fake.held(), 90);
    }

    #[test]
    fn sibling_reservations_draw_on_one_share() {
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let first = MemoryConsumer::new("first").register(&pool);
        // Like the reservation that a sort's streaming merge creates for each batch it reads.
        let sibling = first.new_empty();
        let second = MemoryConsumer::new("second").register(&pool);

        // Both of the first consumer's reservations draw on its one 50-byte share.
        first.try_grow(40).unwrap();
        assert!(sibling.try_grow(40).is_err());
        sibling.try_grow(10).unwrap();
        assert!(first.try_grow(1).is_err());
        assert!(sibling.try_grow(1).is_err());

        // So the second consumer can still reserve its whole share.
        second.try_grow(50).unwrap();
        assert_eq!(pool.reserved(), 100);

        // Dropping a reservation returns its bytes to the consumer's share. The consumer stays
        // registered while its other reservation lives.
        drop(first);
        sibling.try_grow(40).unwrap();
        assert!(sibling.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 100);
        assert_eq!(fake.held(), 100);
    }

    #[test]
    fn split_and_take_keep_the_bytes_on_their_consumer() {
        let fake = FakeSpark::with(usize::MAX);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(CometFairMemoryPool::with_spark(fake.memory(), 100));
        let mut first = MemoryConsumer::new("first").register(&pool);
        let _second = MemoryConsumer::new("second").register(&pool);

        first.try_grow(50).unwrap();
        let split = first.split(20);
        let taken = first.take();
        // The consumer's 50 bytes now sit in split and taken, and the emptied reservation gets no
        // share of its own.
        for reservation in [&first, &split, &taken] {
            assert!(reservation.try_grow(1).is_err());
        }

        // Shrinking one reservation makes room in the share for another.
        split.shrink(10);
        first.try_grow(10).unwrap();
        assert!(taken.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 50);
        assert_eq!(fake.held(), 50);
    }
}
