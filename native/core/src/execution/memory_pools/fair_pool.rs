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
    num: usize,
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("used", &state.used)
            .field("num", &state.num)
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
            state: Mutex::new(CometFairPoolState { used: 0, num: 0 }),
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
            state.num,
            self.spark.overcommit()
        )
    }
}

impl MemoryPool for CometFairMemoryPool {
    fn name(&self) -> &str {
        "CometFairMemoryPool"
    }

    fn register(&self, _: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.num = state
            .num
            .checked_add(1)
            .expect("unexpected amount of register happened");
    }

    fn unregister(&self, _: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.num = state
            .num
            .checked_sub(1)
            .expect("unexpected amount of unregister happened");
    }

    /// Records memory that already exists, so it must not fail and ignores the fair limit.
    /// See [`SparkMemory`].
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        if additional == 0 {
            return;
        }
        let mut state = self.state.lock();
        self.spark.acquire(additional);
        state.used = state.used.saturating_add(additional);
    }

    fn shrink(&self, _reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let mut state = self.state.lock();
            // We don't use reservation.size() here because DataFusion 53+ decrements
            // the reservation's atomic size before calling pool.shrink(), so it would
            // reflect the post-shrink value rather than the pre-shrink value.
            if state.used < subtractive {
                panic!(
                    "Failed to release {subtractive} bytes where only {} bytes tracked by pool",
                    state.used
                )
            }
            self.spark
                .release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            state.used = state.used.checked_sub(subtractive).unwrap();
        }
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            let mut state = self.state.lock();
            let num = state.num;
            let limit = self
                .pool_size
                .checked_div(num)
                .expect("overflow in checked_div");
            // We use state.used instead of reservation.size() because DataFusion 53+
            // calls pool.try_grow() before incrementing the reservation's atomic size,
            // so reservation.size() would not include prior grows.
            let used = state.used;
            if limit < used + additional {
                return resources_err!(
                    "Failed to acquire {additional} bytes where {used} bytes already reserved ({} bytes overcommitted) and the fair limit is {limit} bytes, {num} registered",
                    self.spark.overcommit()
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
}
