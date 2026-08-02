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

use crate::{errors::CometResult, jvm_bridge::JVMClasses};
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
    task_memory_manager_handle: Arc<Global<JObject<'static>>>,
    pool_size: usize,
    state: Mutex<CometFairPoolState>,
}

struct CometFairPoolState {
    used: usize,
    num: usize,
}

/// Result of the fair-share check for a single `try_grow` request.
struct FairShareCheck {
    /// The share of the pool available to each registered consumer.
    limit: usize,
    /// Whether the requesting consumer may grow by `additional` without exceeding `limit`.
    fits: bool,
}

/// Whether a consumer already holding `reserved` bytes may grow by `additional`, given `num`
/// consumers sharing `pool_size`.
///
/// The share is per consumer, so `reserved` must be the requesting consumer's own usage and not
/// the pool-wide total: passing the pool-wide total would cap the entire pool at a single
/// consumer's share, shrinking usable memory linearly as consumers register.
///
/// `num` cannot be zero while a reservation exists, because a consumer registers before it can
/// grow. Fall back to the whole pool rather than dividing by zero if that ever changes.
fn check_fair_share(
    pool_size: usize,
    num: usize,
    reserved: usize,
    additional: usize,
) -> FairShareCheck {
    let limit = pool_size.checked_div(num).unwrap_or(pool_size);
    FairShareCheck {
        limit,
        fits: reserved.saturating_add(additional) <= limit,
    }
}

impl Debug for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        f.debug_struct("CometFairMemoryPool")
            .field("pool_size", &self.pool_size)
            .field("used", &state.used)
            .field("num", &state.num)
            .finish()
    }
}

impl CometFairMemoryPool {
    pub fn new(
        task_memory_manager_handle: Arc<Global<JObject<'static>>>,
        pool_size: usize,
    ) -> CometFairMemoryPool {
        Self {
            task_memory_manager_handle,
            pool_size,
            state: Mutex::new(CometFairPoolState { used: 0, num: 0 }),
        }
    }

    fn acquire(&self, additional: usize) -> CometResult<i64> {
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env,
              comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)
        })
    }

    fn release(&self, size: usize) -> CometResult<()> {
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        })
    }
}

impl Display for CometFairMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let state = self.state.lock();
        write!(
            f,
            "CometFairMemoryPool(pool_size={}, used={}, num={})",
            self.pool_size, state.used, state.num
        )
    }
}

unsafe impl Send for CometFairMemoryPool {}
unsafe impl Sync for CometFairMemoryPool {}

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

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.try_grow(_reservation, additional).unwrap();
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
            self.release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {subtractive} bytes"));
            state.used = state.used.checked_sub(subtractive).unwrap();
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        if additional > 0 {
            let mut state = self.state.lock();
            let num = state.num;
            // `MemoryReservation::try_grow` calls this method before adding `additional` to the
            // reservation's atomic size, so `reservation.size()` is this consumer's usage prior
            // to the current request, which is what the fair share applies to.
            let check = check_fair_share(self.pool_size, num, reservation.size(), additional);
            if !check.fits {
                return resources_err!(
                    "Failed to acquire {} bytes for {} where {} bytes are already reserved by that consumer and the fair limit is {} bytes, {} registered ({} bytes reserved pool-wide)",
                    additional,
                    reservation.consumer().name(),
                    reservation.size(),
                    check.limit,
                    num,
                    state.used
                );
            }

            let acquired = self.acquire(additional)?;
            // If the number of bytes we acquired is less than the requested, return an error,
            // and hopefully will trigger spilling from the caller side.
            if acquired < additional as i64 {
                // Release the acquired bytes before throwing error
                self.release(acquired as usize)?;

                return resources_err!(
                    "Failed to acquire {} bytes, only got {} bytes. Reserved: {} bytes",
                    additional,
                    acquired,
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
    use super::check_fair_share;

    const POOL: usize = 1000;

    #[test]
    fn sole_consumer_may_use_whole_pool() {
        assert!(check_fair_share(POOL, 1, 0, POOL).fits);
        assert!(!check_fair_share(POOL, 1, 0, POOL + 1).fits);
    }

    #[test]
    fn share_is_divided_evenly_between_consumers() {
        assert_eq!(check_fair_share(POOL, 1, 0, 1).limit, POOL);
        assert_eq!(check_fair_share(POOL, 2, 0, 1).limit, POOL / 2);
        assert_eq!(check_fair_share(POOL, 10, 0, 1).limit, POOL / 10);
    }

    #[test]
    fn each_consumer_may_reach_its_own_share() {
        // Two consumers sharing the pool: each may hold 500 bytes, totalling the full pool.
        // Regression guard for comparing the pool-wide total against the per-consumer share,
        // which rejected the second consumer once the first had reached 500 and so capped the
        // whole pool at 500.
        assert!(check_fair_share(POOL, 2, 0, 500).fits);
        assert!(check_fair_share(POOL, 2, 400, 100).fits);
    }

    #[test]
    fn consumer_may_not_exceed_its_own_share() {
        assert!(!check_fair_share(POOL, 2, 500, 1).fits);
        assert!(!check_fair_share(POOL, 2, 0, 501).fits);
        assert!(!check_fair_share(POOL, 10, 100, 1).fits);
    }

    #[test]
    fn no_registered_consumers_does_not_panic() {
        // Unreachable in practice -- a consumer registers before it can grow -- but must not
        // divide by zero if that ever changes.
        assert_eq!(check_fair_share(POOL, 0, 0, 1).limit, POOL);
    }

    #[test]
    fn oversized_request_does_not_overflow() {
        assert!(!check_fair_share(POOL, 2, usize::MAX - 1, 10).fits);
    }
}
