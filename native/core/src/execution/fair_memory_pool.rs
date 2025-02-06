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
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::Arc,
};

use jni::objects::GlobalRef;

use crate::{
    errors::CometResult,
    jvm_bridge::{jni_call, JVMClasses},
};
use datafusion::{
    common::{resources_datafusion_err, DataFusionError},
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use datafusion_execution::memory_pool::MemoryConsumer;
use parking_lot::Mutex;

/// A DataFusion fair `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometFairMemoryPool {
    task_memory_manager_handle: Arc<GlobalRef>,
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
            .finish()
    }
}

impl CometFairMemoryPool {
    pub fn new(
        task_memory_manager_handle: Arc<GlobalRef>,
        pool_size: usize,
    ) -> CometFairMemoryPool {
        Self {
            task_memory_manager_handle,
            pool_size,
            state: Mutex::new(CometFairPoolState { used: 0, num: 0 }),
        }
    }

    fn acquire(&self, additional: usize) -> CometResult<i64> {
        let mut env = JVMClasses::get_env()?;
        let handle = self.task_memory_manager_handle.as_obj();
        unsafe {
            jni_call!(&mut env,
              comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)
        }
    }

    fn release(&self, size: usize) -> CometResult<()> {
        let mut env = JVMClasses::get_env()?;
        let handle = self.task_memory_manager_handle.as_obj();
        unsafe {
            jni_call!(&mut env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        }
    }
}

unsafe impl Send for CometFairMemoryPool {}
unsafe impl Sync for CometFairMemoryPool {}

impl MemoryPool for CometFairMemoryPool {
    fn register(&self, _: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.num = state.num.checked_add(1).unwrap();
    }

    fn unregister(&self, _: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.num = state.num.checked_sub(1).unwrap();
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional).unwrap();
    }

    fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
        if subtractive > 0 {
            let mut state = self.state.lock();
            let size = reservation.size();
            if size < subtractive {
                panic!("Failed to release {subtractive} bytes where only {size} bytes reserved")
            }
            self.release(subtractive)
                .unwrap_or_else(|_| panic!("Failed to release {} bytes", subtractive));
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
            let limit = self.pool_size.checked_div(num).unwrap();
            let size = reservation.size();
            if limit < size + additional {
                return resources_err!(
                    "Failed to acquire {additional} bytes where {size} bytes already reserved and the fair limit is {limit} bytes, {num} registered"
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
            state.used = state.used.checked_add(additional).unwrap();
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }
}
