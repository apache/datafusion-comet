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

/// A DataFusion `MemoryPool` implementation for Comet. Internally this is
/// implemented via delegating calls to [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometUnifiedMemoryPool {
    task_memory_manager_handle: Arc<GlobalRef>,
    used: usize,
}

impl Debug for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CometMemoryPool")
            .field("used", &self.used)
            .finish()
    }
}

impl CometUnifiedMemoryPool {
    pub fn new(task_memory_manager_handle: Arc<GlobalRef>) -> CometUnifiedMemoryPool {
        Self {
            task_memory_manager_handle,
            used: 0,
        }
    }

    fn acquire_from_spark(&self, additional: usize) -> CometResult<i64> {
        let mut env = JVMClasses::get_env()?;
        let handle = self.task_memory_manager_handle.as_obj();
        unsafe {
            jni_call!(&mut env,
              comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)
        }
    }

    fn release_to_spark(&self, size: usize) -> CometResult<()> {
        let mut env = JVMClasses::get_env()?;
        let handle = self.task_memory_manager_handle.as_obj();
        unsafe {
            jni_call!(&mut env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        }
    }
}

impl Drop for CometUnifiedMemoryPool {
    fn drop(&mut self) {
        let used = self.used;
        if used != 0 {
            log::warn!("CometMemoryPool dropped with {used} bytes still reserved");
        }
    }
}

unsafe impl Send for CometUnifiedMemoryPool {}
unsafe impl Sync for CometUnifiedMemoryPool {}

impl MemoryPool for CometUnifiedMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional).unwrap();
    }

    fn shrink(&self, _: &MemoryReservation, size: usize) {
        self.release_to_spark(size)
            .unwrap_or_else(|_| panic!("Failed to release {size} bytes"));
        if self.used.checked_sub(size).is_none() {
            panic!("Failed to release {size} bytes due to overflow")
        }
    }

    fn try_grow(&self, _: &MemoryReservation, additional: usize) -> Result<(), DataFusionError> {
        if additional > 0 {
            let acquired = self.acquire_from_spark(additional)?;
            // If the number of bytes we acquired is less than the requested, return an error,
            // and hopefully will trigger spilling from the caller side.
            if acquired < additional as i64 {
                // Release the acquired bytes before throwing error
                self.release_to_spark(acquired as usize)?;

                return Err(resources_datafusion_err!(
                    "Failed to acquire {} bytes, only got {}. Reserved: {}",
                    additional,
                    acquired,
                    self.reserved()
                ));
            }
            if self.used.checked_add(acquired as usize).is_none() {
                return Err(resources_datafusion_err!(
                    "Failed to acquire {} bytes due to overflow",
                    additional
                ));
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used
    }
}
