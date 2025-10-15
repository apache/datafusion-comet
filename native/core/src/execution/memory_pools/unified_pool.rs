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
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

use crate::{
    errors::CometResult,
    jvm_bridge::{jni_call, JVMClasses},
};
use datafusion::{
    common::{resources_datafusion_err, DataFusionError},
    execution::memory_pool::{MemoryPool, MemoryReservation},
};
use jni::objects::GlobalRef;
use log::warn;

/// A DataFusion `MemoryPool` implementation for Comet that delegates to
/// Spark's off-heap executor memory pool via JNI by calling
/// [`crate::jvm_bridge::CometTaskMemoryManager`].
pub struct CometUnifiedMemoryPool {
    task_memory_manager_handle: Arc<GlobalRef>,
    used: AtomicUsize,
    task_attempt_id: i64,
}

impl Debug for CometUnifiedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CometUnifiedMemoryPool")
            .field("used", &self.used.load(Relaxed))
            .finish()
    }
}

impl CometUnifiedMemoryPool {
    pub fn new(
        task_memory_manager_handle: Arc<GlobalRef>,
        task_attempt_id: i64,
    ) -> CometUnifiedMemoryPool {
        Self {
            task_memory_manager_handle,
            task_attempt_id,
            used: AtomicUsize::new(0),
        }
    }

    /// Request memory from Spark's off-heap memory pool via JNI
    fn acquire_from_spark(&self, additional: usize) -> CometResult<i64> {
        let mut env = JVMClasses::get_env()?;
        let handle = self.task_memory_manager_handle.as_obj();
        unsafe {
            jni_call!(&mut env,
              comet_task_memory_manager(handle).acquire_memory(additional as i64) -> i64)
        }
    }

    /// Release memory to Spark's off-heap memory pool via JNI
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
        let used = self.used.load(Relaxed);
        if used != 0 {
            warn!(
                "Task {} dropped CometUnifiedMemoryPool with {used} bytes still reserved",
                self.task_attempt_id
            );
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
        if let Err(e) = self.release_to_spark(size) {
            panic!(
                "Task {} failed to return {size} bytes to Spark: {e:?}",
                self.task_attempt_id
            );
        }
        if let Err(prev) = self
            .used
            .fetch_update(Relaxed, Relaxed, |old| old.checked_sub(size))
        {
            panic!(
                "Task {} overflow when releasing {size} of {prev} bytes",
                self.task_attempt_id
            );
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
                    "Task {} failed to acquire {} bytes, only got {}. Reserved: {}",
                    self.task_attempt_id,
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
                    self.task_attempt_id,
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
