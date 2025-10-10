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

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use log::info;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct LoggingPool {
    task_attempt_id: u64,
    pool: Arc<dyn MemoryPool>,
}

impl LoggingPool {
    pub fn new(task_attempt_id: u64, pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            task_attempt_id,
            pool,
        }
    }
}

impl MemoryPool for LoggingPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.pool.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.pool.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        info!(
            "[Task {}] MemoryPool[{}].grow({})",
            self.task_attempt_id,
            reservation.consumer().name(),
            additional
        );
        self.pool.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        info!(
            "[Task {}] MemoryPool[{}].shrink({})",
            self.task_attempt_id,
            reservation.consumer().name(),
            shrink
        );
        self.pool.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        match self.pool.try_grow(reservation, additional) {
            Ok(_) => {
                info!(
                    "[Task {}] MemoryPool[{}].try_grow({}) returning Ok",
                    self.task_attempt_id,
                    reservation.consumer().name(),
                    additional
                );
                Ok(())
            }
            Err(e) => {
                info!(
                    "[Task {}] MemoryPool[{}].try_grow({}) returning Err: {e:?}",
                    self.task_attempt_id,
                    reservation.consumer().name(),
                    additional
                );
                Err(e)
            }
        }
    }

    fn reserved(&self) -> usize {
        self.pool.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.pool.memory_limit()
    }
}
