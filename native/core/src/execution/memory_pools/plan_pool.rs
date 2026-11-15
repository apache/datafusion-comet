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
use parking_lot::{Condvar, Mutex};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

/// The pool one native plan reserves memory through. It passes every call on to the pool it
/// wraps, and counts the bytes the plan's reservations hold, so that releasing the plan can wait
/// until all of them have been returned.
///
/// Dropping a plan does not drop every reservation it made. A task that one of its operators
/// spawned, such as the one a sort's merge reads each sorted run through, is only aborted when the
/// stream that owns it is dropped, and it drops the reservations it holds the next time it yields.
#[derive(Debug)]
pub(crate) struct PlanMemoryPool {
    inner: Arc<dyn MemoryPool>,
    /// Bytes the plan's reservations hold. They are counted from before a reservation asks the
    /// wrapped pool for them until after it has returned them, so this cannot read zero while any
    /// of them are still out.
    held: Mutex<usize>,
    /// Notified when `held` falls to zero.
    released: Condvar,
}

impl PlanMemoryPool {
    pub(crate) fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            held: Mutex::new(0),
            released: Condvar::new(),
        }
    }

    /// Parks the calling thread until the plan's reservations hold nothing, or until `deadline`,
    /// and returns the bytes they still hold.
    pub(crate) fn wait_until_released(&self, deadline: Instant) -> usize {
        let mut held = self.held.lock();
        while *held > 0 {
            if self.released.wait_until(&mut held, deadline).timed_out() {
                break;
            }
        }
        *held
    }

    fn add(&self, bytes: usize) {
        let mut held = self.held.lock();
        *held = held.saturating_add(bytes);
    }

    fn subtract(&self, bytes: usize) {
        let mut held = self.held.lock();
        *held = held.saturating_sub(bytes);
        if *held == 0 {
            self.released.notify_all();
        }
    }
}

impl fmt::Display for PlanMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.inner.as_ref(), f)
    }
}

impl MemoryPool for PlanMemoryPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.add(additional);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.subtract(shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.add(additional);
        let result = self.inner.try_grow(reservation, additional);
        if result.is_err() {
            self.subtract(additional);
        }
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::time::Duration;

    fn plan_pool(limit: usize) -> Arc<PlanMemoryPool> {
        Arc::new(PlanMemoryPool::new(Arc::new(GreedyMemoryPool::new(limit))))
    }

    fn register(pool: &Arc<PlanMemoryPool>) -> MemoryReservation {
        MemoryConsumer::new("test").register(&(Arc::clone(pool) as Arc<dyn MemoryPool>))
    }

    fn held(pool: &PlanMemoryPool) -> usize {
        pool.wait_until_released(Instant::now())
    }

    #[test]
    fn counts_what_the_plans_reservations_hold() {
        let pool = plan_pool(1000);
        let first = register(&pool);
        let second = register(&pool);

        first.grow(100);
        second.try_grow(200).unwrap();
        assert_eq!(held(&pool), 300);
        // A refused request holds nothing.
        assert!(second.try_grow(1000).is_err());
        assert_eq!(held(&pool), 300);
        assert_eq!(pool.reserved(), 300);

        first.shrink(40);
        assert_eq!(held(&pool), 260);
        let split = second.split(50);
        drop(second);
        assert_eq!(held(&pool), 110);
        drop(split);
        drop(first);
        assert_eq!(held(&pool), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn waits_for_a_reservation_that_another_thread_drops() {
        let pool = plan_pool(1000);
        let reservation = register(&pool);
        reservation.grow(100);
        let dropping = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(reservation);
        });

        assert_eq!(
            pool.wait_until_released(Instant::now() + Duration::from_secs(10)),
            0
        );
        assert_eq!(pool.reserved(), 0);
        dropping.join().unwrap();
    }

    #[test]
    fn stops_waiting_at_the_deadline_and_reports_what_is_still_held() {
        let pool = plan_pool(1000);
        let reservation = register(&pool);
        reservation.grow(100);

        let start = Instant::now();
        assert_eq!(
            pool.wait_until_released(start + Duration::from_millis(20)),
            100
        );
        assert!(start.elapsed() >= Duration::from_millis(20));
    }
}
