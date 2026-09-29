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
use once_cell::sync::Lazy;
use parking_lot::{Condvar, Mutex};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};

/// The memory pools for active task attempts. Weak references let the pool's normal `Arc`
/// ownership determine its lifetime. An entry whose `Weak` has expired belongs to a pool that is
/// still dropping; it is removed once that drop has finished.
static TASK_SHARED_MEMORY_POOLS: Lazy<Mutex<HashMap<i64, Weak<TaskSharedMemoryPool>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Notified under `TASK_SHARED_MEMORY_POOLS` each time a pool has finished dropping.
static TEARDOWN_COMPLETE: Condvar = Condvar::new();

/// A transparent `MemoryPool` wrapper whose lifetime also controls its registry entry.
#[derive(Debug)]
struct TaskSharedMemoryPool {
    inner: Arc<dyn MemoryPool>,
    /// Declared after `inner` on purpose. Fields drop in declaration order, so the inner pool,
    /// including the anchor byte a `CometFairMemoryPool` hands back to Spark from its drop, is
    /// gone before this guard removes the registry entry and wakes the acquires waiting for it.
    _teardown: TeardownComplete,
}

/// Removes a pool's registry entry once the pool itself has dropped, and wakes any acquire
/// waiting for that.
#[derive(Debug)]
struct TeardownComplete {
    task_attempt_id: i64,
}

impl Drop for TeardownComplete {
    fn drop(&mut self) {
        let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock();
        if let Entry::Occupied(entry) = memory_pool_map.entry(self.task_attempt_id) {
            // In production only this guard removes entries, and `acquire_task_shared_pool`
            // creates a pool only when no entry exists, so the entry found here is this pool's
            // own, expired. The liveness check is a safeguard for an entry replaced out of band,
            // as the test `an_old_pool_does_not_remove_its_replacement` does. It reads
            // `strong_count` rather than calling `upgrade` because dropping an upgraded `Arc`
            // under the lock could be the last reference.
            if entry.get().strong_count() == 0 {
                entry.remove();
            }
        }
        TEARDOWN_COMPLETE.notify_all();
    }
}

impl fmt::Display for TaskSharedMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.inner.as_ref(), f)
    }
}

impl MemoryPool for TaskSharedMemoryPool {
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
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Returns the memory pool shared by every native plan in `task_attempt_id`, creating it with
/// `create` if no live pool exists for the task. The returned `Arc` is the RAII handle: the pool
/// stays registered until the last reference to it drops. `create` runs under the shared registry
/// lock, so it must make no JVM call and must not block. `create` must return a pool nothing else
/// holds a reference to, because the wait below relies on dropping the wrapper dropping the pool
/// itself.
///
/// While the task's previous pool is still dropping, this waits for that drop to finish before it
/// creates the replacement. The fair pool hands its anchor byte back to Spark from its drop, and a
/// replacement created before that release lands holds nothing from Spark yet, so its first
/// acquire could park inside Spark and wake to a task entry the release has removed.
pub(crate) fn acquire_task_shared_pool(
    task_attempt_id: i64,
    create: impl FnOnce() -> Arc<dyn MemoryPool>,
) -> Arc<dyn MemoryPool> {
    let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock();
    loop {
        match memory_pool_map.get(&task_attempt_id).map(Weak::upgrade) {
            Some(Some(memory_pool)) => return memory_pool,
            Some(None) => TEARDOWN_COMPLETE.wait(&mut memory_pool_map),
            None => break,
        }
    }

    let memory_pool = Arc::new(TaskSharedMemoryPool {
        inner: create(),
        _teardown: TeardownComplete { task_attempt_id },
    });
    memory_pool_map.insert(task_attempt_id, Arc::downgrade(&memory_pool));
    memory_pool
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::thread;
    use std::time::Duration;

    /// Bounds every wait so a broken ordering fails the test instead of hanging it.
    const TEST_TIMEOUT: Duration = Duration::from_secs(20);

    /// Tests share the process-wide pool map, so each uses its own task attempt id.
    fn acquire(task_attempt_id: i64) -> Arc<dyn MemoryPool> {
        acquire_task_shared_pool(task_attempt_id, || Arc::new(UnboundedMemoryPool::default()))
    }

    fn is_registered(task_attempt_id: i64) -> bool {
        TASK_SHARED_MEMORY_POOLS
            .lock()
            .contains_key(&task_attempt_id)
    }

    #[test]
    fn plans_in_the_same_task_share_one_pool() {
        let first = acquire(-1001);
        let second = acquire(-1001);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn plans_in_different_tasks_get_different_pools() {
        let first = acquire(-1002);
        let second = acquire(-1003);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn pool_is_removed_only_after_the_last_reference_drops() {
        let first = acquire(-1004);
        let second = acquire(-1004);

        drop(first);
        assert!(
            is_registered(-1004),
            "pool must outlive the first reference to release it"
        );

        drop(second);
        assert!(!is_registered(-1004));
    }

    #[test]
    fn dropping_the_reference_releases_the_pool() {
        // Stands in for `createPlan` failing after the pool was acquired. The ordinary `Arc` drops
        // on unwind, so no explicit release path is needed.
        {
            let _pool = acquire(-1005);
            assert!(is_registered(-1005));
        }
        assert!(!is_registered(-1005));
    }

    #[test]
    fn an_old_pool_does_not_remove_its_replacement() {
        let old_pool = acquire(-1006);
        TASK_SHARED_MEMORY_POOLS.lock().remove(&-1006);
        let replacement = acquire(-1006);

        drop(old_pool);
        assert!(is_registered(-1006));

        drop(replacement);
        assert!(!is_registered(-1006));
    }

    /// Churns acquires and drops of one task id across threads. An acquire that finds the entry
    /// of a pool still dropping waits for it and is woken when that drop ends, so this exercises
    /// the wait-and-wake path, and the registry must be consistent at the end.
    #[test]
    fn concurrent_acquire_and_drop_leaves_a_consistent_registry() {
        let (done_tx, done) = channel();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let done_tx = done_tx.clone();
                thread::spawn(move || {
                    for _ in 0..1_000 {
                        drop(acquire(-1007));
                    }
                    let _ = done_tx.send(());
                })
            })
            .collect();
        for _ in 0..threads.len() {
            done.recv_timeout(TEST_TIMEOUT)
                .expect("a thread never finished; an acquire waiting for a drop was not woken");
        }
        for thread in threads {
            thread.join().unwrap();
        }

        assert!(
            !is_registered(-1007),
            "registry entry survived after every reference was dropped"
        );

        // The registry must still work for the task after the churn.
        let _pool = acquire(-1007);
        assert!(is_registered(-1007));
    }

    /// A pool whose drop can be held open: it announces itself on `entered` and then waits for
    /// `open`, standing in for the anchor byte a `CometFairMemoryPool` hands back to Spark from
    /// its drop.
    #[derive(Debug)]
    struct SlowDropPool {
        inner: UnboundedMemoryPool,
        entered: Sender<()>,
        open: Mutex<Receiver<()>>,
    }

    impl fmt::Display for SlowDropPool {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "SlowDropPool")
        }
    }

    impl MemoryPool for SlowDropPool {
        fn name(&self) -> &str {
            "SlowDropPool"
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
            self.inner.try_grow(reservation, additional)
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    impl Drop for SlowDropPool {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            // A test that fails before opening the gate must not hang this thread.
            let _ = self.open.lock().recv_timeout(TEST_TIMEOUT);
        }
    }

    /// The fair pool hands its anchor byte back to Spark from its drop, and a replacement pool
    /// created while that release is on its way holds nothing from Spark yet, so its first
    /// acquire can park inside Spark on the old byte and wake to a task entry the release has
    /// removed. The registry therefore hands out no replacement until the previous pool has
    /// finished dropping.
    #[test]
    fn a_replacement_waits_for_the_previous_pool_to_finish_dropping() {
        let (entered_tx, entered) = channel();
        let (open_tx, open) = channel();
        let pool = acquire_task_shared_pool(-1008, || {
            Arc::new(SlowDropPool {
                inner: UnboundedMemoryPool::default(),
                entered: entered_tx,
                open: Mutex::new(open),
            })
        });

        let dropping = thread::spawn(move || drop(pool));
        entered
            .recv_timeout(TEST_TIMEOUT)
            .expect("the pool never started dropping");
        assert_eq!(
            TASK_SHARED_MEMORY_POOLS
                .lock()
                .get(&-1008)
                .map(|weak| weak.strong_count() == 0),
            Some(true),
            "a pool in the middle of dropping keeps its entry, expired, until the drop ends"
        );

        let (created_tx, created) = channel();
        let acquiring = thread::spawn(move || {
            let replacement = acquire(-1008);
            let _ = created_tx.send(());
            replacement
        });
        assert!(
            created.recv_timeout(Duration::from_millis(300)).is_err(),
            "a replacement was handed out while the previous pool was still dropping"
        );

        let _ = open_tx.send(());
        dropping.join().unwrap();
        created
            .recv_timeout(TEST_TIMEOUT)
            .expect("the replacement never arrived once the previous pool had dropped");
        let replacement = acquiring.join().unwrap();
        assert!(is_registered(-1008));

        drop(replacement);
        assert!(!is_registered(-1008));
    }
}
