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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Shared state for coordinating spill requests between Spark's memory manager
/// (which calls `NativeMemoryConsumer.spill()` on a Spark thread) and DataFusion
/// operators (which call `try_grow()`/`shrink()` on tokio threads).
///
/// When Spark needs to reclaim memory from Comet, it sets `pressure` via
/// `request_spill()`. The memory pool's `try_grow()` checks this and returns
/// `ResourcesExhausted`, causing operators to spill. As operators call `shrink()`,
/// freed bytes are accumulated and the waiting Spark thread is notified.
#[derive(Debug)]
pub struct SpillState {
    /// Bytes requested to be freed. Set by Spark's spill() callback.
    pressure: AtomicUsize,
    /// Bytes actually freed since pressure was set.
    freed: AtomicUsize,
    /// Mutex + Condvar to allow the spill requester to wait for operators to react.
    notify: (Mutex<()>, Condvar),
}

impl SpillState {
    pub fn new() -> Self {
        Self {
            pressure: AtomicUsize::new(0),
            freed: AtomicUsize::new(0),
            notify: (Mutex::new(()), Condvar::new()),
        }
    }

    /// Returns the current spill pressure in bytes. Called by the memory pool's
    /// `try_grow()` to decide whether to deny allocations.
    pub fn pressure(&self) -> usize {
        self.pressure.load(Ordering::Acquire)
    }

    /// Record that `size` bytes were freed (called from pool's `shrink()`).
    /// Wakes the waiting spill requester.
    pub fn record_freed(&self, size: usize) {
        self.freed.fetch_add(size, Ordering::Release);
        let (_lock, cvar) = &self.notify;
        cvar.notify_all();
    }

    /// Called from JNI when Spark's `NativeMemoryConsumer.spill()` is invoked.
    /// Sets spill pressure and waits (up to `timeout`) for operators to free memory.
    /// Returns the actual number of bytes freed.
    pub fn request_spill(&self, size: usize, timeout: Duration) -> usize {
        // Reset freed counter and set pressure
        self.freed.store(0, Ordering::Release);
        self.pressure.store(size, Ordering::Release);

        // Wait for operators to react
        let (lock, cvar) = &self.notify;
        let mut guard = lock.lock().unwrap();
        let deadline = std::time::Instant::now() + timeout;
        while self.freed.load(Ordering::Acquire) < size {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (new_guard, _timeout_result) = cvar.wait_timeout(guard, remaining).unwrap();
            guard = new_guard;
        }

        // Clear pressure and return freed bytes
        self.pressure.store(0, Ordering::Release);
        self.freed.load(Ordering::Acquire)
    }
}

impl Default for SpillState {
    fn default() -> Self {
        Self::new()
    }
}
