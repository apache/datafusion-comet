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
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use jni::objects::{Global, JObject};

use crate::{errors::CometResult, jvm_bridge::JVMClasses};

/// The source of memory for the unified memory pools. In production this is
/// Spark's off-heap executor memory pool, reached over JNI; tests substitute an
/// in-process implementation so that the pools can be exercised without a JVM.
pub(crate) trait SparkMemoryBackend: Send + Sync {
    /// Request `size` bytes, returning the number of bytes actually granted,
    /// which may be less than requested.
    fn acquire(&self, size: usize) -> CometResult<i64>;

    /// Return `size` bytes.
    fn release(&self, size: usize) -> CometResult<()>;
}

/// A [`SparkMemoryBackend`] that delegates to Spark's unified memory manager by
/// calling [`crate::jvm_bridge::CometTaskMemoryManager`] over JNI.
struct JniMemoryBackend {
    task_memory_manager_handle: Arc<Global<JObject<'static>>>,
}

// The JNI global reference is safe to use from any thread that attaches to the
// JVM, which `JVMClasses::with_env` guarantees.
unsafe impl Send for JniMemoryBackend {}
unsafe impl Sync for JniMemoryBackend {}

impl SparkMemoryBackend for JniMemoryBackend {
    fn acquire(&self, size: usize) -> CometResult<i64> {
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env,
              comet_task_memory_manager(handle).acquire_memory(size as i64) -> i64)
        })
    }

    fn release(&self, size: usize) -> CometResult<()> {
        let handle = self.task_memory_manager_handle.as_obj();
        JVMClasses::with_env(|env| unsafe {
            jni_call!(env, comet_task_memory_manager(handle).release_memory(size as i64) -> ())
        })
    }
}

/// Counters describing the traffic a memory pool has sent to its backend.
///
/// Each `try_grow` or `shrink` that reaches the backend costs a JNI round-trip
/// plus contention on Spark's executor-wide memory manager lock, so these
/// counters are the input to any decision about batching acquisitions or
/// releasing with hysteresis (see issue #5383). They are plain relaxed atomics
/// and are always collected.
#[derive(Default)]
pub(crate) struct MemoryPoolStats {
    acquire_calls: AtomicU64,
    acquire_requested_bytes: AtomicU64,
    acquire_granted_bytes: AtomicU64,
    /// Acquisitions that were granted less than they asked for.
    short_grants: AtomicU64,
    release_calls: AtomicU64,
    release_bytes: AtomicU64,
    /// Wall-clock time spent inside the backend, i.e. in the JNI call and the
    /// Spark-side accounting it performs.
    backend_nanos: AtomicU64,
}

impl MemoryPoolStats {
    fn record_acquire(&self, requested: usize, granted: i64, elapsed: Duration) {
        self.acquire_calls.fetch_add(1, Relaxed);
        self.acquire_requested_bytes
            .fetch_add(requested as u64, Relaxed);
        if granted > 0 {
            self.acquire_granted_bytes
                .fetch_add(granted as u64, Relaxed);
        }
        if granted < requested as i64 {
            self.short_grants.fetch_add(1, Relaxed);
        }
        self.record_elapsed(elapsed);
    }

    fn record_release(&self, size: usize, elapsed: Duration) {
        self.release_calls.fetch_add(1, Relaxed);
        self.release_bytes.fetch_add(size as u64, Relaxed);
        self.record_elapsed(elapsed);
    }

    fn record_elapsed(&self, elapsed: Duration) {
        self.backend_nanos
            .fetch_add(elapsed.as_nanos() as u64, Relaxed);
    }

    pub(crate) fn acquire_calls(&self) -> u64 {
        self.acquire_calls.load(Relaxed)
    }

    pub(crate) fn release_calls(&self) -> u64 {
        self.release_calls.load(Relaxed)
    }

    /// True if no call ever reached the backend, in which case there is nothing
    /// worth logging.
    fn is_empty(&self) -> bool {
        self.acquire_calls() == 0 && self.release_calls() == 0
    }
}

impl Debug for MemoryPoolStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("MemoryPoolStats")
            .field("acquire_calls", &self.acquire_calls())
            .field(
                "acquire_requested_bytes",
                &self.acquire_requested_bytes.load(Relaxed),
            )
            .field(
                "acquire_granted_bytes",
                &self.acquire_granted_bytes.load(Relaxed),
            )
            .field("short_grants", &self.short_grants.load(Relaxed))
            .field("release_calls", &self.release_calls())
            .field("release_bytes", &self.release_bytes.load(Relaxed))
            .field("backend_nanos", &self.backend_nanos.load(Relaxed))
            .finish()
    }
}

impl Display for MemoryPoolStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "acquire(calls={}, requested={} bytes, granted={} bytes, short={}), \
             release(calls={}, bytes={}), backend_time={:?}",
            self.acquire_calls(),
            self.acquire_requested_bytes.load(Relaxed),
            self.acquire_granted_bytes.load(Relaxed),
            self.short_grants.load(Relaxed),
            self.release_calls(),
            self.release_bytes.load(Relaxed),
            Duration::from_nanos(self.backend_nanos.load(Relaxed)),
        )
    }
}

/// Handle used by the unified memory pools to acquire and release memory from
/// Spark, recording [`MemoryPoolStats`] for every call that it makes.
pub(crate) struct SparkMemoryClient {
    backend: Arc<dyn SparkMemoryBackend>,
    task_attempt_id: i64,
    stats: MemoryPoolStats,
}

impl SparkMemoryClient {
    /// Create a client that acquires memory from Spark over JNI.
    pub(crate) fn new(
        task_memory_manager_handle: Arc<Global<JObject<'static>>>,
        task_attempt_id: i64,
    ) -> Self {
        Self::with_backend(
            Arc::new(JniMemoryBackend {
                task_memory_manager_handle,
            }),
            task_attempt_id,
        )
    }

    pub(crate) fn with_backend(backend: Arc<dyn SparkMemoryBackend>, task_attempt_id: i64) -> Self {
        Self {
            backend,
            task_attempt_id,
            stats: MemoryPoolStats::default(),
        }
    }

    pub(crate) fn task_attempt_id(&self) -> i64 {
        self.task_attempt_id
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> &MemoryPoolStats {
        &self.stats
    }

    /// Request `size` bytes from Spark, returning the number of bytes granted.
    pub(crate) fn acquire(&self, size: usize) -> CometResult<i64> {
        let start = Instant::now();
        let result = self.backend.acquire(size);
        let elapsed = start.elapsed();
        match &result {
            Ok(granted) => self.stats.record_acquire(size, *granted, elapsed),
            // A failed call still costs a round-trip, so account for its time.
            Err(_) => self.stats.record_acquire(size, 0, elapsed),
        }
        result
    }

    /// Return `size` bytes to Spark.
    pub(crate) fn release(&self, size: usize) -> CometResult<()> {
        let start = Instant::now();
        let result = self.backend.release(size);
        self.stats.record_release(size, start.elapsed());
        result
    }

    /// Emit the accumulated statistics for this task. Called when a pool is
    /// dropped, which for the task-shared unified pools is when the last native
    /// plan for the task is released.
    ///
    /// This logs at debug level because it produces one line per task attempt.
    /// To collect it without turning on debug logging for every module, supply a
    /// `log4rs.yaml` (via `COMET_CONF_DIR` or the `comet.log.file.path` system
    /// property) that raises the level for this module alone:
    ///
    /// ```yaml
    /// loggers:
    ///   comet::execution::memory_pools:
    ///     level: debug
    /// ```
    pub(crate) fn log_stats(&self, pool_name: &str) {
        if !self.stats.is_empty() {
            log::debug!(
                "Task {} {pool_name} memory pool stats: {}",
                self.task_attempt_id,
                self.stats
            );
        }
    }
}

impl Debug for SparkMemoryClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("SparkMemoryClient")
            .field("task_attempt_id", &self.task_attempt_id)
            .field("stats", &self.stats)
            .finish()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// An in-process [`SparkMemoryBackend`] standing in for Spark's memory
    /// manager, so that the pools can be tested without a JVM.
    pub(crate) struct TestMemoryBackend {
        /// Total memory the backend is willing to hand out.
        capacity: usize,
        granted: std::sync::Mutex<usize>,
    }

    impl TestMemoryBackend {
        pub(crate) fn new(capacity: usize) -> Self {
            Self {
                capacity,
                granted: std::sync::Mutex::new(0),
            }
        }

        pub(crate) fn granted(&self) -> usize {
            *self.granted.lock().unwrap()
        }
    }

    impl SparkMemoryBackend for TestMemoryBackend {
        fn acquire(&self, size: usize) -> CometResult<i64> {
            // Mirror Spark's behaviour of granting as much as is available.
            let mut granted = self.granted.lock().unwrap();
            let available = self.capacity - *granted;
            let grant = size.min(available);
            *granted += grant;
            Ok(grant as i64)
        }

        fn release(&self, size: usize) -> CometResult<()> {
            let mut granted = self.granted.lock().unwrap();
            *granted -= size;
            Ok(())
        }
    }

    fn client(capacity: usize) -> SparkMemoryClient {
        SparkMemoryClient::with_backend(Arc::new(TestMemoryBackend::new(capacity)), 1)
    }

    #[test]
    fn stats_count_acquires_and_releases() {
        let client = client(1024);

        assert_eq!(client.acquire(100).unwrap(), 100);
        assert_eq!(client.acquire(200).unwrap(), 200);
        client.release(100).unwrap();

        let stats = client.stats();
        assert_eq!(stats.acquire_calls(), 2);
        assert_eq!(stats.acquire_requested_bytes.load(Relaxed), 300);
        assert_eq!(stats.acquire_granted_bytes.load(Relaxed), 300);
        assert_eq!(stats.short_grants.load(Relaxed), 0);
        assert_eq!(stats.release_calls(), 1);
        assert_eq!(stats.release_bytes.load(Relaxed), 100);
    }

    #[test]
    fn stats_count_short_grants() {
        let client = client(100);

        assert_eq!(client.acquire(150).unwrap(), 100);

        let stats = client.stats();
        assert_eq!(stats.acquire_calls(), 1);
        assert_eq!(stats.acquire_requested_bytes.load(Relaxed), 150);
        assert_eq!(stats.acquire_granted_bytes.load(Relaxed), 100);
        assert_eq!(stats.short_grants.load(Relaxed), 1);
    }

    #[test]
    fn stats_are_empty_before_any_call() {
        let client = client(1024);
        assert!(client.stats().is_empty());
        client.acquire(1).unwrap();
        assert!(!client.stats().is_empty());
    }
}
