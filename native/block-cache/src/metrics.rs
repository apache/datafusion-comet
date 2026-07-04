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

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide cache counters. Cheap relaxed atomics; read via [`Metrics::snapshot`].
///
/// Phase 1 exposes these through a periodic native log line (the wrapper drives it);
/// Spark-visible SQL metrics are a tracked follow-up.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Block reads served from the memory tier.
    pub hits: AtomicU64,
    /// Block reads that had to go upstream.
    pub misses: AtomicU64,
    /// Upstream fetch calls issued (a coalesced run counts once).
    pub fetches: AtomicU64,
    /// Bytes returned by upstream fetches.
    pub bytes_fetched: AtomicU64,
    /// Blocks evicted from the memory tier.
    pub evictions: AtomicU64,
    /// Files invalidated because their version changed under us.
    pub invalidations: AtomicU64,
}

/// A point-in-time copy of [`Metrics`], safe to format/log.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub fetches: u64,
    pub bytes_fetched: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

impl MetricsSnapshot {
    /// Hit ratio over block reads in `[0.0, 1.0]`; `0.0` when nothing has been read yet.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl Metrics {
    #[inline]
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_fetch(&self, bytes: u64) {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            fetches: self.fetches.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }
}
