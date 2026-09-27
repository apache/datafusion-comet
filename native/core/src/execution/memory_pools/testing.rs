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

//! Memory pools for tests of operators that reserve memory.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::Result;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, MemoryReservation};

/// A [`GreedyMemoryPool`] that also records the most it ever had reserved, so a test can check
/// what an operator reserved while it ran, not only that it gave everything back.
#[derive(Debug)]
pub(crate) struct PeakMemoryPool {
    inner: GreedyMemoryPool,
    peak: AtomicUsize,
}

impl PeakMemoryPool {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            inner: GreedyMemoryPool::new(limit),
            peak: AtomicUsize::new(0),
        }
    }

    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn record(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }
}

impl fmt::Display for PeakMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeakMemoryPool({}, peak {})", self.inner, self.peak())
    }
}

impl MemoryPool for PeakMemoryPool {
    fn name(&self) -> &str {
        "PeakMemoryPool"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}
