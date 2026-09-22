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

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Bytes a Spark-backed pool has recorded beyond what Spark granted.
///
/// `MemoryPool::grow` must always succeed: DataFusion calls it for memory that already exists,
/// such as a spilled batch read back from disk. When Spark grants less than requested, the pool
/// still records the full amount, so `reserved()` stays truthful and later `try_grow` calls are
/// refused, and carries the shortfall here. A shrink repays this debt before returning anything
/// to Spark, so Spark is never handed back more than it granted.
#[derive(Debug, Default)]
pub(super) struct Overcommit(AtomicUsize);

impl Overcommit {
    /// Records `bytes` of an infallible grow that Spark did not grant.
    pub(super) fn add(&self, bytes: usize) {
        if bytes > 0 {
            self.0.fetch_add(bytes, Relaxed);
        }
    }

    /// Repays as much of the debt as a shrink of `size` bytes covers, and returns the remainder,
    /// which is what the pool must release to Spark.
    pub(super) fn repay(&self, size: usize) -> usize {
        let prev = self
            .0
            .fetch_update(Relaxed, Relaxed, |debt| Some(debt - debt.min(size)))
            .unwrap_or_else(|debt| debt);
        size - prev.min(size)
    }

    pub(super) fn get(&self) -> usize {
        self.0.load(Relaxed)
    }
}

/// Clamps Spark's reply to an acquire request: Spark never grants more than asked, and a failed
/// call grants nothing.
pub(super) fn granted(requested: usize, acquired: i64) -> usize {
    usize::try_from(acquired).unwrap_or(0).min(requested)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shrink_repays_the_debt_before_releasing_to_spark() {
        let overcommit = Overcommit::default();
        overcommit.add(100);
        assert_eq!(overcommit.repay(30), 0);
        assert_eq!(overcommit.get(), 70);
        assert_eq!(overcommit.repay(100), 30);
        assert_eq!(overcommit.get(), 0);
        assert_eq!(overcommit.repay(50), 50);
    }

    #[test]
    fn no_debt_releases_everything() {
        let overcommit = Overcommit::default();
        overcommit.add(0);
        assert_eq!(overcommit.repay(64), 64);
    }

    #[test]
    fn granted_is_clamped_to_the_request() {
        assert_eq!(granted(100, 40), 40);
        assert_eq!(granted(100, 100), 100);
        assert_eq!(granted(100, 150), 100);
        assert_eq!(granted(100, -1), 0);
    }
}
