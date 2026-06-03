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

use std::sync::atomic::{AtomicIsize, Ordering};

/// Per-thread drift is flushed into the shared balance once it crosses this.
#[allow(dead_code)]
const SETTLE_THRESHOLD: isize = 64 * 1024;

/// Pure helper: given the current shared balance and a limit, decide whether an
/// armed+stamped thread should trip the breaker. `limit == 0` means "unset".
#[allow(dead_code)]
fn should_trip(balance: isize, limit: usize) -> bool {
    limit != 0 && balance > limit as isize
}

/// Pure helper: add `delta` to `local_drift`; if it crosses `SETTLE_THRESHOLD`
/// in magnitude, flush it into `shared` and return the new shared balance.
/// Otherwise return `None` (nothing flushed).
#[allow(dead_code)]
fn settle(local_drift: &mut isize, delta: isize, shared: &AtomicIsize) -> Option<isize> {
    *local_drift += delta;
    if local_drift.unsigned_abs() >= SETTLE_THRESHOLD as usize {
        let flushed = *local_drift;
        *local_drift = 0;
        let prev = shared.fetch_add(flushed, Ordering::Relaxed);
        Some(prev + flushed)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_trip() {
        assert!(!should_trip(100, 0)); // unset limit never trips
        assert!(!should_trip(100, 200)); // under limit
        assert!(!should_trip(200, 200)); // at limit (strictly greater required)
        assert!(should_trip(201, 200)); // over limit
    }

    #[test]
    fn test_settle_accumulates_then_flushes() {
        let shared = AtomicIsize::new(0);
        let mut drift = 0isize;
        // small allocs below threshold do not flush
        assert_eq!(settle(&mut drift, 1024, &shared), None);
        assert_eq!(shared.load(Ordering::Relaxed), 0);
        // crossing the threshold flushes the accumulated drift
        let new_balance = settle(&mut drift, SETTLE_THRESHOLD, &shared);
        assert_eq!(new_balance, Some(1024 + SETTLE_THRESHOLD));
        assert_eq!(shared.load(Ordering::Relaxed), 1024 + SETTLE_THRESHOLD);
        assert_eq!(drift, 0); // drift reset after flush
    }

    #[test]
    fn test_settle_flushes_negative_drift() {
        let shared = AtomicIsize::new(1_000_000);
        let mut drift = 0isize;
        assert_eq!(
            settle(&mut drift, -SETTLE_THRESHOLD, &shared),
            Some(1_000_000 - SETTLE_THRESHOLD)
        );
    }
}
