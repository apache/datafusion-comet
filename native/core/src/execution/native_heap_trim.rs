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

//! Hands the memory that the C allocator keeps cached back to the operating system once an
//! executor has no Comet native plan left running.
//!
//! glibc keeps much of the memory that native operators free in its per-thread arenas instead of
//! returning it, and Comet's operators free a lot: every spill drops the batches it buffered, which
//! are Arrow buffers of tens of KiB to a few MiB. A stage's plans also run on different threads
//! than the next stage's (a plan with no JVM input runs on Tokio workers, one that reads shuffle
//! on the Spark task thread), so the next stage does not reuse what the previous one left in the
//! other threads' arenas. The executor's resident memory then stays near the previous stage's peak
//! and the next stage's allocations add to it, which can exceed the container even though Comet's
//! memory pools and allocation counter stay within their limits. Spark's own off-heap pages are
//! large enough that glibc maps and unmaps each of them directly, so they do not build up this way.
//!
//! `malloc_trim` returns the free pages in every arena. It runs only when the last plan on the
//! executor is released, typically between stages, so it does not hold the arena locks of
//! operators that are still running.

/// Trims the native heap if `live_plans`, the number of native plans still registered on this
/// executor, is zero and this build can trim.
pub(crate) fn trim_if_idle(live_plans: usize) {
    if should_trim(
        platform::SUPPORTED && crate::ALLOCATOR_BACKEND == "system",
        live_plans,
    ) {
        platform::trim_and_log();
    }
}

/// Whether releasing a plan should trim: trimming is `supported` and no other plan is running.
/// `malloc_trim` is glibc's, so only a Linux build that allocates through glibc, which means the
/// system allocator rather than the `jemalloc` or `mimalloc` feature, supports it.
fn should_trim(supported: bool, live_plans: usize) -> bool {
    supported && live_plans == 0
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod platform {
    use log::info;
    use std::time::Instant;

    pub(super) const SUPPORTED: bool = true;

    pub(super) fn trim_and_log() {
        extern "C" {
            fn malloc_trim(pad: usize) -> std::os::raw::c_int;
        }
        let before = anonymous_rss_mib();
        let start = Instant::now();
        // SAFETY: glibc's malloc_trim takes the lock of each arena it trims and can be called from
        // any thread. A pad of 0 keeps nothing beyond what the allocator needs at the top of the
        // main heap.
        unsafe {
            malloc_trim(0);
        }
        let elapsed_ms = start.elapsed().as_millis();
        match (before, anonymous_rss_mib()) {
            (Some(before), Some(after)) => info!(
                "Trimmed the native heap after the last Comet native plan on this executor \
                 finished, in {elapsed_ms} ms: anonymous resident memory {before:.1} MiB before, \
                 {after:.1} MiB after"
            ),
            _ => info!(
                "Trimmed the native heap after the last Comet native plan on this executor \
                 finished, in {elapsed_ms} ms"
            ),
        }
    }

    /// The process's anonymous resident memory, which the container's memory limit counts, or
    /// None when `/proc` cannot be read.
    fn anonymous_rss_mib() -> Option<f64> {
        let status = procfs::process::Process::myself().ok()?.status().ok()?;
        status.rssanon.map(|kib| kib as f64 / 1024.0)
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod platform {
    pub(super) const SUPPORTED: bool = false;

    pub(super) fn trim_and_log() {}
}

#[cfg(test)]
mod tests {
    use super::should_trim;

    #[test]
    fn trims_only_when_supported_and_no_plan_is_left() {
        assert!(should_trim(true, 0));
        assert!(!should_trim(true, 1), "another plan is still running");
        assert!(!should_trim(false, 0), "the build cannot trim");
    }
}
