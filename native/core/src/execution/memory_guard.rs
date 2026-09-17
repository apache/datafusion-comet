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

//! A container memory guard that fails one task instead of losing the executor.
//!
//! When an executor exceeds its container memory limit the kernel kills the whole JVM, taking
//! every task on it, its cached blocks and its shuffle files. A single failed task is retried by
//! Spark and costs almost nothing by comparison. This guard samples the container's real memory
//! usage at existing checkpoints in the execution loop and fails the current task when usage is
//! close to the limit.
//!
//! # Why the kernel's number and not Comet's own accounting
//!
//! An earlier prototype gated on the bytes Comet's global allocator had handed out. That was
//! measured against kernel RSS on TPC-H SF100 and is not a usable signal: across ~4500 paired
//! samples the correlation of their changes was about 0.43, the allocator balance never led RSS at
//! any horizon (correlation within 0.01 of zero at 1, 2, 5 and 10 samples), and the gap between
//! the two wandered by 2 to 3 GB while the balance itself only spanned 0 to 2.1 GB. A threshold on
//! that number cannot mean anything about the quantity the kernel kills on, because the noise in
//! the offset is larger than the whole signal.
//!
//! The cgroup counter has none of those problems: it is the number the OOM killer compares against
//! the limit, and it already includes the JVM heap, Comet's native allocations, JVM-side Arrow,
//! Spark's own off-heap and mapped files. Reading it costs one small file read.
//!
//! # What this does not do
//!
//! Checks happen at batch boundaries, so a single operator that balloons between checkpoints can
//! still cross the limit unobserved. This narrows the window rather than closing it. Closing it
//! entirely would require acting inside the allocator, which was tried in
//! [#4582](https://github.com/apache/datafusion-comet/pull/4582) and rejected: panicking from an
//! allocator brings reentrancy, unwinding and `spawn_blocking` escape problems of its own.

use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Where the guard reads the container's memory usage from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageSource {
    /// cgroup v2. Inside a Kubernetes pod the container's cgroup is namespaced, so these are
    /// normally at the root of the mount; on a bare host the path comes from `/proc/self/cgroup`.
    CgroupV2 { current: PathBuf },
    /// cgroup v1, used by older Kubernetes and YARN deployments.
    CgroupV1 { usage: PathBuf },
}

impl UsageSource {
    /// Reads current usage in bytes, or `None` if the file has become unreadable (a cgroup can be
    /// removed under us during shutdown).
    pub fn read(&self) -> Option<u64> {
        let path = match self {
            UsageSource::CgroupV2 { current } => current,
            UsageSource::CgroupV1 { usage } => usage,
        };
        parse_bytes(&fs::read_to_string(path).ok()?)
    }

    pub fn describe(&self) -> String {
        match self {
            UsageSource::CgroupV2 { current } => format!("cgroup v2 ({})", current.display()),
            UsageSource::CgroupV1 { usage } => format!("cgroup v1 ({})", usage.display()),
        }
    }
}

/// Samples container memory usage and reports when it crosses a fraction of the limit.
#[derive(Debug)]
pub struct MemoryGuard {
    source: UsageSource,
    limit: u64,
    /// Usage at or above this many bytes trips the guard.
    trip_at: u64,
    min_interval: Duration,
    last_check: Instant,
}

/// Details of a guard trip, used to build the error returned to Spark.
#[derive(Debug, Clone, Copy)]
pub struct Trip {
    pub usage: u64,
    pub limit: u64,
    pub trip_at: u64,
}

impl MemoryGuard {
    /// Builds a guard, or `None` when there is no container limit to guard against.
    ///
    /// Returning `None` is the normal case off Linux and on hosts without a memory limit set, and
    /// it disables the guard rather than failing: a guard with no limit has nothing to say.
    pub fn new(threshold: f64, min_interval: Duration) -> Option<Self> {
        let (source, limit) = discover()?;
        if !(0.0..=1.0).contains(&threshold) || limit == 0 {
            return None;
        }
        let trip_at = (limit as f64 * threshold) as u64;
        Some(Self {
            source,
            limit,
            trip_at,
            min_interval,
            last_check: Instant::now(),
        })
    }

    pub fn describe(&self) -> String {
        format!(
            "{}, limit {} bytes, trips at {} bytes",
            self.source.describe(),
            self.limit,
            self.trip_at
        )
    }

    /// Samples usage if enough time has passed, and reports a trip.
    ///
    /// Throttled because it runs on a hot path. The caller checks often; the file read does not
    /// need to happen every time.
    pub fn check(&mut self) -> Option<Trip> {
        let now = Instant::now();
        if now.duration_since(self.last_check) < self.min_interval {
            return None;
        }
        self.last_check = now;
        let usage = self.source.read()?;
        (usage >= self.trip_at).then_some(Trip {
            usage,
            limit: self.limit,
            trip_at: self.trip_at,
        })
    }
}

/// Finds the container memory usage file and limit for this process.
fn discover() -> Option<(UsageSource, u64)> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let root = PathBuf::from("/sys/fs/cgroup");

    // cgroup v2, container case: the pod's cgroup is namespaced to the mount root.
    if let Some(limit) = read_limit(&root.join("memory.max")) {
        return Some((
            UsageSource::CgroupV2 {
                current: root.join("memory.current"),
            },
            limit,
        ));
    }

    // cgroup v2, host case: resolve this process's own cgroup directory.
    if let Ok(contents) = fs::read_to_string("/proc/self/cgroup") {
        if let Some(rel) = parse_cgroup_v2_path(&contents) {
            let dir = root.join(rel.trim_start_matches('/'));
            if let Some(limit) = read_limit(&dir.join("memory.max")) {
                return Some((
                    UsageSource::CgroupV2 {
                        current: dir.join("memory.current"),
                    },
                    limit,
                ));
            }
        }
    }

    // cgroup v1, used by older Kubernetes and YARN.
    let v1 = root.join("memory");
    if let Some(limit) = read_limit(&v1.join("memory.limit_in_bytes")) {
        return Some((
            UsageSource::CgroupV1 {
                usage: v1.join("memory.usage_in_bytes"),
            },
            limit,
        ));
    }

    None
}

/// Reads a cgroup limit file, treating "no limit" as absent.
///
/// cgroup v2 spells unlimited as the literal `max`. cgroup v1 spells it as a number so large it
/// is meaningless as a limit (typically `u64::MAX` rounded down to a page multiple), so anything
/// at or above this is treated as unlimited.
fn read_limit(path: &PathBuf) -> Option<u64> {
    const V1_UNLIMITED: u64 = 1 << 62;
    let limit = parse_limit(&fs::read_to_string(path).ok()?)?;
    (limit > 0 && limit < V1_UNLIMITED).then_some(limit)
}

/// Parses a cgroup limit value, where cgroup v2 uses `max` for unlimited.
pub fn parse_limit(contents: &str) -> Option<u64> {
    let trimmed = contents.trim();
    if trimmed == "max" {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

/// Parses a plain integer byte count from a cgroup file.
pub fn parse_bytes(contents: &str) -> Option<u64> {
    contents.trim().parse::<u64>().ok()
}

/// Extracts this process's cgroup v2 path from `/proc/self/cgroup`.
///
/// The v2 entry is the line with an empty controller list, formatted `0::/path`. Inside a
/// container that is usually just `0::/`, in which case the caller's root lookup already applies.
pub fn parse_cgroup_v2_path(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        let hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        (hierarchy == "0" && controllers.is_empty() && !path.is_empty()).then(|| path.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_cgroup_v2_path() {
        let contents = "0::/user.slice/user-1000.slice/session-852.scope\n";
        assert_eq!(
            parse_cgroup_v2_path(contents).as_deref(),
            Some("/user.slice/user-1000.slice/session-852.scope")
        );
    }

    /// Inside a container the cgroup is namespaced to the root, which is the Kubernetes case.
    #[test]
    fn parses_the_container_root_path() {
        assert_eq!(parse_cgroup_v2_path("0::/\n").as_deref(), Some("/"));
    }

    /// A v1-only host lists numbered hierarchies with controller names and no v2 line.
    #[test]
    fn finds_no_v2_path_on_a_v1_host() {
        let contents = "11:memory:/docker/abc\n10:cpu,cpuacct:/docker/abc\n";
        assert_eq!(parse_cgroup_v2_path(contents), None);
    }

    #[test]
    fn treats_cgroup_v2_max_as_no_limit() {
        assert_eq!(parse_limit("max\n"), None);
        assert_eq!(parse_limit("2147483648\n"), Some(2147483648));
    }

    #[test]
    fn parses_usage_bytes() {
        assert_eq!(parse_bytes(" 1234567\n"), Some(1234567));
        assert_eq!(parse_bytes("not a number"), None);
    }

    /// A guard is only useful when a real limit exists, so an absent or unlimited one disables it
    /// rather than tripping on nothing.
    #[test]
    fn a_nonsense_threshold_disables_the_guard() {
        assert!(MemoryGuard::new(-1.0, Duration::from_millis(100)).is_none());
        assert!(MemoryGuard::new(2.0, Duration::from_millis(100)).is_none());
    }
}
