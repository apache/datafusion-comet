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

//! Delta Lake integration for Apache DataFusion Comet.
//!
//! Enabled in core via `--features contrib-delta`. Default builds carry zero
//! Delta surface; this crate is not linked unless the feature is on.
//!
//! Surfaces:
//!   - JNI: `Java_org_apache_comet_contrib_delta_Native_planDeltaScan` (driver-side
//!     log replay via delta-kernel-rs; returns a `DeltaScanTaskList` proto)
//!   - [`scan::plan_delta_scan`] / `scan::plan_delta_scan_with_predicate`: driver-side
//!     helpers that replay the log and assemble the scan-task list. (Core's planner
//!     dispatcher calls `planner::plan_delta_scan`, which is still a build-gate stub in
//!     this unit and becomes the real entry point in the executor unit.)
//!
//! No `#[ctor]` registration, no contrib-private operator-planner registry; this
//! crate exposes plain Rust functions that core calls directly under
//! `#[cfg(feature = "contrib-delta")]`.
//!
//! This unit ships the driver side (log replay, predicate pushdown, scan-task
//! assembly, JNI). The executor-side read path (`kernel_scan`, `dv_reader`) and the
//! real `planner` land in the next unit; until then `planner` is a build-gate stub.

pub mod engine;
pub mod error;
pub mod jni;
pub mod planner;
pub mod predicate;
pub mod scan;

/// Re-export of the Delta proto messages the driver modules reference via
/// `crate::proto::Delta...`. The messages themselves live in core's proto crate. (The
/// `DeltaScan` / `DeltaScanCommon` envelopes are consumed only by the planner, which names
/// them directly from `datafusion_comet_proto`, so they're added to this re-export by the
/// executor unit rather than here.)
pub mod proto {
    pub use datafusion_comet_proto::spark_operator::{
        DeltaDvDescriptor, DeltaPartitionValue, DeltaScanTask, DeltaScanTaskList,
    };
}

pub use engine::DeltaStorageConfig;
pub use error::{DeltaError, DeltaResult};
pub use scan::{list_delta_files, plan_delta_scan, DeltaFileEntry, DeltaScanPlan};
