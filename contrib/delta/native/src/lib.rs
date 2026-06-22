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
//!   - [`plan_delta_scan`]: helpers core's planner dispatcher invokes to assemble
//!     a Delta scan's `DataSourceExec` (kernel-rs is JVM-side, so the per-scan
//!     planning the JVM doesn't pre-resolve happens here)
//!
//! No `#[ctor]` registration, no contrib-private operator-planner registry; this
//! crate exposes plain Rust functions that core calls directly under
//! `#[cfg(feature = "contrib-delta")]`.

pub mod dv_reader;
pub mod engine;
pub mod error;
pub mod jni;
pub mod kernel_scan;
pub mod planner;
pub mod predicate;
pub mod scan;

/// Re-export of the Delta proto messages, named so module paths inside this crate
/// can keep their original `use crate::proto::Delta...` form. The messages
/// themselves live in core's proto crate (so the dispatcher arm in core has direct
/// access to the typed variants).
pub mod proto {
    pub use datafusion_comet_proto::spark_operator::{
        DeltaDvDescriptor, DeltaPartitionValue, DeltaScan, DeltaScanCommon, DeltaScanTask,
        DeltaScanTaskList,
    };
}

pub use engine::DeltaStorageConfig;
pub use error::{DeltaError, DeltaResult};
pub use scan::{list_delta_files, plan_delta_scan, DeltaFileEntry, DeltaScanPlan};
