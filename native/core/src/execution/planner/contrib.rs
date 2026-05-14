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

//! Convenience re-exports of the contrib SPI surface.
//!
//! The actual trait + registry live in the standalone `comet-contrib-spi` crate so both
//! core and contribs can depend on them without forming a dependency cycle (core links
//! contribs via Cargo feature flags, contribs need the SPI types). This module just
//! re-exports the surface so existing `crate::execution::planner::contrib::...`
//! imports inside core continue to resolve.

// Re-export the parts of the SPI core itself uses (the dispatcher only needs
// `lookup_contrib_planner_by_kind`). The other helpers — `register_contrib_planner`,
// `registered_contrib_kinds`, `ContribError`, `ContribOperatorPlanner` — are exposed
// directly from the `comet_contrib_spi` crate so contribs import them from there.
pub use comet_contrib_spi::lookup_contrib_planner_by_kind;
#[allow(unused_imports)] // surfaced for tests + diagnostics; consumed in PR1.7 onwards
pub use comet_contrib_spi::{
    register_contrib_planner, registered_contrib_kinds, ContribError, ContribOperatorPlanner,
};
