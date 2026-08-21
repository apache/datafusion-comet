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

//! `comet-contrib-delta`: delta-kernel-rs integration for Comet.
//!
//! BUILD-GATE STUB. This unit establishes the crate boundary and the one symbol core's
//! `planner::delta_scan` shim calls -- [`planner::plan_delta_scan`] -- so a
//! `--features contrib-delta` build compiles end to end. The function returns a clean
//! "not implemented" error today; the real kernel-read scan path (log replay, deletion
//! vectors, column mapping, row tracking) lands in later units. Linked into `libcomet`
//! only when core's `contrib-delta` feature is on; default builds carry zero Delta code.

pub mod planner;
