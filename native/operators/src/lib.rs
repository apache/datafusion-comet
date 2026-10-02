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

//! Native execution operators for Apache DataFusion Comet.
//!
//! These are the `ExecutionPlan` implementations that need nothing from the rest of the plugin
//! beyond `datafusion-comet-common` and `datafusion-comet-spark-expr`. The planner in the `core`
//! crate builds them from Spark's protobuf plan.

// The clippy throws an error if the reference clone not wrapped into `Arc::clone`
// The lint makes easier for code reader/reviewer separate references clones from more heavyweight ones
#![deny(clippy::clone_on_ref_ptr)]

mod expand;
mod explode;
mod filter;
mod range;
mod rank_limit;
mod sample;

pub use expand::ExpandExec;
pub use explode::ExplodeExec;
pub use filter::CometFilterExec;
pub use range::range_exec;
pub use rank_limit::{PartitionedRankLimitExec, WindowFnKind};
pub use sample::SampleExec;
