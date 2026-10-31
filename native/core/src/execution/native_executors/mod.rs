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

//! Running Comet plans outside a Spark executor, on Comet Native Executors. See
//! <https://github.com/apache/datafusion-comet/issues/7288>.
//!
//! A Spark driver splits a query into stages at its exchanges. Each stage is one native block:
//! the serialized `Operator` tree a Spark task would run, whose input leaves (`Scan` and
//! `ShuffleScan`) read the stage's inputs. [`CometFragmentExec`] runs such a block for each
//! partition of a stage, reading its inputs from other DataFusion plans, such as a distributed
//! engine's shuffle readers, instead of from the JVM.

mod fragment;

pub use fragment::{block_schema, partition_descriptor, CometFragmentExec};

/// The version of this Comet build. A Comet Native Executor cluster requires a driver's to match
/// its own, together with [`datafusion_comet_proto::PROTO_FINGERPRINT`].
pub const COMET_VERSION: &str = env!("CARGO_PKG_VERSION");
