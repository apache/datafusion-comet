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

//! Runs Apache DataFusion Comet native plans as leaves inside Apache
//! DataFusion Ballista.
//!
//! - [`scan::CometScanExec`]: a serializable DataFusion leaf that carries the
//!   Comet proto bytes (the "recipe") and builds the FFI plan at execute()
//!   time. This is what Ballista ships to executors and reconstructs there.
//! - [`codec::CometPhysicalCodec`] / [`codec::CometLogicalCodec`]: extension
//!   codecs that (de)serialize Comet nodes as their proto bytes (tagged with
//!   [`codec::COMET_MAGIC`]) and delegate everything else to Ballista's own
//!   codecs — the seam that lets Ballista distribute Comet work without
//!   linking Comet's translation code.
//! - [`table_provider::CometTableProvider`]: a `TableProvider` that produces a
//!   `CometScanExec`, so a Comet scan can participate in a DataFusion logical
//!   plan.

pub mod codec;
pub mod ffi_jni;
pub mod fragment;
pub mod scan;
pub mod table_provider;

pub use codec::{
    CometLogicalCodec, CometPhysicalCodec, COMET_FRAGMENT_MAGIC, COMET_MAGIC,
};
pub use ffi_jni::{
    build_test_proto, execute_comet_proto, execute_two_stage, submit_and_export,
    submit_and_export_distributed,
};
pub use fragment::CometFragmentExec;
pub use scan::CometScanExec;
pub use table_provider::CometTableProvider;
