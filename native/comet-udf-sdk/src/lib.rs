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

//! SDK for writing scalar UDFs in Rust that are loaded and executed by
//! Apache DataFusion Comet.
//!
//! See `docs/source/user-guide/latest/custom-rust-udfs.md` in the Comet
//! repository for a complete walkthrough.

#![warn(missing_docs)]

/// ABI version emitted by `export!`. Bumped only when the C ABI surface
/// or the descriptor wire format changes in a backwards-incompatible way.
pub const COMET_UDF_ABI_VERSION: u32 = 1;

pub mod types;

pub use types::{
    field_from_ipc_bytes, field_to_ipc_bytes, ArrowTypeTag, CometUdfSignature, Volatility,
};

pub mod error;

pub use error::{CometUdfError, UdfError, UdfErrorCode};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_version_is_one() {
        assert_eq!(COMET_UDF_ABI_VERSION, 1);
    }
}
