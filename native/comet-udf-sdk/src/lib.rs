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
//! Apache DataFusion Comet, using only Arrow's stable FFI surface.
//!
//! The ABI is a pure C-callable struct of function pointers built only on
//! the Arrow C Data Interface (`FFI_ArrowSchema` / `FFI_ArrowArray`),
//! modeled on Apache Sedona's `SedonaCScalarKernel`. See [`c_abi`] for the
//! authoring guide.
//!
//! # Why not `datafusion-ffi`?
//!
//! Wrapping the user's `ScalarUDFImpl` as `datafusion_ffi::udf::FFI_ScalarUDF`
//! is the obvious alternative, and it hands the author a much larger surface
//! for free: variadic signatures, type coercion, metadata-aware return types.
//! Comet deliberately does not expose it, for one reason: it would couple
//! every user's cdylib to Comet's DataFusion major version.
//!
//! That is not a hypothetical cost. A prototype carried both ABIs side by
//! side; upgrading Comet from DataFusion 53 to 54 removed `as_any` from
//! `ScalarUDFImpl`, which would have forced an edit and a recompile on every
//! user library built against the `datafusion-ffi` flavor. The C ABI here
//! needed no change, because no DataFusion type appears in it.
//!
//! Comet tracks DataFusion closely and upgrades often, so a UDF ABI pinned to
//! the DataFusion version would break users on a cadence they do not control
//! and cannot opt out of. Keeping the FFI surface to Arrow's C Data Interface
//! means a UDF compiled today keeps working across Comet upgrades, and the
//! same ABI is implementable from C, C++, or any language that speaks the
//! Arrow C Data Interface.
//!
//! The tradeoff is a smaller surface: authors implement [`c_abi::CometCScalarUdf`]
//! and get scalar functions over Arrow arrays, not the full `ScalarUDFImpl`
//! feature set.

#![warn(missing_docs)]

/// Discovery ABI version. Bumped on any backwards-incompatible change to
/// the discovery entry-point signatures or to the FFI structs they yield.
pub const COMET_UDF_ABI_VERSION: u32 = 1;

pub mod c_abi;

/// Symbol name of the discovery entry point exported by every cdylib.
pub const C_ABI_DISCOVERY_SYMBOL: &str = "comet_c_udf_list_v1";

/// Symbol name of the ABI version probe exported by every cdylib.
pub const ABI_VERSION_SYMBOL: &str = "comet_udf_abi_version";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_version_is_one() {
        assert_eq!(COMET_UDF_ABI_VERSION, 1);
    }
}
