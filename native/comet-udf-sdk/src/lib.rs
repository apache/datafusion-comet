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
//! Two ABI flavors are available, behind cargo features:
//!
//! - **C ABI** (`c-abi` feature, on by default) — pure C-callable struct of
//!   function pointers built only on the Arrow C Data Interface
//!   (`FFI_ArrowSchema` / `FFI_ArrowArray`). Modeled on Apache Sedona's
//!   `SedonaCScalarKernel`. Works with any Arrow producer that speaks the
//!   C Data Interface — including C/C++ implementations.
//!
//! - **DataFusion FFI ABI** (`df-abi` feature, on by default) — wraps the
//!   user's `ScalarUDFImpl` as `datafusion_ffi::udf::FFI_ScalarUDF`. Larger
//!   surface, full DataFusion features (variadic signatures, type coercion,
//!   metadata-aware return types) for free, but the user's library is
//!   coupled to the matching `datafusion-ffi` major version.
//!
//! A single library may export either or both ABIs; the host loader tries
//! both discovery functions and prefers the C ABI on duplicate names.
//!
//! See `docs/source/user-guide/latest/custom-rust-udfs.md` for an
//! end-to-end walkthrough.

#![warn(missing_docs)]

/// Discovery ABI version. Bumped on any backwards-incompatible change to
/// the discovery entry-point signatures or to the FFI structs they yield.
pub const COMET_UDF_ABI_VERSION: u32 = 1;

#[cfg(feature = "c-abi")]
pub mod c_abi;

#[cfg(feature = "df-abi")]
pub mod df_abi;

/// Symbol name of the discovery entry point exported by C-ABI cdylibs.
pub const C_ABI_DISCOVERY_SYMBOL: &str = "comet_c_udf_list_v1";

/// Symbol name of the discovery entry point exported by datafusion-ffi
/// cdylibs.
pub const DF_ABI_DISCOVERY_SYMBOL: &str = "comet_df_udf_list_v1";

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
