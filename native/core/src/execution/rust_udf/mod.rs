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

//! Loader and adapters for user-supplied Rust UDFs distributed as
//! cdylibs and registered through the Scala `CometRustUDF` API.
//!
//! The submodules are: `loader` opens cdylibs via `libloading`, `cache`
//! keeps a process-wide handle table so the same path is loaded once,
//! and `adapter` wraps each loaded UDF as a DataFusion `ScalarUDFImpl`.

pub mod adapter;
pub mod cache;
pub mod loader;

/// Canonical type for the `comet_udf_invoke` symbol.
pub(crate) type InvokeFn = unsafe extern "C" fn(
    u32,
    *const arrow::ffi::FFI_ArrowArray,
    *const arrow::ffi::FFI_ArrowSchema,
    u32,
    *mut arrow::ffi::FFI_ArrowArray,
    *mut arrow::ffi::FFI_ArrowSchema,
    *mut comet_udf_sdk::error::UdfError,
) -> i32;

/// Canonical type for the `comet_udf_free_error` symbol.
pub(crate) type FreeErrFn = unsafe extern "C" fn(*mut comet_udf_sdk::error::UdfError);

#[cfg(test)]
pub(crate) mod test_support;

/// Symbol names exported by `comet-udf-sdk`'s `export!` macro.
pub(crate) mod symbols {
    pub(crate) const ABI_VERSION: &[u8] = b"comet_udf_abi_version";
    pub(crate) const COUNT: &[u8] = b"comet_udf_count";
    pub(crate) const DESCRIBE: &[u8] = b"comet_udf_describe";
    pub(crate) const INVOKE: &[u8] = b"comet_udf_invoke";
    pub(crate) const FREE_ERROR: &[u8] = b"comet_udf_free_error";
}
