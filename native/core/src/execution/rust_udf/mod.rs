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

//! Loader and adapters for user-supplied Rust UDF cdylibs registered
//! through `CometRustUDF` on the JVM side.
//!
//! Two ABI flavors are supported, both built only on Arrow's stable FFI
//! (the C Data Interface):
//!
//! - **C ABI** — `comet_c_udf_list_v1` returns sedona-style
//!   `CometCScalarKernel` factory structs. Decoupled from datafusion
//!   versions; suitable for cross-language (C/C++) extensions.
//!
//! - **datafusion-ffi ABI** — `comet_df_udf_list_v1` returns
//!   `FFI_ScalarUDF` values. The user's library inherits the entire
//!   `ScalarUDFImpl` surface for free, at the cost of a major-version
//!   pin against `datafusion-ffi`.
//!
//! A single library may export either or both. The loader walks both
//! discovery functions and exposes whichever it finds; in the rare case
//! of a name collision the C ABI flavor wins (favoring the
//! version-decoupled implementation).

pub mod cache;
pub mod imported_c;
pub mod loader;

#[cfg(test)]
pub(crate) mod test_support;
