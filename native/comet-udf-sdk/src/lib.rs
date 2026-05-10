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
    field_from_ipc_bytes, field_to_ipc_bytes, ArrowTypeTag, CometUdfSignature, UdfDescriptor,
    Volatility,
};

pub mod error;

pub use error::{CometUdfError, UdfError, UdfErrorCode};

use arrow::array::ArrayRef;

pub mod macros;

/// A scalar UDF invokable by Comet's native execution.
///
/// Implementations must be `Send + Sync` and stateless — Comet caches a
/// single instance per UDF and may invoke it from many worker threads
/// concurrently.
pub trait CometScalarUdf: Send + Sync {
    /// Stable function name. Must match the name passed to
    /// `CometRustUDF.register` on the Scala side.
    fn name(&self) -> &str;

    /// Declared signature.
    fn signature(&self) -> &CometUdfSignature;

    /// Evaluate `args` (one `ArrayRef` per declared argument) and return
    /// a single `ArrayRef` of length equal to the longest input.
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef, CometUdfError>;
}

/// Export one or more UDF types as a Comet-loadable cdylib.
///
/// Each type passed to the macro must implement [`Default`] and
/// [`CometScalarUdf`]. The macro emits the full set of `extern "C"`
/// entry points required by Comet's loader.
///
/// # Crate configuration
///
/// Your `Cargo.toml` must declare the user crate as a `cdylib` so the
/// `#[no_mangle]` entry points are exported as dynamic symbols:
///
/// ```toml
/// [lib]
/// crate-type = ["cdylib"]
/// ```
///
/// You also need a direct dependency on `arrow` (matching the version
/// used by Comet's host) so the FFI types in the generated extern blocks
/// resolve.
///
/// # Example
/// ```ignore
/// use comet_udf_sdk::*;
///
/// struct AddOne { /* ... */ }
/// impl Default for AddOne { /* ... */ }
/// impl CometScalarUdf for AddOne { /* ... */ }
///
/// comet_udf_sdk::export!(AddOne);
/// ```
#[macro_export]
macro_rules! export {
    ( $( $ty:ty ),+ $(,)? ) => {
        const _: () = {
            use std::sync::OnceLock;
            use $crate::macros::{
                free_error_impl, invoke_impl, EncodedSignature,
            };

            static UDFS: OnceLock<Vec<Box<dyn $crate::CometScalarUdf>>> = OnceLock::new();
            static ENCODED: OnceLock<Vec<EncodedSignature>> = OnceLock::new();

            fn udfs() -> &'static [Box<dyn $crate::CometScalarUdf>] {
                UDFS.get_or_init(|| {
                    vec![
                        $( Box::new(<$ty as Default>::default()) as Box<dyn $crate::CometScalarUdf> ),+
                    ]
                })
            }

            fn encoded() -> &'static [EncodedSignature] {
                ENCODED.get_or_init(|| {
                    udfs()
                        .iter()
                        .map(|u| EncodedSignature::from_signature(u.name(), u.signature()))
                        .collect()
                })
            }

            #[no_mangle]
            pub extern "C" fn comet_udf_abi_version() -> u32 {
                $crate::COMET_UDF_ABI_VERSION
            }

            #[no_mangle]
            pub extern "C" fn comet_udf_count() -> u32 {
                udfs().len() as u32
            }

            #[no_mangle]
            pub unsafe extern "C" fn comet_udf_describe(
                idx: u32,
                out: *mut $crate::types::UdfDescriptor,
            ) -> i32 {
                if out.is_null() { return -1; }
                let ud = udfs();
                let en = encoded();
                let i = idx as usize;
                if i >= ud.len() { return -2; }
                *out = $crate::types::UdfDescriptor::zeroed();
                en[i].fill_descriptor(ud[i].signature(), &mut *out);
                0
            }

            #[no_mangle]
            pub unsafe extern "C" fn comet_udf_invoke(
                idx: u32,
                in_arrays: *const arrow::ffi::FFI_ArrowArray,
                in_schemas: *const arrow::ffi::FFI_ArrowSchema,
                n_in: u32,
                out_array: *mut arrow::ffi::FFI_ArrowArray,
                out_schema: *mut arrow::ffi::FFI_ArrowSchema,
                err_out: *mut $crate::error::UdfError,
            ) -> i32 {
                let ud = udfs();
                let i = idx as usize;
                if i >= ud.len() { return -2; }
                invoke_impl(
                    ud[i].as_ref(),
                    in_arrays,
                    in_schemas,
                    n_in,
                    out_array,
                    out_schema,
                    err_out,
                )
            }

            #[no_mangle]
            pub unsafe extern "C" fn comet_udf_free_error(err: *mut $crate::error::UdfError) {
                free_error_impl(err)
            }
        };
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_version_is_one() {
        assert_eq!(COMET_UDF_ABI_VERSION, 1);
    }
}

#[cfg(test)]
mod trait_impl_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    struct AddOne {
        sig: CometUdfSignature,
    }

    impl Default for AddOne {
        fn default() -> Self {
            Self {
                sig: CometUdfSignature {
                    args: vec![DataType::Int64],
                    return_type: DataType::Int64,
                    volatility: Volatility::Immutable,
                },
            }
        }
    }

    impl CometScalarUdf for AddOne {
        fn name(&self) -> &str {
            "add_one"
        }
        fn signature(&self) -> &CometUdfSignature {
            &self.sig
        }
        fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
            let arr = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| CometUdfError::new("expected Int64Array"))?;
            let out: Int64Array = arr.iter().map(|v| v.map(|x| x + 1)).collect();
            Ok(Arc::new(out))
        }
    }

    #[test]
    fn add_one_invokes() {
        let udf = AddOne::default();
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let out = udf.invoke(&[input]).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.values(), &[2, 3, 4]);
    }
}
