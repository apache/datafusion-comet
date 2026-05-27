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

//! datafusion-ffi flavor.
//!
//! Discovery returns a list of `FFI_ScalarUDF` values produced by
//! `datafusion_ffi`. The host imports each via
//! `ForeignScalarUDF::try_from`, yielding a `ScalarUDFImpl` it can plug
//! straight into its existing planner — no further adaptation needed.
//!
//! The user's library inherits the entire `ScalarUDFImpl` surface
//! (signature with type coercion, metadata-aware return types, aliases,
//! etc.) for free, at the cost of a hard major-version pin against
//! `datafusion-ffi`.
//!
//! # Authoring a UDF
//!
//! Implement `ScalarUDFImpl` as you would for any DataFusion UDF, then:
//!
//! ```ignore
//! use comet_udf_sdk::comet_df_udf_export;
//! use datafusion::logical_expr::ScalarUDF;
//! use std::sync::Arc;
//!
//! fn make_add_one_df() -> Arc<ScalarUDF> {
//!     // ... build ScalarUDF from your impl ...
//! #   unimplemented!()
//! }
//!
//! comet_df_udf_export!(make_add_one_df);
//! ```

use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;

/// List of UDFs exposed by a cdylib via `comet_df_udf_list_v1`.
///
/// Ownership of the array transfers to the host, which frees via the
/// list's `release` callback after consuming each FFI_ScalarUDF.
#[repr(C)]
pub struct CometDfUdfList {
    /// Pointer to the FFI_ScalarUDF array, or null if `len == 0`.
    pub udfs: *mut FFI_ScalarUDF,
    /// Number of entries.
    pub len: i64,
    /// Release the array storage. Each FFI_ScalarUDF entry is released
    /// individually by its own Drop / clone_release machinery; this
    /// callback only frees the heap-allocated array itself.
    pub release: Option<unsafe extern "C" fn(*mut CometDfUdfList)>,
}

impl Default for CometDfUdfList {
    fn default() -> Self {
        Self { udfs: std::ptr::null_mut(), len: 0, release: None }
    }
}

impl Drop for CometDfUdfList {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // SAFETY: release frees the array storage. The FFI_ScalarUDF
            // entries themselves were already dropped (or moved out) by
            // the host before invoking release.
            unsafe { release(self) };
        }
    }
}

/// Build a heap-allocated `CometDfUdfList` from a vector of `Arc<ScalarUDF>`.
pub fn build_udf_list(udfs: Vec<Arc<ScalarUDF>>) -> CometDfUdfList {
    if udfs.is_empty() {
        return CometDfUdfList::default();
    }
    let ffi_udfs: Vec<FFI_ScalarUDF> =
        udfs.into_iter().map(FFI_ScalarUDF::from).collect();
    let mut boxed = ffi_udfs.into_boxed_slice();
    let len = boxed.len() as i64;
    let udfs_ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    CometDfUdfList {
        udfs: udfs_ptr,
        len,
        release: Some(c_list_release),
    }
}

unsafe extern "C" fn c_list_release(list: *mut CometDfUdfList) {
    debug_assert!(!list.is_null());
    let list_ref = unsafe { &mut *list };
    if list_ref.udfs.is_null() || list_ref.len == 0 {
        list_ref.release = None;
        return;
    }
    let len = list_ref.len as usize;
    // SAFETY: udfs was a Box<[FFI_ScalarUDF]> turned into raw ptr +
    // forgotten in build_udf_list. Reconstruct and drop. Each
    // FFI_ScalarUDF entry's Drop runs its own release callback.
    let _ = unsafe {
        Box::from_raw(std::slice::from_raw_parts_mut(list_ref.udfs, len))
    };
    list_ref.udfs = std::ptr::null_mut();
    list_ref.len = 0;
    list_ref.release = None;
}

/// Emit the datafusion-ffi discovery entry points.
///
/// Each item is the path of a function `fn() -> Arc<ScalarUDF>`. The
/// macro produces:
///
/// - `extern "C" fn comet_udf_abi_version() -> u32` (if not already exported)
/// - `extern "C" fn comet_df_udf_list_v1(out: *mut CometDfUdfList) -> i32`
///
/// Use the `_no_abi_version` variant to suppress the abi_version export
/// when emitting both `comet_c_udf_export!` and `comet_df_udf_export!` from
/// the same crate (`comet_c_udf_export!` already emits it).
#[macro_export]
macro_rules! comet_df_udf_export {
    ( $( $factory:path ),+ $(,)? ) => {
        $crate::comet_df_udf_export!(@list $($factory),+);
        const _: () = {
            #[no_mangle]
            pub extern "C" fn comet_udf_abi_version() -> u32 {
                $crate::COMET_UDF_ABI_VERSION
            }
        };
    };
    (@list $( $factory:path ),+ $(,)? ) => {
        const _: () = {
            #[no_mangle]
            pub unsafe extern "C" fn comet_df_udf_list_v1(
                out: *mut $crate::df_abi::CometDfUdfList,
            ) -> i32 {
                if out.is_null() { return -1; }
                let udfs: Vec<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> = vec![
                    $( $factory(), )+
                ];
                let list = $crate::df_abi::build_udf_list(udfs);
                std::ptr::write(out, list);
                0
            }
        };
    };
    (no_abi_version => $( $factory:path ),+ $(,)? ) => {
        $crate::comet_df_udf_export!(@list $($factory),+);
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::DataType;
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    };
    use std::any::Any;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct AddOne {
        signature: Signature,
    }
    impl AddOne {
        fn new() -> Self {
            Self {
                signature: Signature::new(
                    TypeSignature::Exact(vec![DataType::Int64]),
                    Volatility::Immutable,
                ),
            }
        }
    }
    impl ScalarUDFImpl for AddOne {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "add_one_df"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int64)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            let arr: ArrayRef = match &args.args[0] {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
            };
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Execution("not Int64".into()))?;
            let out: Int64Array = a.iter().map(|v| v.map(|x| x + 1)).collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
    }

    #[test]
    fn build_list_and_round_trip_through_ffi() {
        let udf = Arc::new(ScalarUDF::from(AddOne::new()));
        let mut list = build_udf_list(vec![udf]);
        assert_eq!(list.len, 1);

        // Read the FFI_ScalarUDF entry and import it via the canonical
        // From<&FFI_ScalarUDF> for Arc<dyn ScalarUDFImpl>.
        let entry: &FFI_ScalarUDF = unsafe { &*list.udfs };
        let foreign: Arc<dyn ScalarUDFImpl> = entry.into();
        assert_eq!(foreign.name(), "add_one_df");

        // Drop list to exercise the release path.
        drop(foreign);
        if let Some(rel) = list.release.take() {
            unsafe { rel(&mut list) };
        }
    }
}
