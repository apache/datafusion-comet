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

//! `RustUdfAdapter`: wraps a UDF loaded from a cdylib as a DataFusion
//! `ScalarUDFImpl` so it can plug into DataFusion's expression
//! evaluation pipeline.

use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use crate::execution::rust_udf::loader::{LoadedLibrary, LoadedUdf};
use crate::execution::rust_udf::symbols;
use crate::execution::rust_udf::{FreeErrFn, InvokeFn};

/// DataFusion adapter for a Rust UDF loaded from a cdylib.
///
/// Holds:
/// - an `Arc<LoadedLibrary>` so the cdylib outlives the adapter,
/// - a clone of the per-UDF descriptor (`LoadedUdf`),
/// - a precomputed DataFusion `Signature`,
/// - cached raw function pointers to avoid per-call `dlsym` lookups.
///
/// On every batch, `invoke_with_args` exports each input via the Arrow
/// C Data Interface, calls the cdylib's `comet_udf_invoke`, and imports
/// the result.
pub struct RustUdfHandle {
    library: Arc<LoadedLibrary>,
    udf: LoadedUdf,
    signature: Signature,
    invoke_fn: InvokeFn,
    free_err_fn: FreeErrFn,
}

impl RustUdfHandle {
    /// Build an adapter for `udf` resolved from `library`.
    pub fn new(library: Arc<LoadedLibrary>, udf: LoadedUdf) -> Self {
        let volatility = match udf.volatility {
            0 => Volatility::Immutable,
            1 => Volatility::Stable,
            _ => Volatility::Volatile,
        };
        let signature = Signature::new(TypeSignature::Exact(udf.args.clone()), volatility);

        // SAFETY: comet_udf_invoke and comet_udf_free_error were verified
        // present at load time (see loader::load); resolving them here is
        // a fresh dlsym + cast to fn pointer. The library lives as long
        // as `library` (Arc) which we hold, so the function pointers are
        // valid for the lifetime of self.
        let invoke_fn: InvokeFn = unsafe {
            *library
                .library
                .get::<InvokeFn>(symbols::INVOKE)
                .expect("comet_udf_invoke verified at load time")
        };
        let free_err_fn: FreeErrFn = unsafe {
            *library
                .library
                .get::<FreeErrFn>(symbols::FREE_ERROR)
                .expect("comet_udf_free_error verified at load time")
        };

        Self { library, udf, signature, invoke_fn, free_err_fn }
    }
}

impl std::fmt::Debug for RustUdfHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RustUdfHandle {{ name={}, lib={} }}",
            self.udf.name,
            self.library.path.display()
        )
    }
}

impl PartialEq for RustUdfHandle {
    fn eq(&self, other: &Self) -> bool {
        self.udf.name == other.udf.name
            && self.udf.return_type == other.udf.return_type
            && self.udf.args == other.udf.args
            && self.library.path == other.library.path
    }
}

impl Eq for RustUdfHandle {}

impl std::hash::Hash for RustUdfHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.udf.name.hash(state);
        self.udf.return_type.hash(state);
        self.udf.args.hash(state);
        self.library.path.hash(state);
    }
}

impl ScalarUDFImpl for RustUdfHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.udf.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.udf.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let n = args.number_rows;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(args.args.len());
        for a in args.args {
            let arr = match a {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
            };
            arrays.push(arr);
        }

        let mut in_arrays: Vec<FFI_ArrowArray> = Vec::with_capacity(arrays.len());
        let mut in_schemas: Vec<FFI_ArrowSchema> = Vec::with_capacity(arrays.len());
        for arr in &arrays {
            let (a, s) = to_ffi(&arr.to_data())
                .map_err(|e| DataFusionError::Execution(format!("rust UDF to_ffi: {e}")))?;
            in_arrays.push(a);
            in_schemas.push(s);
        }

        let mut out_arr = FFI_ArrowArray::empty();
        let mut out_sch = FFI_ArrowSchema::empty();
        let mut err = comet_udf_sdk::error::UdfError::zeroed();

        let invoke = self.invoke_fn;
        let free_err = self.free_err_fn;

        // SAFETY: We're upholding the comet_udf_invoke C ABI:
        // - in_arrays/in_schemas are valid for `arrays.len()` elements.
        // - out_arr/out_sch are caller-allocated, zeroed.
        // - err is caller-allocated, zeroed.
        // The cdylib will replace input slots with FFI_ArrowArray::empty()
        // (taking ownership of the Arrow data) and write into the output
        // slots on success.
        let rc = unsafe {
            invoke(
                self.udf.idx,
                in_arrays.as_ptr(),
                in_schemas.as_ptr(),
                arrays.len() as u32,
                &mut out_arr,
                &mut out_sch,
                &mut err,
            )
        };

        // `in_arrays` / `in_schemas` go out of scope at end of function. On
        // success the cdylib has replaced each slot with `FFI_ArrowArray::empty()`
        // (no-op Drop). On a partial-failure rc != 0 path, any un-replaced slots
        // retain their original release callbacks and are freed correctly by
        // Vec::drop — preventing leaks if the cdylib bails mid-loop.

        if rc != 0 {
            // SAFETY: err points at our local UdfError. If invoke wrote a
            // message it's owned by the cdylib's allocator and must be
            // freed via free_err.
            let msg = unsafe { read_err_msg(&err) };
            unsafe { free_err(&mut err) };
            return Err(DataFusionError::Execution(format!(
                "rust UDF '{}' failed (rc={rc}): {msg}",
                self.udf.name
            )));
        }

        // SAFETY: out_arr and out_sch were filled by the cdylib's
        // comet_udf_invoke on success; ownership now belongs to us.
        let array_data = unsafe { from_ffi(out_arr, &out_sch) }
            .map_err(|e| DataFusionError::Execution(format!("rust UDF from_ffi: {e}")))?;
        let array = arrow::array::make_array(array_data);
        if array.len() != n {
            return Err(DataFusionError::Execution(format!(
                "rust UDF '{}' returned {} rows, expected {n}",
                self.udf.name,
                array.len()
            )));
        }
        if array.data_type() != &self.udf.return_type {
            return Err(DataFusionError::Execution(format!(
                "rust UDF '{}' returned type {:?}, expected {:?}",
                self.udf.name,
                array.data_type(),
                self.udf.return_type
            )));
        }
        Ok(ColumnarValue::Array(array))
    }
}

/// Read the UTF-8 message field of an `UdfError` as a `String`. Returns
/// "(no message)" if the message pointer is null.
///
/// # Safety
/// `err` must point at a valid `UdfError` whose message (if non-null)
/// is a NUL-terminated UTF-8 (or otherwise valid C) string.
unsafe fn read_err_msg(err: &comet_udf_sdk::error::UdfError) -> String {
    if err.message.is_null() {
        return String::from("(no message)");
    }
    let cstr = std::ffi::CStr::from_ptr(err.message);
    cstr.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    use crate::execution::rust_udf::cache::get_or_load;
    use crate::execution::rust_udf::test_support::test_udfs_path;

    fn handle(name: &str) -> RustUdfHandle {
        let lib = get_or_load(test_udfs_path()).unwrap();
        let udf = lib.udfs.iter().find(|u| u.name == name).unwrap().clone();
        RustUdfHandle::new(lib, udf)
    }

    #[tokio::test]
    async fn add_one_runs_through_datafusion() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(handle("add_one")));
        let df = ctx
            .sql("SELECT add_one(CAST(x AS BIGINT)) AS y FROM (VALUES (1),(2),(3)) t(x)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array");
        let mut values: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        values.sort();
        assert_eq!(values, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn always_err_returns_error() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(handle("always_err")));
        let df = ctx
            .sql("SELECT always_err(CAST(x AS BIGINT)) FROM (VALUES (1)) t(x)")
            .await
            .unwrap();
        let r = df.collect().await;
        let err = r.expect_err("expected error");
        assert!(
            err.to_string().contains("intentional failure"),
            "msg: {err}"
        );
    }

    #[tokio::test]
    async fn always_panic_returns_error() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(handle("always_panic")));
        let df = ctx
            .sql("SELECT always_panic(CAST(x AS BIGINT)) FROM (VALUES (1)) t(x)")
            .await
            .unwrap();
        let r = df.collect().await;
        let err = r.expect_err("expected error");
        assert!(
            err.to_string().to_lowercase().contains("panic"),
            "msg: {err}"
        );
    }

    #[tokio::test]
    async fn length_one_returns_error() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(handle("length_one")));
        let df = ctx
            .sql("SELECT length_one(CAST(x AS BIGINT)) FROM (VALUES (1),(2),(3)) t(x)")
            .await
            .unwrap();
        let r = df.collect().await;
        let err = r.expect_err("expected error");
        assert!(
            err.to_string().contains("returned 1 rows"),
            "msg: {err}"
        );
    }
}
