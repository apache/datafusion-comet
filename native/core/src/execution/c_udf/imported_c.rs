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

//! Adapter wrapping a C-ABI [`CometCScalarKernel`] as a DataFusion
//! [`ScalarUDFImpl`].
//!
//! Lifecycle inside `invoke_with_args`:
//!
//! 1. Build a fresh [`CometCScalarKernelImpl`] via the kernel's `new_impl`.
//! 2. Call `init` with the input field types to get the return type.
//!    `scalar_args` is always passed as NULL: any `ColumnarValue::Scalar`
//!    is expanded to a full-length array before it reaches a kernel, so
//!    there is nothing to bind. See the field docs in `comet-udf-sdk`.
//! 3. Call `execute` once with the batch.
//! 4. Drop the impl (its `release` callback runs).
//!
//! Steps 1 and 2 repeat work `return_type` already did at planning time,
//! since argument types do not change across batches. See
//! <https://github.com/apache/datafusion-comet/issues/5296>.

use std::ffi::CStr;
use std::sync::Mutex;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use comet_udf_sdk::c_abi::{CometCScalarKernel, CometCScalarKernelImpl};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// Adapter wrapping a [`CometCScalarKernel`] as a DataFusion
/// [`ScalarUDFImpl`].
pub struct ImportedCScalarUdf {
    name: String,
    /// Boxed so the kernel's address is stable, and behind a `Mutex` so
    /// that only one thread is inside the cdylib's callbacks at a time.
    ///
    /// The lock is load-bearing, not defensive. One `ImportedCScalarUdf`
    /// is shared by every caller in the process: the planner builds each
    /// task's `ScalarFunctionExpr` from the process-wide library cache
    /// (`ScalarUDF::new_from_shared_impl` over the cached `Arc`), and
    /// concurrent Spark tasks in an executor run as separate threads, so
    /// concurrent `invoke_with_args` calls on this instance are the normal
    /// case rather than an unusual one.
    ///
    /// Rust's aliasing rules alone would not require the lock: the
    /// callbacks reached from here (`function_name`, `new_impl`) take
    /// `*const CometCScalarKernel`, and per-batch mutable state lives in
    /// the `CometCScalarKernelImpl` each call builds for itself. What the
    /// lock stands in for is an ABI guarantee that does not exist yet:
    /// ABI v1 does not require a kernel's `new_impl` to be callable
    /// concurrently, and the kernel is arbitrary user code.
    ///
    /// The cost is that all batches of a given UDF serialize through one
    /// mutex per process. Removing it means requiring thread-safe
    /// callbacks in the ABI, or holding a kernel per task instead of one
    /// per process; see
    /// <https://github.com/apache/datafusion-comet/issues/5252>.
    kernel: Mutex<Box<CometCScalarKernel>>,
    signature: Signature,
}

impl ImportedCScalarUdf {
    /// Construct from an owned C kernel.
    ///
    /// Reads the kernel's name via its `function_name` callback and
    /// stores it for `name()` lookups; the kernel itself is held inside
    /// a mutex.
    pub fn try_new(kernel: Box<CometCScalarKernel>) -> Result<Self, String> {
        let function_name_cb = kernel
            .function_name
            .ok_or_else(|| "kernel.function_name is null".to_string())?;
        let _ = kernel
            .new_impl
            .ok_or_else(|| "kernel.new_impl is null".to_string())?;

        // SAFETY: function_name_cb is the FFI-supplied callback;
        // implementations promise the returned pointer is a NUL-terminated
        // UTF-8 string valid for the lifetime of the kernel.
        let name_ptr = unsafe { function_name_cb(kernel.as_ref() as *const _) };
        if name_ptr.is_null() {
            return Err("function_name returned null".into());
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }
            .to_str()
            .map_err(|e| format!("function_name not UTF-8: {e}"))?
            .to_string();

        // Use UserDefined signature: per-call init() is what decides
        // whether the input types are acceptable. `coerce_types` is not
        // implemented; user must pass exact types from the JVM register call.
        //
        // Volatility is always Immutable. The signature is built once per
        // library load, while determinism is declared per registration, so
        // the two do not line up: `CometNativeUDF.register` rejects
        // `deterministic = false` rather than let a volatile function be
        // planned as if it were pure.
        let signature = Signature::new(TypeSignature::UserDefined, Volatility::Immutable);

        Ok(Self {
            name,
            kernel: Mutex::new(kernel),
            signature,
        })
    }
}

impl std::fmt::Debug for ImportedCScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedCScalarUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl PartialEq for ImportedCScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for ImportedCScalarUdf {}

impl std::hash::Hash for ImportedCScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for ImportedCScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        // Build a fresh impl, call init, drop. Done at planning time so
        // the planner can know the output type before execution.
        let kernel = self.kernel.lock().unwrap();
        let mut impl_state = CometCScalarKernelImpl::default();
        let new_impl_cb = kernel
            .new_impl
            .ok_or_else(|| DataFusionError::Internal("new_impl is null".into()))?;
        // SAFETY: new_impl_cb is the FFI-supplied factory; impl_state is a
        // caller-allocated default value the cdylib writes into.
        unsafe {
            new_impl_cb(kernel.as_ref() as *const _, &mut impl_state);
        }

        // Build input fields and FFI schemas.
        let fields: Vec<Field> = args
            .iter()
            .map(|dt| Field::new("", dt.clone(), true))
            .collect();
        let ffi_schemas = build_ffi_schemas(&fields)?;
        let ffi_schema_ptrs: Vec<*const FFI_ArrowSchema> =
            ffi_schemas.iter().map(|s| s as *const _).collect();

        let init_cb = impl_state
            .init
            .ok_or_else(|| DataFusionError::Internal("kernel impl missing init".into()))?;
        let mut out_schema = FFI_ArrowSchema::empty();
        // SAFETY: pointers are valid for the duration of the call.
        let rc = unsafe {
            init_cb(
                &mut impl_state,
                ffi_schema_ptrs.as_ptr(),
                std::ptr::null(),
                fields.len() as i64,
                &mut out_schema,
            )
        };
        if rc != 0 {
            let msg = read_last_error(&mut impl_state);
            return Err(DataFusionError::Plan(format!(
                "{}: init failed: {msg}",
                self.name
            )));
        }
        let return_field = Field::try_from(&out_schema)
            .map_err(|e| DataFusionError::Internal(format!("decoding return type: {e}")))?;
        Ok(return_field.data_type().clone())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let n_rows = args.number_rows;
        let kernel = self.kernel.lock().unwrap();

        // Build a fresh impl_state; init then execute.
        let new_impl_cb = kernel
            .new_impl
            .ok_or_else(|| DataFusionError::Internal("new_impl is null".into()))?;
        let mut impl_state = CometCScalarKernelImpl::default();
        // SAFETY: see return_type.
        unsafe {
            new_impl_cb(kernel.as_ref() as *const _, &mut impl_state);
        }

        // Resolve args to Arrays of length n_rows or 1.
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(args.args.len());
        for a in args.args {
            let arr = match a {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(s) => s.to_array_of_size(n_rows)?,
            };
            arrays.push(arr);
        }

        // Build input fields + schemas (the kernel needs init to remember
        // the arg types for execute).
        let fields: Vec<Field> = arrays
            .iter()
            .map(|a| Field::new("", a.data_type().clone(), true))
            .collect();
        let ffi_schemas = build_ffi_schemas(&fields)?;
        let ffi_schema_ptrs: Vec<*const FFI_ArrowSchema> =
            ffi_schemas.iter().map(|s| s as *const _).collect();

        let init_cb = impl_state
            .init
            .ok_or_else(|| DataFusionError::Internal("kernel impl missing init".into()))?;
        let mut out_schema = FFI_ArrowSchema::empty();
        // SAFETY: ffi_schema_ptrs lives for the duration of this call.
        let rc = unsafe {
            init_cb(
                &mut impl_state,
                ffi_schema_ptrs.as_ptr(),
                std::ptr::null(),
                fields.len() as i64,
                &mut out_schema,
            )
        };
        if rc != 0 {
            let msg = read_last_error(&mut impl_state);
            return Err(DataFusionError::Execution(format!(
                "{}: init failed: {msg}",
                self.name
            )));
        }
        let return_field = Field::try_from(&out_schema)
            .map_err(|e| DataFusionError::Internal(format!("decoding return type: {e}")))?;

        // Build FFI arrays.
        let mut ffi_arrays: Vec<FFI_ArrowArray> = arrays
            .iter()
            .map(|a| FFI_ArrowArray::new(&a.to_data()))
            .collect();
        let ffi_array_ptrs: Vec<*mut FFI_ArrowArray> =
            ffi_arrays.iter_mut().map(|x| x as *mut _).collect();

        let execute_cb = impl_state
            .execute
            .ok_or_else(|| DataFusionError::Internal("kernel impl missing execute".into()))?;
        let mut out_arr = FFI_ArrowArray::empty();
        // SAFETY: ffi_array_ptrs live for the duration of this call. The
        // kernel takes ownership of each input by replacing it with an
        // empty FFI_ArrowArray (no-op Drop).
        let rc = unsafe {
            execute_cb(
                &mut impl_state,
                ffi_array_ptrs.as_ptr(),
                arrays.len() as i64,
                n_rows as i64,
                &mut out_arr,
            )
        };

        if rc != 0 {
            let msg = read_last_error(&mut impl_state);
            return Err(DataFusionError::Execution(format!(
                "{}: execute failed: {msg}",
                self.name
            )));
        }

        // Import result.
        // SAFETY: out_arr was filled by the cdylib.
        let data = unsafe { from_ffi_and_data_type(out_arr, return_field.data_type().clone()) }
            .map_err(|e| DataFusionError::Execution(format!("from_ffi: {e}")))?;
        let array = arrow::array::make_array(data);
        if array.len() != n_rows {
            return Err(DataFusionError::Execution(format!(
                "{}: returned {} rows, expected {n_rows}",
                self.name,
                array.len()
            )));
        }
        Ok(ColumnarValue::Array(array))
    }
}

fn build_ffi_schemas(fields: &[Field]) -> datafusion::common::Result<Vec<FFI_ArrowSchema>> {
    fields
        .iter()
        .map(FFI_ArrowSchema::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Internal(format!("encoding arg type: {e}")))
}

fn read_last_error(impl_state: &mut CometCScalarKernelImpl) -> String {
    let cb = match impl_state.get_last_error {
        Some(cb) => cb,
        None => return "(no get_last_error)".to_string(),
    };
    // SAFETY: cb is the FFI-supplied callback.
    let ptr = unsafe { cb(impl_state) };
    if ptr.is_null() {
        return "(empty)".to_string();
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}
