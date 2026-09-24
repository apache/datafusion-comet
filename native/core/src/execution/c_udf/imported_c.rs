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
//!
//! Nothing in that sequence is serialized against other callers: the
//! whole point of building a fresh impl per call is that concurrent
//! Spark tasks sharing one adapter never touch the same mutable state.
//! See the `kernel` field docs for why the kernel itself needs no lock.

use std::ffi::CStr;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use comet_udf_sdk::c_abi::{CometCScalarKernel, CometCScalarKernelImpl};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use libloading::Library;

/// Adapter wrapping a [`CometCScalarKernel`] as a DataFusion
/// [`ScalarUDFImpl`].
pub struct ImportedCScalarUdf {
    name: String,
    /// Boxed so the kernel's address is stable across moves of `self`;
    /// the callbacks are handed `&*kernel` as a raw pointer.
    ///
    /// Shared, not locked. One `ImportedCScalarUdf` is shared by every
    /// caller in the process: the planner builds each task's
    /// `ScalarFunctionExpr` from the process-wide library cache
    /// (`ScalarUDF::new_from_shared_impl` over the cached `Arc`), and
    /// concurrent Spark tasks in an executor run as separate threads, so
    /// concurrent `invoke_with_args` calls on this instance are the
    /// normal case rather than an unusual one.
    ///
    /// That is safe because the only kernel-level callbacks reached from
    /// here, `function_name` and `new_impl`, take
    /// `*const CometCScalarKernel` and the ABI requires both to be
    /// callable concurrently (see the field docs in `comet-udf-sdk`).
    /// Everything mutable is per-call: `new_impl` writes a fresh
    /// `CometCScalarKernelImpl` that never leaves the stack frame that
    /// built it, and `init` / `execute` / `get_last_error` are only ever
    /// reached through that owned instance.
    ///
    /// An earlier revision wrapped this in a `Mutex`, which made every
    /// batch of a given UDF serialize through one lock per executor
    /// process, so a UDF over a wide scan ran at roughly one core no
    /// matter how many tasks were in flight.
    kernel: Box<CometCScalarKernel>,
    signature: Signature,
    /// The library the kernel's callbacks live in, so the adapter cannot
    /// outlive it. Declared after `kernel` on purpose: fields drop in
    /// declaration order, and dropping the kernel calls its `release`
    /// callback, which has to run while the library is still loaded.
    ///
    /// This does not cover the arrays `invoke_with_args` returns, whose
    /// release callbacks also live in the library and which hold no
    /// reference to it. That is safe only because `cache::get_or_load`
    /// never unloads a library.
    _library: Arc<Library>,
}

impl ImportedCScalarUdf {
    /// Construct from an owned C kernel and the library it was read from.
    ///
    /// Reads the kernel's name via its `function_name` callback and
    /// stores it for `name()` lookups.
    pub fn try_new(kernel: Box<CometCScalarKernel>, library: Arc<Library>) -> Result<Self, String> {
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
            kernel,
            signature,
            _library: library,
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
        let mut impl_state = CometCScalarKernelImpl::default();
        let new_impl_cb = self
            .kernel
            .new_impl
            .ok_or_else(|| DataFusionError::Internal("new_impl is null".into()))?;
        // SAFETY: new_impl_cb is the FFI-supplied factory; impl_state is a
        // caller-allocated default value the cdylib writes into.
        unsafe {
            new_impl_cb(self.kernel.as_ref() as *const _, &mut impl_state);
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

        // Build a fresh impl_state; init then execute.
        let new_impl_cb = self
            .kernel
            .new_impl
            .ok_or_else(|| DataFusionError::Internal("new_impl is null".into()))?;
        let mut impl_state = CometCScalarKernelImpl::default();
        // SAFETY: see return_type.
        unsafe {
            new_impl_cb(self.kernel.as_ref() as *const _, &mut impl_state);
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
        Ok(ColumnarValue::Array(conform_to_promised_type(
            &self.name,
            array,
            args.return_field.data_type(),
        )?))
    }
}

/// Give `array` the type the planner promised DataFusion for this call.
///
/// The planner promises the kernel's own return type with list and map
/// child fields renamed to Comet's canonical names (see
/// `canonicalize_child_names`), because every other Comet expression uses
/// those names and operators that combine arrays, such as `if`, reject a
/// column whose type differs from their schema by a field name. The kernel
/// may use any names it likes, since they are positional in the Arrow
/// format, so the result is renamed here. The cast only relabels the
/// fields; the buffers are reused.
///
/// Anything beyond a naming difference is an error rather than a cast: the
/// planner checked the kernel's type, and the SDK checks every result
/// against it, so a real type difference here means the kernel's
/// `return_field` changed between planning and execution.
fn conform_to_promised_type(
    name: &str,
    array: ArrayRef,
    promised: &DataType,
) -> datafusion::common::Result<ArrayRef> {
    if array.data_type() == promised {
        return Ok(array);
    }
    if !array.data_type().equals_datatype(promised) {
        return Err(DataFusionError::Execution(format!(
            "{name}: returned {} but was planned as {promised}",
            array.data_type()
        )));
    }
    Ok(arrow::compute::cast(&array, promised)?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::c_udf::cache::get_or_load;
    use crate::execution::c_udf::test_support::{test_udfs_path, BUILD_HINT};
    use arrow::array::{Array, AsArray, Int64Array};
    use arrow::datatypes::{FieldRef, Int64Type};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ScalarUDFImpl;
    use std::sync::Arc;

    fn udf_from(lib: &super::super::loader::LoadedLibrary, name: &str) -> Arc<dyn ScalarUDFImpl> {
        Arc::clone(
            &lib.udfs
                .iter()
                .find(|u| u.name == name)
                .unwrap_or_else(|| panic!("{name} not exported"))
                .udf_impl,
        )
    }

    /// Goes through the process-wide cache, as the planner does.
    fn cached_udf(name: &str) -> Arc<dyn ScalarUDFImpl> {
        udf_from(&get_or_load(test_udfs_path()).expect(BUILD_HINT), name)
    }

    fn add_one_c() -> Arc<dyn ScalarUDFImpl> {
        cached_udf("add_one_c")
    }

    fn call(
        udf: &Arc<dyn ScalarUDFImpl>,
        args: Vec<ColumnarValue>,
        number_rows: usize,
        return_type: DataType,
    ) -> datafusion::common::Result<ArrayRef> {
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("a", a.data_type(), true)))
            .collect();
        let out = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })?;
        match out {
            ColumnarValue::Array(a) => Ok(a),
            ColumnarValue::Scalar(_) => panic!("expected an array"),
        }
    }

    fn int64_args(values: &[i64], return_field: &FieldRef) -> ScalarFunctionArgs {
        let array: Int64Array = values.iter().copied().map(Some).collect();
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(array))],
            arg_fields: vec![Arc::new(Field::new("a", DataType::Int64, true))],
            number_rows: values.len(),
            return_field: Arc::clone(return_field),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    /// One adapter is shared by every Spark task in an executor, so
    /// `invoke_with_args` has to be callable concurrently. This is the
    /// property that lets the adapter hold the kernel without a lock; if a
    /// lock ever comes back this test still passes but the throughput
    /// claim in the `kernel` field docs no longer holds.
    #[test]
    fn concurrent_invocations_share_one_adapter() {
        let udf = add_one_c();
        let return_field = Arc::new(Field::new("add_one_c", DataType::Int64, true));

        let threads: Vec<_> = (0..8)
            .map(|t| {
                let udf = Arc::clone(&udf);
                let return_field = Arc::clone(&return_field);
                std::thread::spawn(move || {
                    // Each thread drives its own batch of distinct values, so a
                    // kernel that leaked state between concurrent calls would
                    // produce another thread's answers rather than its own.
                    let base = (t as i64) * 1000;
                    let inputs: Vec<i64> = (base..base + 64).collect();
                    for _ in 0..50 {
                        let out = udf
                            .invoke_with_args(int64_args(&inputs, &return_field))
                            .expect("invoke");
                        let array = match out {
                            ColumnarValue::Array(a) => a,
                            ColumnarValue::Scalar(_) => panic!("expected an array"),
                        };
                        let array = array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("Int64Array");
                        assert_eq!(array.len(), inputs.len());
                        for (i, input) in inputs.iter().enumerate() {
                            assert_eq!(array.value(i), input + 1);
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().expect("worker thread panicked");
        }
    }

    /// The adapter keeps its library loaded, so it stays usable after the `LoadedLibrary` it came
    /// from is dropped. The library is copied to a path of its own first: the dynamic loader
    /// reference-counts by file, and the cache holds the original open for the whole test process,
    /// which would keep it mapped regardless of what the adapter holds.
    #[test]
    fn adapter_keeps_its_library_loaded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let copy = dir
            .path()
            .join(test_udfs_path().file_name().expect("file name"));
        std::fs::copy(test_udfs_path(), &copy).expect(BUILD_HINT);

        let lib = crate::execution::c_udf::loader::load(&copy).expect("load copy");
        let udf = udf_from(&lib, "add_one_c");
        drop(lib);

        let out = call(
            &udf,
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                1, 2, 3,
            ])))],
            3,
            DataType::Int64,
        )
        .expect("invoke after the LoadedLibrary was dropped");
        assert_eq!(out.as_primitive::<Int64Type>().values(), &[2, 3, 4]);
        // The result's release callback lives in the library, so it goes before the adapter does.
        drop(out);
        drop(udf);
    }

    /// A literal argument reaches the adapter as a `ColumnarValue::Scalar` and has to be expanded
    /// to the batch length before the kernel sees it, in either argument position.
    #[test]
    fn scalar_arguments_are_expanded_to_the_batch() {
        let sub = cached_udf("sub_c");
        let column = || ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2, 3])));
        let ten = || ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));

        let out = call(&sub, vec![column(), ten()], 3, DataType::Int64).expect("column - 10");
        assert_eq!(out.as_primitive::<Int64Type>().values(), &[-9, -8, -7]);

        let out = call(&sub, vec![ten(), column()], 3, DataType::Int64).expect("10 - column");
        assert_eq!(out.as_primitive::<Int64Type>().values(), &[9, 8, 7]);

        let out = call(
            &add_one_c(),
            vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(41)))],
            2,
            DataType::Int64,
        )
        .expect("all-literal call");
        assert_eq!(out.as_primitive::<Int64Type>().values(), &[42, 42]);
    }

    /// `make_map_c` builds its map with arrow-rs's default `keys` / `values` names. The adapter
    /// hands it back under the canonical names the planner promised.
    #[test]
    fn result_is_relabelled_to_the_promised_child_names() {
        let make_map = cached_udf("make_map_c");
        let kernel_type = make_map
            .return_type(&[DataType::Int64])
            .expect("return_type");
        let promised = crate::execution::c_udf::canonicalize_child_names(&kernel_type);
        assert_ne!(
            kernel_type, promised,
            "make_map_c should use non-canonical names"
        );

        let out = call(
            &make_map,
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                Some(1),
                None,
            ])))],
            2,
            promised.clone(),
        )
        .expect("invoke");
        assert_eq!(out.data_type(), &promised);
        assert!(out.is_valid(0) && out.is_null(1));
    }

    /// A result that differs from the promised type by more than child names is an error, not a
    /// cast.
    #[test]
    fn result_of_a_different_type_is_not_cast() {
        let err = call(
            &add_one_c(),
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![1])))],
            1,
            DataType::Int32,
        )
        .unwrap_err();
        assert!(err.to_string().contains("was planned as Int32"), "{err}");
    }
}
