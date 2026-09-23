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

//! In-process bridge for Spark 4.1+ scalar Arrow UDFs. Each instance owns one
//! unpickled Python callable and must be created for one Spark task/partition.
//! The public API deliberately deals in Arrow arrays; the physical operator is
//! responsible for evaluating Catalyst arguments and preserving input columns.

use arrow::array::{make_array, Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow::error::{ArrowError, Result};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

#[cfg(target_os = "linux")]
fn make_python_symbols_global() -> Result<()> {
    use std::ffi::CStr;
    use std::sync::OnceLock;

    static RESULT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    RESULT
        .get_or_init(|| {
            // The JVM loads libcomet with RTLD_LOCAL. Its libpython dependency is
            // local too, but CPython extension modules resolve Python C API
            // symbols from the global namespace when they are imported.
            let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
            // SAFETY: Py_Initialize is a linked function address and info is
            // writable storage for dladdr's result.
            if unsafe {
                libc::dladdr(
                    pyo3::ffi::Py_Initialize as *const () as *const libc::c_void,
                    info.as_mut_ptr(),
                )
            } == 0
            {
                return Err("cannot locate the linked Python library".to_string());
            }
            // SAFETY: dladdr initialized info on success and dli_fname is a
            // null-terminated path valid for the duration of this call.
            let info = unsafe { info.assume_init() };
            if info.dli_fname.is_null() {
                return Err("linked Python library has no path".to_string());
            }
            let path = unsafe { CStr::from_ptr(info.dli_fname) };
            // RTLD_NOLOAD promotes the already-loaded libpython rather than
            // loading a second copy with separate interpreter state. Keep the
            // handle for the executor lifetime so its symbols remain global.
            // SAFETY: path points to a valid C string returned by dladdr.
            if unsafe {
                libc::dlopen(
                    path.as_ptr(),
                    libc::RTLD_NOW | libc::RTLD_GLOBAL | libc::RTLD_NOLOAD,
                )
            }
            .is_null()
            {
                // SAFETY: dlerror returns a null-terminated message, if any.
                let error = unsafe { libc::dlerror() };
                let detail = if error.is_null() {
                    "unknown dynamic loader error".to_string()
                } else {
                    unsafe { CStr::from_ptr(error) }
                        .to_string_lossy()
                        .into_owned()
                };
                return Err(format!("cannot expose Python C API symbols: {detail}"));
            }
            Ok(())
        })
        .clone()
        .map_err(ArrowError::ComputeError)
}

#[cfg(not(target_os = "linux"))]
fn make_python_symbols_global() -> Result<()> {
    Ok(())
}

/// A scalar Arrow UDF loaded from Spark's pickled `(function, returnType)` command.
/// Spark serializes the return type for its worker; Comet uses the separately
/// serialized Arrow type from the physical plan instead.
pub struct ArrowPythonUdf {
    callable: Py<PyAny>,
    return_type: DataType,
    allow_cast: bool,
    safe_cast: bool,
}

impl ArrowPythonUdf {
    pub fn from_command(
        command: &[u8],
        return_type: DataType,
        allow_cast: bool,
        safe_cast: bool,
        python_version: &str,
    ) -> Result<Self> {
        make_python_symbols_global()?;
        Python::attach(|py| {
            if !python_version.is_empty() {
                let info = py
                    .import("sys")
                    .map_err(python_error)?
                    .getattr("version_info")
                    .map_err(python_error)?;
                let major: u8 = info
                    .get_item(0)
                    .map_err(python_error)?
                    .extract()
                    .map_err(python_error)?;
                let minor: u8 = info
                    .get_item(1)
                    .map_err(python_error)?
                    .extract()
                    .map_err(python_error)?;
                let actual = format!("{major}.{minor}");
                if actual != python_version {
                    return Err(ArrowError::ComputeError(format!(
                        "Arrow UDF requires Python {python_version}, embedded interpreter is {actual}"
                    )));
                }
            }
            let pickle = py.import("pickle").map_err(python_error)?;
            let loaded = pickle
                .call_method1("loads", (PyBytes::new(py, command),))
                .map_err(python_error)?;
            let tuple = loaded.cast::<PyTuple>().map_err(python_error)?;
            if tuple.len() != 2 {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF command must contain (function, returnType), got {} items",
                    tuple.len()
                )));
            }
            let callable = tuple.get_item(0).map_err(python_error)?;
            if !callable.is_callable() {
                return Err(ArrowError::ComputeError(
                    "Arrow UDF command does not contain a callable".to_string(),
                ));
            }
            Ok(Self {
                callable: callable.unbind(),
                return_type,
                allow_cast,
                safe_cast,
            })
        })
    }

    /// Evaluate one Arrow batch, with the same row count for every argument.
    /// Python receives and returns `pyarrow.Array` objects via the Arrow C Data
    /// interface; no row conversion or Arrow IPC serialization occurs here.
    pub fn evaluate(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let names = vec![String::new(); args.len()];
        self.evaluate_named(args, &names, num_rows)
    }

    pub fn evaluate_named(
        &self,
        args: &[ArrayRef],
        names: &[String],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if args.len() != names.len() {
            return Err(ArrowError::ComputeError(
                "Arrow UDF argument names are not aligned with arguments".to_string(),
            ));
        }
        for (index, arg) in args.iter().enumerate() {
            if arg.len() != num_rows {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF argument {index} has {} rows, expected {num_rows}",
                    arg.len()
                )));
            }
        }

        Python::attach(|py| {
            let pa = py.import("pyarrow").map_err(python_error)?;
            let array_class = pa.getattr("Array").map_err(python_error)?;
            let mut py_args = Vec::with_capacity(args.len());
            for arg in args {
                let data = arg.to_data();
                let ffi_array = FFI_ArrowArray::new(&data);
                let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())?;
                let py_arg = array_class
                    .call_method1(
                        "_import_from_c",
                        (
                            &raw const ffi_array as Py_uintptr_t,
                            &raw const ffi_schema as Py_uintptr_t,
                        ),
                    )
                    .map_err(python_error)?;
                py_args.push(py_arg);
            }

            let kwargs = pyo3::types::PyDict::new(py);
            let mut positional = Vec::new();
            for (arg, name) in py_args.into_iter().zip(names) {
                if name.is_empty() {
                    positional.push(arg);
                } else {
                    kwargs.set_item(name, arg).map_err(python_error)?;
                }
            }
            let result = self
                .callable
                .bind(py)
                .call(
                    PyTuple::new(py, positional).map_err(python_error)?,
                    Some(&kwargs),
                )
                .map_err(python_error)?;
            if !result.is_instance(&array_class).map_err(python_error)? {
                return Err(ArrowError::ComputeError(
                    "Arrow UDF must return a pyarrow.Array".to_string(),
                ));
            }
            let result_len = result.len().map_err(python_error)?;
            if result_len != num_rows {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF returned {result_len} rows, expected {num_rows}"
                )));
            }

            let ffi_return_type = FFI_ArrowSchema::try_from(&self.return_type)?;
            let expected_type = pa
                .getattr("DataType")
                .map_err(python_error)?
                .call_method1(
                    "_import_from_c",
                    (&raw const ffi_return_type as Py_uintptr_t,),
                )
                .map_err(python_error)?;
            let actual_type = result.getattr("type").map_err(python_error)?;
            let typed_result = if actual_type.eq(&expected_type).map_err(python_error)? {
                result
            } else if self.allow_cast {
                let kwargs = pyo3::types::PyDict::new(py);
                kwargs
                    .set_item("safe", self.safe_cast)
                    .map_err(python_error)?;
                result
                    .call_method("cast", (expected_type,), Some(&kwargs))
                    .map_err(python_error)?
            } else {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF returned type {}, expected {}",
                    actual_type.str().map_err(python_error)?,
                    expected_type.str().map_err(python_error)?
                )));
            };

            let mut out_array = FFI_ArrowArray::empty();
            let mut out_schema = FFI_ArrowSchema::empty();
            typed_result
                .call_method1(
                    "_export_to_c",
                    (
                        &raw mut out_array as Py_uintptr_t,
                        &raw mut out_schema as Py_uintptr_t,
                    ),
                )
                .map_err(python_error)?;
            // SAFETY: PyArrow filled both C Data structs and transferred ownership
            // of the array to `out_array`; Arrow validates the schema and buffers.
            let data = unsafe { from_ffi(out_array, &out_schema) }?;
            if data.data_type() != &self.return_type {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF returned type {}, expected {}",
                    data.data_type(),
                    self.return_type
                )));
            }
            Ok(make_array(data))
        })
    }
}

fn python_error(error: impl std::fmt::Display) -> ArrowError {
    ArrowError::ComputeError(format!("Arrow UDF Python error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use std::sync::Arc;

    fn pickled_command(py: Python<'_>, module: &str, function: &str) -> Vec<u8> {
        let pickle = py.import("pickle").unwrap();
        let module = py.import(module).unwrap();
        let callable = module.getattr(function).unwrap();
        pickle
            .call_method1("dumps", ((callable.unbind(), py.None()),))
            .unwrap()
            .extract()
            .unwrap()
    }

    #[test]
    fn evaluates_arrow_arrays_and_nulls() {
        Python::attach(|py| {
            let command = pickled_command(py, "pyarrow.compute", "negate");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, false, true, "").unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
            let result = udf.evaluate(&[input], 3).unwrap();
            let expected = Int64Array::from(vec![Some(-1), None, Some(-3)]);
            assert_eq!(result.as_ref(), &expected);
        });
    }

    #[test]
    fn rejects_wrong_length_and_non_array_result() {
        Python::attach(|py| {
            let command = pickled_command(py, "builtins", "len");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, false, true, "").unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            assert!(udf.evaluate(&[Arc::clone(&input)], 1).is_err());
            assert!(udf
                .evaluate(&[input], 2)
                .unwrap_err()
                .to_string()
                .contains("pyarrow.Array"));
        });
    }

    #[test]
    fn supports_named_arguments_and_safe_cast() {
        Python::attach(|py| {
            let callable = py
                .eval(
                    c"lambda *, x, y: __import__('pyarrow.compute', fromlist=['add']).add(x, y)",
                    None,
                    None,
                )
                .unwrap();
            let command: Vec<u8> = py
                .import("cloudpickle")
                .unwrap()
                .call_method1("dumps", ((callable.unbind(), py.None()),))
                .unwrap()
                .extract()
                .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            let names = vec!["x".to_string(), "y".to_string()];
            let strict =
                ArrowPythonUdf::from_command(&command, DataType::Int32, false, true, "").unwrap();
            assert!(strict
                .evaluate_named(&[Arc::clone(&input), Arc::clone(&input)], &names, 2)
                .is_err());
            let cast =
                ArrowPythonUdf::from_command(&command, DataType::Int32, true, true, "").unwrap();
            let result = cast
                .evaluate_named(&[Arc::clone(&input), input], &names, 2)
                .unwrap();
            assert_eq!(result.as_ref(), &Int32Array::from(vec![2, 4]));
        });
    }

    #[test]
    fn rejects_python_result_with_wrong_length() {
        Python::attach(|py| {
            let command = pickled_command(py, "pyarrow.compute", "drop_null");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, "").unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
            assert!(udf
                .evaluate(&[input], 2)
                .unwrap_err()
                .to_string()
                .contains("returned 1 rows, expected 2"));
        });
    }

    #[test]
    fn rejects_mismatched_python_version() {
        let error = ArrowPythonUdf::from_command(&[], DataType::Int64, true, true, "0.0")
            .err()
            .unwrap();
        assert!(error.to_string().contains("requires Python 0.0"));
    }
}
