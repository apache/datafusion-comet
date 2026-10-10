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
use datafusion_comet_common::decode_string_arrays;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::sync::{Mutex, PoisonError};

// Tasks create their callables concurrently. Concurrent first imports of a package
// can hand a thread a partially initialized module (observed with PyArrow on Python
// 3.10), so callables are unpickled one at a time. Take it with `lock_py_attached`,
// which waits for the lock without holding the GIL.
static IMPORT_LOCK: Mutex<()> = Mutex::new(());

fn initialize_python() -> Result<()> {
    use std::ffi::CStr;
    use std::sync::OnceLock;

    static RESULT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    RESULT
        .get_or_init(|| {
            // Spark sets PYTHONHASHSEED=0 on its Python workers by default.
            // Match that seed before any Python object is created in the embedded
            // interpreter, without changing the JVM process environment.
            // SAFETY: OnceLock serializes initialization by Comet. No other Comet
            // code accesses the Python C API before this function returns.
            unsafe {
                if pyo3::ffi::Py_IsInitialized() != 0 {
                    return Ok(());
                }
                let mut config = std::mem::MaybeUninit::<pyo3::ffi::PyConfig>::uninit();
                pyo3::ffi::PyConfig_InitPythonConfig(config.as_mut_ptr());
                let mut config = config.assume_init();
                config.install_signal_handlers = 0;
                config.use_hash_seed = 1;
                config.hash_seed = 0;
                let status = pyo3::ffi::Py_InitializeFromConfig(&config);
                let error = if pyo3::ffi::PyStatus_Exception(status) != 0 {
                    if status.err_msg.is_null() {
                        "Python interpreter initialization failed".to_string()
                    } else {
                        CStr::from_ptr(status.err_msg)
                            .to_string_lossy()
                            .into_owned()
                    }
                } else {
                    String::new()
                };
                pyo3::ffi::PyConfig_Clear(&mut config);
                if !error.is_empty() {
                    return Err(error);
                }
                // No other thread can run Python before PyEval_SaveThread releases
                // the GIL, so importing PyArrow here cannot race with another import.
                let imported = Python::attach(|py| {
                    py.import("pyarrow")
                        .map(drop)
                        .map_err(|error| format!("cannot import pyarrow: {error}"))
                });
                pyo3::ffi::PyEval_SaveThread();
                imported
            }
        })
        .clone()
        .map_err(ArrowError::ComputeError)
}

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
    accept_array_like: bool,
}

impl ArrowPythonUdf {
    /// `accept_array_like` follows Spark 4.2, whose worker passes the result through
    /// `pyarrow.RecordBatch.from_arrays` and therefore accepts lists and NumPy arrays.
    /// Spark 4.1 requires the UDF to return a `pyarrow.Array`.
    pub fn from_command(
        command: &[u8],
        return_type: DataType,
        allow_cast: bool,
        safe_cast: bool,
        accept_array_like: bool,
        python_version: &str,
    ) -> Result<Self> {
        make_python_symbols_global()?;
        initialize_python()?;
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
            let _imports = IMPORT_LOCK
                .lock_py_attached(py)
                .unwrap_or_else(PoisonError::into_inner);
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
                accept_array_like,
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
                // PyArrow takes ownership of these C Data structs and clears their
                // release callbacks, so the pointed-to storage must be writable.
                let mut ffi_array = FFI_ArrowArray::new(&data);
                let mut ffi_schema = FFI_ArrowSchema::try_from(data.data_type())?;
                let py_arg = array_class
                    .call_method1(
                        "_import_from_c",
                        (
                            &raw mut ffi_array as Py_uintptr_t,
                            &raw mut ffi_schema as Py_uintptr_t,
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
                .map_err(|error| python_traceback_error(py, error))?;
            let result = if self.accept_array_like {
                // Spark 4.2 builds its output batch with `RecordBatch.from_arrays`,
                // which converts lists and NumPy arrays and rejects chunked arrays.
                let arrays = PyList::new(py, [result]).map_err(python_error)?;
                let names = PyList::new(py, ["_0"]).map_err(python_error)?;
                pa.getattr("RecordBatch")
                    .map_err(python_error)?
                    .call_method1("from_arrays", (arrays, names))
                    .map_err(|error| python_traceback_error(py, error))?
                    .call_method1("column", (0,))
                    .map_err(python_error)?
            } else if result.is_instance(&array_class).map_err(python_error)? {
                result
            } else {
                return Err(ArrowError::ComputeError(
                    "Arrow UDF must return a pyarrow.Array".to_string(),
                ));
            };
            let result_len = result.len().map_err(python_error)?;
            if result_len != num_rows {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF returned {result_len} rows, expected {num_rows}"
                )));
            }

            let mut ffi_return_type = FFI_ArrowSchema::try_from(&self.return_type)?;
            let expected_type = pa
                .getattr("DataType")
                .map_err(python_error)?
                .call_method1(
                    "_import_from_c",
                    (&raw mut ffi_return_type as Py_uintptr_t,),
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
            // of the array to `out_array`. `from_ffi` checks the schema but builds the
            // array without validating its buffers, so string data is checked below.
            let data = unsafe { from_ffi(out_array, &out_schema) }?;
            if data.data_type() != &self.return_type {
                return Err(ArrowError::ComputeError(format!(
                    "Arrow UDF returned type {}, expected {}",
                    data.data_type(),
                    self.return_type
                )));
            }
            // An unsafe PyArrow cast can return a string array with invalid UTF-8.
            // Decode it the same way as arrays imported from the JVM.
            decode_string_arrays(&make_array(data))
        })
    }
}

fn python_error(error: impl std::fmt::Display) -> ArrowError {
    ArrowError::ComputeError(format!("Arrow UDF Python error: {error}"))
}

/// Formats an exception raised by user code with its Python traceback, as Spark's
/// `PythonException` message does. Before Python 3.12, PyO3 keeps the traceback
/// apart from the exception value, so pass the type, value and traceback explicitly.
fn python_traceback_error(py: Python<'_>, error: PyErr) -> ArrowError {
    let formatted = py.import("traceback").and_then(|traceback| {
        traceback
            .call_method1(
                "format_exception",
                (error.get_type(py), error.value(py), error.traceback(py)),
            )?
            .extract::<Vec<String>>()
    });
    match formatted {
        Ok(lines) => ArrowError::ComputeError(format!(
            "Arrow UDF Python error: {}",
            lines.concat().trim_end()
        )),
        Err(_) => python_error(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use std::sync::Arc;

    fn pickled_command(py: Python<'_>, module: &str, function: &str) -> Vec<u8> {
        let _imports = IMPORT_LOCK
            .lock_py_attached(py)
            .unwrap_or_else(PoisonError::into_inner);
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
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = pickled_command(py, "pyarrow.compute", "negate");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, false, true, false, "")
                    .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
            let result = udf.evaluate(&[input], 3).unwrap();
            let expected = Int64Array::from(vec![Some(-1), None, Some(-3)]);
            assert_eq!(result.as_ref(), &expected);
        });
    }

    #[test]
    fn rejects_wrong_length_and_non_array_result() {
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = pickled_command(py, "builtins", "len");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, false, true, false, "")
                    .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            assert!(udf.evaluate(&[Arc::clone(&input)], 1).is_err());
            let error = udf.evaluate(&[input], 2).unwrap_err().to_string();
            assert!(error.contains("pyarrow.Array"), "{error}");
        });
    }

    #[test]
    fn supports_named_arguments_and_safe_cast() {
        initialize_python().unwrap();
        Python::attach(|py| {
            // Capture the module so that unpickling, not the call, imports it.
            let command = cloudpickled_lambda(
                py,
                c"(lambda pc: lambda *, x, y: pc.add(x, y))(__import__('pyarrow.compute', fromlist=['add']))",
            );
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            let names = vec!["x".to_string(), "y".to_string()];
            let strict =
                ArrowPythonUdf::from_command(&command, DataType::Int32, false, true, false, "")
                    .unwrap();
            assert!(strict
                .evaluate_named(&[Arc::clone(&input), Arc::clone(&input)], &names, 2)
                .is_err());
            let cast =
                ArrowPythonUdf::from_command(&command, DataType::Int32, true, true, false, "")
                    .unwrap();
            let result = cast
                .evaluate_named(&[Arc::clone(&input), input], &names, 2)
                .unwrap();
            assert_eq!(result.as_ref(), &Int32Array::from(vec![2, 4]));
        });
    }

    #[test]
    fn rejects_python_result_with_wrong_length() {
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = pickled_command(py, "pyarrow.compute", "drop_null");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, false, "")
                    .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
            assert!(udf
                .evaluate(&[input], 2)
                .unwrap_err()
                .to_string()
                .contains("returned 1 rows, expected 2"));
        });
    }

    fn cloudpickled_lambda(py: Python<'_>, source: &std::ffi::CStr) -> Vec<u8> {
        let _imports = IMPORT_LOCK
            .lock_py_attached(py)
            .unwrap_or_else(PoisonError::into_inner);
        let callable = py.eval(source, None, None).unwrap();
        py.import("cloudpickle")
            .unwrap()
            .call_method1("dumps", ((callable.unbind(), py.None()),))
            .unwrap()
            .extract()
            .unwrap()
    }

    #[test]
    fn accepts_array_like_results_only_when_requested() {
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = cloudpickled_lambda(py, c"lambda a: a.to_pylist()");
            let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
            let spark_41 =
                ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, false, "")
                    .unwrap();
            assert!(spark_41
                .evaluate(&[Arc::clone(&input)], 3)
                .unwrap_err()
                .to_string()
                .contains("pyarrow.Array"));
            let spark_42 =
                ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, true, "")
                    .unwrap();
            let result = spark_42.evaluate(&[Arc::clone(&input)], 3).unwrap();
            assert_eq!(
                result.as_ref(),
                &Int64Array::from(vec![Some(1), None, Some(3)])
            );

            let chunked =
                cloudpickled_lambda(py, c"lambda a: __import__('pyarrow').chunked_array([a])");
            let spark_42 =
                ArrowPythonUdf::from_command(&chunked, DataType::Int64, true, true, true, "")
                    .unwrap();
            assert!(spark_42.evaluate(&[input], 3).is_err());
        });
    }

    #[test]
    fn decodes_invalid_utf8_in_string_results() {
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = cloudpickled_lambda(
                py,
                c"lambda a: __import__('pyarrow').array([b'ok', b'\\xff'], __import__('pyarrow').binary()).cast(__import__('pyarrow').string(), safe=False)",
            );
            let udf = ArrowPythonUdf::from_command(&command, DataType::Utf8, true, true, false, "")
                .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            let result = udf.evaluate(&[input], 2).unwrap();
            let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(strings.value(0), "ok");
            assert!(std::str::from_utf8(strings.value(1).as_bytes()).is_ok());
        });
    }

    #[test]
    fn reports_python_traceback_for_udf_errors() {
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = cloudpickled_lambda(py, c"lambda a: 1 // 0");
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, false, "")
                    .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
            let error = udf.evaluate(&[input], 1).unwrap_err().to_string();
            assert!(
                error.contains("Traceback (most recent call last)"),
                "{error}"
            );
            assert!(error.contains("line 1"), "{error}");
            assert!(error.contains("ZeroDivisionError"), "{error}");
        });
    }

    #[test]
    fn rejects_mismatched_python_version() {
        let error = ArrowPythonUdf::from_command(&[], DataType::Int64, true, true, false, "0.0")
            .err()
            .unwrap();
        assert!(error.to_string().contains("requires Python 0.0"));
    }
}
