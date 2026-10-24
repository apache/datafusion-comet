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
use pyo3::PyTypeInfo;
use std::collections::HashMap;
use std::os::raw::{c_long, c_ulong};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

// Tasks create their callables concurrently. Concurrent first imports of a package
// can hand a thread a partially initialized module (observed with PyArrow on Python
// 3.10), so callables are unpickled one at a time. Take it with `lock_py_attached`,
// which waits for the lock without holding the GIL.
static IMPORT_LOCK: Mutex<()> = Mutex::new(());

pyo3::create_exception!(
    comet,
    TaskKilled,
    pyo3::exceptions::PyBaseException,
    "Raised in a running Arrow UDF when Spark kills its task."
);

unsafe extern "C" {
    // Part of the stable Python C API, but not declared by pyo3-ffi.
    fn PyThread_get_thread_ident() -> c_ulong;
}

/// The native plans of one Spark task attempt that hold Arrow UDFs.
#[derive(Default)]
struct TaskCalls {
    plans: usize,
    killed: bool,
    /// The calls running now.
    calls: Vec<Call>,
}

struct Call {
    /// Python thread id of the thread running the call.
    thread: c_ulong,
    /// Whether [`interrupt_task`] has raised in the call. It raises once, so that a second
    /// exception does not abort the cleanup the first one started.
    interrupted: bool,
}

// Holders of this lock never wait for the GIL, so a thread attached to Python may take it.
static TASKS: LazyLock<Mutex<HashMap<i64, TaskCalls>>> = LazyLock::new(Default::default);

fn tasks() -> MutexGuard<'static, HashMap<i64, TaskCalls>> {
    TASKS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Registers a native plan's Arrow UDFs under their Spark task attempt, so that
/// [`interrupt_task`] reaches their calls. The task's entry is removed when its last
/// registration is dropped, which happens when the plan is released.
pub struct TaskRegistration {
    task_attempt_id: i64,
}

impl TaskRegistration {
    pub fn new(task_attempt_id: i64) -> Self {
        tasks().entry(task_attempt_id).or_default().plans += 1;
        Self { task_attempt_id }
    }

    fn is_killed(&self) -> bool {
        tasks()
            .get(&self.task_attempt_id)
            .is_some_and(|task| task.killed)
    }

    fn killed_error(&self) -> ArrowError {
        ArrowError::ComputeError(format!(
            "Arrow UDF stopped because task {} was killed",
            self.task_attempt_id
        ))
    }
}

impl Drop for TaskRegistration {
    fn drop(&mut self) {
        let mut tasks = tasks();
        if let Some(task) = tasks.get_mut(&self.task_attempt_id) {
            task.plans -= 1;
            if task.plans == 0 {
                tasks.remove(&self.task_attempt_id);
            }
        }
    }
}

/// A call to the user's callable, recorded so that [`interrupt_task`] can raise in its thread.
struct RunningCall<'a> {
    task: &'a TaskRegistration,
    thread: c_ulong,
}

impl<'a> RunningCall<'a> {
    /// Fails without recording the call when the task has already been killed.
    fn enter(_py: Python<'_>, task: &'a TaskRegistration) -> Result<Self> {
        // SAFETY: the caller is attached to the interpreter.
        let thread = unsafe { PyThread_get_thread_ident() };
        let mut tasks = tasks();
        let calls = tasks.entry(task.task_attempt_id).or_default();
        if calls.killed {
            return Err(task.killed_error());
        }
        calls.calls.push(Call {
            thread,
            interrupted: false,
        });
        Ok(Self { task, thread })
    }
}

impl Drop for RunningCall<'_> {
    fn drop(&mut self) {
        let mut tasks = tasks();
        let Some(calls) = tasks.get_mut(&self.task.task_attempt_id) else {
            return;
        };
        let Some(index) = calls.calls.iter().position(|c| c.thread == self.thread) else {
            return;
        };
        if calls.calls.swap_remove(index).interrupted {
            // An interrupt that arrived as the callable returned is still pending on this
            // thread. Nothing can set another one now that the call is unrecorded, so clear
            // it rather than let it fire in later Python code on this thread.
            // SAFETY: the call is made while attached, on the thread it names.
            unsafe {
                pyo3::ffi::PyThreadState_SetAsyncExc(self.thread as c_long, std::ptr::null_mut())
            };
        }
    }
}

/// Marks a Spark task attempt as killed and raises [`TaskKilled`] in its running Arrow UDF
/// calls that it has not interrupted yet. Later calls of the task fail without entering
/// Python. Returns the number of calls interrupted. A task with no registered plan is not
/// recorded, so the caller repeats this until the task completes.
///
/// The exception takes effect when the thread next runs Python bytecode: a call blocked in
/// native code that released the GIL is interrupted only when that code returns.
pub fn interrupt_task(task_attempt_id: i64) -> usize {
    let running = match tasks().get_mut(&task_attempt_id) {
        Some(task) => {
            task.killed = true;
            task.calls.iter().any(|call| !call.interrupted)
        }
        None => false,
    };
    if !running {
        return 0;
    }
    // Calls cannot start after the flag is set, so only calls recorded before it remain.
    Python::attach(|py| {
        let exception = TaskKilled::type_object(py);
        let mut tasks = tasks();
        let Some(task) = tasks.get_mut(&task_attempt_id) else {
            return 0;
        };
        let mut interrupted = 0;
        for call in task.calls.iter_mut().filter(|call| !call.interrupted) {
            // SAFETY: attached to the interpreter, and `exception` is a live type object.
            let set = unsafe {
                pyo3::ffi::PyThreadState_SetAsyncExc(call.thread as c_long, exception.as_ptr())
            };
            if set > 0 {
                call.interrupted = true;
                interrupted += 1;
            }
        }
        interrupted
    })
}

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
                // Spark starts its Python workers with PYTHONUNBUFFERED. The embedded
                // interpreter is never finalized, so buffered print() output would be
                // lost when the executor exits. Leave the JVM's C streams unchanged.
                config.buffered_stdio = 0;
                config.configure_c_stdio = 0;
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
    task: Option<Arc<TaskRegistration>>,
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
                task: None,
            })
        })
    }

    /// Lets [`interrupt_task`] interrupt this UDF's calls for the registered task.
    pub fn with_task(mut self, task: Arc<TaskRegistration>) -> Self {
        self.task = Some(task);
        self
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
            let positional = PyTuple::new(py, positional).map_err(python_error)?;
            let call = self
                .task
                .as_deref()
                .map(|task| RunningCall::enter(py, task))
                .transpose()?;
            let result = self.callable.bind(py).call(positional, Some(&kwargs));
            drop(call);
            let result = result.map_err(|error| python_traceback_error(py, error))?;
            if let Some(task) = self.task.as_deref().filter(|task| task.is_killed()) {
                return Err(task.killed_error());
            }
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
pub(crate) mod test_util {
    use super::*;

    /// Pickles the Python expression `source` as a `(function, returnType)` command.
    pub(crate) fn cloudpickled_lambda(py: Python<'_>, source: &std::ffi::CStr) -> Vec<u8> {
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

    pub(crate) fn initialize() {
        initialize_python().unwrap();
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

    use super::test_util::cloudpickled_lambda;

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

    fn running_calls(task_attempt_id: i64) -> Option<usize> {
        tasks().get(&task_attempt_id).map(|task| task.calls.len())
    }

    #[test]
    fn interrupts_running_call_of_killed_task() {
        const TASK: i64 = -6811;
        initialize_python().unwrap();
        // `except Exception` must not swallow the interrupt.
        let command = Python::attach(|py| {
            cloudpickled_lambda(
                py,
                c"(lambda ns: (exec('def f(a):\\n    n = 0\\n    while True:\\n        n += 1\\n        try:\\n            n %= 7\\n        except Exception:\\n            pass\\n', ns), ns['f'])[1])({})",
            )
        });
        let task = Arc::new(TaskRegistration::new(TASK));
        let udf = Arc::new(
            ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, false, "")
                .unwrap()
                .with_task(Arc::clone(&task)),
        );
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let (sender, receiver) = std::sync::mpsc::channel();
        {
            let (udf, input) = (Arc::clone(&udf), Arc::clone(&input));
            // Not joined: a call that ignores the interrupt would never return.
            std::thread::spawn(move || sender.send(udf.evaluate(&[input], 1)));
        }
        let start = std::time::Instant::now();
        while running_calls(TASK) != Some(1) {
            assert!(start.elapsed().as_secs() < 30, "the UDF call did not start");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(interrupt_task(TASK), 1);
        let error = receiver
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the interrupted call did not return")
            .unwrap_err()
            .to_string();
        assert!(error.contains("TaskKilled"), "{error}");
        assert_eq!(running_calls(TASK), Some(0));
        // A killed task does not enter Python again.
        let error = udf.evaluate(&[input], 1).unwrap_err().to_string();
        assert!(error.contains("was killed"), "{error}");
        assert_eq!(interrupt_task(TASK), 0);
        drop(udf);
        drop(task);
        assert_eq!(running_calls(TASK), None);
    }

    #[test]
    fn interrupts_a_call_once_so_its_cleanup_runs() {
        const TASK: i64 = -6812;
        initialize_python().unwrap();
        let command = Python::attach(|py| {
            let sys = py.import("sys").unwrap();
            let lock = py
                .import("threading")
                .unwrap()
                .call_method0("Lock")
                .unwrap();
            sys.setattr("comet_cleanup_lock", lock).unwrap();
            sys.setattr("comet_cleaning", false).unwrap();
            // The cleanup outlasts the monitor's 100 ms polling interval.
            cloudpickled_lambda(
                py,
                c"(lambda ns: (exec('def f(a):\\n    import sys, time\\n    sys.comet_cleanup_lock.acquire()\\n    try:\\n        n = 0\\n        while True:\\n            n += 1\\n    finally:\\n        sys.comet_cleaning = True\\n        time.sleep(0.4)\\n        sys.comet_cleanup_lock.release()\\n', ns), ns['f'])[1])({})",
            )
        });
        let sys_attr = |expression: &std::ffi::CStr| -> bool {
            Python::attach(|py| {
                let sys = py.import("sys").unwrap();
                let locals = pyo3::types::PyDict::new(py);
                locals.set_item("sys", sys).unwrap();
                py.eval(expression, None, Some(&locals))
                    .unwrap()
                    .extract()
                    .unwrap()
            })
        };
        let task = Arc::new(TaskRegistration::new(TASK));
        let udf = Arc::new(
            ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, false, "")
                .unwrap()
                .with_task(Arc::clone(&task)),
        );
        let (sender, receiver) = std::sync::mpsc::channel();
        {
            let udf = Arc::clone(&udf);
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
            std::thread::spawn(move || sender.send(udf.evaluate(&[input], 1)));
        }
        let start = std::time::Instant::now();
        while running_calls(TASK) != Some(1) {
            assert!(start.elapsed().as_secs() < 30, "the UDF call did not start");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(interrupt_task(TASK), 1);
        while !sys_attr(c"sys.comet_cleaning") {
            assert!(start.elapsed().as_secs() < 30, "the cleanup did not start");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // A later poll while the call cleans up must not raise in it again.
        assert_eq!(interrupt_task(TASK), 0);
        let error = receiver
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the interrupted call did not return")
            .unwrap_err()
            .to_string();
        assert!(error.contains("TaskKilled"), "{error}");
        assert!(
            !sys_attr(c"sys.comet_cleanup_lock.locked()"),
            "the cleanup did not release its lock"
        );
    }

    const UNBUFFERED_CHILD: &str = "COMET_ARROW_UDF_UNBUFFERED_CHILD";
    const UNBUFFERED_MARKER: &str = "comet-arrow-udf-unbuffered-marker";

    /// Runs in a child process: prints from a UDF, then exits without finalizing
    /// Python, as an executor JVM does.
    #[test]
    fn print_from_udf_then_exit() {
        if std::env::var_os(UNBUFFERED_CHILD).is_none() {
            return;
        }
        initialize_python().unwrap();
        Python::attach(|py| {
            let command = cloudpickled_lambda(
                py,
                c"lambda a: (print('comet-arrow-udf-unbuffered-marker'), a)[1]",
            );
            let udf =
                ArrowPythonUdf::from_command(&command, DataType::Int64, true, true, false, "")
                    .unwrap();
            let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
            udf.evaluate(&[input], 1).unwrap();
        });
        std::process::exit(0);
    }

    #[test]
    fn udf_stdout_survives_exit_without_flush() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "execution::python_udf::tests::print_from_udf_then_exit",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(UNBUFFERED_CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(UNBUFFERED_MARKER),
            "status: {}, stdout: {stdout}, stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn rejects_mismatched_python_version() {
        let error = ArrowPythonUdf::from_command(&[], DataType::Int64, true, true, false, "0.0")
            .err()
            .unwrap();
        assert!(error.to_string().contains("requires Python 0.0"));
    }
}
