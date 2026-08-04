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

//! The Comet UDF C ABI — sedona-style.
//!
//! The wire format is two `#[repr(C)]` structs of function pointers,
//! parameterized only by Arrow's C Data Interface
//! (`FFI_ArrowSchema` / `FFI_ArrowArray`). No DataFusion types appear in
//! the FFI surface, so the user's cdylib only needs a matching `arrow`
//! crate, not a matching `datafusion` version.
//!
//! # Authoring a UDF
//!
//! Implement [`CometCScalarUdf`] for a type that also implements `Default`,
//! then use the [`comet_c_udf_export!`] macro to emit the discovery entry
//! point:
//!
//! ```ignore
//! use comet_udf_sdk::c_abi::*;
//! use arrow::array::{ArrayRef, Int64Array};
//! use arrow::datatypes::{DataType, Field};
//! use std::sync::Arc;
//!
//! #[derive(Default)]
//! pub struct AddOne;
//! impl CometCScalarUdf for AddOne {
//!     fn name(&self) -> &str { "add_one_c" }
//!     fn return_field(&self, args: &[Field]) -> Result<Field, String> {
//!         if args.len() != 1 || args[0].data_type() != &DataType::Int64 {
//!             return Err("expected (Int64) -> Int64".into());
//!         }
//!         Ok(Field::new("add_one_c", DataType::Int64, true))
//!     }
//!     fn invoke(&self, args: &[ArrayRef], _n: usize) -> Result<ArrayRef, String> {
//!         let a = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
//!         Ok(Arc::new(a.iter().map(|v| v.map(|x| x + 1)).collect::<Int64Array>()))
//!     }
//! }
//!
//! comet_udf_sdk::comet_c_udf_export!(AddOne);
//! ```

use std::ffi::{c_char, c_int, c_void};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

/// Generic non-zero error code returned by `init` / `execute` to signal
/// failure. The host treats any non-zero return as an error and calls
/// `get_last_error` for the message; the specific code is informational.
const C_ABI_ERR: c_int = 1;

// -- panic containment -----------------------------------------------------
//
// Every `extern "C"` function in this module is an unwind boundary. A panic
// that escapes one aborts the whole process (Rust's default `extern "C"`
// unwind behavior since 1.81), which for Comet means killing the executor
// JVM and losing every task on it -- not just the query that used the UDF.
//
// User UDF code is arbitrary and panicking is idiomatic Rust (`unwrap`,
// slice indexing, integer overflow in debug), so the SDK treats a panic in
// user code as an ordinary error: catch it at the boundary, convert it to a
// message, and report it through the same `get_last_error` channel as a
// returned `Err`. The query fails; the executor survives.

/// Render a caught panic payload as an error message.
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    let detail = panic
        .downcast_ref::<&'static str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    format!("panic in UDF code: {detail}")
}

/// Run `f`, converting a panic into `Err(message)`.
fn catch_panic<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(panic) => Err(panic_message(panic)),
    }
}

/// Run an infallible `f` (typically a release/cleanup callback), swallowing
/// any panic. Used where the ABI gives us no way to report an error and
/// aborting would be a worse outcome than leaking.
fn catch_panic_infallible(f: impl FnOnce()) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
}

// -- factory struct --------------------------------------------------------

/// Factory for [`CometCScalarKernelImpl`] instances.
///
/// Lives in a registry, may be cloned across an FFI boundary. Calls to
/// `function_name` and `new_impl` must be thread-safe (the implementation
/// is responsible for any internal synchronization).
///
/// `#[repr(C)]` layout, matched by the host loader. Adding new fields
/// requires bumping `COMET_UDF_ABI_VERSION`.
#[repr(C)]
pub struct CometCScalarKernel {
    /// Return the function name this kernel implements as a NUL-terminated
    /// UTF-8 C string. The pointer must remain valid for the lifetime of
    /// the [`CometCScalarKernel`].
    ///
    /// May be `None`, in which case the kernel is treated as anonymous and
    /// won't be discoverable by name. (Comet always sets this; field is
    /// optional for parity with sedona's design.)
    pub function_name: Option<unsafe extern "C" fn(*const CometCScalarKernel) -> *const c_char>,

    /// Initialize a new [`CometCScalarKernelImpl`] into `out`. Called once
    /// per execution, on the thread that will then drive `init`/`execute`.
    pub new_impl:
        Option<unsafe extern "C" fn(*const CometCScalarKernel, out: *mut CometCScalarKernelImpl)>,

    /// Release this kernel. After release, all callbacks must be set to
    /// `None`. Called when the host's `LoadedLibrary` is dropped.
    pub release: Option<unsafe extern "C" fn(*mut CometCScalarKernel)>,

    /// Implementation-private data, opaque to the host.
    pub private_data: *mut c_void,
}

// SAFETY: `CometCScalarKernel` is a thin wrapper around C function
// pointers with caller-defined synchronization semantics; the trait impls
// are required so loaded kernels can be referenced from multi-threaded
// host code. Implementations of the FFI must respect thread safety as
// described in the doc comments.
unsafe impl Send for CometCScalarKernel {}
unsafe impl Sync for CometCScalarKernel {}

impl Default for CometCScalarKernel {
    fn default() -> Self {
        Self {
            function_name: None,
            new_impl: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

impl Drop for CometCScalarKernel {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // SAFETY: release is the FFI-defined cleanup callback;
            // implementations must reset `release` to None per the contract.
            unsafe { release(self) };
        }
    }
}

// -- per-execution instance struct ----------------------------------------

/// Per-execution instance produced by [`CometCScalarKernel::new_impl`].
///
/// Not thread-safe; the caller must serialize access. Typically used on
/// one thread for one batch then dropped.
#[repr(C)]
pub struct CometCScalarKernelImpl {
    /// Compute the return type from arg types and (optionally) bound
    /// scalar arguments.
    ///
    /// On success, `out` is populated with the return type as an
    /// `FFI_ArrowSchema` and the function returns 0. On failure, returns
    /// a non-zero errno and the host calls `get_last_error` to retrieve
    /// the message.
    ///
    /// `arg_types` points to an array of `n_args` `*const FFI_ArrowSchema`.
    /// `scalar_args` may be NULL (no scalars) or point to an array of
    /// `n_args` `*mut FFI_ArrowArray`, each of length 1 (or NULL when
    /// the corresponding argument is not a scalar). Implementations may
    /// take ownership of scalar entries by replacing them with released
    /// arrays.
    pub init: Option<
        unsafe extern "C" fn(
            *mut CometCScalarKernelImpl,
            arg_types: *const *const FFI_ArrowSchema,
            scalar_args: *const *mut FFI_ArrowArray,
            n_args: i64,
            out: *mut FFI_ArrowSchema,
        ) -> c_int,
    >,

    /// Execute one batch.
    ///
    /// `args` points to an array of `n_args` `*mut FFI_ArrowArray`.
    /// Each input must have length `n_rows` or length 1 (scalar broadcast).
    /// On success writes the result into `out` and returns 0.
    pub execute: Option<
        unsafe extern "C" fn(
            *mut CometCScalarKernelImpl,
            args: *const *mut FFI_ArrowArray,
            n_args: i64,
            n_rows: i64,
            out: *mut FFI_ArrowArray,
        ) -> c_int,
    >,

    /// Return the last error message produced by `init` or `execute`.
    ///
    /// Returns NULL if there is no error. The pointer is valid until the
    /// next call to any method on this instance (or `release`).
    pub get_last_error: Option<unsafe extern "C" fn(*mut CometCScalarKernelImpl) -> *const c_char>,

    /// Release this instance. After release `release` must be `None`.
    pub release: Option<unsafe extern "C" fn(*mut CometCScalarKernelImpl)>,

    /// Implementation-private data, opaque to the host.
    pub private_data: *mut c_void,
}

impl Default for CometCScalarKernelImpl {
    fn default() -> Self {
        Self {
            init: None,
            execute: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

impl Drop for CometCScalarKernelImpl {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // SAFETY: per the FFI contract `release` cleans up
            // private_data and resets `release` to None.
            unsafe { release(self) };
        }
    }
}

// -- discovery list --------------------------------------------------------

/// List of kernels exposed by a cdylib via `comet_c_udf_list_v1`.
///
/// Ownership of the underlying `CometCScalarKernel` array is transferred
/// to the host: the host invokes each kernel's `release` and then frees
/// the list via `release_list`.
#[repr(C)]
pub struct CometCScalarKernelList {
    /// Pointer to the kernel array, or null if `len == 0`.
    pub kernels: *mut CometCScalarKernel,
    /// Number of kernels in `kernels`.
    pub len: i64,
    /// Free the array of kernels. Implementations must invoke each
    /// kernel's `release` first, then release the array storage.
    pub release: Option<unsafe extern "C" fn(*mut CometCScalarKernelList)>,
}

impl Default for CometCScalarKernelList {
    fn default() -> Self {
        Self {
            kernels: std::ptr::null_mut(),
            len: 0,
            release: None,
        }
    }
}

impl Drop for CometCScalarKernelList {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // SAFETY: `release` is responsible for freeing each kernel and
            // the array storage that backs `kernels`.
            unsafe { release(self) };
        }
    }
}

// -- high-level Rust trait + adapter --------------------------------------

use arrow::array::ArrayRef;
use arrow::datatypes::Field;

/// High-level Rust trait the user implements to author a UDF.
///
/// Adapted to the C ABI by [`ExportedScalarKernel`].
pub trait CometCScalarUdf: Send + Sync {
    /// Stable function name. Returned via `function_name` over the FFI.
    fn name(&self) -> &str;

    /// Compute the output `Field` from the input `Field`s.
    ///
    /// Called once per execution, before `invoke`. May reject input
    /// arities or types by returning an error; the host then surfaces
    /// the message to the planner.
    fn return_field(&self, args: &[Field]) -> Result<Field, String>;

    /// Evaluate one batch of `n_rows` rows.
    fn invoke(&self, args: &[ArrayRef], n_rows: usize) -> Result<ArrayRef, String>;
}

/// Wraps a user `CometCScalarUdf` impl as a [`CometCScalarKernel`]
/// suitable for emission via the C ABI discovery list.
pub struct ExportedScalarKernel {
    inner: std::sync::Arc<dyn CometCScalarUdf>,
    /// NUL-terminated C string holding the function name. Lifetime is
    /// tied to `self` so the pointer returned to the host stays valid.
    name_c: std::ffi::CString,
}

impl ExportedScalarKernel {
    /// Wrap `udf` for export.
    pub fn new<U: CometCScalarUdf + 'static>(udf: U) -> Self {
        let name_c = std::ffi::CString::new(udf.name().to_string())
            .expect("UDF name must not contain interior NUL bytes");
        Self {
            inner: std::sync::Arc::new(udf),
            name_c,
        }
    }
}

impl From<ExportedScalarKernel> for CometCScalarKernel {
    fn from(value: ExportedScalarKernel) -> Self {
        let boxed: Box<ExportedScalarKernel> = Box::new(value);
        let private = Box::into_raw(boxed) as *mut c_void;
        CometCScalarKernel {
            function_name: Some(c_factory_function_name),
            new_impl: Some(c_factory_new_impl),
            release: Some(c_factory_release),
            private_data: private,
        }
    }
}

unsafe extern "C" fn c_factory_function_name(this: *const CometCScalarKernel) -> *const c_char {
    debug_assert!(!this.is_null());
    let this = unsafe { &*this };
    debug_assert!(!this.private_data.is_null());
    let exp = unsafe { &*(this.private_data as *const ExportedScalarKernel) };
    exp.name_c.as_ptr()
}

unsafe extern "C" fn c_factory_new_impl(
    this: *const CometCScalarKernel,
    out: *mut CometCScalarKernelImpl,
) {
    debug_assert!(!this.is_null());
    debug_assert!(!out.is_null());
    let this = unsafe { &*this };
    let exp = unsafe { &*(this.private_data as *const ExportedScalarKernel) };
    // On panic, leave `out` as the default (all callbacks None). The host
    // detects the missing `init` and reports it as a load error.
    catch_panic_infallible(|| {
        let impl_state = ExportedScalarKernelImpl {
            inner: std::sync::Arc::clone(&exp.inner),
            last_arg_fields: None,
            last_return_field: None,
            last_error: std::ffi::CString::default(),
        };
        unsafe {
            std::ptr::write(out, CometCScalarKernelImpl::from(impl_state));
        }
    });
}

unsafe extern "C" fn c_factory_release(this: *mut CometCScalarKernel) {
    debug_assert!(!this.is_null());
    let this_ref = unsafe { &mut *this };
    if !this_ref.private_data.is_null() {
        // SAFETY: private_data was set via Box::into_raw in
        // From<ExportedScalarKernel>; reclaim and drop. Dropping runs the
        // user type's Drop, which may panic; contain it rather than abort.
        let raw = this_ref.private_data as *mut ExportedScalarKernel;
        this_ref.private_data = std::ptr::null_mut();
        catch_panic_infallible(|| drop(unsafe { Box::from_raw(raw) }));
    }
    this_ref.function_name = None;
    this_ref.new_impl = None;
    this_ref.release = None;
}

struct ExportedScalarKernelImpl {
    inner: std::sync::Arc<dyn CometCScalarUdf>,
    last_arg_fields: Option<Vec<Field>>,
    last_return_field: Option<Field>,
    last_error: std::ffi::CString,
}

impl From<ExportedScalarKernelImpl> for CometCScalarKernelImpl {
    fn from(value: ExportedScalarKernelImpl) -> Self {
        let boxed = Box::new(value);
        let private = Box::into_raw(boxed) as *mut c_void;
        CometCScalarKernelImpl {
            init: Some(c_kernel_init),
            execute: Some(c_kernel_execute),
            get_last_error: Some(c_kernel_get_last_error),
            release: Some(c_kernel_release),
            private_data: private,
        }
    }
}

unsafe extern "C" fn c_kernel_init(
    this: *mut CometCScalarKernelImpl,
    arg_types: *const *const FFI_ArrowSchema,
    _scalar_args: *const *mut FFI_ArrowArray,
    n_args: i64,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    debug_assert!(!this.is_null());
    let this_ref = unsafe { &mut *this };
    let priv_ptr = this_ref.private_data as *mut ExportedScalarKernelImpl;
    debug_assert!(!priv_ptr.is_null());
    let priv_ref = unsafe { &mut *priv_ptr };

    let n = n_args as usize;
    let mut fields = Vec::with_capacity(n);
    for i in 0..n {
        let schema_ptr = unsafe { *arg_types.add(i) };
        if schema_ptr.is_null() {
            priv_ref.last_error =
                std::ffi::CString::new(format!("arg #{i} has null FFI_ArrowSchema"))
                    .unwrap_or_default();
            return C_ABI_ERR;
        }
        let schema = unsafe { &*schema_ptr };
        match Field::try_from(schema) {
            Ok(f) => fields.push(f),
            Err(e) => {
                priv_ref.last_error =
                    std::ffi::CString::new(format!("arg #{i}: {e}")).unwrap_or_default();
                return C_ABI_ERR;
            }
        }
    }

    // `return_field` is user code: contain any panic (see "panic containment").
    match catch_panic(|| priv_ref.inner.return_field(&fields)) {
        Ok(ret_field) => match FFI_ArrowSchema::try_from(&ret_field) {
            Ok(ffi_schema) => {
                unsafe { std::ptr::write(out, ffi_schema) };
                priv_ref.last_arg_fields = Some(fields);
                priv_ref.last_return_field = Some(ret_field);
                0
            }
            Err(e) => {
                priv_ref.last_error = std::ffi::CString::new(format!("encoding return type: {e}"))
                    .unwrap_or_default();
                C_ABI_ERR
            }
        },
        Err(msg) => {
            priv_ref.last_error = std::ffi::CString::new(msg).unwrap_or_default();
            C_ABI_ERR
        }
    }
}

unsafe extern "C" fn c_kernel_execute(
    this: *mut CometCScalarKernelImpl,
    args: *const *mut FFI_ArrowArray,
    n_args: i64,
    n_rows: i64,
    out: *mut FFI_ArrowArray,
) -> c_int {
    debug_assert!(!this.is_null());
    let this_ref = unsafe { &mut *this };
    let priv_ptr = this_ref.private_data as *mut ExportedScalarKernelImpl;
    debug_assert!(!priv_ptr.is_null());
    let priv_ref = unsafe { &mut *priv_ptr };

    let arg_fields = match priv_ref.last_arg_fields.as_ref() {
        Some(f) => f,
        None => {
            priv_ref.last_error =
                std::ffi::CString::new("execute called before init").unwrap_or_default();
            return C_ABI_ERR;
        }
    };
    if arg_fields.len() != n_args as usize {
        priv_ref.last_error = std::ffi::CString::new(format!(
            "execute n_args={} disagrees with init n_args={}",
            n_args,
            arg_fields.len()
        ))
        .unwrap_or_default();
        return C_ABI_ERR;
    }

    // Take ownership of each input FFI_ArrowArray.
    let n = n_args as usize;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n);
    for (i, arg_field) in arg_fields.iter().enumerate().take(n) {
        let raw = unsafe { *args.add(i) };
        if raw.is_null() {
            priv_ref.last_error =
                std::ffi::CString::new(format!("arg #{i} FFI_ArrowArray is null"))
                    .unwrap_or_default();
            return C_ABI_ERR;
        }
        // SAFETY: raw points at an FFI_ArrowArray owned by the caller; we
        // take ownership by reading and zeroing it.
        let owned = unsafe { std::ptr::read(raw) };
        unsafe { std::ptr::write(raw, FFI_ArrowArray::empty()) };
        let dt = arg_field.data_type().clone();
        let data = match unsafe { arrow::ffi::from_ffi_and_data_type(owned, dt) } {
            Ok(d) => d,
            Err(e) => {
                priv_ref.last_error =
                    std::ffi::CString::new(format!("arg #{i} from_ffi: {e}")).unwrap_or_default();
                return C_ABI_ERR;
            }
        };
        arrays.push(arrow::array::make_array(data));
    }

    // `invoke` is user code: contain any panic (see "panic containment").
    let result = match catch_panic(|| priv_ref.inner.invoke(&arrays, n_rows as usize)) {
        Ok(arr) => arr,
        Err(msg) => {
            priv_ref.last_error = std::ffi::CString::new(msg).unwrap_or_default();
            return C_ABI_ERR;
        }
    };

    let ffi_out = FFI_ArrowArray::new(&result.to_data());
    unsafe { std::ptr::write(out, ffi_out) };
    0
}

unsafe extern "C" fn c_kernel_get_last_error(this: *mut CometCScalarKernelImpl) -> *const c_char {
    debug_assert!(!this.is_null());
    let this_ref = unsafe { &*this };
    let priv_ptr = this_ref.private_data as *mut ExportedScalarKernelImpl;
    if priv_ptr.is_null() {
        return std::ptr::null();
    }
    let priv_ref = unsafe { &*priv_ptr };
    priv_ref.last_error.as_ptr()
}

unsafe extern "C" fn c_kernel_release(this: *mut CometCScalarKernelImpl) {
    debug_assert!(!this.is_null());
    let this_ref = unsafe { &mut *this };
    if !this_ref.private_data.is_null() {
        // Dropping may run user Drop code, which may panic; contain it.
        let raw = this_ref.private_data as *mut ExportedScalarKernelImpl;
        this_ref.private_data = std::ptr::null_mut();
        catch_panic_infallible(|| drop(unsafe { Box::from_raw(raw) }));
    }
    this_ref.init = None;
    this_ref.execute = None;
    this_ref.get_last_error = None;
    this_ref.release = None;
}

// -- discovery list construction ------------------------------------------

/// Build a heap-allocated [`CometCScalarKernelList`] from a vector of
/// [`CometCScalarKernel`]s. Ownership transfers to the caller, who must
/// free via the list's `release` callback.
pub fn build_kernel_list(kernels: Vec<CometCScalarKernel>) -> CometCScalarKernelList {
    if kernels.is_empty() {
        return CometCScalarKernelList::default();
    }
    let mut boxed = kernels.into_boxed_slice();
    let len = boxed.len() as i64;
    let kernels_ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    CometCScalarKernelList {
        kernels: kernels_ptr,
        len,
        release: Some(c_list_release),
    }
}

unsafe extern "C" fn c_list_release(list: *mut CometCScalarKernelList) {
    debug_assert!(!list.is_null());
    let list_ref = unsafe { &mut *list };
    if list_ref.kernels.is_null() || list_ref.len == 0 {
        list_ref.release = None;
        return;
    }
    let len = list_ref.len as usize;
    let kernels = list_ref.kernels;
    list_ref.kernels = std::ptr::null_mut();
    list_ref.len = 0;
    list_ref.release = None;
    // SAFETY: kernels was a Box<[CometCScalarKernel]> turned into raw ptr +
    // forgotten in build_kernel_list; reconstruct and drop. Each kernel's
    // own Drop runs its `release` callback, which may reach user Drop code,
    // so contain any panic rather than abort.
    catch_panic_infallible(|| {
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(kernels, len)) })
    });
}

// -- export macro ---------------------------------------------------------

/// Emit the C-ABI discovery entry points for a list of UDF types.
///
/// Each type passed must implement [`CometCScalarUdf`] and `Default`. The
/// macro produces:
///
/// - `extern "C" fn comet_udf_abi_version() -> u32`
/// - `extern "C" fn comet_c_udf_list_v1(out: *mut CometCScalarKernelList) -> i32`
///
/// Construction runs your `Default` impl and reads your `name()`, so it is
/// wrapped in a panic guard: a panic there is reported to the host as a
/// load failure rather than aborting the process.
///
/// Your `Cargo.toml` must declare `crate-type = ["cdylib"]`.
#[macro_export]
macro_rules! comet_c_udf_export {
    ( $( $ty:ty ),+ $(,)? ) => {
        const _: () = {
            #[no_mangle]
            pub extern "C" fn comet_udf_abi_version() -> u32 {
                $crate::COMET_UDF_ABI_VERSION
            }

            #[no_mangle]
            pub unsafe extern "C" fn comet_c_udf_list_v1(
                out: *mut $crate::c_abi::CometCScalarKernelList,
            ) -> i32 {
                if out.is_null() { return -1; }
                let built = std::panic::catch_unwind(|| {
                    let kernels: Vec<$crate::c_abi::CometCScalarKernel> = vec![
                        $(
                            $crate::c_abi::CometCScalarKernel::from(
                                $crate::c_abi::ExportedScalarKernel::new(
                                    <$ty as Default>::default()
                                )
                            ),
                        )+
                    ];
                    $crate::c_abi::build_kernel_list(kernels)
                });
                match built {
                    Ok(list) => {
                        std::ptr::write(out, list);
                        0
                    }
                    Err(_) => -1,
                }
            }
        };
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    struct AddOne;
    impl CometCScalarUdf for AddOne {
        fn name(&self) -> &str {
            "add_one"
        }
        fn return_field(&self, args: &[Field]) -> Result<Field, String> {
            if args.len() != 1 || args[0].data_type() != &DataType::Int64 {
                return Err("expected (Int64) -> Int64".into());
            }
            Ok(Field::new("add_one", DataType::Int64, true))
        }
        fn invoke(&self, args: &[ArrayRef], _n: usize) -> Result<ArrayRef, String> {
            let a = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("not an Int64Array")?;
            Ok(Arc::new(
                a.iter().map(|v| v.map(|x| x + 1)).collect::<Int64Array>(),
            ))
        }
    }

    #[test]
    fn adapter_roundtrip() {
        let exp = ExportedScalarKernel::new(AddOne);
        let kernel: CometCScalarKernel = exp.into();

        // function_name lookup.
        let name_ptr = unsafe { (kernel.function_name.unwrap())(&kernel) };
        let name = unsafe { std::ffi::CStr::from_ptr(name_ptr) };
        assert_eq!(name.to_str().unwrap(), "add_one");

        // new_impl + init + execute.
        let mut impl_state = CometCScalarKernelImpl::default();
        unsafe {
            (kernel.new_impl.unwrap())(&kernel, &mut impl_state);
        }

        let arg_field = Field::new("x", DataType::Int64, true);
        let arg_schema = FFI_ArrowSchema::try_from(&arg_field).unwrap();
        let arg_schema_ptr: *const FFI_ArrowSchema = &arg_schema;
        let mut out_schema = FFI_ArrowSchema::empty();
        let rc = unsafe {
            (impl_state.init.unwrap())(
                &mut impl_state,
                &arg_schema_ptr as *const *const FFI_ArrowSchema,
                std::ptr::null(),
                1,
                &mut out_schema,
            )
        };
        assert_eq!(rc, 0);
        let out_field = Field::try_from(&out_schema).unwrap();
        assert_eq!(out_field.data_type(), &DataType::Int64);

        // execute.
        let input: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let mut input_ffi = FFI_ArrowArray::new(&input.to_data());
        let input_ffi_ptr: *mut FFI_ArrowArray = &mut input_ffi;
        let mut out_arr = FFI_ArrowArray::empty();
        let rc = unsafe {
            (impl_state.execute.unwrap())(
                &mut impl_state,
                &input_ffi_ptr as *const *mut FFI_ArrowArray,
                1,
                3,
                &mut out_arr,
            )
        };
        assert_eq!(rc, 0);
        let result_data =
            unsafe { arrow::ffi::from_ffi_and_data_type(out_arr, DataType::Int64) }.unwrap();
        let result = arrow::array::make_array(result_data);
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.values(), &[2, 3, 4]);

        // Releasing impl_state and kernel runs cleanup callbacks; ensure
        // their function pointers are cleared.
        drop(impl_state);
        drop(kernel);
    }

    /// A UDF that panics wherever it is told to.
    struct Panicky {
        in_return_field: bool,
    }

    impl CometCScalarUdf for Panicky {
        fn name(&self) -> &str {
            "panicky"
        }
        fn return_field(&self, _args: &[Field]) -> Result<Field, String> {
            assert!(!self.in_return_field, "boom in return_field");
            Ok(Field::new("panicky", DataType::Int64, true))
        }
        fn invoke(&self, _args: &[ArrayRef], _n: usize) -> Result<ArrayRef, String> {
            panic!("boom in invoke")
        }
    }

    /// Build a kernel + initialized impl for `udf`, returning the init rc.
    fn init_panicky(in_return_field: bool) -> (CometCScalarKernel, CometCScalarKernelImpl, c_int) {
        let kernel: CometCScalarKernel =
            ExportedScalarKernel::new(Panicky { in_return_field }).into();
        let mut impl_state = CometCScalarKernelImpl::default();
        unsafe {
            (kernel.new_impl.unwrap())(&kernel, &mut impl_state);
        }
        let arg_field = Field::new("x", DataType::Int64, true);
        let arg_schema = FFI_ArrowSchema::try_from(&arg_field).unwrap();
        let arg_schema_ptr: *const FFI_ArrowSchema = &arg_schema;
        let mut out_schema = FFI_ArrowSchema::empty();
        let rc = unsafe {
            (impl_state.init.unwrap())(
                &mut impl_state,
                &arg_schema_ptr as *const *const FFI_ArrowSchema,
                std::ptr::null(),
                1,
                &mut out_schema,
            )
        };
        (kernel, impl_state, rc)
    }

    fn last_error(impl_state: &mut CometCScalarKernelImpl) -> String {
        let ptr = unsafe { (impl_state.get_last_error.unwrap())(impl_state) };
        assert!(!ptr.is_null(), "expected an error message");
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_str()
            .unwrap()
            .to_string()
    }

    /// A panic in `return_field` must come back as an error code plus a
    /// message, not unwind across the `extern "C"` boundary.
    #[test]
    fn panic_in_return_field_is_contained() {
        let (_kernel, mut impl_state, rc) = init_panicky(true);
        assert_eq!(rc, C_ABI_ERR);
        let msg = last_error(&mut impl_state);
        assert!(msg.contains("panic in UDF code"), "msg: {msg}");
        assert!(msg.contains("boom in return_field"), "msg: {msg}");
    }

    /// Same for a panic in `invoke`.
    #[test]
    fn panic_in_invoke_is_contained() {
        let (_kernel, mut impl_state, rc) = init_panicky(false);
        assert_eq!(rc, 0, "init should succeed for this case");

        let input: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let mut input_ffi = FFI_ArrowArray::new(&input.to_data());
        let input_ffi_ptr: *mut FFI_ArrowArray = &mut input_ffi;
        let mut out_arr = FFI_ArrowArray::empty();
        let rc = unsafe {
            (impl_state.execute.unwrap())(
                &mut impl_state,
                &input_ffi_ptr as *const *mut FFI_ArrowArray,
                1,
                3,
                &mut out_arr,
            )
        };
        assert_eq!(rc, C_ABI_ERR);
        let msg = last_error(&mut impl_state);
        assert!(msg.contains("panic in UDF code"), "msg: {msg}");
        assert!(msg.contains("boom in invoke"), "msg: {msg}");
    }
}
