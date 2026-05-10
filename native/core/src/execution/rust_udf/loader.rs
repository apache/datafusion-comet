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

//! Loader: open a UDF cdylib, validate ABI, parse the per-UDF
//! descriptors into native types ready for `RustUdfAdapter`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use libloading::{Library, Symbol};

const HOST_ABI_VERSION: u32 = 1;

/// Result of loading a UDF cdylib: the live `Library` plus per-UDF
/// descriptors converted to native types.
#[derive(Debug)]
pub struct LoadedLibrary {
    /// Canonicalized path the library was loaded from.
    pub path: PathBuf,
    /// The loaded `Library`. Held inside an `Arc` so loaded UDFs can
    /// outlive lookups.
    pub library: Arc<Library>,
    /// One descriptor per UDF, in the order returned by `comet_udf_count`.
    pub udfs: Vec<LoadedUdf>,
}

/// Per-UDF descriptor parsed from `comet_udf_describe`.
#[derive(Debug, Clone)]
pub struct LoadedUdf {
    /// Index in the cdylib's UDF table.
    pub idx: u32,
    /// UDF name as exposed via `comet_udf_describe`.
    pub name: String,
    /// Argument data types in declaration order.
    pub args: Vec<DataType>,
    /// Return data type.
    pub return_type: DataType,
    /// Volatility tag: 0 = Immutable, 1 = Stable, 2 = Volatile.
    pub volatility: u32,
}

/// Errors returned by the loader.
#[derive(Debug)]
pub enum LoaderError {
    /// `libloading::Library::new` failed (file missing, not a shared object,
    /// permissions, etc.).
    Open {
        /// Path that was passed to `Library::new`.
        path: PathBuf,
        /// Underlying `libloading` error.
        source: libloading::Error,
    },
    /// A required `extern "C"` symbol is not exported.
    MissingSymbol {
        /// Path of the library that is missing the symbol.
        path: PathBuf,
        /// Name of the symbol that was not found.
        name: &'static str,
    },
    /// `comet_udf_abi_version()` returned a value other than the host's.
    AbiMismatch {
        /// Path of the offending library.
        path: PathBuf,
        /// ABI version the library reported.
        found: u32,
        /// ABI version this host requires.
        expected: u32,
    },
    /// `comet_udf_describe(idx)` returned a nonzero rc.
    Describe {
        /// Path of the library.
        path: PathBuf,
        /// UDF index passed to `comet_udf_describe`.
        idx: u32,
        /// Return code from `comet_udf_describe`.
        code: i32,
    },
    /// Descriptor parse failure (null name, invalid IPC bytes, etc.).
    BadDescriptor {
        /// Path of the library.
        path: PathBuf,
        /// UDF index whose descriptor could not be parsed.
        idx: u32,
        /// Human-readable explanation.
        reason: String,
    },
}

impl std::fmt::Display for LoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use LoaderError::*;
        match self {
            Open { path, source } => write!(f, "failed to open {}: {source}", path.display()),
            MissingSymbol { path, name } => {
                write!(f, "{} missing required symbol {name}", path.display())
            }
            AbiMismatch { path, found, expected } => write!(
                f,
                "{} reports ABI v{found}, host expects v{expected}",
                path.display()
            ),
            Describe { path, idx, code } => write!(
                f,
                "{} comet_udf_describe(idx={idx}) returned {code}",
                path.display()
            ),
            BadDescriptor { path, idx, reason } => {
                write!(f, "{} idx={idx}: {reason}", path.display())
            }
        }
    }
}

impl std::error::Error for LoaderError {}

/// Open and validate a UDF cdylib. Returns a `LoadedLibrary` containing
/// the live `Library` handle and one parsed `LoadedUdf` per UDF the
/// cdylib exports.
pub fn load(path: impl AsRef<Path>) -> Result<LoadedLibrary, LoaderError> {
    let path = path.as_ref().to_path_buf();
    // SAFETY: `Library::new` runs the cdylib's static initializers. We
    // accept this risk because user UDF cdylibs are explicitly registered
    // by an operator via `CometRustUDF.register`.
    let library = unsafe { Library::new(&path) }
        .map_err(|source| LoaderError::Open { path: path.clone(), source })?;

    let abi: Symbol<unsafe extern "C" fn() -> u32> =
        unsafe { library.get(b"comet_udf_abi_version") }.map_err(|_| {
            LoaderError::MissingSymbol {
                path: path.clone(),
                name: "comet_udf_abi_version",
            }
        })?;
    let v = unsafe { abi() };
    if v != HOST_ABI_VERSION {
        return Err(LoaderError::AbiMismatch {
            path,
            found: v,
            expected: HOST_ABI_VERSION,
        });
    }

    let count: Symbol<unsafe extern "C" fn() -> u32> =
        unsafe { library.get(b"comet_udf_count") }.map_err(|_| LoaderError::MissingSymbol {
            path: path.clone(),
            name: "comet_udf_count",
        })?;
    let n = unsafe { count() };

    let describe: Symbol<
        unsafe extern "C" fn(u32, *mut comet_udf_sdk::types::UdfDescriptor) -> i32,
    > = unsafe { library.get(b"comet_udf_describe") }.map_err(|_| {
        LoaderError::MissingSymbol {
            path: path.clone(),
            name: "comet_udf_describe",
        }
    })?;

    // Verify invoke and free_error are present (we don't call them here).
    type InvokeFn = unsafe extern "C" fn(
        u32,
        *const FFI_ArrowArray,
        *const FFI_ArrowSchema,
        u32,
        *mut FFI_ArrowArray,
        *mut FFI_ArrowSchema,
        *mut comet_udf_sdk::error::UdfError,
    ) -> i32;
    let _: Symbol<InvokeFn> =
        unsafe { library.get(b"comet_udf_invoke") }.map_err(|_| LoaderError::MissingSymbol {
            path: path.clone(),
            name: "comet_udf_invoke",
        })?;
    let _: Symbol<unsafe extern "C" fn(*mut comet_udf_sdk::error::UdfError)> =
        unsafe { library.get(b"comet_udf_free_error") }.map_err(|_| {
            LoaderError::MissingSymbol {
                path: path.clone(),
                name: "comet_udf_free_error",
            }
        })?;

    let mut udfs = Vec::with_capacity(n as usize);
    for idx in 0..n {
        let mut desc = comet_udf_sdk::types::UdfDescriptor::zeroed();
        let rc = unsafe { describe(idx, &mut desc) };
        if rc != 0 {
            return Err(LoaderError::Describe { path: path.clone(), idx, code: rc });
        }
        let parsed = parse_descriptor(&desc).map_err(|reason| LoaderError::BadDescriptor {
            path: path.clone(),
            idx,
            reason,
        })?;
        udfs.push(LoadedUdf { idx, ..parsed });
    }

    Ok(LoadedLibrary { path, library: Arc::new(library), udfs })
}

fn parse_descriptor(desc: &comet_udf_sdk::types::UdfDescriptor) -> Result<LoadedUdf, String> {
    if desc.name_ptr.is_null() {
        return Err("name_ptr is null".to_string());
    }
    let name_bytes = unsafe {
        std::slice::from_raw_parts(desc.name_ptr as *const u8, desc.name_len as usize)
    };
    let name = std::str::from_utf8(name_bytes)
        .map_err(|e| format!("name not UTF-8: {e}"))?
        .to_string();

    let n = desc.n_args as usize;
    let tags: &[u32] = if n == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(desc.arg_tags, n) }
    };
    let ipc_ptrs: &[*const u8] = if n == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(desc.arg_field_ipc_ptrs, n) }
    };
    let ipc_lens: &[u32] = if n == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(desc.arg_field_ipc_lens, n) }
    };

    let mut args = Vec::with_capacity(n);
    for i in 0..n {
        args.push(decode_type(tags[i], ipc_ptrs[i], ipc_lens[i])?);
    }
    let return_type = decode_type(
        desc.return_tag,
        desc.return_field_ipc_ptr,
        desc.return_field_ipc_len,
    )?;

    Ok(LoadedUdf {
        idx: 0, // filled by caller
        name,
        args,
        return_type,
        volatility: desc.volatility,
    })
}

fn decode_type(tag: u32, ipc_ptr: *const u8, ipc_len: u32) -> Result<DataType, String> {
    use comet_udf_sdk::types::ArrowTypeTag;
    if tag == ArrowTypeTag::Field as u32 {
        if ipc_ptr.is_null() {
            return Err("Field tag with null IPC pointer".into());
        }
        let bytes = unsafe { std::slice::from_raw_parts(ipc_ptr, ipc_len as usize) };
        let f = comet_udf_sdk::types::field_from_ipc_bytes(bytes)?;
        Ok(f.data_type().clone())
    } else {
        // SAFETY: ArrowTypeTag is repr(u32) with explicit values 0..=17 and 255.
        // Any other tag value is a protocol violation; transmute would
        // produce an invalid enum value -- check first.
        let primitive_max: u32 = ArrowTypeTag::Date64 as u32;
        let field_tag: u32 = ArrowTypeTag::Field as u32;
        if tag > primitive_max && tag != field_tag {
            return Err(format!("unknown ArrowTypeTag value {tag}"));
        }
        let typed: ArrowTypeTag = unsafe { std::mem::transmute(tag) };
        typed
            .to_data_type()
            .ok_or_else(|| format!("ArrowTypeTag {tag} has no DataType mapping"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::rust_udf::test_support::test_udfs_path;

    #[test]
    fn load_test_udfs_succeeds() {
        let lib = load(test_udfs_path()).expect("load");
        let names: Vec<_> = lib.udfs.iter().map(|u| u.name.as_str()).collect();
        assert!(names.contains(&"add_one"), "names: {names:?}");
        assert!(names.contains(&"struct_field_a"), "names: {names:?}");
        assert!(names.contains(&"always_err"), "names: {names:?}");
        assert!(names.contains(&"always_panic"), "names: {names:?}");
        assert!(names.contains(&"length_one"), "names: {names:?}");
        assert_eq!(lib.udfs.len(), 5);
    }

    #[test]
    fn missing_path_errors_open() {
        let err = load("/no/such/path.so").unwrap_err();
        assert!(matches!(err, LoaderError::Open { .. }), "got: {err:?}");
    }

    #[test]
    fn struct_arg_decoded_correctly() {
        let lib = load(test_udfs_path()).unwrap();
        let u = lib.udfs.iter().find(|u| u.name == "struct_field_a").unwrap();
        assert!(
            matches!(u.args[0], arrow::datatypes::DataType::Struct(_)),
            "got: {:?}",
            u.args[0]
        );
    }
}
