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

//! Loader: open a UDF cdylib via libloading, validate the ABI version,
//! discover UDFs via the C-ABI and/or datafusion-ffi entry points, and
//! produce DataFusion `ScalarUDFImpl` impls for each.
//!
//! See `super::mod.rs` for an overview of the two flavors.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use comet_udf_sdk::c_abi::CometCScalarKernelList;
use comet_udf_sdk::df_abi::CometDfUdfList;
use comet_udf_sdk::{
    ABI_VERSION_SYMBOL, COMET_UDF_ABI_VERSION, C_ABI_DISCOVERY_SYMBOL, DF_ABI_DISCOVERY_SYMBOL,
};
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion_ffi::udf::FFI_ScalarUDF;
use libloading::{Library, Symbol};

use super::imported_c::ImportedCScalarUdf;

/// Which ABI flavor a given UDF was loaded through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfAbi {
    /// Loaded via the pure-C arrow-ffi flavor (sedona-style).
    C,
    /// Loaded via datafusion-ffi (`FFI_ScalarUDF`).
    DataFusion,
}

/// One loaded UDF: name, ABI flavor, and a `ScalarUDFImpl` ready to plug
/// into the planner.
pub struct LoadedUdf {
    /// UDF name as exposed by the cdylib.
    pub name: String,
    /// Which ABI this UDF was discovered through.
    pub abi: UdfAbi,
    /// The `ScalarUDFImpl` adapter the planner will wrap.
    pub udf_impl: Arc<dyn ScalarUDFImpl>,
}

impl std::fmt::Debug for LoadedUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedUdf")
            .field("name", &self.name)
            .field("abi", &self.abi)
            .finish()
    }
}

/// Result of loading a UDF cdylib: the live `Library` plus per-UDF
/// adapters.
pub struct LoadedLibrary {
    /// Canonicalized path the library was loaded from.
    pub path: PathBuf,
    /// The loaded `Library`. Held inside an `Arc` so loaded UDFs can
    /// outlive lookups. Library is never unloaded for the process lifetime.
    pub library: Arc<Library>,
    /// One entry per UDF, with name and ScalarUDFImpl already built.
    pub udfs: Vec<LoadedUdf>,
}

impl std::fmt::Debug for LoadedLibrary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedLibrary")
            .field("path", &self.path)
            .field("udfs", &self.udfs)
            .finish()
    }
}

/// Errors returned by the loader.
#[derive(Debug)]
pub enum LoaderError {
    /// `libloading::Library::new` failed.
    Open {
        /// Path that was passed to `Library::new`.
        path: PathBuf,
        /// Underlying error.
        source: libloading::Error,
    },
    /// `comet_udf_abi_version` is missing or returned an unexpected value.
    AbiMismatch {
        /// Path of the offending library.
        path: PathBuf,
        /// Version reported by the cdylib (or `None` if the symbol is missing).
        found: Option<u32>,
        /// Version this host expects.
        expected: u32,
    },
    /// Library exposes neither `comet_c_udf_list_v1` nor
    /// `comet_df_udf_list_v1`.
    NoDiscovery {
        /// Path of the offending library.
        path: PathBuf,
    },
    /// One of the discovery functions returned a non-zero rc, or a
    /// kernel/UDF entry was malformed.
    Discovery {
        /// Path of the library.
        path: PathBuf,
        /// Human-readable reason.
        reason: String,
    },
}

impl std::fmt::Display for LoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use LoaderError::*;
        match self {
            Open { path, source } => write!(f, "failed to open {}: {source}", path.display()),
            AbiMismatch { path, found, expected } => match found {
                Some(v) => write!(
                    f,
                    "{} reports ABI v{v}, host expects v{expected}",
                    path.display()
                ),
                None => write!(
                    f,
                    "{} missing required symbol {ABI_VERSION_SYMBOL}",
                    path.display()
                ),
            },
            NoDiscovery { path } => write!(
                f,
                "{} exposes neither {C_ABI_DISCOVERY_SYMBOL} nor \
                 {DF_ABI_DISCOVERY_SYMBOL}",
                path.display()
            ),
            Discovery { path, reason } => write!(f, "{}: {reason}", path.display()),
        }
    }
}

impl std::error::Error for LoaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LoaderError::Open { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Open and validate a UDF cdylib.
pub fn load(path: impl AsRef<Path>) -> Result<LoadedLibrary, LoaderError> {
    let path = path.as_ref().to_path_buf();
    // SAFETY: `Library::new` runs the cdylib's static initializers. We
    // accept this risk because user UDF cdylibs are explicitly registered
    // by an operator via `CometRustUDF.register`.
    let library = unsafe { Library::new(&path) }
        .map_err(|source| LoaderError::Open { path: path.clone(), source })?;

    // ABI version probe.
    let v = read_abi_version(&library, &path)?;
    if v != COMET_UDF_ABI_VERSION {
        return Err(LoaderError::AbiMismatch {
            path,
            found: Some(v),
            expected: COMET_UDF_ABI_VERSION,
        });
    }

    let mut udfs: Vec<LoadedUdf> = Vec::new();
    let mut have_any = false;

    if let Some(c_kernels) = read_c_kernels(&library, &path)? {
        have_any = true;
        for udf in c_kernels {
            udfs.push(udf);
        }
    }

    if let Some(df_udfs) = read_df_udfs(&library, &path)? {
        have_any = true;
        for udf in df_udfs {
            // Prefer C-ABI registration on collision.
            if udfs.iter().any(|u| u.name == udf.name) {
                continue;
            }
            udfs.push(udf);
        }
    }

    if !have_any {
        return Err(LoaderError::NoDiscovery { path });
    }

    Ok(LoadedLibrary {
        path,
        library: Arc::new(library),
        udfs,
    })
}

fn read_abi_version(lib: &Library, path: &Path) -> Result<u32, LoaderError> {
    let sym: Symbol<unsafe extern "C" fn() -> u32> =
        unsafe { lib.get(ABI_VERSION_SYMBOL.as_bytes()) }
            .map_err(|_| LoaderError::AbiMismatch {
                path: path.to_path_buf(),
                found: None,
                expected: COMET_UDF_ABI_VERSION,
            })?;
    // SAFETY: comet_udf_abi_version takes no arguments, returns u32, no side effects.
    Ok(unsafe { sym() })
}

fn read_c_kernels(
    lib: &Library,
    path: &Path,
) -> Result<Option<Vec<LoadedUdf>>, LoaderError> {
    let sym: Symbol<
        unsafe extern "C" fn(*mut CometCScalarKernelList) -> i32,
    > = match unsafe { lib.get(C_ABI_DISCOVERY_SYMBOL.as_bytes()) } {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    let mut list = CometCScalarKernelList::default();
    // SAFETY: list is caller-allocated; the cdylib writes into it via `out`.
    let rc = unsafe { sym(&mut list) };
    if rc != 0 {
        return Err(LoaderError::Discovery {
            path: path.to_path_buf(),
            reason: format!("{C_ABI_DISCOVERY_SYMBOL} returned rc={rc}"),
        });
    }
    let mut udfs = Vec::with_capacity(list.len.max(0) as usize);
    if !list.kernels.is_null() && list.len > 0 {
        // Move each kernel out of the array into a Box so it owns itself.
        // We can't simply read each entry because they implement Drop;
        // doing it via std::ptr::read transfers ownership cleanly.
        let len = list.len as usize;
        for i in 0..len {
            // SAFETY: the kernel array was produced by the cdylib's
            // `comet_c_udf_export!` and contains `len` valid entries.
            // We move each entry out into a Box so its Drop runs when
            // the host releases the loaded library.
            let raw = unsafe { list.kernels.add(i) };
            let kernel = unsafe { std::ptr::read(raw) };
            // Replace the slot with a default kernel (no callbacks) so
            // the array's release doesn't double-free.
            unsafe {
                std::ptr::write(
                    raw,
                    comet_udf_sdk::c_abi::CometCScalarKernel::default(),
                );
            }
            let imported = ImportedCScalarUdf::try_new(Box::new(kernel)).map_err(|e| {
                LoaderError::Discovery {
                    path: path.to_path_buf(),
                    reason: format!("import C kernel idx={i}: {e}"),
                }
            })?;
            udfs.push(LoadedUdf {
                name: imported.name().to_string(),
                abi: UdfAbi::C,
                udf_impl: Arc::new(imported),
            });
        }
    }
    // list's Drop releases the array storage.
    drop(list);
    Ok(Some(udfs))
}

fn read_df_udfs(
    lib: &Library,
    path: &Path,
) -> Result<Option<Vec<LoadedUdf>>, LoaderError> {
    let sym: Symbol<unsafe extern "C" fn(*mut CometDfUdfList) -> i32> =
        match unsafe { lib.get(DF_ABI_DISCOVERY_SYMBOL.as_bytes()) } {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
    let mut list = CometDfUdfList::default();
    // SAFETY: list is caller-allocated; the cdylib writes into it.
    let rc = unsafe { sym(&mut list) };
    if rc != 0 {
        return Err(LoaderError::Discovery {
            path: path.to_path_buf(),
            reason: format!("{DF_ABI_DISCOVERY_SYMBOL} returned rc={rc}"),
        });
    }
    let mut udfs = Vec::with_capacity(list.len.max(0) as usize);
    if !list.udfs.is_null() && list.len > 0 {
        let len = list.len as usize;
        for i in 0..len {
            // SAFETY: the FFI_ScalarUDF array was produced by the cdylib's
            // `comet_df_udf_export!` and contains `len` valid entries.
            let entry: &FFI_ScalarUDF = unsafe { &*list.udfs.add(i) };
            // Convert to ScalarUDFImpl via the canonical From impl;
            // the resulting Arc clones the FFI wrapper internally.
            let imp: Arc<dyn ScalarUDFImpl> = entry.into();
            udfs.push(LoadedUdf {
                name: imp.name().to_string(),
                abi: UdfAbi::DataFusion,
                udf_impl: imp,
            });
        }
    }
    // list's Drop releases the array storage; each FFI_ScalarUDF entry
    // remains valid for the lifetime of the loaded Library, since the
    // resulting ScalarUDFImpls cloned the wrappers via the From impl.
    drop(list);
    Ok(Some(udfs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::rust_udf::test_support::test_udfs_path;

    #[test]
    fn load_test_udfs_succeeds() {
        let lib = load(test_udfs_path()).expect("load");
        let names: Vec<_> = lib.udfs.iter().map(|u| u.name.as_str()).collect();
        assert!(names.contains(&"add_one_c"), "names: {names:?}");
        assert!(names.contains(&"add_one_df"), "names: {names:?}");
    }

    #[test]
    fn missing_path_errors_open() {
        let err = load("/no/such/path.so").unwrap_err();
        assert!(matches!(err, LoaderError::Open { .. }), "got: {err:?}");
    }

    #[test]
    fn c_and_df_abi_have_expected_flavors() {
        let lib = load(test_udfs_path()).expect("load");
        let c_udf = lib.udfs.iter().find(|u| u.name == "add_one_c").unwrap();
        let df_udf = lib.udfs.iter().find(|u| u.name == "add_one_df").unwrap();
        assert_eq!(c_udf.abi, UdfAbi::C);
        assert_eq!(df_udf.abi, UdfAbi::DataFusion);
    }
}
