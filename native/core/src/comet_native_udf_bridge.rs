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

//! JNI entry point for driver-side validation of native UDF cdylibs.
//! Used by `org.apache.comet.udf.CometNativeUdfBridge` on the driver.

use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::c_udf::cache::get_or_load;
use crate::execution::c_udf::loader::LoaderError;
use crate::execution::c_udf::policy::NativeUdfPolicy;
use jni::objects::{JClass, JString};
use jni::sys::jboolean;
use jni::EnvUnowned;

/// Validate that `library_path` loads and exposes a UDF named
/// `expected_name`, once `enabled` and `allowed_paths` (the driver's
/// `spark.comet.nativeUdf.*` configs) have allowed the path.
///
/// Returns normally when it does and throws otherwise. The driver only
/// needs that yes-or-no answer: everything else it knows about the UDF
/// comes from the `CometNativeUDF.register` call itself, and a return type
/// cannot be reported from here anyway without argument types to resolve
/// it against.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_udf_CometNativeUdfBridge_validateLibrary(
    e: EnvUnowned,
    _class: JClass,
    library_path: JString,
    expected_name: JString,
    enabled: jboolean,
    allowed_paths: JString,
) {
    try_unwrap_or_throw(&e, |env| {
        let path: String = library_path
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let name: String = expected_name
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let allowed_paths: String = allowed_paths
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        validate_native_udf_library(&path, &name, enabled, &allowed_paths)
    })
}

/// Core of [`Java_org_apache_comet_udf_CometNativeUdfBridge_validateLibrary`].
///
/// A refusal by the policy, an ABI mismatch and an unknown name each fail with the variant that
/// raises its own JVM exception. Any other failure is a [`CometError::Internal`], which
/// `CometNativeUDF` reports as a `CometNativeUdfLoadException`.
pub fn validate_native_udf_library(
    path: &str,
    name: &str,
    enabled: bool,
    allowed_paths: &str,
) -> CometResult<()> {
    NativeUdfPolicy::new(enabled, allowed_paths)
        .check(path)
        .map_err(|e| CometError::NativeUdfNotAllowed(driver_message(e)))?;
    let lib = get_or_load(path).map_err(loader_error)?;
    if !lib.udfs.iter().any(|u| u.name == name) {
        return Err(CometError::NoSuchElement(driver_message(format!(
            "UDF '{name}' not found in {path}"
        ))));
    }
    Ok(())
}

fn loader_error(e: LoaderError) -> CometError {
    match e {
        LoaderError::AbiMismatch { .. } | LoaderError::NoDiscovery { .. } => {
            CometError::NativeUdfAbi(driver_message(e))
        }
        LoaderError::Open { .. } | LoaderError::Discovery { .. } => {
            CometError::Internal(e.to_string())
        }
    }
}

/// The message `CometNativeUDF.register` has always reported for these failures, from when every
/// one of them crossed JNI as a [`CometError::Internal`].
fn driver_message(e: impl std::fmt::Display) -> String {
    CometError::Internal(e.to_string()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::c_udf::test_support::{test_udfs_path, BUILD_HINT};
    use std::path::PathBuf;

    fn udfs_path() -> String {
        test_udfs_path().to_string_lossy().into_owned()
    }

    #[test]
    fn an_exported_udf_validates() {
        validate_native_udf_library(&udfs_path(), "add_one_c", true, "").expect(BUILD_HINT);
    }

    #[test]
    fn an_unknown_name_is_no_such_element() {
        let path = udfs_path();
        match validate_native_udf_library(&path, "no_such_udf_c", true, "") {
            Err(CometError::NoSuchElement(m)) => assert_eq!(
                m,
                format!("Comet Internal Error: UDF 'no_such_udf_c' not found in {path}")
            ),
            other => panic!("{BUILD_HINT}; got {other:?}"),
        }
    }

    #[test]
    fn disabled_is_not_allowed() {
        match validate_native_udf_library(&udfs_path(), "add_one_c", false, "") {
            Err(CometError::NativeUdfNotAllowed(m)) => assert_eq!(
                m,
                "Comet Internal Error: native UDFs are disabled by \
                 spark.comet.nativeUdf.enabled=false"
            ),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn a_library_outside_the_allowed_paths_is_not_allowed() {
        let elsewhere = tempfile::tempdir().unwrap();
        match validate_native_udf_library(
            &udfs_path(),
            "add_one_c",
            true,
            elsewhere.path().to_str().unwrap(),
        ) {
            Err(CometError::NativeUdfNotAllowed(m)) => {
                assert!(m.contains("spark.comet.nativeUdf.allowedPaths"), "{m}")
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn a_missing_library_is_a_load_failure() {
        match validate_native_udf_library("/no/such/ABI/library.so", "add_one_c", true, "") {
            Err(CometError::Internal(m)) => {
                assert!(
                    m.starts_with("failed to open /no/such/ABI/library.so"),
                    "{m}"
                )
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn abi_failures_are_abi_errors() {
        let path = PathBuf::from("/lib/libx.so");
        let missing_version = loader_error(LoaderError::AbiMismatch {
            path: path.clone(),
            found: None,
            expected: 1,
        });
        assert!(
            matches!(&missing_version, CometError::NativeUdfAbi(m)
                if m.starts_with("Comet Internal Error: /lib/libx.so missing required symbol")),
            "got {missing_version:?}"
        );
        let wrong_version = loader_error(LoaderError::AbiMismatch {
            path: path.clone(),
            found: Some(7),
            expected: 1,
        });
        assert!(
            matches!(&wrong_version, CometError::NativeUdfAbi(m)
                if m == "Comet Internal Error: /lib/libx.so reports ABI v7, host expects v1"),
            "got {wrong_version:?}"
        );
        let no_discovery = loader_error(LoaderError::NoDiscovery { path: path.clone() });
        assert!(
            matches!(&no_discovery, CometError::NativeUdfAbi(m) if m.contains("does not export")),
            "got {no_discovery:?}"
        );
    }

    #[test]
    fn a_failed_discovery_is_a_load_failure() {
        let error = loader_error(LoaderError::Discovery {
            path: PathBuf::from("/lib/libx.so"),
            reason: "rc=1".to_string(),
        });
        assert!(
            matches!(&error, CometError::Internal(m) if m == "/lib/libx.so: rc=1"),
            "got {error:?}"
        );
    }
}
