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

//! JNI entry point for driver-side validation of Rust UDF cdylibs.
//! Used by `org.apache.comet.udf.CometRustUdfBridge` on the driver.

use crate::errors::{try_unwrap_or_throw, CometError};
use crate::execution::rust_udf::cache::get_or_load;
use jni::objects::{JClass, JString};
use jni::EnvUnowned;

/// Validate that `library_path` loads and exposes a UDF named
/// `expected_name`.
///
/// Returns normally when it does and throws otherwise. The driver only
/// needs that yes-or-no answer: everything else it knows about the UDF
/// comes from the `CometRustUDF.register` call itself, and a return type
/// cannot be reported from here anyway without argument types to resolve
/// it against.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_udf_CometRustUdfBridge_validateLibrary(
    e: EnvUnowned,
    _class: JClass,
    library_path: JString,
    expected_name: JString,
) {
    try_unwrap_or_throw(&e, |env| {
        let path: String = library_path
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let name: String = expected_name
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let lib = get_or_load(&path).map_err(|e| CometError::Internal(e.to_string()))?;
        if !lib.udfs.iter().any(|u| u.name == name) {
            // `CometRustUDF.classifyNativeError` keys on "' not found in " to turn this into a
            // NoSuchElementException. Keep the two in step.
            return Err(CometError::Internal(format!(
                "UDF '{name}' not found in {path}"
            )));
        }
        Ok(())
    })
}
