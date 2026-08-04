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

//! JNI entry points for driver-side validation of Rust UDF cdylibs.
//! Used by `org.apache.comet.udf.CometRustUdfBridge` on the driver.

use crate::errors::{try_unwrap_or_throw, CometError};
use crate::execution::rust_udf::cache::get_or_load;
use crate::execution::rust_udf::loader::LoadedUdf;
use jni::objects::{JClass, JString};
use jni::sys::jobject;
use jni::EnvUnowned;

/// Best-effort serialization of a single discovered UDF as JSON.
///
/// Reporting `args`/`return_type` would require calling the kernel's
/// `init` to discover a return type; that is deferred, so this returns
/// only what the Scala registry needs today (`name`).
fn udf_to_json(udf: &LoadedUdf) -> serde_json::Value {
    serde_json::json!({
        "name": udf.name,
    })
}

/// Validate that `library_path` loads, exposes a UDF named
/// `expected_name`, and return a JSON description of that UDF. Throws
/// on any error.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_udf_CometRustUdfBridge_validateLibrary(
    e: EnvUnowned,
    _class: JClass,
    library_path: JString,
    expected_name: JString,
) -> jobject {
    try_unwrap_or_throw(&e, |env| {
        let path: String = library_path
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let name: String = expected_name
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let lib = get_or_load(&path).map_err(|e| CometError::Internal(e.to_string()))?;
        let udf = lib
            .udfs
            .iter()
            .find(|u| u.name == name)
            .ok_or_else(|| CometError::Internal(format!("UDF '{name}' not found in {path}")))?;
        let json = udf_to_json(udf).to_string();
        let jstr = env
            .new_string(json)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        Ok(jstr.into_raw())
    })
}

/// Return a JSON array describing every UDF exposed by `library_path`.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_udf_CometRustUdfBridge_listUdfs(
    e: EnvUnowned,
    _class: JClass,
    library_path: JString,
) -> jobject {
    try_unwrap_or_throw(&e, |env| {
        let path: String = library_path
            .try_to_string(env)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        let lib = get_or_load(&path).map_err(|e| CometError::Internal(e.to_string()))?;
        let entries: Vec<serde_json::Value> = lib.udfs.iter().map(udf_to_json).collect();
        let json = serde_json::Value::Array(entries).to_string();
        let jstr = env
            .new_string(json)
            .map_err(|e| CometError::Internal(e.to_string()))?;
        Ok(jstr.into_raw())
    })
}
