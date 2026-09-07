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

use jni::{
    errors::Result as JniResult,
    objects::{Global, JClass, JStaticMethodID},
    signature::ReturnType,
    strings::JNIString,
    Env,
};

/// JNI bindings for the JVM `org.apache.comet.CometSchemaUtils` helper. Used by the native
/// Parquet schema adapter to fold field names with the JVM's `String.toLowerCase(Locale.ROOT)`,
/// so case-insensitive field resolution matches Spark's `ParquetReadSupport` exactly.
pub struct CometSchemaUtils {
    /// Global reference to the helper class. `find_class` hands back a *local* reference that is
    /// only valid inside the native call that created it (here, `JVMClasses::init`); once that
    /// call returns to the JVM the local ref is freed, so a later `toLowerCaseRoot` from a
    /// different native frame would pass a stale `jclass` (`-Xcheck:jni` aborts with "Bad global
    /// or local ref"). Promoting it to a JNI global reference keeps it alive process-wide,
    /// matching the lifetime of the cached `JVMClasses`.
    pub class: Global<JClass<'static>>,
    pub method_to_lower_case_root: JStaticMethodID,
    pub method_to_lower_case_root_ret: ReturnType,
}

impl CometSchemaUtils {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometSchemaUtils";

    pub fn new(env: &mut Env) -> JniResult<CometSchemaUtils> {
        let local_class = env.find_class(JNIString::new(Self::JVM_CLASS))?;
        // Retain the class as a global reference before it is stored in `JVMClasses`, so it
        // survives past this initialization frame (see the `class` field doc).
        let class = env.new_global_ref(&local_class)?;

        Ok(CometSchemaUtils {
            class,
            method_to_lower_case_root: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("toLowerCaseRoot"),
                jni::jni_sig!("(Ljava/lang/String;)Ljava/lang/String;"),
            )?,
            method_to_lower_case_root_ret: ReturnType::Object,
        })
    }
}
