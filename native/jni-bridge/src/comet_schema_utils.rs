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
    objects::{JClass, JStaticMethodID},
    signature::ReturnType,
    strings::JNIString,
    Env,
};

/// JNI bindings for the JVM `org.apache.comet.CometSchemaUtils` helper. Used by the native
/// Parquet schema adapter to fold field names with the JVM's `String.toLowerCase(Locale.ROOT)`,
/// so case-insensitive field resolution matches Spark's `ParquetReadSupport` exactly.
pub struct CometSchemaUtils<'a> {
    pub class: JClass<'a>,
    pub method_to_lower_case_root: JStaticMethodID,
    pub method_to_lower_case_root_ret: ReturnType,
}

impl<'a> CometSchemaUtils<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometSchemaUtils";

    pub fn new(env: &mut Env<'a>) -> JniResult<CometSchemaUtils<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;

        Ok(CometSchemaUtils {
            method_to_lower_case_root: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("toLowerCaseRoot"),
                jni::jni_sig!("(Ljava/lang/String;)Ljava/lang/String;"),
            )?,
            method_to_lower_case_root_ret: ReturnType::Object,
            class,
        })
    }
}
