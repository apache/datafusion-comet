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

use jni::signature::Primitive;
use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::ReturnType,
    strings::JNIString,
    Env,
};

/// A struct that holds JNI methods for the JVM `CometHandleBatchIterator` class.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct CometHandleBatchIterator<'a> {
    pub class: JClass<'a>,
    pub method_next_handle: JMethodID,
    pub method_next_handle_ret: ReturnType,
    pub method_get_source_native_plan: JMethodID,
    pub method_get_source_native_plan_ret: ReturnType,
}

impl<'a> CometHandleBatchIterator<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometHandleBatchIterator";

    pub fn new(env: &mut Env<'a>) -> JniResult<CometHandleBatchIterator<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;

        Ok(CometHandleBatchIterator {
            class,
            method_next_handle: env.get_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("nextHandle"),
                jni::jni_sig!("()J"),
            )?,
            method_next_handle_ret: ReturnType::Primitive(Primitive::Long),
            method_get_source_native_plan: env.get_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("getSourceNativePlan"),
                jni::jni_sig!("()J"),
            )?,
            method_get_source_native_plan_ret: ReturnType::Primitive(Primitive::Long),
        })
    }
}
