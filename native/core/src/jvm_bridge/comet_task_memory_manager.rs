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
    objects::{JClass, JMethodID},
    signature::{Primitive, ReturnType},
    JNIEnv,
};

/// A wrapper which delegate acquire/release memory calls to the
/// JVM side `CometTaskMemoryManager`.
#[derive(Debug)]
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct CometTaskMemoryManager<'a> {
    pub class: JClass<'a>,
    pub method_acquire_memory: JMethodID,
    pub method_release_memory: JMethodID,

    pub method_acquire_memory_ret: ReturnType,
    pub method_release_memory_ret: ReturnType,
}

impl<'a> CometTaskMemoryManager<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/CometTaskMemoryManager";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometTaskMemoryManager<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        let result = CometTaskMemoryManager {
            class,
            method_acquire_memory: env.get_method_id(
                Self::JVM_CLASS,
                "acquireMemory",
                "(J)J".to_string(),
            )?,
            method_release_memory: env.get_method_id(
                Self::JVM_CLASS,
                "releaseMemory",
                "(J)V".to_string(),
            )?,
            method_acquire_memory_ret: ReturnType::Primitive(Primitive::Long),
            method_release_memory_ret: ReturnType::Primitive(Primitive::Void),
        };
        Ok(result)
    }
}
