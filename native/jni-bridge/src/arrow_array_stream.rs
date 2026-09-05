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
    strings::JNIString,
    Env,
};

/// A struct that holds all the JNI methods and fields for the JVM `ArrowArrayStream` class.
/// `memoryAddress()` is read once per partition so native can take ownership of the underlying C
/// struct via `AlignedArrowStreamReader::from_raw`.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct ArrowArrayStream<'a> {
    pub class: JClass<'a>,
    pub method_memory_address: JMethodID,
    pub method_memory_address_ret: ReturnType,
}

impl<'a> ArrowArrayStream<'a> {
    const SHADED_JVM_CLASS: &'static str = "org/apache/comet/shaded/arrow/c/ArrowArrayStream";
    const UNSHADED_JVM_CLASS: &'static str = "org/apache/arrow/c/ArrowArrayStream";

    pub fn new(env: &mut Env<'a>) -> JniResult<ArrowArrayStream<'a>> {
        let (class, class_name) = match env.find_class(JNIString::new(Self::SHADED_JVM_CLASS)) {
            Ok(class) => (class, Self::SHADED_JVM_CLASS),
            Err(_) => {
                env.exception_clear();
                (
                    env.find_class(JNIString::new(Self::UNSHADED_JVM_CLASS))?,
                    Self::UNSHADED_JVM_CLASS,
                )
            }
        };

        Ok(ArrowArrayStream {
            method_memory_address: env.get_method_id(
                JNIString::new(class_name),
                jni::jni_str!("memoryAddress"),
                jni::jni_sig!("()J"),
            )?,
            method_memory_address_ret: ReturnType::Primitive(Primitive::Long),
            class,
        })
    }
}
