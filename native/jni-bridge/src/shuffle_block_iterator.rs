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
    JNIEnv,
};

/// A struct that holds all the JNI methods and fields for JVM `CometShuffleBlockIterator` class.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct CometShuffleBlockIterator<'a> {
    pub class: JClass<'a>,
    pub method_has_next: JMethodID,
    pub method_has_next_ret: ReturnType,
    pub method_get_buffer: JMethodID,
    pub method_get_buffer_ret: ReturnType,
    pub method_get_current_block_length: JMethodID,
    pub method_get_current_block_length_ret: ReturnType,
}

impl<'a> CometShuffleBlockIterator<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometShuffleBlockIterator";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometShuffleBlockIterator<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CometShuffleBlockIterator {
            class,
            method_has_next: env.get_method_id(Self::JVM_CLASS, "hasNext", "()I")?,
            method_has_next_ret: ReturnType::Primitive(Primitive::Int),
            method_get_buffer: env.get_method_id(
                Self::JVM_CLASS,
                "getBuffer",
                "()Ljava/nio/ByteBuffer;",
            )?,
            method_get_buffer_ret: ReturnType::Object,
            method_get_current_block_length: env.get_method_id(
                Self::JVM_CLASS,
                "getCurrentBlockLength",
                "()I",
            )?,
            method_get_current_block_length_ret: ReturnType::Primitive(Primitive::Int),
        })
    }
}
