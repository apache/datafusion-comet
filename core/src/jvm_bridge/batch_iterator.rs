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

use super::get_global_jclass;
use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::ReturnType,
    JNIEnv,
};

/// A struct that holds all the JNI methods and fields for JVM `CometBatchIterator` class.
pub struct CometBatchIterator<'a> {
    pub class: JClass<'a>,
    pub method_next: JMethodID,
    pub method_next_ret: ReturnType,
}

impl<'a> CometBatchIterator<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/CometBatchIterator";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometBatchIterator<'a>> {
        // Get the global class reference
        let class = get_global_jclass(env, Self::JVM_CLASS)?;

        Ok(CometBatchIterator {
            class,
            method_next: env.get_method_id(Self::JVM_CLASS, "next", "()[J").unwrap(),
            method_next_ret: ReturnType::Array,
        })
    }
}
