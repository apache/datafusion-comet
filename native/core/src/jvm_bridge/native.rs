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
    JNIEnv,
};

/// A struct that holds all the JNI methods and fields for JVM Native object.
pub struct Native<'a> {
    pub class: JClass<'a>,
    pub method_get_iceberg_partition_tasks_internal: JStaticMethodID,
}

impl<'a> Native<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/NativeJNIBridge";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<Native<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        let method =
            env.get_static_method_id(Self::JVM_CLASS, "getIcebergPartitionTasksInternal", "()[B")?;

        Ok(Native {
            method_get_iceberg_partition_tasks_internal: method,
            class,
        })
    }
}
