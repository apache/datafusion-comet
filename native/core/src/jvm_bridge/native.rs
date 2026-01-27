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
    JNIEnv,
};

/// A struct that holds all the JNI methods and fields for JVM Native object.
pub struct Native<'a> {
    pub class: JClass<'a>,
    pub method_get_iceberg_partition_tasks_internal: JStaticMethodID,
    pub method_get_iceberg_partition_tasks_internal_ret: ReturnType,
}

impl<'a> Native<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/NativeJNIBridge";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<Native<'a>> {
        eprintln!("→ Initializing Native JNI class...");
        eprintln!("  Looking up class: {}", Self::JVM_CLASS);

        let class = match env.find_class(Self::JVM_CLASS) {
            Ok(c) => {
                eprintln!("  ✓ Found class: {}", Self::JVM_CLASS);
                c
            }
            Err(e) => {
                eprintln!("  ✗ Failed to find class: {}", Self::JVM_CLASS);
                eprintln!("  Error: {:?}", e);
                return Err(e);
            }
        };

        eprintln!("  Looking up method: getIcebergPartitionTasksInternal with signature ()[B");
        let method = match env.get_static_method_id(
            Self::JVM_CLASS,
            "getIcebergPartitionTasksInternal",
            "()[B",
        ) {
            Ok(m) => {
                eprintln!("  ✓ Found method: getIcebergPartitionTasksInternal");
                m
            }
            Err(e) => {
                eprintln!("  ✗ Failed to find method: getIcebergPartitionTasksInternal");
                eprintln!("  Error: {:?}", e);
                return Err(e);
            }
        };

        eprintln!("✓ Native JNI class initialized successfully");
        Ok(Native {
            method_get_iceberg_partition_tasks_internal: method,
            method_get_iceberg_partition_tasks_internal_ret: ReturnType::Array,
            class,
        })
    }
}
