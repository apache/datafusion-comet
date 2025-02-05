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

/// A struct that holds all the JNI methods and fields for JVM CometMetricNode class.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct CometMetricNode<'a> {
    pub class: JClass<'a>,
    pub method_get_child_node: JMethodID,
    pub method_get_child_node_ret: ReturnType,
    pub method_set: JMethodID,
    pub method_set_ret: ReturnType,
    pub method_set_all_from_bytes: JMethodID,
    pub method_set_all_from_bytes_ret: ReturnType,
}

impl<'a> CometMetricNode<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/sql/comet/CometMetricNode";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometMetricNode<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CometMetricNode {
            method_get_child_node: env.get_method_id(
                Self::JVM_CLASS,
                "getChildNode",
                format!("(I)L{:};", Self::JVM_CLASS).as_str(),
            )?,
            method_get_child_node_ret: ReturnType::Object,
            method_set: env.get_method_id(Self::JVM_CLASS, "set", "(Ljava/lang/String;J)V")?,
            method_set_ret: ReturnType::Primitive(Primitive::Void),
            method_set_all_from_bytes: env.get_method_id(
                Self::JVM_CLASS,
                "set_all_from_bytes",
                "([B)V",
            )?,
            method_set_all_from_bytes_ret: ReturnType::Primitive(Primitive::Void),
            class,
        })
    }
}
