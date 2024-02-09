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
    signature::{JavaType, Primitive},
    JNIEnv,
};

use super::get_global_jclass;

/// A struct that holds all the JNI methods and fields for JVM CometMetricNode class.
pub struct CometMetricNode<'a> {
    pub class: JClass<'a>,
    pub method_get_child_node: JMethodID<'a>,
    pub method_get_child_node_ret: JavaType,
    pub method_add: JMethodID<'a>,
    pub method_add_ret: JavaType,
}

impl<'a> CometMetricNode<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/sql/comet/CometMetricNode";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<CometMetricNode<'a>> {
        // Get the global class reference
        let class = get_global_jclass(env, Self::JVM_CLASS)?;

        Ok(CometMetricNode {
            class,
            method_get_child_node: env
                .get_method_id(
                    class,
                    "getChildNode",
                    format!("(I)L{:};", Self::JVM_CLASS).as_str(),
                )
                .unwrap(),
            method_get_child_node_ret: JavaType::Object(Self::JVM_CLASS.to_owned()),
            method_add: env
                .get_method_id(class, "add", "(Ljava/lang/String;J)V")
                .unwrap(),
            method_add_ret: JavaType::Primitive(Primitive::Void),
        })
    }
}
