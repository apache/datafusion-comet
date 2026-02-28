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
    pub method_get_metric_names: JMethodID,
    pub method_get_metric_names_ret: ReturnType,
    pub method_get_node_offsets: JMethodID,
    pub method_get_node_offsets_ret: ReturnType,
    pub method_get_values_array: JMethodID,
    pub method_get_values_array_ret: ReturnType,
    pub method_update_from_values: JMethodID,
    pub method_update_from_values_ret: ReturnType,
}

impl<'a> CometMetricNode<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/sql/comet/CometMetricNode";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometMetricNode<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CometMetricNode {
            method_get_metric_names: env.get_method_id(
                Self::JVM_CLASS,
                "getMetricNames",
                "()[Ljava/lang/String;",
            )?,
            method_get_metric_names_ret: ReturnType::Object,
            method_get_node_offsets: env.get_method_id(
                Self::JVM_CLASS,
                "getNodeOffsets",
                "()[I",
            )?,
            method_get_node_offsets_ret: ReturnType::Object,
            method_get_values_array: env.get_method_id(
                Self::JVM_CLASS,
                "getValuesArray",
                "()[J",
            )?,
            method_get_values_array_ret: ReturnType::Object,
            method_update_from_values: env.get_method_id(
                Self::JVM_CLASS,
                "updateFromValues",
                "()V",
            )?,
            method_update_from_values_ret: ReturnType::Primitive(Primitive::Void),
            class,
        })
    }
}
