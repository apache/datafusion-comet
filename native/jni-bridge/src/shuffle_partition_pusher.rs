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

pub struct ShufflePartitionPusher<'a> {
    pub class: JClass<'a>,
    pub method_push_partition_data: JMethodID,
    pub method_push_partition_data_ret: ReturnType,
}

impl<'a> ShufflePartitionPusher<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/shuffle/ShufflePartitionPusher";

    pub fn new(env: &mut Env<'a>) -> JniResult<ShufflePartitionPusher<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;

        Ok(ShufflePartitionPusher {
            method_push_partition_data: env.get_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("pushPartitionData"),
                jni::jni_sig!("(I[BI)I"),
            )?,
            method_push_partition_data_ret: ReturnType::Primitive(Primitive::Int),
            class,
        })
    }
}
