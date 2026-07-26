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
    signature::{Primitive, ReturnType},
    strings::JNIString,
    Env,
};

/// Cached handle for Spark's exact Julian-to-Gregorian timestamp fallback.
pub struct RebaseDateTime<'a> {
    pub class: JClass<'a>,
    pub method_rebase_julian_to_gregorian_micros: JStaticMethodID,
    pub method_rebase_julian_to_gregorian_micros_ret: ReturnType,
}

impl<'a> RebaseDateTime<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/sql/catalyst/util/RebaseDateTime";

    pub fn new(env: &mut Env<'a>) -> JniResult<Self> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;
        Ok(Self {
            method_rebase_julian_to_gregorian_micros: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("rebaseJulianToGregorianMicros"),
                jni::jni_sig!("(Ljava/lang/String;J)J"),
            )?,
            method_rebase_julian_to_gregorian_micros_ret: ReturnType::Primitive(Primitive::Long),
            class,
        })
    }
}
