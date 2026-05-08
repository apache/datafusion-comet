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

/// JNI handle for the JVM `org.apache.comet.udf.CometUdfBridge` class.
/// Mirrors the static-method pattern in `comet_exec.rs` (`CometScalarSubquery`).
#[allow(dead_code)] // class field is held to keep JStaticMethodID alive
pub struct CometUdfBridge<'a> {
    pub class: JClass<'a>,
    pub method_evaluate: JStaticMethodID,
    pub method_evaluate_ret: ReturnType,
}

impl<'a> CometUdfBridge<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/udf/CometUdfBridge";

    pub fn new(env: &mut Env<'a>) -> JniResult<CometUdfBridge<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;
        Ok(CometUdfBridge {
            method_evaluate: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("evaluate"),
                jni::jni_sig!("(Ljava/lang/String;[J[JJJ)V"),
            )?,
            method_evaluate_ret: ReturnType::Primitive(Primitive::Void),
            class,
        })
    }
}
