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
    JNIEnv,
};

use super::get_global_jclass;

/// A struct that holds all the JNI methods and fields for JVM CometExec object.
pub struct CometExec<'a> {
    pub class: JClass<'a>,
    pub method_get_bool: JStaticMethodID,
    pub method_get_bool_ret: ReturnType,
    pub method_get_byte: JStaticMethodID,
    pub method_get_byte_ret: ReturnType,
    pub method_get_short: JStaticMethodID,
    pub method_get_short_ret: ReturnType,
    pub method_get_int: JStaticMethodID,
    pub method_get_int_ret: ReturnType,
    pub method_get_long: JStaticMethodID,
    pub method_get_long_ret: ReturnType,
    pub method_get_float: JStaticMethodID,
    pub method_get_float_ret: ReturnType,
    pub method_get_double: JStaticMethodID,
    pub method_get_double_ret: ReturnType,
    pub method_get_decimal: JStaticMethodID,
    pub method_get_decimal_ret: ReturnType,
    pub method_get_string: JStaticMethodID,
    pub method_get_string_ret: ReturnType,
    pub method_get_binary: JStaticMethodID,
    pub method_get_binary_ret: ReturnType,
    pub method_is_null: JStaticMethodID,
    pub method_is_null_ret: ReturnType,
}

impl<'a> CometExec<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/sql/comet/CometScalarSubquery";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometExec<'a>> {
        // Get the global class reference
        let class = get_global_jclass(env, Self::JVM_CLASS)?;

        Ok(CometExec {
            method_get_bool: env
                .get_static_method_id(Self::JVM_CLASS, "getBoolean", "(JJ)Z")
                .unwrap(),
            method_get_bool_ret: ReturnType::Primitive(Primitive::Boolean),
            method_get_byte: env
                .get_static_method_id(Self::JVM_CLASS, "getByte", "(JJ)B")
                .unwrap(),
            method_get_byte_ret: ReturnType::Primitive(Primitive::Byte),
            method_get_short: env
                .get_static_method_id(Self::JVM_CLASS, "getShort", "(JJ)S")
                .unwrap(),
            method_get_short_ret: ReturnType::Primitive(Primitive::Short),
            method_get_int: env
                .get_static_method_id(Self::JVM_CLASS, "getInt", "(JJ)I")
                .unwrap(),
            method_get_int_ret: ReturnType::Primitive(Primitive::Int),
            method_get_long: env
                .get_static_method_id(Self::JVM_CLASS, "getLong", "(JJ)J")
                .unwrap(),
            method_get_long_ret: ReturnType::Primitive(Primitive::Long),
            method_get_float: env
                .get_static_method_id(Self::JVM_CLASS, "getFloat", "(JJ)F")
                .unwrap(),
            method_get_float_ret: ReturnType::Primitive(Primitive::Float),
            method_get_double: env
                .get_static_method_id(Self::JVM_CLASS, "getDouble", "(JJ)D")
                .unwrap(),
            method_get_double_ret: ReturnType::Primitive(Primitive::Double),
            method_get_decimal: env
                .get_static_method_id(Self::JVM_CLASS, "getDecimal", "(JJ)[B")
                .unwrap(),
            method_get_decimal_ret: ReturnType::Array,
            method_get_string: env
                .get_static_method_id(Self::JVM_CLASS, "getString", "(JJ)Ljava/lang/String;")
                .unwrap(),
            method_get_string_ret: ReturnType::Object,
            method_get_binary: env
                .get_static_method_id(Self::JVM_CLASS, "getBinary", "(JJ)[B")
                .unwrap(),
            method_get_binary_ret: ReturnType::Array,
            method_is_null: env
                .get_static_method_id(Self::JVM_CLASS, "isNull", "(JJ)Z")
                .unwrap(),
            method_is_null_ret: ReturnType::Primitive(Primitive::Boolean),
            class,
        })
    }
}
