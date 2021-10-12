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
    signature::{JavaType, Primitive},
    JNIEnv,
};

use super::get_global_jclass;

/// A struct that holds all the JNI methods and fields for JVM CometExec object.
pub struct CometExec<'a> {
    pub class: JClass<'a>,
    pub method_get_bool: JStaticMethodID<'a>,
    pub method_get_bool_ret: JavaType,
    pub method_get_byte: JStaticMethodID<'a>,
    pub method_get_byte_ret: JavaType,
    pub method_get_short: JStaticMethodID<'a>,
    pub method_get_short_ret: JavaType,
    pub method_get_int: JStaticMethodID<'a>,
    pub method_get_int_ret: JavaType,
    pub method_get_long: JStaticMethodID<'a>,
    pub method_get_long_ret: JavaType,
    pub method_get_float: JStaticMethodID<'a>,
    pub method_get_float_ret: JavaType,
    pub method_get_double: JStaticMethodID<'a>,
    pub method_get_double_ret: JavaType,
    pub method_get_decimal: JStaticMethodID<'a>,
    pub method_get_decimal_ret: JavaType,
    pub method_get_string: JStaticMethodID<'a>,
    pub method_get_string_ret: JavaType,
    pub method_get_binary: JStaticMethodID<'a>,
    pub method_get_binary_ret: JavaType,
    pub method_is_null: JStaticMethodID<'a>,
    pub method_is_null_ret: JavaType,
}

impl<'a> CometExec<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/sql/comet/CometScalarSubquery";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<CometExec<'a>> {
        // Get the global class reference
        let class = get_global_jclass(env, Self::JVM_CLASS)?;

        Ok(CometExec {
            class,
            method_get_bool: env
                .get_static_method_id(class, "getBoolean", "(JJ)Z")
                .unwrap(),
            method_get_bool_ret: JavaType::Primitive(Primitive::Boolean),
            method_get_byte: env.get_static_method_id(class, "getByte", "(JJ)B").unwrap(),
            method_get_byte_ret: JavaType::Primitive(Primitive::Byte),
            method_get_short: env
                .get_static_method_id(class, "getShort", "(JJ)S")
                .unwrap(),
            method_get_short_ret: JavaType::Primitive(Primitive::Short),
            method_get_int: env.get_static_method_id(class, "getInt", "(JJ)I").unwrap(),
            method_get_int_ret: JavaType::Primitive(Primitive::Int),
            method_get_long: env.get_static_method_id(class, "getLong", "(JJ)J").unwrap(),
            method_get_long_ret: JavaType::Primitive(Primitive::Long),
            method_get_float: env
                .get_static_method_id(class, "getFloat", "(JJ)F")
                .unwrap(),
            method_get_float_ret: JavaType::Primitive(Primitive::Float),
            method_get_double: env
                .get_static_method_id(class, "getDouble", "(JJ)D")
                .unwrap(),
            method_get_double_ret: JavaType::Primitive(Primitive::Double),
            method_get_decimal: env
                .get_static_method_id(class, "getDecimal", "(JJ)[B")
                .unwrap(),
            method_get_decimal_ret: JavaType::Array(Box::new(JavaType::Primitive(Primitive::Byte))),
            method_get_string: env
                .get_static_method_id(class, "getString", "(JJ)Ljava/lang/String;")
                .unwrap(),
            method_get_string_ret: JavaType::Object("java/lang/String".to_owned()),
            method_get_binary: env
                .get_static_method_id(class, "getBinary", "(JJ)[B")
                .unwrap(),
            method_get_binary_ret: JavaType::Array(Box::new(JavaType::Primitive(Primitive::Byte))),
            method_is_null: env.get_static_method_id(class, "isNull", "(JJ)Z").unwrap(),
            method_is_null_ret: JavaType::Primitive(Primitive::Boolean),
        })
    }
}
