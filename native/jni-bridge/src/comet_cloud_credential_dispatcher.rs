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
    objects::{JClass, JFieldID, JStaticMethodID},
    signature::{Primitive, ReturnType},
    strings::JNIString,
    Env,
};

/// JNI handles for the JVM `org.apache.comet.cloud.CometCloudCredentialDispatcher` SPI plus the
/// `CometCredentials` POJO whose fields the native bridge reads back. Cached at JVM-init time
/// so the underlying `JClass` references stay alive for the executor lifetime - acquiring them
/// inside a per-call local frame would let them be freed on frame pop.
pub struct CometCloudCredentialDispatcher<'a> {
    pub class: JClass<'a>,
    pub credentials_class: JClass<'a>,
    pub method_is_provider_registered: JStaticMethodID,
    pub method_is_provider_registered_ret: ReturnType,
    pub method_get_credentials_for_path: JStaticMethodID,
    pub method_get_credentials_for_path_ret: ReturnType,
    pub field_access_key_id: JFieldID,
    pub field_secret_access_key: JFieldID,
    pub field_session_token: JFieldID,
    pub field_expiration_epoch_millis: JFieldID,
}

impl<'a> CometCloudCredentialDispatcher<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/cloud/CometCloudCredentialDispatcher";
    pub const CREDENTIALS_CLASS: &'static str = "org/apache/comet/cloud/CometCredentials";

    pub fn new(env: &mut Env<'a>) -> JniResult<CometCloudCredentialDispatcher<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;
        let credentials_class = env.find_class(JNIString::new(Self::CREDENTIALS_CLASS))?;

        Ok(CometCloudCredentialDispatcher {
            method_is_provider_registered: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("isProviderRegistered"),
                jni::jni_sig!("()Z"),
            )?,
            method_is_provider_registered_ret: ReturnType::Primitive(Primitive::Boolean),
            method_get_credentials_for_path: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("getCredentialsForPath"),
                jni::jni_sig!(
                    "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/comet/cloud/CometCredentials;"
                ),
            )?,
            method_get_credentials_for_path_ret: ReturnType::Object,
            field_access_key_id: env.get_field_id(
                &credentials_class,
                jni::jni_str!("accessKeyId"),
                jni::jni_sig!("Ljava/lang/String;"),
            )?,
            field_secret_access_key: env.get_field_id(
                &credentials_class,
                jni::jni_str!("secretAccessKey"),
                jni::jni_sig!("Ljava/lang/String;"),
            )?,
            field_session_token: env.get_field_id(
                &credentials_class,
                jni::jni_str!("sessionToken"),
                jni::jni_sig!("Ljava/lang/String;"),
            )?,
            field_expiration_epoch_millis: env.get_field_id(
                &credentials_class,
                jni::jni_str!("expirationEpochMillis"),
                jni::jni_sig!("J"),
            )?,
            class,
            credentials_class,
        })
    }
}
