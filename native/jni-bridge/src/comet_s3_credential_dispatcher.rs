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

/// JNI handles for the JVM `CometS3CredentialDispatcher` SPI plus the `CometS3Credentials` POJO
/// whose fields the native bridge reads back.
pub struct CometS3CredentialDispatcher<'a> {
    pub class: JClass<'a>,
    /// Retained so the cached POJO `JFieldID`s stay alive for the executor lifetime.
    #[allow(dead_code)]
    pub credentials_class: JClass<'a>,
    pub method_ensure_initialized: JStaticMethodID,
    pub method_ensure_initialized_ret: ReturnType,
    pub method_get_credentials_for_path: JStaticMethodID,
    pub method_get_credentials_for_path_ret: ReturnType,
    pub field_access_key_id: JFieldID,
    pub field_secret_access_key: JFieldID,
    pub field_session_token: JFieldID,
    pub field_expiration_epoch_millis: JFieldID,
}

impl<'a> CometS3CredentialDispatcher<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/cloud/s3/CometS3CredentialDispatcher";
    pub const CREDENTIALS_CLASS: &'static str = "org/apache/comet/cloud/s3/CometS3Credentials";

    pub fn new(env: &mut Env<'a>) -> JniResult<CometS3CredentialDispatcher<'a>> {
        let class = env.find_class(JNIString::new(Self::JVM_CLASS))?;
        let credentials_class = env.find_class(JNIString::new(Self::CREDENTIALS_CLASS))?;

        Ok(CometS3CredentialDispatcher {
            method_ensure_initialized: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("ensureInitialized"),
                jni::jni_sig!("(Ljava/lang/String;Ljava/lang/String;Ljava/util/Map;)J"),
            )?,
            method_ensure_initialized_ret: ReturnType::Primitive(Primitive::Long),
            method_get_credentials_for_path: env.get_static_method_id(
                JNIString::new(Self::JVM_CLASS),
                jni::jni_str!("getCredentialsForPath"),
                jni::jni_sig!(
                    "(JLjava/lang/String;Ljava/lang/String;I)Lorg/apache/comet/cloud/s3/CometS3Credentials;"
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
