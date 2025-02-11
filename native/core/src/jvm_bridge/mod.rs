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

//! JNI JVM related functions

use crate::errors::CometResult;

use jni::objects::JClass;
use jni::{
    errors::Error,
    objects::{JMethodID, JObject, JString, JThrowable, JValueGen, JValueOwned},
    signature::ReturnType,
    AttachGuard, JNIEnv,
};
use once_cell::sync::OnceCell;

/// Macro for converting JNI Error to Comet Error.
#[macro_export]
macro_rules! jni_map_error {
    ($env:expr, $result:expr) => {{
        match $result {
            Ok(result) => datafusion::error::Result::Ok(result),
            Err(jni_error) => Err($crate::errors::CometError::JNI { source: jni_error }),
        }
    }};
}

/// Macro for converting Rust types to JNI types.
macro_rules! jvalues {
    ($($args:expr,)* $(,)?) => {{
        &[$(jni::objects::JValue::from($args).as_jni()),*] as &[jni::sys::jvalue]
    }}
}

/// Macro for calling a JNI method.
/// The syntax is:
/// jni_call!(env, comet_metric_node(metric_node).add(jname, value) -> ())?;
/// comet_metric_node is the class name stored in [[JVMClasses]].
/// metric_node is the Java object on which the method is called.
/// add is the method name.
/// jname and value are the arguments.
macro_rules! jni_call {
    ($env:expr, $clsname:ident($obj:expr).$method:ident($($args:expr),* $(,)?) -> $ret:ty) => {{
        let method_id = paste::paste! {
            $crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method>]
        };
        let ret_type = paste::paste! {
            $crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method _ret>]
        }.clone();
        let args = $crate::jvm_bridge::jvalues!($($args,)*);

        // Call the JVM method and obtain the returned value
        let ret = $env.call_method_unchecked($obj, method_id, ret_type, args);

        // Check if JVM has thrown any exception, and handle it if so.
        let result = if let Some(exception) = $crate::jvm_bridge::check_exception($env)? {
            Err(exception.into())
        } else {
            $crate::jvm_bridge::jni_map_error!($env, ret)
        };

        result.and_then(|result| $crate::jvm_bridge::jni_map_error!($env, <$ret>::try_from(result)))
    }}
}

macro_rules! jni_static_call {
    ($env:expr, $clsname:ident.$method:ident($($args:expr),* $(,)?) -> $ret:ty) => {{
        let clazz = &paste::paste! {
            $crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<class>]
        };
        let method_id = paste::paste! {
            $crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method>]
        };
        let ret_type = paste::paste! {
            $crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method _ret>]
        }.clone();
        let args = $crate::jvm_bridge::jvalues!($($args,)*);

        // Call the JVM static method and obtain the returned value
        let ret = $env.call_static_method_unchecked(clazz, method_id, ret_type, args);

        // Check if JVM has thrown any exception, and handle it if so.
        let result = if let Some(exception) = $crate::jvm_bridge::check_exception($env)? {
            Err(exception.into())
        } else {
            $crate::jvm_bridge::jni_map_error!($env, ret)
        };

        result.and_then(|result| $crate::jvm_bridge::jni_map_error!($env, <$ret>::try_from(result)))
    }}
}

/// Wrapper for JString. Because we cannot implement `TryFrom` trait for `JString` as they
/// are defined in different crates.
pub struct StringWrapper<'a> {
    value: JString<'a>,
}

impl<'a> StringWrapper<'a> {
    pub fn new(value: JString<'a>) -> StringWrapper<'a> {
        Self { value }
    }

    pub fn get(&self) -> &JString {
        &self.value
    }
}

pub struct BinaryWrapper<'a> {
    value: JObject<'a>,
}

impl<'a> BinaryWrapper<'a> {
    pub fn new(value: JObject<'a>) -> BinaryWrapper<'a> {
        Self { value }
    }

    pub fn get(&self) -> &JObject {
        &self.value
    }
}

impl<'a> TryFrom<JValueOwned<'a>> for StringWrapper<'a> {
    type Error = Error;

    fn try_from(value: JValueOwned<'a>) -> Result<StringWrapper<'a>, Error> {
        match value {
            JValueGen::Object(b) => Ok(StringWrapper::new(JString::from(b))),
            _ => Err(Error::WrongJValueType("object", value.type_name())),
        }
    }
}

impl<'a> TryFrom<JValueOwned<'a>> for BinaryWrapper<'a> {
    type Error = Error;

    fn try_from(value: JValueOwned<'a>) -> Result<BinaryWrapper<'a>, Error> {
        match value {
            JValueGen::Object(b) => Ok(BinaryWrapper::new(b)),
            _ => Err(Error::WrongJValueType("object", value.type_name())),
        }
    }
}

/// Macro for creating a new global reference.
macro_rules! jni_new_global_ref {
    ($env:expr, $obj:expr) => {{
        $crate::jni_map_error!($env, $env.new_global_ref($obj))
    }};
}

pub(crate) use jni_call;
pub(crate) use jni_map_error;
pub(crate) use jni_new_global_ref;
pub(crate) use jni_static_call;
pub(crate) use jvalues;

mod comet_exec;
pub use comet_exec::*;
mod batch_iterator;
mod comet_metric_node;
mod comet_task_memory_manager;

use crate::{errors::CometError, JAVA_VM};
use batch_iterator::CometBatchIterator;
pub use comet_metric_node::*;
pub use comet_task_memory_manager::*;

/// The JVM classes that are used in the JNI calls.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct JVMClasses<'a> {
    /// Cached JClass for "java.lang.Object"
    java_lang_object: JClass<'a>,
    /// Cached JClass for "java.lang.Class"
    java_lang_class: JClass<'a>,
    /// Cached JClass for "java.lang.Throwable"
    java_lang_throwable: JClass<'a>,
    /// Cached method ID for "java.lang.Object#getClass"
    pub object_get_class_method: JMethodID,
    /// Cached method ID for "java.lang.Class#getName"
    pub class_get_name_method: JMethodID,
    /// Cached method ID for "java.lang.Throwable#getMessage"
    pub throwable_get_message_method: JMethodID,
    /// Cached method ID for "java.lang.Throwable#getCause"
    pub throwable_get_cause_method: JMethodID,

    /// The CometMetricNode class. Used for updating the metrics.
    pub comet_metric_node: CometMetricNode<'a>,
    /// The static CometExec class. Used for getting the subquery result.
    pub comet_exec: CometExec<'a>,
    /// The CometBatchIterator class. Used for iterating over the batches.
    pub comet_batch_iterator: CometBatchIterator<'a>,
    /// The CometTaskMemoryManager used for interacting with JVM side to
    /// acquire & release native memory.
    pub comet_task_memory_manager: CometTaskMemoryManager<'a>,
}

unsafe impl<'a> Send for JVMClasses<'a> {}

unsafe impl<'a> Sync for JVMClasses<'a> {}

/// Keeps global references to JVM classes. Used for JNI calls to JVM.
static JVM_CLASSES: OnceCell<JVMClasses> = OnceCell::new();

impl JVMClasses<'_> {
    /// Creates a new JVMClasses struct.
    pub fn init(env: &mut JNIEnv) {
        JVM_CLASSES.get_or_init(|| {
            // A hack to make the `JNIEnv` static. It is not safe but we don't really use the
            // `JNIEnv` except for creating the global references of the classes.
            let env = unsafe { std::mem::transmute::<&mut JNIEnv, &'static mut JNIEnv>(env) };

            let java_lang_object = env.find_class("java/lang/Object").unwrap();
            let object_get_class_method = env
                .get_method_id(&java_lang_object, "getClass", "()Ljava/lang/Class;")
                .unwrap();

            let java_lang_class = env.find_class("java/lang/Class").unwrap();
            let class_get_name_method = env
                .get_method_id(&java_lang_class, "getName", "()Ljava/lang/String;")
                .unwrap();

            let java_lang_throwable = env.find_class("java/lang/Throwable").unwrap();
            let throwable_get_message_method = env
                .get_method_id(&java_lang_throwable, "getMessage", "()Ljava/lang/String;")
                .unwrap();

            let throwable_get_cause_method = env
                .get_method_id(&java_lang_throwable, "getCause", "()Ljava/lang/Throwable;")
                .unwrap();

            // SAFETY: According to the documentation for `JMethodID`, it is our
            // responsibility to maintain a reference to the `JClass` instances where the
            // methods were accessed from to prevent the methods from being garbage-collected
            JVMClasses {
                java_lang_object,
                java_lang_class,
                java_lang_throwable,
                object_get_class_method,
                class_get_name_method,
                throwable_get_message_method,
                throwable_get_cause_method,
                comet_metric_node: CometMetricNode::new(env).unwrap(),
                comet_exec: CometExec::new(env).unwrap(),
                comet_batch_iterator: CometBatchIterator::new(env).unwrap(),
                comet_task_memory_manager: CometTaskMemoryManager::new(env).unwrap(),
            }
        });
    }

    pub fn get() -> &'static JVMClasses<'static> {
        unsafe { JVM_CLASSES.get_unchecked() }
    }

    /// Gets the JNIEnv for the current thread.
    pub fn get_env() -> CometResult<AttachGuard<'static>> {
        unsafe {
            let java_vm = JAVA_VM.get_unchecked();
            java_vm.attach_current_thread().map_err(|e| {
                CometError::Internal(format!(
                    "JVMClasses::get_env() failed to attach current thread: {}",
                    e
                ))
            })
        }
    }
}

pub(crate) fn check_exception(env: &mut JNIEnv) -> CometResult<Option<CometError>> {
    let result = if env.exception_check()? {
        let exception = env.exception_occurred()?;
        env.exception_clear()?;
        let exception_err = convert_exception(env, &exception)?;
        Some(exception_err)
    } else {
        None
    };

    Ok(result)
}

/// get the class name of the exception by:
///  1. get the `Class` object of the input `throwable` via `Object#getClass` method
///  2. get the exception class name via calling `Class#getName` on the above object
fn get_throwable_class_name(
    env: &mut JNIEnv,
    jvm_classes: &JVMClasses,
    throwable: &JThrowable,
) -> CometResult<String> {
    unsafe {
        let class_obj = env
            .call_method_unchecked(
                throwable,
                jvm_classes.object_get_class_method,
                ReturnType::Object,
                &[],
            )?
            .l()?;
        let class_name = env
            .call_method_unchecked(
                class_obj,
                jvm_classes.class_get_name_method,
                ReturnType::Object,
                &[],
            )?
            .l()?
            .into();
        let class_name_str = env.get_string(&class_name)?.into();

        Ok(class_name_str)
    }
}

/// Get the exception message via calling `Throwable#getMessage` on the throwable object
fn get_throwable_message(
    env: &mut JNIEnv,
    jvm_classes: &JVMClasses,
    throwable: &JThrowable,
) -> CometResult<String> {
    unsafe {
        let message: JString = env
            .call_method_unchecked(
                throwable,
                jvm_classes.throwable_get_message_method,
                ReturnType::Object,
                &[],
            )?
            .l()?
            .into();
        let message_str = if !message.is_null() {
            env.get_string(&message)?.into()
        } else {
            String::from("null")
        };

        let cause: JThrowable = env
            .call_method_unchecked(
                throwable,
                jvm_classes.throwable_get_cause_method,
                ReturnType::Object,
                &[],
            )?
            .l()?
            .into();

        if !cause.is_null() {
            let cause_class_name = get_throwable_class_name(env, jvm_classes, &cause)?;
            let cause_message = get_throwable_message(env, jvm_classes, &cause)?;
            Ok(format!(
                "{}\nCaused by: {}: {}",
                message_str, cause_class_name, cause_message
            ))
        } else {
            Ok(message_str)
        }
    }
}

/// Given a `JThrowable` which is thrown from calling a Java method on the native side,
/// this converts it into a `CometError::JavaException` with the exception class name
/// and exception message. This error can then be populated to the JVM side to let
/// users know the cause of the native side error.
pub(crate) fn convert_exception(
    env: &mut JNIEnv,
    throwable: &JThrowable,
) -> CometResult<CometError> {
    let cache = JVMClasses::get();
    let exception_class_name_str = get_throwable_class_name(env, cache, throwable)?;
    let message_str = get_throwable_message(env, cache, throwable)?;

    Ok(CometError::JavaException {
        class: exception_class_name_str,
        msg: message_str,
        throwable: env.new_global_ref(throwable)?,
    })
}
