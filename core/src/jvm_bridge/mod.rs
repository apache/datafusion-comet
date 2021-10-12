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

use jni::{
    errors::{Error, Result as JniResult},
    objects::{JClass, JObject, JString, JValue},
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
        &[$(jni::objects::JValue::from($args)),*] as &[jni::objects::JValue]
    }}
}

/// Macro for create a new JNI string.
macro_rules! jni_new_string {
    ($env:expr, $value:expr) => {{
        $crate::jvm_bridge::jni_map_error!($env, $env.new_string($value))
    }};
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
        $crate::jvm_bridge::jni_map_error!(
            $env,
            $env.call_method_unchecked(
                $obj,
                paste::paste! {$crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method>]},
                paste::paste! {$crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method _ret>]}.clone(),
                $crate::jvm_bridge::jvalues!($($args,)*)
            )
        ).and_then(|result| $crate::jvm_bridge::jni_map_error!($env, <$ret>::try_from(result)))
    }}
}

macro_rules! jni_static_call {
    ($env:expr, $clsname:ident.$method:ident($($args:expr),* $(,)?) -> $ret:ty) => {{
        $crate::jvm_bridge::jni_map_error!(
            $env,
            $env.call_static_method_unchecked(
                paste::paste! {$crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<class>]},
                paste::paste! {$crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method>]},
                paste::paste! {$crate::jvm_bridge::JVMClasses::get().[<$clsname>].[<method_ $method _ret>]}.clone(),
                $crate::jvm_bridge::jvalues!($($args,)*)
            )
        ).and_then(|result| $crate::jvm_bridge::jni_map_error!($env, <$ret>::try_from(result)))
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

impl<'a> TryFrom<JValue<'a>> for StringWrapper<'a> {
    type Error = Error;

    fn try_from(value: JValue<'a>) -> Result<StringWrapper<'a>, Error> {
        match value {
            JValue::Object(b) => Ok(StringWrapper::new(JString::from(b))),
            _ => Err(Error::WrongJValueType("object", value.type_name())),
        }
    }
}

impl<'a> TryFrom<JValue<'a>> for BinaryWrapper<'a> {
    type Error = Error;

    fn try_from(value: JValue<'a>) -> Result<BinaryWrapper<'a>, Error> {
        match value {
            JValue::Object(b) => Ok(BinaryWrapper::new(b)),
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
pub(crate) use jni_new_string;
pub(crate) use jni_static_call;
pub(crate) use jvalues;

/// Gets a global reference to a Java class.
pub fn get_global_jclass(env: &JNIEnv<'_>, cls: &str) -> JniResult<JClass<'static>> {
    let local_jclass = env.find_class(cls)?;
    let global = env.new_global_ref::<JObject>(local_jclass.into())?;

    // A hack to make the `JObject` static. This is safe because the global reference is never
    // gc-ed by the JVM before dropping the global reference.
    let global_obj = unsafe { std::mem::transmute::<_, JObject<'static>>(global.as_obj()) };
    // Prevent the global reference from being dropped.
    let _ = std::mem::ManuallyDrop::new(global);

    Ok(JClass::from(global_obj))
}

mod comet_exec;
pub use comet_exec::*;
mod comet_metric_node;
use crate::JAVA_VM;
pub use comet_metric_node::*;

/// The JVM classes that are used in the JNI calls.
pub struct JVMClasses<'a> {
    /// The CometMetricNode class. Used for updating the metrics.
    pub comet_metric_node: CometMetricNode<'a>,
    /// The static CometExec class. Used for getting the subquery result.
    pub comet_exec: CometExec<'a>,
}

unsafe impl<'a> Send for JVMClasses<'a> {}
unsafe impl<'a> Sync for JVMClasses<'a> {}

/// Keeps global references to JVM classes. Used for JNI calls to JVM.
static JVM_CLASSES: OnceCell<JVMClasses> = OnceCell::new();

impl JVMClasses<'_> {
    /// Creates a new JVMClasses struct.
    pub fn init(env: &JNIEnv) {
        JVM_CLASSES.get_or_init(|| {
            // A hack to make the `JNIEnv` static. It is not safe but we don't really use the
            // `JNIEnv` except for creating the global references of the classes.
            let env = unsafe { std::mem::transmute::<_, &'static JNIEnv>(env) };

            JVMClasses {
                comet_metric_node: CometMetricNode::new(env).unwrap(),
                comet_exec: CometExec::new(env).unwrap(),
            }
        });
    }

    pub fn get() -> &'static JVMClasses<'static> {
        unsafe { JVM_CLASSES.get_unchecked() }
    }

    /// Gets the JNIEnv for the current thread.
    pub fn get_env() -> AttachGuard<'static> {
        unsafe {
            let java_vm = JAVA_VM.get_unchecked();
            java_vm.attach_current_thread().unwrap()
        }
    }
}
