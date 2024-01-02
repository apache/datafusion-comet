use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::{Primitive, ReturnType},
    JNIEnv,
};

use crate::jvm_bridge::get_global_jclass;

/// A DataFusion `MemoryPool` implementation for Comet, which delegate to the JVM
/// side `CometTaskMemoryManager`.
#[derive(Debug)]
pub struct CometTaskMemoryManager<'a> {
    pub class: JClass<'a>,
    pub method_acquire_memory: JMethodID,
    pub method_release_memory: JMethodID,

    pub method_acquire_memory_ret: ReturnType,
    pub method_release_memory_ret: ReturnType,
}

impl<'a> CometTaskMemoryManager<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/spark/CometTaskMemoryManager";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometTaskMemoryManager<'a>> {
        let class = get_global_jclass(env, Self::JVM_CLASS)?;

        let result = CometTaskMemoryManager {
            class,
            method_acquire_memory: env.get_method_id(
                Self::JVM_CLASS,
                "acquireMemory",
                "(J)J".to_string(),
            )?,
            method_release_memory: env.get_method_id(
                Self::JVM_CLASS,
                "releaseMemory",
                "(J)V".to_string(),
            )?,
            method_acquire_memory_ret: ReturnType::Primitive(Primitive::Long),
            method_release_memory_ret: ReturnType::Primitive(Primitive::Void),
        };
        Ok(result)
    }
}
