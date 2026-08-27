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

use crate::{check_exception, errors::CometError, JVMClasses};
use datafusion::common::{DataFusionError, Result};
use jni::objects::{Global, JMethodID, JObject, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::Env;

/// OpenJDK's conservative soft array limit; actual JVM limits can vary.
const MAX_JVM_ARRAY_LENGTH: i32 = i32::MAX - 8;

/// Receives a complete encoded shuffle block for one output partition.
///
/// Implementations must remain safe when invoked from native execution
/// threads that do not inherit Spark's task-local JVM state.
pub trait ShufflePartitionPusher: Send + Sync {
    /// Sends one complete, length-prefixed Arrow IPC shuffle block.
    fn push_partition_data(&self, partition_id: i32, data: &[u8]) -> Result<()>;
}

/// Invokes a task-owned JVM shuffle callback from any native execution thread.
///
/// The global callback reference keeps both the Java object and its class alive
/// for the lifetime of the cached method ID. No thread-local JNI environment
/// or Spark task context is retained between invocations.
pub struct JavaShufflePartitionPusher {
    callback: Global<JObject<'static>>,
    push_method: JMethodID,
}

impl JavaShufflePartitionPusher {
    /// Captures the callback while running on an attached JVM thread.
    pub fn try_new(env: &mut Env<'_>, callback: &JObject<'_>) -> Result<Self> {
        if callback.is_null() {
            return Err(DataFusionError::Execution(
                "Remote shuffle callback must not be null".to_string(),
            ));
        }

        let callback_class = env.get_object_class(callback).map_err(CometError::from)?;
        let push_method = env
            .get_method_id(
                &callback_class,
                jni::jni_str!("pushPartitionData"),
                jni::jni_sig!("(I[BI)V"),
            )
            .map_err(CometError::from)?;
        let callback = env.new_global_ref(callback).map_err(CometError::from)?;

        Ok(Self {
            callback,
            push_method,
        })
    }

    fn checked_payload_length(partition_id: i32, payload_length: usize) -> Result<i32> {
        if partition_id < 0 {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition must be nonnegative, got {partition_id}"
            )));
        }

        match i32::try_from(payload_length) {
            Ok(length) if length <= MAX_JVM_ARRAY_LENGTH => Ok(length),
            _ => Err(DataFusionError::Execution(format!(
                "Remote shuffle payload size {payload_length} exceeds the JVM array limit of \
                 {MAX_JVM_ARRAY_LENGTH} bytes"
            ))),
        }
    }
}

impl ShufflePartitionPusher for JavaShufflePartitionPusher {
    fn push_partition_data(&self, partition_id: i32, data: &[u8]) -> Result<()> {
        let payload_length = Self::checked_payload_length(partition_id, data.len())?;

        JVMClasses::with_env(|env| {
            let payload = env.byte_array_from_slice(data).map_err(CometError::from)?;

            // SAFETY: `push_method` was resolved against the callback object's
            // class with this exact argument list and void return type. The
            // global object reference keeps its defining class alive.
            let result = unsafe {
                env.call_method_unchecked(
                    self.callback.as_obj(),
                    self.push_method,
                    ReturnType::Primitive(Primitive::Void),
                    &[
                        JValue::Int(partition_id).as_jni(),
                        JValue::Object(&payload).as_jni(),
                        JValue::Int(payload_length).as_jni(),
                    ],
                )
            };

            // Inspect the pending exception before consuming the JNI result so
            // its original throwable survives the DataFusion error boundary.
            if let Some(exception) = check_exception(env)? {
                return Err(exception.into());
            }

            result.map_err(CometError::from)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{JavaShufflePartitionPusher, MAX_JVM_ARRAY_LENGTH};

    #[test]
    fn accepts_payload_lengths_up_to_jvm_soft_array_limit() {
        assert_eq!(
            JavaShufflePartitionPusher::checked_payload_length(0, 0).unwrap(),
            0
        );
        assert_eq!(
            JavaShufflePartitionPusher::checked_payload_length(7, MAX_JVM_ARRAY_LENGTH as usize,)
                .unwrap(),
            MAX_JVM_ARRAY_LENGTH
        );
    }

    #[test]
    fn rejects_negative_partition_ids() {
        let error = JavaShufflePartitionPusher::checked_payload_length(-1, 1).unwrap_err();
        assert!(error.to_string().contains("partition must be nonnegative"));
    }

    #[test]
    fn rejects_payload_lengths_larger_than_jvm_soft_array_limit() {
        for oversized_length in [
            MAX_JVM_ARRAY_LENGTH as usize + 1,
            i32::MAX as usize,
            i32::MAX as usize + 1,
            usize::MAX,
        ] {
            let error = JavaShufflePartitionPusher::checked_payload_length(0, oversized_length)
                .unwrap_err();
            let message = error.to_string();
            assert!(message.contains("exceeds the JVM array limit"));
            assert!(message.contains(&MAX_JVM_ARRAY_LENGTH.to_string()));
        }
    }
}
