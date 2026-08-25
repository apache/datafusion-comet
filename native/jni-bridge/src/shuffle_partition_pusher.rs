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

use std::sync::Arc;

use jni::{
    errors::Error as JniError,
    objects::{Global, JClass, JMethodID, JObject, JValue},
    signature::{Primitive, ReturnType},
    Env, JavaVM,
};

use crate::errors::{CometError, CometResult};

/// Task-owned JNI callback. It is resolved only when explicitly constructed, so older JVM
/// artifacts without the RSS interface can still execute existing local Comet plans.
#[derive(Clone, Debug)]
pub struct JavaShufflePartitionPusher {
    inner: Arc<Callback>,
    num_partitions: usize,
    max_push_bytes: usize,
}

#[derive(Debug)]
struct Callback {
    vm: JavaVM,
    object: Global<JObject<'static>>,
    // The class must stay alive for as long as the cached method ID.
    _class: Global<JClass<'static>>,
    push_partition_data: JMethodID,
    reserve_partition_data: JMethodID,
    release_partition_data_reservation: JMethodID,
}

impl JavaShufflePartitionPusher {
    pub const JVM_CLASS: &'static str = "org/apache/comet/shuffle/ShufflePartitionPusher";

    pub fn try_new(
        env: &mut Env,
        object: &JObject,
        num_partitions: usize,
        max_push_bytes: usize,
    ) -> CometResult<Self> {
        if object.is_null() {
            return Err(CometError::NullPointer(
                "ShufflePartitionPusher is null".into(),
            ));
        }
        if num_partitions == 0 || num_partitions > i32::MAX as usize {
            return Err(CometError::Config("Invalid RSS partition count".into()));
        }
        if max_push_bytes == 0 || max_push_bytes > i32::MAX as usize {
            return Err(CometError::Config("Invalid RSS push byte limit".into()));
        }

        let class = env.find_class(jni::strings::JNIString::new(Self::JVM_CLASS))?;
        if !env.is_instance_of(object, &class)? {
            return Err(CometError::Config(
                "Object does not implement ShufflePartitionPusher".into(),
            ));
        }
        let push_partition_data = env.get_method_id(
            &class,
            jni::jni_str!("pushPartitionData"),
            jni::jni_sig!("(I[BI)I"),
        )?;
        let reserve_partition_data = env.get_method_id(
            &class,
            jni::jni_str!("reservePartitionData"),
            jni::jni_sig!("(I)V"),
        )?;
        let release_partition_data_reservation = env.get_method_id(
            &class,
            jni::jni_str!("releasePartitionDataReservation"),
            jni::jni_sig!("()V"),
        )?;
        Ok(Self {
            inner: Arc::new(Callback {
                vm: env.get_java_vm()?,
                object: env.new_global_ref(object)?,
                _class: env.new_global_ref(&class)?,
                push_partition_data,
                reserve_partition_data,
                release_partition_data_reservation,
            }),
            num_partitions,
            max_push_bytes,
        })
    }

    /// Reserve JVM-owned admission before the caller allocates an encoded native frame.
    ///
    /// Arrow's uncompressed IPC scratch space can exceed the configured compressed-frame limit.
    /// The JVM callback validates this reservation against its executor-wide admission budget;
    /// only actual submitted frames are constrained by `max_push_bytes`.
    pub fn reserve_partition_data(&self, reservation_bytes: usize) -> CometResult<()> {
        if reservation_bytes == 0 {
            return Err(CometError::Config(
                "RSS reservation size must be positive".into(),
            ));
        }
        let reservation_bytes = i32::try_from(reservation_bytes)
            .map_err(|_| CometError::Config("RSS reservation size exceeds jint".into()))?;

        self.inner
            .vm
            .attach_current_thread(|env| -> jni::errors::Result<()> {
                let args = [JValue::Int(reservation_bytes).as_jni()];
                // SAFETY: try_new cached the exact interface method and retains its class.
                unsafe {
                    env.call_method_unchecked(
                        &self.inner.object,
                        self.inner.reserve_partition_data,
                        ReturnType::Primitive(Primitive::Void),
                        &args,
                    )?;
                }
                Ok(())
            })
            .map_err(Self::callback_error)
    }

    /// Return a reservation whose encoded frame was never submitted to the Java callback.
    pub fn release_partition_data_reservation(&self) -> CometResult<()> {
        self.inner
            .vm
            .attach_current_thread(|env| -> jni::errors::Result<()> {
                // SAFETY: try_new cached the exact interface method and retains its class.
                unsafe {
                    env.call_method_unchecked(
                        &self.inner.object,
                        self.inner.release_partition_data_reservation,
                        ReturnType::Primitive(Primitive::Void),
                        &[],
                    )?;
                }
                Ok(())
            })
            .map_err(Self::callback_error)
    }

    /// Copies a caller-validated complete frame into Java-owned memory.
    ///
    /// The configured bound is checked before allocating the Java array. This only bounds this
    /// synchronous copy; the backend remains responsible for asynchronous admission and commit.
    pub fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> CometResult<()> {
        if partition_id >= self.num_partitions {
            return Err(CometError::Config(format!(
                "RSS partition {partition_id} is outside 0..{}",
                self.num_partitions
            )));
        }
        if frame.is_empty() || frame.len() > self.max_push_bytes {
            return Err(CometError::Config(format!(
                "RSS push size {} is outside 1..={}",
                frame.len(),
                self.max_push_bytes
            )));
        }
        let partition_id = i32::try_from(partition_id)
            .map_err(|_| CometError::Config("RSS partition ID exceeds jint".into()))?;
        let length = i32::try_from(frame.len())
            .map_err(|_| CometError::Config("RSS push size exceeds jint".into()))?;

        // attach_current_thread creates a local frame and captures/clears Java exceptions. Do not
        // stringify the captured throwable: the outer Comet JNI boundary can rethrow it unchanged.
        let accepted = self
            .inner
            .vm
            .attach_current_thread(|env| -> jni::errors::Result<i32> {
                let bytes = env.byte_array_from_slice(frame)?;
                let args = [
                    JValue::Int(partition_id).as_jni(),
                    JValue::Object(&bytes).as_jni(),
                    JValue::Int(length).as_jni(),
                ];
                // SAFETY: try_new checked the interface and cached its exact (I[BI)I method.
                // The object and declaring class are held by global references.
                unsafe {
                    env.call_method_unchecked(
                        &self.inner.object,
                        self.inner.push_partition_data,
                        ReturnType::Primitive(Primitive::Int),
                        &args,
                    )?
                    .i()
                }
            })
            .map_err(Self::callback_error)?;

        if accepted != length {
            return Err(CometError::Internal(format!(
                "RSS callback accepted {accepted} bytes; expected {length}"
            )));
        }
        Ok(())
    }

    fn callback_error(source: JniError) -> CometError {
        match source {
            JniError::CaughtJavaException {
                exception,
                name,
                msg,
                ..
            } => CometError::JavaException {
                class: name,
                msg,
                throwable: exception,
            },
            source => CometError::JNI { source },
        }
    }
}
