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

use std::fmt::{self, Debug, Formatter};

use crate::{check_exception, errors::CometError, JVMClasses};
use datafusion::common::{DataFusionError, Result};
use jni::objects::{Global, JByteArray, JMethodID, JObject, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::Env;

/// Receives the bytes of a file that the JVM created, in order.
///
/// The JVM creates the file, closes it once the native writer has written all of it, and aborts
/// it when the write fails, so an implementation only ever sees the file's bytes. One writer
/// writes one file, so calls never overlap.
pub trait OutputStreamSink: Send + Sync {
    /// The most bytes one call to [`Self::write`] may carry.
    fn max_write_size(&self) -> usize;

    /// Appends `data`, at most [`Self::max_write_size`] bytes, to the file.
    fn write(&self, data: &[u8]) -> Result<()>;
}

impl Debug for dyn OutputStreamSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputStreamSink")
            .field("max_write_size", &self.max_write_size())
            .finish()
    }
}

/// Writes to a JVM `org.apache.comet.parquet.HadoopFileOutput` from any native thread.
///
/// Each write copies its bytes into a Java array this sink allocates once, then hands the array
/// to `write(byte[], int)`, so a write costs one copy and one upcall but no Java allocation. The
/// global references keep the Java object, its class and the array alive for as long as the
/// cached method ID is used.
pub struct JavaOutputStreamSink {
    output: Global<JObject<'static>>,
    buffer: Global<JByteArray<'static>>,
    write_method: JMethodID,
}

impl JavaOutputStreamSink {
    /// Bytes per upcall, large enough that the upcall costs little next to the copy it carries.
    pub const MAX_WRITE_SIZE: usize = 1024 * 1024;

    /// Captures `output` while running on an attached JVM thread.
    pub fn try_new(env: &mut Env<'_>, output: &JObject<'_>) -> Result<Self> {
        if output.is_null() {
            return Err(DataFusionError::Execution(
                "The output stream of a native write must not be null".to_string(),
            ));
        }

        let output_class = env.get_object_class(output).map_err(CometError::from)?;
        let write_method = env
            .get_method_id(
                &output_class,
                jni::jni_str!("write"),
                jni::jni_sig!("([BI)V"),
            )
            .map_err(CometError::from)?;
        let buffer = env
            .new_byte_array(Self::MAX_WRITE_SIZE)
            .map_err(CometError::from)?;

        Ok(Self {
            output: env.new_global_ref(output).map_err(CometError::from)?,
            buffer: env.new_global_ref(&buffer).map_err(CometError::from)?,
            write_method,
        })
    }
}

impl OutputStreamSink for JavaOutputStreamSink {
    fn max_write_size(&self) -> usize {
        Self::MAX_WRITE_SIZE
    }

    fn write(&self, data: &[u8]) -> Result<()> {
        if data.len() > Self::MAX_WRITE_SIZE {
            return Err(DataFusionError::Internal(format!(
                "A write of {} bytes exceeds the output stream's limit of {} bytes",
                data.len(),
                Self::MAX_WRITE_SIZE
            )));
        }
        // Lossless: the length is bounded by MAX_WRITE_SIZE.
        let length = data.len() as i32;

        JVMClasses::with_env(|env| {
            // SAFETY: `i8` and `u8` have the same size and alignment, and every bit pattern is
            // valid for both.
            let bytes =
                unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<i8>(), data.len()) };
            self.buffer
                .set_region(env, 0, bytes)
                .map_err(CometError::from)?;

            // SAFETY: `write_method` was resolved against the output object's class with this
            // exact argument list and void return type. The global object reference keeps its
            // defining class alive.
            let result = unsafe {
                env.call_method_unchecked(
                    self.output.as_obj(),
                    self.write_method,
                    ReturnType::Primitive(Primitive::Void),
                    &[
                        JValue::Object(self.buffer.as_obj()).as_jni(),
                        JValue::Int(length).as_jni(),
                    ],
                )
            };

            // Inspect the pending exception before consuming the JNI result, so that its
            // throwable survives the DataFusion error boundary.
            if let Some(exception) = check_exception(env)? {
                return Err(exception.into());
            }

            result.map_err(CometError::from)?;
            Ok(())
        })
    }
}
