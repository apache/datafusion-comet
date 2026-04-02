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

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use jni::objects::{GlobalRef, JObject, JValue};
use jni::JavaVM;
use std::io::Read;

/// Size of the internal read-ahead buffer (64 KB).
const READ_AHEAD_BUF_SIZE: usize = 64 * 1024;

/// A Rust `Read` implementation that pulls bytes from a JVM `java.io.InputStream`
/// via JNI callbacks, using an internal read-ahead buffer to minimize JNI crossings.
pub struct JniInputStream {
    /// Handle to the JVM for attaching threads.
    vm: JavaVM,
    /// Global reference to the JVM InputStream object.
    input_stream: GlobalRef,
    /// Global reference to the JVM byte[] used for bulk reads.
    jbuf: GlobalRef,
    /// Internal Rust-side buffer holding bytes read from JVM.
    buf: Vec<u8>,
    /// Current read position within `buf`.
    pos: usize,
    /// Number of valid bytes in `buf`.
    len: usize,
}

impl JniInputStream {
    /// Create a new `JniInputStream` wrapping a JVM InputStream.
    pub fn new(env: &mut jni::JNIEnv, input_stream: &JObject) -> jni::errors::Result<Self> {
        let vm = env.get_java_vm()?;
        let input_stream = env.new_global_ref(input_stream)?;
        let jbuf_local = env.new_byte_array(READ_AHEAD_BUF_SIZE as i32)?;
        let jbuf = env.new_global_ref(&jbuf_local)?;
        Ok(Self {
            vm,
            input_stream,
            jbuf,
            buf: vec![0u8; READ_AHEAD_BUF_SIZE],
            pos: 0,
            len: 0,
        })
    }

    /// Refill the internal buffer by calling `InputStream.read(byte[], 0, len)` via JNI.
    fn refill(&mut self) -> std::io::Result<usize> {
        let mut env = self
            .vm
            .attach_current_thread_as_daemon()
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Get a local reference from the global ref for the byte array
        let jbuf_local = env
            .new_local_ref(self.jbuf.as_obj())
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let n = env
            .call_method(
                &self.input_stream,
                "read",
                "([BII)I",
                &[
                    JValue::Object(&jbuf_local),
                    JValue::Int(0),
                    JValue::Int(READ_AHEAD_BUF_SIZE as i32),
                ],
            )
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .i()
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        if n <= 0 {
            // -1 means end of stream
            self.pos = 0;
            self.len = 0;
            return Ok(0);
        }

        let n = n as usize;

        // Copy bytes from JVM byte[] into our Rust buffer.
        // jbyte is i8; we read into a temporary i8 slice then reinterpret as u8.
        let mut i8_buf = vec![0i8; n];
        let jbuf_array = unsafe { jni::objects::JByteArray::from_raw(jbuf_local.as_raw()) };
        env.get_byte_array_region(&jbuf_array, 0, &mut i8_buf)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Don't let the JByteArray drop free the local ref — it was created from
        // a local ref that we don't own (it came from new_local_ref).
        // Actually, JByteArray::from_raw takes ownership conceptually, but the local
        // ref table manages it. We need to forget it so the underlying JObject local
        // ref doesn't get deleted twice. The new_local_ref created it, and from_raw
        // wrapped it. We should not drop jbuf_array since that would call
        // DeleteLocalRef on the same raw jobject that jbuf_local already points to.
        // However, JByteArray doesn't impl Drop with DeleteLocalRef — jni objects
        // are plain wrappers. So this is fine.

        let src = unsafe { std::slice::from_raw_parts(i8_buf.as_ptr() as *const u8, n) };
        self.buf[..n].copy_from_slice(src);
        self.pos = 0;
        self.len = n;

        Ok(n)
    }
}

impl Read for JniInputStream {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.len {
            // Buffer is empty, refill
            let filled = self.refill()?;
            if filled == 0 {
                return Ok(0); // EOF
            }
        }

        let available = self.len - self.pos;
        let to_copy = available.min(out.len());
        out[..to_copy].copy_from_slice(&self.buf[self.pos..self.pos + to_copy]);
        self.pos += to_copy;
        Ok(to_copy)
    }
}

/// Manages a `StreamReader<JniInputStream>` for reading Arrow IPC streams
/// from a JVM InputStream.
pub struct ShuffleStreamReader {
    reader: StreamReader<JniInputStream>,
}

impl ShuffleStreamReader {
    /// Create a new `ShuffleStreamReader` over a JVM InputStream.
    pub fn new(env: &mut jni::JNIEnv, input_stream: &JObject) -> Result<Self, String> {
        let jni_stream =
            JniInputStream::new(env, input_stream).map_err(|e| format!("JNI error: {e}"))?;
        let reader = unsafe {
            StreamReader::try_new(jni_stream, None)
                .map_err(|e| format!("Arrow IPC error: {e}"))?
                .with_skip_validation(true)
        };
        Ok(Self { reader })
    }

    /// Read the next batch from the stream. Returns `None` when exhausted.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        match self.reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(format!("Arrow IPC read error: {e}")),
            None => Ok(None),
        }
    }

    /// Return the number of fields in the stream's schema.
    pub fn num_fields(&self) -> usize {
        self.reader.schema().fields().len()
    }
}

/// Read a single RecordBatch from a compressed IPC block.
/// The first 4 bytes indicate the compression codec, followed by the IPC stream data.
/// This is the legacy read path used by `ShuffleScanExec`.
pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    match &bytes[0..4] {
        b"SNAP" => {
            let decoder = snap::read::FrameDecoder::new(&bytes[4..]);
            let mut reader =
                unsafe { StreamReader::try_new(decoder, None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        b"LZ4_" => {
            let decoder = lz4_flex::frame::FrameDecoder::new(&bytes[4..]);
            let mut reader =
                unsafe { StreamReader::try_new(decoder, None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        b"ZSTD" => {
            let decoder = zstd::Decoder::new(&bytes[4..])?;
            let mut reader =
                unsafe { StreamReader::try_new(decoder, None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        b"NONE" => {
            let mut reader =
                unsafe { StreamReader::try_new(&bytes[4..], None)?.with_skip_validation(true) };
            reader.next().unwrap().map_err(|e| e.into())
        }
        other => Err(DataFusionError::Execution(format!(
            "Failed to decode batch: invalid compression codec: {other:?}"
        ))),
    }
}
