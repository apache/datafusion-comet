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

use arrow::array::{RecordBatch, RecordBatchOptions, StructArray};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use std::ffi::CStr;
use std::sync::Arc;

/// C Stream Interface reader that calls [`arrow::array::ArrayData::align_buffers`] on every
/// imported batch before constructing typed arrays. Stock `ArrowArrayStreamReader` panics
/// when a JVM producer hands us a `Decimal128` buffer at an offset that is 8-byte but not
/// 16-byte aligned, which Java's allocator does not guarantee. Track upstream:
/// <https://github.com/apache/arrow-rs/issues/10028>.
#[derive(Debug)]
pub struct AlignedArrowStreamReader {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
}

impl AlignedArrowStreamReader {
    /// # Safety
    /// `raw` must point at a valid `FFI_ArrowArrayStream` whose ownership is being transferred
    /// to this reader. The stream's release callback fires when the reader is dropped.
    pub unsafe fn from_raw(raw: *mut FFI_ArrowArrayStream) -> Result<Self, ArrowError> {
        let mut stream = FFI_ArrowArrayStream::from_raw(raw);
        if stream.release.is_none() {
            return Err(ArrowError::CDataInterface(
                "input stream is already released".to_string(),
            ));
        }
        let schema = read_schema(&mut stream)?;
        Ok(Self { stream, schema })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn last_error(&mut self) -> Option<String> {
        let get = self.stream.get_last_error?;
        let ptr = unsafe { get(&mut self.stream) };
        if ptr.is_null() {
            return None;
        }
        Some(
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

impl Iterator for AlignedArrowStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut array = FFI_ArrowArray::empty();
        let ret = unsafe { self.stream.get_next.unwrap()(&mut self.stream, &mut array) };
        if ret != 0 {
            let msg = self
                .last_error()
                .unwrap_or_else(|| format!("get_next returned {ret}"));
            return Some(Err(ArrowError::CDataInterface(msg)));
        }
        if array.is_released() {
            return None;
        }

        let dt = DataType::Struct(self.schema.fields().clone());
        Some(
            unsafe { from_ffi_and_data_type(array, dt) }.and_then(|mut data| {
                data.align_buffers();
                let len = data.len();
                RecordBatch::try_new_with_options(
                    Arc::clone(&self.schema),
                    StructArray::from(data).into_parts().1,
                    &RecordBatchOptions::new().with_row_count(Some(len)),
                )
            }),
        )
    }
}

fn read_schema(stream: &mut FFI_ArrowArrayStream) -> Result<SchemaRef, ArrowError> {
    let mut schema = FFI_ArrowSchema::empty();
    let ret = unsafe { stream.get_schema.unwrap()(stream, &mut schema) };
    if ret != 0 {
        return Err(ArrowError::CDataInterface(format!(
            "Cannot get schema from input stream. Error code: {ret}"
        )));
    }
    Ok(Arc::new(Schema::try_from(&schema)?))
}
