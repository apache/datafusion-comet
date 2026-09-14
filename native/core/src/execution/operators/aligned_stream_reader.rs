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
/// 16-byte aligned, which Java's allocator does not guarantee (apache/arrow-rs#10028).
/// The fix (apache/arrow-rs#10030) makes `from_ffi_and_data_type` align internally and ships
/// in arrow 59.0.0; once Comet is on arrow >= 59 this reader can be dropped for the stock
/// `ArrowArrayStreamReader`.
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

        Some(batch_from_ffi(array, &self.schema))
    }
}

/// Import one C Data Interface array as a `RecordBatch` under `schema`, re-aligning its buffers
/// first. The realignment is what distinguishes this from the stock reader: arrow 58's
/// `from_ffi_and_data_type` returns the producer's buffers untouched (via `new_unchecked`), so a
/// JVM producer's 8-byte-aligned `Decimal128` buffer reaches us under-aligned and would panic in
/// `ScalarBuffer::<i128>::from` once `StructArray::from` materializes the typed column. See
/// apache/arrow-rs#10028; the arrow 59 fix (apache/arrow-rs#10030) folds this `align_buffers` call
/// into `from_ffi_and_data_type` itself.
fn batch_from_ffi(array: FFI_ArrowArray, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let dt = DataType::Struct(schema.fields().clone());
    // SAFETY: the caller transfers ownership of a valid FFI array whose layout matches `schema`.
    let mut data = unsafe { from_ffi_and_data_type(array, dt) }?;
    data.align_buffers();
    let len = data.len();
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        StructArray::from(data).into_parts().1,
        &RecordBatchOptions::new().with_row_count(Some(len)),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayData, Decimal128Array};
    use arrow::buffer::Buffer;
    use arrow::datatypes::Field;

    /// Comet counterpart to arrow-rs#10030's `test_decimal128_under_aligned_round_trip`. A JVM
    /// producer (arrow-java's `NettyAllocationManager`) only guarantees the C Data Interface's
    /// recommended 8-byte alignment, so it can hand us a `Decimal128` buffer that is not 16-byte
    /// aligned. Arrow 58's `from_ffi_and_data_type` passes those buffers through untouched, so
    /// building the typed `Decimal128Array` would panic in `ScalarBuffer::<i128>::from`
    /// (apache/arrow-rs#10028) without [`batch_from_ffi`]'s `align_buffers` call. This test fails
    /// the same way the stock reader would if that realignment is removed; once Comet moves to
    /// arrow >= 59 (apache/arrow-rs#10030) the realignment is built into `from_ffi_and_data_type`
    /// and this reader can be replaced by the stock `ArrowArrayStreamReader`.
    #[test]
    fn realigns_under_aligned_decimal128() {
        let decimal_type = DataType::Decimal128(10, 2);

        // Slice an aligned [0, 1, 2] i128 buffer 8 bytes in to land on an 8-aligned-not-16-aligned
        // address. The little-endian byte shift makes the two visible elements `1 << 64`, `2 << 64`.
        let under_aligned = Buffer::from_vec(vec![0_i128, 1_i128, 2_i128]).slice(8);
        assert_eq!(under_aligned.as_ptr().align_offset(8), 0);
        assert_ne!(under_aligned.as_ptr().align_offset(16), 0);

        // SAFETY: buffer holds room for 2 i128 values; under-alignment is the condition under test.
        // `build_unchecked` avoids the validation read that would itself panic on the misaligned i128s.
        let decimal = unsafe {
            ArrayData::builder(decimal_type.clone())
                .len(2)
                .add_buffer(under_aligned)
                .build_unchecked()
        };

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "d",
            decimal_type.clone(),
            false,
        )]));
        let struct_data = unsafe {
            ArrayData::builder(DataType::Struct(schema.fields().clone()))
                .len(2)
                .add_child_data(decimal)
                .build_unchecked()
        };
        let array = FFI_ArrowArray::new(&struct_data);

        let batch = batch_from_ffi(array, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(col.len(), 2);
        assert_eq!(col.value(0), 1_i128 << 64);
        assert_eq!(col.value(1), 2_i128 << 64);
    }
}
