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

use crate::decode_utf8_spark_lossy;
use arrow::array::{Array, ArrayRef, GenericStringArray, GenericStringBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use std::sync::Arc;

/// Ensure every `Utf8`/`LargeUtf8` array reachable from `array` holds valid UTF-8, decoding invalid
/// bytes the way Spark renders `StringType`. Returns the same `Arc` (zero-copy) when nothing needed
/// decoding. Used at the JVM->native FFI import boundary, where arrow's `from_ffi` builds string
/// arrays via `new_unchecked` and does not validate UTF-8.
pub fn decode_string_arrays(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Utf8 => decode_generic_string::<i32>(array),
        DataType::LargeUtf8 => decode_generic_string::<i64>(array),
        _ => Ok(Arc::clone(array)),
    }
}

fn decode_generic_string<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let arr = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .expect("data type checked by caller");
    let len = arr.len();
    if len == 0 {
        return Ok(Arc::clone(array));
    }
    let values: &[u8] = arr.value_data();
    let offsets = arr.value_offsets(); // &[O], length == len + 1
    let start = offsets[0].as_usize();
    let end = offsets[len].as_usize();

    // Fast path: the used byte range parses as UTF-8 AND no element boundary splits a codepoint.
    // Both are required: a whole-buffer-valid "é" (C3 A9) with per-element offsets [0,1,2] yields
    // element slices `C3` and `A9`, each invalid, and `value()` would decode them unchecked (UB).
    if std::str::from_utf8(&values[start..end]).is_ok() {
        let mut boundaries_ok = true;
        for off in &offsets[1..len] {
            let o = off.as_usize();
            // A boundary landing on a UTF-8 continuation byte (0b10xx_xxxx) splits a codepoint.
            if o < values.len() && (values[o] & 0xC0) == 0x80 {
                boundaries_ok = false;
                break;
            }
        }
        if boundaries_ok {
            return Ok(Arc::clone(array));
        }
    }

    // Slow path: rebuild element-by-element via the Spark-lossy decoder. We slice the raw values
    // buffer directly rather than calling `arr.value(i)`, which uses `from_utf8_unchecked`.
    let mut builder = GenericStringBuilder::<O>::with_capacity(len, end - start);
    for i in 0..len {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let s = offsets[i].as_usize();
            let e = offsets[i + 1].as_usize();
            builder.append_value(decode_utf8_spark_lossy(&values[s..e]));
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod walker_tests {
    use super::decode_string_arrays;
    use arrow::array::{make_array, Array, ArrayData, ArrayRef, LargeStringArray, StringArray};
    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    /// Build a (possibly invalid) Utf8 array from raw offsets + value bytes, the way an FFI import
    /// would deliver it (no validation). `build_unchecked` mirrors arrow's `from_ffi`.
    fn utf8_unchecked(offsets: &[i32], values: &[u8], len: usize) -> ArrayRef {
        let data = unsafe {
            ArrayData::builder(DataType::Utf8)
                .len(len)
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_buffer(Buffer::from(values.to_vec()))
                .build_unchecked()
        };
        make_array(data)
    }

    #[test]
    fn valid_utf8_is_zero_copy() {
        let input: ArrayRef = Arc::new(StringArray::from(vec!["a", "é", "🦀"]));
        let out = decode_string_arrays(&input).unwrap();
        assert!(
            Arc::ptr_eq(&input, &out),
            "valid input must be returned unchanged"
        );
    }

    #[test]
    fn invalid_bytes_decode_to_replacement() {
        // element 0 = [0xFF, 0x41] -> "\u{FFFD}A", element 1 = [0x42] -> "B"
        let input = utf8_unchecked(&[0, 2, 3], &[0xFF, 0x41, 0x42], 2);
        let out = decode_string_arrays(&input).unwrap();
        let s = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "\u{FFFD}A");
        assert_eq!(s.value(1), "B");
    }

    #[test]
    fn split_codepoint_boundary_is_rebuilt() {
        // whole buffer "é" (C3 A9) is valid UTF-8, but offsets split it into two invalid slices.
        let input = utf8_unchecked(&[0, 1, 2], &[0xC3, 0xA9], 2);
        let out = decode_string_arrays(&input).unwrap();
        let s = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "\u{FFFD}");
        assert_eq!(s.value(1), "\u{FFFD}");
    }

    #[test]
    fn nulls_are_preserved() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
        let out = decode_string_arrays(&input).unwrap();
        let s = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(s.is_null(1));
        assert_eq!(s.value(0), "a");
    }

    #[test]
    fn large_utf8_invalid_decodes() {
        let data = unsafe {
            ArrayData::builder(DataType::LargeUtf8)
                .len(1)
                .add_buffer(Buffer::from_slice_ref([0i64, 1]))
                .add_buffer(Buffer::from(vec![0xFFu8]))
                .build_unchecked()
        };
        let out = decode_string_arrays(&make_array(data)).unwrap();
        let s = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
        assert_eq!(s.value(0), "\u{FFFD}");
    }
}
