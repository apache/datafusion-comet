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
use arrow::array::{
    downcast_dictionary_array, Array, ArrayRef, FixedSizeListArray, GenericListArray,
    GenericStringArray, GenericStringBuilder, MapArray, OffsetSizeTrait, StructArray,
};
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
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            // Capture the original Arc before `downcast_dictionary_array!` shadows `array`, so the
            // unchanged branch returns it verbatim, preserving the zero-copy contract that the
            // Struct/List/Map arms rely on via `Arc::ptr_eq`.
            let original = Arc::clone(array);
            downcast_dictionary_array!(
                array => {
                    let values = array.values();
                    let decoded = decode_string_arrays(values)?;
                    if Arc::ptr_eq(&decoded, values) {
                        Ok(original)
                    } else {
                        Ok(Arc::new(array.with_values(decoded)))
                    }
                }
                t => unreachable!("dictionary type checked by guard: {t}"),
            )
        }
        DataType::Struct(fields) => {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("data type checked by caller");
            let mut changed = false;
            let mut columns = Vec::with_capacity(s.num_columns());
            for col in s.columns() {
                let decoded = decode_string_arrays(col)?;
                changed |= !Arc::ptr_eq(&decoded, col);
                columns.push(decoded);
            }
            if !changed {
                return Ok(Arc::clone(array));
            }
            Ok(Arc::new(StructArray::new(
                fields.clone(),
                columns,
                s.nulls().cloned(),
            )))
        }
        DataType::List(_) => decode_list::<i32>(array),
        DataType::LargeList(_) => decode_list::<i64>(array),
        DataType::FixedSizeList(field, size) => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("data type checked by caller");
            let values = list.values();
            let decoded = decode_string_arrays(values)?;
            if Arc::ptr_eq(&decoded, values) {
                return Ok(Arc::clone(array));
            }
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                decoded,
                list.nulls().cloned(),
            )?))
        }
        DataType::Map(field, ordered) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("data type checked by caller");
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let decoded = decode_string_arrays(&entries)?;
            if Arc::ptr_eq(&decoded, &entries) {
                return Ok(Arc::clone(array));
            }
            let decoded_struct = decoded
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("entries are a struct")
                .clone();
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                decoded_struct,
                map.nulls().cloned(),
                *ordered,
            )?))
        }
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

fn decode_list<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let list = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .expect("data type checked by caller");
    let values = list.values();
    let decoded = decode_string_arrays(values)?;
    if Arc::ptr_eq(&decoded, values) {
        return Ok(Arc::clone(array));
    }
    let field = match array.data_type() {
        DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
        _ => unreachable!("decode_list called on non-list"),
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        list.offsets().clone(),
        decoded,
        list.nulls().cloned(),
    )?))
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

    use arrow::array::{
        DictionaryArray, FixedSizeListArray, Int32Array, ListArray, MapArray, StructArray,
    };
    use arrow::datatypes::{Field, Fields, Int32Type};

    /// An invalid Utf8 leaf ["\u{FFFD}"] built from raw bytes.
    fn invalid_leaf() -> ArrayRef {
        utf8_unchecked(&[0, 1], &[0xFF], 1)
    }

    #[test]
    fn dictionary_values_are_decoded() {
        let values = invalid_leaf();
        let keys = Int32Array::from(vec![0, 0]);
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(keys, values));
        let out = decode_string_arrays(&dict).unwrap();
        let d = out
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let vals = d.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(vals.value(0), "\u{FFFD}");
    }

    #[test]
    fn struct_field_is_decoded() {
        let field = Arc::new(Field::new("s", DataType::Utf8, true));
        let input: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![field]),
            vec![invalid_leaf()],
            None,
        ));
        let out = decode_string_arrays(&input).unwrap();
        let s = out.as_any().downcast_ref::<StructArray>().unwrap();
        let col = s.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "\u{FFFD}");
    }

    #[test]
    fn list_values_are_decoded() {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 1].into());
        let input: ArrayRef =
            Arc::new(ListArray::try_new(field, offsets, invalid_leaf(), None).unwrap());
        let out = decode_string_arrays(&input).unwrap();
        let l = out.as_any().downcast_ref::<ListArray>().unwrap();
        let vals = l.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(vals.value(0), "\u{FFFD}");
    }

    #[test]
    fn valid_struct_is_zero_copy() {
        let field = Arc::new(Field::new("s", DataType::Utf8, true));
        let leaf: ArrayRef = Arc::new(StringArray::from(vec!["ok"]));
        let input: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![field]),
            vec![leaf],
            None,
        ));
        let out = decode_string_arrays(&input).unwrap();
        assert!(
            Arc::ptr_eq(&input, &out),
            "all-valid nested input must be unchanged"
        );
    }

    #[test]
    fn dictionary_valid_values_are_zero_copy() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let keys = Int32Array::from(vec![0, 1, 0]);
        let input: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(keys, values));
        let out = decode_string_arrays(&input).unwrap();
        assert!(
            Arc::ptr_eq(&input, &out),
            "dictionary with all-valid values must be returned as the original Arc"
        );
    }

    #[test]
    fn fixed_size_list_values_are_decoded() {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let input: ArrayRef =
            Arc::new(FixedSizeListArray::try_new(field, 1, invalid_leaf(), None).unwrap());
        let out = decode_string_arrays(&input).unwrap();
        let l = out.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let vals = l.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(vals.value(0), "\u{FFFD}");
    }

    #[test]
    fn fixed_size_list_valid_is_zero_copy() {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let leaf: ArrayRef = Arc::new(StringArray::from(vec!["ok"]));
        let input: ArrayRef = Arc::new(FixedSizeListArray::try_new(field, 1, leaf, None).unwrap());
        let out = decode_string_arrays(&input).unwrap();
        assert!(
            Arc::ptr_eq(&input, &out),
            "fixed-size list with all-valid values must be unchanged"
        );
    }

    /// Build a Map array whose single entry maps `key` -> `values` (a Utf8 leaf array of length 1).
    fn build_map(key: &str, values: ArrayRef) -> ArrayRef {
        let entries_fields = Fields::from(vec![
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ]);
        let keys: ArrayRef = Arc::new(StringArray::from(vec![key]));
        let entries = StructArray::new(entries_fields.clone(), vec![keys, values], None);
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields),
            false,
        ));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 1].into());
        Arc::new(MapArray::try_new(map_field, offsets, entries, None, false).unwrap())
    }

    #[test]
    fn map_values_are_decoded() {
        let input = build_map("k", invalid_leaf());
        let out = decode_string_arrays(&input).unwrap();
        let m = out.as_any().downcast_ref::<MapArray>().unwrap();
        let values = m
            .entries()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "\u{FFFD}");
    }

    #[test]
    fn map_valid_is_zero_copy() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["ok"]));
        let input = build_map("k", values);
        let out = decode_string_arrays(&input).unwrap();
        assert!(
            Arc::ptr_eq(&input, &out),
            "map with all-valid values must be unchanged"
        );
    }
}
