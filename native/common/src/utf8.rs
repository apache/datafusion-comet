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
/// arrays via `new_unchecked` and does not validate UTF-8. Unsupported string-bearing Arrow types
/// fail closed rather than preserving unchecked string data.
pub fn decode_string_arrays(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Utf8 => decode_generic_string::<i32>(array),
        DataType::LargeUtf8 => decode_generic_string::<i64>(array),
        DataType::Dictionary(_, value_type) => {
            if !data_type_contains_string(value_type) {
                return Ok(Arc::clone(array));
            }
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
                t => unreachable!("dictionary key type validated by Arrow: {t}"),
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
            let entries = map.entries();
            let mut changed = false;
            let mut columns = Vec::with_capacity(entries.num_columns());
            for col in entries.columns() {
                let decoded = decode_string_arrays(col)?;
                changed |= !Arc::ptr_eq(&decoded, col);
                columns.push(decoded);
            }
            if !changed {
                return Ok(Arc::clone(array));
            }
            let decoded_entries =
                StructArray::new(entries.fields().clone(), columns, entries.nulls().cloned());
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                decoded_entries,
                map.nulls().cloned(),
                *ordered,
            )?))
        }
        // Spark's current JVM producers emit StringType as Utf8/LargeUtf8 and the supported
        // containers above. Fail closed if a different string-bearing representation reaches this
        // boundary: passing it through would preserve the invalid UTF-8 this function exists to
        // remove. Keep `data_type_contains_string` exhaustive so an Arrow upgrade adding another
        // data type forces this safety audit to be revisited.
        data_type => {
            if data_type_contains_string(data_type) {
                Err(unsupported_string_type(data_type))
            } else {
                Ok(Arc::clone(array))
            }
        }
    }
}

fn unsupported_string_type(data_type: &DataType) -> ArrowError {
    ArrowError::NotYetImplemented(format!(
        "decoding FFI-imported string data in {data_type} is not supported"
    ))
}

/// Whether `data_type` is, or can contain, a logical string representation.
///
/// This match is deliberately exhaustive: when Arrow adds a data type, the compiler should force
/// us to decide whether it can carry strings before `decode_string_arrays` passes it through.
fn data_type_contains_string(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field) => data_type_contains_string(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_contains_string(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_contains_string(field.data_type())),
        DataType::Dictionary(_, value_type) => data_type_contains_string(value_type),
        DataType::Map(field, _) => data_type_contains_string(field.data_type()),
        DataType::RunEndEncoded(_, values) => data_type_contains_string(values.data_type()),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => false,
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
    use super::{data_type_contains_string, decode_string_arrays};
    use arrow::array::{
        make_array, Array, ArrayData, ArrayRef, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType;
    use arrow::error::ArrowError;
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
    fn unsupported_string_representation_fails_closed() {
        let input: ArrayRef = Arc::new(StringViewArray::from(vec!["valid"]));
        let err = decode_string_arrays(&input).unwrap_err();
        assert!(matches!(
            err,
            ArrowError::NotYetImplemented(message) if message.contains("Utf8View")
        ));
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
        let data = unsafe {
            ArrayData::builder(DataType::Utf8)
                .len(3)
                .null_count(1)
                .null_bit_buffer(Some(Buffer::from(vec![0b0000_0101_u8])))
                .add_buffer(Buffer::from_slice_ref([0_i32, 1, 2, 3]))
                .add_buffer(Buffer::from(vec![0xff, 0xfe, b'b']))
                .build_unchecked()
        };
        let input = make_array(data);
        let out = decode_string_arrays(&input).unwrap();
        let s = out.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(s.value(0), "\u{FFFD}");
        assert!(s.is_null(1));
        assert_eq!(s.value(2), "b");
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

    #[test]
    fn detects_unhandled_nested_string_representations() {
        let string_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list_view = DataType::ListView(Arc::clone(&string_field));
        let large_list_view = DataType::LargeListView(Arc::clone(&string_field));
        assert!(data_type_contains_string(&list_view));
        assert!(data_type_contains_string(&large_list_view));

        let run_ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
        let run_end_encoded = DataType::RunEndEncoded(run_ends, string_field);
        assert!(data_type_contains_string(&run_end_encoded));

        let int_field = Arc::new(Field::new("item", DataType::Int32, true));
        assert!(!data_type_contains_string(&DataType::ListView(int_field)));
    }

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

    #[test]
    fn trailing_empty_string_offset_is_handled() {
        // element 0 = "ab", element 1 = "" (its offset lands exactly at values.len()).
        let input = utf8_unchecked(&[0, 2, 2], b"ab", 2);
        let out = decode_string_arrays(&input).unwrap();
        assert!(
            Arc::ptr_eq(&input, &out),
            "valid input with a trailing empty element must be returned unchanged"
        );
        let s = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "ab");
        assert_eq!(s.value(1), "");
    }

    #[test]
    fn sliced_array_is_decoded() {
        // Slicing shifts offsets[0] away from 0; the fast path must still handle this correctly.
        let base: ArrayRef = Arc::new(StringArray::from(vec!["aa", "bb", "cc"]));
        let sliced: ArrayRef = base.slice(1, 2);
        let out = decode_string_arrays(&sliced).unwrap();
        // Slicing already produces a new Arc distinct from `base`, so ptr_eq against `base` isn't
        // meaningful here; what matters is that the fast path recognizes the slice as valid and
        // returns it unchanged (same Arc as `sliced`) rather than rebuilding it.
        assert!(
            Arc::ptr_eq(&sliced, &out),
            "valid sliced input must be returned unchanged"
        );
        let s = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "bb");
        assert_eq!(s.value(1), "cc");
    }
}
