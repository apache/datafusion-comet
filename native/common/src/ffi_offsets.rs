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

//! Zeroing array offsets before an array is exported to the JVM.
//!
//! Arrow Java's C Data importer ignores `ArrowArray.offset` at every level
//! (<https://github.com/apache/arrow-java/issues/88>) and reads each buffer from its start. arrow-rs
//! folds a slice into the buffers for almost every type, so a sliced `Int64Array`, `StringArray` or
//! `StructArray` exports offset 0 and nothing is lost. `BooleanArray` is the exception: it keeps its
//! bit offset in `ArrayData::offset`, and the export keeps the validity bitmap at that same offset,
//! so the JVM reads both the values and the nulls of a sliced boolean from bit 0. A struct exports
//! offset 0 even when its children are sliced, so checking only the top level misses a boolean
//! child (<https://github.com/apache/datafusion-comet/issues/6288>).

use arrow::array::{ArrayData, MutableArrayData};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use std::borrow::Cow;

/// `data` with a zero offset at every level, children and dictionary values included, so that a
/// consumer that ignores `ArrowArray.offset` reads the right rows. Apply it to every array exported
/// to the JVM.
///
/// Returns `data` itself when every offset is already zero, which is the common case. A sliced
/// boolean gets its values bitmap re-sliced to start at bit 0, which shares the buffer when the
/// offset is a whole number of bytes and copies the bitmap otherwise. Every other buffer is shared.
/// The validity bitmap needs nothing here, because `FFI_ArrowArray::new` realigns it to the
/// exported offset.
pub fn zero_offsets(data: &ArrayData) -> Result<Cow<'_, ArrayData>, ArrowError> {
    if !has_nonzero_offset(data) {
        return Ok(Cow::Borrowed(data));
    }
    let zeroed = rewrite(data)?;
    debug_assert!(!has_nonzero_offset(&zeroed));
    Ok(Cow::Owned(zeroed))
}

/// Whether `data` or anything below it has a non-zero offset. A dictionary's values are its only
/// child in `ArrayData`, so they are covered too.
fn has_nonzero_offset(data: &ArrayData) -> bool {
    data.offset() != 0 || data.child_data().iter().any(has_nonzero_offset)
}

fn rewrite(data: &ArrayData) -> Result<ArrayData, ArrowError> {
    let data = match (data.offset(), data.data_type()) {
        (0, _) => data.clone(),
        (offset, DataType::Boolean) => {
            let values = data.buffers()[0].bit_slice(offset, data.len());
            // SAFETY: the same bits, now starting at bit 0. The length and nulls are unchanged.
            unsafe {
                data.clone()
                    .into_builder()
                    .offset(0)
                    .buffers(vec![values])
                    .build_unchecked()
            }
        }
        // arrow-rs keeps an offset only for booleans and run-end encoded arrays, and Spark has no
        // run-end encoded type, so this is not expected to run. A copy starts at offset 0.
        _ => {
            let mut copy = MutableArrayData::new(vec![data], false, data.len());
            copy.try_extend(0, 0, data.len())?;
            copy.freeze()
        }
    };
    if !data.child_data().iter().any(has_nonzero_offset) {
        return Ok(data);
    }
    let children = data
        .child_data()
        .iter()
        .map(|child| zero_offsets(child).map(Cow::into_owned))
        .collect::<Result<Vec<_>, _>>()?;
    // SAFETY: each child holds the same values as before, with its offsets folded into its buffers.
    Ok(unsafe { data.into_builder().child_data(children).build_unchecked() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, Int32Array, Int64Array, Int8Array,
        ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Int8Type};
    use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use std::sync::Arc;

    /// Nullable booleans with nulls and values that differ from their neighbours, so that reading
    /// from the wrong bit shows up in both.
    fn booleans(len: usize) -> BooleanArray {
        (0..len)
            .map(|i| (i % 5 != 0).then_some(i % 3 == 0))
            .collect()
    }

    fn assert_exported_offsets_are_zero(array: &FFI_ArrowArray) {
        assert_eq!(array.offset(), 0);
        (0..array.num_children()).for_each(|i| assert_exported_offsets_are_zero(array.child(i)));
        if let Some(dictionary) = array.dictionary() {
            assert_exported_offsets_are_zero(dictionary);
        }
    }

    /// Exports `data` the way Comet hands it to the JVM, checks that the export has offset 0 at
    /// every level, and checks that importing it back gives the same values.
    fn assert_exports_aligned(data: &ArrayData) {
        let zeroed = zero_offsets(data).unwrap();
        let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
        let exported = FFI_ArrowArray::new(&zeroed);
        assert_exported_offsets_are_zero(&exported);
        let imported = unsafe { from_ffi(exported, &schema) }.unwrap();
        assert_eq!(imported, *data);
    }

    #[test]
    fn unsliced_arrays_are_returned_as_is() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(booleans(20)),
            // Slicing folds into the buffers for every type but boolean.
            Arc::new(Int64Array::from_iter_values(0..20).slice(3, 10)),
            Arc::new(StringArray::from_iter_values((0..20).map(|i| i.to_string())).slice(3, 10)),
        ];
        for column in columns {
            let data = column.to_data();
            assert!(matches!(zero_offsets(&data).unwrap(), Cow::Borrowed(_)));
        }
    }

    #[test]
    fn sliced_booleans_start_at_bit_zero() {
        let array = booleans(100);
        // A whole number of bytes in, and part of one.
        for offset in [8, 17] {
            let data = array.slice(offset, 40).to_data();
            assert_exports_aligned(&data);

            let zeroed = zero_offsets(&data).unwrap();
            assert_eq!(zeroed.offset(), 0);
            if offset % 8 == 0 {
                // Re-slicing on a byte boundary shares the bitmap.
                assert_eq!(
                    zeroed.buffers()[0].as_ptr(),
                    data.buffers()[0].as_ptr().wrapping_add(offset / 8)
                );
            }
        }
    }

    #[test]
    fn sliced_boolean_struct_children_start_at_bit_zero() {
        let fields = Fields::from(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("k", DataType::Int64, false),
        ]);
        let array = StructArray::new(
            fields,
            vec![
                Arc::new(booleans(100)),
                Arc::new(Int64Array::from_iter_values(0..100)),
            ],
            None,
        );
        let data = array.slice(17, 40).to_data();
        assert_eq!(data.offset(), 0);
        assert_eq!(data.child_data()[0].offset(), 17);
        assert_exports_aligned(&data);

        // The Int64 child is passed through, not copied.
        let zeroed = zero_offsets(&data).unwrap();
        assert_eq!(
            zeroed.child_data()[1].buffers()[0].as_ptr(),
            data.child_data()[1].buffers()[0].as_ptr()
        );
    }

    #[test]
    fn sliced_booleans_in_a_list_of_structs_start_at_bit_zero() {
        let fields = Fields::from(vec![Field::new("b", DataType::Boolean, true)]);
        let structs = StructArray::new(fields.clone(), vec![Arc::new(booleans(100))], None);
        // A list slice moves only the offsets, so the list has to be built over a sliced struct,
        // which is what collect_list returns for a group with a single run of rows.
        let list = ListArray::new(
            Arc::new(Field::new_struct("item", fields, true)),
            OffsetBuffer::from_lengths([10, 0, 20]),
            Arc::new(structs.slice(5, 30)),
            None,
        );
        assert_exports_aligned(&list.to_data());
    }

    #[test]
    fn sliced_boolean_map_values_start_at_bit_zero() {
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Boolean, true),
            ]),
            vec![
                Arc::new(StringArray::from_iter_values(
                    (0..100).map(|i| i.to_string()),
                )),
                Arc::new(booleans(100)),
            ],
            None,
        );
        let map = MapArray::try_new(
            Arc::new(Field::new_struct(
                "entries",
                entries.fields().clone(),
                false,
            )),
            OffsetBuffer::from_lengths([3, 7, 0, 10]),
            entries.slice(33, 20),
            None,
            false,
        )
        .unwrap();
        assert_exports_aligned(&map.to_data());
    }

    #[test]
    fn sliced_boolean_dictionary_values_start_at_bit_zero() {
        let dictionary = DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![Some(2), None, Some(0), Some(1), Some(2)]),
            Arc::new(booleans(20).slice(3, 3)),
        );
        assert_exports_aligned(&dictionary.to_data());
    }

    #[test]
    fn other_types_with_an_offset_are_copied() {
        // arrow-rs never builds one of these, but an ArrayData can carry an offset for any type.
        let data = Int32Array::from_iter((0..20).map(|i| (i % 4 != 0).then_some(i)))
            .to_data()
            .slice(5, 10);
        assert_eq!(data.offset(), 5);
        assert_exports_aligned(&data);
    }
}
