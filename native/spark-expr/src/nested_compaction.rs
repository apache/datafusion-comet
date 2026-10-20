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

//! Releases excess capacity that Arrow's `take` can leave in nested results.
//!
//! Gathering list, map or struct values with `take` reserves child buffers from the
//! average candidate size. Selecting short values next to much longer alternatives
//! therefore retains capacity sized from values that were never copied. Kernels keep
//! `take` for its speed and pass the result through [`compact_nested_take_result`].

use arrow::array::{make_array, Array, ArrayData, ArrayRef, AsArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::DataType;

/// Shrink the owned buffers of a nested `take` result that retain far more capacity than
/// they use. Flat results and results without excess capacity are returned unchanged.
pub(crate) fn compact_nested_take_result(result: ArrayRef) -> ArrayRef {
    if result.data_type().is_nested() && has_excess_nested_capacity(result.as_ref()) {
        // Convert only oversized results, releasing the array before shrinking its
        // owned buffers. Preserve headroom in other buffers to avoid needless copies.
        make_array(compact_nested_data(result.into_data()))
    } else {
        result
    }
}

// Inspect owned output capacity directly: nested builders can propagate a reservation
// into deeper children even when the immediate child count was estimated correctly.
// Keep ordinary growth headroom and small aligned buffers to avoid unnecessary copies.
fn has_excess_nested_capacity(array: &dyn Array) -> bool {
    let oversized = has_excess_capacity;
    if array
        .nulls()
        .is_some_and(|nulls| oversized(nulls.buffer().capacity(), nulls.len().div_ceil(8)))
    {
        return true;
    }
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            has_excess_nested_capacity(list.values().as_ref())
                || oversized(
                    list.offsets().inner().inner().capacity(),
                    list.offsets().len() * 4,
                )
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            has_excess_nested_capacity(list.values().as_ref())
                || oversized(
                    list.offsets().inner().inner().capacity(),
                    list.offsets().len() * 8,
                )
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            has_excess_nested_capacity(map.entries())
                || oversized(
                    map.offsets().inner().inner().capacity(),
                    map.offsets().len() * 4,
                )
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .any(|column| has_excess_nested_capacity(column.as_ref())),
        DataType::FixedSizeList(_, _) => {
            has_excess_nested_capacity(array.as_fixed_size_list().values().as_ref())
        }
        data_type => {
            let used = match data_type {
                DataType::Boolean => array.len().div_ceil(8),
                DataType::Utf8 => {
                    array.as_string::<i32>().value_data().len() + (array.len() + 1) * 4
                }
                DataType::LargeUtf8 => {
                    array.as_string::<i64>().value_data().len() + (array.len() + 1) * 8
                }
                DataType::Binary => {
                    array.as_binary::<i32>().value_data().len() + (array.len() + 1) * 4
                }
                DataType::LargeBinary => {
                    array.as_binary::<i64>().value_data().len() + (array.len() + 1) * 8
                }
                DataType::FixedSizeBinary(width) => array.len().saturating_mul(*width as usize),
                _ => match data_type.primitive_width() {
                    Some(width) => array.len().saturating_mul(width),
                    None => return false,
                },
            };
            let null_bytes = array.nulls().map_or(0, |nulls| nulls.len().div_ceil(8));
            oversized(
                array.get_buffer_memory_size(),
                used.saturating_add(null_bytes),
            )
        }
    }
}

fn has_excess_capacity(capacity: usize, used: usize) -> bool {
    // Allow growth headroom plus one alignment block. Without the padding allowance,
    // an offset buffer just above four times its used size can trigger compaction.
    capacity > used.saturating_mul(4).saturating_add(64)
}

fn compact_nested_data(data: ArrayData) -> ArrayData {
    let (data_type, len, mut nulls, offset, mut buffers, children) = data.into_parts();
    for buffer in &mut buffers {
        if has_excess_capacity(buffer.capacity(), buffer.len()) {
            buffer.shrink_to_fit();
        }
    }
    if let Some(nulls) = &mut nulls {
        // `MutableArrayData` zero-fills the validity bitmap for its full reservation, so the
        // buffer *length* is oversized, not only its capacity. Copy just the used bits.
        if has_excess_capacity(nulls.buffer().capacity(), nulls.len().div_ceil(8)) {
            let bits = Buffer::from_slice_ref(nulls.inner().sliced().as_slice());
            // SAFETY: the copy holds the same logical validity bits, rebased to bit offset 0,
            // so the length and null count are unchanged.
            *nulls = unsafe {
                NullBuffer::new_unchecked(
                    BooleanBuffer::new(bits, 0, nulls.len()),
                    nulls.null_count(),
                )
            };
        }
    }
    let children = children.into_iter().map(compact_nested_data).collect();
    // SAFETY: this data came from a valid Arrow take result. Value and offset buffers
    // only lost capacity, and a copied validity bitmap keeps the same logical bits and
    // null count. Types, lengths and array offsets are unchanged. Rechecking every
    // nested offset or string would add a scan without changing these invariants.
    unsafe {
        ArrayData::builder(data_type)
            .len(len)
            .offset(offset)
            .nulls(nulls)
            .buffers(buffers)
            .child_data(children)
            .build_unchecked()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    /// A validity bitmap of `len` bits sliced out of a much larger allocation at `offset`.
    fn oversized_nulls(offset: usize, len: usize) -> NullBuffer {
        let bits = BooleanBuffer::from_iter((0..80_000).map(|i| i % 3 != 0));
        NullBuffer::new(bits.slice(offset, len))
    }

    #[test]
    fn copies_oversized_validity_at_any_bit_offset() {
        // 1001 rows leave a partial final byte; offsets 0 and 8 are byte-aligned, 3 is not.
        const ROWS: usize = 1001;
        for offset in [0, 3, 8] {
            let ints = Arc::new(Int32Array::new(
                (0..ROWS as i32).collect(),
                Some(oversized_nulls(offset, ROWS)),
            ));
            let lists = Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                OffsetBuffer::from_lengths(std::iter::repeat_n(1, ROWS)),
                ints,
                None,
            ));
            let structs: ArrayRef = Arc::new(StructArray::new(
                vec![Arc::new(Field::new(
                    "items",
                    lists.data_type().clone(),
                    true,
                ))]
                .into(),
                vec![lists],
                Some(oversized_nulls(offset + 1, ROWS)),
            ));
            let input = structs.to_data();
            let result = compact_nested_take_result(Arc::clone(&structs));
            let data = result.to_data();
            data.validate_full().unwrap();
            assert_eq!(data, input, "offset={offset}");
            assert_eq!(result.null_count(), structs.null_count());
            let leaf = result.as_struct().column(0).as_list::<i32>().values();
            let source_leaf = structs.as_struct().column(0).as_list::<i32>().values();
            assert_eq!(leaf.null_count(), source_leaf.null_count());
            // Both bitmaps now hold just their used bytes rather than the 10 KB source.
            assert!(result.nulls().unwrap().buffer().capacity() <= 4 * ROWS.div_ceil(8) + 64);
            assert!(leaf.nulls().unwrap().buffer().capacity() <= 4 * ROWS.div_ceil(8) + 64);
            // The shared source bitmaps are copied, never shrunk in place.
            assert_eq!(structs.to_data(), input);
            assert!(structs.nulls().unwrap().buffer().capacity() >= 10_000);
        }
    }

    #[test]
    fn leaves_results_without_excess_capacity_untouched() {
        let lists: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(2, 512)),
            Arc::new(Int32Array::from_iter(
                (0..1024).map(|i| (i % 5 != 0).then_some(i)),
            )),
            None,
        ));
        assert!(Arc::ptr_eq(
            &compact_nested_take_result(Arc::clone(&lists)),
            &lists
        ));
        let flat: ArrayRef = Arc::new(Int32Array::new(
            (0..10).collect(),
            Some(oversized_nulls(0, 10)),
        ));
        // Flat results are not inspected even when their validity is oversized.
        assert!(Arc::ptr_eq(
            &compact_nested_take_result(Arc::clone(&flat)),
            &flat
        ));
    }
}
