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

//! Gather selected nested ranges and release excess owned output capacity.

use std::sync::Arc;

use arrow::array::{
    make_array, Array, ArrayData, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder,
    GenericListArray, ListArray, MapArray, MutableArrayData, NullBufferBuilder, OffsetSizeTrait,
    PrimitiveArray, StructArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{ArrowPrimitiveType, DataType, FieldRef};
use arrow::error::{ArrowError, Result};

/// Keep the Arrow gather fast path, then release excess owned capacity without
/// changing values, offsets or validity. Flat and normally sized results are returned
/// unchanged; shared and externally owned buffers retain Arrow's ownership guarantees.
pub(crate) fn compact_nested_buffers(result: ArrayRef) -> ArrayRef {
    if result.data_type().is_nested() && has_excess_nested_capacity(result.as_ref()) {
        // Consume the array before shrinking so its own references do not pin buffers.
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
        if has_excess_capacity(nulls.buffer().capacity(), nulls.buffer().len()) {
            nulls.shrink_to_fit();
        }
    }
    let children = children.into_iter().map(compact_nested_data).collect();
    // SAFETY: this data came from a valid Arrow array. Only buffer capacities
    // changed: bytes, lengths, offsets, types, and validity are preserved. Rechecking
    // every nested offset or string would add a scan without changing these invariants.
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

/// Gather nested values without reserving space for unselected candidate children.
/// Flat values keep Arrow's take kernel. The indices come from a validated lookup.
pub(crate) fn take_nested_values(values: &ArrayRef, indices: &UInt32Array) -> Result<ArrayRef> {
    match values.data_type() {
        DataType::List(_) => take_indexed_lists(values.as_list::<i32>(), indices),
        DataType::LargeList(_) => take_indexed_lists(values.as_list::<i64>(), indices),
        DataType::Map(field, sorted) => {
            let lists = map_as_list(values.as_map(), field);
            list_as_map(
                take_selected_lists(&lists, indices.iter().map(|i| i.map(|i| i as usize)))?,
                field,
                *sorted,
            )
        }
        DataType::Struct(fields) => {
            let columns = values
                .as_struct()
                .columns()
                .iter()
                .map(|c| take_nested_values(c, indices))
                .collect::<Result<Vec<_>>>()?;
            let nulls = match values.nulls().filter(|n| n.null_count() > 0) {
                Some(n) => {
                    let validity = BooleanArray::new(n.inner().clone(), None);
                    let selected = take(&validity, indices, None)?;
                    let selected = selected.as_boolean();
                    NullBuffer::union(
                        Some(&NullBuffer::new(selected.values().clone())),
                        selected.nulls(),
                    )
                }
                None => indices.nulls().cloned(),
            }
            .filter(|n| n.null_count() > 0);
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                nulls,
                indices.len(),
            )?))
        }
        _ => Ok(take(values.as_ref(), indices, None)?),
    }
}

fn map_as_list(map: &MapArray, field: &FieldRef) -> ListArray {
    ListArray::new(
        Arc::clone(field),
        map.offsets().clone(),
        Arc::new(map.entries().clone()),
        map.nulls().cloned(),
    )
}
fn list_as_map(result: ArrayRef, field: &FieldRef, sorted: bool) -> Result<ArrayRef> {
    let list = result.as_list::<i32>();
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(field),
        list.offsets().clone(),
        list.values().as_struct().clone(),
        list.nulls().cloned(),
        sorted,
    )?))
}

/// Resolve list offsets and nulls while the caller produces its selection. Keeping
/// selection and range planning in one pass avoids an intermediate indices array for
/// map lookups whose values are lists. Child copies use only these selected ranges.
pub(crate) fn take_selected_lists<O: OffsetSizeTrait>(
    values: &GenericListArray<O>,
    selected: impl ExactSizeIterator<Item = Option<usize>>,
) -> Result<ArrayRef> {
    let count = selected.len();
    let source = values.value_offsets();
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(O::zero());
    let mut nulls = NullBufferBuilder::new(count);
    let mut ranges = Vec::with_capacity(count);
    let mut length = 0;
    for index in selected {
        if let Some(i) = index.filter(|&i| values.is_valid(i)) {
            let start = source[i].as_usize();
            let end = source[i + 1].as_usize();
            length += end - start;
            if start != end {
                ranges.push((start, end));
            }
            nulls.append(true);
        } else {
            nulls.append(false);
        }
        offsets.push(O::usize_as(length));
    }
    // Every offset is at most the final length. Validate the maximum once before
    // constructing the array; any truncated intermediate offsets are discarded on
    // overflow. Keeping the fallible conversion out of the loop permits bulk copies.
    O::from_usize(length).ok_or(ArrowError::OffsetOverflowError(length))?;
    let child = copy_ranges(values.values().as_ref(), &ranges, length)?;
    let field = match values.data_type() {
        DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
        _ => unreachable!(),
    };
    Ok(Arc::new(GenericListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        child,
        nulls.finish(),
    )?))
}

fn range_nulls(values: &dyn Array, ranges: &[(usize, usize)], len: usize) -> Option<NullBuffer> {
    let source = values.nulls().filter(|n| n.null_count() > 0)?;
    let mut output = BooleanBufferBuilder::new(len);
    let bits = source.inner();
    for &(start, end) in ranges {
        output.append_packed_range(bits.offset() + start..bits.offset() + end, bits.values());
    }
    let nulls = NullBuffer::new(output.finish());
    (nulls.null_count() > 0).then_some(nulls)
}
fn copy_primitive<T: ArrowPrimitiveType>(
    values: &dyn Array,
    ranges: &[(usize, usize)],
    len: usize,
) -> Result<ArrayRef> {
    let typed = values.as_primitive::<T>();
    let mut output = Vec::with_capacity(len);
    let nulls = match values.nulls().filter(|n| n.null_count() > 0) {
        Some(source) => {
            let bits = source.inner();
            let mut validity = BooleanBufferBuilder::new(len);
            for &(start, end) in ranges {
                output.extend_from_slice(&typed.values()[start..end]);
                validity
                    .append_packed_range(bits.offset() + start..bits.offset() + end, bits.values());
            }
            let n = NullBuffer::new(validity.finish());
            (n.null_count() > 0).then_some(n)
        }
        None => {
            for &(start, end) in ranges {
                output.extend_from_slice(&typed.values()[start..end]);
            }
            None
        }
    };
    Ok(Arc::new(
        PrimitiveArray::<T>::new(output.into(), nulls).with_data_type(values.data_type().clone()),
    ))
}

macro_rules! copy_primitive_helper {
    ($t:ty,$values:ident,$ranges:ident,$len:ident) => {
        copy_primitive::<$t>($values, $ranges, $len)
    };
}
fn copy_list_ranges<O: OffsetSizeTrait>(
    values: &GenericListArray<O>,
    ranges: &[(usize, usize)],
    len: usize,
) -> Result<ArrayRef> {
    let input = values.value_offsets();
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(O::zero());
    let mut children = Vec::with_capacity(ranges.len());
    let mut length = 0usize;
    // Child ranges are the offset span of each selected parent range. Copying the
    // whole span preserves hidden values under null child rows, just like Arrow's
    // MutableArrayData; validity is copied separately without rebuilding row masks.
    for &(start_row, end_row) in ranges {
        let start = input[start_row].as_usize();
        let end = input[end_row].as_usize();
        for offset in &input[start_row + 1..end_row + 1] {
            let offset = length + offset.as_usize() - start;
            offsets.push(O::usize_as(offset));
        }
        if start != end {
            children.push((start, end));
        }
        length += end - start;
    }
    // Nonnegative range lengths make the final length the maximum output offset.
    // Validate it before exposing the cast offsets to Arrow or copying children.
    O::from_usize(length).ok_or(ArrowError::OffsetOverflowError(length))?;
    let field = match values.data_type() {
        DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
        _ => unreachable!(),
    };
    Ok(Arc::new(GenericListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        copy_ranges(values.values().as_ref(), &children, length)?,
        range_nulls(values, ranges, len),
    )?))
}
// Dispatch once per child array, then copy contiguous typed slices. In particular,
// list<primitive> avoids a MutableArrayData callback and null bookkeeping per row.
#[inline]
fn copy_ranges(values: &dyn Array, ranges: &[(usize, usize)], len: usize) -> Result<ArrayRef> {
    arrow::array::downcast_primitive! {
        values.data_type() => (copy_primitive_helper, values, ranges, len),
        _ => copy_nested_ranges(values, ranges, len)
    }
}

#[inline]
fn copy_nested_ranges(
    values: &dyn Array,
    ranges: &[(usize, usize)],
    len: usize,
) -> Result<ArrayRef> {
    match values.data_type() {
        DataType::List(_) => copy_list_ranges(values.as_list::<i32>(), ranges, len),
        DataType::LargeList(_) => copy_list_ranges(values.as_list::<i64>(), ranges, len),
        DataType::Map(field, sorted) => {
            let lists = map_as_list(values.as_map(), field);
            list_as_map(copy_list_ranges(&lists, ranges, len)?, field, *sorted)
        }
        DataType::Struct(fields) => {
            let columns = values
                .as_struct()
                .columns()
                .iter()
                .map(|column| copy_ranges(column.as_ref(), ranges, len))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                range_nulls(values, ranges, len),
                len,
            )?))
        }
        _ => {
            let data = values.to_data();
            let mut child = MutableArrayData::new(vec![&data], false, len);
            for &(start, end) in ranges {
                child.try_extend(0, start, end)?;
            }
            Ok(make_array(child.freeze()))
        }
    }
}

fn take_indexed_lists<O: OffsetSizeTrait>(
    values: &GenericListArray<O>,
    indices: &UInt32Array,
) -> Result<ArrayRef> {
    let nulls = match values.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => {
            let validity = BooleanArray::new(n.inner().clone(), None);
            let selected = take(&validity, indices, None)?;
            let selected = selected.as_boolean();
            NullBuffer::union(
                Some(&NullBuffer::new(selected.values().clone())),
                selected.nulls(),
            )
        }
        None => indices.nulls().cloned(),
    }
    .filter(|n| n.null_count() > 0);
    let source = values.value_offsets();
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(O::zero());
    let mut ranges = Vec::with_capacity(indices.len());
    let mut length = 0usize;
    let mut append = |index: usize| {
        let start = source[index].as_usize();
        let end = source[index + 1].as_usize();
        length += end - start;
        if start != end {
            ranges.push((start, end));
        }
        offsets.push(O::usize_as(length));
    };
    match &nulls {
        None => {
            for &index in indices.values() {
                append(index as usize);
            }
        }
        Some(n) => {
            for row in n.valid_indices() {
                offsets.resize(row + 1, O::usize_as(length));
                let index = indices.value(row) as usize;
                let start = source[index].as_usize();
                let end = source[index + 1].as_usize();
                length += end - start;
                if start != end {
                    ranges.push((start, end));
                }
                offsets.push(O::usize_as(length));
            }
            offsets.resize(indices.len() + 1, O::usize_as(length));
        }
    }
    O::from_usize(length).ok_or(ArrowError::OffsetOverflowError(length))?;
    let field = match values.data_type() {
        DataType::List(f) | DataType::LargeList(f) => Arc::clone(f),
        _ => unreachable!(),
    };
    Ok(Arc::new(GenericListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        copy_ranges(values.values().as_ref(), &ranges, length)?,
        nulls,
    )?))
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal128Array, Int32Array, ListArray, StringArray, TimestampMillisecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn empty_lists_with_excess_capacity() -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::from_lengths([0, 0]),
            Arc::new(Int32Array::from(Vec::<i32>::with_capacity(8192))),
            None,
        ))
    }

    #[test]
    fn flat_and_normally_sized_results_keep_the_same_array() {
        let flat: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let nested: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::from_lengths([2, 1]),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        ));
        for result in [flat, nested] {
            let compacted = compact_nested_buffers(Arc::clone(&result));
            assert!(Arc::ptr_eq(&result, &compacted));
        }
    }

    #[test]
    fn compaction_releases_only_owned_capacity() {
        let owned = empty_lists_with_excess_capacity();
        let before = owned.get_buffer_memory_size();
        let compacted = compact_nested_buffers(owned);
        assert!(compacted.get_buffer_memory_size() < before);
        assert_eq!(
            compacted.as_list::<i32>().values().get_buffer_memory_size(),
            0
        );
        assert_eq!(compacted.as_list::<i32>().value_offsets(), &[0, 0, 0]);
        compacted.to_data().validate_full().unwrap();

        let shared = empty_lists_with_excess_capacity();
        let before = shared.get_buffer_memory_size();
        let compacted = compact_nested_buffers(Arc::clone(&shared));
        assert_eq!(compacted.to_data(), shared.to_data());
        assert_eq!(compacted.get_buffer_memory_size(), before);
        assert_eq!(shared.get_buffer_memory_size(), before);
    }
    #[test]
    fn selected_ranges_preserve_slices_and_child_nulls() -> Result<()> {
        let numbers: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(-1),
            Some(1),
            None,
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
        ]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("skip"),
            Some("a"),
            None,
            Some("é"),
            Some("longer"),
            None,
            Some(""),
            Some("tail"),
        ]));
        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(-1),
                Some(1),
                None,
                Some(3),
                Some(4),
                None,
                Some(6),
                Some(7),
            ])
            .with_precision_and_scale(12, 2)?,
        );
        let timestamps: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(vec![
                Some(-1),
                Some(1),
                None,
                Some(3),
                Some(4),
                None,
                Some(6),
                Some(7),
            ])
            .with_timezone("UTC"),
        );
        for child in [numbers, strings, decimals, timestamps] {
            let child = child.slice(1, 6);
            let lists: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", child.data_type().clone(), true)),
                OffsetBuffer::from_lengths([1, 2, 1, 2]),
                child,
                Some([true, true, false, true].into_iter().collect()),
            ));
            let lists = lists.slice(1, 3);
            let indices = UInt32Array::from(vec![Some(2), None, Some(0), Some(1), Some(2)]);
            let expected = take(lists.as_ref(), &indices, None)?;
            let input = lists.to_data();
            let actual = take_nested_values(&lists, &indices)?;
            actual.to_data().validate_full()?;
            assert_eq!(actual.data_type(), expected.data_type());
            assert_eq!(actual.to_data(), expected.to_data());
            assert_eq!(lists.to_data(), input);
        }
        Ok(())
    }
    #[test]
    fn list_gather_reports_offset_overflow_before_copying_children() {
        use arrow::array::NullArray;
        let values: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, i32::MAX].into()),
            Arc::new(NullArray::new(i32::MAX as usize)),
            None,
        ));
        let indices = UInt32Array::from(vec![0, 0]);
        assert!(matches!(
            take_nested_values(&values, &indices),
            Err(ArrowError::OffsetOverflowError(_))
        ));
    }
}
