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

use crate::spark_unsafe::{
    map::append_map_elements,
    row::{append_field, downcast_builder_ref, SparkUnsafeRow},
    unsafe_object::{impl_primitive_accessors, SparkUnsafeObject},
};
use arrow::array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
        ListBuilder, NullBuilder, StringBuilder, StructBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    MapBuilder, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{
    DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
};
use datafusion_comet_jni_bridge::errors::CometError;
use std::sync::Arc;

/// Element count from which a nullable array with no null elements is appended with one copy
/// instead of element by element. Below it the loop is as fast: checking the null bitset and
/// calling `append_slice` cost about as much as a few appends, and arrays of up to five elements
/// with some null elements got slower with the copy. Arrays of 16 elements take about 40% of the
/// time with the copy, and arrays of 50 about a fifth.
const MIN_BULK_APPEND_ELEMENTS: usize = 8;

/// Element count from which an aligned nullable array that holds a null is appended with one copy
/// of its values and a validity buffer built from its null bitset, instead of element by element.
/// Building the two buffers takes four allocations, which cost as much as appending about 50
/// elements one by one, so shorter arrays stay on the loop. The `list_with_one_null` group of the
/// `array_element_append` benchmark times lengths on both sides of the cutoff, and its doc comment
/// shows how to compare the two paths. On an M3 Max with the system allocator, arrays of 32
/// elements took 1.4 times as long with the copy, and arrays of 64 took 69% of the time for 4-byte
/// elements and 86% for 8-byte ones.
const MIN_BULK_NULLABLE_APPEND_ELEMENTS: usize = 64;

/// Generates bulk append methods for primitive types in SparkUnsafeArray.
///
/// # Safety invariants for all generated methods:
/// - `element_offset` points to contiguous element data of length `num_elements`
/// - `null_bitset_ptr()` returns a pointer to `ceil(num_elements/64)` i64 words
/// - These invariants are guaranteed by the SparkUnsafeArray layout from the JVM
///
/// A timestamp appender also takes `timezone`, which must be the builder's timezone, because
/// `append_array` requires the array's data type to match the builder's. A mismatch panics in
/// `append_array` for an aligned array of `MIN_BULK_NULLABLE_APPEND_ELEMENTS` or more elements that
/// holds a null, and goes unnoticed for every other array.
macro_rules! impl_append_to_builder {
    ($method_name:ident, $builder_type:ty, $element_type:ty, $arrow_type:ty
        $(, $timezone:ident)?) => {
        pub(crate) fn $method_name<const NULLABLE: bool>(
            &self,
            builder: &mut $builder_type,
            $($timezone: &Option<Arc<str>>,)?
        ) {
            let num_elements = self.num_elements;
            if num_elements == 0 {
                return;
            }
            debug_assert!(self.element_offset != 0, "element_offset is null");
            let ptr = self.element_offset as *const $element_type;
            // An array can start at an address that is not aligned for its elements (see
            // `SparkUnsafeObject`), and only aligned elements can be read as a slice.
            let aligned = (ptr as usize).is_multiple_of(std::mem::align_of::<$element_type>());

            // A nullable array without nulls takes the copy below too, once it is long enough.
            if NULLABLE && (num_elements < MIN_BULK_APPEND_ELEMENTS || self.has_null()) {
                let null_words = self.null_bitset_ptr();
                if aligned && num_elements >= MIN_BULK_NULLABLE_APPEND_ELEMENTS {
                    // SAFETY: element_offset points to num_elements aligned elements.
                    let values = unsafe { std::slice::from_raw_parts(ptr, num_elements) };
                    let null_mask_len = num_elements.div_ceil(8);
                    // SAFETY: the ceil(num_elements/64) words of the null bitset cover these
                    // bytes. The words are little-endian, so byte i holds the bits of elements
                    // 8i to 8i+7, least significant first, as in an Arrow bitmap.
                    let null_mask = unsafe {
                        std::slice::from_raw_parts(null_words as *const u8, null_mask_len)
                    };
                    // Spark sets the bit of a null element and Arrow the bit of a valid one. The
                    // padding bits past the last element turn to ones, outside the buffer's length.
                    let flipped: Vec<u8> = null_mask.iter().map(|byte| !byte).collect();
                    let validity =
                        NullBuffer::new(BooleanBuffer::new(Buffer::from(flipped), 0, num_elements));
                    let arr = PrimitiveArray::<$arrow_type>::new(
                        ScalarBuffer::from(Buffer::from_slice_ref(values)),
                        Some(validity),
                    );
                    $(let arr = arr.with_timezone_opt($timezone.clone());)?
                    builder.append_array(&arr);
                } else {
                    let mut ptr = ptr;
                    for idx in 0..num_elements {
                        // SAFETY: null_words has ceil(num_elements/64) words, idx < num_elements
                        if unsafe { Self::is_null_in_bitset(null_words, idx) } {
                            builder.append_null();
                        } else {
                            // SAFETY: ptr is within element data bounds
                            builder.append_value(unsafe { ptr.read_unaligned() });
                        }
                        // SAFETY: ptr stays within bounds, iterating num_elements times
                        ptr = unsafe { ptr.add(1) };
                    }
                }
            } else if aligned {
                // SAFETY: element_offset points to num_elements aligned elements.
                let slice = unsafe { std::slice::from_raw_parts(ptr, num_elements) };
                builder.append_slice(slice);
            } else {
                let mut ptr = ptr;
                for _ in 0..num_elements {
                    builder.append_value(unsafe { ptr.read_unaligned() });
                    ptr = unsafe { ptr.add(1) };
                }
            }
        }
    };
}

/// A Spark `UnsafeArray` backed by JVM-allocated memory, providing element access by index.
pub struct SparkUnsafeArray {
    row_addr: i64,
    num_elements: usize,
    element_offset: i64,
}

impl SparkUnsafeObject for SparkUnsafeArray {
    #[inline]
    fn get_row_addr(&self) -> i64 {
        self.row_addr
    }

    #[inline]
    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8 {
        (self.element_offset + (index * element_size) as i64) as *const u8
    }

    // SparkUnsafeArray base address may be unaligned when nested within a row's variable-length
    // region, so we must use ptr::read_unaligned() for all typed accesses.
    impl_primitive_accessors!(read_unaligned);
}

impl SparkUnsafeArray {
    /// Creates a `SparkUnsafeArray` which points to the given address and size in bytes.
    pub fn new(addr: i64) -> Self {
        // SAFETY: addr points to valid Spark UnsafeArray data from the JVM.
        // The first 8 bytes contain the element count as a little-endian i64.
        debug_assert!(addr != 0, "SparkUnsafeArray::new: null address");
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, 8) };
        let num_elements = i64::from_le_bytes(slice.try_into().unwrap());

        if num_elements < 0 {
            panic!("Negative number of elements: {num_elements}");
        }

        if num_elements > i32::MAX as i64 {
            panic!("Number of elements should <= i32::MAX: {num_elements}");
        }

        Self {
            row_addr: addr,
            num_elements: num_elements as usize,
            element_offset: addr + Self::get_header_portion_in_bytes(num_elements),
        }
    }

    pub(crate) fn get_num_elements(&self) -> usize {
        self.num_elements
    }

    /// Returns the size of array header in bytes.
    #[inline]
    const fn get_header_portion_in_bytes(num_fields: i64) -> i64 {
        8 + ((num_fields + 63) / 64) * 8
    }

    /// Returns true if the null bit at the given index of the array is set.
    #[inline]
    pub(crate) fn is_null_at(&self, index: usize) -> bool {
        // SAFETY: row_addr points to valid Spark UnsafeArray data. The null bitset starts
        // at offset 8 and contains ceil(num_elements/64) * 8 bytes. The caller ensures
        // index < num_elements, so word_offset is within the bitset region.
        debug_assert!(
            index < self.num_elements,
            "is_null_at: index {index} >= num_elements {}",
            self.num_elements
        );
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + 8 + (((index >> 6) as i64) << 3)) as *const i64;
            let word: i64 = word_offset.read_unaligned();
            (word & mask) != 0
        }
    }

    /// Returns the null bitset pointer (starts at row_addr + 8).
    #[inline]
    fn null_bitset_ptr(&self) -> *const i64 {
        (self.row_addr + 8) as *const i64
    }

    /// Returns true if any element is null, checking the null bitset a word at a time.
    #[inline]
    fn has_null(&self) -> bool {
        let null_words = self.null_bitset_ptr();
        // SAFETY: the null bitset holds ceil(num_elements/64) words. Spark zeroes the bits past
        // the last element. A stray one would send the array to the per-element loop, or to the
        // validity buffer if it is aligned and long enough, and both ignore the bits past the
        // last element.
        (0..self.num_elements.div_ceil(64))
            .any(|word| unsafe { null_words.add(word).read_unaligned() } != 0)
    }

    /// Checks whether the null bit at `idx` is set in the given null bitset pointer.
    ///
    /// # Safety
    /// `null_words` must point to at least `ceil((idx+1)/64)` i64 words.
    #[inline]
    unsafe fn is_null_in_bitset(null_words: *const i64, idx: usize) -> bool {
        let word_idx = idx >> 6;
        let bit_idx = idx & 0x3f;
        (null_words.add(word_idx).read_unaligned() & (1i64 << bit_idx)) != 0
    }

    impl_append_to_builder!(append_ints_to_builder, Int32Builder, i32, Int32Type);
    impl_append_to_builder!(append_longs_to_builder, Int64Builder, i64, Int64Type);
    impl_append_to_builder!(
        append_time64s_to_builder,
        Time64NanosecondBuilder,
        i64,
        Time64NanosecondType
    );
    impl_append_to_builder!(append_shorts_to_builder, Int16Builder, i16, Int16Type);
    impl_append_to_builder!(append_bytes_to_builder, Int8Builder, i8, Int8Type);
    impl_append_to_builder!(append_floats_to_builder, Float32Builder, f32, Float32Type);
    impl_append_to_builder!(append_doubles_to_builder, Float64Builder, f64, Float64Type);
    impl_append_to_builder!(
        append_timestamps_to_builder,
        TimestampMicrosecondBuilder,
        i64,
        TimestampMicrosecondType,
        timezone
    );
    impl_append_to_builder!(append_dates_to_builder, Date32Builder, i32, Date32Type);

    /// Bulk append boolean values to builder.
    /// Booleans are stored as 1 byte each in SparkUnsafeArray, requiring special handling.
    pub(crate) fn append_booleans_to_builder<const NULLABLE: bool>(
        &self,
        builder: &mut BooleanBuilder,
    ) {
        let num_elements = self.num_elements;
        if num_elements == 0 {
            return;
        }
        debug_assert!(
            self.element_offset != 0,
            "append_booleans: element_offset pointer is null"
        );
        // SAFETY: element_offset points to num_elements one-byte booleans, which need no
        // alignment.
        let values =
            unsafe { std::slice::from_raw_parts(self.element_offset as *const u8, num_elements) };

        if NULLABLE {
            let null_words = self.null_bitset_ptr();
            for (idx, &value) in values.iter().enumerate() {
                // SAFETY: null_words has ceil(num_elements/64) words, idx < num_elements
                if unsafe { Self::is_null_in_bitset(null_words, idx) } {
                    builder.append_null();
                } else {
                    builder.append_value(value != 0);
                }
            }
        } else {
            for &value in values {
                builder.append_value(value != 0);
            }
        }
    }
}

pub fn append_to_builder<const NULLABLE: bool>(
    data_type: &DataType,
    builder: &mut dyn ArrayBuilder,
    array: &SparkUnsafeArray,
) -> Result<(), CometError> {
    macro_rules! add_values {
        ($builder_type:ty, $add_value:expr, $add_null:expr) => {
            let builder = downcast_builder_ref!($builder_type, builder);
            for idx in 0..array.get_num_elements() {
                if NULLABLE && array.is_null_at(idx) {
                    $add_null(builder);
                } else {
                    $add_value(builder, array, idx);
                }
            }
        };
    }

    match data_type {
        DataType::Boolean => {
            let builder = downcast_builder_ref!(BooleanBuilder, builder);
            array.append_booleans_to_builder::<NULLABLE>(builder);
        }
        DataType::Int8 => {
            let builder = downcast_builder_ref!(Int8Builder, builder);
            array.append_bytes_to_builder::<NULLABLE>(builder);
        }
        DataType::Int16 => {
            let builder = downcast_builder_ref!(Int16Builder, builder);
            array.append_shorts_to_builder::<NULLABLE>(builder);
        }
        DataType::Int32 => {
            let builder = downcast_builder_ref!(Int32Builder, builder);
            array.append_ints_to_builder::<NULLABLE>(builder);
        }
        DataType::Int64 => {
            let builder = downcast_builder_ref!(Int64Builder, builder);
            array.append_longs_to_builder::<NULLABLE>(builder);
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let builder = downcast_builder_ref!(Time64NanosecondBuilder, builder);
            array.append_time64s_to_builder::<NULLABLE>(builder);
        }
        DataType::Float32 => {
            let builder = downcast_builder_ref!(Float32Builder, builder);
            array.append_floats_to_builder::<NULLABLE>(builder);
        }
        DataType::Float64 => {
            let builder = downcast_builder_ref!(Float64Builder, builder);
            array.append_doubles_to_builder::<NULLABLE>(builder);
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let builder = downcast_builder_ref!(TimestampMicrosecondBuilder, builder);
            array.append_timestamps_to_builder::<NULLABLE>(builder, tz);
        }
        DataType::Date32 => {
            let builder = downcast_builder_ref!(Date32Builder, builder);
            array.append_dates_to_builder::<NULLABLE>(builder);
        }
        DataType::Null => {
            let builder = downcast_builder_ref!(NullBuilder, builder);
            for _ in 0..array.get_num_elements() {
                builder.append_null();
            }
        }
        DataType::Binary => {
            add_values!(
                BinaryBuilder,
                |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_binary(idx)),
                |builder: &mut BinaryBuilder| builder.append_null()
            );
        }
        DataType::Utf8 => {
            add_values!(
                StringBuilder,
                |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_string(idx)),
                |builder: &mut StringBuilder| builder.append_null()
            );
        }
        DataType::List(field) => {
            let builder = downcast_builder_ref!(ListBuilder<Box<dyn ArrayBuilder>>, builder);
            for idx in 0..array.get_num_elements() {
                if NULLABLE && array.is_null_at(idx) {
                    builder.append_null();
                } else {
                    let nested_array = array.get_array(idx);
                    append_list_element(field.data_type(), builder, &nested_array)?;
                };
            }
        }
        DataType::Struct(fields) => {
            let builder = downcast_builder_ref!(StructBuilder, builder);
            for idx in 0..array.get_num_elements() {
                let nested_row = if NULLABLE && array.is_null_at(idx) {
                    builder.append_null();
                    SparkUnsafeRow::default()
                } else {
                    builder.append(true);
                    array.get_struct(idx, fields.len())
                };

                for (field_idx, field) in fields.into_iter().enumerate() {
                    append_field(field.data_type(), builder, &nested_row, field_idx)?;
                }
            }
        }
        DataType::Decimal128(p, _) => {
            add_values!(
                Decimal128Builder,
                |builder: &mut Decimal128Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_decimal(idx, *p)),
                |builder: &mut Decimal128Builder| builder.append_null()
            );
        }
        DataType::Map(field, _) => {
            let builder = downcast_builder_ref!(
                MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
                builder
            );
            for idx in 0..array.get_num_elements() {
                if NULLABLE && array.is_null_at(idx) {
                    builder.append(false)?;
                } else {
                    let nested_map = array.get_map(idx);
                    append_map_elements(field, builder, &nested_map)?;
                };
            }
        }
        _ => {
            return Err(CometError::Internal(format!(
                "Unsupported map data type: {:?}",
                data_type
            )))
        }
    }

    Ok(())
}

/// Appending the given list stored in `SparkUnsafeArray` into `ListBuilder`.
/// `element_dt` is the data type of the list element. `list_builder` is the list builder.
/// `list` is the list stored in `SparkUnsafeArray`.
pub fn append_list_element(
    element_dt: &DataType,
    list_builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    list: &SparkUnsafeArray,
) -> Result<(), CometError> {
    append_to_builder::<true>(element_dt, list_builder.values(), list)?;
    list_builder.append(true);

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::builder::PrimitiveBuilder;
    use arrow::array::Array;
    use arrow::datatypes::{
        ArrowPrimitiveType, Date32Type, Int16Type, Int32Type, Int64Type, Int8Type,
        TimestampMicrosecondType,
    };

    /// Lays `values` out as a Spark `UnsafeArrayData` of `width`-byte elements: the element
    /// count, the null bitset, then the elements.
    fn unsafe_array_bytes(values: &[Option<i64>], width: usize) -> Vec<u8> {
        let bitset_words = values.len().div_ceil(64);
        let mut bitset = vec![0u64; bitset_words];
        let mut data = vec![0u8; (values.len() * width).div_ceil(8) * 8];
        for (i, value) in values.iter().enumerate() {
            match value {
                None => bitset[i / 64] |= 1 << (i % 64),
                Some(v) => {
                    data[i * width..(i + 1) * width].copy_from_slice(&v.to_le_bytes()[..width])
                }
            }
        }
        let mut bytes = (values.len() as u64).to_le_bytes().to_vec();
        bitset
            .iter()
            .for_each(|word| bytes.extend_from_slice(&word.to_le_bytes()));
        bytes.extend_from_slice(&data);
        bytes
    }

    /// Appends `values`, laid out `shift` bytes past an 8-byte boundary, through
    /// `append_to_builder::<true>` and reads them back.
    fn round_trip<T>(data_type: DataType, values: &[Option<i64>], shift: usize) -> Vec<Option<i64>>
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i64>,
    {
        let bytes = unsafe_array_bytes(values, std::mem::size_of::<T::Native>());
        let mut words = vec![0u64; (shift + bytes.len()).div_ceil(8)];
        // SAFETY: `words` covers `shift + bytes.len()` bytes.
        let base = unsafe { (words.as_mut_ptr() as *mut u8).add(shift) };
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), base, bytes.len()) };
        let array = SparkUnsafeArray::new(base as i64);
        let mut builder = PrimitiveBuilder::<T>::new().with_data_type(data_type.clone());
        append_to_builder::<true>(&data_type, &mut builder, &array).unwrap();
        builder.finish().iter().map(|v| v.map(Into::into)).collect()
    }

    #[test]
    fn nullable_primitive_arrays_round_trip_on_every_append_path() {
        let long: Vec<Option<i64>> = (0..70).map(Some).collect();
        let mut null_in_second_bitset_word = long.clone();
        null_in_second_bitset_word[66] = None;
        let nulls_at_both_ends = |len: i64| -> Vec<Option<i64>> {
            (0..len)
                .map(|v| (v != 0 && v != len - 1).then_some(-v))
                .collect()
        };
        let cases = [
            vec![],
            // Shorter than MIN_BULK_APPEND_ELEMENTS: element by element.
            vec![Some(1), Some(-2), Some(3)],
            // Holds a null: element by element.
            vec![Some(1), None, Some(3), Some(-4), Some(5)],
            // No nulls and long enough: one copy.
            vec![
                Some(-1),
                Some(2),
                Some(-3),
                Some(4),
                Some(-5),
                Some(6),
                Some(-7),
                Some(8),
            ],
            long,
            // Holds a null, one element short of MIN_BULK_NULLABLE_APPEND_ELEMENTS: element by
            // element.
            nulls_at_both_ends(63),
            // Holds a null and long enough: one copy and a validity buffer.
            nulls_at_both_ends(64),
            // The null is only in the second bitset word, which the copy check must read.
            null_in_second_bitset_word,
        ];
        for values in &cases {
            // An 8-byte boundary, as inside an UnsafeRow, and a misaligned one for every width.
            for shift in [0, 1, 4] {
                assert_eq!(
                    round_trip::<Int8Type>(DataType::Int8, values, shift),
                    *values
                );
                assert_eq!(
                    round_trip::<Int16Type>(DataType::Int16, values, shift),
                    *values
                );
                assert_eq!(
                    round_trip::<Int32Type>(DataType::Int32, values, shift),
                    *values
                );
                assert_eq!(
                    round_trip::<Int64Type>(DataType::Int64, values, shift),
                    *values
                );
                assert_eq!(
                    round_trip::<Date32Type>(DataType::Date32, values, shift),
                    *values
                );
                assert_eq!(
                    round_trip::<TimestampMicrosecondType>(
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        values,
                        shift
                    ),
                    *values
                );
                // The builder carries the column's timezone, which `append_array` checks.
                assert_eq!(
                    round_trip::<TimestampMicrosecondType>(
                        DataType::Timestamp(TimeUnit::Microsecond, Some("America/Denver".into())),
                        values,
                        shift
                    ),
                    *values
                );
            }
        }
    }

    fn make_i32_array(num_elements: usize, null_indices: &[usize]) -> Vec<u8> {
        let null_bitset_words = num_elements.div_ceil(64);
        let header_size = 8 + null_bitset_words * 8;
        let mut buffer = vec![0u8; header_size + num_elements * 4];

        buffer[0..8].copy_from_slice(&(num_elements as i64).to_le_bytes());

        for &i in null_indices {
            let word_offset = 8 + (i / 64) * 8;
            let word = i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            buffer[word_offset..word_offset + 8]
                .copy_from_slice(&(word | (1i64 << (i % 64))).to_le_bytes());
        }

        for i in 0..num_elements {
            let offset = header_size + i * 4;
            buffer[offset..offset + 4].copy_from_slice(&(i as i32).to_le_bytes());
        }
        buffer
    }

    /// Copies `bytes` to an 8-byte boundary, as inside an UnsafeRow.
    fn aligned_copy(bytes: &[u8]) -> Vec<u64> {
        let mut words = vec![0u64; bytes.len().div_ceil(8)];
        // SAFETY: `words` covers `bytes.len()` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                words.as_mut_ptr() as *mut u8,
                bytes.len(),
            )
        };
        words
    }

    fn append_i32(buffer: &[u8]) -> arrow::array::Int32Array {
        let words = aligned_copy(buffer);
        let array = SparkUnsafeArray::new(words.as_ptr() as i64);
        let mut builder = Int32Builder::new();
        append_to_builder::<true>(&DataType::Int32, &mut builder, &array).unwrap();
        builder.finish()
    }

    #[test]
    fn test_nullable_null_in_last_byte() {
        // None of the lengths is a multiple of 8, so the null sits in a partial last byte. The
        // first three take the per-element loop, the rest the validity buffer.
        for num_elements in [7, 9, 17, 65, 71, 73, 81] {
            let last = num_elements - 1;
            let arr = append_i32(&make_i32_array(num_elements, &[last]));
            assert_eq!(arr.len(), num_elements);
            assert_eq!(arr.null_count(), 1);
            assert!(arr.is_null(last));
            for i in 0..last {
                assert!(arr.is_valid(i));
                assert_eq!(arr.value(i), i as i32);
            }
        }
    }

    #[test]
    fn test_all_null() {
        for num_elements in [9, 73] {
            let indices: Vec<usize> = (0..num_elements).collect();
            let arr = append_i32(&make_i32_array(num_elements, &indices));
            assert_eq!(arr.len(), num_elements);
            assert_eq!(arr.null_count(), num_elements);
        }
    }

    #[test]
    fn test_all_valid() {
        for num_elements in [9, 73] {
            let arr = append_i32(&make_i32_array(num_elements, &[]));
            assert_eq!(arr.len(), num_elements);
            assert_eq!(arr.null_count(), 0);
        }
    }

    #[test]
    fn nullable_arrays_append_into_one_builder() {
        // The list path appends every row's array to one values builder, so the validity buffer
        // of a long array is appended at an offset that is not a whole byte.
        let arrays: [Vec<Option<i64>>; 4] = [
            vec![Some(1), None, Some(3)],
            (0..70).map(|v| (v % 9 != 4).then_some(v)).collect(),
            vec![None; 5],
            (0..65).map(|v| (v != 64).then_some(-v)).collect(),
        ];
        let mut builder = Int32Builder::new();
        for values in &arrays {
            let words = aligned_copy(&unsafe_array_bytes(values, 4));
            let array = SparkUnsafeArray::new(words.as_ptr() as i64);
            append_to_builder::<true>(&DataType::Int32, &mut builder, &array).unwrap();
        }
        let appended: Vec<Option<i64>> =
            builder.finish().iter().map(|v| v.map(i64::from)).collect();
        assert_eq!(appended, arrays.concat());
    }
}
