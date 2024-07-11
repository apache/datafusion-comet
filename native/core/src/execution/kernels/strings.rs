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

//! String kernels

use std::sync::Arc;

use arrow::{
    array::*,
    buffer::{Buffer, MutableBuffer},
    compute::kernels::substring::{substring as arrow_substring, substring_by_char},
    datatypes::{DataType, Int32Type},
};

use crate::errors::ExpressionError;

/// Returns an ArrayRef with a string consisting of `length` spaces.
///
/// # Preconditions
///
/// - elements in `length` must not be negative
pub fn string_space(length: &dyn Array) -> Result<ArrayRef, ExpressionError> {
    match length.data_type() {
        DataType::Int32 => {
            let array = length.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(generic_string_space::<i32>(array))
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(length);
            let values = string_space(dict.values())?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        dt => panic!(
            "Unsupported input type for function 'string_space': {:?}",
            dt
        ),
    }
}

pub fn substring(array: &dyn Array, start: i64, length: u64) -> Result<ArrayRef, ExpressionError> {
    match array.data_type() {
        DataType::LargeUtf8 => substring_by_char(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            start,
            Some(length),
        )
        .map_err(|e| e.into())
        .map(|t| make_array(t.into_data())),
        DataType::Utf8 => substring_by_char(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            start,
            Some(length),
        )
        .map_err(|e| e.into())
        .map(|t| make_array(t.into_data())),
        DataType::Binary | DataType::LargeBinary => {
            arrow_substring(array, start, Some(length)).map_err(|e| e.into())
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(array);
            let values = substring(dict.values(), start, length)?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        dt => panic!("Unsupported input type for function 'substring': {:?}", dt),
    }
}

/// Returns an ArrayRef with a substring starting from `start` and length.
///
/// # Preconditions
///
/// - `start` can be negative, in which case the start counts from the end of the string.
/// - `array` must  be either [`StringArray`] or [`LargeStringArray`].
///
/// Note: this is different from arrow-rs `substring` kernel in that both `start` and `length` are
/// `Int32Array` here.
pub fn substring_with_array(
    array: &dyn Array,
    start: &Int32Array,
    length: &Int32Array,
) -> ArrayRef {
    match array.data_type() {
        DataType::LargeUtf8 => generic_substring(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            start,
            length,
            |i| i as i64,
        ),
        DataType::Utf8 => generic_substring(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            start,
            length,
            |i| i,
        ),
        _ => panic!("substring does not support type {:?}", array.data_type()),
    }
}

fn generic_string_space<OffsetSize: OffsetSizeTrait>(length: &Int32Array) -> ArrayRef {
    let array_len = length.len();
    let mut offsets = MutableBuffer::new((array_len + 1) * std::mem::size_of::<OffsetSize>());
    let mut length_so_far = OffsetSize::zero();

    // compute null bitmap (copy)
    let null_bit_buffer = length.to_data().nulls().map(|b| b.buffer().clone());

    // Gets slice of length array to access it directly for performance.
    let length_data = length.to_data();
    let lengths = length_data.buffers()[0].typed_data::<i32>();
    let total = lengths.iter().map(|l| *l as usize).sum::<usize>();
    let mut values = MutableBuffer::new(total);

    offsets.push(length_so_far);

    let blank = " ".as_bytes()[0];
    values.resize(total, blank);

    (0..array_len).for_each(|i| {
        let current_len = lengths[i] as usize;

        length_so_far += OffsetSize::from_usize(current_len).unwrap();
        offsets.push(length_so_far);
    });

    let data = unsafe {
        ArrayData::new_unchecked(
            GenericStringArray::<OffsetSize>::DATA_TYPE,
            array_len,
            None,
            null_bit_buffer,
            0,
            vec![offsets.into(), values.into()],
            vec![],
        )
    };
    make_array(data)
}

fn generic_substring<OffsetSize: OffsetSizeTrait, F>(
    array: &GenericStringArray<OffsetSize>,
    start: &Int32Array,
    length: &Int32Array,
    f: F,
) -> ArrayRef
where
    F: Fn(i32) -> OffsetSize,
{
    assert_eq!(array.len(), start.len());
    assert_eq!(array.len(), length.len());

    // compute current offsets
    let offsets = array.to_data().buffers()[0].clone();
    let offsets: &[OffsetSize] = offsets.typed_data::<OffsetSize>();

    // compute null bitmap (copy)
    let null_bit_buffer = array.to_data().nulls().map(|b| b.buffer().clone());

    // Gets slices of start and length arrays to access them directly for performance.
    let start_data = start.to_data();
    let length_data = length.to_data();
    let starts = start_data.buffers()[0].typed_data::<i32>();
    let lengths = length_data.buffers()[0].typed_data::<i32>();

    // compute values
    let array_data = array.to_data();
    let values = &array_data.buffers()[1];
    let data = values.as_slice();

    // we have no way to estimate how much this will be.
    let mut new_values = MutableBuffer::new(0);
    let mut new_offsets: Vec<OffsetSize> = Vec::with_capacity(array.len() + 1);

    let mut length_so_far = OffsetSize::zero();
    new_offsets.push(length_so_far);
    (0..array.len()).for_each(|i| {
        // the length of this entry
        let length_i: OffsetSize = offsets[i + 1] - offsets[i];
        // compute where we should start slicing this entry
        let start_pos: OffsetSize = f(starts[i]);

        let start = offsets[i]
            + if start_pos >= OffsetSize::zero() {
                start_pos
            } else {
                length_i + start_pos
            };

        let start = start.clamp(offsets[i], offsets[i + 1]);
        // compute the length of the slice
        let slice_length: OffsetSize = f(lengths[i].max(0)).min(offsets[i + 1] - start);

        length_so_far += slice_length;

        new_offsets.push(length_so_far);

        // we need usize for ranges
        let start = start.to_usize().unwrap();
        let slice_length = slice_length.to_usize().unwrap();

        new_values.extend_from_slice(&data[start..start + slice_length]);
    });

    let data = unsafe {
        ArrayData::new_unchecked(
            GenericStringArray::<OffsetSize>::DATA_TYPE,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![Buffer::from_slice_ref(&new_offsets), new_values.into()],
            vec![],
        )
    };
    make_array(data)
}
