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

use crate::{
    common::{bit, ValueGetter},
    BoolType, DataType, TypeTrait, BITS_PER_BYTE, STRING_VIEW_LEN, STRING_VIEW_PREFIX_LEN,
};
use arrow::{
    array::{Array, ArrayRef},
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType as ArrowDataType,
};
use arrow_data::ArrayData;

/// A vector that holds elements of plain types (i.e., no nested type such as list, map, struct).
pub struct PlainVector {
    /// The data type for elements in this vector
    data_type: DataType,
    /// Total number of values in this vector
    num_values: usize,
    /// Total number of nulls in this vector. Must <= `num_values`.
    num_nulls: usize,
    /// The value buffer
    value_buffer: ValueBuffer,
    /// Number of bytes for each element in the vector. For variable length types such as string
    /// and binary, this will be the size of [`StringView`] which is always 16 bytes.
    value_size: usize,
    /// Offsets into buffers
    offset: usize,
    /// The validity buffer. If empty, all values in this vector are not null.
    validity_buffer: Option<Buffer>,
    /// Whether this vector is dictionary encoded
    is_dictionary: bool,
    /// Indices (or dictionary keys) when `is_dictionary` is true. Otherwise, this is always
    /// an empty vector.
    indices: IndexBuffer,
}

impl<T: TypeTrait> ValueGetter<T> for PlainVector {
    default fn value(&self, idx: usize) -> T::Native {
        let offset = self.offset(idx);
        unsafe {
            let ptr = self.value_buffer.as_ptr() as *const T::Native;
            *ptr.add(offset)
        }
    }
}

impl ValueGetter<BoolType> for PlainVector {
    fn value(&self, idx: usize) -> bool {
        let offset = self.offset(idx);
        unsafe { bit::get_bit_raw(self.value_buffer.as_ptr(), offset) }
    }
}

impl PlainVector {
    /// Returns the data type of this vector.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the total number of elements in this vector.
    pub fn num_values(&self) -> usize {
        self.num_values
    }

    /// Returns the total number of nulls in this vector.
    pub fn num_nulls(&self) -> usize {
        self.num_nulls
    }

    /// Whether there is any null in this vector.
    pub fn has_null(&self) -> bool {
        self.num_nulls > 0
    }

    /// Whether the element at `idx` is null.
    pub fn is_null(&self, idx: usize) -> bool {
        if let Some(validity_buffer) = &self.validity_buffer {
            unsafe {
                return !bit::get_bit_raw(validity_buffer.as_ptr(), self.offset + idx);
            }
        }

        false
    }

    #[inline(always)]
    pub fn value<T: TypeTrait>(&self, idx: usize) -> T::Native {
        <dyn ValueGetter<T>>::value(self, idx)
    }

    #[inline(always)]
    fn offset(&self, idx: usize) -> usize {
        let idx = self.offset + idx;
        if self.is_dictionary {
            self.indices.get(idx)
        } else {
            idx
        }
    }
}

impl From<ArrayData> for PlainVector {
    fn from(data: ArrayData) -> Self {
        assert!(!data.buffers().is_empty(), "expected at least one buffer");
        let arrow_dt = data.data_type();
        let dt: DataType = arrow_dt.into();
        let is_dictionary = matches!(arrow_dt, ArrowDataType::Dictionary(_, _));

        let mut value_buffers = data.buffers();
        let mut indices = IndexBuffer::empty();
        let validity_buffer = data.nulls().map(|nb| nb.buffer().clone());

        if is_dictionary {
            // in case of dictionary data, the dictionary values are stored in child data, while
            // dictionary keys are stored in `value_buffer`.
            assert_eq!(
                data.child_data().len(),
                1,
                "child data should contain a single array"
            );
            let child_data = &data.child_data()[0];
            indices = IndexBuffer::new(value_buffers[0].clone(), data.len() + data.offset());
            value_buffers = child_data.buffers();
        }

        let value_size = dt.kind().type_size() / BITS_PER_BYTE;
        let value_buffer = ValueBuffer::new(&dt, value_buffers.to_vec(), data.len());

        Self {
            data_type: dt,
            num_values: data.len(),
            num_nulls: data.null_count(),
            value_buffer,
            value_size,
            offset: data.offset(),
            validity_buffer,
            is_dictionary,
            indices,
        }
    }
}

impl From<ArrayRef> for PlainVector {
    fn from(value: ArrayRef) -> Self {
        Self::from(value.into_data())
    }
}

struct ValueBuffer {
    ptr: *const u8,
    /// Keep the `ptr` alive
    original_buffers: Vec<Buffer>,
}

impl ValueBuffer {
    pub fn new(dt: &DataType, buffers: Vec<Buffer>, len: usize) -> Self {
        if matches!(dt, DataType::String | DataType::Binary) {
            assert_eq!(
                2,
                buffers.len(),
                "expected two buffers (offset, value) for string/binary"
            );

            let mut string_view_buf = MutableBuffer::from_len_zeroed(len * 16);
            let buf_mut = string_view_buf.as_mut_ptr();

            let offsets = buffers[0].as_ptr() as *const i32;
            let values = buffers[1].as_ptr();

            let mut dst_offset = 0;
            let mut start = 0;
            unsafe {
                for i in 0..len {
                    // StringView format:
                    //   - length (4 bytes)
                    //   - first 4 bytes of the string/binary (4 bytes)
                    //   - pointer to the string/binary (8 bytes)
                    let end = *offsets.add(i + 1);
                    let len = end - start;
                    let value = values.add(start as usize);
                    *(buf_mut.add(dst_offset) as *mut i32) = len;
                    if len >= STRING_VIEW_PREFIX_LEN as i32 {
                        // only store prefix if the string has at least 4 bytes, otherwise, we'll
                        // zero pad the bytes.
                        std::ptr::copy_nonoverlapping(
                            value,
                            buf_mut.add(dst_offset + STRING_VIEW_PREFIX_LEN),
                            STRING_VIEW_PREFIX_LEN,
                        );
                    }
                    *(buf_mut.add(dst_offset + STRING_VIEW_PREFIX_LEN + 4) as *mut usize) =
                        value as usize;
                    start = end;
                    dst_offset += STRING_VIEW_LEN;
                }
            }

            let string_buffer: Buffer = string_view_buf.into();
            let ptr = string_buffer.as_ptr();

            Self {
                ptr,
                original_buffers: vec![string_buffer, buffers[1].clone()],
            }
        } else {
            let ptr = buffers[0].as_ptr();
            Self {
                ptr,
                original_buffers: buffers,
            }
        }
    }

    /// Returns the raw pointer for the data in this value buffer.
    /// NOTE: caller of this should NOT store the raw pointer to avoid dangling pointers.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
}

struct IndexBuffer {
    ptr: *const u8,
    /// Keep the `ptr` alive.
    buf: Option<Buffer>,
    /// Total number of elements in the index buffer
    len: usize,
}

impl IndexBuffer {
    pub fn new(buf: Buffer, len: usize) -> Self {
        let ptr = buf.as_ptr();
        Self {
            buf: Some(buf),
            ptr,
            len,
        }
    }

    pub fn empty() -> Self {
        Self {
            buf: None,
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    #[inline]
    pub fn get(&self, i: usize) -> usize {
        debug_assert!(i < self.len);
        unsafe {
            let ptr = self.ptr as *const i32;
            *ptr.add(i) as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BoolType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType,
        NativeEqual, ShortType, StringType, TimestampType, TypeTrait, STRING_VIEW_PREFIX_LEN,
    };

    use crate::common::vector::PlainVector;
    use arrow::{
        array::{
            Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
            Int16Array, Int32Array, Int8Array, StringArray,
        },
        buffer::Buffer,
        datatypes::{DataType as ArrowDataType, ToByteSlice},
    };
    use arrow_array::TimestampMicrosecondArray;
    use arrow_data::ArrayData;

    #[test]
    fn primitive_no_null() {
        let arr = Int32Array::from(vec![0, 1, 2, 3, 4]);
        let vector = PlainVector::from(arr.into_data());

        assert_eq!(5, vector.num_values());
        assert_eq!(0, vector.num_nulls());
        assert_eq!(4, vector.value_size);
        assert!(vector.validity_buffer.is_none());

        for i in 0..5 {
            assert!(!vector.is_null(i));
            assert_eq!(i as i32, vector.value::<IntegerType>(i))
        }
    }

    fn check_answer<T: TypeTrait>(expected: &[Option<T::Native>], actual: &PlainVector) {
        assert_eq!(expected.len(), actual.num_values());
        let nulls = expected
            .iter()
            .filter(|v| v.is_none())
            .collect::<Vec<&Option<T::Native>>>();
        assert_eq!(nulls.len(), actual.num_nulls());

        for i in 0..expected.len() {
            if let Some(v) = expected[i] {
                assert!(!actual.is_null(i));
                assert!(v.is_equal(&actual.value::<T>(i)));
            } else {
                assert!(actual.is_null(i));
            }
        }
    }

    #[test]
    fn primitive_with_nulls() {
        let data = vec![Some(0), None, Some(2), None, Some(4)];
        let arr = TimestampMicrosecondArray::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<TimestampType>(&data, &vector);
    }

    #[test]
    fn primitive_with_offsets_nulls() {
        let arr = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4), None, Some(7)]);
        let data = arr.into_data();
        let vector = PlainVector::from(data.slice(2, 3));

        assert_eq!(3, vector.num_values());
        assert_eq!(1, vector.num_nulls());

        for i in 0..2 {
            if i % 2 == 0 {
                assert!(!vector.is_null(i));
                assert_eq!((i + 2) as i32, vector.value::<IntegerType>(i));
            } else {
                assert!(vector.is_null(i));
            }
        }
    }

    #[test]
    fn primitive_dictionary() {
        let value_data = ArrayData::builder(ArrowDataType::Int8)
            .len(8)
            .add_buffer(Buffer::from(
                &[10_i8, 11, 12, 13, 14, 15, 16, 17].to_byte_slice(),
            ))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        let keys = Buffer::from(&[2_i32, 3, 4].to_byte_slice());

        // Construct a dictionary array from the above two
        let key_type = ArrowDataType::Int32;
        let value_type = ArrowDataType::Int8;
        let dict_data_type = ArrowDataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let dict_data = ArrayData::builder(dict_data_type)
            .len(3)
            .add_buffer(keys)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let vector = PlainVector::from(dict_data);

        assert_eq!(DataType::Byte, *vector.data_type());
        assert_eq!(3, vector.num_values());
        assert_eq!(0, vector.num_nulls());
        assert!(!vector.has_null());
        assert_eq!(12, vector.value::<ByteType>(0));
        assert_eq!(13, vector.value::<ByteType>(1));
        assert_eq!(14, vector.value::<ByteType>(2));
    }

    #[test]
    fn bools() {
        let data = vec![Some(true), None, Some(false), None, Some(true)];
        let arr = BooleanArray::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<BoolType>(&data, &vector);
    }

    #[test]
    fn bytes() {
        let data = vec![Some(4_i8), None, None, Some(5_i8), Some(7_i8)];
        let arr = Int8Array::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<ByteType>(&data, &vector);
    }

    #[test]
    fn shorts() {
        let data = vec![Some(4_i16), None, None, Some(-40_i16), Some(-3_i16)];
        let arr = Int16Array::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<ShortType>(&data, &vector);
    }

    #[test]
    fn floats() {
        let data = vec![
            Some(4.0_f32),
            Some(-0.0_f32),
            Some(-3.0_f32),
            Some(0.0_f32),
            Some(std::f32::consts::PI),
        ];
        let arr = Float32Array::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<FloatType>(&data, &vector);
    }

    #[test]
    fn doubles() {
        let data = vec![
            None,
            Some(std::f64::consts::PI),
            Some(4.0_f64),
            Some(f64::NAN),
        ];
        let arr = Float64Array::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<DoubleType>(&data, &vector);
    }

    #[test]
    fn decimals() {
        let data = vec![Some(1_i128), None, None, Some(i128::MAX)];
        let arr = Decimal128Array::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<DecimalType>(&data, &vector);
    }

    #[test]
    fn timestamps() {
        // 1:        00:00:00.001
        // 37800005: 10:30:00.005
        // 86399210: 23:59:59.210
        let data = vec![Some(1), None, Some(37_800_005), Some(86_399_210)];
        let arr = TimestampMicrosecondArray::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<TimestampType>(&data, &vector);
    }

    #[test]
    fn dates() {
        let data = vec![Some(100), None, Some(200), None];
        let arr = Date32Array::from(data.clone());
        let vector = PlainVector::from(arr.into_data());

        check_answer::<DateType>(&data, &vector);
    }

    #[test]
    fn string_no_nulls() {
        let values: Vec<&str> = vec!["hello", "", "comet"];
        let arr = StringArray::from(values.clone());

        let vector = PlainVector::from(arr.into_data());
        assert_eq!(3, vector.num_values());
        assert_eq!(0, vector.num_nulls());

        for i in 0..values.len() {
            let expected = values[i];
            let actual = vector.value::<StringType>(i);
            assert_eq!(expected.len(), actual.len as usize);
            if expected.len() >= STRING_VIEW_PREFIX_LEN {
                assert_eq!(
                    &expected[..STRING_VIEW_PREFIX_LEN],
                    String::from_utf8_lossy(&actual.prefix)
                );
            }
            assert_eq!(expected, actual.as_utf8_str());
        }
    }

    #[test]
    fn string_with_nulls() {
        let data = [Some("hello"), None, Some("comet")];
        let arr = StringArray::from(data.to_vec().clone());

        let vector = PlainVector::from(arr.into_data());
        assert_eq!(3, vector.num_values());
        assert_eq!(1, vector.num_nulls());

        for i in 0..data.len() {
            if data[i].is_none() {
                assert!(vector.is_null(i));
            } else {
                let expected = data[i].unwrap();
                let actual = vector.value::<StringType>(i);
                if expected.len() >= STRING_VIEW_PREFIX_LEN {
                    assert_eq!(
                        &expected[..STRING_VIEW_PREFIX_LEN],
                        String::from_utf8_lossy(&actual.prefix)
                    );
                }
                assert_eq!(expected, actual.as_utf8_str());
            }
        }
    }
}
