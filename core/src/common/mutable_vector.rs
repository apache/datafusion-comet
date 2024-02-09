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

use arrow::{
    array::ArrayData, buffer::Buffer as ArrowBuffer, datatypes::DataType as ArrowDataType,
};

use crate::{
    common::{bit, CometBuffer, ValueGetter, ValueSetter},
    BinaryType, StringType, StringView, TypeTrait,
};

const DEFAULT_ARRAY_LEN: usize = 4;

/// A mutable vector that can be re-used across batches.
#[derive(Debug)]
pub struct MutableVector {
    /// The Arrow type for the elements of this vector.
    pub(crate) arrow_type: ArrowDataType,

    /// The number of total elements in this vector.
    pub(crate) num_values: usize,

    /// The number of null elements in this vector, must <= `num_values`.
    pub(crate) num_nulls: usize,

    /// The capacity of the vector
    pub(crate) capacity: usize,

    /// How many bits are required to store a single value
    pub(crate) bit_width: usize,

    /// The validity buffer of this Arrow vector. A bit set at position `i` indicates the `i`th
    /// element is not null. Otherwise, an unset bit at position `i` indicates the `i`th element is
    /// null.
    pub(crate) validity_buffer: CometBuffer,

    /// The value buffer of this Arrow vector. This could store either offsets if the vector
    /// is of list or struct type, or actual values themselves otherwise.
    pub(crate) value_buffer: CometBuffer,

    /// Child vectors for non-primitive types (e.g., list, struct).
    pub(crate) children: Vec<MutableVector>,

    /// Dictionary (i.e., values) associated with this vector. Only set if using dictionary
    /// encoding.
    pub(crate) dictionary: Option<Box<MutableVector>>,

    /// Whether all the values in the vector are not null.
    ///
    /// This is useful so we can skip setting non-null for each individual value, in the
    /// `validity_buffer`.
    all_non_null: bool,
}

impl MutableVector {
    pub fn new(capacity: usize, arrow_type: &ArrowDataType) -> Self {
        let bit_width = Self::bit_width(arrow_type);
        Self::new_with_bit_width(capacity, arrow_type.clone(), bit_width)
    }

    pub fn new_with_bit_width(
        capacity: usize,
        arrow_type: ArrowDataType,
        bit_width: usize,
    ) -> Self {
        let validity_len = bit::ceil(capacity, 8);
        let validity_buffer = CometBuffer::new(validity_len);

        let mut value_capacity = capacity;
        if Self::is_binary_type(&arrow_type) {
            // Arrow offset array needs to have one extra slot
            value_capacity += 1;
        }
        // Make sure the capacity is positive
        let len = bit::ceil(value_capacity * bit_width, 8);
        let mut value_buffer = CometBuffer::new(len);

        let mut children = Vec::new();

        match arrow_type {
            ArrowDataType::Binary | ArrowDataType::Utf8 => {
                children.push(MutableVector::new_with_bit_width(
                    capacity,
                    ArrowDataType::Int8,
                    DEFAULT_ARRAY_LEN * 8,
                ));
            }
            _ => {}
        }

        if Self::is_binary_type(&arrow_type) {
            // Setup the first offset which is always 0.
            let zero: u32 = 0;
            bit::memcpy_value(&zero, 4, &mut value_buffer);
        }

        Self {
            arrow_type,
            num_values: 0,
            num_nulls: 0,
            capacity,
            bit_width,
            validity_buffer,
            value_buffer,
            children,
            dictionary: None,
            all_non_null: false,
        }
    }

    /// Appends a non-null value `v` to the end of this vector.
    #[inline]
    pub fn append_value<T: TypeTrait>(&mut self, v: &T::Native) {
        <dyn ValueSetter<T>>::append_value(self, v);
    }

    /// Gets the non-null value at `idx` of this vector.
    #[inline]
    pub fn value<T: TypeTrait>(&self, idx: usize) -> T::Native {
        <dyn ValueGetter<T>>::value(self, idx)
    }

    /// Whether the given value at `idx` of this vector is null.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        unsafe { !bit::get_bit_raw(self.validity_buffer.as_ptr(), idx) }
    }

    /// Resets this vector to the initial state.
    #[inline]
    pub fn reset(&mut self) {
        self.num_values = 0;
        self.num_nulls = 0;
        self.all_non_null = false;
        self.validity_buffer.reset();
        if Self::is_binary_type(&self.arrow_type) {
            // Reset the first offset to 0
            let zero: u32 = 0;
            bit::memcpy_value(&zero, 4, &mut self.value_buffer);
            // Also reset the child value vector
            let child = &mut self.children[0];
            child.reset();
        } else if Self::should_reset_value_buffer(&self.arrow_type) {
            self.value_buffer.reset();
        }
    }

    /// Appends a new null value to the end of this vector.
    #[inline]
    pub fn put_null(&mut self) {
        self.put_nulls(1)
    }

    /// Appends `n` null values to the end of this vector.
    #[inline]
    pub fn put_nulls(&mut self, n: usize) {
        // We need to update offset buffer for binary.
        if Self::is_binary_type(&self.arrow_type) {
            let mut offset = self.num_values * 4;
            let prev_offset_value = bit::read_num_bytes_u32(4, &self.value_buffer[offset..]);
            offset += 4;
            (0..n).for_each(|_| {
                bit::memcpy_value(&prev_offset_value, 4, &mut self.value_buffer[offset..]);
                offset += 4;
            });
        }

        self.num_nulls += n;
        self.num_values += n;
    }

    /// Returns the number of total values (including both null and non-null) of this vector.
    #[inline]
    pub fn num_values(&self) -> usize {
        self.num_values
    }

    /// Returns the number of null values of this vector.
    #[inline]
    pub fn num_nulls(&self) -> usize {
        self.num_nulls
    }

    #[inline]
    pub fn set_not_null(&mut self, i: usize) {
        unsafe {
            bit::set_bit_raw(self.validity_buffer.as_mut_ptr(), i);
        }
    }

    /// Sets all values in this vector to be non-null.
    #[inline]
    pub fn set_all_non_null(&mut self) {
        self.all_non_null = true;
    }

    /// Sets the content of validity buffer to be `buffer`.
    pub fn set_validity_buffer(&mut self, buffer: &ArrowBuffer) {
        self.validity_buffer = buffer.into();
    }

    /// Sets the content of value buffer to be `buffer`.
    pub fn set_value_buffer(&mut self, buffer: &ArrowBuffer) {
        self.value_buffer = buffer.into();
    }

    /// Sets the dictionary of this to be `dict`.
    pub fn set_dictionary(&mut self, dict: MutableVector) {
        self.dictionary = Some(Box::new(dict))
    }

    /// Clones this into an Arrow [`ArrayData`](arrow::array::ArrayData). Note that the caller of
    /// this method MUST make sure the returned `ArrayData` won't live longer than this vector
    /// itself. Otherwise, dangling pointer may happen.
    ///
    /// # Safety
    ///
    /// This method is highly unsafe since it calls `to_immutable` which leaks raw pointer to the
    /// memory region that are tracked by `ArrowMutableBuffer`. Please see comments on
    /// `to_immutable` buffer to understand the motivation.
    pub fn get_array_data(&mut self) -> ArrayData {
        unsafe {
            let data_type = if let Some(d) = &self.dictionary {
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::Int32),
                    Box::new(d.arrow_type.clone()),
                )
            } else {
                self.arrow_type.clone()
            };
            let mut builder = ArrayData::builder(data_type)
                .len(self.num_values)
                .add_buffer(self.value_buffer.to_arrow());

            builder = if self.all_non_null {
                builder.null_count(0)
            } else {
                builder
                    .null_bit_buffer(Some(self.validity_buffer.to_arrow()))
                    .null_count(self.num_nulls)
            };

            if Self::is_binary_type(&self.arrow_type) && self.dictionary.is_none() {
                let child = &mut self.children[0];
                builder = builder.add_buffer(child.value_buffer.to_arrow());
            }

            if let Some(d) = &mut self.dictionary {
                builder = builder.add_child_data(d.get_array_data());
            }

            builder.build_unchecked()
        }
    }

    /// Returns the number of bits it takes to store one element of `arrow_type` in the value buffer
    /// of this vector.
    pub fn bit_width(arrow_type: &ArrowDataType) -> usize {
        match arrow_type {
            ArrowDataType::Boolean => 1,
            ArrowDataType::Int8 => 8,
            ArrowDataType::Int16 => 16,
            ArrowDataType::Int32 | ArrowDataType::Float32 | ArrowDataType::Date32 => 32,
            ArrowDataType::Int64 | ArrowDataType::Float64 | ArrowDataType::Timestamp(_, _) => 64,
            ArrowDataType::FixedSizeBinary(type_length) => *type_length as usize * 8,
            ArrowDataType::Decimal128(..) => 128, // Arrow stores decimal with 16 bytes
            ArrowDataType::Binary | ArrowDataType::Utf8 => 32, // Only count offset size
            dt => panic!("Unsupported Arrow data type: {:?}", dt),
        }
    }

    #[inline]
    fn is_binary_type(dt: &ArrowDataType) -> bool {
        matches!(dt, ArrowDataType::Binary | ArrowDataType::Utf8)
    }

    #[inline]
    fn should_reset_value_buffer(dt: &ArrowDataType) -> bool {
        // - Boolean type expects have a zeroed value buffer
        // - Decimal may pad buffer with 0xff so we need to clear them before a new batch
        matches!(dt, ArrowDataType::Boolean | ArrowDataType::Decimal128(_, _))
    }

    /// Creates an immutable reference from a mutable Arrow buffer `buf`.
    ///
    /// # Safety
    ///
    /// This function is highly unsafe. Please see documentation of the [`to_arrow`] method for
    /// details.
    #[inline]
    unsafe fn to_immutable(buf: &CometBuffer) -> ArrowBuffer {
        buf.to_arrow()
    }
}

impl<T: TypeTrait> ValueGetter<T> for MutableVector {
    default fn value(&self, idx: usize) -> T::Native {
        unsafe {
            let ptr = self.value_buffer.as_ptr() as *const T::Native;
            *ptr.add(idx)
        }
    }
}

impl ValueGetter<StringType> for MutableVector {
    fn value(&self, _: usize) -> StringView {
        unimplemented!("'value' on StringType is currently unsupported");
    }
}

impl ValueGetter<BinaryType> for MutableVector {
    fn value(&self, _: usize) -> StringView {
        unimplemented!("'value' on BinaryType is currently unsupported");
    }
}

impl<T: TypeTrait> ValueSetter<T> for MutableVector {
    default fn append_value(&mut self, v: &T::Native) {
        unsafe {
            let ptr = self.value_buffer.as_mut_ptr() as *mut T::Native;
            *ptr.add(self.num_values) = *v;
        }
        self.num_values += 1;
    }
}

impl ValueSetter<StringType> for MutableVector {
    fn append_value(&mut self, _: &StringView) {
        unimplemented!("'append_value' on StringType is currently unsupported");
    }
}

impl ValueSetter<BinaryType> for MutableVector {
    fn append_value(&mut self, _: &StringView) {
        unimplemented!("'append_value' on BinaryType is currently unsupported");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IntegerType;

    #[test]
    fn set_and_get() {
        let mut mv = MutableVector::new(1024, &ArrowDataType::Int32);

        for n in 0..100 {
            mv.append_value::<IntegerType>(&(n * n));
        }
        assert_eq!(mv.num_values(), 100);
        assert_eq!(mv.num_nulls(), 0);

        for n in 0..100 {
            assert_eq!(mv.value::<IntegerType>(n) as usize, n * n);
        }

        mv.reset();

        for n in 0..200 {
            if n % 2 == 0 {
                mv.put_null();
            } else {
                mv.append_value::<IntegerType>(&(n * 2));
            }
        }

        assert_eq!(mv.num_values(), 200);
        assert_eq!(mv.num_nulls(), 100);

        for n in 0..200 {
            if n % 2 == 0 {
                assert!(mv.is_null(n));
            } else {
                assert_eq!(mv.value::<IntegerType>(n) as usize, n * 2);
            }
        }
    }

    #[test]
    #[should_panic]
    fn set_string_unsupported() {
        let mut mv = MutableVector::new(1024, &ArrowDataType::Utf8);
        let sv = StringView::default();
        mv.append_value::<StringType>(&sv);
    }

    #[test]
    #[should_panic]
    fn get_string_unsupported() {
        let mv = MutableVector::new(1024, &ArrowDataType::Utf8);
        mv.value::<StringType>(0);
    }
}
