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

use arrow::compute::{cast_with_options, CastOptions};
use std::sync::Arc;

use arrow::array::{downcast_dictionary_array, make_array, Array, ArrayRef, MutableArrayData};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

#[derive(Debug, PartialEq, Clone)]
pub enum CopyMode {
    /// Perform a deep copy and also unpack dictionaries
    UnpackOrDeepCopy,
    /// Perform a clone and also unpack dictionaries
    UnpackOrClone,
}

/// Copy an Arrow Array
pub(crate) fn copy_array(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let capacity = array.len();
    let data = array.to_data();

    let mut mutable = MutableArrayData::new(vec![&data], false, capacity);

    mutable.try_extend(0, 0, capacity)?;

    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        let copied_dict = make_array(mutable.freeze());
        let ref_copied_dict = &copied_dict;

        downcast_dictionary_array!(
            ref_copied_dict => {
                // Copying dictionary value array
                let values = ref_copied_dict.values();
                let data = values.to_data();

                let mut mutable = MutableArrayData::new(vec![&data], false, values.len());
                mutable.try_extend(0, 0, values.len())?;

                let copied_dict = ref_copied_dict.with_values(make_array(mutable.freeze()));
                Ok(Arc::new(copied_dict))
            }
            t => unreachable!("Should not reach here: {}", t)
        )
    } else {
        Ok(make_array(mutable.freeze()))
    }
}

/// Copy an Arrow Array or cast to primitive type if it is a dictionary array.
/// This is used for `CopyExec` to copy/cast the input array. If the input array
/// is a dictionary array, we will cast the dictionary array to primitive type
/// (i.e., unpack the dictionary array) and copy the primitive array. If the input
/// array is a primitive array, we simply copy the array.
pub(crate) fn copy_or_unpack_array(
    array: &Arc<dyn Array>,
    mode: &CopyMode,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            let options = CastOptions::default();
            // We need to copy the array after `cast` because arrow-rs `take` kernel which is used
            // to unpack dictionary array might reuse the input array's null buffer.
            copy_array(&cast_with_options(array, value_type.as_ref(), &options)?)
        }
        _ => {
            if mode == &CopyMode::UnpackOrDeepCopy {
                copy_array(array)
            } else {
                Ok(Arc::clone(array))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, Int32Array, Int8Array, ListViewArray, NullArray};
    use arrow::datatypes::{Field, Int8Type};

    fn overflowing_list_view() -> ArrayRef {
        // Overlapping views expand past i32::MAX when copied. NullArray stores only
        // a length, so the large logical child does not require a large allocation.
        Arc::new(ListViewArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            vec![0_i32, 0].into(),
            vec![i32::MAX, 1].into(),
            Arc::new(NullArray::new(i32::MAX as usize)),
            None,
        ))
    }

    #[test]
    fn copy_array_propagates_offset_overflow() {
        let array = overflowing_list_view();
        assert!(matches!(
            copy_array(array.as_ref()),
            Err(ArrowError::InvalidArgumentError(_))
        ));
        assert!(matches!(
            copy_or_unpack_array(&array, &CopyMode::UnpackOrDeepCopy),
            Err(ArrowError::InvalidArgumentError(_))
        ));
        let cloned = copy_or_unpack_array(&array, &CopyMode::UnpackOrClone).unwrap();
        assert!(Arc::ptr_eq(&array, &cloned));
    }

    #[test]
    fn copy_dictionary_values_propagates_offset_overflow() {
        let array =
            DictionaryArray::<Int8Type>::new(Int8Array::from(vec![0, 1]), overflowing_list_view());
        assert!(matches!(
            copy_array(&array),
            Err(ArrowError::InvalidArgumentError(_))
        ));
    }

    #[test]
    fn copy_array_preserves_sliced_nullable_values() {
        let source = Int32Array::from(vec![Some(0), Some(1), None, Some(3)]);
        let array = source.slice(1, 3);
        let copied = copy_array(&array).unwrap();
        assert_eq!(copied.to_data(), array.to_data());
        let copied = copied.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_ne!(copied.values().as_ptr(), array.values().as_ptr());
    }

    #[test]
    fn copy_dictionary_preserves_values_and_unpacks() {
        let values = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
        let dictionary = DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![Some(2), None, Some(0), Some(1)]),
            Arc::clone(&values) as ArrayRef,
        );
        let copied = copy_array(&dictionary).unwrap();
        assert_eq!(copied.to_data(), dictionary.to_data());
        let copied = copied
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_ne!(
            copied.keys().values().as_ptr(),
            dictionary.keys().values().as_ptr()
        );
        let copied_values = copied
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_ne!(copied_values.values().as_ptr(), values.values().as_ptr());

        let array: ArrayRef = Arc::new(dictionary);
        for mode in [CopyMode::UnpackOrDeepCopy, CopyMode::UnpackOrClone] {
            let unpacked = copy_or_unpack_array(&array, &mode).unwrap();
            let expected = Int32Array::from(vec![Some(30), None, Some(10), None]);
            assert_eq!(unpacked.to_data(), expected.to_data());
        }
    }
}
