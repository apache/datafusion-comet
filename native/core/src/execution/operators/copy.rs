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

/// Copy an Arrow Array
fn copy_array(array: &dyn Array) -> ArrayRef {
    let capacity = array.len();
    let data = array.to_data();

    let mut mutable = MutableArrayData::new(vec![&data], false, capacity);

    mutable.extend(0, 0, capacity);

    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        let copied_dict = make_array(mutable.freeze());
        let ref_copied_dict = &copied_dict;

        downcast_dictionary_array!(
            ref_copied_dict => {
                // Copying dictionary value array
                let values = ref_copied_dict.values();
                let data = values.to_data();

                let mut mutable = MutableArrayData::new(vec![&data], false, values.len());
                mutable.extend(0, 0, values.len());

                let copied_dict = ref_copied_dict.with_values(make_array(mutable.freeze()));
                Arc::new(copied_dict)
            }
            t => unreachable!("Should not reach here: {}", t)
        )
    } else {
        make_array(mutable.freeze())
    }
}

/// Unpack dictionary arrays to primitive type, or clone non-dictionary arrays.
pub(crate) fn copy_or_unpack_array(array: &Arc<dyn Array>) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            let options = CastOptions::default();
            // We need to copy the array after `cast` because arrow-rs `take` kernel which is used
            // to unpack dictionary array might reuse the input array's null buffer.
            Ok(copy_array(&cast_with_options(
                array,
                value_type.as_ref(),
                &options,
            )?))
        }
        _ => Ok(Arc::clone(array)),
    }
}
