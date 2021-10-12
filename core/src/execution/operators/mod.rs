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

//! Operators

use arrow::{
    array::{make_array, Array, ArrayRef, MutableArrayData},
    datatypes::DataType,
    downcast_dictionary_array,
};

use arrow::compute::{cast_with_options, CastOptions};
use arrow_schema::ArrowError;
use std::{fmt::Debug, sync::Arc};

mod scan;
pub use scan::*;

mod copy;
pub use copy::*;

/// Error returned during executing operators.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    /// Simple error
    #[allow(dead_code)]
    #[error("General execution error with reason {0}.")]
    GeneralError(String),

    /// Error when deserializing an operator.
    #[error("Fail to deserialize to native operator with reason {0}.")]
    DeserializeError(String),

    /// Error when processing Arrow array.
    #[error("Fail to process Arrow array with reason {0}.")]
    ArrowError(String),

    /// DataFusion error
    #[error("Error from DataFusion {0}.")]
    DataFusionError(String),
}

/// Copy an Arrow Array
pub fn copy_array(array: &dyn Array) -> ArrayRef {
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

/// Copy an Arrow Array or cast to primitive type if it is a dictionary array.
/// This is used for `CopyExec` to copy/cast the input array. If the input array
/// is a dictionary array, we will cast the dictionary array to primitive type
/// (i.e., unpack the dictionary array) and copy the primitive array. If the input
/// array is a primitive array, we simply copy the array.
pub fn copy_or_cast_array(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            let options = CastOptions::default();
            let casted = cast_with_options(array, value_type.as_ref(), &options);

            casted.and_then(|a| copy_or_cast_array(a.as_ref()))
        }
        _ => Ok(copy_array(array)),
    }
}
