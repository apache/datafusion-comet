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
    compute::kernels::substring::{substring as arrow_substring, substring_by_char},
    datatypes::{DataType, Int32Type},
};
use datafusion::common::DataFusionError;

pub fn substring(array: &dyn Array, start: i64, length: u64) -> Result<ArrayRef, DataFusionError> {
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
        dt => panic!("Unsupported input type for function 'substring': {dt:?}"),
    }
}
