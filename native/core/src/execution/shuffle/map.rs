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
    errors::CometError,
    execution::shuffle::list::{append_to_builder, SparkUnsafeArray},
};
use arrow::array::builder::{ArrayBuilder, MapBuilder, MapFieldNames};
use arrow::datatypes::{DataType, FieldRef};

pub struct SparkUnsafeMap {
    pub(crate) keys: SparkUnsafeArray,
    pub(crate) values: SparkUnsafeArray,
}

impl SparkUnsafeMap {
    /// Creates a `SparkUnsafeMap` which points to the given address and size in bytes.
    pub(crate) fn new(addr: i64, size: i32) -> Self {
        // Read the number of bytes of key array from the first 8 bytes.
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, 8) };
        let key_array_size = i64::from_le_bytes(slice.try_into().unwrap());

        if key_array_size < 0 {
            panic!("Negative key size in bytes of map: {key_array_size}");
        }

        if key_array_size > i32::MAX as i64 {
            panic!("Number of key size in bytes should <= i32::MAX: {key_array_size}");
        }

        let value_array_size = size - key_array_size as i32 - 8;
        if value_array_size < 0 {
            panic!("Negative value size in bytes of map: {value_array_size}");
        }

        let keys = SparkUnsafeArray::new(addr + 8);
        let values = SparkUnsafeArray::new(addr + 8 + key_array_size);

        if keys.get_num_elements() != values.get_num_elements() {
            panic!(
                "Number of elements of keys and values should be the same: {} vs {}",
                keys.get_num_elements(),
                values.get_num_elements()
            );
        }

        Self { keys, values }
    }
}

/// Appending the given map stored in `SparkUnsafeMap` into `MapBuilder`.
/// `field` includes data types of the map element. `map_builder` is the map builder.
/// `map` is the map stored in `SparkUnsafeMap`.
pub fn append_map_elements(
    field: &FieldRef,
    map_builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
    map: &SparkUnsafeMap,
) -> Result<(), CometError> {
    let (key_field, value_field, _) = get_map_key_value_fields(field)?;

    let keys = &map.keys;
    let values = &map.values;

    append_to_builder::<false>(key_field.data_type(), map_builder.keys(), keys)?;

    append_to_builder::<true>(value_field.data_type(), map_builder.values(), values)?;

    map_builder.append(true)?;

    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
pub fn get_map_key_value_fields(
    field: &FieldRef,
) -> Result<(&FieldRef, &FieldRef, MapFieldNames), CometError> {
    let mut map_fieldnames = MapFieldNames::default();
    map_fieldnames.entry = field.name().to_string();

    let (key_field, value_field) = match field.data_type() {
        DataType::Struct(fields) => {
            if fields.len() != 2 {
                return Err(CometError::Internal(format!(
                    "Map field should have 2 fields, but got {}",
                    fields.len()
                )));
            }

            let key = &fields[0];
            let value = &fields[1];

            map_fieldnames.key = key.name().to_string();
            map_fieldnames.value = value.name().to_string();

            (key, value)
        }
        _ => {
            return Err(CometError::Internal(format!(
                "Map field should be a struct, but got {:?}",
                field.data_type()
            )));
        }
    };

    Ok((key_field, value_field, map_fieldnames))
}
