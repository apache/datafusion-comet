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
    array::{DictionaryArray, Int64Array, PrimitiveArray},
    datatypes::{ArrowPrimitiveType, Int32Type},
};
use arrow_schema::ArrowError;
use rand::{
    distributions::{Distribution, Standard},
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::sync::Arc;

/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

pub fn create_int64_array(size: usize, null_density: f32, min: i64, max: i64) -> Int64Array {
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen_range(min..max))
            }
        })
        .collect()
}

pub fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}

/// Creates a dictionary with random keys and values, with value type `T`.
/// Note here the keys are the dictionary indices.
pub fn create_dictionary_array<T>(
    size: usize,
    value_size: usize,
    null_density: f32,
) -> Result<DictionaryArray<Int32Type>, ArrowError>
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    // values are not null
    let values = create_primitive_array::<T>(value_size, 0.0);
    let keys = create_primitive_array::<Int32Type>(size, null_density)
        .iter()
        .map(|v| v.map(|w| w.abs() % (value_size as i32)))
        .collect();
    DictionaryArray::try_new(keys, Arc::new(values))
}
