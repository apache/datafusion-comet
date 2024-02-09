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

use crate::common::bit;
use ahash::RandomState;
use arrow::datatypes::{i256, ArrowNativeType};
use arrow_array::{
    downcast_dictionary_array, downcast_primitive_array, Array, ArrayAccessor, ArrayRef,
    ArrowPrimitiveType, PrimitiveArray,
};
use std::fmt::Debug;

pub fn hash(src: &[ArrayRef], dst: &mut [u64]) {
    let state = RandomState::with_seed(42);
    src.iter().enumerate().for_each(|(idx, v)| {
        downcast_dictionary_array!(
            v => {
                let keys = v.keys();
                let values = v.values();
                downcast_primitive_array!(
                    values => hash_dict_typed(&state, idx > 0, keys, values, dst),
                    dt => panic!("Expected only primitive type but found {}", dt)
                )
            },
            dt => {
                downcast_primitive_array!(
                    v => hash_typed(&state, idx > 0, v, dst),
                    _ => panic!("Expected only primitive type but found {}", dt)
                )
            }
        )
    });
}

fn hash_typed<T>(state: &RandomState, mix: bool, array: T, dst: &mut [u64])
where
    T: ArrayAccessor,
    T::Item: Hashable + Debug,
{
    let nullable = array.null_count() > 0;
    let num_values = array.len();
    if nullable {
        for i in 0..num_values {
            if !array.is_null(i) {
                unsafe {
                    let value = array.value_unchecked(i);
                    hash1(state, mix, i, value, dst);
                }
            }
        }
    } else {
        for i in 0..num_values {
            unsafe {
                let value = array.value_unchecked(i);
                hash1(state, mix, i, value, dst);
            }
        }
    }
}

fn hash_dict_typed<K, V>(
    state: &RandomState,
    mix: bool,
    keys: &PrimitiveArray<K>,
    values: V,
    dst: &mut [u64],
) where
    K: ArrowPrimitiveType,
    V: ArrayAccessor,
    V::Item: Hashable + Debug,
{
    let nullable = keys.null_count() > 0;
    let num_keys = keys.len();
    let mut value_hashes = vec![0; values.len()];

    for (i, value_hash) in value_hashes.iter_mut().enumerate() {
        unsafe {
            *value_hash = values.value_unchecked(i).create_hash(state);
        }
    }

    if nullable {
        for i in 0..num_keys {
            if !keys.is_null(i) {
                unsafe {
                    let idx = keys.value_unchecked(i);
                    let hash = value_hashes[idx.as_usize()];
                    hash1_helper(mix, i, hash, dst);
                }
            }
        }
    } else {
        for i in 0..num_keys {
            unsafe {
                let idx = keys.value_unchecked(i);
                let hash = value_hashes[idx.as_usize()];
                hash1_helper(mix, i, hash, dst);
            }
        }
    }
}

#[inline(always)]
fn hash1<T: Hashable>(state: &RandomState, mix: bool, i: usize, value: T, dst: &mut [u64]) {
    let hash = value.create_hash(state);
    hash1_helper(mix, i, hash, dst);
}

#[inline(always)]
fn hash1_helper(mix: bool, i: usize, hash: u64, dst: &mut [u64]) {
    dst[i] = if mix {
        bit::mix_hash(dst[i], hash)
    } else {
        hash
    }
}

pub(crate) trait Hashable {
    fn create_hash(&self, state: &RandomState) -> u64;
}

macro_rules! impl_hashable {
    ($($t:ty),+) => {
        $(impl Hashable for $t {
            #[inline]
            fn create_hash(&self, state: &RandomState) -> u64 {
                state.hash_one(self)
            }
        })+
    };
}

impl_hashable!(i8, i16, i32, u8, u16, u32, u64, i128, i256);

impl Hashable for i64 {
    fn create_hash(&self, state: &RandomState) -> u64 {
        state.hash_one(self)
    }
}

impl Hashable for half::f16 {
    fn create_hash(&self, _: &RandomState) -> u64 {
        unimplemented!("hashing on f16 is not supported")
    }
}

impl Hashable for f32 {
    fn create_hash(&self, state: &RandomState) -> u64 {
        state.hash_one(u32::from_ne_bytes(self.to_ne_bytes()))
    }
}

impl Hashable for f64 {
    fn create_hash(&self, state: &RandomState) -> u64 {
        state.hash_one(u64::from_ne_bytes(self.to_ne_bytes()))
    }
}
