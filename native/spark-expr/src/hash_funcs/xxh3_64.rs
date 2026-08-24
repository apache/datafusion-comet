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

use arrow::array::{Array, ArrayRef};
use arrow::compute::take;
use datafusion::{
    arrow::{
        array::*,
        datatypes::{ArrowDictionaryKeyType, ArrowNativeType},
    },
    error::{DataFusionError, Result},
};
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::create_hashes_internal;

/// One-shot XXH3-64 with a chained seed. Used by the shuffle partitioner's
/// `RoundRobin` arm to compute a per-row hash across columns. Not tied to any
/// Spark expression (Spark has no XXH3-64 hash).
#[inline]
fn xxh3_64_oneshot<T: AsRef<[u8]>>(data: T, seed: u64) -> u64 {
    xxh3_64_with_seed(data.as_ref(), seed)
}

fn create_xxh3_64_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u64],
    first_col: bool,
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if !first_col {
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        create_xxh3_64_hashes(&[unpacked], hashes_buffer)?;
    } else {
        // Hash each dictionary value once and reuse per key, so a large
        // dictionary element (e.g. a long string) is not rehashed per row.
        let dict_values = Arc::clone(dict_array.values());
        let mut dict_hashes = vec![42u64; dict_values.len()];
        create_xxh3_64_hashes(&[dict_values], &mut dict_hashes)?;

        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    ))
                })?;
                *hash = dict_hashes[idx]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

/// Compute a per-row XXH3-64 hash across `arrays`, chaining each column's
/// hash through the previous row hash. The number of rows is
/// `hashes_buffer.len()`. Callers must seed `hashes_buffer` before calling.
pub fn create_xxh3_64_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut [u64],
) -> Result<&'a mut [u64]> {
    create_hashes_internal!(
        arrays,
        hashes_buffer,
        xxh3_64_oneshot,
        create_xxh3_64_hashes_dictionary,
        create_xxh3_64_hashes
    );
    Ok(hashes_buffer)
}
