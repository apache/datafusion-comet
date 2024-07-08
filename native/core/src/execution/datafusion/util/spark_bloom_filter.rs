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

use crate::execution::datafusion::{
    spark_hash::spark_compatible_murmur3_hash, util::spark_bit_array::SparkBitArray,
};
use arrow_array::{ArrowNativeTypeOp, BooleanArray, Int64Array};

const SPARK_BLOOM_FILTER_VERSION_1: i32 = 1;

/// A Bloom filter implementation that simulates the behavior of Spark's BloomFilter.
/// It's not a complete implementation of Spark's BloomFilter, but just add the minimum
/// methods to support mightContainsLong in the native side.
#[derive(Debug, Hash)]
pub struct SparkBloomFilter {
    bits: SparkBitArray,
    num_hash_functions: u32,
}

impl SparkBloomFilter {
    pub fn new(buf: &[u8]) -> Self {
        let mut offset = 0;
        let version = read_num_be_bytes!(i32, 4, buf[offset..]);
        offset += 4;
        assert_eq!(
            version, SPARK_BLOOM_FILTER_VERSION_1,
            "Unsupported BloomFilter version: {}, expecting version: {}",
            version, SPARK_BLOOM_FILTER_VERSION_1
        );
        let num_hash_functions = read_num_be_bytes!(i32, 4, buf[offset..]);
        offset += 4;
        let num_words = read_num_be_bytes!(i32, 4, buf[offset..]);
        offset += 4;
        let mut bits = vec![0u64; num_words as usize];
        for i in 0..num_words {
            bits[i as usize] = read_num_be_bytes!(i64, 8, buf[offset..]) as u64;
            offset += 8;
        }
        Self {
            bits: SparkBitArray::new(bits),
            num_hash_functions: num_hash_functions as u32,
        }
    }

    pub fn put_long(&mut self, item: i64) -> bool {
        // Here we first hash the input long element into 2 int hash values, h1 and h2, then produce
        // n hash values by `h1 + i * h2` with 1 <= i <= num_hash_functions.
        let h1 = spark_compatible_murmur3_hash(item.to_le_bytes(), 0);
        let h2 = spark_compatible_murmur3_hash(item.to_le_bytes(), h1);
        let bit_size = self.bits.bit_size() as i32;
        let mut bit_changed = false;
        for i in 1..=self.num_hash_functions {
            let mut combined_hash = (h1 as i32).add_wrapping((i as i32).mul_wrapping(h2 as i32));
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            bit_changed |= self.bits.set((combined_hash % bit_size) as usize)
        }
        bit_changed
    }

    pub fn might_contain_long(&self, item: i64) -> bool {
        let h1 = spark_compatible_murmur3_hash(item.to_le_bytes(), 0);
        let h2 = spark_compatible_murmur3_hash(item.to_le_bytes(), h1);
        let bit_size = self.bits.bit_size() as i32;
        for i in 1..=self.num_hash_functions {
            let mut combined_hash = (h1 as i32).add_wrapping((i as i32).mul_wrapping(h2 as i32));
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            if !self.bits.get((combined_hash % bit_size) as usize) {
                return false;
            }
        }
        true
    }

    pub fn might_contain_longs(&self, items: &Int64Array) -> BooleanArray {
        items
            .iter()
            .map(|v| v.map(|x| self.might_contain_long(x)))
            .collect()
    }
}
