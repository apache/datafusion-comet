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

use arrow::array::{ArrowNativeTypeOp, BooleanArray, Int64Array};
use arrow::datatypes::ToByteSlice;
use std::cmp;

use crate::bloom_filter::spark_bit_array;
use crate::bloom_filter::spark_bit_array::SparkBitArray;
use crate::hash_funcs::murmur3::spark_compatible_murmur3_hash;

const SPARK_BLOOM_FILTER_VERSION_1: i32 = 1;

/// A Bloom filter implementation that simulates the behavior of Spark's BloomFilter.
/// It's not a complete implementation of Spark's BloomFilter, but just add the minimum
/// methods to support mightContainsLong in the native side.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SparkBloomFilter {
    bits: SparkBitArray,
    num_hash_functions: u32,
}

pub fn optimal_num_hash_functions(expected_items: i32, num_bits: i32) -> i32 {
    cmp::max(
        1,
        ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).round() as i32,
    )
}

impl From<(i32, i32)> for SparkBloomFilter {
    /// Creates an empty SparkBloomFilter given number of hash functions and bits.
    fn from((num_hash_functions, num_bits): (i32, i32)) -> Self {
        let num_words = spark_bit_array::num_words(num_bits as usize);
        let bits = vec![0u64; num_words];
        Self {
            bits: SparkBitArray::new(bits),
            num_hash_functions: num_hash_functions as u32,
        }
    }
}

impl From<&[u8]> for SparkBloomFilter {
    /// Creates a SparkBloomFilter from a serialized byte array conforming to Spark's BloomFilter
    /// binary format version 1.
    fn from(buf: &[u8]) -> Self {
        let mut offset = 0;
        let version = read_num_be_bytes!(i32, 4, buf[offset..]);
        offset += 4;
        assert_eq!(
            version, SPARK_BLOOM_FILTER_VERSION_1,
            "Unsupported BloomFilter version: {version}, expecting version: {SPARK_BLOOM_FILTER_VERSION_1}"
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
}

impl SparkBloomFilter {
    /// Serializes a SparkBloomFilter to a byte array conforming to Spark's BloomFilter
    /// binary format version 1.
    pub fn spark_serialization(&self) -> Vec<u8> {
        // There might be a more efficient way to do this, even with all the endianness stuff.
        let mut spark_bloom_filter: Vec<u8> = 1_u32.to_be_bytes().to_vec();
        spark_bloom_filter.append(&mut self.num_hash_functions.to_be_bytes().to_vec());
        spark_bloom_filter.append(&mut (self.bits.word_size() as u32).to_be_bytes().to_vec());
        let mut filter_state: Vec<u64> = self.bits.data();
        for i in filter_state.iter_mut() {
            *i = i.to_be();
        }
        // Does it make sense to do a std::mem::take of filter_state here? Unclear to me if a deep
        // copy of filter_state as a Vec<u64> to a Vec<u8> is happening here.
        spark_bloom_filter.append(&mut Vec::from(filter_state.to_byte_slice()));
        spark_bloom_filter
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

    pub fn put_binary(&mut self, item: &[u8]) -> bool {
        // Here we first hash the input long element into 2 int hash values, h1 and h2, then produce
        // n hash values by `h1 + i * h2` with 1 <= i <= num_hash_functions.
        let h1 = spark_compatible_murmur3_hash(item, 0);
        let h2 = spark_compatible_murmur3_hash(item, h1);
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

    pub fn state_as_bytes(&self) -> Vec<u8> {
        self.bits.to_bytes()
    }

    pub fn merge_filter(&mut self, other: &[u8]) {
        // Extract bits data if other is in Spark's full serialization format
        // We need to compute the expected size and extract data before borrowing self.bits mutably
        let expected_bits_size = self.bits.byte_size();
        const SPARK_HEADER_SIZE: usize = 12; // version (4) + num_hash_functions (4) + num_words (4)

        let bits_data = if other.len() >= SPARK_HEADER_SIZE {
            // Check if this is Spark's serialization format by reading the version
            let version = i32::from_be_bytes([
                other[0], other[1], other[2], other[3],
            ]);
            if version == SPARK_BLOOM_FILTER_VERSION_1 {
                // This is Spark's full format, parse it to extract bits data
                let num_words = i32::from_be_bytes([
                    other[8], other[9], other[10], other[11],
                ]) as usize;
                let bits_start = SPARK_HEADER_SIZE;
                let bits_end = bits_start + (num_words * 8);
                
                // Verify the buffer is large enough
                if bits_end > other.len() {
                    panic!(
                        "Cannot merge SparkBloomFilters: buffer too short. Expected at least {} bytes ({} words), got {} bytes",
                        bits_end,
                        num_words,
                        other.len()
                    );
                }
                
                // Check if the incoming bloom filter has compatible size
                let incoming_bits_size = bits_end - bits_start;
                if incoming_bits_size != expected_bits_size {
                    panic!(
                        "Cannot merge SparkBloomFilters with incompatible sizes. Expected {} bytes ({} words), got {} bytes ({} words) from Spark partial aggregate. Full buffer size: {} bytes",
                        expected_bits_size,
                        self.bits.word_size(),
                        incoming_bits_size,
                        num_words,
                        other.len()
                    );
                }
                
                // Extract just the bits portion
                &other[bits_start..bits_end]
            } else if other.len() == expected_bits_size {
                // Not Spark format but size matches, treat as raw bits data (Comet format)
                other
            } else {
                // Size doesn't match and not Spark format - provide helpful error
                panic!(
                    "Cannot merge SparkBloomFilters: unexpected format. Expected {} bytes (Comet format) or Spark format (version 1, {} bytes header + bits), but got {} bytes with version {}",
                    expected_bits_size,
                    SPARK_HEADER_SIZE,
                    other.len(),
                    version
                );
            }
        } else {
            // Too short to be Spark format
            if other.len() != expected_bits_size {
                panic!(
                    "Cannot merge SparkBloomFilters: buffer too short. Expected {} bytes (Comet format) or at least {} bytes (Spark format), got {} bytes",
                    expected_bits_size,
                    SPARK_HEADER_SIZE,
                    other.len()
                );
            }
            // Size matches, treat as raw bits data
            other
        };

        self.bits.merge_bits(bits_data);
    }
}
