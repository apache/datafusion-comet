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

    /// Merges another bloom filter's state into this one. Accepts both Comet's
    /// raw bits format (native endian, no header) and Spark's full serialization
    /// format (12-byte big-endian header + big-endian bit data).
    #[allow(dead_code)] // used in tests
    pub fn num_hash_functions(&self) -> u32 {
        self.num_hash_functions
    }

    /// Merges another bloom filter's state into this one. Accepts both Comet's
    /// raw bits format (native endian, no header) and Spark's full serialization
    /// format (12-byte big-endian header + big-endian bit data).
    pub fn merge_filter(&mut self, other: &[u8]) {
        let expected_bytes = self.bits.byte_size();
        let header_size = 12; // version (4) + num_hash_functions (4) + num_words (4)

        if other.len() == expected_bytes {
            // Comet state format: raw bits in native endianness
            self.bits.merge_bits(other);
        } else if other.len() == expected_bytes + header_size {
            // Spark serialization format: 12-byte big-endian header + big-endian bit data.
            // Skip the header and convert from big-endian to native endianness.
            let bits_data = &other[header_size..];
            let native_bytes: Vec<u8> = bits_data
                .chunks(8)
                .flat_map(|chunk| {
                    let be_val = u64::from_be_bytes(chunk.try_into().unwrap());
                    be_val.to_ne_bytes()
                })
                .collect();
            self.bits.merge_bits(&native_bytes);
        } else {
            panic!(
                "Cannot merge SparkBloomFilter: unexpected buffer length {}. \
                 Expected {} (raw bits) or {} (Spark serialization format).",
                other.len(),
                expected_bytes,
                expected_bytes + header_size
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bloom_filter::spark_bloom_filter::optimal_num_hash_functions;
    use arrow::array::{ArrayRef, BinaryArray};
    use datafusion::physical_plan::Accumulator;
    use std::sync::Arc;

    const NUM_ITEMS: i32 = 1000;
    const NUM_BITS: i32 = 8192;

    fn new_bloom_filter() -> SparkBloomFilter {
        let num_hash = optimal_num_hash_functions(NUM_ITEMS, NUM_BITS);
        SparkBloomFilter::from((num_hash, NUM_BITS))
    }

    #[test]
    fn test_merge_comet_state_format() {
        // Simulates Comet partial -> Comet final: state() produces raw bits
        let mut partial = new_bloom_filter();
        partial.put_long(42);
        partial.put_long(100);

        let raw_bits = partial.state_as_bytes();

        let mut final_agg = new_bloom_filter();
        final_agg.merge_filter(&raw_bits);

        assert!(final_agg.might_contain_long(42));
        assert!(final_agg.might_contain_long(100));
        assert!(!final_agg.might_contain_long(999));
    }

    #[test]
    fn test_merge_spark_serialization_format() {
        // Simulates Spark partial -> Comet final: evaluate() produces Spark format
        // with 12-byte header. This is the scenario from issue #2889.
        let mut partial = new_bloom_filter();
        partial.put_long(42);
        partial.put_long(100);

        let spark_format = partial.spark_serialization();
        // Verify the Spark format has the 12-byte header
        assert_eq!(spark_format.len(), partial.state_as_bytes().len() + 12);

        let mut final_agg = new_bloom_filter();
        final_agg.merge_filter(&spark_format);

        assert!(final_agg.might_contain_long(42));
        assert!(final_agg.might_contain_long(100));
        assert!(!final_agg.might_contain_long(999));
    }

    #[test]
    fn test_merge_batch_with_spark_format() {
        // End-to-end test using the Accumulator trait, matching what happens
        // when Spark partial sends its state to Comet final via merge_batch.
        let mut partial = new_bloom_filter();
        partial.put_long(42);
        partial.put_long(100);

        let spark_bytes = partial.spark_serialization();
        let binary_array: ArrayRef = Arc::new(BinaryArray::from_vec(vec![&spark_bytes]));

        let mut final_agg = new_bloom_filter();
        final_agg.merge_batch(&[binary_array]).unwrap();

        assert!(final_agg.might_contain_long(42));
        assert!(final_agg.might_contain_long(100));
        assert!(!final_agg.might_contain_long(999));
    }

    #[test]
    fn test_merge_batch_with_comet_state() {
        // Comet partial -> Comet final via merge_batch using state() output
        let mut partial = new_bloom_filter();
        partial.put_long(42);

        let state = partial.state().unwrap();
        let raw_bits = match &state[0] {
            datafusion::common::ScalarValue::Binary(Some(b)) => b.clone(),
            _ => panic!("expected Binary"),
        };
        let binary_array: ArrayRef = Arc::new(BinaryArray::from_vec(vec![&raw_bits]));

        let mut final_agg = new_bloom_filter();
        final_agg.merge_batch(&[binary_array]).unwrap();

        assert!(final_agg.might_contain_long(42));
    }

    #[test]
    fn test_roundtrip_spark_serialization() {
        let mut original = new_bloom_filter();
        for i in 0..50 {
            original.put_long(i);
        }

        let spark_bytes = original.spark_serialization();
        let deserialized = SparkBloomFilter::from(spark_bytes.as_slice());

        assert_eq!(
            deserialized.num_hash_functions(),
            original.num_hash_functions()
        );
        for i in 0..50 {
            assert!(deserialized.might_contain_long(i));
        }
    }

    #[test]
    #[should_panic(expected = "unexpected buffer length")]
    fn test_merge_invalid_length_panics() {
        let mut filter = new_bloom_filter();
        filter.merge_filter(&[0u8; 37]);
    }
}
