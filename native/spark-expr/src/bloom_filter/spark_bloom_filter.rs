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
const SPARK_BLOOM_FILTER_VERSION_2: i32 = 2;

/// Serialization format / hashing scheme used by a [`SparkBloomFilter`].
///
/// Spark 4.1 (SPARK-XXXXX, see Spark's `BloomFilter.java`) introduced a V2 format
/// that adds a `seed` field, switches the bit-scattering algorithm to use 64-bit
/// arithmetic, and is now the default for `BloomFilter.create`. Spark 4.0 and
/// earlier only know V1. The version is encoded in the first 4 bytes of the
/// serialized form, and the read path must honour it.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum SparkBloomFilterVersion {
    V1,
    V2,
}

impl SparkBloomFilterVersion {
    fn from_int(v: i32) -> Self {
        match v {
            SPARK_BLOOM_FILTER_VERSION_1 => Self::V1,
            SPARK_BLOOM_FILTER_VERSION_2 => Self::V2,
            _ => panic!(
                "Unsupported BloomFilter version: {v}, expecting {SPARK_BLOOM_FILTER_VERSION_1} or {SPARK_BLOOM_FILTER_VERSION_2}"
            ),
        }
    }

    fn to_int(self) -> i32 {
        match self {
            Self::V1 => SPARK_BLOOM_FILTER_VERSION_1,
            Self::V2 => SPARK_BLOOM_FILTER_VERSION_2,
        }
    }
}

/// A Bloom filter implementation that simulates the behavior of Spark's BloomFilter.
/// It's not a complete implementation of Spark's BloomFilter, but just add the minimum
/// methods to support mightContainsLong in the native side.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SparkBloomFilter {
    bits: SparkBitArray,
    num_hash_functions: u32,
    /// Serialization format and hash-scattering scheme.
    version: SparkBloomFilterVersion,
    /// Murmur3 seed. V1 always uses 0; V2 stores a per-filter seed (Spark's
    /// `BloomFilterImplV2.DEFAULT_SEED` is also 0, so 0 is the common case).
    seed: i32,
}

pub fn optimal_num_hash_functions(expected_items: i32, num_bits: i32) -> i32 {
    cmp::max(
        1,
        ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).round() as i32,
    )
}

impl From<(i32, i32)> for SparkBloomFilter {
    /// Creates an empty SparkBloomFilter given number of hash functions and bits.
    /// Defaults to V1 for backwards compatibility; use [`SparkBloomFilter::new_v2`]
    /// to construct an empty V2 filter.
    fn from((num_hash_functions, num_bits): (i32, i32)) -> Self {
        Self::new(SparkBloomFilterVersion::V1, num_hash_functions, num_bits, 0)
    }
}

impl From<&[u8]> for SparkBloomFilter {
    /// Creates a SparkBloomFilter from a serialized byte array conforming to either
    /// Spark's BloomFilter binary format V1 or V2. The version is read from the
    /// first 4 bytes.
    fn from(buf: &[u8]) -> Self {
        let mut offset = 0;
        let version_int = read_num_be_bytes!(i32, 4, buf[offset..]);
        offset += 4;
        let version = SparkBloomFilterVersion::from_int(version_int);
        let num_hash_functions = read_num_be_bytes!(i32, 4, buf[offset..]);
        offset += 4;
        // V2 adds a 4-byte seed before the bit array. V1 has no seed.
        let seed = match version {
            SparkBloomFilterVersion::V1 => 0,
            SparkBloomFilterVersion::V2 => {
                let s = read_num_be_bytes!(i32, 4, buf[offset..]);
                offset += 4;
                s
            }
        };
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
            version,
            seed,
        }
    }
}

impl SparkBloomFilter {
    /// Construct an empty filter with the given version, number of hash functions,
    /// and bit count. The `seed` is ignored for V1 (always treated as 0) but is
    /// honoured for V2.
    pub fn new(
        version: SparkBloomFilterVersion,
        num_hash_functions: i32,
        num_bits: i32,
        seed: i32,
    ) -> Self {
        let num_words = spark_bit_array::num_words(num_bits as usize);
        let bits = vec![0u64; num_words];
        Self {
            bits: SparkBitArray::new(bits),
            num_hash_functions: num_hash_functions as u32,
            version,
            seed: match version {
                SparkBloomFilterVersion::V1 => 0,
                SparkBloomFilterVersion::V2 => seed,
            },
        }
    }

    /// Returns the serialization/scattering format this filter uses.
    #[allow(dead_code)]
    pub fn version(&self) -> SparkBloomFilterVersion {
        self.version
    }

    /// Serializes a SparkBloomFilter to a byte array conforming to Spark's BloomFilter
    /// binary format. The output format follows the filter's `version`.
    pub fn spark_serialization(&self) -> Vec<u8> {
        let mut out: Vec<u8> = (self.version.to_int() as u32).to_be_bytes().to_vec();
        out.append(&mut self.num_hash_functions.to_be_bytes().to_vec());
        if let SparkBloomFilterVersion::V2 = self.version {
            // Spark's BloomFilterImplV2.writeTo writes the seed between
            // numHashFunctions and the bit array.
            out.append(&mut (self.seed as u32).to_be_bytes().to_vec());
        }
        out.append(&mut (self.bits.word_size() as u32).to_be_bytes().to_vec());
        let mut filter_state: Vec<u64> = self.bits.data();
        for i in filter_state.iter_mut() {
            *i = i.to_be();
        }
        out.append(&mut Vec::from(filter_state.to_byte_slice()));
        out
    }

    /// V1 bit-scattering: `combinedHash = h1 + i*h2` for `i in 1..=numHashFunctions`,
    /// matching `BloomFilterImpl.scatterHashAndSetAllBits` (Spark <= 4.0; still
    /// available as the V1 codepath in 4.1+).
    fn scatter_v1(&mut self, h1: u32, h2: u32, set: bool) -> Option<bool> {
        let bit_size = self.bits.bit_size() as i32;
        for i in 1..=self.num_hash_functions {
            let mut combined_hash = (h1 as i32).add_wrapping((i as i32).mul_wrapping(h2 as i32));
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            let idx = (combined_hash % bit_size) as usize;
            if set {
                self.bits.set(idx);
            } else if !self.bits.get(idx) {
                return Some(false);
            }
        }
        if set {
            None
        } else {
            Some(true)
        }
    }

    /// V2 bit-scattering: `combinedHash = (long)h1 * Integer.MAX_VALUE; for (i = 0; i <
    /// numHashFunctions; i++) combinedHash += h2;`. Mirrors Spark 4.1's
    /// `BloomFilterImplV2.scatterHashAndSetAllBits`. Note 64-bit accumulator,
    /// zero-indexed loop, and `combinedHash < 0 ? ~combinedHash : combinedHash` for the
    /// non-negative bit index.
    fn scatter_v2(&mut self, h1: u32, h2: u32, set: bool) -> Option<bool> {
        let bit_size = self.bits.bit_size() as i64;
        // (long) h1 * Integer.MAX_VALUE - sign-extend h1, then i64 multiply with wrapping.
        let mut combined_hash = (h1 as i32 as i64).wrapping_mul(i32::MAX as i64);
        let h2_long = h2 as i32 as i64;
        for _ in 0..self.num_hash_functions {
            combined_hash = combined_hash.wrapping_add(h2_long);
            let combined_index = if combined_hash < 0 {
                !combined_hash
            } else {
                combined_hash
            };
            let idx = (combined_index % bit_size) as usize;
            if set {
                self.bits.set(idx);
            } else if !self.bits.get(idx) {
                return Some(false);
            }
        }
        if set {
            None
        } else {
            Some(true)
        }
    }

    fn scatter(&mut self, h1: u32, h2: u32, set: bool) -> Option<bool> {
        match self.version {
            SparkBloomFilterVersion::V1 => self.scatter_v1(h1, h2, set),
            SparkBloomFilterVersion::V2 => self.scatter_v2(h1, h2, set),
        }
    }

    /// Put a long item into the filter. Returns `false`; the original Spark
    /// `BloomFilter.put` returns whether any bit changed, but no current Comet
    /// caller uses that, so we don't bother computing it.
    pub fn put_long(&mut self, item: i64) -> bool {
        let h1 = spark_compatible_murmur3_hash(item.to_le_bytes(), self.seed as u32);
        let h2 = spark_compatible_murmur3_hash(item.to_le_bytes(), h1);
        self.scatter(h1, h2, true);
        false
    }

    pub fn put_binary(&mut self, item: &[u8]) -> bool {
        let h1 = spark_compatible_murmur3_hash(item, self.seed as u32);
        let h2 = spark_compatible_murmur3_hash(item, h1);
        self.scatter(h1, h2, true);
        false
    }

    pub fn might_contain_long(&self, item: i64) -> bool {
        let h1 = spark_compatible_murmur3_hash(item.to_le_bytes(), self.seed as u32);
        let h2 = spark_compatible_murmur3_hash(item.to_le_bytes(), h1);
        match self.version {
            SparkBloomFilterVersion::V1 => {
                might_contain_long_v1(&self.bits, self.num_hash_functions, h1, h2)
            }
            SparkBloomFilterVersion::V2 => {
                might_contain_long_v2(&self.bits, self.num_hash_functions, h1, h2)
            }
        }
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
        assert_eq!(
            other.len(),
            self.bits.byte_size(),
            "Cannot merge SparkBloomFilters with different lengths."
        );
        self.bits.merge_bits(other);
    }
}

fn might_contain_long_v1(bits: &SparkBitArray, num_hash_functions: u32, h1: u32, h2: u32) -> bool {
    let bit_size = bits.bit_size() as i32;
    for i in 1..=num_hash_functions {
        let mut combined_hash = (h1 as i32).add_wrapping((i as i32).mul_wrapping(h2 as i32));
        if combined_hash < 0 {
            combined_hash = !combined_hash;
        }
        if !bits.get((combined_hash % bit_size) as usize) {
            return false;
        }
    }
    true
}

fn might_contain_long_v2(bits: &SparkBitArray, num_hash_functions: u32, h1: u32, h2: u32) -> bool {
    let bit_size = bits.bit_size() as i64;
    let mut combined_hash = (h1 as i32 as i64).wrapping_mul(i32::MAX as i64);
    let h2_long = h2 as i32 as i64;
    for _ in 0..num_hash_functions {
        combined_hash = combined_hash.wrapping_add(h2_long);
        let combined_index = if combined_hash < 0 {
            !combined_hash
        } else {
            combined_hash
        };
        if !bits.get((combined_index % bit_size) as usize) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a V1 filter through put + serialize + deserialize and verify
    /// `might_contain` agrees both before and after, and that the version flag
    /// is preserved across (de)serialization.
    #[test]
    fn v1_round_trip() {
        let mut filter = SparkBloomFilter::new(SparkBloomFilterVersion::V1, 4, 256, 0);
        for x in [1_i64, 42, 1_000_000, -7, i64::MIN, i64::MAX] {
            filter.put_long(x);
        }
        let bytes = filter.spark_serialization();
        // V1: [version=1][numHashFunctions][numWords][bits...]
        assert_eq!(&bytes[..4], &1_i32.to_be_bytes());

        let parsed = SparkBloomFilter::from(bytes.as_slice());
        assert_eq!(parsed.version(), SparkBloomFilterVersion::V1);
        assert_eq!(parsed.num_hash_functions, 4);
        for x in [1_i64, 42, 1_000_000, -7, i64::MIN, i64::MAX] {
            assert!(parsed.might_contain_long(x), "{x} should be present");
        }
    }

    /// Round-trip a V2 filter (Spark 4.1+ default). The serialized form has a
    /// `seed` between `numHashFunctions` and the bit array, and the hash-scattering
    /// uses 64-bit accumulator arithmetic — different from V1, so the same input
    /// produces different bit patterns.
    #[test]
    fn v2_round_trip() {
        let mut filter = SparkBloomFilter::new(SparkBloomFilterVersion::V2, 4, 256, 0);
        for x in [1_i64, 42, 1_000_000, -7, i64::MIN, i64::MAX] {
            filter.put_long(x);
        }
        let bytes = filter.spark_serialization();
        // V2: [version=2][numHashFunctions][seed][numWords][bits...]
        assert_eq!(&bytes[..4], &2_i32.to_be_bytes());
        // seed lives at offset 8 (after version + numHashFunctions)
        assert_eq!(&bytes[8..12], &0_i32.to_be_bytes());

        let parsed = SparkBloomFilter::from(bytes.as_slice());
        assert_eq!(parsed.version(), SparkBloomFilterVersion::V2);
        assert_eq!(parsed.num_hash_functions, 4);
        for x in [1_i64, 42, 1_000_000, -7, i64::MIN, i64::MAX] {
            assert!(parsed.might_contain_long(x), "{x} should be present");
        }
    }

    /// V1 and V2 use different scattering algorithms, so for the same inputs the
    /// resulting bit arrays must not match. If this test ever starts passing,
    /// the V2 implementation has likely regressed back to V1 semantics.
    #[test]
    fn v1_and_v2_produce_different_bits() {
        let inputs = [1_i64, 2, 3, 100, 1_000_000];
        let mut v1 = SparkBloomFilter::new(SparkBloomFilterVersion::V1, 4, 256, 0);
        let mut v2 = SparkBloomFilter::new(SparkBloomFilterVersion::V2, 4, 256, 0);
        for x in inputs {
            v1.put_long(x);
            v2.put_long(x);
        }
        assert_ne!(
            v1.state_as_bytes(),
            v2.state_as_bytes(),
            "V1 and V2 scattering must differ"
        );
    }

    /// The deserializer must reject an unsupported version number rather than
    /// silently producing a misconfigured filter.
    #[test]
    #[should_panic(expected = "Unsupported BloomFilter version: 3")]
    fn rejects_unknown_version() {
        let mut buf: Vec<u8> = 3_i32.to_be_bytes().to_vec();
        buf.extend_from_slice(&4_i32.to_be_bytes()); // numHashFunctions
        buf.extend_from_slice(&4_i32.to_be_bytes()); // numWords
        buf.extend_from_slice(&[0u8; 32]); // 4 words * 8 bytes
        let _ = SparkBloomFilter::from(buf.as_slice());
    }
}
