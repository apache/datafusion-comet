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

/// A simple bit array implementation that simulates the behavior of Spark's BitArray which is
/// used in the BloomFilter implementation. Some methods are not implemented as they are not
/// required for the current use case.
#[derive(Debug, Hash)]
pub struct SparkBitArray {
    data: Vec<u64>,
    bit_count: usize,
}

impl SparkBitArray {
    pub fn new(buf: Vec<u64>) -> Self {
        let num_bits = buf.iter().map(|x| x.count_ones() as usize).sum();
        Self {
            data: buf,
            bit_count: num_bits,
        }
    }

    pub fn set(&mut self, index: usize) -> bool {
        if !self.get(index) {
            // see the get method for the explanation of the shift operators
            self.data[index >> 6] |= 1u64 << (index & 0x3f);
            self.bit_count += 1;
            true
        } else {
            false
        }
    }

    pub fn get(&self, index: usize) -> bool {
        // Java version: (data[(int) (index >> 6)] & (1L << (index))) != 0
        // Rust and Java have different semantics for the shift operators. Java's shift operators
        // explicitly mask the right-hand operand with 0x3f [1], while Rust's shift operators does
        // not do this, it will panic with shift left with overflow for large right-hand operand.
        // To fix this, we need to mask the right-hand operand with 0x3f in the rust side.
        // [1]: https://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.19
        (self.data[index >> 6] & (1u64 << (index & 0x3f))) != 0
    }

    pub fn bit_size(&self) -> u64 {
        self.data.len() as u64 * 64
    }

    pub fn cardinality(&self) -> usize {
        self.bit_count
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_spark_bit_array() {
        let buf = vec![0u64; 4];
        let mut array = SparkBitArray::new(buf);
        assert_eq!(array.bit_size(), 256);
        assert_eq!(array.cardinality(), 0);

        assert!(!array.get(0));
        assert!(!array.get(1));
        assert!(!array.get(63));
        assert!(!array.get(64));
        assert!(!array.get(65));
        assert!(!array.get(127));
        assert!(!array.get(128));
        assert!(!array.get(129));

        assert!(array.set(0));
        assert!(array.set(1));
        assert!(array.set(63));
        assert!(array.set(64));
        assert!(array.set(65));
        assert!(array.set(127));
        assert!(array.set(128));
        assert!(array.set(129));

        assert_eq!(array.cardinality(), 8);
        assert_eq!(array.bit_size(), 256);

        assert!(array.get(0));
        // already set so should return false
        assert!(!array.set(0));

        // not set values should return false for get
        assert!(!array.get(2));
        assert!(!array.get(62));
    }

    #[test]
    fn test_spark_bit_with_non_empty_buffer() {
        let buf = vec![8u64; 4];
        let mut array = SparkBitArray::new(buf);
        assert_eq!(array.bit_size(), 256);
        assert_eq!(array.cardinality(), 4);

        // already set bits should return true
        assert!(array.get(3));
        assert!(array.get(67));
        assert!(array.get(131));
        assert!(array.get(195));

        // other unset bits should return false
        assert!(!array.get(0));
        assert!(!array.get(1));

        // set bits
        assert!(array.set(0));
        assert!(array.set(1));

        // check cardinality
        assert_eq!(array.cardinality(), 6);
    }
}
