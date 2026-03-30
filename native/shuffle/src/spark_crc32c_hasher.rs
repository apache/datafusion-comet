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

//! Provide a CRC-32C implementor of [Hasher].
use std::hash::Hasher;

use crc32c::crc32c_append;

/// Implementor of [Hasher] for CRC-32C.
///
/// Note that CRC-32C produces a 32-bit hash (as [u32]),
/// but the trait requires that the output value be [u64].
///
/// This implementation is necessary because the existing [Hasher] implementation does not support
/// [Clone].
#[derive(Default, Clone)]
pub struct SparkCrc32cHasher {
    checksum: u32,
}

impl SparkCrc32cHasher {
    /// Create the [Hasher] pre-loaded with a particular checksum.
    ///
    /// Use the [Default::default()] constructor for a clean start.
    pub fn new(initial: u32) -> Self {
        Self { checksum: initial }
    }

    pub fn finalize(&self) -> u32 {
        self.checksum
    }
}

impl Hasher for SparkCrc32cHasher {
    fn finish(&self) -> u64 {
        self.checksum as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        self.checksum = crc32c_append(self.checksum, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_STRING: &[u8] =
        b"This is a very long string which is used to test the CRC-32-Castagnoli function.";
    const CHECKSUM: u32 = 0x20_CB_1E_59;

    #[test]
    fn can_hash() {
        let mut hasher = SparkCrc32cHasher::default();
        hasher.write(TEST_STRING);
        assert_eq!(hasher.finish(), CHECKSUM as u64);
    }

    /// Demonstrate writing in multiple chunks by splitting the [TEST_STRING] and getting the same
    /// [CHECKSUM].
    #[test]
    fn can_hash_in_chunks() {
        let (head, tail) = TEST_STRING.split_at(20);

        let mut hasher = SparkCrc32cHasher::default();
        hasher.write(head);
        hasher.write(tail);
        assert_eq!(hasher.finish(), CHECKSUM as u64);
    }

}
