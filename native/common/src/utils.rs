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

/// Converts a slice of bytes to i128. The bytes are serialized in big-endian order by
/// `BigInteger.toByteArray()` in Java.
pub fn bytes_to_i128(slice: &[u8]) -> i128 {
    let mut bytes = [0; 16];
    let mut i = 0;
    while i != 16 && i != slice.len() {
        bytes[i] = slice[slice.len() - 1 - i];
        i += 1;
    }

    // if the decimal is negative, we need to flip all the bits
    if (slice[0] as i8) < 0 {
        while i < 16 {
            bytes[i] = !bytes[i];
            i += 1;
        }
    }

    i128::from_le_bytes(bytes)
}
