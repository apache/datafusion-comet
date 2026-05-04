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

use crate::spark_crc32c_hasher::SparkCrc32cHasher;
use bytes::Buf;
use datafusion_comet_jni_bridge::errors::{CometError, CometResult};
use simd_adler32::Adler32;
use std::default::Default;
use std::hash::Hasher;
use std::io::{Cursor, SeekFrom};

/// Checksum algorithms for writing IPC bytes.
#[derive(Clone)]
pub(crate) enum Checksum {
    /// CRC32 checksum algorithm.
    CRC32(crc32fast::Hasher),
    /// Adler32 checksum algorithm.
    Adler32(Adler32),
    /// CRC32C checksum algorithm.
    CRC32C(SparkCrc32cHasher),
}

impl Checksum {
    pub(crate) fn try_new(algo: i32, initial_opt: Option<u32>) -> CometResult<Self> {
        match algo {
            0 => {
                let hasher = if let Some(initial) = initial_opt {
                    crc32fast::Hasher::new_with_initial(initial)
                } else {
                    crc32fast::Hasher::new()
                };
                Ok(Checksum::CRC32(hasher))
            }
            1 => {
                let hasher = if let Some(initial) = initial_opt {
                    // Note that Adler32 initial state is not zero.
                    // i.e., `Adler32::from_checksum(0)` is not the same as `Adler32::new()`.
                    Adler32::from_checksum(initial)
                } else {
                    Adler32::new()
                };
                Ok(Checksum::Adler32(hasher))
            }
            2 => {
                let hasher = if let Some(initial) = initial_opt {
                    SparkCrc32cHasher::new(initial)
                } else {
                    Default::default()
                };
                Ok(Checksum::CRC32C(hasher))
            }
            _ => Err(CometError::Internal(
                "Unsupported checksum algorithm".to_string(),
            )),
        }
    }

    pub(crate) fn update(&mut self, cursor: &mut Cursor<&mut Vec<u8>>) -> CometResult<()> {
        match self {
            Checksum::CRC32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.update(cursor.chunk());
                Ok(())
            }
            Checksum::Adler32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.write(cursor.chunk());
                Ok(())
            }
            Checksum::CRC32C(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.write(cursor.chunk());
                Ok(())
            }
        }
    }

    pub(crate) fn finalize(self) -> u32 {
        match self {
            Checksum::CRC32(hasher) => hasher.finalize(),
            Checksum::Adler32(hasher) => hasher.finish(),
            Checksum::CRC32C(hasher) => hasher.finalize(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_crc32() {
        let mut checksum = Checksum::try_new(0, None).unwrap();
        let message = b"123456789";

        let mut vector: Vec<u8> = message.to_vec();
        let mut buff = Cursor::new(&mut vector);

        checksum.update(&mut buff).unwrap();
        let result = checksum.finalize();

        let expected_crc = 0xcbf43926u32;
        assert_eq!(result, expected_crc)
    }

    #[test]
    fn test_adler32() {
        let mut checksum = Checksum::try_new(1, None).unwrap();
        let message = b"123456789";

        let mut vector: Vec<u8> = message.to_vec();
        let mut buff = Cursor::new(&mut vector);

        checksum.update(&mut buff).unwrap();
        let result = checksum.finalize();

        let expected_crc = 0x091e01deu32;
        assert_eq!(result, expected_crc)
    }

    #[test]
    fn test_crc32c() {
        let mut checksum = Checksum::try_new(2, None).unwrap();
        let message = b"123456789";

        let mut vector: Vec<u8> = message.to_vec();
        let mut buff = Cursor::new(&mut vector);

        checksum.update(&mut buff).unwrap();
        let result = checksum.finalize();

        let expected_crc = 0xe3069283u32;
        assert_eq!(result, expected_crc)
    }
}
