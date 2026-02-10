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

use datafusion::common::DataFusionError;

#[derive(Debug, PartialEq)]
pub enum CryptoError {
    InvalidKeyLength(usize),
    InvalidIvLength { expected: usize, actual: usize },
    UnsupportedMode(String, String),
    UnsupportedIv(String),
    UnsupportedAad(String),
    EncryptionFailed(String),
}

impl From<CryptoError> for DataFusionError {
    fn from(err: CryptoError) -> Self {
        DataFusionError::Execution(format!("{:?}", err))
    }
}

pub fn validate_key_length(key: &[u8]) -> Result<(), CryptoError> {
    match key.len() {
        16 | 24 | 32 => Ok(()),
        len => Err(CryptoError::InvalidKeyLength(len)),
    }
}

pub fn generate_random_iv(length: usize) -> Vec<u8> {
    use rand::Rng;
    let mut iv = vec![0u8; length];
    rand::rng().fill(&mut iv[..]);
    iv
}

pub fn validate_iv_length(iv: &[u8], expected: usize) -> Result<(), CryptoError> {
    if iv.len() == expected {
        Ok(())
    } else {
        Err(CryptoError::InvalidIvLength {
            expected,
            actual: iv.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_key_length_valid_16() {
        let key = vec![0u8; 16];
        assert!(validate_key_length(&key).is_ok());
    }

    #[test]
    fn test_validate_key_length_valid_24() {
        let key = vec![0u8; 24];
        assert!(validate_key_length(&key).is_ok());
    }

    #[test]
    fn test_validate_key_length_valid_32() {
        let key = vec![0u8; 32];
        assert!(validate_key_length(&key).is_ok());
    }

    #[test]
    fn test_validate_key_length_invalid_short() {
        let key = vec![0u8; 8];
        let result = validate_key_length(&key);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CryptoError::InvalidKeyLength(8));
    }

    #[test]
    fn test_validate_key_length_invalid_long() {
        let key = vec![0u8; 64];
        let result = validate_key_length(&key);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CryptoError::InvalidKeyLength(64));
    }

    #[test]
    fn test_validate_key_length_invalid_zero() {
        let key = vec![];
        let result = validate_key_length(&key);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CryptoError::InvalidKeyLength(0));
    }

    #[test]
    fn test_generate_random_iv_length_12() {
        let iv = generate_random_iv(12);
        assert_eq!(iv.len(), 12);
    }

    #[test]
    fn test_generate_random_iv_length_16() {
        let iv = generate_random_iv(16);
        assert_eq!(iv.len(), 16);
    }

    #[test]
    fn test_generate_random_iv_is_random() {
        let iv1 = generate_random_iv(16);
        let iv2 = generate_random_iv(16);
        assert_ne!(iv1, iv2);
    }

    #[test]
    fn test_validate_iv_length_valid() {
        let iv = vec![0u8; 16];
        assert!(validate_iv_length(&iv, 16).is_ok());
    }

    #[test]
    fn test_validate_iv_length_too_short() {
        let iv = vec![0u8; 8];
        let result = validate_iv_length(&iv, 16);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            CryptoError::InvalidIvLength {
                expected: 16,
                actual: 8
            }
        );
    }

    #[test]
    fn test_validate_iv_length_too_long() {
        let iv = vec![0u8; 20];
        let result = validate_iv_length(&iv, 16);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            CryptoError::InvalidIvLength {
                expected: 16,
                actual: 20
            }
        );
    }
}
