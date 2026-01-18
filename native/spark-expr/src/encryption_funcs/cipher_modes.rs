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

use super::crypto_utils::{
    generate_random_iv, validate_iv_length, validate_key_length, CryptoError,
};

pub trait CipherMode: Send + Sync + std::fmt::Debug {
    #[allow(dead_code)]
    fn name(&self) -> &str;
    #[allow(dead_code)]
    fn iv_length(&self) -> usize;
    #[allow(dead_code)]
    fn supports_aad(&self) -> bool;

    fn encrypt(
        &self,
        input: &[u8],
        key: &[u8],
        iv: Option<&[u8]>,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptoError>;
}

#[derive(Debug)]
pub struct EcbMode;
#[derive(Debug)]
pub struct CbcMode;
#[derive(Debug)]
pub struct GcmMode;

impl CipherMode for EcbMode {
    fn name(&self) -> &str {
        "ECB"
    }

    fn iv_length(&self) -> usize {
        0
    }

    fn supports_aad(&self) -> bool {
        false
    }

    fn encrypt(
        &self,
        input: &[u8],
        key: &[u8],
        iv: Option<&[u8]>,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptoError> {
        use aes::{Aes128, Aes192, Aes256};
        use cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyInit};
        use ecb::Encryptor;

        validate_key_length(key)?;

        if iv.is_some() {
            return Err(CryptoError::UnsupportedIv("ECB".to_string()));
        }
        if aad.is_some() {
            return Err(CryptoError::UnsupportedAad("ECB".to_string()));
        }

        let encrypted = match key.len() {
            16 => {
                let cipher = Encryptor::<Aes128>::new(key.into());
                cipher.encrypt_padded_vec_mut::<Pkcs7>(input)
            }
            24 => {
                let cipher = Encryptor::<Aes192>::new(key.into());
                cipher.encrypt_padded_vec_mut::<Pkcs7>(input)
            }
            32 => {
                let cipher = Encryptor::<Aes256>::new(key.into());
                cipher.encrypt_padded_vec_mut::<Pkcs7>(input)
            }
            _ => unreachable!("Key length validated above"),
        };

        Ok(encrypted)
    }
}

impl CipherMode for CbcMode {
    fn name(&self) -> &str {
        "CBC"
    }

    fn iv_length(&self) -> usize {
        16
    }

    fn supports_aad(&self) -> bool {
        false
    }

    fn encrypt(
        &self,
        input: &[u8],
        key: &[u8],
        iv: Option<&[u8]>,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptoError> {
        use aes::{Aes128, Aes192, Aes256};
        use cbc::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
        use cbc::Encryptor;

        validate_key_length(key)?;

        if aad.is_some() {
            return Err(CryptoError::UnsupportedAad("CBC".to_string()));
        }

        let iv_bytes = match iv {
            Some(iv) => {
                validate_iv_length(iv, 16)?;
                iv.to_vec()
            }
            None => generate_random_iv(16),
        };

        let ciphertext = match key.len() {
            16 => {
                let cipher = Encryptor::<Aes128>::new(key.into(), iv_bytes.as_slice().into());
                cipher.encrypt_padded_vec_mut::<Pkcs7>(input)
            }
            24 => {
                let cipher = Encryptor::<Aes192>::new(key.into(), iv_bytes.as_slice().into());
                cipher.encrypt_padded_vec_mut::<Pkcs7>(input)
            }
            32 => {
                let cipher = Encryptor::<Aes256>::new(key.into(), iv_bytes.as_slice().into());
                cipher.encrypt_padded_vec_mut::<Pkcs7>(input)
            }
            _ => unreachable!("Key length validated above"),
        };

        let mut result = iv_bytes;
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }
}

impl CipherMode for GcmMode {
    fn name(&self) -> &str {
        "GCM"
    }

    fn iv_length(&self) -> usize {
        12
    }

    fn supports_aad(&self) -> bool {
        true
    }

    fn encrypt(
        &self,
        input: &[u8],
        key: &[u8],
        iv: Option<&[u8]>,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>, CryptoError> {
        use aes_gcm::aead::{Aead, Payload};
        use aes_gcm::{Aes128Gcm, Aes256Gcm, KeyInit, Nonce};

        validate_key_length(key)?;

        let iv_bytes = match iv {
            Some(iv) => {
                validate_iv_length(iv, 12)?;
                iv.to_vec()
            }
            None => generate_random_iv(12),
        };

        let nonce = Nonce::from_slice(&iv_bytes);

        let ciphertext = match key.len() {
            16 => {
                let cipher = Aes128Gcm::new(key.into());
                let payload = match aad {
                    Some(aad_data) => Payload {
                        msg: input,
                        aad: aad_data,
                    },
                    None => Payload {
                        msg: input,
                        aad: &[],
                    },
                };
                cipher
                    .encrypt(nonce, payload)
                    .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?
            }
            24 | 32 => {
                let cipher = Aes256Gcm::new(key.into());
                let payload = match aad {
                    Some(aad_data) => Payload {
                        msg: input,
                        aad: aad_data,
                    },
                    None => Payload {
                        msg: input,
                        aad: &[],
                    },
                };
                cipher
                    .encrypt(nonce, payload)
                    .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?
            }
            _ => unreachable!("Key length validated above"),
        };

        let mut result = iv_bytes;
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }
}

pub fn get_cipher_mode(mode: &str, padding: &str) -> Result<Box<dyn CipherMode>, CryptoError> {
    let mode_upper = mode.to_uppercase();
    let padding_upper = padding.to_uppercase();

    match (mode_upper.as_str(), padding_upper.as_str()) {
        ("ECB", "PKCS" | "DEFAULT") => Ok(Box::new(EcbMode)),
        ("CBC", "PKCS" | "DEFAULT") => Ok(Box::new(CbcMode)),
        ("GCM", "NONE" | "DEFAULT") => Ok(Box::new(GcmMode)),
        _ => Err(CryptoError::UnsupportedMode(
            mode.to_string(),
            padding.to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ecb_mode_properties() {
        let mode = EcbMode;
        assert_eq!(mode.name(), "ECB");
        assert_eq!(mode.iv_length(), 0);
        assert!(!mode.supports_aad());
    }

    #[test]
    fn test_ecb_encrypt_basic() {
        let mode = EcbMode;
        let input = b"Spark SQL";
        let key = b"1234567890abcdef";

        let result = mode.encrypt(input, key, None, None);
        assert!(result.is_ok());
        let encrypted = result.unwrap();
        assert!(!encrypted.is_empty());
        assert_ne!(&encrypted[..], input);
    }

    #[test]
    fn test_ecb_rejects_iv() {
        let mode = EcbMode;
        let input = b"test";
        let key = b"1234567890abcdef";
        let iv = vec![0u8; 16];

        let result = mode.encrypt(input, key, Some(&iv), None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CryptoError::UnsupportedIv(_)));
    }

    #[test]
    fn test_ecb_rejects_aad() {
        let mode = EcbMode;
        let input = b"test";
        let key = b"1234567890abcdef";
        let aad = b"metadata";

        let result = mode.encrypt(input, key, None, Some(aad));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CryptoError::UnsupportedAad(_)
        ));
    }

    #[test]
    fn test_ecb_invalid_key() {
        let mode = EcbMode;
        let input = b"test";
        let key = b"short";

        let result = mode.encrypt(input, key, None, None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CryptoError::InvalidKeyLength(_)
        ));
    }

    #[test]
    fn test_cbc_mode_properties() {
        let mode = CbcMode;
        assert_eq!(mode.name(), "CBC");
        assert_eq!(mode.iv_length(), 16);
        assert!(!mode.supports_aad());
    }

    #[test]
    fn test_cbc_encrypt_generates_iv() {
        let mode = CbcMode;
        let input = b"Apache Spark";
        let key = b"1234567890abcdef";

        let result = mode.encrypt(input, key, None, None);
        assert!(result.is_ok());
        let encrypted = result.unwrap();
        assert!(encrypted.len() > 16);
    }

    #[test]
    fn test_cbc_encrypt_with_provided_iv() {
        let mode = CbcMode;
        let input = b"test";
        let key = b"1234567890abcdef";
        let iv = vec![0u8; 16];

        let result = mode.encrypt(input, key, Some(&iv), None);
        assert!(result.is_ok());
        let encrypted = result.unwrap();
        assert_eq!(&encrypted[..16], &iv[..]);
    }

    #[test]
    fn test_cbc_rejects_aad() {
        let mode = CbcMode;
        let input = b"test";
        let key = b"1234567890abcdef";
        let aad = b"metadata";

        let result = mode.encrypt(input, key, None, Some(aad));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CryptoError::UnsupportedAad(_)
        ));
    }

    #[test]
    fn test_cbc_invalid_iv_length() {
        let mode = CbcMode;
        let input = b"test";
        let key = b"1234567890abcdef";
        let iv = vec![0u8; 8];

        let result = mode.encrypt(input, key, Some(&iv), None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CryptoError::InvalidIvLength { .. }
        ));
    }

    #[test]
    fn test_gcm_mode_properties() {
        let mode = GcmMode;
        assert_eq!(mode.name(), "GCM");
        assert_eq!(mode.iv_length(), 12);
        assert!(mode.supports_aad());
    }

    #[test]
    fn test_gcm_encrypt_generates_iv() {
        let mode = GcmMode;
        let input = b"Spark";
        let key = b"0000111122223333";

        let result = mode.encrypt(input, key, None, None);
        assert!(result.is_ok());
        let encrypted = result.unwrap();
        assert!(encrypted.len() > 12);
    }

    #[test]
    fn test_gcm_encrypt_with_aad() {
        let mode = GcmMode;
        let input = b"Spark";
        let key = b"abcdefghijklmnop12345678ABCDEFGH";
        let iv = vec![0u8; 12];
        let aad = b"This is an AAD mixed into the input";

        let result = mode.encrypt(input, key, Some(&iv), Some(aad));
        assert!(result.is_ok());
    }

    #[test]
    fn test_gcm_invalid_iv_length() {
        let mode = GcmMode;
        let input = b"test";
        let key = b"1234567890abcdef";
        let iv = vec![0u8; 16];

        let result = mode.encrypt(input, key, Some(&iv), None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CryptoError::InvalidIvLength { .. }
        ));
    }

    #[test]
    fn test_get_cipher_mode_ecb() {
        let mode = get_cipher_mode("ECB", "PKCS");
        assert!(mode.is_ok());
        assert_eq!(mode.unwrap().name(), "ECB");
    }

    #[test]
    fn test_get_cipher_mode_cbc() {
        let mode = get_cipher_mode("CBC", "PKCS");
        assert!(mode.is_ok());
        assert_eq!(mode.unwrap().name(), "CBC");
    }

    #[test]
    fn test_get_cipher_mode_gcm() {
        let mode = get_cipher_mode("GCM", "NONE");
        assert!(mode.is_ok());
        assert_eq!(mode.unwrap().name(), "GCM");
    }

    #[test]
    fn test_get_cipher_mode_default_padding() {
        let mode = get_cipher_mode("GCM", "DEFAULT");
        assert!(mode.is_ok());
        assert_eq!(mode.unwrap().name(), "GCM");
    }

    #[test]
    fn test_get_cipher_mode_invalid() {
        let mode = get_cipher_mode("CTR", "NONE");
        assert!(mode.is_err());
        assert!(matches!(
            mode.unwrap_err(),
            CryptoError::UnsupportedMode(_, _)
        ));
    }

    #[test]
    fn test_get_cipher_mode_invalid_combination() {
        let mode = get_cipher_mode("GCM", "PKCS");
        assert!(mode.is_err());
    }
}
