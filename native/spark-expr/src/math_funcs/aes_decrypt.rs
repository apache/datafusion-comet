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

use std::sync::Arc;

use aes::cipher::consts::{U12, U16};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, KeyInit, Nonce};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow::datatypes::DataType;
use cbc::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
use datafusion::common::{exec_err, DataFusionError};
use datafusion::logical_expr::ColumnarValue;

const GCM_IV_LEN: usize = 12;
const CBC_IV_LEN: usize = 16;

#[derive(Clone, Copy)]
enum AesMode {
    Ecb,
    Cbc,
    Gcm,
}

impl AesMode {
    fn from_mode_padding(mode: &str, padding: &str) -> Result<Self, DataFusionError> {
        let is_none = padding.eq_ignore_ascii_case("NONE");
        let is_pkcs = padding.eq_ignore_ascii_case("PKCS");
        let is_default = padding.eq_ignore_ascii_case("DEFAULT");

        if mode.eq_ignore_ascii_case("ECB") && (is_pkcs || is_default) {
            Ok(Self::Ecb)
        } else if mode.eq_ignore_ascii_case("CBC") && (is_pkcs || is_default) {
            Ok(Self::Cbc)
        } else if mode.eq_ignore_ascii_case("GCM") && (is_none || is_default) {
            Ok(Self::Gcm)
        } else {
            exec_err!("Unsupported AES mode/padding combination: {mode}/{padding}")
        }
    }
}

enum BinaryArg<'a> {
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}

impl<'a> BinaryArg<'a> {
    fn from(arg_name: &str, arr: &'a ArrayRef) -> Result<Self, DataFusionError> {
        match arr.data_type() {
            DataType::Binary => Ok(Self::Binary(
                arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Failed to downcast {arg_name} to BinaryArray"
                    ))
                })?,
            )),
            DataType::LargeBinary => Ok(Self::LargeBinary(
                arr.as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to downcast {arg_name} to LargeBinaryArray"
                        ))
                    })?,
            )),
            other => exec_err!("{arg_name} must be Binary/LargeBinary, got {other:?}"),
        }
    }

    fn value(&self, i: usize) -> Option<&'a [u8]> {
        match self {
            Self::Binary(arr) => (!arr.is_null(i)).then(|| arr.value(i)),
            Self::LargeBinary(arr) => (!arr.is_null(i)).then(|| arr.value(i)),
        }
    }
}

enum StringArg<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> StringArg<'a> {
    fn from(arg_name: &str, arr: &'a ArrayRef) -> Result<Self, DataFusionError> {
        match arr.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(
                arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Failed to downcast {arg_name} to StringArray"
                    ))
                })?,
            )),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(
                arr.as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to downcast {arg_name} to LargeStringArray"
                        ))
                    })?,
            )),
            other => exec_err!("{arg_name} must be Utf8/LargeUtf8, got {other:?}"),
        }
    }

    fn value(&self, i: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(arr) => (!arr.is_null(i)).then(|| arr.value(i)),
            Self::LargeUtf8(arr) => (!arr.is_null(i)).then(|| arr.value(i)),
        }
    }
}

type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes192CbcDec = cbc::Decryptor<Aes192>;
type Aes256CbcDec = cbc::Decryptor<Aes256>;
type Aes128EcbDec = ecb::Decryptor<Aes128>;
type Aes192EcbDec = ecb::Decryptor<Aes192>;
type Aes256EcbDec = ecb::Decryptor<Aes256>;
type Aes192Gcm = AesGcm<Aes192, U12, U16>;

fn decrypt_pkcs_cbc(input: &[u8], key: &[u8]) -> Result<Vec<u8>, DataFusionError> {
    if input.len() < CBC_IV_LEN {
        return exec_err!("AES decryption input is too short for CBC");
    }
    let (iv, ciphertext) = input.split_at(CBC_IV_LEN);
    let mut buf = ciphertext.to_vec();

    let out = match key.len() {
        16 => Aes128CbcDec::new_from_slices(key, iv)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?,
        24 => Aes192CbcDec::new_from_slices(key, iv)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?,
        32 => Aes256CbcDec::new_from_slices(key, iv)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?,
        _ => return exec_err!("Invalid AES key length: {}", key.len()),
    };

    Ok(out.to_vec())
}

fn decrypt_pkcs_ecb(input: &[u8], key: &[u8]) -> Result<Vec<u8>, DataFusionError> {
    let mut buf = input.to_vec();

    let out = match key.len() {
        16 => Aes128EcbDec::new_from_slice(key)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?,
        24 => Aes192EcbDec::new_from_slice(key)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?,
        32 => Aes256EcbDec::new_from_slice(key)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt_padded_mut::<Pkcs7>(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?,
        _ => return exec_err!("Invalid AES key length: {}", key.len()),
    };

    Ok(out.to_vec())
}

fn decrypt_gcm(input: &[u8], key: &[u8], aad: &[u8]) -> Result<Vec<u8>, DataFusionError> {
    if input.len() < GCM_IV_LEN {
        return exec_err!("AES decryption input is too short for GCM");
    }
    let (iv, ciphertext) = input.split_at(GCM_IV_LEN);
    let nonce = Nonce::from_slice(iv);
    let payload = Payload {
        msg: ciphertext,
        aad,
    };

    match key.len() {
        16 => Aes128Gcm::new_from_slice(key)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt(nonce, payload)
            .map_err(|_| {
                DataFusionError::Execution("AES crypto error: decrypt failed".to_string())
            }),
        24 => Aes192Gcm::new_from_slice(key)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt(nonce, payload)
            .map_err(|_| {
                DataFusionError::Execution("AES crypto error: decrypt failed".to_string())
            }),
        32 => Aes256Gcm::new_from_slice(key)
            .map_err(|e| DataFusionError::Execution(format!("AES crypto error: {e}")))?
            .decrypt(nonce, payload)
            .map_err(|_| {
                DataFusionError::Execution("AES crypto error: decrypt failed".to_string())
            }),
        _ => exec_err!("Invalid AES key length: {}", key.len()),
    }
}

fn decrypt_one(
    input: &[u8],
    key: &[u8],
    mode: &str,
    padding: &str,
    aad: &[u8],
) -> Result<Vec<u8>, DataFusionError> {
    match AesMode::from_mode_padding(mode, padding)? {
        AesMode::Ecb => decrypt_pkcs_ecb(input, key),
        AesMode::Cbc => decrypt_pkcs_cbc(input, key),
        AesMode::Gcm => decrypt_gcm(input, key, aad),
    }
}

pub fn spark_aes_decrypt(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if !(2..=5).contains(&args.len()) {
        return exec_err!("aes_decrypt expects 2 to 5 arguments, got {}", args.len());
    }

    let are_scalars = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let num_rows = arrays[0].len();

    let input = BinaryArg::from("input", &arrays[0])?;
    let key = BinaryArg::from("key", &arrays[1])?;

    let mode = if args.len() >= 3 {
        Some(StringArg::from("mode", &arrays[2])?)
    } else {
        None
    };
    let padding = if args.len() >= 4 {
        Some(StringArg::from("padding", &arrays[3])?)
    } else {
        None
    };
    let aad = if args.len() >= 5 {
        Some(BinaryArg::from("aad", &arrays[4])?)
    } else {
        None
    };

    let mut builder = BinaryBuilder::new();

    for row in 0..num_rows {
        let Some(input_value) = input.value(row) else {
            builder.append_null();
            continue;
        };
        let Some(key_value) = key.value(row) else {
            builder.append_null();
            continue;
        };

        let mode_value = match mode.as_ref() {
            Some(mode) => {
                let Some(mode) = mode.value(row) else {
                    builder.append_null();
                    continue;
                };
                mode
            }
            None => "GCM",
        };

        let padding_value = match padding.as_ref() {
            Some(padding) => {
                let Some(padding) = padding.value(row) else {
                    builder.append_null();
                    continue;
                };
                padding
            }
            None => "DEFAULT",
        };

        let aad_value = match aad.as_ref() {
            Some(aad) => {
                let Some(aad) = aad.value(row) else {
                    builder.append_null();
                    continue;
                };
                aad
            }
            None => &[],
        };

        let plaintext = decrypt_one(input_value, key_value, mode_value, padding_value, aad_value)?;
        builder.append_value(plaintext);
    }

    let array = Arc::new(builder.finish());
    if are_scalars {
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::try_from_array(array.as_ref(), 0)?,
        ))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}
