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

use arrow::array::{Array, BinaryArray, BinaryBuilder, StringArray};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

use super::cipher_modes::get_cipher_mode;

pub fn spark_aes_encrypt(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() < 2 || args.len() > 6 {
        return Err(DataFusionError::Execution(format!(
            "aes_encrypt expects 2-6 arguments, got {}",
            args.len()
        )));
    }

    let mode_default = ColumnarValue::Scalar(ScalarValue::Utf8(Some("GCM".to_string())));
    let padding_default = ColumnarValue::Scalar(ScalarValue::Utf8(Some("DEFAULT".to_string())));
    let iv_default = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![])));
    let aad_default = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![])));

    let input_arg = &args[0];
    let key_arg = &args[1];
    let mode_arg = args.get(2).unwrap_or(&mode_default);
    let padding_arg = args.get(3).unwrap_or(&padding_default);
    let iv_arg = args.get(4).unwrap_or(&iv_default);
    let aad_arg = args.get(5).unwrap_or(&aad_default);

    let batch_size = get_batch_size(args)?;

    if batch_size == 1 {
        encrypt_scalar(input_arg, key_arg, mode_arg, padding_arg, iv_arg, aad_arg)
    } else {
        encrypt_batch(
            input_arg, key_arg, mode_arg, padding_arg, iv_arg, aad_arg, batch_size,
        )
    }
}

fn encrypt_scalar(
    input_arg: &ColumnarValue,
    key_arg: &ColumnarValue,
    mode_arg: &ColumnarValue,
    padding_arg: &ColumnarValue,
    iv_arg: &ColumnarValue,
    aad_arg: &ColumnarValue,
) -> Result<ColumnarValue> {
    let input = match input_arg {
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => opt,
        _ => return Err(DataFusionError::Execution("Invalid input type".to_string())),
    };

    let key = match key_arg {
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => opt,
        _ => return Err(DataFusionError::Execution("Invalid key type".to_string())),
    };

    if input.is_none() || key.is_none() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
    }

    let mode = match mode_arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.as_str(),
        _ => "GCM",
    };

    let padding = match padding_arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.as_str(),
        _ => "DEFAULT",
    };

    let iv = match iv_arg {
        ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) if !v.is_empty() => Some(v.as_slice()),
        _ => None,
    };

    let aad = match aad_arg {
        ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) if !v.is_empty() => {
            Some(v.as_slice())
        }
        _ => None,
    };

    let cipher = get_cipher_mode(mode, padding)?;

    let encrypted = cipher
        .encrypt(input.as_ref().unwrap(), key.as_ref().unwrap(), iv, aad)
        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

    Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
        encrypted,
    ))))
}

fn encrypt_batch(
    input_arg: &ColumnarValue,
    key_arg: &ColumnarValue,
    mode_arg: &ColumnarValue,
    padding_arg: &ColumnarValue,
    iv_arg: &ColumnarValue,
    aad_arg: &ColumnarValue,
    batch_size: usize,
) -> Result<ColumnarValue> {
    let input_array = to_binary_array(input_arg, batch_size)?;
    let key_array = to_binary_array(key_arg, batch_size)?;
    let mode_array = to_string_array(mode_arg, batch_size)?;
    let padding_array = to_string_array(padding_arg, batch_size)?;
    let iv_array = to_binary_array(iv_arg, batch_size)?;
    let aad_array = to_binary_array(aad_arg, batch_size)?;

    let mut builder = BinaryBuilder::new();

    for i in 0..batch_size {
        if input_array.is_null(i) || key_array.is_null(i) {
            builder.append_null();
            continue;
        }

        let input = input_array.value(i);
        let key = key_array.value(i);
        let mode = mode_array.value(i);
        let padding = padding_array.value(i);
        let iv = if iv_array.is_null(i) || iv_array.value(i).is_empty() {
            None
        } else {
            Some(iv_array.value(i))
        };
        let aad = if aad_array.is_null(i) || aad_array.value(i).is_empty() {
            None
        } else {
            Some(aad_array.value(i))
        };

        match get_cipher_mode(mode, padding) {
            Ok(cipher) => match cipher.encrypt(input, key, iv, aad) {
                Ok(encrypted) => builder.append_value(&encrypted),
                Err(_) => builder.append_null(),
            },
            Err(_) => builder.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn get_batch_size(args: &[ColumnarValue]) -> Result<usize> {
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            return Ok(array.len());
        }
    }
    Ok(1)
}

fn to_binary_array(col: &ColumnarValue, size: usize) -> Result<BinaryArray> {
    match col {
        ColumnarValue::Array(array) => Ok(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected binary array".to_string()))?
            .clone()),
        ColumnarValue::Scalar(ScalarValue::Binary(opt_val)) => {
            let mut builder = BinaryBuilder::new();
            for _ in 0..size {
                if let Some(val) = opt_val {
                    builder.append_value(val);
                } else {
                    builder.append_null();
                }
            }
            Ok(builder.finish())
        }
        _ => Err(DataFusionError::Execution(
            "Invalid argument type".to_string(),
        )),
    }
}

fn to_string_array(col: &ColumnarValue, size: usize) -> Result<StringArray> {
    match col {
        ColumnarValue::Array(array) => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected string array".to_string()))?
            .clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8(opt_val)) => {
            let val = opt_val.as_deref().unwrap_or("GCM");
            Ok(StringArray::from(vec![val; size]))
        }
        _ => Err(DataFusionError::Execution(
            "Invalid argument type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BinaryArray;
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_aes_encrypt_basic_gcm() {
        let input = ScalarValue::Binary(Some(b"Spark".to_vec()));
        let key = ScalarValue::Binary(Some(b"0000111122223333".to_vec()));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());

        if let ColumnarValue::Scalar(ScalarValue::Binary(Some(encrypted))) = result.unwrap() {
            assert!(encrypted.len() > 12);
        } else {
            panic!("Expected binary scalar result");
        }
    }

    #[test]
    fn test_aes_encrypt_with_mode() {
        let input = ScalarValue::Binary(Some(b"Spark SQL".to_vec()));
        let key = ScalarValue::Binary(Some(b"1234567890abcdef".to_vec()));
        let mode = ScalarValue::Utf8(Some("ECB".to_string()));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
            ColumnarValue::Scalar(mode),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_aes_encrypt_with_mode_padding() {
        let input = ScalarValue::Binary(Some(b"test".to_vec()));
        let key = ScalarValue::Binary(Some(b"1234567890abcdef".to_vec()));
        let mode = ScalarValue::Utf8(Some("CBC".to_string()));
        let padding = ScalarValue::Utf8(Some("PKCS".to_string()));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
            ColumnarValue::Scalar(mode),
            ColumnarValue::Scalar(padding),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_aes_encrypt_with_iv() {
        let input = ScalarValue::Binary(Some(b"Apache Spark".to_vec()));
        let key = ScalarValue::Binary(Some(b"1234567890abcdef".to_vec()));
        let mode = ScalarValue::Utf8(Some("CBC".to_string()));
        let padding = ScalarValue::Utf8(Some("PKCS".to_string()));
        let iv = ScalarValue::Binary(Some(vec![0u8; 16]));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
            ColumnarValue::Scalar(mode),
            ColumnarValue::Scalar(padding),
            ColumnarValue::Scalar(iv.clone()),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());

        if let ColumnarValue::Scalar(ScalarValue::Binary(Some(encrypted))) = result.unwrap() {
            if let ScalarValue::Binary(Some(iv_bytes)) = iv {
                assert_eq!(&encrypted[..16], &iv_bytes[..]);
            }
        }
    }

    #[test]
    fn test_aes_encrypt_gcm_with_aad() {
        let input = ScalarValue::Binary(Some(b"Spark".to_vec()));
        let key = ScalarValue::Binary(Some(b"abcdefghijklmnop12345678ABCDEFGH".to_vec()));
        let mode = ScalarValue::Utf8(Some("GCM".to_string()));
        let padding = ScalarValue::Utf8(Some("DEFAULT".to_string()));
        let iv = ScalarValue::Binary(Some(vec![0u8; 12]));
        let aad = ScalarValue::Binary(Some(b"This is an AAD mixed into the input".to_vec()));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
            ColumnarValue::Scalar(mode),
            ColumnarValue::Scalar(padding),
            ColumnarValue::Scalar(iv),
            ColumnarValue::Scalar(aad),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_aes_encrypt_invalid_key_length() {
        let input = ScalarValue::Binary(Some(b"test".to_vec()));
        let key = ScalarValue::Binary(Some(b"short".to_vec()));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_err() || matches!(result.unwrap(), ColumnarValue::Scalar(ScalarValue::Binary(None))));
    }

    #[test]
    fn test_aes_encrypt_null_input() {
        let input = ScalarValue::Binary(None);
        let key = ScalarValue::Binary(Some(b"0000111122223333".to_vec()));

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ColumnarValue::Scalar(ScalarValue::Binary(None))));
    }

    #[test]
    fn test_aes_encrypt_null_key() {
        let input = ScalarValue::Binary(Some(b"test".to_vec()));
        let key = ScalarValue::Binary(None);

        let args = vec![
            ColumnarValue::Scalar(input),
            ColumnarValue::Scalar(key),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ColumnarValue::Scalar(ScalarValue::Binary(None))));
    }

    #[test]
    fn test_aes_encrypt_vectorized() {
        let input_array = BinaryArray::from(vec![
            Some(b"message1".as_ref()),
            Some(b"message2".as_ref()),
            Some(b"message3".as_ref()),
        ]);
        let key_array = BinaryArray::from(vec![
            Some(b"key1key1key1key1".as_ref()),
            Some(b"key2key2key2key2".as_ref()),
            Some(b"key3key3key3key3".as_ref()),
        ]);

        let args = vec![
            ColumnarValue::Array(Arc::new(input_array)),
            ColumnarValue::Array(Arc::new(key_array)),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());

        if let ColumnarValue::Array(array) = result.unwrap() {
            assert_eq!(array.len(), 3);
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            for i in 0..3 {
                assert!(!binary_array.is_null(i));
                assert!(binary_array.value(i).len() > 0);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_aes_encrypt_vectorized_with_nulls() {
        let input_array = BinaryArray::from(vec![
            Some(b"message1".as_ref()),
            None,
            Some(b"message3".as_ref()),
        ]);
        let key_array = BinaryArray::from(vec![
            Some(b"key1key1key1key1".as_ref()),
            Some(b"key2key2key2key2".as_ref()),
            Some(b"key3key3key3key3".as_ref()),
        ]);

        let args = vec![
            ColumnarValue::Array(Arc::new(input_array)),
            ColumnarValue::Array(Arc::new(key_array)),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());

        if let ColumnarValue::Array(array) = result.unwrap() {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert!(!binary_array.is_null(0));
            assert!(binary_array.is_null(1));
            assert!(!binary_array.is_null(2));
        }
    }

    #[test]
    fn test_aes_encrypt_mixed_scalar_array() {
        let input_array = BinaryArray::from(vec![
            Some(b"message1".as_ref()),
            Some(b"message2".as_ref()),
        ]);
        let key = ScalarValue::Binary(Some(b"0000111122223333".to_vec()));

        let args = vec![
            ColumnarValue::Array(Arc::new(input_array)),
            ColumnarValue::Scalar(key),
        ];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_ok());

        if let ColumnarValue::Array(array) = result.unwrap() {
            assert_eq!(array.len(), 2);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_aes_encrypt_too_few_args() {
        let input = ScalarValue::Binary(Some(b"test".to_vec()));
        let args = vec![ColumnarValue::Scalar(input)];

        let result = spark_aes_encrypt(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_aes_encrypt_too_many_args() {
        let args: Vec<ColumnarValue> = (0..7)
            .map(|_| ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![0u8; 16]))))
            .collect();

        let result = spark_aes_encrypt(&args);
        assert!(result.is_err());
    }
}
