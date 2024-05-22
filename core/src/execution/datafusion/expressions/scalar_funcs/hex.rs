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

use arrow::{
    array::{as_dictionary_array, as_string_array},
    datatypes::Int32Type,
};
use arrow_array::StringArray;
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{
    cast::{as_binary_array, as_fixed_size_binary_array, as_int64_array},
    exec_err, DataFusionError, ScalarValue,
};
use std::fmt::Write;

fn hex_int64(num: i64) -> String {
    if num >= 0 {
        format!("{:X}", num)
    } else {
        format!("{:016X}", num as u64)
    }
}

fn hex_bytes(bytes: &[u8]) -> Vec<u8> {
    let length = bytes.len();
    let mut value = vec![0; length * 2];
    let mut i = 0;
    while i < length {
        value[i * 2] = (bytes[i] & 0xF0) >> 4;
        value[i * 2 + 1] = bytes[i] & 0x0F;
        i += 1;
    }
    value
}

fn hex_string(s: &str) -> Vec<u8> {
    hex_bytes(s.as_bytes())
}

fn hex_bytes_to_string(bytes: &[u8]) -> Result<String, std::fmt::Error> {
    let mut hex_string = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut hex_string, "{:X}", byte)?;
    }
    Ok(hex_string)
}

pub(super) fn spark_hex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "hex expects exactly one argument".to_string(),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 => {
                let array = as_int64_array(array)?;

                let hexed: Vec<Option<String>> = array.iter().map(|v| v.map(hex_int64)).collect();

                let string_array = StringArray::from(hexed);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let array = as_string_array(array);

                let hexed: Vec<Option<String>> = array
                    .iter()
                    .map(|v| v.map(|v| hex_bytes_to_string(&hex_string(v))).transpose())
                    .collect::<Result<_, _>>()?;

                let string_array = StringArray::from(hexed);

                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Binary => {
                let array = as_binary_array(array)?;

                let hexed: Vec<Option<String>> = array
                    .iter()
                    .map(|v| v.map(|v| hex_bytes_to_string(&hex_bytes(v))).transpose())
                    .collect::<Result<_, _>>()?;

                let string_array = StringArray::from(hexed);

                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;

                let hexed: Vec<Option<String>> = array
                    .iter()
                    .map(|v| v.map(|v| hex_bytes_to_string(&hex_bytes(v))).transpose())
                    .collect::<Result<_, _>>()?;

                let string_array = StringArray::from(hexed);

                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Dictionary(_, value_type) if matches!(**value_type, DataType::Int64) => {
                let dict = as_dictionary_array::<Int32Type>(&array);

                let hexed_values = as_int64_array(dict.values())?;
                let values = hexed_values
                    .iter()
                    .map(|v| v.map(hex_int64))
                    .collect::<Vec<_>>();

                let keys = dict.keys().clone();
                let mut new_keys = Vec::with_capacity(values.len());

                for key in keys.iter() {
                    let key = key.map(|k| values[k as usize].clone()).unwrap_or(None);
                    new_keys.push(key);
                }

                let string_array_values = StringArray::from(new_keys);
                Ok(ColumnarValue::Array(Arc::new(string_array_values)))
            }
            DataType::Dictionary(_, value_type) if matches!(**value_type, DataType::Utf8) => {
                let dict = as_dictionary_array::<Int32Type>(&array);

                let hexed_values = as_string_array(dict.values());
                let values: Vec<Option<String>> = hexed_values
                    .iter()
                    .map(|v| v.map(|v| hex_bytes_to_string(&hex_string(v))).transpose())
                    .collect::<Result<_, _>>()?;

                let keys = dict.keys().clone();

                let mut new_keys = Vec::with_capacity(values.len());

                for key in keys.iter() {
                    let key = key.map(|k| values[k as usize].clone()).unwrap_or(None);
                    new_keys.push(key);
                }

                let string_array_values = StringArray::from(new_keys);
                Ok(ColumnarValue::Array(Arc::new(string_array_values)))
            }
            DataType::Dictionary(_, value_type) if matches!(**value_type, DataType::Binary) => {
                let dict = as_dictionary_array::<Int32Type>(&array);

                let hexed_values = as_binary_array(dict.values())?;
                let values: Vec<Option<String>> = hexed_values
                    .iter()
                    .map(|v| v.map(|v| hex_bytes_to_string(&hex_bytes(v))).transpose())
                    .collect::<Result<_, _>>()?;

                let keys = dict.keys().clone();
                let mut new_keys = Vec::with_capacity(values.len());

                for key in keys.iter() {
                    let key = key.map(|k| values[k as usize].clone()).unwrap_or(None);
                    new_keys.push(key);
                }

                let string_array_values = StringArray::from(new_keys);
                Ok(ColumnarValue::Array(Arc::new(string_array_values)))
            }
            _ => exec_err!(
                "hex got an unexpected argument type: {:?}",
                array.data_type()
            ),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Int64(Some(v)) => {
                let hex_string = hex_int64(*v);

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(hex_string))))
            }
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::FixedSizeBinary(_, Some(v)) => {
                let hex_bytes = hex_bytes(v);
                let hex_string = hex_bytes_to_string(&hex_bytes)?;

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(hex_string))))
            }
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                let hex_bytes = hex_string(v);
                let hex_string = hex_bytes_to_string(&hex_bytes)?;

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(hex_string))))
            }
            ScalarValue::Int64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::Binary(None)
            | ScalarValue::FixedSizeBinary(_, None) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => exec_err!(
                "hex got an unexpected argument type: {:?}",
                scalar.data_type()
            ),
        },
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{
            as_string_array, BinaryDictionaryBuilder, PrimitiveDictionaryBuilder, StringBuilder,
            StringDictionaryBuilder,
        },
        datatypes::{Int32Type, Int64Type},
    };
    use arrow_array::{Int64Array, StringArray};
    use datafusion::logical_expr::ColumnarValue;

    #[test]
    fn test_dictionary_hex_utf8() {
        let mut input_builder = StringDictionaryBuilder::<Int32Type>::new();
        input_builder.append_value("hi");
        input_builder.append_value("bye");
        input_builder.append_null();
        input_builder.append_value("rust");
        let input = input_builder.finish();

        let mut string_builder = StringBuilder::new();
        string_builder.append_value("6869");
        string_builder.append_value("627965");
        string_builder.append_null();
        string_builder.append_value("72757374");
        let expected = string_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_string_array(&result);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_dictionary_hex_int64() {
        let mut input_builder = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();
        input_builder.append_value(1);
        input_builder.append_value(2);
        input_builder.append_null();
        input_builder.append_value(3);
        let input = input_builder.finish();

        let mut string_builder = StringBuilder::new();
        string_builder.append_value("1");
        string_builder.append_value("2");
        string_builder.append_null();
        string_builder.append_value("3");
        let expected = string_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_string_array(&result);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_dictionary_hex_binary() {
        let mut input_builder = BinaryDictionaryBuilder::<Int32Type>::new();
        input_builder.append_value("1");
        input_builder.append_value("1");
        input_builder.append_null();
        input_builder.append_value("3");
        let input = input_builder.finish();

        let mut expected_builder = StringBuilder::new();
        expected_builder.append_value("31");
        expected_builder.append_value("31");
        expected_builder.append_null();
        expected_builder.append_value("33");
        let expected = expected_builder.finish();

        let columnar_value = ColumnarValue::Array(Arc::new(input));
        let result = super::spark_hex(&[columnar_value]).unwrap();

        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let result = as_string_array(&result);

        assert_eq!(result, &expected);
    }

    #[test]
    fn test_hex_bytes() {
        let bytes = [0x01, 0x02, 0x03, 0x04];
        let hexed = super::hex_bytes(&bytes);
        assert_eq!(hexed, vec![0, 1, 0, 2, 0, 3, 0, 4]);
    }

    #[test]
    fn test_hex_bytes_to_string() -> Result<(), std::fmt::Error> {
        let bytes = [0x01, 0x02, 0x03, 0x04];
        let hexed = super::hex_bytes_to_string(&bytes)?;
        assert_eq!(hexed, "1234".to_string());

        let large_bytes = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];
        let hexed = super::hex_bytes_to_string(&large_bytes)?;
        assert_eq!(hexed, "123456789ABCDEF0".to_string());

        Ok(())
    }

    #[test]
    fn test_hex_int64() {
        let num = 1234;
        let hexed = super::hex_int64(num);
        assert_eq!(hexed, "4D2".to_string());

        let num = -1;
        let hexed = super::hex_int64(num);
        assert_eq!(hexed, "FFFFFFFFFFFFFFFF".to_string());
    }

    #[test]
    fn test_spark_hex_int64() {
        let int_array = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let columnar_value = ColumnarValue::Array(Arc::new(int_array));

        let result = super::spark_hex(&[columnar_value]).unwrap();
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        let string_array = as_string_array(&result);
        let expected_array = StringArray::from(vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
        ]);

        assert_eq!(string_array, &expected_array);
    }
}
