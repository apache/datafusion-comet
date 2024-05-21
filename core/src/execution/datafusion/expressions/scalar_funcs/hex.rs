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

use arrow::array::as_string_array;
use arrow_array::{Int16Array, Int8Array, StringArray};
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{
    cast::{as_binary_array, as_fixed_size_binary_array, as_int32_array, as_int64_array},
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

fn hex_int32(num: i32) -> String {
    if num >= 0 {
        format!("{:X}", num)
    } else {
        format!("{:08X}", num as u32)
    }
}

fn hex_int16(num: i16) -> String {
    if num >= 0 {
        format!("{:X}", num)
    } else {
        format!("{:04X}", num as u16)
    }
}

fn hex_int8(num: i8) -> String {
    if num >= 0 {
        format!("{:X}", num)
    } else {
        format!("{:02X}", num as u8)
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
            DataType::Int32 => {
                let array = as_int32_array(array)?;

                let hexed: Vec<Option<String>> = array.iter().map(|v| v.map(hex_int32)).collect();

                let string_array = StringArray::from(hexed);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();

                let hexed: Vec<Option<String>> = array.iter().map(|v| v.map(hex_int16)).collect();

                let string_array = StringArray::from(hexed);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();

                let hexed: Vec<Option<String>> = array.iter().map(|v| v.map(hex_int8)).collect();

                let string_array = StringArray::from(hexed);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::UInt64 => {
                let array = as_int64_array(array)?;

                let hexed: Vec<Option<String>> = array.iter().map(|v| v.map(hex_int64)).collect();

                let string_array = StringArray::from(hexed);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::UInt8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();

                let hexed: Vec<Option<String>> = array.iter().map(|v| v.map(hex_int8)).collect();

                let string_array = StringArray::from(hexed);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Utf8 => {
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
            _ => exec_err!(
                "hex expects a string, binary or integer argument, got {:?}",
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
                "hex expects a string, binary or integer argument, got {:?}",
                scalar.data_type()
            ),
        },
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::as_string_array;
    use arrow_array::{Int64Array, StringArray};
    use datafusion::logical_expr::ColumnarValue;

    #[test]
    fn test_hex_bytes() {
        let bytes = [0x01, 0x02, 0x03, 0x04];
        let hexed = super::hex_bytes(&bytes);
        assert_eq!(hexed, vec![0, 1, 0, 2, 0, 3, 0, 4]);
    }

    #[test]
    fn test_hex_string() {
        let s = "1234";
        let hexed = super::hex_string(s);
        assert_eq!(hexed, vec![0x31, 0x32, 0x33, 0x34]);

        let hexed_string = super::hex_bytes_to_string(&hexed).unwrap();
        assert_eq!(hexed_string, "31323334".to_string());
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
    fn test_hex_i8() {
        let num = 123;
        let hexed = super::hex_int8(num);
        assert_eq!(hexed, "7B".to_string());

        let num = -1;
        let hexed = super::hex_int8(num);
        assert_eq!(hexed, "FF".to_string());
    }

    #[test]
    fn test_hex_i16() {
        let num = 1234;
        let hexed = super::hex_int16(num);
        assert_eq!(hexed, "4D2".to_string());

        let num = -1;
        let hexed = super::hex_int16(num);
        assert_eq!(hexed, "FFFF".to_string());
    }

    #[test]
    fn test_hex_i32() {
        let num = 1234;
        let hexed = super::hex_int32(num);
        assert_eq!(hexed, "4D2".to_string());

        let num = -1;
        let hexed = super::hex_int32(num);
        assert_eq!(hexed, "FFFFFFFF".to_string());
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
