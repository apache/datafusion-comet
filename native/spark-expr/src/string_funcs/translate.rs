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

use arrow::array::builder::GenericStringBuilder;
use arrow::array::cast::as_dictionary_array;
use arrow::array::types::Int32Type;
use arrow::array::{make_array, Array, DictionaryArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_generic_string_array, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::collections::HashMap;
use std::sync::Arc;

pub fn spark_translate(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Utf8(Some(from))), ColumnarValue::Scalar(ScalarValue::Utf8(Some(to)))] =>
        {
            let translation_map = build_translation_map(from, to);

            match array.data_type() {
                DataType::Utf8 => translate_array_internal::<i32>(array, &translation_map),
                DataType::LargeUtf8 => translate_array_internal::<i64>(array, &translation_map),
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        translate_array_internal::<i32>(dict.values(), &translation_map)?
                    } else {
                        translate_array_internal::<i64>(dict.values(), &translation_map)?
                    };
                    let values = col.to_array(0)?;
                    let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
                    Ok(ColumnarValue::Array(make_array(result.into())))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function translate",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function translate",
        ))),
    }
}

#[derive(Clone, Copy)]
enum TranslateAction {
    Replace(char),
    Delete,
}

fn build_translation_map(from: &str, to: &str) -> HashMap<char, TranslateAction> {
    let from_chars: Vec<char> = from.chars().collect();
    let to_chars: Vec<char> = to.chars().collect();

    let mut map = HashMap::with_capacity(from_chars.len());

    for (i, from_char) in from_chars.into_iter().enumerate() {
        // Only insert the first occurrence of each character to match Spark behaviour
        if !map.contains_key(&from_char) {
            if i < to_chars.len() {
                map.insert(from_char, TranslateAction::Replace(to_chars[i]));
            } else {
                map.insert(from_char, TranslateAction::Delete);
            }
        }
    }

    map
}

fn translate_array_internal<T: OffsetSizeTrait>(
    array: &Arc<dyn Array>,
    translation_map: &HashMap<char, TranslateAction>,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;

    let estimated_capacity = string_array.len();
    let mut builder = GenericStringBuilder::<T>::with_capacity(
        estimated_capacity,
        string_array.value_data().len(),
    );

    let mut buffer = String::new();

    for string in string_array.iter() {
        match string {
            Some(s) => {
                buffer.clear();
                translate_string(&mut buffer, s, translation_map);
                builder.append_value(&buffer);
            }
            None => builder.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

#[inline]
fn translate_string(
    buffer: &mut String,
    input: &str,
    translation_map: &HashMap<char, TranslateAction>,
) {
    buffer.reserve(input.len());

    for ch in input.chars() {
        match translation_map.get(&ch) {
            Some(TranslateAction::Replace(replacement)) => buffer.push(*replacement),
            Some(TranslateAction::Delete) => {}
            None => buffer.push(ch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_translate_basic() {
        let input = Arc::new(StringArray::from(vec![
            Some("Spark SQL"),
            Some("hello"),
            None,
            Some(""),
        ])) as Arc<dyn Array>;

        let result = spark_translate(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("SL".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("12".to_string()))),
        ])
        .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(result_array.value(0), "1park 1Q2");
            assert_eq!(result_array.value(1), "hello");
            assert!(result_array.is_null(2));
            assert_eq!(result_array.value(3), "");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_translate_with_delete() {
        // When `from` is longer than `to`, extra characters in `from` should be deleted
        let input = Arc::new(StringArray::from(vec![Some("abcdef")])) as Arc<dyn Array>;

        let result = spark_translate(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abcd".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("XY".to_string()))),
        ])
        .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
            // 'a' -> 'X', 'b' -> 'Y', 'c' -> deleted, 'd' -> deleted
            assert_eq!(result_array.value(0), "XYef");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_translate_unicode() {
        let input = Arc::new(StringArray::from(vec![Some("苹果手机")])) as Arc<dyn Array>;

        let result = spark_translate(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("苹".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("1".to_string()))),
        ])
        .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(result_array.value(0), "1果手机");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_translate_duplicate_from_chars() {
        // Only the first occurrence of each character in `from` should be used
        let input = Arc::new(StringArray::from(vec![Some("aaa")])) as Arc<dyn Array>;

        let result = spark_translate(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("aaa".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("xyz".to_string()))),
        ])
        .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
            // All 'a' should map to 'x' (first mapping wins)
            assert_eq!(result_array.value(0), "xxx");
        } else {
            panic!("Expected array result");
        }
    }
}
