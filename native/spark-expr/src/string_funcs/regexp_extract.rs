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

use arrow::array::{
    Array, ArrayRef, GenericStringArray, GenericStringBuilder, ListArray, OffsetSizeTrait,
};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::logical_expr_common::signature::TypeSignature::Exact;
use regex::Regex;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible regexp_extract function
///
/// Extracts a substring matching a [regular expression](https://docs.rs/regex/latest/regex/#syntax)
/// and returns the specified capture group.
///
/// The function signature is: `regexp_extract(str, regexp, idx)`
/// where:
/// - `str`: The input string to search in
/// - `regexp`: The regular expression pattern (must be a literal)
/// - `idx`: The capture group index (0 for the entire match, must be a literal)
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegExpExtract {
    signature: Signature,
}

impl Default for SparkRegExpExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegExpExtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int32]),
                    Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRegExpExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &_arg_types[0] {
            DataType::Utf8 => DataType::Utf8,
            DataType::LargeUtf8 => DataType::LargeUtf8,
            _ => {
                return exec_err!(
                    "regexp_extract expects utf8 or largeutf8 input but got {:?}",
                    _arg_types[0]
                )
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });
        let is_scalar = len.is_none();
        let result = match args[0].data_type() {
            DataType::Utf8 => regexp_extract_func::<i32>(args),
            DataType::LargeUtf8 => regexp_extract_func::<i64>(args),
            _ => {
                return exec_err!(
                    "regexp_extract expects the data type of subject to be utf8 or largeutf8 but got {:?}",
                    args[0].data_type()
                );
            }
        };
        if is_scalar {
            result
                .and_then(|arr| ScalarValue::try_from_array(&arr, 0))
                .map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

/// Spark-compatible regexp_extract_all function
///
/// Extracts all substrings matching a [regular expression](https://docs.rs/regex/latest/regex/#syntax)
/// and returns them as an array.
///
/// The function signature is: `regexp_extract_all(str, regexp, idx)`
/// where:
/// - `str`: The input string to search in
/// - `regexp`: The regular expression pattern (must be a literal)
/// - `idx`: The capture group index (0 for the entire match, must be a literal)
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegExpExtractAll {
    signature: Signature,
}

impl Default for SparkRegExpExtractAll {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegExpExtractAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int32]),
                    Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRegExpExtractAll {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &_arg_types[0] {
            DataType::Utf8 => DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Utf8,
                false,
            ))),
            DataType::LargeUtf8 => DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::LargeUtf8,
                false,
            ))),
            _ => {
                return exec_err!(
                    "regexp_extract_all expects utf8 or largeutf8 input but got {:?}",
                    _arg_types[0]
                )
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });
        let is_scalar = len.is_none();
        let result = match args[0].data_type() {
            DataType::Utf8 => regexp_extract_all_func::<i32>(args),
            DataType::LargeUtf8 => regexp_extract_all_func::<i64>(args),
            _ => {
                return exec_err!(
                    "regexp_extract_all expects the data type of subject to be utf8 or largeutf8 but got {:?}",
                    args[0].data_type()
                );
            }
        };
        if is_scalar {
            result
                .and_then(|arr| ScalarValue::try_from_array(&arr, 0))
                .map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

// Helper functions

fn regexp_extract_func<T: OffsetSizeTrait>(args: &[ColumnarValue]) -> Result<ArrayRef> {
    let (subject, regex, idx) = parse_args(args, "regexp_extract")?;

    let subject_array = match subject {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array()?,
    };

    regexp_extract_array::<T>(&subject_array, &regex, idx)
}

fn regexp_extract_all_func<T: OffsetSizeTrait>(args: &[ColumnarValue]) -> Result<ArrayRef> {
    let (subject, regex, idx) = parse_args(args, "regexp_extract_all")?;

    let subject_array = match subject {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array()?,
    };

    regexp_extract_all_array::<T>(&subject_array, &regex, idx)
}

fn parse_args<'a>(
    args: &'a [ColumnarValue],
    fn_name: &str,
) -> Result<(&'a ColumnarValue, Regex, i32)> {
    if args.len() != 3 {
        return exec_err!("{} expects 3 arguments, got {}", fn_name, args.len());
    }

    let subject = &args[0];
    let pattern = &args[1];
    let idx = &args[2];

    let pattern_str = match pattern {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
        _ => {
            return exec_err!("{} pattern must be a string literal", fn_name);
        }
    };

    let idx_val = match idx {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => *i,
        _ => {
            return exec_err!("{} idx must be an integer literal", fn_name);
        }
    };
    if idx_val < 0 {
        return exec_err!("{fn_name} group index must be non-negative");
    }

    let regex = Regex::new(&pattern_str).map_err(|e| {
        DataFusionError::Execution(format!("Invalid regex pattern '{}': {}", pattern_str, e))
    })?;

    Ok((subject, regex, idx_val))
}

fn regexp_extract_array<T: OffsetSizeTrait>(
    array: &ArrayRef,
    regex: &Regex,
    idx: i32,
) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Execution("regexp_extract expects string array input".to_string())
        })?;

    let mut builder = GenericStringBuilder::<T>::new();
    for s in string_array.iter() {
        match s {
            Some(text) => {
                let extracted = regexp_extract(text, regex, idx)?;
                builder.append_value(extracted);
            }
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn regexp_extract(text: &str, regex: &Regex, idx: i32) -> Result<String> {
    let idx = idx as usize;
    match regex.captures(text) {
        Some(caps) => {
            // Spark behavior: throw error if group index is out of bounds
            let group_cnt = caps.len() - 1;
            if idx > group_cnt {
                return exec_err!(
                    "Regex group index out of bounds, group count: {}, index: {}",
                    group_cnt,
                    idx
                );
            }
            Ok(caps
                .get(idx)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default())
        }
        None => {
            // No match: return empty string (Spark behavior)
            Ok(String::new())
        }
    }
}

fn regexp_extract_all_array<T: OffsetSizeTrait>(
    array: &ArrayRef,
    regex: &Regex,
    idx: i32,
) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Execution("regexp_extract_all expects string array input".to_string())
        })?;

    let item_data_type = match array.data_type() {
        DataType::Utf8 => DataType::Utf8,
        DataType::LargeUtf8 => DataType::LargeUtf8,
        _ => {
            return exec_err!(
                "regexp_extract_all expects utf8 or largeutf8 array but got {:?}",
                array.data_type()
            );
        }
    };
    let item_field = Arc::new(arrow::datatypes::Field::new("item", item_data_type, false));

    let string_builder = GenericStringBuilder::<T>::new();
    let mut list_builder =
        arrow::array::ListBuilder::new(string_builder).with_field(Arc::clone(&item_field));

    for s in string_array.iter() {
        match s {
            Some(text) => {
                let matches = regexp_extract_all(text, regex, idx)?;
                for m in matches {
                    list_builder.values().append_value(m);
                }
                list_builder.append(true);
            }
            None => {
                list_builder.append(false);
            }
        }
    }

    let list_array = list_builder.finish();

    // Manually create a new ListArray with the correct field schema to ensure nullable is false
    // This ensures the schema matches what we declared in return_type
    Ok(Arc::new(ListArray::new(
        FieldRef::from(Arc::clone(&item_field)),
        list_array.offsets().clone(),
        Arc::clone(list_array.values()),
        list_array.nulls().cloned(),
    )))
}

fn regexp_extract_all(text: &str, regex: &Regex, idx: i32) -> Result<Vec<String>> {
    let idx = idx as usize;
    let mut results = Vec::new();

    for caps in regex.captures_iter(text) {
        // Check bounds for each capture (matches Spark behavior)
        let group_num = caps.len() - 1;
        if idx > group_num {
            return exec_err!(
                "Regex group index out of bounds, group count: {}, index: {}",
                group_num,
                idx
            );
        }

        let matched = caps
            .get(idx)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        results.push(matched);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray};

    #[test]
    fn test_regexp_extract_basic() {
        let regex = Regex::new(r"(\d+)-(\w+)").unwrap();

        // Spark behavior: return "" on no match, not None
        assert_eq!(regexp_extract("123-abc", &regex, 0).unwrap(), "123-abc");
        assert_eq!(regexp_extract("123-abc", &regex, 1).unwrap(), "123");
        assert_eq!(regexp_extract("123-abc", &regex, 2).unwrap(), "abc");
        assert_eq!(regexp_extract("no match", &regex, 0).unwrap(), ""); // no match → ""

        // Spark behavior: group index out of bounds → error
        assert!(regexp_extract("123-abc", &regex, 3).is_err());
        assert!(regexp_extract("123-abc", &regex, 99).is_err());
        assert!(regexp_extract("123-abc", &regex, -1).is_err());
    }

    #[test]
    fn test_regexp_extract_all_basic() {
        let regex = Regex::new(r"(\d+)").unwrap();

        // Multiple matches
        let matches = regexp_extract_all("a1b2c3", &regex, 0).unwrap();
        assert_eq!(matches, vec!["1", "2", "3"]);

        // Same with group index 1
        let matches = regexp_extract_all("a1b2c3", &regex, 1).unwrap();
        assert_eq!(matches, vec!["1", "2", "3"]);

        // No match: returns empty vec, not error
        let matches = regexp_extract_all("no digits", &regex, 0).unwrap();
        assert!(matches.is_empty());
        assert_eq!(matches, Vec::<String>::new());

        // Group index out of bounds → error
        assert!(regexp_extract_all("a1b2c3", &regex, 2).is_err());
    }

    #[test]
    fn test_regexp_extract_all_array() -> Result<()> {
        use datafusion::common::cast::as_list_array;

        let regex = Regex::new(r"(\d+)").unwrap();
        let array = Arc::new(StringArray::from(vec![
            Some("a1b2"),
            Some("no digits"),
            None,
            Some("c3d4e5"),
        ])) as ArrayRef;

        let result = regexp_extract_all_array::<i32>(&array, &regex, 0)?;
        let list_array = as_list_array(&result)?;

        // Row 0: "a1b2" → ["1", "2"]
        let row0 = list_array.value(0);
        let row0_str = row0
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(row0_str.len(), 2);
        assert_eq!(row0_str.value(0), "1");
        assert_eq!(row0_str.value(1), "2");

        // Row 1: "no digits" → [] (empty array, not NULL)
        let row1 = list_array.value(1);
        let row1_str = row1
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(row1_str.len(), 0); // Empty array
        assert!(!list_array.is_null(1)); // Not NULL, just empty

        // Row 2: NULL input → NULL output
        assert!(list_array.is_null(2));

        // Row 3: "c3d4e5" → ["3", "4", "5"]
        let row3 = list_array.value(3);
        let row3_str = row3
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(row3_str.len(), 3);
        assert_eq!(row3_str.value(0), "3");
        assert_eq!(row3_str.value(1), "4");
        assert_eq!(row3_str.value(2), "5");

        Ok(())
    }

    #[test]
    fn test_regexp_extract_array() -> Result<()> {
        let regex = Regex::new(r"(\d+)-(\w+)").unwrap();
        let array = Arc::new(StringArray::from(vec![
            Some("123-abc"),
            Some("456-def"),
            None,
            Some("no-match"),
        ])) as ArrayRef;

        let result = regexp_extract_array::<i32>(&array, &regex, 1)?;
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "123");
        assert_eq!(result_array.value(1), "456");
        assert!(result_array.is_null(2)); // NULL input → NULL output
        assert_eq!(result_array.value(3), ""); // no match → "" (empty string)

        Ok(())
    }

    #[test]
    fn test_regexp_extract_largeutf8() -> Result<()> {
        let regex = Regex::new(r"(\d+)").unwrap();
        let array = Arc::new(LargeStringArray::from(vec![
            Some("a1b2c3"),
            Some("x5y6"),
            None,
            Some("no digits"),
        ])) as ArrayRef;

        let result = regexp_extract_array::<i64>(&array, &regex, 1)?;
        let result_array = result.as_any().downcast_ref::<LargeStringArray>().unwrap();

        assert_eq!(result_array.value(0), "1"); // First digit from "a1b2c3"
        assert_eq!(result_array.value(1), "5"); // First digit from "x5y6"
        assert!(result_array.is_null(2)); // NULL input → NULL output
        assert_eq!(result_array.value(3), ""); // no match → "" (empty string)

        Ok(())
    }

    #[test]
    fn test_regexp_extract_all_largeutf8() -> Result<()> {
        use datafusion::common::cast::as_list_array;

        let regex = Regex::new(r"(\d+)").unwrap();
        let array = Arc::new(LargeStringArray::from(vec![
            Some("a1b2c3"),
            Some("x5y6"),
            None,
            Some("no digits"),
        ])) as ArrayRef;

        let result = regexp_extract_all_array::<i64>(&array, &regex, 0)?;
        let list_array = as_list_array(&result)?;

        // Row 0: "a1b2c3" → ["1", "2", "3"] (all matches)
        let row0 = list_array.value(0);
        let row0_str = row0
            .as_any()
            .downcast_ref::<GenericStringArray<i64>>()
            .unwrap();
        assert_eq!(row0_str.len(), 3);
        assert_eq!(row0_str.value(0), "1");
        assert_eq!(row0_str.value(1), "2");
        assert_eq!(row0_str.value(2), "3");

        // Row 1: "x5y6" → ["5", "6"] (all matches)
        let row1 = list_array.value(1);
        let row1_str = row1
            .as_any()
            .downcast_ref::<GenericStringArray<i64>>()
            .unwrap();
        assert_eq!(row1_str.len(), 2);
        assert_eq!(row1_str.value(0), "5");
        assert_eq!(row1_str.value(1), "6");

        // Row 2: NULL input → NULL output
        assert!(list_array.is_null(2));

        // Row 3: "no digits" → [] (empty array, not NULL)
        let row3 = list_array.value(3);
        let row3_str = row3
            .as_any()
            .downcast_ref::<GenericStringArray<i64>>()
            .unwrap();
        assert_eq!(row3_str.len(), 0); // Empty array
        assert!(!list_array.is_null(3)); // Not NULL, just empty

        Ok(())
    }
}
