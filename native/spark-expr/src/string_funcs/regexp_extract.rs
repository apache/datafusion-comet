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

use arrow::array::{Array, ArrayRef, GenericStringArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, internal_datafusion_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;
use std::sync::Arc;
use std::any::Any;

/// Spark-compatible regexp_extract function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegExpExtract {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkRegExpExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegExpExtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
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
        // regexp_extract always returns String
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // regexp_extract(subject, pattern, idx)
        if args.args.len() != 3 {
            return exec_err!(
                "regexp_extract expects 3 arguments, got {}",
                args.args.len()
            );
        }

        let subject = &args.args[0];
        let pattern = &args.args[1];
        let idx = &args.args[2];

        // Pattern must be a literal string
        let pattern_str = match pattern {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return exec_err!("regexp_extract pattern must be a string literal");
            }
        };

        // idx must be a literal int
        let idx_val = match idx {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => *i as usize,
            _ => {
                return exec_err!("regexp_extract idx must be an integer literal");
            }
        };

        // Compile regex once
        let regex = Regex::new(&pattern_str).map_err(|e| {
            internal_datafusion_err!("Invalid regex pattern '{}': {}", pattern_str, e)
        })?;

        match subject {
            ColumnarValue::Array(array) => {
                let result = regexp_extract_array(array, &regex, idx_val)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                let result = match s {
                    Some(text) => Some(extract_group(text, &regex, idx_val)),
                    None => None,  // NULL input → NULL output
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => exec_err!("regexp_extract expects string input"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Spark-compatible regexp_extract_all function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegExpExtractAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkRegExpExtractAll {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegExpExtractAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
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
        // regexp_extract_all returns Array<String>
        Ok(DataType::List(Arc::new(
            arrow::datatypes::Field::new("item", DataType::Utf8, true),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // regexp_extract_all(subject, pattern) or regexp_extract_all(subject, pattern, idx)
        if args.args.len() < 2 || args.args.len() > 3 {
            return exec_err!(
                "regexp_extract_all expects 2 or 3 arguments, got {}",
                args.args.len()
            );
        }

        let subject = &args.args[0];
        let pattern = &args.args[1];
        let idx_val = if args.args.len() == 3 {
            match &args.args[2] {
                ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => *i as usize,
                _ => {
                    return exec_err!("regexp_extract_all idx must be an integer literal");
                }
            }
        } else {
            0 // default to group 0 (entire match)
        };

        // Pattern must be a literal string
        let pattern_str = match pattern {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return exec_err!("regexp_extract_all pattern must be a string literal");
            }
        };

        // Compile regex once
        let regex = Regex::new(&pattern_str).map_err(|e| {
            internal_datafusion_err!("Invalid regex pattern '{}': {}", pattern_str, e)
        })?;

        match subject {
            ColumnarValue::Array(array) => {
                let result = regexp_extract_all_array(array, &regex, idx_val)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                match s {
                    Some(text) => {
                        let matches = extract_all_groups(text, &regex, idx_val);
                        // Build a list array with a single element
                        let mut list_builder =
                            arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
                        for m in matches {
                            list_builder.values().append_value(m);
                        }
                        list_builder.append(true);
                        let list_array = list_builder.finish();
                        
                        Ok(ColumnarValue::Scalar(ScalarValue::List(
                            Arc::new(list_array),
                        )))
                    }
                    None => {
                        // Return NULL list using try_into (same as planner.rs:424)
                        let null_list: ScalarValue = DataType::List(Arc::new(
                            arrow::datatypes::Field::new("item", DataType::Utf8, true)
                        )).try_into()?;
                        Ok(ColumnarValue::Scalar(null_list))
                    }
                }
            }
            _ => exec_err!("regexp_extract_all expects string input"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

// Helper functions

fn extract_group(text: &str, regex: &Regex, idx: usize) -> String {
    regex
        .captures(text)
        .and_then(|caps| caps.get(idx))
        .map(|m| m.as_str().to_string())
        // Spark behavior: return empty string "" if no match or group not found
        .unwrap_or_else(|| String::new())
}

fn regexp_extract_array(array: &ArrayRef, regex: &Regex, idx: usize) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .ok_or_else(|| {
            internal_datafusion_err!("regexp_extract expects string array input")
        })?;

    let result: GenericStringArray<i32> = string_array
        .iter()
        .map(|s| s.map(|text| extract_group(text, regex, idx)))  // NULL → None, non-NULL → Some("")
        .collect();

    Ok(Arc::new(result))
}

fn extract_all_groups(text: &str, regex: &Regex, idx: usize) -> Vec<String> {
    regex
        .captures_iter(text)
        .filter_map(|caps| caps.get(idx).map(|m| m.as_str().to_string()))
        .collect()
}

fn regexp_extract_all_array(array: &ArrayRef, regex: &Regex, idx: usize) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .ok_or_else(|| {
            internal_datafusion_err!("regexp_extract_all expects string array input")
        })?;

    let mut list_builder =
        arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());

    for s in string_array.iter() {
        match s {
            Some(text) => {
                let matches = extract_all_groups(text, regex, idx);
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

    Ok(Arc::new(list_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_regexp_extract_basic() {
        let regex = Regex::new(r"(\d+)-(\w+)").unwrap();

        // Spark behavior: return "" on no match, not None
        assert_eq!(extract_group("123-abc", &regex, 0), "123-abc");
        assert_eq!(extract_group("123-abc", &regex, 1), "123");
        assert_eq!(extract_group("123-abc", &regex, 2), "abc");
        assert_eq!(extract_group("123-abc", &regex, 3), "");  // no such group → ""
        assert_eq!(extract_group("no match", &regex, 0), "");  // no match → ""
    }

    #[test]
    fn test_regexp_extract_all_basic() {
        let regex = Regex::new(r"(\d+)").unwrap();

        // Multiple matches
        let matches = extract_all_groups("a1b2c3", &regex, 0);
        assert_eq!(matches, vec!["1", "2", "3"]);

        // Same with group index 1
        let matches = extract_all_groups("a1b2c3", &regex, 1);
        assert_eq!(matches, vec!["1", "2", "3"]);

        // No match
        let matches = extract_all_groups("no digits", &regex, 0);
        assert!(matches.is_empty());
        assert_eq!(matches, Vec::<String>::new());
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

        let result = regexp_extract_all_array(&array, &regex, 0)?;
        let list_array = as_list_array(&result)?;

        // Row 0: "a1b2" → ["1", "2"]
        let row0 = list_array.value(0);
        let row0_str = row0.as_any().downcast_ref::<GenericStringArray<i32>>().unwrap();
        assert_eq!(row0_str.len(), 2);
        assert_eq!(row0_str.value(0), "1");
        assert_eq!(row0_str.value(1), "2");

        // Row 1: "no digits" → [] (empty array, not NULL)
        let row1 = list_array.value(1);
        let row1_str = row1.as_any().downcast_ref::<GenericStringArray<i32>>().unwrap();
        assert_eq!(row1_str.len(), 0);  // Empty array
        assert!(!list_array.is_null(1));  // Not NULL, just empty

        // Row 2: NULL input → NULL output
        assert!(list_array.is_null(2));

        // Row 3: "c3d4e5" → ["3", "4", "5"]
        let row3 = list_array.value(3);
        let row3_str = row3.as_any().downcast_ref::<GenericStringArray<i32>>().unwrap();
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

        let result = regexp_extract_array(&array, &regex, 1)?;
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "123");
        assert_eq!(result_array.value(1), "456");
        assert!(result_array.is_null(2));  // NULL input → NULL output
        assert_eq!(result_array.value(3), "");  // no match → "" (empty string)

        Ok(())
    }
}

