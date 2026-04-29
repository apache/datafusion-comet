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

use arrow::array::{Array, ArrayRef, GenericStringArray, GenericStringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::as_generic_string_array, exec_err, DataFusionError, Result as DataFusionResult,
    ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;
use std::sync::Arc;

/// Spark-compatible `regexp_extract(subject, pattern, idx)`.
///
/// Returns the substring of `subject` matched by group `idx` of the first match of `pattern`.
/// `idx = 0` returns the entire match. Returns an empty string when there is no match or the
/// matched group is unset (optional group). Returns null when any input is null. Errors when
/// `idx` is out of range for the pattern's group count.
///
/// Note: this uses the Rust `regex` crate, whose syntax differs from Java's regex engine in
/// some ways. The expression is therefore reported as Incompatible.
pub fn spark_regexp_extract(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "regexp_extract expects 2 or 3 arguments (subject, pattern, [idx]), got {}",
            args.len()
        );
    }

    let idx: i32 = if args.len() == 3 {
        match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => *i,
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {
                return Ok(null_result(subject_len(&args[0])));
            }
            _ => {
                return exec_err!("regexp_extract idx must be an Int32 scalar");
            }
        }
    } else {
        1
    };

    if idx < 0 {
        return exec_err!("regexp_extract idx must be non-negative, got {}", idx);
    }

    let pattern = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(p)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(p))) => p.clone(),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            return Ok(null_result(subject_len(&args[0])));
        }
        _ => {
            return exec_err!("regexp_extract pattern must be a scalar string");
        }
    };

    let regex = Regex::new(&pattern).map_err(|e| {
        DataFusionError::Execution(format!("Invalid regex pattern '{pattern}': {e}"))
    })?;

    let group_count = regex.captures_len() as i32 - 1;
    if idx > group_count {
        return Err(DataFusionError::Execution(format!(
            "Regex group count is {group_count}, but the specified group index is {idx}"
        )));
    }
    let group_idx = idx as usize;

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let strings = as_generic_string_array::<i32>(array.as_ref())?;
                Ok(ColumnarValue::Array(extract_array::<i32>(
                    strings, &regex, group_idx,
                )))
            }
            DataType::LargeUtf8 => {
                let strings = as_generic_string_array::<i64>(array.as_ref())?;
                Ok(ColumnarValue::Array(extract_array::<i64>(
                    strings, &regex, group_idx,
                )))
            }
            other => exec_err!(
                "regexp_extract expects Utf8 or LargeUtf8 subject, got {:?}",
                other
            ),
        },
        ColumnarValue::Scalar(ScalarValue::Utf8(s))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(s)) => match s {
            None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            Some(s) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                extract_one(s, &regex, group_idx),
            )))),
        },
        _ => exec_err!("regexp_extract subject must be a string"),
    }
}

fn extract_array<O: arrow::array::OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    regex: &Regex,
    group_idx: usize,
) -> ArrayRef {
    let mut builder = GenericStringBuilder::<i32>::with_capacity(array.len(), array.value_data().len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(extract_one(array.value(i), regex, group_idx));
        }
    }
    Arc::new(builder.finish())
}

fn extract_one(input: &str, regex: &Regex, group_idx: usize) -> String {
    match regex.captures(input) {
        Some(caps) => caps
            .get(group_idx)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default(),
        None => String::new(),
    }
}

fn subject_len(value: &ColumnarValue) -> Option<usize> {
    match value {
        ColumnarValue::Array(a) => Some(a.len()),
        ColumnarValue::Scalar(_) => None,
    }
}

fn null_result(len: Option<usize>) -> ColumnarValue {
    match len {
        Some(n) => {
            let mut builder = GenericStringBuilder::<i32>::with_capacity(n, 0);
            for _ in 0..n {
                builder.append_null();
            }
            ColumnarValue::Array(Arc::new(builder.finish()))
        }
        None => ColumnarValue::Scalar(ScalarValue::Utf8(None)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn run(args: Vec<ColumnarValue>) -> DataFusionResult<Vec<Option<String>>> {
        let result = spark_regexp_extract(&args)?;
        match result {
            ColumnarValue::Array(arr) => {
                let s = arr
                    .as_any()
                    .downcast_ref::<GenericStringArray<i32>>()
                    .expect("expected Utf8 array");
                Ok((0..s.len())
                    .map(|i| {
                        if s.is_null(i) {
                            None
                        } else {
                            Some(s.value(i).to_string())
                        }
                    })
                    .collect())
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => Ok(vec![v]),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    fn array(values: Vec<Option<&str>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(values)))
    }

    fn pattern(p: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(p.to_string())))
    }

    fn idx(i: i32) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(i)))
    }

    #[test]
    fn basic_group_extraction() {
        let result = run(vec![
            array(vec![Some("100-200"), Some("foo-bar"), Some("nodelim")]),
            pattern(r"(\d+)-(\d+)"),
            idx(1),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![
                Some("100".to_string()),
                Some(String::new()),
                Some(String::new()),
            ]
        );
    }

    #[test]
    fn idx_zero_returns_whole_match() {
        let result = run(vec![
            array(vec![Some("abc123def456")]),
            pattern(r"\d+"),
            idx(0),
        ])
        .unwrap();
        assert_eq!(result, vec![Some("123".to_string())]);
    }

    #[test]
    fn default_idx_is_one() {
        let result = run(vec![array(vec![Some("100-200")]), pattern(r"(\d+)-(\d+)")]).unwrap();
        assert_eq!(result, vec![Some("100".to_string())]);
    }

    #[test]
    fn null_subject_returns_null() {
        let result = run(vec![
            array(vec![Some("a1b"), None, Some("c2d")]),
            pattern(r"(\d)"),
            idx(1),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![Some("1".to_string()), None, Some("2".to_string())]
        );
    }

    #[test]
    fn null_pattern_returns_null() {
        let result = run(vec![
            array(vec![Some("abc")]),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            idx(1),
        ])
        .unwrap();
        assert_eq!(result, vec![None]);
    }

    #[test]
    fn unmatched_optional_group_returns_empty_string() {
        let result = run(vec![
            array(vec![Some("foo")]),
            pattern(r"(foo)(bar)?"),
            idx(2),
        ])
        .unwrap();
        assert_eq!(result, vec![Some(String::new())]);
    }

    #[test]
    fn group_index_out_of_range_errors() {
        let err = spark_regexp_extract(&[
            array(vec![Some("abc")]),
            pattern(r"(a)(b)"),
            idx(3),
        ])
        .err()
        .unwrap();
        assert!(err.to_string().contains("group count"));
    }

    #[test]
    fn negative_index_errors() {
        let err = spark_regexp_extract(&[array(vec![Some("abc")]), pattern(r"(a)"), idx(-1)])
            .err()
            .unwrap();
        assert!(err.to_string().contains("non-negative"));
    }

    #[test]
    fn invalid_regex_errors() {
        let err = spark_regexp_extract(&[array(vec![Some("abc")]), pattern(r"(unclosed"), idx(0)])
            .err()
            .unwrap();
        assert!(err.to_string().contains("Invalid regex"));
    }
}
