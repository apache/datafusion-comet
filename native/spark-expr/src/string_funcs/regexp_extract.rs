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
    Array, ArrayRef, GenericStringArray, OffsetSizeTrait, StringArray, StringBuilder,
};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::as_generic_string_array, exec_err, Result as DataFusionResult, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;
use std::sync::Arc;

use super::regexp_extract_common::{parse_args, ParsedArgs};

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
    let (regex, group_idx, subject) = match parse_args("regexp_extract", args)? {
        ParsedArgs::Parsed {
            regex,
            group_idx,
            subject,
        } => (regex, group_idx, subject),
        ParsedArgs::NullResult { len } => return Ok(null_result(len)),
    };

    match subject {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let strings = as_generic_string_array::<i32>(array.as_ref())?;
                Ok(ColumnarValue::Array(extract_array(
                    strings, &regex, group_idx,
                )))
            }
            DataType::LargeUtf8 => {
                let strings = as_generic_string_array::<i64>(array.as_ref())?;
                Ok(ColumnarValue::Array(extract_array(
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
            Some(s) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(extract_one(
                s, &regex, group_idx,
            ))))),
        },
        _ => exec_err!("regexp_extract subject must be a string"),
    }
}

/// Always produces a `StringArray` (i32 offsets) regardless of the input offset width:
/// Spark's `RegExpExtract.dataType` is `StringType` and the Comet serde serializes that as
/// the protobuf return type, so handing back a `LargeStringArray` would be a type mismatch.
/// `&str` slices are width-agnostic, so it is safe to copy them into a 32-bit-offset builder.
fn extract_array<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    regex: &Regex,
    group_idx: usize,
) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(array.len(), array.value_data().len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let extracted = match regex.captures(array.value(i)) {
                Some(caps) => caps.get(group_idx).map(|m| m.as_str()).unwrap_or(""),
                None => "",
            };
            builder.append_value(extracted);
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

fn null_result(len: Option<usize>) -> ColumnarValue {
    match len {
        Some(n) => ColumnarValue::Array(Arc::new(StringArray::new_null(n))),
        None => ColumnarValue::Scalar(ScalarValue::Utf8(None)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray};
    use datafusion::common::DataFusionError;

    fn run(args: Vec<ColumnarValue>) -> DataFusionResult<Vec<Option<String>>> {
        let result = spark_regexp_extract(&args)?;
        match result {
            ColumnarValue::Array(arr) => {
                let s = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("expected Utf8 array (regexp_extract must always return StringArray)");
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
        let err = spark_regexp_extract(&[array(vec![Some("abc")]), pattern(r"(a)(b)"), idx(3)])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("group index"), "{msg}");
        assert!(msg.contains("but got 3"), "{msg}");
    }

    #[test]
    fn negative_index_errors() {
        let err = spark_regexp_extract(&[array(vec![Some("abc")]), pattern(r"(a)"), idx(-1)])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("group index"), "{msg}");
        assert!(msg.contains("but got -1"), "{msg}");
    }

    #[test]
    fn invalid_regex_errors() {
        let err = spark_regexp_extract(&[array(vec![Some("abc")]), pattern(r"(unclosed"), idx(0)])
            .err()
            .unwrap();
        assert!(err.to_string().contains("`regexp`"));
    }

    /// `LargeUtf8` subject must still produce a `StringArray` (i32 offsets) so the result type
    /// matches Spark's `RegExpExtract.dataType` = `StringType`. Regression for the bug where
    /// `extract_array::<i64>` used to build a `LargeStringArray` and trip a type mismatch.
    #[test]
    fn large_utf8_subject_returns_utf8_array() {
        let array = ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![
            Some("100-200"),
            None,
            Some("foo-bar"),
        ])));
        let result = spark_regexp_extract(&[array, pattern(r"(\d+)-(\d+)"), idx(1)]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                arr.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "expected StringArray, got {:?}",
                            arr.data_type()
                        ))
                    })
                    .unwrap();
                assert_eq!(arr.len(), 3);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
