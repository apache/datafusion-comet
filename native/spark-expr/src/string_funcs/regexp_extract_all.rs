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
    Array, ArrayBuilder, ArrayRef, BooleanBufferBuilder, GenericStringArray, ListArray,
    OffsetSizeTrait, StringArray, StringBuilder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{
    cast::as_generic_string_array, exec_err, DataFusionError, Result as DataFusionResult,
    ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;
use std::sync::Arc;

/// Spark-compatible `regexp_extract_all(subject, pattern, idx)`.
///
/// Returns an array of all substrings of `subject` matched by group `idx` across every
/// non-overlapping match of `pattern`. `idx = 0` returns the entire match. An unmatched
/// optional group contributes the empty string. No matches yields an empty array. Returns
/// null when any input is null. Errors when `idx` is out of range for the pattern's group
/// count.
///
/// Note: this uses the Rust `regex` crate, whose syntax differs from Java's regex engine in
/// some ways. The expression is therefore reported as Incompatible.
pub fn spark_regexp_extract_all(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "regexp_extract_all expects 2 or 3 arguments (subject, pattern, [idx]), got {}",
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
                return exec_err!("regexp_extract_all idx must be an Int32 scalar");
            }
        }
    } else {
        1
    };

    let pattern: &str = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(p)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(p))) => p,
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            return Ok(null_result(subject_len(&args[0])));
        }
        _ => {
            return exec_err!("regexp_extract_all pattern must be a scalar string");
        }
    };

    let regex = Regex::new(pattern).map_err(|e| {
        DataFusionError::Execution(format!(
            "The value of parameter `regexp` in `regexp_extract_all` is invalid: \
             '{pattern}' ({e})"
        ))
    })?;

    let group_count = regex.captures_len() as i32 - 1;
    if idx < 0 || idx > group_count {
        return Err(DataFusionError::Execution(format!(
            "The value of parameter `idx` in `regexp_extract_all` is invalid: \
             Expects group index between 0 and {group_count}, but got {idx}."
        )));
    }
    let group_idx = idx as usize;

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let strings = as_generic_string_array::<i32>(array.as_ref())?;
                Ok(ColumnarValue::Array(extract_all_array(
                    strings, &regex, group_idx,
                )))
            }
            DataType::LargeUtf8 => {
                let strings = as_generic_string_array::<i64>(array.as_ref())?;
                Ok(ColumnarValue::Array(extract_all_array(
                    strings, &regex, group_idx,
                )))
            }
            other => exec_err!(
                "regexp_extract_all expects Utf8 or LargeUtf8 subject, got {:?}",
                other
            ),
        },
        ColumnarValue::Scalar(ScalarValue::Utf8(s))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(s)) => match s {
            None => Ok(ColumnarValue::Scalar(scalar_null_list())),
            Some(s) => {
                let matches = extract_one(s, &regex, group_idx);
                let values: Arc<dyn Array> = Arc::new(StringArray::from(matches));
                let field = Arc::new(Field::new("item", DataType::Utf8, true));
                let offsets = OffsetBuffer::new(vec![0i32, values.len() as i32].into());
                let list = ListArray::new(field, offsets, values, None);
                Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(list))))
            }
        },
        _ => exec_err!("regexp_extract_all subject must be a string"),
    }
}

fn extract_all_array<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    regex: &Regex,
    group_idx: usize,
) -> ArrayRef {
    let mut values_builder = StringBuilder::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(array.len() + 1);
    let mut null_buffer = BooleanBufferBuilder::new(array.len());
    offsets.push(0);

    for i in 0..array.len() {
        if array.is_null(i) {
            offsets.push(values_builder.len() as i32);
            null_buffer.append(false);
        } else {
            for caps in regex.captures_iter(array.value(i)) {
                let s = caps.get(group_idx).map(|m| m.as_str()).unwrap_or("");
                values_builder.append_value(s);
            }
            offsets.push(values_builder.len() as i32);
            null_buffer.append(true);
        }
    }

    let values = Arc::new(values_builder.finish()) as ArrayRef;
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let nulls = NullBuffer::new(null_buffer.finish());
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        Some(nulls),
    ))
}

fn extract_one(input: &str, regex: &Regex, group_idx: usize) -> Vec<String> {
    regex
        .captures_iter(input)
        .map(|caps| {
            caps.get(group_idx)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default()
        })
        .collect()
}

fn subject_len(value: &ColumnarValue) -> Option<usize> {
    match value {
        ColumnarValue::Array(a) => Some(a.len()),
        ColumnarValue::Scalar(_) => None,
    }
}

fn null_result(len: Option<usize>) -> ColumnarValue {
    match len {
        Some(n) => ColumnarValue::Array(null_list_array(n)),
        None => ColumnarValue::Scalar(scalar_null_list()),
    }
}

fn null_list_array(len: usize) -> ArrayRef {
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let offsets = OffsetBuffer::new(vec![0i32; len + 1].into());
    let nulls = NullBuffer::new_null(len);
    Arc::new(ListArray::new(field, offsets, values, Some(nulls)))
}

fn scalar_null_list() -> ScalarValue {
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let offsets = OffsetBuffer::new(vec![0i32, 0].into());
    let nulls = NullBuffer::new_null(1);
    ScalarValue::List(Arc::new(ListArray::new(
        field,
        offsets,
        values,
        Some(nulls),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn run(args: Vec<ColumnarValue>) -> DataFusionResult<Vec<Option<Vec<String>>>> {
        let result = spark_regexp_extract_all(&args)?;
        let list = match result {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(ScalarValue::List(arr)) => arr as ArrayRef,
            other => panic!("unexpected result: {other:?}"),
        };
        let list = list
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("expected ListArray");
        Ok((0..list.len())
            .map(|i| {
                if list.is_null(i) {
                    None
                } else {
                    let inner = list.value(i);
                    let strs = inner
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("expected inner StringArray");
                    Some((0..strs.len()).map(|j| strs.value(j).to_string()).collect())
                }
            })
            .collect())
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
            array(vec![
                Some("100-200, 300-400"),
                Some("foo-bar"),
                Some("nodelim"),
            ]),
            pattern(r"(\d+)-(\d+)"),
            idx(1),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![
                Some(vec!["100".to_string(), "300".to_string()]),
                Some(vec![]),
                Some(vec![]),
            ]
        );
    }

    #[test]
    fn second_group() {
        let result = run(vec![
            array(vec![Some("100-200, 300-400")]),
            pattern(r"(\d+)-(\d+)"),
            idx(2),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![Some(vec!["200".to_string(), "400".to_string()])]
        );
    }

    #[test]
    fn idx_zero_returns_whole_matches() {
        let result = run(vec![
            array(vec![Some("abc123def456")]),
            pattern(r"\d+"),
            idx(0),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![Some(vec!["123".to_string(), "456".to_string()])]
        );
    }

    #[test]
    fn default_idx_is_one() {
        let result = run(vec![
            array(vec![Some("100-200, 300-400")]),
            pattern(r"(\d+)-(\d+)"),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![Some(vec!["100".to_string(), "300".to_string()])]
        );
    }

    #[test]
    fn no_match_returns_empty_array() {
        let result = run(vec![array(vec![Some("abc")]), pattern(r"(\d+)"), idx(1)]).unwrap();
        assert_eq!(result, vec![Some(vec![])]);
    }

    #[test]
    fn null_subject_returns_null() {
        let result = run(vec![
            array(vec![Some("1 2 3"), None, Some("4 5")]),
            pattern(r"(\d)"),
            idx(1),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![
                Some(vec!["1".to_string(), "2".to_string(), "3".to_string()]),
                None,
                Some(vec!["4".to_string(), "5".to_string()]),
            ]
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
            array(vec![Some("foo foo")]),
            pattern(r"(foo)(bar)?"),
            idx(2),
        ])
        .unwrap();
        assert_eq!(result, vec![Some(vec![String::new(), String::new()])]);
    }

    #[test]
    fn group_index_out_of_range_errors() {
        let err = spark_regexp_extract_all(&[array(vec![Some("abc")]), pattern(r"(a)(b)"), idx(3)])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("group index"), "{msg}");
        assert!(msg.contains("but got 3"), "{msg}");
    }

    #[test]
    fn negative_index_errors() {
        let err = spark_regexp_extract_all(&[array(vec![Some("abc")]), pattern(r"(a)"), idx(-1)])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("group index"), "{msg}");
        assert!(msg.contains("but got -1"), "{msg}");
    }

    #[test]
    fn invalid_regex_errors() {
        let err =
            spark_regexp_extract_all(&[array(vec![Some("abc")]), pattern(r"(unclosed"), idx(0)])
                .err()
                .unwrap();
        assert!(err.to_string().contains("`regexp`"));
    }
}
