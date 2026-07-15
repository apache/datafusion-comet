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
    cast::as_generic_string_array, exec_err, Result as DataFusionResult, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;
use std::sync::Arc;

use super::regexp_extract_common::{parse_args, ParsedArgs};

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
    let (regex, group_idx, subject) = match parse_args("regexp_extract_all", args)? {
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

/// The inner value array is always a `StringArray` (i32 offsets) regardless of the input
/// offset width, mirroring the fix in `regexp_extract::extract_array` so the result type
/// matches Spark's `RegExpExtractAll.dataType` = `ArrayType(StringType)`.
fn extract_all_array<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    regex: &Regex,
    group_idx: usize,
) -> ArrayRef {
    let mut values_builder = StringBuilder::with_capacity(array.len(), values_capacity(array));
    let mut offsets: Vec<i32> = Vec::with_capacity(array.len() + 1);
    let mut null_buffer = BooleanBufferBuilder::new(array.len());
    offsets.push(0);

    let mut matcher = GroupMatcher::new(regex, group_idx);
    for value in array.iter() {
        match value {
            Some(s) => {
                matcher.for_each_match(s, |s| values_builder.append_value(s));
                null_buffer.append(true);
            }
            None => null_buffer.append(false),
        }
        offsets.push(values_builder.len() as i32);
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

/// Byte capacity to pre-allocate for the extracted strings of `array`.
///
/// The extracted text is a subset of the subject, so the subject's own byte length is an upper
/// bound; it is capped so that a large batch does not reserve far more than it is likely to use.
fn values_capacity<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> usize {
    const MAX_CAPACITY: usize = 1 << 20;
    let offsets = array.value_offsets();
    let subject_bytes = offsets[array.len()].as_usize() - offsets[0].as_usize();
    subject_bytes.min(MAX_CAPACITY)
}

fn extract_one(input: &str, regex: &Regex, group_idx: usize) -> Vec<String> {
    let mut matches = Vec::new();
    GroupMatcher::new(regex, group_idx).for_each_match(input, |s| matches.push(s.to_string()));
    matches
}

/// Walks the non-overlapping matches of a regex, yielding the text of one capture group.
///
/// This is equivalent to iterating `Regex::captures_iter` and reading group `group_idx` from
/// each `Captures`, but `captures_iter` allocates a fresh capture-slot buffer for every match.
/// Here a single `CaptureLocations` is reused across all matches and all rows, and when only
/// the whole match is needed (`idx = 0`) the capture groups are not resolved at all.
struct GroupMatcher<'a> {
    regex: &'a Regex,
    group_idx: usize,
    locations: regex::CaptureLocations,
}

impl<'a> GroupMatcher<'a> {
    fn new(regex: &'a Regex, group_idx: usize) -> Self {
        Self {
            regex,
            group_idx,
            locations: regex.capture_locations(),
        }
    }

    /// Calls `emit` once per match with the matched text of the capture group, using the empty
    /// string for a group that did not participate in the match.
    fn for_each_match<F: FnMut(&str)>(&mut self, input: &str, mut emit: F) {
        if self.group_idx == 0 {
            for m in self.regex.find_iter(input) {
                emit(m.as_str());
            }
            return;
        }

        let mut search_start = 0;
        let mut last_match_end: Option<usize> = None;
        while search_start <= input.len() {
            let Some(m) = self
                .regex
                .captures_read_at(&mut self.locations, input, search_start)
            else {
                return;
            };
            // The same empty match is never reported twice: an empty match landing on the end of
            // the previous match is dropped.
            if m.is_empty() && Some(m.end()) == last_match_end {
                search_start = next_char_start(input, m.end());
                continue;
            }
            emit(
                self.locations
                    .get(self.group_idx)
                    .map_or("", |(start, end)| &input[start..end]),
            );
            last_match_end = Some(m.end());
            // Resuming at the end of an empty match would just find it again, so step over the
            // character it sits on.
            search_start = if m.is_empty() {
                next_char_start(input, m.end())
            } else {
                m.end()
            };
        }
    }
}

/// Byte index of the character following the one starting at `index`. An `index` at the end of
/// `input` yields an index past the end, which terminates the search.
fn next_char_start(input: &str, index: usize) -> usize {
    index + input[index..].chars().next().map_or(1, char::len_utf8)
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
    use arrow::array::{LargeStringArray, StringArray};

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

    /// Regression: `LargeUtf8` subject must still produce a `ListArray` whose inner values
    /// are a `StringArray` (i32 offsets), matching Spark's `RegExpExtractAll.dataType` =
    /// `ArrayType(StringType)`.
    #[test]
    fn large_utf8_subject_returns_inner_utf8() {
        let array = ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![
            Some("1 2 3"),
            None,
            Some("4 5"),
        ])));
        let result = spark_regexp_extract_all(&[array, pattern(r"(\d)"), idx(1)]).unwrap();
        let list = match result {
            ColumnarValue::Array(arr) => arr,
            other => panic!("unexpected result: {other:?}"),
        };
        let list = list
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("expected ListArray");
        assert_eq!(list.len(), 3);
        // Inner values must be StringArray, not LargeStringArray
        list.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("inner values must be StringArray");
    }
}
