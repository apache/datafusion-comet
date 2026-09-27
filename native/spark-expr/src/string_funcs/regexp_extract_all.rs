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
use regex::{CaptureLocations, Regex};
use std::sync::Arc;

use super::pattern_cache::PatternCache;
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
pub fn spark_regexp_extract_all(
    args: &[ColumnarValue],
    regex_cache: &PatternCache,
) -> DataFusionResult<ColumnarValue> {
    let (regex, group_idx, subject) = match parse_args("regexp_extract_all", args, regex_cache)? {
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
    let mut values_builder = StringBuilder::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(array.len() + 1);
    let mut null_buffer = BooleanBufferBuilder::new(array.len());
    offsets.push(0);

    // One set of capture locations serves the whole batch, so no per-match allocation
    // touches the compiled regex's shared group-info Arc from several threads at once.
    let mut locations = regex.capture_locations();
    for i in 0..array.len() {
        if array.is_null(i) {
            offsets.push(values_builder.len() as i32);
            null_buffer.append(false);
        } else {
            for_each_group_match(regex, &mut locations, array.value(i), group_idx, |s| {
                values_builder.append_value(s)
            });
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
    let mut locations = regex.capture_locations();
    let mut matches = Vec::new();
    for_each_group_match(regex, &mut locations, input, group_idx, |s| {
        matches.push(s.to_string())
    });
    matches
}

/// Calls `f` with the text of group `group_idx` for every non-overlapping match of `regex`
/// in `haystack`, in the order `captures_iter` yields them, or with the empty string when
/// the group does not participate. Each match costs a single capture search into
/// `locations` and no allocation.
fn for_each_group_match(
    regex: &Regex,
    locations: &mut CaptureLocations,
    haystack: &str,
    group_idx: usize,
    mut f: impl FnMut(&str),
) {
    let mut start = 0;
    let mut last_end = None;
    while start <= haystack.len() {
        let Some(m) = regex.captures_read_at(locations, haystack, start) else {
            break;
        };
        // The regex crate's iterators drop an empty match that sits at the end of the
        // previous match and search again one byte further on. Such a match can only be
        // found from that end, so the retry never skips twice in a row.
        if m.is_empty() && Some(m.end()) == last_end {
            debug_assert_eq!(Some(start), last_end);
            start += 1;
            continue;
        }
        start = m.end();
        last_end = Some(m.end());
        let group = locations
            .get(group_idx)
            .map_or("", |(group_start, group_end)| {
                &haystack[group_start..group_end]
            });
        f(group);
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
    use arrow::array::{LargeStringArray, StringArray};

    fn call_raw(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        spark_regexp_extract_all(args, &PatternCache::new())
    }

    fn run(args: Vec<ColumnarValue>) -> DataFusionResult<Vec<Option<Vec<String>>>> {
        let result = call_raw(&args)?;
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
    fn empty_matches_are_kept_and_iteration_terminates() {
        // The regex crate yields empty matches but skips one that sits at the end of the
        // previous match, so `a*` on "ba" is ["", "a"] with no trailing empty.
        let result = run(vec![array(vec![Some("ba")]), pattern(r"a*"), idx(0)]).unwrap();
        assert_eq!(result, vec![Some(vec![String::new(), "a".to_string()])]);
    }

    #[test]
    fn empty_matches_advance_over_multibyte_chars() {
        let result = run(vec![array(vec![Some("日x本")]), pattern(r"x*"), idx(0)]).unwrap();
        assert_eq!(
            result,
            vec![Some(vec![String::new(), "x".to_string(), String::new()])]
        );
    }

    #[test]
    fn anchors_and_word_boundaries_see_full_context() {
        let result = run(vec![
            array(vec![Some("cat hat bat")]),
            pattern(r"\b\w+\b"),
            idx(0),
        ])
        .unwrap();
        assert_eq!(
            result,
            vec![Some(vec![
                "cat".to_string(),
                "hat".to_string(),
                "bat".to_string()
            ])]
        );
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
        let err = call_raw(&[array(vec![Some("abc")]), pattern(r"(a)(b)"), idx(3)])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("group index"), "{msg}");
        assert!(msg.contains("but got 3"), "{msg}");
    }

    #[test]
    fn negative_index_errors() {
        let err = call_raw(&[array(vec![Some("abc")]), pattern(r"(a)"), idx(-1)])
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(msg.contains("group index"), "{msg}");
        assert!(msg.contains("but got -1"), "{msg}");
    }

    #[test]
    fn invalid_regex_errors() {
        let err = call_raw(&[array(vec![Some("abc")]), pattern(r"(unclosed"), idx(0)])
            .err()
            .unwrap();
        assert!(err.to_string().contains("`regexp`"));
    }

    /// One expression evaluates many batches; the pattern must compile once and results
    /// must stay correct on every batch.
    #[test]
    fn compiles_regex_once_across_batches() {
        let cache = PatternCache::new();
        for batch in 0..4 {
            let subject = format!("{batch}1-{batch}2, {batch}3-{batch}4");
            let result = spark_regexp_extract_all(
                &[array(vec![Some(&subject)]), pattern(r"(\d+)-(\d+)"), idx(1)],
                &cache,
            )
            .unwrap();
            match result {
                ColumnarValue::Array(arr) => {
                    let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
                    let inner = list.value(0);
                    let strs = inner.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(strs.value(0), format!("{batch}1"));
                    assert_eq!(strs.value(1), format!("{batch}3"));
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert_eq!(cache.compile_count(), 1);
    }

    /// The match walk must visit exactly the matches `captures_iter` yields, including the
    /// empty-match and multibyte cases where the crate's iterator skips or nudges forward.
    #[test]
    fn matches_agree_with_captures_iter() {
        let patterns = [
            r"a*",
            r"(a*)",
            r"\b",
            r"(\d*)",
            r"(?:)",
            r"(\d+)",
            r"(a+)",
            r"zzz",
            r"(foo)(bar)?",
            r"x*",
            r"\b\w+\b",
            r"(.)(.)?",
        ];
        let haystacks = [
            "",
            "a",
            "ba",
            "aaa",
            "123-456-789-123",
            "日x本",
            "café résumé",
            "こんにちは世界",
            "a😀b😀",
            "foo foo bar",
            "cat hat bat",
            "a b\tc",
        ];
        for pattern in patterns {
            let regex = Regex::new(pattern).unwrap();
            let mut locations = regex.capture_locations();
            for haystack in haystacks {
                for group_idx in [0usize, 1, 2, 7] {
                    let expected: Vec<String> = regex
                        .captures_iter(haystack)
                        .map(|caps| caps.get(group_idx).map_or("", |m| m.as_str()).to_string())
                        .collect();
                    let mut actual = Vec::new();
                    for_each_group_match(&regex, &mut locations, haystack, group_idx, |s| {
                        actual.push(s.to_string())
                    });
                    assert_eq!(
                        actual, expected,
                        "pattern {pattern:?} on {haystack:?} group {group_idx}"
                    );
                }
            }
        }
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
        let result = call_raw(&[array, pattern(r"(\d)"), idx(1)]).unwrap();
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
