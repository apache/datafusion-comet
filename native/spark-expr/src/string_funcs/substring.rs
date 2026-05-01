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

#![allow(deprecated)]

use crate::kernels::strings::substring;
use arrow::array::{as_dictionary_array, as_largestring_array, as_string_array, Array, ArrayRef};
use arrow::datatypes::{DataType, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct SubstringExpr {
    pub child: Arc<dyn PhysicalExpr>,
    pub start: i64,
    pub len: u64,
}

impl Hash for SubstringExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.start.hash(state);
        self.len.hash(state);
    }
}

impl PartialEq for SubstringExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.start.eq(&other.start) && self.len.eq(&other.len)
    }
}

impl SubstringExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, start: i64, len: u64) -> Self {
        Self { child, start, len }
    }
}

impl Display for SubstringExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Substring [start: {}, len: {}, child: {}]",
            self.start, self.len, self.child
        )
    }
}

impl PhysicalExpr for SubstringExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        let is_scalar = matches!(arg, ColumnarValue::Scalar(_));
        let array = arg.into_array(1)?;
        // Spark and Arrow differ for negative start: Arrow clamps
        // start to 0 then takes `len` chars, but Spark computes
        // end = unclamped_start + len, then clamps both independently.
        let result = if self.start < 0 {
            spark_substring_negative_start(&array, self.start, self.len)?
        } else {
            substring(&array, self.start, self.len)?
        };
        if is_scalar {
            let scalar = datafusion::common::ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(SubstringExpr::new(
            Arc::clone(&children[0]),
            self.start,
            self.len,
        )))
    }
}

/// Implement Spark's substring semantics for negative start positions.
/// Spark: start = numChars + pos, end = start + len, clamp both, empty if start >= end.
/// Arrow: start = max(0, numChars + pos), take len chars — differs when start is clamped.
fn spark_substring_negative_start(
    array: &ArrayRef,
    start: i64,
    len: u64,
) -> datafusion::common::Result<ArrayRef> {
    use arrow::array::{
        BinaryArray, DictionaryArray, GenericBinaryBuilder, GenericStringBuilder, LargeBinaryArray,
    };

    match array.data_type() {
        DataType::Utf8 => {
            let str_array = as_string_array(array);
            let mut builder = GenericStringBuilder::<i32>::new();
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(spark_substr_negative(str_array.value(i), start, len));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let str_array = as_largestring_array(array);
            let mut builder = GenericStringBuilder::<i64>::new();
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(spark_substr_negative(str_array.value(i), start, len));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Binary => {
            let bin_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut builder = GenericBinaryBuilder::<i32>::new();
            for i in 0..bin_array.len() {
                if bin_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(spark_binary_substr_negative(
                        bin_array.value(i),
                        start,
                        len,
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::LargeBinary => {
            let bin_array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let mut builder = GenericBinaryBuilder::<i64>::new();
            for i in 0..bin_array.len() {
                if bin_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(spark_binary_substr_negative(
                        bin_array.value(i),
                        start,
                        len,
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(array);
            let values = spark_substring_negative_start(dict.values(), start, len)?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        dt => Err(datafusion::common::DataFusionError::Internal(format!(
            "Unsupported input type for substring with negative start: {dt:?}"
        ))),
    }
}

fn spark_substr_negative(s: &str, pos: i64, len: u64) -> &str {
    let num_chars = s.chars().count() as i64;
    let end = (num_chars + pos).saturating_add(len as i64).min(num_chars);
    let start = (num_chars + pos).max(0);
    if start >= end {
        return "";
    }

    let mut it = s.char_indices();
    let byte_start = it
        .by_ref()
        .nth(start as usize)
        .map(|(b, _)| b)
        .unwrap_or(s.len());
    let span = (end - start - 1) as usize;
    let byte_end = it.nth(span).map(|(b, _)| b).unwrap_or(s.len());

    &s[byte_start..byte_end]
}

fn spark_binary_substr_negative(bytes: &[u8], pos: i64, len: u64) -> &[u8] {
    let num_bytes = bytes.len() as i64;
    let start = num_bytes + pos;
    let end = start.saturating_add(len as i64).min(num_bytes);
    let start = start.max(0);

    if start >= end {
        return &[];
    }

    &bytes[start as usize..end as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;

    fn make_batch(values: Vec<Option<&str>>) -> RecordBatch {
        let array = Arc::new(StringArray::from(values)) as ArrayRef;
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap()
    }

    fn evaluate_substring(values: Vec<Option<&str>>, start: i64, len: u64) -> Vec<Option<String>> {
        let batch = make_batch(values);
        let child = Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>;
        let expr = SubstringExpr::new(child, start, len);
        let result = expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = as_string_array(&arr);
                (0..str_arr.len())
                    .map(|i| {
                        if str_arr.is_null(i) {
                            None
                        } else {
                            Some(str_arr.value(i).to_string())
                        }
                    })
                    .collect()
            }
            _ => panic!("Expected Array result"),
        }
    }

    // --- Unit tests for spark_substr_negative ---

    #[test]
    fn test_negative_basic() {
        assert_eq!(spark_substr_negative("hello", -3, 3), "llo");
    }

    #[test]
    fn test_negative_len_clips_at_end() {
        assert_eq!(spark_substr_negative("hello", -3, 100), "llo");
    }

    #[test]
    fn test_negative_len_shorter_than_available() {
        assert_eq!(spark_substr_negative("hello", -3, 1), "l");
    }

    #[test]
    fn test_negative_start_beyond_string() {
        assert_eq!(spark_substr_negative("hello", -10, 3), "");
    }

    #[test]
    fn test_negative_start_beyond_but_len_reaches_into_string() {
        // pos=-7 on "hello"(5 chars): start = 5 + (-7) = -2, end = min(-2+8, 5) = 5,
        // clamped start = 0, take 5 chars
        assert_eq!(spark_substr_negative("hello", -7, 8), "hello");
    }

    #[test]
    fn test_negative_start_equals_length() {
        assert_eq!(spark_substr_negative("hello", -5, 5), "hello");
    }

    #[test]
    fn test_negative_zero_len() {
        assert_eq!(spark_substr_negative("hello", -3, 0), "");
    }

    #[test]
    fn test_negative_empty_string() {
        assert_eq!(spark_substr_negative("", -1, 1), "");
    }

    #[test]
    fn test_negative_single_char() {
        assert_eq!(spark_substr_negative("a", -1, 1), "a");
    }

    #[test]
    fn test_negative_multibyte_utf8() {
        assert_eq!(spark_substr_negative("こんにちは", -2, 2), "ちは");
    }

    #[test]
    fn test_negative_emoji() {
        assert_eq!(spark_substr_negative("🎉🎊🎈", -1, 1), "🎈");
    }

    #[test]
    fn test_negative_mixed_ascii_multibyte() {
        // "ab🎉cd" has 5 chars. pos=-3: start=5+(-3)=2, end=min(2+2,5)=4 → chars 2,3 = "🎉c"
        assert_eq!(spark_substr_negative("ab🎉cd", -3, 2), "🎉c");
    }

    // --- End-to-end SubstringExpr tests (positive start) ---
    // NOTE: SubstringExpr.start uses 0-based indexing. The serde layer
    // converts Spark's 1-based positions before constructing SubstringExpr.

    #[test]
    fn test_basic_positive_start() {
        // start=0 (0-based) → first character
        let result =
            evaluate_substring(vec![Some("hello world"), Some("abc"), Some(""), None], 0, 5);
        assert_eq!(
            result,
            vec![
                Some("hello".to_string()),
                Some("abc".to_string()),
                Some("".to_string()),
                None,
            ]
        );
    }

    #[test]
    fn test_positive_start_offset() {
        // start=1 (0-based) → skip first character
        let result = evaluate_substring(vec![Some("hello world"), Some("abc")], 1, 5);
        assert_eq!(
            result,
            vec![Some("ello ".to_string()), Some("bc".to_string())]
        );
    }

    #[test]
    fn test_start_zero() {
        let result = evaluate_substring(vec![Some("hello")], 0, 3);
        assert_eq!(result, vec![Some("hel".to_string())]);
    }

    #[test]
    fn test_start_beyond_string_length() {
        let result = evaluate_substring(vec![Some("hello"), Some("ab")], 100, 5);
        assert_eq!(result, vec![Some("".to_string()), Some("".to_string())]);
    }

    #[test]
    fn test_len_zero() {
        let result = evaluate_substring(vec![Some("hello")], 1, 0);
        assert_eq!(result, vec![Some("".to_string())]);
    }

    #[test]
    fn test_len_exceeds_string() {
        // start=0 (0-based), len=100 on "hi" → "hi"
        let result = evaluate_substring(vec![Some("hi")], 0, 100);
        assert_eq!(result, vec![Some("hi".to_string())]);
    }

    #[test]
    fn test_start_at_last_char() {
        // "hello" has 5 chars, 0-based index 4 → 'o'
        let result = evaluate_substring(vec![Some("hello")], 4, 10);
        assert_eq!(result, vec![Some("o".to_string())]);
    }

    #[test]
    fn test_very_large_start() {
        let result = evaluate_substring(vec![Some("hello")], i64::from(i32::MAX), 5);
        assert_eq!(result, vec![Some("".to_string())]);
    }

    #[test]
    fn test_very_large_len() {
        // start=0 (0-based) with u64::MAX length
        let result = evaluate_substring(vec![Some("hello")], 0, u64::MAX);
        assert_eq!(result, vec![Some("hello".to_string())]);
    }

    // --- End-to-end SubstringExpr tests (negative start) ---

    #[test]
    fn test_negative_start_end_to_end() {
        let result = evaluate_substring(
            vec![Some("hello world"), Some("abc"), Some(""), None],
            -3,
            3,
        );
        assert_eq!(
            result,
            vec![
                Some("rld".to_string()),
                Some("abc".to_string()),
                Some("".to_string()),
                None,
            ]
        );
    }

    #[test]
    fn test_negative_start_with_clip() {
        // -2 with len=1 on "hello": start=3, end=4 → "l"
        let result = evaluate_substring(vec![Some("hello")], -2, 1);
        assert_eq!(result, vec![Some("l".to_string())]);
    }

    #[test]
    fn test_negative_start_beyond_string_end_to_end() {
        let result = evaluate_substring(vec![Some("hello")], -10, 3);
        assert_eq!(result, vec![Some("".to_string())]);
    }

    #[test]
    fn test_negative_start_far_beyond_with_large_len() {
        // -7 on "hello"(5): start=-2, end=min(-2+8,5)=5, clamped start=0 → "hello"
        let result = evaluate_substring(vec![Some("hello")], -7, 8);
        assert_eq!(result, vec![Some("hello".to_string())]);
    }

    #[test]
    fn test_negative_start_equals_string_length() {
        let result = evaluate_substring(vec![Some("hello")], -5, 5);
        assert_eq!(result, vec![Some("hello".to_string())]);
    }

    // --- Multi-byte UTF-8 through SubstringExpr (0-based start) ---

    #[test]
    fn test_multibyte_positive_start() {
        // start=0 (0-based), len=3 on "こんにちは世界" → "こんに"
        let result = evaluate_substring(vec![Some("こんにちは世界")], 0, 3);
        assert_eq!(result, vec![Some("こんに".to_string())]);
    }

    #[test]
    fn test_multibyte_middle() {
        // start=3 (0-based) on "こんにちは世界" → 'ち','は' → "ちは"
        let result = evaluate_substring(vec![Some("こんにちは世界")], 3, 2);
        assert_eq!(result, vec![Some("ちは".to_string())]);
    }

    #[test]
    fn test_multibyte_negative_start() {
        let result = evaluate_substring(vec![Some("こんにちは世界")], -2, 2);
        assert_eq!(result, vec![Some("世界".to_string())]);
    }

    #[test]
    fn test_emoji_substring() {
        // start=1 (0-based) on "🎉🎊🎈🎁" → '🎊','🎈' → "🎊🎈"
        let result = evaluate_substring(vec![Some("🎉🎊🎈🎁")], 1, 2);
        assert_eq!(result, vec![Some("🎊🎈".to_string())]);
    }

    #[test]
    fn test_mixed_ascii_emoji() {
        // start=2 (0-based) on "ab🎉cd" → '🎉'
        let result = evaluate_substring(vec![Some("ab🎉cd")], 2, 1);
        assert_eq!(result, vec![Some("🎉".to_string())]);
    }

    // --- LargeUtf8 support ---

    #[test]
    fn test_large_utf8_negative_start() {
        let array = Arc::new(LargeStringArray::from(vec![
            Some("hello world"),
            None,
            Some("abc"),
        ])) as ArrayRef;
        let result = spark_substring_negative_start(&array, -3, 3).unwrap();
        let str_arr = as_largestring_array(&result);
        assert_eq!(str_arr.value(0), "rld");
        assert!(str_arr.is_null(1));
        assert_eq!(str_arr.value(2), "abc");
    }

    // --- Binary negative start ---

    #[test]
    fn test_binary_negative_basic() {
        assert_eq!(
            spark_binary_substr_negative(&[1, 2, 3, 4, 5], -2, 2),
            &[4, 5]
        );
    }

    #[test]
    fn test_binary_negative_clips_at_end() {
        assert_eq!(
            spark_binary_substr_negative(&[1, 2, 3, 4, 5], -2, 100),
            &[4, 5]
        );
    }

    #[test]
    fn test_binary_negative_beyond_length() {
        let empty: &[u8] = &[];
        assert_eq!(spark_binary_substr_negative(&[1, 2, 3], -10, 3), empty);
    }

    #[test]
    fn test_binary_negative_start_array() {
        use arrow::array::BinaryArray;
        let array = Arc::new(BinaryArray::from(vec![
            Some(vec![1, 2, 3, 4, 5].as_slice()),
            Some(&[0xFF]),
            Some(&[]),
            None,
        ])) as ArrayRef;
        let result = spark_substring_negative_start(&array, -2, 2).unwrap();
        let bin_arr = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(bin_arr.value(0), &[4, 5]);
        assert_eq!(bin_arr.value(1), &[0xFF]);
        assert_eq!(bin_arr.value(2), &[] as &[u8]);
        assert!(bin_arr.is_null(3));
    }

    // --- Unicode edge cases: decomposed vs precomposed and combining characters ---
    // Spark substring operates on code points, not graphemes.
    // "é" as e + \u{301} (combining acute) = 2 code points
    // "é" as \u{e9} (precomposed) = 1 code point
    // "తెలుగు" (Telugu) = 6 code points: త, ె, ల, ు, గ, ు

    #[test]
    fn test_negative_decomposed_e_acute() {
        // "e\u{301}" has 2 code points; pos=-1 → just the combining accent
        assert_eq!(spark_substr_negative("e\u{301}", -1, 1), "\u{301}");
    }

    #[test]
    fn test_negative_precomposed_e_acute() {
        // "\u{e9}" has 1 code point; pos=-1 → the whole character
        assert_eq!(spark_substr_negative("\u{e9}", -1, 1), "\u{e9}");
    }

    #[test]
    fn test_negative_telugu() {
        // "తెలుగు" has 6 code points; pos=-2 → last 2 code points "గు"
        assert_eq!(spark_substr_negative("తెలుగు", -2, 2), "గు");
    }

    #[test]
    fn test_decomposed_e_acute_split() {
        // "e\u{301}" = 2 code points; start=0, len=1 → just "e" (strips combining accent)
        let result = evaluate_substring(vec![Some("e\u{301}")], 0, 1);
        assert_eq!(result, vec![Some("e".to_string())]);
    }

    #[test]
    fn test_decomposed_e_acute_accent_only() {
        // start=1, len=1 → just the combining acute accent
        let result = evaluate_substring(vec![Some("e\u{301}")], 1, 1);
        assert_eq!(result, vec![Some("\u{301}".to_string())]);
    }

    #[test]
    fn test_decomposed_e_acute_full() {
        // start=0, len=2 → both code points "é" (decomposed)
        let result = evaluate_substring(vec![Some("e\u{301}")], 0, 2);
        assert_eq!(result, vec![Some("e\u{301}".to_string())]);
    }

    #[test]
    fn test_precomposed_e_acute() {
        // "\u{e9}" = 1 code point; start=0, len=1 → "é"
        let result = evaluate_substring(vec![Some("\u{e9}")], 0, 1);
        assert_eq!(result, vec![Some("\u{e9}".to_string())]);
    }

    #[test]
    fn test_decomposed_vs_precomposed_different_len() {
        // Same visual character but different code point counts
        let decomposed = "e\u{301}";
        let precomposed = "\u{e9}";
        let result = evaluate_substring(vec![Some(decomposed), Some(precomposed)], 0, 1);
        assert_eq!(
            result,
            vec![
                Some("e".to_string()),      // only base 'e', accent stripped
                Some("\u{e9}".to_string()), // full precomposed character
            ]
        );
    }

    #[test]
    fn test_telugu_first_two_codepoints() {
        // "తెలుగు" start=0, len=2 → "తె" (base + vowel sign)
        let result = evaluate_substring(vec![Some("తెలుగు")], 0, 2);
        assert_eq!(result, vec![Some("తె".to_string())]);
    }

    #[test]
    fn test_telugu_middle() {
        // "తెలుగు" start=2, len=2 → "లు"
        let result = evaluate_substring(vec![Some("తెలుగు")], 2, 2);
        assert_eq!(result, vec![Some("లు".to_string())]);
    }

    #[test]
    fn test_telugu_negative_start() {
        // "తెలుగు" has 6 code points; -3 with len=3 → last 3 code points "ుగు"
        let result = evaluate_substring(vec![Some("తెలుగు")], -3, 3);
        assert_eq!(result, vec![Some("ుగు".to_string())]);
    }

    #[test]
    fn test_telugu_full() {
        let result = evaluate_substring(vec![Some("తెలుగు")], 0, 100);
        assert_eq!(result, vec![Some("తెలుగు".to_string())]);
    }

    // --- All-null input ---

    #[test]
    fn test_all_nulls() {
        let result = evaluate_substring(vec![None, None, None], 1, 5);
        assert_eq!(result, vec![None, None, None]);
    }

    // --- Empty strings ---

    #[test]
    fn test_all_empty_strings() {
        let result = evaluate_substring(vec![Some(""), Some(""), Some("")], 1, 5);
        assert_eq!(
            result,
            vec![
                Some("".to_string()),
                Some("".to_string()),
                Some("".to_string()),
            ]
        );
    }

    // --- Scalar support ---

    fn evaluate_scalar_substring(value: Option<&str>, start: i64, len: u64) -> ColumnarValue {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::Literal;

        let scalar = ScalarValue::Utf8(value.map(|s| s.to_string()));
        let child = Arc::new(Literal::new(scalar)) as Arc<dyn PhysicalExpr>;
        let expr = SubstringExpr::new(child, start, len);
        let schema = Schema::new(vec![Field::new("dummy", DataType::Utf8, true)]);
        let batch = RecordBatch::new_empty(Arc::new(schema));
        expr.evaluate(&batch).unwrap()
    }

    #[test]
    fn test_scalar_basic() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some("hello world"), 0, 5) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "hello"),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_negative_start() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some("hello world"), -3, 3) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "rld"),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_null() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(None, 0, 5) {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("Expected Scalar Utf8(None), got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_empty_string() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some(""), 0, 5) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, ""),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_multibyte() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some("こんにちは"), 0, 3) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "こんに"),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_negative_start_multibyte() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some("こんにちは"), -2, 2) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "ちは"),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_decomposed_e_acute() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some("e\u{301}"), 0, 1) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "e"),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }

    #[test]
    fn test_scalar_telugu() {
        use datafusion::common::ScalarValue;
        match evaluate_scalar_substring(Some("తెలుగు"), -2, 2) {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "గు"),
            other => panic!("Expected Scalar Utf8, got {:?}", other),
        }
    }
}
