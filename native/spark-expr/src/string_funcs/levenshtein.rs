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

//! Levenshtein distance expression implementation.
//!
//! Computes the Levenshtein edit distance between two strings,
//! matching Apache Spark's `levenshtein(str1, str2)` semantics.

use arrow::array::{Array, ArrayRef, GenericStringArray, Int32Array, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_generic_string_array, DataFusionError, Result};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Computes the Levenshtein edit distance between two UTF-8 strings.
///
/// This uses the standard dynamic programming algorithm with O(min(m,n)) space.
fn levenshtein_distance(s: &str, t: &str) -> i32 {
    let s_chars: Vec<char> = s.chars().collect();
    let t_chars: Vec<char> = t.chars().collect();
    let m = s_chars.len();
    let n = t_chars.len();

    // Optimization: if one string is empty, distance is the length of the other
    if m == 0 {
        return n as i32;
    }
    if n == 0 {
        return m as i32;
    }

    // Use the shorter string for the "column" to minimize space usage
    let (s_chars, t_chars, m, n) = if m > n {
        (t_chars, s_chars, n, m)
    } else {
        (s_chars, t_chars, m, n)
    };

    // Previous and current row of distances
    let mut prev = vec![0i32; m + 1];
    let mut curr = vec![0i32; m + 1];

    // Initialize base case: distance from empty string
    for (i, val) in prev.iter_mut().enumerate() {
        *val = i as i32;
    }

    for j in 1..=n {
        curr[0] = j as i32;
        for i in 1..=m {
            let cost = if s_chars[i - 1] == t_chars[j - 1] {
                0
            } else {
                1
            };
            curr[i] = (prev[i] + 1) // deletion
                .min(curr[i - 1] + 1) // insertion
                .min(prev[i - 1] + cost); // substitution
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[m]
}

/// Computes the Levenshtein distance up to `threshold` using a diagonal band.
///
/// Spark's three-argument form uses the threshold to avoid evaluating cells that cannot
/// contribute to a result within the requested distance. This keeps the complexity at
/// O(threshold * max(m, n)) when the threshold is small rather than always using O(m * n).
fn levenshtein_distance_with_threshold(s: &str, t: &str, threshold: i32) -> i32 {
    if threshold < 0 {
        return -1;
    }

    let s_chars: Vec<char> = s.chars().collect();
    let t_chars: Vec<char> = t.chars().collect();
    let (shorter, longer) = if s_chars.len() <= t_chars.len() {
        (s_chars, t_chars)
    } else {
        (t_chars, s_chars)
    };
    let m = shorter.len();
    let n = longer.len();
    let threshold = threshold as usize;

    if n - m > threshold {
        return -1;
    }
    if m == 0 {
        return if n <= threshold { n as i32 } else { -1 };
    }

    let out_of_band = n.saturating_add(1);
    let mut prev = vec![out_of_band; m + 1];
    let mut curr = vec![out_of_band; m + 1];
    for (i, value) in prev.iter_mut().enumerate().take(m.min(threshold) + 1) {
        *value = i;
    }

    for j in 1..=n {
        let start = 1.max(j.saturating_sub(threshold));
        let end = m.min(j.saturating_add(threshold));
        if start > end {
            return -1;
        }

        curr[0] = if j <= threshold { j } else { out_of_band };
        curr[start - 1] = if start == 1 { curr[0] } else { out_of_band };
        for i in start..=end {
            let cost = usize::from(shorter[i - 1] != longer[j - 1]);
            curr[i] = prev[i]
                .saturating_add(1)
                .min(curr[i - 1].saturating_add(1))
                .min(prev[i - 1].saturating_add(cost));
        }
        if end < m {
            curr[end + 1] = out_of_band;
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    if prev[m] <= threshold {
        prev[m] as i32
    } else {
        -1
    }
}

fn evaluate_levenshtein<LeftOffset, RightOffset>(
    left: &GenericStringArray<LeftOffset>,
    right: &GenericStringArray<RightOffset>,
    threshold: Option<&Int32Array>,
) -> Int32Array
where
    LeftOffset: OffsetSizeTrait,
    RightOffset: OffsetSizeTrait,
{
    left.iter()
        .zip(right.iter())
        .enumerate()
        .map(|(i, (left_value, right_value))| {
            if threshold.is_some_and(|values| values.is_null(i)) {
                return None;
            }

            match (left_value, right_value) {
                (Some(left_value), Some(right_value)) => Some(match threshold {
                    Some(values) => levenshtein_distance_with_threshold(
                        left_value,
                        right_value,
                        values.value(i),
                    ),
                    None => levenshtein_distance(left_value, right_value),
                }),
                _ => None,
            }
        })
        .collect()
}

fn evaluate_string_arrays(
    left: &ArrayRef,
    right: &ArrayRef,
    threshold: Option<&Int32Array>,
) -> Result<Int32Array> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8) => Ok(evaluate_levenshtein(
            as_generic_string_array::<i32>(left.as_ref())?,
            as_generic_string_array::<i32>(right.as_ref())?,
            threshold,
        )),
        (DataType::Utf8, DataType::LargeUtf8) => Ok(evaluate_levenshtein(
            as_generic_string_array::<i32>(left.as_ref())?,
            as_generic_string_array::<i64>(right.as_ref())?,
            threshold,
        )),
        (DataType::LargeUtf8, DataType::Utf8) => Ok(evaluate_levenshtein(
            as_generic_string_array::<i64>(left.as_ref())?,
            as_generic_string_array::<i32>(right.as_ref())?,
            threshold,
        )),
        (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(evaluate_levenshtein(
            as_generic_string_array::<i64>(left.as_ref())?,
            as_generic_string_array::<i64>(right.as_ref())?,
            threshold,
        )),
        (left_type, right_type) => Err(DataFusionError::Execution(format!(
            "levenshtein expects Utf8 or LargeUtf8 arguments, got {left_type:?} and {right_type:?}"
        ))),
    }
}

/// Spark-compatible levenshtein scalar function.
///
/// Accepts two or three arguments:
/// - `levenshtein(str1, str2)` → edit distance
/// - `levenshtein(str1, str2, threshold)` → edit distance if <= threshold, else -1
///
/// The threshold argument can be either a scalar or a column (array).
/// NULL inputs produce NULL outputs. NULL threshold produces NULL output for that row.
pub fn spark_levenshtein(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DataFusionError::Internal(format!(
            "levenshtein requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    // Determine array length from any array argument
    let len = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        })
        .unwrap_or(1);

    let left = args[0].clone().into_array(len)?;
    let right = args[1].clone().into_array(len)?;
    if left.len() != len || right.len() != len {
        return Err(DataFusionError::Internal(
            "levenshtein arguments must have the same length".to_string(),
        ));
    }

    // Handle the optional threshold argument (scalar or array)
    let threshold_array = if args.len() == 3 {
        let threshold_array = args[2].clone().into_array(len)?;
        if threshold_array.len() != len {
            return Err(DataFusionError::Internal(
                "levenshtein threshold must have the same length as string arguments".to_string(),
            ));
        }
        Some(threshold_array)
    } else {
        None
    };
    let threshold = threshold_array
        .as_ref()
        .map(|array| {
            array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("levenshtein threshold must be Int32".to_string())
            })
        })
        .transpose()?;

    let result = evaluate_string_arrays(&left, &right, threshold)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray};
    use datafusion::common::ScalarValue;

    #[test]
    fn test_levenshtein_basic() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("abc", ""), 3);
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
        assert_eq!(levenshtein_distance("frog", "fog"), 1);
    }

    #[test]
    fn test_levenshtein_unicode() {
        // Spark counts character-level (not byte-level) edit distance
        assert_eq!(levenshtein_distance("你好", "你坏"), 1);
        assert_eq!(levenshtein_distance("abc", "äbc"), 1);
    }

    #[test]
    fn test_spark_levenshtein_nulls() {
        let left = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("abc"),
            None,
            Some("hello"),
        ])));
        let right = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("adc"),
            Some("test"),
            None,
        ])));

        let result = spark_levenshtein(&[left, right]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 1); // abc -> adc = 1
                assert!(int_arr.is_null(1)); // NULL -> test = NULL
                assert!(int_arr.is_null(2)); // hello -> NULL = NULL
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_spark_levenshtein_large_utf8() {
        let left =
            ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![Some("kitten"), None])));
        let right = ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![
            Some("sitting"),
            None,
        ])));

        let result = spark_levenshtein(&[left, right]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 3);
                assert!(int_arr.is_null(1));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_spark_levenshtein_with_threshold() {
        let left = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("kitten"),
            Some("abc"),
            Some("frog"),
        ])));
        let right = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("sitting"),
            Some("adc"),
            Some("fog"),
        ])));
        let threshold = ColumnarValue::Scalar(ScalarValue::Int32(Some(2)));

        let result = spark_levenshtein(&[left, right, threshold]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), -1); // kitten->sitting=3 > 2, return -1
                assert_eq!(int_arr.value(1), 1); // abc->adc=1 <= 2, return 1
                assert_eq!(int_arr.value(2), 1); // frog->fog=1 <= 2, return 1
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_spark_levenshtein_null_threshold() {
        let left = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("abc")])));
        let right = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("adc")])));
        let threshold = ColumnarValue::Scalar(ScalarValue::Int32(None));

        let result = spark_levenshtein(&[left, right, threshold]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.len(), 1);
                assert!(int_arr.is_null(0)); // NULL threshold -> NULL result
            }
            _ => panic!("Expected array result with NULL for NULL threshold"),
        }
    }

    #[test]
    fn test_spark_levenshtein_threshold_as_array() {
        // threshold is a column (array) with per-row values
        let left = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("kitten"),
            Some("frog"),
            Some("abc"),
            Some("hello"),
        ])));
        let right = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("sitting"),
            Some("fog"),
            Some("abc"),
            Some("world"),
        ])));
        // Per-row thresholds: 2, 5, 0, 3
        let threshold = ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(2),
            Some(5),
            Some(0),
            Some(3),
        ])));

        let result = spark_levenshtein(&[left, right, threshold]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), -1); // kitten->sitting=3 > 2, return -1
                assert_eq!(int_arr.value(1), 1); // frog->fog=1 <= 5, return 1
                assert_eq!(int_arr.value(2), 0); // abc->abc=0 <= 0, return 0
                assert_eq!(int_arr.value(3), -1); // hello->world=4 > 3, return -1
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_spark_levenshtein_threshold_array_with_nulls() {
        // threshold array where some values are NULL
        let left = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("abc"),
            Some("hello"),
            Some("frog"),
        ])));
        let right = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("adc"),
            Some("world"),
            Some("fog"),
        ])));
        let threshold = ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(2),
            None, // NULL threshold for this row
            Some(0),
        ])));

        let result = spark_levenshtein(&[left, right, threshold]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(int_arr.value(0), 1); // abc->adc=1 <= 2, return 1
                assert!(int_arr.is_null(1)); // NULL threshold -> NULL
                assert_eq!(int_arr.value(2), -1); // frog->fog=1 > 0, return -1
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_spark_levenshtein_threshold_negative() {
        // Negative threshold means distance always exceeds threshold → return -1
        let left =
            ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("abc"), Some("abc")])));
        let right =
            ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("abc"), Some("adc")])));
        let threshold = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(-1), Some(-5)])));

        let result = spark_levenshtein(&[left, right, threshold]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                // dist=0 > -1 is true, so return -1
                assert_eq!(int_arr.value(0), -1);
                // dist=1 > -5 is true, so return -1
                assert_eq!(int_arr.value(1), -1);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_levenshtein_threshold_banded() {
        assert_eq!(
            levenshtein_distance_with_threshold("kitten", "sitting", 2),
            -1
        );
        assert_eq!(
            levenshtein_distance_with_threshold("kitten", "sitting", 3),
            3
        );
        assert_eq!(
            levenshtein_distance_with_threshold("abcdefghij", "abxdefghij", 1),
            1
        );
        assert_eq!(
            levenshtein_distance_with_threshold("short", "a much longer string", 2),
            -1
        );
    }
}
