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

/// Maximum retained scratch buffer capacity (1024 elements * 4 bytes = 4 KB).
/// Inputs requiring larger buffers bypass TLS to avoid unbounded memory retention.
const MAX_RETAINED_CAPACITY: usize = 1024;

// Thread-local scratch buffers to avoid heap allocations in the row processing loop
thread_local! {
    static LEVENSHTEIN_SCRATCH: std::cell::RefCell<(Vec<i32>, Vec<i32>)> =
        std::cell::RefCell::new((Vec::with_capacity(64), Vec::with_capacity(64)));
}

/// Executes a closure using scratch buffers.
/// For sizes up to `MAX_RETAINED_CAPACITY`, reuses TLS buffers.
/// For oversized rows, allocates temporary vectors to prevent TLS memory bloat.
/// Executes a closure using scratch buffers.
/// For sizes up to `MAX_RETAINED_CAPACITY`, reuses TLS buffers.
/// For oversized rows, allocates temporary vectors to prevent TLS memory bloat.
#[inline]
fn with_scratch_buffers<F, R>(len: usize, default_val: i32, f: F) -> R
where
    F: FnOnce(&mut Vec<i32>, &mut Vec<i32>) -> R,
{
    if len > MAX_RETAINED_CAPACITY {
        let mut prev = vec![default_val; len];
        let mut curr = vec![default_val; len];
        f(&mut prev, &mut curr)
    } else {
        LEVENSHTEIN_SCRATCH.with(|scratch| {
            let mut borrow = scratch.borrow_mut();
            let (prev, curr) = &mut *borrow;

            prev.clear();
            prev.resize(len, default_val);
            curr.clear();
            curr.resize(len, default_val);

            f(prev, curr)
        })
    }
}

/// Computes the Levenshtein edit distance between two UTF-8 strings.
///
/// This uses the standard dynamic programming algorithm with O(min(m,n)) space.
fn levenshtein_distance(s: &str, t: &str) -> i32 {
    // Fast path for ASCII strings: operate directly on raw bytes without vector allocations
    if s.is_ascii() && t.is_ascii() {
        let s_bytes = s.as_bytes();
        let t_bytes = t.as_bytes();
        let m = s_bytes.len();
        let n = t_bytes.len();

        if m == 0 {
            return n as i32;
        }
        if n == 0 {
            return m as i32;
        }

        let (s_bytes, t_bytes, m, n) = if m > n {
            (t_bytes, s_bytes, n, m)
        } else {
            (s_bytes, t_bytes, m, n)
        };

        return with_scratch_buffers(m + 1, 0, |prev, curr| {
            for (i, val) in prev.iter_mut().enumerate() {
                *val = i as i32;
            }

            for (j, &t_byte) in t_bytes.iter().enumerate().take(n) {
                curr[0] = (j + 1) as i32;
                for i in 1..=m {
                    let cost = if s_bytes[i - 1] == t_byte { 0 } else { 1 };
                    curr[i] = (prev[i] + 1).min(curr[i - 1] + 1).min(prev[i - 1] + cost);
                }
                std::mem::swap(prev, curr);
            }

            prev[m]
        });
    }

    // General Unicode path for non-ASCII strings
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

    with_scratch_buffers(m + 1, 0, |prev, curr| {
        // Initialize base case: distance from empty string
        for (i, val) in prev.iter_mut().enumerate() {
            *val = i as i32;
        }

        for (j, &t_char) in t_chars.iter().enumerate().take(n) {
            curr[0] = (j + 1) as i32;
            for i in 1..=m {
                let cost = if s_chars[i - 1] == t_char { 0 } else { 1 };
                curr[i] = (prev[i] + 1).min(curr[i - 1] + 1).min(prev[i - 1] + cost);
            }
            std::mem::swap(prev, curr);
        }

        prev[m]
    })
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

    // Fast path for ASCII strings
    if s.is_ascii() && t.is_ascii() {
        let s_bytes = s.as_bytes();
        let t_bytes = t.as_bytes();
        let (shorter, longer) = if s_bytes.len() <= t_bytes.len() {
            (s_bytes, t_bytes)
        } else {
            (t_bytes, s_bytes)
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

        let out_of_band = n.saturating_add(1) as i32;

        return with_scratch_buffers(m + 1, out_of_band, |prev, curr| {
            for (i, value) in prev.iter_mut().enumerate().take(m.min(threshold) + 1) {
                *value = i as i32;
            }

            for (j_idx, &t_char) in longer.iter().enumerate() {
                let j = j_idx + 1;
                let start = j.saturating_sub(threshold).max(1);
                let end = (j + threshold).min(m);

                if start > end {
                    return -1;
                }

                if start > 1 {
                    curr[start - 1] = out_of_band;
                }

                // Explicitly set curr[0] to out_of_band when outside the threshold band
                if j <= threshold {
                    curr[0] = j as i32;
                } else {
                    curr[0] = out_of_band;
                }

                for (i_idx, &s_char) in shorter.iter().enumerate().take(end).skip(start - 1) {
                    let i = i_idx + 1;
                    let cost = if s_char == t_char { 0 } else { 1 };
                    curr[i] = (prev[i] + 1).min(curr[i - 1] + 1).min(prev[i - 1] + cost);
                }

                if end < m {
                    curr[end + 1] = out_of_band;
                }

                std::mem::swap(prev, curr);
            }

            let result = prev[m];
            if result <= threshold as i32 {
                result
            } else {
                -1
            }
        });
    }

    // General Unicode path for non-ASCII strings with threshold
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

    let out_of_band = n.saturating_add(1) as i32;

    with_scratch_buffers(m + 1, out_of_band, |prev, curr| {
        for (i, value) in prev.iter_mut().enumerate().take(m.min(threshold) + 1) {
            *value = i as i32;
        }

        for (j_idx, &t_char) in longer.iter().enumerate() {
            let j = j_idx + 1;
            let start = j.saturating_sub(threshold).max(1);
            let end = (j + threshold).min(m);

            if start > end {
                return -1;
            }

            if start > 1 {
                curr[start - 1] = out_of_band;
            }

            // Explicitly set curr[0] to out_of_band when outside the threshold band
            if j <= threshold {
                curr[0] = j as i32;
            } else {
                curr[0] = out_of_band;
            }

            for (i_idx, &s_char) in shorter.iter().enumerate().take(end).skip(start - 1) {
                let i = i_idx + 1;
                let cost = if s_char == t_char { 0 } else { 1 };
                curr[i] = (prev[i] + 1).min(curr[i - 1] + 1).min(prev[i - 1] + cost);
            }

            if end < m {
                curr[end + 1] = out_of_band;
            }

            std::mem::swap(prev, curr);
        }

        let result = prev[m];
        if result <= threshold as i32 {
            result
        } else {
            -1
        }
    })
}

fn evaluate_levenshtein<LeftOffset: OffsetSizeTrait, RightOffset: OffsetSizeTrait>(
    left: &GenericStringArray<LeftOffset>,
    right: &GenericStringArray<RightOffset>,
    threshold: Option<&Int32Array>,
) -> Result<ArrayRef> {
    let mut builder = Int32Array::builder(left.len());

    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
            continue;
        }

        if let Some(threshold_arr) = threshold {
            if threshold_arr.is_null(i) {
                builder.append_null();
                continue;
            }
            let s = left.value(i);
            let t = right.value(i);
            let th = threshold_arr.value(i);
            builder.append_value(levenshtein_distance_with_threshold(s, t, th));
        } else {
            let s = left.value(i);
            let t = right.value(i);
            builder.append_value(levenshtein_distance(s, t));
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn evaluate_string_arrays(
    left: &ArrayRef,
    right: &ArrayRef,
    threshold: Option<&Int32Array>,
) -> Result<ArrayRef> {
    match (left.data_type(), right.data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let left = as_generic_string_array::<i32>(left)?;
            let right = as_generic_string_array::<i32>(right)?;
            evaluate_levenshtein(left, right, threshold)
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            let left = as_generic_string_array::<i32>(left)?;
            let right = as_generic_string_array::<i64>(right)?;
            evaluate_levenshtein(left, right, threshold)
        }
        (DataType::LargeUtf8, DataType::Utf8) => {
            let left = as_generic_string_array::<i64>(left)?;
            let right = as_generic_string_array::<i32>(right)?;
            evaluate_levenshtein(left, right, threshold)
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let left = as_generic_string_array::<i64>(left)?;
            let right = as_generic_string_array::<i64>(right)?;
            evaluate_levenshtein(left, right, threshold)
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported types for spark_levenshtein: {:?}",
            other
        ))),
    }
}

/// Spark-compatible levenshtein scalar function.
///
/// Accepts two or three arguments:
/// - `levenshtein(str1, str2)` -> edit distance
/// - `levenshtein(str1, str2, threshold)` -> edit distance if <= threshold, else -1
///
/// The threshold argument can be either a scalar or a column (array).
/// NULL inputs produce NULL outputs. NULL threshold produces NULL output for that row.
pub fn spark_levenshtein(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DataFusionError::Internal(format!(
            "spark_levenshtein expects 2 or 3 arguments, got {}",
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

    // If all arguments are scalars, compute directly
    if args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        let left = match &args[0] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(datafusion::scalar::ScalarValue::LargeUtf8(Some(s))) => {
                s.as_str()
            }
            _ => {
                return Ok(ColumnarValue::Scalar(
                    datafusion::scalar::ScalarValue::Int32(None),
                ))
            }
        };

        let right = match &args[1] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(datafusion::scalar::ScalarValue::LargeUtf8(Some(s))) => {
                s.as_str()
            }
            _ => {
                return Ok(ColumnarValue::Scalar(
                    datafusion::scalar::ScalarValue::Int32(None),
                ))
            }
        };

        if args.len() == 3 {
            let threshold = match &args[2] {
                ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int32(Some(t))) => *t,
                _ => {
                    return Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::Int32(None),
                    ))
                }
            };
            return Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Int32(Some(levenshtein_distance_with_threshold(
                    left, right, threshold,
                ))),
            ));
        }

        return Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Int32(Some(levenshtein_distance(left, right))),
        ));
    }

    let left = match &args[0] {
        ColumnarValue::Array(a) => Arc::clone(a),
        ColumnarValue::Scalar(s) => s.to_array_of_size(len)?,
    };

    let right = match &args[1] {
        ColumnarValue::Array(a) => Arc::clone(a),
        ColumnarValue::Scalar(s) => s.to_array_of_size(len)?,
    };

    let threshold_array = if args.len() == 3 {
        let array = match &args[2] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(len)?,
        };
        let int_array = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Threshold argument to levenshtein must be Int32".to_string(),
                )
            })?
            .clone();
        Some(int_array)
    } else {
        None
    };

    let result = evaluate_string_arrays(&left, &right, threshold_array.as_ref())?;
    Ok(ColumnarValue::Array(result))
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

    #[test]
    fn test_batch_scratch_state_leak_order_invariance() {
        // Dataset mix:
        // 1. Pathologically long string to expand thread-local scratch buffer
        // 2. Long string with small threshold (maximum uninitialized band cells)
        // 3. Short strings, edge cases, and non-ASCII (accents/umlauts)
        let s_vec = vec![
            "the_quick_brown_fox_jumps_over_the_lazy_dog_and_runs_away_very_far_beyond_the_hills",
            "abcdefghijklmnopqrstuvwxyz_0123456789_abcdefghijklmnopqrstuvwxyz_window_test",
            "kitten",
            "café_au_lait_with_crème_brûlée_and_smörgåsbord_delight",
            "rust",
            "naïve_coöperation",
            "spark",
        ];

        let t_vec = vec![
            "the_quick_brown_fox_jumps_over_the_lazy_dog_and_runs_away_far_away_beyond_the_hills",
            "abcdefghijklmnopqrstuvwxyz_0123456789_abcdefghijklmnopqrstuvwxyz_window_diff",
            "sitting",
            "cafe_au_lait_with_creme_brulee_and_smorgasbord_delight",
            "rust_lang",
            "naive_cooperation",
            "park",
        ];

        // Small thresholds for edge cases where uninitialized cells could leak
        let thresh_vec = vec![10, 2, 3, 5, 5, 2, 1];

        let run_batch =
            |s_list: Vec<&str>, t_list: Vec<&str>, th_list: Vec<i32>| -> Vec<Option<i32>> {
                let s_arr = Arc::new(StringArray::from(s_list)) as ArrayRef;
                let t_arr = Arc::new(StringArray::from(t_list)) as ArrayRef;
                let th_arr = Arc::new(Int32Array::from(th_list)) as ArrayRef;

                let result = spark_levenshtein(&[
                    ColumnarValue::Array(s_arr),
                    ColumnarValue::Array(t_arr),
                    ColumnarValue::Array(th_arr),
                ])
                .unwrap();

                if let ColumnarValue::Array(res_arr) = result {
                    let int_arr = res_arr.as_any().downcast_ref::<Int32Array>().unwrap();
                    (0..int_arr.len())
                        .map(|i| {
                            if int_arr.is_null(i) {
                                None
                            } else {
                                Some(int_arr.value(i))
                            }
                        })
                        .collect()
                } else {
                    panic!("Expected Array result");
                }
            };

        // Forward batch evaluation
        let forward_results = run_batch(s_vec.clone(), t_vec.clone(), thresh_vec.clone());

        // Reverse batch evaluation
        let mut s_rev = s_vec.clone();
        let mut t_rev = t_vec.clone();
        let mut th_rev = thresh_vec.clone();
        s_rev.reverse();
        t_rev.reverse();
        th_rev.reverse();

        let mut reverse_results = run_batch(s_rev, t_rev, th_rev);
        reverse_results.reverse();

        // Ensure results are strictly identical and independent of evaluation order
        assert_eq!(
            forward_results, reverse_results,
            "Results differed depending on row evaluation order in scratch buffer"
        );
    }

    #[test]
    fn test_threshold_out_of_band_reset() {
        // ("a", "bb", 1) -> edit distance is 2, threshold is 1 -> must return -1
        assert_eq!(levenshtein_distance_with_threshold("a", "bb", 1), -1);
        assert_eq!(levenshtein_distance_with_threshold("bb", "a", 1), -1);
    }
}
