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

use arrow::array::{as_string_array, Array, ArrayRef, Int32Array};
use datafusion::common::{DataFusionError, Result, ScalarValue};
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

/// Spark-compatible levenshtein scalar function.
///
/// Accepts two or three arguments:
/// - `levenshtein(str1, str2)` → edit distance
/// - `levenshtein(str1, str2, threshold)` → edit distance if <= threshold, else -1
///
/// NULL inputs produce NULL outputs. NULL threshold produces NULL output.
pub fn spark_levenshtein(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DataFusionError::Internal(format!(
            "levenshtein requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    // Extract optional threshold (3rd argument must be a scalar Int32)
    let threshold: Option<i32> = if args.len() == 3 {
        match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int32(t)) => match t {
                Some(val) => Some(*val),
                None => return Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
            },
            _ => {
                return Err(DataFusionError::Internal(
                    "levenshtein threshold must be an Int32 scalar".to_string(),
                ));
            }
        }
    } else {
        None
    };

    // Expand scalars to arrays for uniform processing
    let len = args
        .iter()
        .take(2)
        .find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        })
        .unwrap_or(1);

    let left = args[0].clone().into_array(len)?;
    let right = args[1].clone().into_array(len)?;

    let left_arr = as_string_array(&left);
    let right_arr = as_string_array(&right);

    let result: Int32Array = left_arr
        .iter()
        .zip(right_arr.iter())
        .map(|(l, r)| match (l, r) {
            (Some(l), Some(r)) => {
                let dist = levenshtein_distance(l, r);
                match threshold {
                    Some(t) if dist > t => Some(-1),
                    _ => Some(dist),
                }
            }
            _ => None, // NULL propagation
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

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
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {} // NULL threshold -> NULL
            _ => panic!("Expected NULL scalar result for NULL threshold"),
        }
    }
}
