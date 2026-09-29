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
use datafusion::common::{cast::as_generic_string_array, DataFusionError, Result, ScalarValue};
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
///
/// For sizes up to `MAX_RETAINED_CAPACITY`, reuses TLS buffers (bounded to
/// at most `2 * MAX_RETAINED_CAPACITY * 4` bytes per worker thread).
/// For oversized rows, allocates temporary vectors in the call scope so the
/// TLS buffers never grow beyond the cap.
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
                // Validating scratch-row lengths before the inner loop eliminates
                // compiler bounds checks inside the DP hot loop.
                assert!(prev.len() > m && curr.len() > m);
                assert!(s_bytes.len() >= m);

                let mut left = (j + 1) as i32;
                curr[0] = left;
                let mut diag = prev[0];

                for i in 1..=m {
                    let up = prev[i];
                    let cost = if s_bytes[i - 1] == t_byte { 0 } else { 1 };
                    let val = (up + 1).min(left + 1).min(diag + cost);
                    curr[i] = val;
                    left = val;
                    diag = up;
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
            assert!(prev.len() > m && curr.len() > m);
            assert!(s_chars.len() >= m);

            let mut left = (j + 1) as i32;
            curr[0] = left;
            let mut diag = prev[0];

            for i in 1..=m {
                let up = prev[i];
                let cost = if s_chars[i - 1] == t_char { 0 } else { 1 };
                let val = (up + 1).min(left + 1).min(diag + cost);
                curr[i] = val;
                left = val;
                diag = up;
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

    if s.is_ascii() && t.is_ascii() {
        let s_bytes = s.as_bytes();
        let t_bytes = t.as_bytes();
        let m = s_bytes.len();
        let n = t_bytes.len();

        if (m as i32 - n as i32).abs() > threshold {
            return -1;
        }
        if m == 0 {
            return if n as i32 <= threshold { n as i32 } else { -1 };
        }
        if n == 0 {
            return if m as i32 <= threshold { m as i32 } else { -1 };
        }

        let (s_bytes, t_bytes, m, n) = if m > n {
            (t_bytes, s_bytes, n, m)
        } else {
            (s_bytes, t_bytes, m, n)
        };

        if (n as i32 - m as i32) > threshold {
            return -1;
        }

        // The Levenshtein distance between strings of length m and n (where m <= n)
        // cannot exceed n. Capping threshold at n prevents integer overflow when threshold
        // is i32::MAX, while preserving identical Spark semantics.
        let effective_threshold = threshold.min(n as i32);
        let out_of_band = effective_threshold + 1;

        return with_scratch_buffers(m + 1, out_of_band, |prev, curr| {
            for (i, val) in prev.iter_mut().enumerate() {
                *val = if i as i32 <= effective_threshold {
                    i as i32
                } else {
                    out_of_band
                };
            }

            for (j, &t_byte) in t_bytes.iter().enumerate().take(n) {
                let j_1 = (j + 1) as i32;
                curr[0] = if j_1 <= effective_threshold {
                    j_1
                } else {
                    out_of_band
                };

                let min_i = if j_1 > effective_threshold {
                    ((j_1 - effective_threshold) as usize).max(1)
                } else {
                    1
                };
                let max_i = (j_1 as usize)
                    .saturating_add(effective_threshold as usize)
                    .min(m);

                if min_i > 1 {
                    curr[min_i - 1] = out_of_band;
                }

                assert!(prev.len() > m && curr.len() > m);
                assert!(s_bytes.len() >= m);

                for i in min_i..=max_i {
                    let cost = if s_bytes[i - 1] == t_byte { 0 } else { 1 };
                    curr[i] = (prev[i] + 1).min(curr[i - 1] + 1).min(prev[i - 1] + cost);
                }

                if max_i < m {
                    curr[max_i + 1] = out_of_band;
                }

                std::mem::swap(prev, curr);
            }

            let result = prev[m];
            if result <= threshold {
                result
            } else {
                -1
            }
        });
    }

    let s_chars: Vec<char> = s.chars().collect();
    let t_chars: Vec<char> = t.chars().collect();
    let m = s_chars.len();
    let n = t_chars.len();

    if (m as i32 - n as i32).abs() > threshold {
        return -1;
    }
    if m == 0 {
        return if n as i32 <= threshold { n as i32 } else { -1 };
    }
    if n == 0 {
        return if m as i32 <= threshold { m as i32 } else { -1 };
    }

    let (s_chars, t_chars, m, n) = if m > n {
        (t_chars, s_chars, n, m)
    } else {
        (s_chars, t_chars, m, n)
    };

    if (n as i32 - m as i32) > threshold {
        return -1;
    }

    let effective_threshold = threshold.min(n as i32);
    let out_of_band = effective_threshold + 1;

    with_scratch_buffers(m + 1, out_of_band, |prev, curr| {
        for (i, val) in prev.iter_mut().enumerate() {
            *val = if i as i32 <= effective_threshold {
                i as i32
            } else {
                out_of_band
            };
        }

        for (j, &t_char) in t_chars.iter().enumerate().take(n) {
            let j_1 = (j + 1) as i32;
            curr[0] = if j_1 <= effective_threshold {
                j_1
            } else {
                out_of_band
            };

            let min_i = if j_1 > effective_threshold {
                ((j_1 - effective_threshold) as usize).max(1)
            } else {
                1
            };
            let max_i = (j_1 as usize)
                .saturating_add(effective_threshold as usize)
                .min(m);

            if min_i > 1 {
                curr[min_i - 1] = out_of_band;
            }

            assert!(prev.len() > m && curr.len() > m);
            assert!(s_chars.len() >= m);

            for i in min_i..=max_i {
                let cost = if s_chars[i - 1] == t_char { 0 } else { 1 };
                curr[i] = (prev[i] + 1).min(curr[i - 1] + 1).min(prev[i - 1] + cost);
            }

            if max_i < m {
                curr[max_i + 1] = out_of_band;
            }

            std::mem::swap(prev, curr);
        }

        let result = prev[m];
        if result <= threshold {
            result
        } else {
            -1
        }
    })
}

/// Evaluates Levenshtein distance across arrays with independent left and right string offset types.
fn levenshtein<L: OffsetSizeTrait, R: OffsetSizeTrait>(
    left: &GenericStringArray<L>,
    right: &GenericStringArray<R>,
) -> Result<ArrayRef> {
    let mut builder = Int32Array::builder(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(levenshtein_distance(left.value(i), right.value(i)));
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Evaluates thresholded Levenshtein distance across arrays with independent left and right string offset types.
fn levenshtein_with_threshold<L: OffsetSizeTrait, R: OffsetSizeTrait>(
    left: &GenericStringArray<L>,
    right: &GenericStringArray<R>,
    threshold: &Int32Array,
) -> Result<ArrayRef> {
    let mut builder = Int32Array::builder(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) || threshold.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(levenshtein_distance_with_threshold(
                left.value(i),
                right.value(i),
                threshold.value(i),
            ));
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Computes the Levenshtein distance between two strings, matching Spark semantics.
pub fn spark_levenshtein(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match args.len() {
        2 => {
            if let (ColumnarValue::Scalar(s1), ColumnarValue::Scalar(s2)) = (&args[0], &args[1]) {
                let res = match (s1, s2) {
                    (ScalarValue::Utf8(Some(v1)), ScalarValue::Utf8(Some(v2)))
                    | (ScalarValue::LargeUtf8(Some(v1)), ScalarValue::LargeUtf8(Some(v2)))
                    | (ScalarValue::Utf8(Some(v1)), ScalarValue::LargeUtf8(Some(v2)))
                    | (ScalarValue::LargeUtf8(Some(v1)), ScalarValue::Utf8(Some(v2))) => {
                        Some(levenshtein_distance(v1, v2))
                    }
                    (ScalarValue::Utf8(None), _)
                    | (_, ScalarValue::Utf8(None))
                    | (ScalarValue::LargeUtf8(None), _)
                    | (_, ScalarValue::LargeUtf8(None)) => None,
                    _ => {
                        return Err(DataFusionError::Internal(
                            "Expected string scalar for levenshtein".to_string(),
                        ))
                    }
                };
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(res)));
            }

            let num_rows = match (&args[0], &args[1]) {
                (ColumnarValue::Array(a), _) | (_, ColumnarValue::Array(a)) => a.len(),
                _ => unreachable!(),
            };

            let left = args[0].clone().into_array(num_rows)?;
            let right = args[1].clone().into_array(num_rows)?;

            let result = match (left.data_type(), right.data_type()) {
                (DataType::Utf8, DataType::Utf8) => {
                    let left = as_generic_string_array::<i32>(&left)?;
                    let right = as_generic_string_array::<i32>(&right)?;
                    levenshtein::<i32, i32>(left, right)?
                }
                (DataType::Utf8, DataType::LargeUtf8) => {
                    let left = as_generic_string_array::<i32>(&left)?;
                    let right = as_generic_string_array::<i64>(&right)?;
                    levenshtein::<i32, i64>(left, right)?
                }
                (DataType::LargeUtf8, DataType::Utf8) => {
                    let left = as_generic_string_array::<i64>(&left)?;
                    let right = as_generic_string_array::<i32>(&right)?;
                    levenshtein::<i64, i32>(left, right)?
                }
                (DataType::LargeUtf8, DataType::LargeUtf8) => {
                    let left = as_generic_string_array::<i64>(&left)?;
                    let right = as_generic_string_array::<i64>(&right)?;
                    levenshtein::<i64, i64>(left, right)?
                }
                (l, r) => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data types for levenshtein: ({l:?}, {r:?})"
                    )))
                }
            };
            Ok(ColumnarValue::Array(result))
        }
        3 => {
            if let (
                ColumnarValue::Scalar(s1),
                ColumnarValue::Scalar(s2),
                ColumnarValue::Scalar(s3),
            ) = (&args[0], &args[1], &args[2])
            {
                let threshold = match s3 {
                    ScalarValue::Int32(Some(t)) => Some(*t),
                    ScalarValue::Int32(None) => None,
                    _ => {
                        return Err(DataFusionError::Internal(
                            "Expected Int32 scalar for threshold".to_string(),
                        ))
                    }
                };

                let res = match (s1, s2, threshold) {
                    (ScalarValue::Utf8(Some(v1)), ScalarValue::Utf8(Some(v2)), Some(t))
                    | (
                        ScalarValue::LargeUtf8(Some(v1)),
                        ScalarValue::LargeUtf8(Some(v2)),
                        Some(t),
                    )
                    | (ScalarValue::Utf8(Some(v1)), ScalarValue::LargeUtf8(Some(v2)), Some(t))
                    | (ScalarValue::LargeUtf8(Some(v1)), ScalarValue::Utf8(Some(v2)), Some(t)) => {
                        Some(levenshtein_distance_with_threshold(v1, v2, t))
                    }
                    _ => None,
                };
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(res)));
            }

            let num_rows = match (&args[0], &args[1], &args[2]) {
                (ColumnarValue::Array(a), _, _)
                | (_, ColumnarValue::Array(a), _)
                | (_, _, ColumnarValue::Array(a)) => a.len(),
                _ => unreachable!(),
            };

            let left = args[0].clone().into_array(num_rows)?;
            let right = args[1].clone().into_array(num_rows)?;
            let threshold = args[2].clone().into_array(num_rows)?;
            let threshold = threshold
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Int32Array for threshold".to_string())
                })?;

            let result = match (left.data_type(), right.data_type()) {
                (DataType::Utf8, DataType::Utf8) => {
                    let left = as_generic_string_array::<i32>(&left)?;
                    let right = as_generic_string_array::<i32>(&right)?;
                    levenshtein_with_threshold::<i32, i32>(left, right, threshold)?
                }
                (DataType::Utf8, DataType::LargeUtf8) => {
                    let left = as_generic_string_array::<i32>(&left)?;
                    let right = as_generic_string_array::<i64>(&right)?;
                    levenshtein_with_threshold::<i32, i64>(left, right, threshold)?
                }
                (DataType::LargeUtf8, DataType::Utf8) => {
                    let left = as_generic_string_array::<i64>(&left)?;
                    let right = as_generic_string_array::<i32>(&right)?;
                    levenshtein_with_threshold::<i64, i32>(left, right, threshold)?
                }
                (DataType::LargeUtf8, DataType::LargeUtf8) => {
                    let left = as_generic_string_array::<i64>(&left)?;
                    let right = as_generic_string_array::<i64>(&right)?;
                    levenshtein_with_threshold::<i64, i64>(left, right, threshold)?
                }
                (l, r) => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data types for levenshtein: ({l:?}, {r:?})"
                    )))
                }
            };
            Ok(ColumnarValue::Array(result))
        }
        n => Err(DataFusionError::Internal(format!(
            "levenshtein expects 2 or 3 arguments, got {n}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray};

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("a", ""), 1);
        assert_eq!(levenshtein_distance("", "a"), 1);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
    }

    #[test]
    fn test_levenshtein_distance_unicode() {
        assert_eq!(levenshtein_distance("naïve", "naive"), 1);
        assert_eq!(levenshtein_distance("café", "cafe"), 1);
        assert_eq!(levenshtein_distance("smörgås", "smorgas"), 2);
    }

    #[test]
    fn test_levenshtein_distance_with_threshold() {
        assert_eq!(
            levenshtein_distance_with_threshold("kitten", "sitting", 3),
            3
        );
        assert_eq!(
            levenshtein_distance_with_threshold("kitten", "sitting", 2),
            -1
        );
        assert_eq!(
            levenshtein_distance_with_threshold("kitten", "sitting", -1),
            -1
        );
        assert_eq!(levenshtein_distance_with_threshold("a", "bb", 1), -1);
    }

    #[test]
    fn test_levenshtein_distance_with_threshold_max_int() {
        // Boundary cases for i32::MAX and i32::MAX - 1 to ensure overflow safety in debug builds
        assert_eq!(
            levenshtein_distance_with_threshold("frog", "fog", i32::MAX),
            1
        );
        assert_eq!(
            levenshtein_distance_with_threshold("frog", "fog", i32::MAX - 1),
            1
        );
        assert_eq!(
            levenshtein_distance_with_threshold("café", "cafe", i32::MAX),
            1
        );
        assert_eq!(
            levenshtein_distance_with_threshold("café", "cafe", i32::MAX - 1),
            1
        );
    }

    #[test]
    fn test_levenshtein_distance_with_threshold_unicode() {
        assert_eq!(levenshtein_distance_with_threshold("café", "cafe", 1), 1);
        assert_eq!(levenshtein_distance_with_threshold("café", "cafe", 0), -1);
    }

    #[test]
    fn test_spark_levenshtein_scalars() {
        let arg0 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("kitten".to_string())));
        let arg1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some("sitting".to_string())));
        let result = spark_levenshtein(&[arg0, arg1]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(3))) => {}
            other => panic!("Expected Scalar(Some(3)), got {other:?}"),
        }
    }

    #[test]
    fn test_spark_levenshtein_mixed_offset_types() {
        let left_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("kitten".to_string())));
        let right_array = Arc::new(LargeStringArray::from(vec![Some("sitting")])) as ArrayRef;

        let result = spark_levenshtein(&[left_scalar, ColumnarValue::Array(right_array)]).unwrap();
        let array = result.into_array(1).unwrap();
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 3);

        let left_large = Arc::new(LargeStringArray::from(vec![Some("kitten")])) as ArrayRef;
        let right_utf8 = Arc::new(StringArray::from(vec![Some("sitting")])) as ArrayRef;

        let res1 = spark_levenshtein(&[
            ColumnarValue::Array(left_large.clone()),
            ColumnarValue::Array(right_utf8.clone()),
        ])
        .unwrap();
        let arr1 = res1.into_array(1).unwrap();
        assert_eq!(
            arr1.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            3
        );

        let res2 = spark_levenshtein(&[
            ColumnarValue::Array(right_utf8),
            ColumnarValue::Array(left_large),
        ])
        .unwrap();
        let arr2 = res2.into_array(1).unwrap();
        assert_eq!(
            arr2.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            3
        );

        let threshold = ColumnarValue::Scalar(ScalarValue::Int32(Some(3)));
        let left_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("kitten".to_string())));
        let right_large = Arc::new(LargeStringArray::from(vec![Some("sitting")])) as ArrayRef;

        let res3 = spark_levenshtein(&[left_scalar, ColumnarValue::Array(right_large), threshold])
            .unwrap();
        let arr3 = res3.into_array(1).unwrap();
        assert_eq!(
            arr3.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            3
        );
    }

    #[test]
    fn test_spark_levenshtein_arrays() {
        let left = Arc::new(StringArray::from(vec![Some("kitten"), None, Some("abc")])) as ArrayRef;
        let right =
            Arc::new(StringArray::from(vec![Some("sitting"), Some("xyz"), None])) as ArrayRef;

        let result =
            spark_levenshtein(&[ColumnarValue::Array(left), ColumnarValue::Array(right)]).unwrap();
        let array = result.into_array(3).unwrap();
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(int_array.value(0), 3);
        assert!(int_array.is_null(1));
        assert!(int_array.is_null(2));
    }

    #[test]
    fn test_scratch_buffer_retained_capacity() {
        let large_s = "a".repeat(1500);
        let large_t = "b".repeat(1500);

        let dist = levenshtein_distance(&large_s, &large_t);
        assert_eq!(dist, 1500);

        LEVENSHTEIN_SCRATCH.with(|scratch| {
            let borrow = scratch.borrow();
            assert!(borrow.0.capacity() <= MAX_RETAINED_CAPACITY);
            assert!(borrow.1.capacity() <= MAX_RETAINED_CAPACITY);
        });
    }

    #[test]
    fn test_longer_inputs_correctness() {
        let s = "a".repeat(512);
        let t = "b".repeat(512);
        assert_eq!(levenshtein_distance(&s, &t), 512);
    }
}
