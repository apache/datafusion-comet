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
//
//! Shared helpers for `regexp_extract` and `regexp_extract_all`. Both UDFs accept
//! `(subject, pattern, [idx])` and need to validate the same things in the same way; this
//! module centralizes that parsing and the null short-circuit so each UDF is left with
//! its own per-row loop.

use super::pattern_cache::PatternCache;
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::as_generic_string_array, exec_err, DataFusionError, Result as DataFusionResult,
    ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;

/// Result of parsing the `(subject, pattern, [idx])` arguments shared by `regexp_extract`
/// and `regexp_extract_all`.
pub(super) enum ParsedArgs<'a> {
    /// Arguments validated; the caller can run its per-row extraction loop.
    Parsed {
        regex: Regex,
        group_idx: usize,
        subject: &'a ColumnarValue,
    },
    /// A scalar pattern or idx was null, or no subject row is non-null; the result is null for
    /// every input row. `len` is `Some(n)` when the subject is an array (one null per row) and
    /// `None` for a scalar subject.
    NullResult { len: Option<usize> },
    /// `idx` is out of range but no non-null row matches, so the caller returns an empty
    /// result for every non-null row and null for every null row. `subject` is a `Utf8` or
    /// `LargeUtf8` array or scalar.
    NoMatch { subject: &'a ColumnarValue },
}

/// Validate `(subject, pattern, [idx])` and return either a compiled `Regex` and group index
/// ready for extraction, or a request to short-circuit to a null result.
pub(super) fn parse_args<'a>(
    fn_name: &'static str,
    args: &'a [ColumnarValue],
    regex_cache: &PatternCache,
) -> DataFusionResult<ParsedArgs<'a>> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "{fn_name} expects 2 or 3 arguments (subject, pattern, [idx]), got {}",
            args.len()
        );
    }

    let idx: i32 = if args.len() == 3 {
        match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => *i,
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {
                return Ok(ParsedArgs::NullResult {
                    len: subject_len(&args[0]),
                });
            }
            _ => {
                return exec_err!("{fn_name} idx must be an Int32 scalar");
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
            return Ok(ParsedArgs::NullResult {
                len: subject_len(&args[0]),
            });
        }
        _ => {
            return exec_err!("{fn_name} pattern must be a scalar string");
        }
    };

    // Spark compiles the pattern only for a row whose subject is not null, so an invalid pattern
    // is not an error for a batch without one.
    if !has_non_null_subject(&args[0]) {
        return Ok(ParsedArgs::NullResult {
            len: subject_len(&args[0]),
        });
    }

    // The pattern is a plan-time literal, so the cache makes this compile a one-time cost
    // for the expression instead of a per-batch cost.
    let regex = regex_cache.get_or_compile(pattern).map_err(|e| {
        DataFusionError::Execution(format!(
            "The value of parameter `regexp` in `{fn_name}` is invalid: '{pattern}' ({e})"
        ))
    })?;

    // Spark checks `idx` only on a row where the pattern matches, so an out-of-range index is
    // an error only when some row matches. Checking per batch fails the same query: Spark
    // fails the task on the first matching row and this fails the batch that contains it.
    // A matching scalar subject still fails a batch with no rows (#7725).
    let group_count = regex.captures_len() as i32 - 1;
    if !(0..=group_count).contains(&idx) {
        if any_row_matches(fn_name, &regex, &args[0])? {
            return Err(DataFusionError::Execution(format!(
                "The value of parameter `idx` in `{fn_name}` is invalid: \
                 Expects group index between 0 and {group_count}, but got {idx}."
            )));
        }
        return Ok(ParsedArgs::NoMatch { subject: &args[0] });
    }

    Ok(ParsedArgs::Parsed {
        regex,
        group_idx: idx as usize,
        subject: &args[0],
    })
}

fn any_row_matches(
    fn_name: &'static str,
    regex: &Regex,
    subject: &ColumnarValue,
) -> DataFusionResult<bool> {
    match subject {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(as_generic_string_array::<i32>(array.as_ref())?
                .iter()
                .flatten()
                .any(|s| regex.is_match(s))),
            DataType::LargeUtf8 => Ok(as_generic_string_array::<i64>(array.as_ref())?
                .iter()
                .flatten()
                .any(|s| regex.is_match(s))),
            other => exec_err!("{fn_name} expects Utf8 or LargeUtf8 subject, got {other:?}"),
        },
        ColumnarValue::Scalar(ScalarValue::Utf8(s))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(s)) => {
            Ok(s.as_deref().is_some_and(|s| regex.is_match(s)))
        }
        ColumnarValue::Scalar(_) => exec_err!("{fn_name} subject must be a string"),
    }
}

/// Whether `subject` has a non-null row. A subject that is not `Utf8` or `LargeUtf8` counts as
/// having one, so that it reaches the type check instead of becoming null.
fn has_non_null_subject(subject: &ColumnarValue) -> bool {
    match subject {
        ColumnarValue::Array(array) => {
            !matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8)
                || array.null_count() < array.len()
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s)) => s.is_some(),
        ColumnarValue::Scalar(_) => true,
    }
}

/// Returns `Some(row_count)` for an array subject and `None` for a scalar.
pub(super) fn subject_len(value: &ColumnarValue) -> Option<usize> {
    match value {
        ColumnarValue::Array(a) => Some(a.len()),
        ColumnarValue::Scalar(_) => None,
    }
}
