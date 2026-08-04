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

use datafusion::common::{exec_err, DataFusionError, Result as DataFusionResult, ScalarValue};
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
    /// A scalar pattern or idx was null; the result is null for every input row. `len` is
    /// `Some(n)` when the subject is an array (one null per row) and `None` for a scalar
    /// subject.
    NullResult { len: Option<usize> },
}

/// Validate `(subject, pattern, [idx])` and return either a compiled `Regex` and group index
/// ready for extraction, or a request to short-circuit to a null result.
pub(super) fn parse_args<'a>(
    fn_name: &'static str,
    args: &'a [ColumnarValue],
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

    let regex = Regex::new(pattern).map_err(|e| {
        DataFusionError::Execution(format!(
            "The value of parameter `regexp` in `{fn_name}` is invalid: '{pattern}' ({e})"
        ))
    })?;

    let group_count = regex.captures_len() as i32 - 1;
    if idx < 0 || idx > group_count {
        return Err(DataFusionError::Execution(format!(
            "The value of parameter `idx` in `{fn_name}` is invalid: \
             Expects group index between 0 and {group_count}, but got {idx}."
        )));
    }

    Ok(ParsedArgs::Parsed {
        regex,
        group_idx: idx as usize,
        subject: &args[0],
    })
}

/// Returns `Some(row_count)` for an array subject and `None` for a scalar.
pub(super) fn subject_len(value: &ColumnarValue) -> Option<usize> {
    match value {
        ColumnarValue::Array(a) => Some(a.len()),
        ColumnarValue::Scalar(_) => None,
    }
}
