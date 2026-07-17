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

use arrow::array::{Array, StringArray, Time64NanosecondArray};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;

/// Spark-compatible to_time: parse a string to time (nanoseconds from midnight).
/// When fail_on_error is true (to_time), returns an error for unparseable input.
/// When fail_on_error is false (try_to_time), returns null for unparseable input.
pub fn spark_to_time(args: &[ColumnarValue], fail_on_error: bool) -> Result<ColumnarValue> {
    if args.is_empty() {
        return Err(DataFusionError::Execution(
            "to_time requires at least 1 argument".to_string(),
        ));
    }

    let num_rows = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1);

    let str_arr = args[0].clone().into_array(num_rows)?;
    let str_array = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("to_time: expected String argument".to_string())
        })?;

    let len = str_array.len();
    let mut builder = Time64NanosecondArray::builder(len);

    for i in 0..len {
        if str_array.is_null(i) {
            builder.append_null();
        } else {
            let s = str_array.value(i);
            match string_to_time(s) {
                Some(nanos) => builder.append_value(nanos),
                None => {
                    if fail_on_error {
                        return Err(DataFusionError::Execution(format!(
                            "The input string '{}' cannot be parsed to a TIME value",
                            s
                        )));
                    }
                    builder.append_null();
                }
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Parse a time string to nanoseconds from midnight, matching Spark's stringToTime behavior.
/// Returns None for invalid input.
fn string_to_time(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Spark's parseTimestampString gates the T-prefix branch on j == 0 (start of
    // the trimmed string), so " T12:30" is rejected even though leading whitespace
    // is trimmed: the original segment start differs from the trimmed position.
    if trimmed.as_bytes()[0] == b'T' && s.as_bytes()[0].is_ascii_whitespace() {
        return None;
    }

    let bytes = trimmed.as_bytes();
    let num_chars = bytes.len();

    // Detect AM/PM suffix
    let (is_am, is_pm, has_suffix) = if num_chars > 2 {
        let last = bytes[num_chars - 1];
        if last == b'M' || last == b'm' {
            let second_last = bytes[num_chars - 2];
            let am = second_last == b'A' || second_last == b'a';
            let pm = second_last == b'P' || second_last == b'p';
            (am, pm, am || pm)
        } else {
            (false, false, false)
        }
    } else {
        (false, false, false)
    };

    // Strip AM/PM suffix (and optional space before it)
    let time_str = if has_suffix {
        let end = num_chars - 2;
        let s = &trimmed[..end];
        s.trim_end()
    } else {
        trimmed
    };

    // Parse the time components
    let (hour, minute, second, micros) = parse_time_components(time_str)?;

    // Validate and convert hours
    let hr = if !has_suffix {
        if hour > 23 {
            return None;
        }
        hour
    } else {
        if !(1..=12).contains(&hour) {
            return None;
        }
        if is_am {
            if hour == 12 {
                0
            } else {
                hour
            }
        } else if is_pm {
            if hour == 12 {
                12
            } else {
                hour + 12
            }
        } else {
            return None;
        }
    };

    // Validate minutes and seconds
    if minute > 59 || second > 59 {
        return None;
    }

    let nanos = hr as i64 * NANOS_PER_HOUR
        + minute as i64 * NANOS_PER_MINUTE
        + second as i64 * NANOS_PER_SECOND
        + micros as i64 * NANOS_PER_MICRO;

    Some(nanos)
}

/// Parse time components from a string like "HH:mm:ss.ffffff" or "T HH:mm:ss".
/// Returns (hour, minute, second, microseconds) or None if invalid.
fn parse_time_components(s: &str) -> Option<(i32, i32, i32, i32)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let mut pos = 0;

    // Skip optional 'T' prefix
    if bytes[pos] == b'T' {
        pos += 1;
    }

    // Parse hour
    let (hour, new_pos) = parse_digits(bytes, pos)?;
    pos = new_pos;
    if hour < 0 {
        return None;
    }

    // Expect ':'
    if pos >= bytes.len() || bytes[pos] != b':' {
        return None;
    }
    pos += 1;

    // Parse minute
    let (minute, new_pos) = parse_digits(bytes, pos)?;
    pos = new_pos;

    // Optional seconds
    if pos >= bytes.len() {
        return Some((hour, minute, 0, 0));
    }

    if bytes[pos] != b':' {
        return None;
    }
    pos += 1;

    // Parse seconds
    let (second, new_pos) = parse_digits(bytes, pos)?;
    pos = new_pos;

    // Optional fractional seconds
    if pos >= bytes.len() {
        return Some((hour, minute, second, 0));
    }

    if bytes[pos] != b'.' {
        // No more content allowed (timezone would invalidate)
        return None;
    }
    pos += 1;

    // Parse fractional seconds (up to 6 digits, pad with zeros)
    let (micros, new_pos) = parse_fractional(bytes, pos)?;
    pos = new_pos;

    // Nothing should follow the fractional seconds (timezone not allowed for time)
    if pos < bytes.len() {
        return None;
    }

    Some((hour, minute, second, micros))
}

/// Parse consecutive digits starting at pos. Returns (value, new_pos).
/// At least 1 digit is required.
fn parse_digits(bytes: &[u8], start: usize) -> Option<(i32, usize)> {
    let mut pos = start;
    let mut value: i32 = 0;
    let mut count = 0;

    while pos < bytes.len() {
        let b = bytes[pos];
        if b.is_ascii_digit() {
            value = value * 10 + (b - b'0') as i32;
            count += 1;
            pos += 1;
        } else {
            break;
        }
    }

    if count == 0 || count > 2 {
        return None;
    }

    Some((value, pos))
}

/// Parse fractional seconds (microseconds). Up to 6 digits, padded with zeros.
/// Returns (microseconds, new_pos).
fn parse_fractional(bytes: &[u8], start: usize) -> Option<(i32, usize)> {
    let mut pos = start;
    let mut value: i32 = 0;
    let mut count = 0;

    while pos < bytes.len() && count < 6 {
        let b = bytes[pos];
        if b.is_ascii_digit() {
            value = value * 10 + (b - b'0') as i32;
            count += 1;
            pos += 1;
        } else {
            break;
        }
    }

    if count == 0 {
        return None;
    }

    // Skip any remaining digits beyond 6 (truncation)
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }

    // Pad with zeros to 6 digits
    while count < 6 {
        value *= 10;
        count += 1;
    }

    Some((value, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_time_parsing() {
        // HH:mm
        assert_eq!(string_to_time("00:00"), Some(0));
        assert_eq!(
            string_to_time("12:30"),
            Some(12 * NANOS_PER_HOUR + 30 * NANOS_PER_MINUTE)
        );
        assert_eq!(
            string_to_time("23:59"),
            Some(23 * NANOS_PER_HOUR + 59 * NANOS_PER_MINUTE)
        );

        // HH:mm:ss
        assert_eq!(
            string_to_time("12:30:45"),
            Some(12 * NANOS_PER_HOUR + 30 * NANOS_PER_MINUTE + 45 * NANOS_PER_SECOND)
        );
        assert_eq!(string_to_time("00:00:00"), Some(0));
        assert_eq!(
            string_to_time("23:59:59"),
            Some(23 * NANOS_PER_HOUR + 59 * NANOS_PER_MINUTE + 59 * NANOS_PER_SECOND)
        );
    }

    #[test]
    fn test_fractional_seconds() {
        // 1 digit
        assert_eq!(
            string_to_time("00:00:00.1"),
            Some(100_000 * NANOS_PER_MICRO)
        );
        // 3 digits
        assert_eq!(
            string_to_time("00:00:00.001"),
            Some(1_000 * NANOS_PER_MICRO)
        );
        // 6 digits
        assert_eq!(string_to_time("00:00:00.000001"), Some(NANOS_PER_MICRO));
        // >6 digits truncated to microseconds
        assert_eq!(
            string_to_time("00:00:00.1234567"),
            Some(123_456 * NANOS_PER_MICRO)
        );
        // Full precision
        assert_eq!(
            string_to_time("23:59:59.999999"),
            Some(
                23 * NANOS_PER_HOUR
                    + 59 * NANOS_PER_MINUTE
                    + 59 * NANOS_PER_SECOND
                    + 999_999 * NANOS_PER_MICRO
            )
        );
    }

    #[test]
    fn test_single_digit_components() {
        // Single digit hour, minute, second
        assert_eq!(
            string_to_time("1:2:3"),
            Some(NANOS_PER_HOUR + 2 * NANOS_PER_MINUTE + 3 * NANOS_PER_SECOND)
        );
        assert_eq!(
            string_to_time("1:2:3.04"),
            Some(
                NANOS_PER_HOUR
                    + 2 * NANOS_PER_MINUTE
                    + 3 * NANOS_PER_SECOND
                    + 40_000 * NANOS_PER_MICRO
            )
        );
    }

    #[test]
    fn test_t_prefix() {
        assert_eq!(
            string_to_time("T1:02:3.04"),
            Some(
                NANOS_PER_HOUR
                    + 2 * NANOS_PER_MINUTE
                    + 3 * NANOS_PER_SECOND
                    + 40_000 * NANOS_PER_MICRO
            )
        );
        assert_eq!(
            string_to_time("T12:30:45"),
            Some(12 * NANOS_PER_HOUR + 30 * NANOS_PER_MINUTE + 45 * NANOS_PER_SECOND)
        );
    }

    #[test]
    fn test_am_pm() {
        // 12:00:00 AM = midnight
        assert_eq!(string_to_time("12:00:00 AM"), Some(0));
        // 1:00:00 AM
        assert_eq!(string_to_time("1:00:00 AM"), Some(NANOS_PER_HOUR));
        // 11:59:59 AM
        assert_eq!(
            string_to_time("11:59:59 AM"),
            Some(11 * NANOS_PER_HOUR + 59 * NANOS_PER_MINUTE + 59 * NANOS_PER_SECOND)
        );
        // 12:00:00 PM = noon
        assert_eq!(string_to_time("12:00:00 PM"), Some(12 * NANOS_PER_HOUR));
        // 1:00:00 PM = 13:00
        assert_eq!(string_to_time("1:00:00 PM"), Some(13 * NANOS_PER_HOUR));
        // 11:59:59 PM = 23:59:59
        assert_eq!(
            string_to_time("11:59:59 PM"),
            Some(23 * NANOS_PER_HOUR + 59 * NANOS_PER_MINUTE + 59 * NANOS_PER_SECOND)
        );
        // Case insensitive
        assert_eq!(string_to_time("12:00:00 am"), Some(0));
        assert_eq!(string_to_time("12:00:00 pm"), Some(12 * NANOS_PER_HOUR));
        // No space before AM/PM
        assert_eq!(string_to_time("12:00:00AM"), Some(0));
        assert_eq!(string_to_time("1:00:00PM"), Some(13 * NANOS_PER_HOUR));
        // With fractional seconds
        assert_eq!(
            string_to_time("12:59:59.999999 PM"),
            Some(
                12 * NANOS_PER_HOUR
                    + 59 * NANOS_PER_MINUTE
                    + 59 * NANOS_PER_SECOND
                    + 999_999 * NANOS_PER_MICRO
            )
        );
    }

    #[test]
    fn test_invalid_am_pm() {
        // Hour 0 invalid in 12-hour format
        assert_eq!(string_to_time("0:00:00 AM"), None);
        // Hour 13 invalid in 12-hour format
        assert_eq!(string_to_time("13:00:00 AM"), None);
        assert_eq!(string_to_time("13:00:00 PM"), None);
    }

    #[test]
    fn test_invalid_inputs() {
        assert_eq!(string_to_time(""), None);
        assert_eq!(string_to_time(" "), None);
        assert_eq!(string_to_time("XYZ"), None);
        assert_eq!(string_to_time("24:00:00"), None);
        assert_eq!(string_to_time("23:60:00"), None);
        assert_eq!(string_to_time("23:00:60"), None);
        // Date component present
        assert_eq!(string_to_time("2025-03-09 00:00:00"), None);
        // Timezone present
        assert_eq!(string_to_time("00:01:02 UTC"), None);
        // Just digits without separators
        assert_eq!(string_to_time("120000"), None);
    }

    #[test]
    fn test_trailing_whitespace() {
        assert_eq!(string_to_time("12:30:45  "), string_to_time("12:30:45"));
        assert_eq!(string_to_time("1:00:00 AM  "), string_to_time("1:00:00 AM"));
    }

    #[test]
    fn test_three_digit_components() {
        // 3-digit hour/minute/second must be rejected (Spark requires 1-2 digits)
        assert_eq!(string_to_time("001:02:03"), None);
        assert_eq!(string_to_time("12:001:03"), None);
        assert_eq!(string_to_time("12:02:003"), None);
    }

    #[test]
    fn test_leading_whitespace() {
        assert_eq!(string_to_time("  12:30"), string_to_time("12:30"));
        assert_eq!(string_to_time(" 12:30:45"), string_to_time("12:30:45"));
        assert_eq!(string_to_time(" 12:30:45 "), string_to_time("12:30:45"));
        assert_eq!(string_to_time("  1:00:00 AM"), string_to_time("1:00:00 AM"));
        // Tabs and newlines are also trimmed (Spark's isWhitespaceOrISOControl)
        assert_eq!(string_to_time("\t12:30:45"), string_to_time("12:30:45"));
        assert_eq!(string_to_time("\n12:30:45"), string_to_time("12:30:45"));
        // T-prefix is rejected when preceded by whitespace because Spark's
        // parseTimestampString gates the T-prefix branch on j == 0 (start of
        // the already-trimmed segment), so leading whitespace moves j past 0.
        assert_eq!(string_to_time("  T12:30:45"), None);
        assert_eq!(string_to_time(" T12:30"), None);
    }
}
