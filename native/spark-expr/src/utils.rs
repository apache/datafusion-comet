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

use arrow::datatypes::{DataType, TimeUnit, DECIMAL128_MAX_PRECISION};
use arrow::{
    array::{
        cast::as_primitive_array,
        types::{Int32Type, TimestampMicrosecondType},
        BooleanBufferBuilder,
    },
    buffer::BooleanBuffer,
};
use datafusion::logical_expr::EmitTo;
use std::borrow::Cow;
use std::sync::Arc;

use crate::timezone::Tz;
use arrow::array::types::TimestampMillisecondType;
use arrow::array::TimestampMicrosecondArray;
use arrow::datatypes::{MAX_DECIMAL128_FOR_EACH_PRECISION, MIN_DECIMAL128_FOR_EACH_PRECISION};
use arrow::error::ArrowError;
use arrow::{
    array::{as_dictionary_array, Array, ArrayRef, PrimitiveArray},
    temporal_conversions::as_datetime,
};
use chrono::{DateTime, LocalResult, NaiveDateTime, Offset, TimeZone};

/// Preprocesses input arrays to add timezone information from Spark to Arrow array datatype or
/// to apply timezone offset.
//
//  We consider the following cases:
//
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Conversion            | Input array  | Timezone          | Output array                     |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp ->          | Array in UTC | Timezone of input | A timestamp with the timezone    |
//  |  Utf8 or Date32       |              |                   | offset applied and timezone      |
//  |                       |              |                   | removed                          |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp ->          | Array in UTC | Timezone of input | Same as input array              |
//  |  Timestamp  w/Timezone|              |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp_ntz ->      | Array in     | Timezone of input | Same as input array              |
//  |   Utf8 or Date32      | timezone     |                   |                                  |
//  |                       | session local|                   |                                  |
//  |                       | timezone     |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp_ntz ->      | Array in     | Timezone of input |  Array in UTC and timezone       |
//  |  Timestamp w/Timezone | session local|                   |  specified in input              |
//  |                       | timezone     |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp(_ntz) ->    |                                                                     |
//  |        Any other type |              Not Supported                                          |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//
pub fn array_with_timezone(
    array: ArrayRef,
    timezone: String,
    to_type: Option<&DataType>,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            assert!(!timezone.is_empty());
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => Ok(array),
                Some(DataType::Timestamp(_, Some(target_tz))) => {
                    // Interpret NTZ as local time in session TZ; annotate output with target TZ
                    // so the result has the exact annotation the caller expects.
                    timestamp_ntz_to_timestamp(array, timezone.as_str(), Some(target_tz.as_ref()))
                }
                Some(DataType::Timestamp(TimeUnit::Microsecond, None)) => {
                    // Convert from Timestamp(Millisecond, None) to Timestamp(Microsecond, None)
                    let millis_array = as_primitive_array::<TimestampMillisecondType>(&array);
                    let micros_array: TimestampMicrosecondArray =
                        arrow::compute::kernels::arity::unary(millis_array, |v| v * 1000);
                    Ok(Arc::new(micros_array))
                }
                _ => {
                    // Not supported
                    Err(ArrowError::CastError(format!(
                        "Cannot convert from {:?} to {:?}",
                        array.data_type(),
                        to_type.unwrap()
                    )))
                }
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            assert!(!timezone.is_empty());
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => Ok(array),
                Some(DataType::Timestamp(_, Some(target_tz))) => {
                    timestamp_ntz_to_timestamp(array, timezone.as_str(), Some(target_tz.as_ref()))
                }
                _ => {
                    // Not supported
                    Err(ArrowError::CastError(format!(
                        "Cannot convert from {:?} to {:?}",
                        array.data_type(),
                        to_type.unwrap()
                    )))
                }
            }
        }
        DataType::Timestamp(_, None) => {
            assert!(!timezone.is_empty());
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => Ok(array),
                Some(DataType::Timestamp(_, Some(target_tz))) => {
                    timestamp_ntz_to_timestamp(array, timezone.as_str(), Some(target_tz.as_ref()))
                }
                _ => {
                    // Not supported
                    Err(ArrowError::CastError(format!(
                        "Cannot convert from {:?} to {:?}",
                        array.data_type(),
                        to_type.unwrap()
                    )))
                }
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            assert!(!timezone.is_empty());
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let array_with_timezone = array.clone().with_timezone(timezone.clone());
            let array = Arc::new(array_with_timezone) as ArrayRef;
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => {
                    pre_timestamp_cast(array, timezone)
                }
                _ => Ok(array),
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
            assert!(!timezone.is_empty());
            let array = as_primitive_array::<TimestampMillisecondType>(&array);
            let array_with_timezone = array.clone().with_timezone(timezone.clone());
            let array = Arc::new(array_with_timezone) as ArrayRef;
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => {
                    pre_timestamp_cast(array, timezone)
                }
                _ => Ok(array),
            }
        }
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            let dict = as_dictionary_array::<Int32Type>(&array);
            let array = as_primitive_array::<TimestampMicrosecondType>(dict.values());
            let array_with_timezone =
                array_with_timezone(Arc::new(array.clone()) as ArrayRef, timezone, to_type)?;
            let dict = dict.with_values(array_with_timezone);
            Ok(Arc::new(dict))
        }
        _ => Ok(array),
    }
}

fn datetime_cast_err(value: i64) -> ArrowError {
    ArrowError::CastError(format!(
        "Cannot convert TimestampMicrosecondType {value} to datetime. Comet only supports dates between Jan 1, 262145 BCE and Dec 31, 262143 CE",
    ))
}

/// Resolves a local datetime in the given timezone to an absolute DateTime,
/// handling DST ambiguity and spring-forward gaps.
/// Parameters:
///     tz - timezone used to interpret local_datetime
///     local_datetime - a naive local datetime to resolve
pub(crate) fn resolve_local_datetime(tz: &Tz, local_datetime: NaiveDateTime) -> DateTime<Tz> {
    match tz.from_local_datetime(&local_datetime) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(dt, _) => dt,
        LocalResult::None => {
            // Determine offset before time-change
            let probe = local_datetime - chrono::Duration::hours(3);
            let pre_offset = match tz.from_local_datetime(&probe) {
                LocalResult::Single(dt) => dt.offset().fix(),
                LocalResult::Ambiguous(dt, _) => dt.offset().fix(),
                LocalResult::None => {
                    // Cannot determine offset; fall back to UTC interpretation
                    return local_datetime.and_utc().with_timezone(tz);
                }
            };
            let offset_secs = pre_offset.local_minus_utc() as i64;

            let utc_naive = local_datetime - chrono::Duration::seconds(offset_secs);
            utc_naive.and_utc().with_timezone(tz)
        }
    }
}

/// Takes in a Timestamp(Microsecond, None) array and a timezone id, and returns
/// a Timestamp(Microsecond, Some<_>) array.
/// The understanding is that the input array has time in the timezone specified in the second
/// argument.
/// Parameters:
///     array - input array of timestamp without timezone
///     tz - timezone of the values in the input array
///     to_timezone - timezone to change the input values to
pub(crate) fn timestamp_ntz_to_timestamp(
    array: ArrayRef,
    tz: &str,
    to_timezone: Option<&str>,
) -> Result<ArrayRef, ArrowError> {
    assert!(!tz.is_empty());
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let tz: Tz = tz.parse()?;
            let array: PrimitiveArray<TimestampMicrosecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMicrosecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|local_datetime| {
                        let datetime = resolve_local_datetime(&tz, local_datetime);

                        datetime.timestamp_micros()
                    })
            })?;
            let array_with_tz = if let Some(to_tz) = to_timezone {
                array.with_timezone(to_tz)
            } else {
                array
            };
            Ok(Arc::new(array_with_tz))
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = as_primitive_array::<TimestampMillisecondType>(&array);
            let tz: Tz = tz.parse()?;
            let array: PrimitiveArray<TimestampMillisecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMillisecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|local_datetime| {
                        let datetime = resolve_local_datetime(&tz, local_datetime);

                        datetime.timestamp_millis()
                    })
            })?;
            let array_with_tz = if let Some(to_tz) = to_timezone {
                array.with_timezone(to_tz)
            } else {
                array
            };
            Ok(Arc::new(array_with_tz))
        }
        _ => Ok(array),
    }
}

/// Converts a `Timestamp(Microsecond, Some(_))` array to `Timestamp(Microsecond, None)`
/// (TIMESTAMP_NTZ) by interpreting the UTC epoch value in the given session timezone and
/// storing the resulting local datetime as epoch-relative microseconds without a TZ annotation.
///
/// Matches Spark: `convertTz(ts, ZoneOffset.UTC, zoneId)`
pub(crate) fn cast_timestamp_to_ntz(
    array: ArrayRef,
    timezone: &str,
) -> Result<ArrayRef, ArrowError> {
    assert!(!timezone.is_empty());
    let tz: Tz = timezone.parse()?;
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let result: PrimitiveArray<TimestampMicrosecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMicrosecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|utc_naive| {
                        // Convert UTC naive datetime → local datetime in session TZ
                        let local_dt = tz.from_utc_datetime(&utc_naive);
                        // Re-encode as epoch-relative μs treating local time as UTC anchor.
                        // This produces the NTZ representation (no offset applied).
                        local_dt.naive_local().and_utc().timestamp_micros()
                    })
            })?;
            // No timezone annotation on output = TIMESTAMP_NTZ
            Ok(Arc::new(result))
        }
        _ => Err(ArrowError::CastError(format!(
            "cast_timestamp_to_ntz: unexpected input type {:?}",
            array.data_type()
        ))),
    }
}

/// This takes for special pre-casting cases of Spark. E.g., Timestamp to String.
fn pre_timestamp_cast(array: ArrayRef, timezone: String) -> Result<ArrayRef, ArrowError> {
    assert!(!timezone.is_empty());
    match array.data_type() {
        DataType::Timestamp(_, _) => {
            // Spark doesn't output timezone while casting timestamp to string, but arrow's cast
            // kernel does if timezone exists. So we need to apply offset of timezone to array
            // timestamp value and remove timezone from array datatype.
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);

            let tz: Tz = timezone.parse()?;
            let array: PrimitiveArray<TimestampMicrosecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMicrosecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|datetime| {
                        let offset = tz.offset_from_utc_datetime(&datetime).fix();
                        let datetime = datetime + offset;
                        datetime.and_utc().timestamp_micros()
                    })
            })?;

            Ok(Arc::new(array))
        }
        _ => Ok(array),
    }
}

/// Adapted from arrow-rs `validate_decimal_precision` but returns bool
/// instead of Err to avoid the cost of formatting the error strings and is
/// optimized to remove a memcpy that exists in the original function
/// we can remove this code once we upgrade to a version of arrow-rs that
/// includes https://github.com/apache/arrow-rs/pull/6419
#[inline]
pub fn is_valid_decimal_precision(value: i128, precision: u8) -> bool {
    precision <= DECIMAL128_MAX_PRECISION
        && value >= MIN_DECIMAL128_FOR_EACH_PRECISION[precision as usize]
        && value <= MAX_DECIMAL128_FOR_EACH_PRECISION[precision as usize]
}

/// Build a boolean buffer from the state and reset the state, based on the emit_to
/// strategy.
pub fn build_bool_state(state: &mut BooleanBufferBuilder, emit_to: &EmitTo) -> BooleanBuffer {
    let bool_state: BooleanBuffer = state.finish();

    match emit_to {
        EmitTo::All => bool_state,
        EmitTo::First(n) => {
            state.append_buffer(&bool_state.slice(*n, bool_state.len() - n));
            bool_state.slice(0, *n)
        }
    }
}

// These are borrowed from hashbrown crate:
//   https://github.com/rust-lang/hashbrown/blob/master/src/raw/mod.rs

// On stable we can use #[cold] to get a equivalent effect: this attributes
// suggests that the function is unlikely to be called
#[inline]
#[cold]
pub fn cold() {}

#[inline]
pub fn likely(b: bool) -> bool {
    if !b {
        cold();
    }
    b
}
#[inline]
pub fn unlikely(b: bool) -> bool {
    if b {
        cold();
    }
    b
}

/// Decode `bytes` as UTF-8 the way Spark renders `StringType` -- `new String(bytes, UTF_8)` on the
/// JVM -- replacing each ill-formed sequence with a single `U+FFFD` and skipping the same number of
/// bytes the JDK's UTF-8 `CharsetDecoder` (action REPLACE) would. Valid UTF-8 is returned as a
/// zero-cost borrow.
///
/// This intentionally differs from `str::from_utf8_lossy` for surrogate-range three-byte sequences
/// (`ED A0..BF ..`, e.g. CESU-8 / Java modified-UTF-8 supplementary chars) and for some other
/// ill-formed multi-byte units: `from_utf8_lossy` follows the Unicode "maximal subpart" rule and
/// can emit one `U+FFFD` per byte, whereas the JDK collapses certain ill-formed units into a single
/// `U+FFFD`. Matching the JDK byte-for-byte means Comet renders arbitrary bytes identically to
/// Spark -- whether they arrive via a columnar shuffle or a `CAST(binary AS string)`. The
/// per-class malformed lengths below (E0/ED overlong & surrogate handling, F0/F4 range checks)
/// match the observable replacement behavior of the JDK UTF-8 decoder; they were determined from
/// observed `new String(bytes, UTF_8)` output, not by reviewing the OpenJDK source.
pub fn decode_utf8_spark_lossy(bytes: &[u8]) -> Cow<'_, str> {
    // Fast path: well-formed UTF-8 borrows with zero copy (the overwhelmingly common case).
    if let Ok(s) = std::str::from_utf8(bytes) {
        return Cow::Borrowed(s);
    }

    const RC: char = '\u{FFFD}';
    let n = bytes.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    while i < n {
        let b1 = bytes[i];
        if b1 < 0x80 {
            out.push(b1 as char);
            i += 1;
        } else if (0xC2..=0xDF).contains(&b1) {
            // 2-byte lead. Bad/absent continuation -> single FFFD, skip 1.
            if i + 1 < n && (bytes[i + 1] & 0xC0) == 0x80 {
                let cp = (((b1 as u32) & 0x1F) << 6) | ((bytes[i + 1] as u32) & 0x3F);
                out.push(char::from_u32(cp).unwrap());
                i += 2;
            } else {
                out.push(RC);
                i += 1;
            }
        } else if (0xE0..=0xEF).contains(&b1) {
            // 3-byte lead.
            if i + 1 >= n {
                out.push(RC); // truncated lead at EOF
                i = n;
            } else {
                let b2 = bytes[i + 1];
                if (b1 == 0xE0 && (b2 & 0xE0) == 0x80) || (b2 & 0xC0) != 0x80 {
                    // overlong (E0 80..9F) or b2 not a continuation -> skip 1
                    out.push(RC);
                    i += 1;
                } else if i + 2 >= n {
                    out.push(RC); // truncated after a valid b2 at EOF
                    i = n;
                } else {
                    let b3 = bytes[i + 2];
                    if (b3 & 0xC0) != 0x80 {
                        out.push(RC); // b3 not a continuation -> skip 2
                        i += 2;
                    } else {
                        let cp = (((b1 as u32) & 0x0F) << 12)
                            | (((b2 as u32) & 0x3F) << 6)
                            | ((b3 as u32) & 0x3F);
                        if (0xD800..=0xDFFF).contains(&cp) {
                            // surrogate (e.g. ED A0 80) -> JDK skips all 3, single FFFD
                            out.push(RC);
                            i += 3;
                        } else {
                            out.push(char::from_u32(cp).unwrap());
                            i += 3;
                        }
                    }
                }
            }
        } else if (0xF0..=0xF4).contains(&b1) {
            // 4-byte lead.
            if i + 1 >= n {
                out.push(RC);
                i = n;
            } else {
                let b2 = bytes[i + 1];
                if (b1 == 0xF0 && !(0x90..=0xBF).contains(&b2))
                    || (b1 == 0xF4 && (b2 & 0xF0) != 0x80)
                    || (b2 & 0xC0) != 0x80
                {
                    out.push(RC); // bad b2 -> skip 1
                    i += 1;
                } else if i + 2 >= n {
                    out.push(RC);
                    i = n;
                } else if (bytes[i + 2] & 0xC0) != 0x80 {
                    out.push(RC); // b3 not a continuation -> skip 2
                    i += 2;
                } else if i + 3 >= n {
                    out.push(RC);
                    i = n;
                } else if (bytes[i + 3] & 0xC0) != 0x80 {
                    out.push(RC); // b4 not a continuation -> skip 3
                    i += 3;
                } else {
                    let cp = (((b1 as u32) & 0x07) << 18)
                        | (((b2 as u32) & 0x3F) << 12)
                        | (((bytes[i + 2] as u32) & 0x3F) << 6)
                        | ((bytes[i + 3] as u32) & 0x3F);
                    out.push(char::from_u32(cp).unwrap());
                    i += 4;
                }
            }
        } else {
            // Lone continuation (0x80..0xBF), overlong 2-byte leads (0xC0/0xC1), or out-of-range
            // 4-byte leads (0xF5..0xFF): each is a single ill-formed byte -> skip 1.
            out.push(RC);
            i += 1;
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn array_containing(local_datetime: &str) -> ArrayRef {
        let dt = NaiveDateTime::parse_from_str(local_datetime, "%Y-%m-%d %H:%M:%S").unwrap();
        let ts = dt.and_utc().timestamp_micros();
        Arc::new(TimestampMicrosecondArray::from(vec![ts]))
    }

    fn micros_for(datetime: &str) -> i64 {
        NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    /// Oracle = JDK 17 `new String(bytes, StandardCharsets.UTF_8)` (the renderer Spark uses for
    /// StringType). Each row's expected output was verified against the JVM. The decoder must match
    /// it byte-for-byte -- including the surrogate-range case where `str::from_utf8_lossy` differs.
    #[test]
    fn decode_utf8_spark_lossy_matches_jvm_replacement_granularity() {
        let cases: &[(&[u8], &str)] = &[
            (&[0xFF, 0xFE, 0x41], "\u{FFFD}\u{FFFD}A"),
            (&[0x80, 0x42], "\u{FFFD}B"),
            (&[0xE0, 0x80], "\u{FFFD}\u{FFFD}"),
            (&[0xF0, 0x80, 0x80, 0x41], "\u{FFFD}\u{FFFD}\u{FFFD}A"),
            (&[0xC0, 0xAF], "\u{FFFD}\u{FFFD}"),
            // The parity case: Rust's from_utf8_lossy would give three U+FFFD here.
            (&[0xED, 0xA0, 0x80], "\u{FFFD}"),
            (
                &[0xF4, 0x90, 0x80, 0x80],
                "\u{FFFD}\u{FFFD}\u{FFFD}\u{FFFD}",
            ),
        ];
        for (bytes, expected) in cases {
            assert_eq!(
                decode_utf8_spark_lossy(bytes),
                *expected,
                "bytes {bytes:02x?} should render like the JVM"
            );
        }
    }

    #[test]
    fn decode_utf8_spark_lossy_valid_utf8_is_borrowed_zero_copy() {
        let s = "café — 日本語 🦀";
        match decode_utf8_spark_lossy(s.as_bytes()) {
            Cow::Borrowed(b) => assert_eq!(b, s),
            Cow::Owned(_) => panic!("valid UTF-8 must borrow, not allocate"),
        }
    }

    #[test]
    fn decode_utf8_spark_lossy_valid_multibyte_around_invalid_bytes_decodes() {
        // 'a' | é (C3 A9) | stray 0xFF | 'b' | 🦀 (F0 9F A6 80) -> valid chars preserved, one FFFD.
        let mut bytes = vec![b'a'];
        bytes.extend_from_slice("é".as_bytes());
        bytes.push(0xFF);
        bytes.push(b'b');
        bytes.extend_from_slice("🦀".as_bytes());
        assert_eq!(decode_utf8_spark_lossy(&bytes), "aé\u{FFFD}b🦀");
    }

    #[test]
    fn test_build_bool_state() {
        let mut builder = BooleanBufferBuilder::new(0);
        builder.append_packed_range(0..16, &[0x42u8, 0x39u8]);

        let mut first_nine = BooleanBufferBuilder::new(0);
        first_nine.append_packed_range(0..9, &[0x42u8, 0x01u8]);
        let first_nine = first_nine.finish();
        let mut last = BooleanBufferBuilder::new(0);
        last.append_packed_range(0..7, &[0x1cu8]);
        let last = last.finish();

        assert_eq!(
            first_nine,
            build_bool_state(&mut builder, &EmitTo::First(9))
        );
        assert_eq!(last, build_bool_state(&mut builder, &EmitTo::All));
    }

    #[test]
    fn test_timestamp_ntz_to_timestamp_handles_non_existent_time() {
        let output = timestamp_ntz_to_timestamp(
            array_containing("2024-03-31 01:30:00"),
            "Europe/London",
            None,
        )
        .unwrap();

        assert_eq!(
            as_primitive_array::<TimestampMicrosecondType>(&output).value(0),
            micros_for("2024-03-31 01:30:00")
        );
    }

    #[test]
    fn test_timestamp_ntz_to_timestamp_handles_ambiguous_time() {
        let output = timestamp_ntz_to_timestamp(
            array_containing("2024-10-27 01:30:00"),
            "Europe/London",
            None,
        )
        .unwrap();

        assert_eq!(
            as_primitive_array::<TimestampMicrosecondType>(&output).value(0),
            micros_for("2024-10-27 00:30:00")
        );
    }

    // Helper: build a Timestamp(Microsecond, Some(tz)) array from a UTC datetime string
    fn ts_with_tz(utc_datetime: &str, tz: &str) -> ArrayRef {
        let dt = NaiveDateTime::parse_from_str(utc_datetime, "%Y-%m-%d %H:%M:%S").unwrap();
        let ts = dt.and_utc().timestamp_micros();
        Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone(tz.to_string()))
    }

    #[test]
    fn test_cast_timestamp_to_ntz_utc() {
        // In UTC, local time == UTC time, so NTZ value == UTC epoch value
        let input = ts_with_tz("2024-01-15 10:30:00", "UTC");
        let result = cast_timestamp_to_ntz(input, "UTC").unwrap();
        let out = as_primitive_array::<TimestampMicrosecondType>(&result);
        // Expected NTZ value: epoch μs for "2024-01-15 10:30:00" as if it were UTC
        let expected = NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(out.value(0), expected);
        assert_eq!(out.timezone(), None); // no TZ annotation = NTZ
    }

    #[test]
    fn test_cast_timestamp_to_ntz_offset_timezone() {
        // UTC epoch for "2024-01-15 15:30:00 UTC" cast to NTZ with session TZ = America/New_York (UTC-5)
        // Local time in NY = 10:30:00 → NTZ should store epoch μs for "2024-01-15 10:30:00"
        let input = ts_with_tz("2024-01-15 15:30:00", "UTC");
        let result = cast_timestamp_to_ntz(input, "America/New_York").unwrap();
        let out = as_primitive_array::<TimestampMicrosecondType>(&result);
        let expected = NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(out.value(0), expected);
        assert_eq!(out.timezone(), None);
    }

    #[test]
    fn test_cast_timestamp_to_ntz_dst() {
        // During DST: UTC epoch for "2024-07-04 16:30:00 UTC", session TZ = America/New_York (UTC-4 in summer)
        // Local time in NY = 12:30:00 → NTZ stores epoch μs for "2024-07-04 12:30:00"
        let input = ts_with_tz("2024-07-04 16:30:00", "UTC");
        let result = cast_timestamp_to_ntz(input, "America/New_York").unwrap();
        let out = as_primitive_array::<TimestampMicrosecondType>(&result);
        let expected = NaiveDateTime::parse_from_str("2024-07-04 12:30:00", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(out.value(0), expected);
    }
}
