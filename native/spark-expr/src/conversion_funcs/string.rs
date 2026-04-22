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

use crate::{timezone, EvalMode, SparkError, SparkResult};
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Builder, GenericStringArray,
    OffsetSizeTrait, PrimitiveArray, PrimitiveBuilder, StringArray,
};
use arrow::compute::DecimalCast;
use arrow::datatypes::{
    i256, is_validate_decimal_precision, DataType, Date32Type, Decimal256Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
};
use chrono::{DateTime, LocalResult, NaiveDate, NaiveTime, Offset, TimeZone, Timelike};
use num::traits::CheckedNeg;
use num::{CheckedSub, Integer};
use regex::Regex;
use std::num::Wrapping;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

macro_rules! cast_utf8_to_timestamp {
    // $tz is a Timezone:Tz object and contains the session timezone.
    // $to_tz_str is a string containing the to_type timezone
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident, $tz:expr, $to_tz_str:expr, $is_spark4_plus:expr) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len).with_timezone($to_tz_str);
        let mut cast_err: Option<SparkError> = None;
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else {
                // we use trim_end instead of trim because strings with leading spaces are interpreted differently
                // by Spark in cases where the string has only the time component starting with T.
                // The string " T2" results in null while "T2" results in a valid timestamp.
                match $cast_method($array.value(i).trim_end(), $eval_mode, $tz, $is_spark4_plus) {
                    Ok(Some(cast_value)) => cast_array.append_value(cast_value),
                    Ok(None) => cast_array.append_null(),
                    Err(e) => {
                        if $eval_mode == EvalMode::Ansi {
                            // Replace the error value with the raw (untrimmed) input to match
                            // Spark's behavior: Spark reports the original string in CAST_INVALID_INPUT.
                            let raw_value = $array.value(i).to_string();
                            let e = match e {
                                SparkError::InvalidInputInCastToDatetime {
                                    from_type,
                                    to_type,
                                    ..
                                } => SparkError::InvalidInputInCastToDatetime {
                                    value: raw_value,
                                    from_type,
                                    to_type,
                                },
                                other => other,
                            };
                            cast_err = Some(e);
                            break;
                        }
                        cast_array.append_null()
                    }
                }
            }
        }
        if let Some(e) = cast_err {
            Err(e)
        } else {
            Ok(Arc::new(cast_array.finish()) as ArrayRef)
        }
    }};
}

macro_rules! cast_utf8_to_timestamp_ntz {
    ($array:expr, $eval_mode:expr, $cast_method:ident, $allow_tz:expr, $is_spark4_plus:expr) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<TimestampMicrosecondType>::builder(len);
        let mut cast_err: Option<SparkError> = None;
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else {
                match $cast_method(
                    $array.value(i).trim_end(),
                    $eval_mode,
                    $allow_tz,
                    $is_spark4_plus,
                ) {
                    Ok(Some(cast_value)) => cast_array.append_value(cast_value),
                    Ok(None) => cast_array.append_null(),
                    Err(e) => {
                        if $eval_mode == EvalMode::Ansi {
                            let raw_value = $array.value(i).to_string();
                            let e = match e {
                                SparkError::InvalidInputInCastToDatetime {
                                    from_type,
                                    to_type,
                                    ..
                                } => SparkError::InvalidInputInCastToDatetime {
                                    value: raw_value,
                                    from_type,
                                    to_type,
                                },
                                other => other,
                            };
                            cast_err = Some(e);
                            break;
                        }
                        cast_array.append_null()
                    }
                }
            }
        }
        if let Some(e) = cast_err {
            Err(e)
        } else {
            Ok(Arc::new(cast_array.finish()) as ArrayRef)
        }
    }};
}

macro_rules! cast_utf8_to_int {
    ($array:expr, $array_type:ty, $parse_fn:expr) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        let parse_fn = $parse_fn;
        if $array.null_count() == 0 {
            for i in 0..len {
                if let Some(cast_value) = parse_fn($array.value(i))? {
                    cast_array.append_value(cast_value);
                } else {
                    cast_array.append_null()
                }
            }
        } else {
            for i in 0..len {
                if $array.is_null(i) {
                    cast_array.append_null()
                } else if let Some(cast_value) = parse_fn($array.value(i))? {
                    cast_array.append_value(cast_value);
                } else {
                    cast_array.append_null()
                }
            }
        }
        let result: SparkResult<ArrayRef> = Ok(Arc::new(cast_array.finish()) as ArrayRef);
        result
    }};
}

struct TimeStampInfo {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    microsecond: u32,
}

impl Default for TimeStampInfo {
    fn default() -> Self {
        TimeStampInfo {
            year: 1,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
        }
    }
}

impl TimeStampInfo {
    fn with_year(&mut self, year: i32) -> &mut Self {
        self.year = year;
        self
    }

    fn with_month(&mut self, month: u32) -> &mut Self {
        self.month = month;
        self
    }

    fn with_day(&mut self, day: u32) -> &mut Self {
        self.day = day;
        self
    }

    fn with_hour(&mut self, hour: u32) -> &mut Self {
        self.hour = hour;
        self
    }

    fn with_minute(&mut self, minute: u32) -> &mut Self {
        self.minute = minute;
        self
    }

    fn with_second(&mut self, second: u32) -> &mut Self {
        self.second = second;
        self
    }

    fn with_microsecond(&mut self, microsecond: u32) -> &mut Self {
        self.microsecond = microsecond;
        self
    }
}

pub(crate) fn is_df_cast_from_string_spark_compatible(to_type: &DataType) -> bool {
    matches!(to_type, DataType::Binary)
}

pub(crate) fn cast_string_to_float(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    match to_type {
        DataType::Float32 => cast_string_to_float_impl::<Float32Type>(array, eval_mode, "FLOAT"),
        DataType::Float64 => cast_string_to_float_impl::<Float64Type>(array, eval_mode, "DOUBLE"),
        _ => Err(SparkError::Internal(format!(
            "Unsupported cast to float type: {:?}",
            to_type
        ))),
    }
}

fn cast_string_to_float_impl<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    eval_mode: EvalMode,
    type_name: &str,
) -> SparkResult<ArrayRef>
where
    T::Native: FromStr + num::Float,
{
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SparkError::Internal("Expected string array".to_string()))?;

    let mut builder = PrimitiveBuilder::<T>::with_capacity(arr.len());

    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let str_value = arr.value(i).trim();
            match parse_string_to_float(str_value) {
                Some(v) => builder.append_value(v),
                None => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(invalid_value(arr.value(i), "STRING", type_name));
                    }
                    builder.append_null();
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// helper to parse floats from string inputs
fn parse_string_to_float<F>(s: &str) -> Option<F>
where
    F: FromStr + num::Float,
{
    // Handle +inf / -inf
    if s.eq_ignore_ascii_case("inf")
        || s.eq_ignore_ascii_case("+inf")
        || s.eq_ignore_ascii_case("infinity")
        || s.eq_ignore_ascii_case("+infinity")
    {
        return Some(F::infinity());
    }
    if s.eq_ignore_ascii_case("-inf") || s.eq_ignore_ascii_case("-infinity") {
        return Some(F::neg_infinity());
    }
    if s.eq_ignore_ascii_case("nan") {
        return Some(F::nan());
    }
    // Remove D/F suffix if present
    let pruned_float_str =
        if s.ends_with("d") || s.ends_with("D") || s.ends_with('f') || s.ends_with('F') {
            &s[..s.len() - 1]
        } else {
            s
        };
    // Rust's parse logic already handles scientific notations so we just rely on it
    pruned_float_str.parse::<F>().ok()
}

pub(crate) fn spark_cast_utf8_to_boolean<OffsetSize>(
    from: &dyn Array,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    let array = from
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .unwrap();

    let output_array = array
        .iter()
        .map(|value| match value {
            Some(value) => match value.to_ascii_lowercase().trim() {
                "t" | "true" | "y" | "yes" | "1" => Ok(Some(true)),
                "f" | "false" | "n" | "no" | "0" => Ok(Some(false)),
                _ if eval_mode == EvalMode::Ansi => Err(SparkError::CastInvalidValue {
                    value: value.to_string(),
                    from_type: "STRING".to_string(),
                    to_type: "BOOLEAN".to_string(),
                }),
                _ => Ok(None),
            },
            _ => Ok(None),
        })
        .collect::<Result<BooleanArray, _>>()?;

    Ok(Arc::new(output_array))
}

pub(crate) fn cast_string_to_decimal(
    array: &ArrayRef,
    to_type: &DataType,
    precision: &u8,
    scale: &i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    match to_type {
        DataType::Decimal128(_, _) => {
            cast_string_to_decimal128_impl(array, eval_mode, *precision, *scale)
        }
        DataType::Decimal256(_, _) => {
            cast_string_to_decimal256_impl(array, eval_mode, *precision, *scale)
        }
        _ => Err(SparkError::Internal(format!(
            "Unexpected type in cast_string_to_decimal: {:?}",
            to_type
        ))),
    }
}

fn cast_string_to_decimal128_impl(
    array: &ArrayRef,
    eval_mode: EvalMode,
    precision: u8,
    scale: i8,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SparkError::Internal("Expected string array".to_string()))?;

    let mut decimal_builder = Decimal128Builder::with_capacity(string_array.len());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            decimal_builder.append_null();
        } else {
            let str_value = string_array.value(i);
            match parse_string_to_decimal(str_value, precision, scale) {
                Ok(Some(decimal_value)) => {
                    decimal_builder.append_value(decimal_value);
                }
                Ok(None) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(invalid_value(
                            string_array.value(i),
                            "STRING",
                            &format!("DECIMAL({},{})", precision, scale),
                        ));
                    }
                    decimal_builder.append_null();
                }
                Err(e) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(e);
                    }
                    decimal_builder.append_null();
                }
            }
        }
    }

    Ok(Arc::new(
        decimal_builder
            .with_precision_and_scale(precision, scale)
            .map_err(|e| {
                if matches!(e, arrow::error::ArrowError::InvalidArgumentError(_))
                    && e.to_string().contains("too large to store in a Decimal128")
                {
                    // Fallback error handling
                    SparkError::NumericValueOutOfRange {
                        value: "overflow".to_string(),
                        precision,
                        scale,
                    }
                } else {
                    SparkError::Arrow(Arc::new(e))
                }
            })?
            .finish(),
    ))
}

fn cast_string_to_decimal256_impl(
    array: &ArrayRef,
    eval_mode: EvalMode,
    precision: u8,
    scale: i8,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SparkError::Internal("Expected string array".to_string()))?;

    let mut decimal_builder = PrimitiveBuilder::<Decimal256Type>::with_capacity(string_array.len());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            decimal_builder.append_null();
        } else {
            let str_value = string_array.value(i);
            match parse_string_to_decimal(str_value, precision, scale) {
                Ok(Some(decimal_value)) => {
                    // Convert i128 to i256
                    let i256_value = i256::from_i128(decimal_value);
                    decimal_builder.append_value(i256_value);
                }
                Ok(None) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(invalid_value(
                            str_value,
                            "STRING",
                            &format!("DECIMAL({},{})", precision, scale),
                        ));
                    }
                    decimal_builder.append_null();
                }
                Err(e) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(e);
                    }
                    decimal_builder.append_null();
                }
            }
        }
    }

    Ok(Arc::new(
        decimal_builder
            .with_precision_and_scale(precision, scale)
            .map_err(|e| {
                if matches!(e, arrow::error::ArrowError::InvalidArgumentError(_))
                    && e.to_string().contains("too large to store in a Decimal128")
                {
                    // Fallback error handling
                    SparkError::NumericValueOutOfRange {
                        value: "overflow".to_string(),
                        precision,
                        scale,
                    }
                } else {
                    SparkError::Arrow(Arc::new(e))
                }
            })?
            .finish(),
    ))
}

/// Normalize fullwidth Unicode digits (U+FF10–U+FF19) to their ASCII equivalents.
///
/// Spark's UTF8String parser treats fullwidth digits as numerically equivalent to
/// ASCII digits, e.g. "１２３.４５" parses as 123.45. Each fullwidth digit encodes
/// to exactly three UTF-8 bytes: [0xEF, 0xBC, 0x90+n] for digit n. The ASCII
/// equivalent is 0x30+n, so the conversion is: third_byte - 0x60.
///
/// All other bytes (ASCII or other multi-byte sequences) are passed through
/// unchanged, so the output is valid UTF-8 whenever the input is.
fn normalize_fullwidth_digits(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 2 < bytes.len()
            && bytes[i] == 0xEF
            && bytes[i + 1] == 0xBC
            && bytes[i + 2] >= 0x90
            && bytes[i + 2] <= 0x99
        {
            // e.g. 0x91 - 0x60 = 0x31 = b'1'
            out.push(bytes[i + 2] - 0x60);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    // SAFETY: we only replace valid 3-byte UTF-8 sequences [EF BC 9X] with a
    // single ASCII byte; all other bytes are copied unchanged, preserving the
    // UTF-8 invariant of the input.
    unsafe { String::from_utf8_unchecked(out) }
}

/// Parse a decimal string into mantissa and scale
/// e.g., "123.45" -> (12345, 2), "-0.001" -> (-1, 3) , 0e50 -> (0,50) etc
/// Parse a string to decimal following Spark's behavior
fn parse_string_to_decimal(input_str: &str, precision: u8, scale: i8) -> SparkResult<Option<i128>> {
    let string_bytes = input_str.as_bytes();
    let mut start = 0;
    let mut end = string_bytes.len();

    // Trim ASCII whitespace and null bytes from both ends. Spark's UTF8String
    // trims null bytes the same way it trims whitespace: "123\u0000" and
    // "\u0000123" both parse as 123. Null bytes in the middle are not trimmed
    // and will fail the digit validation in parse_decimal_str, producing NULL.
    while start < end && (string_bytes[start].is_ascii_whitespace() || string_bytes[start] == 0) {
        start += 1;
    }
    while end > start && (string_bytes[end - 1].is_ascii_whitespace() || string_bytes[end - 1] == 0)
    {
        end -= 1;
    }

    let trimmed = &input_str[start..end];

    // Normalize fullwidth digits to ASCII. Fast path skips the allocation for
    // pure-ASCII strings, which is the common case.
    let normalized;
    let trimmed = if trimmed.bytes().any(|b| b > 0x7F) {
        normalized = normalize_fullwidth_digits(trimmed);
        normalized.as_str()
    } else {
        trimmed
    };

    if trimmed.is_empty() {
        return Ok(None);
    }
    // Handle special values (inf, nan, etc.)
    if trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
        || trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
        || trimmed.eq_ignore_ascii_case("-inf")
        || trimmed.eq_ignore_ascii_case("-infinity")
        || trimmed.eq_ignore_ascii_case("nan")
    {
        return Ok(None);
    }

    // validate and parse mantissa and exponent or bubble up the error
    let (mantissa, exponent) = parse_decimal_str(trimmed, input_str, precision, scale)?;

    // Early return mantissa 0, Spark checks if it fits digits and throw error in ansi
    if mantissa == 0 {
        if exponent < -37 {
            return Err(SparkError::NumericOutOfRange {
                value: input_str.to_string(),
            });
        }
        return Ok(Some(0));
    }

    // scale adjustment
    let target_scale = scale as i32;
    let scale_adjustment = target_scale - exponent;

    let scaled_value = if scale_adjustment >= 0 {
        // Need to multiply (increase scale) but return None if scale is too high to fit i128
        if scale_adjustment > 38 {
            return Ok(None);
        }
        mantissa.checked_mul(10_i128.pow(scale_adjustment as u32))
    } else {
        // Need to divide (decrease scale)
        let abs_scale_adjustment = (-scale_adjustment) as u32;
        if abs_scale_adjustment > 38 {
            return Ok(Some(0));
        }

        let divisor = 10_i128.pow(abs_scale_adjustment);
        let quotient_opt = mantissa.checked_div(divisor);
        // Check if divisor is 0
        if quotient_opt.is_none() {
            return Ok(None);
        }
        let quotient = quotient_opt.unwrap();
        let remainder = mantissa % divisor;

        // Round half up: if abs(remainder) >= divisor/2, round away from zero
        let half_divisor = divisor / 2;
        let rounded = if remainder.abs() >= half_divisor {
            if mantissa >= 0 {
                quotient + 1
            } else {
                quotient - 1
            }
        } else {
            quotient
        };
        Some(rounded)
    };

    match scaled_value {
        Some(value) => {
            if is_validate_decimal_precision(value, precision) {
                Ok(Some(value))
            } else {
                // Value ok but exceeds precision mentioned . THrow error
                Err(SparkError::NumericValueOutOfRange {
                    value: trimmed.to_string(),
                    precision,
                    scale,
                })
            }
        }
        None => {
            // Overflow when scaling raise exception
            Err(SparkError::NumericValueOutOfRange {
                value: trimmed.to_string(),
                precision,
                scale,
            })
        }
    }
}

fn invalid_decimal_cast(value: &str, precision: u8, scale: i8) -> SparkError {
    invalid_value(
        value,
        "STRING",
        &format!("DECIMAL({},{})", precision, scale),
    )
}

/// Parse a decimal string into mantissa and scale
/// e.g., "123.45" -> (12345, 2), "-0.001" -> (-1, 3) , 0e50 -> (0,50) etc
fn parse_decimal_str(
    s: &str,
    original_str: &str,
    precision: u8,
    scale: i8,
) -> SparkResult<(i128, i32)> {
    if s.is_empty() {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    let (mantissa_str, exponent) = if let Some(e_pos) = s.find(|c| ['e', 'E'].contains(&c)) {
        let mantissa_part = &s[..e_pos];
        let exponent_part = &s[e_pos + 1..];
        // Parse exponent
        let exp: i32 = exponent_part
            .parse()
            .map_err(|_| invalid_decimal_cast(original_str, precision, scale))?;

        (mantissa_part, exp)
    } else {
        (s, 0)
    };

    let negative = mantissa_str.starts_with('-');
    let mantissa_str = if negative || mantissa_str.starts_with('+') {
        &mantissa_str[1..]
    } else {
        mantissa_str
    };

    if mantissa_str.starts_with('+') || mantissa_str.starts_with('-') {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    let (integral_part, fractional_part) = match mantissa_str.find('.') {
        Some(dot_pos) => {
            if mantissa_str[dot_pos + 1..].contains('.') {
                return Err(invalid_decimal_cast(original_str, precision, scale));
            }
            (&mantissa_str[..dot_pos], &mantissa_str[dot_pos + 1..])
        }
        None => (mantissa_str, ""),
    };

    if integral_part.is_empty() && fractional_part.is_empty() {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    if !integral_part.is_empty() && !integral_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    if !fractional_part.is_empty() && !fractional_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    // Parse integral part
    let integral_value: i128 = if integral_part.is_empty() {
        // Empty integral part is valid (e.g., ".5" or "-.7e9")
        0
    } else {
        integral_part
            .parse()
            .map_err(|_| invalid_decimal_cast(original_str, precision, scale))?
    };

    // Parse fractional part
    let fractional_scale = fractional_part.len() as i32;
    let fractional_value: i128 = if fractional_part.is_empty() {
        0
    } else {
        fractional_part
            .parse()
            .map_err(|_| invalid_decimal_cast(original_str, precision, scale))?
    };

    // Combine: value = integral * 10^fractional_scale + fractional
    let mantissa = integral_value
        .checked_mul(10_i128.pow(fractional_scale as u32))
        .and_then(|v| v.checked_add(fractional_value))
        .ok_or_else(|| invalid_decimal_cast(original_str, precision, scale))?;

    let final_mantissa = if negative { -mantissa } else { mantissa };
    // final scale = fractional_scale - exponent
    // For example : "1.23E-5" has fractional_scale=2, exponent=-5, so scale = 2 - (-5) = 7
    let final_scale = fractional_scale - exponent;
    Ok((final_mantissa, final_scale))
}

pub(crate) fn cast_string_to_date(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .expect("Expected a string array");

    if to_type != &DataType::Date32 {
        unreachable!("Invalid data type {:?} in cast from string", to_type);
    }

    let len = string_array.len();
    let mut cast_array = PrimitiveArray::<Date32Type>::builder(len);

    for i in 0..len {
        let value = if string_array.is_null(i) {
            None
        } else {
            match date_parser(string_array.value(i), eval_mode) {
                Ok(Some(cast_value)) => Some(cast_value),
                Ok(None) => None,
                Err(e) => return Err(e),
            }
        };

        match value {
            Some(cast_value) => cast_array.append_value(cast_value),
            None => cast_array.append_null(),
        }
    }

    Ok(Arc::new(cast_array.finish()) as ArrayRef)
}

pub(crate) fn cast_string_to_timestamp(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
    timezone_str: &str,
    is_spark4_plus: bool,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .expect("Expected a string array");

    let tz = &timezone::Tz::from_str(timezone_str)
        .map_err(|_| SparkError::Internal(format!("Invalid timezone string: {timezone_str}")))?;

    let cast_array: ArrayRef = match to_type {
        DataType::Timestamp(_, tz_opt) => {
            let to_tz = tz_opt.as_deref().unwrap_or("UTC");
            cast_utf8_to_timestamp!(
                string_array,
                eval_mode,
                TimestampMicrosecondType,
                timestamp_parser,
                tz,
                to_tz,
                is_spark4_plus
            )?
        }
        _ => unreachable!("Invalid data type {:?} in cast from string", to_type),
    };
    Ok(cast_array)
}

pub(crate) fn cast_string_to_timestamp_ntz(
    array: &ArrayRef,
    eval_mode: EvalMode,
    allow_time_zone: bool,
    is_spark4_plus: bool,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .expect("Expected a string array");

    let cast_array: ArrayRef = cast_utf8_to_timestamp_ntz!(
        string_array,
        eval_mode,
        timestamp_ntz_parser,
        allow_time_zone,
        is_spark4_plus
    )?;
    Ok(cast_array)
}

pub(crate) fn cast_string_to_int<OffsetSize: OffsetSizeTrait>(
    to_type: &DataType,
    array: &ArrayRef,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .expect("cast_string_to_int expected a string array");

    // Select parse function once per batch based on eval_mode
    let cast_array: ArrayRef =
        match (to_type, eval_mode) {
            (DataType::Int8, EvalMode::Legacy) => {
                cast_utf8_to_int!(string_array, Int8Type, parse_string_to_i8_legacy)?
            }
            (DataType::Int8, EvalMode::Ansi) => {
                cast_utf8_to_int!(string_array, Int8Type, parse_string_to_i8_ansi)?
            }
            (DataType::Int8, EvalMode::Try) => {
                cast_utf8_to_int!(string_array, Int8Type, parse_string_to_i8_try)?
            }
            (DataType::Int16, EvalMode::Legacy) => {
                cast_utf8_to_int!(string_array, Int16Type, parse_string_to_i16_legacy)?
            }
            (DataType::Int16, EvalMode::Ansi) => {
                cast_utf8_to_int!(string_array, Int16Type, parse_string_to_i16_ansi)?
            }
            (DataType::Int16, EvalMode::Try) => {
                cast_utf8_to_int!(string_array, Int16Type, parse_string_to_i16_try)?
            }
            (DataType::Int32, EvalMode::Legacy) => cast_utf8_to_int!(
                string_array,
                Int32Type,
                |s| do_parse_string_to_int_legacy::<i32>(s, i32::MIN)
            )?,
            (DataType::Int32, EvalMode::Ansi) => {
                cast_utf8_to_int!(string_array, Int32Type, |s| do_parse_string_to_int_ansi::<
                    i32,
                >(
                    s, "INT", i32::MIN
                ))?
            }
            (DataType::Int32, EvalMode::Try) => {
                cast_utf8_to_int!(
                    string_array,
                    Int32Type,
                    |s| do_parse_string_to_int_try::<i32>(s, i32::MIN)
                )?
            }
            (DataType::Int64, EvalMode::Legacy) => cast_utf8_to_int!(
                string_array,
                Int64Type,
                |s| do_parse_string_to_int_legacy::<i64>(s, i64::MIN)
            )?,
            (DataType::Int64, EvalMode::Ansi) => {
                cast_utf8_to_int!(string_array, Int64Type, |s| do_parse_string_to_int_ansi::<
                    i64,
                >(
                    s, "BIGINT", i64::MIN
                ))?
            }
            (DataType::Int64, EvalMode::Try) => {
                cast_utf8_to_int!(
                    string_array,
                    Int64Type,
                    |s| do_parse_string_to_int_try::<i64>(s, i64::MIN)
                )?
            }
            (dt, _) => unreachable!(
                "{}",
                format!("invalid integer type {dt} in cast from string")
            ),
        };
    Ok(cast_array)
}

/// Finalizes the result by applying the sign. Returns None if overflow would occur.
fn finalize_int_result<T: Integer + CheckedNeg + Copy>(result: T, negative: bool) -> Option<T> {
    if negative {
        Some(result)
    } else {
        result.checked_neg().filter(|&n| n >= T::zero())
    }
}

/// Equivalent to
/// - org.apache.spark.unsafe.types.UTF8String.toInt(IntWrapper intWrapper, boolean allowDecimal)
/// - org.apache.spark.unsafe.types.UTF8String.toLong(LongWrapper longWrapper, boolean allowDecimal)
fn do_parse_string_to_int_legacy<T: Integer + CheckedSub + CheckedNeg + From<u8> + Copy>(
    str: &str,
    min_value: T,
) -> SparkResult<Option<T>> {
    let trimmed_bytes = str.as_bytes().trim_ascii();

    let (negative, digits) = match parse_sign(trimmed_bytes) {
        Some(result) => result,
        None => return Ok(None),
    };

    let mut result: T = T::zero();
    let radix = T::from(10_u8);
    let stop_value = min_value / radix;

    let mut iter = digits.iter();

    // Parse integer portion until '.' or end
    for &ch in iter.by_ref() {
        if ch == b'.' {
            break;
        }

        if !ch.is_ascii_digit() {
            return Ok(None);
        }

        if result < stop_value {
            return Ok(None);
        }
        let v = result * radix;
        let digit: T = T::from(ch - b'0');
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => return Ok(None),
        }
    }

    // Validate decimal portion (digits only, values ignored)
    for &ch in iter {
        if !ch.is_ascii_digit() {
            return Ok(None);
        }
    }

    Ok(finalize_int_result(result, negative))
}

fn do_parse_string_to_int_ansi<T: Integer + CheckedSub + CheckedNeg + From<u8> + Copy>(
    str: &str,
    type_name: &str,
    min_value: T,
) -> SparkResult<Option<T>> {
    let error = || Err(invalid_value(str, "STRING", type_name));

    let trimmed_bytes = str.as_bytes().trim_ascii();

    let (negative, digits) = match parse_sign(trimmed_bytes) {
        Some(result) => result,
        None => return error(),
    };

    let mut result: T = T::zero();
    let radix = T::from(10_u8);
    let stop_value = min_value / radix;

    for &ch in digits {
        if ch == b'.' || !ch.is_ascii_digit() {
            return error();
        }

        if result < stop_value {
            return error();
        }
        let v = result * radix;
        let digit: T = T::from(ch - b'0');
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => return error(),
        }
    }

    finalize_int_result(result, negative)
        .map(Some)
        .ok_or_else(|| invalid_value(str, "STRING", type_name))
}

fn do_parse_string_to_int_try<T: Integer + CheckedSub + CheckedNeg + From<u8> + Copy>(
    str: &str,
    min_value: T,
) -> SparkResult<Option<T>> {
    let trimmed_bytes = str.as_bytes().trim_ascii();

    let (negative, digits) = match parse_sign(trimmed_bytes) {
        Some(result) => result,
        None => return Ok(None),
    };

    let mut result: T = T::zero();
    let radix = T::from(10_u8);
    let stop_value = min_value / radix;

    for &ch in digits {
        if ch == b'.' || !ch.is_ascii_digit() {
            return Ok(None);
        }

        if result < stop_value {
            return Ok(None);
        }
        let v = result * radix;
        let digit: T = T::from(ch - b'0');
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => return Ok(None),
        }
    }

    Ok(finalize_int_result(result, negative))
}

fn parse_string_to_i8_legacy(str: &str) -> SparkResult<Option<i8>> {
    match do_parse_string_to_int_legacy::<i32>(str, i32::MIN)? {
        Some(v) if v >= i8::MIN as i32 && v <= i8::MAX as i32 => Ok(Some(v as i8)),
        _ => Ok(None),
    }
}

fn parse_string_to_i8_ansi(str: &str) -> SparkResult<Option<i8>> {
    match do_parse_string_to_int_ansi::<i32>(str, "TINYINT", i32::MIN)? {
        Some(v) if v >= i8::MIN as i32 && v <= i8::MAX as i32 => Ok(Some(v as i8)),
        _ => Err(invalid_value(str, "STRING", "TINYINT")),
    }
}

fn parse_string_to_i8_try(str: &str) -> SparkResult<Option<i8>> {
    match do_parse_string_to_int_try::<i32>(str, i32::MIN)? {
        Some(v) if v >= i8::MIN as i32 && v <= i8::MAX as i32 => Ok(Some(v as i8)),
        _ => Ok(None),
    }
}

fn parse_string_to_i16_legacy(str: &str) -> SparkResult<Option<i16>> {
    match do_parse_string_to_int_legacy::<i32>(str, i32::MIN)? {
        Some(v) if v >= i16::MIN as i32 && v <= i16::MAX as i32 => Ok(Some(v as i16)),
        _ => Ok(None),
    }
}

fn parse_string_to_i16_ansi(str: &str) -> SparkResult<Option<i16>> {
    match do_parse_string_to_int_ansi::<i32>(str, "SMALLINT", i32::MIN)? {
        Some(v) if v >= i16::MIN as i32 && v <= i16::MAX as i32 => Ok(Some(v as i16)),
        _ => Err(invalid_value(str, "STRING", "SMALLINT")),
    }
}

fn parse_string_to_i16_try(str: &str) -> SparkResult<Option<i16>> {
    match do_parse_string_to_int_try::<i32>(str, i32::MIN)? {
        Some(v) if v >= i16::MIN as i32 && v <= i16::MAX as i32 => Ok(Some(v as i16)),
        _ => Ok(None),
    }
}

/// Parses sign and returns (is_negative, remaining_bytes after sign)
/// Returns None if invalid (empty input, or just "+" or "-")
fn parse_sign(bytes: &[u8]) -> Option<(bool, &[u8])> {
    let (&first, rest) = bytes.split_first()?;
    match first {
        b'-' if !rest.is_empty() => Some((true, rest)),
        b'+' if !rest.is_empty() => Some((false, rest)),
        _ => Some((false, bytes)),
    }
}

#[inline]
pub fn invalid_value(value: &str, from_type: &str, to_type: &str) -> SparkError {
    SparkError::CastInvalidValue {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

fn get_timestamp_values<T: TimeZone>(
    value: &str,
    timestamp_type: &str,
    tz: &T,
) -> SparkResult<Option<i64>> {
    // Handle negative year: strip leading '-' and remember the sign.
    let (sign, date_part) = if let Some(stripped) = value.strip_prefix('-') {
        (-1i32, stripped)
    } else {
        (1i32, value)
    };
    let mut parts = date_part.split(['T', ' ', '-', ':', '.']);
    let year = sign
        * parts
            .next()
            .unwrap_or("")
            .parse::<i32>()
            .unwrap_or_default();

    // Guard against years that cannot produce a valid i64 microsecond timestamp.
    // The Long.MaxValue/MinValue boundaries correspond to years 294247 / -290308.
    // We allow a slightly wider range and let parse_timestamp_to_micros perform the
    // exact overflow check via checked arithmetic.
    if !(-290309..=294248).contains(&year) {
        return Ok(None);
    }

    let month = parts.next().map_or(1, |m| m.parse::<u32>().unwrap_or(1));
    let day = parts.next().map_or(1, |d| d.parse::<u32>().unwrap_or(1));
    let hour = parts.next().map_or(0, |h| h.parse::<u32>().unwrap_or(0));
    let minute = parts.next().map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = parts.next().map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let microsecond = parts.next().map_or(0, |ms| {
        // Truncate to at most 6 digits then scale to fill the microsecond field.
        // E.g. ".123" -> 123 * 10^3 = 123_000 µs; ".1234567" -> truncated to 123_456 µs.
        let ms = &ms[..ms.len().min(6)];
        let n = ms.len();
        ms.parse::<u32>().unwrap_or(0) * 10u32.pow((6 - n) as u32)
    });

    let mut timestamp_info = TimeStampInfo::default();

    let timestamp_info = match timestamp_type {
        "year" => timestamp_info.with_year(year),
        "month" => timestamp_info.with_year(year).with_month(month),
        "day" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day),
        "hour" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour),
        "minute" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute),
        "second" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute)
            .with_second(second),
        "microsecond" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute)
            .with_second(second)
            .with_microsecond(microsecond),
        _ => {
            return Err(SparkError::InvalidInputInCastToDatetime {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP".to_string(),
            })
        }
    };
    parse_timestamp_to_micros(timestamp_info, tz)
}

/// Howard Hinnant's algorithm: proleptic Gregorian days since 1970-01-01 for any i64 year.
/// Works correctly for positive and negative years via Euclidean floor division.
/// Spark uses Java's equivalent [LocalDate.toEpochDay](https://github.com/openjdk/jdk/blob/cddee6d6eb3e048635c380a32bd2f6ebfd2c18b5/src/java.base/share/classes/java/time/LocalDate.java#L1954)
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = y - era * 400; // year of era [0, 399]
    let doy = (153 * m + 2) / 5 + d - 1; // day of year [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // day of era [0, 146096]
    era * 146097 + doe - 719468
}

fn parse_timestamp_to_micros<T: TimeZone>(
    timestamp_info: &TimeStampInfo,
    tz: &T,
) -> SparkResult<Option<i64>> {
    // Build NaiveDateTime explicitly so we can pattern-match LocalResult variants and
    // handle the DST spring-forward gap case.
    let naive_date_opt = NaiveDate::from_ymd_opt(
        timestamp_info.year,
        timestamp_info.month,
        timestamp_info.day,
    );

    // NaiveTime is used for the common path; also validates hour/min/sec.
    let naive_time = match NaiveTime::from_hms_opt(
        timestamp_info.hour,
        timestamp_info.minute,
        timestamp_info.second,
    ) {
        Some(t) => t,
        None => return Ok(None), // invalid time components
    };

    if let Some(naive_date) = naive_date_opt {
        let local_naive = naive_date.and_time(naive_time);

        // Resolve local datetime to UTC, handling DST transitions.
        // We compute base_micros with second precision (local_naive has no sub-second component),
        // then add microseconds at the end to avoid calling with_nanosecond(), which internally
        // calls from_local_datetime().single() and returns None for ambiguous (fall-back) times.
        let base_micros: Option<i64> = match tz.from_local_datetime(&local_naive) {
            // Unambiguous local time.
            LocalResult::Single(dt) => Some(dt.timestamp_micros()),
            // DST fall-back overlap: Spark picks the earlier UTC instant (pre-transition offset).
            LocalResult::Ambiguous(earlier, _) => Some(earlier.timestamp_micros()),
            // DST spring-forward gap: the local time does not exist.
            // Java's ZonedDateTime.of() advances by the gap length, which is equivalent to
            //   utc = local_naive − pre_gap_offset
            LocalResult::None => {
                let probe = local_naive - chrono::Duration::hours(3);
                let pre_offset = match tz.from_local_datetime(&probe) {
                    LocalResult::Single(dt) => dt.offset().fix(),
                    LocalResult::Ambiguous(dt, _) => dt.offset().fix(),
                    LocalResult::None => return Ok(None),
                };
                let offset_secs = pre_offset.local_minus_utc() as i64;
                let utc_naive = local_naive - chrono::Duration::seconds(offset_secs);
                Some(utc_naive.and_utc().timestamp_micros())
            }
        };

        Ok(base_micros.map(|m| m + timestamp_info.microsecond as i64))
    } else {
        // NaiveDate::from_ymd_opt returned None. This means either:
        //   (a) invalid calendar date (Feb 29 on non-leap year, month 13, etc.)
        //   (b) year outside chrono's representable range (> 262143 or < -262144)
        //
        // For case (b) we fall back to Howard Hinnant's direct arithmetic, which works
        // for any year that fits in i64.  This covers the Long.MaxValue / Long.MinValue
        // boundary timestamps (year 294247 / -290308).
        let year = timestamp_info.year as i64;
        if (-262144..=262143).contains(&year) {
            // Year is in chrono's range but date was rejected -> truly invalid date.
            return Ok(None);
        }
        // Validate month and day manually for extreme years.
        let m = timestamp_info.month;
        let d = timestamp_info.day;
        if !(1..=12).contains(&m) {
            return Ok(None);
        }
        let max_day = match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
                if leap {
                    29
                } else {
                    28
                }
            }
            _ => return Ok(None),
        };
        if d < 1 || d > max_day {
            return Ok(None);
        }
        // Compute the timezone offset using epoch as a surrogate probe point.
        // Extreme-year timestamps are only valid with a UTC-like fixed offset (any DST
        // zone would overflow).  Using epoch gives us the standard offset.
        let epoch_probe = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let tz_offset_secs: i64 = match tz.from_local_datetime(&epoch_probe) {
            LocalResult::Single(dt) => dt.offset().fix().local_minus_utc() as i64,
            LocalResult::Ambiguous(dt, _) => dt.offset().fix().local_minus_utc() as i64,
            LocalResult::None => 0,
        };
        // Compute seconds since epoch via direct calendar arithmetic.
        // Use i128 for the intermediate multiply-by-1_000_000 step: the seconds value can be
        // just outside the i64 range while the final microseconds result is still within range
        // (e.g., Long.MinValue boundary: seconds = -9_223_372_036_855, result = i64::MIN).
        let days = days_from_civil(year, m as i64, d as i64);
        let time_secs = timestamp_info.hour as i64 * 3600
            + timestamp_info.minute as i64 * 60
            + timestamp_info.second as i64;
        let total_secs = days
            .checked_mul(86400)
            .and_then(|s| s.checked_add(time_secs))
            .and_then(|s| s.checked_sub(tz_offset_secs));
        let utc_micros = total_secs.and_then(|s| {
            let micros128 = s as i128 * 1_000_000 + timestamp_info.microsecond as i128;
            i64::try_from(micros128).ok()
        });
        Ok(utc_micros)
    }
}

fn local_datetime_to_micros(timestamp_info: &TimeStampInfo) -> SparkResult<Option<i64>> {
    let year = timestamp_info.year as i64;
    let m = timestamp_info.month;
    let d = timestamp_info.day;

    if !(1..=12).contains(&m) {
        return Ok(None);
    }
    let max_day = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31u32,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            if leap {
                29
            } else {
                28
            }
        }
        _ => return Ok(None),
    };
    if d < 1 || d > max_day {
        return Ok(None);
    }
    if timestamp_info.hour >= 24 || timestamp_info.minute >= 60 || timestamp_info.second >= 60 {
        return Ok(None);
    }

    let days = days_from_civil(year, m as i64, d as i64);
    let time_secs = timestamp_info.hour as i64 * 3600
        + timestamp_info.minute as i64 * 60
        + timestamp_info.second as i64;
    let total_secs = days
        .checked_mul(86400)
        .and_then(|s| s.checked_add(time_secs));
    let micros = total_secs.and_then(|s| {
        let micros128 = s as i128 * 1_000_000 + timestamp_info.microsecond as i128;
        i64::try_from(micros128).ok()
    });
    Ok(micros)
}

fn parse_str_to_year_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "year", tz)
}

fn parse_str_to_month_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "month", tz)
}

fn parse_str_to_day_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "day", tz)
}

fn parse_str_to_hour_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "hour", tz)
}

fn parse_str_to_minute_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "minute", tz)
}

fn parse_str_to_second_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "second", tz)
}

fn parse_str_to_microsecond_timestamp<T: TimeZone>(
    value: &str,
    tz: &T,
) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "microsecond", tz)
}

fn timestamp_parser<T: TimeZone>(
    value: &str,
    eval_mode: EvalMode,
    tz: &T,
    is_spark4_plus: bool,
) -> SparkResult<Option<i64>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    // Spark 4.0+ rejects leading whitespace for ALL T-prefixed time-only strings
    // (T<h>, T<h>:<m>, T<h>:<m>:<s>, T<h>:<m>:<s>.<f>), but accepts trailing whitespace.
    // Spark 3.x trims all whitespace first, so leading whitespace is accepted there.
    // Check the raw (pre-trim) value for leading whitespace before any T-time-only match.
    if is_spark4_plus
        && value.len() > value.trim_start().len()
        && (RE_TIME_ONLY_H.is_match(trimmed)
            || RE_TIME_ONLY_HM.is_match(trimmed)
            || RE_TIME_ONLY_HMS.is_match(trimmed)
            || RE_TIME_ONLY_HMSU.is_match(trimmed))
    {
        return if eval_mode == EvalMode::Ansi {
            Err(SparkError::InvalidInputInCastToDatetime {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP".to_string(),
            })
        } else {
            Ok(None)
        };
    }
    let value = trimmed;
    // Spark accepts a leading '+' year sign on full date-time strings (e.g. "+2020-01-01T12:34:56")
    // but rejects it on time-only strings (e.g. "+12:12:12" -> null).
    // Detect: '+' followed by at least one digit and then a '-' separator -> year prefix -> strip '+'.
    // Anything else starting with '+' (time-only, bare number, etc.) -> null.
    let value = if let Some(rest) = value.strip_prefix('+') {
        let first_non_digit = rest.find(|c: char| !c.is_ascii_digit());
        match first_non_digit {
            Some(i) if i >= 1 && rest.as_bytes()[i] == b'-' => rest,
            _ => return Ok(None),
        }
    } else {
        value
    };

    // Only attempt offset-suffix extraction when the value does not already match a
    // base pattern.  This prevents the '-' in plain date strings like "2015-03-18"
    // from being misidentified as a negative-offset sign.
    let has_direct_match = RE_YEAR.is_match(value)
        || RE_MONTH.is_match(value)
        || RE_DAY.is_match(value)
        || RE_HOUR.is_match(value)
        || RE_MINUTE.is_match(value)
        || RE_SECOND.is_match(value)
        || RE_MICROSECOND.is_match(value)
        || RE_TIME_ONLY_H.is_match(value)
        || RE_TIME_ONLY_HM.is_match(value)
        || RE_TIME_ONLY_HMS.is_match(value)
        || RE_TIME_ONLY_HMSU.is_match(value)
        || RE_BARE_HM.is_match(value)
        || RE_BARE_HMS.is_match(value)
        || RE_BARE_HMSU.is_match(value);

    if !has_direct_match {
        if let Some((stripped, suffix_tz)) = extract_offset_suffix(value) {
            return timestamp_parser_with_tz(stripped, eval_mode, &suffix_tz);
        }
    }

    timestamp_parser_with_tz(value, eval_mode, tz)
}

/// Parses the portion of an offset string AFTER any "UTC"/"GMT"/"UT" prefix (or the
/// full bare +/- offset including its sign character).  Returns the offset in whole seconds,
/// or `None` for any malformed, out-of-range, or trailing-garbage input.
///
/// Accepted formats (H = 1–2 digit hour, M = 1–2 digit minute):
///   ""        -> 0          (bare "UTC" / "GMT" / "UT")
///   "+H"      -> +H*3600    (hour-only, e.g. "+0" from "UTC+0")
///   "+HH"     -> same
///   "+HHMM"   -> +H*3600+M*60  (4 digits, no colon)
///   "+H:M"    -> same (with colon, any digit count 1-2 each)
///   "+HH:MM"  -> same
///   (negative with '-' analogously)
///
/// Hours must be 0–18 and minutes 0–59.  A trailing colon ("+8:") is rejected.
fn parse_sign_offset(s: &str) -> Option<i32> {
    if s.is_empty() {
        return Some(0);
    }
    let (sign, rest) = match s.as_bytes().first() {
        Some(&b'+') => (1i32, &s[1..]),
        Some(&b'-') => (-1i32, &s[1..]),
        _ => return None,
    };
    if rest.is_empty() {
        return None; // lone '+' or '-'
    }
    let (h, m) = if let Some(colon_pos) = rest.find(':') {
        let h_str = &rest[..colon_pos];
        let m_str = &rest[colon_pos + 1..];
        if m_str.is_empty() {
            return None; // trailing colon: "+8:"
        }
        let h: i32 = h_str.parse().ok()?;
        // Note: "+HH:MM:SS" (with seconds) is not handled; Spark accepts it but it is rare.
        let m: i32 = m_str.parse().ok()?;
        (h, m)
    } else {
        match rest.len() {
            1 | 2 => (rest.parse::<i32>().ok()?, 0),
            4 => (
                rest[..2].parse::<i32>().ok()?,
                rest[2..].parse::<i32>().ok()?,
            ),
            _ => return None,
        }
    };
    if !(0..=18).contains(&h) || !(0..=59).contains(&m) {
        return None;
    }
    Some(sign * (h * 3600 + m * 60))
}

/// Constructs a `timezone::Tz` from an offset measured in seconds.
/// E.g. `+7*3600 + 30*60` -> `"+07:30"`.
fn tz_from_offset_secs(secs: i32) -> Option<timezone::Tz> {
    let abs = secs.abs();
    let h = abs / 3600;
    let m = (abs % 3600) / 60;
    let sign = if secs >= 0 { '+' } else { '-' };
    timezone::Tz::from_str(&format!("{}{:02}:{:02}", sign, h, m)).ok()
}

/// Returns the last (rightmost) byte position where `needle` starts inside `haystack`.
fn rfind_str(haystack: &str, needle: &str) -> Option<usize> {
    let hb = haystack.as_bytes();
    let nb = needle.as_bytes();
    if nb.len() > hb.len() {
        return None;
    }
    (0..=(hb.len() - nb.len()))
        .rev()
        .find(|&i| hb[i..].starts_with(nb))
}

/// If `value` ends with a recognised timezone suffix, returns `(datetime_prefix, Tz)`.
/// Returns `None` when no suffix is found.
///
/// Recognised forms (in matching priority order):
///   Z                  -> UTC+0
///   UTC / " UTC"       -> UTC+0  (or UTC +/- offset, e.g. "UTC+0", " UTC+07:30")
///   GMT / " GMT"       -> UTC+0  (or GMT +/- offset)
///   UT  / " UT"        -> UTC+0  (or UT +/- offset)
///   Named IANA zone    -> e.g. " Europe/Moscow"
///   Bare +/-offset       -> e.g. "+07:30", "-1:0", "+0730"
///
/// **The caller must ensure the value does not already match a base timestamp pattern.**
/// Without that guard a bare '-' in "2015-03-18" would be misread as a -18:00 offset.
fn extract_offset_suffix(value: &str) -> Option<(&str, timezone::Tz)> {
    // 1. Z suffix
    if let Some(stripped) = value.strip_suffix('Z') {
        return Some((stripped, tz_from_offset_secs(0)?));
    }

    // 2. Named text-prefix forms: "UTC", "GMT", "UT" (optionally space-prefixed),
    //    each optionally followed by a bare +/-offset.
    //    Longest first so " UTC" is tried before " UT", etc.
    for prefix in &[" UTC", "UTC", " GMT", "GMT", " UT", "UT"] {
        if let Some(pos) = rfind_str(value, prefix) {
            let offset_str = &value[pos + prefix.len()..];
            if let Some(secs) = parse_sign_offset(offset_str) {
                return Some((&value[..pos], tz_from_offset_secs(secs)?));
            }
        }
    }

    // 3. Java SHORT_IDS fixed-offset abbreviations recognised by ZoneId.of() via SHORT_IDS map.
    //    Only three have purely fixed offsets (no '/'):
    //      EST -> -05:00  (-18 000 s)
    //      MST -> -07:00  (-25 200 s)
    //      HST -> -10:00  (-36 000 s)
    //    These may appear with or without a leading space; no sub-offset is allowed after them.
    for (abbr, offset_secs) in &[
        (" EST", -18_000i32),
        ("EST", -18_000),
        (" MST", -25_200),
        ("MST", -25_200),
        (" HST", -36_000),
        ("HST", -36_000),
    ] {
        if let Some(pos) = rfind_str(value, abbr) {
            if pos + abbr.len() == value.len() {
                return Some((&value[..pos], tz_from_offset_secs(*offset_secs)?));
            }
        }
    }

    // 4. Named IANA timezone: a space followed by a slash-containing word at the end.
    //    e.g. "2015-03-18T12:03:17.123456 Europe/Moscow"
    if let Some(space_pos) = value.rfind(' ') {
        let tz_name = &value[space_pos + 1..];
        if tz_name.contains('/') {
            if let Ok(tz) = timezone::Tz::from_str(tz_name) {
                return Some((&value[..space_pos], tz));
            }
        }
    }

    // 5. Bare +/-offset: find the rightmost '+' or '-' and try to parse everything
    //    from that position to the end as a complete valid offset.
    let last_sign = {
        let p = value.rfind('+');
        let m = value.rfind('-');
        match (p, m) {
            (Some(p), Some(m)) => Some(p.max(m)),
            (a, b) => a.or(b),
        }
    };
    if let Some(pos) = last_sign {
        let offset_str = &value[pos..];
        if let Some(secs) = parse_sign_offset(offset_str) {
            return Some((&value[..pos], tz_from_offset_secs(secs)?));
        }
    }

    None
}

type TimestampParsePattern<T> = (&'static Regex, fn(&str, &T) -> SparkResult<Option<i64>>);

// RE_YEAR allows only 4-6 digits (not 7) because a bare 7-digit string like "0119704"
// is ambiguous and Spark rejects it. The other patterns (RE_MONTH, RE_DAY, etc.) keep
// \d{4,7} because the `-` separator disambiguates the year portion, so "0002020-01-01"
// is validly year 2020 with leading zeros. date_parser's is_valid_digits also allows up
// to 7 year digits for the same reason.
static RE_YEAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?\d{4,6}$").unwrap());
static RE_MONTH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}$").unwrap());
static RE_DAY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}$").unwrap());
static RE_HOUR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{1,2}$").unwrap());
static RE_MINUTE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{2}:\d{2}$").unwrap());
static RE_SECOND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$").unwrap());
static RE_MICROSECOND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.\d+$").unwrap());
static RE_TIME_ONLY_H: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^T\d{1,2}$").unwrap());
static RE_TIME_ONLY_HM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}:\d{1,2}$").unwrap());
static RE_TIME_ONLY_HMS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}:\d{1,2}:\d{1,2}$").unwrap());
static RE_TIME_ONLY_HMSU: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}:\d{1,2}:\d{1,2}\.\d+$").unwrap());
static RE_BARE_HM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{1,2}:\d{1,2}$").unwrap());
static RE_BARE_HMS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{1,2}:\d{1,2}:\d{1,2}$").unwrap());
static RE_BARE_HMSU: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{1,2}:\d{1,2}:\d{1,2}\.\d+$").unwrap());

fn timestamp_parser_with_tz<T: TimeZone>(
    value: &str,
    eval_mode: EvalMode,
    tz: &T,
) -> SparkResult<Option<i64>> {
    // Both T-separator and space-separator date-time forms are supported.
    // Negative years are handled by get_timestamp_values detecting a leading '-'.
    let patterns: &[TimestampParsePattern<T>] = &[
        // Year only: 4-7 digits, optionally negative
        (
            &RE_YEAR,
            parse_str_to_year_timestamp as fn(&str, &T) -> SparkResult<Option<i64>>,
        ),
        // Year-month
        (&RE_MONTH, parse_str_to_month_timestamp),
        // Year-month-day
        (&RE_DAY, parse_str_to_day_timestamp),
        // Date T-or-space hour (1 or 2 digits)
        (&RE_HOUR, parse_str_to_hour_timestamp),
        // Date T-or-space hour:minute
        (&RE_MINUTE, parse_str_to_minute_timestamp),
        // Date T-or-space hour:minute:second
        (&RE_SECOND, parse_str_to_second_timestamp),
        // Date T-or-space hour:minute:second.fraction
        (&RE_MICROSECOND, parse_str_to_microsecond_timestamp),
        // Time-only: T hour (1 or 2 digits, no colon)
        (&RE_TIME_ONLY_H, parse_str_to_time_only_timestamp),
        // Time-only: T hour:minute
        (&RE_TIME_ONLY_HM, parse_str_to_time_only_timestamp),
        // Time-only: T hour:minute:second
        (&RE_TIME_ONLY_HMS, parse_str_to_time_only_timestamp),
        // Time-only: T hour:minute:second.fraction
        (&RE_TIME_ONLY_HMSU, parse_str_to_time_only_timestamp),
        // Bare time-only: hour:minute (without T prefix)
        (&RE_BARE_HM, parse_str_to_time_only_timestamp),
        // Bare time-only: hour:minute:second
        (&RE_BARE_HMS, parse_str_to_time_only_timestamp),
        // Bare time-only: hour:minute:second.fraction
        (&RE_BARE_HMSU, parse_str_to_time_only_timestamp),
    ];

    let mut timestamp = None;

    // Iterate through patterns and try matching
    for (pattern, parse_func) in patterns {
        if pattern.is_match(value) {
            timestamp = parse_func(value, tz)?;
            break;
        }
    }

    if timestamp.is_none() {
        return if eval_mode == EvalMode::Ansi {
            Err(SparkError::InvalidInputInCastToDatetime {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP".to_string(),
            })
        } else {
            Ok(None)
        };
    }

    Ok(timestamp)
}

fn get_timestamp_ntz_values(value: &str, timestamp_type: &str) -> SparkResult<Option<i64>> {
    let (sign, date_part) = if let Some(stripped) = value.strip_prefix('-') {
        (-1i32, stripped)
    } else {
        (1i32, value)
    };
    let mut parts = date_part.split(['T', ' ', '-', ':', '.']);
    let year = sign
        * parts
            .next()
            .unwrap_or("")
            .parse::<i32>()
            .unwrap_or_default();

    if !(-290309..=294248).contains(&year) {
        return Ok(None);
    }

    let month = parts.next().map_or(1, |m| m.parse::<u32>().unwrap_or(1));
    let day = parts.next().map_or(1, |d| d.parse::<u32>().unwrap_or(1));
    let hour = parts.next().map_or(0, |h| h.parse::<u32>().unwrap_or(0));
    let minute = parts.next().map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = parts.next().map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let microsecond = parts.next().map_or(0, |ms| {
        let ms = &ms[..ms.len().min(6)];
        let n = ms.len();
        ms.parse::<u32>().unwrap_or(0) * 10u32.pow((6 - n) as u32)
    });

    let mut timestamp_info = TimeStampInfo::default();

    let timestamp_info = match timestamp_type {
        "year" => timestamp_info.with_year(year),
        "month" => timestamp_info.with_year(year).with_month(month),
        "day" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day),
        "hour" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour),
        "minute" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute),
        "second" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute)
            .with_second(second),
        "microsecond" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute)
            .with_second(second)
            .with_microsecond(microsecond),
        _ => {
            return Err(SparkError::InvalidInputInCastToDatetime {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP_NTZ".to_string(),
            })
        }
    };
    local_datetime_to_micros(timestamp_info)
}

fn timestamp_ntz_parser(
    value: &str,
    eval_mode: EvalMode,
    allow_time_zone: bool,
    _is_spark4_plus: bool,
) -> SparkResult<Option<i64>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    // NTZ rejects leading whitespace for T-prefixed time-only strings on Spark 4+
    // (same logic as timestamp_parser), but time-only is rejected entirely for NTZ anyway.

    let value = trimmed;

    // Handle leading '+' the same way as timestamp_parser
    let value = if let Some(rest) = value.strip_prefix('+') {
        let first_non_digit = rest.find(|c: char| !c.is_ascii_digit());
        match first_non_digit {
            Some(i) if i >= 1 && rest.as_bytes()[i] == b'-' => rest,
            _ => return Ok(None),
        }
    } else {
        value
    };

    // Reject time-only patterns: NTZ requires a date component
    if RE_TIME_ONLY_H.is_match(value)
        || RE_TIME_ONLY_HM.is_match(value)
        || RE_TIME_ONLY_HMS.is_match(value)
        || RE_TIME_ONLY_HMSU.is_match(value)
        || RE_BARE_HM.is_match(value)
        || RE_BARE_HMS.is_match(value)
        || RE_BARE_HMSU.is_match(value)
    {
        return if eval_mode == EvalMode::Ansi {
            Err(SparkError::InvalidInputInCastToDatetime {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP_NTZ".to_string(),
            })
        } else {
            Ok(None)
        };
    }

    // Check if value matches a date-based pattern directly
    let has_direct_match = RE_YEAR.is_match(value)
        || RE_MONTH.is_match(value)
        || RE_DAY.is_match(value)
        || RE_HOUR.is_match(value)
        || RE_MINUTE.is_match(value)
        || RE_SECOND.is_match(value)
        || RE_MICROSECOND.is_match(value);

    // If no direct match, try stripping a timezone suffix
    let value_to_parse = if !has_direct_match {
        if let Some((stripped, _tz)) = extract_offset_suffix(value) {
            if !allow_time_zone {
                return if eval_mode == EvalMode::Ansi {
                    Err(SparkError::InvalidInputInCastToDatetime {
                        value: value.to_string(),
                        from_type: "STRING".to_string(),
                        to_type: "TIMESTAMP_NTZ".to_string(),
                    })
                } else {
                    Ok(None)
                };
            }
            stripped.trim_end()
        } else {
            value
        }
    } else {
        value
    };

    timestamp_ntz_parser_inner(value_to_parse, eval_mode)
}

fn timestamp_ntz_parser_inner(value: &str, eval_mode: EvalMode) -> SparkResult<Option<i64>> {
    type NtzParsePattern = (&'static Regex, fn(&str) -> SparkResult<Option<i64>>);

    fn parse_ntz_year(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "year")
    }
    fn parse_ntz_month(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "month")
    }
    fn parse_ntz_day(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "day")
    }
    fn parse_ntz_hour(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "hour")
    }
    fn parse_ntz_minute(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "minute")
    }
    fn parse_ntz_second(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "second")
    }
    fn parse_ntz_microsecond(value: &str) -> SparkResult<Option<i64>> {
        get_timestamp_ntz_values(value, "microsecond")
    }

    let patterns: &[NtzParsePattern] = &[
        (
            &RE_YEAR,
            parse_ntz_year as fn(&str) -> SparkResult<Option<i64>>,
        ),
        (&RE_MONTH, parse_ntz_month),
        (&RE_DAY, parse_ntz_day),
        (&RE_HOUR, parse_ntz_hour),
        (&RE_MINUTE, parse_ntz_minute),
        (&RE_SECOND, parse_ntz_second),
        (&RE_MICROSECOND, parse_ntz_microsecond),
    ];

    let mut timestamp = None;

    for (pattern, parse_func) in patterns {
        if pattern.is_match(value) {
            timestamp = parse_func(value)?;
            break;
        }
    }

    if timestamp.is_none() {
        return if eval_mode == EvalMode::Ansi {
            Err(SparkError::InvalidInputInCastToDatetime {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP_NTZ".to_string(),
            })
        } else {
            Ok(None)
        };
    }

    Ok(timestamp)
}

fn parse_str_to_time_only_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    // The 'T' is optional in the time format; strip it if specified.
    let time_part = value.strip_prefix('T').unwrap_or(value);

    // Parse time components: hour[:minute[:second[.fraction]]]
    // Use splitn(3) so "12:34:56.789" splits into ["12", "34", "56.789"].
    let colon_parts: Vec<&str> = time_part.splitn(3, ':').collect();
    let hour: u32 = colon_parts[0].parse().unwrap_or(0);
    let minute: u32 = colon_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let (second, nanosecond) = if let Some(sec_frac) = colon_parts.get(2) {
        let dot_idx = sec_frac.find('.');
        let sec: u32 = sec_frac[..dot_idx.unwrap_or(sec_frac.len())]
            .parse()
            .unwrap_or(0);
        let ns: u32 = if let Some(dot) = dot_idx {
            let frac = &sec_frac[dot + 1..];
            // Interpret up to 6 digits as microseconds, padding with trailing zeros.
            let trimmed = &frac[..frac.len().min(6)];
            let padded = format!("{:0<6}", trimmed);
            padded.parse::<u32>().unwrap_or(0) * 1000
        } else {
            0
        };
        (sec, ns)
    } else {
        (0, 0)
    };

    let datetime = tz.from_utc_datetime(&chrono::Utc::now().naive_utc());
    let result = datetime
        .with_timezone(tz)
        .with_hour(hour)
        .and_then(|dt| dt.with_minute(minute))
        .and_then(|dt| dt.with_second(second))
        .and_then(|dt| dt.with_nanosecond(nanosecond))
        .map(|dt| dt.timestamp_micros());

    Ok(result)
}

//a string to date parser - port of spark's SparkDateTimeUtils#stringToDate.
fn date_parser(date_str: &str, eval_mode: EvalMode) -> SparkResult<Option<i32>> {
    // local functions
    fn get_trimmed_start(bytes: &[u8]) -> usize {
        let mut start = 0;
        while start < bytes.len() && is_whitespace_or_iso_control(bytes[start]) {
            start += 1;
        }
        start
    }

    fn get_trimmed_end(start: usize, bytes: &[u8]) -> usize {
        let mut end = bytes.len() - 1;
        while end > start && is_whitespace_or_iso_control(bytes[end]) {
            end -= 1;
        }
        end + 1
    }

    fn is_whitespace_or_iso_control(byte: u8) -> bool {
        byte.is_ascii_whitespace() || byte.is_ascii_control()
    }

    fn is_valid_digits(segment: i32, digits: usize) -> bool {
        // NaiveDate is bounded to [-262142, 262142] (6 digits). We allow up to 7 digits to support
        // leading-zero year strings like "0002020" (= year 2020), matching Spark's
        // isValidDigits. Values outside the bounds are caught by an explicit bounds
        // check below.
        let max_digits_year = 7;
        // year (segment 0) can be between 4 to 7 digits,
        // month and day (segment 1 and 2) can be between 1 to 2 digits
        (segment == 0 && digits >= 4 && digits <= max_digits_year)
            || (segment != 0 && digits > 0 && digits <= 2)
    }

    fn return_result(date_str: &str, eval_mode: EvalMode) -> SparkResult<Option<i32>> {
        if eval_mode == EvalMode::Ansi {
            Err(SparkError::InvalidInputInCastToDatetime {
                value: date_str.to_string(),
                from_type: "STRING".to_string(),
                to_type: "DATE".to_string(),
            })
        } else {
            Ok(None)
        }
    }
    // end local functions

    if date_str.is_empty() {
        return return_result(date_str, eval_mode);
    }

    //values of date segments year, month and day defaulting to 1
    let mut date_segments = [1, 1, 1];
    let mut sign = 1;
    let mut current_segment = 0;
    let mut current_segment_value = Wrapping(0);
    let mut current_segment_digits = 0;
    let bytes = date_str.as_bytes();

    let mut j = get_trimmed_start(bytes);
    let str_end_trimmed = get_trimmed_end(j, bytes);

    if j == str_end_trimmed {
        return return_result(date_str, eval_mode);
    }

    // assign a sign to the date; both '-' and '+' are accepted (Spark stringToDate line 357-360)
    if bytes[j] == b'-' {
        sign = -1;
        j += 1;
    } else if bytes[j] == b'+' {
        // sign remains 1 (positive)
        j += 1;
    }

    //loop to the end of string until we have processed 3 segments,
    //exit loop on encountering any space ' ' or 'T' after the 3rd segment
    while j < str_end_trimmed && (current_segment < 3 && !(bytes[j] == b' ' || bytes[j] == b'T')) {
        let b = bytes[j];
        if current_segment < 2 && b == b'-' {
            //check for validity of year and month segments if current byte is separator
            if !is_valid_digits(current_segment, current_segment_digits) {
                return return_result(date_str, eval_mode);
            }
            //if valid update corresponding segment with the current segment value.
            date_segments[current_segment as usize] = current_segment_value.0;
            current_segment_value = Wrapping(0);
            current_segment_digits = 0;
            current_segment += 1;
        } else if !b.is_ascii_digit() {
            return return_result(date_str, eval_mode);
        } else {
            //increment value of current segment by the next digit
            let parsed_value = Wrapping((b - b'0') as i32);
            current_segment_value = current_segment_value * Wrapping(10) + parsed_value;
            current_segment_digits += 1;
        }
        j += 1;
    }

    //check for validity of last segment
    if !is_valid_digits(current_segment, current_segment_digits) {
        return return_result(date_str, eval_mode);
    }

    if current_segment < 2 && j < str_end_trimmed {
        // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
        return return_result(date_str, eval_mode);
    }

    date_segments[current_segment as usize] = current_segment_value.0;

    // Reject out-of-range years explicitly
    let year = sign * date_segments[0];
    if !(-262143..=262142).contains(&year) {
        return Ok(None);
    }

    match NaiveDate::from_ymd_opt(year, date_segments[1] as u32, date_segments[2] as u32) {
        Some(date) => {
            let duration_since_epoch = date
                .signed_duration_since(DateTime::UNIX_EPOCH.naive_utc().date())
                .num_days();
            Ok(Some(duration_since_epoch.to_i32().unwrap()))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::cast_array;
    use crate::SparkCastOptions;
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::TimeUnit;
    use datafusion::common::Result as DataFusionResult;

    /// Test helper that wraps the mode-specific parse functions
    fn cast_string_to_i8(str: &str, eval_mode: EvalMode) -> SparkResult<Option<i8>> {
        match eval_mode {
            EvalMode::Legacy => parse_string_to_i8_legacy(str),
            EvalMode::Ansi => parse_string_to_i8_ansi(str),
            EvalMode::Try => parse_string_to_i8_try(str),
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn test_cast_string_to_timestamp() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("T2"),
            Some("0100-01-01T12:34:56.123456"),
            Some("10000-01-01T12:34:56.123456"),
            // 7-digit year-only strings must return null (Spark returns null for these)
            Some("0119704"),
            Some("2024001"),
        ]));
        let tz = &timezone::Tz::from_str("UTC").unwrap();

        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let eval_mode = EvalMode::Legacy;
        let result = cast_utf8_to_timestamp!(
            &string_array,
            eval_mode,
            TimestampMicrosecondType,
            timestamp_parser,
            tz,
            "UTC",
            true
        )
        .unwrap();

        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(result.len(), 6);
        // 7-digit year-only strings must be null
        assert!(result.is_null(4), "0119704 should be null");
        assert!(result.is_null(5), "2024001 should be null");
    }

    #[test]
    fn test_cast_string_to_timestamp_ansi_error() {
        // In ANSI mode, an invalid timestamp string must produce an error rather than null.
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("not_a_timestamp"),
        ]));
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let eval_mode = EvalMode::Ansi;
        let result = cast_utf8_to_timestamp!(
            &string_array,
            eval_mode,
            TimestampMicrosecondType,
            timestamp_parser,
            tz,
            "UTC",
            true
        );
        assert!(
            result.is_err(),
            "ANSI mode should return Err for an invalid timestamp string"
        );
    }

    #[test]
    fn test_cast_string_to_timestamp_ansi_error_trimmed_value() {
        // The error value in InvalidInputInCastToDatetime must match the raw input
        // (including trailing whitespace) to match Spark's CAST_INVALID_INPUT behavior.
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("91\n3       "), // trailing spaces after a newline in the middle
        ]));
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let eval_mode = EvalMode::Ansi;
        let result = cast_utf8_to_timestamp!(
            &string_array,
            eval_mode,
            TimestampMicrosecondType,
            timestamp_parser,
            tz,
            "UTC",
            true
        );
        match result {
            Err(SparkError::InvalidInputInCastToDatetime { value, .. }) => {
                assert_eq!(
                    value, "91\n3       ",
                    "ANSI error value should match the raw (untrimmed) input to match Spark behavior"
                );
            }
            other => panic!("Expected InvalidInputInCastToDatetime error, got {other:?}"),
        }
    }

    #[test]
    fn test_cast_string_to_timestamp_ntz() {
        // Helper to reduce boilerplate
        fn parse(s: &str, allow_tz: bool) -> Option<i64> {
            timestamp_ntz_parser(s, EvalMode::Legacy, allow_tz, false).unwrap()
        }

        // Basic: "2020-01-01 12:34:56" -> local micros
        // days_from_civil(2020,1,1) = 18262; 18262*86400 = 1577836800
        // + 12*3600 + 34*60 + 56 = 45296; total = 1577882096s
        assert_eq!(
            parse("2020-01-01 12:34:56", true),
            Some(1_577_882_096_000_000)
        );
        assert_eq!(
            parse("2020-01-01T12:34:56", true),
            Some(1_577_882_096_000_000)
        );

        // With microseconds
        assert_eq!(
            parse("2020-01-01 12:34:56.123456", true),
            Some(1_577_882_096_123_456)
        );

        // Date only
        assert_eq!(parse("2020-01-01", true), Some(1_577_836_800_000_000));

        // Timezone discarded (allow_time_zone=true): same result as without TZ
        assert_eq!(
            parse("2020-01-01T12:34:56Z", true),
            Some(1_577_882_096_000_000)
        );
        assert_eq!(
            parse("2020-01-01T12:34:56+05:30", true),
            Some(1_577_882_096_000_000)
        );
        assert_eq!(
            parse("2020-01-01T12:34:56-08:00", true),
            Some(1_577_882_096_000_000)
        );
        // Space-separated offset (e.g. "2021-11-22 10:54:27 +08:00")
        assert_eq!(
            parse("2021-11-22 10:54:27 +08:00", true),
            parse("2021-11-22 10:54:27", true)
        );

        // Timezone rejected (allow_time_zone=false)
        assert_eq!(parse("2020-01-01T12:34:56Z", false), None);
        assert_eq!(parse("2020-01-01T12:34:56+05:30", false), None);

        // Time-only rejected
        assert_eq!(parse("T12:34:56", true), None);
        assert_eq!(parse("12:34", true), None);
        assert_eq!(parse("T2", true), None);

        // Invalid -> None in Legacy
        assert_eq!(parse("invalid", true), None);
        assert_eq!(parse("", true), None);

        // Invalid -> Error in ANSI
        assert!(timestamp_ntz_parser("invalid", EvalMode::Ansi, true, false).is_err());
        assert!(timestamp_ntz_parser("T12:34", EvalMode::Ansi, true, false).is_err());

        // Invalid -> None in Try
        assert_eq!(
            timestamp_ntz_parser("invalid", EvalMode::Try, true, false).unwrap(),
            None
        );

        // DST gap time works for NTZ (pure arithmetic, no DST)
        // days_from_civil(2024,3,10) * 86400 + 2*3600 + 30*60 = 1710037800s
        assert_eq!(
            parse("2024-03-10 02:30:00", true),
            Some(1_710_037_800_000_000)
        );

        // Invalid leap day -> None
        assert_eq!(parse("2023-02-29 00:00:00", true), None);

        // Valid leap day
        assert!(parse("2020-02-29 00:00:00", true).is_some());
    }

    #[test]
    fn test_cast_string_to_timestamp_ntz_array() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("T2"),
            Some("2020-01-01"),
            None,
            Some("invalid"),
            Some("2020-06-15T12:30:00Z"),
        ]));
        let result = cast_string_to_timestamp_ntz(&array, EvalMode::Legacy, true, false).unwrap();
        let ts_array = result
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>()
            .unwrap();
        assert_eq!(ts_array.len(), 6);
        assert!(!ts_array.is_null(0)); // valid
        assert!(ts_array.is_null(1)); // time-only -> null
        assert!(!ts_array.is_null(2)); // date-only -> valid
        assert!(ts_array.is_null(3)); // null input
        assert!(ts_array.is_null(4)); // invalid -> null
        assert!(!ts_array.is_null(5)); // TZ discarded -> valid
                                       // TZ discarded: "2020-06-15T12:30:00Z" should give same micros as "2020-06-15T12:30:00"
        assert_eq!(
            ts_array.value(5),
            timestamp_ntz_parser("2020-06-15T12:30:00", EvalMode::Legacy, true, false)
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn test_cast_string_to_timestamp_ntz_ansi_error() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("invalid")]));
        let result = cast_string_to_timestamp_ntz(&array, EvalMode::Ansi, true, false);
        assert!(result.is_err());
        match result.unwrap_err() {
            SparkError::InvalidInputInCastToDatetime { to_type, .. } => {
                assert_eq!(to_type, "TIMESTAMP_NTZ");
            }
            other => panic!("Expected InvalidInputInCastToDatetime, got {other:?}"),
        }
    }

    #[test]
    fn test_cast_dict_string_to_timestamp() -> DataFusionResult<()> {
        // prepare input data
        let keys = Int32Array::from(vec![0, 1]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("T2"),
        ]));
        let dict_array = Arc::new(DictionaryArray::new(keys, values));

        let timezone = "UTC".to_string();
        // test casting string dictionary array to timestamp array
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, &timezone, false);
        let result = cast_array(
            dict_array,
            &DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone().into())),
            &cast_options,
        )?;
        assert_eq!(
            *result.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into()))
        );
        assert_eq!(result.len(), 2);

        Ok(())
    }

    #[test]
    fn extreme_year_boundary_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // Long.MaxValue = 9223372036854775807 μs -> 294247-01-10T04:00:54.775807Z
        assert_eq!(
            timestamp_parser("294247-01-10T04:00:54.775807Z", EvalMode::Legacy, tz, true).unwrap(),
            Some(i64::MAX),
        );
        // Long.MinValue = -9223372036854775808 μs -> -290308-12-21T19:59:05.224192Z
        assert_eq!(
            timestamp_parser("-290308-12-21T19:59:05.224192Z", EvalMode::Legacy, tz, true).unwrap(),
            Some(i64::MIN),
        );
        // One beyond Long.MaxValue -> null (overflow)
        assert_eq!(
            timestamp_parser("294247-01-10T04:00:54.775808Z", EvalMode::Legacy, tz, true).unwrap(),
            None,
        );
        // One before Long.MinValue -> null (overflow)
        assert_eq!(
            timestamp_parser("-290308-12-21T19:59:05.224191Z", EvalMode::Legacy, tz, true).unwrap(),
            None,
        );
    }

    #[test]
    fn test_leading_whitespace_t_hm() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // Spark 4.0+ rejects leading whitespace for ALL T-prefixed time-only patterns.
        for ws_input in &[" T2:30", "\tT2:30", "\nT2:30", " T2", "\tT2", "\nT2"] {
            assert!(
                timestamp_parser(ws_input, EvalMode::Legacy, tz, true)
                    .unwrap()
                    .is_none(),
                "'{ws_input}' should be null in Legacy mode on Spark 4.0+"
            );
            // In ANSI mode the same inputs must raise an error (not silently return null).
            assert!(
                timestamp_parser(ws_input, EvalMode::Ansi, tz, true).is_err(),
                "'{ws_input}' should error in ANSI mode on Spark 4.0+"
            );
            // Spark 3.x trims all whitespace first, so leading whitespace is valid.
            assert!(
                timestamp_parser(ws_input, EvalMode::Legacy, tz, false)
                    .unwrap()
                    .is_some(),
                "'{ws_input}' should be valid in Legacy mode on Spark 3.x"
            );
        }
        // Without leading whitespace, these must be valid on all versions.
        for ok_input in &["T2:30", "T2"] {
            assert!(
                timestamp_parser(ok_input, EvalMode::Legacy, tz, true)
                    .unwrap()
                    .is_some(),
                "'{ok_input}' should be valid"
            );
        }
    }

    #[test]
    fn plus_sign_year_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // Spark accepts '+year' prefix on full date-time strings for TIMESTAMP casts.
        // "+2020-01-01T12:34:56" -> 2020-01-01T12:34:56 UTC = 1577882096 seconds.
        assert_eq!(
            timestamp_parser("+2020-01-01T12:34:56", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096000000),
            "+year on full datetime should parse the same as without the + prefix"
        );
        // But '+' on a time-only string is rejected (Spark returns null).
        assert_eq!(
            timestamp_parser("+12:12:12", EvalMode::Legacy, tz, true).unwrap(),
            None,
            "+hour:min:sec must return null"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn timestamp_parser_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // write for all formats
        assert_eq!(
            timestamp_parser("2020", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577836800000000) // this is in milliseconds
        );
        assert_eq!(
            timestamp_parser("2020-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577880000000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882040000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123456", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096123456)
        );
        assert_eq!(
            timestamp_parser("0100", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011416000000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011413960000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34:56", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011413904000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34:56.123456", EvalMode::Legacy, tz, true).unwrap(),
            Some(-59011413903876544)
        );
        assert_eq!(
            timestamp_parser("10000", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402344000000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402346040000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34:56", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402346096000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34:56.123456", EvalMode::Legacy, tz, true).unwrap(),
            Some(253402346096123456)
        );
        // Space separator (same values as T separator)
        assert_eq!(
            timestamp_parser("2020-01-01 12", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577880000000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01 12:34", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882040000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01 12:34:56", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01 12:34:56.123456", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096123456)
        );
        // Z suffix (UTC)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56Z", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096000000)
        );
        // Positive offset suffix
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+05:30", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577862296000000) // 12:34:56 UTC+5:30 = 07:04:56 UTC
        );
        // T-prefixed time-only with colon
        assert!(timestamp_parser("T12:34", EvalMode::Legacy, tz, true)
            .unwrap()
            .is_some());
        assert!(timestamp_parser("T12:34:56", EvalMode::Legacy, tz, true)
            .unwrap()
            .is_some());
        assert!(
            timestamp_parser("T12:34:56.123456", EvalMode::Legacy, tz, true)
                .unwrap()
                .is_some()
        );
        // Bare time-only (hour:minute without T prefix)
        assert!(timestamp_parser("12:34", EvalMode::Legacy, tz, true)
            .unwrap()
            .is_some());
        assert!(timestamp_parser("12:34:56", EvalMode::Legacy, tz, true)
            .unwrap()
            .is_some());
        // Negative year
        assert!(timestamp_parser("-0001", EvalMode::Legacy, tz, true)
            .unwrap()
            .is_some());
        assert!(
            timestamp_parser("-0001-01-01T12:34:56", EvalMode::Legacy, tz, true)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn timestamp_parser_fraction_scaling_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // Base: "2020-01-01T12:34:56" = 1577882096000000 µs (confirmed by timestamp_parser_test)
        let base = 1577882096000000i64;

        // 3-digit fraction: ".123" -> 123_000 µs
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 123_000)
        );
        // 1-digit fraction: ".1" -> 100_000 µs
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.1", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 100_000)
        );
        // 4-digit fraction: ".1000" -> 100_000 µs  (trailing zeros not extra precision)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.1000", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 100_000)
        );
        // 5-digit fraction: ".12312" -> 123_120 µs
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.12312", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 123_120)
        );
        // 6-digit fraction (exact): unchanged
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123456", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 123_456)
        );
        // >6 digits: truncated to 6
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123456789", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 123_456)
        );
        // Fraction after Z-stripped offset
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123Z", EvalMode::Legacy, tz, true).unwrap(),
            Some(base + 123_000)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn timestamp_parser_tz_offset_formats_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // All of these represent 2020-01-01T12:34:56 UTC = 1577882096000000 µs.
        let utc = 1577882096000000i64;
        // +05:30 offset -> UTC = 12:34:56 − 5h30m = 07:04:56 UTC = 1577862296000000 µs
        let plus530 = 1577862296000000i64;

        // +/-HHMM (no colon)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+0000", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+0530", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        // +/-H:MM (single-digit hour)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+5:30", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+0:00", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        // +/-H:M (single-digit both)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+5:3", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577863916000000) // 12:34:56 − 5h3m = 07:31:56 UTC = 1577836800+27116
        );
        // bare UTC / " UTC"
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56UTC", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 UTC", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        // UTC+offset
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 UTC+5:30", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        // UTC+0 (single-digit zero)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 UTC+0", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        // GMT+/-HH:MM (no space)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56GMT+00:00", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56GMT+05:30", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        // " GMT+/-..." (space-prefixed)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 GMT+05:30", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        // " GMT+/-HHMM" (space + GMT + no colon)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 GMT+0530", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        // " UT+/-HH:MM"
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 UT+05:30", EvalMode::Legacy, tz, true).unwrap(),
            Some(plus530)
        );
        // Bare "UT" (no leading space) — Spark accepts "UT" as a UTC alias.
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56UT", EvalMode::Legacy, tz, true).unwrap(),
            Some(utc)
        );
        // Java SHORT_IDS: EST (-05:00), MST (-07:00), HST (-10:00)
        // 2020-01-01T12:34:56 EST = 2020-01-01T17:34:56 UTC = 1577896496 s
        let est_utc = utc + 5 * 3600 * 1_000_000;
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 EST", EvalMode::Legacy, tz, true).unwrap(),
            Some(est_utc)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56EST", EvalMode::Legacy, tz, true).unwrap(),
            Some(est_utc)
        );
        // 2020-01-01T12:34:56 MST = 2020-01-01T19:34:56 UTC
        let mst_utc = utc + 7 * 3600 * 1_000_000;
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 MST", EvalMode::Legacy, tz, true).unwrap(),
            Some(mst_utc)
        );
        // 2020-01-01T12:34:56 HST = 2020-01-01T22:34:56 UTC
        let hst_utc = utc + 10 * 3600 * 1_000_000;
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56 HST", EvalMode::Legacy, tz, true).unwrap(),
            Some(hst_utc)
        );
        // Named IANA zone " Europe/Moscow" (UTC+3 in winter 2020)
        // 2020-01-01T12:34:56 Europe/Moscow = 2020-01-01T09:34:56 UTC = 1577871296000000 µs
        assert_eq!(
            timestamp_parser(
                "2020-01-01T12:34:56 Europe/Moscow",
                EvalMode::Legacy,
                tz,
                true
            )
            .unwrap(),
            Some(1577871296000000)
        );
        // Plain date strings must NOT be affected by the offset-extraction logic.
        assert_eq!(
            timestamp_parser("2020-01-01", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577836800000000)
        );
        // Invalid offset formats -> null
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56-8:", EvalMode::Legacy, tz, true).unwrap(),
            None
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56-20:0", EvalMode::Legacy, tz, true).unwrap(),
            None // h=20 > 18 invalid
        );
        // Positive year-sign prefix is accepted for timestamps (see plus_sign_year_test)
        assert_eq!(
            timestamp_parser("+2020-01-01T12:34:56", EvalMode::Legacy, tz, true).unwrap(),
            Some(1577882096000000)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn timestamp_parser_dst_test() {
        // DST spring-forward: America/New_York springs forward 2020-03-08 02:00 -> 03:00.
        // 02:30 does not exist; Spark advances to 03:30 EDT (UTC-4) = 07:30 UTC.
        // 2020-03-08T07:30:00Z = 1577836800 + 67*86400 + 27000 = 1583652600 seconds.
        let ny_tz = &timezone::Tz::from_str("America/New_York").unwrap();
        assert_eq!(
            timestamp_parser("2020-03-08 02:30:00", EvalMode::Legacy, ny_tz, true).unwrap(),
            Some(1583652600000000)
        );
        // Just before gap: 01:59:59 EST (UTC-5) = 06:59:59 UTC = 1583650799 seconds.
        assert_eq!(
            timestamp_parser("2020-03-08 01:59:59", EvalMode::Legacy, ny_tz, true).unwrap(),
            Some(1583650799000000)
        );
        // Just after gap: 03:00:00 EDT (UTC-4) = 07:00:00 UTC = 1583650800 seconds.
        assert_eq!(
            timestamp_parser("2020-03-08 03:00:00", EvalMode::Legacy, ny_tz, true).unwrap(),
            Some(1583650800000000)
        );

        // DST fall-back: 2020-11-01 02:00 EDT -> 01:00 EST. Ambiguous: [01:00, 02:00).
        // Spark picks the earlier UTC instant (pre-transition = EDT = UTC-4).
        // 01:30 EDT (UTC-4) = 05:30 UTC.
        // 2020-11-01 = 2020-01-01 + 305 days = 1577836800 + 305*86400 = 1604188800 seconds.
        assert_eq!(
            timestamp_parser("2020-11-01 01:30:00", EvalMode::Legacy, ny_tz, true).unwrap(),
            Some(1604208600000000) // 1604188800 + 5*3600 + 30*60 = 1604208600
        );
    }

    #[test]
    fn date_parser_test() {
        for date in &[
            "2020",
            "2020-01",
            "2020-01-01",
            "+2020-01-01", // Spark accepts '+' year prefix on dates
            "02020-01-01",
            "002020-01-01",
            "0002020-01-01",
            "2020-1-1",
            "2020-01-01 ",
            "2020-01-01T",
        ] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), Some(18262));
            }
        }

        //dates in invalid formats
        for date in &[
            "abc",
            "",
            "not_a_date",
            "3/",
            "3/12",
            "3/12/2020",
            "3/12/2002 T",
            "202",
            "2020-010-01",
            "2020-10-010",
            "2020-10-010T",
            "--262143-12-31",
            "--262143-12-31 ",
        ] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), None);
            }
            assert!(date_parser(date, EvalMode::Ansi).is_err());
        }

        for date in &["-3638-5"] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), Some(-2048160));
            }
        }

        //Naive Date only supports years 262142 AD to 262143 BC
        //returns None for dates out of range supported by Naive Date.
        for date in &[
            "-262144-1-1",
            "262143-01-1",
            "262143-1-1",
            "262143-01-1 ",
            "262143-01-01T ",
            "262143-1-01T 1234",
            "-0973250",
        ] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), None);
            }
        }
    }

    #[test]
    fn test_cast_string_to_date() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020"),
            Some("2020-01"),
            Some("2020-01-01"),
            Some("2020-01-01T"),
        ]));

        let result = cast_string_to_date(&array, &DataType::Date32, EvalMode::Legacy).unwrap();

        let date32_array = result
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(date32_array.len(), 4);
        date32_array
            .iter()
            .for_each(|v| assert_eq!(v.unwrap(), 18262));
    }

    #[test]
    fn test_cast_string_array_with_valid_dates() {
        let array_with_invalid_date: ArrayRef = Arc::new(StringArray::from(vec![
            Some("-262143-12-31"),
            Some("\n -262143-12-31 "),
            Some("-262143-12-31T \t\n"),
            Some("\n\t-262143-12-31T\r"),
            Some("-262143-12-31T 123123123"),
            Some("\r\n-262143-12-31T \r123123123"),
            Some("\n -262143-12-31T \n\t"),
        ]));

        for eval_mode in &[EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
            let result =
                cast_string_to_date(&array_with_invalid_date, &DataType::Date32, *eval_mode)
                    .unwrap();

            let date32_array = result
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap();
            assert_eq!(result.len(), 7);
            date32_array
                .iter()
                .for_each(|v| assert_eq!(v.unwrap(), -96464928));
        }
    }

    #[test]
    fn test_cast_string_array_with_invalid_dates() {
        let array_with_invalid_date: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020"),
            Some("2020-01"),
            Some("2020-01-01"),
            //4 invalid dates
            Some("2020-010-01T"),
            Some("202"),
            Some(" 202 "),
            Some("\n 2020-\r8 "),
            Some("2020-01-01T"),
            // Overflows i32
            Some("-4607172990231812908"),
        ]));

        for eval_mode in &[EvalMode::Legacy, EvalMode::Try] {
            let result =
                cast_string_to_date(&array_with_invalid_date, &DataType::Date32, *eval_mode)
                    .unwrap();

            let date32_array = result
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap();
            assert_eq!(
                date32_array.iter().collect::<Vec<_>>(),
                vec![
                    Some(18262),
                    Some(18262),
                    Some(18262),
                    None,
                    None,
                    None,
                    None,
                    Some(18262),
                    None
                ]
            );
        }

        let result =
            cast_string_to_date(&array_with_invalid_date, &DataType::Date32, EvalMode::Ansi);
        match result {
            Err(e) => assert!(
                e.to_string().contains(
                    "[CAST_INVALID_INPUT] The value '2020-010-01T' of the type \"STRING\" cannot be cast to \"DATE\" because it is malformed")
            ),
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_cast_string_as_i8() {
        // basic
        assert_eq!(
            cast_string_to_i8("127", EvalMode::Legacy).unwrap(),
            Some(127_i8)
        );
        assert_eq!(cast_string_to_i8("128", EvalMode::Legacy).unwrap(), None);
        assert!(cast_string_to_i8("128", EvalMode::Ansi).is_err());
        // decimals
        assert_eq!(
            cast_string_to_i8("0.2", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        assert_eq!(
            cast_string_to_i8(".", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        // TRY should always return null for decimals
        assert_eq!(cast_string_to_i8("0.2", EvalMode::Try).unwrap(), None);
        assert_eq!(cast_string_to_i8(".", EvalMode::Try).unwrap(), None);
        // ANSI mode should throw error on decimal
        assert!(cast_string_to_i8("0.2", EvalMode::Ansi).is_err());
        assert!(cast_string_to_i8(".", EvalMode::Ansi).is_err());
    }
}
