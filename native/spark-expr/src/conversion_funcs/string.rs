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
use chrono::{DateTime, NaiveDate, TimeZone, Timelike};
use num::traits::CheckedNeg;
use num::{CheckedSub, Integer};
use regex::Regex;
use std::num::Wrapping;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

macro_rules! cast_utf8_to_timestamp {
    // $tz is a Timezone:Tz object and contains the session timezone.
    // $to_tz_str is a string containing the to_type timezone
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident, $tz:expr, $to_tz_str:expr) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len).with_timezone($to_tz_str);
        let mut cast_err: Option<SparkError> = None;
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else {
                match $cast_method($array.value(i).trim(), $eval_mode, $tz) {
                    Ok(Some(cast_value)) => cast_array.append_value(cast_value),
                    Ok(None) => cast_array.append_null(),
                    Err(e) => {
                        if $eval_mode == EvalMode::Ansi {
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

/// Parse a decimal string into mantissa and scale
/// e.g., "123.45" -> (12345, 2), "-0.001" -> (-1, 3) , 0e50 -> (0,50) etc
/// Parse a string to decimal following Spark's behavior
fn parse_string_to_decimal(input_str: &str, precision: u8, scale: i8) -> SparkResult<Option<i128>> {
    let string_bytes = input_str.as_bytes();
    let mut start = 0;
    let mut end = string_bytes.len();

    // trim whitespaces
    while start < end && string_bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && string_bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }

    let trimmed = &input_str[start..end];

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
                to_tz
            )?
        }
        _ => unreachable!("Invalid data type {:?} in cast from string", to_type),
    };
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

    // NaiveDate (used internally by chrono's with_ymd_and_hms) is bounded to ±262142.
    if !(-262143..=262142).contains(&year) {
        return Ok(None);
    }

    let month = parts.next().map_or(1, |m| m.parse::<u32>().unwrap_or(1));
    let day = parts.next().map_or(1, |d| d.parse::<u32>().unwrap_or(1));
    let hour = parts.next().map_or(0, |h| h.parse::<u32>().unwrap_or(0));
    let minute = parts.next().map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = parts.next().map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let microsecond = parts.next().map_or(0, |ms| ms.parse::<u32>().unwrap_or(0));

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

fn parse_timestamp_to_micros<T: TimeZone>(
    timestamp_info: &TimeStampInfo,
    tz: &T,
) -> SparkResult<Option<i64>> {
    let datetime = tz.with_ymd_and_hms(
        timestamp_info.year,
        timestamp_info.month,
        timestamp_info.day,
        timestamp_info.hour,
        timestamp_info.minute,
        timestamp_info.second,
    );

    // Spark uses the offset before daylight savings change so we need to use earliest()
    // Return None for LocalResult::None which is the invalid time in a DST spring forward gap).
    let tz_datetime = match datetime.earliest() {
        Some(dt) => dt
            .with_timezone(tz)
            .with_nanosecond(timestamp_info.microsecond * 1000),
        None => return Ok(None),
    };

    match tz_datetime {
        Some(dt) => Ok(Some(dt.timestamp_micros())),
        None => Ok(None),
    }
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
) -> SparkResult<Option<i64>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    // Handle Z or ±HH:MM offset suffix: strip it and parse with the explicit fixed offset.
    if let Some((stripped, offset_secs)) = extract_offset_suffix(value) {
        let fixed_tz = chrono::FixedOffset::east_opt(offset_secs)
            .ok_or_else(|| SparkError::Internal("Invalid timezone offset".to_string()))?;
        return timestamp_parser_with_tz(stripped, eval_mode, &fixed_tz);
    }

    timestamp_parser_with_tz(value, eval_mode, tz)
}

/// If `value` ends with a UTC offset suffix (`Z`, `+HH:MM`, or `-HH:MM`), returns the
/// stripped string and the offset in seconds. Returns `None` if no offset suffix is present.
fn extract_offset_suffix(value: &str) -> Option<(&str, i32)> {
    if let Some(stripped) = value.strip_suffix('Z') {
        return Some((stripped, 0));
    }
    // Check for ±HH:MM at the end (exactly 6 chars: sign + 2 digits + ':' + 2 digits)
    if value.len() >= 6 {
        let suffix_start = value.len() - 6;
        let suffix = &value[suffix_start..];
        let sign_byte = suffix.as_bytes()[0];
        if (sign_byte == b'+' || sign_byte == b'-') && suffix.as_bytes()[3] == b':' {
            if let (Ok(h), Ok(m)) = (suffix[1..3].parse::<i32>(), suffix[4..6].parse::<i32>()) {
                let sign = if sign_byte == b'+' { 1i32 } else { -1i32 };
                return Some((&value[..suffix_start], sign * (h * 3600 + m * 60)));
            }
        }
    }
    None
}

type TimestampParsePattern<T> = (&'static Regex, fn(&str, &T) -> SparkResult<Option<i64>>);

static RE_YEAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?\d{4,7}$").unwrap());
static RE_MONTH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}$").unwrap());
static RE_DAY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}$").unwrap());
static RE_HOUR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{1,2}$").unwrap());
static RE_MINUTE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{2}:\d{2}$").unwrap());
static RE_SECOND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$").unwrap());
static RE_MICROSECOND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?\d{4,7}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap());
static RE_TIME_ONLY_H: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^T\d{1,2}$").unwrap());
static RE_TIME_ONLY_HM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}:\d{2}$").unwrap());
static RE_TIME_ONLY_HMS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}:\d{2}:\d{2}$").unwrap());
static RE_TIME_ONLY_HMSU: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}:\d{2}:\d{2}\.\d{1,6}$").unwrap());
static RE_BARE_HM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{1,2}:\d{2}$").unwrap());
static RE_BARE_HMS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{1,2}:\d{2}:\d{2}$").unwrap());
static RE_BARE_HMSU: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{1,2}:\d{2}:\d{2}\.\d{1,6}$").unwrap());

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

    //assign a sign to the date
    if bytes[j] == b'-' || bytes[j] == b'+' {
        sign = if bytes[j] == b'-' { -1 } else { 1 };
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
            "UTC"
        )
        .unwrap();

        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(result.len(), 4);
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
            "UTC"
        );
        assert!(
            result.is_err(),
            "ANSI mode should return Err for an invalid timestamp string"
        );
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
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn timestamp_parser_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // write for all formats
        assert_eq!(
            timestamp_parser("2020", EvalMode::Legacy, tz).unwrap(),
            Some(1577836800000000) // this is in milliseconds
        );
        assert_eq!(
            timestamp_parser("2020-01", EvalMode::Legacy, tz).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01", EvalMode::Legacy, tz).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12", EvalMode::Legacy, tz).unwrap(),
            Some(1577880000000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34", EvalMode::Legacy, tz).unwrap(),
            Some(1577882040000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096123456)
        );
        assert_eq!(
            timestamp_parser("0100", EvalMode::Legacy, tz).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01", EvalMode::Legacy, tz).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01", EvalMode::Legacy, tz).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12", EvalMode::Legacy, tz).unwrap(),
            Some(-59011416000000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34", EvalMode::Legacy, tz).unwrap(),
            Some(-59011413960000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(-59011413904000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(-59011413903876544)
        );
        assert_eq!(
            timestamp_parser("10000", EvalMode::Legacy, tz).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01", EvalMode::Legacy, tz).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01", EvalMode::Legacy, tz).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12", EvalMode::Legacy, tz).unwrap(),
            Some(253402344000000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34", EvalMode::Legacy, tz).unwrap(),
            Some(253402346040000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(253402346096000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(253402346096123456)
        );
        // Space separator (same values as T separator)
        assert_eq!(
            timestamp_parser("2020-01-01 12", EvalMode::Legacy, tz).unwrap(),
            Some(1577880000000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01 12:34", EvalMode::Legacy, tz).unwrap(),
            Some(1577882040000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01 12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01 12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096123456)
        );
        // Z suffix (UTC)
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56Z", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096000000)
        );
        // Positive offset suffix
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56+05:30", EvalMode::Legacy, tz).unwrap(),
            Some(1577862296000000) // 12:34:56 UTC+5:30 = 07:04:56 UTC
        );
        // T-prefixed time-only with colon
        assert!(timestamp_parser("T12:34", EvalMode::Legacy, tz)
            .unwrap()
            .is_some());
        assert!(timestamp_parser("T12:34:56", EvalMode::Legacy, tz)
            .unwrap()
            .is_some());
        assert!(timestamp_parser("T12:34:56.123456", EvalMode::Legacy, tz)
            .unwrap()
            .is_some());
        // Bare time-only (hour:minute without T prefix)
        assert!(timestamp_parser("12:34", EvalMode::Legacy, tz)
            .unwrap()
            .is_some());
        assert!(timestamp_parser("12:34:56", EvalMode::Legacy, tz)
            .unwrap()
            .is_some());
        // Negative year
        assert!(timestamp_parser("-0001", EvalMode::Legacy, tz)
            .unwrap()
            .is_some());
        assert!(
            timestamp_parser("-0001-01-01T12:34:56", EvalMode::Legacy, tz)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn date_parser_test() {
        for date in &[
            "2020",
            "2020-01",
            "2020-01-01",
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
