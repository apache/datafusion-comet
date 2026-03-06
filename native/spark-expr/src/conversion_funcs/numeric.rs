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

use crate::conversion_funcs::utils::cast_overflow;
use crate::conversion_funcs::utils::MICROS_PER_SECOND;
use crate::{EvalMode, SparkError, SparkResult};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Decimal128Array, Decimal128Builder, Float32Array,
    Float64Array, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array,
    OffsetSizeTrait, PrimitiveArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    i256, is_validate_decimal_precision, ArrowPrimitiveType, DataType, Decimal128Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use num::{cast::AsPrimitive, ToPrimitive, Zero};
use std::sync::Arc;

/// Check if DataFusion cast from integer types is Spark compatible
pub(crate) fn is_df_cast_from_int_spark_compatible(to_type: &DataType) -> bool {
    matches!(
        to_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
    )
}

/// Check if DataFusion cast from float types is Spark compatible
pub(crate) fn is_df_cast_from_float_spark_compatible(to_type: &DataType) -> bool {
    matches!(
        to_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Check if DataFusion cast from decimal types is Spark compatible
pub(crate) fn is_df_cast_from_decimal_spark_compatible(to_type: &DataType) -> bool {
    matches!(
        to_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8 // note that there can be formatting differences
    )
}

macro_rules! cast_float_to_timestamp_impl {
    ($array:expr, $builder:expr, $primitive_type:ty, $eval_mode:expr) => {{
        let arr = $array.as_primitive::<$primitive_type>();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                $builder.append_null();
            } else {
                let val = arr.value(i) as f64;
                // Path 1: NaN/Infinity check - error says TIMESTAMP
                if val.is_nan() || val.is_infinite() {
                    if $eval_mode == EvalMode::Ansi {
                        return Err(SparkError::CastInvalidValue {
                            value: val.to_string(),
                            from_type: "DOUBLE".to_string(),
                            to_type: "TIMESTAMP".to_string(),
                        });
                    }
                    $builder.append_null();
                } else {
                    // Path 2: Multiply then check overflow - error says BIGINT
                    let micros = val * MICROS_PER_SECOND as f64;
                    if micros.floor() <= i64::MAX as f64 && micros.ceil() >= i64::MIN as f64 {
                        $builder.append_value(micros as i64);
                    } else {
                        if $eval_mode == EvalMode::Ansi {
                            let value_str = if micros.is_infinite() {
                                if micros.is_sign_positive() {
                                    "Infinity".to_string()
                                } else {
                                    "-Infinity".to_string()
                                }
                            } else if micros.is_nan() {
                                "NaN".to_string()
                            } else {
                                format!("{:e}", micros).to_uppercase() + "D"
                            };
                            return Err(SparkError::CastOverFlow {
                                value: value_str,
                                from_type: "DOUBLE".to_string(),
                                to_type: "BIGINT".to_string(),
                            });
                        }
                        $builder.append_null();
                    }
                }
            }
        }
    }};
}

macro_rules! cast_float_to_string {
    ($from:expr, $eval_mode:expr, $type:ty, $output_type:ty, $offset_type:ty) => {{

        fn cast<OffsetSize>(
            from: &dyn Array,
            _eval_mode: EvalMode,
        ) -> SparkResult<ArrayRef>
        where
            OffsetSize: OffsetSizeTrait, {
                let array = from.as_any().downcast_ref::<$output_type>().unwrap();

                // If the absolute number is less than 10,000,000 and greater or equal than 0.001, the
                // result is expressed without scientific notation with at least one digit on either side of
                // the decimal point. Otherwise, Spark uses a mantissa followed by E and an
                // exponent. The mantissa has an optional leading minus sign followed by one digit to the
                // left of the decimal point, and the minimal number of digits greater than zero to the
                // right. The exponent has and optional leading minus sign.
                // source: https://docs.databricks.com/en/sql/language-manual/functions/cast.html

                const LOWER_SCIENTIFIC_BOUND: $type = 0.001;
                const UPPER_SCIENTIFIC_BOUND: $type = 10000000.0;

                let output_array = array
                    .iter()
                    .map(|value| match value {
                        Some(value) if value == <$type>::INFINITY => Ok(Some("Infinity".to_string())),
                        Some(value) if value == <$type>::NEG_INFINITY => Ok(Some("-Infinity".to_string())),
                        Some(value)
                            if (value.abs() < UPPER_SCIENTIFIC_BOUND
                                && value.abs() >= LOWER_SCIENTIFIC_BOUND)
                                || value.abs() == 0.0 =>
                        {
                            let trailing_zero = if value.fract() == 0.0 { ".0" } else { "" };

                            Ok(Some(format!("{value}{trailing_zero}")))
                        }
                        Some(value)
                            if value.abs() >= UPPER_SCIENTIFIC_BOUND
                                || value.abs() < LOWER_SCIENTIFIC_BOUND =>
                        {
                            let formatted = format!("{value:E}");

                            if formatted.contains(".") {
                                Ok(Some(formatted))
                            } else {
                                // `formatted` is already in scientific notation and can be split up by E
                                // in order to add the missing trailing 0 which gets removed for numbers with a fraction of 0.0
                                let prepare_number: Vec<&str> = formatted.split("E").collect();

                                let coefficient = prepare_number[0];

                                let exponent = prepare_number[1];

                                Ok(Some(format!("{coefficient}.0E{exponent}")))
                            }
                        }
                        Some(value) => Ok(Some(value.to_string())),
                        _ => Ok(None),
                    })
                    .collect::<Result<GenericStringArray<OffsetSize>, SparkError>>()?;

                Ok(Arc::new(output_array))
            }

        cast::<$offset_type>($from, $eval_mode)
    }};
}

// eval mode is not needed since all ints can be implemented in binary format
#[macro_export]
macro_rules! cast_whole_num_to_binary {
    ($array:expr, $primitive_type:ty, $byte_size:expr) => {{
        let input_arr = $array
            .as_any()
            .downcast_ref::<$primitive_type>()
            .ok_or_else(|| SparkError::Internal("Expected numeric array".to_string()))?;

        let len = input_arr.len();
        let mut builder = BinaryBuilder::with_capacity(len, len * $byte_size);

        for i in 0..input_arr.len() {
            if input_arr.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(input_arr.value(i).to_be_bytes());
            }
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

macro_rules! cast_int_to_timestamp_impl {
    ($array:expr, $builder:expr, $primitive_type:ty) => {{
        let arr = $array.as_primitive::<$primitive_type>();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                $builder.append_null();
            } else {
                // saturating_mul limits to i64::MIN/MAX on overflow instead of panicking,
                // which could occur when converting extreme values (e.g., Long.MIN_VALUE)
                // matching spark behavior (irrespective of EvalMode)
                let micros = (arr.value(i) as i64).saturating_mul(MICROS_PER_SECOND);
                $builder.append_value(micros);
            }
        }
    }};
}

macro_rules! cast_int_to_int_macro {
    (
        $array: expr,
        $eval_mode:expr,
        $from_arrow_primitive_type: ty,
        $to_arrow_primitive_type: ty,
        $from_data_type: expr,
        $to_native_type: ty,
        $spark_from_data_type_name: expr,
        $spark_to_data_type_name: expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$from_arrow_primitive_type>>()
            .unwrap();
        let spark_int_literal_suffix = match $from_data_type {
            &DataType::Int64 => "L",
            &DataType::Int16 => "S",
            &DataType::Int8 => "T",
            _ => "",
        };

        let output_array = match $eval_mode {
            EvalMode::Legacy => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        Ok::<Option<$to_native_type>, SparkError>(Some(value as $to_native_type))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>, _>>(),
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let res = <$to_native_type>::try_from(value);
                        if res.is_err() {
                            Err(cast_overflow(
                                &(value.to_string() + spark_int_literal_suffix),
                                $spark_from_data_type_name,
                                $spark_to_data_type_name,
                            ))
                        } else {
                            Ok::<Option<$to_native_type>, SparkError>(Some(res.unwrap()))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>, _>>(),
        }?;
        let result: SparkResult<ArrayRef> = Ok(Arc::new(output_array) as ArrayRef);
        result
    }};
}

// When Spark casts to Byte/Short Types, it does not cast directly to Byte/Short.
// It casts to Int first and then to Byte/Short. Because of potential overflows in the Int cast,
// this can cause unexpected Short/Byte cast results. Replicate this behavior.
macro_rules! cast_float_to_int16_down {
    (
        $array:expr,
        $eval_mode:expr,
        $src_array_type:ty,
        $dest_array_type:ty,
        $rust_src_type:ty,
        $rust_dest_type:ty,
        $src_type_str:expr,
        $dest_type_str:expr,
        $format_str:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<$src_array_type>()
            .expect(concat!("Expected a ", stringify!($src_array_type)));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let is_overflow = value.is_nan() || value.abs() as i32 == i32::MAX;
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!($format_str, value).replace("e", "E"),
                                $src_type_str,
                                $dest_type_str,
                            ));
                        }
                        let i32_value = value as i32;
                        <$rust_dest_type>::try_from(i32_value)
                            .map_err(|_| {
                                cast_overflow(
                                    &format!($format_str, value).replace("e", "E"),
                                    $src_type_str,
                                    $dest_type_str,
                                )
                            })
                            .map(Some)
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let i32_value = value as i32;
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(
                            i32_value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

macro_rules! cast_float_to_int32_up {
    (
        $array:expr,
        $eval_mode:expr,
        $src_array_type:ty,
        $dest_array_type:ty,
        $rust_src_type:ty,
        $rust_dest_type:ty,
        $src_type_str:expr,
        $dest_type_str:expr,
        $max_dest_val:expr,
        $format_str:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<$src_array_type>()
            .expect(concat!("Expected a ", stringify!($src_array_type)));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let is_overflow =
                            value.is_nan() || value.abs() as $rust_dest_type == $max_dest_val;
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!($format_str, value).replace("e", "E"),
                                $src_type_str,
                                $dest_type_str,
                            ));
                        }
                        Ok(Some(value as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(value as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

// When Spark casts to Byte/Short Types, it does not cast directly to Byte/Short.
// It casts to Int first and then to Byte/Short. Because of potential overflows in the Int cast,
// this can cause unexpected Short/Byte cast results. Replicate this behavior.
macro_rules! cast_decimal_to_int16_down {
    (
        $array:expr,
        $eval_mode:expr,
        $dest_array_type:ty,
        $rust_dest_type:ty,
        $dest_type_str:expr,
        $precision:expr,
        $scale:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Expected a Decimal128ArrayType");

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        let is_overflow = truncated.abs() > i32::MAX.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!(
                                    "{}BD",
                                    format_decimal_str(
                                        &value.to_string(),
                                        $precision as usize,
                                        $scale
                                    )
                                ),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        let i32_value = truncated as i32;
                        <$rust_dest_type>::try_from(i32_value)
                            .map_err(|_| {
                                cast_overflow(
                                    &format!(
                                        "{}BD",
                                        format_decimal_str(
                                            &value.to_string(),
                                            $precision as usize,
                                            $scale
                                        )
                                    ),
                                    &format!("DECIMAL({},{})", $precision, $scale),
                                    $dest_type_str,
                                )
                            })
                            .map(Some)
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let i32_value = (value / divisor) as i32;
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(
                            i32_value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

macro_rules! cast_decimal_to_int32_up {
    (
        $array:expr,
        $eval_mode:expr,
        $dest_array_type:ty,
        $rust_dest_type:ty,
        $dest_type_str:expr,
        $max_dest_val:expr,
        $precision:expr,
        $scale:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Expected a Decimal128ArrayType");

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        let is_overflow = truncated.abs() > $max_dest_val.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!(
                                    "{}BD",
                                    format_decimal_str(
                                        &value.to_string(),
                                        $precision as usize,
                                        $scale
                                    )
                                ),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        Ok(Some(truncated as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(
                            truncated as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

// copied from arrow::dataTypes::Decimal128Type since Decimal128Type::format_decimal can't be called directly
pub(crate) fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
    let (sign, rest) = match value_str.strip_prefix('-') {
        Some(stripped) => ("-", stripped),
        None => ("", value_str),
    };
    let bound = precision.min(rest.len()) + sign.len();
    let value_str = &value_str[0..bound];

    if scale == 0 {
        value_str.to_string()
    } else if scale < 0 {
        let padding = value_str.len() + scale.unsigned_abs() as usize;
        format!("{value_str:0<padding$}")
    } else if rest.len() > scale as usize {
        // Decimal separator is in the middle of the string
        let (whole, decimal) = value_str.split_at(value_str.len() - scale as usize);
        format!("{whole}.{decimal}")
    } else {
        // String has to be padded
        format!("{}0.{:0>width$}", sign, rest, width = scale as usize)
    }
}

pub(crate) fn spark_cast_float64_to_utf8<OffsetSize>(
    from: &dyn Array,
    _eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, _eval_mode, f64, Float64Array, OffsetSize)
}

pub(crate) fn spark_cast_float32_to_utf8<OffsetSize>(
    from: &dyn Array,
    _eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, _eval_mode, f32, Float32Array, OffsetSize)
}

fn cast_int_to_decimal128_internal<T>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let mut builder = Decimal128Builder::with_capacity(array.len());
    let multiplier = 10_i128.pow(scale as u32);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let v = array.value(i).into();
            let scaled = v.checked_mul(multiplier);
            match scaled {
                Some(scaled) => {
                    if !is_validate_decimal_precision(scaled, precision) {
                        match eval_mode {
                            EvalMode::Ansi => {
                                return Err(SparkError::NumericValueOutOfRange {
                                    value: v.to_string(),
                                    precision,
                                    scale,
                                });
                            }
                            EvalMode::Try | EvalMode::Legacy => builder.append_null(),
                        }
                    } else {
                        builder.append_value(scaled);
                    }
                }
                _ => match eval_mode {
                    EvalMode::Ansi => {
                        return Err(SparkError::NumericValueOutOfRange {
                            value: v.to_string(),
                            precision,
                            scale,
                        })
                    }
                    EvalMode::Legacy | EvalMode::Try => builder.append_null(),
                },
            }
        }
    }
    Ok(Arc::new(
        builder.with_precision_and_scale(precision, scale)?.finish(),
    ))
}

pub(crate) fn cast_int_to_decimal128(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
    precision: u8,
    scale: i8,
) -> SparkResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Int8, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int8Type>(
                array.as_primitive::<Int8Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        (DataType::Int16, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int16Type>(
                array.as_primitive::<Int16Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        (DataType::Int32, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int32Type>(
                array.as_primitive::<Int32Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        (DataType::Int64, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int64Type>(
                array.as_primitive::<Int64Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        _ => Err(SparkError::Internal(format!(
            "Unsupported cast from datatype : {}",
            from_type
        ))),
    }
}

pub(crate) fn spark_cast_int_to_int(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
) -> SparkResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Int64, DataType::Int32) => cast_int_to_int_macro!(
            array, eval_mode, Int64Type, Int32Type, from_type, i32, "BIGINT", "INT"
        ),
        (DataType::Int64, DataType::Int16) => cast_int_to_int_macro!(
            array, eval_mode, Int64Type, Int16Type, from_type, i16, "BIGINT", "SMALLINT"
        ),
        (DataType::Int64, DataType::Int8) => cast_int_to_int_macro!(
            array, eval_mode, Int64Type, Int8Type, from_type, i8, "BIGINT", "TINYINT"
        ),
        (DataType::Int32, DataType::Int16) => cast_int_to_int_macro!(
            array, eval_mode, Int32Type, Int16Type, from_type, i16, "INT", "SMALLINT"
        ),
        (DataType::Int32, DataType::Int8) => cast_int_to_int_macro!(
            array, eval_mode, Int32Type, Int8Type, from_type, i8, "INT", "TINYINT"
        ),
        (DataType::Int16, DataType::Int8) => cast_int_to_int_macro!(
            array, eval_mode, Int16Type, Int8Type, from_type, i8, "SMALLINT", "TINYINT"
        ),
        _ => unreachable!(
            "{}",
            format!("invalid integer type {to_type} in cast from {from_type}")
        ),
    }
}

pub(crate) fn spark_cast_decimal_to_boolean(array: &dyn Array) -> SparkResult<ArrayRef> {
    let decimal_array = array.as_primitive::<Decimal128Type>();
    let mut result = BooleanBuilder::with_capacity(decimal_array.len());
    for i in 0..decimal_array.len() {
        if decimal_array.is_null(i) {
            result.append_null()
        } else {
            result.append_value(!decimal_array.value(i).is_zero());
        }
    }
    Ok(Arc::new(result.finish()))
}

pub(crate) fn cast_float64_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    cast_floating_point_to_decimal128::<Float64Type>(array, precision, scale, eval_mode)
}

pub(crate) fn cast_float32_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    cast_floating_point_to_decimal128::<Float32Type>(array, precision, scale, eval_mode)
}

fn cast_floating_point_to_decimal128<T: ArrowPrimitiveType>(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let input = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let mut cast_array = PrimitiveArray::<Decimal128Type>::builder(input.len());

    let mul = 10_f64.powi(scale as i32);

    for i in 0..input.len() {
        if input.is_null(i) {
            cast_array.append_null();
            continue;
        }

        let input_value = input.value(i).as_();
        if let Some(v) = (input_value * mul).round().to_i128() {
            if is_validate_decimal_precision(v, precision) {
                cast_array.append_value(v);
                continue;
            }
        };

        if eval_mode == EvalMode::Ansi {
            return Err(SparkError::NumericValueOutOfRange {
                value: input_value.to_string(),
                precision,
                scale,
            });
        }
        cast_array.append_null();
    }

    let res = Arc::new(
        cast_array
            .with_precision_and_scale(precision, scale)?
            .finish(),
    ) as ArrayRef;
    Ok(res)
}

pub(crate) fn spark_cast_nonintegral_numeric_to_integral(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
) -> SparkResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Float32, DataType::Int8) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float32Array,
            Int8Array,
            f32,
            i8,
            "FLOAT",
            "TINYINT",
            "{:e}"
        ),
        (DataType::Float32, DataType::Int16) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float32Array,
            Int16Array,
            f32,
            i16,
            "FLOAT",
            "SMALLINT",
            "{:e}"
        ),
        (DataType::Float32, DataType::Int32) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float32Array,
            Int32Array,
            f32,
            i32,
            "FLOAT",
            "INT",
            i32::MAX,
            "{:e}"
        ),
        (DataType::Float32, DataType::Int64) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float32Array,
            Int64Array,
            f32,
            i64,
            "FLOAT",
            "BIGINT",
            i64::MAX,
            "{:e}"
        ),
        (DataType::Float64, DataType::Int8) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float64Array,
            Int8Array,
            f64,
            i8,
            "DOUBLE",
            "TINYINT",
            "{:e}D"
        ),
        (DataType::Float64, DataType::Int16) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float64Array,
            Int16Array,
            f64,
            i16,
            "DOUBLE",
            "SMALLINT",
            "{:e}D"
        ),
        (DataType::Float64, DataType::Int32) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float64Array,
            Int32Array,
            f64,
            i32,
            "DOUBLE",
            "INT",
            i32::MAX,
            "{:e}D"
        ),
        (DataType::Float64, DataType::Int64) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float64Array,
            Int64Array,
            f64,
            i64,
            "DOUBLE",
            "BIGINT",
            i64::MAX,
            "{:e}D"
        ),
        (DataType::Decimal128(precision, scale), DataType::Int8) => {
            cast_decimal_to_int16_down!(
                array, eval_mode, Int8Array, i8, "TINYINT", *precision, *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int16) => {
            cast_decimal_to_int16_down!(
                array, eval_mode, Int16Array, i16, "SMALLINT", *precision, *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int32) => {
            cast_decimal_to_int32_up!(
                array,
                eval_mode,
                Int32Array,
                i32,
                "INT",
                i32::MAX,
                *precision,
                *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int64) => {
            cast_decimal_to_int32_up!(
                array,
                eval_mode,
                Int64Array,
                i64,
                "BIGINT",
                i64::MAX,
                *precision,
                *scale
            )
        }
        _ => unreachable!(
            "{}",
            format!("invalid cast from non-integral numeric type: {from_type} to integral numeric type: {to_type}")
        ),
    }
}

pub(crate) fn cast_int_to_timestamp(
    array_ref: &ArrayRef,
    target_tz: &Option<Arc<str>>,
) -> SparkResult<ArrayRef> {
    // Input is seconds since epoch, multiply by MICROS_PER_SECOND to get microseconds.
    let mut builder = TimestampMicrosecondBuilder::with_capacity(array_ref.len());

    match array_ref.data_type() {
        DataType::Int8 => cast_int_to_timestamp_impl!(array_ref, builder, Int8Type),
        DataType::Int16 => cast_int_to_timestamp_impl!(array_ref, builder, Int16Type),
        DataType::Int32 => cast_int_to_timestamp_impl!(array_ref, builder, Int32Type),
        DataType::Int64 => cast_int_to_timestamp_impl!(array_ref, builder, Int64Type),
        dt => {
            return Err(SparkError::Internal(format!(
                "Unsupported type for cast_int_to_timestamp: {:?}",
                dt
            )))
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(target_tz.clone())) as ArrayRef)
}

pub(crate) fn cast_decimal_to_timestamp(
    array_ref: &ArrayRef,
    target_tz: &Option<Arc<str>>,
    scale: i8,
) -> SparkResult<ArrayRef> {
    let arr = array_ref.as_primitive::<Decimal128Type>();
    let scale_factor = 10_i128.pow(scale as u32);
    let mut builder = TimestampMicrosecondBuilder::with_capacity(arr.len());

    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let value = arr.value(i);
            // Note: spark's big decimal truncates to
            // long value and does not throw error (in all leval modes)
            let value_256 = i256::from_i128(value);
            let micros_256 = value_256 * i256::from_i128(MICROS_PER_SECOND as i128);
            let ts = micros_256 / i256::from_i128(scale_factor);
            builder.append_value(ts.as_i128() as i64);
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(target_tz.clone())) as ArrayRef)
}

pub(crate) fn cast_float_to_timestamp(
    array_ref: &ArrayRef,
    target_tz: &Option<Arc<str>>,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(array_ref.len());

    match array_ref.data_type() {
        DataType::Float32 => {
            cast_float_to_timestamp_impl!(array_ref, builder, Float32Type, eval_mode)
        }
        DataType::Float64 => {
            cast_float_to_timestamp_impl!(array_ref, builder, Float64Type, eval_mode)
        }
        dt => {
            return Err(SparkError::Internal(format!(
                "Unsupported type for cast_float_to_timestamp: {:?}",
                dt
            )))
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(target_tz.clone())) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::TimestampMicrosecondType;
    use core::f64;

    #[test]
    fn test_spark_cast_int_to_int_overflow() {
        // Test Int64 -> Int32 overflow
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            Some(i64::MIN),
            Some(100),
        ]));

        // Legacy mode should wrap around
        let result =
            spark_cast_int_to_int(&array, EvalMode::Legacy, &DataType::Int64, &DataType::Int32)
                .unwrap();
        let int32_array = result.as_primitive::<Int32Type>();
        assert_eq!(int32_array.value(2), 100);

        // Ansi mode should error on overflow
        let result =
            spark_cast_int_to_int(&array, EvalMode::Ansi, &DataType::Int64, &DataType::Int32);
        assert!(result.is_err());
    }

    #[test]
    fn test_spark_cast_decimal_to_boolean() {
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(0), Some(100), Some(-100), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let result = spark_cast_decimal_to_boolean(&array).unwrap();
        let bool_array = result.as_boolean();
        assert!(!bool_array.value(0)); // 0 -> false
        assert!(bool_array.value(1)); // 100 -> true
        assert!(bool_array.value(2)); // -100 -> true
        assert!(bool_array.is_null(3)); // null -> null
    }

    #[test]
    fn test_cast_int_to_decimal128() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(100), Some(-100), None]));
        let result = cast_int_to_decimal128(
            &array,
            EvalMode::Legacy,
            &DataType::Int32,
            &DataType::Decimal128(10, 2),
            10,
            2,
        )
        .unwrap();
        let decimal_array = result.as_primitive::<Decimal128Type>();
        assert_eq!(decimal_array.value(0), 10000); // 100 * 10^2
        assert_eq!(decimal_array.value(1), -10000); // -100 * 10^2
        assert!(decimal_array.is_null(2));
    }
    #[test]
    fn test_cast_int_to_timestamp() {
        let timezones: [Option<Arc<str>>; 6] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/New_York")),
            Some(Arc::from("America/Los_Angeles")),
            Some(Arc::from("Europe/London")),
            Some(Arc::from("Asia/Tokyo")),
            Some(Arc::from("Australia/Sydney")),
        ];

        for tz in &timezones {
            let int8_array: ArrayRef = Arc::new(Int8Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(127),
                Some(-128),
                None,
            ]));

            let result = cast_int_to_timestamp(&int8_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 127_000_000);
            assert_eq!(ts_array.value(4), -128_000_000);
            assert!(ts_array.is_null(5));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            let int16_array: ArrayRef = Arc::new(Int16Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(32767),
                Some(-32768),
                None,
            ]));

            let result = cast_int_to_timestamp(&int16_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 32_767_000_000_i64);
            assert_eq!(ts_array.value(4), -32_768_000_000_i64);
            assert!(ts_array.is_null(5));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            let int32_array: ArrayRef = Arc::new(Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(1704067200),
                None,
            ]));

            let result = cast_int_to_timestamp(&int32_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 1_704_067_200_000_000_i64);
            assert!(ts_array.is_null(4));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            let int64_array: ArrayRef = Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(i64::MAX),
                Some(i64::MIN),
            ]));

            let result = cast_int_to_timestamp(&int64_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000_i64);
            assert_eq!(ts_array.value(2), -1_000_000_i64);
            assert_eq!(ts_array.value(3), i64::MAX);
            assert_eq!(ts_array.value(4), i64::MIN);
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));
        }
    }
    #[test]
    // Currently the cast function depending on `f64::powi`, which has unspecified precision according to the doc
    // https://doc.rust-lang.org/std/primitive.f64.html#unspecified-precision.
    // Miri deliberately apply random floating-point errors to these operations to expose bugs
    // https://github.com/rust-lang/miri/issues/4395.
    // The random errors may interfere with test cases at rounding edge, so we ignore it on miri for now.
    // Once https://github.com/apache/datafusion-comet/issues/1371 is fixed, this should no longer be an issue.
    #[cfg_attr(miri, ignore)]
    fn test_cast_float_to_decimal() {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(42.),
            Some(0.5153125),
            Some(-42.4242415),
            Some(42e-314),
            Some(0.),
            Some(-4242.424242),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
        ]));
        let b =
            cast_floating_point_to_decimal128::<Float64Type>(&a, 8, 6, EvalMode::Legacy).unwrap();
        assert_eq!(b.len(), a.len());
        let casted = b.as_primitive::<Decimal128Type>();
        assert_eq!(casted.value(0), 42000000);
        // https://github.com/apache/datafusion-comet/issues/1371
        // assert_eq!(casted.value(1), 515313);
        assert_eq!(casted.value(2), -42424242);
        assert_eq!(casted.value(3), 0);
        assert_eq!(casted.value(4), 0);
        assert!(casted.is_null(5));
        assert!(casted.is_null(6));
        assert!(casted.is_null(7));
        assert!(casted.is_null(8));
        assert!(casted.is_null(9));
    }

    #[test]
    fn test_cast_decimal_to_timestamp() {
        let timezones: [Option<Arc<str>>; 3] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/Los_Angeles")),
            None,
        ];

        for tz in &timezones {
            // Decimal128 with scale 6
            let decimal_array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![
                    Some(0_i128),
                    Some(1_000_000_i128),
                    Some(-1_000_000_i128),
                    Some(1_500_000_i128),
                    Some(123_456_789_i128),
                    None,
                ])
                .with_precision_and_scale(18, 6)
                .unwrap(),
            );

            let result = cast_decimal_to_timestamp(&decimal_array, tz, 6).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 1_500_000);
            assert_eq!(ts_array.value(4), 123_456_789);
            assert!(ts_array.is_null(5));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            // Test with scale 2
            let decimal_array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(100_i128), Some(150_i128), Some(-250_i128)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            );

            let result = cast_decimal_to_timestamp(&decimal_array, tz, 2).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 1_000_000);
            assert_eq!(ts_array.value(1), 1_500_000);
            assert_eq!(ts_array.value(2), -2_500_000);
        }
    }

    #[test]
    fn test_cast_float_to_timestamp() {
        let timezones: [Option<Arc<str>>; 3] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/Los_Angeles")),
            None,
        ];
        let eval_modes = [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try];

        for tz in &timezones {
            for eval_mode in &eval_modes {
                // Float64 tests
                let f64_array: ArrayRef = Arc::new(Float64Array::from(vec![
                    Some(0.0),
                    Some(1.0),
                    Some(-1.0),
                    Some(1.5),
                    Some(0.000001),
                    None,
                ]));

                let result = cast_float_to_timestamp(&f64_array, tz, *eval_mode).unwrap();
                let ts_array = result.as_primitive::<TimestampMicrosecondType>();

                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_000_000);
                assert_eq!(ts_array.value(2), -1_000_000);
                assert_eq!(ts_array.value(3), 1_500_000);
                assert_eq!(ts_array.value(4), 1);
                assert!(ts_array.is_null(5));
                assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

                // Float32 tests
                let f32_array: ArrayRef = Arc::new(Float32Array::from(vec![
                    Some(0.0_f32),
                    Some(1.0_f32),
                    Some(-1.0_f32),
                    None,
                ]));

                let result = cast_float_to_timestamp(&f32_array, tz, *eval_mode).unwrap();
                let ts_array = result.as_primitive::<TimestampMicrosecondType>();

                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_000_000);
                assert_eq!(ts_array.value(2), -1_000_000);
                assert!(ts_array.is_null(3));
            }
        }

        // ANSI mode errors on NaN/Infinity
        let tz = &Some(Arc::from("UTC"));
        let f64_nan: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN)]));
        assert!(cast_float_to_timestamp(&f64_nan, tz, EvalMode::Ansi).is_err());

        let f64_inf: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::INFINITY)]));
        assert!(cast_float_to_timestamp(&f64_inf, tz, EvalMode::Ansi).is_err());
    }
}
