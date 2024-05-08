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

use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    compute::{cast_with_options, CastOptions},
    datatypes::{
        ArrowPrimitiveType, Decimal128Type, DecimalType, Float32Type, Float64Type,
        TimestampMicrosecondType,
    },
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use arrow_array::{
    types::{Date32Type, Int16Type, Int32Type, Int64Type, Int8Type},
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, GenericStringArray, OffsetSizeTrait,
    PrimitiveArray,
    Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, GenericStringArray,
    Int16Array, Int32Array, Int64Array, Int8Array, OffsetSizeTrait, PrimitiveArray,
};
use arrow_schema::{DataType, Schema};
use chrono::{Datelike, NaiveDate, TimeZone, Timelike};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{internal_err, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use num::{cast::AsPrimitive, traits::CheckedNeg, CheckedSub, Integer, Num, ToPrimitive};
use log::info;
use regex::Regex;

use crate::{
    errors::{CometError, CometResult},
    execution::datafusion::expressions::utils::{
        array_with_timezone, down_cast_any_ref, spark_cast,
    },
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");
static CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

#[derive(Debug, Hash, PartialEq, Clone, Copy)]
pub enum EvalMode {
    Legacy,
    Ansi,
    Try,
}

#[derive(Debug, Hash)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub eval_mode: EvalMode,

    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    pub timezone: String,
}

macro_rules! cast_utf8_to_int {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) = $cast_method($array.value(i).trim(), $eval_mode)? {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: CometResult<ArrayRef> = Ok(Arc::new(cast_array.finish()) as ArrayRef);
        result
    }};
}

macro_rules! cast_utf8_to_timestamp {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len).with_timezone("UTC");
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Ok(Some(cast_value)) = $cast_method($array.value(i).trim(), $eval_mode) {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: ArrayRef = Arc::new(cast_array.finish()) as ArrayRef;
        result
    }};
}
macro_rules! cast_utf8_to_date {
    ($array:expr, $eval_mode:expr, $array_type:ty, $date_parser:ident) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Ok(Some(cast_value)) = $date_parser($array.value(i).trim(), $eval_mode) {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: ArrayRef = Arc::new(cast_array.finish()) as ArrayRef;
        result
    }};
}
macro_rules! cast_float_to_string {
    ($from:expr, $eval_mode:expr, $type:ty, $output_type:ty, $offset_type:ty) => {{

        fn cast<OffsetSize>(
            from: &dyn Array,
            _eval_mode: EvalMode,
        ) -> CometResult<ArrayRef>
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
                    .collect::<Result<GenericStringArray<OffsetSize>, CometError>>()?;

                Ok(Arc::new(output_array))
            }

        cast::<$offset_type>($from, $eval_mode)
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
                        Ok::<Option<$to_native_type>, CometError>(Some(value as $to_native_type))
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
                            Ok::<Option<$to_native_type>, CometError>(Some(res.unwrap()))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>, _>>(),
        }?;
        let result: CometResult<ArrayRef> = Ok(Arc::new(output_array) as ArrayRef);
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
                        let is_overflow = value.is_nan() || value.abs() as i32 == std::i32::MAX;
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
                        Ok::<Option<$rust_dest_type>, CometError>(Some(
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
                        Ok::<Option<$rust_dest_type>, CometError>(Some(value as $rust_dest_type))
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
            .expect(concat!("Expected a Decimal128ArrayType"));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let (truncated, decimal) = (value / divisor, (value % divisor).abs());
                        let is_overflow = truncated.abs() > std::i32::MAX.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!("{}.{}BD", truncated, decimal),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        let i32_value = truncated as i32;
                        <$rust_dest_type>::try_from(i32_value)
                            .map_err(|_| {
                                cast_overflow(
                                    &format!("{}.{}BD", truncated, decimal),
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
                        Ok::<Option<$rust_dest_type>, CometError>(Some(
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
            .expect(concat!("Expected a Decimal128ArrayType"));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let (truncated, decimal) = (value / divisor, (value % divisor).abs());
                        let is_overflow = truncated.abs() > $max_dest_val.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!("{}.{}BD", truncated, decimal),
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
                        Ok::<Option<$rust_dest_type>, CometError>(Some(
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

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
        timezone: String,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone,
            eval_mode,
        }
    }

    pub fn new_without_timezone(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone: "".to_string(),
            eval_mode,
        }
    }

    fn cast_array(&self, array: ArrayRef) -> DataFusionResult<ArrayRef> {
        let to_type = &self.data_type;
        let array = array_with_timezone(array, self.timezone.clone(), Some(to_type));
        let from_type = array.data_type();
        let cast_result = match (from_type, to_type) {
            (DataType::Utf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i32>(&array, self.eval_mode)?
            }
            (DataType::LargeUtf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i64>(&array, self.eval_mode)?
            }
            (DataType::Utf8, DataType::Timestamp(_, _)) => {
                Self::cast_string_to_timestamp(&array, to_type, self.eval_mode)?
            }
            (DataType::Utf8, DataType::Date32) | (DataType::Utf8, DataType::Date64) => {
                Self::cast_string_to_date(&array, to_type, self.eval_mode)?
            }
            (DataType::Int64, DataType::Int32)
            | (DataType::Int64, DataType::Int16)
            | (DataType::Int64, DataType::Int8)
            | (DataType::Int32, DataType::Int16)
            | (DataType::Int32, DataType::Int8)
            | (DataType::Int16, DataType::Int8)
                if self.eval_mode != EvalMode::Try =>
            {
                Self::spark_cast_int_to_int(&array, self.eval_mode, from_type, to_type)?
            }
            (
                DataType::Utf8,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => Self::cast_string_to_int::<i32>(to_type, &array, self.eval_mode)?,
            (
                DataType::LargeUtf8,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => Self::cast_string_to_int::<i64>(to_type, &array, self.eval_mode)?,
            (
                DataType::Dictionary(key_type, value_type),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) if key_type.as_ref() == &DataType::Int32
                && (value_type.as_ref() == &DataType::Utf8
                    || value_type.as_ref() == &DataType::LargeUtf8) =>
            {
                // TODO: we are unpacking a dictionary-encoded array and then performing
                // the cast. We could potentially improve performance here by casting the
                // dictionary values directly without unpacking the array first, although this
                // would add more complexity to the code
                match value_type.as_ref() {
                    DataType::Utf8 => {
                        let unpacked_array =
                            cast_with_options(&array, &DataType::Utf8, &CAST_OPTIONS)?;
                        Self::cast_string_to_int::<i32>(to_type, &unpacked_array, self.eval_mode)?
                    }
                    DataType::LargeUtf8 => {
                        let unpacked_array =
                            cast_with_options(&array, &DataType::LargeUtf8, &CAST_OPTIONS)?;
                        Self::cast_string_to_int::<i64>(to_type, &unpacked_array, self.eval_mode)?
                    }
                    dt => unreachable!(
                        "{}",
                        format!("invalid value type {dt} for dictionary-encoded string array")
                    ),
                }
            }
            (DataType::Float64, DataType::Utf8) => {
                Self::spark_cast_float64_to_utf8::<i32>(&array, self.eval_mode)?
            }
            (DataType::Float64, DataType::LargeUtf8) => {
                Self::spark_cast_float64_to_utf8::<i64>(&array, self.eval_mode)?
            }
            (DataType::Float32, DataType::Utf8) => {
                Self::spark_cast_float32_to_utf8::<i32>(&array, self.eval_mode)?
            }
            (DataType::Float32, DataType::LargeUtf8) => {
                Self::spark_cast_float32_to_utf8::<i64>(&array, self.eval_mode)?
            }
            (DataType::Float32, DataType::Decimal128(precision, scale)) => {
                Self::cast_float32_to_decimal128(&array, *precision, *scale, self.eval_mode)?
            }
            (DataType::Float64, DataType::Decimal128(precision, scale)) => {
                Self::cast_float64_to_decimal128(&array, *precision, *scale, self.eval_mode)?
            }
            (DataType::Float32, DataType::Int8)
            | (DataType::Float32, DataType::Int16)
            | (DataType::Float32, DataType::Int32)
            | (DataType::Float32, DataType::Int64)
            | (DataType::Float64, DataType::Int8)
            | (DataType::Float64, DataType::Int16)
            | (DataType::Float64, DataType::Int32)
            | (DataType::Float64, DataType::Int64)
            | (DataType::Decimal128(_, _), DataType::Int8)
            | (DataType::Decimal128(_, _), DataType::Int16)
            | (DataType::Decimal128(_, _), DataType::Int32)
            | (DataType::Decimal128(_, _), DataType::Int64)
                if self.eval_mode != EvalMode::Try =>
            {
                Self::spark_cast_nonintegral_numeric_to_integral(
                    &array,
                    self.eval_mode,
                    from_type,
                    to_type,
                )?
            }
            _ => {
                // when we have no Spark-specific casting we delegate to DataFusion
                cast_with_options(&array, to_type, &CAST_OPTIONS)?
            }
        };
        Ok(spark_cast(cast_result, from_type, to_type))
    }

    fn cast_string_to_int<OffsetSize: OffsetSizeTrait>(
        to_type: &DataType,
        array: &ArrayRef,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .expect("cast_string_to_int expected a string array");

        let cast_array: ArrayRef = match to_type {
            DataType::Int8 => {
                cast_utf8_to_int!(string_array, eval_mode, Int8Type, cast_string_to_i8)?
            }
            DataType::Int16 => {
                cast_utf8_to_int!(string_array, eval_mode, Int16Type, cast_string_to_i16)?
            }
            DataType::Int32 => {
                cast_utf8_to_int!(string_array, eval_mode, Int32Type, cast_string_to_i32)?
            }
            DataType::Int64 => {
                cast_utf8_to_int!(string_array, eval_mode, Int64Type, cast_string_to_i64)?
            }
            dt => unreachable!(
                "{}",
                format!("invalid integer type {dt} in cast from string")
            ),
        };
        Ok(cast_array)
    }

    fn cast_string_to_date(
        array: &ArrayRef,
        to_type: &DataType,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let cast_array: ArrayRef = match to_type {
            DataType::Date32 | DataType::Date64 => {
                let len = string_array.len();
                let mut cast_array = PrimitiveArray::<Date32Type>::builder(len);
                for i in 0..len {
                    if string_array.is_null(i) {
                        cast_array.append_null()
                    } else if let Ok(Some(cast_value)) =
                        date_parser(string_array.value(i).trim(), eval_mode)
                    {
                        cast_array.append_value(cast_value);
                    } else {
                        cast_array.append_null()
                    }
                }
                Arc::new(cast_array.finish()) as ArrayRef
            }
            _ => unreachable!("Invalid data type {:?} in cast from string", to_type),
        };
        Ok(cast_array)
    }

    fn cast_string_to_timestamp(
        array: &ArrayRef,
        to_type: &DataType,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let cast_array: ArrayRef = match to_type {
            DataType::Timestamp(_, _) => {
                cast_utf8_to_timestamp!(
                    string_array,
                    eval_mode,
                    TimestampMicrosecondType,
                    timestamp_parser
                )
            }
            _ => unreachable!("Invalid data type {:?} in cast from string", to_type),
        };
        Ok(cast_array)
    }

    fn cast_float64_to_decimal128(
        array: &dyn Array,
        precision: u8,
        scale: i8,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        Self::cast_floating_point_to_decimal128::<Float64Type>(array, precision, scale, eval_mode)
    }

    fn cast_float32_to_decimal128(
        array: &dyn Array,
        precision: u8,
        scale: i8,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        Self::cast_floating_point_to_decimal128::<Float32Type>(array, precision, scale, eval_mode)
    }

    fn cast_floating_point_to_decimal128<T: ArrowPrimitiveType>(
        array: &dyn Array,
        precision: u8,
        scale: i8,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
    {
        let input = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let mut cast_array = PrimitiveArray::<Decimal128Type>::builder(input.len());

        let mul = 10_f64.powi(scale as i32);

        for i in 0..input.len() {
            if input.is_null(i) {
                cast_array.append_null();
            } else {
                let input_value = input.value(i).as_();
                let value = (input_value * mul).round().to_i128();

                match value {
                    Some(v) => {
                        if Decimal128Type::validate_decimal_precision(v, precision).is_err() {
                            if eval_mode == EvalMode::Ansi {
                                return Err(CometError::NumericValueOutOfRange {
                                    value: input_value.to_string(),
                                    precision,
                                    scale,
                                });
                            } else {
                                cast_array.append_null();
                            }
                        }
                        cast_array.append_value(v);
                    }
                    None => {
                        if eval_mode == EvalMode::Ansi {
                            return Err(CometError::NumericValueOutOfRange {
                                value: input_value.to_string(),
                                precision,
                                scale,
                            });
                        } else {
                            cast_array.append_null();
                        }
                    }
                }
            }
        }

        let res = Arc::new(
            cast_array
                .with_precision_and_scale(precision, scale)?
                .finish(),
        ) as ArrayRef;
        Ok(res)
    }

    fn spark_cast_float64_to_utf8<OffsetSize>(
        from: &dyn Array,
        _eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        cast_float_to_string!(from, _eval_mode, f64, Float64Array, OffsetSize)
    }

    fn spark_cast_float32_to_utf8<OffsetSize>(
        from: &dyn Array,
        _eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        cast_float_to_string!(from, _eval_mode, f32, Float32Array, OffsetSize)
    }

    fn spark_cast_int_to_int(
        array: &dyn Array,
        eval_mode: EvalMode,
        from_type: &DataType,
        to_type: &DataType,
    ) -> CometResult<ArrayRef> {
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

    fn spark_cast_utf8_to_boolean<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
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
                    _ if eval_mode == EvalMode::Ansi => Err(CometError::CastInvalidValue {
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

    fn spark_cast_nonintegral_numeric_to_integral(
        array: &dyn Array,
        eval_mode: EvalMode,
        from_type: &DataType,
        to_type: &DataType,
    ) -> CometResult<ArrayRef> {
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
                std::i32::MAX,
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
                std::i64::MAX,
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
                std::i32::MAX,
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
                std::i64::MAX,
                "{:e}D"
            ),
            (DataType::Decimal128(precision, scale), DataType::Int8) => {
                cast_decimal_to_int16_down!(
                    array, eval_mode, Int8Array, i8, "TINYINT", precision, *scale
                )
            }
            (DataType::Decimal128(precision, scale), DataType::Int16) => {
                cast_decimal_to_int16_down!(
                    array, eval_mode, Int16Array, i16, "SMALLINT", precision, *scale
                )
            }
            (DataType::Decimal128(precision, scale), DataType::Int32) => {
                cast_decimal_to_int32_up!(
                    array,
                    eval_mode,
                    Int32Array,
                    i32,
                    "INT",
                    std::i32::MAX,
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
                    std::i64::MAX,
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
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toByte
fn cast_string_to_i8(str: &str, eval_mode: EvalMode) -> CometResult<Option<i8>> {
    Ok(cast_string_to_int_with_range_check(
        str,
        eval_mode,
        "TINYINT",
        i8::MIN as i32,
        i8::MAX as i32,
    )?
    .map(|v| v as i8))
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toShort
fn cast_string_to_i16(str: &str, eval_mode: EvalMode) -> CometResult<Option<i16>> {
    Ok(cast_string_to_int_with_range_check(
        str,
        eval_mode,
        "SMALLINT",
        i16::MIN as i32,
        i16::MAX as i32,
    )?
    .map(|v| v as i16))
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toInt(IntWrapper intWrapper)
fn cast_string_to_i32(str: &str, eval_mode: EvalMode) -> CometResult<Option<i32>> {
    do_cast_string_to_int::<i32>(str, eval_mode, "INT", i32::MIN)
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toLong(LongWrapper intWrapper)
fn cast_string_to_i64(str: &str, eval_mode: EvalMode) -> CometResult<Option<i64>> {
    do_cast_string_to_int::<i64>(str, eval_mode, "BIGINT", i64::MIN)
}

fn cast_string_to_int_with_range_check(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min: i32,
    max: i32,
) -> CometResult<Option<i32>> {
    match do_cast_string_to_int(str, eval_mode, type_name, i32::MIN)? {
        None => Ok(None),
        Some(v) if v >= min && v <= max => Ok(Some(v)),
        _ if eval_mode == EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

#[derive(PartialEq)]
enum State {
    SkipLeadingWhiteSpace,
    SkipTrailingWhiteSpace,
    ParseSignAndDigits,
    ParseFractionalDigits,
}

/// Equivalent to
/// - org.apache.spark.unsafe.types.UTF8String.toInt(IntWrapper intWrapper, boolean allowDecimal)
/// - org.apache.spark.unsafe.types.UTF8String.toLong(LongWrapper longWrapper, boolean allowDecimal)
fn do_cast_string_to_int<
    T: Num + PartialOrd + Integer + CheckedSub + CheckedNeg + From<i32> + Copy,
>(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min_value: T,
) -> CometResult<Option<T>> {
    let len = str.len();
    if str.is_empty() {
        return none_or_err(eval_mode, type_name, str);
    }

    let mut result: T = T::zero();
    let mut negative = false;
    let radix = T::from(10);
    let stop_value = min_value / radix;
    let mut state = State::SkipLeadingWhiteSpace;
    let mut parsed_sign = false;

    for (i, ch) in str.char_indices() {
        // skip leading whitespace
        if state == State::SkipLeadingWhiteSpace {
            if ch.is_whitespace() {
                // consume this char
                continue;
            }
            // change state and fall through to next section
            state = State::ParseSignAndDigits;
        }

        if state == State::ParseSignAndDigits {
            if !parsed_sign {
                negative = ch == '-';
                let positive = ch == '+';
                parsed_sign = true;
                if negative || positive {
                    if i + 1 == len {
                        // input string is just "+" or "-"
                        return none_or_err(eval_mode, type_name, str);
                    }
                    // consume this char
                    continue;
                }
            }

            if ch == '.' {
                if eval_mode == EvalMode::Legacy {
                    // truncate decimal in legacy mode
                    state = State::ParseFractionalDigits;
                    continue;
                } else {
                    return none_or_err(eval_mode, type_name, str);
                }
            }

            let digit = if ch.is_ascii_digit() {
                (ch as u32) - ('0' as u32)
            } else {
                return none_or_err(eval_mode, type_name, str);
            };

            // We are going to process the new digit and accumulate the result. However, before
            // doing this, if the result is already smaller than the
            // stopValue(Integer.MIN_VALUE / radix), then result * 10 will definitely be
            // smaller than minValue, and we can stop
            if result < stop_value {
                return none_or_err(eval_mode, type_name, str);
            }

            // Since the previous result is greater than or equal to stopValue(Integer.MIN_VALUE /
            // radix), we can just use `result > 0` to check overflow. If result
            // overflows, we should stop
            let v = result * radix;
            let digit = (digit as i32).into();
            match v.checked_sub(&digit) {
                Some(x) if x <= T::zero() => result = x,
                _ => {
                    return none_or_err(eval_mode, type_name, str);
                }
            }
        }

        if state == State::ParseFractionalDigits {
            // This is the case when we've encountered a decimal separator. The fractional
            // part will not change the number, but we will verify that the fractional part
            // is well-formed.
            if ch.is_whitespace() {
                // finished parsing fractional digits, now need to skip trailing whitespace
                state = State::SkipTrailingWhiteSpace;
                // consume this char
                continue;
            }
            if !ch.is_ascii_digit() {
                return none_or_err(eval_mode, type_name, str);
            }
        }

        // skip trailing whitespace
        if state == State::SkipTrailingWhiteSpace && !ch.is_whitespace() {
            return none_or_err(eval_mode, type_name, str);
        }
    }

    if !negative {
        if let Some(neg) = result.checked_neg() {
            if neg < T::zero() {
                return none_or_err(eval_mode, type_name, str);
            }
            result = neg;
        } else {
            return none_or_err(eval_mode, type_name, str);
        }
    }

    Ok(Some(result))
}

/// Either return Ok(None) or Err(CometError::CastInvalidValue) depending on the evaluation mode
#[inline]
fn none_or_err<T>(eval_mode: EvalMode, type_name: &str, str: &str) -> CometResult<Option<T>> {
    match eval_mode {
        EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

#[inline]
fn invalid_value(value: &str, from_type: &str, to_type: &str) -> CometError {
    CometError::CastInvalidValue {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

#[inline]
fn cast_overflow(value: &str, from_type: &str, to_type: &str) -> CometError {
    CometError::CastOverFlow {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.timezone, self.child, &self.eval_mode
        )
    }
}

impl PartialEq<dyn Any> for Cast {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.child.eq(&x.child)
                    && self.timezone.eq(&x.timezone)
                    && self.data_type.eq(&x.data_type)
                    && self.eval_mode.eq(&x.eval_mode)
            })
            .unwrap_or(false)
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.cast_array(array)?)),
            ColumnarValue::Scalar(scalar) => {
                // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
                // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
                // here.
                let array = scalar.to_array()?;
                let scalar = ScalarValue::try_from_array(&self.cast_array(array)?, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Cast::new(
                children[0].clone(),
                self.data_type.clone(),
                self.eval_mode,
                self.timezone.clone(),
            ))),
            _ => internal_err!("Cast should have exactly one child"),
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.data_type.hash(&mut s);
        self.timezone.hash(&mut s);
        self.eval_mode.hash(&mut s);
        self.hash(&mut s);
    }
}

fn timestamp_parser(value: &str, eval_mode: EvalMode) -> CometResult<Option<i64>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    // Define regex patterns and corresponding parsing functions
    let patterns = &[
        (
            Regex::new(r"^\d{4}$").unwrap(),
            parse_str_to_year_timestamp as fn(&str) -> CometResult<Option<i64>>,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}$").unwrap(),
            parse_str_to_month_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap(),
            parse_str_to_day_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{1,2}$").unwrap(),
            parse_str_to_hour_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$").unwrap(),
            parse_str_to_minute_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap(),
            parse_str_to_second_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap(),
            parse_str_to_microsecond_timestamp,
        ),
        (
            Regex::new(r"^T\d{1,2}$").unwrap(),
            parse_str_to_time_only_timestamp,
        ),
    ];

    let mut timestamp = None;

    // Iterate through patterns and try matching
    for (pattern, parse_func) in patterns {
        if pattern.is_match(value) {
            timestamp = parse_func(value)?;
            break;
        }
    }

    if timestamp.is_none() {
        return if eval_mode == EvalMode::Ansi {
            Err(CometError::CastInvalidValue {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP".to_string(),
            })
        } else {
            Ok(None)
        };
    }

    match timestamp {
        Some(ts) => Ok(Some(ts)),
        None => Err(CometError::Internal(
            "Failed to parse timestamp".to_string(),
        )),
    }
}

fn parse_ymd_timestamp(year: i32, month: u32, day: u32) -> CometResult<Option<i64>> {
    let datetime = chrono::Utc.with_ymd_and_hms(year, month, day, 0, 0, 0);

    // Check if datetime is not None
    let utc_datetime = match datetime.single() {
        Some(dt) => dt.with_timezone(&chrono::Utc),
        None => {
            return Err(CometError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    Ok(Some(utc_datetime.timestamp_micros()))
}

fn parse_hms_timestamp(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    microsecond: u32,
) -> CometResult<Option<i64>> {
    let datetime = chrono::Utc.with_ymd_and_hms(year, month, day, hour, minute, second);

    // Check if datetime is not None
    let utc_datetime = match datetime.single() {
        Some(dt) => dt
            .with_timezone(&chrono::Utc)
            .with_nanosecond(microsecond * 1000),
        None => {
            return Err(CometError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    let result = match utc_datetime {
        Some(dt) => dt.timestamp_micros(),
        None => {
            return Err(CometError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    Ok(Some(result))
}

fn get_timestamp_values(value: &str, timestamp_type: &str) -> CometResult<Option<i64>> {
    let values: Vec<_> = value
        .split(|c| c == 'T' || c == '-' || c == ':' || c == '.')
        .collect();
    let year = values[0].parse::<i32>().unwrap_or_default();
    let month = values.get(1).map_or(1, |m| m.parse::<u32>().unwrap_or(1));
    let day = values.get(2).map_or(1, |d| d.parse::<u32>().unwrap_or(1));
    let hour = values.get(3).map_or(0, |h| h.parse::<u32>().unwrap_or(0));
    let minute = values.get(4).map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = values.get(5).map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let microsecond = values.get(6).map_or(0, |ms| ms.parse::<u32>().unwrap_or(0));

    match timestamp_type {
        "year" => parse_ymd_timestamp(year, 1, 1),
        "month" => parse_ymd_timestamp(year, month, 1),
        "day" => parse_ymd_timestamp(year, month, day),
        "hour" => parse_hms_timestamp(year, month, day, hour, 0, 0, 0),
        "minute" => parse_hms_timestamp(year, month, day, hour, minute, 0, 0),
        "second" => parse_hms_timestamp(year, month, day, hour, minute, second, 0),
        "microsecond" => parse_hms_timestamp(year, month, day, hour, minute, second, microsecond),
        _ => Err(CometError::CastInvalidValue {
            value: value.to_string(),
            from_type: "STRING".to_string(),
            to_type: "TIMESTAMP".to_string(),
        }),
    }
}

fn parse_str_to_year_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "year")
}

fn parse_str_to_month_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "month")
}

fn parse_str_to_day_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "day")
}

fn parse_str_to_hour_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "hour")
}

fn parse_str_to_minute_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "minute")
}

fn parse_str_to_second_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "second")
}

fn parse_str_to_microsecond_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "microsecond")
}

fn parse_str_to_time_only_timestamp(value: &str) -> CometResult<Option<i64>> {
    let values: Vec<&str> = value.split('T').collect();
    let time_values: Vec<u32> = values[1]
        .split(':')
        .map(|v| v.parse::<u32>().unwrap_or(0))
        .collect();

    let datetime = chrono::Utc::now();
    let timestamp = datetime
        .with_hour(time_values.first().copied().unwrap_or_default())
        .and_then(|dt| dt.with_minute(*time_values.get(1).unwrap_or(&0)))
        .and_then(|dt| dt.with_second(*time_values.get(2).unwrap_or(&0)))
        .and_then(|dt| dt.with_nanosecond(*time_values.get(3).unwrap_or(&0) * 1_000))
        .map(|dt| dt.to_utc().timestamp_micros())
        .unwrap_or_default();

    Ok(Some(timestamp))
}

fn date_parser(value: &str, eval_mode: EvalMode) -> CometResult<Option<i32>> {
    info!("Date String is {:?}", value);
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let parts: Vec<&str> = value.split('-').collect();
    let date = match parts.len() {
        1 => match parts[0].parse() {
            Ok(year) => NaiveDate::from_ymd_opt(year, 1, 1),
            _ => None,
        },
        2 => match parts[0].parse() {
            Ok(year) => {
                let month = parts[1].parse();
                match month {
                    Ok(month) => NaiveDate::from_ymd_opt(year, month, 1),
                    _ => None,
                }
            }
            _ => None,
        },
        3 => {
            let value = value.trim_end_matches('T');
            let date = NaiveDate::parse_from_str(value, "%Y-%m-%d");
            match date {
                Ok(date) => Some(date),
                _ => None,
            }
        }
        _ => None,
    };

    info!("Returned Date is {:?}", date);
    if date.is_none() && eval_mode == EvalMode::Ansi {
        return Err(CometError::CastInvalidValue {
            value: value.to_string(),
            from_type: "STRING".to_string(),
            to_type: "DATE".to_string(),
        });
    }

    match date {
        Some(date) => Ok(Some(date.num_days_from_ce())),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Date32Type, TimestampMicrosecondType};
    use arrow_array::StringArray;
    use arrow_schema::TimeUnit;

    use super::*;

    #[test]
    fn timestamp_parser_test() {
        // write for all formats
        assert_eq!(
            timestamp_parser("2020", EvalMode::Legacy).unwrap(),
            Some(1577836800000000) // this is in milliseconds
        );
        assert_eq!(
            timestamp_parser("2020-01", EvalMode::Legacy).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01", EvalMode::Legacy).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12", EvalMode::Legacy).unwrap(),
            Some(1577880000000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34", EvalMode::Legacy).unwrap(),
            Some(1577882040000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56", EvalMode::Legacy).unwrap(),
            Some(1577882096000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123456", EvalMode::Legacy).unwrap(),
            Some(1577882096123456)
        );
        // assert_eq!(
        //     timestamp_parser("T2",  EvalMode::Legacy).unwrap(),
        //     Some(1714356000000000) // this value needs to change everyday.
        // );
    }

    #[test]
    fn test_cast_string_to_timestamp() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("T2"),
        ]));

        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let eval_mode = EvalMode::Legacy;
        let result = cast_utf8_to_timestamp!(
            &string_array,
            eval_mode,
            TimestampMicrosecondType,
            timestamp_parser
        );

        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn date_parser_test() {
        //test valid dates for all eval modes
        for date in &["2020", "2020-01", "2020-01-01", "2020-01-01T"] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
                assert_eq!(date_parser(*date, *eval_mode).unwrap(), Some(737425));
            }
        }

        //test fuzzy dates for all modes
        assert_eq!(date_parser("-0973250", EvalMode::Legacy).unwrap(), None);
        assert_eq!(date_parser("-3638-5", EvalMode::Legacy).unwrap(), None);

        assert!(date_parser("-0973250", EvalMode::Ansi).is_err());
        assert!(date_parser("-3638-5", EvalMode::Ansi).is_err());

        assert_eq!(date_parser("-0973250", EvalMode::Try).unwrap(), None);
        assert_eq!(date_parser("-3638-5", EvalMode::Try).unwrap(), None);

        let invalid_dates = ["20200-01-01", "2020-010-01", "2020-10-010", "2020-10-010T"];
        //test invalid dates return err for Ansi
        for date in &invalid_dates {
            assert!(date_parser(*date, EvalMode::Ansi).is_err());
        }

        //test invalid dates return None for Legacy and Try
        for date in &invalid_dates {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try] {
                assert_eq!(date_parser(*date, *eval_mode).unwrap(), None);
            }
        }

        let fuzzy_dates = ["-0973250", "-3638-5"];
        //test invalid dates return err for Ansi
        for date in &fuzzy_dates {
            assert!(date_parser(*date, EvalMode::Ansi).is_err());
        }

        //test invalid dates return None for Legacy and Try
        for date in &fuzzy_dates {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try] {
                assert_eq!(date_parser(*date, *eval_mode).unwrap(), None);
            }
        }
    }

    #[test]
    fn test_cast_string_as_date() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020"),
            Some("2020-01"),
            Some("2020-01-01"),
            Some("2020-01-01T"),
        ]));

        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let eval_mode = EvalMode::Legacy;
        let result = cast_utf8_to_date!(&string_array, eval_mode, Date32Type, date_parser);

        assert_eq!(result.data_type(), &DataType::Date32);
        assert_eq!(result.len(), 4);
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
