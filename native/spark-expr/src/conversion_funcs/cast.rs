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

use crate::conversion_funcs::boolean::{
    cast_boolean_to_decimal, is_df_cast_from_bool_spark_compatible,
};
use crate::conversion_funcs::string::{
    cast_string_to_date, cast_string_to_decimal, cast_string_to_float, cast_string_to_int,
    cast_string_to_timestamp, is_df_cast_from_string_spark_compatible, spark_cast_utf8_to_boolean,
};
use crate::conversion_funcs::utils::cast_overflow;
use crate::conversion_funcs::utils::spark_cast_postprocess;
use crate::utils::array_with_timezone;
use crate::EvalMode::Legacy;
use crate::{timezone, BinaryOutputStyle};
use crate::{EvalMode, SparkError, SparkResult};
use arrow::array::builder::StringBuilder;
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Decimal128Builder, DictionaryArray, GenericByteArray, ListArray,
    StringArray, StructArray, TimestampMicrosecondBuilder,
};
use arrow::compute::can_cast_types;
use arrow::datatypes::GenericBinaryType;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType, Schema};
use arrow::error::ArrowError;
use arrow::{
    array::{
        cast::AsArray,
        types::{Date32Type, Int16Type, Int32Type, Int8Type},
        Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, GenericStringArray,
        Int16Array, Int32Array, Int64Array, Int8Array, OffsetSizeTrait, PrimitiveArray,
    },
    compute::{cast_with_options, take, CastOptions},
    datatypes::{
        is_validate_decimal_precision, ArrowPrimitiveType, Decimal128Type, Float32Type,
        Float64Type, Int64Type,
    },
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use chrono::{NaiveDate, TimeZone};
use datafusion::common::{internal_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use num::{cast::AsPrimitive, ToPrimitive, Zero};
use std::str::FromStr;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::Hash,
    sync::Arc,
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

pub(crate) const MICROS_PER_SECOND: i64 = 1000000;

static CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

#[derive(Debug, Eq)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub cast_options: SparkCastOptions,
}

impl PartialEq for Cast {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.data_type.eq(&other.data_type)
            && self.cast_options.eq(&other.cast_options)
    }
}

impl Hash for Cast {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
        self.cast_options.hash(state);
    }
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

// copied from arrow::dataTypes::Decimal128Type since Decimal128Type::format_decimal can't be called directly
fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
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

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        cast_options: SparkCastOptions,
    ) -> Self {
        Self {
            child,
            data_type,
            cast_options,
        }
    }
}

/// Spark cast options
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SparkCastOptions {
    /// Spark evaluation mode
    pub eval_mode: EvalMode,
    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    // TODO we should change timezone to Tz to avoid repeated parsing
    pub timezone: String,
    /// Allow casts that are supported but not guaranteed to be 100% compatible
    pub allow_incompat: bool,
    /// Support casting unsigned ints to signed ints (used by Parquet SchemaAdapter)
    pub allow_cast_unsigned_ints: bool,
    /// We also use the cast logic for adapting Parquet schemas, so this flag is used
    /// for that use case
    pub is_adapting_schema: bool,
    /// String to use to represent null values
    pub null_string: String,
    /// SparkSQL's binaryOutputStyle
    pub binary_output_style: Option<BinaryOutputStyle>,
}

impl SparkCastOptions {
    pub fn new(eval_mode: EvalMode, timezone: &str, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: timezone.to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            is_adapting_schema: false,
            null_string: "null".to_string(),
            binary_output_style: None,
        }
    }

    pub fn new_without_timezone(eval_mode: EvalMode, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: "".to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            is_adapting_schema: false,
            null_string: "null".to_string(),
            binary_output_style: None,
        }
    }
}

/// Spark-compatible cast implementation. Defers to DataFusion's cast where that is known
/// to be compatible, and returns an error when a not supported and not DF-compatible cast
/// is requested.
pub fn spark_cast(
    arg: ColumnarValue,
    data_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ColumnarValue> {
    let result = match arg {
        ColumnarValue::Array(array) => {
            let result_array = cast_array(array, data_type, cast_options)?;
            ColumnarValue::Array(result_array)
        }
        ColumnarValue::Scalar(scalar) => {
            // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
            // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
            // here.
            let array = scalar.to_array()?;
            let scalar =
                ScalarValue::try_from_array(&cast_array(array, data_type, cast_options)?, 0)?;
            ColumnarValue::Scalar(scalar)
        }
    };

    Ok(result)
}

// copied from datafusion common scalar/mod.rs
fn dict_from_values<K: ArrowDictionaryKeyType>(
    values_array: ArrayRef,
) -> datafusion::common::Result<ArrayRef> {
    // Create a key array with `size` elements of 0..array_len for all
    // non-null value elements
    let key_array: PrimitiveArray<K> = (0..values_array.len())
        .map(|index| {
            if values_array.is_valid(index) {
                let native_index = K::Native::from_usize(index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not create index of type {} from value {}",
                        K::DATA_TYPE,
                        index
                    ))
                })?;
                Ok(Some(native_index))
            } else {
                Ok(None)
            }
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?
        .into_iter()
        .collect();

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    let dict_array = DictionaryArray::<K>::try_new(key_array, values_array)?;
    Ok(Arc::new(dict_array))
}

pub(crate) fn cast_array(
    array: ArrayRef,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type().clone();

    if &from_type == to_type {
        return Ok(Arc::new(array));
    }

    let array = array_with_timezone(array, cast_options.timezone.clone(), Some(to_type))?;
    let eval_mode = cast_options.eval_mode;

    let native_cast_options: CastOptions = CastOptions {
        safe: !matches!(cast_options.eval_mode, EvalMode::Ansi), // take safe mode from cast_options passed
        format_options: FormatOptions::new()
            .with_timestamp_tz_format(TIMESTAMP_FORMAT)
            .with_timestamp_format(TIMESTAMP_FORMAT),
    };

    let array = match &from_type {
        Dictionary(key_type, value_type)
            if key_type.as_ref() == &Int32
                && (value_type.as_ref() == &Utf8
                    || value_type.as_ref() == &LargeUtf8
                    || value_type.as_ref() == &Binary
                    || value_type.as_ref() == &LargeBinary) =>
        {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected a dictionary array");

            let casted_result = match to_type {
                Dictionary(_, to_value_type) => {
                    let casted_dictionary = DictionaryArray::<Int32Type>::new(
                        dict_array.keys().clone(),
                        cast_array(Arc::clone(dict_array.values()), to_value_type, cast_options)?,
                    );
                    Arc::new(casted_dictionary.clone())
                }
                _ => {
                    let casted_dictionary = DictionaryArray::<Int32Type>::new(
                        dict_array.keys().clone(),
                        cast_array(Arc::clone(dict_array.values()), to_type, cast_options)?,
                    );
                    take(casted_dictionary.values().as_ref(), dict_array.keys(), None)?
                }
            };
            return Ok(spark_cast_postprocess(casted_result, &from_type, to_type));
        }
        _ => {
            if let Dictionary(_, _) = to_type {
                let dict_array = dict_from_values::<Int32Type>(array)?;
                let casted_result = cast_array(dict_array, to_type, cast_options)?;
                return Ok(spark_cast_postprocess(casted_result, &from_type, to_type));
            } else {
                array
            }
        }
    };

    let cast_result = match (&from_type, to_type) {
        (Utf8, Boolean) => spark_cast_utf8_to_boolean::<i32>(&array, eval_mode),
        (LargeUtf8, Boolean) => spark_cast_utf8_to_boolean::<i64>(&array, eval_mode),
        (Utf8, Timestamp(_, _)) => {
            cast_string_to_timestamp(&array, to_type, eval_mode, &cast_options.timezone)
        }
        (Utf8, Date32) => cast_string_to_date(&array, to_type, eval_mode),
        (Date32, Int32) => {
            // Date32 is stored as days since epoch (i32), so this is a simple reinterpret cast
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }
        (Utf8, Float32 | Float64) => cast_string_to_float(&array, to_type, eval_mode),
        (Utf8 | LargeUtf8, Decimal128(precision, scale)) => {
            cast_string_to_decimal(&array, to_type, precision, scale, eval_mode)
        }
        (Utf8 | LargeUtf8, Decimal256(precision, scale)) => {
            cast_string_to_decimal(&array, to_type, precision, scale, eval_mode)
        }
        (Int64, Int32)
        | (Int64, Int16)
        | (Int64, Int8)
        | (Int32, Int16)
        | (Int32, Int8)
        | (Int16, Int8)
            if eval_mode != EvalMode::Try =>
        {
            spark_cast_int_to_int(&array, eval_mode, &from_type, to_type)
        }
        (Int8 | Int16 | Int32 | Int64, Decimal128(precision, scale)) => {
            cast_int_to_decimal128(&array, eval_mode, &from_type, to_type, *precision, *scale)
        }
        (Utf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int::<i32>(to_type, &array, eval_mode)
        }
        (LargeUtf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int::<i64>(to_type, &array, eval_mode)
        }
        (Float64, Utf8) => spark_cast_float64_to_utf8::<i32>(&array, eval_mode),
        (Float64, LargeUtf8) => spark_cast_float64_to_utf8::<i64>(&array, eval_mode),
        (Float32, Utf8) => spark_cast_float32_to_utf8::<i32>(&array, eval_mode),
        (Float32, LargeUtf8) => spark_cast_float32_to_utf8::<i64>(&array, eval_mode),
        (Float32, Decimal128(precision, scale)) => {
            cast_float32_to_decimal128(&array, *precision, *scale, eval_mode)
        }
        (Float64, Decimal128(precision, scale)) => {
            cast_float64_to_decimal128(&array, *precision, *scale, eval_mode)
        }
        (Float32, Int8)
        | (Float32, Int16)
        | (Float32, Int32)
        | (Float32, Int64)
        | (Float64, Int8)
        | (Float64, Int16)
        | (Float64, Int32)
        | (Float64, Int64)
        | (Decimal128(_, _), Int8)
        | (Decimal128(_, _), Int16)
        | (Decimal128(_, _), Int32)
        | (Decimal128(_, _), Int64)
            if eval_mode != EvalMode::Try =>
        {
            spark_cast_nonintegral_numeric_to_integral(&array, eval_mode, &from_type, to_type)
        }
        (Decimal128(_p, _s), Boolean) => spark_cast_decimal_to_boolean(&array),
        (Utf8View, Utf8) => Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?),
        (Struct(_), Utf8) => Ok(casts_struct_to_string(array.as_struct(), cast_options)?),
        (Struct(_), Struct(_)) => Ok(cast_struct_to_struct(
            array.as_struct(),
            &from_type,
            to_type,
            cast_options,
        )?),
        (List(_), Utf8) => Ok(cast_array_to_string(array.as_list(), cast_options)?),
        (List(_), List(_)) if can_cast_types(&from_type, to_type) => {
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }
        (Map(_, _), Map(_, _)) => Ok(cast_map_to_map(&array, &from_type, to_type, cast_options)?),
        (UInt8 | UInt16 | UInt32 | UInt64, Int8 | Int16 | Int32 | Int64)
            if cast_options.allow_cast_unsigned_ints =>
        {
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }
        (Binary, Utf8) => Ok(cast_binary_to_string::<i32>(&array, cast_options)?),
        (Date32, Timestamp(_, tz)) => Ok(cast_date_to_timestamp(&array, cast_options, tz)?),
        (Int8, Binary) if (eval_mode == Legacy) => cast_whole_num_to_binary!(&array, Int8Array, 1),
        (Int16, Binary) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary!(&array, Int16Array, 2)
        }
        (Int32, Binary) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary!(&array, Int32Array, 4)
        }
        (Int64, Binary) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary!(&array, Int64Array, 8)
        }
        (Boolean, Decimal128(precision, scale)) => {
            cast_boolean_to_decimal(&array, *precision, *scale)
        }
        (Int8 | Int16 | Int32 | Int64, Timestamp(_, tz)) => cast_int_to_timestamp(&array, tz),
        _ if cast_options.is_adapting_schema
            || is_datafusion_spark_compatible(&from_type, to_type) =>
        {
            // use DataFusion cast only when we know that it is compatible with Spark
            Ok(cast_with_options(&array, to_type, &native_cast_options)?)
        }
        _ => {
            // we should never reach this code because the Scala code should be checking
            // for supported cast operations and falling back to Spark for anything that
            // is not yet supported
            Err(SparkError::Internal(format!(
                "Native cast invoked for unsupported cast from {from_type:?} to {to_type:?}"
            )))
        }
    };

    Ok(spark_cast_postprocess(cast_result?, &from_type, to_type))
}

fn cast_int_to_timestamp(
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

fn cast_date_to_timestamp(
    array_ref: &ArrayRef,
    cast_options: &SparkCastOptions,
    target_tz: &Option<Arc<str>>,
) -> SparkResult<ArrayRef> {
    let tz_str = if cast_options.timezone.is_empty() {
        "UTC"
    } else {
        cast_options.timezone.as_str()
    };
    // safe to unwrap since we are falling back to UTC above
    let tz = timezone::Tz::from_str(tz_str)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date_array = array_ref.as_primitive::<Date32Type>();

    let mut builder = TimestampMicrosecondBuilder::with_capacity(date_array.len());

    for date in date_array.iter() {
        match date {
            Some(date) => {
                // safe to unwrap since chrono's range ( 262,143 yrs) is higher than
                // number of years possible with days as i32 (~ 6 mil yrs)
                // convert date in session timezone to timestamp in UTC
                let naive_date = epoch + chrono::Duration::days(date as i64);
                let local_midnight = naive_date.and_hms_opt(0, 0, 0).unwrap();
                let local_midnight_in_microsec = tz
                    .from_local_datetime(&local_midnight)
                    // return earliest possible time (edge case with spring / fall DST changes)
                    .earliest()
                    .map(|dt| dt.timestamp_micros())
                    // in case there is an issue with DST and returns None , we fall back to UTC
                    .unwrap_or((date as i64) * 86_400 * 1_000_000);
                builder.append_value(local_midnight_in_microsec);
            }
            None => {
                builder.append_null();
            }
        }
    }
    Ok(Arc::new(
        builder.finish().with_timezone_opt(target_tz.clone()),
    ))
}

/// Determines if DataFusion supports the given cast in a way that is
/// compatible with Spark
fn is_datafusion_spark_compatible(from_type: &DataType, to_type: &DataType) -> bool {
    if from_type == to_type {
        return true;
    }
    match from_type {
        DataType::Null => {
            matches!(to_type, DataType::List(_))
        }
        DataType::Boolean => is_df_cast_from_bool_spark_compatible(to_type),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
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
        DataType::Float32 | DataType::Float64 => matches!(
            to_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        ),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => matches!(
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
        ),
        DataType::Utf8 => is_df_cast_from_string_spark_compatible(to_type),
        DataType::Date32 => matches!(to_type, DataType::Int32 | DataType::Utf8),
        DataType::Timestamp(_, _) => {
            matches!(
                to_type,
                DataType::Int64 | DataType::Date32 | DataType::Utf8 | DataType::Timestamp(_, _)
            )
        }
        DataType::Binary => {
            // note that this is not completely Spark compatible because
            // DataFusion only supports binary data containing valid UTF-8 strings
            matches!(to_type, DataType::Utf8)
        }
        _ => false,
    }
}

/// Cast between struct types based on logic in
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
fn cast_struct_to_struct(
    array: &StructArray,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            let cast_fields: Vec<ArrayRef> = from_fields
                .iter()
                .enumerate()
                .zip(to_fields.iter())
                .map(|((idx, _from), to)| {
                    let from_field = Arc::clone(array.column(idx));
                    let array_length = from_field.len();
                    let cast_result = spark_cast(
                        ColumnarValue::from(from_field),
                        to.data_type(),
                        cast_options,
                    )
                    .unwrap();
                    cast_result.to_array(array_length).unwrap()
                })
                .collect();

            Ok(Arc::new(StructArray::new(
                to_fields.clone(),
                cast_fields,
                array.nulls().cloned(),
            )))
        }
        _ => unreachable!(),
    }
}

/// Cast between map types, handling field name differences between Parquet ("key_value")
/// and Spark ("entries") while preserving the map's structure.
fn cast_map_to_map(
    array: &ArrayRef,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    let map_array = array
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("Expected a MapArray");

    match (from_type, to_type) {
        (
            DataType::Map(from_entries_field, from_sorted),
            DataType::Map(to_entries_field, _to_sorted),
        ) => {
            // Get the struct types for entries
            let from_struct_type = from_entries_field.data_type();
            let to_struct_type = to_entries_field.data_type();

            match (from_struct_type, to_struct_type) {
                (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
                    // Get the key and value types
                    let from_key_type = from_fields[0].data_type();
                    let from_value_type = from_fields[1].data_type();
                    let to_key_type = to_fields[0].data_type();
                    let to_value_type = to_fields[1].data_type();

                    // Cast keys if needed
                    let keys = map_array.keys();
                    let cast_keys = if from_key_type != to_key_type {
                        cast_array(Arc::clone(keys), to_key_type, cast_options)?
                    } else {
                        Arc::clone(keys)
                    };

                    // Cast values if needed
                    let values = map_array.values();
                    let cast_values = if from_value_type != to_value_type {
                        cast_array(Arc::clone(values), to_value_type, cast_options)?
                    } else {
                        Arc::clone(values)
                    };

                    // Build the new entries struct with the target field names
                    let new_key_field = Arc::new(Field::new(
                        to_fields[0].name(),
                        to_key_type.clone(),
                        to_fields[0].is_nullable(),
                    ));
                    let new_value_field = Arc::new(Field::new(
                        to_fields[1].name(),
                        to_value_type.clone(),
                        to_fields[1].is_nullable(),
                    ));

                    let struct_fields = Fields::from(vec![new_key_field, new_value_field]);
                    let entries_struct =
                        StructArray::new(struct_fields, vec![cast_keys, cast_values], None);

                    // Create the new map field with the target name
                    let new_entries_field = Arc::new(Field::new(
                        to_entries_field.name(),
                        DataType::Struct(entries_struct.fields().clone()),
                        to_entries_field.is_nullable(),
                    ));

                    // Build the new MapArray
                    let new_map = MapArray::new(
                        new_entries_field,
                        map_array.offsets().clone(),
                        entries_struct,
                        map_array.nulls().cloned(),
                        *from_sorted,
                    );

                    Ok(Arc::new(new_map))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Map entries must be structs, got {:?} and {:?}",
                    from_struct_type, to_struct_type
                ))),
            }
        }
        _ => unreachable!("cast_map_to_map called with non-Map types"),
    }
}

fn cast_array_to_string(
    array: &ListArray,
    spark_cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut str = String::with_capacity(array.len() * 16);

    let casted_values = cast_array(
        Arc::clone(array.values()),
        &DataType::Utf8,
        spark_cast_options,
    )?;
    let string_values = casted_values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Casted values should be StringArray");

    let offsets = array.offsets();
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            str.clear();
            let start = offsets[row_index] as usize;
            let end = offsets[row_index + 1] as usize;

            str.push('[');
            let mut first = true;
            for idx in start..end {
                if !first {
                    str.push_str(", ");
                }
                if string_values.is_null(idx) {
                    str.push_str(&spark_cast_options.null_string);
                } else {
                    str.push_str(string_values.value(idx));
                }
                first = false;
            }
            str.push(']');
            builder.append_value(&str);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn casts_struct_to_string(
    array: &StructArray,
    spark_cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    // cast each field to a string
    let string_arrays: Vec<ArrayRef> = array
        .columns()
        .iter()
        .map(|arr| {
            spark_cast(
                ColumnarValue::Array(Arc::clone(arr)),
                &DataType::Utf8,
                spark_cast_options,
            )
            .and_then(|cv| cv.into_array(arr.len()))
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    let string_arrays: Vec<&StringArray> =
        string_arrays.iter().map(|arr| arr.as_string()).collect();
    // build the struct string containing entries in the format `"field_name":field_value`
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut str = String::with_capacity(array.len() * 16);
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            str.clear();
            let mut any_fields_written = false;
            str.push('{');
            for field in &string_arrays {
                if any_fields_written {
                    str.push_str(", ");
                }
                if field.is_null(row_index) {
                    str.push_str(&spark_cast_options.null_string);
                } else {
                    str.push_str(field.value(row_index));
                }
                any_fields_written = true;
            }
            str.push('}');
            builder.append_value(&str);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn cast_float64_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    cast_floating_point_to_decimal128::<Float64Type>(array, precision, scale, eval_mode)
}

fn cast_float32_to_decimal128(
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

fn spark_cast_float64_to_utf8<OffsetSize>(
    from: &dyn Array,
    _eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, _eval_mode, f64, Float64Array, OffsetSize)
}

fn spark_cast_float32_to_utf8<OffsetSize>(
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

fn cast_int_to_decimal128(
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

fn spark_cast_int_to_int(
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

fn spark_cast_decimal_to_boolean(array: &dyn Array) -> SparkResult<ArrayRef> {
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

fn spark_cast_nonintegral_numeric_to_integral(
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

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.cast_options.timezone, self.child, &self.cast_options.eval_mode
        )
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        spark_cast(arg, &self.data_type, &self.cast_options)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Cast::new(
                Arc::clone(&children[0]),
                self.data_type.clone(),
                self.cast_options.clone(),
            ))),
            _ => internal_err!("Cast should have exactly one child"),
        }
    }
}

fn cast_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    spark_cast_options: &SparkCastOptions,
) -> Result<ArrayRef, ArrowError> {
    let input = array
        .as_any()
        .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
        .unwrap();

    fn binary_formatter(value: &[u8], spark_cast_options: &SparkCastOptions) -> String {
        match spark_cast_options.binary_output_style {
            Some(s) => spark_binary_formatter(value, s),
            None => cast_binary_formatter(value),
        }
    }

    let output_array = input
        .iter()
        .map(|value| match value {
            Some(value) => Ok(Some(binary_formatter(value, spark_cast_options))),
            _ => Ok(None),
        })
        .collect::<Result<GenericStringArray<O>, ArrowError>>()?;
    Ok(Arc::new(output_array))
}

/// This function mimics the [BinaryFormatter]: https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ToStringBase.scala#L449-L468
/// used by SparkSQL's ToPrettyString expression.
/// The BinaryFormatter was [introduced]: https://issues.apache.org/jira/browse/SPARK-47911 in Spark 4.0.0
/// Before Spark 4.0.0, the default is SPACE_DELIMITED_UPPERCASE_HEX
fn spark_binary_formatter(value: &[u8], binary_output_style: BinaryOutputStyle) -> String {
    match binary_output_style {
        BinaryOutputStyle::Utf8 => String::from_utf8(value.to_vec()).unwrap(),
        BinaryOutputStyle::Basic => {
            format!(
                "{:?}",
                value
                    .iter()
                    .map(|v| i8::from_ne_bytes([*v]))
                    .collect::<Vec<i8>>()
            )
        }
        BinaryOutputStyle::Base64 => BASE64_STANDARD_NO_PAD.encode(value),
        BinaryOutputStyle::Hex => value
            .iter()
            .map(|v| hex::encode_upper([*v]))
            .collect::<String>(),
        BinaryOutputStyle::HexDiscrete => {
            // Spark's default SPACE_DELIMITED_UPPERCASE_HEX
            format!(
                "[{}]",
                value
                    .iter()
                    .map(|v| hex::encode_upper([*v]))
                    .collect::<Vec<String>>()
                    .join(" ")
            )
        }
    }
}

fn cast_binary_formatter(value: &[u8]) -> String {
    match String::from_utf8(value.to_vec()) {
        Ok(value) => value,
        Err(_) => unsafe { String::from_utf8_unchecked(value.to_vec()) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::TimestampMicrosecondType;
    use arrow::datatypes::{Field, Fields, TimeUnit};
    use core::f64;

    #[test]
    fn test_cast_unsupported_timestamp_to_date() {
        // Since datafusion uses chrono::Datetime internally not all dates representable by TimestampMicrosecondType are supported
        let timestamps: PrimitiveArray<TimestampMicrosecondType> = vec![i64::MAX].into();
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
        let result = cast_array(
            Arc::new(timestamps.with_timezone("Europe/Copenhagen")),
            &DataType::Date32,
            &cast_options,
        );
        assert!(result.is_err())
    }

    #[test]
    fn test_cast_invalid_timezone() {
        let timestamps: PrimitiveArray<TimestampMicrosecondType> = vec![i64::MAX].into();
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, "Not a valid timezone", false);
        let result = cast_array(
            Arc::new(timestamps.with_timezone("Europe/Copenhagen")),
            &DataType::Date32,
            &cast_options,
        );
        assert!(result.is_err())
    }

    #[test]
    fn test_cast_date_to_timestamp() {
        use arrow::array::Date32Array;

        // verifying epoch , DST change dates (US) and a null value (comprehensive tests on spark side)
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(19723),
            Some(19793),
            None,
        ]));

        let non_dst_date = 1704067200000000i64;
        let dst_date = 1710115200000000i64;
        let seven_hours_ts = 25200000000i64;
        let eight_hours_ts = 28800000000i64;

        // validate UTC
        let result = cast_array(
            Arc::clone(&dates),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), non_dst_date);
        assert_eq!(ts.value(2), dst_date);
        assert!(ts.is_null(3));

        // validate LA timezone (follows Daylight savings)
        let result = cast_array(
            Arc::clone(&dates),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &SparkCastOptions::new(EvalMode::Legacy, "America/Los_Angeles", false),
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), eight_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + eight_hours_ts);
        // should adjust for DST
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));

        // Phoenix timezone (does not follow Daylight savings)
        let result = cast_array(
            Arc::clone(&dates),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &SparkCastOptions::new(EvalMode::Legacy, "America/Phoenix", false),
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), seven_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + seven_hours_ts);
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));
    }

    #[test]
    fn test_cast_struct_to_utf8() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        let string_array = cast_array(
            c,
            &DataType::Utf8,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(5, string_array.len());
        assert_eq!(r#"{1, a}"#, string_array.value(0));
        assert_eq!(r#"{2, b}"#, string_array.value(1));
        assert_eq!(r#"{null, c}"#, string_array.value(2));
        assert_eq!(r#"{4, d}"#, string_array.value(3));
        assert_eq!(r#"{5, e}"#, string_array.value(4));
    }

    #[test]
    fn test_cast_struct_to_struct() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        // change type of "a" from Int32 to Utf8
        let fields = Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let cast_array = spark_cast(
            ColumnarValue::Array(c),
            &DataType::Struct(fields),
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        if let ColumnarValue::Array(cast_array) = cast_array {
            assert_eq!(5, cast_array.len());
            let a = cast_array.as_struct().column(0).as_string::<i32>();
            assert_eq!("1", a.value(0));
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_cast_struct_to_struct_drop_column() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        // change type of "a" from Int32 to Utf8 and drop "b"
        let fields = Fields::from(vec![Field::new("a", DataType::Utf8, true)]);
        let cast_array = spark_cast(
            ColumnarValue::Array(c),
            &DataType::Struct(fields),
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        if let ColumnarValue::Array(cast_array) = cast_array {
            assert_eq!(5, cast_array.len());
            let struct_array = cast_array.as_struct();
            assert_eq!(1, struct_array.columns().len());
            let a = struct_array.column(0).as_string::<i32>();
            assert_eq!("1", a.value(0));
        } else {
            unreachable!()
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
    fn test_cast_string_array_to_string() {
        use arrow::array::ListArray;
        use arrow::buffer::OffsetBuffer;
        let values_array =
            StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("a"), None, None]);
        let offsets_buffer = OffsetBuffer::<i32>::new(vec![0, 3, 5, 6, 6].into());
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list_array = Arc::new(ListArray::new(
            item_field,
            offsets_buffer,
            Arc::new(values_array),
            None,
        ));
        let string_array = cast_array_to_string(
            &list_array,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(r#"[a, b, c]"#, string_array.value(0));
        assert_eq!(r#"[a, null]"#, string_array.value(1));
        assert_eq!(r#"[null]"#, string_array.value(2));
        assert_eq!(r#"[]"#, string_array.value(3));
    }

    #[test]
    fn test_cast_i32_array_to_string() {
        use arrow::array::ListArray;
        use arrow::buffer::OffsetBuffer;
        let values_array = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(1), None, None]);
        let offsets_buffer = OffsetBuffer::<i32>::new(vec![0, 3, 5, 6, 6].into());
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array = Arc::new(ListArray::new(
            item_field,
            offsets_buffer,
            Arc::new(values_array),
            None,
        ));
        let string_array = cast_array_to_string(
            &list_array,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(r#"[1, 2, 3]"#, string_array.value(0));
        assert_eq!(r#"[1, null]"#, string_array.value(1));
        assert_eq!(r#"[null]"#, string_array.value(2));
        assert_eq!(r#"[]"#, string_array.value(3));
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
}
