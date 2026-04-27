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
    cast_boolean_to_decimal, cast_boolean_to_timestamp, is_df_cast_from_bool_spark_compatible,
};
use crate::conversion_funcs::numeric::{
    cast_decimal128_to_utf8, cast_decimal128_to_utf8view, cast_decimal_to_timestamp,
    cast_float32_to_decimal128, cast_float64_to_decimal128, cast_float_to_timestamp,
    cast_int_to_decimal128, cast_int_to_timestamp, is_df_cast_from_decimal_spark_compatible,
    is_df_cast_from_float_spark_compatible, is_df_cast_from_int_spark_compatible,
    spark_cast_decimal_to_boolean, spark_cast_float32_to_utf8, spark_cast_float32_to_utf8view,
    spark_cast_float64_to_utf8, spark_cast_float64_to_utf8view, spark_cast_int_to_int,
    spark_cast_nonintegral_numeric_to_integral,
};
use crate::conversion_funcs::string::{
    cast_string_to_date, cast_string_to_decimal128, cast_string_to_decimal256,
    cast_string_to_float, cast_string_to_int, cast_string_to_timestamp,
    cast_string_to_timestamp_ntz, is_df_cast_from_string_spark_compatible,
    spark_cast_utf8_to_boolean,
};
use crate::conversion_funcs::temporal::{
    cast_date_to_timestamp, is_df_cast_from_date_spark_compatible,
    is_df_cast_from_timestamp_spark_compatible,
};
use crate::conversion_funcs::utils::spark_cast_postprocess;
use crate::utils::{array_with_timezone, cast_timestamp_to_ntz, timestamp_ntz_to_timestamp};
use crate::EvalMode::Legacy;
use crate::{cast_whole_num_to_binary, cast_whole_num_to_binary_view, BinaryOutputStyle};
use crate::{EvalMode, SparkError};
use arrow::array::builder::{StringBuilder, StringViewBuilder};
use arrow::array::{
    new_null_array, BinaryBuilder, DictionaryArray, GenericByteArray, ListArray, MapArray,
    StringArray, StructArray,
};
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Float32Type, Float64Type, Schema,
};
use arrow::datatypes::{Field, Fields, GenericBinaryType};
use arrow::error::ArrowError;
use arrow::{
    array::{
        cast::AsArray, types::Int32Type, Array, ArrayRef, GenericStringArray, Int16Array,
        Int32Array, Int64Array, Int8Array, OffsetSizeTrait, PrimitiveArray,
    },
    compute::{cast_with_options, take, CastOptions},
    record_batch::RecordBatch,
    util::display::{ArrayFormatter, FormatOptions},
};
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use datafusion::common::{internal_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::Hash,
    sync::Arc,
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

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
    pub expr_id: Option<u64>,
    pub query_context: Option<Arc<crate::QueryContext>>,
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

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        cast_options: SparkCastOptions,
        expr_id: Option<u64>,
        query_context: Option<Arc<crate::QueryContext>>,
    ) -> Self {
        Self {
            child,
            data_type,
            cast_options,
            expr_id,
            query_context,
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
    /// True when running against Spark 4.0+. Enables version-specific cast behaviour
    /// such as the handling of leading whitespace before T-prefixed time-only strings.
    pub is_spark4_plus: bool,
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
            is_spark4_plus: false,
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
            is_spark4_plus: false,
            allow_cast_unsigned_ints: false,
            is_adapting_schema: false,
            null_string: "null".to_string(),
            binary_output_style: None,
        }
    }

    pub fn new_with_version(
        eval_mode: EvalMode,
        timezone: &str,
        allow_incompat: bool,
        is_spark4_plus: bool,
    ) -> Self {
        Self {
            is_spark4_plus,
            ..Self::new(eval_mode, timezone, allow_incompat)
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
        // Null arrays carry no concrete values, so Arrow's native cast can change only the
        // logical type while preserving length and nullness.
        (Null, _) => Ok(cast_with_options(&array, to_type, &native_cast_options)?),
        // FROM string → Boolean
        (Utf8, Boolean) => spark_cast_utf8_to_boolean(array.as_string::<i32>(), eval_mode),
        (LargeUtf8, Boolean) => spark_cast_utf8_to_boolean(array.as_string::<i64>(), eval_mode),
        (Utf8View, Boolean) => spark_cast_utf8_to_boolean(array.as_string_view(), eval_mode),
        // FROM string → Timestamp NTZ
        (Utf8, Timestamp(_, None)) => cast_string_to_timestamp_ntz(
            array.as_string::<i32>(),
            eval_mode,
            true,
            cast_options.is_spark4_plus,
        ),
        (Utf8View, Timestamp(_, None)) => cast_string_to_timestamp_ntz(
            array.as_string_view(),
            eval_mode,
            true,
            cast_options.is_spark4_plus,
        ),
        // FROM string → Timestamp TZ
        (Utf8, Timestamp(_, _)) => cast_string_to_timestamp(
            array.as_string::<i32>(),
            to_type,
            eval_mode,
            &cast_options.timezone,
            cast_options.is_spark4_plus,
        ),
        (Utf8View, Timestamp(_, _)) => cast_string_to_timestamp(
            array.as_string_view(),
            to_type,
            eval_mode,
            &cast_options.timezone,
            cast_options.is_spark4_plus,
        ),
        // FROM string → Date
        (Utf8, Date32) => cast_string_to_date(array.as_string::<i32>(), to_type, eval_mode),
        (Utf8View, Date32) => cast_string_to_date(array.as_string_view(), to_type, eval_mode),
        (Date32, Int32) => Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?),
        // FROM string → Float
        (Utf8, Float32) => {
            cast_string_to_float::<_, Float32Type>(array.as_string::<i32>(), eval_mode, "FLOAT")
        }
        (Utf8, Float64) => {
            cast_string_to_float::<_, Float64Type>(array.as_string::<i32>(), eval_mode, "DOUBLE")
        }
        (Utf8View, Float32) => {
            cast_string_to_float::<_, Float32Type>(array.as_string_view(), eval_mode, "FLOAT")
        }
        (Utf8View, Float64) => {
            cast_string_to_float::<_, Float64Type>(array.as_string_view(), eval_mode, "DOUBLE")
        }
        // FROM string → Decimal
        (Utf8 | LargeUtf8, Decimal128(precision, scale)) => {
            cast_string_to_decimal128(array.as_string::<i32>(), eval_mode, *precision, *scale)
        }
        (Utf8View, Decimal128(precision, scale)) => {
            cast_string_to_decimal128(array.as_string_view(), eval_mode, *precision, *scale)
        }
        (Utf8 | LargeUtf8, Decimal256(precision, scale)) => {
            cast_string_to_decimal256(array.as_string::<i32>(), eval_mode, *precision, *scale)
        }
        (Utf8View, Decimal256(precision, scale)) => {
            cast_string_to_decimal256(array.as_string_view(), eval_mode, *precision, *scale)
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
        // FROM string → Int
        (Utf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int(to_type, array.as_string::<i32>(), eval_mode)
        }
        (LargeUtf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int(to_type, array.as_string::<i64>(), eval_mode)
        }
        (Utf8View, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int(to_type, array.as_string_view(), eval_mode)
        }
        // TO string from Float (Spark-specific formatting)
        (Float64, Utf8) => spark_cast_float64_to_utf8::<i32>(&array, eval_mode),
        (Float64, LargeUtf8) => spark_cast_float64_to_utf8::<i64>(&array, eval_mode),
        (Float64, Utf8View) => spark_cast_float64_to_utf8view(&array, eval_mode),
        (Float32, Utf8) => spark_cast_float32_to_utf8::<i32>(&array, eval_mode),
        (Float32, LargeUtf8) => spark_cast_float32_to_utf8::<i64>(&array, eval_mode),
        (Float32, Utf8View) => spark_cast_float32_to_utf8view(&array, eval_mode),
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
        // Spark LEGACY cast uses Java BigDecimal.toString() which produces scientific notation
        // when adjusted_exponent < -6 (e.g. "0E-18" for zero with scale=18).
        // TRY and ANSI use plain notation ("0.000000000000000000") so DataFusion handles those.
        (Decimal128(_, scale), Utf8) if eval_mode == EvalMode::Legacy => {
            cast_decimal128_to_utf8(&array, *scale)
        }
        (Decimal128(_, scale), Utf8View) if eval_mode == EvalMode::Legacy => {
            cast_decimal128_to_utf8view(&array, *scale)
        }
        (Utf8View, Utf8) => Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?),
        (Struct(_), Utf8) => Ok(casts_struct_to_string(array.as_struct(), cast_options)?),
        (Struct(_), Utf8View) => Ok(casts_struct_to_string_view(
            array.as_struct(),
            cast_options,
        )?),
        (Struct(_), Struct(_)) => Ok(cast_struct_to_struct(
            array.as_struct(),
            &from_type,
            to_type,
            cast_options,
        )?),
        (List(_), Utf8) => Ok(cast_array_to_string(array.as_list(), cast_options)?),
        (List(_), Utf8View) => Ok(cast_array_to_string_view(array.as_list(), cast_options)?),
        (List(_), List(to)) => {
            // Cast list elements recursively so nested array casts follow Spark semantics
            // instead of relying on Arrow's top-level cast support.
            let list_array = array.as_list::<i32>();
            let casted_values = match (list_array.values().data_type(), to.data_type()) {
                // Spark legacy array casts produce null elements for array<Date> -> array<Int>.
                (Date32, Int32) => new_null_array(to.data_type(), list_array.values().len()),
                _ => cast_array(
                    Arc::clone(list_array.values()),
                    to.data_type(),
                    cast_options,
                )?,
            };
            Ok(Arc::new(ListArray::new(
                Arc::clone(to),
                list_array.offsets().clone(),
                casted_values,
                list_array.nulls().cloned(),
            )) as ArrayRef)
        }
        (Map(_, _), Map(_, _)) => Ok(cast_map_to_map(&array, &from_type, to_type, cast_options)?),
        (UInt8 | UInt16 | UInt32 | UInt64, Int8 | Int16 | Int32 | Int64)
            if cast_options.allow_cast_unsigned_ints =>
        {
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }
        (Binary, Utf8) => Ok(cast_binary_to_string::<i32>(&array, cast_options)?),
        (Binary, Utf8View) => {
            let utf8_result = cast_binary_to_string::<i32>(&array, cast_options)?;
            Ok(cast_with_options(
                &utf8_result,
                &DataType::Utf8View,
                &CAST_OPTIONS,
            )?)
        }
        (BinaryView, Utf8) => Ok(cast_binary_view_to_string(&array, cast_options)?),
        (BinaryView, Utf8View) => {
            let utf8_result = cast_binary_view_to_string(&array, cast_options)?;
            Ok(cast_with_options(
                &utf8_result,
                &DataType::Utf8View,
                &CAST_OPTIONS,
            )?)
        }
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
        (Int8, BinaryView) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary_view!(&array, Int8Array, 1)
        }
        (Int16, BinaryView) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary_view!(&array, Int16Array, 2)
        }
        (Int32, BinaryView) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary_view!(&array, Int32Array, 4)
        }
        (Int64, BinaryView) if (eval_mode == Legacy) => {
            cast_whole_num_to_binary_view!(&array, Int64Array, 8)
        }
        (Boolean, Decimal128(precision, scale)) => {
            cast_boolean_to_decimal(&array, *precision, *scale)
        }
        (Int8 | Int16 | Int32 | Int64, Timestamp(_, tz)) => cast_int_to_timestamp(&array, tz),
        (Float32 | Float64, Timestamp(_, tz)) => cast_float_to_timestamp(&array, tz, eval_mode),
        (Boolean, Timestamp(_, tz)) => cast_boolean_to_timestamp(&array, tz),
        (Decimal128(_, scale), Timestamp(_, tz)) => cast_decimal_to_timestamp(&array, tz, *scale),
        // NTZ → TIMESTAMP: interpret NTZ local-epoch value as session-TZ local time, convert to UTC.
        // Must come before the is_datafusion_spark_compatible fallthrough which would
        // incorrectly copy raw μs without any timezone conversion.
        (Timestamp(_, None), Timestamp(_, Some(target_tz))) => Ok(timestamp_ntz_to_timestamp(
            array,
            &cast_options.timezone,
            Some(target_tz.as_ref()),
        )?),
        // TIMESTAMP → NTZ: shift UTC epoch to local time in session TZ, store as local epoch.
        (Timestamp(_, Some(_)), Timestamp(_, None)) => {
            Ok(cast_timestamp_to_ntz(array, &cast_options.timezone)?)
        }
        // NTZ → Date32 and NTZ → Utf8 are handled by the DataFusion fall-through below
        // (is_df_cast_from_timestamp_spark_compatible returns true for Date32 and Utf8).
        // These casts are timezone-independent and DataFusion's implementation matches Spark.
        // NTZ → Utf8View: Arrow doesn't support this directly, so we format using
        // ArrayFormatter which respects our timestamp format options.
        (Timestamp(_, None), Utf8View) => {
            Ok(timestamp_to_string_view(&array, &native_cast_options)?)
        }
        _ if cast_options.is_adapting_schema
            || is_datafusion_spark_compatible(&from_type, to_type) =>
        {
            Ok(cast_with_options(&array, to_type, &native_cast_options)?)
        }
        _ => Err(SparkError::Internal(format!(
            "Native cast invoked for unsupported cast from {from_type:?} to {to_type:?}"
        ))),
    };

    Ok(spark_cast_postprocess(cast_result?, &from_type, to_type))
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
            is_df_cast_from_int_spark_compatible(to_type)
        }
        DataType::Float32 | DataType::Float64 => is_df_cast_from_float_spark_compatible(to_type),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            is_df_cast_from_decimal_spark_compatible(to_type)
        }
        DataType::Utf8 | DataType::Utf8View => is_df_cast_from_string_spark_compatible(to_type),
        DataType::Date32 => is_df_cast_from_date_spark_compatible(to_type),
        DataType::Timestamp(_, _) => is_df_cast_from_timestamp_spark_compatible(to_type),
        DataType::Binary | DataType::BinaryView => {
            // note that this is not completely Spark compatible because
            // DataFusion only supports binary data containing valid UTF-8 strings
            matches!(to_type, DataType::Utf8 | DataType::Utf8View)
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

fn cast_array_to_string_view(
    array: &ListArray,
    spark_cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
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

fn casts_struct_to_string_view(
    array: &StructArray,
    spark_cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
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
    let mut builder = StringViewBuilder::with_capacity(array.len());
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
        let result = spark_cast(arg, &self.data_type, &self.cast_options);

        // If there's an error and we have query_context, wrap it
        match result {
            Err(DataFusionError::External(e)) if self.query_context.is_some() => {
                if let Some(spark_err) = e.downcast_ref::<crate::SparkError>() {
                    let wrapped = crate::SparkErrorWithContext::with_context(
                        spark_err.clone(),
                        Arc::clone(self.query_context.as_ref().unwrap()),
                    );
                    Err(DataFusionError::External(Box::new(wrapped)))
                } else {
                    Err(DataFusionError::External(e))
                }
            }
            other => other,
        }
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
                self.expr_id,
                self.query_context.clone(),
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

fn cast_binary_view_to_string(
    array: &dyn Array,
    spark_cast_options: &SparkCastOptions,
) -> Result<ArrayRef, ArrowError> {
    let input = array.as_binary_view();

    fn binary_formatter(value: &[u8], spark_cast_options: &SparkCastOptions) -> String {
        match spark_cast_options.binary_output_style {
            Some(s) => spark_binary_formatter(value, s),
            None => cast_binary_formatter(value),
        }
    }

    let mut builder = StringBuilder::with_capacity(input.len(), input.len() * 16);
    for opt_val in input.iter() {
        match opt_val {
            Some(value) => builder.append_value(binary_formatter(value, spark_cast_options)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Cast any array to Utf8View using ArrayFormatter, which respects format options
/// (e.g., timestamp format). This mirrors Arrow's internal `value_to_string_view`.
fn timestamp_to_string_view(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
    let formatter = ArrayFormatter::try_new(array, &cast_options.format_options)?;
    let nulls = array.nulls();
    let mut buffer = String::new();
    for i in 0..array.len() {
        if nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            builder.append_null();
        } else {
            buffer.clear();
            formatter.value(i).write(&mut buffer)?;
            builder.append_value(&buffer);
        }
    }
    Ok(Arc::new(builder.finish()))
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
    use arrow::array::{ListArray, NullArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::TimestampMicrosecondType;
    use arrow::datatypes::{Field, Fields};
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
    fn test_cast_string_array_to_string() {
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
    fn test_cast_array_of_nulls_to_array() {
        let offsets_buffer = OffsetBuffer::<i32>::new(vec![0, 2, 3, 3].into());
        let from_item_field = Arc::new(Field::new("item", DataType::Null, true));
        let from_array: ArrayRef = Arc::new(ListArray::new(
            from_item_field,
            offsets_buffer,
            Arc::new(NullArray::new(3)),
            None,
        ));

        let to_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let to_array = cast_array(
            from_array,
            &to_type,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();

        let result = to_array.as_list::<i32>();
        assert_eq!(3, result.len());
        assert_eq!(result.value_offsets(), &[0, 2, 3, 3]);

        let values = result.values().as_primitive::<Int32Type>();
        assert_eq!(3, values.len());
        assert_eq!(3, values.null_count());
        assert!(values.iter().all(|value| value.is_none()));
    }
}
