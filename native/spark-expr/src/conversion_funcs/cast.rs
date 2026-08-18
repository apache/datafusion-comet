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
    cast_boolean_to_timestamp, is_df_cast_from_bool_spark_compatible,
};
use crate::conversion_funcs::numeric::{
    cast_decimal128_to_utf8, cast_decimal_to_timestamp, cast_float32_to_decimal128,
    cast_float64_to_decimal128, cast_float_to_timestamp, cast_int_to_decimal128,
    cast_int_to_timestamp, is_df_cast_from_decimal_spark_compatible,
    is_df_cast_from_float_spark_compatible, is_df_cast_from_int_spark_compatible,
    spark_cast_decimal_to_boolean, spark_cast_float32_to_utf8, spark_cast_float64_to_utf8,
    spark_cast_int_to_int, spark_cast_nonintegral_numeric_to_integral,
};
use crate::conversion_funcs::string::{
    cast_string_to_date, cast_string_to_decimal, cast_string_to_float, cast_string_to_int,
    cast_string_to_timestamp, cast_string_to_timestamp_ntz,
    is_df_cast_from_string_spark_compatible, spark_cast_utf8_to_boolean,
};
use crate::conversion_funcs::temporal::{
    cast_date_to_timestamp, is_df_cast_from_date_spark_compatible,
    is_df_cast_from_timestamp_spark_compatible,
};
use crate::conversion_funcs::utils::spark_cast_postprocess;
use crate::utils::{array_with_timezone, cast_timestamp_to_ntz, timestamp_ntz_to_timestamp};
use crate::EvalMode::Legacy;
use crate::{cast_whole_num_to_binary, BinaryOutputStyle};
use crate::{EvalMode, SparkError};
use arrow::array::builder::{GenericStringBuilder, StringBuilder};
use arrow::array::{
    new_null_array, BinaryBuilder, DictionaryArray, GenericByteArray, ListArray, MapArray,
    StringArray, StructArray,
};
use arrow::datatypes::GenericBinaryType;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType, Schema};
use arrow::error::ArrowError;
use arrow::{
    array::{
        cast::AsArray, types::Int32Type, Array, ArrayRef, Int16Array, Int32Array, Int64Array,
        Int8Array, OffsetSizeTrait, PrimitiveArray,
    },
    compute::{cast_with_options, take, CastOptions},
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use datafusion::common::{internal_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_common::decode_utf8_spark_lossy;
use std::{
    fmt::{Debug, Display, Formatter, Write},
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

/// Arrow `CastOptions` matching a Comet eval mode: ANSI surfaces conversion failures as errors,
/// the other modes substitute null.
fn arrow_cast_options(eval_mode: EvalMode) -> CastOptions<'static> {
    CastOptions {
        safe: !matches!(eval_mode, EvalMode::Ansi),
        format_options: FormatOptions::new()
            .with_timestamp_tz_format(TIMESTAMP_FORMAT)
            .with_timestamp_format(TIMESTAMP_FORMAT),
    }
}

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

    let native_cast_options: CastOptions = arrow_cast_options(cast_options.eval_mode);

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
        (Utf8, Boolean) => spark_cast_utf8_to_boolean::<i32>(&array, eval_mode),
        (LargeUtf8, Boolean) => spark_cast_utf8_to_boolean::<i64>(&array, eval_mode),
        (Utf8, Timestamp(_, None)) => {
            cast_string_to_timestamp_ntz(&array, eval_mode, true, cast_options.is_spark4_plus)
        }
        (Utf8, Timestamp(_, _)) => cast_string_to_timestamp(
            &array,
            to_type,
            eval_mode,
            &cast_options.timezone,
            cast_options.is_spark4_plus,
        ),
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
        // Spark LEGACY cast uses Java BigDecimal.toString() which produces scientific notation
        // when adjusted_exponent < -6 (e.g. "0E-18" for zero with scale=18).
        // TRY and ANSI use plain notation ("0.000000000000000000") so DataFusion handles those.
        (Decimal128(_, scale), Utf8) if eval_mode == EvalMode::Legacy => {
            cast_decimal128_to_utf8(&array, *scale)
        }
        (Utf8View, Utf8) => Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?),
        (Struct(_), Utf8) => Ok(casts_struct_to_string(array.as_struct(), cast_options)?),
        (Struct(_), Struct(_)) => Ok(cast_struct_to_struct(
            array.as_struct(),
            &from_type,
            to_type,
            cast_options,
        )?),
        (List(_), Utf8) => Ok(cast_array_to_string(array.as_list(), cast_options)?),
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
        DataType::Utf8 => is_df_cast_from_string_spark_compatible(to_type),
        DataType::Date32 => is_df_cast_from_date_spark_compatible(to_type),
        DataType::Timestamp(_, _) => is_df_cast_from_timestamp_spark_compatible(to_type),
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
                    )?;
                    cast_result.to_array(array_length)
                })
                .collect::<DataFusionResult<Vec<_>>>()?;

            Ok(Arc::new(StructArray::new(
                to_fields.clone(),
                cast_fields,
                array.nulls().cloned(),
            )))
        }
        _ => unreachable!(),
    }
}

/// Cast between map types (e.g. Parquet "key_value" -> Spark "entries").
///
/// - Rename-only (unchanged key/value types and sort order): delegate to arrow's `cast`, which
///   relabels to the target fields and preserves their metadata with no value transformation.
/// - Otherwise (a child type or the sort flag differs): recurse with Comet's `cast_array` for a
///   changed child and hand-build the result with the target sort flag. `try_new` is used so a
///   malformed target returns `Err` rather than panicking.
///
/// Either way the result `data_type()` equals `to_type`.
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
            DataType::Map(to_entries_field, to_sorted),
        ) => {
            let (from_fields, to_fields) =
                match (from_entries_field.data_type(), to_entries_field.data_type()) {
                    (DataType::Struct(f), DataType::Struct(t)) => (f, t),
                    (from_struct_type, to_struct_type) => {
                        return Err(DataFusionError::Internal(format!(
                            "Map entries must be structs, got {from_struct_type:?} and \
                             {to_struct_type:?}"
                        )))
                    }
                };
            // A map entries struct is always exactly (key, value); guard before indexing [0]/[1] so
            // a malformed target (0/1/3+ fields) returns Err instead of panicking.
            if from_fields.len() != 2 || to_fields.len() != 2 {
                return Err(DataFusionError::Internal(format!(
                    "Map entries struct must have exactly 2 fields (key, value); got from={} to={}",
                    from_fields.len(),
                    to_fields.len()
                )));
            }
            let key_type_unchanged = from_fields[0].data_type() == to_fields[0].data_type();
            let value_type_unchanged = from_fields[1].data_type() == to_fields[1].data_type();

            // Rename-only fast path (the common Parquet "key_value" -> Spark "entries" case): the
            // key and value types are unchanged and the sort order is unchanged, so only the field
            // labels/metadata differ. Delegate to arrow's cast, whose map arm requires matching sort
            // flags and relabels to the target fields, preserving their metadata and the values.
            if key_type_unchanged && value_type_unchanged && from_sorted == to_sorted {
                return Ok(cast_with_options(
                    array,
                    to_type,
                    &arrow_cast_options(cast_options.eval_mode),
                )?);
            }

            // Otherwise a child type or the sort flag differs. Recurse with Comet's Spark-compatible
            // casts for the changed children and hand-build the result carrying the target sort flag.
            // `try_new` reports a malformed target as `Err` rather than panicking.
            let keys = map_array.keys();
            let cast_keys = if key_type_unchanged {
                Arc::clone(keys)
            } else {
                cast_array(Arc::clone(keys), to_fields[0].data_type(), cast_options)?
            };
            let values = map_array.values();
            let cast_values = if value_type_unchanged {
                Arc::clone(values)
            } else {
                cast_array(Arc::clone(values), to_fields[1].data_type(), cast_options)?
            };

            let entries_struct = StructArray::try_new(
                to_fields.clone(),
                vec![cast_keys, cast_values],
                map_array.entries().nulls().cloned(),
            )?;
            let new_map = MapArray::try_new(
                Arc::clone(to_entries_field),
                map_array.offsets().clone(),
                entries_struct,
                map_array.nulls().cloned(),
                *to_sorted,
            )?;
            Ok(Arc::new(new_map))
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

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.cast_options.timezone, self.child, self.cast_options.eval_mode
        )
    }
}

impl PhysicalExpr for Cast {
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

const UPPER_HEX_DIGITS: [u8; 16] = *b"0123456789ABCDEF";

/// Writes `byte` as two uppercase hex digits.
#[inline]
fn write_upper_hex<W: Write>(out: &mut W, byte: u8) -> std::fmt::Result {
    let buf = [
        UPPER_HEX_DIGITS[(byte >> 4) as usize],
        UPPER_HEX_DIGITS[(byte & 0x0f) as usize],
    ];
    // SAFETY: both bytes come from the ASCII UPPER_HEX_DIGITS table, so `buf` is valid UTF-8.
    out.write_str(unsafe { std::str::from_utf8_unchecked(&buf) })
}

/// Writes `byte` reinterpreted as a signed decimal, as Spark does when printing a byte array.
#[inline]
fn write_i8<W: Write>(out: &mut W, byte: u8) -> std::fmt::Result {
    write!(out, "{}", byte as i8)
}

/// Writes the bytes of `value` between square brackets, encoded with `encode` and joined by
/// `separator`.
fn write_bracketed<W: Write>(
    out: &mut W,
    value: &[u8],
    separator: &str,
    encode: fn(&mut W, u8) -> std::fmt::Result,
) -> std::fmt::Result {
    out.write_char('[')?;
    for (i, byte) in value.iter().enumerate() {
        if i > 0 {
            out.write_str(separator)?;
        }
        encode(out, *byte)?;
    }
    out.write_char(']')
}

/// Casts a binary array to a string array. Without a binary output style the bytes are
/// reinterpreted as a string as-is, which is what Spark's `Cast` does.
///
/// The other styles mimic the [BinaryFormatter]: https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ToStringBase.scala#L449-L468
/// used by SparkSQL's ToPrettyString expression.
/// The BinaryFormatter was [introduced]: https://issues.apache.org/jira/browse/SPARK-47911 in Spark 4.0.0
/// Before Spark 4.0.0, the default is SPACE_DELIMITED_UPPERCASE_HEX
fn cast_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    spark_cast_options: &SparkCastOptions,
) -> Result<ArrayRef, ArrowError> {
    let input = array
        .as_any()
        .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
        .unwrap();

    let num_rows = input.len();
    let offsets = input.value_offsets();
    let value_bytes = offsets[num_rows].as_usize() - offsets[0].as_usize();
    // Upper bound on the encoded length so the value buffer is allocated once. For the
    // JVM-lossy UTF-8 decoder path, valid UTF-8 sits at 1x; the builder grows on the rare
    // invalid byte that expands to U+FFFD. Base64 rounds up to a full 4-char group via
    // div_ceil, which already covers the group padding.
    let capacity = match spark_cast_options.binary_output_style {
        None | Some(BinaryOutputStyle::Utf8) => value_bytes,
        Some(BinaryOutputStyle::Basic) => 6 * value_bytes + 2 * num_rows,
        Some(BinaryOutputStyle::Base64) => 4 * value_bytes.div_ceil(3),
        Some(BinaryOutputStyle::Hex) => 2 * value_bytes,
        Some(BinaryOutputStyle::HexDiscrete) => 3 * value_bytes + 2 * num_rows,
    };

    let mut builder = GenericStringBuilder::<O>::with_capacity(num_rows, capacity);
    // Base64 is the only style that cannot encode straight into the builder.
    let mut base64_buffer = String::new();
    for value in input.iter() {
        let Some(value) = value else {
            // The previous iteration always finalized its row with `append_value("")`, so no
            // bytes are pending in the builder here; a future edit that adds a `continue` or
            // an early return inside the match below would break that invariant.
            builder.append_null();
            continue;
        };
        // Encode directly into the builder's value buffer; `append_value("")` then terminates
        // the row. Writing to the builder is infallible.
        let written = match spark_cast_options.binary_output_style {
            // Default CAST(binary AS string) and the UTF8 ToPrettyString style (Spark 4.0+) both
            // render via `new String(bytes, UTF_8)`. Route through the shared JVM-compatible lossy
            // decoder so ill-formed bytes become U+FFFD (matching Spark) instead of being
            // reinterpreted unchecked (UB) or panicking on non-UTF-8 input (#4488, #4763). The
            // valid-UTF-8 path borrows, so it copies once (into the Arrow value buffer). Divergence
            // for byte-level round-trips such as CAST(CAST(x AS string) AS binary) and value
            // identity is documented in the compatibility guide and tracked by #4764.
            None | Some(BinaryOutputStyle::Utf8) => {
                builder.write_str(&decode_utf8_spark_lossy(value))
            }
            Some(BinaryOutputStyle::Basic) => write_bracketed(&mut builder, value, ", ", write_i8),
            Some(BinaryOutputStyle::Base64) => {
                base64_buffer.clear();
                BASE64_STANDARD_NO_PAD.encode_string(value, &mut base64_buffer);
                builder.write_str(&base64_buffer)
            }
            Some(BinaryOutputStyle::Hex) => value
                .iter()
                .try_for_each(|byte| write_upper_hex(&mut builder, *byte)),
            // Spark's default SPACE_DELIMITED_UPPERCASE_HEX
            Some(BinaryOutputStyle::HexDiscrete) => {
                write_bracketed(&mut builder, value, " ", write_upper_hex)
            }
        };
        written.expect("writing to a string builder cannot fail");
        builder.append_value("");
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, ListArray, NullArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::TimestampMicrosecondType;
    use arrow::datatypes::{Field, Fields};

    #[test]
    fn test_cast_binary_to_string_replaces_invalid_utf8_jvm_compatibly() {
        // Invalid bytes are replaced with U+FFFD instead of reinterpreted as an invalid `str`,
        // and the granularity matches the JVM: the surrogate-range sequence [ED A0 80] collapses
        // to a single U+FFFD (Rust's `from_utf8_lossy` would emit three). Valid UTF-8 ("abc") is
        // preserved exactly and NULL stays NULL.
        let input = BinaryArray::from_opt_vec(vec![
            Some(&[0xFFu8, 0xFE][..]),
            None,
            Some("abc".as_bytes()),
            Some(&[0xEDu8, 0xA0, 0x80][..]),
        ]);
        // binary_output_style defaults to None, i.e. the plain (non-ToPrettyString) cast path.
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);

        let result = cast_binary_to_string::<i32>(&input, &cast_options).unwrap();

        let strings = result.as_string::<i32>();
        assert_eq!(strings.len(), 4);
        assert_eq!(strings.value(0), "\u{FFFD}\u{FFFD}");
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), "abc");
        assert_eq!(strings.value(3), "\u{FFFD}");
    }

    #[test]
    fn test_cast_binary_to_string_utf8_output_style_replaces_invalid_utf8() {
        // Spark's `binaryOutputStyle=UTF8` (Spark 4.0+) ToPrettyString formatter renders binary via
        // `new String(bytes, UTF_8)`. Previously this arm called `String::from_utf8(..).unwrap()`,
        // which panicked the executor on non-UTF-8 input. It must now decode JVM-compatibly-lossily,
        // matching the default cast path's replacement behavior.
        let input = BinaryArray::from_opt_vec(vec![
            Some(&[0xFFu8, 0xFE][..]),
            None,
            Some("abc".as_bytes()),
            Some(&[0xEDu8, 0xA0, 0x80][..]),
        ]);
        let mut cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
        cast_options.binary_output_style = Some(BinaryOutputStyle::Utf8);

        let result = cast_binary_to_string::<i32>(&input, &cast_options).unwrap();

        let strings = result.as_string::<i32>();
        assert_eq!(strings.len(), 4);
        assert_eq!(strings.value(0), "\u{FFFD}\u{FFFD}");
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), "abc");
        assert_eq!(strings.value(3), "\u{FFFD}");
    }

    #[test]
    fn test_cast_binary_to_string_styles() {
        let input: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"\x00\x01\xfe".as_slice()),
            Some(b"".as_slice()),
            None,
            Some(b"hi".as_slice()),
        ]));
        let cast = |style: Option<BinaryOutputStyle>| {
            let mut options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
            options.binary_output_style = style;
            let result = spark_cast(
                ColumnarValue::Array(Arc::clone(&input)),
                &DataType::Utf8,
                &options,
            )
            .unwrap()
            .into_array(input.len())
            .unwrap();
            let result = result.as_string::<i32>();
            (0..result.len())
                .map(|i| (!result.is_null(i)).then(|| result.value(i).to_string()))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            cast(Some(BinaryOutputStyle::HexDiscrete)),
            vec![
                Some("[00 01 FE]".to_string()),
                Some("[]".to_string()),
                None,
                Some("[68 69]".to_string()),
            ]
        );
        assert_eq!(
            cast(Some(BinaryOutputStyle::Hex)),
            vec![
                Some("0001FE".to_string()),
                Some("".to_string()),
                None,
                Some("6869".to_string()),
            ]
        );
        assert_eq!(
            cast(Some(BinaryOutputStyle::Basic)),
            vec![
                Some("[0, 1, -2]".to_string()),
                Some("[]".to_string()),
                None,
                Some("[104, 105]".to_string()),
            ]
        );
        assert_eq!(
            cast(Some(BinaryOutputStyle::Base64)),
            vec![
                Some("AAH+".to_string()),
                Some("".to_string()),
                None,
                Some("aGk".to_string()),
            ]
        );
    }

    #[test]
    fn test_cast_binary_to_string_default_style_valid_utf8_through_spark_cast() {
        // Exercises the default cast path through the public `spark_cast` entry point. Invalid bytes
        // decode JVM-compatibly-lossily to U+FFFD (#4763), while valid multi-byte UTF-8 ("héllo") is
        // preserved exactly.
        let input: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"\xff\xfe".as_slice()),
            None,
            Some("héllo".as_bytes()),
        ]));
        let options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
        let result = spark_cast(
            ColumnarValue::Array(Arc::clone(&input)),
            &DataType::Utf8,
            &options,
        )
        .unwrap()
        .into_array(input.len())
        .unwrap();
        let result = result.as_string::<i32>();
        assert_eq!(result.value(0), "\u{FFFD}\u{FFFD}");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "héllo");
    }

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
    fn test_cast_nested_struct_to_struct_ansi_overflow_returns_error() {
        let inner_values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(128), None]));
        let from_nested_fields =
            Fields::from(vec![Field::new("long_value", DataType::Int64, true)]);
        let nested: ArrayRef = Arc::new(StructArray::new(
            from_nested_fields.clone(),
            vec![inner_values],
            None,
        ));
        let from_fields = Fields::from(vec![Field::new(
            "nested",
            DataType::Struct(from_nested_fields),
            true,
        )]);
        let outer: ArrayRef = Arc::new(StructArray::new(from_fields, vec![nested], None));

        let to_nested_fields = Fields::from(vec![Field::new("byte_value", DataType::Int8, true)]);
        let to_fields = Fields::from(vec![Field::new(
            "renamed_nested",
            DataType::Struct(to_nested_fields),
            true,
        )]);
        let result = spark_cast(
            ColumnarValue::Array(outer),
            &DataType::Struct(to_fields),
            &SparkCastOptions::new(EvalMode::Ansi, "UTC", false),
        );

        assert!(result.is_err());
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

    fn legacy_opts() -> SparkCastOptions {
        SparkCastOptions::new(EvalMode::Legacy, "UTC", false)
    }

    /// Build a `Map<Utf8, Int32>` MapArray (Parquet-style "key_value" field names).
    fn build_str_i32_map(
        keys: Vec<&str>,
        values: Vec<Option<i32>>,
        offsets: Vec<i32>,
        map_nulls: Option<arrow::buffer::NullBuffer>,
        entries_nulls: Option<arrow::buffer::NullBuffer>,
        sorted: bool,
    ) -> MapArray {
        use arrow::array::{Int32Array, StringArray};
        let key_field = Arc::new(Field::new("key_value_key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("key_value_value", DataType::Int32, true));
        let entries_fields = Fields::from(vec![key_field, value_field]);
        let ks = Arc::new(StringArray::from(keys)) as ArrayRef;
        let vs = Arc::new(Int32Array::from(values)) as ArrayRef;
        let entries_struct = StructArray::new(entries_fields, vec![ks, vs], entries_nulls);
        let entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(entries_struct.fields().clone()),
            false,
        ));
        MapArray::new(
            entries_field,
            OffsetBuffer::<i32>::new(offsets.into()),
            entries_struct,
            map_nulls,
            sorted,
        )
    }

    /// Build a target `Map<Utf8, val_type>` type ("entries"/"key"/"value" Spark-style names).
    fn build_to_map_type(val_type: DataType, val_nullable: bool, sorted: bool) -> DataType {
        let to_key = Arc::new(Field::new("key", DataType::Utf8, false));
        let to_val = Arc::new(Field::new("value", val_type, val_nullable));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![to_key, to_val])),
            false,
        ));
        DataType::Map(entries, sorted)
    }

    /// Assert which branch of `cast_map_to_map` a (from, to) type pair selects, by checking the
    /// three inputs the branch condition is built from.
    fn assert_map_child_types_and_sort(
        from_type: &DataType,
        to_type: &DataType,
        expect_key_unchanged: bool,
        expect_value_unchanged: bool,
        expect_same_sort: bool,
    ) {
        let children = |t: &DataType| match t {
            DataType::Map(entries, sorted) => match entries.data_type() {
                DataType::Struct(f) => {
                    assert_eq!(f.len(), 2, "map entries must be (key, value)");
                    (f[0].data_type().clone(), f[1].data_type().clone(), *sorted)
                }
                other => panic!("map entries must be a struct, got {other:?}"),
            },
            other => panic!("expected a Map type, got {other:?}"),
        };
        let (from_key, from_val, from_sorted) = children(from_type);
        let (to_key, to_val, to_sorted) = children(to_type);
        assert_eq!(
            from_key == to_key,
            expect_key_unchanged,
            "key type unchanged: {from_key:?} vs {to_key:?}"
        );
        assert_eq!(
            from_val == to_val,
            expect_value_unchanged,
            "value type unchanged: {from_val:?} vs {to_val:?}"
        );
        assert_eq!(
            from_sorted == to_sorted,
            expect_same_sort,
            "sort flag unchanged: {from_sorted} vs {to_sorted}"
        );
    }

    fn sorted_src(from_sorted: bool) -> ArrayRef {
        Arc::new(build_str_i32_map(
            vec!["a", "b", "c"],
            vec![Some(1), Some(2), Some(3)],
            vec![0, 3],
            None,
            None,
            from_sorted,
        )) as ArrayRef
    }

    #[test]
    fn test_cast_map_to_map_sorted_equal_flags_allowed() {
        // false -> false and true -> true: the sort flag is unchanged and the result type matches.
        for s in [false, true] {
            let to_type = build_to_map_type(DataType::Int32, true, s);
            let casted = cast_array(sorted_src(s), &to_type, &legacy_opts()).unwrap();
            assert_eq!(casted.data_type(), &to_type, "sorted={s}");
        }
    }

    #[test]
    fn test_cast_map_to_map_sorted_true_to_false_allowed() {
        // Downgrade a sorted map to unsorted: allowed, result carries the target (false) flag.
        let to_type = build_to_map_type(DataType::Int32, true, false);
        let casted = cast_array(sorted_src(true), &to_type, &legacy_opts()).unwrap();
        assert_eq!(casted.data_type(), &to_type);
        match casted.data_type() {
            DataType::Map(_, is_sorted) => assert!(!*is_sorted),
            _ => panic!("Expected Map DataType"),
        }
    }

    #[test]
    fn test_cast_map_to_map_sorted_value_only_cast_allowed() {
        use arrow::array::{Int64Array, StringArray};
        // Sorted source, key type unchanged, value Int32 -> Int64, target sorted=true. The key
        // ordering is unaffected by a value cast, so this is allowed and stays sorted.
        let src = Arc::new(build_str_i32_map(
            vec!["a", "b", "c"],
            vec![Some(10), Some(20), Some(30)],
            vec![0, 3],
            None,
            None,
            true, // source sorted
        )) as ArrayRef;
        let to_key = Arc::new(Field::new("key", DataType::Utf8, false));
        let to_val = Arc::new(Field::new("value", DataType::Int64, true));
        let to_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![to_key, to_val])),
            false,
        ));
        let to_type = DataType::Map(Arc::clone(&to_entries), true);

        let casted = cast_array(src, &to_type, &legacy_opts()).unwrap();
        // Exact target type, and the sort flag is preserved.
        assert_eq!(casted.data_type(), &to_type);
        let m = casted.as_any().downcast_ref::<MapArray>().unwrap();
        match m.data_type() {
            DataType::Map(_, is_sorted) => assert!(*is_sorted),
            _ => panic!("Expected Map DataType"),
        }
        // Keys unchanged; values correctly cast to Int64.
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = m.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            (0..3).map(|i| keys.value(i)).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(vals.values(), &[10i64, 20, 30]);
    }

    #[test]
    fn test_cast_map_to_map_preserves_metadata_and_child_type_casts() {
        use arrow::array::{Int32Array, Int64Array, StringArray};
        use std::collections::HashMap;

        let key_field = Arc::new(Field::new("key_value_key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("key_value_value", DataType::Int32, true));
        let entries_fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(StringArray::from(vec!["k1", "k2"]));
        let values = Arc::new(Int32Array::from(vec![10, 20]));
        let entries_struct = StructArray::new(entries_fields, vec![keys, values], None);

        let from_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(entries_struct.fields().clone()),
            false,
        ));
        let map_array = Arc::new(MapArray::new(
            from_entries_field,
            OffsetBuffer::<i32>::new(vec![0, 2].into()),
            entries_struct,
            None,
            false,
        )) as ArrayRef;

        // Target key field with custom metadata
        let mut key_meta = HashMap::new();
        key_meta.insert("tag".to_string(), "map_key_meta".to_string());
        let to_key_field =
            Arc::new(Field::new("key", DataType::Utf8, false).with_metadata(key_meta));
        let to_value_field = Arc::new(Field::new("value", DataType::Int64, true));
        let to_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![to_key_field, to_value_field])),
            false,
        ));
        let to_type = DataType::Map(to_entries_field, false);

        let casted = cast_array(
            map_array,
            &to_type,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();

        let casted_map = casted.as_any().downcast_ref::<MapArray>().unwrap();
        // Result type is exactly the requested target type (incl. field metadata).
        assert_eq!(casted_map.data_type(), &to_type);
        // Assert key field metadata is preserved
        assert_eq!(
            casted_map.entries().fields()[0].metadata().get("tag"),
            Some(&"map_key_meta".to_string())
        );

        // Assert child values were cast from Int32 to Int64
        let casted_values = casted_map
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(casted_values.value(0), 10i64);
        assert_eq!(casted_values.value(1), 20i64);
    }

    #[test]
    fn test_cast_map_to_map_preserves_map_level_nulls_and_offsets() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::buffer::NullBuffer;

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let entries_fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(StringArray::from(vec!["a", "b"]));
        let values = Arc::new(Int32Array::from(vec![1, 2]));
        let entries_struct = StructArray::new(entries_fields, vec![keys, values], None);

        let from_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(entries_struct.fields().clone()),
            false,
        ));

        let map_nulls = NullBuffer::from(vec![true, false]);
        let src_map = MapArray::new(
            from_entries_field,
            OffsetBuffer::<i32>::new(vec![0, 2, 2].into()),
            entries_struct,
            Some(map_nulls.clone()),
            false,
        );
        let map_array = Arc::new(src_map) as ArrayRef;

        let to_key_field = Arc::new(Field::new("new_key", DataType::Utf8, false));
        let to_value_field = Arc::new(Field::new("new_value", DataType::Int32, true));
        let to_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![to_key_field, to_value_field])),
            false,
        ));
        let to_type = DataType::Map(to_entries_field, false);

        let casted = cast_array(
            map_array,
            &to_type,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();

        let casted_map = casted.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(casted_map.data_type(), &to_type);
        assert_eq!(casted_map.nulls(), Some(&map_nulls));
        assert!(casted_map.is_null(1));
        assert_eq!(casted_map.offsets().as_ref(), &[0, 2, 2]);
    }

    // NOTE on entries-struct null buffers: an entries struct carrying a non-None null buffer is not
    // constructible through the inspected safe Arrow constructors — `StructArray::new`/`try_new` and
    // `MapArray` all normalize an all-valid `NullBuffer` to `None`, and a struct with real nulls is
    // not a valid map entries array. (ArrayData/FFI paths were not audited, so this is not a claim
    // of global unobservability.) The production code still clones `entries().nulls()` to mirror
    // arrow's `cast_map_values`, but entries-level null preservation is not asserted (it would be a
    // vacuous `None == None`). Map-LEVEL null preservation is covered by
    // `test_cast_map_to_map_preserves_map_level_nulls_and_offsets`.
    #[test]
    fn test_cast_map_to_map_rename_only_fast_path() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::buffer::NullBuffer;
        use std::collections::HashMap;

        // Key/value types unchanged and sort order unchanged -> the rename-only fast path (arrow
        // cast) is used. Three rows including a null row so offsets and map nulls are non-trivial.
        let map_nulls = NullBuffer::from(vec![true, false, true]);
        let src = build_str_i32_map(
            vec!["a", "b", "c"],
            vec![Some(1), Some(2), Some(3)],
            vec![0, 2, 2, 3],
            Some(map_nulls.clone()),
            None,
            false,
        );
        let src_offsets: Vec<i32> = src.offsets().as_ref().to_vec();
        let map_array = Arc::new(src) as ArrayRef;

        // Complete target schema: renamed entries/key/value fields, outer + key metadata, same
        // (unchanged) child types, same sort flag.
        let mut outer_meta = HashMap::new();
        outer_meta.insert("outer".to_string(), "entries_meta".to_string());
        let mut key_meta = HashMap::new();
        key_meta.insert("k".to_string(), "kmeta".to_string());
        let to_key = Arc::new(Field::new("key", DataType::Utf8, false).with_metadata(key_meta));
        let to_val = Arc::new(Field::new("value", DataType::Int32, true));
        let to_entries = Arc::new(
            Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![to_key, to_val])),
                false,
            )
            .with_metadata(outer_meta),
        );
        let to_type = DataType::Map(Arc::clone(&to_entries), false);

        let casted = cast_array(map_array, &to_type, &legacy_opts()).unwrap();
        let casted_map = casted.as_any().downcast_ref::<MapArray>().unwrap();

        // The complete target schema is reproduced exactly (field names, metadata, nullability, sort).
        assert_eq!(casted_map.data_type(), &to_type);
        // Map-level nulls and offsets are unchanged by the relabel.
        assert_eq!(casted_map.nulls(), Some(&map_nulls));
        assert!(casted_map.is_null(1));
        assert_eq!(casted_map.offsets().as_ref(), src_offsets.as_slice());
        // Keys and values are unchanged by the relabel.
        let keys = casted_map
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = casted_map
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..3).map(|i| keys.value(i)).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(vals.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_cast_map_to_map_empty_map() {
        let src = Arc::new(build_str_i32_map(
            vec![],
            vec![],
            vec![0],
            None,
            None,
            false,
        )) as ArrayRef;
        let to_type = build_to_map_type(DataType::Int64, true, false);
        let casted = cast_array(src, &to_type, &legacy_opts()).unwrap();
        assert_eq!(casted.data_type(), &to_type);
        let m = casted.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn test_cast_map_to_map_mixed_null_and_empty_rows() {
        use arrow::buffer::NullBuffer;
        // row0 = NULL, row1 = empty {}, row2 = {a:1, b:2}
        let map_nulls = NullBuffer::from(vec![false, true, true]);
        let src = Arc::new(build_str_i32_map(
            vec!["a", "b"],
            vec![Some(1), Some(2)],
            vec![0, 0, 0, 2],
            Some(map_nulls.clone()),
            None,
            false,
        )) as ArrayRef;
        let to_type = build_to_map_type(DataType::Int64, true, false);
        let casted = cast_array(src, &to_type, &legacy_opts()).unwrap();
        assert_eq!(casted.data_type(), &to_type);
        let m = casted.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(m.nulls(), Some(&map_nulls));
        assert!(m.is_null(0));
        assert!(!m.is_null(1)); // empty but valid
        assert_eq!(m.offsets().as_ref(), &[0, 0, 0, 2]);
    }

    #[test]
    fn test_cast_map_to_map_sliced() {
        use arrow::array::{Int64Array, StringArray};
        // 3 rows: {a:1}, {b:2, c:3}, {d:4}; slice to rows [1, 2].
        let full = Arc::new(build_str_i32_map(
            vec!["a", "b", "c", "d"],
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![0, 1, 3, 4],
            None,
            None,
            false,
        )) as ArrayRef;
        let sliced = full.slice(1, 2);
        let to_type = build_to_map_type(DataType::Int64, true, false);
        let casted = cast_array(sliced, &to_type, &legacy_opts()).unwrap();
        assert_eq!(casted.data_type(), &to_type);
        let m = casted.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(m.len(), 2);
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = m.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let o = m.offsets();
        let (start, end) = (o[0] as usize, o[2] as usize);
        let got_keys: Vec<&str> = (start..end).map(|i| keys.value(i)).collect();
        let got_vals: Vec<i64> = (start..end).map(|i| vals.value(i)).collect();
        assert_eq!(got_keys, vec!["b", "c", "d"]);
        assert_eq!(got_vals, vec![2i64, 3, 4]);
    }

    #[test]
    fn test_cast_map_to_map_sliced_rename_only_fast_path() {
        use arrow::array::{Int32Array, StringArray};
        // Same slicing as `test_cast_map_to_map_sliced`, but the value type is Int32 -> Int32.
        // Key type, value type and sort flag are all unchanged, so only the field labels differ
        // ("key_value"/"key_value_key" -> "entries"/"key") and arrow's cast handles the relabel.
        let full = Arc::new(build_str_i32_map(
            vec!["a", "b", "c", "d"],
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![0, 1, 3, 4],
            None,
            None,
            false,
        )) as ArrayRef;
        let sliced = full.slice(1, 2);
        let to_type = build_to_map_type(DataType::Int32, true, false);

        // The rename-only path is selected on unchanged child types and an unchanged sort flag.
        // Assert that predicate on this input rather than assuming it.
        assert_map_child_types_and_sort(sliced.data_type(), &to_type, true, true, true);

        let casted = cast_array(sliced, &to_type, &legacy_opts()).unwrap();
        assert_eq!(casted.data_type(), &to_type);
        let m = casted.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(m.len(), 2);
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = m.values().as_any().downcast_ref::<Int32Array>().unwrap();
        let o = m.offsets();
        let (start, end) = (o[0] as usize, o[2] as usize);
        let got_keys: Vec<&str> = (start..end).map(|i| keys.value(i)).collect();
        let got_vals: Vec<i32> = (start..end).map(|i| vals.value(i)).collect();
        assert_eq!(got_keys, vec!["b", "c", "d"]);
        assert_eq!(got_vals, vec![2i32, 3, 4]);

        // The rename-only path transforms no values, so the eval mode cannot change the result.
        let ansi = cast_array(
            full.slice(1, 2),
            &to_type,
            &SparkCastOptions::new(EvalMode::Ansi, "UTC", false),
        )
        .unwrap();
        assert_eq!(ansi.to_data(), casted.to_data());
    }

    #[test]
    fn test_cast_map_to_map_both_paths_agree() {
        use arrow::array::{Int32Array, StringArray};
        // Two independent implementations reach the same target type, so pin that they agree.
        // The only difference between the two inputs is the source `sorted` flag, which is not
        // part of the output type: equal flags select the arrow delegation, differing flags
        // select the hand-built path. Everything else (values, offsets, map-level nulls) matches,
        // so the two results must be identical.
        let rows = |sorted: bool| {
            Arc::new(build_str_i32_map(
                vec!["a", "b", "c", "d"],
                vec![Some(1), None, Some(3), Some(4)],
                vec![0, 1, 3, 3, 4],
                Some(arrow::buffer::NullBuffer::from(vec![
                    true, true, false, true,
                ])),
                None,
                sorted,
            )) as ArrayRef
        };
        // Slice off the first row so both paths see a non-zero offset window and a null row.
        let unsorted_src = rows(false).slice(1, 3);
        let sorted_src = rows(true).slice(1, 3);
        let to_type = build_to_map_type(DataType::Int32, true, false);

        // Fast path: child types and sort flag all unchanged.
        assert_map_child_types_and_sort(unsorted_src.data_type(), &to_type, true, true, true);
        // Hand-built path: child types unchanged but the sort flag differs (true -> false).
        assert_map_child_types_and_sort(sorted_src.data_type(), &to_type, true, true, false);

        let fast = cast_array(unsorted_src, &to_type, &legacy_opts()).unwrap();
        let hand_built = cast_array(sorted_src, &to_type, &legacy_opts()).unwrap();

        assert_eq!(fast.data_type(), &to_type);
        assert_eq!(hand_built.data_type(), &to_type);
        assert_eq!(fast.to_data(), hand_built.to_data());

        // Assert the shared result is actually right, so agreement on a wrong value cannot pass.
        let m = fast.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(m.len(), 3);
        assert!(m.is_valid(0) && !m.is_valid(1) && m.is_valid(2));
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = m.values().as_any().downcast_ref::<Int32Array>().unwrap();
        let o = m.offsets();
        let (start, end) = (o[0] as usize, o[3] as usize);
        let got_keys: Vec<&str> = (start..end).map(|i| keys.value(i)).collect();
        let got_vals: Vec<Option<i32>> = (start..end)
            .map(|i| (!vals.is_null(i)).then(|| vals.value(i)))
            .collect();
        assert_eq!(got_keys, vec!["b", "c", "d"]);
        assert_eq!(got_vals, vec![None, Some(3), Some(4)]);
    }

    #[test]
    fn test_cast_map_to_map_casts_key_and_value() {
        use arrow::array::{Int32Array, Int64Array};
        // Source Map<Int32, Int32> -> target Map<Int64, Int64>: both key and value are cast.
        let key_field = Arc::new(Field::new("key_value_key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("key_value_value", DataType::Int32, true));
        let entries_fields = Fields::from(vec![key_field, value_field]);
        let ks = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let vs = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let entries_struct = StructArray::new(entries_fields, vec![ks, vs], None);
        let entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(entries_struct.fields().clone()),
            false,
        ));
        let src = Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::<i32>::new(vec![0, 2].into()),
            entries_struct,
            None,
            false,
        )) as ArrayRef;

        let to_key = Arc::new(Field::new("key", DataType::Int64, false));
        let to_val = Arc::new(Field::new("value", DataType::Int64, true));
        let to_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![to_key, to_val])),
            false,
        ));
        let to_type = DataType::Map(Arc::clone(&to_entries), false);

        let casted = cast_array(src, &to_type, &legacy_opts()).unwrap();
        assert_eq!(casted.data_type(), &to_type);
        let m = casted.as_any().downcast_ref::<MapArray>().unwrap();
        let keys = m.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = m.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.values(), &[1i64, 2]);
        assert_eq!(vals.values(), &[10i64, 20]);
    }

    #[test]
    fn test_cast_map_to_map_malformed_target_returns_err_without_panic() {
        // Source value has a NULL; the target changes the value type (Int32 -> Int64) AND declares
        // it NON-nullable. The type change routes through the hand-built child-cast path, and the
        // resulting null in a non-nullable field makes `StructArray::try_new` return Err (no panic).
        let src = Arc::new(build_str_i32_map(
            vec!["a", "b"],
            vec![Some(1), None],
            vec![0, 2],
            None,
            None,
            false,
        )) as ArrayRef;
        let to_type = build_to_map_type(DataType::Int64, false, false);
        assert!(
            cast_array(src, &to_type, &legacy_opts()).is_err(),
            "non-nullable target value with null data must return Err via hand-built try_new"
        );
    }

    fn one_row_src() -> ArrayRef {
        Arc::new(build_str_i32_map(
            vec!["a"],
            vec![Some(1)],
            vec![0, 1],
            None,
            None,
            false,
        )) as ArrayRef
    }

    fn map_target_with_entry_fields(fields: Vec<Arc<Field>>) -> DataType {
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(fields)),
            false,
        ));
        DataType::Map(entries, false)
    }

    fn entry_field(n: &str, t: DataType) -> Arc<Field> {
        Arc::new(Field::new(n, t, false))
    }

    // A target whose entries struct does not have exactly (key, value) must return Err, never panic
    // on index [0]/[1]. Split per field-count so a baseline panic in one case does not hide others.
    #[test]
    fn test_cast_map_to_map_zero_entry_fields_errs() {
        let to_type = map_target_with_entry_fields(vec![]);
        assert!(
            cast_array(one_row_src(), &to_type, &legacy_opts()).is_err(),
            "0 entry fields must return Err, not panic"
        );
    }

    #[test]
    fn test_cast_map_to_map_one_entry_field_errs() {
        let to_type = map_target_with_entry_fields(vec![entry_field("key", DataType::Utf8)]);
        assert!(
            cast_array(one_row_src(), &to_type, &legacy_opts()).is_err(),
            "1 entry field must return Err, not panic"
        );
    }

    #[test]
    fn test_cast_map_to_map_three_entry_fields_errs() {
        let to_type = map_target_with_entry_fields(vec![
            entry_field("key", DataType::Utf8),
            entry_field("value", DataType::Int32),
            entry_field("extra", DataType::Int32),
        ]);
        assert!(
            cast_array(one_row_src(), &to_type, &legacy_opts()).is_err(),
            "3 entry fields must return Err, not panic"
        );
    }
}
