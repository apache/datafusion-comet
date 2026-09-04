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

use crate::execution::operators::ExecutionError;
use crate::parquet::name_fold::fold_names;
use arrow::array::{FixedSizeBinaryArray, LargeListArray, ListArray, MapArray, StringArray};
use arrow::buffer::NullBuffer;
use arrow::compute::can_cast_types;
use arrow::datatypes::{FieldRef, Fields};
use arrow::{
    array::{
        cast::AsArray, new_null_array, types::TimestampMicrosecondType,
        types::TimestampMillisecondType, Array, ArrayRef, ArrowNativeTypeOp, StructArray,
    },
    compute::{cast_with_options, CastOptions},
    datatypes::{DataType, TimeUnit},
    util::display::FormatOptions,
};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_common::SparkError;
use datafusion_comet_spark_expr::EvalMode;
use log::debug;
use object_store::path::Path;
use object_store::{parse_url, ObjectStore};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use std::{collections::hash_map::DefaultHasher, hash::Hasher, sync::RwLock};
use std::{fmt::Debug, hash::Hash, sync::Arc};
use url::Url;

use super::objectstore;
use super::objectstore::s3_blob_fs_support::normalize_object_store_url;

// This file originates from cast.rs. While developing native scan support and implementing
// SparkSchemaAdapter we observed that Spark's type conversion logic on Parquet reads does not
// always align to the CAST expression's logic, so it was duplicated here to adapt its behavior.

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

static PARQUET_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

/// Spark Parquet type conversion options
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SparkParquetOptions {
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
    /// Whether to read dates/timestamps that were written in the legacy hybrid Julian + Gregorian calendar as it is. If false, throw exceptions instead. If the spark type is TimestampNTZ, this should be true.
    pub use_legacy_date_timestamp_or_ntz: bool,
    // Whether schema field names are case sensitive
    pub case_sensitive: bool,
    /// SPARK-53535 (Spark 4.1+): when reading a struct whose requested fields are all
    /// missing in the Parquet file, true returns the entire struct as null (pre-4.1
    /// legacy behavior); false preserves the parent struct's nullness from the file
    /// so non-null parents return a struct of all-null fields.
    pub return_null_struct_if_all_fields_missing: bool,
    /// When true, resolve fields by parquet.field.id metadata instead of name
    /// (mirrors Spark's `spark.sql.parquet.fieldId.read.enabled`). Only takes effect
    /// when both physical and logical fields actually carry IDs.
    pub use_field_id: bool,
    /// When false (Spark's default), reading a file that has no field ids while the
    /// requested schema does carry ids raises a runtime error rather than silently
    /// producing nulls (mirrors `spark.sql.parquet.fieldId.read.ignoreMissing`).
    pub ignore_missing_field_id: bool,
    /// Whether type promotion (schema evolution) is allowed, e.g. INT32 -> INT64,
    /// FLOAT -> DOUBLE. Mirrors spark.comet.schemaEvolution.enabled.
    pub allow_type_promotion: bool,
    /// When true, reading a Parquet TimestampLTZ column as TimestampNTZ is
    /// permitted (Spark 4.0+, SPARK-47447); when false, it is rejected
    /// (Spark 3.x, SPARK-36182). Mirrors Comet's per-Spark-version constant
    /// in ShimCometConf.
    pub allow_timestamp_ltz_to_ntz: bool,
    /// When true (the default), a top-level TIMESTAMP_MILLIS column that overflows during
    /// the millis->micros upscale raises an error, matching Spark's checked
    /// `millisToMicros`. Filtered scans set this to false and retain the safe cast
    /// (overflow -> NULL), because Spark may discard values through pruning paths that
    /// DataFusion cannot fully mirror before conversion.
    pub checked_timestamp_overflow: bool,
}

impl SparkParquetOptions {
    pub fn new(eval_mode: EvalMode, timezone: &str, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: timezone.to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            use_legacy_date_timestamp_or_ntz: false,
            case_sensitive: false,
            return_null_struct_if_all_fields_missing: true,
            use_field_id: false,
            ignore_missing_field_id: false,
            allow_type_promotion: false,
            allow_timestamp_ltz_to_ntz: false,
            checked_timestamp_overflow: true,
        }
    }

    pub fn new_without_timezone(eval_mode: EvalMode, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: "".to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            use_legacy_date_timestamp_or_ntz: false,
            case_sensitive: false,
            return_null_struct_if_all_fields_missing: true,
            use_field_id: false,
            ignore_missing_field_id: false,
            allow_type_promotion: false,
            allow_timestamp_ltz_to_ntz: false,
            checked_timestamp_overflow: true,
        }
    }
}

/// Spark-compatible cast implementation. Defers to DataFusion's cast where that is known
/// to be compatible, and returns an error when a not supported and not DF-compatible cast
/// is requested. Resolves the nested field mapping for this one value; a per-file caller
/// resolves once and uses [`spark_parquet_convert_with_mapping`] for every batch.
pub fn spark_parquet_convert(
    arg: ColumnarValue,
    data_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ColumnarValue> {
    let mapping =
        resolve_field_mapping(&arg.data_type(), data_type, parquet_options).map_err(spark_error)?;
    spark_parquet_convert_with_mapping(arg, data_type, &mapping, parquet_options)
}

/// [`spark_parquet_convert`] with a mapping already resolved for the value's type.
pub(crate) fn spark_parquet_convert_with_mapping(
    arg: ColumnarValue,
    data_type: &DataType,
    mapping: &FieldMapping,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(convert_array(
            array,
            data_type,
            mapping,
            parquet_options,
            true,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
            // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
            // here.
            let array = scalar.to_array()?;
            let scalar = ScalarValue::try_from_array(
                &convert_array(array, data_type, mapping, parquet_options, true)?,
                0,
            )?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

/// Wrap a [`SparkError`] the way every native operator surfaces it to the JVM.
pub(crate) fn spark_error(error: SparkError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Outcome of matching one requested id or name against a struct's file fields: the last
/// file field that matched and whether more than one did. A plain `Copy` value, so resolving
/// a wide struct allocates nothing per id or per name; the matched names are only gathered
/// when an ambiguity is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FieldMatch {
    pub(crate) index: usize,
    pub(crate) ambiguous: bool,
}

impl FieldMatch {
    pub(crate) fn new(index: usize, ambiguous: bool) -> Self {
        Self { index, ambiguous }
    }

    /// The first file field carrying this id or name.
    pub(crate) fn first(index: usize) -> Self {
        Self::new(index, false)
    }

    /// A further file field carrying the same id or name: the later index wins, as Spark's
    /// `toMap` does for exact names, and the entry turns ambiguous.
    pub(crate) fn also(self, index: usize) -> Self {
        Self::new(index, true)
    }
}

/// Record file field `index` under `key`, keeping the entry `Copy`-sized however many fields
/// share the key.
pub(crate) fn record_field_match<K: Hash + Eq>(
    matches: &mut HashMap<K, FieldMatch>,
    key: K,
    index: usize,
) {
    matches
        .entry(key)
        .and_modify(|m| *m = m.also(index))
        .or_insert_with(|| FieldMatch::first(index));
}

/// Comma-joined names of the fields carrying `id`, for the duplicate-id error message.
pub(crate) fn field_names_with_id(fields: &Fields, id: i32) -> String {
    fields
        .iter()
        .filter(|f| field_id(f) == Some(id))
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Which file field supplies each requested field, resolved once per file and reused for
/// every batch. Follows the requested type as Spark's `clipParquetSchema` does: a struct
/// lists one source per requested field, a list (large or not) or map carries the mapping
/// of its element or key and value types, and anything else is a leaf converted by type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FieldMapping {
    Struct(Vec<StructFieldSource>),
    List(Box<FieldMapping>),
    Map(Box<FieldMapping>, Box<FieldMapping>),
    Leaf,
}

/// The file field behind one requested struct field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StructFieldSource {
    /// Index of the file field supplying the requested field; `None` null-fills it.
    pub(crate) from_index: Option<usize>,
    /// Mapping of the requested field's own type.
    pub(crate) nested: FieldMapping,
}

impl FieldMapping {
    /// True when every requested field reads the file field at its own position, so a
    /// metadata-only relabel of the file array already yields the requested layout.
    pub(crate) fn is_positional(&self) -> bool {
        match self {
            FieldMapping::Struct(sources) => sources
                .iter()
                .enumerate()
                .all(|(i, s)| s.from_index == Some(i) && s.nested.is_positional()),
            FieldMapping::List(inner) => inner.is_positional(),
            FieldMapping::Map(key, value) => key.is_positional() && value.is_positional(),
            FieldMapping::Leaf => true,
        }
    }
}

/// Resolve how `to_type` reads from `from_type`, recursing through struct, list, and map
/// types. Raises the ambiguity Spark reports from `clipParquetGroupFields` when a requested
/// id or case-insensitive name matches more than one file field at any level.
pub(crate) fn resolve_field_mapping(
    from_type: &DataType,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> Result<FieldMapping, SparkError> {
    use DataType::*;
    match (from_type, to_type) {
        (Struct(from_fields), Struct(to_fields)) => {
            resolve_struct_mapping(from_fields, to_fields, parquet_options)
        }
        (List(from_item), List(to_item)) | (LargeList(from_item), LargeList(to_item)) => {
            Ok(FieldMapping::List(Box::new(resolve_field_mapping(
                from_item.data_type(),
                to_item.data_type(),
                parquet_options,
            )?)))
        }
        (Map(from_entries, from_ordered), Map(to_entries, to_ordered))
            if from_ordered == to_ordered =>
        {
            match (from_entries.data_type(), to_entries.data_type()) {
                (Struct(from_kv), Struct(to_kv)) if from_kv.len() == 2 && to_kv.len() == 2 => {
                    let key = resolve_field_mapping(
                        from_kv[0].data_type(),
                        to_kv[0].data_type(),
                        parquet_options,
                    )?;
                    let value = resolve_field_mapping(
                        from_kv[1].data_type(),
                        to_kv[1].data_type(),
                        parquet_options,
                    )?;
                    Ok(FieldMapping::Map(Box::new(key), Box::new(value)))
                }
                _ => Ok(FieldMapping::Leaf),
            }
        }
        _ => Ok(FieldMapping::Leaf),
    }
}

/// Match `to` (requested) struct fields to `from` (file) fields. Mirrors Spark's
/// `clipParquetGroupFields`: when the requested struct carries Parquet field ids anywhere,
/// id-bearing requested fields match only by id and the rest by name; otherwise every field
/// matches by name.
fn resolve_struct_mapping(
    from_fields: &Fields,
    to_fields: &Fields,
    parquet_options: &SparkParquetOptions,
) -> Result<FieldMapping, SparkError> {
    let should_match_by_id =
        parquet_options.use_field_id && to_fields.iter().any(|f| field_id(f).is_some());

    let mut id_matches: HashMap<i32, FieldMatch> = HashMap::new();
    if should_match_by_id {
        for (i, field) in from_fields.iter().enumerate() {
            if let Some(id) = field_id(field) {
                record_field_match(&mut id_matches, id, i);
            }
        }
    }

    // Fold the file and requested names once via the same `toLowerCase(Locale.ROOT)` the
    // top-level schema adapter uses, so nested case-insensitive matching agrees with it.
    let mut all_names: Vec<&str> = Vec::with_capacity(from_fields.len() + to_fields.len());
    all_names.extend(from_fields.iter().map(|f| f.name().as_str()));
    all_names.extend(to_fields.iter().map(|f| f.name().as_str()));
    let all_folded = fold_names(&all_names, parquet_options.case_sensitive);
    let (from_folded, to_folded) = all_folded.split_at(from_fields.len());

    let mut name_matches: HashMap<&str, FieldMatch> = HashMap::new();
    for (i, folded) in from_folded.iter().enumerate() {
        record_field_match(&mut name_matches, folded.as_str(), i);
    }

    let mut sources = Vec::with_capacity(to_fields.len());
    for (to_pos, to_field) in to_fields.iter().enumerate() {
        let from_index = match (should_match_by_id, field_id(to_field)) {
            // A missing id match is a missing column, never a name match.
            (true, Some(id)) => match id_matches.get(&id) {
                Some(m) if m.ambiguous => {
                    return Err(SparkError::DuplicateFieldByFieldId {
                        required_id: id,
                        matched_fields: field_names_with_id(from_fields, id),
                    });
                }
                Some(m) => Some(m.index),
                None => None,
            },
            _ => match name_matches.get(to_folded[to_pos].as_str()) {
                // Spark's `caseInsensitiveParquetFieldMap` rejects a requested name that folds
                // onto more than one file field. In case-sensitive mode the fold is identity, so
                // a collision means byte-identical siblings and the later one wins silently,
                // as with Spark's `caseSensitiveParquetFieldMap` built by `toMap`.
                Some(m) if m.ambiguous && !parquet_options.case_sensitive => {
                    let matched: Vec<&str> = from_folded
                        .iter()
                        .zip(from_fields.iter())
                        .filter(|(folded, _)| *folded == &to_folded[to_pos])
                        .map(|(_, f)| f.name().as_str())
                        .collect();
                    return Err(SparkError::duplicate_field_case_insensitive(
                        to_field.name(),
                        &matched,
                    ));
                }
                Some(m) => Some(m.index),
                None => None,
            },
        };
        let nested = match from_index {
            Some(i) => resolve_field_mapping(
                from_fields[i].data_type(),
                to_field.data_type(),
                parquet_options,
            )?,
            None => FieldMapping::Leaf,
        };
        sources.push(StructFieldSource { from_index, nested });
    }
    Ok(FieldMapping::Struct(sources))
}

/// Convert `array` to `to_type` through its resolved `mapping`. `top_level` is true only for
/// the column itself, never for a struct field, list element, or map entry beneath it.
fn convert_array(
    array: ArrayRef,
    to_type: &DataType,
    mapping: &FieldMapping,
    parquet_options: &SparkParquetOptions,
    top_level: bool,
) -> DataFusionResult<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type();

    // Try Comet specific handlers first, then arrow-rs cast if supported,
    // return uncasted data otherwise
    match (from_type, to_type, mapping) {
        (Struct(_), Struct(to_fields), FieldMapping::Struct(sources)) => {
            convert_struct(array.as_struct(), to_fields, sources, parquet_options)
        }
        (List(_), List(to_inner_type), FieldMapping::List(inner)) => {
            let list_arr: &ListArray = array.as_list();
            let cast_field = convert_array(
                Arc::clone(list_arr.values()),
                to_inner_type.data_type(),
                inner,
                parquet_options,
                false,
            )?;

            Ok(Arc::new(ListArray::new(
                Arc::clone(to_inner_type),
                list_arr.offsets().clone(),
                cast_field,
                list_arr.nulls().cloned(),
            )))
        }
        (LargeList(_), LargeList(to_inner_type), FieldMapping::List(inner)) => {
            let list_arr: &LargeListArray = array.as_list();
            let cast_field = convert_array(
                Arc::clone(list_arr.values()),
                to_inner_type.data_type(),
                inner,
                parquet_options,
                false,
            )?;

            Ok(Arc::new(LargeListArray::new(
                Arc::clone(to_inner_type),
                list_arr.offsets().clone(),
                cast_field,
                list_arr.nulls().cloned(),
            )))
        }
        (Timestamp(TimeUnit::Millisecond, _), Timestamp(TimeUnit::Microsecond, target_tz), _)
            if top_level && parquet_options.checked_timestamp_overflow =>
        {
            // Spark's Parquet reader calls the checked `millisToMicros` conversion for both
            // direct and dictionary values, independent of CAST evaluation mode:
            // https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetVectorUpdaterFactory.java#L817-L833
            // `millisToMicros` uses `Math.multiplyExact`:
            // https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala#L103-L108
            //
            // The checked conversion is limited to TOP-LEVEL columns. Spark only avoids the
            // error for filtered-out values through row-group statistics pruning, and
            // DataFusion's PruningPredicate does not support nested fields yet, so a checked
            // conversion on a nested field would fail queries whose predicates Spark prunes
            // (e.g. `WHERE s.ts < X` over an all-overflowing file). Nested fields keep the
            // pre-existing safe-cast behavior below (overflow -> NULL).
            let micros = array
                .as_primitive::<TimestampMillisecondType>()
                .try_unary::<_, TimestampMicrosecondType, _>(|value| value.mul_checked(1_000))?
                .with_timezone_opt(target_tz.clone());
            Ok(Arc::new(micros))
        }
        (Timestamp(TimeUnit::Microsecond, None), Timestamp(TimeUnit::Microsecond, Some(tz)), _) => {
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .reinterpret_cast::<TimestampMicrosecondType>()
                    .with_timezone(Arc::clone(tz)),
            ))
        }
        (Map(_, ordered_from), Map(_, ordered_to), FieldMapping::Map(key, value))
            if ordered_from == ordered_to =>
        {
            parquet_convert_map_to_map(
                array.as_map(),
                to_type,
                key,
                value,
                parquet_options,
                *ordered_to,
            )
        }
        // Iceberg stores UUIDs as 16-byte fixed binary but Spark expects string representation.
        // Arrow doesn't support casting FixedSizeBinary to Utf8, so we handle it manually.
        (FixedSizeBinary(16), Utf8, _) => {
            let binary_array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("Expected a FixedSizeBinaryArray");

            let string_array: StringArray = binary_array
                .iter()
                .map(|opt_bytes| {
                    opt_bytes.map(|bytes| {
                        let uuid =
                            uuid::Uuid::from_bytes(bytes.try_into().expect("Expected 16 bytes"));
                        uuid.to_string()
                    })
                })
                .collect();

            Ok(Arc::new(string_array))
        }
        // If Arrow cast supports the cast, delegate the cast to Arrow
        _ if can_cast_types(from_type, to_type) => {
            Ok(cast_with_options(&array, to_type, &PARQUET_OPTIONS)?)
        }
        _ => Ok(array),
    }
}

/// Read the Parquet field id stored under arrow-rs's `PARQUET_FIELD_ID_META_KEY`.
pub(crate) fn field_id(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// Build the requested struct from the file struct, reading each requested field from the
/// file field at its resolved source index. Based on
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
fn convert_struct(
    array: &StructArray,
    to_fields: &Fields,
    sources: &[StructFieldSource],
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ArrayRef> {
    if sources.len() != to_fields.len() {
        return Err(DataFusionError::Internal(format!(
            "struct field mapping has {} sources for {} requested fields",
            sources.len(),
            to_fields.len()
        )));
    }

    let mut field_overlap = false;
    let mut cast_fields: Vec<ArrayRef> = Vec::with_capacity(to_fields.len());
    for (to_field, source) in to_fields.iter().zip(sources) {
        match source.from_index {
            Some(from_index) => {
                cast_fields.push(convert_array(
                    Arc::clone(array.column(from_index)),
                    to_field.data_type(),
                    &source.nested,
                    parquet_options,
                    false,
                )?);
                field_overlap = true;
            }
            None => cast_fields.push(new_null_array(to_field.data_type(), array.len())),
        }
    }

    // When the file's struct contains none of the requested fields, the
    // returned validity buffer depends on Spark's
    // `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing` (SPARK-53535,
    // Spark 4.1+). Legacy mode marks the whole column null; the new default
    // preserves the file's parent-row nullness so non-null parents materialize
    // as a struct of all-null fields.
    let nulls = if !field_overlap && parquet_options.return_null_struct_if_all_fields_missing {
        Some(NullBuffer::new_null(array.len()))
    } else {
        array.nulls().cloned()
    };

    Ok(Arc::new(StructArray::new(
        to_fields.clone(),
        cast_fields,
        nulls,
    )))
}

/// Cast a map type to another map type. The same as arrow-cast except we recursively call our own
/// convert_array with the resolved key and value mappings.
fn parquet_convert_map_to_map(
    from: &MapArray,
    to_data_type: &DataType,
    key_mapping: &FieldMapping,
    value_mapping: &FieldMapping,
    parquet_options: &SparkParquetOptions,
    to_ordered: bool,
) -> Result<ArrayRef, DataFusionError> {
    match to_data_type {
        DataType::Map(entries_field, _) => {
            let key_field = key_field(entries_field).ok_or(DataFusionError::Internal(
                "map is missing key field".to_string(),
            ))?;
            let value_field = value_field(entries_field).ok_or(DataFusionError::Internal(
                "map is missing value field".to_string(),
            ))?;

            let key_array = convert_array(
                Arc::clone(from.keys()),
                key_field.data_type(),
                key_mapping,
                parquet_options,
                false,
            )?;
            let value_array = convert_array(
                Arc::clone(from.values()),
                value_field.data_type(),
                value_mapping,
                parquet_options,
                false,
            )?;

            Ok(Arc::new(MapArray::new(
                Arc::<arrow::datatypes::Field>::clone(entries_field),
                from.offsets().clone(),
                StructArray::new(
                    Fields::from(vec![key_field, value_field]),
                    vec![key_array, value_array],
                    from.entries().nulls().cloned(),
                ),
                from.nulls().cloned(),
                to_ordered,
            )))
        }
        dt => Err(DataFusionError::Internal(format!(
            "Expected MapType. Got: {dt}"
        ))),
    }
}

/// Gets the key field from the entries of a map.  For all other types returns None.
fn key_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.first().cloned()
    } else {
        None
    }
}

/// Gets the value field from the entries of a map.  For all other types returns None.
fn value_field(entries_field: &FieldRef) -> Option<FieldRef> {
    if let DataType::Struct(fields) = entries_field.data_type() {
        fields.get(1).cloned()
    } else {
        None
    }
}

/// True if `scheme` appears in a Comet comma-separated scheme-list config, compared trimmed and
/// case-insensitively. Shared by every such config (`fs.comet.libhdfs.schemes`,
/// `fs.comet.s3Compliant.schemes`) so native parses them exactly like the JVM's
/// `NativeConfig.parseSchemeSet`, which feeds the planner's fallback gate.
pub(crate) fn scheme_in_list(list: &str, scheme: &str) -> bool {
    list.split(',')
        .any(|s| s.trim().eq_ignore_ascii_case(scheme))
}

pub fn is_hdfs_scheme(url: &Url, object_store_configs: &HashMap<String, String>) -> bool {
    const COMET_LIBHDFS_SCHEMES_KEY: &str = "fs.comet.libhdfs.schemes";
    let scheme = url.scheme();
    match object_store_configs.get(COMET_LIBHDFS_SCHEMES_KEY) {
        Some(libhdfs_schemes) => scheme_in_list(libhdfs_schemes, scheme),
        None => scheme == "hdfs",
    }
}

/// Check if the scheme is an Azure ABFS URL.
fn is_azure_scheme(scheme: &str) -> bool {
    matches!(scheme, "abfs" | "abfss")
}

// Creates an OpenDAL HDFS Operator from a URL with optional configuration
#[cfg(feature = "hdfs-opendal")]
pub(crate) fn create_hdfs_operator(url: &Url) -> Result<opendal::Operator, object_store::Error> {
    let name_node = get_name_node_uri(url)?;
    let builder = opendal::services::Hdfs::default().name_node(&name_node);

    opendal::Operator::new(builder).map_err(|error| object_store::Error::Generic {
        store: "hdfs-opendal",
        source: error.into(),
    })
}

// Creates an HDFS object store from a URL using OpenDAL
#[cfg(feature = "hdfs-opendal")]
pub(crate) fn create_hdfs_object_store(
    url: &Url,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    let op = create_hdfs_operator(url)?;
    let store = object_store_opendal::OpendalStore::new(op);
    let path = Path::parse(url.path())?;
    Ok((Box::new(store), path))
}

#[cfg(feature = "hdfs-opendal")]
fn get_name_node_uri(url: &Url) -> Result<String, object_store::Error> {
    use std::fmt::Write;
    if let Some(host) = url.host() {
        let schema = url.scheme();
        let mut uri_builder = String::new();
        write!(&mut uri_builder, "{schema}://{host}").unwrap();

        if let Some(port) = url.port() {
            write!(&mut uri_builder, ":{port}").unwrap();
        }
        Ok(uri_builder)
    } else {
        Err(object_store::Error::InvalidPath {
            source: object_store::path::Error::InvalidPath {
                path: std::path::PathBuf::from(url.as_str()),
            },
        })
    }
}

// Stub implementation when HDFS support is not enabled
#[cfg(not(feature = "hdfs-opendal"))]
fn create_hdfs_object_store(
    _url: &Url,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    Err(object_store::Error::Generic {
        store: "HadoopFileSystem",
        source: "Hdfs support is not enabled in this build".into(),
    })
}

type ObjectStoreCache = RwLock<HashMap<(String, u64), Arc<dyn ObjectStore>>>;

/// Process-wide cache of object stores, keyed by `(scheme://host:port, config_hash)`.
///
/// ## Why static / process lifetime?
///
/// Comet's JNI architecture builds a fresh `SessionContext`/`RuntimeEnv` per native plan
/// (`Java_org_apache_comet_Native_createPlan`, once per Spark task).  There is therefore no
/// executor-scoped Rust object with a lifetime longer than a single task's plan that could
/// own this cache.  The executor process itself is the natural scope for HTTP
/// connection-pool reuse, so process lifetime
/// (i.e. `static`) is the appropriate choice here.  In the standard Spark-on-Kubernetes
/// deployment model each executor process is dedicated to a single Spark application, so
/// process lifetime and application lifetime are equivalent; the cache is reclaimed when
/// the executor pod terminates.
///
/// ## Unbounded size
///
/// Cache entries are indexed by `(scheme://host:port, hash-of-configs)`.  A typical Spark
/// job accesses a small, fixed set of buckets with a stable configuration, so the number of
/// distinct keys is O(buckets × credential-configs) and remains small throughout the job.
/// Entries are cheap relative to the cost of creating a new object store (new HTTP
/// connection pool + DNS resolution), and there is no meaningful benefit from eviction, so
/// no eviction policy is applied.
///
/// ## Credential invalidation
///
/// Object stores that use dynamic credentials (IMDS, WebIdentity, ECS role, STS assume-role)
/// delegate credential refresh to a `CometCredentialProvider` that fetches fresh credentials
/// on every request, so credential rotation is transparent and requires no cache
/// invalidation.  Object stores whose credentials are embedded in the Hadoop configuration
/// (e.g. `fs.s3a.access.key` / `fs.s3a.secret.key`) produce a different `config_hash` when
/// those values change, which causes a new store to be created and inserted under the new
/// key; the old entry is harmlessly superseded.
fn object_store_cache() -> &'static ObjectStoreCache {
    static CACHE: OnceLock<ObjectStoreCache> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Compute a hash of the object store configuration for cache keying.
fn hash_object_store_configs(configs: &HashMap<String, String>) -> u64 {
    let mut hasher = DefaultHasher::new();
    let mut keys: Vec<&String> = configs.keys().collect();
    keys.sort();
    for key in keys {
        key.hash(&mut hasher);
        configs[key].hash(&mut hasher);
    }
    hasher.finish()
}

/// Parses the url, registers the object store with configurations, and returns a tuple of the object store url
/// and object store path
pub(crate) fn prepare_object_store_with_configs(
    runtime_env: Arc<RuntimeEnv>,
    url: String,
    object_store_configs: &HashMap<String, String>,
) -> Result<(ObjectStoreUrl, Path), ExecutionError> {
    let url = normalize_object_store_url(url.as_str(), object_store_configs)?;
    let is_hdfs_scheme = is_hdfs_scheme(&url, object_store_configs);
    let scheme = url.scheme();
    let url_key = format!(
        "{}://{}",
        scheme,
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    );

    let config_hash = hash_object_store_configs(object_store_configs);
    let cache_key = (url_key.clone(), config_hash);

    // Check the cache first to reuse existing object store instances.
    // This enables HTTP connection pooling and avoids redundant DNS lookups.
    let cached = {
        let cache = object_store_cache()
            .read()
            .map_err(|e| ExecutionError::GeneralError(format!("Object store cache error: {e}")))?;
        cache.get(&cache_key).cloned()
    };

    let (object_store, object_store_path): (Arc<dyn ObjectStore>, Path) =
        if let Some(store) = cached {
            debug!("Reusing cached object store for {url_key}");
            let path = Path::from_url_path(url.path())
                .map_err(|e| ExecutionError::GeneralError(e.to_string()))?;
            (store, path)
        } else {
            debug!("Creating new object store for {url_key}");
            let (store, path): (Box<dyn ObjectStore>, Path) = if is_hdfs_scheme {
                create_hdfs_object_store(&url)
            } else if scheme == "s3" {
                objectstore::s3::create_store(&url, object_store_configs, Duration::from_secs(300))
            } else if is_azure_scheme(scheme) {
                objectstore::azure::create_store(&url, object_store_configs)
            } else {
                parse_url(&url)
            }
            .map_err(|e| ExecutionError::GeneralError(e.to_string()))?;

            let store: Arc<dyn ObjectStore> = Arc::from(store);
            // Insert into cache
            if let Ok(mut cache) = object_store_cache().write() {
                cache.insert(cache_key, Arc::clone(&store));
            }
            (store, path)
        };

    let object_store_url = ObjectStoreUrl::parse(url_key.clone())?;
    runtime_env.register_object_store(&url, object_store);
    Ok((object_store_url, object_store_path))
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "hdfs-opendal"))]
    use datafusion::execution::object_store::ObjectStoreUrl;
    #[cfg(not(feature = "hdfs-opendal"))]
    use datafusion::execution::runtime_env::RuntimeEnv;
    #[cfg(not(feature = "hdfs-opendal"))]
    use object_store::path::Path;
    #[cfg(not(feature = "hdfs-opendal"))]
    use std::sync::Arc;
    #[cfg(not(feature = "hdfs-opendal"))]
    use url::Url;

    #[cfg(not(feature = "hdfs-opendal"))]
    use crate::execution::operators::ExecutionError;
    #[cfg(not(feature = "hdfs-opendal"))]
    use std::collections::HashMap;

    /// Parses the url, registers the object store, and returns a tuple of the object store url and object store path
    #[cfg(not(feature = "hdfs-opendal"))]
    pub(crate) fn prepare_object_store(
        runtime_env: Arc<RuntimeEnv>,
        url: String,
    ) -> Result<(ObjectStoreUrl, Path), ExecutionError> {
        use crate::parquet::parquet_support::prepare_object_store_with_configs;
        prepare_object_store_with_configs(runtime_env, url, &HashMap::new())
    }

    #[cfg(not(feature = "hdfs-opendal"))]
    #[test]
    fn test_prepare_object_store() {
        use crate::execution::operators::ExecutionError;

        let local_file_system_url = "file:///comet/spark-warehouse/part-00000.snappy.parquet";
        let hdfs_url = "hdfs://localhost:8020/comet/spark-warehouse/part-00000.snappy.parquet";

        let all_urls = [local_file_system_url, hdfs_url];
        let expected: Vec<Result<(ObjectStoreUrl, Path), ExecutionError>> = vec![
            Ok((
                ObjectStoreUrl::parse("file://").unwrap(),
                Path::from("/comet/spark-warehouse/part-00000.snappy.parquet"),
            )),
            Err(ExecutionError::GeneralError(
                "Generic HadoopFileSystem error: Hdfs support is not enabled in this build"
                    .parse()
                    .unwrap(),
            )),
        ];

        for (i, url_str) in all_urls.iter().enumerate() {
            let url = &Url::parse(url_str).unwrap();
            let res = prepare_object_store(Arc::new(RuntimeEnv::default()), url.to_string());

            let expected = expected.get(i).unwrap();
            match expected {
                Ok((o, p)) => {
                    let (r_o, r_p) = res.unwrap();
                    assert_eq!(r_o, *o);
                    assert_eq!(r_p, *p);
                }
                Err(e) => {
                    assert!(res.is_err());
                    let Err(res_e) = res else {
                        panic!("test failed")
                    };
                    assert_eq!(e.to_string(), res_e.to_string())
                }
            }
        }
    }

    /// Convert one array through the public entry point, resolving its mapping.
    fn parquet_convert_array(
        array: arrow::array::ArrayRef,
        to_type: &arrow::datatypes::DataType,
        parquet_options: &crate::parquet::parquet_support::SparkParquetOptions,
    ) -> datafusion::common::Result<arrow::array::ArrayRef> {
        use crate::parquet::parquet_support::spark_parquet_convert;
        use datafusion::physical_plan::ColumnarValue;
        match spark_parquet_convert(ColumnarValue::Array(array), to_type, parquet_options)? {
            ColumnarValue::Array(array) => Ok(array),
            ColumnarValue::Scalar(_) => unreachable!("array input yields an array"),
        }
    }

    #[test]
    fn test_millis_to_micros_overflow_checked_only_at_top_level() {
        use crate::parquet::parquet_support::SparkParquetOptions;
        use arrow::array::{Array, ArrayRef, StructArray, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
        use datafusion_comet_spark_expr::EvalMode;
        use std::sync::Arc;

        let options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let overflow_millis = 9_223_372_036_854_776_i64;
        let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(overflow_millis),
            None,
        ]));
        let micros_type = DataType::Timestamp(TimeUnit::Microsecond, None);

        // Top-level: checked, matching Spark's `millisToMicros` (`Math.multiplyExact`).
        let err = parquet_convert_array(Arc::clone(&millis), &micros_type, &options)
            .expect_err("top-level overflow must error");
        assert!(
            err.to_string().to_lowercase().contains("overflow"),
            "unexpected error: {err}"
        );

        // Filtered scans disable checked conversion because Spark may prune values before
        // conversion through paths DataFusion cannot fully mirror.
        let mut unchecked_options = options.clone();
        unchecked_options.checked_timestamp_overflow = false;
        let converted =
            parquet_convert_array(Arc::clone(&millis), &micros_type, &unchecked_options)
                .expect("unchecked overflow must not error");
        assert!(converted.is_null(0), "overflow must become NULL");
        assert!(converted.is_null(1));

        // Nested: DataFusion's PruningPredicate cannot prune nested fields, so a
        // checked conversion would fail queries whose predicates Spark satisfies via
        // row-group statistics pruning. The nested field keeps the safe-cast behavior:
        // overflow becomes NULL.
        let child_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));
        let strukt: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Arc::clone(&child_field)]),
            vec![millis],
            None,
        ));
        let target = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "ts",
            micros_type.clone(),
            true,
        ))]));
        let converted = parquet_convert_array(strukt, &target, &options)
            .expect("nested overflow must not error");
        let converted_child = Arc::clone(
            converted
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0),
        );
        assert_eq!(converted_child.data_type(), &micros_type);
        assert!(converted_child.is_null(0), "overflow must become NULL");
        assert!(converted_child.is_null(1));
    }

    #[cfg(not(feature = "hdfs-opendal"))]
    #[test]
    #[cfg_attr(miri, ignore)] // AWS credential providers and object_store call foreign functions
    fn test_prepare_object_store_rewrites_blob_alias_to_s3() {
        // `fs.comet.s3Compliant.schemes` opts `blob` in, so `prepare_object_store_with_configs`
        // must rewrite the alias to `s3://`. Otherwise `ObjectStoreScheme::parse` rejects the URL
        // and the native scan fails at runtime (`Unsupported filesystem schemes: blob`). Two forms
        // must both land on `s3://bucket/key`: the canonical `blob://bucket/key`, and the empty-
        // authority `blob:///bucket/key` (host=None), whose first path segment is promoted into the
        // host because object_store 0.13 needs a `Some(host)` (a naive `s3:///bucket/key` fails).
        use crate::parquet::parquet_support::prepare_object_store_with_configs;
        let mut configs: HashMap<String, String> = HashMap::new();
        configs.insert(
            "fs.comet.s3Compliant.schemes".to_string(),
            "blob".to_string(),
        );
        configs.insert(
            "fs.s3a.aws.credentials.provider".to_string(),
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string(),
        );
        configs.insert(
            "fs.s3a.endpoint.region".to_string(),
            "us-east-1".to_string(),
        );

        for (input, expected_bucket, expected_path) in [
            (
                "blob://test_bucket/comet/spark-warehouse/part-00000.snappy.parquet",
                "s3://test_bucket",
                "/comet/spark-warehouse/part-00000.snappy.parquet",
            ),
            (
                "blob:///mybucket/warehouse/data/part-0.snappy.parquet",
                "s3://mybucket",
                "warehouse/data/part-0.snappy.parquet",
            ),
        ] {
            let (object_store_url, path) = prepare_object_store_with_configs(
                Arc::new(RuntimeEnv::default()),
                input.to_string(),
                &configs,
            )
            .unwrap_or_else(|e| panic!("{input} should normalize to s3://: {e}"));
            assert_eq!(
                object_store_url,
                ObjectStoreUrl::parse(expected_bucket).unwrap()
            );
            assert_eq!(path, Path::from(expected_path));
        }
    }

    mod struct_field_matching {
        use super::parquet_convert_array;
        use crate::parquet::parquet_support::{
            resolve_field_mapping, FieldMapping, FieldMatch, SparkParquetOptions,
        };
        use arrow::array::{Array, ArrayRef, Int32Array, LargeListArray, StructArray};
        use arrow::datatypes::{DataType, Field, Fields};
        use datafusion_comet_spark_expr::EvalMode;

        /// The per-id lookup entry is a plain `Copy` value: the second field sharing an id
        /// only flips the ambiguity flag, so resolving a wide struct allocates no vector
        /// per id.
        #[test]
        fn field_match_records_ambiguity_without_allocating() {
            fn assert_copy<T: Copy>() {}
            assert_copy::<FieldMatch>();

            let first = FieldMatch::first(3);
            assert_eq!(first, FieldMatch::new(3, false));
            let again = first.also(5);
            assert_eq!(again, FieldMatch::new(5, true));
            assert!(again.ambiguous);
        }

        /// Every requested id resolves to exactly one file field: the resolved mapping is
        /// positional and carries one source per requested field.
        #[test]
        fn resolve_mapping_by_id_is_positional_for_unique_ids() {
            let fields: Vec<Field> = (0..256)
                .map(|i| field_with_id(&format!("c{i}"), i))
                .collect();
            let from_type = DataType::Struct(Fields::from(fields.clone()));
            let to_type = DataType::Struct(Fields::from(fields));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.use_field_id = true;

            let mapping = resolve_field_mapping(&from_type, &to_type, &opts).unwrap();
            assert!(mapping.is_positional());
            let FieldMapping::Struct(sources) = &mapping else {
                panic!("expected a struct mapping");
            };
            assert_eq!(sources.len(), 256);
            assert!(sources
                .iter()
                .enumerate()
                .all(|(i, s)| s.from_index == Some(i)));
        }

        /// Requested ids in a different order than the file resolve by id, so the mapping
        /// is not positional and a metadata-only relabel would read the wrong columns.
        #[test]
        fn resolve_mapping_by_id_reorders_swapped_ids() {
            let from_type = DataType::Struct(Fields::from(vec![
                field_with_id("x", 1),
                field_with_id("y", 2),
            ]));
            let to_type = DataType::Struct(Fields::from(vec![
                field_with_id("x", 2),
                field_with_id("y", 1),
            ]));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.use_field_id = true;

            let mapping = resolve_field_mapping(&from_type, &to_type, &opts).unwrap();
            assert!(!mapping.is_positional());
            let FieldMapping::Struct(sources) = &mapping else {
                panic!("expected a struct mapping");
            };
            assert_eq!(sources[0].from_index, Some(1));
            assert_eq!(sources[1].from_index, Some(0));
        }

        /// A large list element resolves like a list element: swapped ids inside it make the
        /// mapping non-positional and the conversion reads each field by id.
        #[test]
        fn resolve_mapping_recurses_into_large_list_element() {
            let from_elem = Fields::from(vec![field_with_id("x", 1), field_with_id("y", 2)]);
            let to_elem = Fields::from(vec![field_with_id("x", 2), field_with_id("y", 1)]);
            let from_field = Arc::new(Field::new("item", DataType::Struct(from_elem), true));
            let to_field = Arc::new(Field::new("item", DataType::Struct(to_elem), true));
            let from_type = DataType::LargeList(Arc::clone(&from_field));
            let to_type = DataType::LargeList(to_field);

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.use_field_id = true;

            let mapping = resolve_field_mapping(&from_type, &to_type, &opts).unwrap();
            assert!(!mapping.is_positional());

            let element = struct_of(
                vec![field_with_id("x", 1), field_with_id("y", 2)],
                vec![42, 43],
            );
            let list = LargeListArray::new(
                from_field,
                arrow::buffer::OffsetBuffer::new(vec![0i64, 1].into()),
                element,
                None,
            );
            let result = parquet_convert_array(Arc::new(list), &to_type, &opts).unwrap();
            assert_eq!(result.data_type(), &to_type);
            let values = result
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone();
            let x = values
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let y = values
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(x.value(0), 43);
            assert_eq!(y.value(0), 42);
        }

        /// A duplicated requested id nested under a list element is rejected at resolution
        /// time, mirroring Spark's `clipParquetListType` recursing into `matchIdField`.
        #[test]
        fn resolve_mapping_rejects_duplicate_id_inside_list_element() {
            let from_elem = DataType::Struct(Fields::from(vec![
                field_with_id("x", 1),
                field_with_id("y", 1),
            ]));
            let to_elem = DataType::Struct(Fields::from(vec![field_with_id("x", 1)]));
            let from_type = DataType::List(Arc::new(Field::new("item", from_elem, true)));
            let to_type = DataType::List(Arc::new(Field::new("element", to_elem, true)));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.use_field_id = true;

            let err = resolve_field_mapping(&from_type, &to_type, &opts).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("_LEGACY_ERROR_TEMP_2094") && msg.contains("[x, y]"),
                "unexpected error: {msg}"
            );
        }
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::collections::HashMap;
        use std::sync::Arc;

        fn field_with_id(name: &str, id: i32) -> Field {
            Field::new(name, DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        }

        fn struct_of(fields: Vec<Field>, values: Vec<i32>) -> ArrayRef {
            let arrays: Vec<ArrayRef> = values
                .into_iter()
                .map(|v| Arc::new(Int32Array::from(vec![Some(v)])) as ArrayRef)
                .collect();
            Arc::new(StructArray::new(Fields::from(fields), arrays, None))
        }

        /// Two physical struct fields share field ID 1 and the logical struct requests that
        /// ID: Spark's `matchIdField` raises `foundDuplicateFieldInFieldIdLookupModeError`
        /// (`_LEGACY_ERROR_TEMP_2094`) rather than silently reading the first match.
        #[test]
        fn requested_duplicate_field_id_errors() {
            let from = struct_of(
                vec![field_with_id("x", 1), field_with_id("y", 1)],
                vec![42, 43],
            );
            let to_type = DataType::Struct(Fields::from(vec![field_with_id("f", 1)]));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.use_field_id = true;

            let err = parquet_convert_array(from, &to_type, &opts).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("_LEGACY_ERROR_TEMP_2094") && msg.contains("id=1"),
                "unexpected error: {msg}"
            );
        }

        /// Companion to `requested_duplicate_field_id_errors`: a duplicated file ID that no
        /// requested field looks up must stay harmless (Spark only raises inside
        /// `matchIdField`, i.e. for requested IDs).
        #[test]
        fn unrequested_duplicate_field_id_reads_fine() {
            let from = struct_of(
                vec![
                    field_with_id("x", 1),
                    field_with_id("y", 1),
                    field_with_id("z", 2),
                ],
                vec![42, 43, 44],
            );
            let to_type = DataType::Struct(Fields::from(vec![field_with_id("f", 2)]));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.use_field_id = true;

            let result = parquet_convert_array(from, &to_type, &opts).unwrap();
            let result_struct = result.as_any().downcast_ref::<StructArray>().unwrap();
            let col = result_struct
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), 44);
        }

        /// Two physical struct fields carry the IDENTICAL name in case-sensitive mode.
        /// Spark's `caseSensitiveParquetFieldMap` is built with `.toMap`, where the later
        /// entry wins silently; the exact-name lookup here must do the same rather than
        /// return the first field.
        #[test]
        fn duplicate_exact_names_resolve_to_the_last_field() {
            let from = struct_of(
                vec![
                    Field::new("d", DataType::Int32, true),
                    Field::new("d", DataType::Int32, true),
                ],
                vec![1, 2],
            );
            let to_type =
                DataType::Struct(Fields::from(vec![Field::new("d", DataType::Int32, true)]));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.case_sensitive = true;

            let result = parquet_convert_array(from, &to_type, &opts).unwrap();
            let result_struct = result.as_any().downcast_ref::<StructArray>().unwrap();
            let col = result_struct
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), 2);
        }
    }
}
