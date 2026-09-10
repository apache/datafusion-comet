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
use arrow::array::{
    make_array, FixedSizeBinaryArray, GenericListViewArray, MapArray, OffsetSizeTrait, StringArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::can_cast_types;
use arrow::datatypes::{Field, FieldRef, Fields, Schema};
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
use object_store::{parse_url, ObjectStore, ObjectStoreScheme};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use std::{collections::hash_map::DefaultHasher, hash::Hasher, sync::RwLock};
use std::{fmt::Debug, hash::Hash, sync::Arc};
use url::Url;

use super::objectstore;
use super::objectstore::s3_blob_fs_support::{
    normalize_object_store_url, NormalizedObjectStoreUrl,
};

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
    /// When true (the default), a TIMESTAMP_MILLIS field that overflows during
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
            None,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
            // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
            // here.
            let array = scalar.to_array()?;
            let scalar = ScalarValue::try_from_array(
                &convert_array(array, data_type, mapping, parquet_options, None)?,
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
    /// Mapping of a list's element type. A `Leaf` list converts its elements by type alone,
    /// as the adapter hands one to every column whose type holds no struct.
    pub(crate) fn list_element(&self) -> DataFusionResult<&FieldMapping> {
        match self {
            FieldMapping::List(inner) => Ok(inner),
            FieldMapping::Leaf => Ok(&FieldMapping::Leaf),
            other => Err(DataFusionError::Internal(format!(
                "list column resolved to a non-list field mapping: {other:?}"
            ))),
        }
    }

    /// Mappings of a map's key and value types; see [`FieldMapping::list_element`].
    pub(crate) fn map_entries(&self) -> DataFusionResult<(&FieldMapping, &FieldMapping)> {
        match self {
            FieldMapping::Map(key, value) => Ok((key, value)),
            FieldMapping::Leaf => Ok((&FieldMapping::Leaf, &FieldMapping::Leaf)),
            other => Err(DataFusionError::Internal(format!(
                "map column resolved to a non-map field mapping: {other:?}"
            ))),
        }
    }

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

/// True when a field of `schema`, at any nesting depth, carries a Parquet field id.
pub(crate) fn schema_holds_field_ids(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_holds_id(f))
}

fn field_holds_id(field: &Field) -> bool {
    field_id(field).is_some()
        || match field.data_type() {
            DataType::Struct(fields) => fields.iter().any(|f| field_holds_id(f)),
            DataType::List(f) | DataType::LargeList(f) | DataType::Map(f, _) => field_holds_id(f),
            _ => false,
        }
}

/// Resolve every requested root field against `file_schema` the way the expression adapter
/// does, keeping only the ambiguity Spark reports. DataFusion's opener creates the adapter
/// only when a predicate is pushed or the file schema differs from the requested one, so the
/// reader factory runs this on every footer it loads to cover the files the adapter never sees.
pub(crate) fn validate_field_mapping(
    file_schema: &Schema,
    requested_schema: &Schema,
    parquet_options: &SparkParquetOptions,
) -> Result<(), SparkError> {
    // `ParquetMissingFieldIds` needs no counterpart here: a file with no ids differs from an
    // id-bearing requested schema in field metadata, so the opener runs the adapter for it.
    resolve_field_mapping(
        &DataType::Struct(file_schema.fields().clone()),
        &DataType::Struct(requested_schema.fields().clone()),
        parquet_options,
    )
    .map(|_| ())
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
    // Dictionary encoding is a physical detail: resolve against the value type it wraps.
    // Parquet dictionary encoding only wraps a leaf type, so this mirrors the adapter's
    // conversion check and never changes which mapping is built.
    if let Dictionary(_, value_type) = from_type {
        return resolve_field_mapping(value_type, to_type, parquet_options);
    }
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

/// Convert `array` to `to_type` through its resolved `mapping`. `parent_nulls` masks the rows
/// hidden beneath null ancestors, so only values Spark reads are checked for overflow.
fn convert_array(
    array: ArrayRef,
    to_type: &DataType,
    mapping: &FieldMapping,
    parquet_options: &SparkParquetOptions,
    parent_nulls: Option<&NullBuffer>,
) -> DataFusionResult<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type();
    // Only checked millis-to-micros casts consume ancestor visibility. In particular,
    // unchanged array/map siblings must not expand a mask over their backing values.
    let checked_timestamp_overflow = parquet_options.checked_timestamp_overflow
        && has_timestamp_unit(from_type, TimeUnit::Millisecond)
        && has_timestamp_unit(to_type, TimeUnit::Microsecond);
    let visible = if checked_timestamp_overflow {
        NullBuffer::union(array.nulls(), parent_nulls)
    } else {
        None
    };

    // Try Comet specific handlers first, then arrow-rs cast if supported, and fail otherwise.
    match (from_type, to_type, mapping) {
        (Struct(_), Struct(to_fields), FieldMapping::Struct(sources)) => convert_struct(
            array.as_struct(),
            to_fields,
            sources,
            parquet_options,
            visible.as_ref(),
        ),
        // A struct always resolves to a struct mapping; anything else is a planning bug and
        // must not fall through to a silent pass-through of the file's struct.
        (Struct(_), Struct(_), other) => Err(DataFusionError::Internal(format!(
            "struct column resolved to a non-struct field mapping: {other:?}"
        ))),
        (
            List(_) | LargeList(_) | FixedSizeList(_, _) | ListView(_) | LargeListView(_),
            List(to_inner_type) | LargeList(to_inner_type) | FixedSizeList(to_inner_type, _)
                | ListView(to_inner_type) | LargeListView(to_inner_type),
            _,
        ) => {
            let inner = mapping.list_element()?;
            let data = array.to_data();
            let child_visibility = if checked_timestamp_overflow {
                list_child_visibility(array.as_ref(), visible.as_ref())
            } else {
                None
            };
            let cast_field = convert_array(
                make_array(data.child_data()[0].clone()),
                to_inner_type.data_type(),
                inner,
                parquet_options,
                child_visibility.as_ref(),
            )?;
            // Resolve element fields with Spark's rules before Arrow changes list layout.
            // Casting the original list directly can match missing struct fields by position.
            let resolved_type = match from_type {
                List(_) => List(Arc::clone(to_inner_type)),
                LargeList(_) => LargeList(Arc::clone(to_inner_type)),
                FixedSizeList(_, size) => FixedSizeList(Arc::clone(to_inner_type), *size),
                ListView(_) => ListView(Arc::clone(to_inner_type)),
                LargeListView(_) => LargeListView(Arc::clone(to_inner_type)),
                _ => unreachable!(),
            };
            // Retain the source offsets, sizes and null buffer while replacing its values.
            let resolved = make_array(data.into_builder()
                .data_type(resolved_type)
                .child_data(vec![cast_field.to_data()])
                .build()?);
            if resolved.data_type() == to_type {
                Ok(resolved)
            } else {
                Ok(cast_with_options(&resolved, to_type, &PARQUET_OPTIONS)?)
            }
        }
        (Timestamp(TimeUnit::Millisecond, _), Timestamp(TimeUnit::Microsecond, target_tz), _)
            if checked_timestamp_overflow =>
        {
            // Spark's Parquet reader calls the checked `millisToMicros` conversion for both
            // direct and dictionary values, independent of CAST evaluation mode:
            // https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetVectorUpdaterFactory.java#L817-L833
            // `millisToMicros` uses `Math.multiplyExact`:
            // https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala#L103-L108
            //
            // Filtered scans retain safe conversion until DataFusion can mirror Spark's
            // pruning paths, including nested predicates.
            let millis = array.as_primitive::<TimestampMillisecondType>();
            // Ignore values hidden by null ancestors or by sliced list/map offsets.
            // Restore the original child validity: required fields must remain non-null.
            let micros =
                arrow::array::TimestampMillisecondArray::new(millis.values().clone(), visible)
                    .try_unary::<_, TimestampMicrosecondType, _>(|value| value.mul_checked(1_000))?;
            let micros = arrow::array::TimestampMicrosecondArray::new(
                micros.values().clone(),
                millis.nulls().cloned(),
            )
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
        (Map(_, ordered_from), Map(_, ordered_to), _) if ordered_from == ordered_to => {
            let (key, value) = mapping.map_entries()?;
            parquet_convert_map_to_map(
                array.as_map(),
                to_type,
                key,
                value,
                parquet_options,
                visible.as_ref(),
                checked_timestamp_overflow,
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
        // Every pair reaching here should already have passed the schema adapter's
        // `check_conversion` (Spark's `getUpdater` matrix), so this is a gap in that gate. Fail
        // instead of handing back an array of the wrong type, which a parent `StructArray` /
        // `ListArray` constructor would otherwise panic on (#5671).
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported Parquet type conversion from {from_type} to {to_type}"
        ))),
    }
}

// Struct fields are matched by name/field ID later. This type-only check is conservative
// until that matching occurs; each selected child is checked again before conversion.
fn has_timestamp_unit(data_type: &DataType, unit: TimeUnit) -> bool {
    match data_type {
        DataType::Timestamp(timestamp_unit, _) => *timestamp_unit == unit,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_timestamp_unit(field.data_type(), unit)),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => has_timestamp_unit(field.data_type(), unit),
        _ => false,
    }
}

// List/map values can include entries outside a slice or beneath a null parent.
fn repeated_visibility<O: OffsetSizeTrait>(
    offsets: &[O],
    len: usize,
    nulls: Option<&NullBuffer>,
) -> Option<NullBuffer> {
    if nulls.is_none() && offsets[0].as_usize() == 0 && offsets.last().unwrap().as_usize() == len {
        return None;
    }
    let mut valid = vec![false; len];
    for (row, range) in offsets.windows(2).enumerate() {
        if nulls.is_none_or(|nulls| nulls.is_valid(row)) {
            valid[range[0].as_usize()..range[1].as_usize()].fill(true);
        }
    }
    Some(NullBuffer::from(valid))
}

fn list_child_visibility(array: &dyn Array, nulls: Option<&NullBuffer>) -> Option<NullBuffer> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            repeated_visibility(list.value_offsets(), list.values().len(), nulls)
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            repeated_visibility(list.value_offsets(), list.values().len(), nulls)
        }
        DataType::FixedSizeList(_, size) => {
            // Arrow slices the child values along with a fixed-size list. Expand in those
            // child coordinates; applying a parent slice offset again would shift the mask.
            nulls.map(|nulls| nulls.expand(*size as usize))
        }
        DataType::ListView(_) => list_view_visibility(array.as_list_view::<i32>(), nulls),
        DataType::LargeListView(_) => list_view_visibility(array.as_list_view::<i64>(), nulls),
        _ => unreachable!("only called for list arrays"),
    }
}

fn list_view_visibility<O: OffsetSizeTrait>(
    list: &GenericListViewArray<O>,
    nulls: Option<&NullBuffer>,
) -> Option<NullBuffer> {
    let mut valid = vec![false; list.values().len()];
    for (row, (offset, size)) in list
        .value_offsets()
        .iter()
        .zip(list.value_sizes())
        .enumerate()
    {
        if nulls.is_none_or(|nulls| nulls.is_valid(row)) {
            let start = offset.as_usize();
            // Views can overlap or be out of order. Accumulate the union of visible ranges;
            // a null parent must never clear values another parent can see.
            valid[start..start + size.as_usize()].fill(true);
        }
    }
    Some(NullBuffer::from(valid))
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
    parent_nulls: Option<&NullBuffer>,
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
                // The mapping is resolved once per file against the physical schema; a batch
                // whose struct carries fewer children than that schema must error, not panic.
                let child = array.columns().get(from_index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "struct field {} maps to file child {from_index}, but the struct has {} \
                         child(ren)",
                        to_field.name(),
                        array.num_columns()
                    ))
                })?;
                cast_fields.push(convert_array(
                    Arc::clone(child),
                    to_field.data_type(),
                    &source.nested,
                    parquet_options,
                    parent_nulls,
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

    Ok(Arc::new(StructArray::try_new(
        to_fields.clone(),
        cast_fields,
        nulls,
    )?))
}

/// Cast a map type to another map type. The same as arrow-cast except we recursively call our own
/// convert_array with the resolved key and value mappings.
fn parquet_convert_map_to_map(
    from: &MapArray,
    to_data_type: &DataType,
    key_mapping: &FieldMapping,
    value_mapping: &FieldMapping,
    parquet_options: &SparkParquetOptions,
    parent_nulls: Option<&NullBuffer>,
    checked_timestamp_overflow: bool,
) -> Result<ArrayRef, DataFusionError> {
    match to_data_type {
        DataType::Map(entries_field, to_ordered) => {
            let key_field = key_field(entries_field).ok_or(DataFusionError::Internal(
                "map is missing key field".to_string(),
            ))?;
            let value_field = value_field(entries_field).ok_or(DataFusionError::Internal(
                "map is missing value field".to_string(),
            ))?;

            let child_visibility = if checked_timestamp_overflow {
                repeated_visibility(from.value_offsets(), from.keys().len(), parent_nulls)
            } else {
                None
            };
            let key_array = convert_array(
                Arc::clone(from.keys()),
                key_field.data_type(),
                key_mapping,
                parquet_options,
                child_visibility.as_ref(),
            )?;
            let value_array = convert_array(
                Arc::clone(from.values()),
                value_field.data_type(),
                value_mapping,
                parquet_options,
                child_visibility.as_ref(),
            )?;

            Ok(Arc::new(MapArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(entries_field),
                from.offsets().clone(),
                StructArray::try_new(
                    Fields::from(vec![key_field, value_field]),
                    vec![key_array, value_array],
                    from.entries().nulls().cloned(),
                )?,
                from.nulls().cloned(),
                *to_ordered,
            )?))
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

/// Cache identity: `(scheme://host:port, config_hash, hdfs_backend)`.
/// Native `s3a` is normalized to `s3`; Hadoop-selected schemes keep their spelling.
/// The hash covers the object-store configuration. The boolean is `true` for the
/// Hadoop backend (including custom schemes routed through Hadoop), `false` for native.
type ObjectStoreCacheKey = (String, u64, bool);
type ObjectStoreCache = RwLock<HashMap<ObjectStoreCacheKey, Arc<dyn ObjectStore>>>;

/// Process-wide cache keyed by `(physical_scheme://host:port, config_hash, hdfs_backend)`.
/// Backend identity is separate from the normalized URL: a configuration can route `s3`
/// through Hadoop while native `s3a` is normalized to the same `s3` scheme.
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
/// Cache entries include the physical URL, configuration hash and backend. A typical Spark
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

/// The selected backend, independent of the URL used to register it in DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectStoreBackend {
    Local,
    Remote,
    Other,
}

/// Classifies a normalized URL using the same parser as backend construction. `is_hdfs` records
/// the configured libhdfs routing decision, which takes precedence over cloud-like schemes.
/// This does not create a store or perform I/O; unsupported native URLs return a parsing error.
fn object_store_backend(url: &Url, is_hdfs: bool) -> Result<ObjectStoreBackend, ExecutionError> {
    if is_hdfs {
        // Custom libhdfs schemes may look like cloud URLs but retain their own range-read API.
        return Ok(ObjectStoreBackend::Other);
    }
    let (scheme, _) =
        ObjectStoreScheme::parse(url).map_err(|e| ExecutionError::GeneralError(e.to_string()))?;
    Ok(match scheme {
        ObjectStoreScheme::Local => ObjectStoreBackend::Local,
        ObjectStoreScheme::AmazonS3
        | ObjectStoreScheme::GoogleCloudStorage
        | ObjectStoreScheme::MicrosoftAzure
        | ObjectStoreScheme::Http => ObjectStoreBackend::Remote,
        // Memory and future backends are not implicitly classified as remote network traffic.
        _ => ObjectStoreBackend::Other,
    })
}

/// Normalizes the owned URL using the borrowed configuration, selects and registers the backend
/// in `runtime_env`, and returns its registry URL, object path, and I/O classification. Stores are
/// reused from the process-wide cache when their normalized physical URL, configuration, and
/// selected backend match. URL, configuration, and store-construction failures propagate to the
/// caller. Callers must use the returned backend classification rather than infer it from an
/// original alias or the synthetic registration scheme.
pub(crate) fn prepare_object_store_with_configs(
    runtime_env: Arc<RuntimeEnv>,
    url: String,
    object_store_configs: &HashMap<String, String>,
) -> Result<(ObjectStoreUrl, Path, ObjectStoreBackend), ExecutionError> {
    // `is_hdfs` comes back from normalization because it must be decided on the URL as written.
    // Re-deriving it from the normalized URL would let an `s3a`/alias rewrite land on an `s3`
    // entry in `fs.comet.libhdfs.schemes` and route an S3 read through libhdfs.
    let NormalizedObjectStoreUrl {
        url,
        is_hdfs: is_hdfs_scheme,
    } = normalize_object_store_url(url.as_str(), object_store_configs)?;
    // Configured S3 aliases must be normalized before the object-store parser classifies them.
    // HDFS routing still wins, including when its configured schemes resemble remote stores.
    let backend = object_store_backend(&url, is_hdfs_scheme)?;
    let scheme = url.scheme();
    let url_key = format!(
        "{}://{}",
        scheme,
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    );

    let config_hash = hash_object_store_configs(object_store_configs);
    let cache_key = (url_key.clone(), config_hash, is_hdfs_scheme);

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

    // A RuntimeEnv can plan multiple scans with different backends or credentials
    // for the same bucket. Use the same identity as the cache, even for the first
    // registration, so neither later registration nor planning order changes the
    // store used by an existing scan. Native s3/s3a share the normalized s3 scheme;
    // a Hadoop-selected scheme retains its physical spelling.
    //
    // Native LocalFileSystem ignores these Hadoop options and keeps file:// for
    // compatibility. An explicitly Hadoop-routed file scheme is still isolated.
    let object_store_url = if scheme == "file" && !is_hdfs_scheme {
        ObjectStoreUrl::parse(url_key)?
    } else {
        let backend = if is_hdfs_scheme { "hdfs" } else { "native" };
        // DataFusion keys stores only by scheme and authority, so put configuration
        // and backend identity in the scheme while preserving the physical authority.
        // `+comet-` marks our internal registration suffix; encryption lookup strips
        // the complete suffix to recover the physical URI.
        ObjectStoreUrl::parse(format!(
            "{scheme}+comet-{config_hash:016x}-{backend}://{}",
            &url[url::Position::BeforeHost..url::Position::AfterPort],
        ))?
    };
    runtime_env.register_object_store(object_store_url.as_ref(), object_store);
    Ok((object_store_url, object_store_path, backend))
}

#[cfg(test)]
mod tests {
    /// Checks parser-backed I/O labels without constructing stores, including libhdfs overrides
    /// and rejection of unknown native schemes. Configured S3 aliases follow URL normalization.
    #[test]
    fn classifies_the_selected_backend_using_object_store_parser() {
        use super::{
            is_hdfs_scheme, normalize_object_store_url, object_store_backend, ObjectStoreBackend,
        };
        let configs = std::collections::HashMap::from([(
            "fs.comet.libhdfs.schemes".to_string(),
            "s3,abfs".to_string(),
        )]);
        for address in [
            "s3://bucket/path",
            "s3a://bucket/path",
            "gs://bucket/path",
            "az://container/path",
            "adl://container/path",
            "azure://container/path",
            "abfs://container/path",
            "abfss://container/path",
            "http://example.com/path",
            "https://example.com/path",
            "https://account.blob.core.windows.net/container/path",
        ] {
            let url = url::Url::parse(address).unwrap();
            assert_eq!(
                object_store_backend(&url, false).unwrap(),
                ObjectStoreBackend::Remote,
                "{address}"
            );
            if is_hdfs_scheme(&url, &configs) {
                assert_eq!(
                    object_store_backend(&url, true).unwrap(),
                    ObjectStoreBackend::Other
                );
            }
        }
        assert_eq!(
            object_store_backend(&url::Url::parse("file:///tmp/a").unwrap(), false).unwrap(),
            ObjectStoreBackend::Local
        );
        assert_eq!(
            object_store_backend(&url::Url::parse("memory:///a").unwrap(), false).unwrap(),
            ObjectStoreBackend::Other
        );
        // These spellings are not accepted native backends in pinned object_store 0.13.2.
        for scheme in ["gcs", "wasb", "wasbs", "s3n"] {
            let url = url::Url::parse(&format!("{scheme}://bucket/path")).unwrap();
            assert!(object_store_backend(&url, false).is_err());
            assert_eq!(
                object_store_backend(&url, true).unwrap(),
                ObjectStoreBackend::Other
            );
        }

        // An alias selected for libhdfs must keep its original scheme and I/O classification,
        // even when the same alias is also configured as S3-compatible.
        let configs = std::collections::HashMap::from([
            (
                "fs.comet.s3Compliant.schemes".to_string(),
                "blob".to_string(),
            ),
            ("fs.comet.libhdfs.schemes".to_string(), "blob".to_string()),
        ]);
        let normalized = normalize_object_store_url("blob://bucket/path", &configs).unwrap();
        assert_eq!(normalized.url.scheme(), "blob");
        assert_eq!(
            object_store_backend(&normalized.url, normalized.is_hdfs).unwrap(),
            ObjectStoreBackend::Other
        );
    }

    /// Normalizes original URL spellings and classifies their carried routing decisions without
    /// creating stores or performing I/O. Aliases remain remote unless explicitly routed to
    /// libhdfs, even when normalization rewrites their scheme to a configured libhdfs scheme.
    #[test]
    fn classifies_s3_aliases_using_original_libhdfs_routing() {
        use super::{normalize_object_store_url, object_store_backend, ObjectStoreBackend};

        for (input, libhdfs_schemes, expected) in [
            ("s3a://bucket/path", "s3", ObjectStoreBackend::Remote),
            ("blob://bucket/path", "s3", ObjectStoreBackend::Remote),
            ("s3://bucket/path", "s3", ObjectStoreBackend::Other),
            ("s3a://bucket/path", "s3a", ObjectStoreBackend::Other),
            ("blob://bucket/path", "blob", ObjectStoreBackend::Other),
        ] {
            let configs = std::collections::HashMap::from([
                (
                    "fs.comet.s3Compliant.schemes".to_string(),
                    "blob".to_string(),
                ),
                (
                    "fs.comet.libhdfs.schemes".to_string(),
                    libhdfs_schemes.to_string(),
                ),
            ]);
            let normalized = normalize_object_store_url(input, &configs).unwrap();
            assert_eq!(
                object_store_backend(&normalized.url, normalized.is_hdfs).unwrap(),
                expected,
                "{input} with libhdfs.schemes={libhdfs_schemes}"
            );
        }
    }

    use super::{
        hash_object_store_configs, object_store_cache, prepare_object_store_with_configs,
        ObjectStoreBackend,
    };
    use bytes::Bytes;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::collections::HashMap;
    use std::sync::Arc;
    #[cfg(not(feature = "hdfs-opendal"))]
    use url::Url;

    #[cfg(not(feature = "hdfs-opendal"))]
    use crate::execution::operators::ExecutionError;

    struct StoreConfig {
        input_scheme: &'static str,
        physical_scheme: &'static str,
        hdfs_backend: bool,
        options: HashMap<String, String>,
    }

    /// Seeds two distinct in-memory stores under the expected cache identities for `cases`,
    /// using a unique `bucket` to isolate concurrent tests. Checks registration, returned I/O
    /// classification, and actual reads in both planning orders without cloud/Hadoop access.
    /// Removes the seeded entries on success; assertion failures panic with test evidence.
    async fn check_isolated_stores(bucket: &str, cases: [StoreConfig; 2]) {
        let path = Path::from("directory/part one.parquet");
        let stores: [Arc<dyn ObjectStore>; 2] =
            [Arc::new(InMemory::new()), Arc::new(InMemory::new())];
        let keys = cases.each_ref().map(|case| {
            (
                format!("{}://{bucket}", case.physical_scheme),
                hash_object_store_configs(&case.options),
                case.hdfs_backend,
            )
        });
        for (index, store) in stores.iter().enumerate() {
            store
                .put(&path, Bytes::from(format!("store-{index}")).into())
                .await
                .unwrap();
        }
        {
            let mut cache = object_store_cache().write().unwrap();
            for (key, store) in keys.iter().zip(&stores) {
                cache.insert(key.clone(), Arc::clone(store));
            }
        }

        let mut previous_urls = None;
        for order in [[0, 1], [1, 0]] {
            let runtime = Arc::new(RuntimeEnv::default());
            let mut prepared = [None, None];
            for index in order {
                let case = &cases[index];
                prepared[index] = Some(
                    prepare_object_store_with_configs(
                        Arc::clone(&runtime),
                        format!(
                            "{}://{bucket}/directory/part%20one.parquet",
                            case.input_scheme
                        ),
                        &case.options,
                    )
                    .unwrap(),
                );
            }
            let prepared = prepared.map(Option::unwrap);
            let urls = prepared.each_ref().map(|(url, _, _)| url.clone());
            assert_ne!(urls[0], urls[1]);
            if let Some(previous) = &previous_urls {
                assert_eq!(
                    &urls, previous,
                    "registration must not depend on planning order"
                );
            }
            previous_urls = Some(urls);
            for (index, (url, actual_path, backend)) in prepared.iter().enumerate() {
                assert_eq!(actual_path, &path);
                // The label must follow the selected backend even when both original URLs
                // normalize to s3 with identical configuration and reuse cached stores.
                assert_eq!(
                    *backend,
                    if cases[index].hdfs_backend {
                        ObjectStoreBackend::Other
                    } else {
                        ObjectStoreBackend::Remote
                    },
                );
                let store = runtime.object_store(url).unwrap();
                assert!(Arc::ptr_eq(&store, &stores[index]));
                assert_eq!(
                    store.get(actual_path).await.unwrap().bytes().await.unwrap(),
                    Bytes::from(format!("store-{index}")),
                );
                assert!(url.as_str().starts_with(&format!(
                    "{}+comet-{:016x}-{}://",
                    cases[index].physical_scheme,
                    keys[index].1,
                    if cases[index].hdfs_backend {
                        "hdfs"
                    } else {
                        "native"
                    },
                )));
            }
        }
        let mut cache = object_store_cache().write().unwrap();
        for key in keys {
            cache.remove(&key);
        }
    }

    /// Verifies that `s3a` stays Remote while explicitly Hadoop-routed `s3` stays Other
    /// under the same options, with distinct cached contents in either planning order.
    #[tokio::test]
    async fn isolates_backends_even_when_s3_alias_and_configs_match() {
        let options = HashMap::from([("fs.comet.libhdfs.schemes".into(), "s3".into())]);
        check_isolated_stores(
            "comet-isolation-backend-alias",
            [
                StoreConfig {
                    input_scheme: "s3a",
                    physical_scheme: "s3",
                    hdfs_backend: false,
                    options: options.clone(),
                },
                StoreConfig {
                    input_scheme: "s3",
                    physical_scheme: "s3",
                    hdfs_backend: true,
                    options,
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn isolates_native_stores_with_different_configurations() {
        check_isolated_stores(
            "comet-isolation-configurations",
            ["first", "second"].map(|endpoint| StoreConfig {
                input_scheme: "s3a",
                physical_scheme: "s3",
                hdfs_backend: false,
                options: HashMap::from([("fs.s3a.endpoint".into(), endpoint.into())]),
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn preserves_custom_hadoop_scheme_when_routing_changes() {
        check_isolated_stores(
            "comet-isolation-custom-hadoop",
            [
                StoreConfig {
                    input_scheme: "s3a",
                    physical_scheme: "s3",
                    hdfs_backend: false,
                    options: HashMap::new(),
                },
                StoreConfig {
                    input_scheme: "s3a",
                    physical_scheme: "s3a",
                    hdfs_backend: true,
                    options: HashMap::from([("fs.comet.libhdfs.schemes".into(), "s3a".into())]),
                },
            ],
        )
        .await;
    }

    /// Checks native alias cache reuse in both planning orders, including the returned Remote
    /// label. Seeds and removes one in-memory cache entry; no remote requests are performed.
    #[test]
    fn native_s3_aliases_share_cache_and_registration_identity() {
        let options = HashMap::new();
        let key = (
            "s3://comet-isolation-native-aliases".to_string(),
            hash_object_store_configs(&options),
            false,
        );
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        object_store_cache()
            .write()
            .unwrap()
            .insert(key.clone(), Arc::clone(&store));
        let mut previous = None;
        for schemes in [["s3", "s3a"], ["s3a", "s3"]] {
            let runtime = Arc::new(RuntimeEnv::default());
            for scheme in schemes {
                let (url, _, backend) = prepare_object_store_with_configs(
                    Arc::clone(&runtime),
                    format!("{scheme}://comet-isolation-native-aliases/file.parquet"),
                    &options,
                )
                .unwrap();
                assert_eq!(backend, ObjectStoreBackend::Remote);
                assert!(Arc::ptr_eq(&runtime.object_store(&url).unwrap(), &store));
                assert!(url.as_str().starts_with("s3+comet-"));
                if let Some(previous) = &previous {
                    assert_eq!(&url, previous);
                }
                previous = Some(url);
            }
        }
        object_store_cache().write().unwrap().remove(&key);
    }

    /// Checks that native file construction returns Local and cached Hadoop file routing returns
    /// Other, with distinct registered stores. Removes its synthetic Hadoop cache entry on success.
    #[test]
    fn keeps_native_file_url_separate_from_explicit_hadoop_file_routing() {
        let runtime = Arc::new(RuntimeEnv::default());
        let options = HashMap::from([("fs.comet.libhdfs.schemes".into(), "file".into())]);
        let key = (
            "file://".to_string(),
            hash_object_store_configs(&options),
            true,
        );
        let hdfs_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        object_store_cache()
            .write()
            .unwrap()
            .insert(key.clone(), Arc::clone(&hdfs_store));
        let (hdfs_url, _, hdfs_backend) = prepare_object_store_with_configs(
            Arc::clone(&runtime),
            "file:///comet-isolation-file-routing.parquet".into(),
            &options,
        )
        .unwrap();
        let (native_url, _, native_backend) = prepare_object_store_with_configs(
            Arc::clone(&runtime),
            "file:///comet-isolation-file-routing.parquet".into(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(native_backend, ObjectStoreBackend::Local);
        assert_eq!(hdfs_backend, ObjectStoreBackend::Other);
        assert_eq!(native_url, ObjectStoreUrl::local_filesystem());
        assert_ne!(native_url, hdfs_url);
        assert!(Arc::ptr_eq(
            &runtime.object_store(&hdfs_url).unwrap(),
            &hdfs_store
        ));
        assert!(!Arc::ptr_eq(
            &runtime.object_store(&native_url).unwrap(),
            &hdfs_store
        ));
        object_store_cache().write().unwrap().remove(&key);
    }

    /// Parses the url, registers the object store, and returns a tuple of the object store url and object store path
    #[cfg(not(feature = "hdfs-opendal"))]
    pub(crate) fn prepare_object_store(
        runtime_env: Arc<RuntimeEnv>,
        url: String,
    ) -> Result<(ObjectStoreUrl, Path), ExecutionError> {
        use crate::parquet::parquet_support::prepare_object_store_with_configs;
        prepare_object_store_with_configs(runtime_env, url, &HashMap::new())
            .map(|(url, path, _)| (url, path))
    }

    /// A conversion the schema adapter should have rejected must surface as an error, never
    /// as a mismatched child array that `StructArray::new` panics on (#5671).
    #[test]
    fn convert_list_to_int_inside_struct_errors_instead_of_panicking() {
        use crate::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
        use arrow::array::{Array, ListArray, StructArray};
        use arrow::datatypes::{DataType, Field, Fields, Int32Type};
        use datafusion::physical_plan::ColumnarValue;
        use datafusion_comet_spark_expr::EvalMode;
        use std::sync::Arc;

        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)])]);
        let from_fields = Fields::from(vec![Field::new("x", list.data_type().clone(), true)]);
        let array = StructArray::new(from_fields, vec![Arc::new(list)], None);
        let to_type = DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)]));
        let err = spark_parquet_convert(
            ColumnarValue::Array(Arc::new(array)),
            &to_type,
            &SparkParquetOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .expect_err("array<int> -> int must be an error");
        assert!(
            err.to_string()
                .contains("Unsupported Parquet type conversion"),
            "unexpected error: {err}"
        );
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
    fn test_millis_to_micros_overflow_checked_in_nested_fields() {
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

        // Unfiltered nested reads must also report overflow.
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
        assert!(parquet_convert_array(Arc::clone(&strukt), &target, &options).is_err());
        let converted = parquet_convert_array(strukt, &target, &unchecked_options)
            .expect("filtered nested overflow must not error");
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

    #[test]
    fn test_millis_to_micros_preserves_unchanged_siblings() {
        use crate::parquet::parquet_support::SparkParquetOptions;
        use arrow::array::{
            cast::AsArray, Array, ArrayRef, Int32Array, ListArray, MapArray, StructArray,
            TimestampMicrosecondArray, TimestampMillisecondArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, TimeUnit};
        use datafusion_comet_spark_expr::EvalMode;
        use std::sync::Arc;

        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(0..16));
        let offsets = OffsetBuffer::new(vec![0, 0, 16].into());
        let nulls = NullBuffer::from(vec![false, true]);
        let ints: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets.clone(),
            Arc::clone(&values),
            Some(nulls.clone()),
        ));
        let entries = StructArray::new(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Field::new("value", DataType::Int32, false)),
            ]
            .into(),
            vec![Arc::clone(&values), Arc::clone(&values)],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            offsets,
            entries,
            Some(nulls.clone()),
            false,
        ));
        let input = StructArray::new(
            vec![
                Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )),
                Arc::new(Field::new("ints", ints.data_type().clone(), true)),
                Arc::new(Field::new("map", map.data_type().clone(), true)),
            ]
            .into(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![i64::MAX, 7])),
                ints,
                map,
            ],
            Some(nulls),
        );
        let mut target_fields = input.fields().to_vec();
        target_fields[0] = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let target = DataType::Struct(target_fields.into());
        let input: ArrayRef = Arc::new(input);
        let options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let output = parquet_convert_array(Arc::clone(&input), &target, &options).unwrap();
        assert_eq!(output.data_type(), &target);
        assert!(output.is_null(0));
        let output = output.as_struct();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(1),
            7000
        );
        // Unchanged siblings keep their value buffers, offsets, and parent validity.
        assert_eq!(
            output.column(1).to_data(),
            input.as_struct().column(1).to_data()
        );
        assert_eq!(
            output.column(2).to_data(),
            input.as_struct().column(2).to_data()
        );
        for child in [
            output.column(1).as_list::<i32>().values(),
            output.column(2).as_map().keys(),
            output.column(2).as_map().values(),
        ] {
            assert_eq!(
                child.to_data().buffers()[0].as_ptr(),
                values.to_data().buffers()[0].as_ptr()
            );
        }
    }

    #[test]
    fn test_millis_to_micros_nested_visibility() {
        use crate::parquet::parquet_support::SparkParquetOptions;
        use arrow::array::{
            Array, ArrayRef, ListArray, MapArray, StructArray, TimestampMicrosecondArray,
            TimestampMillisecondArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, TimeUnit};
        use datafusion_comet_spark_expr::EvalMode;
        use std::sync::Arc;

        for timezone in [None::<Arc<str>>, Some(Arc::from("UTC"))] {
            for overflow in [i64::MAX, i64::MIN] {
                let options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
                let millis: ArrayRef = Arc::new(
                    TimestampMillisecondArray::from(vec![overflow, 7, overflow])
                        .with_timezone_opt(timezone.clone()),
                );
                let field = Arc::new(Field::new("ts", millis.data_type().clone(), false));
                let target_field = Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                    false,
                ));
                let validity = Some(NullBuffer::from(vec![false, true, false]));
                let strukt: ArrayRef = Arc::new(StructArray::new(
                    vec![Arc::clone(&field)].into(),
                    vec![Arc::clone(&millis)],
                    validity.clone(),
                ));
                let list: ArrayRef = Arc::new(ListArray::new(
                    Arc::clone(&field),
                    OffsetBuffer::new(vec![0, 1, 2, 3].into()),
                    Arc::clone(&millis),
                    validity.clone(),
                ));
                let entries = StructArray::new(
                    vec![
                        Arc::clone(&field),
                        Arc::new(Field::new("value", millis.data_type().clone(), false)),
                    ]
                    .into(),
                    vec![Arc::clone(&millis), Arc::clone(&millis)],
                    None,
                );
                let map: ArrayRef = Arc::new(MapArray::new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    OffsetBuffer::new(vec![0, 1, 2, 3].into()),
                    entries,
                    validity,
                    false,
                ));
                let target_map = DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::clone(&target_field),
                                Arc::new(Field::new(
                                    "value",
                                    target_field.data_type().clone(),
                                    false,
                                )),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                );
                for (array, target) in [
                    (
                        strukt,
                        DataType::Struct(vec![Arc::clone(&target_field)].into()),
                    ),
                    (list, DataType::List(Arc::clone(&target_field))),
                    (map, target_map),
                ] {
                    // Overflow beneath null parents is not a value Spark reads. Required
                    // children remain non-null, and slicing must not expose hidden entries.
                    for input in [Arc::clone(&array), array.slice(1, 1)] {
                        let converted = parquet_convert_array(input, &target, &options).unwrap();
                        let values = match converted.data_type() {
                            DataType::Struct(_) => Arc::clone(
                                converted
                                    .as_any()
                                    .downcast_ref::<StructArray>()
                                    .unwrap()
                                    .column(0),
                            ),
                            DataType::List(_) => converted
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .unwrap()
                                .value(if converted.len() == 1 { 0 } else { 1 }),
                            DataType::Map(_, _) => Arc::clone(
                                converted
                                    .as_any()
                                    .downcast_ref::<MapArray>()
                                    .unwrap()
                                    .value(if converted.len() == 1 { 0 } else { 1 })
                                    .column(0),
                            ),
                            _ => unreachable!(),
                        };
                        let values = values
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        assert_eq!(values.value(if values.len() == 3 { 1 } else { 0 }), 7000);
                        assert_eq!(values.null_count(), 0);
                    }
                    // Removing parent nulls makes the overflow visible and must fail.
                    let visible = arrow::array::make_array(
                        array.to_data().into_builder().nulls(None).build().unwrap(),
                    );
                    assert!(
                        parquet_convert_array(Arc::clone(&visible), &target, &options).is_err()
                    );
                    // Propagate nullness through more than one level of nesting.
                    let outer: ArrayRef = Arc::new(StructArray::new(
                        vec![Arc::new(Field::new(
                            "nested",
                            visible.data_type().clone(),
                            false,
                        ))]
                        .into(),
                        vec![Arc::clone(&visible)],
                        array.nulls().cloned(),
                    ));
                    let outer_target = DataType::Struct(
                        vec![Arc::new(Field::new("nested", target.clone(), false))].into(),
                    );
                    parquet_convert_array(outer, &outer_target, &options).unwrap();
                    // A slice of a non-null list/map also hides its backing prefix/suffix.
                    parquet_convert_array(visible.slice(1, 1), &target, &options).unwrap();
                }
            }
        }
    }

    #[test]
    fn test_millis_to_micros_list_representations_visibility() {
        use crate::parquet::parquet_support::SparkParquetOptions;
        use arrow::array::{
            cast::AsArray, make_array, Array, ArrayRef, ListArray, StructArray,
            TimestampMillisecondArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::{DataType, Field, TimeUnit, TimestampMicrosecondType};
        use datafusion_comet_spark_expr::EvalMode;
        use std::sync::Arc;

        let options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        for timezone in [None::<Arc<str>>, Some(Arc::from("UTC"))] {
            for overflow in [i64::MIN, i64::MAX] {
                let values: ArrayRef = Arc::new(
                    TimestampMillisecondArray::from(vec![
                        overflow, overflow, 7, 8, overflow, overflow,
                    ])
                    .with_timezone_opt(timezone.clone()),
                );
                let field = Arc::new(Field::new("item", values.data_type().clone(), false));
                let target_field = Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                    false,
                ));
                let representations = |field: &Arc<Field>| {
                    [
                        DataType::List(Arc::clone(field)),
                        DataType::LargeList(Arc::clone(field)),
                        DataType::FixedSizeList(Arc::clone(field), 2),
                        DataType::ListView(Arc::clone(field)),
                        DataType::LargeListView(Arc::clone(field)),
                    ]
                };
                let list = ListArray::new(
                    Arc::clone(&field),
                    OffsetBuffer::new(vec![0, 2, 4, 6].into()),
                    values,
                    None,
                );
                for (source, target) in representations(&field)
                    .into_iter()
                    .zip(representations(&target_field))
                {
                    let unmasked = arrow::compute::cast(&list, &source).unwrap();
                    let validity = NullBuffer::from(vec![false, true, false]);
                    let masked = make_array(
                        unmasked
                            .to_data()
                            .into_builder()
                            .nulls(Some(validity.clone()))
                            .build()
                            .unwrap(),
                    );
                    for input in [
                        Arc::clone(&masked),
                        masked.slice(1, 1),
                        unmasked.slice(1, 1),
                    ] {
                        let output = parquet_convert_array(input, &target, &options).unwrap();
                        let output = arrow::compute::cast(
                            &output,
                            &DataType::List(Arc::clone(&target_field)),
                        )
                        .unwrap();
                        let output = output.as_list::<i32>();
                        let row = if output.len() == 1 { 0 } else { 1 };
                        if output.len() == 3 {
                            assert!(output.is_null(0) && output.is_null(2));
                        }
                        let values = output.value(row);
                        let values = values.as_primitive::<TimestampMicrosecondType>();
                        assert_eq!(values.values().as_ref(), &[7000, 8000], "{source:?}");
                        assert_eq!(values.null_count(), 0);
                    }
                    assert!(
                        parquet_convert_array(Arc::clone(&unmasked), &target, &options).is_err(),
                        "{source:?}"
                    );
                    parquet_convert_array(unmasked.slice(0, 0), &target, &options).unwrap();
                    // The list itself is non-null: visibility comes from the enclosing struct.
                    let outer: ArrayRef = Arc::new(StructArray::new(
                        vec![Arc::new(Field::new("a", source, false))].into(),
                        vec![unmasked],
                        Some(validity),
                    ));
                    let outer_target =
                        DataType::Struct(vec![Arc::new(Field::new("a", target, false))].into());
                    parquet_convert_array(outer, &outer_target, &options).unwrap();
                }
            }
        }
    }

    #[test]
    fn test_millis_to_micros_overlapping_list_views() {
        use crate::parquet::parquet_support::SparkParquetOptions;
        use arrow::array::{
            cast::AsArray, make_array, Array, ArrayRef, LargeListViewArray, ListViewArray,
            TimestampMillisecondArray,
        };
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{DataType, Field, TimeUnit, TimestampMicrosecondType};
        use datafusion_comet_spark_expr::EvalMode;
        use std::sync::Arc;

        let options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let values: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            i64::MAX,
            7,
            8,
            9,
            i64::MIN,
        ]));
        let field = Arc::new(Field::new("item", values.data_type().clone(), false));
        let target_field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));
        let nulls = Some(NullBuffer::from(vec![
            false, true, true, false, false, true,
        ]));
        let small: ArrayRef = Arc::new(ListViewArray::new(
            Arc::clone(&field),
            vec![0, 2, 1, 3, 0, 4].into(),
            vec![4, 2, 2, 1, 1, 0].into(),
            Arc::clone(&values),
            nulls.clone(),
        ));
        let large: ArrayRef = Arc::new(LargeListViewArray::new(
            field,
            vec![0, 2, 1, 3, 0, 4].into(),
            vec![4, 2, 2, 1, 1, 0].into(),
            values,
            nulls,
        ));
        for (array, target) in [
            (small, DataType::ListView(Arc::clone(&target_field))),
            (large, DataType::LargeListView(Arc::clone(&target_field))),
        ] {
            // Null views overlap live views both before and after them. Unreferenced prefix
            // and suffix values overflow, and the final live view is empty.
            for input in [Arc::clone(&array), array.slice(1, 2)] {
                let output = parquet_convert_array(input, &target, &options).unwrap();
                let output =
                    arrow::compute::cast(&output, &DataType::List(Arc::clone(&target_field)))
                        .unwrap();
                let output = output.as_list::<i32>();
                let first = if output.len() == 2 { 0 } else { 1 };
                for (row, expected) in [(first, [8000, 9000]), (first + 1, [7000, 8000])] {
                    let values = output.value(row);
                    let values = values.as_primitive::<TimestampMicrosecondType>();
                    assert_eq!(values.values().as_ref(), &expected);
                    assert_eq!(values.null_count(), 0);
                }
                if output.len() == 6 {
                    assert!(output.is_null(0) && output.is_null(3) && output.is_null(4));
                    assert!(output.value(5).is_empty());
                }
            }
            let visible = make_array(array.to_data().into_builder().nulls(None).build().unwrap());
            assert!(parquet_convert_array(visible, &target, &options).is_err());
        }
    }

    /// Constructs S3 stores using anonymous credentials, without reading remote objects, and
    /// checks that both alias URL forms return the normalized bucket, key, and remote I/O label.
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
                "test_bucket",
                "/comet/spark-warehouse/part-00000.snappy.parquet",
            ),
            (
                "blob:///mybucket/warehouse/data/part-0.snappy.parquet",
                "mybucket",
                "warehouse/data/part-0.snappy.parquet",
            ),
        ] {
            let (object_store_url, path, backend) = prepare_object_store_with_configs(
                Arc::new(RuntimeEnv::default()),
                input.to_string(),
                &configs,
            )
            .unwrap_or_else(|e| panic!("{input} should normalize to s3://: {e}"));
            assert_eq!(
                object_store_url,
                ObjectStoreUrl::parse(format!(
                    "s3+comet-{:016x}-native://{expected_bucket}",
                    hash_object_store_configs(&configs),
                ))
                .unwrap()
            );
            assert_eq!(path, Path::from(expected_path));
            assert_eq!(backend, super::ObjectStoreBackend::Remote);
        }
    }

    #[cfg(not(feature = "hdfs-opendal"))]
    #[test]
    #[cfg_attr(miri, ignore)] // AWS credential providers and object_store call foreign functions
    fn test_prepare_object_store_keeps_s3a_off_libhdfs_when_only_s3_is_listed() {
        // `fs.comet.libhdfs.schemes=s3` routes `s3://` through libhdfs and says nothing about
        // `s3a` or the opted-in aliases. Both normalize onto `s3://`, so deciding libhdfs from the
        // normalized URL would hand these scans to `create_hdfs_object_store` -- which in this
        // build is the "not enabled" stub, and in a default build would point libhdfs at a name
        // node of `s3://bucket`. The JVM gate classifies them as object_store-native and admits
        // them, so this dispatch is what keeps native in lockstep with the planner.
        use crate::parquet::parquet_support::prepare_object_store_with_configs;
        let mut configs: HashMap<String, String> = HashMap::new();
        configs.insert("fs.comet.libhdfs.schemes".to_string(), "s3".to_string());
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

        for input in [
            "s3a://test_bucket/comet/part-00000.snappy.parquet",
            "blob://test_bucket/comet/part-00000.snappy.parquet",
        ] {
            let (object_store_url, path, backend) = prepare_object_store_with_configs(
                Arc::new(RuntimeEnv::default()),
                input.to_string(),
                &configs,
            )
            .unwrap_or_else(|e| panic!("{input} must build an S3 store, not libhdfs: {e}"));
            assert_eq!(
                object_store_url,
                ObjectStoreUrl::parse(format!(
                    "s3+comet-{:016x}-native://test_bucket",
                    hash_object_store_configs(&configs),
                ))
                .unwrap()
            );
            assert_eq!(path, Path::from("/comet/part-00000.snappy.parquet"));
            assert_eq!(backend, super::ObjectStoreBackend::Remote);
        }

        // Listing `s3a` is the supported way to route it through libhdfs, and still does.
        configs.insert("fs.comet.libhdfs.schemes".to_string(), "s3a".to_string());
        let err = prepare_object_store_with_configs(
            Arc::new(RuntimeEnv::default()),
            "s3a://test_bucket/comet/part-00000.snappy.parquet".to_string(),
            &configs,
        )
        .expect_err("an explicitly listed s3a must reach the libhdfs backend");
        assert!(
            err.to_string().contains("Hdfs support is not enabled"),
            "unexpected error: {err}"
        );
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

        /// Two file children differ only by case and the requested name folds onto both:
        /// Spark's `caseInsensitiveParquetFieldMap` raises `_LEGACY_ERROR_TEMP_2093` rather
        /// than picking either, and case-sensitive mode reads the exact match.
        #[test]
        fn case_insensitive_ambiguous_names_error_but_exact_match_reads() {
            let from = struct_of(
                vec![
                    Field::new("A", DataType::Int32, true),
                    Field::new("a", DataType::Int32, true),
                ],
                vec![1, 2],
            );
            let to_type =
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));

            let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            opts.case_sensitive = false;
            let err = parquet_convert_array(Arc::clone(&from), &to_type, &opts).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("duplicate field") && msg.contains("A") && msg.contains("a"),
                "unexpected error: {msg}"
            );

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

        /// A list or map column without any struct inside gets a `Leaf` mapping from the
        /// adapter; its elements still convert through the checked millis-to-micros path.
        #[test]
        fn leaf_mapping_converts_list_and_map_elements() {
            use crate::parquet::parquet_support::spark_parquet_convert_with_mapping;
            use arrow::array::{ListArray, MapArray, TimestampMillisecondArray};
            use arrow::buffer::OffsetBuffer;
            use arrow::datatypes::TimeUnit;
            use datafusion::physical_plan::ColumnarValue;

            let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![i64::MAX]));
            let ms_field = Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ));
            let us_field = Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ));
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::clone(&ms_field),
                OffsetBuffer::new(vec![0, 1].into()),
                Arc::clone(&millis),
                None,
            ));
            let entries = StructArray::new(
                Fields::from(vec![
                    Field::new(
                        "key",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("value", DataType::Int32, true),
                ]),
                vec![Arc::clone(&millis), Arc::new(Int32Array::from(vec![1]))],
                None,
            );
            let map: ArrayRef = Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0, 1].into()),
                entries,
                None,
                false,
            ));
            let map_target = DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new(
                            "key",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            false,
                        ),
                        Field::new("value", DataType::Int32, true),
                    ])),
                    false,
                )),
                false,
            );
            let opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
            for (array, target) in [(list, DataType::List(us_field)), (map, map_target)] {
                let result = spark_parquet_convert_with_mapping(
                    ColumnarValue::Array(array),
                    &target,
                    &FieldMapping::Leaf,
                    &opts,
                );
                assert!(result.is_err(), "overflow must be reported for {target:?}");
            }
        }

        /// A mapping resolved against a wider struct than the array actually carries must
        /// surface as an error naming the field, not as an index panic in the executor.
        #[test]
        fn mapping_index_beyond_struct_children_errors() {
            use crate::parquet::parquet_support::{
                spark_parquet_convert_with_mapping, StructFieldSource,
            };
            use datafusion::physical_plan::ColumnarValue;

            let from = struct_of(vec![Field::new("x", DataType::Int32, true)], vec![1]);
            let to_type =
                DataType::Struct(Fields::from(vec![Field::new("y", DataType::Int32, true)]));
            let mapping = FieldMapping::Struct(vec![StructFieldSource {
                from_index: Some(1),
                nested: FieldMapping::Leaf,
            }]);
            let opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);

            let err = spark_parquet_convert_with_mapping(
                ColumnarValue::Array(from),
                &to_type,
                &mapping,
                &opts,
            )
            .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("y") && msg.contains("1 child"),
                "unexpected error: {msg}"
            );
        }
    }
}
