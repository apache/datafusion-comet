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
use arrow::array::{FixedSizeBinaryArray, ListArray, MapArray, StringArray};
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
/// is requested.
pub fn spark_parquet_convert(
    arg: ColumnarValue,
    data_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(parquet_convert_array(
            array,
            data_type,
            parquet_options,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
            // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
            // here.
            let array = scalar.to_array()?;
            let scalar = ScalarValue::try_from_array(
                &parquet_convert_array(array, data_type, parquet_options)?,
                0,
            )?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

fn parquet_convert_array(
    array: ArrayRef,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ArrayRef> {
    parquet_convert_array_impl(array, to_type, parquet_options, true)
}

fn parquet_convert_array_impl(
    array: ArrayRef,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
    top_level: bool,
) -> DataFusionResult<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type();

    // Try Comet specific handlers first, then arrow-rs cast if supported, and fail otherwise.
    match (from_type, to_type) {
        (Struct(_), Struct(_)) => Ok(parquet_convert_struct_to_struct(
            array.as_struct(),
            from_type,
            to_type,
            parquet_options,
        )?),
        (List(_), List(to_inner_type)) => {
            let list_arr: &ListArray = array.as_list();
            let cast_field = parquet_convert_array_impl(
                Arc::clone(list_arr.values()),
                to_inner_type.data_type(),
                parquet_options,
                false,
            )?;

            Ok(Arc::new(ListArray::try_new(
                Arc::clone(to_inner_type),
                list_arr.offsets().clone(),
                cast_field,
                list_arr.nulls().cloned(),
            )?))
        }
        (
            Timestamp(TimeUnit::Millisecond, _),
            Timestamp(TimeUnit::Microsecond, target_tz),
        ) if top_level && parquet_options.checked_timestamp_overflow => {
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
        (Timestamp(TimeUnit::Microsecond, None), Timestamp(TimeUnit::Microsecond, Some(tz))) => {
            Ok(Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .reinterpret_cast::<TimestampMicrosecondType>()
                    .with_timezone(Arc::clone(tz)),
            ))
        }
        (Map(_, ordered_from), Map(_, ordered_to)) if ordered_from == ordered_to =>
            parquet_convert_map_to_map(array.as_map(), to_type, parquet_options, *ordered_to)
            ,
        // Iceberg stores UUIDs as 16-byte fixed binary but Spark expects string representation.
        // Arrow doesn't support casting FixedSizeBinary to Utf8, so we handle it manually.
        (FixedSizeBinary(16), Utf8) => {
            let binary_array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("Expected a FixedSizeBinaryArray");

            let string_array: StringArray = binary_array
                .iter()
                .map(|opt_bytes| {
                    opt_bytes.map(|bytes| {
                        let uuid = uuid::Uuid::from_bytes(
                            bytes.try_into().expect("Expected 16 bytes")
                        );
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

/// Read the Parquet field id stored under arrow-rs's `PARQUET_FIELD_ID_META_KEY`.
fn field_id(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// Resolve each requested (`to`) struct field to the index of the file (`from`) field it reads
/// from, or `None` when the file holds no such field. Mirrors Spark's `clipParquetGroupFields`:
/// when the requested struct carries Parquet field IDs anywhere (and `use_field_id` is set),
/// ID-bearing requested fields match ONLY by ID (a missing ID is a missing column, never a name
/// fallback); other fields match by name, folded with the same `toLowerCase(Locale.ROOT)` fold
/// the top-level schema adapter uses when `case_sensitive` is false. A requested field whose
/// folded name matches more than one file field in case-insensitive mode raises Spark's
/// `foundDuplicateFieldInCaseInsensitiveModeError`.
///
/// Shared by the runtime convert (`parquet_convert_struct_to_struct`) and the plan-time
/// conversion check in `schema_adapter`, so both resolve nested fields identically.
pub(crate) fn match_struct_fields(
    from_fields: &[FieldRef],
    to_fields: &[FieldRef],
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<Vec<Option<usize>>> {
    let should_match_by_id =
        parquet_options.use_field_id && to_fields.iter().any(|f| field_id(f).is_some());

    let from_id_to_index: HashMap<i32, usize> = if should_match_by_id {
        let mut map = HashMap::new();
        for (i, field) in from_fields.iter().enumerate() {
            if let Some(id) = field_id(field) {
                map.entry(id).or_insert(i);
            }
        }
        map
    } else {
        HashMap::new()
    };

    // Fold the file (`from`) and requested (`to`) field names once via the JVM's
    // `toLowerCase(Locale.ROOT)` (the same fold the top-level schema adapter uses), so
    // nested case-insensitive matching is byte-for-byte consistent with the top level.
    let mut all_names: Vec<&str> = Vec::with_capacity(from_fields.len() + to_fields.len());
    all_names.extend(from_fields.iter().map(|f| f.name().as_str()));
    all_names.extend(to_fields.iter().map(|f| f.name().as_str()));
    let all_folded = fold_names(&all_names, parquet_options.case_sensitive);
    let (from_folded, to_folded) = all_folded.split_at(from_fields.len());

    // Group file field indices by folded name so a case-insensitive collision is detected
    // (Spark's `caseInsensitiveParquetFieldMap`) rather than silently overwritten.
    let mut folded_to_indices: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, folded) in from_folded.iter().enumerate() {
        folded_to_indices
            .entry(folded.as_str())
            .or_default()
            .push(i);
    }

    to_fields
        .iter()
        .enumerate()
        .map(
            |(to_pos, to_field)| match (should_match_by_id, field_id(to_field)) {
                // Spark treats a missing ID match as a missing column rather than
                // falling back to name match.
                (true, Some(id)) => Ok(from_id_to_index.get(&id).copied()),
                _ => match folded_to_indices.get(to_folded[to_pos].as_str()) {
                    // Mirror Spark's `foundDuplicateFieldInCaseInsensitiveModeError`: a
                    // requested field matching more than one file field is ambiguous. Gated on
                    // case-insensitive mode to match the top-level check (which only runs when
                    // `!case_sensitive`): when case-sensitive the fold is identity, so a
                    // collision means byte-identical sibling names, and raising an error whose
                    // message says "in case-insensitive mode" would be wrong. Fall through to
                    // the first match in that case.
                    Some(indices) if indices.len() > 1 && !parquet_options.case_sensitive => {
                        let matched: Vec<&str> = indices
                            .iter()
                            .map(|&i| from_fields[i].name().as_str())
                            .collect();
                        Err(DataFusionError::External(Box::new(
                            SparkError::duplicate_field_case_insensitive(to_field.name(), &matched),
                        )))
                    }
                    Some(indices) => Ok(Some(indices[0])),
                    None => Ok(None),
                },
            },
        )
        .collect()
}

/// Cast between struct types based on logic in
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
fn parquet_convert_struct_to_struct(
    array: &StructArray,
    from_type: &DataType,
    to_type: &DataType,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            let from_indices = match_struct_fields(from_fields, to_fields, parquet_options)?;

            let mut field_overlap = false;
            let mut cast_fields: Vec<ArrayRef> = Vec::with_capacity(to_fields.len());
            for (to_field, from_index) in to_fields.iter().zip(from_indices) {
                if let Some(from_index) = from_index {
                    cast_fields.push(parquet_convert_array_impl(
                        Arc::clone(array.column(from_index)),
                        to_field.data_type(),
                        parquet_options,
                        false,
                    )?);
                    field_overlap = true;
                } else {
                    cast_fields.push(new_null_array(to_field.data_type(), array.len()));
                }
            }

            // When the file's struct contains none of the requested fields, the
            // returned validity buffer depends on Spark's
            // `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing` (SPARK-53535,
            // Spark 4.1+). Legacy mode marks the whole column null; the new default
            // preserves the file's parent-row nullness so non-null parents materialize
            // as a struct of all-null fields.
            let nulls =
                if !field_overlap && parquet_options.return_null_struct_if_all_fields_missing {
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
        _ => unreachable!(),
    }
}

/// Cast a map type to another map type. The same as arrow-cast except we recursively call our own
/// parquet_convert_array
fn parquet_convert_map_to_map(
    from: &MapArray,
    to_data_type: &DataType,
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

            let key_array = parquet_convert_array_impl(
                Arc::clone(from.keys()),
                key_field.data_type(),
                parquet_options,
                false,
            )?;
            let value_array = parquet_convert_array_impl(
                Arc::clone(from.values()),
                value_field.data_type(),
                parquet_options,
                false,
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
                to_ordered,
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

pub fn is_hdfs_scheme(url: &Url, object_store_configs: &HashMap<String, String>) -> bool {
    const COMET_LIBHDFS_SCHEMES_KEY: &str = "fs.comet.libhdfs.schemes";
    let scheme = url.scheme();
    if let Some(libhdfs_schemes) = object_store_configs.get(COMET_LIBHDFS_SCHEMES_KEY) {
        use itertools::Itertools;
        libhdfs_schemes.split(",").contains(scheme)
    } else {
        scheme == "hdfs"
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
    let mut url = Url::parse(url.as_str())
        .map_err(|e| ExecutionError::GeneralError(format!("Error parsing URL {url}: {e}")))?;
    let is_hdfs_scheme = is_hdfs_scheme(&url, object_store_configs);
    let mut scheme = url.scheme();
    if !is_hdfs_scheme && scheme == "s3a" {
        scheme = "s3";
        url.set_scheme("s3").map_err(|_| {
            ExecutionError::GeneralError("Could not convert scheme from s3a to s3".to_string())
        })?;
    }
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

    #[test]
    fn test_millis_to_micros_overflow_checked_only_at_top_level() {
        use crate::parquet::parquet_support::{parquet_convert_array, SparkParquetOptions};
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
}
