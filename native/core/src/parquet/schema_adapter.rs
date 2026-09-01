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

use crate::jvm_bridge::{JVMClasses, StringWrapper};
use crate::parquet::cast_column::CometCastColumnExpr;
use crate::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion_comet_common::SparkError;
use datafusion_comet_spark_expr::{Cast, SparkCastOptions};
use datafusion_physical_expr_adapter::{
    replace_columns_with_literals, DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use jni::objects::JString;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock, RwLock};

/// Factory for creating Spark-compatible physical expression adapters.
///
/// This factory creates adapters that rewrite expressions at planning time
/// to inject Spark-compatible casts where needed.
#[derive(Clone, Debug)]
pub struct SparkPhysicalExprAdapterFactory {
    /// Spark-specific parquet options for type conversions
    parquet_options: SparkParquetOptions,
    /// Default values for columns that may be missing from the physical schema.
    /// The key is the Column (containing name and index).
    default_values: Option<HashMap<Column, ScalarValue>>,
}

impl SparkPhysicalExprAdapterFactory {
    /// Create a new factory with the given options.
    pub fn new(
        parquet_options: SparkParquetOptions,
        default_values: Option<HashMap<Column, ScalarValue>>,
    ) -> Self {
        Self {
            parquet_options,
            default_values,
        }
    }
}

/// Read the Parquet field id stored under arrow-rs's `PARQUET_FIELD_ID_META_KEY`.
fn parse_field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

fn schema_has_field_ids(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(|f| parse_field_id(f).is_some())
}

/// Fold field names for case-insensitive matching under Spark's Parquet rules. In case-sensitive
/// mode names are returned unchanged. In case-insensitive mode this mirrors `ParquetReadSupport`,
/// which resolves fields by `name.toLowerCase(Locale.ROOT)`; the fold is delegated to the JVM
/// (`CometSchemaUtils.toLowerCaseRoot`) so Comet folds names exactly as Spark does, including
/// non-ASCII characters. Outside a Comet task there is no attached JVM (e.g. Rust unit tests) or
/// the JNI call fails, so fall back to ASCII folding.
///
/// Folding is a pure function of the name, and the same field names recur across every batch and
/// file, so results are memoized process-wide (see [`fold_cache`]). That turns the per-file
/// (schema-adapter) and per-batch (nested `Struct -> Struct` convert) folds into one JVM crossing
/// per distinct name for the life of the process.
pub(crate) fn fold_names(names: &[&str], case_sensitive: bool) -> Vec<String> {
    if case_sensitive {
        return names.iter().map(|n| n.to_string()).collect();
    }

    let cache = fold_cache();
    let mut result: Vec<Option<String>> = vec![None; names.len()];
    let mut miss_positions: Vec<usize> = Vec::new();
    {
        let read = cache.read().unwrap();
        for (i, name) in names.iter().enumerate() {
            match read.get(*name) {
                Some(folded) => result[i] = Some(folded.clone()),
                None => miss_positions.push(i),
            }
        }
    }

    if !miss_positions.is_empty() {
        let miss_names: Vec<&str> = miss_positions.iter().map(|&i| names[i]).collect();
        let (folded, cacheable) = fold_uncached(&miss_names);
        if cacheable {
            let mut write = cache.write().unwrap();
            for (pos, &i) in miss_positions.iter().enumerate() {
                // Bound the process-wide cache: an executor JVM is long-lived, so stop inserting
                // once the distinct-name vocabulary grows large. Names beyond the cap still fold
                // correctly on each call, just uncached.
                if write.len() >= FOLD_CACHE_MAX_ENTRIES {
                    break;
                }
                write.insert(names[i].to_string(), folded[pos].clone());
            }
        }
        for (pos, &i) in miss_positions.iter().enumerate() {
            result[i] = Some(folded[pos].clone());
        }
    }

    result
        .into_iter()
        .map(|folded| folded.expect("every name folded"))
        .collect()
}

/// Fold a single field name. Convenience wrapper over [`fold_names`] for the per-column-reference
/// lookups; the schema side is always folded in bulk via [`fold_schema_names`].
pub(crate) fn fold_name(name: &str, case_sensitive: bool) -> String {
    let mut folded = fold_names(&[name], case_sensitive);
    folded
        .pop()
        .expect("fold_names returns one entry per input name")
}

/// Fold every field name in `schema`. See [`fold_names`].
pub(crate) fn fold_schema_names(schema: &SchemaRef, case_sensitive: bool) -> Vec<String> {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    fold_names(&names, case_sensitive)
}

/// Upper bound on [`fold_cache`] entries, comfortably above any realistic distinct-field-name
/// vocabulary. Caps memory for pathological workloads (an executor reading very many wide or
/// high-cardinality schemas over its lifetime).
const FOLD_CACHE_MAX_ENTRIES: usize = 100_000;

/// Process-wide memo of `original_name -> folded_name`, shared across all tasks in the (long-lived)
/// executor JVM. Insertion stops at [`FOLD_CACHE_MAX_ENTRIES`], so it cannot grow without bound;
/// names beyond the cap still fold correctly, just uncached.
fn fold_cache() -> &'static RwLock<HashMap<String, String>> {
    static CACHE: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Fold names that missed the cache. Returns the folds and whether they may be cached: a transient
/// JVM failure returns the ASCII fallback but is NOT cached, so it cannot poison later lookups once
/// the JVM recovers.
fn fold_uncached(names: &[&str]) -> (Vec<String>, bool) {
    if crate::JAVA_VM.get().is_some() {
        match jvm_fold_all(names) {
            Ok(folded) => return (folded, true),
            Err(e) => log::warn!(
                "JVM case-fold failed; falling back to ASCII lowercasing, so non-ASCII field \
                 names may not resolve the way Spark does: {e}"
            ),
        }
        (
            names.iter().map(|n| n.to_ascii_lowercase()).collect(),
            false,
        )
    } else {
        // No attached JVM (e.g. Rust unit tests): ASCII folding is the permanent mode, cacheable.
        (names.iter().map(|n| n.to_ascii_lowercase()).collect(), true)
    }
}

/// Lower-case a batch of names via the JVM's `String.toLowerCase(Locale.ROOT)`, matching Spark's
/// `ParquetReadSupport` byte-for-byte. Folds in chunks so at most `CHUNK * 2` JNI local refs are
/// live in a single frame, keeping wide schemas within `DEFAULT_LOCAL_FRAME_CAPACITY` (32).
fn jvm_fold_all(names: &[&str]) -> DataFusionResult<Vec<String>> {
    const CHUNK: usize = 16;
    let mut folded = Vec::with_capacity(names.len());
    for chunk in names.chunks(CHUNK) {
        JVMClasses::with_env(|env| -> DataFusionResult<()> {
            // SAFETY: the JNI static call and `JString::from_raw` are sound here: the class and
            // method id are cached in `JVMClasses`, and `toLowerCaseRoot` returns a valid String
            // local ref that lives for this frame.
            unsafe {
                for name in chunk {
                    let jname = env
                        .new_string(name)
                        .map_err(|e| DataFusionError::Execution(format!("new_string: {e}")))?;
                    let lowered = jni_static_call!(
                        env,
                        comet_schema_utils.to_lower_case_root(&jname) -> StringWrapper
                    )?;
                    folded.push(
                        JString::from_raw(env, lowered.get().as_raw())
                            .try_to_string(env)
                            .map_err(|e| {
                                DataFusionError::Execution(format!("try_to_string: {e}"))
                            })?,
                    );
                }
            }
            Ok(())
        })?;
    }
    Ok(folded)
}

/// Remap physical schema field names to match logical schema field names. Mirrors Spark's
/// `clipParquetGroupFields`: prefer ID match for any logical field that carries a
/// `PARQUET:field_id`, fall back to case-insensitive name match otherwise.
///
/// The remap only changes top-level field NAMES so that `DefaultPhysicalExprAdapter`'s
/// exact-name lookup hits. Indices, types, nullability, and metadata stay as in the file.
/// Returns the rewritten schema and a `logical_name -> original_physical_name` map used
/// downstream to restore the original physical names before stream consumption.
fn remap_physical_schema(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    case_sensitive: bool,
    use_field_id: bool,
    ignore_missing_field_id: bool,
) -> DataFusionResult<(SchemaRef, HashMap<String, String>)> {
    let should_match_by_id = use_field_id && schema_has_field_ids(logical_schema);

    if should_match_by_id && !ignore_missing_field_id && !schema_has_field_ids(physical_schema) {
        // Mirrors `ParquetReadSupport.inferSchema`'s eager check (Spark throws a runtime
        // error rather than silently returning null columns).
        return Err(DataFusionError::External(Box::new(
            SparkError::ParquetMissingFieldIds,
        )));
    }

    // Build id -> all matching physical field names. We need the full list so we can mirror
    // Spark's `_LEGACY_ERROR_TEMP_2094` "Found duplicate field(s)" error when an ID-bearing
    // logical field would resolve to more than one physical field.
    let mut id_to_phys_names: HashMap<i32, Vec<String>> = HashMap::new();
    if should_match_by_id {
        for pf in physical_schema.fields() {
            if let Some(id) = parse_field_id(pf) {
                id_to_phys_names
                    .entry(id)
                    .or_default()
                    .push(pf.name().clone());
            }
        }
        for lf in logical_schema.fields() {
            if let Some(id) = parse_field_id(lf) {
                if let Some(matches) = id_to_phys_names.get(&id) {
                    if matches.len() > 1 {
                        return Err(DataFusionError::External(Box::new(
                            SparkError::DuplicateFieldByFieldId {
                                required_id: id,
                                matched_fields: matches.join(", "),
                            },
                        )));
                    }
                }
            }
        }
    }

    // Pre-build id -> first matching logical field for the per-physical rename pass below.
    let id_to_logical: HashMap<i32, &FieldRef> = if should_match_by_id {
        let mut map = HashMap::new();
        for lf in logical_schema.fields() {
            if let Some(id) = parse_field_id(lf) {
                map.entry(id).or_insert(lf);
            }
        }
        map
    } else {
        HashMap::new()
    };

    // Fold every logical and physical field name once (Spark's `toLowerCase(Locale.ROOT)`, via the
    // JVM) so the O(physical x logical) name matching below compares pre-folded strings instead of
    // crossing into the JVM for every pair. Mirrors Spark's `caseInsensitiveParquetFieldMap`, which
    // groups file fields by their folded name a single time.
    let logical_folded = fold_schema_names(logical_schema, case_sensitive);
    let physical_folded = fold_schema_names(physical_schema, case_sensitive);

    // Folded names of ID-bearing logical fields whose ID is not present in the file. Any physical
    // field that shares one of these names must be renamed to something the
    // `DefaultPhysicalExprAdapter` cannot name-match, otherwise the read would silently fall
    // through to a name match. Spark's `matchIdField` solves the same problem with
    // `generateFakeColumnName` (see `ParquetReadSupport.scala`).
    let unmatched_id_logical_folded: std::collections::HashSet<String> = if should_match_by_id {
        logical_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(j, lf)| {
                parse_field_id(lf).and_then(|id| {
                    if id_to_phys_names.contains_key(&id) {
                        None
                    } else {
                        Some(logical_folded[j].clone())
                    }
                })
            })
            .collect()
    } else {
        std::collections::HashSet::new()
    };
    let mut fake_counter: usize = 0;

    let mut name_map: HashMap<String, String> = HashMap::new();
    let remapped_fields: Vec<FieldRef> = physical_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(phys_idx, field)| {
            // ID match first when the logical schema is ID-bearing.
            if should_match_by_id {
                if let Some(phys_id) = parse_field_id(field) {
                    if let Some(logical_field) = id_to_logical.get(&phys_id) {
                        if logical_field.name() != field.name() {
                            name_map.insert(logical_field.name().clone(), field.name().clone());
                            return Arc::new(
                                Field::new(
                                    logical_field.name(),
                                    field.data_type().clone(),
                                    field.is_nullable(),
                                )
                                .with_metadata(field.metadata().clone()),
                            );
                        }
                        return Arc::clone(field);
                    }
                }
            }

            // Block accidental name match for ID-bearing logical fields whose ID is missing
            // from the file. Mirrors Spark's `generateFakeColumnName` in `matchIdField`.
            if should_match_by_id
                && unmatched_id_logical_folded.contains(&physical_folded[phys_idx])
            {
                fake_counter += 1;
                let fake_name = format!("__comet_unmatched_field_id_{}", fake_counter);
                return Arc::new(
                    Field::new(fake_name, field.data_type().clone(), field.is_nullable())
                        .with_metadata(field.metadata().clone()),
                );
            }

            // Name match. Spark's `matchIdField` does not fall through to a name match for
            // ID-bearing logical fields, so skip those when the schema is ID-bearing.
            if !case_sensitive {
                let logical_field = logical_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(j, lf)| {
                        let lf_has_id = should_match_by_id && parse_field_id(lf).is_some();
                        !lf_has_id && logical_folded[*j] == physical_folded[phys_idx]
                    })
                    .map(|(_, lf)| lf);
                if let Some(logical_field) = logical_field {
                    if logical_field.name() != field.name() {
                        name_map.insert(logical_field.name().clone(), field.name().clone());
                        return Arc::new(
                            Field::new(
                                logical_field.name(),
                                field.data_type().clone(),
                                field.is_nullable(),
                            )
                            .with_metadata(field.metadata().clone()),
                        );
                    }
                }
            }

            Arc::clone(field)
        })
        .collect();

    Ok((Arc::new(Schema::new(remapped_fields)), name_map))
}

/// Format an Arrow `DataType` as Spark's catalog string (e.g. `Int64` -> `bigint`),
/// so SchemaColumnConvertNotSupportedException messages match Spark's vectorized reader.
fn spark_catalog_name(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Binary | DataType::LargeBinary => "binary".to_string(),
        DataType::Date32 => "date".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        DataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            format!("decimal({p},{s})")
        }
        _ => "unknown".to_string(),
    }
}

/// Format an Arrow `DataType` as the Parquet primitive type name
/// (e.g. `Int64` -> `INT64`, matching `PrimitiveTypeName.toString()` in parquet-mr).
fn parquet_primitive_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "INT32",
        DataType::Int64 => "INT64",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => "BINARY",
        // Spark stores DATE as INT32 with a DATE logical-type annotation.
        DataType::Date32 => "INT32",
        // Spark stores TIMESTAMP as INT64 with a timestamp annotation, or as
        // INT96 (legacy nanos). arrow-rs surfaces both as `Timestamp`; without
        // the original physical name we report INT64, which matches the
        // common case.
        DataType::Timestamp(_, _) => "INT64",
        // Mirror Spark's `SparkToParquetSchemaConverter` decimal mapping:
        // precision 1-9 -> INT32, 10-18 -> INT64, 19+ -> FIXED_LEN_BYTE_ARRAY.
        DataType::Decimal128(p, _) | DataType::Decimal256(p, _) => {
            if *p <= 9 {
                "INT32"
            } else if *p <= 18 {
                "INT64"
            } else {
                "FIXED_LEN_BYTE_ARRAY"
            }
        }
        _ => "UNKNOWN",
    }
}

fn is_string_or_binary(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

/// Build a Spark-shaped `SchemaColumnConvertNotSupportedException` carrier for a
/// rejected Parquet -> Spark conversion. The bracketed column wrapping mirrors
/// `Arrays.toString(descriptor.getPath())` in Spark's vectorized reader.
fn parquet_schema_convert_err(
    field_name: &str,
    physical_type: &DataType,
    target_type: &DataType,
) -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::ParquetSchemaConvert {
        file_path: String::new(),
        column: format!("[{}]", field_name),
        physical_type: parquet_primitive_name(physical_type).to_string(),
        spark_type: spark_catalog_name(target_type),
    }))
}

/// Build a `RejectOnNonEmpty` expr wrapping `child`. The rejection fires only
/// when the input batch is non-empty (mirrors Spark's per-row-group check).
fn reject_on_non_empty_expr(
    child: Arc<dyn PhysicalExpr>,
    target_field: &FieldRef,
    field_name: &str,
    physical_type: &DataType,
    target_type: &DataType,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(RejectOnNonEmpty {
        child,
        target_field: Arc::clone(target_field),
        column: format!("[{}]", field_name),
        physical_type: parquet_primitive_name(physical_type).to_string(),
        spark_type: spark_catalog_name(target_type),
    })
}

/// Check if a specific column name has duplicate case-insensitive matches in the physical schema.
/// `col_folded` and `physical_folded` are the pre-folded forms (see `fold_names`), so this compares
/// folded strings instead of re-folding on every call. Returns the Spark-shaped error if there
/// is more than one match.
fn check_column_duplicate(
    col_name: &str,
    col_folded: &str,
    physical_schema: &SchemaRef,
    physical_folded: &[String],
) -> Option<SparkError> {
    let matches: Vec<&str> = physical_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| physical_folded[*i] == col_folded)
        .map(|(_, pf)| pf.name().as_str())
        .collect();
    (matches.len() > 1).then(|| SparkError::duplicate_field_case_insensitive(col_name, &matches))
}

impl PhysicalExprAdapterFactory for SparkPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DataFusionResult<Arc<dyn PhysicalExprAdapter>> {
        // Remap physical schema field names to match logical names by Parquet field id
        // (when the logical schema carries IDs and `use_field_id` is set) and/or by
        // case-insensitive name match. The DefaultPhysicalExprAdapter uses exact name
        // matching, so without this remapping, columns whose file names differ from the
        // logical names won't match and will be filled with NULLs.
        //
        // We also keep a reverse map (logical name -> original physical name) so that
        // after the default adapter produces expressions, we can remap column names back
        // to the original physical names. This is necessary because downstream code
        // (reassign_expr_columns) looks up columns by name in the actual stream schema,
        // which uses the original physical file column names.
        let needs_remap = !self.parquet_options.case_sensitive
            || (self.parquet_options.use_field_id && schema_has_field_ids(&logical_file_schema));
        let (
            adapted_physical_schema,
            logical_to_physical_names,
            original_physical_schema,
            original_physical_folded,
        ) = if needs_remap {
            let (remapped, logical_to_physical) = remap_physical_schema(
                &logical_file_schema,
                &physical_file_schema,
                self.parquet_options.case_sensitive,
                self.parquet_options.use_field_id,
                self.parquet_options.ignore_missing_field_id,
            )?;
            // Keep the original physical schema (and its folded names) for per-column duplicate
            // detection. Only meaningful in case-insensitive mode (matches existing behavior).
            let (original_physical_schema, original_physical_folded) =
                if !self.parquet_options.case_sensitive {
                    (
                        Some(Arc::clone(&physical_file_schema)),
                        Some(fold_schema_names(&physical_file_schema, false)),
                    )
                } else {
                    (None, None)
                };
            (
                remapped,
                if logical_to_physical.is_empty() {
                    None
                } else {
                    Some(logical_to_physical)
                },
                original_physical_schema,
                original_physical_folded,
            )
        } else {
            (Arc::clone(&physical_file_schema), None, None, None)
        };

        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&adapted_physical_schema),
        )?;

        // Fold both stored schemas once here so the per-column rewrite paths reuse them instead
        // of re-folding on every `rewrite` call. Case-sensitive mode folds to identity.
        let case_sensitive = self.parquet_options.case_sensitive;
        let logical_folded = fold_schema_names(&logical_file_schema, case_sensitive);
        let physical_folded = fold_schema_names(&adapted_physical_schema, case_sensitive);

        Ok(Arc::new(SparkPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema: adapted_physical_schema,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
            default_adapter,
            logical_to_physical_names,
            original_physical_schema,
            original_physical_folded,
            logical_folded,
            physical_folded,
        }))
    }
}

/// Spark-compatible physical expression adapter.
///
/// This adapter rewrites expressions at planning time to:
/// 1. Replace references to missing columns with default values or nulls
/// 2. Replace standard DataFusion cast expressions with Spark-compatible casts
/// 3. Handle case-insensitive column matching
#[derive(Debug)]
struct SparkPhysicalExprAdapter {
    /// The logical schema expected by the query
    logical_file_schema: SchemaRef,
    /// The physical schema of the actual file being read
    physical_file_schema: SchemaRef,
    /// Spark-specific options for type conversions
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns (keyed by Column)
    default_values: Option<HashMap<Column, ScalarValue>>,
    /// The default DataFusion adapter to delegate standard handling to
    default_adapter: Arc<dyn PhysicalExprAdapter>,
    /// Mapping from logical column names to original physical column names,
    /// used for case-insensitive mode where names differ in casing.
    /// After the default adapter rewrites expressions using the remapped
    /// physical schema (with logical names), we need to restore the original
    /// physical names so that downstream reassign_expr_columns can find
    /// columns in the actual stream schema.
    logical_to_physical_names: Option<HashMap<String, String>>,
    /// The original (un-remapped) physical schema, kept for per-column duplicate
    /// detection in case-insensitive mode. Only set when `!case_sensitive`.
    original_physical_schema: Option<SchemaRef>,
    /// Original physical field names pre-folded for case-insensitive matching, parallel to
    /// `original_physical_schema.fields()`. Set together with `original_physical_schema` so the
    /// per-column duplicate check folds only the queried name (not the whole schema) per call.
    original_physical_folded: Option<Vec<String>>,
    /// `logical_file_schema` field names pre-folded once (see `fold_names`), parallel to
    /// `logical_file_schema.fields()`. Lets the per-column rewrite fallbacks match by folded name
    /// without re-folding the schema on every `rewrite` call.
    logical_folded: Vec<String>,
    /// `physical_file_schema` field names pre-folded once, parallel to
    /// `physical_file_schema.fields()`. See `logical_folded`.
    physical_folded: Vec<String>,
}

impl PhysicalExprAdapter for SparkPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        // In case-insensitive mode, check if any Column in this expression references
        // a field with multiple case-insensitive matches in the physical schema.
        // Only the columns actually referenced trigger the error (not the whole schema).
        if let Some(orig_physical) = &self.original_physical_schema {
            let orig_folded = self.original_physical_folded.as_deref().unwrap_or(&[]);
            // Collect referenced column names, then fold them in one JVM crossing rather than one
            // per Column node. Physical names were already folded once in `create()`.
            let mut col_names: Vec<String> = Vec::new();
            let _ = Arc::<dyn PhysicalExpr>::clone(&expr).transform(|e| {
                if let Some(col) = e.downcast_ref::<Column>() {
                    col_names.push(col.name().to_string());
                }
                Ok(Transformed::no(e))
            });
            let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
            let col_folded = fold_names(&col_refs, false);
            for (name, folded) in col_names.iter().zip(&col_folded) {
                if let Some(err) = check_column_duplicate(name, folded, orig_physical, orig_folded)
                {
                    return Err(DataFusionError::External(Box::new(err)));
                }
            }
        }

        // First let the default adapter handle column remapping, missing columns,
        // and simple scalar type casts. Then replace DataFusion's CastColumnExpr
        // with Spark-compatible equivalents.
        //
        // The default adapter may fail for complex nested type casts (List, Map).
        // In that case, fall back to wrapping everything ourselves.
        let expr = self.replace_missing_with_defaults(expr)?;
        let expr = match self.default_adapter.rewrite(Arc::clone(&expr)) {
            Ok(rewritten) => {
                // Replace references to missing columns with default values
                // Replace DataFusion's CastColumnExpr with either:
                // - CometCastColumnExpr (for Struct/List/Map, uses spark_parquet_convert)
                // - Spark Cast (for simple scalar types)
                rewritten
                    .transform(|e| self.replace_with_spark_cast(e))
                    .data()?
            }
            Err(e) => {
                // Default adapter failed (likely complex nested type cast).
                // Handle all type mismatches ourselves using spark_parquet_convert.
                log::debug!("Default schema adapter error: {}", e);
                self.wrap_all_type_mismatches(expr)?
            }
        };

        // For case-insensitive mode: remap column names from logical back to
        // original physical names. The default adapter was given a remapped
        // physical schema (with logical names) so it could find columns. But
        // downstream code (reassign_expr_columns) looks up columns by name in
        // the actual parquet stream schema, which uses the original physical names.
        let expr = if let Some(name_map) = &self.logical_to_physical_names {
            expr.transform(|e| {
                if let Some(col) = e.downcast_ref::<Column>() {
                    if let Some(physical_name) = name_map.get(col.name()) {
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            physical_name,
                            col.index(),
                        ))));
                    }
                }
                Ok(Transformed::no(e))
            })
            .data()?
        } else {
            expr
        };

        Ok(expr)
    }
}

impl SparkPhysicalExprAdapter {
    /// Wrap ALL Column expressions that have type mismatches with CometCastColumnExpr.
    /// This is the fallback path when the default adapter fails (e.g., for complex
    /// nested type casts like List<Struct> or Map). Uses `spark_parquet_convert`
    /// under the hood for the actual type conversion.
    fn wrap_all_type_mismatches(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let case_sensitive = self.parquet_options.case_sensitive;
        // Both schemas were folded once in `create()`; reuse those instead of re-folding here.
        let logical_folded = &self.logical_folded;
        let physical_folded = &self.physical_folded;
        expr.transform(|e| {
            if let Some(column) = e.downcast_ref::<Column>() {
                let col_name = column.name();
                let col_folded = fold_name(col_name, case_sensitive);

                // Resolve fields by name because this is the fallback path
                // that runs on the original expression when the default
                // adapter fails. The original expression was built against
                // the required (pruned) schema, so column indices refer to
                // that schema — not the logical or physical file schemas.
                // DataFusion's DefaultPhysicalExprAdapter::resolve_physical_column
                // also resolves by name for the same reason.
                let logical_field = logical_folded
                    .iter()
                    .position(|f| f == &col_folded)
                    .and_then(|i| self.logical_file_schema.fields().get(i));

                // Remap the column index to the physical file schema so
                // downstream evaluation reads the correct column from the
                // parquet batch.
                let physical_index = physical_folded.iter().position(|f| f == &col_folded);
                let physical_field =
                    physical_index.and_then(|i| self.physical_file_schema.fields().get(i));

                if let (Some(logical_field), Some(physical_field), Some(phys_idx)) =
                    (logical_field, physical_field, physical_index)
                {
                    let remapped: Arc<dyn PhysicalExpr> = if column.index() != phys_idx {
                        Arc::new(Column::new(col_name, phys_idx))
                    } else {
                        Arc::clone(&e)
                    };

                    if logical_field.data_type() != physical_field.data_type() {
                        // Mirror the same string/binary -> non-string/binary rejection in
                        // `replace_with_spark_cast`; this branch is reached when the default
                        // adapter rejected the cast and we'd otherwise build a CometCastColumnExpr
                        // that can't actually convert (e.g. BINARY -> DECIMAL with no
                        // `DecimalLogicalTypeAnnotation`). See #4088 and #4351.
                        let physical_type = physical_field.data_type();
                        let target_type = logical_field.data_type();
                        if is_string_or_binary(physical_type) && !is_string_or_binary(target_type) {
                            return Err(parquet_schema_convert_err(
                                physical_field.name(),
                                physical_type,
                                target_type,
                            ));
                        }

                        let cast_expr: Arc<dyn PhysicalExpr> = Arc::new(
                            CometCastColumnExpr::new(
                                remapped,
                                Arc::clone(physical_field),
                                Arc::clone(logical_field),
                                None,
                            )
                            .with_parquet_options(self.parquet_options.clone()),
                        );
                        return Ok(Transformed::yes(cast_expr));
                    } else if column.index() != phys_idx {
                        return Ok(Transformed::yes(remapped));
                    }
                }
            }
            Ok(Transformed::no(e))
        })
        .data()
    }

    /// Replace CastExpr (DataFusion's cast) with Spark's Cast expression.
    fn replace_with_spark_cast(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Transformed<Arc<dyn PhysicalExpr>>> {
        // Check for CastExpr and replace with spark_expr::Cast
        if let Some(cast) = expr.downcast_ref::<datafusion::physical_expr::expressions::CastExpr>()
        {
            let child = Arc::clone(cast.expr());
            let target_type = cast.target_field().data_type();

            // Derive input field from the child Column expression and the physical schema.
            // DF main removed CastColumnExpr in favor of CastExpr, so we recover the input
            // field from the child Column rather than calling cast.input_field().
            let input_field = if let Some(col) = child.downcast_ref::<Column>() {
                Arc::new(self.physical_file_schema.field(col.index()).clone())
            } else {
                // Fallback: synthesize a field from the target field name and child data type
                let child_type = cast.expr().data_type(&self.physical_file_schema)?;
                Arc::new(Field::new(cast.target_field().name(), child_type, true))
            };
            let physical_type = input_field.data_type();

            // Identity cast: DataFusion's default adapter inserts a CastExpr
            // whenever the logical and physical Arrow Fields differ in any
            // attribute (data type, nullability, or metadata), so with identical
            // data types but mismatched nullability or metadata, we receive a
            // no-op cast. Unwrapping is safe because Spark `Cast` with equal
            // source and target types is value-level identity (it does not
            // null-strip or enforce non-null), and field nullability/metadata is
            // informational rather than computational. Leaving the wrapper in
            // place blocks DataFusion's pruning-predicate analyzer from
            // recognizing the column reference, defeating row-group / page-index
            // stats pruning.
            if physical_type == target_type {
                return Ok(Transformed::yes(child));
            }

            // Reject reading a string/binary Parquet column as anything else. Spark's
            // `ParquetVectorUpdaterFactory.getUpdater` BINARY case allows StringType /
            // BinaryType, or DecimalType only when the column carries a
            // `DecimalLogicalTypeAnnotation` (which arrow-rs surfaces as `Decimal128`,
            // not `Binary`). Without this guard, runtime cast paths silently return
            // nulls, parse strings, or surface as a generic Arrow type-mismatch error.
            // See #4088 and #4351.
            if is_string_or_binary(physical_type) && !is_string_or_binary(target_type) {
                return Err(parquet_schema_convert_err(
                    input_field.name(),
                    physical_type,
                    target_type,
                ));
            }

            // Reject reading a primitive numeric Parquet column as StringType /
            // BinaryType. Spark has no `int -> string` etc. updater. Defer to
            // runtime via `RejectOnNonEmpty` so empty Parquet files (SPARK-26709)
            // pass and the JVM shim translates to
            // `SchemaColumnConvertNotSupportedException`.
            let physical_is_primitive_numeric = matches!(
                physical_type,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
            );
            if physical_is_primitive_numeric && is_string_or_binary(target_type) {
                let rejection = reject_on_non_empty_expr(
                    child,
                    cast.target_field(),
                    input_field.name(),
                    physical_type,
                    target_type,
                );
                return Ok(Transformed::yes(rejection));
            }

            // Decimal-to-decimal narrowing. Spark's `isDecimalTypeMatched` (the
            // `DecimalLogicalTypeAnnotation` branch) allows the read only when
            //   `dst_scale >= src_scale` AND
            //   `dst_precision - dst_scale >= src_precision - src_scale`.
            // Either failure means silently dropping fractional digits or losing
            // integer-side magnitude. See #4089 and #4343.
            if let (DataType::Decimal128(src_p, src_s), DataType::Decimal128(dst_p, dst_s)) =
                (physical_type, target_type)
            {
                let src_int_precision = i32::from(*src_p) - i32::from(*src_s);
                let dst_int_precision = i32::from(*dst_p) - i32::from(*dst_s);
                if dst_s < src_s || dst_int_precision < src_int_precision {
                    return Err(parquet_schema_convert_err(
                        input_field.name(),
                        physical_type,
                        target_type,
                    ));
                }
            }

            // Integer-to-decimal narrowing. Spark's `canReadAsDecimal` requires
            // `precision - scale >= 10` for an INT32 source and `>= 20` for INT64.
            // Unconditional in all Spark versions, so reject at plan time. See #4344.
            let int_decimal_min_int_precision = match physical_type {
                DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(10i32),
                DataType::Int64 => Some(20i32),
                _ => None,
            };
            if let Some(min_int_precision) = int_decimal_min_int_precision {
                let dst_precision_scale = match target_type {
                    DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Some((*p, *s)),
                    _ => None,
                };
                if let Some((dst_p, dst_s)) = dst_precision_scale {
                    let dst_int_precision = i32::from(dst_p) - i32::from(dst_s);
                    if dst_int_precision < min_int_precision {
                        return Err(parquet_schema_convert_err(
                            input_field.name(),
                            physical_type,
                            target_type,
                        ));
                    }
                }
            }

            // Type promotion (widening). When `allow_type_promotion` is false,
            // reject the three widenings (INT32→INT64, FLOAT→DOUBLE, INT32→DOUBLE)
            // that Spark 3.x's vectorized reader rejects. The flag tracks Comet's
            // per-Spark-version constant in ShimCometConf. Deferred to runtime so
            // empty files (SPARK-26709) pass.
            if !self.parquet_options.allow_type_promotion {
                let is_disallowed_promotion = matches!(
                    (physical_type, target_type),
                    (DataType::Int32, DataType::Int64)
                        | (DataType::Float32, DataType::Float64)
                        | (DataType::Int32, DataType::Float64)
                );
                if is_disallowed_promotion {
                    let rejection = reject_on_non_empty_expr(
                        Arc::clone(&child),
                        cast.target_field(),
                        input_field.name(),
                        physical_type,
                        target_type,
                    );
                    return Ok(Transformed::yes(rejection));
                }
            }

            // Reject primitive Parquet conversions Spark's vectorized reader rejects
            // on every supported version (no matching branch in
            // `ParquetVectorUpdaterFactory.getUpdater`):
            //
            //   - `INT64 -> Int*` truncates lower bits.
            //   - `INT64 -> Float*` and `INT32 -> Float32` lose precision.
            //   - `Float* -> Int*` and `Float64 -> Float32` truncate / overflow.
            //   - `INT32 -> Timestamp` / `INT64 -> Date32` / `INT64 -> Timestamp`:
            //     date/timestamp-annotated columns surface as Date32 / Timestamp,
            //     so reaching this branch means the column was un-annotated.
            //   - `Date32 -> Timestamp(LTZ)`: Spark only allows Date -> TimestampNTZ.
            //   - `Timestamp -> Date32`: no Timestamp updater branches into Date.
            //
            // Deferred to runtime (SPARK-26709). See #4297.
            let is_spark_rejected_conversion = matches!(
                (physical_type, target_type),
                // Long -> narrower int.
                (
                    DataType::Int64,
                    DataType::Int8 | DataType::Int16 | DataType::Int32,
                )
                // Long -> floating point.
                | (DataType::Int64, DataType::Float32 | DataType::Float64)
                // Long -> date / timestamp (raw INT64; annotated columns surface as Date32/Timestamp).
                | (DataType::Int64, DataType::Date32)
                | (DataType::Int64, DataType::Timestamp(_, _))
                // Int -> float (DoubleType is allowed via IntegerToDoubleUpdater; FloatType is not).
                | (
                    DataType::Int8 | DataType::Int16 | DataType::Int32,
                    DataType::Float32,
                )
                // Int -> timestamp (raw INT32; DATE-annotated columns surface as Date32).
                | (
                    DataType::Int8 | DataType::Int16 | DataType::Int32,
                    DataType::Timestamp(_, _),
                )
                // Float -> int / Double -> int (no integer branches under FLOAT/DOUBLE).
                | (
                    DataType::Float32 | DataType::Float64,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
                )
                // Double -> float (narrowing).
                | (DataType::Float64, DataType::Float32)
                // Date -> Timestamp(LTZ). Spark allows Date -> TimestampNTZ only.
                | (DataType::Date32, DataType::Timestamp(_, Some(_)))
                // Timestamp -> Date.
                | (DataType::Timestamp(_, _), DataType::Date32)
            );
            if is_spark_rejected_conversion {
                let rejection = reject_on_non_empty_expr(
                    child,
                    cast.target_field(),
                    input_field.name(),
                    physical_type,
                    target_type,
                );
                return Ok(Transformed::yes(rejection));
            }

            // Spark 3.x refuses to read a Parquet TimestampLTZ column as
            // TimestampNTZ (SPARK-36182); Spark 4.0 (SPARK-47447) lifted that.
            // The flag tracks Comet's per-Spark-version constant in
            // ShimCometConf. Deferred to runtime so empty files (SPARK-26709)
            // still pass. See #4219.
            //
            // This catches all LTZ physical encodings: TIMESTAMP_MICROS /
            // TIMESTAMP_MILLIS arrive as `Timestamp(_, Some(_))` directly, and
            // INT96 arrives as `Timestamp(_, Some("UTC"))` because `coerce_int96_tz`
            // attaches the UTC timezone (see `get_options`) instead of letting
            // `coerce_int96` strip it to a timezone-free `Timestamp(_, None)`.
            if !self.parquet_options.allow_timestamp_ltz_to_ntz
                && matches!(
                    (physical_type, target_type),
                    (
                        DataType::Timestamp(_, Some(_)),
                        DataType::Timestamp(_, None)
                    )
                )
            {
                let rejection = reject_on_non_empty_expr(
                    Arc::clone(&child),
                    cast.target_field(),
                    input_field.name(),
                    physical_type,
                    target_type,
                );
                return Ok(Transformed::yes(rejection));
            }

            // Scalar/complex mismatch (e.g. TIMESTAMP read as ARRAY<TIMESTAMP>):
            // Spark's vectorized reader rejects with
            // SchemaColumnConvertNotSupportedException (SPARK-45604). Same-shape
            // complex pairs and timestamp→timestamp / timestamp→int64 fall through
            // to CometCastColumnExpr below.
            let is_complex = |t: &DataType| {
                matches!(
                    t,
                    DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
                )
            };
            if is_complex(physical_type) != is_complex(target_type) {
                return Err(parquet_schema_convert_err(
                    input_field.name(),
                    physical_type,
                    target_type,
                ));
            }

            // Same-shape complex casts, timestamp tz relabel (e.g. Timestamp(us, None)
            // -> Timestamp(us, Some("UTC")) for INT96 reads), and Timestamp -> Int64
            // (Spark's `nanosAsLong`) need spark_parquet_convert: it handles nested
            // field selection, metadata-only tz changes, and raw-value reinterpretation
            // that Spark's Cast would otherwise convert incorrectly.
            if matches!(
                (physical_type, target_type),
                (DataType::Struct(_), DataType::Struct(_))
                    | (DataType::List(_), DataType::List(_))
                    | (DataType::Map(_, _), DataType::Map(_, _))
                    | (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                    | (DataType::Timestamp(_, _), DataType::Int64)
            ) {
                let comet_cast: Arc<dyn PhysicalExpr> = Arc::new(
                    CometCastColumnExpr::new(
                        child,
                        input_field,
                        Arc::clone(cast.target_field()),
                        None,
                    )
                    .with_parquet_options(self.parquet_options.clone()),
                );
                return Ok(Transformed::yes(comet_cast));
            }

            // For simple scalar type casts, use Spark-compatible Cast expression
            let mut cast_options = SparkCastOptions::new(
                self.parquet_options.eval_mode,
                &self.parquet_options.timezone,
                self.parquet_options.allow_incompat,
            );
            cast_options.allow_cast_unsigned_ints = self.parquet_options.allow_cast_unsigned_ints;
            cast_options.is_adapting_schema = true;

            let spark_cast = Arc::new(Cast::new(
                child,
                target_type.clone(),
                cast_options,
                None,
                None,
            ));

            return Ok(Transformed::yes(spark_cast as Arc<dyn PhysicalExpr>));
        }

        Ok(Transformed::no(expr))
    }

    /// Replace references to missing columns with default values.
    fn replace_missing_with_defaults(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let Some(defaults) = &self.default_values else {
            return Ok(expr);
        };

        if defaults.is_empty() {
            return Ok(expr);
        }

        // Build owned (column_name, default_value) pairs for columns missing from the physical file.
        // For each default: filter to only columns absent from physical schema, then type-cast
        // the value to match the logical schema's field type if they differ (using Spark cast semantics).
        let case_sensitive = self.parquet_options.case_sensitive;
        // Physical schema names were folded once in `create()`; reuse them here.
        let physical_folded = &self.physical_folded;
        let missing_column_defaults: Vec<(String, ScalarValue)> = defaults
            .iter()
            .filter_map(|(col, val)| {
                let col_name = col.name();
                let col_folded = fold_name(col_name, case_sensitive);

                // Only include defaults for columns missing from the physical file schema
                let is_missing = !physical_folded.iter().any(|f| f == &col_folded);

                if !is_missing {
                    return None;
                }

                // Cast value to logical schema type if needed (only if types differ)
                let value = self
                    .logical_file_schema
                    .field_with_name(col_name)
                    .ok()
                    .filter(|field| val.data_type() != *field.data_type())
                    .and_then(|field| {
                        spark_parquet_convert(
                            ColumnarValue::Scalar(val.clone()),
                            field.data_type(),
                            &self.parquet_options,
                        )
                        .ok()
                        .and_then(|cv| match cv {
                            ColumnarValue::Scalar(s) => Some(s),
                            _ => None,
                        })
                    })
                    .unwrap_or_else(|| val.clone());

                Some((col_name.to_string(), value))
            })
            .collect();

        let name_based: HashMap<&str, &ScalarValue> = missing_column_defaults
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();

        if name_based.is_empty() {
            return Ok(expr);
        }

        replace_columns_with_literals(expr, &name_based)
    }
}

/// Defers a Parquet type-promotion rejection to runtime: returns an empty array
/// when the input batch has no rows, and raises `ParquetSchemaConvert` otherwise.
///
/// Mirrors Spark's vectorized reader, which only invokes
/// `ParquetVectorUpdaterFactory.getUpdater` while decoding a row group. A
/// Parquet file with no row groups (e.g. one written from an empty DataFrame)
/// never triggers the per-row-group check, so a partition mixing such a file
/// with another whose schema would otherwise fail the type-promotion check
/// (SPARK-26709) is still readable.
#[derive(Debug, Eq)]
struct RejectOnNonEmpty {
    child: Arc<dyn PhysicalExpr>,
    target_field: FieldRef,
    column: String,
    physical_type: String,
    spark_type: String,
}

impl PartialEq for RejectOnNonEmpty {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.target_field.eq(&other.target_field)
            && self.column == other.column
            && self.physical_type == other.physical_type
            && self.spark_type == other.spark_type
    }
}

impl Hash for RejectOnNonEmpty {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.target_field.hash(state);
        self.column.hash(state);
        self.physical_type.hash(state);
        self.spark_type.hash(state);
    }
}

impl Display for RejectOnNonEmpty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "REJECT_PARQUET_TYPE_PROMOTION({} AS {})",
            self.column, self.spark_type
        )
    }
}

impl PhysicalExpr for RejectOnNonEmpty {
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        if batch.num_rows() == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                self.target_field.data_type(),
            )));
        }
        Err(DataFusionError::External(Box::new(
            SparkError::ParquetSchemaConvert {
                file_path: String::new(),
                column: self.column.clone(),
                physical_type: self.physical_type.clone(),
                spark_type: self.spark_type.clone(),
            },
        )))
    }

    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(RejectOnNonEmpty {
            child: children.pop().expect("child"),
            target_field: Arc::clone(&self.target_field),
            column: self.column.clone(),
            physical_type: self.physical_type.clone(),
            spark_type: self.spark_type.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod test {
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
    use arrow::array::UInt32Array;
    use arrow::array::{
        BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_comet_spark_expr::test_common::file_util::get_temp_filename;
    use datafusion_comet_spark_expr::EvalMode;
    use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    /// Reading a non-BINARY Parquet column as `StringType` must raise the same
    /// `_LEGACY_ERROR_TEMP_2063`-shaped error as Spark's vectorized reader
    /// (`ParquetVectorUpdaterFactory.getUpdater` has no INT32 -> string updater).
    #[tokio::test]
    async fn parquet_int_read_as_string_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Utf8,
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: string")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Companion: BINARY (string physical) read as IntegerType must raise the
    /// same Spark-compatible error.
    #[tokio::test]
    async fn parquet_string_read_as_int_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(StringArray::from(vec!["bcd", "efg"])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Utf8, false),
            values,
            DataType::Int32,
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: BINARY"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Reading a plain BINARY Parquet column (no `DecimalLogicalTypeAnnotation`)
    /// as `DecimalType` must raise a Spark-compatible `ParquetSchemaConvert`
    /// error. Spark's `canReadAsDecimal` / `canReadAsBinaryDecimal` both require
    /// the column to carry a `DecimalLogicalTypeAnnotation`. See #4351.
    #[tokio::test]
    async fn parquet_binary_read_as_decimal_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(BinaryArray::from_vec(vec![b"1.2", b"3.4"])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Binary, false),
            values,
            DataType::Decimal128(37, 1),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal(37,1)")
                && msg.contains("Found: BINARY"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// INT32 -> Decimal where `precision - scale < 10` (the minimum that can
    /// represent the full INT32 range). See #4344.
    #[tokio::test]
    async fn parquet_int32_read_as_narrow_decimal_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Decimal128(9, 0),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// INT32 -> Decimal where `precision - scale >= 10` is allowed.
    #[tokio::test]
    async fn parquet_int32_read_as_wide_decimal_succeeds() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(10, 0),
            false,
        )]));
        let _ = roundtrip(&batch, required_schema).await?;
        Ok(())
    }

    /// INT64 -> Decimal where `precision - scale < 20`. See #4344.
    #[tokio::test]
    async fn parquet_int64_read_as_narrow_decimal_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int64Array::from(vec![1i64, 2, 3])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Decimal128(19, 0),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Non-zero scale that pushes `precision - scale` below the integer minimum
    /// (INT32 -> Decimal(10, 1) leaves int-precision 9).
    #[tokio::test]
    async fn parquet_int32_read_as_decimal_with_scale_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Decimal128(10, 1),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Helper to build a tiny decimal Parquet batch for the decimal-to-decimal tests.
    fn decimal_batch(precision: u8, scale: i8) -> Result<RecordBatch, DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(precision, scale),
            false,
        )]));
        let values = Arc::new(
            Decimal128Array::from(vec![123i128, 456])
                .with_precision_and_scale(precision, scale)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        ) as Arc<dyn arrow::array::Array>;
        Ok(RecordBatch::try_new(file_schema, vec![values])?)
    }

    /// Reading Decimal(P, S) as Decimal(P', S) where P' < P (precision-only
    /// narrowing, equal scale) must raise the Spark-compatible error. Spark's
    /// `isDecimalTypeMatched` rejects this because `precisionIncrease < 0`
    /// while `scaleIncrease == 0`. See #4343.
    #[tokio::test]
    async fn parquet_decimal_precision_narrowing_errors() -> Result<(), DataFusionError> {
        let batch = decimal_batch(10, 2)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(5, 2),
            false,
        )]));

        let err = roundtrip(&batch, required_schema)
            .await
            .expect_err("expected ParquetSchemaConvert for Decimal(10, 2) -> Decimal(5, 2)");
        let msg = err.to_string();
        assert!(
            msg.contains("Column: [[a]]") && msg.contains("Expected: decimal(5,2)"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Reading Decimal(P, S) as Decimal(P', S') where the integer-precision
    /// `P - S` shrinks must raise the Spark-compatible error. Example:
    /// Decimal(10, 4) (int-precision 6) -> Decimal(5, 2) (int-precision 3).
    /// See #4343.
    #[tokio::test]
    async fn parquet_decimal_int_precision_narrowing_errors() -> Result<(), DataFusionError> {
        let batch = decimal_batch(10, 4)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(5, 2),
            false,
        )]));

        let err = roundtrip(&batch, required_schema)
            .await
            .expect_err("expected ParquetSchemaConvert for Decimal(10, 4) -> Decimal(5, 2)");
        let msg = err.to_string();
        assert!(msg.contains("Column: [[a]]"), "unexpected error: {msg}");
        Ok(())
    }

    /// Reading Decimal(P, S) as Decimal(P, S') where S' > S but `P - S` did
    /// not grow means the cast would shift integer digits into the fractional
    /// part and lose the most-significant digit. Example: Decimal(5, 2) ->
    /// Decimal(5, 3): scaleIncrease=1, precisionIncrease=0. See #4343.
    #[tokio::test]
    async fn parquet_decimal_scale_widening_without_precision_errors() -> Result<(), DataFusionError>
    {
        let batch = decimal_batch(5, 2)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(5, 3),
            false,
        )]));

        let err = roundtrip(&batch, required_schema)
            .await
            .expect_err("expected ParquetSchemaConvert for Decimal(5, 2) -> Decimal(5, 3)");
        let msg = err.to_string();
        assert!(msg.contains("Column: [[a]]"), "unexpected error: {msg}");
        Ok(())
    }

    /// Sanity check: widening both precision and scale by the same amount is
    /// allowed (the cast is lossless). Decimal(5, 2) -> Decimal(7, 4) gives
    /// scaleIncrease=2, precisionIncrease=2, so `precisionIncrease >= scaleIncrease`.
    #[tokio::test]
    async fn parquet_decimal_widening_succeeds() -> Result<(), DataFusionError> {
        let batch = decimal_batch(5, 2)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(7, 4),
            false,
        )]));

        let _ = roundtrip(&batch, required_schema).await?;
        Ok(())
    }

    /// Helper for the #4297 rejection tests: write a 1-row batch and assert
    /// that reading it under `read_type` raises `ParquetSchemaConvert`.
    async fn assert_rejected_conversion(
        file_field: Field,
        values: Arc<dyn arrow::array::Array>,
        read_type: DataType,
    ) -> Result<String, DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![file_field]));
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let read_field_name = file_schema.field(0).name();
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            read_field_name,
            read_type,
            false,
        )]));
        let err = roundtrip(&batch, required_schema)
            .await
            .expect_err("expected ParquetSchemaConvert");
        Ok(err.to_string())
    }

    /// `INT64 -> INT32` truncates to the lower 32 bits in DataFusion's cast.
    /// Spark's vectorized reader rejects this. See #4297.
    #[tokio::test]
    async fn parquet_long_read_as_int_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(Int64Array::from(vec![1i64, 1 << 33])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Int32,
        )
        .await?;
        assert!(
            msg.contains("Found: INT64") && msg.contains("Expected: int"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `INT64 -> Float64` loses precision for large values; Spark rejects.
    #[tokio::test]
    async fn parquet_long_read_as_double_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int64Array::from(vec![1i64, (1i64 << 54) + 1]))
            as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Float64,
        )
        .await?;
        assert!(
            msg.contains("Found: INT64") && msg.contains("Expected: double"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Float64 -> Float32` overflows / loses precision; Spark rejects.
    #[tokio::test]
    async fn parquet_double_read_as_float_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(Float64Array::from(vec![1.5_f64, 1e40])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Float64, false),
            values,
            DataType::Float32,
        )
        .await?;
        assert!(
            msg.contains("Found: DOUBLE") && msg.contains("Expected: float"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Float32 -> Int64` truncates the fractional part; Spark rejects.
    #[tokio::test]
    async fn parquet_float_read_as_long_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(Float32Array::from(vec![1.5_f32, 2.5])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Float32, false),
            values,
            DataType::Int64,
        )
        .await?;
        assert!(
            msg.contains("Found: FLOAT") && msg.contains("Expected: bigint"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Float64 -> Int64` similarly.
    #[tokio::test]
    async fn parquet_double_read_as_long_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(Float64Array::from(vec![1.5_f64, 2.5])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Float64, false),
            values,
            DataType::Int64,
        )
        .await?;
        assert!(
            msg.contains("Found: DOUBLE") && msg.contains("Expected: bigint"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Int32 -> Float32` loses precision for values past `2^24`. Spark
    /// allows `Int32 -> Float64` but rejects `Int32 -> Float32`.
    #[tokio::test]
    async fn parquet_int_read_as_float_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(Int32Array::from(vec![1, (1 << 25) + 1])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Float32,
        )
        .await?;
        assert!(
            msg.contains("Found: INT32") && msg.contains("Expected: float"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Int32 -> Timestamp(_, None)`: raw INT32 reinterpreted as epoch seconds
    /// produces dates near the Unix epoch. Only DATE-annotated INT32 columns
    /// (which surface as `Date32`) are allowed to read as `TimestampNTZ`.
    #[tokio::test]
    async fn parquet_int_read_as_timestamp_ntz_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        .await?;
        assert!(
            msg.contains("Found: INT32") && msg.contains("Expected: timestamp"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Int64 -> Date32` similarly: raw INT64 (no DATE annotation, otherwise
    /// the file would surface as `Date32`).
    #[tokio::test]
    async fn parquet_long_read_as_date_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(Int64Array::from(vec![1i64, 2])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Date32,
        )
        .await?;
        assert!(
            msg.contains("Found: INT64") && msg.contains("Expected: date"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Date32 -> Timestamp(_, Some(_))` (LTZ). Spark's vectorized reader
    /// allows `Date -> TimestampNTZ` but not `Date -> Timestamp(LTZ)`.
    #[tokio::test]
    async fn parquet_date_read_as_ltz_timestamp_errors() -> Result<(), DataFusionError> {
        let values =
            Arc::new(Date32Array::from(vec![18262, 18263])) as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Date32, false),
            values,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        )
        .await?;
        assert!(
            msg.contains("Found: INT32") && msg.contains("Expected: timestamp"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Timestamp(_, _) -> Date32`: no Timestamp updater branches into
    /// `DateType`, so Spark rejects.
    #[tokio::test]
    async fn parquet_timestamp_read_as_date_errors() -> Result<(), DataFusionError> {
        let values = Arc::new(TimestampMicrosecondArray::from(vec![0i64, 1_000_000]))
            as Arc<dyn arrow::array::Array>;
        let msg = assert_rejected_conversion(
            Field::new(
                "a",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            values,
            DataType::Date32,
        )
        .await?;
        assert!(msg.contains("Expected: date"), "unexpected error: {msg}");
        Ok(())
    }

    /// SPARK-26709: an empty Parquet file with a column that would otherwise fail
    /// the type-promotion check (INT32 read as INT64 when allow_type_promotion is
    /// false) must still be readable. Spark's vectorized reader only enforces the
    /// check per row group, so a file with no row groups passes silently. The
    /// adapter's plan-time rejection must not fire for the empty-file case.
    #[tokio::test]
    async fn parquet_empty_file_disallowed_widening() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename)?;
        let writer = ArrowWriter::try_new(file, Arc::clone(&file_schema), None)?;
        writer.close()?;

        let required_schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));

        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.allow_type_promotion = false;

        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
            SparkPhysicalExprAdapterFactory::new(spark_parquet_options, None),
        );

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![PartitionedFile::from_path(filename)?]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_file_groups(vec![files])
                .with_expr_adapter(Some(expr_adapter_factory))
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            assert_eq!(batch.num_rows(), 0);
        }
        Ok(())
    }

    /// Companion to `parquet_empty_file_disallowed_widening`: a file with rows
    /// must still raise `ParquetSchemaConvert` when the same widening is
    /// rejected. Verifies the runtime check fires on non-empty input,
    /// matching Spark's per-row-group behavior.
    #[tokio::test]
    async fn parquet_non_empty_file_disallowed_widening_errors() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;

        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename)?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&file_schema), None)?;
        writer.write(&batch)?;
        writer.close()?;

        let required_schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));

        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.allow_type_promotion = false;

        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
            SparkPhysicalExprAdapterFactory::new(spark_parquet_options, None),
        );

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![PartitionedFile::from_path(filename)?]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_file_groups(vec![files])
                .with_expr_adapter(Some(expr_adapter_factory))
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        let first = stream.next().await.unwrap();
        let err = first.expect_err("expected ParquetSchemaConvert error on non-empty file");
        let msg = err.to_string();
        // The JVM shim sees the inner "[col]" via the JSON `column` field, matching
        // Spark's `Arrays.toString(descriptor.getPath())` format. The Rust display
        // wraps with another `[...]` from the error template.
        assert!(
            msg.contains("Column: [[col]]")
                && msg.contains("Expected: bigint")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn parquet_roundtrip_unsigned_int() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));

        let ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids])?;

        let required_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let _ = roundtrip(&batch, required_schema).await?;

        Ok(())
    }

    /// Create a Parquet file containing a single batch and then read the batch back using
    /// the specified required_schema. This will cause the PhysicalExprAdapter code to be used.
    async fn roundtrip(
        batch: &RecordBatch,
        required_schema: SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename)?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None)?;
        writer.write(batch)?;
        writer.close()?;

        let object_store_url = ObjectStoreUrl::local_filesystem();

        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.allow_cast_unsigned_ints = true;

        // Create expression adapter factory for Spark-compatible schema adaptation
        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
            SparkPhysicalExprAdapterFactory::new(spark_parquet_options, None),
        );

        let parquet_source = ParquetSource::new(required_schema);

        let files = FileGroup::new(vec![PartitionedFile::from_path(filename.to_string())?]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_file_groups(vec![files])
                .with_expr_adapter(Some(expr_adapter_factory))
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));

        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        stream.next().await.unwrap()
    }

    #[tokio::test]
    async fn parquet_duplicate_fields_case_insensitive() {
        // Parquet file has columns "A", "B", "b" - reading "b" in case-insensitive mode
        // should fail with duplicate field error matching Spark's _LEGACY_ERROR_TEMP_2093
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("A", DataType::Int32, false),
            Field::new("B", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let col_b1 = Arc::new(Int32Array::from(vec![4, 5, 6])) as Arc<dyn arrow::array::Array>;
        let col_b2 = Arc::new(Int32Array::from(vec![7, 8, 9])) as Arc<dyn arrow::array::Array>;
        let batch =
            RecordBatch::try_new(Arc::clone(&file_schema), vec![col_a, col_b1, col_b2]).unwrap();

        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read with case-insensitive mode, requesting column "b" which matches both "B" and "b"
        let required_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));

        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.case_sensitive = false;

        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
            SparkPhysicalExprAdapterFactory::new(spark_parquet_options, None),
        );

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![
            PartitionedFile::from_path(filename.to_string()).unwrap()
        ]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_file_groups(vec![files])
                .with_expr_adapter(Some(expr_adapter_factory))
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        let result = stream.next().await.unwrap();

        // Should fail with duplicate field error
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Found duplicate field"),
            "Expected duplicate field error, got: {err_msg}"
        );
    }

    /// Crate-level check of the case-insensitive remap. Under `cargo test` there is no attached
    /// JVM, so `fold_names` uses the ASCII fallback; ASCII casing still distinguishes match from
    /// no-match, so this documents that `remap_physical_schema` renames the physical field to the
    /// logical name and records the reverse mapping.
    #[test]
    fn remap_physical_schema_case_insensitive_renames_to_logical() {
        let logical = Arc::new(Schema::new(vec![Field::new("Name", DataType::Int32, true)]));
        let physical = Arc::new(Schema::new(vec![Field::new("NAME", DataType::Int32, true)]));
        let (remapped, name_map) =
            super::remap_physical_schema(&logical, &physical, false, false, false).unwrap();
        assert_eq!(remapped.field(0).name(), "Name");
        assert_eq!(name_map.get("Name").map(String::as_str), Some("NAME"));
    }

    /// Field-id remap: an ID-bearing logical field whose id is absent from the file must not
    /// name-match a physical field that folds to the same name. The physical field is fake-renamed
    /// (Spark's `generateFakeColumnName`), using the folded name for the collision check.
    #[test]
    fn remap_field_id_missing_fake_renames_colliding_physical() {
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let id_meta = |id: &str| {
            std::collections::HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])
        };
        // Logical `foo` carries field id 99; the file has no ids but a physical `FOO` whose folded
        // name collides with `foo`.
        let logical = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Int32, true).with_metadata(id_meta("99"))
        ]));
        let physical = Arc::new(Schema::new(vec![Field::new("FOO", DataType::Int32, true)]));
        let (remapped, _name_map) =
            super::remap_physical_schema(&logical, &physical, false, true, true).unwrap();
        assert!(
            remapped
                .field(0)
                .name()
                .starts_with("__comet_unmatched_field_id"),
            "expected fake rename, got {}",
            remapped.field(0).name()
        );
    }

    /// The process-wide fold cache populates on first fold and serves repeated lookups. Runs under
    /// the ASCII fallback (no JVM in `cargo test`), which exercises the cacheable path.
    #[test]
    fn fold_names_memoizes_case_insensitive() {
        // Unique name so parallel tests don't share this cache entry.
        let name = "FoldMemoUnique";
        let first = super::fold_names(&[name], false);
        assert_eq!(first, vec!["foldmemounique".to_string()]);
        assert_eq!(
            super::fold_cache()
                .read()
                .unwrap()
                .get(name)
                .map(String::as_str),
            Some("foldmemounique")
        );
        // Second call is served from the cache with the same result.
        assert_eq!(super::fold_names(&[name], false), first);
    }

    /// Case-sensitive folding is identity and must never populate the cache.
    #[test]
    fn fold_names_case_sensitive_is_identity_and_uncached() {
        let name = "FoldCaseSensitiveUnique";
        assert_eq!(super::fold_names(&[name], true), vec![name.to_string()]);
        assert!(super::fold_cache().read().unwrap().get(name).is_none());
    }
}
