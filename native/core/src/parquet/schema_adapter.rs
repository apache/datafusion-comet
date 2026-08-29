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

use crate::parquet::cast_column::CometCastColumnExpr;
use crate::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    in_list, BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion_comet_common::SparkError;
use datafusion_comet_spark_expr::{Cast, SparkCastOptions};
use datafusion_physical_expr_adapter::{
    replace_columns_with_literals, DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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

    // Names of ID-bearing logical fields whose ID is not present in the file. Any physical
    // field that shares one of these names must be renamed to something the
    // `DefaultPhysicalExprAdapter` cannot name-match, otherwise the read would silently fall
    // through to a name match. Spark's `matchIdField` solves the same problem with
    // `generateFakeColumnName` (see `ParquetReadSupport.scala`).
    let unmatched_id_logical_names: std::collections::HashSet<String> = if should_match_by_id {
        logical_schema
            .fields()
            .iter()
            .filter_map(|lf| {
                parse_field_id(lf).and_then(|id| {
                    if id_to_phys_names.contains_key(&id) {
                        None
                    } else {
                        Some(lf.name().clone())
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
        .map(|field| {
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
                && unmatched_id_logical_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(field.name()))
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
                let logical_field = logical_schema.fields().iter().find(|lf| {
                    let lf_has_id = should_match_by_id && parse_field_id(lf).is_some();
                    !lf_has_id && lf.name().eq_ignore_ascii_case(field.name())
                });
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

/// Check if a specific column name has duplicate matches in the physical schema
/// (case-insensitive). Returns the error info if so.
fn check_column_duplicate(col_name: &str, physical_schema: &SchemaRef) -> Option<(String, String)> {
    let matches: Vec<&str> = physical_schema
        .fields()
        .iter()
        .filter(|pf| pf.name().eq_ignore_ascii_case(col_name))
        .map(|pf| pf.name().as_str())
        .collect();
    if matches.len() > 1 {
        // Include brackets to match the format expected by ShimSparkErrorConverter
        Some((col_name.to_string(), format!("[{}]", matches.join(", "))))
    } else {
        None
    }
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
        let (adapted_physical_schema, logical_to_physical_names, original_physical_schema) =
            if needs_remap {
                let (remapped, logical_to_physical) = remap_physical_schema(
                    &logical_file_schema,
                    &physical_file_schema,
                    self.parquet_options.case_sensitive,
                    self.parquet_options.use_field_id,
                    self.parquet_options.ignore_missing_field_id,
                )?;
                (
                    remapped,
                    if logical_to_physical.is_empty() {
                        None
                    } else {
                        Some(logical_to_physical)
                    },
                    // Keep original physical schema for per-column duplicate detection.
                    // Only meaningful in case-insensitive mode (matches existing behavior).
                    if !self.parquet_options.case_sensitive {
                        Some(Arc::clone(&physical_file_schema))
                    } else {
                        None
                    },
                )
            } else {
                (Arc::clone(&physical_file_schema), None, None)
            };

        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&adapted_physical_schema),
        )?;

        Ok(Arc::new(SparkPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema: adapted_physical_schema,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
            default_adapter,
            logical_to_physical_names,
            original_physical_schema,
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
}

impl PhysicalExprAdapter for SparkPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        // In case-insensitive mode, check if any Column in this expression references
        // a field with multiple case-insensitive matches in the physical schema.
        // Only the columns actually referenced trigger the error (not the whole schema).
        if let Some(orig_physical) = &self.original_physical_schema {
            // Walk the expression tree to find Column references
            let mut duplicate_err: Option<DataFusionError> = None;
            let _ = Arc::<dyn PhysicalExpr>::clone(&expr).transform(|e| {
                if let Some(col) = e.downcast_ref::<Column>() {
                    if let Some((req, matched)) = check_column_duplicate(col.name(), orig_physical)
                    {
                        duplicate_err = Some(DataFusionError::External(Box::new(
                            SparkError::DuplicateFieldCaseInsensitive {
                                required_field_name: req,
                                matched_fields: matched,
                            },
                        )));
                    }
                }
                Ok(Transformed::no(e))
            });
            if let Some(err) = duplicate_err {
                return Err(err);
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

        // Rewrite predicate comparisons over a millisecond timestamp file column into
        // the millisecond domain so DataFusion can prune row groups from statistics and
        // the row filter never runs the checked millis->micros conversion on values the
        // filter would discard. Values that survive the filter still go through the
        // checked conversion when the scan output is adapted.
        let expr = expr
            .transform(|e| self.rewrite_millis_timestamp_comparison(e))
            .data()?;

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

/// The wrapped expression and the file column's timezone of a millis->micros cast.
type MillisCastParts = (Arc<dyn PhysicalExpr>, Option<Arc<str>>);

/// If `expr` is a checked millis->micros [`CometCastColumnExpr`], return its wrapped
/// expression and the file column's timezone.
fn as_millis_to_micros_cast(expr: &Arc<dyn PhysicalExpr>) -> Option<MillisCastParts> {
    let cast = expr.downcast_ref::<CometCastColumnExpr>()?;
    match (
        cast.input_physical_field().data_type(),
        cast.target_field().data_type(),
    ) {
        (
            DataType::Timestamp(TimeUnit::Millisecond, file_tz),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => Some((Arc::clone(cast.expr()), file_tz.clone())),
        _ => None,
    }
}

/// If `expr` is a microsecond timestamp literal (null or not), return its value.
fn as_micros_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<Option<i64>> {
    match expr.downcast_ref::<Literal>()?.value() {
        ScalarValue::TimestampMicrosecond(micros, _) => Some(*micros),
        _ => None,
    }
}

impl SparkPhysicalExprAdapter {
    /// Rewrite predicate expressions over a `TIMESTAMP_MILLIS` file column read as
    /// microseconds into the millisecond domain — comparisons (including null-safe
    /// `<=>`), `IN` lists, and null checks — mirroring Spark's `ParquetFilters`,
    /// which pushes timestamp predicates down in the file's physical unit:
    /// https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFilters.scala#L192-L196
    ///
    /// The wrapped `CometCastColumnExpr` is opaque to DataFusion's pruning-predicate
    /// analyzer, so leaving the conversion inside the predicate defeats row-group
    /// statistics pruning — and the checked conversion then raises overflow errors for
    /// values Spark never reads, because Spark prunes them from the millisecond
    /// statistics. Comparing raw millisecond values against a rescaled literal is exact
    /// (`m * 1000 OP lit` over the integers), never overflows, and DataFusion can prune
    /// with it. Values that survive the filter still go through the checked conversion
    /// when the scan output is adapted, so genuine reads of overflowing values keep
    /// failing like Spark's `millisToMicros`.
    fn rewrite_millis_timestamp_comparison(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Transformed<Arc<dyn PhysicalExpr>>> {
        // `ts IS NULL` / `ts IS NOT NULL`: the checked conversion preserves null-ness
        // (it errors rather than producing nulls), so test the raw column directly.
        if let Some(is_null) = expr.downcast_ref::<IsNullExpr>() {
            if let Some((inner, _)) = as_millis_to_micros_cast(is_null.arg()) {
                return Ok(Transformed::yes(Arc::new(IsNullExpr::new(inner))));
            }
        }
        if let Some(is_not_null) = expr.downcast_ref::<IsNotNullExpr>() {
            if let Some((inner, _)) = as_millis_to_micros_cast(is_not_null.arg()) {
                return Ok(Transformed::yes(Arc::new(IsNotNullExpr::new(inner))));
            }
        }

        // `ts IN (...)` / `ts NOT IN (...)`: rescale every microsecond literal in the
        // list. DataFusion's pruning predicate understands `InListExpr` (up to 20
        // elements), so keeping the raw column visible restores row-group pruning,
        // mirroring Spark's `ParquetFilters` handling of `In`.
        if let Some(in_list_expr) = expr.downcast_ref::<InListExpr>() {
            if let Some((inner, file_tz)) = as_millis_to_micros_cast(in_list_expr.expr()) {
                return self.rewrite_millis_in_list(&expr, in_list_expr, inner, file_tz);
            }
        }

        let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
            return Ok(Transformed::no(expr));
        };
        let matched = if let (Some(cast), Some(micros)) = (
            as_millis_to_micros_cast(binary.left()),
            as_micros_literal(binary.right()),
        ) {
            Some((cast, *binary.op(), micros))
        } else if let (Some(micros), Some(cast)) = (
            as_micros_literal(binary.left()),
            as_millis_to_micros_cast(binary.right()),
        ) {
            binary.op().swap().map(|op| (cast, op, micros))
        } else {
            None
        };
        let Some(((inner, file_tz), op, micros)) = matched else {
            return Ok(Transformed::no(expr));
        };

        let Some(micros) = micros else {
            // NULL literal. The ordinary comparisons return NULL for every row on both
            // sides of the rewrite, so rescaling the literal to a NULL millisecond
            // literal is exact. The null-safe comparisons test null-ness of the value,
            // which the checked conversion preserves, so probe the raw column.
            let rewritten: Arc<dyn PhysicalExpr> = match op {
                Operator::IsNotDistinctFrom => Arc::new(IsNullExpr::new(inner)),
                Operator::IsDistinctFrom => Arc::new(IsNotNullExpr::new(inner)),
                Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Eq
                | Operator::NotEq => Arc::new(BinaryExpr::new(
                    inner,
                    op,
                    Arc::new(Literal::new(ScalarValue::TimestampMillisecond(
                        None, file_tz,
                    ))),
                )),
                _ => return Ok(Transformed::no(expr)),
            };
            return Ok(Transformed::yes(rewritten));
        };

        // For a file value `m` (milliseconds) the logical value is exactly `m * 1000`
        // microseconds, so `m * 1000 OP L` rewrites to an exact comparison on `m`.
        let floor = micros.div_euclid(1_000);
        let ceil = floor + i64::from(micros.rem_euclid(1_000) != 0);
        let exact = micros % 1_000 == 0;
        let (op, millis) = match op {
            Operator::Lt => (Operator::Lt, ceil),
            Operator::LtEq => (Operator::LtEq, floor),
            Operator::Gt => (Operator::Gt, floor),
            Operator::GtEq => (Operator::GtEq, ceil),
            Operator::Eq if exact => (Operator::Eq, floor),
            Operator::NotEq if exact => (Operator::NotEq, floor),
            Operator::IsNotDistinctFrom if exact => (Operator::IsNotDistinctFrom, floor),
            Operator::IsDistinctFrom if exact => (Operator::IsDistinctFrom, floor),
            // A sub-millisecond literal can never equal `m * 1000`. `col < i64::MIN`
            // (resp. `col >= i64::MIN`) is false (resp. true) for every non-null value
            // and NULL for nulls, matching `=` / `!=` null semantics while remaining a
            // plain, prunable comparison.
            Operator::Eq => (Operator::Lt, i64::MIN),
            Operator::NotEq => (Operator::GtEq, i64::MIN),
            // The null-safe comparisons never return NULL, so an impossible literal
            // folds to a constant. DataFusion's pruning predicate evaluates boolean
            // literals, so `false` still prunes every row group.
            Operator::IsNotDistinctFrom => {
                return Ok(Transformed::yes(Arc::new(Literal::new(
                    ScalarValue::Boolean(Some(false)),
                ))));
            }
            Operator::IsDistinctFrom => {
                return Ok(Transformed::yes(Arc::new(Literal::new(
                    ScalarValue::Boolean(Some(true)),
                ))));
            }
            _ => return Ok(Transformed::no(expr)),
        };
        let literal = Arc::new(Literal::new(ScalarValue::TimestampMillisecond(
            Some(millis),
            file_tz,
        )));
        Ok(Transformed::yes(Arc::new(BinaryExpr::new(
            inner, op, literal,
        ))))
    }

    /// Rewrite `ts IN (...)` / `ts NOT IN (...)` over a millis->micros conversion into
    /// the millisecond domain. Divisible literals rescale exactly; sub-millisecond
    /// literals can never equal `m * 1000` and drop out of the list, which preserves
    /// IN's three-valued logic (a dropped element contributes `false` to the OR / `true`
    /// to the AND for every non-null probe, and the null-probe result stays NULL either
    /// way). A list emptied this way degenerates to the same always-false /
    /// always-true-with-null-semantics sentinels the `=` / `!=` rewrite uses.
    fn rewrite_millis_in_list(
        &self,
        original: &Arc<dyn PhysicalExpr>,
        in_list_expr: &InListExpr,
        inner: Arc<dyn PhysicalExpr>,
        file_tz: Option<Arc<str>>,
    ) -> DataFusionResult<Transformed<Arc<dyn PhysicalExpr>>> {
        let mut millis_list: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        for item in in_list_expr.list() {
            // Any non-literal or non-timestamp element: leave the expression alone.
            let Some(micros) = as_micros_literal(item) else {
                return Ok(Transformed::no(Arc::clone(original)));
            };
            match micros {
                None => millis_list.push(Arc::new(Literal::new(
                    ScalarValue::TimestampMillisecond(None, file_tz.clone()),
                ))),
                Some(v) if v % 1_000 == 0 => millis_list.push(Arc::new(Literal::new(
                    ScalarValue::TimestampMillisecond(Some(v / 1_000), file_tz.clone()),
                ))),
                Some(_) => {}
            }
        }
        let negated = in_list_expr.negated();
        if millis_list.is_empty() {
            let (op, sentinel) = if negated {
                (Operator::GtEq, i64::MIN)
            } else {
                (Operator::Lt, i64::MIN)
            };
            return Ok(Transformed::yes(Arc::new(BinaryExpr::new(
                inner,
                op,
                Arc::new(Literal::new(ScalarValue::TimestampMillisecond(
                    Some(sentinel),
                    file_tz,
                ))),
            ))));
        }
        let rewritten = in_list(
            inner,
            millis_list,
            &negated,
            self.physical_file_schema.as_ref(),
        )?;
        Ok(Transformed::yes(rewritten))
    }

    /// Wrap ALL Column expressions that have type mismatches with CometCastColumnExpr.
    /// This is the fallback path when the default adapter fails (e.g., for complex
    /// nested type casts like List<Struct> or Map). Uses `spark_parquet_convert`
    /// under the hood for the actual type conversion.
    fn wrap_all_type_mismatches(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            if let Some(column) = e.downcast_ref::<Column>() {
                let col_name = column.name();

                // Resolve fields by name because this is the fallback path
                // that runs on the original expression when the default
                // adapter fails. The original expression was built against
                // the required (pruned) schema, so column indices refer to
                // that schema — not the logical or physical file schemas.
                // DataFusion's DefaultPhysicalExprAdapter::resolve_physical_column
                // also resolves by name for the same reason.
                let logical_field = if self.parquet_options.case_sensitive {
                    self.logical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name() == col_name)
                } else {
                    self.logical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name().eq_ignore_ascii_case(col_name))
                };
                let physical_field = if self.parquet_options.case_sensitive {
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name() == col_name)
                } else {
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name().eq_ignore_ascii_case(col_name))
                };

                // Remap the column index to the physical file schema so
                // downstream evaluation reads the correct column from the
                // parquet batch.
                let physical_index = if self.parquet_options.case_sensitive {
                    self.physical_file_schema.index_of(col_name).ok()
                } else {
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(col_name))
                };

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
                            CometCastColumnExpr::try_new(
                                remapped,
                                Arc::clone(physical_field),
                                Arc::clone(logical_field),
                                None,
                            )?
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
                    CometCastColumnExpr::try_new(
                        child,
                        input_field,
                        Arc::clone(cast.target_field()),
                        None,
                    )?
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
        let missing_column_defaults: Vec<(String, ScalarValue)> = defaults
            .iter()
            .filter_map(|(col, val)| {
                let col_name = col.name();

                // Only include defaults for columns missing from the physical file schema
                let is_missing = if self.parquet_options.case_sensitive {
                    self.physical_file_schema.field_with_name(col_name).is_err()
                } else {
                    !self
                        .physical_file_schema
                        .fields()
                        .iter()
                        .any(|f| f.name().eq_ignore_ascii_case(col_name))
                };

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

    use arrow::datatypes::TimeUnit;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        in_list, BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
    };
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::scalar::ScalarValue;
    use datafusion_physical_expr_adapter::PhysicalExprAdapter;

    fn millis_file_adapter() -> Arc<dyn PhysicalExprAdapter> {
        let tz: Arc<str> = Arc::from("UTC");
        let logical = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(&tz))),
            true,
        )]));
        let physical = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some(tz)),
            true,
        )]));
        SparkPhysicalExprAdapterFactory::new(
            SparkParquetOptions::new(EvalMode::Legacy, "UTC", false),
            None,
        )
        .create(logical, physical)
        .unwrap()
    }

    fn micros_lit(value: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::TimestampMicrosecond(
            Some(value),
            Some(Arc::from("UTC")),
        )))
    }

    fn assert_millis_comparison(
        rewritten: &Arc<dyn PhysicalExpr>,
        expected_op: Operator,
        expected_millis: i64,
    ) {
        let binary = rewritten
            .downcast_ref::<BinaryExpr>()
            .expect("expected BinaryExpr");
        assert!(
            binary.left().downcast_ref::<Column>().is_some(),
            "expected raw column on the left, got {binary}"
        );
        assert_eq!(*binary.op(), expected_op);
        let literal = binary
            .right()
            .downcast_ref::<Literal>()
            .expect("expected Literal");
        assert_eq!(
            literal.value(),
            &ScalarValue::TimestampMillisecond(Some(expected_millis), Some(Arc::from("UTC")))
        );
    }

    #[test]
    fn test_millis_timestamp_predicate_rewrites_to_millis_domain() {
        let adapter = millis_file_adapter();
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        // (predicate op, literal micros, expected op, expected millis)
        let cases = vec![
            // Exact multiples of 1000 rescale directly.
            (Operator::Lt, 2_000, Operator::Lt, 2),
            (Operator::LtEq, 2_000, Operator::LtEq, 2),
            (Operator::Gt, 2_000, Operator::Gt, 2),
            (Operator::GtEq, 2_000, Operator::GtEq, 2),
            (Operator::Eq, 2_000, Operator::Eq, 2),
            (Operator::NotEq, 2_000, Operator::NotEq, 2),
            // m * 1000 < 1500 iff m < 2, and m * 1000 <= 1500 iff m <= 1.
            (Operator::Lt, 1_500, Operator::Lt, 2),
            (Operator::LtEq, 1_500, Operator::LtEq, 1),
            (Operator::Gt, 1_500, Operator::Gt, 1),
            (Operator::GtEq, 1_500, Operator::GtEq, 2),
            // Negative literals round toward the correct side (floor/ceil, not
            // truncation): m * 1000 < -1500 iff m < -1.
            (Operator::Lt, -1_500, Operator::Lt, -1),
            (Operator::LtEq, -1_500, Operator::LtEq, -2),
            (Operator::Gt, -1_500, Operator::Gt, -2),
            (Operator::GtEq, -1_500, Operator::GtEq, -1),
            // Sub-millisecond equality can never hold for any file value; the
            // rewrite keeps NULL semantics with an always-false / always-true
            // comparison.
            (Operator::Eq, 1_500, Operator::Lt, i64::MIN),
            (Operator::NotEq, 1_500, Operator::GtEq, i64::MIN),
        ];
        for (op, micros, expected_op, expected_millis) in cases {
            let pred: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(Arc::clone(&col), op, micros_lit(micros)));
            let rewritten = adapter.rewrite(pred).unwrap();
            assert_millis_comparison(&rewritten, expected_op, expected_millis);

            // The literal-on-left form swaps the operator and rescales the same way.
            let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                micros_lit(micros),
                op.swap().unwrap(),
                Arc::clone(&col),
            ));
            let rewritten = adapter.rewrite(pred).unwrap();
            assert_millis_comparison(&rewritten, expected_op, expected_millis);
        }
    }

    #[test]
    fn test_millis_timestamp_null_checks_rewrite_to_raw_column() {
        let adapter = millis_file_adapter();
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        let pred: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::clone(&col)));
        let rewritten = adapter.rewrite(pred).unwrap();
        let is_null = rewritten
            .downcast_ref::<IsNullExpr>()
            .expect("expected IsNullExpr");
        assert!(is_null.arg().downcast_ref::<Column>().is_some());

        let pred: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(col));
        let rewritten = adapter.rewrite(pred).unwrap();
        let is_not_null = rewritten
            .downcast_ref::<IsNotNullExpr>()
            .expect("expected IsNotNullExpr");
        assert!(is_not_null.arg().downcast_ref::<Column>().is_some());
    }

    #[test]
    fn test_millis_timestamp_null_safe_equality_rewrites() {
        let adapter = millis_file_adapter();
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        // Divisible literal: same operator, rescaled.
        for (op, expected_op) in [
            (Operator::IsNotDistinctFrom, Operator::IsNotDistinctFrom),
            (Operator::IsDistinctFrom, Operator::IsDistinctFrom),
        ] {
            let pred: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(Arc::clone(&col), op, micros_lit(2_000)));
            let rewritten = adapter.rewrite(pred).unwrap();
            assert_millis_comparison(&rewritten, expected_op, 2);
        }

        // Sub-millisecond literal: null-safe comparisons never return NULL, so the
        // expression folds to a boolean constant.
        for (op, expected) in [
            (Operator::IsNotDistinctFrom, false),
            (Operator::IsDistinctFrom, true),
        ] {
            let pred: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(Arc::clone(&col), op, micros_lit(1_500)));
            let rewritten = adapter.rewrite(pred).unwrap();
            let literal = rewritten
                .downcast_ref::<Literal>()
                .expect("expected boolean Literal");
            assert_eq!(literal.value(), &ScalarValue::Boolean(Some(expected)));
        }

        // NULL literal: `<=> NULL` tests null-ness of the raw value.
        let null_lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(
            ScalarValue::TimestampMicrosecond(None, Some(Arc::from("UTC"))),
        ));
        let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&col),
            Operator::IsNotDistinctFrom,
            Arc::clone(&null_lit),
        ));
        let rewritten = adapter.rewrite(pred).unwrap();
        assert!(rewritten.downcast_ref::<IsNullExpr>().is_some());

        let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&col),
            Operator::IsDistinctFrom,
            Arc::clone(&null_lit),
        ));
        let rewritten = adapter.rewrite(pred).unwrap();
        assert!(rewritten.downcast_ref::<IsNotNullExpr>().is_some());

        // NULL literal with an ordinary comparison: NULL result either way, so the
        // literal rescales to a NULL millisecond literal.
        let pred: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Lt, null_lit));
        let rewritten = adapter.rewrite(pred).unwrap();
        let binary = rewritten
            .downcast_ref::<BinaryExpr>()
            .expect("expected BinaryExpr");
        let literal = binary
            .right()
            .downcast_ref::<Literal>()
            .expect("expected Literal");
        assert_eq!(
            literal.value(),
            &ScalarValue::TimestampMillisecond(None, Some(Arc::from("UTC")))
        );
    }

    fn millis_values_of(list: &[Arc<dyn PhysicalExpr>]) -> Vec<Option<i64>> {
        list.iter()
            .map(|item| {
                match item
                    .downcast_ref::<Literal>()
                    .expect("expected Literal list element")
                    .value()
                {
                    ScalarValue::TimestampMillisecond(v, _) => *v,
                    other => panic!("expected millisecond literal, got {other:?}"),
                }
            })
            .collect()
    }

    #[test]
    fn test_millis_timestamp_in_list_rewrites_to_millis_domain() {
        let adapter = millis_file_adapter();
        let physical = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        )]);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        for negated in [false, true] {
            // Divisible literals rescale, a sub-millisecond literal drops out, and a
            // NULL literal stays NULL.
            let null_lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(
                ScalarValue::TimestampMicrosecond(None, Some(Arc::from("UTC"))),
            ));
            let logical = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            )]);
            let pred = in_list(
                Arc::clone(&col),
                vec![micros_lit(2_000), micros_lit(1_500), null_lit],
                &negated,
                &logical,
            )
            .unwrap();
            let rewritten = adapter.rewrite(pred).unwrap();
            let rewritten_in_list = rewritten
                .downcast_ref::<InListExpr>()
                .expect("expected InListExpr");
            assert!(rewritten_in_list.expr().downcast_ref::<Column>().is_some());
            assert_eq!(rewritten_in_list.negated(), negated);
            assert_eq!(
                millis_values_of(rewritten_in_list.list()),
                vec![Some(2), None]
            );
            assert_eq!(
                rewritten_in_list.expr().data_type(&physical).unwrap(),
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
            );

            // A list of only impossible literals degenerates to the always-false /
            // always-true sentinel comparison with IN's null semantics.
            let logical = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            )]);
            let pred = in_list(
                Arc::clone(&col),
                vec![micros_lit(1_500), micros_lit(-1)],
                &negated,
                &logical,
            )
            .unwrap();
            let rewritten = adapter.rewrite(pred).unwrap();
            let expected_op = if negated {
                Operator::GtEq
            } else {
                Operator::Lt
            };
            assert_millis_comparison(&rewritten, expected_op, i64::MIN);
        }
    }
}
