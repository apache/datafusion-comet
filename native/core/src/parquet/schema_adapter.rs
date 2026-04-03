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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
use std::collections::HashMap;
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
    /// Names of partition columns to exclude from schema validation.
    /// Partition columns get their values from directory paths, not from the Parquet file,
    /// so a type mismatch between the file's data column and the logical partition column
    /// is expected and should not be rejected.
    partition_column_names: Option<Vec<String>>,
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
            partition_column_names: None,
        }
    }

    /// Set partition column names to enable schema validation that skips partition columns.
    /// When not set, schema validation is skipped entirely (safe default for paths
    /// where partition info is unavailable).
    pub fn with_partition_column_names(mut self, names: Vec<String>) -> Self {
        self.partition_column_names = Some(names);
        self
    }
}

/// Remap physical schema field names to match logical schema field names using
/// case-insensitive matching. This allows the DefaultPhysicalExprAdapter (which
/// uses exact name matching) to correctly find columns when the parquet file has
/// different casing than the table schema (e.g., file has "a" but table has "A").
fn remap_physical_schema_names(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
) -> SchemaRef {
    let remapped_fields: Vec<_> = physical_schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(logical_field) = logical_schema
                .fields()
                .iter()
                .find(|lf| lf.name().eq_ignore_ascii_case(field.name()))
            {
                if logical_field.name() != field.name() {
                    Arc::new(Field::new(
                        logical_field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    ))
                } else {
                    Arc::clone(field)
                }
            } else {
                Arc::clone(field)
            }
        })
        .collect();

    Arc::new(Schema::new(remapped_fields))
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
    ) -> Arc<dyn PhysicalExprAdapter> {
        // When case-insensitive, remap physical schema field names to match logical
        // field names. The DefaultPhysicalExprAdapter uses exact name matching, so
        // without this remapping, columns like "a" won't match logical "A" and will
        // be filled with nulls.
        //
        // We also build a reverse map (logical name -> physical name) so that after
        // the default adapter produces expressions, we can remap column names back
        // to the original physical names. This is necessary because downstream code
        // (reassign_expr_columns) looks up columns by name in the actual stream
        // schema, which uses the original physical file column names.
        let (adapted_physical_schema, logical_to_physical_names, original_physical_schema) =
            if !self.parquet_options.case_sensitive {
                let logical_to_physical: HashMap<String, String> = logical_file_schema
                    .fields()
                    .iter()
                    .filter_map(|logical_field| {
                        physical_file_schema
                            .fields()
                            .iter()
                            .find(|pf| {
                                pf.name().eq_ignore_ascii_case(logical_field.name())
                                    && pf.name() != logical_field.name()
                            })
                            .map(|pf| (logical_field.name().clone(), pf.name().clone()))
                    })
                    .collect();
                let remapped =
                    remap_physical_schema_names(&logical_file_schema, &physical_file_schema);
                (
                    remapped,
                    if logical_to_physical.is_empty() {
                        None
                    } else {
                        Some(logical_to_physical)
                    },
                    // Keep original physical schema for per-column duplicate detection
                    Some(Arc::clone(&physical_file_schema)),
                )
            } else {
                (Arc::clone(&physical_file_schema), None, None)
            };

        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&adapted_physical_schema),
        );

        // Only validate schema when partition column info is available (NativeScan path).
        // Without partition info (JNI/native_iceberg_compat path), we cannot distinguish
        // partition/data column name overlaps from genuine schema mismatches.
        let schema_validation_error = self.partition_column_names.as_ref().and_then(|pcols| {
            validate_spark_schema_compatibility(
                &logical_file_schema,
                &adapted_physical_schema,
                self.parquet_options.case_sensitive,
                pcols,
                self.parquet_options.allow_type_widening,
            )
        });

        Arc::new(SparkPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema: adapted_physical_schema,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
            default_adapter,
            logical_to_physical_names,
            schema_validation_error,
            original_physical_schema,
        })
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
    /// Schema validation error detected during adapter creation. Stored as a SparkError
    /// so it can be propagated via DataFusionError::External → CometQueryExecutionException
    /// → SparkErrorConverter, matching Spark's SchemaColumnConvertNotSupportedException.
    schema_validation_error: Option<SparkError>,
    /// The original (un-remapped) physical schema, kept for per-column duplicate
    /// detection in case-insensitive mode. Only set when `!case_sensitive`.
    original_physical_schema: Option<SchemaRef>,
}

impl PhysicalExprAdapter for SparkPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        // Check for schema incompatibility (e.g., reading binary as timestamp, int as long).
        // This validation was performed during adapter creation; surface the error now.
        if let Some(spark_err) = &self.schema_validation_error {
            return Err(DataFusionError::External(Box::new(spark_err.clone())));
        }

        // In case-insensitive mode, check if any Column in this expression references
        // a field with multiple case-insensitive matches in the physical schema.
        // Only the columns actually referenced trigger the error (not the whole schema).
        if let Some(orig_physical) = &self.original_physical_schema {
            // Walk the expression tree to find Column references
            let mut duplicate_err: Option<DataFusionError> = None;
            let _ = Arc::<dyn PhysicalExpr>::clone(&expr).transform(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
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

        // For case-insensitive mode: remap column names from logical back to
        // original physical names. The default adapter was given a remapped
        // physical schema (with logical names) so it could find columns. But
        // downstream code (reassign_expr_columns) looks up columns by name in
        // the actual parquet stream schema, which uses the original physical names.
        let expr = if let Some(name_map) = &self.logical_to_physical_names {
            expr.transform(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
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
        expr.transform(|e| {
            if let Some(column) = e.as_any().downcast_ref::<Column>() {
                let col_idx = column.index();
                let col_name = column.name();

                let logical_field = self.logical_file_schema.fields().get(col_idx);
                // Look up physical field by name instead of index for correctness
                // when logical and physical schemas have different column orderings
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

                if let (Some(logical_field), Some(physical_field)) = (logical_field, physical_field)
                {
                    if logical_field.data_type() != physical_field.data_type() {
                        let cast_expr: Arc<dyn PhysicalExpr> = Arc::new(
                            CometCastColumnExpr::new(
                                Arc::clone(&e),
                                Arc::clone(physical_field),
                                Arc::clone(logical_field),
                                None,
                            )
                            .with_parquet_options(self.parquet_options.clone()),
                        );
                        return Ok(Transformed::yes(cast_expr));
                    }
                }
            }
            Ok(Transformed::no(e))
        })
        .data()
    }

    /// Replace CastColumnExpr (DataFusion's cast) with Spark's Cast expression.
    fn replace_with_spark_cast(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Transformed<Arc<dyn PhysicalExpr>>> {
        // Check for CastColumnExpr and replace with spark_expr::Cast
        // CastColumnExpr is in datafusion_physical_expr::expressions
        if let Some(cast) = expr
            .as_any()
            .downcast_ref::<datafusion::physical_expr::expressions::CastColumnExpr>()
        {
            let child = Arc::clone(cast.expr());
            let physical_type = cast.input_field().data_type();
            let target_type = cast.target_field().data_type();

            // For complex nested types (Struct, List, Map), Timestamp timezone
            // mismatches, and Timestamp→Int64 (nanosAsLong), use CometCastColumnExpr
            // with spark_parquet_convert which handles field-name-based selection,
            // reordering, nested type casting, metadata-only timestamp timezone
            // relabeling, and raw value reinterpretation correctly.
            //
            // Timestamp mismatches (e.g., Timestamp(us, None) -> Timestamp(us, Some("UTC")))
            // occur when INT96 Parquet timestamps are coerced to Timestamp(us, None) by
            // DataFusion but the logical schema expects Timestamp(us, Some("UTC")).
            // Using Spark's Cast here would incorrectly treat the None-timezone values as
            // local time (TimestampNTZ) and apply a timezone conversion, but the values are
            // already in UTC. spark_parquet_convert handles this as a metadata-only change.
            //
            // Timestamp→Int64 occurs when Spark's `nanosAsLong` config converts
            // TIMESTAMP(NANOS) to LongType. Spark's Cast would divide by MICROS_PER_SECOND
            // (assuming microseconds), but the values are nanoseconds. Arrow cast correctly
            // reinterprets the raw i64 value without conversion.
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
                        Arc::clone(cast.input_field()),
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

/// Validates physical-vs-logical schema compatibility per Spark's `TypeUtil.checkParquetType()`.
/// Returns an error message for the first incompatible column, or None if all compatible.
/// Partition columns are skipped because their values come from directory paths, not from the
/// Parquet file. A data column in the file may share a name with a partition column but have a
/// completely different type (e.g., Int32 data column vs String partition column).
pub fn validate_spark_schema_compatibility(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    case_sensitive: bool,
    partition_column_names: &[String],
    allow_type_widening: bool,
) -> Option<SparkError> {
    for logical_field in logical_schema.fields() {
        // Skip partition columns — their values come from the directory path, not the file
        let is_partition_col = if case_sensitive {
            partition_column_names
                .iter()
                .any(|p| p == logical_field.name())
        } else {
            partition_column_names
                .iter()
                .any(|p| p.to_lowercase() == logical_field.name().to_lowercase())
        };
        if is_partition_col {
            continue;
        }

        let physical_field = if case_sensitive {
            physical_schema
                .fields()
                .iter()
                .find(|f| f.name() == logical_field.name())
        } else {
            physical_schema
                .fields()
                .iter()
                .find(|f| f.name().to_lowercase() == logical_field.name().to_lowercase())
        };

        if let Some(physical_field) = physical_field {
            let physical_type = physical_field.data_type();
            let logical_type = logical_field.data_type();
            if physical_type != logical_type
                && !is_spark_compatible_read(physical_type, logical_type, allow_type_widening)
            {
                return Some(SparkError::SchemaColumnConvertError {
                    column: logical_field.name().clone(),
                    expected_type: spark_type_name(logical_type),
                    physical_type: arrow_to_parquet_type_name(physical_type),
                });
            }
        }
    }
    None
}

/// Whether reading a Parquet column with `physical_type` as Spark `logical_type` is allowed.
/// See Spark's `TypeUtil.checkParquetType()`.
///
/// When `allow_type_widening` is true (Spark 4.0+), additional conversions are permitted:
/// Int→Long, Int→Double, LTZ→NTZ, Date→TimestampNTZ, and all Decimal↔Decimal.
fn is_spark_compatible_read(
    physical_type: &DataType,
    logical_type: &DataType,
    allow_type_widening: bool,
) -> bool {
    use DataType::*;

    match (physical_type, logical_type) {
        _ if physical_type == logical_type => true,

        // RunEndEncoded is an Arrow encoding wrapper (e.g., from Iceberg).
        // Unwrap to the inner values type and check compatibility.
        (RunEndEncoded(_, values_field), _) => {
            is_spark_compatible_read(values_field.data_type(), logical_type, allow_type_widening)
        }

        (_, Null) => true,

        // Integer family: same-width always allowed.
        // Int→Long only with type widening (Spark 4.0+).
        (Int8 | Int16 | Int32, Int8 | Int16 | Int32) => true,
        (Int8 | Int16 | Int32, Int64) => allow_type_widening,
        (Int64, Int64) => true,
        (Int32 | Int8 | Int16, Date32) => true,

        // Unsigned int conversions
        (UInt8, Int8 | Int16 | Int32 | Int64) => true,
        (UInt16, Int16 | Int32 | Int64) => true,
        (UInt32, Int32 | Int64) => true,
        (UInt64, Decimal128(20, 0)) => true,

        // Float widening
        (Float32, Float64) => true,
        // Spark 4.0+ type widening: Int→Double
        (Int8 | Int16 | Int32, Float64) => allow_type_widening,

        // Date → TimestampNTZ (Spark 4.0+ type widening only)
        (Int32 | Date32, Timestamp(_, None)) => allow_type_widening,
        (Date32, Date32) => true,

        // Timestamps: NTZ→LTZ always allowed (INT96 coercion).
        // LTZ→NTZ only allowed with type widening (Spark 4.0+, SPARK-47447).
        // Spark 3.x rejects LTZ→NTZ (SPARK-36182).
        (Timestamp(_, tz_physical), Timestamp(_, tz_logical)) => {
            if tz_physical.is_some() && tz_logical.is_none() {
                allow_type_widening
            } else {
                true
            }
        }

        // Timestamp ↔ Int64: nanosAsLong and Iceberg timestamp partition columns
        (Timestamp(_, _), Int64) | (Int64, Timestamp(_, _)) => true,

        // BINARY / String interop
        (Binary | LargeBinary | Utf8 | LargeUtf8, Binary | LargeBinary | Utf8 | LargeUtf8) => true,
        (FixedSizeBinary(_), Binary | LargeBinary | Utf8 | LargeUtf8) => true,

        // Non-decimal-annotated Parquet primitives read as Decimal:
        // When a Parquet column has no DECIMAL annotation, DataFusion reports the raw
        // physical type (Int32/Int64/Binary). Reading these as Decimal is only valid if
        // the Decimal has enough integer digits to represent all possible values of the
        // physical type. Matches Spark's ParquetVectorUpdaterFactory.isDecimalTypeMatched
        // for non-annotated types:
        //   INT32: integerPrecision (= precision - scale) >= 10
        //   INT64: integerPrecision >= 20
        // FixedSizeBinary/Binary always rejected (no decimal annotation means not decimal).
        (Int8 | Int16 | Int32, Decimal128(p, s)) if allow_type_widening => {
            let integer_precision = *p as i16 - *s as i16;
            integer_precision >= 10
        }
        (Int64, Decimal128(p, s)) if allow_type_widening => {
            let integer_precision = *p as i16 - *s as i16;
            integer_precision >= 20
        }

        // Decimal → Decimal conversions:
        // Spark 3.x: physical precision <= logical, scales must match.
        // Spark 4.0+: scale can increase as long as precision increases by at least as much
        //   (scaleIncrease >= 0 && precisionIncrease >= scaleIncrease).
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            if allow_type_widening {
                let scale_increase = *s2 as i16 - *s1 as i16;
                let precision_increase = *p2 as i16 - *p1 as i16;
                scale_increase >= 0 && precision_increase >= scale_increase
            } else {
                p1 <= p2 && s1 == s2
            }
        }

        // Nested types (DataFusion handles inner-type adaptation)
        (Struct(_), Struct(_))
        | (List(_), List(_))
        | (LargeList(_), List(_) | LargeList(_))
        | (Map(_, _), Map(_, _)) => true,

        _ => false,
    }
}

fn spark_type_name(dt: &DataType) -> String {
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
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamp".to_string(),
        DataType::Timestamp(unit, tz) => format!("timestamp({unit:?}, {tz:?})"),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::List(f) => format!("array<{}>", spark_type_name(f.data_type())),
        DataType::LargeList(f) => format!("array<{}>", spark_type_name(f.data_type())),
        DataType::Map(f, _) => {
            if let DataType::Struct(fields) = f.data_type() {
                if fields.len() == 2 {
                    return format!(
                        "map<{},{}>",
                        spark_type_name(fields[0].data_type()),
                        spark_type_name(fields[1].data_type())
                    );
                }
            }
            format!("map<{}>", spark_type_name(f.data_type()))
        }
        DataType::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), spark_type_name(f.data_type())))
                .collect();
            format!("struct<{}>", field_strs.join(","))
        }
        other => format!("{other}"),
    }
}

fn arrow_to_parquet_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt32
        | DataType::Date32 => "INT32".to_string(),
        DataType::Int64 | DataType::UInt64 => "INT64".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            "BINARY".to_string()
        }
        DataType::FixedSizeBinary(n) => format!("FIXED_LEN_BYTE_ARRAY({n})"),
        DataType::Timestamp(_, _) => "INT64".to_string(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::RunEndEncoded(_, values_field) => {
            arrow_to_parquet_type_name(values_field.data_type())
        }
        other => format!("{other}"),
    }
}

#[cfg(test)]
mod test {
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
    use arrow::array::Int32Array;
    use arrow::array::UInt32Array;
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

    #[tokio::test]
    async fn parquet_roundtrip_unsigned_int() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));

        let ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids])?;

        let required_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let _ = roundtrip(&batch, required_schema).await?;

        Ok(())
    }

    // Int32→Int64: integer widening is standard schema evolution, always allowed
    #[tokio::test]
    async fn parquet_int_as_long() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("_1", DataType::Int32, true)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new("_1", DataType::Int64, true)]));

        // Int32→Int64 is rejected in Spark 3.x (allow_type_widening=false)
        let result = roundtrip(&batch, Arc::clone(&required_schema)).await;
        assert!(
            result.is_err(),
            "Int32→Int64 should fail without type widening"
        );

        // Int32→Int64 is allowed in Spark 4.0+ (allow_type_widening=true)
        let result = roundtrip_with_options(&batch, required_schema, true).await?;
        assert_eq!(result.num_rows(), 3);
        Ok(())
    }

    // SPARK-36182 / SPARK-47447: TimestampLTZ as TimestampNTZ
    // Rejected in Spark 3.x, allowed in Spark 4.0+ (type widening)
    #[tokio::test]
    async fn parquet_timestamp_ltz_as_ntz() -> Result<(), DataFusionError> {
        use arrow::datatypes::TimeUnit;
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let values = Arc::new(
            arrow::array::TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000, 3_000_000])
                .with_timezone("UTC"),
        ) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));

        // Spark 3.x: LTZ→NTZ is rejected
        let result = roundtrip(&batch, Arc::clone(&required_schema)).await;
        assert!(result.is_err());

        // Spark 4.0+: LTZ→NTZ is allowed
        let result = roundtrip_with_options(&batch, required_schema, true).await?;
        assert_eq!(result.num_rows(), 3);
        Ok(())
    }

    // SPARK-35640: reading binary data as timestamp should throw schema incompatible error
    #[tokio::test]
    async fn parquet_binary_as_timestamp() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("_1", DataType::Utf8, true)]));
        let values = Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"]))
            as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "_1",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        let result = roundtrip(&batch, required_schema).await;
        assert!(
            result.is_err(),
            "Binary→Timestamp should fail with schema mismatch"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Column: [_1]"),
            "Expected schema mismatch error, got: {err_msg}"
        );
        Ok(())
    }

    // SPARK-45604: reading timestamp_ntz as array<timestamp_ntz> should fail
    #[tokio::test]
    async fn parquet_timestamp_ntz_as_array_timestamp_ntz() -> Result<(), DataFusionError> {
        use arrow::datatypes::TimeUnit;
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "_1",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let values = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
            1_000_000, 2_000_000, 3_000_000,
        ])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "_1",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
            true,
        )]));

        let result = roundtrip(&batch, required_schema).await;
        assert!(
            result.is_err(),
            "TimestampNTZ→Array<TimestampNTZ> should fail with schema mismatch"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Column: [_1]"),
            "Expected schema mismatch error, got: {err_msg}"
        );
        Ok(())
    }

    #[test]
    fn test_is_spark_compatible_read() {
        use super::is_spark_compatible_read;
        use arrow::datatypes::TimeUnit;

        // === Always compatible (both Spark 3.x and 4.0) ===
        assert!(is_spark_compatible_read(
            &DataType::Binary,
            &DataType::Utf8,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::UInt32,
            &DataType::Int64,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Date32,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(18, 2),
            false
        ));
        // NTZ → LTZ allowed (INT96 coercion produces NTZ, Spark schema expects LTZ)
        assert!(is_spark_compatible_read(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false
        ));
        // Timestamp → Int64 allowed (nanosAsLong)
        assert!(is_spark_compatible_read(
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            &DataType::Int64,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Float32,
            &DataType::Float64,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Int64,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            false
        ));

        // Non-annotated Int → Decimal (Spark 4.0+): requires enough integer precision
        // Int32 can hold up to 10 digits, so integerPrecision must be >= 10
        assert!(!is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Decimal128(3, 2), // integerPrecision=1 < 10
            true
        ));
        assert!(is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Decimal128(11, 1), // integerPrecision=10 >= 10
            true
        ));
        // Not allowed on Spark 3.x
        assert!(!is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Decimal128(11, 1),
            false
        ));

        // === Always incompatible ===
        assert!(!is_spark_compatible_read(
            &DataType::Utf8,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true
        ));
        assert!(!is_spark_compatible_read(
            &DataType::Utf8,
            &DataType::Int32,
            true
        ));

        // Integer widening: Int→Long requires type widening (Spark 4.0+)
        assert!(!is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Int64,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Int64,
            true
        ));
        assert!(!is_spark_compatible_read(
            &DataType::Int8,
            &DataType::Int64,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Int8,
            &DataType::Int64,
            true
        ));

        // === Spark 3.x rejects, Spark 4.0 allows (type widening) ===

        // LTZ → NTZ (SPARK-36182 / SPARK-47447)
        assert!(!is_spark_compatible_read(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            true
        ));

        // Decimal narrowing (SPARK-34212): precision decrease is never allowed
        assert!(!is_spark_compatible_read(
            &DataType::Decimal128(18, 2),
            &DataType::Decimal128(10, 2),
            false
        ));
        assert!(!is_spark_compatible_read(
            &DataType::Decimal128(18, 2),
            &DataType::Decimal128(10, 2),
            true
        ));

        // Decimal scale change without enough precision increase is rejected
        assert!(!is_spark_compatible_read(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(10, 3),
            false
        ));
        assert!(!is_spark_compatible_read(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(10, 3),
            true
        ));

        // Decimal widening: scale increase with sufficient precision increase is allowed
        assert!(is_spark_compatible_read(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(11, 3),
            true
        ));
        // Same scale, larger precision is allowed
        assert!(is_spark_compatible_read(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(18, 2),
            true
        ));

        // Int → Double
        assert!(!is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Float64,
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Int32,
            &DataType::Float64,
            true
        ));

        // Date → TimestampNTZ
        assert!(!is_spark_compatible_read(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            false
        ));
        assert!(is_spark_compatible_read(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            true
        ));
    }

    async fn roundtrip(
        batch: &RecordBatch,
        required_schema: SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        roundtrip_with_options(batch, required_schema, false).await
    }

    async fn roundtrip_with_options(
        batch: &RecordBatch,
        required_schema: SchemaRef,
        allow_type_widening: bool,
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
        spark_parquet_options.allow_type_widening = allow_type_widening;

        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(
            SparkPhysicalExprAdapterFactory::new(spark_parquet_options, None)
                .with_partition_column_names(vec![]),
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
}
