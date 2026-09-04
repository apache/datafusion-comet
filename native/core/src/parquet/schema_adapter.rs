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
use crate::parquet::name_fold::{fold_name, fold_names, fold_schema_names};
use crate::parquet::parquet_support::{
    match_struct_fields, spark_parquet_convert, SparkParquetOptions,
};
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
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::{HashMap, HashSet};
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
    let unmatched_id_logical_folded: HashSet<String> = if should_match_by_id {
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
        HashSet::new()
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
        // Spark's `catalogString` for the complex types (e.g. `array<int>`), so a
        // scalar-vs-complex rejection reads like Spark's.
        DataType::List(item) | DataType::LargeList(item) => {
            format!("array<{}>", spark_catalog_name(item.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "map<{},{}>",
                spark_catalog_name(kv[0].data_type()),
                spark_catalog_name(kv[1].data_type())
            ),
            _ => "unknown".to_string(),
        },
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), spark_catalog_name(f.data_type())))
                .collect::<Vec<_>>()
                .join(",")
        ),
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
/// rejected Parquet -> Spark conversion. `column` is the Spark-style column path (`a`, or
/// `s, x` for a nested leaf); the bracketed wrapping mirrors
/// `Arrays.toString(descriptor.getPath())` in Spark's vectorized reader.
fn parquet_schema_convert_err(
    column: &str,
    physical_type: &DataType,
    target_type: &DataType,
) -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::ParquetSchemaConvert {
        file_path: String::new(),
        column: format!("[{}]", column),
        physical_type: parquet_primitive_name(physical_type).to_string(),
        spark_type: spark_catalog_name(target_type),
    }))
}

/// Build a `RejectOnNonEmpty` expr wrapping `child`. The rejection fires only
/// when the input batch is non-empty (mirrors Spark's per-row-group check).
/// `column` is the Spark-style column path, as for [`parquet_schema_convert_err`].
fn reject_on_non_empty_expr(
    child: Arc<dyn PhysicalExpr>,
    target_field: &FieldRef,
    column: &str,
    physical_type: &DataType,
    target_type: &DataType,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(RejectOnNonEmpty {
        child,
        target_field: Arc::clone(target_field),
        column: format!("[{}]", column),
        physical_type: parquet_primitive_name(physical_type).to_string(),
        spark_type: spark_catalog_name(target_type),
    })
}

/// Outcome of checking one Parquet (physical) -> Spark (logical) type pair against the
/// conversion rules of Spark's vectorized Parquet reader.
enum ConversionCheck {
    /// Spark has an updater for the pair (for a same-shape complex pair: for every leaf).
    Accept,
    /// Spark rejects the pair; raised at plan time.
    Reject(DataFusionError),
    /// Spark rejects the pair, but only while decoding a row group, so the rejection is
    /// deferred to runtime via [`RejectOnNonEmpty`] (SPARK-26709). Carries the offending
    /// leaf's column path and physical / requested types for the error message.
    RejectOnNonEmpty {
        column: String,
        physical_type: DataType,
        target_type: DataType,
    },
}

/// Apply the rejection matrix of Spark's `ParquetVectorUpdaterFactory.getUpdater` to a single
/// physical/logical leaf pair. `column` is the Spark-style column path used in the error (`a`
/// for a top-level column, `s, x` for a nested leaf, mirroring
/// `Arrays.toString(descriptor.getPath())`). The rules and their order are exactly those the
/// adapter applies to top-level columns; [`check_conversion`] applies them to nested leaves.
fn check_leaf_conversion(
    physical_type: &DataType,
    target_type: &DataType,
    column: &str,
    options: &SparkParquetOptions,
) -> ConversionCheck {
    // arrow-rs surfaces a column whose file carries an `ARROW:schema` with a dictionary
    // encoding as `Dictionary(_, value)`; Spark only ever sees the value's Parquet type.
    let physical_type = match physical_type {
        DataType::Dictionary(_, value_type) => value_type.as_ref(),
        other => other,
    };
    if physical_type == target_type {
        return ConversionCheck::Accept;
    }
    let reject = || {
        ConversionCheck::Reject(parquet_schema_convert_err(
            column,
            physical_type,
            target_type,
        ))
    };
    let reject_on_non_empty = || ConversionCheck::RejectOnNonEmpty {
        column: column.to_string(),
        physical_type: physical_type.clone(),
        target_type: target_type.clone(),
    };

    // Reject reading a string/binary Parquet column as anything else. Spark's
    // `ParquetVectorUpdaterFactory.getUpdater` BINARY case allows StringType /
    // BinaryType, or DecimalType only when the column carries a
    // `DecimalLogicalTypeAnnotation` (which arrow-rs surfaces as `Decimal128`,
    // not `Binary`). Without this guard, runtime cast paths silently return
    // nulls, parse strings, or surface as a generic Arrow type-mismatch error.
    // See #4088 and #4351.
    if is_string_or_binary(physical_type) && !is_string_or_binary(target_type) {
        return reject();
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
        return reject_on_non_empty();
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
            return reject();
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
                return reject();
            }
        }
    }

    // Type promotion (widening). When `allow_type_promotion` is false,
    // reject the three widenings (INT32→INT64, FLOAT→DOUBLE, INT32→DOUBLE)
    // that Spark 3.x's vectorized reader rejects. The flag tracks Comet's
    // per-Spark-version constant in ShimCometConf. Deferred to runtime so
    // empty files (SPARK-26709) pass.
    if !options.allow_type_promotion {
        let is_disallowed_promotion = matches!(
            (physical_type, target_type),
            (DataType::Int32, DataType::Int64)
                | (DataType::Float32, DataType::Float64)
                | (DataType::Int32, DataType::Float64)
        );
        if is_disallowed_promotion {
            return reject_on_non_empty();
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
        return reject_on_non_empty();
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
    if !options.allow_timestamp_ltz_to_ntz
        && matches!(
            (physical_type, target_type),
            (
                DataType::Timestamp(_, Some(_)),
                DataType::Timestamp(_, None)
            )
        )
    {
        return reject_on_non_empty();
    }

    // Scalar/complex mismatch (e.g. TIMESTAMP read as ARRAY<TIMESTAMP>):
    // Spark's vectorized reader rejects with
    // SchemaColumnConvertNotSupportedException (SPARK-45604). Same-shape
    // complex pairs never reach this leaf check (`check_conversion` walks their
    // leaves instead), so two complex types here differ in shape (e.g. STRUCT
    // read as ARRAY), which Spark rejects just the same.
    let is_complex = |t: &DataType| {
        matches!(
            t,
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
        )
    };
    if is_complex(physical_type) || is_complex(target_type) {
        return reject();
    }

    ConversionCheck::Accept
}

/// Check a physical/logical type pair the way Spark's vectorized reader does. Spark runs
/// `getUpdater` on every *leaf* column regardless of nesting, so same-shape complex pairs
/// (struct / list / map, at any depth) are walked and [`check_leaf_conversion`] is applied to
/// each leaf, extending the column path the way `descriptor.getPath()` does (struct field
/// names, the map entries field plus its `key` / `value`, the list element field). Requested
/// struct fields resolve to file fields with the same field-id / case-fold rules the runtime
/// convert uses ([`match_struct_fields`]); requested fields missing from the file are skipped
/// (they read as null / default, as before). The first non-`Accept` verdict in leaf order
/// wins, like Spark, which raises for the first offending column it initializes.
fn check_conversion(
    physical_type: &DataType,
    target_type: &DataType,
    column: &str,
    options: &SparkParquetOptions,
) -> DataFusionResult<ConversionCheck> {
    match (physical_type, target_type) {
        (DataType::Struct(physical_fields), DataType::Struct(target_fields)) => {
            let physical_indices = match_struct_fields(physical_fields, target_fields, options)?;
            for (target_field, physical_index) in target_fields.iter().zip(physical_indices) {
                let Some(physical_index) = physical_index else {
                    continue;
                };
                let physical_field = &physical_fields[physical_index];
                let check = check_conversion(
                    physical_field.data_type(),
                    target_field.data_type(),
                    &format!("{column}, {}", physical_field.name()),
                    options,
                )?;
                if !matches!(check, ConversionCheck::Accept) {
                    return Ok(check);
                }
            }
            Ok(ConversionCheck::Accept)
        }
        (DataType::List(physical_item), DataType::List(target_item)) => check_conversion(
            physical_item.data_type(),
            target_item.data_type(),
            &format!("{column}, {}", physical_item.name()),
            options,
        ),
        (DataType::Map(physical_entries, _), DataType::Map(target_entries, _)) => {
            // Map entries are `key` / `value` structs that the runtime convert pairs
            // positionally (`parquet_convert_map_to_map`), so do the same here.
            if let (DataType::Struct(physical_kv), DataType::Struct(target_kv)) =
                (physical_entries.data_type(), target_entries.data_type())
            {
                for (physical_field, target_field) in physical_kv.iter().zip(target_kv.iter()) {
                    let check = check_conversion(
                        physical_field.data_type(),
                        target_field.data_type(),
                        &format!(
                            "{column}, {}, {}",
                            physical_entries.name(),
                            physical_field.name()
                        ),
                        options,
                    )?;
                    if !matches!(check, ConversionCheck::Accept) {
                        return Ok(check);
                    }
                }
            }
            Ok(ConversionCheck::Accept)
        }
        _ => Ok(check_leaf_conversion(
            physical_type,
            target_type,
            column,
            options,
        )),
    }
}

/// Whether `col_name` (with folded form `col_folded`) is case-insensitively ambiguous in the
/// file. `folded_to_indices` maps each folded physical name to the indices of the original
/// physical fields that fold to it (built once in `create`), so more than one index under the
/// column's folded key is the same ambiguity Spark reports as `_LEGACY_ERROR_TEMP_2093`. The
/// matched names are resolved from `physical_schema` only on that (rare) error path.
fn check_column_duplicate(
    col_name: &str,
    col_folded: &str,
    folded_to_indices: &HashMap<String, Vec<usize>>,
    physical_schema: &SchemaRef,
) -> Option<SparkError> {
    match folded_to_indices.get(col_folded) {
        Some(indices) if indices.len() > 1 => {
            let matched: Vec<&str> = indices
                .iter()
                .map(|&i| physical_schema.field(i).name().as_str())
                .collect();
            Some(SparkError::duplicate_field_case_insensitive(
                col_name, &matched,
            ))
        }
        _ => None,
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
        let case_sensitive = self.parquet_options.case_sensitive;
        let should_match_by_id =
            self.parquet_options.use_field_id && schema_has_field_ids(&logical_file_schema);
        let needs_remap = !case_sensitive || should_match_by_id;
        let (adapted_physical_schema, logical_to_physical_names, original_physical_dup_check) =
            if needs_remap {
                let (remapped, logical_to_physical) = remap_physical_schema(
                    &logical_file_schema,
                    &physical_file_schema,
                    case_sensitive,
                    self.parquet_options.use_field_id,
                    self.parquet_options.ignore_missing_field_id,
                )?;
                // Build the folded-name -> original-physical-field-indices map once for per-column
                // duplicate detection, paired with the original schema so the rare error path can
                // resolve the colliding names. Only meaningful in case-insensitive mode; it mirrors
                // the `folded_to_indices` map the nested convert builds in `parquet_support`, so
                // both paths detect ambiguity the same way instead of drifting.
                let original_physical_dup_check = if !case_sensitive {
                    let folded = fold_schema_names(&physical_file_schema, false);
                    let mut map: HashMap<String, Vec<usize>> = HashMap::new();
                    for (i, folded_name) in folded.into_iter().enumerate() {
                        map.entry(folded_name).or_default().push(i);
                    }
                    Some((Arc::clone(&physical_file_schema), map))
                } else {
                    None
                };
                (
                    remapped,
                    if logical_to_physical.is_empty() {
                        None
                    } else {
                        Some(logical_to_physical)
                    },
                    original_physical_dup_check,
                )
            } else {
                (Arc::clone(&physical_file_schema), None, None)
            };

        // Fold both schemas once here so the per-column rewrite paths reuse them instead of
        // re-folding on every `rewrite` call. Case-sensitive mode folds to identity.
        let logical_folded = fold_schema_names(&logical_file_schema, case_sensitive);
        let physical_folded = fold_schema_names(&adapted_physical_schema, case_sensitive);

        // Folded names of logical fields that resolve by Parquet field id. Spark's `matchIdField`
        // selects these by id before comparing names, so the case-insensitive duplicate check must
        // skip them: an explicit `ω` (id 2) can select the file's `ω` (id 2) even when the file
        // also holds `Ω` (id 1). Derived from `logical_folded`, which is the case-insensitive fold
        // here since this only runs when `!case_sensitive`.
        let id_resolved_logical_folded = if should_match_by_id && !case_sensitive {
            Some(
                logical_file_schema
                    .fields()
                    .iter()
                    .zip(&logical_folded)
                    .filter(|(lf, _)| parse_field_id(lf).is_some())
                    .map(|(_, folded)| folded.clone())
                    .collect::<HashSet<String>>(),
            )
        } else {
            None
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
            original_physical_dup_check,
            id_resolved_logical_folded,
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
    /// Case-insensitive duplicate detection, built once in `create`: the original (un-remapped)
    /// physical schema paired with a `folded physical name -> field indices` map. A referenced
    /// column whose folded name maps to more than one index is the `_LEGACY_ERROR_TEMP_2093`
    /// ambiguity Spark raises; the schema resolves the colliding names on that error path. `None`
    /// in case-sensitive mode (no folding, so nothing to detect).
    original_physical_dup_check: Option<(SchemaRef, HashMap<String, Vec<usize>>)>,
    /// Folded names of logical fields resolved by Parquet field id (see `create`). Spark selects
    /// these by id before comparing names, so the duplicate check above must not fire for them.
    /// `None` when not matching by id.
    id_resolved_logical_folded: Option<HashSet<String>>,
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
        if let Some((orig_physical, folded_to_indices)) = &self.original_physical_dup_check {
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
                // Fields resolved by Parquet field id are selected by id before names are
                // compared, so an id-resolved column must not trip the name-ambiguity check
                // (mirrors Spark's `matchIdField`, which never raises the duplicate-field error).
                if self
                    .id_resolved_logical_folded
                    .as_ref()
                    .is_some_and(|ids| ids.contains(folded))
                {
                    continue;
                }
                if let Some(err) =
                    check_column_duplicate(name, folded, folded_to_indices, orig_physical)
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
                        // Apply the same Spark conversion rules as `replace_with_spark_cast`;
                        // this branch is reached when the default adapter rejected the cast and
                        // we'd otherwise build a CometCastColumnExpr that silently converts, or
                        // can't actually convert (e.g. BINARY -> DECIMAL with no
                        // `DecimalLogicalTypeAnnotation`). See #4088, #4351 and #5671.
                        match check_conversion(
                            physical_field.data_type(),
                            logical_field.data_type(),
                            physical_field.name(),
                            &self.parquet_options,
                        )? {
                            ConversionCheck::Accept => {}
                            ConversionCheck::Reject(err) => return Err(err),
                            ConversionCheck::RejectOnNonEmpty {
                                column,
                                physical_type: leaf_physical_type,
                                target_type: leaf_target_type,
                            } => {
                                return Ok(Transformed::yes(reject_on_non_empty_expr(
                                    remapped,
                                    logical_field,
                                    &column,
                                    &leaf_physical_type,
                                    &leaf_target_type,
                                )));
                            }
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

            // Spark's vectorized reader validates every (file type, requested type) leaf pair
            // in `ParquetVectorUpdaterFactory.getUpdater`, nested or not. Apply the same rules
            // here (see `check_leaf_conversion`) so a pair Spark rejects never reaches a
            // runtime cast that would silently null, parse, or reinterpret values (#5671).
            match check_conversion(
                physical_type,
                target_type,
                input_field.name(),
                &self.parquet_options,
            )? {
                ConversionCheck::Accept => {}
                ConversionCheck::Reject(err) => return Err(err),
                ConversionCheck::RejectOnNonEmpty {
                    column,
                    physical_type: leaf_physical_type,
                    target_type: leaf_target_type,
                } => {
                    return Ok(Transformed::yes(reject_on_non_empty_expr(
                        child,
                        cast.target_field(),
                        &column,
                        &leaf_physical_type,
                        &leaf_target_type,
                    )));
                }
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
    use arrow::array::cast::AsArray;
    use arrow::array::UInt32Array;
    use arrow::array::{
        Array, ArrayRef, BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::{
        DataType, Field, Fields, Int64Type, Schema, TimeUnit, TimestampMicrosecondType,
    };
    use arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
    use datafusion_comet_spark_expr::test_common::file_util::get_temp_filename;
    use datafusion_comet_spark_expr::EvalMode;
    use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    /// Build field metadata carrying a Parquet field id, for the field-id remap tests.
    fn id_meta(id: &str) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    }

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

    /// Write `batch` to a temp Parquet file and execute a `DataSourceExec` over it with
    /// `required_schema` and the Spark adapter configured from `options`, so the
    /// `PhysicalExprAdapter` code runs exactly as it does in a native scan.
    fn scan_parquet(
        batch: &RecordBatch,
        required_schema: SchemaRef,
        options: SparkParquetOptions,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename)?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None)?;
        writer.write(batch)?;
        writer.close()?;

        let object_store_url = ObjectStoreUrl::local_filesystem();

        // Create expression adapter factory for Spark-compatible schema adaptation
        let expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(SparkPhysicalExprAdapterFactory::new(options, None));

        let parquet_source = ParquetSource::new(required_schema);

        let files = FileGroup::new(vec![PartitionedFile::from_path(filename)?]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_file_groups(vec![files])
                .with_expr_adapter(Some(expr_adapter_factory))
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        parquet_exec.execute(0, Arc::new(TaskContext::default()))
    }

    /// Create a Parquet file containing a single batch and then read the batch back using
    /// the specified required_schema. This will cause the PhysicalExprAdapter code to be used.
    async fn roundtrip(
        batch: &RecordBatch,
        required_schema: SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.allow_cast_unsigned_ints = true;
        let mut stream = scan_parquet(batch, required_schema, spark_parquet_options)?;
        stream.next().await.unwrap()
    }

    /// Build a one-column batch `s: struct<field>` holding `values`, for the nested
    /// conversion tests (#5671).
    fn struct_batch(field: Field, values: ArrayRef) -> Result<RecordBatch, DataFusionError> {
        let fields = Fields::from(vec![field]);
        let s = StructArray::try_new(fields.clone(), vec![values], None)?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(fields),
            true,
        )]));
        Ok(RecordBatch::try_new(schema, vec![Arc::new(s)])?)
    }

    /// Read schema `s: struct<fields>` for the nested conversion tests.
    fn struct_schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(fields)),
            true,
        )]))
    }

    /// Read `batch` under `required_schema` with the default options, asserting the read is
    /// rejected, and return the error message.
    async fn nested_rejection_message(batch: &RecordBatch, required_schema: SchemaRef) -> String {
        roundtrip(batch, required_schema)
            .await
            .expect_err("expected ParquetSchemaConvert for the nested conversion")
            .to_string()
    }

    /// `INT64 -> int` inside a struct. Spark's vectorized reader runs `getUpdater` per leaf,
    /// so the rejection of the top-level `parquet_long_read_as_int_errors` applies to `s.x`
    /// too; Comet previously cast with `safe: true` and returned `{null}` for the
    /// overflowing row. See #5671.
    #[tokio::test]
    async fn nested_long_read_as_int_errors() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![5_000_000_000i64, 1])),
        )?;
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("x", DataType::Int32, true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, x]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `INT32 -> string` inside a struct (previously stringified to `"1"`, `"2"`).
    #[tokio::test]
    async fn nested_int_read_as_string_errors() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1, 2])),
        )?;
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("x", DataType::Utf8, true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, x]]")
                && msg.contains("Expected: string")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Decimal precision narrowing inside a struct (previously nulled the value).
    #[tokio::test]
    async fn nested_decimal_narrowing_errors() -> Result<(), DataFusionError> {
        let values = Decimal128Array::from(vec![12_345_678i128])
            .with_precision_and_scale(10, 2)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let batch = struct_batch(
            Field::new("d", DataType::Decimal128(10, 2), true),
            Arc::new(values),
        )?;
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("d", DataType::Decimal128(5, 2), true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, d]]")
                && msg.contains("Expected: decimal(5,2)")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `BINARY -> int` inside a struct (previously parsed `"12"` and nulled `"abc"`).
    #[tokio::test]
    async fn nested_string_read_as_int_errors() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["12", "abc"])),
        )?;
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("x", DataType::Int32, true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, x]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: BINARY"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `INT32 -> array<int>` inside a struct (previously wrapped each value as `[1]`).
    #[tokio::test]
    async fn nested_int_read_as_list_errors() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1, 2])),
        )?;
        let list_type = DataType::List(Arc::new(Field::new("element", DataType::Int32, true)));
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("x", list_type, true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, x]]")
                && msg.contains("Expected: array<int>")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `array<int> -> int` inside a struct. Previously the unconverted list array was handed
    /// to `StructArray::new`, which panicked with "Incorrect datatype for StructArray field";
    /// this goes through the default adapter's failure path (`wrap_all_type_mismatches`).
    #[tokio::test]
    async fn nested_list_read_as_int_errors() -> Result<(), DataFusionError> {
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        );
        let batch = struct_batch(
            Field::new("x", list.data_type().clone(), true),
            Arc::new(list),
        )?;
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("x", DataType::Int32, true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, x]]") && msg.contains("Expected: int"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// The walk descends through lists: `array<struct<x: INT64>>` read as
    /// `array<struct<x: int>>` is rejected at the leaf, with the list element in the path.
    #[tokio::test]
    async fn nested_list_of_struct_long_read_as_int_errors() -> Result<(), DataFusionError> {
        let element_fields = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let elements = StructArray::try_new(
            element_fields.clone(),
            vec![Arc::new(Int64Array::from(vec![5_000_000_000i64, 1]))],
            None,
        )?;
        let element_field = Arc::new(Field::new(
            "element",
            DataType::Struct(element_fields),
            true,
        ));
        let list = ListArray::new(
            element_field,
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(elements),
            None,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![Arc::new(list)])?;

        let read_element = Field::new(
            "element",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
            true,
        );
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(read_element)),
            true,
        )]));
        let msg = nested_rejection_message(&batch, required_schema).await;
        assert!(
            msg.contains("Column: [[a, element, x]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Build the Arrow `Map<string, value_type>` type with Parquet's `key_value` / `key` /
    /// `value` names, as Spark-written files surface it.
    fn map_type(value_type: DataType) -> DataType {
        let entries = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", value_type, true),
        ]);
        DataType::Map(
            Arc::new(Field::new("key_value", DataType::Struct(entries), false)),
            false,
        )
    }

    /// The walk descends through maps: `map<string, INT64>` read as `map<string, int>` is
    /// rejected at the value leaf, with the entries field in the path.
    #[tokio::test]
    async fn nested_map_value_long_read_as_int_errors() -> Result<(), DataFusionError> {
        let DataType::Map(entries_field, _) = map_type(DataType::Int64) else {
            unreachable!()
        };
        let DataType::Struct(entries_fields) = entries_field.data_type() else {
            unreachable!()
        };
        let entries = StructArray::try_new(
            entries_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(Int64Array::from(vec![5_000_000_000i64])),
            ],
            None,
        )?;
        let map = MapArray::try_new(
            Arc::clone(&entries_field),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        )?;
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            map.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![Arc::new(map)])?;

        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            map_type(DataType::Int32),
            true,
        )]));
        let msg = nested_rejection_message(&batch, required_schema).await;
        assert!(
            msg.contains("Column: [[m, key_value, value]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Nested fields resolve with the same case-insensitive fold as the runtime convert, so
    /// the rule applies to file field `X` read as `x` and the path names the file field.
    #[tokio::test]
    async fn nested_case_insensitive_field_match_applies_rules() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("X", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1i64, 2])),
        )?;
        let msg = nested_rejection_message(
            &batch,
            struct_schema(vec![Field::new("x", DataType::Int32, true)]),
        )
        .await;
        assert!(
            msg.contains("Column: [[s, X]]") && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Nested fields resolve by Parquet field id when `use_field_id` is set (names differ),
    /// and the rule applies to the id-matched pair.
    #[tokio::test]
    async fn nested_field_id_match_applies_rules() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("a", DataType::Int64, true).with_metadata(id_meta("1")),
            Arc::new(Int64Array::from(vec![1i64, 2])),
        )?;
        let required_schema = struct_schema(vec![
            Field::new("b", DataType::Int32, true).with_metadata(id_meta("1"))
        ]);
        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.use_field_id = true;
        let mut stream = scan_parquet(&batch, required_schema, options)?;
        let err = stream
            .next()
            .await
            .unwrap()
            .expect_err("expected ParquetSchemaConvert for the id-matched nested field");
        let msg = err.to_string();
        assert!(
            msg.contains("Column: [[s, a]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Disallowed widening (`INT32 -> bigint` with `allow_type_promotion` off) inside a
    /// struct defers to `RejectOnNonEmpty`, like the top level: a non-empty file fails ...
    #[tokio::test]
    async fn nested_disallowed_widening_rejects_non_empty() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        )?;
        let required_schema = struct_schema(vec![Field::new("x", DataType::Int64, true)]);
        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.allow_type_promotion = false;
        let mut stream = scan_parquet(&batch, required_schema, options)?;
        let err = stream
            .next()
            .await
            .unwrap()
            .expect_err("expected ParquetSchemaConvert for nested disallowed widening");
        let msg = err.to_string();
        assert!(
            msg.contains("Column: [[s, x]]")
                && msg.contains("Expected: bigint")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// ... while an empty file (no row groups, SPARK-26709) still reads.
    #[tokio::test]
    async fn nested_disallowed_widening_passes_for_empty_file() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int32, true),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
        )?;
        let required_schema = struct_schema(vec![Field::new("x", DataType::Int64, true)]);
        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.allow_type_promotion = false;
        let mut stream = scan_parquet(&batch, required_schema, options)?;
        while let Some(batch) = stream.next().await {
            assert_eq!(batch?.num_rows(), 0);
        }
        Ok(())
    }

    /// Positive: `INT32 -> bigint` inside a struct still converts when type promotion is
    /// allowed (Spark 4.x behaviour).
    #[tokio::test]
    async fn nested_int_widening_succeeds_with_type_promotion() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        )?;
        let required_schema = struct_schema(vec![Field::new("x", DataType::Int64, true)]);
        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.allow_type_promotion = true;
        let mut stream = scan_parquet(&batch, required_schema, options)?;
        let result = stream.next().await.unwrap()?;
        let x = result
            .column(0)
            .as_struct()
            .column(0)
            .as_primitive::<Int64Type>();
        assert_eq!(x.values().to_vec(), vec![1i64, 2, 3]);
        Ok(())
    }

    /// Positive: TIMESTAMP_MILLIS inside a list still converts to microseconds.
    #[tokio::test]
    async fn nested_timestamp_millis_read_as_micros_succeeds() -> Result<(), DataFusionError> {
        let list = ListArray::new(
            Arc::new(Field::new(
                "element",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_000i64, 2_000, 3_000,
            ])),
            None,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![Arc::new(list)])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
            true,
        )]));
        let result = roundtrip(&batch, required_schema).await?;
        let values = result
            .column(0)
            .as_list::<i32>()
            .values()
            .as_primitive::<TimestampMicrosecondType>();
        assert_eq!(
            values.values().to_vec(),
            vec![1_000_000i64, 2_000_000, 3_000_000]
        );
        Ok(())
    }

    /// Positive: a requested nested field missing from the file still reads as null while
    /// the present field is passed through.
    #[tokio::test]
    async fn nested_missing_field_reads_as_null() -> Result<(), DataFusionError> {
        let batch = struct_batch(
            Field::new("x", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1, 2])),
        )?;
        let required_schema = struct_schema(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int64, true),
        ]);
        let result = roundtrip(&batch, required_schema).await?;
        let s = result.column(0).as_struct();
        assert_eq!(
            s.column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .values()
                .to_vec(),
            vec![1, 2]
        );
        assert_eq!(s.column(1).null_count(), 2);
        Ok(())
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

    /// Case-sensitive companion to `remap_field_id_missing_fake_renames_colliding_physical`: an
    /// ID-bearing logical field whose id is absent from the file must NOT fake-rename a physical
    /// field that merely shares its name in case-sensitive mode. Logical `[A(id=5), a(no id)]` read
    /// against a file `[a(id=9)]`: `A`'s id is missing, but the collision check folds to identity
    /// under case-sensitivity, so physical `a` is left alone for the non-id logical `a` to resolve.
    /// Fails with `left: "__comet_unmatched_field_id_1"` if the check reverts to
    /// `eq_ignore_ascii_case`.
    #[test]
    fn remap_field_id_missing_does_not_fake_rename_case_sensitive() {
        let logical = Arc::new(Schema::new(vec![
            Field::new("A", DataType::Int32, true).with_metadata(id_meta("5")),
            Field::new("a", DataType::Int32, true),
        ]));
        let physical = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(id_meta("9"))
        ]));
        let (remapped, _name_map) =
            super::remap_physical_schema(&logical, &physical, true, true, false).unwrap();
        assert_eq!(remapped.field(0).name(), "a");
    }

    /// Field-id precedence in the case-insensitive duplicate check: an explicit `ω` (id 2)
    /// reading a file that holds both `ω` (id 2) and `Ω` (id 1) must resolve by id (Spark's
    /// `matchIdField` selects the id before ever comparing names) rather than raising a
    /// duplicate-field error for the two names that fold together. Exercises the adapter's
    /// `rewrite` so the duplicate check runs. Fails with a `DuplicateFieldCaseInsensitive` error
    /// if id-resolved fields are not exempted from that check.
    #[test]
    fn duplicate_check_skipped_for_id_resolved_field() {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::PhysicalExpr;
        let logical = Arc::new(Schema::new(vec![
            Field::new("ω", DataType::Int32, true).with_metadata(id_meta("2"))
        ]));
        // The file holds both `Ω` (id 1) and `ω` (id 2); their names fold together.
        let physical = Arc::new(Schema::new(vec![
            Field::new("Ω", DataType::Int32, true).with_metadata(id_meta("1")),
            Field::new("ω", DataType::Int32, true).with_metadata(id_meta("2")),
        ]));

        let mut opts = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        opts.case_sensitive = false;
        opts.use_field_id = true;
        let adapter = SparkPhysicalExprAdapterFactory::new(opts, None)
            .create(Arc::clone(&logical), Arc::clone(&physical))
            .unwrap();

        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ω", 0));
        let rewritten = adapter.rewrite(expr);
        assert!(
            rewritten.is_ok(),
            "id-resolved read must not raise a duplicate-field error: {:?}",
            rewritten.err()
        );
    }
}
