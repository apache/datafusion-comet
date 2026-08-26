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
use arrow::{
    array::{
        make_array, Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, LargeListArray,
        ListArray, ListLikeArray, MapArray, StructArray, TimestampMicrosecondArray,
        TimestampMillisecondArray,
    },
    buffer::NullBuffer,
    compute::{cast, cast_with_options, CastOptions},
    datatypes::{DataType, FieldRef, Schema, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::{
    execution::serde::is_variant_field,
    parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions},
};
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::ScalarValue;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use parquet::variant::{
    unshred_variant, BorrowedShreddingState, ListBuilder, MetadataBuilder, ObjectBuilder,
    ParentState, ReadOnlyMetadataBuilder, ValueBuilder, Variant, VariantArray, VariantBuilder,
    VariantDecimal4, VariantDecimal8, VariantMetadata, WritableMetadataBuilder,
};
use std::{
    collections::HashSet,
    fmt::{self, Display},
    hash::Hash,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

/// Returns true if two DataTypes are structurally equivalent (same data layout)
/// but may differ in field names within nested types.
fn types_differ_only_in_field_names(physical: &DataType, logical: &DataType) -> bool {
    match (physical, logical) {
        (DataType::List(pf), DataType::List(lf)) => {
            pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::LargeList(pf), DataType::LargeList(lf)) => {
            pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::Map(pf, p_sorted), DataType::Map(lf, l_sorted)) => {
            p_sorted == l_sorted
                && pf.is_nullable() == lf.is_nullable()
                && (pf.data_type() == lf.data_type()
                    || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
        }
        (DataType::Struct(pfields), DataType::Struct(lfields)) => {
            // For Struct types, field names are semantically meaningful (they
            // identify different columns), so we require name equality here.
            // This distinguishes from List/Map wrapper field names ("item" vs
            // "element") which are purely cosmetic.
            pfields.len() == lfields.len()
                && pfields.iter().zip(lfields.iter()).all(|(pf, lf)| {
                    pf.name() == lf.name()
                        && pf.is_nullable() == lf.is_nullable()
                        && (pf.data_type() == lf.data_type()
                            || types_differ_only_in_field_names(pf.data_type(), lf.data_type()))
                })
        }
        _ => false,
    }
}

/// Recursively relabel an array so its DataType matches `target_type`.
/// This only changes metadata (field names, nullability flags in nested fields);
/// it does NOT change the underlying buffer data.
fn relabel_array(array: ArrayRef, target_type: &DataType) -> ArrayRef {
    if array.data_type() == target_type {
        return array;
    }
    match target_type {
        DataType::List(target_field) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = relabel_array(Arc::clone(list.values()), target_field.data_type());
            Arc::new(ListArray::new(
                Arc::clone(target_field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(target_field) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = relabel_array(Arc::clone(list.values()), target_field.data_type());
            Arc::new(LargeListArray::new(
                Arc::clone(target_field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        DataType::Map(target_entries_field, sorted) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = relabel_array(
                Arc::new(map.entries().clone()),
                target_entries_field.data_type(),
            );
            let entries_struct = entries.as_any().downcast_ref::<StructArray>().unwrap();
            Arc::new(MapArray::new(
                Arc::clone(target_entries_field),
                map.offsets().clone(),
                entries_struct.clone(),
                map.nulls().cloned(),
                *sorted,
            ))
        }
        DataType::Struct(target_fields) => {
            let struct_arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns: Vec<ArrayRef> = target_fields
                .iter()
                .zip(struct_arr.columns())
                .map(|(tf, col)| relabel_array(Arc::clone(col), tf.data_type()))
                .collect();
            Arc::new(StructArray::new(
                target_fields.clone(),
                columns,
                struct_arr.nulls().cloned(),
            ))
        }
        // Primitive types - shallow swap is safe
        _ => {
            let data = array.to_data();
            let new_data = data
                .into_builder()
                .data_type(target_type.clone())
                .build()
                .expect("relabel_array: data layout must be compatible");
            make_array(new_data)
        }
    }
}

/// Casts a Timestamp(Microsecond) array to Timestamp(Millisecond) by dividing values by 1000.
/// Preserves the timezone from the target type.
fn cast_timestamp_micros_to_millis_array(
    array: &ArrayRef,
    target_tz: Option<Arc<str>>,
) -> ArrayRef {
    let micros_array = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("Expected TimestampMicrosecondArray");

    let millis_values: TimestampMillisecondArray =
        arrow::compute::kernels::arity::unary(micros_array, |v| v / 1000);

    // Apply timezone if present
    let result = if let Some(tz) = target_tz {
        millis_values.with_timezone(tz)
    } else {
        millis_values
    };

    Arc::new(result)
}

/// Casts a Timestamp(Microsecond) scalar to Timestamp(Millisecond) by dividing the value by 1000.
/// Preserves the timezone from the target type.
fn cast_timestamp_micros_to_millis_scalar(
    opt_val: Option<i64>,
    target_tz: Option<Arc<str>>,
) -> ScalarValue {
    let new_val = opt_val.map(|v| v / 1000);
    ScalarValue::TimestampMillisecond(new_val, target_tz)
}

fn normalize_variant_array(
    array: &ArrayRef,
    target_field: &FieldRef,
) -> DataFusionResult<ArrayRef> {
    let DataType::Struct(fields) = target_field.data_type() else {
        return Err(DataFusionError::Execution(
            "Variant extension field must use Struct storage".to_string(),
        ));
    };
    if fields.len() != 2
        || fields[0].name() != "value"
        || fields[1].name() != "metadata"
        || fields
            .iter()
            .any(|field| field.data_type() != &DataType::Binary)
    {
        return Err(DataFusionError::Execution(
            "Variant output must contain Binary children [value, metadata]".to_string(),
        ));
    }

    let array = decode_variant_metadata_dictionary(array)?;
    let array = normalize_variant_typed_value(&array)?;
    let variant = VariantArray::try_new(array.as_ref())?;
    let was_shredded = variant.typed_value_field().is_some();
    let unshredded = unshred_variant_for_spark(&variant)?;
    let value = unshredded.value_field().ok_or_else(|| {
        DataFusionError::Execution("Unshredded Variant is missing its value field".to_string())
    })?;
    let value = cast(value.as_ref(), &DataType::Binary)?;
    let metadata = cast(unshredded.metadata_field().as_ref(), &DataType::Binary)?;
    let (value, metadata) = if was_shredded {
        rebuild_shredded_variant_for_spark(&variant, &value, &metadata, unshredded.inner().nulls())?
    } else {
        let value = reorder_variant_values(
            &value,
            &metadata,
            unshredded.inner().nulls(),
            VariantObjectKeyOrder::SparkUtf16,
            false,
        )?;
        (value, metadata)
    };
    let output = StructArray::try_new(
        fields.clone(),
        vec![value, metadata],
        unshredded.inner().nulls().cloned(),
    )?;
    Ok(Arc::new(output))
}

fn unshred_variant_for_spark(variant: &VariantArray) -> DataFusionResult<VariantArray> {
    let first =
        prepare_variant_for_unshredding(variant).and_then(|array| Ok(unshred_variant(&array)?));
    let first_error = match first {
        Ok(array) => return Ok(array),
        Err(error) => error,
    };
    let Some(variant) = canonicalize_spark_empty_key_metadata(variant)? else {
        return Err(first_error);
    };
    let variant = prepare_variant_for_unshredding(&variant)?;
    Ok(unshred_variant(&variant)?)
}

fn normalize_variant_type(data_type: &DataType) -> Option<DataType> {
    fn normalize_field(field: &FieldRef) -> Option<FieldRef> {
        normalize_variant_type(field.data_type())
            .map(|data_type| Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    match data_type {
        DataType::UInt8 => Some(DataType::Int16),
        DataType::UInt16 => Some(DataType::Int32),
        DataType::UInt32 => Some(DataType::Int64),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
        }
        DataType::FixedSizeList(field, _) => Some(DataType::List(
            normalize_field(field).unwrap_or_else(|| Arc::clone(field)),
        )),
        DataType::List(field) => normalize_field(field).map(DataType::List),
        DataType::LargeList(field) => normalize_field(field).map(DataType::LargeList),
        DataType::ListView(field) => normalize_field(field).map(DataType::ListView),
        DataType::LargeListView(field) => normalize_field(field).map(DataType::LargeListView),
        DataType::Struct(fields) => {
            let mut changed = false;
            let fields = fields
                .iter()
                .map(|field| match normalize_field(field) {
                    Some(field) => {
                        changed = true;
                        field
                    }
                    None => Arc::clone(field),
                })
                .collect::<Vec<_>>();
            changed.then(|| DataType::Struct(fields.into()))
        }
        _ => None,
    }
}

/// Normalize Arrow types that Spark's Parquet reader accepts but `VariantArray` 58.4 rejects.
/// Parquet restores unsigned integers to Arrow unsigned arrays and millisecond timestamps at their
/// annotated unit; embedded Arrow schemas may also restore fixed-size lists. Spark widens the
/// integers and timestamps and treats fixed-size lists as ordinary Variant arrays.
/// arrow-rs #10416/#10417 would move this widening into `VariantArray`/`unshred_variant`; remove
/// the unsigned arms after that ships and Comet upgrades:
/// https://github.com/apache/arrow-rs/issues/10416
/// https://github.com/apache/arrow-rs/pull/10417
/// Arrow #50622/#50810 instead proposes removing unsigned `typed_value` mappings because the
/// Parquet Variant shredding table permits only signed integer fields. Until upstream resolves
/// that choice, keep this compatibility path for unsigned files Spark already reads:
/// https://github.com/apache/arrow/issues/50622
/// https://github.com/apache/arrow/pull/50810
fn normalize_variant_typed_value(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
        return Ok(Arc::clone(array));
    };
    let Some((typed_value_index, typed_value_field)) = struct_array
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == "typed_value")
    else {
        return Ok(Arc::clone(array));
    };
    let Some(data_type) = normalize_variant_type(typed_value_field.data_type()) else {
        return Ok(Arc::clone(array));
    };

    let mut fields = struct_array.fields().iter().cloned().collect::<Vec<_>>();
    fields[typed_value_index] = Arc::new(
        typed_value_field
            .as_ref()
            .clone()
            .with_data_type(data_type.clone()),
    );
    let mut columns = struct_array.columns().to_vec();
    columns[typed_value_index] = cast_with_options(
        columns[typed_value_index].as_ref(),
        &data_type,
        &DEFAULT_CAST_OPTIONS,
    )?;
    Ok(Arc::new(StructArray::try_new(
        fields.into(),
        columns,
        struct_array.nulls().cloned(),
    )?))
}

/// Arrow's unshredder fully validates any residual `value` in a partially shredded object. Spark
/// writes object keys in Java UTF-16 order, so put that residual value in Arrow UTF-8 order only
/// while it passes through the upstream unshredder.
fn prepare_variant_for_unshredding(variant: &VariantArray) -> DataFusionResult<VariantArray> {
    let (Some(value), Some(_)) = (variant.value_field(), variant.typed_value_field()) else {
        return Ok(variant.clone());
    };

    let value = cast(value.as_ref(), &DataType::Binary)?;
    let metadata = cast(variant.metadata_field().as_ref(), &DataType::Binary)?;
    let value = reorder_variant_values(
        &value,
        &metadata,
        variant.inner().nulls(),
        VariantObjectKeyOrder::ArrowUtf8,
        true,
    )?;

    let value_index = variant
        .inner()
        .fields()
        .iter()
        .position(|field| field.name() == "value")
        .unwrap();
    let mut fields = variant.inner().fields().iter().cloned().collect::<Vec<_>>();
    fields[value_index] = Arc::new(
        fields[value_index]
            .as_ref()
            .clone()
            .with_data_type(DataType::Binary),
    );
    let mut columns = variant.inner().columns().to_vec();
    columns[value_index] = value;
    let array = StructArray::try_new(fields.into(), columns, variant.inner().nulls().cloned())?;
    Ok(VariantArray::try_new(&array)?)
}

/// Arrow 58.4 rejects Spark metadata dictionaries containing an empty object key because their
/// unsorted offsets can be equal. Rebuild only those rows before any residual value is traversed,
/// sorting the dictionary and remapping the residual value's field IDs at the same time.
/// https://github.com/apache/arrow-rs/pull/10352
fn canonicalize_spark_empty_key_metadata(
    variant: &VariantArray,
) -> DataFusionResult<Option<VariantArray>> {
    type Replacement = Option<(Vec<u8>, Option<Vec<u8>>)>;

    let metadata = cast(variant.metadata_field().as_ref(), &DataType::Binary)?;
    let metadata = metadata.as_binary::<i32>();
    let value = variant
        .value_field()
        .map(|value| cast(value.as_ref(), &DataType::Binary))
        .transpose()?;
    let value = value.as_ref().map(|value| value.as_binary::<i32>());
    let mut replacements = Vec::with_capacity(variant.len());
    let mut changed = false;

    for index in 0..variant.len() {
        if variant.inner().is_null(index) || metadata.is_null(index) {
            replacements.push(None);
            continue;
        }
        let metadata_bytes = metadata.value(index);
        if VariantMetadata::try_new(metadata_bytes).is_ok() {
            replacements.push(None);
            continue;
        }

        let replacement = catch_unwind(AssertUnwindSafe(|| -> Result<Replacement, ArrowError> {
            let old_metadata = VariantMetadata::new(metadata_bytes);
            let mut names = old_metadata
                .iter_try()
                .map(|name| name.map(str::to_string))
                .collect::<Result<Vec<_>, _>>()?;
            if !names.iter().any(String::is_empty) {
                return Ok(None);
            }
            names.sort_unstable();
            if names.windows(2).any(|names| names[0] == names[1]) {
                return Ok(None);
            }

            let mut builder =
                VariantBuilder::new().with_field_names(names.iter().map(String::as_str));
            match value {
                Some(value) if !value.is_null(index) => {
                    builder
                        .append_value(Variant::new_with_metadata(old_metadata, value.value(index)));
                    let (metadata, value) = builder.finish();
                    Ok(Some((metadata, Some(value))))
                }
                _ => Ok(Some((builder.finish().0, None))),
            }
        }))
        .map_err(|_| {
            DataFusionError::Execution(format!(
                "Invalid Variant metadata with an empty key at row {index}"
            ))
        })??;
        changed |= replacement.is_some();
        replacements.push(replacement);
    }

    if !changed {
        return Ok(None);
    }

    let mut metadata_builder = BinaryBuilder::new();
    let mut value_builder = value.map(|_| BinaryBuilder::new());
    for (index, replacement) in replacements.iter().enumerate() {
        match replacement {
            Some((metadata, value)) => {
                metadata_builder.append_value(metadata);
                if let Some(builder) = &mut value_builder {
                    match value {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
            }
            None => {
                if metadata.is_null(index) {
                    metadata_builder.append_null();
                } else {
                    metadata_builder.append_value(metadata.value(index));
                }
                if let (Some(value), Some(builder)) = (value, &mut value_builder) {
                    if value.is_null(index) {
                        builder.append_null();
                    } else {
                        builder.append_value(value.value(index));
                    }
                }
            }
        }
    }

    let mut fields = variant.inner().fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = variant.inner().columns().to_vec();
    let metadata_index = fields
        .iter()
        .position(|field| field.name() == "metadata")
        .unwrap();
    fields[metadata_index] = Arc::new(
        fields[metadata_index]
            .as_ref()
            .clone()
            .with_data_type(DataType::Binary),
    );
    columns[metadata_index] = Arc::new(metadata_builder.finish());
    if let Some(mut value_builder) = value_builder {
        let value_index = fields
            .iter()
            .position(|field| field.name() == "value")
            .unwrap();
        fields[value_index] = Arc::new(
            fields[value_index]
                .as_ref()
                .clone()
                .with_data_type(DataType::Binary),
        );
        columns[value_index] = Arc::new(value_builder.finish());
    }
    let array = StructArray::try_new(fields.into(), columns, variant.inner().nulls().cloned())?;
    Ok(Some(VariantArray::try_new(&array)?))
}

/// Arrow-rs parquet-variant-compute allows dictionary-encoded metadata in its contract, but 58.4's
/// `VariantArray::try_new` validates only Binary, LargeBinary, and BinaryView. Decode just that
/// child and keep the physical struct otherwise unchanged.
/// https://github.com/apache/arrow-rs/blob/58.4.0/parquet-variant-compute/src/variant_array.rs#L276-L310
/// Upstream issue: https://github.com/apache/arrow-rs/issues/10802
fn decode_variant_metadata_dictionary(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
        return Ok(Arc::clone(array));
    };
    let Some((metadata_index, metadata_field)) = struct_array
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == "metadata")
    else {
        return Ok(Arc::clone(array));
    };
    let DataType::Dictionary(_, value_type) = metadata_field.data_type() else {
        return Ok(Arc::clone(array));
    };

    let decoded = cast(struct_array.column(metadata_index).as_ref(), value_type)?;
    let mut fields = struct_array.fields().iter().cloned().collect::<Vec<_>>();
    fields[metadata_index] = Arc::new(
        metadata_field
            .as_ref()
            .clone()
            .with_data_type(decoded.data_type().clone()),
    );
    let mut columns = struct_array.columns().to_vec();
    columns[metadata_index] = decoded;
    Ok(Arc::new(StructArray::try_new(
        fields.into(),
        columns,
        struct_array.nulls().cloned(),
    )?))
}

/// Supplies sort-only field names whose Rust ordering matches Java `String.compareTo` ordering.
/// The original metadata dictionary still supplies the field IDs written to the Variant value.
#[derive(Debug)]
struct SparkMetadataBuilder<'a, 'm> {
    metadata: &'a VariantMetadata<'m>,
    sort_keys: Vec<String>,
}

impl<'a, 'm> SparkMetadataBuilder<'a, 'm> {
    fn new(metadata: &'a VariantMetadata<'m>) -> Self {
        let sort_keys = metadata
            .iter()
            .map(|field_name| {
                field_name
                    .encode_utf16()
                    .map(|unit| char::from_u32(0x10000 + u32::from(unit)).unwrap())
                    .collect()
            })
            .collect();
        Self {
            metadata,
            sort_keys,
        }
    }
}

impl MetadataBuilder for SparkMetadataBuilder<'_, '_> {
    fn try_upsert_field_name(&mut self, field_name: &str) -> Result<u32, ArrowError> {
        self.metadata
            .get_entry(field_name)
            .map(|(field_id, _)| field_id)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Field name '{field_name}' not found in metadata dictionary"
                ))
            })
    }

    fn field_name(&self, field_id: usize) -> &str {
        &self.sort_keys[field_id]
    }

    fn num_field_names(&self) -> usize {
        self.metadata.len()
    }

    fn truncate_field_names(&mut self, new_size: usize) {
        debug_assert_eq!(self.metadata.len(), new_size);
    }

    fn finish(&mut self) -> usize {
        self.metadata.size()
    }
}

#[derive(Clone, Copy)]
enum VariantObjectKeyOrder {
    ArrowUtf8,
    SparkUtf16,
}

fn is_compatible_variant(variant: &Variant<'_, '_>, order: VariantObjectKeyOrder) -> bool {
    match variant {
        Variant::Object(object) => {
            let mut previous = None;
            object.iter().all(|(name, value)| {
                let ordered = previous
                    .map(|previous: &str| match order {
                        VariantObjectKeyOrder::ArrowUtf8 => previous <= name,
                        VariantObjectKeyOrder::SparkUtf16 => {
                            previous.encode_utf16().cmp(name.encode_utf16())
                                != std::cmp::Ordering::Greater
                        }
                    })
                    .unwrap_or(true);
                previous = Some(name);
                ordered && is_compatible_variant(&value, order)
            })
        }
        Variant::List(list) => list
            .iter()
            .all(|value| is_compatible_variant(&value, order)),
        _ => true,
    }
}

fn compact_spark_integer(value: i64) -> Variant<'static, 'static> {
    if let Ok(value) = i8::try_from(value) {
        Variant::Int8(value)
    } else if let Ok(value) = i16::try_from(value) {
        Variant::Int16(value)
    } else if let Ok(value) = i32::try_from(value) {
        Variant::Int32(value)
    } else {
        Variant::Int64(value)
    }
}

fn compact_spark_typed_variant<'m, 'v>(variant: Variant<'m, 'v>) -> Variant<'m, 'v> {
    match variant {
        Variant::Int16(value) => compact_spark_integer(value.into()),
        Variant::Int32(value) => compact_spark_integer(value.into()),
        Variant::Int64(value) => compact_spark_integer(value),
        Variant::Decimal8(value) => i32::try_from(value.integer())
            .ok()
            .and_then(|integer| VariantDecimal4::try_new(integer, value.scale()).ok())
            .map(Variant::Decimal4)
            .unwrap_or(Variant::Decimal8(value)),
        Variant::Decimal16(value) => i32::try_from(value.integer())
            .ok()
            .and_then(|integer| VariantDecimal4::try_new(integer, value.scale()).ok())
            .map(Variant::Decimal4)
            .or_else(|| {
                i64::try_from(value.integer())
                    .ok()
                    .and_then(|integer| VariantDecimal8::try_new(integer, value.scale()).ok())
                    .map(Variant::Decimal8)
            })
            .unwrap_or(Variant::Decimal16(value)),
        Variant::Float(value) if value.is_nan() => Variant::Float(f32::from_bits(0x7fc0_0000)),
        Variant::Double(value) if value.is_nan() => {
            Variant::Double(f64::from_bits(0x7ff8_0000_0000_0000))
        }
        variant => variant,
    }
}

/// Re-encode a residual Variant against `metadata`. Scalar widths are intentionally preserved,
/// matching Spark's `VariantBuilder.appendVariant` behavior.
fn spark_variant_bytes(
    metadata: &VariantMetadata<'_>,
    variant: Variant<'_, '_>,
) -> Result<Vec<u8>, ArrowError> {
    let mut value_builder = ValueBuilder::new();
    match variant {
        Variant::Object(object) => {
            let mut metadata_builder = SparkMetadataBuilder::new(metadata);
            let mut builder = ObjectBuilder::new(
                ParentState::variant(&mut value_builder, &mut metadata_builder),
                false,
            );
            for (name, value) in object.iter() {
                let value = spark_variant_bytes(metadata, value)?;
                builder
                    .try_insert_bytes(name, Variant::new_with_metadata(metadata.clone(), &value))?;
            }
            builder.finish();
        }
        Variant::List(list) => {
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
            let mut builder = ListBuilder::new(
                ParentState::variant(&mut value_builder, &mut metadata_builder),
                false,
            );
            for value in list.iter() {
                let value = spark_variant_bytes(metadata, value)?;
                builder.append_value_bytes(Variant::new_with_metadata(metadata.clone(), &value));
            }
            builder.finish();
        }
        variant => {
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
            ValueBuilder::try_append_variant(
                ParentState::variant(&mut value_builder, &mut metadata_builder),
                variant,
            )?;
        }
    }
    Ok(value_builder.into_inner())
}

fn variant_binary_value(array: &ArrayRef, index: usize) -> Result<Option<&[u8]>, ArrowError> {
    if array.is_null(index) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().value(index),
        DataType::LargeBinary => array.as_binary::<i64>().value(index),
        DataType::BinaryView => array.as_binary_view().value(index),
        data_type => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Variant value must be binary-like, got {data_type}"
            )))
        }
    };
    Ok(Some(value))
}

fn shredding_state_has_value(state: &BorrowedShreddingState<'_>, index: usize) -> bool {
    state
        .typed_value_field()
        .is_some_and(|array| array.is_valid(index))
        || state
            .value_field()
            .is_some_and(|array| array.is_valid(index))
}

fn collect_spark_field_name(name: &str, field_names: &mut Vec<String>, seen: &mut HashSet<String>) {
    if seen.insert(name.to_string()) {
        field_names.push(name.to_string());
    }
}

fn collect_residual_field_names(
    variant: Variant<'_, '_>,
    field_names: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<(), ArrowError> {
    match variant {
        Variant::Object(object) => {
            for (name, value) in object.iter() {
                collect_spark_field_name(name, field_names, seen);
                collect_residual_field_names(value, field_names, seen)?;
            }
        }
        Variant::List(list) => {
            for value in list.iter() {
                collect_residual_field_names(value, field_names, seen)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_list_field_names<L: ListLikeArray>(
    list: &L,
    index: usize,
    source_metadata: &VariantMetadata<'_>,
    field_names: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<(), ArrowError> {
    let values = list.values().as_struct();
    let state = BorrowedShreddingState::try_from(values)?;
    for element_index in list.element_range(index) {
        collect_shredded_field_names(
            state.clone(),
            element_index,
            source_metadata,
            field_names,
            seen,
        )?;
    }
    Ok(())
}

fn collect_shredded_field_names(
    state: BorrowedShreddingState<'_>,
    index: usize,
    source_metadata: &VariantMetadata<'_>,
    field_names: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<(), ArrowError> {
    let Some(typed_value) = state
        .typed_value_field()
        .filter(|array| array.is_valid(index))
    else {
        if let Some(value) = state.value_field() {
            if let Some(value) = variant_binary_value(value, index)? {
                collect_residual_field_names(
                    Variant::new_with_metadata(source_metadata.clone(), value),
                    field_names,
                    seen,
                )?;
            }
        }
        return Ok(());
    };

    match typed_value.data_type() {
        DataType::Struct(_) => {
            let object = typed_value.as_struct();
            for (field, column) in object.fields().iter().zip(object.columns()) {
                let child = column.as_struct_opt().ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "Invalid shredded Variant object field '{}': expected Struct, got {}",
                        field.name(),
                        column.data_type()
                    ))
                })?;
                if child.is_null(index) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Shredded Variant object field '{}' is null",
                        field.name()
                    )));
                }
                let child_state = BorrowedShreddingState::try_from(child)?;
                if shredding_state_has_value(&child_state, index) {
                    collect_spark_field_name(field.name(), field_names, seen);
                    collect_shredded_field_names(
                        child_state,
                        index,
                        source_metadata,
                        field_names,
                        seen,
                    )?;
                }
            }

            if let Some(value) = state.value_field() {
                if let Some(value) = variant_binary_value(value, index)? {
                    let Variant::Object(residual) =
                        Variant::new_with_metadata(source_metadata.clone(), value)
                    else {
                        return Err(ArrowError::InvalidArgumentError(
                            "Partially shredded Variant object has a non-object value".to_string(),
                        ));
                    };
                    for (name, value) in residual.iter() {
                        if object.fields().iter().any(|field| field.name() == name) {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Variant field '{name}' appears in both value and typed_value"
                            )));
                        }
                        collect_spark_field_name(name, field_names, seen);
                        collect_residual_field_names(value, field_names, seen)?;
                    }
                }
            }
        }
        DataType::List(_) => collect_list_field_names(
            typed_value.as_list::<i32>(),
            index,
            source_metadata,
            field_names,
            seen,
        )?,
        DataType::LargeList(_) => collect_list_field_names(
            typed_value.as_list::<i64>(),
            index,
            source_metadata,
            field_names,
            seen,
        )?,
        DataType::ListView(_) => collect_list_field_names(
            typed_value.as_list_view::<i32>(),
            index,
            source_metadata,
            field_names,
            seen,
        )?,
        DataType::LargeListView(_) => collect_list_field_names(
            typed_value.as_list_view::<i64>(),
            index,
            source_metadata,
            field_names,
            seen,
        )?,
        _ => {}
    }
    Ok(())
}

fn spark_typed_variant_bytes(
    metadata: &VariantMetadata<'_>,
    variant: Variant<'_, '_>,
) -> Result<Vec<u8>, ArrowError> {
    let mut value_builder = ValueBuilder::new();
    let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
    ValueBuilder::try_append_variant(
        ParentState::variant(&mut value_builder, &mut metadata_builder),
        compact_spark_typed_variant(variant),
    )?;
    Ok(value_builder.into_inner())
}

fn spark_list_bytes<L: ListLikeArray>(
    list: &L,
    index: usize,
    semantic: Variant<'_, '_>,
    source_metadata: &VariantMetadata<'_>,
    target_metadata: &VariantMetadata<'_>,
) -> Result<Vec<u8>, ArrowError> {
    let Variant::List(semantic) = semantic else {
        return Err(ArrowError::InvalidArgumentError(
            "Shredded Variant list did not unshred to a list".to_string(),
        ));
    };
    let semantic = semantic.iter_try().collect::<Result<Vec<_>, _>>()?;
    let element_range = list.element_range(index);
    if element_range.len() != semantic.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Shredded Variant list length changed while unshredding".to_string(),
        ));
    }

    let values = list.values().as_struct();
    let state = BorrowedShreddingState::try_from(values)?;
    let mut elements = Vec::with_capacity(semantic.len());
    for (element_index, semantic) in element_range.zip(semantic) {
        elements.push(spark_shredded_variant_bytes(
            state.clone(),
            element_index,
            semantic,
            source_metadata,
            target_metadata,
        )?);
    }

    let mut value_builder = ValueBuilder::new();
    let mut metadata_builder = ReadOnlyMetadataBuilder::new(target_metadata);
    let mut builder = ListBuilder::new(
        ParentState::variant(&mut value_builder, &mut metadata_builder),
        false,
    );
    for element in elements {
        builder.append_value_bytes(Variant::new_with_metadata(
            target_metadata.clone(),
            &element,
        ));
    }
    builder.finish();
    Ok(value_builder.into_inner())
}

fn spark_object_bytes(
    state: BorrowedShreddingState<'_>,
    object: &StructArray,
    index: usize,
    semantic: Variant<'_, '_>,
    source_metadata: &VariantMetadata<'_>,
    target_metadata: &VariantMetadata<'_>,
) -> Result<Vec<u8>, ArrowError> {
    let Variant::Object(semantic) = semantic else {
        return Err(ArrowError::InvalidArgumentError(
            "Shredded Variant object did not unshred to an object".to_string(),
        ));
    };
    let mut entries = Vec::new();
    for (field, column) in object.fields().iter().zip(object.columns()) {
        let child = column.as_struct_opt().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid shredded Variant object field '{}': expected Struct, got {}",
                field.name(),
                column.data_type()
            ))
        })?;
        if child.is_null(index) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Shredded Variant object field '{}' is null",
                field.name()
            )));
        }
        let child_state = BorrowedShreddingState::try_from(child)?;
        if shredding_state_has_value(&child_state, index) {
            let value = semantic.get(field.name()).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Unshredded Variant is missing field '{}'",
                    field.name()
                ))
            })?;
            entries.push((
                field.name().to_string(),
                spark_shredded_variant_bytes(
                    child_state,
                    index,
                    value,
                    source_metadata,
                    target_metadata,
                )?,
            ));
        }
    }

    if let Some(value) = state.value_field() {
        if let Some(value) = variant_binary_value(value, index)? {
            let Variant::Object(residual) =
                Variant::new_with_metadata(source_metadata.clone(), value)
            else {
                return Err(ArrowError::InvalidArgumentError(
                    "Partially shredded Variant object has a non-object value".to_string(),
                ));
            };
            for (name, value) in residual.iter() {
                if object.fields().iter().any(|field| field.name() == name) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Variant field '{name}' appears in both value and typed_value"
                    )));
                }
                entries.push((
                    name.to_string(),
                    spark_variant_bytes(target_metadata, value)?,
                ));
            }
        }
    }

    let mut value_builder = ValueBuilder::new();
    let mut metadata_builder = SparkMetadataBuilder::new(target_metadata);
    let mut builder = ObjectBuilder::new(
        ParentState::variant(&mut value_builder, &mut metadata_builder),
        false,
    );
    for (name, value) in entries {
        builder.try_insert_bytes(
            &name,
            Variant::new_with_metadata(target_metadata.clone(), &value),
        )?;
    }
    builder.finish();
    Ok(value_builder.into_inner())
}

// ponytail: recursive child buffers can be O(depth²); use a streaming encoder only if deeply
// nested Variant profiles show this compatibility path is a bottleneck.
fn spark_shredded_variant_bytes(
    state: BorrowedShreddingState<'_>,
    index: usize,
    semantic: Variant<'_, '_>,
    source_metadata: &VariantMetadata<'_>,
    target_metadata: &VariantMetadata<'_>,
) -> Result<Vec<u8>, ArrowError> {
    let Some(typed_value) = state
        .typed_value_field()
        .filter(|array| array.is_valid(index))
    else {
        return match state.value_field() {
            Some(value) => match variant_binary_value(value, index)? {
                Some(value) => spark_variant_bytes(
                    target_metadata,
                    Variant::new_with_metadata(source_metadata.clone(), value),
                ),
                None => Err(ArrowError::InvalidArgumentError(
                    "Shredded Variant has neither value nor typed_value".to_string(),
                )),
            },
            None => Err(ArrowError::InvalidArgumentError(
                "Shredded Variant has neither value nor typed_value".to_string(),
            )),
        };
    };

    match typed_value.data_type() {
        DataType::Struct(_) => spark_object_bytes(
            state,
            typed_value.as_struct(),
            index,
            semantic,
            source_metadata,
            target_metadata,
        ),
        DataType::List(_) => spark_list_bytes(
            typed_value.as_list::<i32>(),
            index,
            semantic,
            source_metadata,
            target_metadata,
        ),
        DataType::LargeList(_) => spark_list_bytes(
            typed_value.as_list::<i64>(),
            index,
            semantic,
            source_metadata,
            target_metadata,
        ),
        DataType::ListView(_) => spark_list_bytes(
            typed_value.as_list_view::<i32>(),
            index,
            semantic,
            source_metadata,
            target_metadata,
        ),
        DataType::LargeListView(_) => spark_list_bytes(
            typed_value.as_list_view::<i64>(),
            index,
            semantic,
            source_metadata,
            target_metadata,
        ),
        _ => spark_typed_variant_bytes(target_metadata, semantic),
    }
}

/// Arrow's unshredder preserves the source metadata but rebuilds object slots in UTF-8 order.
/// Rebuild from the physical shredding state so Spark's metadata insertion order, UTF-16 object
/// headers, typed scalar widths, and residual scalar bytes all remain compatible.
fn rebuild_shredded_variant_for_spark(
    source: &VariantArray,
    value: &ArrayRef,
    metadata: &ArrayRef,
    parent_nulls: Option<&NullBuffer>,
) -> DataFusionResult<(ArrayRef, ArrayRef)> {
    let source_metadata = cast(source.metadata_field().as_ref(), &DataType::Binary)?;
    let source_metadata = source_metadata.as_binary::<i32>();
    let source_state = source.shredding_state().borrow();
    let value = value.as_binary::<i32>();
    let metadata = metadata.as_binary::<i32>();
    let mut value_output = BinaryBuilder::new();
    let mut metadata_output = BinaryBuilder::new();

    for index in 0..value.len() {
        if parent_nulls.is_some_and(|nulls| nulls.is_null(index)) {
            value_output.append_null();
            metadata_output.append_null();
            continue;
        }
        if value.is_null(index) || metadata.is_null(index) {
            return Err(DataFusionError::Execution(format!(
                "Variant value or metadata is null at row {index}"
            )));
        }

        let (rebuilt_value, rebuilt_metadata) = catch_unwind(AssertUnwindSafe(
            || -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
                let source_metadata = VariantMetadata::new(source_metadata.value(index));
                let semantic_metadata = VariantMetadata::try_new(metadata.value(index))?;
                let semantic = Variant::new_with_metadata(semantic_metadata, value.value(index));
                let mut field_names = Vec::new();
                collect_shredded_field_names(
                    source_state.clone(),
                    index,
                    &source_metadata,
                    &mut field_names,
                    &mut HashSet::new(),
                )
                .map_err(|error| {
                    ArrowError::InvalidArgumentError(format!(
                        "Failed to collect Spark Variant metadata: {error}"
                    ))
                })?;

                let mut metadata_builder =
                    WritableMetadataBuilder::from_iter(field_names.iter().map(String::as_str));
                metadata_builder.finish();
                let mut rebuilt_metadata = metadata_builder.into_inner();
                // Spark's VariantBuilder never marks its insertion-ordered dictionary as sorted.
                rebuilt_metadata[0] &= !0x10;
                let target = VariantMetadata::new(&rebuilt_metadata);
                let rebuilt_value = spark_shredded_variant_bytes(
                    source_state.clone(),
                    index,
                    semantic,
                    &source_metadata,
                    &target,
                )
                .map_err(|error| {
                    ArrowError::InvalidArgumentError(format!(
                        "Failed to rebuild Spark Variant value: {error}"
                    ))
                })?;
                Ok((rebuilt_value, rebuilt_metadata))
            },
        ))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid shredded Variant at row {index}"))
        })??;
        value_output.append_value(rebuilt_value);
        metadata_output.append_value(rebuilt_metadata);
    }

    Ok((
        Arc::new(value_output.finish()),
        Arc::new(metadata_output.finish()),
    ))
}

/// Reorder object keys for either Arrow's UTF-8 order or Spark's Java UTF-16 order. Preserve
/// already-compatible values byte-for-byte and retain the original metadata dictionary.
/// SPARK-58949 tracks this mismatch and legacy compatibility. The metadata dictionary's sorted
/// flag affects dictionary lookup, not object-entry ordering; Spark's builder and lookup must
/// agree while continuing to read Variant values already written by Spark 4.x in UTF-16 order.
/// https://issues.apache.org/jira/browse/SPARK-58949
/// https://github.com/apache/parquet-java/issues/3735
fn reorder_variant_values(
    value: &ArrayRef,
    metadata: &ArrayRef,
    parent_nulls: Option<&NullBuffer>,
    order: VariantObjectKeyOrder,
    allow_null_value: bool,
) -> DataFusionResult<ArrayRef> {
    let value = value.as_any().downcast_ref::<BinaryArray>().unwrap();
    let metadata = metadata.as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut output = BinaryBuilder::new();

    for index in 0..value.len() {
        if parent_nulls.is_some_and(|nulls| nulls.is_null(index)) {
            output.append_null();
            continue;
        }
        if value.is_null(index) {
            if allow_null_value {
                output.append_null();
                continue;
            }
            return Err(DataFusionError::Execution(format!(
                "Variant value is null at row {index}"
            )));
        }
        if metadata.is_null(index) {
            return Err(DataFusionError::Execution(format!(
                "Variant metadata is null at row {index}"
            )));
        }

        let rebuilt = catch_unwind(AssertUnwindSafe(|| -> DataFusionResult<Option<Vec<u8>>> {
            // Spark encodes empty object keys with equal metadata offsets, which Arrow 58.4's
            // full validator rejects. Keep shallow parsing and all accesses inside this boundary.
            // https://github.com/apache/arrow-rs/blob/58.4.0/parquet-variant/src/variant/metadata.rs#L307-L317
            // Upstream fix: https://github.com/apache/arrow-rs/pull/10352
            let metadata = VariantMetadata::new(metadata.value(index));
            let variant = Variant::new_with_metadata(metadata.clone(), value.value(index));
            if is_compatible_variant(&variant, order) {
                return Ok(None);
            }
            let value = match order {
                VariantObjectKeyOrder::ArrowUtf8 => {
                    let mut value_builder = ValueBuilder::new();
                    let mut metadata_builder = ReadOnlyMetadataBuilder::new(&metadata);
                    ValueBuilder::try_append_variant(
                        ParentState::variant(&mut value_builder, &mut metadata_builder),
                        variant,
                    )?;
                    value_builder.into_inner()
                }
                VariantObjectKeyOrder::SparkUtf16 => spark_variant_bytes(&metadata, variant)?,
            };
            Ok(Some(value))
        }))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid Variant value at row {index}"))
        })??;
        output.append_value(rebuilt.as_deref().unwrap_or_else(|| value.value(index)));
    }

    Ok(Arc::new(output.finish()))
}

#[derive(Debug, Clone, Eq)]
pub struct CometCastColumnExpr {
    /// The physical expression producing the value to cast.
    expr: Arc<dyn PhysicalExpr>,
    /// The physical field of the input column.
    input_physical_field: FieldRef,
    /// The field type required by query
    target_field: FieldRef,
    /// Options forwarded to [`cast_column`].
    cast_options: CastOptions<'static>,
    /// Spark parquet options for complex nested type conversions.
    /// When present, enables `spark_parquet_convert` as a fallback.
    parquet_options: Option<SparkParquetOptions>,
}

// Manually derive `PartialEq`/`Hash` as `Arc<dyn PhysicalExpr>` does not
// implement these traits by default for the trait object.
impl PartialEq for CometCastColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_physical_field.eq(&other.input_physical_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
            && self.parquet_options.eq(&other.parquet_options)
    }
}

impl Hash for CometCastColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_physical_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
        self.parquet_options.hash(state);
    }
}

impl CometCastColumnExpr {
    /// Create a new [`CometCastColumnExpr`].
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        physical_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            input_physical_field: physical_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
            parquet_options: None,
        }
    }

    /// Set Spark parquet options to enable complex nested type conversions.
    pub fn with_parquet_options(mut self, options: SparkParquetOptions) -> Self {
        self.parquet_options = Some(options);
        self
    }
}

impl Display for CometCastColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "COMET_CAST_COLUMN({} AS {})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for CometCastColumnExpr {
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;

        if is_variant_field(&self.target_field) {
            return match value {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(normalize_variant_array(
                    &array,
                    &self.target_field,
                )?)),
                ColumnarValue::Scalar(_) => Err(DataFusionError::Execution(
                    "Variant Parquet projection requires an array".to_string(),
                )),
            };
        }

        // Use == (PartialEq) instead of equals_datatype because equals_datatype
        // ignores field names in nested types (Struct, List, Map). We need to detect
        // when field names differ (e.g., Struct("a","b") vs Struct("c","d")) so that
        // we can apply spark_parquet_convert for field-name-based selection.
        if value.data_type() == *self.target_field.data_type() {
            return Ok(value);
        }

        let input_physical_field = self.input_physical_field.data_type();
        let target_field = self.target_field.data_type();

        // Handle specific type conversions with custom casts
        match (input_physical_field, target_field) {
            // Timestamp(Microsecond) -> Timestamp(Millisecond)
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Timestamp(TimeUnit::Millisecond, target_tz),
            ) => match value {
                ColumnarValue::Array(array) => {
                    let casted = cast_timestamp_micros_to_millis_array(&array, target_tz.clone());
                    Ok(ColumnarValue::Array(casted))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(opt_val, _)) => {
                    let casted = cast_timestamp_micros_to_millis_scalar(opt_val, target_tz.clone());
                    Ok(ColumnarValue::Scalar(casted))
                }
                _ => Ok(value),
            },
            // Nested types that differ only in field names (e.g., List element named
            // "item" vs "element", or Map entries named "key_value" vs "entries").
            // Re-label the array so the DataType metadata matches the logical schema.
            (physical, logical)
                if physical != logical && types_differ_only_in_field_names(physical, logical) =>
            {
                match value {
                    ColumnarValue::Array(array) => {
                        let relabeled = relabel_array(array, logical);
                        Ok(ColumnarValue::Array(relabeled))
                    }
                    other => Ok(other),
                }
            }
            // Fallback: use spark_parquet_convert for complex nested type conversions
            // (e.g., List<Struct{a,b,c}> → List<Struct{a,c}>, Map field selection, etc.)
            _ => {
                if let Some(parquet_options) = &self.parquet_options {
                    let converted = spark_parquet_convert(value, target_field, parquet_options)?;
                    Ok(converted)
                } else {
                    Ok(value)
                }
            }
        }
    }

    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().expect("CastColumnExpr child");
        let mut new_expr = Self::new(
            child,
            Arc::clone(&self.input_physical_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
        );
        if let Some(opts) = &self.parquet_options {
            new_expr = new_expr.with_parquet_options(opts.clone());
        }
        Ok(Arc::new(new_expr))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, AsArray, BinaryArray, Decimal128Array, DictionaryArray, FixedSizeListArray,
        Int32Array, Int64Array, StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array,
        UInt8Array,
    };
    use arrow::datatypes::{Field, Fields, Int32Type};
    use datafusion::physical_expr::expressions::Column;
    use parquet::variant::{VariantArrayBuilder, VariantBuilder, VariantType};

    fn unicode_object_keys() -> Vec<String> {
        let mut keys = (0..30).map(|i| format!("k{i:02}")).collect::<Vec<_>>();
        keys.push("\u{e000}".to_string());
        keys.push("😀".to_string());
        keys
    }

    fn assert_spark_unicode_variant(variant: Variant<'_, '_>) {
        let Variant::Object(object) = variant else {
            panic!("expected object")
        };
        let fields = object.iter().collect::<Vec<_>>();

        assert_eq!(fields.len(), 32);
        assert_eq!(fields[30].0, "😀");
        assert_eq!(fields[31].0, "\u{e000}");
        let emoji = fields
            .binary_search_by(|(name, _)| name.encode_utf16().cmp("😀".encode_utf16()))
            .unwrap();
        assert_eq!(fields[emoji].1.as_int64(), Some(531));
        let private_use = fields
            .binary_search_by(|(name, _)| name.encode_utf16().cmp("\u{e000}".encode_utf16()))
            .unwrap();
        assert_eq!(fields[private_use].1.as_int64(), Some(30));
    }

    fn assert_spark_unicode_object(output: &StructArray) {
        let value = output.column(0).as_binary::<i32>();
        let metadata = output.column(1).as_binary::<i32>();
        assert_spark_unicode_variant(Variant::new(metadata.value(0), value.value(0)));
    }

    fn normalize_typed_value(typed_value: ArrayRef, field_names: &[&str]) -> VariantArray {
        let (metadata_bytes, _) = VariantBuilder::new()
            .with_field_names(field_names.iter().copied())
            .finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", typed_value.data_type().clone(), false),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        VariantArray::try_new(output.as_ref()).unwrap()
    }

    #[test]
    fn test_normalize_shredded_variant_with_dictionary_metadata_for_spark() {
        let mut builder = VariantArrayBuilder::new(3);
        builder.append_variant(Variant::from(1_i64));
        builder.append_null();
        builder.append_variant(Variant::from(3_i64));
        let base = builder.build();
        let metadata = cast(base.metadata_field().as_ref(), &DataType::Binary).unwrap();
        let metadata_bytes = metadata.as_binary::<i32>().value(0).to_vec();
        let metadata_values: ArrayRef =
            Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let metadata: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(0), Some(0)]),
                metadata_values,
            )
            .unwrap(),
        );
        let typed_value: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        let physical_fields = Fields::from(vec![
            Field::new("typed_value", DataType::Int64, true),
            Field::new("metadata", metadata.data_type().clone(), false),
        ]);
        let physical = StructArray::try_new(
            physical_fields,
            vec![typed_value, metadata],
            base.inner().nulls().cloned(),
        )
        .unwrap();

        let input_field = Arc::new(Field::new("v", physical.data_type().clone(), true));
        let target_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let target_field = Arc::new(
            Field::new("v", DataType::Struct(target_fields), true).with_extension_type(VariantType),
        );
        let schema = Schema::new(vec![Arc::clone(&input_field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(physical)]).unwrap();
        let expr = CometCastColumnExpr::new(
            Arc::new(Column::new("v", 0)),
            input_field,
            target_field,
            None,
        );

        let ColumnarValue::Array(output) = expr.evaluate(&batch).unwrap() else {
            panic!("expected array")
        };
        let output = output.as_struct();
        assert_eq!(
            output
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["value", "metadata"]
        );
        assert!(output
            .columns()
            .iter()
            .all(|column| column.data_type() == &DataType::Binary));
        assert!(output.is_null(1));

        let variant = VariantArray::try_new(output).unwrap();
        assert_eq!(variant.value(0), Variant::from(10_i8));
        assert_eq!(variant.value(2), Variant::from(30_i8));
    }

    #[test]
    fn test_normalize_shredded_variant_widens_unsigned_values() {
        let metadata_builder = VariantBuilder::new().with_field_names(["u8", "u16", "u32"]);
        let (metadata_bytes, _) = metadata_builder.finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));

        let fields = [
            ("u8", Arc::new(UInt8Array::from(vec![u8::MAX])) as ArrayRef),
            (
                "u16",
                Arc::new(UInt16Array::from(vec![u16::MAX])) as ArrayRef,
            ),
            (
                "u32",
                Arc::new(UInt32Array::from(vec![u32::MAX])) as ArrayRef,
            ),
        ];
        let mut object_fields = Vec::with_capacity(fields.len());
        let mut object_columns = Vec::with_capacity(fields.len());
        for (name, value) in fields {
            let shredded = StructArray::try_new(
                Fields::from(vec![Field::new(
                    "typed_value",
                    value.data_type().clone(),
                    false,
                )]),
                vec![value],
                None,
            )
            .unwrap();
            object_fields.push(Field::new(name, shredded.data_type().clone(), false));
            object_columns.push(Arc::new(shredded) as ArrayRef);
        }
        let typed_value: ArrayRef =
            Arc::new(StructArray::try_new(object_fields.into(), object_columns, None).unwrap());
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", typed_value.data_type().clone(), false),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = VariantArray::try_new(output.as_ref()).unwrap();
        let variant = output.value(0);
        let Variant::Object(object) = variant else {
            panic!("expected object")
        };
        assert_eq!(object.get("u8"), Some(Variant::from(255_i16)));
        assert_eq!(object.get("u16"), Some(Variant::from(65_535_i32)));
        assert_eq!(object.get("u32"), Some(Variant::from(4_294_967_295_i64)));
    }

    #[test]
    fn test_normalize_shredded_variant_widens_millisecond_timestamps() {
        let millis = 1_704_067_200_123_i64;
        let ltz: ArrayRef =
            Arc::new(TimestampMillisecondArray::from(vec![millis]).with_timezone("UTC"));
        let ntz: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![millis]));
        let shredded = |value: ArrayRef| -> ArrayRef {
            Arc::new(
                StructArray::try_new(
                    Fields::from(vec![Field::new(
                        "typed_value",
                        value.data_type().clone(),
                        false,
                    )]),
                    vec![value],
                    None,
                )
                .unwrap(),
            )
        };
        let ltz = shredded(ltz);
        let ntz = shredded(ntz);
        let object: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("ltz", ltz.data_type().clone(), false),
                    Field::new("ntz", ntz.data_type().clone(), false),
                ]),
                vec![ltz, ntz],
                None,
            )
            .unwrap(),
        );
        let output = normalize_typed_value(object, &["ltz", "ntz"]);
        let Variant::Object(object) = output.value(0) else {
            panic!("expected object")
        };

        let Some(Variant::TimestampMicros(ltz)) = object.get("ltz") else {
            panic!("expected timestamp")
        };
        assert_eq!(ltz.timestamp_micros(), millis * 1_000);

        let Some(Variant::TimestampNtzMicros(ntz)) = object.get("ntz") else {
            panic!("expected timestamp_ntz")
        };
        assert_eq!(ntz.and_utc().timestamp_micros(), millis * 1_000);
    }

    #[test]
    fn test_normalize_shredded_variant_converts_fixed_size_list() {
        let elements: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![42, 43]))],
                None,
            )
            .unwrap(),
        );
        let typed_value: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("element", elements.data_type().clone(), false)),
                2,
                elements,
                None,
            )
            .unwrap(),
        );

        let output = normalize_typed_value(typed_value, &[]);
        let Variant::List(list) = output.value(0) else {
            panic!("expected list")
        };
        assert_eq!(
            list.iter()
                .map(|value| value.as_int64())
                .collect::<Vec<_>>(),
            vec![Some(42), Some(43)]
        );
    }

    #[test]
    fn test_normalize_shredded_variant_compacts_spark_integer_widths() {
        let (metadata_bytes, _) = VariantBuilder::new().finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(metadata_bytes.as_slice()),
            Some(metadata_bytes.as_slice()),
            Some(metadata_bytes.as_slice()),
            Some(metadata_bytes.as_slice()),
        ]));
        let typed_value: ArrayRef = Arc::new(Int64Array::from(vec![
            1,
            i64::from(i8::MAX) + 1,
            i64::from(i16::MAX) + 1,
            i64::from(i32::MAX) + 1,
        ]));
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", DataType::Int64, false),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = VariantArray::try_new(output.as_ref()).unwrap();
        assert_eq!(output.value(0), Variant::Int8(1));
        assert_eq!(output.value(1), Variant::Int16(128));
        assert_eq!(output.value(2), Variant::Int32(32_768));
        assert_eq!(output.value(3), Variant::Int64(2_147_483_648));
    }

    #[test]
    fn test_compact_spark_typed_variant_canonicalizes_nan() {
        let Variant::Float(float) =
            compact_spark_typed_variant(Variant::Float(f32::from_bits(0x7fc0_0001)))
        else {
            panic!("expected float")
        };
        assert_eq!(float.to_bits(), 0x7fc0_0000);

        let Variant::Double(double) =
            compact_spark_typed_variant(Variant::Double(f64::from_bits(0x7ff8_0000_0000_0001)))
        else {
            panic!("expected double")
        };
        assert_eq!(double.to_bits(), 0x7ff8_0000_0000_0000);

        let (metadata, _) = VariantBuilder::new().finish();
        let metadata = VariantMetadata::new(&metadata);
        let residual = f32::from_bits(0x7fc0_0001);
        let bytes = spark_variant_bytes(&metadata, Variant::Float(residual)).unwrap();
        let Variant::Float(output) = Variant::new_with_metadata(metadata, &bytes) else {
            panic!("expected residual float")
        };
        assert_eq!(output.to_bits(), residual.to_bits());
    }

    #[test]
    fn test_normalize_shredded_variant_rejects_missing_required_value() {
        let (metadata_bytes, _) = VariantBuilder::new().finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let typed_value: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", DataType::Int64, true),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        assert!(normalize_variant_array(&physical, &target_field).is_err());
    }

    #[test]
    fn test_normalize_shredded_variant_uses_physical_metadata_order() {
        let (metadata_bytes, _) = VariantBuilder::new().with_field_names(["a", "b"]).finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (name, value) in [("b", 2_i64), ("a", 1_i64)] {
            let child = StructArray::try_new(
                Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![value]))],
                None,
            )
            .unwrap();
            fields.push(Field::new(name, child.data_type().clone(), false));
            columns.push(Arc::new(child) as ArrayRef);
        }
        let typed_value: ArrayRef =
            Arc::new(StructArray::try_new(fields.into(), columns, None).unwrap());
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", typed_value.data_type().clone(), false),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = output.as_struct();
        assert_eq!(
            output.column(1).as_binary::<i32>().value(0),
            &[0x01, 2, 0, 1, 2, b'b', b'a']
        );

        let mut expected = VariantBuilder::new().with_field_names(["b", "a"]);
        let mut object = expected.new_object();
        object.insert("b", 2_i8);
        object.insert("a", 1_i8);
        object.finish();
        let (_, expected_value) = expected.finish();
        assert_eq!(output.column(0).as_binary::<i32>().value(0), expected_value);
    }

    #[test]
    fn test_normalize_shredded_variant_preserves_residual_scalar_width() {
        let mut builder = VariantBuilder::new().with_field_names(["known", "residual"]);
        let mut object = builder.new_object();
        object.insert("residual", 1_i64);
        object.finish();
        let (metadata_bytes, value_bytes) = builder.finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
        let known: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![2]))],
                None,
            )
            .unwrap(),
        );
        let typed_value: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("known", known.data_type().clone(), false)]),
                vec![known],
                None,
            )
            .unwrap(),
        );
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::Binary, true),
                    Field::new("typed_value", typed_value.data_type().clone(), true),
                ]),
                vec![metadata, value, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = VariantArray::try_new(output.as_ref()).unwrap();
        let Variant::Object(object) = output.value(0) else {
            panic!("expected object")
        };
        assert_eq!(object.get("known"), Some(Variant::Int8(2)));
        assert_eq!(object.get("residual"), Some(Variant::Int64(1)));
    }

    #[test]
    fn test_normalize_shredded_variant_compacts_spark_decimal_width() {
        let (metadata_bytes, _) = VariantBuilder::new().finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let typed_value: ArrayRef = Arc::new(
            Decimal128Array::from(vec![123_i128])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", typed_value.data_type().clone(), false),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = VariantArray::try_new(output.as_ref()).unwrap();
        assert_eq!(
            output.value(0),
            Variant::Decimal4(VariantDecimal4::try_new(123, 2).unwrap())
        );
    }

    #[test]
    fn test_normalize_shredded_variant_uses_spark_object_key_order() {
        let keys = unicode_object_keys();

        let metadata_builder =
            VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
        let (metadata_bytes, _) = metadata_builder.finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));

        let mut object_fields = Vec::with_capacity(keys.len());
        let mut object_columns = Vec::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            let value = if key == "😀" { 531 } else { index as i64 };
            let field = Field::new("typed_value", DataType::Int64, false);
            let column: ArrayRef = Arc::new(Int64Array::from(vec![value]));
            let shredded_field =
                StructArray::try_new(Fields::from(vec![field]), vec![column], None).unwrap();
            object_fields.push(Field::new(key, shredded_field.data_type().clone(), false));
            object_columns.push(Arc::new(shredded_field) as ArrayRef);
        }
        let typed_value: ArrayRef =
            Arc::new(StructArray::try_new(object_fields.into(), object_columns, None).unwrap());
        let physical_fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("typed_value", typed_value.data_type().clone(), false),
        ]);
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(physical_fields, vec![metadata, typed_value], None).unwrap(),
        );
        let target_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let target_field = Arc::new(
            Field::new("v", DataType::Struct(target_fields), false)
                .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = output.as_struct();
        assert_spark_unicode_object(output);
    }

    #[test]
    fn test_normalize_nested_shredded_variant_uses_spark_object_key_order() {
        let keys = unicode_object_keys();
        let field_names = std::iter::once("nested")
            .chain(keys.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let (metadata_bytes, _) = VariantBuilder::new().with_field_names(field_names).finish();
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));

        let mut nested_fields = Vec::with_capacity(keys.len());
        let mut nested_columns = Vec::with_capacity(keys.len());
        for (index, key) in keys.iter().enumerate() {
            let value = if key == "😀" { 531 } else { index as i64 };
            let state = StructArray::try_new(
                Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![value]))],
                None,
            )
            .unwrap();
            nested_fields.push(Field::new(key, state.data_type().clone(), false));
            nested_columns.push(Arc::new(state) as ArrayRef);
        }
        let nested_value: ArrayRef =
            Arc::new(StructArray::try_new(nested_fields.into(), nested_columns, None).unwrap());
        let nested_state: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new(
                    "typed_value",
                    nested_value.data_type().clone(),
                    false,
                )]),
                vec![nested_value],
                None,
            )
            .unwrap(),
        );
        let typed_value: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new(
                    "nested",
                    nested_state.data_type().clone(),
                    false,
                )]),
                vec![nested_state],
                None,
            )
            .unwrap(),
        );
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("typed_value", typed_value.data_type().clone(), false),
                ]),
                vec![metadata, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = VariantArray::try_new(output.as_ref()).unwrap();
        let Variant::Object(object) = output.value(0) else {
            panic!("expected outer object")
        };
        assert_spark_unicode_variant(object.get("nested").expect("nested field"));
    }

    #[test]
    fn test_normalize_partially_shredded_spark_object_key_order() {
        let keys = unicode_object_keys();
        let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
        let mut object = builder.new_object();
        for (index, key) in keys.iter().enumerate().skip(1) {
            object.insert(key, if key == "😀" { 531 } else { index as i64 });
        }
        object.finish();
        let (metadata_bytes, value_bytes) = builder.finish();

        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let spark_value = reorder_variant_values(
            &value,
            &metadata,
            None,
            VariantObjectKeyOrder::SparkUtf16,
            false,
        )
        .unwrap();

        let shredded_k00: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![0]))],
                None,
            )
            .unwrap(),
        );
        let typed_value: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new(
                    "k00",
                    shredded_k00.data_type().clone(),
                    false,
                )]),
                vec![shredded_k00],
                None,
            )
            .unwrap(),
        );
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::Binary, true),
                    Field::new("typed_value", typed_value.data_type().clone(), true),
                ]),
                vec![metadata, spark_value, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        assert_spark_unicode_object(output.as_struct());
    }

    #[test]
    fn test_normalize_unshredded_variant_uses_spark_object_key_order() {
        let keys = unicode_object_keys();
        let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
        let mut object = builder.new_object();
        for (index, key) in keys.iter().enumerate() {
            object.insert(key, if key == "😀" { 531 } else { index as i64 });
        }
        object.finish();
        let (metadata_bytes, value_bytes) = builder.finish();

        let Variant::Object(canonical) = Variant::new(&metadata_bytes, &value_bytes) else {
            panic!("expected object")
        };
        let canonical_fields = canonical.iter().collect::<Vec<_>>();
        assert_eq!(canonical_fields[30].0, "\u{e000}");
        assert_eq!(canonical_fields[31].0, "😀");

        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let physical_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let physical: ArrayRef =
            Arc::new(StructArray::try_new(physical_fields, vec![value, metadata], None).unwrap());
        let target_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let target_field = Arc::new(
            Field::new("v", DataType::Struct(target_fields), false)
                .with_extension_type(VariantType),
        );

        let first = normalize_variant_array(&physical, &target_field).unwrap();
        let first_value = first
            .as_struct()
            .column(0)
            .as_binary::<i32>()
            .value(0)
            .to_vec();
        assert_spark_unicode_object(first.as_struct());

        // Spark-produced already-unshredded input is UTF-16 ordered. Normalizing it again must
        // remain valid without Arrow's UTF-8-order full validation.
        let second = normalize_variant_array(&first, &target_field).unwrap();
        assert_spark_unicode_object(second.as_struct());
        assert_eq!(
            second.as_struct().column(0).as_binary::<i32>().value(0),
            first_value
        );
    }

    #[test]
    fn test_normalize_spark_ordered_variant_preserves_value_bytes() {
        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        object.insert("b", 1_i64);
        object.insert("a", 2_i64);
        object.finish();
        let (metadata_bytes, value_bytes) = builder.finish();

        let Variant::Object(object) = Variant::new(&metadata_bytes, &value_bytes) else {
            panic!("expected object")
        };
        assert_eq!(
            object.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let physical_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let physical: ArrayRef =
            Arc::new(StructArray::try_new(physical_fields, vec![value, metadata], None).unwrap());
        let target_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let target_field = Arc::new(
            Field::new("v", DataType::Struct(target_fields), false)
                .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        assert_eq!(
            output.as_struct().column(0).as_binary::<i32>().value(0),
            value_bytes
        );
    }

    #[test]
    fn test_normalize_variant_preserves_empty_object_keys() {
        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        object.insert("", 1_i64);
        let mut nested = object.new_object("nested");
        nested.insert("", 2_i64);
        nested.finish();
        object.finish();
        let (mut metadata_bytes, value_bytes) = builder.finish();

        // Spark leaves the metadata dictionary unsorted. Equal offsets encode the empty key.
        metadata_bytes[0] &= !0x10;
        assert!(VariantMetadata::try_new(&metadata_bytes).is_err());

        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ]),
                vec![value, metadata],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = output.as_struct();
        assert_eq!(output.column(0).as_binary::<i32>().value(0), value_bytes);
        assert_eq!(output.column(1).as_binary::<i32>().value(0), metadata_bytes);

        let Variant::Object(object) = Variant::new(&metadata_bytes, &value_bytes) else {
            panic!("expected object")
        };
        assert_eq!(object.get(""), Some(Variant::from(1_i64)));
        let Variant::Object(nested) = object.get("nested").unwrap() else {
            panic!("expected nested object")
        };
        assert_eq!(nested.get(""), Some(Variant::from(2_i64)));
    }

    #[test]
    fn test_normalize_partially_shredded_nested_unicode_and_empty_keys() {
        let keys = unicode_object_keys();
        let mut builder = VariantBuilder::new().with_field_names(["known"]);
        let mut object = builder.new_object();
        object.insert("", 1_i64);
        let mut nested = object.new_object("nested");
        for (index, key) in keys.iter().enumerate() {
            nested.insert(key, if key == "😀" { 531_i64 } else { index as i64 });
        }
        nested.finish();
        object.finish();
        let (metadata_bytes, value_bytes) = builder.finish();
        assert!(VariantMetadata::try_new(&metadata_bytes).is_err());

        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(value_bytes.as_slice())]));
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(metadata_bytes.as_slice())]));
        let known: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("typed_value", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![3]))],
                None,
            )
            .unwrap(),
        );
        let typed_value: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("known", known.data_type().clone(), false)]),
                vec![known],
                None,
            )
            .unwrap(),
        );
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![
                    Field::new("metadata", DataType::Binary, false),
                    Field::new("value", DataType::Binary, true),
                    Field::new("typed_value", typed_value.data_type().clone(), true),
                ]),
                vec![metadata, value, typed_value],
                None,
            )
            .unwrap(),
        );
        let target_field = Arc::new(
            Field::new(
                "v",
                DataType::Struct(Fields::from(vec![
                    Field::new("value", DataType::Binary, false),
                    Field::new("metadata", DataType::Binary, false),
                ])),
                false,
            )
            .with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        let output = VariantArray::try_new(output.as_ref()).unwrap();
        let Variant::Object(object) = output.value(0) else {
            panic!("expected object")
        };
        assert_eq!(object.get("").unwrap().as_int64(), Some(1));
        assert_eq!(object.get("known").unwrap().as_int64(), Some(3));
        let Variant::Object(nested) = object.get("nested").unwrap() else {
            panic!("expected nested object")
        };
        let fields = nested.iter().collect::<Vec<_>>();
        assert_eq!(fields.len(), 32);
        assert_eq!(fields[30].0, "😀");
        assert_eq!(fields[31].0, "\u{e000}");
        assert_eq!(nested.get("😀").unwrap().as_int64(), Some(531));
    }

    #[test]
    fn test_normalize_variant_skips_empty_children_of_null_parent() {
        let value: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b""[..])]));
        let metadata: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b""[..])]));
        let physical_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let physical: ArrayRef = Arc::new(
            StructArray::try_new(
                physical_fields,
                vec![value, metadata],
                Some(NullBuffer::from(vec![false])),
            )
            .unwrap(),
        );
        let target_fields = Fields::from(vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]);
        let target_field = Arc::new(
            Field::new("v", DataType::Struct(target_fields), true).with_extension_type(VariantType),
        );

        let output = normalize_variant_array(&physical, &target_field).unwrap();
        assert!(output.is_null(0));
        assert!(output.as_struct().column(0).is_null(0));
    }

    #[test]
    fn test_cast_timestamp_micros_to_millis_array() {
        // Create a TimestampMicrosecond array with some values
        let micros_array: TimestampMicrosecondArray = vec![
            Some(1_000_000),  // 1 second in micros
            Some(2_500_000),  // 2.5 seconds in micros
            None,             // null value
            Some(0),          // zero
            Some(-1_000_000), // negative value (before epoch)
        ]
        .into();
        let array_ref: ArrayRef = Arc::new(micros_array);

        // Cast without timezone
        let result = cast_timestamp_micros_to_millis_array(&array_ref, None);
        let millis_array = result
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Expected TimestampMillisecondArray");

        assert_eq!(millis_array.len(), 5);
        assert_eq!(millis_array.value(0), 1000); // 1_000_000 / 1000
        assert_eq!(millis_array.value(1), 2500); // 2_500_000 / 1000
        assert!(millis_array.is_null(2));
        assert_eq!(millis_array.value(3), 0);
        assert_eq!(millis_array.value(4), -1000); // -1_000_000 / 1000
    }

    #[test]
    fn test_cast_timestamp_micros_to_millis_array_with_timezone() {
        let micros_array: TimestampMicrosecondArray = vec![Some(1_000_000), Some(2_000_000)].into();
        let array_ref: ArrayRef = Arc::new(micros_array);

        let target_tz: Option<Arc<str>> = Some(Arc::from("UTC"));
        let result = cast_timestamp_micros_to_millis_array(&array_ref, target_tz);
        let millis_array = result
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Expected TimestampMillisecondArray");

        assert_eq!(millis_array.value(0), 1000);
        assert_eq!(millis_array.value(1), 2000);
        // Verify timezone is preserved
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
        );
    }

    #[test]
    fn test_cast_timestamp_micros_to_millis_scalar() {
        // Test with a value
        let result = cast_timestamp_micros_to_millis_scalar(Some(1_500_000), None);
        assert_eq!(result, ScalarValue::TimestampMillisecond(Some(1500), None));

        // Test with null
        let null_result = cast_timestamp_micros_to_millis_scalar(None, None);
        assert_eq!(null_result, ScalarValue::TimestampMillisecond(None, None));

        // Test with timezone
        let target_tz: Option<Arc<str>> = Some(Arc::from("UTC"));
        let tz_result = cast_timestamp_micros_to_millis_scalar(Some(2_000_000), target_tz.clone());
        assert_eq!(
            tz_result,
            ScalarValue::TimestampMillisecond(Some(2000), target_tz)
        );
    }

    #[test]
    fn test_comet_cast_column_expr_evaluate_micros_to_millis_array() {
        // Create input schema with TimestampMicrosecond column
        let input_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let schema = Schema::new(vec![Arc::clone(&input_field)]);

        // Create target field with TimestampMillisecond
        let target_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));

        // Create a column expression
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));

        // Create the CometCastColumnExpr
        let cast_expr = CometCastColumnExpr::new(col_expr, input_field, target_field, None);

        // Create a record batch with TimestampMicrosecond data
        let micros_array: TimestampMicrosecondArray =
            vec![Some(1_000_000), Some(2_000_000), None].into();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(micros_array)]).unwrap();

        // Evaluate
        let result = cast_expr.evaluate(&batch).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let millis_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("Expected TimestampMillisecondArray");
                assert_eq!(millis_array.value(0), 1000);
                assert_eq!(millis_array.value(1), 2000);
                assert!(millis_array.is_null(2));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_comet_cast_column_expr_evaluate_micros_to_millis_scalar() {
        // Create input schema with TimestampMicrosecond column
        let input_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        let schema = Schema::new(vec![Arc::clone(&input_field)]);

        // Create target field with TimestampMillisecond
        let target_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));

        // Create a literal expression that returns a scalar
        let scalar = ScalarValue::TimestampMicrosecond(Some(1_500_000), None);
        let literal_expr: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Literal::new(scalar));

        // Create the CometCastColumnExpr
        let cast_expr = CometCastColumnExpr::new(literal_expr, input_field, target_field, None);

        // Create an empty batch (scalar doesn't need data)
        let batch = RecordBatch::new_empty(Arc::new(schema));

        // Evaluate
        let result = cast_expr.evaluate(&batch).unwrap();

        match result {
            ColumnarValue::Scalar(s) => {
                assert_eq!(s, ScalarValue::TimestampMillisecond(Some(1500), None));
            }
            _ => panic!("Expected Scalar result"),
        }
    }

    #[test]
    fn test_relabel_list_field_name() {
        // Physical: List(Field("item", Int32))
        // Logical:  List(Field("element", Int32))
        let physical_field = Arc::new(Field::new("item", DataType::Int32, true));
        let logical_field = Arc::new(Field::new("element", DataType::Int32, true));

        let values = Int32Array::from(vec![1, 2, 3]);
        let list = ListArray::new(
            physical_field,
            arrow::buffer::OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(values),
            None,
        );
        let array: ArrayRef = Arc::new(list);

        let target_type = DataType::List(Arc::clone(&logical_field));
        let result = relabel_array(array, &target_type);
        assert_eq!(result.data_type(), &target_type);
    }

    #[test]
    fn test_relabel_map_entries_field_name() {
        // Physical: Map(Field("key_value", Struct{key, value}))
        // Logical:  Map(Field("entries", Struct{key, value}))
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let struct_fields = Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]);

        let physical_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(struct_fields.clone()),
            false,
        ));
        let logical_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(struct_fields.clone()),
            false,
        ));

        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int32Array::from(vec![1, 2]);
        let entries = StructArray::new(struct_fields, vec![Arc::new(keys), Arc::new(values)], None);
        let map = MapArray::new(
            physical_entries_field,
            arrow::buffer::OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        );
        let array: ArrayRef = Arc::new(map);

        let target_type = DataType::Map(logical_entries_field, false);
        let result = relabel_array(array, &target_type);
        assert_eq!(result.data_type(), &target_type);
    }

    #[test]
    fn test_relabel_struct_metadata() {
        // Physical: Struct { Field("a", Int32, metadata={"PARQUET:field_id": "1"}) }
        // Logical:  Struct { Field("a", Int32, metadata={}) }
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), "1".to_string());
        let physical_field =
            Arc::new(Field::new("a", DataType::Int32, true).with_metadata(metadata));
        let logical_field = Arc::new(Field::new("a", DataType::Int32, true));

        let col = Int32Array::from(vec![10, 20]);
        let physical_fields = Fields::from(vec![physical_field]);
        let logical_fields = Fields::from(vec![logical_field]);

        let struct_arr = StructArray::new(physical_fields, vec![Arc::new(col)], None);
        let array: ArrayRef = Arc::new(struct_arr);

        let target_type = DataType::Struct(logical_fields);
        let result = relabel_array(array, &target_type);
        assert_eq!(result.data_type(), &target_type);
    }

    #[test]
    fn test_relabel_nested_struct_containing_list() {
        // Physical: Struct { Field("col", List(Field("item", Int32))) }
        // Logical:  Struct { Field("col", List(Field("element", Int32))) }
        let physical_list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let logical_list_field = Arc::new(Field::new("element", DataType::Int32, true));

        let physical_struct_field = Arc::new(Field::new(
            "col",
            DataType::List(Arc::clone(&physical_list_field)),
            true,
        ));
        let logical_struct_field = Arc::new(Field::new(
            "col",
            DataType::List(Arc::clone(&logical_list_field)),
            true,
        ));

        let values = Int32Array::from(vec![1, 2, 3]);
        let list = ListArray::new(
            physical_list_field,
            arrow::buffer::OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(values),
            None,
        );

        let physical_fields = Fields::from(vec![physical_struct_field]);
        let logical_fields = Fields::from(vec![logical_struct_field]);

        let struct_arr = StructArray::new(physical_fields, vec![Arc::new(list) as ArrayRef], None);
        let array: ArrayRef = Arc::new(struct_arr);

        let target_type = DataType::Struct(logical_fields);
        let result = relabel_array(array, &target_type);
        assert_eq!(result.data_type(), &target_type);

        // Verify we can access the nested data without panics
        let result_struct = result.as_any().downcast_ref::<StructArray>().unwrap();
        let result_list = result_struct
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(result_list.len(), 2);
    }
}
