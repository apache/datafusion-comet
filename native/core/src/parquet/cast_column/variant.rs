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
        make_array, Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, ListLikeArray,
        StructArray,
    },
    buffer::NullBuffer,
    compute::{cast, cast_with_options},
    datatypes::{DataType, FieldRef, TimeUnit, DECIMAL128_MAX_PRECISION},
    error::ArrowError,
};
use datafusion::common::{
    format::DEFAULT_CAST_OPTIONS, DataFusionError, Result as DataFusionResult,
};
use parquet::variant::{
    unshred_variant, BorrowedShreddingState, ListBuilder, MetadataBuilder, ObjectBuilder,
    ParentState, ReadOnlyMetadataBuilder, ValueBuilder, Variant, VariantArray, VariantBuilder,
    VariantDecimal4, VariantDecimal8, VariantMetadata, WritableMetadataBuilder,
};
use std::{
    collections::HashSet,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

pub(super) fn normalize_variant_array(
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

    let array = normalize_variant_storage(array)?;
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
    let first_error = match unshred_variant(variant) {
        Ok(array) => return Ok(array),
        Err(error) => DataFusionError::from(error),
    };

    if let Ok(prepared) = prepare_variant_for_unshredding(variant, None) {
        if let Ok(array) = unshred_variant(&prepared) {
            return Ok(array);
        }
    }
    let Some(metadata) = canonicalize_spark_empty_key_metadata(variant)? else {
        return Err(first_error);
    };
    let Ok(prepared) = prepare_variant_for_unshredding(variant, Some(metadata.as_binary::<i32>()))
    else {
        return Err(first_error);
    };
    match unshred_variant(&prepared) {
        Ok(array) => Ok(array),
        Err(_) => Err(first_error),
    }
}

fn normalize_variant_type(data_type: &DataType) -> Option<DataType> {
    fn normalize_field(field: &FieldRef) -> Option<FieldRef> {
        normalize_variant_type(field.data_type())
            .map(|data_type| Arc::new(field.as_ref().clone().with_data_type(data_type)))
    }

    match data_type {
        DataType::Dictionary(_, value_type) => {
            Some(normalize_variant_type(value_type).unwrap_or_else(|| value_type.as_ref().clone()))
        }
        DataType::UInt8 => Some(DataType::Int16),
        DataType::UInt16 => Some(DataType::Int32),
        DataType::UInt32 => Some(DataType::Int64),
        // Spark reads Parquet UINT_64 as Decimal(20, 0). This is lossless for the full range and
        // lets the existing Spark-compatible rebuild choose the Variant decimal width per value.
        DataType::UInt64 => Some(DataType::Decimal128(20, 0)),
        // Arrow chooses Decimal256 from the physical byte width, but Spark's DecimalType is
        // precision-based and stores every supported precision (<= 38) in 128 bits.
        DataType::Decimal256(precision, scale) if *precision <= DECIMAL128_MAX_PRECISION => {
            Some(DataType::Decimal128(*precision, *scale))
        }
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
        }
        DataType::FixedSizeBinary(_) => Some(DataType::Binary),
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

fn contains_uuid_extension(data_type: &DataType) -> bool {
    fn field_contains_uuid(field: &FieldRef) -> bool {
        (field.data_type() == &DataType::FixedSizeBinary(16)
            && field.extension_type_name() == Some("arrow.uuid"))
            || contains_uuid_extension(field.data_type())
    }

    match data_type {
        DataType::Struct(fields) => fields.iter().any(field_contains_uuid),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => field_contains_uuid(field),
        DataType::Dictionary(_, value_type) => contains_uuid_extension(value_type),
        _ => false,
    }
}

/// Normalize Arrow types that Spark's Parquet reader accepts but `VariantArray` 58.4 rejects.
/// Parquet restores unsigned integers to Arrow unsigned arrays and millisecond timestamps at their
/// annotated unit; it also exposes unannotated FIXED_LEN_BYTE_ARRAY as FixedSizeBinary. The
/// `arrow_canonical_extension_types` feature retains UUID annotations so they are not mistaken for
/// Spark-compatible Binary. Embedded Arrow schemas may restore dictionary arrays and fixed-size
/// lists. Spark decodes dictionaries, widens integers and timestamps, treats unannotated
/// fixed-length binary as Binary, and treats fixed-size lists as ordinary Variant arrays.
/// arrow-rs #10416/#10417 would move the UInt8/16/32 widening into
/// `VariantArray`/`unshred_variant`; remove those three arms after that ships and Comet upgrades:
/// https://github.com/apache/arrow-rs/issues/10416
/// https://github.com/apache/arrow-rs/pull/10417
/// Arrow #50622/#50810 instead proposes removing unsigned Parquet `typed_value` mappings; keep
/// compatibility until upstream settles that schema boundary:
/// https://github.com/apache/arrow/issues/50622
/// https://github.com/apache/arrow/pull/50810
/// Spark maps Parquet UINT_64 to Decimal(20,0), so its UInt64 arm remains Spark-specific:
/// https://github.com/apache/spark/blob/v4.0.4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetSchemaConverter.scala#L281-L296
/// arrow-rs #10810 covers encoded canonical metadata only. Embedded Arrow schemas can also restore
/// dictionary `value` and `typed_value` children, so keep decoding those for Spark compatibility:
/// https://github.com/apache/arrow-rs/pull/10810
fn normalize_variant_storage(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    if contains_uuid_extension(array.data_type()) {
        return Err(DataFusionError::Execution(
            "Parquet UUID is not supported as a shredded Variant child".to_string(),
        ));
    }
    let Some(data_type) = normalize_variant_type(array.data_type()) else {
        return Ok(Arc::clone(array));
    };
    Ok(cast_with_options(
        array.as_ref(),
        &data_type,
        &DEFAULT_CAST_OPTIONS,
    )?)
}

/// Arrow fully validates residual `value` fields while unshredding. Spark versions before
/// SPARK-58949 wrote their object keys in Java UTF-16 order, so rewrite every reachable legacy
/// residual to Arrow's UTF-8 order before calling the upstream unshredder. Keep this input-side
/// compatibility for historical files after #5474 removes the output-side UTF-16 rewrite:
/// https://github.com/apache/datafusion-comet/issues/5474
/// `metadata_rows` carries the root metadata row through nested lists.
fn rewrite_shredding_state(
    state: &StructArray,
    source_metadata: &BinaryArray,
    target_metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
    remap_metadata: bool,
) -> DataFusionResult<(ArrayRef, bool)> {
    if state.len() != metadata_rows.len() {
        return Err(DataFusionError::Execution(
            "Variant shredding state and metadata row mapping have different lengths".to_string(),
        ));
    }
    let active_rows = metadata_rows
        .iter()
        .enumerate()
        .map(|(index, row)| state.is_valid(index).then_some(*row).flatten())
        .collect::<Vec<_>>();
    let mut fields = state.fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = state.columns().to_vec();
    let mut changed = false;

    if let Some(index) = fields.iter().position(|field| field.name() == "value") {
        let (value, value_changed) = rewrite_residual_values(
            &columns[index],
            source_metadata,
            target_metadata,
            &active_rows,
            remap_metadata,
        )?;
        if value_changed {
            fields[index] = Arc::new(
                fields[index]
                    .as_ref()
                    .clone()
                    .with_data_type(value.data_type().clone()),
            );
            columns[index] = value;
            changed = true;
        }
    }

    if let Some(index) = fields
        .iter()
        .position(|field| field.name() == "typed_value")
    {
        let typed_rows = active_rows
            .iter()
            .enumerate()
            .map(|(row, metadata)| columns[index].is_valid(row).then_some(*metadata).flatten())
            .collect::<Vec<_>>();
        let (typed_value, typed_changed) = rewrite_typed_value(
            &columns[index],
            source_metadata,
            target_metadata,
            &typed_rows,
            remap_metadata,
        )?;
        if typed_changed {
            fields[index] = Arc::new(
                fields[index]
                    .as_ref()
                    .clone()
                    .with_data_type(typed_value.data_type().clone()),
            );
            columns[index] = typed_value;
            changed = true;
        }
    }

    if !changed {
        return Ok((Arc::new(state.clone()), false));
    }
    Ok((
        Arc::new(StructArray::try_new(
            fields.into(),
            columns,
            state.nulls().cloned(),
        )?),
        true,
    ))
}

fn rewrite_residual_values(
    value: &ArrayRef,
    source_metadata: &BinaryArray,
    target_metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
    remap_metadata: bool,
) -> DataFusionResult<(ArrayRef, bool)> {
    let binary = cast(value.as_ref(), &DataType::Binary)?;
    let binary = binary.as_binary::<i32>();
    let mut output = BinaryBuilder::new();
    let mut changed = false;

    for (index, metadata_row) in metadata_rows.iter().enumerate() {
        if binary.is_null(index) {
            output.append_null();
            continue;
        }
        let Some(metadata_row) = metadata_row else {
            output.append_value(binary.value(index));
            continue;
        };
        if source_metadata.is_null(*metadata_row) || target_metadata.is_null(*metadata_row) {
            return Err(DataFusionError::Execution(format!(
                "Variant metadata is null at row {metadata_row}"
            )));
        }

        let rebuilt = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Vec<u8>>, ArrowError> {
                let source = VariantMetadata::new(source_metadata.value(*metadata_row));
                let target = VariantMetadata::new(target_metadata.value(*metadata_row));
                let variant = Variant::new_with_metadata(source, binary.value(index));
                let arrow_ordered =
                    is_compatible_variant(&variant, VariantObjectKeyOrder::ArrowUtf8);
                if !remap_metadata && arrow_ordered {
                    return Ok(None);
                }
                if !arrow_ordered
                    && !is_compatible_variant(&variant, VariantObjectKeyOrder::SparkUtf16)
                {
                    return Err(ArrowError::InvalidArgumentError(
                        "Variant residual is neither UTF-8 nor Spark UTF-16 ordered".to_string(),
                    ));
                }
                Ok(Some(arrow_variant_bytes(&target, variant)?))
            },
        ))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid Variant residual at row {metadata_row}"))
        })??;
        changed |= rebuilt.is_some();
        output.append_value(rebuilt.as_deref().unwrap_or_else(|| binary.value(index)));
    }

    if changed {
        Ok((Arc::new(output.finish()), true))
    } else {
        Ok((Arc::clone(value), false))
    }
}

fn list_metadata_rows<L: ListLikeArray>(
    list: &L,
    parent_rows: &[Option<usize>],
) -> DataFusionResult<Vec<Option<usize>>> {
    let mut child_rows = vec![None; list.values().len()];
    for (index, metadata_row) in parent_rows.iter().enumerate() {
        let Some(metadata_row) = metadata_row else {
            continue;
        };
        for child_index in list.element_range(index) {
            match child_rows[child_index] {
                Some(existing) if existing != *metadata_row => {
                    return Err(DataFusionError::Execution(
                        "A shared Variant list child refers to different metadata rows".to_string(),
                    ));
                }
                _ => child_rows[child_index] = Some(*metadata_row),
            }
        }
    }
    Ok(child_rows)
}

fn rewrite_list_typed_value<L: ListLikeArray>(
    array: &ArrayRef,
    list: &L,
    source_metadata: &BinaryArray,
    target_metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
    remap_metadata: bool,
) -> DataFusionResult<(ArrayRef, bool)> {
    let child_rows = list_metadata_rows(list, metadata_rows)?;
    let values = list.values().as_struct_opt().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Invalid shredded Variant list values: expected Struct, got {}",
            list.values().data_type()
        ))
    })?;
    let (values, changed) = rewrite_shredding_state(
        values,
        source_metadata,
        target_metadata,
        &child_rows,
        remap_metadata,
    )?;
    if !changed {
        return Ok((Arc::clone(array), false));
    }

    let data_type = match array.data_type() {
        DataType::List(field) => DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(values.data_type().clone()),
        )),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(values.data_type().clone()),
        )),
        DataType::ListView(field) => DataType::ListView(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(values.data_type().clone()),
        )),
        DataType::LargeListView(field) => DataType::LargeListView(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(values.data_type().clone()),
        )),
        data_type => {
            return Err(DataFusionError::Execution(format!(
                "Expected a Variant list, got {data_type}"
            )));
        }
    };
    let data = array
        .to_data()
        .into_builder()
        .data_type(data_type)
        .child_data(vec![values.to_data()])
        .build()?;
    Ok((make_array(data), true))
}

fn rewrite_typed_value(
    typed_value: &ArrayRef,
    source_metadata: &BinaryArray,
    target_metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
    remap_metadata: bool,
) -> DataFusionResult<(ArrayRef, bool)> {
    match typed_value.data_type() {
        DataType::Struct(_) => {
            let object = typed_value.as_struct();
            let mut fields = object.fields().iter().cloned().collect::<Vec<_>>();
            let mut columns = object.columns().to_vec();
            let mut changed = false;
            for (index, column) in object.columns().iter().enumerate() {
                let child = column.as_struct_opt().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Invalid shredded Variant object field '{}': expected Struct, got {}",
                        fields[index].name(),
                        column.data_type()
                    ))
                })?;
                let (child, child_changed) = rewrite_shredding_state(
                    child,
                    source_metadata,
                    target_metadata,
                    metadata_rows,
                    remap_metadata,
                )?;
                if child_changed {
                    fields[index] = Arc::new(
                        fields[index]
                            .as_ref()
                            .clone()
                            .with_data_type(child.data_type().clone()),
                    );
                    columns[index] = child;
                    changed = true;
                }
            }
            if !changed {
                return Ok((Arc::clone(typed_value), false));
            }
            Ok((
                Arc::new(StructArray::try_new(
                    fields.into(),
                    columns,
                    object.nulls().cloned(),
                )?),
                true,
            ))
        }
        DataType::List(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list::<i32>(),
            source_metadata,
            target_metadata,
            metadata_rows,
            remap_metadata,
        ),
        DataType::LargeList(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list::<i64>(),
            source_metadata,
            target_metadata,
            metadata_rows,
            remap_metadata,
        ),
        DataType::ListView(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list_view::<i32>(),
            source_metadata,
            target_metadata,
            metadata_rows,
            remap_metadata,
        ),
        DataType::LargeListView(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list_view::<i64>(),
            source_metadata,
            target_metadata,
            metadata_rows,
            remap_metadata,
        ),
        _ => Ok((Arc::clone(typed_value), false)),
    }
}

fn prepare_variant_for_unshredding(
    variant: &VariantArray,
    target_metadata: Option<&BinaryArray>,
) -> DataFusionResult<VariantArray> {
    if variant.typed_value_field().is_none() {
        return Ok(variant.clone());
    }
    let source_metadata = cast(variant.metadata_field().as_ref(), &DataType::Binary)?;
    let source_metadata = source_metadata.as_binary::<i32>();
    let remap_metadata = target_metadata.is_some();
    let target_metadata = target_metadata.unwrap_or(source_metadata);
    let metadata_rows = (0..variant.len())
        .map(|index| variant.inner().is_valid(index).then_some(index))
        .collect::<Vec<_>>();
    let (array, _) = rewrite_shredding_state(
        variant.inner(),
        source_metadata,
        target_metadata,
        &metadata_rows,
        remap_metadata,
    )?;
    let array = array.as_struct();
    let mut fields = array.fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = array.columns().to_vec();
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
    columns[metadata_index] = Arc::new(target_metadata.clone());
    let array = StructArray::try_new(fields.into(), columns, array.nulls().cloned())?;
    Ok(VariantArray::try_new(&array)?)
}

/// Arrow 58.4 rejects Spark metadata dictionaries containing an empty object key when Spark's
/// insertion-order encoding leaves equal offsets. On an upstream validation failure, canonicalize
/// only that exact case; the recursive rewrite above remaps every residual to the new field IDs.
/// https://github.com/apache/arrow-rs/pull/10352
fn canonicalize_spark_empty_key_metadata(
    variant: &VariantArray,
) -> DataFusionResult<Option<ArrayRef>> {
    let metadata = cast(variant.metadata_field().as_ref(), &DataType::Binary)?;
    let metadata = metadata.as_binary::<i32>();
    let mut output = BinaryBuilder::new();
    let mut changed = false;

    for index in 0..variant.len() {
        if variant.inner().is_null(index) || metadata.is_null(index) {
            if metadata.is_null(index) {
                output.append_null();
            } else {
                output.append_value(metadata.value(index));
            }
            continue;
        }
        let metadata_bytes = metadata.value(index);
        if VariantMetadata::try_new(metadata_bytes).is_ok() {
            output.append_value(metadata_bytes);
            continue;
        }

        let replacement = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Vec<u8>>, ArrowError> {
                let old_metadata = VariantMetadata::new(metadata_bytes);
                let mut names = old_metadata
                    .iter_try()
                    .map(|name| name.map(str::to_string))
                    .collect::<Result<Vec<_>, _>>()?;
                if !names.iter().any(String::is_empty) {
                    return Ok(None);
                }
                let mut source =
                    WritableMetadataBuilder::from_iter(names.iter().map(String::as_str));
                source.finish();
                let mut source = source.into_inner();
                source[0] &= !0x10;
                if source != metadata_bytes {
                    return Ok(None);
                }
                names.sort_unstable();
                if names.windows(2).any(|names| names[0] == names[1]) {
                    return Ok(None);
                }
                let metadata = VariantBuilder::new()
                    .with_field_names(names.iter().map(String::as_str))
                    .finish()
                    .0;
                VariantMetadata::try_new(&metadata)?;
                Ok(Some(metadata))
            },
        ));
        let Ok(Ok(Some(replacement))) = replacement else {
            return Ok(None);
        };
        output.append_value(replacement);
        changed = true;
    }

    if changed {
        Ok(Some(Arc::new(output.finish())))
    } else {
        Ok(None)
    }
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

fn arrow_variant_bytes(
    metadata: &VariantMetadata<'_>,
    variant: Variant<'_, '_>,
) -> Result<Vec<u8>, ArrowError> {
    let mut value_builder = ValueBuilder::new();
    match variant {
        Variant::Object(object) => {
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
            let mut builder = ObjectBuilder::new(
                ParentState::variant(&mut value_builder, &mut metadata_builder),
                true,
            );
            for (name, value) in object.iter() {
                let value = arrow_variant_bytes(metadata, value)?;
                builder
                    .try_insert_bytes(name, Variant::new_with_metadata(metadata.clone(), &value))?;
            }
            builder.finish();
        }
        Variant::List(list) => {
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
            let mut builder = ListBuilder::new(
                ParentState::variant(&mut value_builder, &mut metadata_builder),
                true,
            );
            for value in list.iter() {
                let value = arrow_variant_bytes(metadata, value)?;
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
/// SPARK-58949 fixes this mismatch and retains legacy lookup compatibility. Once every Spark
/// profile supported by Comet contains that fix, #5474 can remove this output-side UTF-16 rewrite;
/// the input-side residual rewrite above remains necessary for historical Spark-written files.
/// https://issues.apache.org/jira/browse/SPARK-58949
/// https://github.com/apache/datafusion-comet/issues/5474
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
                VariantObjectKeyOrder::ArrowUtf8 => arrow_variant_bytes(&metadata, variant)?,
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

#[cfg(test)]
mod tests;
