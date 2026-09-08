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
    compute::cast,
    datatypes::{DataType, FieldRef},
    error::ArrowError,
};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use parquet::variant::{
    unshred_variant, ListBuilder, MetadataBuilder, ObjectBuilder, ParentState,
    ReadOnlyMetadataBuilder, ValueBuilder, Variant, VariantArray, VariantMetadata,
};
use std::{
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

    // VariantArray resolves metadata/value/typed_value by name, so the reader's child order is
    // irrelevant. Legacy Spark residuals must be put in Arrow order before the single upstream
    // unshred call; the whole output is then put back in the order expected by released Spark 4.
    let variant = VariantArray::try_new(array.as_ref())?;
    let prepared = prepare_variant_for_unshredding(&variant)?;
    let unshredded = unshred_variant(&prepared)?;
    let value = unshredded.value_column();
    let value = cast(value.as_ref(), &DataType::Binary)?;
    let metadata = cast(unshredded.metadata_column().as_ref(), &DataType::Binary)?;
    let value = reorder_variant_values(&value, &metadata, unshredded.inner().nulls())?;

    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![value, metadata],
        unshredded.inner().nulls().cloned(),
    )?))
}

/// Arrow validates every residual `value` while unshredding. Spark versions before SPARK-58949
/// wrote object keys in Java UTF-16 order, so rewrite every reachable legacy residual to Arrow's
/// UTF-8 order before unshredding. `metadata_rows` carries each root metadata row through nested
/// lists.
fn rewrite_shredding_state(
    state: &StructArray,
    metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
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
        let (value, value_changed) =
            rewrite_residual_values(&columns[index], metadata, &active_rows)?;
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
        let (typed_value, typed_changed) =
            rewrite_typed_value(&columns[index], metadata, &typed_rows)?;
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
    metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
) -> DataFusionResult<(ArrayRef, bool)> {
    let binary = cast(value.as_ref(), &DataType::Binary)?;
    let binary = binary.as_binary::<i32>();
    let mut output: Option<BinaryBuilder> = None;

    for (index, metadata_row) in metadata_rows.iter().enumerate() {
        if binary.is_null(index) {
            if let Some(output) = &mut output {
                output.append_null();
            }
            continue;
        }
        let Some(metadata_row) = metadata_row else {
            if let Some(output) = &mut output {
                output.append_value(binary.value(index));
            }
            continue;
        };
        if metadata.is_null(*metadata_row) {
            return Err(DataFusionError::Execution(format!(
                "Variant metadata is null at row {metadata_row}"
            )));
        }

        let rebuilt = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Vec<u8>>, ArrowError> {
                let metadata = VariantMetadata::try_new(metadata.value(*metadata_row))?;
                let variant = Variant::new_with_metadata(metadata.clone(), binary.value(index));
                if is_compatible_variant(&variant, VariantObjectKeyOrder::ArrowUtf8) {
                    return Ok(None);
                }
                if !is_compatible_variant(&variant, VariantObjectKeyOrder::SparkUtf16) {
                    return Err(ArrowError::InvalidArgumentError(
                        "Variant residual is neither UTF-8 nor Spark UTF-16 ordered".to_string(),
                    ));
                }
                Ok(Some(variant_bytes(
                    &metadata,
                    variant,
                    VariantObjectKeyOrder::ArrowUtf8,
                )?))
            },
        ))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid Variant residual at row {metadata_row}"))
        })??;
        if rebuilt.is_some() && output.is_none() {
            output = Some(binary_prefix_builder(binary, index));
        }
        if let Some(output) = &mut output {
            output.append_value(rebuilt.as_deref().unwrap_or_else(|| binary.value(index)));
        }
    }

    if let Some(mut output) = output {
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
    metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
) -> DataFusionResult<(ArrayRef, bool)> {
    let child_rows = list_metadata_rows(list, metadata_rows)?;
    let values = list.values().as_struct_opt().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Invalid shredded Variant list values: expected Struct, got {}",
            list.values().data_type()
        ))
    })?;
    let (values, changed) = rewrite_shredding_state(values, metadata, &child_rows)?;
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
    metadata: &BinaryArray,
    metadata_rows: &[Option<usize>],
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
                let (child, child_changed) =
                    rewrite_shredding_state(child, metadata, metadata_rows)?;
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
            metadata,
            metadata_rows,
        ),
        DataType::LargeList(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list::<i64>(),
            metadata,
            metadata_rows,
        ),
        DataType::ListView(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list_view::<i32>(),
            metadata,
            metadata_rows,
        ),
        DataType::LargeListView(_) => rewrite_list_typed_value(
            typed_value,
            typed_value.as_list_view::<i64>(),
            metadata,
            metadata_rows,
        ),
        _ => Ok((Arc::clone(typed_value), false)),
    }
}

fn prepare_variant_for_unshredding(variant: &VariantArray) -> DataFusionResult<VariantArray> {
    if variant.typed_value_column().is_none() {
        return Ok(variant.clone());
    }

    let metadata = cast(variant.metadata_column().as_ref(), &DataType::Binary)?;
    let metadata = metadata.as_binary::<i32>();
    let metadata_rows = (0..variant.len())
        .map(|index| variant.inner().is_valid(index).then_some(index))
        .collect::<Vec<_>>();
    let (array, changed) = rewrite_shredding_state(variant.inner(), metadata, &metadata_rows)?;
    if changed {
        Ok(VariantArray::try_new(array.as_ref())?)
    } else {
        Ok(variant.clone())
    }
}

/// Supplies sort-only field names whose Rust ordering matches Java `String.compareTo` ordering.
/// Field IDs still come from the original metadata dictionary.
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

fn variant_bytes(
    metadata: &VariantMetadata<'_>,
    variant: Variant<'_, '_>,
    order: VariantObjectKeyOrder,
) -> Result<Vec<u8>, ArrowError> {
    let mut value_builder = ValueBuilder::new();
    match variant {
        Variant::Object(object) => {
            let mut metadata_builder: Box<dyn MetadataBuilder> = match order {
                VariantObjectKeyOrder::ArrowUtf8 => {
                    Box::new(ReadOnlyMetadataBuilder::new(metadata))
                }
                VariantObjectKeyOrder::SparkUtf16 => Box::new(SparkMetadataBuilder::new(metadata)),
            };
            let mut builder = ObjectBuilder::new(
                ParentState::variant(&mut value_builder, metadata_builder.as_mut()),
                matches!(order, VariantObjectKeyOrder::ArrowUtf8),
            );
            for (name, value) in object.iter() {
                let value = variant_bytes(metadata, value, order)?;
                builder
                    .try_insert_bytes(name, Variant::new_with_metadata(metadata.clone(), &value))?;
            }
            builder.finish();
        }
        Variant::List(list) => {
            let mut metadata_builder = ReadOnlyMetadataBuilder::new(metadata);
            let mut builder = ListBuilder::new(
                ParentState::variant(&mut value_builder, &mut metadata_builder),
                matches!(order, VariantObjectKeyOrder::ArrowUtf8),
            );
            for value in list.iter() {
                let value = variant_bytes(metadata, value, order)?;
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

/// Released Spark 4 profiles search object fields in Java UTF-16 order. Convert whole-value output
/// to that order until #5474 can remove this rewrite after every supported profile includes
/// SPARK-58949. Values already in the requested order remain byte-for-byte unchanged.
/// https://github.com/apache/datafusion-comet/issues/5474
fn reorder_variant_values(
    value: &ArrayRef,
    metadata: &ArrayRef,
    parent_nulls: Option<&NullBuffer>,
) -> DataFusionResult<ArrayRef> {
    let original = value;
    let value = value.as_binary::<i32>();
    let metadata = metadata.as_binary::<i32>();
    let mut output: Option<BinaryBuilder> = None;

    for index in 0..value.len() {
        if parent_nulls.is_some_and(|nulls| nulls.is_null(index)) {
            if value.is_valid(index) && output.is_none() {
                output = Some(binary_prefix_builder(value, index));
            }
            if let Some(output) = &mut output {
                output.append_null();
            }
            continue;
        }
        if value.is_null(index) {
            return Err(DataFusionError::Execution(format!(
                "Variant value is null at row {index}"
            )));
        }
        if metadata.is_null(index) {
            return Err(DataFusionError::Execution(format!(
                "Variant metadata is null at row {index}"
            )));
        }

        let rebuilt = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Vec<u8>>, ArrowError> {
                let metadata = VariantMetadata::try_new(metadata.value(index))?;
                let variant = Variant::new_with_metadata(metadata.clone(), value.value(index));
                if is_compatible_variant(&variant, VariantObjectKeyOrder::SparkUtf16) {
                    return Ok(None);
                }
                Ok(Some(variant_bytes(
                    &metadata,
                    variant,
                    VariantObjectKeyOrder::SparkUtf16,
                )?))
            },
        ))
        .map_err(|_| {
            DataFusionError::Execution(format!("Invalid Variant value at row {index}"))
        })??;
        if rebuilt.is_some() && output.is_none() {
            output = Some(binary_prefix_builder(value, index));
        }
        if let Some(output) = &mut output {
            output.append_value(rebuilt.as_deref().unwrap_or_else(|| value.value(index)));
        }
    }

    match output {
        Some(mut output) => Ok(Arc::new(output.finish())),
        None => Ok(Arc::clone(original)),
    }
}

// Called only at the first changed row; the unchanged prefix is copied once.
fn binary_prefix_builder(value: &BinaryArray, end: usize) -> BinaryBuilder {
    let mut builder = BinaryBuilder::new();
    builder.extend(value.iter().take(end));
    builder
}

#[cfg(test)]
mod tests;
