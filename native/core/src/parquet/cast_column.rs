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
        make_array, Array, ArrayRef, BinaryArray, BinaryBuilder, LargeListArray, ListArray,
        MapArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    },
    buffer::NullBuffer,
    compute::{cast, CastOptions},
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
    unshred_variant, MetadataBuilder, ParentState, ReadOnlyMetadataBuilder, ValueBuilder, Variant,
    VariantArray, VariantMetadata,
};
use std::{
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
    let variant = prepare_variant_for_unshredding(&VariantArray::try_new(array.as_ref())?)?;
    let unshredded = unshred_variant(&variant)?;
    let value = unshredded.value_field().ok_or_else(|| {
        DataFusionError::Execution("Unshredded Variant is missing its value field".to_string())
    })?;
    let value = cast(value.as_ref(), &DataType::Binary)?;
    let metadata = cast(unshredded.metadata_field().as_ref(), &DataType::Binary)?;
    let value = reorder_variant_values(
        &value,
        &metadata,
        unshredded.inner().nulls(),
        VariantObjectKeyOrder::SparkUtf16,
        false,
    )?;
    let output = StructArray::try_new(
        fields.clone(),
        vec![value, metadata],
        unshredded.inner().nulls().cloned(),
    )?;
    Ok(Arc::new(output))
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

/// Arrow-rs parquet-variant-compute allows dictionary-encoded metadata in its contract, but 58.4's
/// `VariantArray::try_new` validates only Binary, LargeBinary, and BinaryView. Decode just that
/// child and keep the physical struct otherwise unchanged.
/// https://github.com/apache/arrow-rs/blob/0ff81c1215cc026a1de93ce3d2078df1ecba6f09/parquet-variant-compute/src/variant_array.rs#L276-L310
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

/// Reorder object keys for either Arrow's UTF-8 order or Spark's Java UTF-16 order. Preserve
/// already-compatible values byte-for-byte and retain the original metadata dictionary.
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

        let rebuilt = catch_unwind(AssertUnwindSafe(|| {
            // Spark encodes empty object keys with equal metadata offsets, which Arrow 58.4's
            // full validator rejects. Keep shallow parsing and all accesses inside this boundary.
            // https://github.com/apache/arrow-rs/blob/58.4.0/parquet-variant/src/variant/metadata.rs#L307-L317
            let metadata = VariantMetadata::new(metadata.value(index));
            let variant = Variant::new_with_metadata(metadata.clone(), value.value(index));
            if is_compatible_variant(&variant, order) {
                return None;
            }
            let mut value_builder = ValueBuilder::new();
            match order {
                VariantObjectKeyOrder::ArrowUtf8 => {
                    let mut metadata_builder = ReadOnlyMetadataBuilder::new(&metadata);
                    ValueBuilder::append_variant(
                        ParentState::variant(&mut value_builder, &mut metadata_builder),
                        variant,
                    );
                }
                VariantObjectKeyOrder::SparkUtf16 => {
                    let mut metadata_builder = SparkMetadataBuilder::new(&metadata);
                    ValueBuilder::append_variant(
                        ParentState::variant(&mut value_builder, &mut metadata_builder),
                        variant,
                    );
                }
            }
            Some(value_builder.into_inner())
        }))
        .map_err(|_| DataFusionError::Execution(format!("Invalid Variant value at row {index}")))?;
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
        Array, AsArray, BinaryArray, DictionaryArray, Int32Array, Int64Array, StringArray,
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

    fn assert_spark_unicode_object(output: &StructArray) {
        let value = output.column(0).as_binary::<i32>();
        let metadata = output.column(1).as_binary::<i32>();
        let Variant::Object(object) = Variant::new(metadata.value(0), value.value(0)) else {
            panic!("expected object")
        };
        let fields = object.iter().collect::<Vec<_>>();

        assert_eq!(fields.len(), 32);
        assert_eq!(fields[30].0, "😀");
        assert_eq!(fields[31].0, "\u{e000}");
        let emoji = fields
            .binary_search_by(|(name, _)| name.encode_utf16().cmp("😀".encode_utf16()))
            .unwrap();
        assert_eq!(fields[emoji].1, Variant::from(531_i64));
        let private_use = fields
            .binary_search_by(|(name, _)| name.encode_utf16().cmp("\u{e000}".encode_utf16()))
            .unwrap();
        assert_eq!(fields[private_use].1, Variant::from(30_i64));
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
        assert_eq!(variant.value(0), Variant::from(10_i64));
        assert_eq!(variant.value(2), Variant::from(30_i64));
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
