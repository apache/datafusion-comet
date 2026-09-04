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

//! Decoders for Spark JVM aggregate state consumed by native PartialMerge.

use std::{borrow::Cow, sync::Arc};

use arrow::array::{
    builder::{make_builder, ArrayBuilder, ListBuilder},
    Array, ArrayRef, BinaryArray, GenericByteArray, LargeBinaryArray, OffsetSizeTrait,
};
use arrow::datatypes::{DataType, FieldRef, GenericBinaryType};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_comet_shuffle::spark_unsafe::list::{append_to_builder, SparkUnsafeArray};

/// Decoder used by `MergeAsPartial` before forwarding state to the inner accumulator.
#[derive(Clone, Debug)]
pub(crate) enum PartialMergeStateDecoder {
    PassThrough,
    SparkCollect(SparkCollectStateDecoder),
}

impl PartialMergeStateDecoder {
    pub(crate) fn try_new(
        inner_expr: &AggregateFunctionExpr,
        state_fields: &[FieldRef],
    ) -> Result<Self> {
        Ok(
            match SparkCollectStateDecoder::try_new(inner_expr, state_fields)? {
                Some(decoder) => Self::SparkCollect(decoder),
                None => Self::PassThrough,
            },
        )
    }

    pub(crate) fn decode<'a>(&self, values: &'a [ArrayRef]) -> Result<Cow<'a, [ArrayRef]>> {
        match self {
            Self::PassThrough => Ok(Cow::Borrowed(values)),
            Self::SparkCollect(decoder) => decoder.decode(values),
        }
    }
}

/// Decodes Spark JVM collect aggregate buffers into DataFusion collect state.
///
/// Spark's `CollectList` / `CollectSet` are `TypedImperativeAggregate`s. When Spark runs the
/// lower Partial aggregate, each buffer is serialized as a `BinaryType` value containing a
/// single-field `UnsafeRow`; field 0 is the `UnsafeArrayData` with the collected elements.
/// DataFusion's collect accumulators expect the merge input to be a list-typed state column, so
/// mixed Spark-Partial -> Comet-PartialMerge plans must materialize those unsafe bytes into an
/// Arrow `ListArray` before calling the inner accumulator's `merge_batch`. The single-field
/// `UnsafeRow` and nested `UnsafeArrayData` layouts used here are unchanged across Spark 3.4-4.2.
#[derive(Clone, Debug)]
pub(crate) struct SparkCollectStateDecoder {
    item_field: FieldRef,
}

impl SparkCollectStateDecoder {
    fn try_new(
        inner_expr: &AggregateFunctionExpr,
        state_fields: &[FieldRef],
    ) -> Result<Option<Self>> {
        if !matches!(inner_expr.fun().name(), "collect_list" | "collect_set") {
            return Ok(None);
        }

        let [state_field] = state_fields else {
            return Err(DataFusionError::Internal(format!(
                "Spark collect state decoder expected one state field, got {}",
                state_fields.len()
            )));
        };
        let DataType::List(item_field) = state_field.data_type() else {
            return Err(DataFusionError::Internal(format!(
                "Spark collect state decoder expected List state, got {}",
                state_field.data_type()
            )));
        };

        Ok(Some(Self {
            item_field: Arc::clone(item_field),
        }))
    }

    fn decode<'a>(&self, values: &'a [ArrayRef]) -> Result<Cow<'a, [ArrayRef]>> {
        if values.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Spark collect state decoder expected one state column, got {}",
                values.len()
            )));
        }

        match values[0].data_type() {
            DataType::Binary => {
                let array = values[0]
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        Self::decode_error("expected BinaryArray for Binary collect state")
                    })?;
                Ok(Cow::Owned(vec![self.decode_binary_array(array)?]))
            }
            DataType::LargeBinary => {
                let array = values[0]
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        Self::decode_error(
                            "expected LargeBinaryArray for LargeBinary collect state",
                        )
                    })?;
                Ok(Cow::Owned(vec![self.decode_binary_array(array)?]))
            }
            _ => Ok(Cow::Borrowed(values)),
        }
    }

    fn decode_binary_array<O: OffsetSizeTrait>(
        &self,
        array: &GenericByteArray<GenericBinaryType<O>>,
    ) -> Result<ArrayRef> {
        let mut builder = self.new_list_builder(array.len());

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                builder.append_null();
            } else {
                self.append_unsafe_row_array(array.value(row_idx), &mut builder)?;
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn new_list_builder(&self, capacity: usize) -> ListBuilder<Box<dyn ArrayBuilder>> {
        let value_builder = make_builder(self.item_field.data_type(), capacity);
        ListBuilder::with_capacity(value_builder, capacity).with_field(Arc::clone(&self.item_field))
    }

    fn append_unsafe_row_array(
        &self,
        row_bytes: &[u8],
        builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> Result<()> {
        match self.spark_array_from_single_field_unsafe_row(row_bytes)? {
            Some(array) => {
                append_to_builder::<true>(self.item_field.data_type(), builder.values(), &array)
                    .map_err(|e| Self::decode_error(e.to_string()))?;
                builder.append(true);
            }
            None => builder.append_null(),
        }
        Ok(())
    }

    fn spark_array_from_single_field_unsafe_row(
        &self,
        row_bytes: &[u8],
    ) -> Result<Option<SparkUnsafeArray>> {
        const BITSET_WIDTH: usize = 8;
        const FIXED_FIELD_WIDTH: usize = 8;
        const ARRAY_FIELD_INDEX: usize = 0;
        const MIN_ROW_WIDTH: usize = BITSET_WIDTH + FIXED_FIELD_WIDTH;

        if row_bytes.len() < MIN_ROW_WIDTH {
            return Err(Self::decode_error(format!(
                "UnsafeRow collect buffer is too small: {} bytes",
                row_bytes.len()
            )));
        }

        let null_bits = i64::from_le_bytes(
            row_bytes[0..BITSET_WIDTH]
                .try_into()
                .expect("slice length checked"),
        );
        if (null_bits & (1_i64 << ARRAY_FIELD_INDEX)) != 0 {
            return Ok(None);
        }

        let offset_and_size = i64::from_le_bytes(
            row_bytes[BITSET_WIDTH..MIN_ROW_WIDTH]
                .try_into()
                .expect("slice length checked"),
        );
        let offset = (offset_and_size >> 32) as i32;
        let size = offset_and_size as i32;

        if offset != MIN_ROW_WIDTH as i32 || size < 0 {
            return Err(Self::decode_error(format!(
                "Invalid single-field UnsafeRow array offset/size: offset={offset}, size={size}, row_size={}",
                row_bytes.len()
            )));
        }

        let offset = offset as usize;
        let size = size as usize;
        let end = offset.checked_add(size).ok_or_else(|| {
            Self::decode_error(format!(
                "UnsafeRow array field range overflows: offset={offset}, size={size}"
            ))
        })?;
        if end > row_bytes.len() {
            return Err(Self::decode_error(format!(
                "UnsafeRow array field range is out of bounds: offset={offset}, size={size}, row_size={}",
                row_bytes.len()
            )));
        }
        let array_bytes = &row_bytes[offset..end];
        Self::validate_unsafe_array(array_bytes, self.item_field.data_type(), true)?;
        Ok(Some(SparkUnsafeArray::new(array_bytes.as_ptr() as i64)))
    }

    fn validate_unsafe_array(
        bytes: &[u8],
        element_type: &DataType,
        nullable: bool,
    ) -> Result<usize> {
        const NUM_ELEMENTS_WIDTH: usize = 8;

        if bytes.len() < NUM_ELEMENTS_WIDTH {
            return Err(Self::decode_error(format!(
                "UnsafeArrayData is too small: {} bytes",
                bytes.len()
            )));
        }

        let num_elements = Self::read_i64(bytes, 0, "UnsafeArrayData element count")?;
        if !(0..=i32::MAX as i64).contains(&num_elements) {
            return Err(Self::decode_error(format!(
                "Invalid UnsafeArrayData element count: {num_elements}"
            )));
        }
        let num_elements = num_elements as usize;
        let bitset_width = num_elements
            .div_ceil(64)
            .checked_mul(8)
            .ok_or_else(|| Self::decode_error("UnsafeArrayData bitset size overflows"))?;
        let header_width = NUM_ELEMENTS_WIDTH
            .checked_add(bitset_width)
            .ok_or_else(|| Self::decode_error("UnsafeArrayData header size overflows"))?;
        let (element_width, variable_width) = Self::unsafe_array_layout(element_type)?;
        let fixed_width = num_elements
            .checked_mul(element_width)
            .ok_or_else(|| Self::decode_error("UnsafeArrayData fixed region size overflows"))?;
        let fixed_end = header_width
            .checked_add(fixed_width)
            .ok_or_else(|| Self::decode_error("UnsafeArrayData fixed region end overflows"))?;
        if fixed_end > bytes.len() {
            return Err(Self::decode_error(format!(
                "UnsafeArrayData fixed region is out of bounds: required={fixed_end}, size={}",
                bytes.len()
            )));
        }

        if variable_width {
            for index in 0..num_elements {
                if nullable && Self::is_null(bytes, NUM_ELEMENTS_WIDTH, index)? {
                    continue;
                }
                let slot_offset = header_width + index * element_width;
                let value = Self::variable_value(bytes, slot_offset, fixed_end)?;
                Self::validate_variable_value(value, element_type)?;
            }
        }

        Ok(num_elements)
    }

    fn validate_unsafe_row(bytes: &[u8], fields: &arrow::datatypes::Fields) -> Result<()> {
        const FIELD_WIDTH: usize = 8;

        let bitset_width = fields
            .len()
            .div_ceil(64)
            .checked_mul(8)
            .ok_or_else(|| Self::decode_error("UnsafeRow bitset size overflows"))?;
        let fixed_width = fields
            .len()
            .checked_mul(FIELD_WIDTH)
            .ok_or_else(|| Self::decode_error("UnsafeRow fixed region size overflows"))?;
        let fixed_end = bitset_width
            .checked_add(fixed_width)
            .ok_or_else(|| Self::decode_error("UnsafeRow fixed region end overflows"))?;
        if fixed_end > bytes.len() {
            return Err(Self::decode_error(format!(
                "UnsafeRow fixed region is out of bounds: required={fixed_end}, size={}",
                bytes.len()
            )));
        }

        for (index, field) in fields.iter().enumerate() {
            let (_, variable_width) = Self::unsafe_array_layout(field.data_type())?;
            if variable_width && !Self::is_null(bytes, 0, index)? {
                let slot_offset = bitset_width + index * FIELD_WIDTH;
                let value = Self::variable_value(bytes, slot_offset, fixed_end)?;
                Self::validate_variable_value(value, field.data_type())?;
            }
        }

        Ok(())
    }

    fn validate_unsafe_map(bytes: &[u8], entry_field: &FieldRef) -> Result<()> {
        const KEY_SIZE_WIDTH: usize = 8;

        if bytes.len() < KEY_SIZE_WIDTH || bytes.len() > i32::MAX as usize {
            return Err(Self::decode_error(format!(
                "Invalid UnsafeMapData size: {} bytes",
                bytes.len()
            )));
        }
        let key_size = Self::read_i64(bytes, 0, "UnsafeMapData key array size")?;
        if !(0..=i32::MAX as i64).contains(&key_size) {
            return Err(Self::decode_error(format!(
                "Invalid UnsafeMapData key array size: {key_size}"
            )));
        }
        let key_end = KEY_SIZE_WIDTH
            .checked_add(key_size as usize)
            .ok_or_else(|| Self::decode_error("UnsafeMapData key array end overflows"))?;
        if key_end > bytes.len() {
            return Err(Self::decode_error(format!(
                "UnsafeMapData key array is out of bounds: key_end={key_end}, size={}",
                bytes.len()
            )));
        }

        let DataType::Struct(fields) = entry_field.data_type() else {
            return Err(Self::decode_error(format!(
                "UnsafeMapData entry field must be Struct, got {}",
                entry_field.data_type()
            )));
        };
        if fields.len() != 2 {
            return Err(Self::decode_error(format!(
                "UnsafeMapData entry struct must have two fields, got {}",
                fields.len()
            )));
        }

        let key_count = Self::validate_unsafe_array(
            &bytes[KEY_SIZE_WIDTH..key_end],
            fields[0].data_type(),
            false,
        )?;
        let value_count =
            Self::validate_unsafe_array(&bytes[key_end..], fields[1].data_type(), true)?;
        if key_count != value_count {
            return Err(Self::decode_error(format!(
                "UnsafeMapData key/value counts differ: {key_count} vs {value_count}"
            )));
        }

        Ok(())
    }

    fn validate_variable_value(bytes: &[u8], data_type: &DataType) -> Result<()> {
        match data_type {
            DataType::Binary | DataType::Utf8 => Ok(()),
            DataType::Decimal128(precision, _) if *precision > 18 => {
                if bytes.is_empty() || bytes.len() > 16 {
                    Err(Self::decode_error(format!(
                        "Invalid wide decimal byte length: {}",
                        bytes.len()
                    )))
                } else {
                    Ok(())
                }
            }
            DataType::List(field) => {
                Self::validate_unsafe_array(bytes, field.data_type(), true).map(|_| ())
            }
            DataType::Struct(fields) => Self::validate_unsafe_row(bytes, fields),
            DataType::Map(field, _) => Self::validate_unsafe_map(bytes, field),
            _ => Err(Self::decode_error(format!(
                "Unsupported variable-width collect state type: {data_type}"
            ))),
        }
    }

    fn unsafe_array_layout(data_type: &DataType) -> Result<(usize, bool)> {
        let layout = match data_type {
            DataType::Boolean | DataType::Int8 => (1, false),
            DataType::Int16 => (2, false),
            DataType::Int32 | DataType::Float32 | DataType::Date32 => (4, false),
            DataType::Int64
            | DataType::Float64
            | DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)
            | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => (8, false),
            DataType::Decimal128(precision, _) if *precision <= 18 => (8, false),
            DataType::Null => (0, false),
            DataType::Binary
            | DataType::Utf8
            | DataType::List(_)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Decimal128(_, _) => (8, true),
            _ => {
                return Err(Self::decode_error(format!(
                    "Unsupported collect state type: {data_type}"
                )))
            }
        };
        Ok(layout)
    }

    fn variable_value<'a>(
        bytes: &'a [u8],
        slot_offset: usize,
        fixed_end: usize,
    ) -> Result<&'a [u8]> {
        let offset_and_size = Self::read_i64(bytes, slot_offset, "offset/size slot")?;
        let offset = (offset_and_size >> 32) as i32;
        let size = offset_and_size as i32;
        if offset == 0 && size == 0 {
            return Ok(&[]);
        }
        if offset < 0 || size < 0 || (offset as usize) < fixed_end {
            return Err(Self::decode_error(format!(
                "Invalid variable-width offset/size: offset={offset}, size={size}, fixed_end={fixed_end}"
            )));
        }

        let offset = offset as usize;
        let size = size as usize;
        let end = offset.checked_add(size).ok_or_else(|| {
            Self::decode_error(format!(
                "Variable-width range overflows: offset={offset}, size={size}"
            ))
        })?;
        bytes.get(offset..end).ok_or_else(|| {
            Self::decode_error(format!(
                "Variable-width range is out of bounds: offset={offset}, size={size}, buffer_size={}",
                bytes.len()
            ))
        })
    }

    fn is_null(bytes: &[u8], bitset_offset: usize, index: usize) -> Result<bool> {
        let word_offset = bitset_offset + (index / 64) * 8;
        let word = Self::read_i64(bytes, word_offset, "null bitset")?;
        Ok((word & (1_i64 << (index % 64))) != 0)
    }

    fn read_i64(bytes: &[u8], offset: usize, label: &str) -> Result<i64> {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| Self::decode_error(format!("{label} offset overflows")))?;
        let value = bytes.get(offset..end).ok_or_else(|| {
            Self::decode_error(format!(
                "{label} is out of bounds: offset={offset}, size={}",
                bytes.len()
            ))
        })?;
        Ok(i64::from_le_bytes(
            value.try_into().expect("slice length checked"),
        ))
    }

    fn decode_error(message: impl Into<String>) -> DataFusionError {
        DataFusionError::Execution(format!(
            "Failed to decode Spark UnsafeRow collect aggregate buffer: {}",
            message.into()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, StringArray};
    use arrow::datatypes::Field;

    fn collect_state_decoder(element_type: DataType) -> SparkCollectStateDecoder {
        SparkCollectStateDecoder {
            item_field: Arc::new(Field::new_list_field(element_type, true)),
        }
    }

    fn unsafe_row_with_array(array: Vec<u8>) -> Vec<u8> {
        const ARRAY_OFFSET: usize = 16;
        let mut row = vec![0_u8; ARRAY_OFFSET];
        let offset_and_size = ((ARRAY_OFFSET as i64) << 32) | array.len() as i64;
        row[8..16].copy_from_slice(&offset_and_size.to_le_bytes());
        row.extend_from_slice(&array);
        row
    }

    fn unsafe_array_i32(values: &[Option<i32>]) -> Vec<u8> {
        let num_elements = values.len();
        let bitset_words = num_elements.div_ceil(64);
        let header_len = 8 + bitset_words * 8;
        let mut bytes = vec![0_u8; header_len + num_elements * std::mem::size_of::<i32>()];
        bytes[0..8].copy_from_slice(&(num_elements as i64).to_le_bytes());

        let mut null_bits = 0_i64;
        for (idx, value) in values.iter().enumerate() {
            let value_offset = header_len + idx * std::mem::size_of::<i32>();
            match value {
                Some(value) => {
                    bytes[value_offset..value_offset + 4].copy_from_slice(&value.to_le_bytes())
                }
                None => null_bits |= 1_i64 << idx,
            }
        }
        if bitset_words > 0 {
            bytes[8..16].copy_from_slice(&null_bits.to_le_bytes());
        }

        bytes
    }

    fn unsafe_array_utf8(values: &[Option<&str>]) -> Vec<u8> {
        let num_elements = values.len();
        let bitset_words = num_elements.div_ceil(64);
        let header_len = 8 + bitset_words * 8;
        let fixed_len = num_elements * 8;
        let mut bytes = vec![0_u8; header_len + fixed_len];
        bytes[0..8].copy_from_slice(&(num_elements as i64).to_le_bytes());

        let mut null_bits = 0_i64;
        for (idx, value) in values.iter().enumerate() {
            let slot_offset = header_len + idx * 8;
            match value {
                Some(value) => {
                    let value_offset = bytes.len();
                    let value_bytes = value.as_bytes();
                    bytes.extend_from_slice(value_bytes);
                    let padded_len = value_bytes.len().next_multiple_of(8);
                    bytes.resize(value_offset + padded_len, 0);

                    let offset_and_size = ((value_offset as i64) << 32) | value_bytes.len() as i64;
                    bytes[slot_offset..slot_offset + 8]
                        .copy_from_slice(&offset_and_size.to_le_bytes());
                }
                None => null_bits |= 1_i64 << idx,
            }
        }
        if bitset_words > 0 {
            bytes[8..16].copy_from_slice(&null_bits.to_le_bytes());
        }

        bytes
    }

    #[test]
    fn decodes_spark_collect_binary_int_state() {
        let first = unsafe_row_with_array(unsafe_array_i32(&[Some(1)]));
        // The first row is intentionally 36 bytes, so the second row starts at an unaligned
        // address in the Arrow Binary values buffer. The decoder must not rely on UnsafeRow
        // alignment once Spark bytes have been materialized into Arrow BinaryArray storage.
        assert_eq!(first.len(), 36);
        let second = unsafe_row_with_array(unsafe_array_i32(&[Some(2), None, Some(4)]));
        let third = unsafe_row_with_array(unsafe_array_i32(&[]));

        let binary = BinaryArray::from_iter([
            Some(first.as_slice()),
            Some(second.as_slice()),
            None,
            Some(third.as_slice()),
        ]);
        let input = [Arc::new(binary) as ArrayRef];
        let decoder = collect_state_decoder(DataType::Int32);
        let decoded = decoder.decode(&input).unwrap();
        let list = decoded.as_ref()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(list.len(), 4);
        assert!(!list.is_null(0));
        assert!(!list.is_null(1));
        assert!(list.is_null(2));
        assert!(!list.is_null(3));

        let first_values = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(first_values, vec![1]);

        let second_values = list
            .value(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(second_values.values(), &[2, 0, 4]);
        assert!(!second_values.is_null(0));
        assert!(second_values.is_null(1));
        assert!(!second_values.is_null(2));

        assert_eq!(list.value(3).len(), 0);
    }

    #[test]
    fn decodes_spark_collect_binary_string_state() {
        let row = unsafe_row_with_array(unsafe_array_utf8(&[
            Some("alpha"),
            Some("βeta"),
            None,
            Some("spark"),
        ]));
        let binary = BinaryArray::from_iter([Some(row.as_slice())]);
        let input = [Arc::new(binary) as ArrayRef];
        let decoder = collect_state_decoder(DataType::Utf8);
        let decoded = decoder.decode(&input).unwrap();
        let list = decoded.as_ref()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list
            .value(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();

        assert_eq!(values.value(0), "alpha");
        assert_eq!(values.value(1), "βeta");
        assert!(values.is_null(2));
        assert_eq!(values.value(3), "spark");
    }

    #[test]
    fn decodes_null_spark_collect_state() {
        let mut row = vec![0_u8; 16];
        row[0] = 1;
        let binary = BinaryArray::from_iter([Some(row.as_slice())]);
        let input = [Arc::new(binary) as ArrayRef];
        let decoder = collect_state_decoder(DataType::Int32);
        let decoded = decoder.decode(&input).unwrap();
        let list = decoded.as_ref()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert!(list.is_null(0));
    }

    #[test]
    fn rejects_malformed_spark_collect_state() {
        let truncated_row = vec![0_u8; 15];

        let mut two_field_row = vec![0_u8; 32];
        let offset_and_size = (24_i64 << 32) | 8;
        two_field_row[8..16].copy_from_slice(&offset_and_size.to_le_bytes());

        let mut truncated_array = vec![0_u8; 16];
        truncated_array[0..8].copy_from_slice(&1_i64.to_le_bytes());
        let truncated_array_row = unsafe_row_with_array(truncated_array);

        let decoder = collect_state_decoder(DataType::Int32);
        for (name, row) in [
            ("truncated row", truncated_row),
            ("different field count", two_field_row),
            ("truncated array", truncated_array_row),
        ] {
            let binary = BinaryArray::from_iter([Some(row.as_slice())]);
            let input = [Arc::new(binary) as ArrayRef];
            let error = match decoder.decode(&input) {
                Ok(_) => panic!("{name} was accepted"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("Failed to decode Spark UnsafeRow collect aggregate buffer"),
                "{name}: {error}"
            );
        }
    }
}
