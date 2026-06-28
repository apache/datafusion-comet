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

use std::sync::Arc;

use arrow::array::{
    builder::{make_builder, ListBuilder},
    Array, ArrayRef, BinaryArray, LargeBinaryArray,
};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_comet_shuffle::spark_unsafe::list::{append_to_builder, SparkUnsafeArray};

/// State arrays after optional Spark-JVM-state decoding.
pub(crate) enum DecodedState<'a> {
    Borrowed(&'a [ArrayRef]),
    Owned(Vec<ArrayRef>),
}

impl DecodedState<'_> {
    pub(crate) fn arrays(&self) -> &[ArrayRef] {
        match self {
            Self::Borrowed(values) => values,
            Self::Owned(values) => values,
        }
    }
}

/// Decoder used by `MergeAsPartial` before forwarding state to the inner accumulator.
#[derive(Clone, Debug)]
pub(crate) enum PartialMergeStateDecoder {
    PassThrough,
    SparkCollect(SparkCollectStateDecoder),
}

impl PartialMergeStateDecoder {
    pub(crate) fn new(inner_expr: &AggregateFunctionExpr, state_fields: &[FieldRef]) -> Self {
        SparkCollectStateDecoder::try_new(inner_expr, state_fields)
            .map(Self::SparkCollect)
            .unwrap_or(Self::PassThrough)
    }

    pub(crate) fn decode<'a>(&self, values: &'a [ArrayRef]) -> Result<DecodedState<'a>> {
        match self {
            Self::PassThrough => Ok(DecodedState::Borrowed(values)),
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
/// Arrow `ListArray` before calling the inner accumulator's `merge_batch`.
#[derive(Clone, Debug)]
pub(crate) struct SparkCollectStateDecoder {
    state_field: FieldRef,
}

impl SparkCollectStateDecoder {
    fn try_new(inner_expr: &AggregateFunctionExpr, state_fields: &[FieldRef]) -> Option<Self> {
        match (inner_expr.fun().name(), state_fields) {
            ("collect_list" | "collect_set", [state_field])
                if matches!(state_field.data_type(), DataType::List(_)) =>
            {
                Some(Self {
                    state_field: Arc::clone(state_field),
                })
            }
            _ => None,
        }
    }

    fn decode<'a>(&self, values: &'a [ArrayRef]) -> Result<DecodedState<'a>> {
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
                Ok(DecodedState::Owned(vec![self.decode_binary_array(array)?]))
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
                Ok(DecodedState::Owned(vec![
                    self.decode_large_binary_array(array)?
                ]))
            }
            _ => Ok(DecodedState::Borrowed(values)),
        }
    }

    fn decode_binary_array(&self, array: &BinaryArray) -> Result<ArrayRef> {
        let item_field = self.item_field()?;
        let mut builder = self.new_list_builder(item_field, array.len());

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                builder.append_null();
            } else {
                self.append_unsafe_row_array(array.value(row_idx), item_field, &mut builder)?;
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn decode_large_binary_array(&self, array: &LargeBinaryArray) -> Result<ArrayRef> {
        let item_field = self.item_field()?;
        let mut builder = self.new_list_builder(item_field, array.len());

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                builder.append_null();
            } else {
                self.append_unsafe_row_array(array.value(row_idx), item_field, &mut builder)?;
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn item_field(&self) -> Result<&FieldRef> {
        match self.state_field.data_type() {
            DataType::List(item_field) => Ok(item_field),
            other => Err(Self::decode_error(format!(
                "collect state field must be List, got {other:?}"
            ))),
        }
    }

    fn new_list_builder(
        &self,
        item_field: &FieldRef,
        capacity: usize,
    ) -> ListBuilder<Box<dyn arrow::array::builder::ArrayBuilder>> {
        let value_builder = make_builder(item_field.data_type(), capacity);
        ListBuilder::with_capacity(value_builder, capacity).with_field(Arc::clone(item_field))
    }

    fn append_unsafe_row_array(
        &self,
        row_bytes: &[u8],
        item_field: &FieldRef,
        builder: &mut ListBuilder<Box<dyn arrow::array::builder::ArrayBuilder>>,
    ) -> Result<()> {
        match Self::spark_array_from_single_field_unsafe_row(row_bytes)? {
            Some(array) => {
                append_to_builder::<true>(item_field.data_type(), builder.values(), &array)
                    .map_err(|e| Self::decode_error(e.to_string()))?;
                builder.append(true);
            }
            None => builder.append_null(),
        }
        Ok(())
    }

    fn spark_array_from_single_field_unsafe_row(
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

        if offset < MIN_ROW_WIDTH as i32 || size < 0 {
            return Err(Self::decode_error(format!(
                "Invalid UnsafeRow array field offset/size: offset={offset}, size={size}, row_size={}",
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
        if size < 8 {
            return Err(Self::decode_error(format!(
                "UnsafeArrayData collect buffer is too small: {size} bytes"
            )));
        }

        let array_addr = unsafe { row_bytes.as_ptr().add(offset) } as i64;
        Ok(Some(SparkUnsafeArray::new(array_addr)))
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
            state_field: Arc::new(Field::new_list(
                "collect_state",
                Field::new_list_field(element_type, true),
                true,
            )),
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
        let list = decoded.arrays()[0]
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
        let list = decoded.arrays()[0]
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
}
