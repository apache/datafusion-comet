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

//! Group small decoded shuffle batches before exporting them to the JVM.
//!
//! Groups are bounded by rows, retained Arrow bytes, and input batch count. An
//! oversized input passes through unchanged instead of being split or copied.
//!
//! Keep inputs until a group is ready, then concatenate once at its actual size.

use std::collections::VecDeque;
use std::ptr::NonNull;

use arrow::array::RecordBatch;
use arrow::buffer::Buffer;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};

const MAX_ROWS: usize = 65_536;
const MAX_GROUP_BATCHES: usize = 1024;
const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;

/// Charge the shared IPC body once, but count each column's output copy separately.
/// Other shared allocations may be conservatively overcounted.
#[derive(Default)]
struct BatchBytes {
    first_owner: Option<NonNull<u8>>,
    retained: usize,
    copied: usize,
}

impl BatchBytes {
    fn add_buffer(&mut self, buffer: &Buffer) {
        let owner = buffer.data_ptr();
        if self.first_owner == Some(owner) {
            return;
        }
        let capacity = buffer.capacity().max(buffer.len());
        if capacity != 0 {
            if self.first_owner.is_none() {
                self.first_owner = Some(owner);
            }
            self.retained = self.retained.saturating_add(capacity);
        }
    }
}

/// Ordered grouping of batches returned by the native shuffle IPC decoder.
///
/// `push` and `finish` enqueue output. Inspect it with `peek_ready`, and drain
/// it with `take_ready` before pushing another input. Call `finish` at EOF.
/// A singleton, an oversized input, or an input at least half the row target is
/// never copied. The byte budget bounds retained group inputs, not the whole
/// reader: concatenation also owns its output and may have one decoded lookahead
/// input. Callers must drop the helper on any error.
#[derive(Debug)]
pub struct DecodedBatchCoalescer {
    max_rows: usize,
    max_bytes: usize,
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    ready: VecDeque<RecordBatch>,
}

impl DecodedBatchCoalescer {
    /// Creates an empty, reader-owned grouper with the production byte budget.
    /// Caps positive row targets at 65536.
    pub fn try_new(max_rows: usize) -> Result<Self> {
        Self::try_new_with_limits(max_rows, MAX_GROUP_BYTES)
    }

    /// Creates an empty grouper with explicit limits, allowing small test budgets.
    /// Invalid limits return a configuration error. Keeping the byte budget at
    /// most 8 MiB also keeps every concatenated STRING/BINARY offset within i32.
    fn try_new_with_limits(max_rows: usize, max_bytes: usize) -> Result<Self> {
        if max_rows == 0 {
            return Err(DataFusionError::Configuration(
                "Decoded shuffle grouping requires a positive row target".into(),
            ));
        }
        if !(1..=MAX_GROUP_BYTES).contains(&max_bytes) {
            return Err(DataFusionError::Configuration(
                "Decoded shuffle grouping requires 1..8388608 bytes".into(),
            ));
        }
        Ok(Self {
            max_rows: max_rows.min(MAX_ROWS),
            max_bytes,
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            ready: VecDeque::new(),
        })
    }

    /// Transfers the oldest ready output to the caller without copying buffers.
    /// Returns None when empty and leaves any pending group unchanged.
    pub fn take_ready(&mut self) -> Option<RecordBatch> {
        self.ready.pop_front()
    }

    /// Borrows the oldest ready output until the next mutation, without draining it.
    pub fn peek_ready(&self) -> Option<&RecordBatch> {
        self.ready.front()
    }

    /// Takes ownership of one decoded input. A flush followed by a passthrough can make two
    /// outputs ready; drain both before reading another input. Drop this helper on error.
    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if !self.ready.is_empty() {
            return Err(DataFusionError::Execution(
                "Drain shuffle grouping output before reading another input".into(),
            ));
        }
        let rows = batch.num_rows();
        let bytes = self.eligible_bytes(&batch);

        if !self.batches.is_empty()
            && (bytes.is_none_or(|bytes| bytes > self.max_bytes - self.bytes)
                || self.batches[0].schema() != batch.schema()
                || rows > self.max_rows - self.rows)
        {
            self.flush()?;
        }
        if let Some(bytes) = bytes {
            self.rows += rows;
            self.bytes += bytes;
            self.batches.push(batch);
            if self.rows == self.max_rows
                || self.bytes == self.max_bytes
                || self.batches.len() == MAX_GROUP_BATCHES
            {
                self.flush()?;
            }
        } else {
            self.ready.push_back(batch);
        }
        Ok(())
    }

    /// Flushes the final group. Repeated calls do not copy or emit it again.
    pub fn finish(&mut self) -> Result<()> {
        self.flush()
    }

    /// Bounds both retained backing allocations and output copies for flat columns.
    /// None means pass through without copying.
    fn eligible_bytes(&self, batch: &RecordBatch) -> Option<usize> {
        if batch.num_rows() == 0
            || batch.num_rows() >= self.max_rows.div_ceil(2)
            || batch.num_columns() == 0
        {
            return None;
        }
        let rows = batch.num_rows();
        // ArrayData reports logical bytes; bitmap builders round up to 64-byte blocks.
        let bitmap_bytes = rows.div_ceil(8);
        let padded_bitmap_bytes = bitmap_bytes.next_multiple_of(64);
        let mut bytes = BatchBytes::default();
        for column in batch.columns() {
            if !matches!(
                column.data_type(),
                DataType::Int32
                    | DataType::Int64
                    | DataType::Boolean
                    | DataType::Utf8
                    | DataType::Binary
            ) {
                return None;
            }
            let data = column.to_data();
            let metadata = std::mem::size_of_val(column.as_ref());
            // to_data may omit an all-valid bitmap; account for the original nulls below.
            let copied =
                data.get_slice_memory_size().ok()? - data.nulls().map_or(0, |_| bitmap_bytes);
            bytes.copied = bytes.copied.saturating_add(copied).saturating_add(metadata);
            bytes.retained = bytes.retained.saturating_add(metadata);
            for buffer in data.buffers() {
                bytes.add_buffer(buffer);
            }
            if let Some(nulls) = column.nulls() {
                bytes.add_buffer(nulls.buffer());
                bytes.copied = bytes.copied.saturating_add(padded_bitmap_bytes);
            }
            if data.data_type() == &DataType::Boolean {
                bytes.copied = bytes
                    .copied
                    .saturating_add(padded_bitmap_bytes - bitmap_bytes);
            }
            if bytes.retained.max(bytes.copied) > self.max_bytes {
                return None;
            }
        }
        Some(bytes.retained.max(bytes.copied))
    }

    fn flush(&mut self) -> Result<()> {
        if self.batches.len() < 2 {
            self.ready.extend(self.batches.drain(..));
        } else {
            self.ready
                .push_back(concat_batches(&self.batches[0].schema(), &self.batches)?);
            self.batches.clear();
        }
        self.rows = 0;
        self.bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, AsArray, BinaryArray, BooleanArray, Int32Array, Int64Array, StringArray,
    };
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::ipc::writer::StreamWriter;

    fn one_column(array: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn ints(values: impl IntoIterator<Item = i32>) -> RecordBatch {
        one_column(Arc::new(Int32Array::from_iter_values(values)))
    }

    fn encode_frame(batch: &RecordBatch) -> Vec<u8> {
        let mut frame = b"NONE".to_vec();
        let mut writer = StreamWriter::try_new(&mut frame, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        frame
    }

    fn collect(max_rows: usize, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
        collect_with_limits(max_rows, MAX_GROUP_BYTES, batches)
    }

    fn collect_with_limits(
        max_rows: usize,
        max_bytes: usize,
        batches: Vec<RecordBatch>,
    ) -> Vec<RecordBatch> {
        let mut coalescer =
            DecodedBatchCoalescer::try_new_with_limits(max_rows, max_bytes).unwrap();
        let mut output = Vec::new();
        for batch in batches {
            coalescer.push(batch).unwrap();
            while let Some(batch) = coalescer.take_ready() {
                output.push(batch);
            }
        }
        coalescer.finish().unwrap();
        while let Some(batch) = coalescer.take_ready() {
            output.push(batch);
        }
        output
    }

    #[test]
    fn finishing_repeatedly_emits_the_pending_group_once() {
        let mut coalescer = DecodedBatchCoalescer::try_new(8).unwrap();
        coalescer.push(ints(0..1)).unwrap();
        coalescer.push(ints(1..2)).unwrap();
        coalescer.finish().unwrap();
        coalescer.finish().unwrap();
        assert_eq!(coalescer.take_ready(), Some(ints(0..2)));
        coalescer.finish().unwrap();
        assert!(coalescer.take_ready().is_none());
    }

    #[test]
    fn singleton_large_and_near_full_inputs_are_not_copied() {
        for batches in [
            vec![ints(0..1)],
            vec![ints(0..1), ints(1..7)],
            vec![ints(0..6), ints(6..7)],
            vec![ints(0..1), ints(1..10)],
        ] {
            let originals: Vec<_> = batches
                .iter()
                .map(|batch| batch.column(0).clone())
                .collect();
            let output = collect(8, batches);
            assert_eq!(output.len(), originals.len());
            for (batch, original) in output.iter().zip(originals) {
                assert!(Arc::ptr_eq(batch.column(0), &original));
            }
        }
    }

    #[test]
    fn row_and_input_count_limits_preserve_order() {
        let batches = (0..4).map(|i| ints(i * 2..i * 2 + 2)).collect();
        assert_eq!(collect(8, batches), vec![ints(0..8)]);
        assert_eq!(
            collect(8, vec![ints(0..3), ints(3..6), ints(6..9)]),
            vec![ints(0..6), ints(6..9)]
        );
        let batches = (0..1025).map(|i| ints(i..i + 1)).collect();
        assert_eq!(
            collect(MAX_ROWS, batches),
            vec![ints(0..1024), ints(1024..1025)]
        );
    }

    #[test]
    fn byte_limit_flushes_at_and_before_crossing_the_boundary() {
        let inputs: Vec<_> = (0..3).map(|i| ints(i..i + 1)).collect();
        let two_batches = inputs[0].get_array_memory_size() + inputs[1].get_array_memory_size();
        assert_eq!(
            collect_with_limits(32, two_batches, inputs.clone()),
            vec![ints(0..2), ints(2..3)]
        );
        let output = collect_with_limits(32, two_batches - 1, inputs.clone());
        assert_eq!(output, inputs);
        for (output, input) in output.iter().zip(&inputs) {
            assert!(Arc::ptr_eq(output.column(0), input.column(0)));
        }

        let mut coalescer = DecodedBatchCoalescer::try_new_with_limits(32, two_batches).unwrap();
        coalescer.push(inputs[0].clone()).unwrap();
        assert!(coalescer.peek_ready().is_none());
        coalescer.push(inputs[1].clone()).unwrap();
        assert_eq!(coalescer.take_ready(), Some(ints(0..2)));
        coalescer.push(inputs[2].clone()).unwrap();
        assert!(coalescer.take_ready().is_none());
        coalescer.finish().unwrap();
        assert_eq!(coalescer.take_ready(), Some(ints(2..3)));
    }

    #[test]
    fn wide_json_values_pass_through_without_copying() {
        let small_json = format!("{{\"payload\":\"{}\"}}", "x".repeat(32));
        let large_json = format!("{{\"payload\":\"{}\"}}", "x".repeat(2048));
        let small = one_column(Arc::new(StringArray::from(vec![small_json.as_str()])));
        let large = one_column(Arc::new(StringArray::from(vec![large_json.as_str()])));
        let original = large.column(0).clone();
        let max_bytes = 2 * small.get_array_memory_size();
        assert!(large.get_array_memory_size() > max_bytes);
        let output = collect_with_limits(
            32,
            max_bytes,
            vec![small.clone(), large.clone(), small.clone(), small, large],
        );
        let expected = one_column(Arc::new(StringArray::from(vec![
            small_json.as_str(),
            small_json.as_str(),
        ])));
        assert_eq!(output.len(), 4);
        assert_eq!(output[0].num_rows(), 1);
        assert!(Arc::ptr_eq(output[1].column(0), &original));
        assert_eq!(output[2], expected);
        assert!(Arc::ptr_eq(output[3].column(0), &original));
    }

    #[test]
    fn sliced_backing_buffers_count_toward_the_byte_budget() {
        let wide = "x".repeat(2048);
        let backing = one_column(Arc::new(StringArray::from(vec!["a", wide.as_str()])));
        let slice = backing.slice(0, 1);
        let small = one_column(Arc::new(StringArray::from(vec!["b"])));
        let max_bytes = 2 * small.get_array_memory_size();
        assert!(slice.get_array_memory_size() > max_bytes);
        let original = slice.column(0).clone();
        let output = collect_with_limits(32, max_bytes, vec![small.clone(), slice, small]);
        assert_eq!(output.len(), 3);
        assert!(Arc::ptr_eq(output[1].column(0), &original));
    }

    #[test]
    fn all_valid_null_backing_counts_toward_the_byte_budget() {
        let nulls = arrow::buffer::NullBuffer::new_valid(16 * 1024).slice(0, 1);
        let input = one_column(Arc::new(Int32Array::new(vec![1].into(), Some(nulls))));
        assert!(input.column(0).to_data().nulls().is_none());
        let output = collect_with_limits(8, 512, vec![ints(0..1), input.clone(), ints(2..3)]);
        assert_eq!(output, vec![ints(0..1), input.clone(), ints(2..3)]);
        assert!(Arc::ptr_eq(output[1].column(0), input.column(0)));
    }

    #[test]
    fn shared_ipc_body_is_charged_once_for_wide_batches() {
        let schema = Arc::new(Schema::new(
            (0..128)
                .map(|column| Field::new(format!("c{column}"), DataType::Int32, true))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<ArrayRef> = (0..128)
            .map(|column| {
                Arc::new(Int32Array::from_iter((0..64).map(|row| {
                    ((row + column + 1) % 17 != 0).then_some(row * 31 + column)
                }))) as ArrayRef
            })
            .collect();
        let input = RecordBatch::try_new(schema, arrays).unwrap();
        let frame = encode_frame(&input);
        let decoded = crate::read_ipc_compressed(&frame).unwrap();
        let mut coalescer = DecodedBatchCoalescer::try_new(8192).unwrap();
        let first_buffer = decoded
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .inner();
        let last_buffer = decoded
            .column(127)
            .as_primitive::<Int32Type>()
            .values()
            .inner();
        assert_eq!(first_buffer.data_ptr(), last_buffer.data_ptr());
        assert!(decoded.get_array_memory_size() > MAX_GROUP_BYTES);

        for _ in 0..128 {
            coalescer
                .push(crate::read_ipc_compressed(&frame).unwrap())
                .unwrap();
        }
        let output = coalescer.take_ready().unwrap();
        assert_eq!(output.num_rows(), 8192);
        assert!(coalescer.take_ready().is_none());
        for offset in (0..8192).step_by(64) {
            assert_eq!(output.slice(offset, 64), input);
        }
    }

    #[test]
    fn aliased_columns_are_charged_for_each_output_copy() {
        let array = ints(0..160).column(0).clone();
        let schema = Arc::new(Schema::new(
            (0..16)
                .map(|column| Field::new(format!("c{column}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        let input = RecordBatch::try_new(schema, vec![array.clone(); 16]).unwrap();
        let metadata = 16 * std::mem::size_of_val(array.as_ref());
        let copied = metadata + 16 * 160 * 4;
        let retained = metadata
            + array
                .as_primitive::<Int32Type>()
                .values()
                .inner()
                .capacity();
        assert!(copied > retained);
        let output = collect_with_limits(8192, copied * 2 - 1, vec![input.clone(), input.clone()]);
        assert_eq!(output, vec![input.clone(), input]);
        for batch in output {
            for column in batch.columns() {
                assert!(Arc::ptr_eq(column, &array));
            }
        }
    }

    #[test]
    fn invalid_limits_are_configuration_errors() {
        assert!(DecodedBatchCoalescer::try_new(0).is_err());
        assert!(DecodedBatchCoalescer::try_new_with_limits(8, 0).is_err());
        assert!(DecodedBatchCoalescer::try_new_with_limits(8, MAX_GROUP_BYTES + 1).is_err());
    }

    #[test]
    fn large_row_targets_are_clamped() {
        let batch = ints(0..(MAX_ROWS / 2) as i32);
        let mut coalescer = DecodedBatchCoalescer::try_new(usize::MAX).unwrap();
        coalescer.push(batch.clone()).unwrap();
        let output = coalescer.take_ready().unwrap();
        assert!(Arc::ptr_eq(output.column(0), batch.column(0)));
    }

    #[test]
    fn nulls_and_supported_variable_width_values_keep_order() {
        let families: Vec<(ArrayRef, ArrayRef, ArrayRef)> = vec![
            (
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
            ),
            (
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            ),
            (
                Arc::new(StringArray::from(vec![Some(""), None])),
                Arc::new(StringArray::from(vec![Some("json")])),
                Arc::new(StringArray::from(vec![Some(""), None, Some("json")])),
            ),
            (
                Arc::new(BinaryArray::from(vec![Some(b"".as_slice()), None])),
                Arc::new(BinaryArray::from(vec![Some(b"bytes".as_slice())])),
                Arc::new(BinaryArray::from(vec![
                    Some(b"".as_slice()),
                    None,
                    Some(b"bytes".as_slice()),
                ])),
            ),
        ];
        for (first, second, expected) in families {
            assert_eq!(
                collect(8, vec![one_column(first), one_column(second)]),
                vec![one_column(expected)]
            );
        }
    }

    #[test]
    fn schema_changes_and_unsupported_types_pass_in_order() {
        let first = ints(0..1);
        let renamed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int32,
                true,
            )])),
            first.columns().to_vec(),
        )
        .unwrap();
        let unsupported = one_column(Arc::new(arrow::array::UInt32Array::from(vec![1])));
        let batches = vec![first, renamed, unsupported, ints(1..2)];
        assert_eq!(collect(8, batches.clone()), batches);
    }

    #[test]
    fn ready_lookahead_and_pending_inputs_have_owned_lifetimes() {
        let first = ints(0..1);
        let large = ints(1..9);
        let first_weak = Arc::downgrade(first.column(0));
        let large_weak = Arc::downgrade(large.column(0));
        let mut coalescer = DecodedBatchCoalescer::try_new(8).unwrap();
        coalescer.push(first).unwrap();
        assert!(coalescer.peek_ready().is_none());
        coalescer.push(large).unwrap();
        assert_eq!(coalescer.peek_ready().unwrap().num_rows(), 1);
        let output = coalescer.take_ready().unwrap();
        assert!(first_weak.upgrade().is_some());
        drop(output);
        assert!(first_weak.upgrade().is_none());
        assert!(large_weak.upgrade().is_some());
        let lookahead = coalescer.take_ready().unwrap();
        assert_eq!(lookahead, ints(1..9));
        drop(lookahead);
        assert!(large_weak.upgrade().is_none());
        let pending = ints(10..11);
        let pending_weak = Arc::downgrade(pending.column(0));
        coalescer.push(pending).unwrap();
        assert!(coalescer.peek_ready().is_none());
        drop(coalescer);
        assert!(pending_weak.upgrade().is_none());
    }

    #[test]
    fn empty_inputs_and_unit_row_target_pass_through() {
        let empty = ints(0..0);
        assert_eq!(collect(8, vec![empty.clone()]), vec![empty]);
        assert_eq!(collect(1, vec![ints(0..1)]), vec![ints(0..1)]);
    }
}
