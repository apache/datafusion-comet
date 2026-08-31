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

use crate::codec_context::ShuffleCodecContext;
use crate::metrics::ShufflePartitionerMetrics;
use crate::writers::partition_writer::PartitionWriter;
use crate::ShuffleBlockWriter;
use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayRef, BinaryViewArray, FixedSizeListArray, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, MapArray, RecordBatch, RunArray, StringViewArray, StructArray,
    UnionArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int16Type, Int32Type, Int64Type};
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::common::{DataFusionError, Result};
use datafusion_comet_jni_bridge::ShufflePartitionPusher;
use std::io::{self, Cursor, Seek, SeekFrom, Write};
use std::sync::Arc;

/// Sends complete Comet shuffle blocks to a task-owned remote shuffle pusher.
///
/// Each callback receives exactly one self-contained, length-prefixed Arrow
/// IPC block. Keeping block boundaries intact lets remote shuffle services
/// concatenate or retrieve the resulting payloads without repairing partially
/// encoded blocks. The encoded frame is checked against `max_frame_size`
/// before it is exposed to the pusher.
///
/// Partition data may arrive in any order until its partition is finalized.
/// Finalization follows the same ascending-partition contract as the existing
/// local shuffle writer.
pub(crate) struct RssPartitionWriter {
    block_writer: ShuffleBlockWriter,
    pusher: Arc<dyn ShufflePartitionPusher>,
    num_partitions: usize,
    max_frame_size: usize,
    /// One remote writer serves all of a task's partitions, so the context is task-scoped by
    /// construction. Only the Arrow IPC scratch persists between blocks; `write_rss_batch`
    /// frees the zstd workspace with each admitted encode.
    codec_context: ShuffleCodecContext,
    next_partition_to_finish: usize,
    finished: bool,
    failed: bool,
}

impl RssPartitionWriter {
    /// Creates a remote writer without retaining any thread-local JNI state.
    ///
    /// `num_partitions` must be nonzero and each partition identifier must fit
    /// in a JVM `int`. `max_frame_size` is the maximum complete encoded shuffle
    /// block passed to a callback and must also be nonzero.
    pub(crate) fn try_new(
        block_writer: ShuffleBlockWriter,
        pusher: Arc<dyn ShufflePartitionPusher>,
        num_partitions: usize,
        max_frame_size: usize,
    ) -> Result<Self> {
        if num_partitions == 0 {
            return Err(DataFusionError::Execution(
                "Remote shuffle output requires at least one partition".to_string(),
            ));
        }

        if num_partitions - 1 > i32::MAX as usize {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition count {num_partitions} exceeds the JVM partition limit"
            )));
        }

        if max_frame_size == 0 {
            return Err(DataFusionError::Execution(
                "Remote shuffle maximum frame size must be greater than zero".to_string(),
            ));
        }

        Ok(Self {
            block_writer,
            pusher,
            num_partitions,
            max_frame_size,
            codec_context: ShuffleCodecContext::default(),
            next_partition_to_finish: 0,
            finished: false,
            failed: false,
        })
    }

    fn validate_writable_partition(&self, partition_id: usize) -> Result<i32> {
        if self.finished || self.failed {
            return Err(DataFusionError::Execution(
                "Remote shuffle writer has already finished".to_string(),
            ));
        }

        if partition_id >= self.num_partitions {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition {partition_id} is outside the configured range 0..{}",
                self.num_partitions
            )));
        }

        if partition_id < self.next_partition_to_finish {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partition {partition_id} has already been finalized"
            )));
        }

        i32::try_from(partition_id).map_err(|_| {
            DataFusionError::Execution(format!(
                "Remote shuffle partition {partition_id} exceeds the JVM partition limit"
            ))
        })
    }

    fn push_batches<I>(
        &mut self,
        partition_id: i32,
        batches: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let result = (|| {
            for batch in batches.by_ref() {
                self.push_batch_within_limit(partition_id, &batch?, metrics)?;
            }
            Ok(())
        })();
        if result.is_err() {
            // Earlier frames may have been accepted remotely; a partial map cannot be resumed or
            // committed after its input, encoding, reservation, or callback fails.
            self.failed = true;
        }
        result
    }

    fn push_batch_within_limit(
        &mut self,
        partition_id: i32,
        batch: &RecordBatch,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let (metadata_scratch, planning_scratch) = Self::estimated_ipc_metadata_scratch(batch);
        let codec_workspace = self.block_writer.rss_codec_workspace()?;
        let reservation_limit = self.pusher.max_reservation_size();
        if metadata_scratch
            .saturating_add(codec_workspace)
            .saturating_add(60)
            > reservation_limit
        {
            // Neither schema descriptors nor codec workspace gets smaller when rows are split.
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle frame schema and encoding workspace exceed the byte admission budget of {reservation_limit} bytes"
            )));
        }

        // ArrayData and zero-copy nested slices still allocate descriptors. Admit that planning
        // scratch before computing precise live-row sizes; the bound above only borrows the
        // schema/arrays. Drop all planning temporaries and release this short-lived reservation
        // before acquiring the full encoding bound, so there is no partial-reservation growth.
        self.pusher.reserve_partition_data(planning_scratch)?;
        let estimates = (|| {
            Ok::<_, DataFusionError>((
                Self::estimated_pre_compaction_ipc_data_size(batch)?,
                Self::estimated_compaction_scratch(batch)?,
                Self::estimated_minimum_compacted_ipc_data_size(batch)?,
            ))
        })();
        let planning_released = self.pusher.release_partition_data_reservation();
        let (original_size, compaction_scratch, minimum_size) = estimates?;
        planning_released?;
        if minimum_size > self.max_frame_size && batch.num_rows() > 1 {
            return self.push_split_batch(partition_id, batch, metrics);
        }

        // Arrow IPC first materializes uncompressed message bodies even for a compressed frame.
        // Those Vecs, schema/FlatBuffers scratch, compacted arrays, codec workspace, and the output
        // buffer overlap. They must be admitted together, not only when the encoded output grows.
        let ipc_scratch = original_size
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(metadata_scratch))
            .and_then(|bytes| bytes.checked_add(compaction_scratch))
            .and_then(|bytes| bytes.checked_add(codec_workspace))
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Remote shuffle encoding workspace exceeds the native integer limit"
                        .to_string(),
                )
            })?;
        let admitted_frame_limit =
            (reservation_limit.saturating_sub(ipc_scratch) / 3).min(self.max_frame_size);
        if admitted_frame_limit < 20 {
            if batch.num_rows() > 1 {
                return self.push_split_batch(partition_id, batch, metrics);
            }
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle frame for a single row and encoding workspace exceed the byte admission budget of {reservation_limit} bytes"
            )));
        }

        // This controls only the first output capacity; metadata scratch is already charged above.
        // A failed estimate drops every temporary allocation and releases its reservation before
        // retrying, and no output buffer is allowed to grow beyond the admitted capacity.
        const IPC_METADATA_RESERVATION_BYTES: usize = 4 * 1024;
        let mut frame_bound = original_size
            .saturating_add(IPC_METADATA_RESERVATION_BYTES)
            .min(admitted_frame_limit);

        loop {
            // Native output capacity, its JNI byte array, and Celeborn's copied transport request
            // overlap. Acquire all three plus the encoding workspace atomically, before either
            // compaction or encoding, without ever clamping a calculated memory bound.
            let reservation = frame_bound
                .checked_mul(3)
                .and_then(|bytes| bytes.checked_add(ipc_scratch))
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Remote shuffle frame-copy reservation exceeds the native integer limit"
                            .to_string(),
                    )
                })?;
            self.pusher.reserve_partition_data(reservation)?;

            let compacted = match Self::compact_dictionary_columns(batch) {
                Ok(compacted) => compacted,
                Err(error) => {
                    self.pusher.release_partition_data_reservation()?;
                    return Err(error);
                }
            };
            let compacted_batch = compacted.as_ref().unwrap_or(batch);
            let encoded_estimate = match Self::estimated_ipc_data_size(compacted_batch) {
                Ok(estimate) => estimate,
                Err(error) => {
                    drop(compacted);
                    self.pusher.release_partition_data_reservation()?;
                    return Err(error);
                }
            };
            if encoded_estimate > self.max_frame_size && compacted_batch.num_rows() > 1 {
                drop(compacted);
                self.pusher.release_partition_data_reservation()?;
                return self.push_split_batch(partition_id, batch, metrics);
            }

            // Bound encoding by the admitted estimate, not only the configured maximum, so all
            // three eventual frame copies fit the reservation that was acquired atomically.
            let mut output = match BoundedBuffer::try_new(frame_bound) {
                Ok(output) => output,
                Err(error) => {
                    drop(compacted);
                    self.pusher.release_partition_data_reservation()?;
                    return Err(error);
                }
            };
            if let Err(error) = self.block_writer.write_rss_batch(
                compacted_batch,
                &mut output,
                &mut self.codec_context,
                &metrics.encode_time,
            ) {
                let exceeded = output.exceeded;
                drop(output);
                drop(compacted);
                self.pusher.release_partition_data_reservation()?;
                if !exceeded {
                    return Err(error);
                }
                if frame_bound < admitted_frame_limit {
                    frame_bound = frame_bound.saturating_mul(2).min(admitted_frame_limit);
                    continue;
                }
                if batch.num_rows() <= 1 {
                    return Err(DataFusionError::Execution(format!(
                        "Remote shuffle frame exceeds its configured maximum: a single row exceeds {} bytes",
                        admitted_frame_limit
                    )));
                }
                return self.push_split_batch(partition_id, batch, metrics);
            }

            // The Java callback must keep this reservation until both transport ownership and
            // this native encoder's explicit acknowledgement have ended.
            drop(compacted);
            let frame = output.inner.get_ref();
            if frame.is_empty() {
                drop(output);
                self.pusher.release_partition_data_reservation()?;
                return Ok(());
            }
            let mut timer = metrics.write_time.timer();
            let result = self.pusher.push_partition_data(partition_id, frame);
            timer.stop();
            // The JNI bridge has popped its local frame on return. Drop the native capacity
            // before acknowledging completion even when the callback failed or completed inline.
            drop(output);
            let released = self.pusher.release_partition_data_reservation();
            return result.and(released);
        }
    }

    fn push_split_batch(
        &mut self,
        partition_id: i32,
        batch: &RecordBatch,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()> {
        let midpoint = batch.num_rows() / 2;
        self.push_batch_within_limit(partition_id, &batch.slice(0, midpoint), metrics)?;
        self.push_batch_within_limit(
            partition_id,
            &batch.slice(midpoint, batch.num_rows() - midpoint),
            metrics,
        )
    }

    /// Bound the pinned Arrow IPC writer's schema, message, and array-descriptor allocations.
    ///
    /// Field/type/dictionary tables and metadata entries each fit in the 256-byte allowance;
    /// record/dictionary messages add 16-byte field nodes and 16-byte buffer descriptors. Names,
    /// metadata keys/values, and timestamp timezones are charged separately. Four copies cover
    /// FlatBuffer/Vec geometric growth, realloc overlap, and the finished-message copy.
    ///
    /// ArrayData (136 bytes) and Buffer (24 bytes) descriptors are cloned while nested list/map
    /// children are sliced for IPC. The depth-weighted allowance covers these simultaneously
    /// retained trees, record-batch column vectors, and descriptor-Vec growth. This is independent
    /// of the actual IPC body bytes and dictionary compaction scratch charged by the caller.
    fn estimated_ipc_metadata_scratch(batch: &RecordBatch) -> (usize, usize) {
        #[derive(Default)]
        struct Metadata {
            bytes: usize,
            entries: usize,
            nodes: usize,
            dictionaries: usize,
            depth: usize,
        }

        fn add_field(field: &Field, depth: usize, metadata: &mut Metadata) {
            metadata.bytes = metadata.bytes.saturating_add(field.name().len());
            metadata.entries = metadata.entries.saturating_add(field.metadata().len());
            for (key, value) in field.metadata() {
                metadata.bytes = metadata
                    .bytes
                    .saturating_add(key.len())
                    .saturating_add(value.len());
            }
            add_type(field.data_type(), depth, metadata);
        }

        fn add_type(data_type: &DataType, depth: usize, metadata: &mut Metadata) {
            metadata.nodes = metadata.nodes.saturating_add(1);
            metadata.depth = metadata.depth.max(depth);
            match data_type {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::ListView(field)
                | DataType::LargeListView(field)
                | DataType::FixedSizeList(field, _)
                | DataType::Map(field, _) => add_field(field, depth + 1, metadata),
                DataType::Struct(fields) => {
                    for field in fields {
                        add_field(field, depth + 1, metadata);
                    }
                }
                DataType::Union(fields, _) => {
                    for (_, field) in fields.iter() {
                        add_field(field, depth + 1, metadata);
                    }
                }
                DataType::RunEndEncoded(run_ends, values) => {
                    add_field(run_ends, depth + 1, metadata);
                    add_field(values, depth + 1, metadata);
                }
                DataType::Dictionary(_, values) => {
                    metadata.dictionaries = metadata.dictionaries.saturating_add(1);
                    add_type(values, depth + 1, metadata);
                }
                DataType::Timestamp(_, Some(timezone)) => {
                    metadata.bytes = metadata.bytes.saturating_add(timezone.len());
                }
                _ => {}
            }
        }

        // All ordinary Arrow layouts have at most three buffers including validity. View arrays
        // add a variable number of backing buffers. Count those by borrowing concrete arrays,
        // without ArrayData conversion or child slicing before the planning reservation exists.
        fn view_backing_buffers(array: &dyn Array) -> usize {
            if let Some(dictionary) = array.as_any_dictionary_opt() {
                return view_backing_buffers(dictionary.values().as_ref());
            }
            match array.data_type() {
                DataType::Utf8View => array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .data_buffers()
                    .len(),
                DataType::BinaryView => array
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .unwrap()
                    .data_buffers()
                    .len(),
                DataType::List(_) => view_backing_buffers(
                    array
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .values()
                        .as_ref(),
                ),
                DataType::LargeList(_) => view_backing_buffers(
                    array
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .unwrap()
                        .values()
                        .as_ref(),
                ),
                DataType::ListView(_) => view_backing_buffers(
                    array
                        .as_any()
                        .downcast_ref::<ListViewArray>()
                        .unwrap()
                        .values()
                        .as_ref(),
                ),
                DataType::LargeListView(_) => view_backing_buffers(
                    array
                        .as_any()
                        .downcast_ref::<LargeListViewArray>()
                        .unwrap()
                        .values()
                        .as_ref(),
                ),
                DataType::FixedSizeList(_, _) => view_backing_buffers(
                    array
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .unwrap()
                        .values()
                        .as_ref(),
                ),
                DataType::Map(_, _) => view_backing_buffers(
                    array.as_any().downcast_ref::<MapArray>().unwrap().entries(),
                ),
                DataType::Struct(_) => array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .columns()
                    .iter()
                    .fold(0usize, |total, column| {
                        total.saturating_add(view_backing_buffers(column.as_ref()))
                    }),
                DataType::Union(fields, _) => {
                    let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
                    fields.iter().fold(0usize, |total, (type_id, _)| {
                        total.saturating_add(view_backing_buffers(union.child(type_id).as_ref()))
                    })
                }
                DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
                    DataType::Int16 => view_backing_buffers(
                        array
                            .as_any()
                            .downcast_ref::<RunArray<Int16Type>>()
                            .unwrap()
                            .values()
                            .as_ref(),
                    ),
                    DataType::Int32 => view_backing_buffers(
                        array
                            .as_any()
                            .downcast_ref::<RunArray<Int32Type>>()
                            .unwrap()
                            .values()
                            .as_ref(),
                    ),
                    DataType::Int64 => view_backing_buffers(
                        array
                            .as_any()
                            .downcast_ref::<RunArray<Int64Type>>()
                            .unwrap()
                            .values()
                            .as_ref(),
                    ),
                    _ => unreachable!("Arrow run ends must use a supported signed integer"),
                },
                _ => 0,
            }
        }

        let schema = batch.schema();
        let mut metadata = Metadata {
            entries: schema.metadata().len(),
            ..Metadata::default()
        };
        for (key, value) in schema.metadata() {
            metadata.bytes = metadata
                .bytes
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        for field in schema.fields() {
            add_field(field, 1, &mut metadata);
        }
        let buffers = batch
            .columns()
            .iter()
            .fold(metadata.nodes.saturating_mul(3), |total, column| {
                total.saturating_add(view_backing_buffers(column.as_ref()))
            });
        let messages = metadata
            .nodes
            .saturating_add(metadata.dictionaries)
            .saturating_add(metadata.entries)
            .saturating_add(1);
        let serialized = 1_024usize
            .saturating_add(metadata.bytes)
            .saturating_add(messages.saturating_mul(256))
            .saturating_add(buffers.saturating_mul(32));
        let descriptors = metadata
            .nodes
            .saturating_mul(512)
            .saturating_add(buffers.saturating_mul(128))
            .saturating_mul(metadata.depth.saturating_add(1));
        let planning = descriptors.saturating_add(4 * 1024);
        (
            serialized.saturating_mul(4).saturating_add(planning),
            planning,
        )
    }

    fn estimated_ipc_data_size(batch: &RecordBatch) -> Result<usize> {
        batch.columns().iter().try_fold(0usize, |total, column| {
            let data = column.to_data();
            let logical = data.get_slice_memory_size()?;
            let additional = Self::additional_ipc_data_size(&data);
            // IPC aligns every buffer to 64 bytes and may synthesize a validity buffer even
            // when an array has no nulls. Include that padding before charging one frame.
            let padding = Self::ipc_buffer_count(&data).saturating_mul(64);
            Ok(total
                .saturating_add(logical)
                .saturating_add(additional)
                .saturating_add(padding))
        })
    }

    /// Estimate logical rows without charging unrelated list/map backing entries.
    ///
    /// `ArrayData::get_slice_memory_size` recurses through an entire list/map child even when the
    /// parent offsets select only one entry. Their zero-copy child slices are sufficient here;
    /// dictionary values remain fully charged because dense garbage collection traverses them.
    fn estimated_pre_compaction_ipc_data_size(batch: &RecordBatch) -> Result<usize> {
        batch.columns().iter().try_fold(0usize, |total, column| {
            Ok(
                total.saturating_add(Self::estimated_live_array_ipc_data_size(
                    column.as_ref(),
                    true,
                )?),
            )
        })
    }

    /// Lower-bound the IPC buffers that must survive any dictionary-value compaction.
    fn estimated_minimum_compacted_ipc_data_size(batch: &RecordBatch) -> Result<usize> {
        batch.columns().iter().try_fold(0usize, |total, column| {
            Ok(
                total.saturating_add(Self::estimated_live_array_ipc_data_size(
                    column.as_ref(),
                    false,
                )?),
            )
        })
    }

    fn estimated_live_array_ipc_data_size(
        array: &dyn Array,
        include_dictionary_values: bool,
    ) -> Result<usize> {
        let data = array.to_data();
        let child_logical = data.child_data().iter().try_fold(0usize, |total, child| {
            Ok::<_, DataFusionError>(total.saturating_add(child.get_slice_memory_size()?))
        })?;
        let logical = data.get_slice_memory_size()?.saturating_sub(child_logical);
        let child_additional = data.child_data().iter().fold(0usize, |total, child| {
            total.saturating_add(Self::additional_ipc_data_size(child))
        });
        let additional = Self::additional_ipc_data_size(&data).saturating_sub(child_additional);
        let padding = data.buffers().len().saturating_add(1).saturating_mul(64);
        let own = logical.saturating_add(additional).saturating_add(padding);

        let children =
            if let Some(dictionary) = array.as_any_dictionary_opt() {
                if include_dictionary_values {
                    Self::estimated_live_array_ipc_data_size(
                        dictionary.values().as_ref(),
                        include_dictionary_values,
                    )?
                } else {
                    0
                }
            } else {
                match array.data_type() {
                    DataType::List(_) => {
                        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                        let offsets = list.value_offsets();
                        let start = offsets[0] as usize;
                        let end = offsets[offsets.len() - 1] as usize;
                        let live_values = list.values().slice(start, end - start);
                        Self::estimated_live_array_ipc_data_size(
                            live_values.as_ref(),
                            include_dictionary_values,
                        )?
                    }
                    DataType::LargeList(_) => {
                        let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                        let offsets = list.value_offsets();
                        let start = offsets[0] as usize;
                        let end = offsets[offsets.len() - 1] as usize;
                        let live_values = list.values().slice(start, end - start);
                        Self::estimated_live_array_ipc_data_size(
                            live_values.as_ref(),
                            include_dictionary_values,
                        )?
                    }
                    DataType::FixedSizeList(_, _) => {
                        let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                        Self::estimated_live_array_ipc_data_size(
                            list.values().as_ref(),
                            include_dictionary_values,
                        )?
                    }
                    DataType::Struct(_) => {
                        let structure = array.as_any().downcast_ref::<StructArray>().unwrap();
                        structure
                            .columns()
                            .iter()
                            .try_fold(0usize, |total, column| {
                                Ok::<_, DataFusionError>(total.saturating_add(
                                    Self::estimated_live_array_ipc_data_size(
                                        column.as_ref(),
                                        include_dictionary_values,
                                    )?,
                                ))
                            })?
                    }
                    DataType::Map(_, _) => {
                        let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                        let offsets = map.value_offsets();
                        let start = offsets[0] as usize;
                        let end = offsets[offsets.len() - 1] as usize;
                        let live_entries = map.entries().slice(start, end - start);
                        Self::estimated_live_array_ipc_data_size(
                            &live_entries,
                            include_dictionary_values,
                        )?
                    }
                    _ => data.child_data().iter().try_fold(0usize, |total, child| {
                        let logical = child.get_slice_memory_size()?;
                        let additional = Self::additional_ipc_data_size(child);
                        let padding = Self::ipc_buffer_count(child).saturating_mul(64);
                        Ok::<_, DataFusionError>(total.saturating_add(
                            logical.saturating_add(additional).saturating_add(padding),
                        ))
                    })?,
                }
            };
        Ok(own.saturating_add(children))
    }

    fn additional_ipc_data_size(data: &arrow::array::ArrayData) -> usize {
        // Arrow IPC writes a validity bitmap even when the original array has no null buffer.
        let validity = if data.nulls().is_none() {
            data.len().div_ceil(8)
        } else {
            0
        };
        // ArrayData's slice estimate charges one offset per row, while IPC writes the additional
        // terminating offset. View arrays instead serialize every shared backing data buffer.
        let own = if matches!(data.data_type(), DataType::BinaryView | DataType::Utf8View) {
            data.buffers()
                .iter()
                .skip(1)
                .fold(0usize, |total, buffer| total.saturating_add(buffer.len()))
        } else {
            match data.data_type() {
                DataType::Binary | DataType::Utf8 => {
                    let temporary = if data.buffer::<i32>(0)[0] != 0 {
                        data.len().saturating_add(1).saturating_mul(4)
                    } else {
                        0
                    };
                    4usize.saturating_add(temporary)
                }
                DataType::LargeBinary | DataType::LargeUtf8 => {
                    let temporary = if data.buffer::<i64>(0)[0] != 0 {
                        data.len().saturating_add(1).saturating_mul(8)
                    } else {
                        0
                    };
                    8usize.saturating_add(temporary)
                }
                DataType::List(_) | DataType::Map(_, _) => 4,
                DataType::LargeList(_) => 8,
                _ => 0,
            }
        };
        data.child_data()
            .iter()
            .fold(own.saturating_add(validity), |total, child| {
                total.saturating_add(Self::additional_ipc_data_size(child))
            })
    }

    fn ipc_buffer_count(data: &arrow::array::ArrayData) -> usize {
        data.child_data()
            .iter()
            .fold(data.buffers().len().saturating_add(1), |total, child| {
                total.saturating_add(Self::ipc_buffer_count(child))
            })
    }

    /// Conservatively charge dense dictionary garbage collection and nested offset rebasing.
    ///
    /// This walks the original arrays without compacting, building occupancy masks, or allocating
    /// per-row scratch. In particular, dense remaps scale with the *full* dictionary cardinality,
    /// even when only one value is referenced by the current batch slice.
    fn estimated_compaction_scratch(batch: &RecordBatch) -> Result<usize> {
        batch.columns().iter().try_fold(0usize, |total, column| {
            Ok(total.saturating_add(Self::estimated_array_compaction_scratch(column.as_ref())?))
        })
    }

    fn estimated_array_compaction_scratch(array: &dyn Array) -> Result<usize> {
        if let Some(dictionary) = array.as_any_dictionary_opt() {
            let key_width = match dictionary.keys().data_type() {
                DataType::Int8 | DataType::UInt8 => 1,
                DataType::Int16 | DataType::UInt16 => 2,
                DataType::Int32 | DataType::UInt32 => 4,
                DataType::Int64 | DataType::UInt64 => 8,
                _ => unreachable!("Arrow dictionary keys must have an integer type"),
            };
            let value_count = dictionary.values().len();
            let key_count = dictionary.keys().len();
            let occupancy = value_count.div_ceil(8).saturating_add(64);
            let dense_remap = value_count.saturating_mul(key_width);
            let copied_keys = key_count
                .saturating_mul(key_width)
                .saturating_add(key_count.div_ceil(8))
                .saturating_add(64);
            let filter_indices = key_count
                .min(value_count)
                .saturating_mul(std::mem::size_of::<(usize, usize)>());
            let values = dictionary.values().to_data();
            let copied_values = values
                .get_slice_memory_size()?
                .saturating_add(Self::additional_ipc_data_size(&values))
                .saturating_add(Self::ipc_buffer_count(&values).saturating_mul(64));
            let nested = Self::estimated_array_compaction_scratch(dictionary.values().as_ref())?;
            return Ok(occupancy
                .saturating_add(dense_remap)
                .saturating_add(copied_keys)
                .saturating_add(filter_indices)
                .saturating_add(copied_values)
                .saturating_add(nested));
        }

        match array.data_type() {
            DataType::List(_) => {
                let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                let offsets = list.value_offsets();
                let start = offsets[0] as usize;
                let end = offsets[offsets.len() - 1] as usize;
                let normalized = start != 0 || end != list.values().len();
                let rebased = if normalized {
                    list.len().saturating_add(1).saturating_mul(4)
                } else {
                    0
                };
                let live_values = list.values().slice(start, end - start);
                Ok(
                    rebased.saturating_add(Self::estimated_array_compaction_scratch(
                        live_values.as_ref(),
                    )?),
                )
            }
            DataType::LargeList(_) => {
                let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                let offsets = list.value_offsets();
                let start = offsets[0] as usize;
                let end = offsets[offsets.len() - 1] as usize;
                let normalized = start != 0 || end != list.values().len();
                let rebased = if normalized {
                    list.len().saturating_add(1).saturating_mul(8)
                } else {
                    0
                };
                let live_values = list.values().slice(start, end - start);
                Ok(
                    rebased.saturating_add(Self::estimated_array_compaction_scratch(
                        live_values.as_ref(),
                    )?),
                )
            }
            DataType::FixedSizeList(_, _) => {
                let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                Self::estimated_array_compaction_scratch(list.values().as_ref())
            }
            DataType::Struct(_) => {
                let structure = array.as_any().downcast_ref::<StructArray>().unwrap();
                structure
                    .columns()
                    .iter()
                    .try_fold(0usize, |total, column| {
                        Ok(
                            total.saturating_add(Self::estimated_array_compaction_scratch(
                                column.as_ref(),
                            )?),
                        )
                    })
            }
            DataType::Map(_, _) => {
                let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                let offsets = map.value_offsets();
                let start = offsets[0] as usize;
                let end = offsets[offsets.len() - 1] as usize;
                let normalized = start != 0 || end != map.entries().len();
                let rebased = if normalized {
                    map.len().saturating_add(1).saturating_mul(4)
                } else {
                    0
                };
                let live_entries = map.entries().slice(start, end - start);
                Ok(
                    rebased
                        .saturating_add(Self::estimated_array_compaction_scratch(&live_entries)?),
                )
            }
            _ => Ok(0),
        }
    }

    fn compact_dictionary_columns(batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let mut changed = false;
        let columns = batch
            .columns()
            .iter()
            .map(|column| -> Result<ArrayRef> {
                let (compacted, column_changed) = Self::compact_array(column)?;
                changed |= column_changed;
                Ok(compacted)
            })
            .collect::<Result<Vec<_>>>()?;

        if changed {
            Ok(Some(RecordBatch::try_new(batch.schema(), columns)?))
        } else {
            Ok(None)
        }
    }

    fn compact_array(array: &ArrayRef) -> Result<(ArrayRef, bool)> {
        if let Some(dictionary) = array.as_any_dictionary_opt() {
            let compacted = garbage_collect_any_dictionary(dictionary)?;
            let compacted_dictionary = compacted.as_any_dictionary();
            let dictionary_changed =
                compacted_dictionary.values().len() != dictionary.values().len();
            let (values, values_changed) = Self::compact_array(compacted_dictionary.values())?;
            if values_changed {
                return Ok((compacted_dictionary.with_values(values), true));
            }
            return Ok(if dictionary_changed {
                (compacted, true)
            } else {
                (Arc::clone(array), false)
            });
        }

        match array.data_type() {
            DataType::List(field) => {
                let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                let offsets = list.value_offsets();
                let start = offsets[0] as usize;
                let end = offsets[offsets.len() - 1] as usize;
                let live_values = list.values().slice(start, end - start);
                let (values, child_changed) = Self::compact_array(&live_values)?;
                let normalized = start != 0 || end != list.values().len();
                if !child_changed && !normalized {
                    return Ok((Arc::clone(array), false));
                }
                let rebased = if start == 0 {
                    list.offsets().clone()
                } else {
                    OffsetBuffer::new(
                        offsets
                            .iter()
                            .map(|offset| offset - offsets[0])
                            .collect::<Vec<_>>()
                            .into(),
                    )
                };
                let rebuilt =
                    ListArray::try_new(Arc::clone(field), rebased, values, list.nulls().cloned())?;
                Ok((Arc::new(rebuilt), true))
            }
            DataType::LargeList(field) => {
                let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                let offsets = list.value_offsets();
                let start = offsets[0] as usize;
                let end = offsets[offsets.len() - 1] as usize;
                let live_values = list.values().slice(start, end - start);
                let (values, child_changed) = Self::compact_array(&live_values)?;
                let normalized = start != 0 || end != list.values().len();
                if !child_changed && !normalized {
                    return Ok((Arc::clone(array), false));
                }
                let rebased = if start == 0 {
                    list.offsets().clone()
                } else {
                    OffsetBuffer::new(
                        offsets
                            .iter()
                            .map(|offset| offset - offsets[0])
                            .collect::<Vec<_>>()
                            .into(),
                    )
                };
                let rebuilt = LargeListArray::try_new(
                    Arc::clone(field),
                    rebased,
                    values,
                    list.nulls().cloned(),
                )?;
                Ok((Arc::new(rebuilt), true))
            }
            DataType::FixedSizeList(field, size) => {
                let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                let (values, changed) = Self::compact_array(list.values())?;
                if !changed {
                    return Ok((Arc::clone(array), false));
                }
                let rebuilt = FixedSizeListArray::try_new_with_length(
                    Arc::clone(field),
                    *size,
                    values,
                    list.nulls().cloned(),
                    list.len(),
                )?;
                Ok((Arc::new(rebuilt), true))
            }
            DataType::Struct(fields) => {
                let structure = array.as_any().downcast_ref::<StructArray>().unwrap();
                let mut changed = false;
                let columns = structure
                    .columns()
                    .iter()
                    .map(|column| {
                        let (compacted, child_changed) = Self::compact_array(column)?;
                        changed |= child_changed;
                        Ok(compacted)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !changed {
                    return Ok((Arc::clone(array), false));
                }
                let rebuilt = StructArray::try_new_with_length(
                    fields.clone(),
                    columns,
                    structure.nulls().cloned(),
                    structure.len(),
                )?;
                Ok((Arc::new(rebuilt), true))
            }
            DataType::Map(field, ordered) => {
                let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                let offsets = map.value_offsets();
                let start = offsets[0] as usize;
                let end = offsets[offsets.len() - 1] as usize;
                let live_entries: ArrayRef = Arc::new(map.entries().slice(start, end - start));
                let (entries, child_changed) = Self::compact_array(&live_entries)?;
                let normalized = start != 0 || end != map.entries().len();
                if !child_changed && !normalized {
                    return Ok((Arc::clone(array), false));
                }
                let rebased = if start == 0 {
                    map.offsets().clone()
                } else {
                    OffsetBuffer::new(
                        offsets
                            .iter()
                            .map(|offset| offset - offsets[0])
                            .collect::<Vec<_>>()
                            .into(),
                    )
                };
                let entries = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone();
                let rebuilt = MapArray::try_new(
                    Arc::clone(field),
                    rebased,
                    entries,
                    map.nulls().cloned(),
                    *ordered,
                )?;
                Ok((Arc::new(rebuilt), true))
            }
            _ => Ok((Arc::clone(array), false)),
        }
    }
}

impl PartitionWriter for RssPartitionWriter {
    fn write<I>(
        &mut self,
        partition_id: usize,
        batches: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let jvm_partition_id = self.validate_writable_partition(partition_id)?;
        self.push_batches(jvm_partition_id, batches, metrics)
    }

    fn finish_partition<I>(
        &mut self,
        partition_id: usize,
        batches: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let jvm_partition_id = self.validate_writable_partition(partition_id)?;
        if partition_id != self.next_partition_to_finish {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle partitions must be finalized in order: expected {}, got {partition_id}",
                self.next_partition_to_finish
            )));
        }

        self.push_batches(jvm_partition_id, batches, metrics)?;
        self.next_partition_to_finish += 1;
        Ok(())
    }

    fn finish_all(&mut self, _metrics: &ShufflePartitionerMetrics) -> Result<()> {
        if self.finished || self.failed {
            return Err(DataFusionError::Execution(
                "Remote shuffle writer has already finished".to_string(),
            ));
        }

        if self.next_partition_to_finish != self.num_partitions {
            return Err(DataFusionError::Execution(format!(
                "Remote shuffle writer cannot finish before all partitions are finalized: \
                 finalized {} of {}",
                self.next_partition_to_finish, self.num_partitions
            )));
        }

        self.finished = true;
        Ok(())
    }
}

/// A seekable encoder destination that rejects an oversized frame before growing its buffer.
struct BoundedBuffer {
    inner: Cursor<Vec<u8>>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBuffer {
    fn try_new(limit: usize) -> Result<Self> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(limit).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!(
                "Cannot allocate the admitted remote shuffle frame buffer: {error}"
            ))
        })?;
        // Cursor<Vec<u8>> normally doubles capacity when its length grows. Preallocate the full
        // admitted capacity so every permitted write is allocation-free instead.
        if bytes.capacity() != limit {
            return Err(DataFusionError::ResourcesExhausted(
                "Remote shuffle allocator exceeded the admitted frame capacity".to_string(),
            ));
        }
        Ok(Self {
            inner: Cursor::new(bytes),
            limit,
            exceeded: false,
        })
    }

    fn limit_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "RSS frame exceeds its byte limit",
        )
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self.inner.position().checked_add(bytes.len() as u64);
        if end.is_none_or(|end| end > self.limit as u64) {
            self.exceeded = true;
            return Err(Self::limit_error());
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for BoundedBuffer {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let previous = self.inner.position();
        let next = self.inner.seek(position)?;
        if next > self.limit as u64 {
            self.inner.set_position(previous);
            self.exceeded = true;
            return Err(Self::limit_error());
        }
        Ok(next)
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Schema;
    use datafusion::physical_plan::metrics::Time;

    #[test]
    fn large_uncompressed_frame_never_grows_past_its_admitted_capacity() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from_iter_values(0..16_384))],
        )
        .unwrap();
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), crate::CompressionCodec::None)
                .unwrap();
        let mut output = BoundedBuffer::try_new(67_996).unwrap();
        block_writer
            .write_rss_batch(
                &batch,
                &mut output,
                &mut ShuffleCodecContext::default(),
                &Time::default(),
            )
            .unwrap();
        assert_eq!(output.inner.get_ref().len(), 67_996);
        assert_eq!(output.inner.get_ref().capacity(), 67_996);
        assert!(output.write(&[1]).is_err());
        assert!(output.exceeded);
        assert_eq!(output.inner.get_ref().capacity(), 67_996);
    }
}
