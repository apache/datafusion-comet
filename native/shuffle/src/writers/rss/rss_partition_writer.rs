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

use std::io::{self, Cursor, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_jni_bridge::shuffle_partition_pusher::JavaShufflePartitionPusher;

use crate::metrics::ShufflePartitionerMetrics;
use crate::writers::PartitionWriter;
use crate::ShuffleBlockWriter;

/// Backend-neutral, all-or-error transport for one complete Comet frame.
///
/// Acceptance is not a remote map commit. Implementations own asynchronous admission, retry,
/// cancellation, and commit; they must not split a frame into independently interleavable pushes.
pub trait PartitionPusher: Send + Sync {
    /// Reserve executor-wide capacity for encoding scratch and every overlapping frame copy.
    fn reserve_partition_data(&self, _reservation_bytes: usize) -> Result<()> {
        Ok(())
    }

    /// Return an unsubmitted encoding reservation.
    fn release_partition_data_reservation(&self) -> Result<()> {
        Ok(())
    }

    fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()>;
}

impl<P: PartitionPusher + ?Sized> PartitionPusher for Arc<P> {
    fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
        self.as_ref().reserve_partition_data(reservation_bytes)
    }

    fn release_partition_data_reservation(&self) -> Result<()> {
        self.as_ref().release_partition_data_reservation()
    }

    fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()> {
        self.as_ref().push_partition_data(partition_id, frame)
    }
}

impl PartitionPusher for JavaShufflePartitionPusher {
    fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
        JavaShufflePartitionPusher::reserve_partition_data(self, reservation_bytes)
            .map_err(Into::into)
    }

    fn release_partition_data_reservation(&self) -> Result<()> {
        JavaShufflePartitionPusher::release_partition_data_reservation(self).map_err(Into::into)
    }

    fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()> {
        JavaShufflePartitionPusher::push_partition_data(self, partition_id, frame)
            .map_err(Into::into)
    }
}

/// Encodes already-partitioned batches using the existing Comet format and sends one frame per
/// callback. The native planner selects it only for a registered task-owned pusher.
///
/// There are no retained per-reducer buffers. Oversized batches are split at row boundaries into
/// independently decodable frames before Arrow can allocate an oversized IPC body. A single row
/// with an oversized uncompressed body may still fit after compression; its complete scratch
/// budget is reserved before encoding, and an oversized encoded frame remains an error. Every
/// reservation also covers the simultaneously live native frame, JNI array, and backend request
/// payload. The backend owns asynchronous transport admission.
pub struct RssPartitionWriter<P: PartitionPusher> {
    pusher: P,
    block_writer: ShuffleBlockWriter,
    num_partitions: usize,
    max_frame_bytes: usize,
    next_partition: usize,
    finished: bool,
    failed: bool,
}

impl<P: PartitionPusher> RssPartitionWriter<P> {
    pub fn try_new(
        pusher: P,
        block_writer: ShuffleBlockWriter,
        num_partitions: usize,
        max_frame_bytes: usize,
    ) -> Result<Self> {
        Self::validate_options(num_partitions, max_frame_bytes)?;
        Ok(Self {
            pusher,
            block_writer,
            num_partitions,
            max_frame_bytes,
            next_partition: 0,
            finished: false,
            failed: false,
        })
    }

    pub(crate) fn validate_options(num_partitions: usize, max_frame_bytes: usize) -> Result<()> {
        if num_partitions == 0 || num_partitions > i32::MAX as usize {
            return Err(DataFusionError::Configuration(
                "Invalid RSS partition count".into(),
            ));
        }
        if !(20..=i32::MAX as usize).contains(&max_frame_bytes) {
            return Err(DataFusionError::Configuration(
                "Invalid RSS frame byte limit".into(),
            ));
        }
        Ok(())
    }

    /// Encodes and synchronously submits one batch. A failed writer cannot be reused.
    pub fn write_batch(
        &mut self,
        partition_id: usize,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> Result<()> {
        let result = (|| {
            self.check_partition(partition_id)?;
            self.write_batch_within_limit(partition_id, batch, encode_time, write_time)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn write_batch_within_limit(
        &mut self,
        partition_id: usize,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Arrow materializes full dictionary remaps, copied values, and the IPC body before
        // writing anything to our bounded destination. Estimate those allocations without
        // compacting first; ordinary oversized batches can still be split before admission.
        let original_ipc_data_size = Self::estimated_pre_compaction_ipc_data_size(batch)?;
        let compaction_scratch = Self::estimated_compaction_scratch(batch)?;
        // Dictionary compaction can shrink values but cannot remove keys, list/map offsets, or
        // other ordinary columns. Split before admission when those surviving buffers alone make
        // one frame impossible, even if a different or nested column contains a dictionary.
        let minimum_compacted_ipc_data_size =
            Self::estimated_minimum_compacted_ipc_data_size(batch)?;
        if minimum_compacted_ipc_data_size > self.max_frame_bytes && batch.num_rows() > 1 {
            return self.write_split_batch(partition_id, batch, encode_time, write_time);
        }

        // The encoded native frame remains live while JNI copies it into a Java array and the
        // backend copies that array into its asynchronous request. Acquire all three copies
        // together before encoding: growing admission after any allocation can deadlock other
        // concurrently admitted encoders. The JVM accounts for its additional request header.
        let overlapping_frame_bytes = self.max_frame_bytes.checked_mul(3).ok_or_else(|| {
            DataFusionError::Configuration("RSS frame copy reservation exceeds usize".into())
        })?;
        let reservation_bytes =
            overlapping_frame_bytes.max(original_ipc_data_size.saturating_add(compaction_scratch));
        self.pusher.reserve_partition_data(reservation_bytes)?;

        let compacted = match Self::compact_dictionary_columns(batch) {
            Ok(compacted) => compacted,
            Err(error) => {
                self.pusher.release_partition_data_reservation()?;
                return Err(error);
            }
        };
        let compacted_batch = compacted.as_ref().unwrap_or(batch);
        let ipc_data_size = match Self::estimated_ipc_data_size(compacted_batch) {
            Ok(ipc_data_size) => ipc_data_size,
            Err(error) => {
                drop(compacted);
                self.pusher.release_partition_data_reservation()?;
                return Err(error);
            }
        };
        if ipc_data_size > self.max_frame_bytes && compacted_batch.num_rows() > 1 {
            // Do not retain the compacted parent or its thread-owned reservation while children
            // compete for executor admission. Re-compact each original child after it is admitted.
            drop(compacted);
            self.pusher.release_partition_data_reservation()?;
            return self.write_split_batch(partition_id, batch, encode_time, write_time);
        }

        let mut output = BoundedBuffer::new(self.max_frame_bytes);
        if let Err(error) = self.block_writer.write_batch(
            compacted_batch,
            &mut output,
            &mut self.compression_context,
            encode_time,
        ) {
            let exceeded = output.exceeded;
            drop(output);
            drop(compacted);
            self.pusher.release_partition_data_reservation()?;
            if !exceeded {
                return Err(error);
            }
            if batch.num_rows() <= 1 {
                return Err(self.oversized_row());
            }
            return self.write_split_batch(partition_id, batch, encode_time, write_time);
        }

        // The JVM shrinks admission to three actual frame copies when claiming this reservation,
        // then to the retained request only after submission. Release dictionary copies and
        // normalization scratch before the smaller synchronous-copy claim becomes visible.
        drop(compacted);
        let frame = output.inner.get_ref();
        if !frame.is_empty() {
            let _timer = write_time.timer();
            if let Err(error) = self.pusher.push_partition_data(partition_id, frame) {
                drop(output);
                self.pusher.release_partition_data_reservation()?;
                return Err(error);
            }
        } else {
            drop(output);
            self.pusher.release_partition_data_reservation()?;
        }
        Ok(())
    }

    fn write_split_batch(
        &mut self,
        partition_id: usize,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> Result<()> {
        let midpoint = batch.num_rows() / 2;
        self.write_batch_within_limit(
            partition_id,
            &batch.slice(0, midpoint),
            encode_time,
            write_time,
        )?;
        self.write_batch_within_limit(
            partition_id,
            &batch.slice(midpoint, batch.num_rows() - midpoint),
            encode_time,
            write_time,
        )
    }

    fn oversized_row(&self) -> DataFusionError {
        DataFusionError::Execution(format!(
            "RSS frame exceeds its byte limit: a single RSS row exceeds the {}-byte limit",
            self.max_frame_bytes
        ))
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

    fn check_partition(&self, partition_id: usize) -> Result<()> {
        if self.failed || self.finished {
            return Err(DataFusionError::Execution("RSS writer is closed".into()));
        }
        if partition_id >= self.num_partitions || partition_id < self.next_partition {
            return Err(DataFusionError::Execution(format!(
                "RSS partition {partition_id} is invalid or already finalized"
            )));
        }
        Ok(())
    }

    fn write_batches<I>(
        &mut self,
        partition_id: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let result = (|| {
            self.check_partition(partition_id)?;
            for batch in iter {
                self.write_batch(
                    partition_id,
                    &batch?,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
            }
            Ok(())
        })();
        if result.is_err() {
            // Earlier batches may already have been accepted. An input error must not allow
            // this attempt to be resumed or finalized as if its output were complete.
            self.failed = true;
        }
        result
    }
}

impl<P: PartitionPusher> PartitionWriter for RssPartitionWriter<P> {
    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        self.write_batches(pid, iter, metrics)
    }

    fn finish_partition<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        if pid != self.next_partition {
            return Err(DataFusionError::Execution(format!(
                "Expected RSS partition {}, got {pid}",
                self.next_partition
            )));
        }
        self.write_batches(pid, iter, metrics)?;
        self.next_partition += 1;
        Ok(())
    }

    fn finish_all(&mut self, _metrics: &ShufflePartitionerMetrics) -> Result<()> {
        if self.failed || self.finished || self.next_partition != self.num_partitions {
            return Err(DataFusionError::Execution(
                "RSS partitions are not ready for finalization".into(),
            ));
        }
        // Remote map commit belongs to the task-owned JVM adapter, not this encoder.
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
    fn new(limit: usize) -> Self {
        Self {
            inner: Cursor::new(Vec::new()),
            limit,
            exceeded: false,
        }
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
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use arrow::array::{
        BinaryArray, BooleanArray, DictionaryArray, Int32Array, Int64Array, StringArray,
        StringViewArray,
    };
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    use crate::{read_ipc_compressed, CompressionCodec};

    type CapturedFrames = Arc<Mutex<Vec<(usize, Vec<u8>)>>>;

    #[derive(Clone, Default)]
    struct RecordingPusher(CapturedFrames);

    impl PartitionPusher for RecordingPusher {
        fn push_partition_data(&self, pid: usize, frame: &[u8]) -> Result<()> {
            self.0.lock().unwrap().push((pid, frame.to_vec()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordedReservations {
        active: Option<usize>,
        reservations: Vec<usize>,
        releases: usize,
        frames: Vec<Vec<u8>>,
    }

    #[derive(Clone, Default)]
    struct ReservationRecordingPusher(Arc<Mutex<RecordedReservations>>);

    impl PartitionPusher for ReservationRecordingPusher {
        fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
            let mut recorded = self.0.lock().unwrap();
            assert!(recorded.active.is_none());
            recorded.active = Some(reservation_bytes);
            recorded.reservations.push(reservation_bytes);
            Ok(())
        }

        fn release_partition_data_reservation(&self) -> Result<()> {
            let mut recorded = self.0.lock().unwrap();
            assert!(recorded.active.take().is_some());
            recorded.releases += 1;
            Ok(())
        }

        fn push_partition_data(&self, _partition_id: usize, frame: &[u8]) -> Result<()> {
            let mut recorded = self.0.lock().unwrap();
            assert!(recorded.active.take().unwrap() >= 3 * frame.len());
            recorded.frames.push(frame.to_vec());
            Ok(())
        }
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    fn sparse_int32_dictionary_batch(keys: Vec<i32>, value_count: i32) -> RecordBatch {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(keys),
            Arc::new(Int32Array::from_iter_values(0..value_count)),
        )
        .unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
                false,
            )])),
            vec![Arc::new(dictionary)],
        )
        .unwrap()
    }

    fn metrics() -> ShufflePartitionerMetrics {
        ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0)
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call ZSTD_createCCtx.
    fn rss_partition_writer_routes_complete_frames() {
        let first = batch(vec![1, 2]);
        let second = batch(vec![3]);
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Snappy,
            CompressionCodec::Zstd(1),
        ] {
            let pusher = RecordingPusher::default();
            let captured = Arc::clone(&pusher.0);
            let encoder = ShuffleBlockWriter::try_new(first.schema().as_ref(), codec).unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 3, 1024 * 1024).unwrap();
            let metrics = metrics();
            writer
                .write(1, &mut [Ok(second.clone())].into_iter(), &metrics)
                .unwrap();
            writer
                .finish_partition(0, &mut [Ok(first.clone())].into_iter(), &metrics)
                .unwrap();
            writer
                .finish_partition(1, &mut std::iter::empty(), &metrics)
                .unwrap();
            writer
                .finish_partition(2, &mut std::iter::empty(), &metrics)
                .unwrap();
            writer.finish_all(&metrics).unwrap();

            let frames = captured.lock().unwrap();
            assert_eq!(frames.len(), 2);
            for ((pid, frame), (expected_pid, expected)) in
                frames.iter().zip([(1, &second), (0, &first)])
            {
                assert_eq!(*pid, expected_pid);
                let declared = u64::from_le_bytes(frame[..8].try_into().unwrap());
                assert_eq!(declared + 8, frame.len() as u64);
                assert_eq!(u64::from_le_bytes(frame[8..16].try_into().unwrap()), 1);
                assert_eq!(&read_ipc_compressed(&frame[16..]).unwrap(), expected);
            }
        }
    }

    #[test]
    fn rss_partition_writer_reserves_overlapping_frame_copies_before_encoding() {
        let limit = 1_024;
        let input = batch(vec![1, 2]);
        let scratch =
            RssPartitionWriter::<ReservationRecordingPusher>::estimated_ipc_data_size(&input)
                .unwrap();
        assert!(scratch < 3 * limit);

        let pusher = ReservationRecordingPusher::default();
        let recorded = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, limit).unwrap();
        let metrics = metrics();

        writer
            .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
            .unwrap();

        let recorded = recorded.lock().unwrap();
        assert!(recorded.active.is_none());
        assert_eq!(recorded.reservations, vec![3 * limit]);
        assert_eq!(recorded.releases, 0);
        assert_eq!(recorded.frames.len(), 1);
        assert_eq!(
            read_ipc_compressed(&recorded.frames[0][16..]).unwrap(),
            input
        );
    }

    #[test]
    fn rss_partition_writer_rejects_unadmitted_frame_copies_before_encoding() {
        struct RejectingCopiesPusher(Arc<Mutex<Vec<usize>>>);

        impl PartitionPusher for RejectingCopiesPusher {
            fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
                self.0.lock().unwrap().push(reservation_bytes);
                Err(DataFusionError::External(Box::new(io::Error::other(
                    "overlapping frame copies were not admitted",
                ))))
            }

            fn release_partition_data_reservation(&self) -> Result<()> {
                panic!("a denied frame-copy reservation must not be released")
            }

            fn push_partition_data(&self, _partition_id: usize, _frame: &[u8]) -> Result<()> {
                panic!("a denied frame-copy reservation must prevent encoding and push")
            }
        }

        let limit = 1_024;
        let input = batch(vec![1, 2]);
        let reservations = Arc::new(Mutex::new(Vec::new()));
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(
            RejectingCopiesPusher(Arc::clone(&reservations)),
            encoder,
            1,
            limit,
        )
        .unwrap();
        let metrics = metrics();

        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("frame-copy admission failure was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "overlapping frame copies were not admitted"
        );
        assert_eq!(*reservations.lock().unwrap(), vec![3 * limit]);
    }

    #[test]
    fn rss_partition_writer_checks_lifecycle_and_empty_batches() {
        let input = batch(vec![]);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        for (partitions, limit) in [
            (0, 1024),
            (i32::MAX as usize + 1, 1024),
            (2, 19),
            (2, i32::MAX as usize + 1),
        ] {
            assert!(RssPartitionWriter::try_new(
                pusher.clone(),
                encoder.clone(),
                partitions,
                limit,
            )
            .is_err());
        }
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 2, 1024).unwrap();
        let metrics = metrics();
        assert!(writer.finish_all(&metrics).is_err());
        assert!(writer
            .finish_partition(1, &mut std::iter::empty(), &metrics)
            .is_err());
        writer
            .finish_partition(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap();
        assert!(writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .is_err());
        writer
            .finish_partition(1, &mut std::iter::empty(), &metrics)
            .unwrap();
        writer.finish_all(&metrics).unwrap();
        assert!(writer.finish_all(&metrics).is_err());
        assert!(captured.lock().unwrap().is_empty());
    }

    #[test]
    fn rss_partition_writer_rejects_oversized_frames_before_push() {
        let input = batch(vec![1]);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 20).unwrap();
        let metrics = metrics();
        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        assert!(error.to_string().contains("single RSS row exceeds"));
        assert!(captured.lock().unwrap().is_empty());
        assert!(writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .is_err());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call ZSTD_createCCtx.
    fn rss_partition_writer_splits_oversized_batches_at_row_boundaries() {
        let expected: Vec<i64> = (0i64..1024)
            .map(|value| {
                value
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407)
            })
            .collect();
        let input = batch(expected.clone());
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Snappy,
            CompressionCodec::Zstd(1),
        ] {
            let pusher = RecordingPusher::default();
            let captured = Arc::clone(&pusher.0);
            let encoder = ShuffleBlockWriter::try_new(input.schema().as_ref(), codec).unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 1024).unwrap();
            let metrics = metrics();

            writer
                .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
                .unwrap();
            writer.finish_all(&metrics).unwrap();

            let frames = captured.lock().unwrap();
            assert!(frames.len() > 1);
            let mut actual = Vec::new();
            for (partition, frame) in frames.iter() {
                assert_eq!(*partition, 0);
                assert!(frame.len() <= 1024);
                let decoded = read_ipc_compressed(&frame[16..]).unwrap();
                let values = decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                actual.extend(values.iter().map(|value| value.unwrap()));
            }
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn rss_partition_writer_splits_wide_batches_before_allocating_arrow_ipc() {
        let primitive = batch((0..4_096).collect());
        let binary_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let binary = RecordBatch::try_new(
            Arc::clone(&binary_schema),
            vec![Arc::new(BinaryArray::from_iter_values(
                (0..128).map(|index| vec![index as u8; 512]),
            ))],
        )
        .unwrap();

        for input in [primitive, binary] {
            let limit = 2_048;
            assert!(
                RssPartitionWriter::<RecordingPusher>::estimated_ipc_data_size(&input).unwrap()
                    > limit
            );
            let pusher = RecordingPusher::default();
            let captured = Arc::clone(&pusher.0);
            let encoder =
                ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None)
                    .unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, limit).unwrap();
            let metrics = metrics();

            writer
                .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
                .unwrap();

            let frames = captured.lock().unwrap();
            assert!(frames.len() > 1);
            let decoded = frames
                .iter()
                .map(|(_, frame)| {
                    assert!(frame.len() <= limit);
                    read_ipc_compressed(&frame[16..]).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                arrow_select::concat::concat_batches(&input.schema(), &decoded).unwrap(),
                input
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call ZSTD_createCCtx.
    fn rss_partition_writer_reserves_oversized_single_row_before_compressing_it() {
        let limit = 2_048;
        let payload = vec![b'a'; 32 * 1_024];
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from_iter_values(
                [payload.as_slice()],
            ))],
        )
        .unwrap();
        let estimated_ipc_data_size =
            RssPartitionWriter::<ReservationRecordingPusher>::estimated_ipc_data_size(&input)
                .unwrap();
        assert!(estimated_ipc_data_size > 3 * limit);

        for codec in [
            CompressionCodec::Lz4Frame,
            CompressionCodec::Snappy,
            CompressionCodec::Zstd(1),
        ] {
            let pusher = ReservationRecordingPusher::default();
            let recorded = Arc::clone(&pusher.0);
            let encoder = ShuffleBlockWriter::try_new(input.schema().as_ref(), codec).unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, limit).unwrap();
            let metrics = metrics();

            writer
                .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
                .unwrap();
            writer.finish_all(&metrics).unwrap();

            let recorded = recorded.lock().unwrap();
            assert!(recorded.active.is_none());
            assert_eq!(recorded.reservations, vec![estimated_ipc_data_size]);
            assert_eq!(recorded.releases, 0);
            assert_eq!(recorded.frames.len(), 1);
            assert!(recorded.frames[0].len() <= limit);
            assert_eq!(
                read_ipc_compressed(&recorded.frames[0][16..]).unwrap(),
                input
            );
        }
    }

    #[test]
    fn rss_partition_writer_rejects_single_row_before_encoding_when_scratch_is_not_admitted() {
        struct RejectingPusher(Arc<Mutex<Vec<usize>>>);

        impl PartitionPusher for RejectingPusher {
            fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
                self.0.lock().unwrap().push(reservation_bytes);
                Err(DataFusionError::External(Box::new(io::Error::other(
                    "scratch admission denied",
                ))))
            }

            fn release_partition_data_reservation(&self) -> Result<()> {
                panic!("a denied scratch reservation must not be released")
            }

            fn push_partition_data(&self, _partition_id: usize, _frame: &[u8]) -> Result<()> {
                panic!("a denied scratch reservation must prevent encoding and push")
            }
        }

        let limit = 1_024;
        let payload = vec![b'a'; 32 * 1_024];
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from_iter_values(
                [payload.as_slice()],
            ))],
        )
        .unwrap();
        let estimated_ipc_data_size =
            RssPartitionWriter::<RejectingPusher>::estimated_ipc_data_size(&input).unwrap();
        assert!(estimated_ipc_data_size > limit);

        let reservations = Arc::new(Mutex::new(Vec::new()));
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::Lz4Frame)
                .unwrap();
        let mut writer = RssPartitionWriter::try_new(
            RejectingPusher(Arc::clone(&reservations)),
            encoder,
            1,
            limit,
        )
        .unwrap();
        let metrics = metrics();

        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("scratch admission failure was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "scratch admission denied"
        );
        assert_eq!(*reservations.lock().unwrap(), vec![estimated_ipc_data_size]);
    }

    #[test]
    fn rss_partition_writer_releases_oversized_single_row_when_compression_cannot_fit() {
        let limit = 1_024;
        let payload = (0..16 * 1_024)
            .scan(0x243f_6a88u32, |state, _| {
                *state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                Some((*state >> 24) as u8)
            })
            .collect::<Vec<_>>();
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from_iter_values(
                [payload.as_slice()],
            ))],
        )
        .unwrap();
        let estimated_ipc_data_size =
            RssPartitionWriter::<ReservationRecordingPusher>::estimated_ipc_data_size(&input)
                .unwrap();
        assert!(estimated_ipc_data_size > limit);

        let pusher = ReservationRecordingPusher::default();
        let recorded = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::Lz4Frame)
                .unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, limit).unwrap();
        let metrics = metrics();

        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        assert!(error.to_string().contains("single RSS row exceeds"));

        let recorded = recorded.lock().unwrap();
        assert!(recorded.active.is_none());
        assert_eq!(recorded.reservations, vec![estimated_ipc_data_size]);
        assert_eq!(recorded.releases, 1);
        assert!(recorded.frames.is_empty());
    }

    #[test]
    fn rss_partition_writer_reserves_dense_dictionary_remap_before_compaction() {
        struct RejectingDictionaryPusher {
            requests: Arc<Mutex<Vec<usize>>>,
            max_frame_bytes: usize,
        }

        impl PartitionPusher for RejectingDictionaryPusher {
            fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
                self.requests.lock().unwrap().push(reservation_bytes);
                if reservation_bytes > self.max_frame_bytes {
                    return Err(DataFusionError::External(Box::new(io::Error::other(
                        "dictionary compaction admission denied",
                    ))));
                }
                Ok(())
            }

            fn release_partition_data_reservation(&self) -> Result<()> {
                panic!("a denied dictionary reservation must not be released")
            }

            fn push_partition_data(&self, _partition_id: usize, _frame: &[u8]) -> Result<()> {
                panic!("dictionary compaction must not run before admission")
            }
        }

        let value_count = 65_536;
        let limit = 1_024;
        let input = sparse_int32_dictionary_batch(vec![value_count / 2], value_count);
        let original_ipc_size =
            RssPartitionWriter::<RejectingDictionaryPusher>::estimated_ipc_data_size(&input)
                .unwrap();
        let compaction_scratch =
            RssPartitionWriter::<RejectingDictionaryPusher>::estimated_compaction_scratch(&input)
                .unwrap();
        assert!(compaction_scratch >= value_count as usize * std::mem::size_of::<i32>());
        assert!(original_ipc_size > limit);

        let requests = Arc::new(Mutex::new(Vec::new()));
        let pusher = RejectingDictionaryPusher {
            requests: Arc::clone(&requests),
            max_frame_bytes: limit,
        };
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, limit).unwrap();
        let metrics = metrics();

        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("dictionary admission failure was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "dictionary compaction admission denied"
        );
        assert_eq!(
            *requests.lock().unwrap(),
            vec![original_ipc_size + compaction_scratch]
        );
    }

    #[test]
    fn rss_partition_writer_releases_dictionary_compaction_before_splitting() {
        let value_count = 8_192;
        let limit = 1_024;
        let input = sparse_int32_dictionary_batch((0..128).collect(), value_count);
        let pusher = ReservationRecordingPusher::default();
        let recorded = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, limit).unwrap();
        let metrics = metrics();

        writer
            .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
            .unwrap();

        let recorded = recorded.lock().unwrap();
        assert!(recorded.active.is_none());
        assert!(recorded.releases > 0);
        assert!(recorded.frames.len() > 1);
        assert_eq!(
            recorded.reservations.len(),
            recorded.releases + recorded.frames.len()
        );
        assert!(recorded
            .reservations
            .iter()
            .all(|bytes| *bytes >= value_count as usize * std::mem::size_of::<i32>()));

        let decoded = recorded
            .frames
            .iter()
            .map(|frame| {
                assert!(frame.len() <= limit);
                read_ipc_compressed(&frame[16..]).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            arrow_select::concat::concat_batches(&input.schema(), &decoded).unwrap(),
            input
        );
    }

    #[test]
    fn rss_partition_writer_admits_only_live_entries_of_sliced_lists_and_maps() {
        struct BoundedAdmissionPusher {
            recorder: ReservationRecordingPusher,
            capacity: usize,
        }

        impl PartitionPusher for BoundedAdmissionPusher {
            fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
                if reservation_bytes > self.capacity {
                    return Err(DataFusionError::External(Box::new(io::Error::other(
                        "unrelated nested backing exceeded admission",
                    ))));
                }
                self.recorder.reserve_partition_data(reservation_bytes)
            }

            fn release_partition_data_reservation(&self) -> Result<()> {
                self.recorder.release_partition_data_reservation()
            }

            fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()> {
                self.recorder.push_partition_data(partition_id, frame)
            }
        }

        let value_count = 16_384;
        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(0..value_count));
        let offsets = OffsetBuffer::new((0..=value_count).collect::<Vec<i32>>().into());
        let item = Arc::new(Field::new("item", DataType::Int32, false));
        let list = ListArray::try_new(
            Arc::clone(&item),
            offsets.clone(),
            Arc::clone(&values),
            None,
        )
        .unwrap()
        .slice(value_count as usize / 2, 1);
        let list_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                DataType::List(item),
                false,
            )])),
            vec![Arc::new(list)],
        )
        .unwrap();

        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ];
        let entries = StructArray::try_new(
            fields.clone().into(),
            vec![Arc::clone(&values), Arc::clone(&values)],
            None,
        )
        .unwrap();
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.into()),
            false,
        ));
        let map = MapArray::try_new(Arc::clone(&entry), offsets, entries, None, false)
            .unwrap()
            .slice(value_count as usize / 2, 1);
        let map_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "entries",
                DataType::Map(entry, false),
                false,
            )])),
            vec![Arc::new(map)],
        )
        .unwrap();

        let frame_limit = 2_048;
        let admission_capacity = 3 * frame_limit;
        for input in [list_batch, map_batch] {
            let raw_backing_size =
                RssPartitionWriter::<BoundedAdmissionPusher>::estimated_ipc_data_size(&input)
                    .unwrap();
            assert!(raw_backing_size > admission_capacity);

            let recorder = ReservationRecordingPusher::default();
            let recorded = Arc::clone(&recorder.0);
            let pusher = BoundedAdmissionPusher {
                recorder,
                capacity: admission_capacity,
            };
            let encoder =
                ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None)
                    .unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, frame_limit).unwrap();
            let metrics = metrics();

            writer
                .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
                .unwrap();

            let recorded = recorded.lock().unwrap();
            assert_eq!(recorded.reservations.len(), 1);
            assert!(recorded.reservations[0] <= admission_capacity);
            assert_eq!(recorded.frames.len(), 1);
            assert_eq!(
                read_ipc_compressed(&recorded.frames[0][16..]).unwrap(),
                input
            );
        }
    }

    #[test]
    fn rss_partition_writer_splits_oversized_lists_and_maps_before_admission() {
        struct BoundedAdmissionPusher {
            recorder: ReservationRecordingPusher,
            capacity: usize,
        }

        impl PartitionPusher for BoundedAdmissionPusher {
            fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
                if reservation_bytes > self.capacity {
                    return Err(DataFusionError::External(Box::new(io::Error::other(
                        "nested batch exceeded admission before it was split",
                    ))));
                }
                self.recorder.reserve_partition_data(reservation_bytes)
            }

            fn release_partition_data_reservation(&self) -> Result<()> {
                self.recorder.release_partition_data_reservation()
            }

            fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()> {
                self.recorder.push_partition_data(partition_id, frame)
            }
        }

        let row_count = 4_096;
        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(0..row_count));
        let offsets = OffsetBuffer::new((0..=row_count).collect::<Vec<i32>>().into());
        let item = Arc::new(Field::new("item", DataType::Int32, false));
        let list = ListArray::try_new(
            Arc::clone(&item),
            offsets.clone(),
            Arc::clone(&values),
            None,
        )
        .unwrap();
        let list_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                DataType::List(item),
                false,
            )])),
            vec![Arc::new(list)],
        )
        .unwrap();

        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ];
        let entries = StructArray::try_new(
            fields.clone().into(),
            vec![Arc::clone(&values), Arc::clone(&values)],
            None,
        )
        .unwrap();
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.into()),
            false,
        ));
        let map =
            MapArray::try_new(Arc::clone(&entry), offsets.clone(), entries, None, false).unwrap();
        let map_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "entries",
                DataType::Map(entry, false),
                false,
            )])),
            vec![Arc::new(map)],
        )
        .unwrap();

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let tiny_dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0; row_count as usize]),
                Arc::new(Int32Array::from(vec![42])),
            )
            .unwrap(),
        );
        let dictionary_field = Field::new("dictionary", dictionary_type.clone(), false);

        let mixed_list_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                list_batch.schema().field(0).clone(),
                dictionary_field.clone(),
            ])),
            vec![
                Arc::clone(list_batch.column(0)),
                Arc::clone(&tiny_dictionary),
            ],
        )
        .unwrap();
        let mixed_map_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                map_batch.schema().field(0).clone(),
                dictionary_field,
            ])),
            vec![
                Arc::clone(map_batch.column(0)),
                Arc::clone(&tiny_dictionary),
            ],
        )
        .unwrap();

        let dictionary_item = Arc::new(Field::new("item", dictionary_type.clone(), false));
        let nested_list = ListArray::try_new(
            Arc::clone(&dictionary_item),
            offsets.clone(),
            Arc::clone(&tiny_dictionary),
            None,
        )
        .unwrap();
        let nested_dictionary_list_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                DataType::List(dictionary_item),
                false,
            )])),
            vec![Arc::new(nested_list)],
        )
        .unwrap();

        let dictionary_entries_fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", dictionary_type, false)),
        ];
        let dictionary_entries = StructArray::try_new(
            dictionary_entries_fields.clone().into(),
            vec![values, tiny_dictionary],
            None,
        )
        .unwrap();
        let dictionary_entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(dictionary_entries_fields.into()),
            false,
        ));
        let nested_map = MapArray::try_new(
            Arc::clone(&dictionary_entry),
            offsets,
            dictionary_entries,
            None,
            false,
        )
        .unwrap();
        let nested_dictionary_map_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "entries",
                DataType::Map(dictionary_entry, false),
                false,
            )])),
            vec![Arc::new(nested_map)],
        )
        .unwrap();

        let frame_limit = 2_048;
        let admission_capacity = 8_192 - 16;
        for input in [
            list_batch,
            map_batch,
            mixed_list_batch,
            mixed_map_batch,
            nested_dictionary_list_batch,
            nested_dictionary_map_batch,
        ] {
            let first_split = input.slice(0, row_count as usize / 2);
            let split_ipc_data_size =
                RssPartitionWriter::<BoundedAdmissionPusher>::estimated_pre_compaction_ipc_data_size(
                    &first_split,
                )
                .unwrap();
            let split_compaction_scratch =
                RssPartitionWriter::<BoundedAdmissionPusher>::estimated_compaction_scratch(
                    &first_split,
                )
                .unwrap();
            assert!(split_compaction_scratch > 0);
            assert!(split_ipc_data_size + split_compaction_scratch > admission_capacity);

            let recorder = ReservationRecordingPusher::default();
            let recorded = Arc::clone(&recorder.0);
            let pusher = BoundedAdmissionPusher {
                recorder,
                capacity: admission_capacity,
            };
            let encoder =
                ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None)
                    .unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, frame_limit).unwrap();
            let metrics = metrics();

            writer
                .finish_partition(0, &mut [Ok(input.clone())].into_iter(), &metrics)
                .unwrap();
            writer.finish_all(&metrics).unwrap();

            let recorded = recorded.lock().unwrap();
            assert!(recorded.active.is_none());
            assert!(recorded.frames.len() > 1);
            assert!(recorded
                .reservations
                .iter()
                .all(|reservation| *reservation <= admission_capacity));
            assert_eq!(
                recorded.reservations.len(),
                recorded.releases + recorded.frames.len()
            );

            let decoded = recorded
                .frames
                .iter()
                .map(|frame| {
                    assert!(frame.len() <= frame_limit);
                    read_ipc_compressed(&frame[16..]).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                arrow_select::concat::concat_batches(&input.schema(), &decoded).unwrap(),
                input
            );
        }
    }

    #[test]
    fn rss_partition_writer_counts_validity_offsets_and_view_backing_buffers() {
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true; 8_192]));
        let boolean_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Boolean,
                false,
            )])),
            vec![Arc::clone(&booleans)],
        )
        .unwrap();
        let raw_boolean_size = booleans.to_data().get_slice_memory_size().unwrap();
        let estimated_boolean_size =
            RssPartitionWriter::<RecordingPusher>::estimated_ipc_data_size(&boolean_batch).unwrap();
        assert!(estimated_boolean_size >= raw_boolean_size + 1_024);

        let strings: ArrayRef = Arc::new(StringArray::from(vec!["x"; 8_192]));
        let sliced_strings = strings.slice(1, 8_191);
        let string_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::clone(&sliced_strings)],
        )
        .unwrap();
        let raw_string_size = sliced_strings.to_data().get_slice_memory_size().unwrap();
        let estimated_string_size =
            RssPartitionWriter::<RecordingPusher>::estimated_ipc_data_size(&string_batch).unwrap();
        assert!(estimated_string_size >= raw_string_size + 8_192 * 4);

        let views: ArrayRef = Arc::new(StringViewArray::from(vec![
            "small".to_owned(),
            "unused".repeat(1_024),
        ]));
        let sliced = views.slice(0, 1);
        let view_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::clone(&sliced)],
        )
        .unwrap();
        assert!(sliced.to_data().get_slice_memory_size().unwrap() < 128);
        assert!(
            RssPartitionWriter::<RecordingPusher>::estimated_ipc_data_size(&view_batch).unwrap()
                > 6_000
        );
    }

    #[test]
    fn rss_partition_writer_reserves_only_batches_that_are_small_enough_to_encode() {
        #[derive(Default)]
        struct ReservationState {
            reserved: bool,
            reservations: usize,
            releases: usize,
            pushes: usize,
        }

        struct TrackingPusher(Arc<Mutex<ReservationState>>);

        impl PartitionPusher for TrackingPusher {
            fn reserve_partition_data(&self, reservation_bytes: usize) -> Result<()> {
                let mut state = self.0.lock().unwrap();
                assert!(!state.reserved);
                assert_eq!(reservation_bytes, 3 * 1_024);
                state.reserved = true;
                state.reservations += 1;
                Ok(())
            }

            fn release_partition_data_reservation(&self) -> Result<()> {
                let mut state = self.0.lock().unwrap();
                assert!(state.reserved);
                state.reserved = false;
                state.releases += 1;
                Ok(())
            }

            fn push_partition_data(&self, _partition_id: usize, _frame: &[u8]) -> Result<()> {
                let mut state = self.0.lock().unwrap();
                assert!(state.reserved);
                state.reserved = false;
                state.pushes += 1;
                Ok(())
            }
        }

        let input = batch((0..512).collect());
        let state = Arc::new(Mutex::new(ReservationState::default()));
        let pusher = TrackingPusher(Arc::clone(&state));
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 1_024).unwrap();
        let metrics = metrics();

        writer
            .finish_partition(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap();

        let state = state.lock().unwrap();
        assert!(!state.reserved);
        assert!(state.pushes > 1);
        // Wide batches are now split before admission, so no reservation is wasted on an
        // encoding attempt already known to exceed the Arrow IPC scratch-space budget.
        assert_eq!(state.releases, 0);
        assert_eq!(state.reservations, state.releases + state.pushes);
    }

    #[test]
    fn rss_partition_writer_compacts_dictionary_values_before_splitting_frames() {
        let values: Vec<String> = (0..256)
            .map(|index| {
                format!(
                    "value-{index:03}-{}",
                    (0..72)
                        .map(|offset| char::from(b'a' + ((index + offset) % 26) as u8))
                        .collect::<String>()
                )
            })
            .collect();
        let expected: Vec<String> = (0..96).map(|index| values[index * 2].clone()).collect();
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from((0..96).map(|index| index * 2).collect::<Vec<i32>>()),
            Arc::new(StringArray::from(values.clone())),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let input = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)]).unwrap();
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder = ShuffleBlockWriter::try_new(schema.as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 2_048).unwrap();
        let metrics = metrics();

        writer
            .finish_partition(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap();
        writer.finish_all(&metrics).unwrap();

        let frames = captured.lock().unwrap();
        assert!(frames.len() > 1);
        let mut actual = Vec::new();
        for (_, frame) in frames.iter() {
            assert!(frame.len() <= 2_048);
            let decoded = read_ipc_compressed(&frame[16..]).unwrap();
            let dictionary = decoded
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            assert!(dictionary.values().len() < values.len());
            let plain = cast(dictionary, &DataType::Utf8).unwrap();
            let strings = plain.as_any().downcast_ref::<StringArray>().unwrap();
            actual.extend(strings.iter().map(|value| value.unwrap().to_owned()));
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn rss_partition_writer_compacts_an_oversized_dictionary_for_a_single_row() {
        let mut values = vec!["small".to_owned()];
        values.extend((0..64).map(|index| format!("unused-{index}-{}", "x".repeat(256))));
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(values)),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)]).unwrap();
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder = ShuffleBlockWriter::try_new(schema.as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 1_024).unwrap();
        let metrics = metrics();

        writer
            .finish_partition(0, &mut [Ok(batch)].into_iter(), &metrics)
            .unwrap();

        let frames = captured.lock().unwrap();
        assert_eq!(frames.len(), 1);
        let decoded = read_ipc_compressed(&frames[0].1[16..]).unwrap();
        let dictionary = decoded
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.values().len(), 1);
        assert_eq!(
            dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "small"
        );
    }

    #[test]
    fn rss_partition_writer_compacts_dictionaries_nested_in_sliced_lists_and_structs() {
        let values = (0..128)
            .map(|index| format!("value-{index:03}-{}", "x".repeat(128)))
            .collect::<Vec<_>>();
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from((0..32).collect::<Vec<_>>()),
            Arc::new(StringArray::from(values.clone())),
        )
        .unwrap();
        let item = Arc::new(Field::new("item", dictionary_type, false));
        let lists = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new((0..=32).collect::<Vec<i32>>().into()),
            Arc::new(dictionary),
            None,
        )
        .unwrap();
        let nested = Arc::new(Field::new("items", DataType::List(item), false));
        let structure = StructArray::try_new(
            vec![Arc::clone(&nested)].into(),
            vec![Arc::new(lists)],
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(vec![nested].into()),
            false,
        )]));
        let input = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(structure)])
            .unwrap()
            .slice(7, 16);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder = ShuffleBlockWriter::try_new(schema.as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 2_048).unwrap();
        let metrics = metrics();

        writer
            .finish_partition(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap();

        let frames = captured.lock().unwrap();
        assert!(frames.len() > 1);
        let mut actual = Vec::new();
        for (_, frame) in frames.iter() {
            assert!(frame.len() <= 2_048);
            let decoded = read_ipc_compressed(&frame[16..]).unwrap();
            let structure = decoded
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let lists = structure
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(lists.value_offsets()[0], 0);
            let dictionary = lists
                .values()
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            assert!(dictionary.values().len() < values.len());
            let plain = cast(dictionary, &DataType::Utf8).unwrap();
            actual.extend(
                plain
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned()),
            );
        }
        assert_eq!(actual, values[7..23].to_vec());
    }

    #[test]
    fn rss_partition_writer_preserves_transport_errors() {
        struct FailingPusher;
        impl PartitionPusher for FailingPusher {
            fn push_partition_data(&self, _pid: usize, _frame: &[u8]) -> Result<()> {
                Err(DataFusionError::External(Box::new(io::Error::other(
                    "push failed",
                ))))
            }
        }
        let input = batch(vec![1]);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(FailingPusher, encoder, 1, 1024).unwrap();
        let metrics = metrics();
        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("transport error was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "push failed"
        );
    }

    #[test]
    fn rss_partition_writer_stays_failed_after_input_error() {
        let input = batch(vec![1]);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 1024).unwrap();
        let metrics = metrics();
        let error = DataFusionError::External(Box::new(io::Error::other("input failed")));
        let error = writer
            .write(
                0,
                &mut [Ok(input.clone()), Err(error)].into_iter(),
                &metrics,
            )
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("input error was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "input failed"
        );
        assert_eq!(captured.lock().unwrap().len(), 1);
        assert!(writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .is_err());
        assert!(writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .is_err());
        assert!(writer.finish_all(&metrics).is_err());
        assert_eq!(captured.lock().unwrap().len(), 1);
    }

    #[test]
    fn rss_partition_writer_buffer_bounds_seek_and_write() {
        let mut buffer = BoundedBuffer::new(4);
        buffer.write_all(&[1, 2, 3, 4]).unwrap();
        assert!(buffer.write_all(&[5]).is_err());
        assert!(buffer.seek(SeekFrom::Start(5)).is_err());
        buffer.seek(SeekFrom::Start(0)).unwrap();
        buffer.write_all(&[9]).unwrap();
        assert_eq!(buffer.inner.into_inner(), vec![9, 2, 3, 4]);
    }
}
