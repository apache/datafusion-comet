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

use super::ShuffleBlockWriter;
use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::PartitionedBatchIterator;
use crate::writers::buf_batch_writer::BufBatchWriter;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

/// The byte range of a single partition's data within a combined spill file.
#[derive(Debug, Clone)]
pub(crate) struct PartitionSpillRange {
    pub offset: u64,
    pub length: u64,
}

/// Represents a single spill file that contains data from multiple partitions.
/// Data is written sequentially ordered by partition ID. Each partition's byte
/// range is tracked in `partition_ranges` so it can be read back during merge.
pub(crate) struct SpillInfo {
    /// The temporary file handle -- kept alive to prevent cleanup until we are done.
    _temp_file: RefCountedTempFile,
    /// Path to the spill file on disk.
    path: std::path::PathBuf,
    /// Byte range for each partition. None means the partition had no data in this spill.
    pub partition_ranges: Vec<Option<PartitionSpillRange>>,
}

impl SpillInfo {
    pub(crate) fn new(
        temp_file: RefCountedTempFile,
        partition_ranges: Vec<Option<PartitionSpillRange>>,
    ) -> Self {
        let path = temp_file.path().to_path_buf();
        Self {
            _temp_file: temp_file,
            path,
            partition_ranges,
        }
    }

    /// Copy the data for `partition_id` using a pre-opened file handle.
    /// Avoids repeated File::open() calls when iterating over partitions.
    /// Returns the number of bytes copied.
    pub(crate) fn copy_partition_with_handle(
        &self,
        partition_id: usize,
        spill_file: &mut File,
        output: &mut impl Write,
    ) -> datafusion::common::Result<u64> {
        if let Some(ref range) = self.partition_ranges[partition_id] {
            if range.length == 0 {
                return Ok(0);
            }
            spill_file.seek(SeekFrom::Start(range.offset))?;
            let mut limited = Read::take(spill_file, range.length);
            let copied = std::io::copy(&mut limited, output)?;
            Ok(copied)
        } else {
            Ok(0)
        }
    }

    /// Open the spill file for reading. The returned handle can be reused
    /// across multiple copy_partition_with_handle() calls.
    pub(crate) fn open_for_read(&self) -> datafusion::common::Result<File> {
        File::open(&self.path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to open spill file for reading: {e}"))
        })
    }
}

/// Manages encoding for a single shuffle partition. Does not own any spill file --
/// spill files are managed at the repartitioner level as combined SpillInfo objects.
pub(crate) struct PartitionWriter {
    /// Writer that performs encoding and compression.
    shuffle_block_writer: ShuffleBlockWriter,
}

impl PartitionWriter {
    pub(crate) fn try_new(
        shuffle_block_writer: ShuffleBlockWriter,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            shuffle_block_writer,
        })
    }

    /// Encode and write a partition's batches to the provided writer.
    /// Returns the number of bytes written.
    pub(crate) fn write_to<W: Write>(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        writer: &mut W,
        metrics: &ShufflePartitionerMetrics,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<usize> {
        if let Some(batch) = iter.next() {
            let total_bytes_written = {
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    writer,
                    write_buffer_size,
                    batch_size,
                );
                let mut bytes_written =
                    buf_batch_writer.write(&batch?, &metrics.encode_time, &metrics.write_time)?;
                for batch in iter {
                    let batch = batch?;
                    bytes_written += buf_batch_writer.write(
                        &batch,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
                buf_batch_writer.flush(&metrics.encode_time, &metrics.write_time)?;
                bytes_written
            };
            Ok(total_bytes_written)
        } else {
            Ok(0)
        }
    }
}
