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

use super::{IpcStreamWriter, ShuffleBlockWriter};
use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::PartitionedBatchIterator;
use crate::writers::buf_batch_writer::BufBatchWriter;
use crate::ShuffleFormat;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek};

/// Manages encoding and optional disk spilling for a single shuffle partition.
///
/// For block format, each `spill()` call appends self-contained blocks to the
/// spill file. For IPC stream format, an `IpcStreamWriter` is kept open across
/// spill calls so that all spilled data forms a single IPC stream (one schema
/// header, many batch messages, one EOS marker written at finish time).
pub(crate) struct PartitionWriter {
    spill_file: Option<RefCountedTempFile>,
    shuffle_block_writer: ShuffleBlockWriter,
    /// Persistent IPC stream writer for the spill file, kept open across
    /// multiple `spill()` calls.
    ipc_spill_writer: Option<IpcStreamWriter<BufWriter<File>>>,
    /// Start position of the current IPC stream in the spill file (for length prefix).
    ipc_spill_start_pos: u64,
    format: ShuffleFormat,
}

impl PartitionWriter {
    pub(crate) fn try_new(
        shuffle_block_writer: ShuffleBlockWriter,
        format: ShuffleFormat,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            spill_file: None,
            shuffle_block_writer,
            ipc_spill_writer: None,
            ipc_spill_start_pos: 0,
            format,
        })
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            self.spill_file = Some(
                runtime
                    .disk_manager
                    .create_tmp_file("shuffle writer spill")?,
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spill(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        write_buffer_size: usize,
        batch_size: usize,
        codec: &crate::CompressionCodec,
        schema: &arrow::datatypes::Schema,
    ) -> datafusion::common::Result<usize> {
        let Some(batch) = iter.next() else {
            return Ok(0);
        };

        self.ensure_spill_file_created(runtime)?;
        let spill_path = self.spill_file.as_ref().unwrap().path().to_owned();

        match &self.format {
            ShuffleFormat::Block => {
                let mut file = OpenOptions::new()
                    .append(true)
                    .open(&spill_path)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Error occurred while spilling {e}"))
                    })?;
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    &mut file,
                    write_buffer_size,
                    batch_size,
                );
                let mut bytes_written =
                    buf_batch_writer.write(&batch?, &metrics.encode_time, &metrics.write_time)?;
                for batch in iter {
                    bytes_written += buf_batch_writer.write(
                        &batch?,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
                buf_batch_writer.flush(&metrics.encode_time, &metrics.write_time)?;
                Ok(bytes_written)
            }
            ShuffleFormat::IpcStream => {
                // Lazily open the IPC stream writer on first spill. It stays
                // open so subsequent spills append batches to the same stream.
                // Uses length prefix so the spill file can be raw-copied into
                // the output and the reader can frame it.
                if self.ipc_spill_writer.is_none() {
                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&spill_path)
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Error occurred while spilling {e}"))
                        })?;
                    let mut buf_writer = BufWriter::with_capacity(write_buffer_size, file);
                    self.ipc_spill_start_pos = buf_writer.stream_position()?;
                    self.ipc_spill_writer = Some(IpcStreamWriter::try_new_length_prefixed(
                        buf_writer,
                        schema,
                        codec.clone(),
                    )?);
                }
                let ipc_writer = self.ipc_spill_writer.as_mut().unwrap();
                ipc_writer.write_batch(&batch?, &metrics.encode_time)?;
                for batch in iter {
                    ipc_writer.write_batch(&batch?, &metrics.encode_time)?;
                }
                Ok(0)
            }
        }
    }

    /// Finish the IPC spill stream writer if one is open. Must be called
    /// before raw-copying the spill file to the output.
    pub(crate) fn finish_spill(&mut self) -> datafusion::common::Result<()> {
        if let Some(writer) = self.ipc_spill_writer.take() {
            let buf_writer = writer.finish_length_prefixed(self.ipc_spill_start_pos)?;
            buf_writer
                .into_inner()
                .map_err(|e| DataFusionError::Execution(format!("flush error: {e}")))?;
        }
        Ok(())
    }

    pub(crate) fn path(&self) -> Option<&std::path::Path> {
        self.spill_file.as_ref().map(|f| f.path())
    }

    #[cfg(test)]
    pub(crate) fn has_spill_file(&self) -> bool {
        self.spill_file.is_some()
    }
}
