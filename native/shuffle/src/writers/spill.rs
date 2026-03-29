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
use std::io::BufWriter;

/// A temporary disk file for spilling a partition's intermediate shuffle data.
struct SpillFile {
    temp_file: RefCountedTempFile,
    file: File,
}

/// Manages encoding and optional disk spilling for a single shuffle partition.
pub(crate) struct PartitionWriter {
    /// Spill file for intermediate shuffle output for this partition. Each spill event
    /// will append to this file and the contents will be copied to the shuffle file at
    /// the end of processing.
    spill_file: Option<SpillFile>,
    /// Writer that performs encoding and compression (block format)
    shuffle_block_writer: ShuffleBlockWriter,
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
            format,
        })
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            let spill_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let spill_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(spill_file.path())
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error occurred while spilling {e}"))
                })?;
            self.spill_file = Some(SpillFile {
                temp_file: spill_file,
                file: spill_data,
            });
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
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;
            let file = &mut self.spill_file.as_mut().unwrap().file;

            match &self.format {
                ShuffleFormat::Block => {
                    let mut buf_batch_writer = BufBatchWriter::new(
                        &mut self.shuffle_block_writer,
                        file,
                        write_buffer_size,
                        batch_size,
                    );
                    let mut bytes_written = buf_batch_writer.write(
                        &batch?,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
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
                    let buf_writer = BufWriter::with_capacity(write_buffer_size, file);
                    let mut ipc_writer =
                        IpcStreamWriter::try_new(buf_writer, schema, codec.clone())?;
                    ipc_writer.write_batch(&batch?, &metrics.encode_time)?;
                    for batch in iter {
                        ipc_writer.write_batch(&batch?, &metrics.encode_time)?;
                    }
                    let buf_writer = ipc_writer.finish()?;
                    buf_writer.into_inner().map_err(|e| {
                        DataFusionError::Execution(format!("flush error: {e}"))
                    })?;
                    // IPC stream doesn't easily track bytes written; return 0
                    // (spilled_bytes metric will be approximate)
                    Ok(0)
                }
            }
        } else {
            Ok(0)
        }
    }

    pub(crate) fn path(&self) -> Option<&std::path::Path> {
        self.spill_file
            .as_ref()
            .map(|spill_file| spill_file.temp_file.path())
    }

    #[cfg(test)]
    pub(crate) fn has_spill_file(&self) -> bool {
        self.spill_file.is_some()
    }
}
