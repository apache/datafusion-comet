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

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::PartitionedBatchIterator;
use crate::writers::buf_batch_writer::BufBatchWriter;
use crate::ShuffleBlockWriter;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};

struct SpillFile {
    temp_file: RefCountedTempFile,
}

pub(crate) struct PartitionWriter {
    /// Spill file for intermediate shuffle output for this partition. Each spill event
    /// will append to this file and the contents will be copied to the shuffle file at
    /// the end of processing.
    spill_file: Option<SpillFile>,
    /// Writer that performs encoding and compression
    shuffle_block_writer: ShuffleBlockWriter,
}

impl PartitionWriter {
    pub(crate) fn try_new(
        shuffle_block_writer: ShuffleBlockWriter,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            spill_file: None,
            shuffle_block_writer,
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
            // Create the file (truncating any pre-existing content)
            File::create(spill_file.path()).map_err(|e| {
                DataFusionError::Execution(format!("Error occurred while spilling {e}"))
            })?;
            self.spill_file = Some(SpillFile {
                temp_file: spill_file,
            });
        }
        Ok(())
    }

    fn open_spill_file_for_append(&self) -> datafusion::common::Result<File> {
        OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.spill_file.as_ref().unwrap().temp_file.path())
            .map_err(|e| DataFusionError::Execution(format!("Error occurred while spilling {e}")))
    }

    pub(crate) fn spill(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<usize> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            // Open the file for this spill and close it when done, so we don't
            // hold open one FD per partition across multiple spill events.
            let mut spill_data = self.open_spill_file_for_append()?;
            let total_bytes_written = {
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    &mut spill_data,
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
            // spill_data is dropped here, closing the file descriptor

            Ok(total_bytes_written)
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
