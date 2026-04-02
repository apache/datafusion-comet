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
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};

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
    /// Schema used for creating IPC stream writers
    schema: SchemaRef,
    /// IPC write options (includes compression settings)
    write_options: IpcWriteOptions,
}

impl PartitionWriter {
    pub(crate) fn try_new(
        schema: SchemaRef,
        write_options: IpcWriteOptions,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            spill_file: None,
            schema,
            write_options,
        })
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            // Spill file is not yet created, create it
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

    pub(crate) fn spill(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<usize> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            let file = &mut self.spill_file.as_mut().unwrap().file;
            let start_pos = file.metadata().map(|m| m.len()).unwrap_or(0);

            let mut writer = StreamWriter::try_new_with_options(
                file,
                &self.schema,
                self.write_options.clone(),
            )?;

            let batch = batch?;
            let mut encode_timer = metrics.encode_time.timer();
            writer.write(&batch)?;
            encode_timer.stop();

            for batch in iter {
                let batch = batch?;
                let mut encode_timer = metrics.encode_time.timer();
                writer.write(&batch)?;
                encode_timer.stop();
            }

            let mut write_timer = metrics.write_time.timer();
            writer.finish()?;
            write_timer.stop();

            let end_pos = self
                .spill_file
                .as_ref()
                .unwrap()
                .file
                .metadata()
                .map(|m| m.len())
                .unwrap_or(0);

            Ok((end_pos - start_pos) as usize)
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
