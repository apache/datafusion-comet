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
use crate::writers::BufBatchWriter;
use crate::ShuffleBlockWriter;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SpillFile as DfSpillFile;
use datafusion::execution::SpillWriter as DfSpillWriter;
use std::sync::Arc;

struct ActiveSpillFile {
    temp_file: Arc<dyn DfSpillFile>,
    writer: Box<dyn DfSpillWriter>,
}

pub(crate) struct SpillWriter {
    shuffle_block_writer: ShuffleBlockWriter,
    write_buffer_size: usize,
    batch_size: usize,
    spill_file: Option<ActiveSpillFile>,
}

impl SpillWriter {
    pub(crate) fn try_new(
        shuffle_block_writer: ShuffleBlockWriter,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            shuffle_block_writer,
            write_buffer_size,
            batch_size,
            spill_file: None,
        })
    }

    /// `recycled_buffer` is a scratch byte buffer shared by the sequential per-partition
    /// spill writes; it is left drained on return so one buffer's capacity serves every
    /// partition instead of each write regrowing its own.
    pub(crate) fn write<I: Iterator<Item = datafusion::common::Result<RecordBatch>>>(
        &mut self,
        iter: &mut I,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        recycled_buffer: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            let result = (|| {
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    &mut self.spill_file.as_mut().unwrap().writer,
                    self.write_buffer_size,
                    self.batch_size,
                );
                buf_batch_writer.write(
                    &batch?,
                    recycled_buffer,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
                for batch in iter.by_ref() {
                    let batch = batch?;
                    buf_batch_writer.write(
                        &batch,
                        recycled_buffer,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
                buf_batch_writer.flush(
                    recycled_buffer,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
                // `SpillWriter` is not `Seek`, so bytes are tracked by the writer itself rather
                // than measured via stream position.
                let bytes_written = buf_batch_writer.bytes_written();
                usize::try_from(bytes_written).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Spill file byte count exceeds platform capacity: {bytes_written}"
                    ))
                })
            })();
            // An errored spill must hand back a drained buffer, or its bytes leak into
            // the next partition's block.
            let total_bytes_written = result.inspect_err(|_| recycled_buffer.clear())?;
            metrics.spilled_bytes.add(total_bytes_written);
        }
        Ok(())
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            // Spill file is not yet created, create it
            let temp_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let writer = temp_file.open_writer()?;
            self.spill_file = Some(ActiveSpillFile { temp_file, writer });
        }
        Ok(())
    }

    pub(crate) fn path(&self) -> Option<&std::path::Path> {
        self.spill_file
            .as_ref()
            .and_then(|spill_file| spill_file.temp_file.path())
    }

    #[cfg(test)]
    pub(crate) fn has_spill_file(&self) -> bool {
        self.spill_file.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CompressionCodec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use std::sync::Arc;

    /// A spill whose batch iterator fails after a batch was already encoded must hand
    /// back a drained scratch; leftover bytes would land in the next partition's block.
    #[test]
    fn write_error_drains_recycled_buffer() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..100))],
        )
        .unwrap();
        let block_writer =
            ShuffleBlockWriter::try_new(schema.as_ref(), CompressionCodec::None).unwrap();
        // batch_size below the row count so the first write serializes into the scratch.
        let mut spill = SpillWriter::try_new(block_writer, 1 << 20, 10).unwrap();
        let runtime = RuntimeEnv::default();
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut recycled = Vec::new();
        let mut iter = vec![
            Ok(batch),
            Err(DataFusionError::Execution("injected failure".to_string())),
        ]
        .into_iter();

        assert!(spill
            .write(&mut iter, &runtime, &metrics, &mut recycled)
            .is_err());
        assert!(
            recycled.is_empty(),
            "errored spill left {} bytes in the recycled buffer",
            recycled.len()
        );
    }
}
