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
use crate::partitioners::ShufflePartitioner;
use crate::writers::BufBatchWriter;
use crate::CompressionCodec;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use tokio::time::Instant;

/// A partitioner that writes all shuffle data to a single file and a single index file.
/// Uses a persistent Arrow IPC StreamWriter via BufBatchWriter, so the schema is written
/// once and batches are appended with built-in body compression.
pub(crate) struct SinglePartitionShufflePartitioner {
    output_data_writer: BufBatchWriter<File>,
    output_data_path: String,
    output_index_path: String,
    /// Metrics for the repartitioner
    metrics: ShufflePartitionerMetrics,
}

impl SinglePartitionShufflePartitioner {
    pub(crate) fn try_new(
        output_data_path: String,
        output_index_path: String,
        schema: SchemaRef,
        metrics: ShufflePartitionerMetrics,
        batch_size: usize,
        codec: CompressionCodec,
        _write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let write_options = codec.ipc_write_options()?;

        let output_data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&output_data_path)?;

        let output_data_writer =
            BufBatchWriter::try_new(output_data_file, schema, write_options, batch_size)?;

        Ok(Self {
            output_data_writer,
            output_data_path,
            output_index_path,
            metrics,
        })
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for SinglePartitionShufflePartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        let start_time = Instant::now();
        let num_rows = batch.num_rows();

        if num_rows > 0 {
            self.metrics.data_size.add(batch.get_array_memory_size());
            self.metrics.baseline.record_output(num_rows);

            self.output_data_writer.write(
                &batch,
                &self.metrics.encode_time,
                &self.metrics.write_time,
            )?;
        }

        self.metrics.input_batches.add(1);
        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }

    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        let start_time = Instant::now();

        self.output_data_writer
            .flush(&self.metrics.encode_time, &self.metrics.write_time)?;

        // Get data file length via filesystem metadata
        let data_file_length = std::fs::metadata(&self.output_data_path)
            .map(|m| m.len())
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

        // Write index file. It should only contain 2 entries: 0 and the total number of bytes written
        let index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.output_index_path)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let mut index_buf_writer = BufWriter::new(index_file);
        for offset in [0u64, data_file_length] {
            index_buf_writer.write_all(&(offset as i64).to_le_bytes()[..])?;
        }
        index_buf_writer.flush()?;

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}
